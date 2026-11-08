package main

// M5 on MinIO: concurrent takeovers, the same operation ID on several nodes,
// and a caller-side routing client whose histories are checked for
// per-entity linearizability with porcupine (PLAN M5, §4.3, §15.2, §16).

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"net/http"
	"os"
	"slices"
	"strconv"
	"sync"
	"syscall"
	"testing"
	"time"

	garden "github.com/OrdalieTech/big-garden"
	"github.com/OrdalieTech/big-garden/internal/storetest"
)

func killRuns() int {
	if s := os.Getenv("GARDEN_KILL_RUNS"); s != "" {
		n, _ := strconv.Atoi(s)
		return n
	}
	return 20
}

// endpoint is one node a test client can address: a node process (mnode) or
// an in-process garden.Node running the same serve function (inproc).
type endpoint interface {
	do(r Req, timeout time.Duration) (Resp, bool)
}

// mnode multiplexes concurrent tagged requests over one node process. A
// request that times out stays pending in the process (a stopped process
// runs it after SIGCONT); its late reply is dropped.
type mnode struct {
	name  string
	cache string // removed by kill: a run's caches must not pile up in /tmp
	p     *proc
	mu    sync.Mutex
	wait  map[string]chan Resp
	dead  bool
	tags  int
}

func startNode(t *testing.T, ep storetest.Endpoint, name string, args ...string) *mnode {
	cache, err := os.MkdirTemp("", "killnode-cache")
	if err != nil {
		t.Fatal(err)
	}
	p := start(t, ep, cache, "", append([]string{"-node", name}, args...)...)
	m := &mnode{name: name, cache: cache, p: p, wait: map[string]chan Resp{}}
	go func() {
		for line := range p.lines {
			var r Resp
			if json.Unmarshal(line, &r) != nil {
				continue
			}
			m.mu.Lock()
			c := m.wait[r.Tag]
			delete(m.wait, r.Tag)
			m.mu.Unlock()
			if c != nil {
				c <- r
			}
		}
		m.mu.Lock()
		m.dead = true
		for k, c := range m.wait {
			close(c)
			delete(m.wait, k)
		}
		m.mu.Unlock()
	}()
	return m
}

func (m *mnode) do(r Req, timeout time.Duration) (Resp, bool) {
	c := m.send(r)
	if c == nil {
		return Resp{}, false
	}
	select {
	case resp, ok := <-c:
		return resp, ok
	case <-time.After(timeout):
		return Resp{}, false
	}
}

// send writes r and returns the channel its reply arrives on (nil: dead).
func (m *mnode) send(r Req) chan Resp {
	c := make(chan Resp, 1)
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.dead {
		return nil
	}
	m.tags++
	r.Tag = strconv.Itoa(m.tags)
	m.wait[r.Tag] = c
	b, _ := json.Marshal(r)
	if _, err := m.p.in.Write(append(b, '\n')); err != nil {
		delete(m.wait, r.Tag)
		return nil
	}
	return c
}

func (m *mnode) signal(s syscall.Signal) { m.p.cmd.Process.Signal(s) }

// kill SIGKILLs the process, reaps it and deletes its cache directory.
func (m *mnode) kill() {
	m.p.cmd.Process.Signal(syscall.SIGCONT)
	m.p.cmd.Process.Kill()
	m.p.stop()
	os.RemoveAll(m.cache)
}

// resolveAfterStall resolves key on e. While a SIGSTOPped process is stopped
// between the headers and the body of its head CAS (If-Match PUT), the pinned
// MinIO holds the object's lock and every GET of that head blocks until the
// server drops the stalled request after about 30 s (measured: an
// unconditional PUT does not block readers). The library then correctly
// answers unavailable; this waits up to 60 s for the store to recover and
// reports whether it had to. Any other failure is fatal.
func resolveAfterStall(t *testing.T, e endpoint, key string) (*Rt, bool) {
	t.Helper()
	for deadline, waited := time.Now().Add(60*time.Second), false; ; waited = true {
		r, ok := e.do(Req{Op: "resolve", Key: key}, 60*time.Second)
		if ok && r.OK {
			return r.Route, waited
		}
		if !ok || r.Code != "unavailable" || time.Now().After(deadline) {
			t.Fatalf("resolve %s: %+v", key, r)
		}
		time.Sleep(500 * time.Millisecond)
	}
}

type inproc struct{ n *garden.Node }

func (x inproc) do(r Req, _ time.Duration) (Resp, bool) {
	return serve(context.Background(), x.n, r), true
}

// openInproc opens an in-process Node; the returned func closes it and
// deletes its cache directory.
func openInproc(t *testing.T, ep storetest.Endpoint, name string) (inproc, func()) {
	cache, err := os.MkdirTemp("", "killnode-inproc")
	if err != nil {
		t.Fatal(err)
	}
	hc := &http.Client{Transport: http.DefaultTransport.(*http.Transport).Clone()}
	n, err := garden.Open(context.Background(), garden.Config{S3: ep.Client(hc), Bucket: ep.Bucket, Prefix: ep.Prefix,
		NodeID: name, CacheDir: cache})
	if err != nil {
		t.Fatal(err)
	}
	return inproc{n}, func() { n.Close(context.Background()); hc.CloseIdleConnections(); os.RemoveAll(cache) }
}

// ledger is the parent-side acknowledgment record of one entity.
type ledger struct {
	mu        sync.Mutex
	committed map[string]Resp // acknowledged or retried-to-ack add IDs -> first ack
	absent    map[string]bool // definitely failed adds: must never appear
}

func newLedger() *ledger { return &ledger{committed: map[string]Resp{}, absent: map[string]bool{}} }

// ack records an acknowledged add; every ack of one ID must agree on value
// and sequence (one committed effect, exact replay).
func (l *ledger) ack(t *testing.T, id string, r Resp) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if prev, ok := l.committed[id]; ok && (prev.Value != r.Value || prev.Seq != r.Seq) {
		t.Errorf("ID %s acknowledged twice with different results: %+v and %+v", id, prev, r)
	}
	if _, ok := l.committed[id]; !ok {
		l.committed[id] = r
	}
}

// check reads the entity through a fresh node process (empty cache, new
// session, explicit takeover: recovery from S3 alone) and checks the ledger:
// every acknowledged ID exactly once at the position its count names, no
// ID twice, no definitely failed or never-acknowledged-nor-retried ID.
func (l *ledger) check(t *testing.T, ep storetest.Endpoint, key, inc string, issued map[string]bool) []string {
	t.Helper()
	z := startNode(t, ep, "z"+storetest.RandomHex(2))
	defer z.kill()
	if r, ok := z.do(Req{Op: "takeover", Key: key}, 30*time.Second); !ok || !r.OK {
		t.Fatalf("%s: recovery takeover: %+v", key, r)
	}
	rd, ok := z.do(Req{Op: "read", Key: key, Inc: inc}, 30*time.Second)
	if !ok || !rd.OK {
		t.Fatalf("%s: recovery read: %+v", key, rd)
	}
	pos := map[string]int{}
	for i, id := range rd.IDs {
		if _, dup := pos[id]; dup {
			t.Errorf("%s: ID %s committed twice", key, id)
		}
		pos[id] = i + 1
		_, acked := l.committed[id]
		if l.absent[id] || (!acked && !issued[id]) {
			t.Errorf("%s: ID %s committed but definitely failed or never issued (acked %v)", key, id, acked)
		}
	}
	for id, r := range l.committed {
		p, ok := pos[id]
		if !ok {
			t.Errorf("%s: acknowledged ID %s lost", key, id)
		} else if r.Value != strconv.Itoa(p) {
			t.Errorf("%s: acknowledged ID %s answered %s but sits at %d", key, id, r.Value, p)
		}
	}
	return rd.IDs
}

// TestConcurrentTakeover: owner a (a process) is SIGSTOPped with an add in
// flight; then in each of three rounds, three node processes and 32
// in-process nodes call Takeover on the same observed Route at once. Exactly
// one wins per epoch, every loser gets route_changed or not_owner carrying the
// winner's Route, the previous owner and the losers cannot commit, the
// resumed a publishes nothing new, and the ledger survives.
func TestConcurrentTakeover(t *testing.T) {
	base := storetest.MinIO(t)
	runs := killRuns()
	var procWins, inWins, staleAcks, stalls int
	for i := range runs {
		ep := base.Sub(fmt.Sprintf("takeover-%d", i))
		p, n, s, w := takeoverRun(t, ep)
		procWins, inWins, staleAcks, stalls = procWins+p, inWins+n, staleAcks+s, stalls+w
		clean(t, ep)
		if t.Failed() {
			return
		}
	}
	t.Logf("%d runs x 3 rounds of 35 candidates: %d rounds won by a process, %d by an in-process node; stopped owner's in-flight add acknowledged in %d runs; %d resolves waited for MinIO to drop a stalled CAS",
		runs, procWins, inWins, staleAcks, stalls)
}

func takeoverRun(t *testing.T, ep storetest.Endpoint) (procWins, inWins, staleAck, stalls int) {
	a := startNode(t, ep, "a")
	defer a.kill()
	cands := map[string]endpoint{}
	for _, name := range []string{"b", "c", "d"} {
		m := startNode(t, ep, name)
		defer m.kill()
		cands[name] = m
	}
	for i := range 32 {
		name := fmt.Sprintf("m%02d", i)
		x, closer := openInproc(t, ep, name)
		defer closer()
		cands[name] = x
	}
	led := newLedger()
	issued := map[string]bool{}
	must := func(e endpoint, r Req) Resp {
		t.Helper()
		if r.ID != "" {
			issued[r.ID] = true
		}
		resp, ok := e.do(r, 30*time.Second)
		if !ok || !resp.OK {
			t.Fatalf("%+v: %+v", r, resp)
		}
		if r.Op == "add" {
			led.ack(t, r.ID, resp)
		}
		return resp
	}
	inc := must(a, Req{Op: "create", Key: "k", ID: newID()}).Inc
	for i := range 3 {
		must(a, Req{Op: "add", Key: "k", Inc: inc, ID: newID(), Input: strconv.Itoa(i), Big: i == 1})
	}
	pending := Req{Op: "add", Key: "k", Inc: inc, ID: newID(), Input: "pending", Big: rand.N(2) == 0}
	issued[pending.ID] = true
	pc := a.send(pending)
	time.Sleep(time.Duration(rand.N(8)) * time.Millisecond)
	a.signal(syscall.SIGSTOP)

	owner := endpoint(a)
	ownerName := "a"
	for round := range 3 {
		rt, waited := resolveAfterStall(t, cands["b"], "k")
		if waited {
			stalls++
		}
		if rt.Node != ownerName {
			t.Fatalf("round %d: resolved owner %q, expected %q", round, rt.Node, ownerName)
		}
		type res struct {
			name string
			resp Resp
			ok   bool
		}
		out := make(chan res, len(cands))
		var gate sync.WaitGroup
		gate.Add(1)
		for name, e := range cands {
			go func() {
				gate.Wait()
				r, ok := e.do(Req{Op: "takeover", Key: "k", Route: rt}, 60*time.Second)
				out <- res{name, r, ok}
			}()
		}
		gate.Done()
		var all []res
		var win *res
		for range cands {
			r := <-out
			all = append(all, r)
			if !r.ok {
				t.Fatalf("round %d: candidate %s gave no reply", round, r.name)
			}
			// the incumbent itself may answer OK with the observed Route if
			// its fresh GET preceded the winner's claim (PROTOCOL §7 step 3)
			if r.resp.OK && r.name == ownerName && *r.resp.Route == *rt {
				continue
			}
			if r.resp.OK {
				if win != nil {
					t.Fatalf("round %d: two takeover winners after epoch %d: %s %+v and %s %+v", round, rt.Epoch, win.name, win.resp.Route, r.name, r.resp.Route)
				}
				win = &all[len(all)-1]
			}
		}
		if win == nil {
			t.Fatalf("round %d: no takeover winner: %+v", round, all)
		}
		wr := win.resp.Route
		if wr.Node != win.name || wr.Epoch != rt.Epoch+1 {
			t.Fatalf("round %d: winner %s route %+v after observed %+v", round, win.name, wr, rt)
		}
		for _, r := range all {
			if r.name == win.name || (r.resp.OK && r.name == ownerName) {
				continue
			}
			if (r.resp.Code != "route_changed" && r.resp.Code != "not_owner") || r.resp.Route == nil ||
				r.resp.Route.Node != wr.Node || r.resp.Route.Session != wr.Session || r.resp.Route.Epoch != wr.Epoch {
				t.Fatalf("round %d: loser %s: %+v (winner %s %+v)", round, r.name, r.resp, win.name, wr)
			}
		}
		if win.name[0] == 'm' {
			inWins++
		} else {
			procWins++
		}
		// losers and a live previous owner cannot commit: not_owner with the winner's Route
		for _, name := range []string{"b", "c", "d", "m00", "m31"} {
			if name == win.name {
				continue
			}
			r := Req{Op: "add", Key: "k", Inc: inc, ID: newID(), Input: "loser"}
			issued[r.ID] = true
			resp, _ := cands[name].do(r, 30*time.Second)
			if resp.OK || resp.Code != "not_owner" || resp.Route == nil || resp.Route.Node != win.name {
				t.Fatalf("round %d: add on non-owner %s: %+v", round, name, resp)
			}
			led.absent[r.ID] = true
		}
		if ownerName != "a" {
			r := Req{Op: "add", Key: "k", Inc: inc, ID: newID(), Input: "stale"}
			issued[r.ID] = true
			if resp, _ := owner.do(r, 30*time.Second); resp.OK || resp.Code != "not_owner" {
				t.Fatalf("round %d: add on the previous owner %s: %+v", round, ownerName, resp)
			}
			led.absent[r.ID] = true
		}
		owner, ownerName = cands[win.name], win.name
		for i := range 2 {
			must(owner, Req{Op: "add", Key: "k", Inc: inc, ID: newID(), Input: fmt.Sprintf("r%d-%d", round, i), Big: i == 1})
		}
	}

	a.signal(syscall.SIGCONT)
	var pr Resp
	select {
	case r, ok := <-pc:
		if !ok {
			t.Fatal("stopped owner died")
		}
		pr = r
	case <-time.After(30 * time.Second):
		t.Fatal("stopped owner hung")
	}
	switch {
	case pr.OK:
		staleAck = 1
		led.ack(t, pending.ID, pr)
	case pr.Code == "outcome_unknown": // retried on the owner with the same ID
		led.ack(t, pending.ID, must(owner, pending))
	default:
		led.absent[pending.ID] = true
	}
	// the resumed stale owner publishes nothing new, and its strong reads are current
	q := Req{Op: "add", Key: "k", Inc: inc, ID: newID(), Input: "after-cont"}
	issued[q.ID] = true
	if resp, _ := a.do(q, 30*time.Second); resp.OK || resp.Code != "not_owner" {
		t.Fatalf("resumed stale owner's add: %+v", resp)
	}
	led.absent[q.ID] = true
	if rd, _ := a.do(Req{Op: "read", Key: "k", Inc: inc}, 30*time.Second); rd.OK {
		t.Fatalf("resumed stale owner served a strong read: %+v", rd)
	}
	led.check(t, ep, "k", inc, issued)
	return procWins, inWins, staleAck, stalls
}

// TestSameIDSeveralNodes sends one Request (same ID and hash) to two node
// processes concurrently and sequentially, also across a takeover and with
// the owner stopped while it runs it: at most one commit, every
// acknowledgment replays the same value and sequence, and the same ID with
// another hash is id_conflict on any node.
func TestSameIDSeveralNodes(t *testing.T) {
	base := storetest.MinIO(t)
	runs := killRuns()
	var tl [4]int // both nodes acked across a takeover, conflict refusals, replays after takeover, stopped-owner acks
	for i := range runs {
		ep := base.Sub(fmt.Sprintf("sameid-%d", i))
		r := sameIDRun(t, ep)
		for j := range tl {
			tl[j] += r[j]
		}
		clean(t, ep)
		if t.Failed() {
			return
		}
	}
	t.Logf("%d runs: same ID acknowledged by both the old owner and the taker %d; id_conflict refusals %d; replays after takeover %d; stopped owner's op acknowledged %d",
		runs, tl[0], tl[1], tl[2], tl[3])
}

func sameIDRun(t *testing.T, ep storetest.Endpoint) (tl [4]int) {
	nodes := map[string]*mnode{"b": startNode(t, ep, "b"), "c": startNode(t, ep, "c")}
	for _, m := range nodes {
		defer m.kill()
	}
	led := newLedger()
	issued := map[string]bool{}
	cur := "b"
	other := func() string {
		if cur == "b" {
			return "c"
		}
		return "b"
	}
	call := func(name string, r Req) Resp {
		issued[r.ID] = true
		resp, ok := nodes[name].do(r, 30*time.Second)
		if !ok {
			t.Fatalf("%s died on %+v", name, r)
		}
		if resp.OK && r.Op == "add" {
			led.ack(t, r.ID, resp)
		}
		return resp
	}
	takeover := func(name string) Resp {
		rt, _ := resolveAfterStall(t, nodes[name], "k")
		return call(name, Req{Op: "takeover", Key: "k", Route: rt})
	}
	inc := call("b", Req{Op: "create", Key: "k", ID: newID()}).Inc
	add := func(input string) Req { return Req{Op: "add", Key: "k", Inc: inc, ID: newID(), Input: input} }
	allowed := func(what string, r Resp, codes ...string) {
		if !r.OK && !slices.Contains(codes, r.Code) {
			t.Fatalf("%s: %+v", what, r)
		}
	}
	// conflict asserts that the same ID with another input never commits.
	conflict := func(name string, r Req) {
		r.Input += "-other"
		if resp := call(name, r); resp.OK || (resp.Code != "id_conflict" && resp.Code != "not_owner") {
			t.Fatalf("same ID, other hash on %s: %+v", name, resp)
		} else if resp.Code == "id_conflict" {
			tl[1]++
		}
	}
	retryOnOwner := func(r Req) Resp { // to the owner the head names now
		cur = call(cur, Req{Op: "resolve", Key: "k"}).Route.Node
		resp := call(cur, r)
		if !resp.OK {
			t.Fatalf("retry of %s on owner %s: %+v", r.ID, cur, resp)
		}
		return resp
	}
	for i := range 2 {
		call(cur, add(strconv.Itoa(i)))
	}

	// (i) concurrently to the owner and the other node
	x := add("x")
	var rb, rc Resp
	var wg sync.WaitGroup
	wg.Go(func() { rb, _ = nodes[cur].do(x, 30*time.Second) })
	wg.Go(func() { rc, _ = nodes[other()].do(x, 30*time.Second) })
	wg.Wait()
	issued[x.ID] = true
	allowed("owner, concurrent same ID", rb, "outcome_unknown")
	if rc.OK || rc.Code != "not_owner" || rc.Route == nil || rc.Route.Node != cur {
		t.Fatalf("non-owner, concurrent same ID: %+v", rc)
	}
	if rb.OK {
		led.ack(t, x.ID, rb)
	}
	if resp := retryOnOwner(x); rb.OK && !resp.Replayed {
		t.Fatalf("retry of acknowledged %s committed again: %+v", x.ID, resp)
	} else {
		led.ack(t, x.ID, resp)
	}
	conflict(cur, x)
	conflict(other(), x)

	// (ii) sequentially: ack on the owner, not_owner elsewhere, takeover, replay there
	y := add("y")
	ry := retryOnOwner(y)
	if resp := call(other(), y); resp.OK || resp.Code != "not_owner" {
		t.Fatalf("same ID on the non-owner: %+v", resp)
	}
	if r := takeover(other()); !r.OK {
		t.Fatalf("takeover: %+v", r)
	}
	cur = other()
	if resp := retryOnOwner(y); !resp.Replayed || resp.Value != ry.Value || resp.Seq != ry.Seq {
		t.Fatalf("replay after takeover: %+v, acknowledged %+v", resp, ry)
	}
	tl[2]++
	conflict(cur, y)
	conflict(other(), y)

	// (iii) concurrently with a takeover: to the owner, and to the taker after its takeover
	z := add("z")
	var rz1, rz2, rt Resp
	taker := other()
	wg.Go(func() { rz1, _ = nodes[cur].do(z, 30*time.Second) })
	wg.Go(func() {
		r, _ := nodes[taker].do(Req{Op: "resolve", Key: "k"}, 30*time.Second)
		rt, _ = nodes[taker].do(Req{Op: "takeover", Key: "k", Route: r.Route}, 30*time.Second)
		rz2, _ = nodes[taker].do(z, 30*time.Second)
	})
	wg.Wait()
	issued[z.ID] = true
	allowed("old owner, same ID across a takeover", rz1, "outcome_unknown", "not_owner", "route_changed")
	allowed("taker, same ID across a takeover", rz2, "outcome_unknown", "not_owner", "route_changed")
	if rz1.OK && rz2.OK {
		tl[0]++
	}
	for _, r := range []Resp{rz1, rz2} {
		if r.OK {
			led.ack(t, z.ID, r)
		}
	}
	cur = call(cur, Req{Op: "resolve", Key: "k"}).Route.Node
	if rt.OK && cur != taker {
		t.Fatalf("takeover by %s acknowledged but the head names %s", taker, cur)
	}
	led.ack(t, z.ID, retryOnOwner(z))
	conflict(cur, z)

	// (iv) same ID, two hashes, concurrently on both nodes across a takeover
	w := add("p")
	wq := w
	wq.Input = "q"
	var rp, rq Resp
	taker = other()
	wg.Go(func() { rp, _ = nodes[cur].do(w, 30*time.Second) })
	wg.Go(func() {
		r, _ := nodes[taker].do(Req{Op: "resolve", Key: "k"}, 30*time.Second)
		rt, _ = nodes[taker].do(Req{Op: "takeover", Key: "k", Route: r.Route}, 30*time.Second)
		rq, _ = nodes[taker].do(wq, 30*time.Second)
	})
	wg.Wait()
	issued[w.ID] = true
	if rp.OK && rq.OK {
		t.Fatalf("same ID with two hashes acknowledged twice: %+v %+v", rp, rq)
	}
	allowed("hash p", rp, "outcome_unknown", "not_owner", "route_changed", "id_conflict")
	allowed("hash q", rq, "outcome_unknown", "not_owner", "route_changed", "id_conflict")
	cur = call(cur, Req{Op: "resolve", Key: "k"}).Route.Node
	if rt.OK && cur != taker {
		t.Fatalf("takeover by %s acknowledged but the head names %s", taker, cur)
	}
	for _, r := range []Resp{rp, rq} {
		if r.OK {
			led.ack(t, w.ID, r)
		}
	}
	// whichever hash committed (if any) replays; the other is id_conflict
	gp, gq := call(cur, w), call(cur, wq)
	if gp.OK == gq.OK || (!gp.OK && gp.Code != "id_conflict") || (!gq.OK && gq.Code != "id_conflict") {
		t.Fatalf("same ID, two hashes, after the race: p %+v, q %+v", gp, gq)
	}
	if (rp.OK && !gp.OK) || (rq.OK && !gq.OK) {
		t.Fatalf("the acknowledged hash does not replay: p %+v/%+v q %+v/%+v", rp, gp, rq, gq)
	}
	tl[1]++

	// (v) the owner is SIGSTOPped while it runs the operation; the other node
	// takes over and runs the same Request; the stopped owner is resumed
	v := add("v")
	issued[v.ID] = true
	pc := nodes[cur].send(v)
	time.Sleep(time.Duration(rand.N(8)) * time.Millisecond)
	nodes[cur].signal(syscall.SIGSTOP)
	stoppedName := cur
	if r := takeover(other()); !r.OK {
		t.Fatalf("takeover from a stopped owner: %+v", r)
	}
	cur = other()
	rv := retryOnOwner(v)
	led.ack(t, v.ID, rv)
	nodes[stoppedName].signal(syscall.SIGCONT)
	select {
	case r := <-pc:
		allowed("resumed owner's same-ID op", r, "outcome_unknown", "not_owner", "route_changed")
		if r.OK { // its own commit won before the takeover, or it resolved to the taker's receipt: same result
			tl[3]++
			led.ack(t, v.ID, r)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("resumed owner hung")
	}
	if resp := call(stoppedName, v); resp.OK || resp.Code != "not_owner" {
		t.Fatalf("same ID on the resumed stale owner: %+v", resp)
	}
	conflict(stoppedName, v)
	led.check(t, ep, "k", inc, issued)
	return tl
}
