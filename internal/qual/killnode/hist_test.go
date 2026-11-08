package main

// Caller-side routing client, recorded histories and the porcupine check
// (PLAN M5, §4.3, §15.2): concurrent clients on two entities across three
// node processes while SIGSTOP/SIGCONT, SIGKILL, node replacement and
// membership reordering are injected.

import (
	"encoding/json"
	"fmt"
	"math"
	"math/rand/v2"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	garden "github.com/OrdalieTech/big-garden"
	"github.com/OrdalieTech/big-garden/internal/storetest"
	"github.com/anishathalye/porcupine"
)

// hop is one client-level operation as the parent observed it: from the
// first attempt's invocation to the final response, across routing hops.
type hop struct {
	Client             int
	Op, Key, ID, Input string
	Class              string // ack, unknown (may or may not have taken effect), failed (definitely no effect)
	Code, Value, Node  string
	Seq                uint64
	Replayed           bool
	Ambiguous          bool // an earlier attempt timed out, died or answered outcome_unknown
	IDs                []string
	Start, End         int64 // monotonic ns since the history began; End is math.MaxInt64 when unknown
}

// attempt is one request to one node (operations, resolves and takeovers).
type attempt struct {
	Client                  int
	Op, Key, ID, Node, Code string
	OK, Replied             bool
	Start, End              int64
}

// history lives in the parent process, outside every node.
type history struct {
	mu       sync.Mutex
	t0       time.Time
	Ops      []hop
	Attempts []attempt
	Faults   []string
}

func (h *history) now() int64 { return int64(time.Since(h.t0)) }

func (h *history) fault(f string, args ...any) {
	h.mu.Lock()
	h.Faults = append(h.Faults, fmt.Sprintf("%d ", h.now())+fmt.Sprintf(f, args...))
	h.mu.Unlock()
}

// cluster is the believed membership and the current process of each name.
type cluster struct {
	t       *testing.T
	ep      storetest.Endpoint
	mu      sync.Mutex
	members []string
	nodes   map[string]*mnode
	all     []*mnode
	fresh   int
}

func (cl *cluster) start(name string) *mnode {
	m := startNode(cl.t, cl.ep, name)
	cl.mu.Lock()
	cl.nodes[name] = m
	cl.all = append(cl.all, m)
	cl.mu.Unlock()
	return m
}

func (cl *cluster) view() []string {
	cl.mu.Lock()
	defer cl.mu.Unlock()
	return slices.Clone(cl.members)
}

func (cl *cluster) get(name string) *mnode {
	cl.mu.Lock()
	defer cl.mu.Unlock()
	return cl.nodes[name]
}

const routeTimeout = 1500 * time.Millisecond

// rclient is a caller-side router: it sends to the believed owner (or the
// Preferred node), follows the Route of not_owner/route_changed, and after a
// timeout decides the node is dead (wrongly, when it is only stopped),
// observes the Route elsewhere and calls Takeover on a Preferred live node.
type rclient struct {
	id      int
	cl      *cluster
	h       *history
	owner   map[string]string
	suspect map[string]time.Time
}

func (c *rclient) attempt(name string, r Req) (Resp, bool) {
	start := c.h.now()
	var resp Resp
	ok := false
	if m := c.cl.get(name); m != nil {
		resp, ok = m.do(r, routeTimeout)
	}
	c.h.mu.Lock()
	c.h.Attempts = append(c.h.Attempts, attempt{c.id, r.Op, r.Key, r.ID, name, resp.Code, resp.OK, ok, start, c.h.now()})
	c.h.mu.Unlock()
	return resp, ok
}

// pick is the Preferred node among believed-available members.
func (c *rclient) pick(key string, not string) string {
	members := c.cl.view()
	live := slices.DeleteFunc(slices.Clone(members), func(n string) bool {
		return n == not || time.Since(c.suspect[n]) < 3*time.Second
	})
	if len(live) == 0 {
		live = slices.DeleteFunc(members, func(n string) bool { return n == not })
	}
	p, err := garden.Preferred(key, live)
	if err != nil {
		c.cl.t.Errorf("Preferred(%q, %v): %v", key, live, err)
	}
	return p
}

// recover observes the Route through another node and takes over there if
// the recorded owner is the suspected node or no longer a member.
func (c *rclient) recover(key, dead string) {
	via := c.pick(key, dead)
	rr, ok := c.attempt(via, Req{Op: "resolve", Key: key})
	if !ok || !rr.OK {
		return
	}
	if o := rr.Route.Node; o != "" && o != dead && slices.Contains(c.cl.view(), o) {
		c.owner[key] = o
		return
	}
	tr, ok := c.attempt(via, Req{Op: "takeover", Key: key, Route: rr.Route})
	switch {
	case ok && tr.OK:
		c.owner[key] = via
	case ok && tr.Route != nil:
		c.owner[key] = tr.Route.Node
	}
}

func (c *rclient) run(r Req) hop {
	o := hop{Client: c.id, Op: r.Op, Key: r.Key, ID: r.ID, Input: r.Input, Start: c.h.now()}
	ambiguous := false
	for range 12 {
		target := c.owner[r.Key]
		if target == "" || !slices.Contains(c.cl.view(), target) {
			target = c.pick(r.Key, "")
		}
		resp, ok := c.attempt(target, r)
		if !ok { // timed out or the process died: the attempt may still run
			ambiguous, o.Ambiguous = true, true
			c.suspect[target] = time.Now()
			if rand.N(3) == 0 { // this caller gives up and reports outcome_unknown
				o.Class, o.End = "unknown", math.MaxInt64
				return o
			}
			c.recover(r.Key, target)
			continue
		}
		o.Code, o.Node = resp.Code, target
		switch resp.Code {
		case "":
			o.Class, o.Value, o.Seq, o.Replayed, o.IDs, o.End = "ack", resp.Value, resp.Seq, resp.Replayed, resp.IDs, c.h.now()
			return o
		case "not_owner", "route_changed":
			if rt := resp.Route; rt != nil && rt.Node != "" && rt.Node != target && slices.Contains(c.cl.view(), rt.Node) {
				c.owner[r.Key] = rt.Node
			} else {
				dead := target
				if rt != nil && rt.Node != "" {
					dead = rt.Node
				}
				c.recover(r.Key, dead)
			}
		case "outcome_unknown":
			ambiguous, o.Ambiguous = true, true
			if rand.N(2) == 0 {
				o.Class, o.End = "unknown", math.MaxInt64
				return o
			}
		case "unavailable", "capacity":
			time.Sleep(20 * time.Millisecond)
		default: // a definite error
			o.End = c.h.now()
			o.Class = "failed"
			if ambiguous {
				o.Class, o.End = "unknown", math.MaxInt64
			}
			return o
		}
	}
	o.Class, o.End = "failed", c.h.now()
	if ambiguous {
		o.Class, o.End = "unknown", math.MaxInt64
	}
	return o
}

func (c *rclient) record(o hop) {
	c.h.mu.Lock()
	c.h.Ops = append(c.h.Ops, o)
	c.h.mu.Unlock()
}

// Porcupine model of one ledger entity: the state is the committed ID list
// in commit order. add(id) appends unless present and answers the count at
// its commit (a replay answers the stored count); a strong read answers the
// whole list. An unknown add has no known output and an unbounded return.
type pin struct {
	Add bool
	ID  string
}

type pout struct {
	Known bool
	Value string
	IDs   []string
}

var ledgerModel = porcupine.Model{
	Init: func() any { return "" },
	Step: func(st, in, out any) (bool, any) {
		s, i, o := st.(string), in.(pin), out.(pout)
		if !i.Add {
			return strings.Join(o.IDs, ",") == s, s
		}
		var log []string
		if s != "" {
			log = strings.Split(s, ",")
		}
		if p := slices.Index(log, i.ID); p >= 0 {
			return !o.Known || o.Value == strconv.Itoa(p+1), s
		}
		return !o.Known || o.Value == strconv.Itoa(len(log)+1), strings.Trim(s+","+i.ID, ",")
	},
	DescribeOperation: func(in, out any) string {
		i, o := in.(pin), out.(pout)
		if !i.Add {
			return fmt.Sprintf("read -> %v", o.IDs)
		}
		if !o.Known {
			return fmt.Sprintf("add(%s) -> ?", i.ID[len(i.ID)-6:])
		}
		return fmt.Sprintf("add(%s) -> %s", i.ID[len(i.ID)-6:], o.Value)
	},
}

// operations converts one entity's acknowledged and unknown adds and its
// acknowledged strong reads; definitely failed adds and unanswered reads
// had no effect and are left out.
func operations(ops []hop, key string) []porcupine.Operation {
	var out []porcupine.Operation
	for _, o := range ops {
		if o.Key != key || o.Class == "failed" || (o.Op == "read" && o.Class != "ack") {
			continue
		}
		out = append(out, porcupine.Operation{ClientId: o.Client, Input: pin{o.Op == "add", o.ID},
			Output: pout{o.Class == "ack", o.Value, o.IDs}, Call: o.Start, Return: o.End})
	}
	return out
}

const checkTimeout = 60 * time.Second

// TestLinearizabilityChecker is the negative control of the checker itself
// over hand-made histories: it must reject stale reads, lost
// acknowledgments, double commits and wrong replays, and accept an unknown
// add either way.
func TestLinearizabilityChecker(t *testing.T) {
	add := func(id string, call, ret int64, value string) porcupine.Operation {
		return porcupine.Operation{Input: pin{true, id}, Output: pout{Known: value != "", Value: value}, Call: call, Return: ret}
	}
	read := func(call, ret int64, ids ...string) porcupine.Operation {
		return porcupine.Operation{ClientId: 1, Input: pin{}, Output: pout{Known: true, IDs: ids}, Call: call, Return: ret}
	}
	unknown := func(id string, call int64) porcupine.Operation { return add(id, call, math.MaxInt64, "") }
	for _, c := range []struct {
		name string
		ok   bool
		ops  []porcupine.Operation
	}{
		{"ack then read", true, []porcupine.Operation{add("a", 0, 10, "1"), read(20, 30, "a")}},
		{"stale read after ack", false, []porcupine.Operation{add("a", 0, 10, "1"), read(20, 30)}},
		{"concurrent read may miss", true, []porcupine.Operation{add("a", 0, 30, "1"), read(10, 20)}},
		{"unknown applied", true, []porcupine.Operation{unknown("a", 0), read(20, 30, "a")}},
		{"unknown not applied", true, []porcupine.Operation{unknown("a", 0), read(20, 30)}},
		{"unknown then retry replays", true, []porcupine.Operation{unknown("a", 0), add("b", 5, 8, "1"), read(20, 30, "b", "a"), add("a", 40, 50, "2")}},
		{"lost ack", false, []porcupine.Operation{add("a", 0, 10, "1"), add("b", 20, 30, "1")}},
		{"wrong replay", false, []porcupine.Operation{add("a", 0, 10, "1"), add("b", 11, 12, "2"), add("a", 20, 30, "2")}},
		{"double commit", false, []porcupine.Operation{add("a", 0, 10, "1"), read(20, 30, "a", "a")}},
		{"read goes back", false, []porcupine.Operation{add("a", 0, 1, "1"), add("b", 0, 1, "2"), read(20, 30, "a", "b"), read(40, 50, "a")}},
	} {
		res := porcupine.CheckOperationsTimeout(ledgerModel, c.ops, checkTimeout)
		if want := map[bool]porcupine.CheckResult{true: porcupine.Ok, false: porcupine.Illegal}[c.ok]; res != want {
			t.Errorf("%s: %s, want %s", c.name, res, want)
		}
	}
}

// TestRoutedHistories runs GARDEN_KILL_RUNS workloads: 4 routing clients on
// 2 entities across 3 node processes for 6 s while faults are injected, then
// every unknown add is retried with its ID, each entity's history is checked
// with porcupine (inconclusive fails), a stale-read mutation of the real
// history must be rejected, and the ledger is checked from S3 after every
// node process and cache is gone.
func TestRoutedHistories(t *testing.T) {
	base := storetest.MinIO(t)
	runs := killRuns()
	var st histStats
	for i := range runs {
		ep := base.Sub(fmt.Sprintf("hist-%d", i))
		histRun(t, ep, i, &st)
		clean(t, ep)
		if t.Failed() {
			return
		}
	}
	t.Logf("%d runs: %d histories checked linearizable (0 inconclusive), %d-%d ops each (%d total: %d acked adds (%d replayed, %d after an ambiguous attempt), %d unknown adds, %d failed adds, %d reads); %d stale-read mutations rejected (%d histories without a candidate read); %d attempts: %d unanswered, %d not_owner/route_changed followed, %d client takeovers (%d won); faults: %v",
		runs, st.histories, st.minOps, st.maxOps, st.ops, st.acked, st.replayed, st.ambiguous, st.unknown, st.failed, st.reads, st.controls, st.noControl, st.attempts, st.timeouts, st.redirects, st.takeovers, st.takeoversWon, st.faults)
}

type histStats struct {
	histories, minOps, maxOps, ops, acked, replayed, ambiguous, unknown, failed, reads, controls, noControl int
	attempts, timeouts, redirects, takeovers, takeoversWon                                                  int
	faults                                                                                                  map[string]int
}

func histRun(t *testing.T, ep storetest.Endpoint, run int, st *histStats) {
	cl := &cluster{t: t, ep: ep, nodes: map[string]*mnode{}, members: []string{"n1", "n2", "n3"}, fresh: 3}
	for _, n := range cl.members {
		cl.start(n)
	}
	defer func() {
		for _, m := range cl.all {
			m.kill()
		}
	}()
	h := &history{t0: time.Now()}
	keys := []string{"k0", "k1"}
	inc := map[string]string{}
	for _, k := range keys {
		p, _ := garden.Preferred(k, cl.members)
		r, ok := cl.get(p).do(Req{Op: "create", Key: k, ID: newID()}, 30*time.Second)
		if !ok || !r.OK {
			t.Fatalf("create %s on %s: %+v", k, p, r)
		}
		inc[k] = r.Inc
	}
	done := make(chan struct{})
	var wg sync.WaitGroup
	if st.faults == nil {
		st.faults = map[string]int{}
	}
	wg.Go(func() { inject(cl, h, done, st.faults) })
	var cwg sync.WaitGroup
	for id := range 4 {
		cwg.Go(func() {
			c := &rclient{id: id, cl: cl, h: h, owner: map[string]string{}, suspect: map[string]time.Time{}}
			for deadline := time.Now().Add(6 * time.Second); time.Now().Before(deadline); {
				k := keys[rand.N(len(keys))]
				r := Req{Op: "read", Key: k, Inc: inc[k]}
				if rand.N(10) < 6 {
					r = Req{Op: "add", Key: k, Inc: inc[k], ID: newID(), Input: fmt.Sprintf("c%d", id), Big: rand.N(8) == 0}
				}
				c.record(c.run(r))
			}
		})
	}
	cwg.Wait()
	close(done)
	wg.Wait()

	// stable connectivity: every unknown add is retried with its ID and must
	// be acknowledged (an exact replay or its single commit); then one strong
	// read per entity closes each history
	c := &rclient{id: 99, cl: cl, h: h, owner: map[string]string{}, suspect: map[string]time.Time{}}
	for _, o := range slices.Clone(h.Ops) {
		if o.Op == "add" && o.Class == "unknown" {
			r := c.run(Req{Op: "add", Key: o.Key, Inc: inc[o.Key], ID: o.ID, Input: o.Input})
			c.record(r)
			if r.Class != "ack" {
				t.Errorf("retry of unknown %s after recovery: %+v", o.ID, r)
			}
		}
	}
	for _, k := range keys {
		if r := c.run(Req{Op: "read", Key: k, Inc: inc[k]}); r.Class != "ack" {
			t.Errorf("final read of %s: %+v", k, r)
		} else {
			c.record(r)
		}
	}
	dump := func(why string) {
		b, _ := json.MarshalIndent(h, "", " ")
		p := filepath.Join(os.TempDir(), fmt.Sprintf("killnode-history-%d-%s.json", run, storetest.RandomHex(3)))
		os.WriteFile(p, b, 0o644)
		t.Errorf("run %d: %s; history in %s", run, why, p)
	}
	for _, k := range keys {
		ops := operations(h.Ops, k)
		res, info := porcupine.CheckOperationsVerbose(ledgerModel, ops, checkTimeout)
		switch res {
		case porcupine.Unknown:
			dump(fmt.Sprintf("%s: porcupine inconclusive after %v (%d ops) — not a pass", k, checkTimeout, len(ops)))
			return
		case porcupine.Illegal:
			p := filepath.Join(os.TempDir(), fmt.Sprintf("killnode-porcupine-%d-%s.html", run, k))
			porcupine.VisualizePath(ledgerModel, info, p)
			dump(fmt.Sprintf("%s: history not linearizable (%d ops, visualization %s)", k, len(ops), p))
			return
		}
		st.histories++
		if st.minOps == 0 || len(ops) < st.minOps {
			st.minOps = len(ops)
		}
		st.maxOps = max(st.maxOps, len(ops))
		st.ops += len(ops)
		// negative control on the real history: drop from one read the last
		// ID whose add was acknowledged before that read began
		ackEnd := map[string]int64{}
		for _, o := range h.Ops {
			if o.Key == k && o.Op == "add" && o.Class == "ack" {
				if e, ok := ackEnd[o.ID]; !ok || o.End < e {
					ackEnd[o.ID] = o.End
				}
			}
		}
		control := false
		for i, op := range ops {
			out := op.Output.(pout)
			if op.Input.(pin).Add || len(out.IDs) == 0 {
				continue
			}
			if e, ok := ackEnd[out.IDs[len(out.IDs)-1]]; !ok || e >= op.Call {
				continue
			}
			bad := slices.Clone(ops)
			bad[i].Output = pout{Known: true, IDs: out.IDs[:len(out.IDs)-1]}
			if res := porcupine.CheckOperationsTimeout(ledgerModel, bad, checkTimeout); res != porcupine.Illegal {
				t.Errorf("run %d %s: a stale read of the real history checked %s, want Illegal", run, k, res)
			}
			control = true
			st.controls++
			break
		}
		if !control {
			st.noControl++
		}
	}
	for _, a := range h.Attempts {
		st.attempts++
		switch {
		case !a.Replied:
			st.timeouts++
		case a.Op == "takeover":
			st.takeovers++
			if a.OK {
				st.takeoversWon++
			}
		case a.Code == "not_owner" || a.Code == "route_changed":
			st.redirects++
		}
	}
	// independent ledger: every node process and cache destroyed, recovery from S3
	for _, m := range cl.all {
		m.kill()
	}
	for _, k := range keys {
		led := newLedger()
		issued := map[string]bool{}
		for _, o := range h.Ops {
			if o.Key != k || o.Op != "add" {
				continue
			}
			issued[o.ID] = true
			switch o.Class {
			case "ack":
				led.ack(t, o.ID, Resp{Value: o.Value, Seq: o.Seq})
				st.acked++
				if o.Replayed {
					st.replayed++
				}
				if o.Ambiguous {
					st.ambiguous++
				}
			case "unknown":
				st.unknown++
			case "failed":
				st.failed++
			}
		}
		for _, o := range h.Ops {
			if o.Key == k && o.Op == "add" && o.Class == "failed" {
				if _, ok := led.committed[o.ID]; !ok {
					led.absent[o.ID] = true
				}
			}
			if o.Key == k && o.Op == "read" {
				st.reads++
			}
		}
		led.check(t, ep, k, inc[k], issued)
	}
	if t.Failed() {
		dump("ledger check failed")
	}
}

// inject applies one fault every 300-700 ms until done: SIGSTOP for longer
// than the routing timeout (false failure detection), SIGKILL and restart
// under the same NodeID (new session, D4), replacement by a fresh process
// with a new NodeID (membership changes, so Preferred does), or a reordered
// membership list.
func inject(cl *cluster, h *history, done chan struct{}, counts map[string]int) {
	for i := rand.N(4); ; i++ { // the four faults in turn, from a random one
		select {
		case <-done:
			return
		case <-time.After(time.Duration(300+rand.N(400)) * time.Millisecond):
		}
		// target the current owner of a random entity (any member if unknown)
		members := cl.view()
		name := members[rand.N(len(members))]
		if r, ok := cl.get(name).do(Req{Op: "resolve", Key: fmt.Sprintf("k%d", rand.N(2))}, routeTimeout); ok && r.OK && slices.Contains(members, r.Route.Node) {
			name = r.Route.Node
		}
		m := cl.get(name)
		switch i % 4 {
		case 0:
			h.fault("SIGSTOP %s", name)
			m.signal(syscall.SIGSTOP)
			select {
			case <-done:
			case <-time.After(time.Duration(1600+rand.N(1400)) * time.Millisecond):
			}
			m.signal(syscall.SIGCONT)
			h.fault("SIGCONT %s", name)
			counts["stop"]++
		case 1:
			h.fault("SIGKILL %s, restart same NodeID", name)
			m.kill()
			cl.start(name)
			counts["kill-restart"]++
		case 2:
			cl.mu.Lock()
			cl.fresh++
			nn := fmt.Sprintf("n%d", cl.fresh)
			cl.mu.Unlock()
			h.fault("SIGKILL %s, replace by %s", name, nn)
			m.kill()
			cl.start(nn)
			cl.mu.Lock()
			cl.members = slices.DeleteFunc(cl.members, func(n string) bool { return n == name })
			cl.members = append(cl.members, nn)
			rand.Shuffle(len(cl.members), func(i, j int) { cl.members[i], cl.members[j] = cl.members[j], cl.members[i] })
			cl.mu.Unlock()
			counts["replace"]++
		default:
			cl.mu.Lock()
			rand.Shuffle(len(cl.members), func(i, j int) { cl.members[i], cl.members[j] = cl.members[j], cl.members[i] })
			cl.mu.Unlock()
			h.fault("reorder %v", cl.view())
			counts["reorder"]++
		}
	}
}
