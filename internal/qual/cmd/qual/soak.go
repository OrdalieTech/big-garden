package main

import (
	"bufio"
	"compress/gzip"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"maps"
	"math"
	"math/rand/v2"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	garden "github.com/OrdalieTech/big-garden"
	"github.com/OrdalieTech/big-garden/internal/store"
	"github.com/OrdalieTech/big-garden/internal/storetest"
	"github.com/anishathalye/porcupine"
)

// soak is the PLAN §17 Q6 soak: three node processes (qual soak-node) on a
// bounded set of ledger entities, 70% reads (strong reads and receipt
// lookups) and 30% writes (inline and upload-size), clients that follow
// Routes and take over from nodes they believe dead, and a fault every
// -fault: SIGKILL+restart, SIGSTOP/SIGCONT of an owner while clients take
// over elsewhere, node replacement. Node n0 is never killed, so its memory
// series spans the run (§18.3). Every -report it writes a rolling report
// with a ledger check; porcupine checks sampled windows of one entity at a
// time continuously. At the end every node is killed, caches are deleted,
// a fresh node reconstructs every entity from S3 for the final ledger check,
// and summary.json and a done marker are written. The prefix is deleted.
func soak(args []string) error {
	fs := flag.NewFlagSet("soak", flag.ContinueOnError)
	local := fs.Bool("local", false, "use the pinned local S3 fixture")
	remote := fs.Bool("remote", false, "use QUAL_S3_* and AWS_* (prefix under qual-state/)")
	dur := fs.Duration("duration", 10*time.Minute, "measured duration")
	minOps := fs.Int64("min-ops", 0, "keep running past -duration until this many operations")
	entities := fs.Int("entities", 64, "entities")
	clients := fs.Int("clients", 16, "concurrent clients")
	rate := fs.Float64("rate", 150, "target operations per second over all clients (0: unthrottled)")
	reportEvery := fs.Duration("report", 5*time.Minute, "rolling report and ledger check period")
	sampleEvery := fs.Duration("sample", 30*time.Second, "resource sample period")
	faultEvery := fs.Duration("fault", 3*time.Minute, "fault period (0: none)")
	window := fs.Duration("window", time.Minute, "porcupine window per sampled entity")
	collectEvery := fs.Duration("collect", time.Second, "one Collect call (round-robin over entities) per period")
	ring := fs.Int("ring", 1024, "ledger ring rows per entity")
	upload := fs.Int("upload", 40<<10, "payload bytes of upload-size writes")
	inline := fs.Int("inline", 256, "payload bytes of inline writes")
	out := fs.String("out", "", "output directory (required)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *out == "" {
		return errors.New("-out is required")
	}
	if a, err := filepath.Abs(*out); err == nil {
		*out = a // node cache directories must be absolute
	}
	var ep storetest.Endpoint
	var err error
	switch {
	case *remote:
		ep, err = storetest.RemoteFromEnv()
	case *local:
		ep, err = storetest.StartLocal(context.Background())
	default:
		return errors.New("choose --local or --remote")
	}
	if err != nil {
		return err
	}
	ep = ep.Sub("soak")
	if err := os.MkdirAll(*out, 0o755); err != nil {
		return err
	}
	s := &soakRun{ep: ep, out: *out, ring: *ring, t0: time.Now(), nodes: map[string]*snode{},
		lat: map[string][]time.Duration{}, counts: map[string]map[string]int64{}, codes: map[string]int64{},
		pending: map[string][]ack{}, recent: map[string][]ack{}, unknown: map[string][]unk{}, gates: map[string]*sync.RWMutex{}, windows: map[string]*swindow{}}
	s.self, _ = os.Executable()
	hf, err := os.Create(filepath.Join(*out, "history.jsonl.gz"))
	if err != nil {
		return err
	}
	s.hz = gzip.NewWriter(hf)
	s.hist = json.NewEncoder(s.hz)
	s.logf, err = os.OpenFile(filepath.Join(*out, "soak.log"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	cfg, _ := json.Marshal(map[string]any{"endpoint": ep.URL, "bucket": ep.Bucket, "prefix": ep.Prefix, "args": args, "started": s.t0.UTC()})
	os.WriteFile(filepath.Join(*out, "config.json"), cfg, 0o644)
	s.logx("config %s", cfg)
	defer func() {
		k, cerr := cleanup(context.Background(), store.NewS3(ep.Client(nil), ep.Bucket, ep.Prefix, time.Minute))
		s.logx("cleanup: %d objects deleted under %s (%v)", k, ep.Prefix, cerr)
		os.RemoveAll(filepath.Join(*out, "cache"))
	}()

	for i := range 3 {
		if err := s.start(fmt.Sprintf("n%d", i), true); err != nil {
			s.killAll()
			return err
		}
	}
	keys := make([]string, *entities)
	refs := map[string]string{}
	boot := &sclient{id: -1, s: s, owner: map[string]string{}, suspect: map[string]time.Time{}}
	for i := range keys {
		keys[i] = fmt.Sprintf("e%03d", i)
		s.gates[keys[i]] = &sync.RWMutex{}
		var o shop
		for try := 0; try < 5 && o.Class != "ack"; try++ {
			o = boot.run(sReq{Op: "create", Key: keys[i], ID: newID()}, keys[i])
		}
		if o.Class != "ack" {
			s.killAll()
			return fmt.Errorf("create %s: %s %s", keys[i], o.Code, o.Err)
		}
		refs[keys[i]] = o.Inc
	}
	s.refs = refs
	s.logx("created %d entities on %v", len(keys), s.view())

	stop := make(chan struct{})
	var bg sync.WaitGroup
	var issued atomic.Int64
	deadline := s.t0.Add(*dur)
	done := func() bool {
		select {
		case <-stop:
			return true
		default:
		}
		return time.Now().After(deadline) && issued.Load() >= *minOps
	}
	var cw sync.WaitGroup
	for c := range *clients {
		cw.Go(func() {
			cl := &sclient{id: c, s: s, owner: map[string]string{}, suspect: map[string]time.Time{}}
			var tick <-chan time.Time
			if *rate > 0 {
				t := time.NewTicker(time.Duration(float64(*clients) / *rate * float64(time.Second)))
				defer t.Stop()
				tick = t.C
			}
			for !done() {
				if tick != nil {
					<-tick
				}
				issued.Add(1)
				cl.one(keys[rand.N(len(keys))], *inline, *upload)
			}
		})
	}
	bg.Go(func() { s.sampler(stop, keys, *window) })
	bg.Go(func() { s.collector(stop, keys, *collectEvery) })
	bg.Go(func() { s.metrics(stop, *sampleEvery) })
	if *faultEvery > 0 {
		bg.Go(func() { s.faults(stop, keys, *faultEvery) })
	}
	bg.Go(func() {
		t := time.NewTicker(*reportEvery)
		defer t.Stop()
		for {
			select {
			case <-stop:
				return
			case <-t.C:
				s.checkpoint(keys, false)
			}
		}
	})
	cw.Wait()
	close(stop)
	bg.Wait()
	s.hmu.Lock()
	s.hz.Flush()
	s.hmu.Unlock()
	s.checkpoint(keys, false)
	s.logx("measured phase over after %v and %d operations", time.Since(s.t0).Round(time.Second), issued.Load())

	// Final reconstruction: every node dies, caches are deleted, one fresh
	// node rebuilds every entity from S3.
	s.killAll()
	os.RemoveAll(filepath.Join(*out, "cache"))
	s.mu.Lock()
	s.members = nil
	s.mu.Unlock()
	if err := s.start("final", true); err != nil {
		s.fail("final node: %v", err)
	} else {
		s.checkpoint(keys, true)
	}
	s.killAll()
	s.hmu.Lock()
	s.hz.Close()
	hf.Close()
	s.hmu.Unlock()
	sum := s.summary()
	b, _ := json.MarshalIndent(sum, "", "  ")
	os.WriteFile(filepath.Join(*out, "summary.json"), append(b, '\n'), 0o644)
	s.logx("summary: %s", b)
	os.WriteFile(filepath.Join(*out, "done"), []byte(time.Now().UTC().Format(time.RFC3339)+" "+sum.Verdict+"\n"), 0o644)
	if sum.Verdict != "pass" {
		return fmt.Errorf("soak failed: %s", strings.Join(sum.Failures, "; "))
	}
	return nil
}

type ack struct {
	ID       string
	N        int64
	End      time.Time
	Replayed bool
}

type unk struct {
	ID    string
	Start time.Time
}

// soakRun is the driver: it lives outside every node.
type soakRun struct {
	ep            storetest.Endpoint
	out, self     string
	ring          int
	t0            time.Time
	refs          map[string]string // key -> incarnation hex
	mu            sync.Mutex        // membership and processes
	members       []string
	nodes         map[string]*snode
	fresh         int
	retired       []procStats // last sample of every ended process (cumulative S3 counts)
	hmu           sync.Mutex  // history file
	hz            *gzip.Writer
	hist          *json.Encoder
	logmu         sync.Mutex
	logf          *os.File
	smu           sync.Mutex // everything below
	lat           map[string][]time.Duration
	counts        map[string]map[string]int64 // op -> class/code -> count (cumulative)
	codes         map[string]int64
	ops           int64
	pending       map[string][]ack // acknowledged writes not yet ledger-checked
	recent        map[string][]ack // checked ones still within the ring: rechecked after the final reconstruction
	unknown       map[string][]unk // recent unknown writes (window boundary)
	checkedAcks   int64
	lookups       int64
	failures      []string
	faultLog      []string
	faultsBy      map[string]int
	windowsOK     int
	windowsBad    int
	windowsSkip   int
	windowOps     int64
	series        []seriesPoint
	reports       int
	collects      int64
	collectErrs   int64
	gates         map[string]*sync.RWMutex
	windows       map[string]*swindow
	lastReportOps int64
	lastReportAt  time.Time
}

type seriesPoint struct {
	T     float64 // seconds since start
	Node  string
	PID   int
	Proc  procStats
	Alive bool
}

func (s *soakRun) logx(f string, a ...any) {
	line := fmt.Sprintf("%s %s\n", time.Now().UTC().Format("15:04:05"), fmt.Sprintf(f, a...))
	s.logmu.Lock()
	s.logf.WriteString(line)
	os.Stdout.WriteString(line)
	s.logmu.Unlock()
}

func (s *soakRun) fail(f string, a ...any) {
	m := fmt.Sprintf("%.0fs ", time.Since(s.t0).Seconds()) + fmt.Sprintf(f, a...)
	s.smu.Lock()
	if len(s.failures) < 1000 {
		s.failures = append(s.failures, m)
	}
	s.smu.Unlock()
	s.logx("FAILURE %s", m)
}

func (s *soakRun) fault(kind, f string, a ...any) {
	m := fmt.Sprintf("%.0fs %s ", time.Since(s.t0).Seconds(), kind) + fmt.Sprintf(f, a...)
	s.smu.Lock()
	s.faultLog = append(s.faultLog, m)
	if s.faultsBy == nil {
		s.faultsBy = map[string]int{}
	}
	s.faultsBy[kind]++
	s.smu.Unlock()
	s.logx("fault %s", m)
}

func newID() string {
	id, err := garden.NewMutationID(time.Now())
	if err != nil {
		panic(err)
	}
	return hex.EncodeToString(id[:])
}

// snode is one node process.
type snode struct {
	name, cache string
	cmd         *exec.Cmd
	in          io.WriteCloser
	mu          sync.Mutex
	wait        map[string]chan sResp
	dead        bool
	tags        int
	last        *procStats
	exited      chan struct{}
}

func (s *soakRun) start(name string, member bool) error { // member: add to the believed membership
	cache := filepath.Join(s.out, "cache", name)
	if err := os.MkdirAll(cache, 0o755); err != nil {
		return err
	}
	cmd := exec.Command(s.self, "soak-node", "-url", s.ep.URL, "-region", s.ep.Region, "-bucket", s.ep.Bucket,
		"-prefix", s.ep.Prefix, "-cache", cache, "-node", name, "-ring", strconv.Itoa(s.ring))
	cmd.Env = append(os.Environ(), "AWS_ACCESS_KEY_ID="+s.ep.AccessKey, "AWS_SECRET_ACCESS_KEY="+s.ep.SecretKey)
	ef, err := os.OpenFile(filepath.Join(s.out, "node-"+name+".stderr"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	cmd.Stderr = ef
	in, err := cmd.StdinPipe()
	if err != nil {
		return err
	}
	outp, err := cmd.StdoutPipe()
	if err != nil {
		return err
	}
	if err := cmd.Start(); err != nil {
		return err
	}
	m := &snode{name: name, cache: cache, cmd: cmd, in: in, wait: map[string]chan sResp{}, exited: make(chan struct{})}
	go func() {
		sc := bufio.NewScanner(outp)
		sc.Buffer(make([]byte, 4<<20), 4<<20)
		for sc.Scan() {
			var r sResp
			if json.Unmarshal(sc.Bytes(), &r) != nil {
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
		for _, c := range m.wait {
			close(c)
		}
		m.wait = nil
		m.mu.Unlock()
		cmd.Wait()
		ef.Close()
		close(m.exited)
	}()
	s.mu.Lock()
	if old := s.nodes[name]; old != nil && old.last != nil {
		s.retired = append(s.retired, *old.last)
	}
	s.nodes[name] = m
	if member && !slices.Contains(s.members, name) {
		s.members = append(s.members, name)
	}
	s.mu.Unlock()
	s.logx("started %s pid %d", name, cmd.Process.Pid)
	return nil
}

func (m *snode) send(r sReq) chan sResp {
	c := make(chan sResp, 1)
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.dead {
		return nil
	}
	m.tags++
	r.Tag = strconv.Itoa(m.tags)
	m.wait[r.Tag] = c
	b, _ := json.Marshal(r)
	if _, err := m.in.Write(append(b, '\n')); err != nil {
		delete(m.wait, r.Tag)
		return nil
	}
	return c
}

func (m *snode) do(r sReq, timeout time.Duration) (sResp, bool) {
	c := m.send(r)
	if c == nil {
		return sResp{}, false
	}
	select {
	case resp, ok := <-c:
		return resp, ok
	case <-time.After(timeout):
		return sResp{}, false
	}
}

// kill SIGKILLs the process and waits for it (SIGCONT first: a stopped
// process must not hold its pipes).
func (m *snode) kill() {
	m.cmd.Process.Signal(syscall.SIGCONT)
	m.cmd.Process.Kill()
	<-m.exited
}

func (s *soakRun) killAll() {
	s.mu.Lock()
	all := make([]*snode, 0, len(s.nodes))
	for _, m := range s.nodes {
		all = append(all, m)
	}
	s.mu.Unlock()
	for _, m := range all {
		m.kill()
	}
	s.mu.Lock()
	for _, m := range all {
		if m.last != nil {
			s.retired = append(s.retired, *m.last)
			m.last = nil
		}
	}
	s.nodes = map[string]*snode{}
	s.mu.Unlock()
}

func (s *soakRun) view() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.members)
}

func (s *soakRun) get(name string) *snode {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.nodes[name]
}

// shop is one client-level operation as the driver observed it, across
// routing hops (the hop of internal/qual/killnode's routed histories).
type shop struct {
	Client             int
	Op, Key, ID, Class string // ack, unknown, failed
	Code, Err, Value   string `json:",omitempty"`
	Node, Inc          string `json:",omitempty"`
	Size               int    `json:",omitempty"`
	Replayed           bool   `json:",omitempty"`
	Ambiguous          bool   `json:",omitempty"`
	IDs                []string
	Ns                 []int64 `json:"-"`
	Start, End         int64   // ns since the soak began; End is MaxInt64 when unknown
}

const sRouteTimeout = 2 * time.Second

// sclient is the caller-side router of internal/qual/killnode
// (hist_test.go rclient): it sends to the believed owner or the Preferred
// node, follows the Route of not_owner/route_changed, and after a timeout
// suspects the node, observes the Route elsewhere and takes over there.
type sclient struct {
	id      int
	s       *soakRun
	owner   map[string]string
	suspect map[string]time.Time
}

func (c *sclient) now() int64 { return int64(time.Since(c.s.t0)) }

func (c *sclient) attempt(name string, r sReq) (sResp, bool) {
	m := c.s.get(name)
	if m == nil {
		return sResp{}, false
	}
	ch := m.send(r)
	if ch == nil {
		return sResp{}, false
	}
	select {
	case resp, ok := <-ch:
		return resp, ok
	case <-time.After(sRouteTimeout):
		if r.Op == "write" { // a late acknowledgment is still an acknowledgment
			go func() {
				if resp, ok := <-ch; ok && resp.OK {
					if n, err := strconv.ParseInt(resp.Value, 10, 64); err == nil {
						c.s.smu.Lock()
						c.s.pending[r.Key] = append(c.s.pending[r.Key], ack{r.ID, n, time.Now(), resp.Replayed})
						c.s.smu.Unlock()
					}
				}
			}()
		}
		return sResp{}, false
	}
}

func (c *sclient) pick(key, not string) string {
	members := c.s.view()
	live := slices.DeleteFunc(slices.Clone(members), func(n string) bool {
		return n == not || time.Since(c.suspect[n]) < 3*time.Second
	})
	if len(live) == 0 {
		live = slices.DeleteFunc(members, func(n string) bool { return n == not })
	}
	if len(live) == 0 {
		return ""
	}
	p, _ := garden.Preferred(key, live)
	return p
}

func (c *sclient) recover(key, dead string) {
	via := c.pick(key, dead)
	if via == "" {
		return
	}
	rr, ok := c.attempt(via, sReq{Op: "resolve", Key: key})
	if !ok || !rr.OK {
		return
	}
	if o := rr.Route.Node; o != "" && o != dead && slices.Contains(c.s.view(), o) {
		c.owner[key] = o
		return
	}
	tr, ok := c.attempt(via, sReq{Op: "takeover", Key: key, Route: rr.Route})
	switch {
	case ok && tr.OK:
		c.owner[key] = via
	case ok && tr.Route != nil:
		c.owner[key] = tr.Route.Node
	}
}

func (c *sclient) run(r sReq, key string) shop {
	r.Key = key
	o := shop{Client: c.id, Op: r.Op, Key: key, ID: r.ID, Size: r.Size, Start: c.now()}
	ambiguous := false
	for range 12 {
		target := c.owner[key]
		if target == "" || !slices.Contains(c.s.view(), target) {
			target = c.pick(key, "")
		}
		resp, ok := c.attempt(target, r)
		if !ok {
			if r.Op == "write" || r.Op == "create" {
				ambiguous, o.Ambiguous = true, true
			}
			c.suspect[target] = time.Now()
			c.recover(key, target)
			continue
		}
		o.Code, o.Node, o.Err = resp.Code, target, resp.Err
		switch resp.Code {
		case "":
			o.Class, o.Value, o.Replayed, o.IDs, o.Ns, o.Inc, o.End = "ack", resp.Value, resp.Replayed, resp.IDs, resp.Ns, resp.Inc, c.now()
			return o
		case "not_owner", "route_changed":
			if rt := resp.Route; rt != nil && rt.Node != "" && rt.Node != target && slices.Contains(c.s.view(), rt.Node) {
				c.owner[key] = rt.Node
			} else {
				dead := target
				if rt != nil && rt.Node != "" {
					dead = rt.Node
				}
				c.recover(key, dead)
			}
		case "outcome_unknown":
			ambiguous, o.Ambiguous = true, true // retry with the same request: it replays or commits once
		case "unavailable", "capacity", "closed":
			time.Sleep(50 * time.Millisecond)
		default:
			o.End, o.Class = c.now(), "failed"
			if ambiguous {
				o.Class, o.End = "unknown", math.MaxInt64
			}
			return o
		}
	}
	o.Class, o.End = "failed", c.now()
	if ambiguous {
		o.Class, o.End = "unknown", math.MaxInt64
	}
	return o
}

// one issues one random operation: 55% strong read, 15% receipt lookup of a
// recent acknowledged write, 22.5% inline write, 7.5% upload-size write.
func (c *sclient) one(key string, inline, upload int) {
	s := c.s
	x := rand.N(1000)
	r := sReq{Inc: s.refs[key]}
	label := ""
	var want ack
	switch {
	case x < 550:
		r.Op, label = "read", "read"
	case x < 700:
		s.smu.Lock()
		p := s.pending[key]
		if len(p) > 0 {
			want = p[rand.N(len(p))]
		}
		s.smu.Unlock()
		if want.ID == "" {
			r.Op, label = "read", "read"
		} else {
			r.Op, r.ID, label = "lookup", want.ID, "lookup"
		}
	case x < 925:
		r.Op, r.ID, r.Size, label = "write", newID(), inline, "write-inline"
	default:
		r.Op, r.ID, r.Size, label = "write", newID(), upload, "write-upload"
	}
	g := s.gates[key]
	g.RLock()
	t := time.Now()
	o := c.run(r, key)
	d := time.Since(t)
	s.smu.Lock()
	if w := s.windows[key]; w != nil && (r.Op == "write" || r.Op == "read") && o.Class != "failed" && !(r.Op == "read" && o.Class != "ack") {
		w.ops = append(w.ops, o)
	}
	g.RUnlock()
	s.ops++
	if s.counts[label] == nil {
		s.counts[label] = map[string]int64{}
	}
	s.counts[label][o.Class]++
	if o.Code != "" {
		s.codes[label+":"+o.Code]++
	}
	if o.Class == "ack" {
		s.lat[label] = append(s.lat[label], d)
	}
	switch {
	case r.Op == "write" && o.Class == "ack":
		n, err := strconv.ParseInt(o.Value, 10, 64)
		if err != nil {
			s.smu.Unlock()
			s.fail("write %s on %s: bad value %q", o.ID, key, o.Value)
			s.smu.Lock()
		} else {
			s.pending[key] = append(s.pending[key], ack{o.ID, n, time.Now(), o.Replayed})
		}
	case r.Op == "write" && o.Class == "unknown":
		u := append(s.unknown[key], unk{o.ID, t})
		s.unknown[key] = slices.DeleteFunc(u, func(u unk) bool { return time.Since(u.Start) > 15*time.Minute })
	case r.Op == "lookup" && o.Class == "ack":
		s.lookups++
		if o.Value != strconv.FormatInt(want.N, 10) {
			s.smu.Unlock()
			s.fail("lookup %s on %s: receipt %q, acknowledged %d", want.ID, key, o.Value, want.N)
			s.smu.Lock()
		}
	case r.Op == "lookup" && o.Class == "failed" && (o.Code == "absent_at_version" || o.Code == "id_expired"):
		s.smu.Unlock()
		s.fail("lookup %s on %s (acknowledged %v ago): %s", want.ID, key, time.Since(want.End).Round(time.Second), o.Code)
		s.smu.Lock()
	}
	s.smu.Unlock()
	s.record(o)
}

func (s *soakRun) record(o shop) {
	s.hmu.Lock()
	s.hist.Encode(o)
	s.hmu.Unlock()
}

// collector calls Collect on one entity per period, round-robin, through
// the believed owner (maintenance itself runs after every commit).
func (s *soakRun) collector(stop chan struct{}, keys []string, every time.Duration) {
	c := &sclient{id: -2, s: s, owner: map[string]string{}, suspect: map[string]time.Time{}}
	t := time.NewTicker(every)
	defer t.Stop()
	for i := 0; ; i++ {
		select {
		case <-stop:
			return
		case <-t.C:
		}
		k := keys[i%len(keys)]
		o := c.run(sReq{Op: "collect", Inc: s.refs[k]}, k)
		s.smu.Lock()
		s.collects++
		if o.Class != "ack" {
			s.collectErrs++
		}
		s.smu.Unlock()
	}
}

// metrics samples every live node (its own stats plus /proc RSS) and the
// driver itself.
func (s *soakRun) metrics(stop chan struct{}, every time.Duration) {
	t := time.NewTicker(every)
	defer t.Stop()
	f, _ := os.OpenFile(filepath.Join(s.out, "metrics.jsonl"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	defer f.Close()
	enc := json.NewEncoder(f)
	for {
		select {
		case <-stop:
			return
		case <-t.C:
		}
		s.mu.Lock()
		nodes := make(map[string]*snode, len(s.nodes))
		for k, v := range s.nodes {
			nodes[k] = v
		}
		s.mu.Unlock()
		for name, m := range nodes {
			resp, ok := m.do(sReq{Op: "stats"}, 5*time.Second)
			p := seriesPoint{T: time.Since(s.t0).Seconds(), Node: name, PID: m.cmd.Process.Pid, Alive: ok}
			if ok && resp.Proc != nil {
				p.Proc = *resp.Proc
				s.mu.Lock()
				m.last = resp.Proc
				s.mu.Unlock()
			} else {
				p.Proc.RSS = rssOf(m.cmd.Process.Pid)
			}
			s.smu.Lock()
			s.series = append(s.series, p)
			s.smu.Unlock()
			enc.Encode(p)
		}
	}
}

// faults injects one fault per period, cycling through SIGKILL+restart
// (keeping the cache on odd rounds, wiping it on even ones), SIGSTOP of an
// entity's owner for 6 s while clients time out and take over elsewhere,
// and replacement of a node by a fresh name. n0 is never killed.
func (s *soakRun) faults(stop chan struct{}, keys []string, every time.Duration) {
	t := time.NewTicker(every)
	defer t.Stop()
	for round := 0; ; round++ {
		select {
		case <-stop:
			return
		case <-t.C:
		}
		victims := slices.DeleteFunc(s.view(), func(n string) bool { return n == "n0" })
		switch round % 3 {
		case 0:
			v := victims[rand.N(len(victims))]
			m := s.get(v)
			m.kill()
			wipe := round%2 == 0
			if wipe {
				os.RemoveAll(m.cache)
			}
			s.fault("kill", "SIGKILL %s (cache wiped: %v), restart", v, wipe)
			time.Sleep(time.Second)
			if err := s.start(v, true); err != nil {
				s.fail("restart %s: %v", v, err)
			}
		case 1:
			k := keys[rand.N(len(keys))]
			via := s.get(victims[rand.N(len(victims))])
			if via == nil {
				continue
			}
			rr, ok := via.do(sReq{Op: "resolve", Key: k}, 5*time.Second)
			if !ok || rr.Route == nil {
				continue
			}
			m := s.get(rr.Route.Node)
			if m == nil {
				continue
			}
			m.cmd.Process.Signal(syscall.SIGSTOP)
			s.fault("stop", "SIGSTOP %s (owner of %s) for 6s", rr.Route.Node, k)
			time.Sleep(6 * time.Second)
			m.cmd.Process.Signal(syscall.SIGCONT)
		case 2:
			v := victims[rand.N(len(victims))]
			s.mu.Lock()
			s.fresh++
			name := fmt.Sprintf("r%d", s.fresh)
			s.mu.Unlock()
			if err := s.start(name, true); err != nil {
				s.fail("start %s: %v", name, err)
				continue
			}
			m := s.get(v)
			s.mu.Lock()
			s.members = slices.DeleteFunc(s.members, func(n string) bool { return n == v })
			s.mu.Unlock()
			m.kill()
			os.RemoveAll(m.cache)
			s.mu.Lock()
			if m.last != nil {
				s.retired = append(s.retired, *m.last)
			}
			delete(s.nodes, v)
			s.mu.Unlock()
			s.fault("replace", "%s replaced by %s", v, name)
		}
	}
}

// swindow is a porcupine window on one entity: the boundary state (counter
// and ring at the window's start, read while no operation on the entity was
// in flight) and the operations invoked during it.
type swindow struct {
	key   string
	start int64
	n0    int64
	ids   []string
	ops   []shop
}

// sampler checks one entity window after another for the whole run.
func (s *soakRun) sampler(stop chan struct{}, keys []string, length time.Duration) {
	c := &sclient{id: -4, s: s, owner: map[string]string{}, suspect: map[string]time.Time{}}
	for i := 0; ; i++ {
		k := keys[i%len(keys)]
		g := s.gates[k]
		g.Lock()
		o := c.run(sReq{Op: "ring", Inc: s.refs[k]}, k)
		var w *swindow
		if o.Class == "ack" {
			n0, _ := strconv.ParseInt(o.Value, 10, 64)
			w = &swindow{key: k, start: c.now(), n0: n0, ids: o.IDs}
			s.smu.Lock()
			s.windows[k] = w
			s.smu.Unlock()
		}
		g.Unlock()
		if w == nil {
			s.smu.Lock()
			s.windowsSkip++
			s.smu.Unlock()
			select {
			case <-stop:
				return
			case <-time.After(time.Second):
			}
			continue
		}
		select {
		case <-stop:
		case <-time.After(length):
		}
		g.Lock()
		s.smu.Lock()
		delete(s.windows, k)
		var pre []unk
		for _, u := range s.unknown[k] {
			if int64(u.Start.Sub(s.t0)) < w.start {
				pre = append(pre, u)
			}
		}
		s.smu.Unlock()
		g.Unlock()
		res, nops := checkWindow(w, pre)
		s.smu.Lock()
		s.windowOps += int64(nops)
		if res == porcupine.Ok {
			s.windowsOK++
		} else {
			s.windowsBad++
		}
		s.smu.Unlock()
		if res != porcupine.Ok {
			s.fail("porcupine %s on %s window at %ds (%d ops)", res, k, w.start/int64(time.Second), nops)
			b, _ := json.Marshal(map[string]any{"window": w.key, "start": w.start, "n0": w.n0, "ids": w.ids, "ops": w.ops, "pre": pre})
			os.WriteFile(filepath.Join(s.out, fmt.Sprintf("window-%s-%d.json", k, w.start)), b, 0o644)
		}
		select {
		case <-stop:
			return
		default:
		}
	}
}

// Porcupine model of one bounded ledger entity: the state is the counter and
// the concatenated IDs known to be committed (the boundary ring, then the
// window's commits). A new write appends and answers counter+1; a replay
// answers the counter at its commit; a strong read answers the counter and
// the last (up to 8) IDs.
type wstate struct {
	n   int64
	ids string
}

type win struct {
	Write bool
	ID    string
}

type wout struct {
	Known  bool
	Value  string
	joined string
	count  int
}

const idLen = 2 * len(garden.MutationID{}) // hex

var windowModel = porcupine.Model{
	Init: func() any { return wstate{} }, // replaced per window by checkWindow
	Step: func(st, in, out any) (bool, any) {
		s, i, o := st.(wstate), in.(win), out.(wout)
		if !i.Write {
			want := min(int(s.n), 8)
			return o.count == want && strings.HasSuffix(s.ids, o.joined) && len(o.joined) == want*idLen && o.Value == strconv.FormatInt(s.n, 10), s
		}
		for off := 0; ; {
			p := strings.Index(s.ids[off:], i.ID)
			if p < 0 {
				break
			}
			if p += off; p%idLen == 0 {
				at := s.n - int64(len(s.ids)/idLen) + int64(p/idLen) + 1
				return !o.Known || o.Value == strconv.FormatInt(at, 10), s
			}
			off = p + 1
		}
		return !o.Known || o.Value == strconv.FormatInt(s.n+1, 10), wstate{s.n + 1, s.ids + i.ID}
	},
	DescribeOperation: func(in, out any) string {
		i, o := in.(win), out.(wout)
		if !i.Write {
			return "read -> " + o.Value
		}
		return fmt.Sprintf("write(%s) -> %s", i.ID[len(i.ID)-6:], o.Value)
	},
}

const windowTimeout = 30 * time.Second

// checkWindow checks a window, first without its unknown writes (a
// linearization of the strict history extends to the whole one, as in
// killnode's linearizable), then with them; Unknown is a failure.
func checkWindow(w *swindow, pre []unk) (porcupine.CheckResult, int) {
	var ops []porcupine.Operation
	for _, u := range pre {
		ops = append(ops, porcupine.Operation{ClientId: 1000 + len(ops), Input: win{true, u.ID}, Output: wout{}, Call: w.start - 1, Return: math.MaxInt64})
	}
	for _, o := range w.ops {
		po := wout{Known: o.Class == "ack", Value: o.Value}
		if o.Op == "read" {
			po.joined, po.count = strings.Join(o.IDs, ""), len(o.IDs)
		}
		ops = append(ops, porcupine.Operation{ClientId: o.Client, Input: win{o.Op == "write", o.ID}, Output: po, Call: o.Start, Return: o.End})
	}
	m := windowModel
	init := wstate{w.n0, strings.Join(w.ids, "")}
	m.Init = func() any { return init }
	strict := slices.DeleteFunc(slices.Clone(ops), func(o porcupine.Operation) bool { return o.Return == math.MaxInt64 })
	if porcupine.CheckOperationsTimeout(m, strict, windowTimeout) == porcupine.Ok {
		return porcupine.Ok, len(ops)
	}
	return porcupine.CheckOperationsTimeout(m, ops, windowTimeout), len(ops)
}

// checkpoint writes a rolling report after a ledger check of every entity:
// each acknowledged write invoked before the entity's ring read must sit at
// its counter value in the ring (or be older than the ring: a coverage gap,
// also a failure), and the ring holds consecutive counters with distinct
// IDs (no duplicate commit). In the final check acknowledgments not yet
// checked must all be found.
func (s *soakRun) checkpoint(keys []string, final bool) {
	c := &sclient{id: -5, s: s, owner: map[string]string{}, suspect: map[string]time.Time{}}
	checked, unread := 0, 0
	for _, k := range keys {
		var o shop
		t := time.Now()
		for try := 0; try < 3 && o.Class != "ack"; try++ {
			o = c.run(sReq{Op: "ring", Inc: s.refs[k]}, k)
		}
		if o.Class != "ack" {
			unread++
			if final {
				s.fail("final ledger: ring of %s unreadable: %s %s", k, o.Code, o.Err)
			}
			continue
		}
		C, _ := strconv.ParseInt(o.Value, 10, 64)
		at := map[int64]string{}
		seen := map[string]bool{}
		for i, n := range o.Ns {
			if seen[o.IDs[i]] {
				s.fail("ledger %s: ID %s committed twice", k, o.IDs[i])
			}
			seen[o.IDs[i]] = true
			at[n] = o.IDs[i]
			if want := C - int64(len(o.Ns)) + int64(i) + 1; n != want {
				s.fail("ledger %s: ring row %d has counter %d, want %d (counter %d)", k, i, n, want, C)
				break
			}
		}
		if int64(len(o.Ns)) != min(C, int64(s.ring)) {
			s.fail("ledger %s: ring holds %d rows at counter %d", k, len(o.Ns), C)
		}
		s.smu.Lock()
		var keep []ack
		before := checked
		todo := s.pending[k]
		if final {
			todo = append(s.recent[k], todo...)
		}
		for _, a := range todo {
			if a.End.After(t) && !final {
				keep = append(keep, a)
				continue
			}
			checked++
			switch {
			case a.N > C:
				defer s.fail("ledger %s: acknowledged write %s at %d beyond counter %d (lost)", k, a.ID, a.N, C)
			case a.N <= C-int64(s.ring):
				defer s.fail("ledger %s: acknowledged write %s at %d aged out of the ring before its check (counter %d)", k, a.ID, a.N, C)
			case at[a.N] != a.ID:
				defer s.fail("ledger %s: acknowledged write %s at %d, ring has %q there", k, a.ID, a.N, at[a.N])
			}
		}
		if !final {
			r := append(s.recent[k], slices.DeleteFunc(slices.Clone(s.pending[k]), func(a ack) bool { return a.End.After(t) })...)
			r = slices.DeleteFunc(r, func(a ack) bool { return a.N <= C-int64(s.ring) })
			s.recent[k] = r
		}
		s.pending[k] = keep
		s.checkedAcks += int64(checked - before)
		s.smu.Unlock()
	}
	s.hmu.Lock()
	s.hz.Flush()
	s.hmu.Unlock()
	s.report(checked, unread, final)
}

// opStat: N/OK/Unknown/Failed are cumulative since the start; the rate and
// latencies (of acknowledged operations) cover the report interval.
type opStat struct {
	N, OK, Unknown, Failed int64
	PerSecond              float64
	P50, P95, P99, Max     string
}

func (s *soakRun) report(checked, unread int, final bool) {
	s.smu.Lock()
	s.reports++
	now := time.Now()
	if s.lastReportAt.IsZero() {
		s.lastReportAt = s.t0
	}
	el := now.Sub(s.lastReportAt).Seconds()
	ops := map[string]opStat{}
	for label, d := range s.lat {
		slices.Sort(d)
		q := func(x float64) string {
			if len(d) == 0 {
				return "-"
			}
			return d[min(len(d)-1, int(x*float64(len(d))))].Round(100 * time.Microsecond).String()
		}
		cnt := s.counts[label]
		ops[label] = opStat{N: cnt["ack"] + cnt["unknown"] + cnt["failed"], OK: cnt["ack"], Unknown: cnt["unknown"], Failed: cnt["failed"],
			PerSecond: float64(len(d)) / el, P50: q(.5), P95: q(.95), P99: q(.99), Max: q(1)}
	}
	s.lat = map[string][]time.Duration{}
	rep := map[string]any{"at": now.UTC(), "elapsed": now.Sub(s.t0).Round(time.Second).String(), "final": final,
		"ops_total": s.ops, "ops_per_s_interval": float64(s.ops-s.lastReportOps) / el, "interval_ops": ops,
		"codes": s.codes, "failures": len(s.failures), "faults": s.faultsBy, "ledger_checked_interval": checked,
		"ledger_checked_total": s.checkedAcks, "ledger_unreadable": unread, "lookups_verified": s.lookups,
		"windows_ok": s.windowsOK, "windows_failed": s.windowsBad, "windows_skipped": s.windowsSkip, "window_ops": s.windowOps,
		"collects": s.collects, "collect_errors": s.collectErrs}
	s.lastReportOps, s.lastReportAt = s.ops, now
	nodes := map[string]procStats{}
	for i := len(s.series) - 1; i >= 0 && len(nodes) < 8; i-- {
		if p := s.series[i]; p.T > now.Sub(s.t0).Seconds()-120 {
			if _, ok := nodes[p.Node]; !ok {
				nodes[p.Node] = p.Proc
			}
		}
	}
	s.smu.Unlock()
	type nodeLine struct {
		RSS, HeapAlloc, CacheDirBytes, CacheBytes int64
		Goroutines, FDs                           int
		S3                                        map[string]uint64
	}
	nl := map[string]nodeLine{}
	for name, p := range nodes {
		nl[name] = nodeLine{p.RSS, p.HeapAlloc, p.CacheDirBytes, p.Stats.CacheBytes, p.Goroutines, p.FDs, s3counts(p.Stats)}
	}
	rep["nodes"] = nl
	s.mu.Lock()
	var tot garden.Stats
	for _, p := range s.retired {
		addStorage(&tot, p.Stats)
	}
	for _, m := range s.nodes {
		if m.last != nil {
			addStorage(&tot, m.last.Stats)
		}
	}
	s.mu.Unlock()
	rep["s3_requests_total"] = s3counts(tot)
	b, _ := json.Marshal(rep)
	f, _ := os.OpenFile(filepath.Join(s.out, "report.jsonl"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	f.Write(append(b, '\n'))
	f.Close()
	bi, _ := json.MarshalIndent(rep, "", "  ")
	os.WriteFile(filepath.Join(s.out, "report-latest.json"), append(bi, '\n'), 0o644)
	var lines []string
	for _, l := range slices.Sorted(maps.Keys(ops)) {
		o := ops[l]
		lines = append(lines, fmt.Sprintf("%s n=%d ok=%d unk=%d fail=%d %.1f/s p50=%s p95=%s p99=%s", l, o.N, o.OK, o.Unknown, o.Failed, o.PerSecond, o.P50, o.P95, o.P99))
	}
	s.logx("report %d: total=%d %.1f ops/s ledger+%d windows ok=%d bad=%d failures=%d | %s", s.reports, rep["ops_total"],
		rep["ops_per_s_interval"], checked, rep["windows_ok"], rep["windows_failed"], rep["failures"], strings.Join(lines, " | "))
}

func s3counts(st garden.Stats) map[string]uint64 {
	m := map[string]uint64{}
	for i, name := range []string{"get", "put", "list", "delete"} {
		m[name] = st.Storage[i].Attempts
		m[name+"_failed"] = st.Storage[i].Failures
	}
	m["cas"] = st.CASAttempts
	m["cas_conflicts"] = st.CASConflicts
	return m
}

func addStorage(t *garden.Stats, s garden.Stats) {
	for i := range t.Storage {
		t.Storage[i].Attempts += s.Storage[i].Attempts
		t.Storage[i].Failures += s.Storage[i].Failures
	}
	t.CASAttempts += s.CASAttempts
	t.CASConflicts += s.CASConflicts
}

type soakSummary struct {
	Verdict          string
	Failures         []string
	Elapsed          string
	Ops              int64
	ByOp             map[string]map[string]int64
	Codes            map[string]int64
	Faults           map[string]int
	FaultLog         []string
	LedgerChecked    int64
	LookupsVerified  int64
	WindowsOK        int
	WindowsFailed    int
	WindowsSkipped   int
	WindowOps        int64
	Collects         int64
	CollectErrors    int64
	Growth           map[string]any
	MaxGoroutines    int
	MaxFDs           int
	MaxRSS           int64
	MaxCacheDirBytes int64
	S3RequestsTotal  map[string]uint64
}

// summary applies the §18.3 gates: the long-lived node n0's median RSS in
// the final quarter of the run (at most its final six hours) may exceed the
// median of the window of equal length before it by at most 5%, after
// accounting for intentionally retained live data/cache: each sample is
// moved to the windows' mean resident-entity count along the least-squares
// per-resident-entity cost (raw growth is reported beside it); every node
// stays within 1,024 goroutines and 2,500 descriptors, and n0's goroutine
// and descriptor medians do not grow either (5% plus 10 of slack).
func (s *soakRun) summary() soakSummary {
	s.smu.Lock()
	defer s.smu.Unlock()
	sum := soakSummary{Elapsed: time.Since(s.t0).Round(time.Second).String(), Ops: s.ops, ByOp: s.counts, Codes: s.codes, Faults: s.faultsBy,
		FaultLog: s.faultLog, LedgerChecked: s.checkedAcks, LookupsVerified: s.lookups, WindowsOK: s.windowsOK,
		WindowsFailed: s.windowsBad, WindowsSkipped: s.windowsSkip, WindowOps: s.windowOps, Collects: s.collects, CollectErrors: s.collectErrs}
	fails := slices.Clone(s.failures)
	for _, p := range s.series {
		sum.MaxGoroutines = max(sum.MaxGoroutines, p.Proc.Goroutines)
		sum.MaxFDs = max(sum.MaxFDs, p.Proc.FDs)
		sum.MaxRSS = max(sum.MaxRSS, p.Proc.RSS)
		sum.MaxCacheDirBytes = max(sum.MaxCacheDirBytes, p.Proc.CacheDirBytes)
	}
	if sum.MaxGoroutines > 1024 {
		fails = append(fails, fmt.Sprintf("goroutines peaked at %d > 1024", sum.MaxGoroutines))
	}
	if sum.MaxFDs > 2500 {
		fails = append(fails, fmt.Sprintf("descriptors peaked at %d > 2500", sum.MaxFDs))
	}
	var n0 []seriesPoint
	for _, p := range s.series {
		if p.Node == "n0" && p.Alive {
			n0 = append(n0, p)
		}
	}
	growth := map[string]any{}
	if len(n0) >= 8 {
		end := n0[len(n0)-1].T
		w := min(end/4, 6*3600)
		lo := end - 2*w
		res := func(p procStats) float64 { return float64(p.Stats.ResidentEntities) }
		med := func(lo, hi float64, f func(procStats) float64) float64 {
			var v []float64
			for _, p := range n0 {
				if p.T > lo && p.T <= hi {
					v = append(v, f(p.Proc))
				}
			}
			if len(v) == 0 {
				return 0
			}
			slices.Sort(v)
			return v[len(v)/2]
		}
		// slope is the least-squares cost of one resident entity over both
		// windows (never negative): the live data and cache §18.3 accounts for.
		slope := func(f func(procStats) float64) (b, mean float64) {
			var n, sx, sy, sxx, sxy float64
			for _, p := range n0 {
				if p.T > lo {
					x, y := res(p.Proc), f(p.Proc)
					n, sx, sy, sxx, sxy = n+1, sx+x, sy+y, sxx+x*x, sxy+x*y
				}
			}
			if d := n*sxx - sx*sx; n > 1 && d > 0 {
				b = max(0, (n*sxy-sx*sy)/d)
			}
			return b, sx / max(n, 1)
		}
		growth["window_s"] = w
		growth["resident_entities"] = map[string]float64{"before": med(lo, end-w, res), "final": med(end-w, end, res)}
		for _, m := range []struct {
			name  string
			f     func(procStats) float64
			slack float64
			gate  bool
		}{{"rss", func(p procStats) float64 { return float64(p.RSS) }, 0, true},
			{"heap_alloc", func(p procStats) float64 { return float64(p.HeapAlloc) }, 0, false},
			{"goroutines", func(p procStats) float64 { return float64(p.Goroutines) }, 10, true},
			{"fds", func(p procStats) float64 { return float64(p.FDs) }, 10, true}} {
			b, mean := slope(m.f)
			adj := func(p procStats) float64 { return m.f(p) - b*(res(p)-mean) } // at the mean resident count
			before, after := med(lo, end-w, m.f), med(end-w, end, m.f)
			abefore, aafter := med(lo, end-w, adj), med(end-w, end, adj)
			g, ag := 0.0, 0.0
			if before > 0 {
				g = after/before - 1
			}
			if abefore > 0 {
				ag = aafter/abefore - 1
			}
			growth[m.name] = map[string]float64{"before": before, "final": after, "growth": g, "per_resident": b,
				"adjusted_before": abefore, "adjusted_final": aafter, "adjusted_growth": ag}
			if m.gate && aafter > abefore*1.05+m.slack {
				fails = append(fails, fmt.Sprintf("n0 %s grew %.1f%% in the final %.0fs at equal resident entities (%.0f -> %.0f; raw %.1f%%)",
					m.name, 100*ag, w, abefore, aafter, 100*g))
			}
		}
	} else {
		fails = append(fails, fmt.Sprintf("only %d resource samples of n0: growth verdict inconclusive", len(n0)))
	}
	sum.Growth = growth
	var tot garden.Stats
	for _, p := range s.retired {
		addStorage(&tot, p.Stats)
	}
	sum.S3RequestsTotal = s3counts(tot)
	if s.windowsOK == 0 {
		fails = append(fails, "no porcupine window was checked")
	}
	sum.Failures = fails
	sum.Verdict = "pass"
	if len(fails) > 0 {
		sum.Verdict = "fail"
	}
	return sum
}
