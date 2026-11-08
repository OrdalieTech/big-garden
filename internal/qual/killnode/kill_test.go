package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/rand/v2"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/OrdalieTech/big-garden/internal/store"
	"github.com/OrdalieTech/big-garden/internal/storetest"
)

// failpoints are the commit boundaries of PLAN §16 rows 1-3 (garden's
// failpoint() calls) plus "reply", after the response reached the parent,
// and two network faults through a storetest.Proxy after which the parent
// kills a node that answered outcome_unknown: "net.lost" (head CAS applied,
// reply lost, head unreadable) and "net.late" (head CAS held, head
// unreadable; the held CAS applies after the kill, before the restart in odd
// runs and after the restarted node's retry in even runs).
var failpoints = []string{"sql.before", "sql.inside", "sql.after", "capture",
	"put.before", "put.mid", "put.after", "cas.before", "cas.after", "reply", "net.lost", "net.late"}

var (
	buildOnce sync.Once
	binary    string
	buildErr  error
)

func nodeBinary(t *testing.T) string {
	buildOnce.Do(func() {
		dir, err := os.MkdirTemp("", "killnode")
		if err != nil {
			buildErr = err
			return
		}
		binary = filepath.Join(dir, "killnode")
		out, err := exec.Command("go", "build", "-tags", "gardenfail", "-o", binary, ".").CombinedOutput()
		if err != nil {
			buildErr = fmt.Errorf("build: %v: %s", err, out)
		}
	})
	if buildErr != nil {
		t.Fatal(buildErr)
	}
	return binary
}

// proc is one running node process.
type proc struct {
	cmd   *exec.Cmd
	in    io.WriteCloser
	lines chan []byte
}

func start(t *testing.T, ep storetest.Endpoint, cache, failpoint string, args ...string) *proc {
	cmd := exec.Command(nodeBinary(t), append([]string{"-url", ep.URL, "-bucket", ep.Bucket, "-prefix", ep.Prefix, "-cache", cache,
		"-commit-timeout", "1s"}, args...)...)
	cmd.Env = append(os.Environ(), "AWS_ACCESS_KEY_ID="+ep.AccessKey, "AWS_SECRET_ACCESS_KEY="+ep.SecretKey,
		"GARDEN_FAILPOINT="+failpoint)
	cmd.Stderr = os.Stderr
	in, err := cmd.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	out, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	p := &proc{cmd: cmd, in: in, lines: make(chan []byte)}
	go func() {
		s := bufio.NewScanner(out)
		s.Buffer(nil, 1<<20)
		for s.Scan() {
			p.lines <- append([]byte(nil), s.Bytes()...)
		}
		close(p.lines)
	}()
	return p
}

// call sends r; ok is false if the process died before answering.
func (p *proc) call(t *testing.T, r Req) (Resp, bool) {
	b, _ := json.Marshal(r)
	if _, err := p.in.Write(append(b, '\n')); err != nil {
		return Resp{}, false
	}
	select {
	case line, ok := <-p.lines:
		if !ok {
			return Resp{}, false
		}
		var resp Resp
		if err := json.Unmarshal(line, &resp); err != nil {
			t.Fatal(err)
		}
		return resp, true
	case <-time.After(60 * time.Second):
		t.Fatalf("node hung on %+v", r)
		return Resp{}, false
	}
}

// stop closes stdin and reports whether the process was SIGKILLed.
func (p *proc) stop() (killed bool) {
	p.in.Close()
	for range p.lines {
	}
	err := p.cmd.Wait()
	ee, ok := err.(*exec.ExitError)
	return ok && ee.ProcessState.String() == "signal: killed"
}

func newID() string {
	var b [16]byte
	binary := uint64(time.Now().UnixMilli())
	for i := range 8 {
		b[i] = byte(binary >> (56 - 8*i))
	}
	for i := 8; i < 16; i++ {
		b[i] = byte(rand.N(256))
	}
	return fmt.Sprintf("%x", b)
}

// TestProcessKill runs the M4 kill matrix against MinIO: for each failpoint,
// GARDEN_KILL_RUNS times (default 20) one node process works on a fresh
// entity and is SIGKILLed at the failpoint's nth pass (n random in 1..4),
// then a new process (on an empty cache directory in even runs, on the
// killed one's directory in odd runs) retries the unknown operation with the
// same ID and the parent's acknowledgment ledger is checked.
func TestProcessKill(t *testing.T) {
	base := storetest.Local(t)
	runs := 20
	if s := os.Getenv("GARDEN_KILL_RUNS"); s != "" {
		runs, _ = strconv.Atoi(s)
	}
	for _, fp := range failpoints {
		t.Run(fp, func(t *testing.T) {
			var tot tally
			for i := range runs {
				ep := base.Sub(fmt.Sprintf("%s-%d", fp, i))
				r := killRun(t, ep, fmt.Sprintf("%s:%d", fp, 1+rand.N(4)), i%2 == 1)
				tot.killed, tot.acked, tot.unknown, tot.replayed = tot.killed+r.killed, tot.acked+r.acked, tot.unknown+r.unknown, tot.replayed+r.replayed
				clean(t, ep)
				if t.Failed() {
					return
				}
			}
			t.Logf("%s: %d runs, %d killed at the failpoint; %d acknowledged ops, %d unknown retried (%d replayed a commit)",
				fp, runs, tot.killed, tot.acked, tot.unknown, tot.replayed)
			if tot.killed == 0 {
				t.Fatalf("failpoint %s never fired", fp)
			}
		})
	}
}

type op struct {
	req     Req
	outcome string // "ack", "unknown" (outcome_unknown or killed), "failed" (any other definite error)
	resp    Resp
}

type tally struct{ killed, acked, unknown, replayed int }

// killRun restarts with an empty cache directory, or with the killed
// process's own one when reuse is set (its residue must be ignored).
func killRun(t *testing.T, ep storetest.Endpoint, failpoint string, reuse bool) (tl tally) {
	t.Helper()
	cache := t.TempDir()
	mode, k, _ := strings.Cut(failpoint, ":")
	armAt, _ := strconv.Atoi(k)
	var px *storetest.Proxy
	hold := make(chan struct{})
	if strings.HasPrefix(mode, "net.") {
		var err error
		if px, err = storetest.NewProxy(ep.URL); err != nil {
			t.Fatal(err)
		}
		defer px.Close()
		defer func() {
			select {
			case <-hold:
			default:
				close(hold)
			}
		}()
		ep.URL, failpoint = px.URL(), ""
	}
	release := func() { close(hold); px.Wait() }
	p := start(t, ep, cache, failpoint)
	var ops []*op
	inc := ""
	do := func(r Req) bool {
		o := &op{req: r}
		if px != nil && len(ops) == armAt {
			px.SetPlan(func(r storetest.Request) storetest.Fault {
				switch {
				case !strings.HasSuffix(r.Path, "/head.json"):
				case r.Method == "GET":
					return storetest.Fault{Status: 503, Code: "SlowDown"}
				case r.Method == "PUT" && mode == "net.lost":
					return storetest.Fault{Drop: true}
				case r.Method == "PUT":
					return storetest.Fault{Hold: hold}
				}
				return storetest.Fault{}
			})
		}
		ops = append(ops, o)
		resp, alive := p.call(t, r)
		if px != nil && alive && resp.Code == "outcome_unknown" {
			p.cmd.Process.Kill()
			alive = false
		}
		switch {
		case !alive:
			o.outcome = "unknown"
		case resp.OK:
			o.outcome, o.resp = "ack", resp
		case resp.Code == "outcome_unknown":
			o.outcome = "unknown"
		default:
			o.outcome, o.resp = "failed", resp
		}
		return o.outcome == "ack"
	}
	if do(Req{Op: "create", Key: "k", ID: newID()}) {
		inc = ops[0].resp.Inc
		for i := range 8 {
			if !do(Req{Op: "add", Key: "k", Inc: inc, ID: newID(), Input: strconv.Itoa(i), Big: i%2 == 1}) {
				break
			}
		}
	}
	if p.stop() {
		tl.killed = 1
	}
	if last := ops[len(ops)-1]; last.outcome == "failed" {
		t.Logf("%s: definite failure %s: %s", failpoint, last.resp.Code, last.resp.Err)
	}

	if px != nil {
		px.SetPlan(nil)
		if mode == "net.late" && reuse {
			release()
		}
	}
	// restart with an empty cache; retry every unknown with the same ID and input
	if !reuse {
		cache = t.TempDir()
	}
	p = start(t, ep, cache, "")
	defer p.stop()
	call := func(r Req) Resp {
		resp, alive := p.call(t, r)
		if !alive {
			t.Fatalf("%s: restarted node died on %+v", failpoint, r)
		}
		return resp
	}
	// sql.* and capture run only on the request path, before any head CAS:
	// the killed operation cannot be published, so the restarted node, even
	// on the killed one's cache, must not show it (no speculative exposure)
	// and its retry must commit it anew.
	preCAS := strings.HasPrefix(mode, "sql.") || mode == "capture"
	if preCAS && inc != "" {
		resp := call(Req{Op: "read", Key: "k", Inc: inc})
		for _, o := range ops {
			if o.outcome == "unknown" && contains(resp.IDs, o.req.ID) {
				t.Fatalf("%s: unpublished %s visible after restart (reuse %v)", failpoint, o.req.ID, reuse)
			}
		}
	}
	committed := map[string]bool{}
	for _, o := range ops {
		if o.outcome != "unknown" {
			continue
		}
		r := o.req
		r.Inc = inc
		resp := call(r)
		if !resp.OK {
			t.Fatalf("%s: retry of unknown %s %s: %s %s", failpoint, r.Op, r.ID, resp.Code, resp.Err)
		}
		if preCAS && resp.Replayed {
			t.Fatalf("%s: retry of an unpublished %s replayed", failpoint, r.ID)
		}
		tl.unknown++
		if resp.Replayed {
			tl.replayed++
		}
		if r.Op == "create" {
			if inc != "" && resp.Inc != inc {
				t.Fatalf("%s: create retry changed the incarnation", failpoint)
			}
			inc = resp.Inc
		} else {
			committed[r.ID] = true
		}
	}
	if mode == "net.late" && !reuse && inc != "" {
		release() // the held CAS applies late, after the retry fenced it
		r := Req{Op: "add", Key: "k", Inc: inc, ID: newID(), Input: "after-late"}
		if resp := call(r); !resp.OK {
			t.Fatalf("%s: add after the late apply: %s %s", failpoint, resp.Code, resp.Err)
		}
		committed[r.ID] = true
	}
	if inc == "" { // the create definitely failed: nothing may exist
		resp := call(Req{Op: "create", Key: "k", ID: newID()})
		if !resp.OK {
			t.Fatalf("%s: create after a definitely failed create: %s %s", failpoint, resp.Code, resp.Err)
		}
		return tl
	}
	// acknowledged and retried operations replay exactly, with their input only
	for _, o := range ops {
		r := o.req
		r.Inc = inc
		switch o.outcome {
		case "ack":
			tl.acked++
			resp := call(r)
			if !resp.OK || !resp.Replayed || resp.Value != o.resp.Value || resp.Seq != o.resp.Seq || (r.Op == "create" && resp.Inc != inc) {
				t.Fatalf("%s: replay of acknowledged %s %s: %+v, acknowledged %+v", failpoint, r.Op, r.ID, resp, o.resp)
			}
			if r.Op == "add" {
				committed[r.ID] = true
			}
			r.Input += "-other"
			if resp := call(r); resp.Code != "id_conflict" {
				t.Fatalf("%s: same ID, other input on %s: %+v", failpoint, r.Op, resp)
			}
		case "failed":
			if r.Op == "add" {
				if resp := call(Req{Op: "read", Key: "k", Inc: inc}); contains(resp.IDs, r.ID) {
					t.Fatalf("%s: a definitely failed add %s committed", failpoint, r.ID)
				}
			}
		}
	}
	// no empty reinitialization: another create of the key never succeeds
	if resp := call(Req{Op: "create", Key: "k", ID: newID()}); resp.OK {
		t.Fatalf("%s: a second create of an existing key succeeded: %+v", failpoint, resp)
	}
	resp := call(Req{Op: "read", Key: "k", Inc: inc})
	if !resp.OK {
		t.Fatalf("%s: read: %s %s", failpoint, resp.Code, resp.Err)
	}
	seen := map[string]int{}
	for _, id := range resp.IDs {
		seen[id]++
	}
	for id, c := range seen {
		if c != 1 || !committed[id] {
			t.Fatalf("%s: committed ID %s appears %d times (expected committed: %v)", failpoint, id, c, committed[id])
		}
	}
	for id := range committed {
		if seen[id] != 1 {
			t.Fatalf("%s: acknowledged or retried ID %s lost", failpoint, id)
		}
	}
	return tl
}

func contains(s []string, v string) bool {
	for _, x := range s {
		if x == v {
			return true
		}
	}
	return false
}

func clean(t *testing.T, ep storetest.Endpoint) {
	st := store.NewS3(ep.Client(nil), ep.Bucket, ep.Prefix, 5*time.Second)
	for range 100 {
		page, _, err := st.List(context.Background(), "", "", 1000)
		if err != nil || len(page) == 0 {
			return
		}
		for _, o := range page {
			st.Delete(context.Background(), o.Key)
		}
	}
}

// stopped reports whether the process is stopped by a signal.
func (p *proc) stopped() bool {
	b, _ := os.ReadFile(fmt.Sprintf("/proc/%d/stat", p.cmd.Process.Pid))
	i := strings.LastIndexByte(string(b), ')')
	return i > 0 && i+2 < len(b) && b[i+2] == 'T'
}

// TestStaleOwner is the first M5 scenario on MinIO: owner a is SIGSTOPped at
// a commit boundary (its nth pass, n random in 1..4), node b takes over and
// commits, then a is continued. a's pending operation may only succeed if
// b's lineage holds it; a strong read from a never shows a state without b's
// acknowledged writes; the ledger is checked through a third node c.
func TestStaleOwner(t *testing.T) {
	base := storetest.Local(t)
	runs := 20
	if s := os.Getenv("GARDEN_KILL_RUNS"); s != "" {
		runs, _ = strconv.Atoi(s)
	}
	for _, fp := range []string{"sql.after", "capture", "put.after", "cas.before", "cas.after"} {
		t.Run(fp, func(t *testing.T) {
			stops, aOK := 0, 0
			for i := range runs {
				ep := base.Sub(fmt.Sprintf("stale-%s-%d", fp, i))
				s, ok := staleRun(t, ep, fmt.Sprintf("%s:%d:stop", fp, 2+rand.N(4)))
				stops, aOK = stops+s, aOK+ok
				clean(t, ep)
				if t.Failed() {
					return
				}
			}
			t.Logf("%s: %d runs, %d stopped at the failpoint, %d stale-owner operations acknowledged after SIGCONT", fp, runs, stops, aOK)
			if stops == 0 {
				t.Fatalf("failpoint %s never stopped the owner", fp)
			}
		})
	}
}

func staleRun(t *testing.T, ep storetest.Endpoint, failpoint string) (stops, aOK int) {
	a := start(t, ep, t.TempDir(), failpoint, "-node", "a")
	defer a.stop()
	b := start(t, ep, t.TempDir(), "", "-node", "b")
	defer b.stop()
	must := func(p *proc, r Req) Resp {
		resp, alive := p.call(t, r)
		if !alive || !resp.OK {
			t.Fatalf("%s: %+v: %+v", failpoint, r, resp)
		}
		return resp
	}
	inc := must(a, Req{Op: "create", Key: "k", ID: newID()}).Inc
	committed := map[string]bool{}
	var pending Req
	for i := range 8 {
		r := Req{Op: "add", Key: "k", Inc: inc, ID: newID(), Input: strconv.Itoa(i), Big: i%2 == 1}
		bs, _ := json.Marshal(r)
		a.in.Write(append(bs, '\n'))
		var line []byte
		for line == nil && !a.stopped() {
			select {
			case line = <-a.lines:
			case <-time.After(5 * time.Millisecond):
			}
		}
		if line == nil {
			pending = r
			break
		}
		var resp Resp
		json.Unmarshal(line, &resp)
		if !resp.OK {
			t.Fatalf("%s: add before the stop: %+v", failpoint, resp)
		}
		committed[r.ID] = true
	}
	if pending.ID == "" {
		return 0, 0 // the failpoint was not reached
	}
	must(b, Req{Op: "takeover", Key: "k"})
	var bIDs []string
	for i := range 2 {
		r := Req{Op: "add", Key: "k", Inc: inc, ID: newID(), Input: "b" + strconv.Itoa(i), Big: i == 1}
		must(b, r)
		committed[r.ID] = true
		bIDs = append(bIDs, r.ID)
	}
	a.cmd.Process.Signal(syscall.SIGCONT)
	var resp Resp
	select {
	case line, ok := <-a.lines:
		if !ok {
			t.Fatalf("%s: stale owner died", failpoint)
		}
		json.Unmarshal(line, &resp)
	case <-time.After(30 * time.Second):
		t.Fatalf("%s: stale owner hung", failpoint)
	}
	if resp.OK {
		aOK = 1
		committed[pending.ID] = true
	}
	if rd, _ := a.call(t, Req{Op: "read", Key: "k", Inc: inc}); rd.OK {
		for _, id := range bIDs {
			if !contains(rd.IDs, id) {
				t.Fatalf("%s: stale owner's strong read misses b's acknowledged %s", failpoint, id)
			}
		}
	}
	if !resp.OK && resp.Code == "outcome_unknown" { // retried on the owner with the same ID: at most once
		must(b, pending)
		committed[pending.ID] = true
	}
	c := start(t, ep, t.TempDir(), "", "-node", "c")
	defer c.stop()
	must(c, Req{Op: "takeover", Key: "k"}) // recovered from S3 alone
	seen := map[string]int{}
	for _, id := range must(c, Req{Op: "read", Key: "k", Inc: inc}).IDs {
		seen[id]++
	}
	for id, n := range seen {
		if n != 1 || !committed[id] {
			t.Fatalf("%s: ID %s committed %d times (acknowledged or retried: %v); stale reply %+v", failpoint, id, n, committed[id], resp)
		}
	}
	for id := range committed {
		if seen[id] != 1 {
			t.Fatalf("%s: acknowledged ID %s lost; stale reply %+v", failpoint, id, resp)
		}
	}
	return 1, aOK
}
