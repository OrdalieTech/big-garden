package main

import (
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/OrdalieTech/big-garden/internal/storetest"
)

// TestBatchKill is TestProcessKill with group commit (PROTOCOL §6 Batches):
// one entity, 8 concurrent clients (8 adds each, every other one 40 KiB so
// most publications upload) against one node process with BatchMax 16,
// SIGKILLed at each failpoint's nth pass (n random in 4..6; reply: the
// 10th..39th reply; net.*: the proxy fault armed after 10..39 replies).
// The restart alternates an empty cache and the killed one's, every unknown
// is retried with its ID, and the ledger is checked as in killRun. The
// "unpublished invisible" check of killRun does not apply: with concurrent
// clients a committed member may die before its reply is written. Every
// add reply carries the node's Stats.Batches, so the test reports the batch
// sizes committed before the kill and requires multi-member batches in most
// runs of each failpoint.
func TestBatchKill(t *testing.T) {
	base := storetest.MinIO(t)
	t.Setenv("GARDEN_TEST_BATCH", "16")
	runs := 20
	if s := os.Getenv("GARDEN_KILL_RUNS"); s != "" {
		runs, _ = strconv.Atoi(s)
	}
	for _, fp := range failpoints {
		t.Run(fp, func(t *testing.T) {
			var tot tally
			var sizes [16]uint64
			multi := 0
			for i := range runs {
				ep := base.Sub(fmt.Sprintf("batch-%s-%d", fp, i))
				n := 4 + rand.N(3)
				if fp == "reply" || strings.HasPrefix(fp, "net.") {
					n = 10 + rand.N(30)
				}
				r, b := batchKillRun(t, ep, fp, n, i%2 == 1)
				tot.killed, tot.acked, tot.unknown, tot.replayed = tot.killed+r.killed, tot.acked+r.acked, tot.unknown+r.unknown, tot.replayed+r.replayed
				m := uint64(0)
				for k := range b {
					sizes[k] += b[k]
					if k > 0 {
						m += b[k]
					}
				}
				if m > 0 {
					multi++
				}
				clean(t, ep)
				if t.Failed() {
					return
				}
			}
			t.Logf("%s: %d runs, %d killed at the failpoint, %d with a multi-member batch committed before the kill; "+
				"batch sizes before the kill (members:publications) %s; %d acknowledged ops, %d unknown retried (%d replayed a commit)",
				fp, runs, tot.killed, multi, batchSizes(sizes), tot.acked, tot.unknown, tot.replayed)
			if tot.killed == 0 {
				t.Fatalf("failpoint %s never fired", fp)
			}
			if 2*multi <= runs {
				t.Fatalf("multi-member batches formed before the kill in only %d of %d runs", multi, runs)
			}
		})
	}
}

// batchKillRun returns the largest Stats.Batches seen in a reply of the
// killed process.
func batchKillRun(t *testing.T, ep storetest.Endpoint, mode string, armAt int, reuse bool) (tl tally, batches []uint64) {
	t.Helper()
	cache := t.TempDir()
	failpoint := fmt.Sprintf("%s:%d", mode, armAt)
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
	create := &op{req: Req{Op: "create", Key: "k", ID: newID()}}
	resp, alive := p.call(t, create.req)
	switch {
	case !alive || resp.Code == "outcome_unknown":
		create.outcome = "unknown"
	case resp.OK:
		create.outcome, create.resp = "ack", resp
	default:
		create.outcome, create.resp = "failed", resp
	}
	ops := []*op{create}
	inc := create.resp.Inc
	done := make(chan struct{})
	if create.outcome != "ack" {
		close(done)
	} else {
		// a dispatcher routes tagged replies; a closed stream answers every
		// pending request as dead
		var mu sync.Mutex
		pending := map[string]chan *Resp{}
		dead, replies := false, 1
		go func() {
			defer close(done)
			for line := range p.lines {
				var r Resp
				if err := json.Unmarshal(line, &r); err != nil {
					panic(err)
				}
				mu.Lock()
				if replies++; px != nil && replies == armAt {
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
				if len(r.Batches) > 0 && sum(r.Batches) > sum(batches) {
					batches = r.Batches
				}
				c := pending[r.Tag]
				delete(pending, r.Tag)
				mu.Unlock()
				c <- &r
			}
			mu.Lock()
			dead = true
			for _, c := range pending {
				c <- nil
			}
			clear(pending)
			mu.Unlock()
		}()
		var wg sync.WaitGroup
		var omu sync.Mutex
		for c := range 8 {
			wg.Go(func() {
				for i := range 8 {
					o := &op{req: Req{Op: "add", Key: "k", Inc: inc, ID: newID(), Input: fmt.Sprintf("%d-%d", c, i), Big: i%2 == 1,
						Tag: fmt.Sprintf("%d-%d", c, i)}}
					omu.Lock()
					ops = append(ops, o)
					omu.Unlock()
					ch := make(chan *Resp, 1)
					mu.Lock()
					if dead {
						mu.Unlock()
						o.outcome = "unknown"
						return
					}
					pending[o.req.Tag] = ch
					b, _ := json.Marshal(o.req)
					p.in.Write(append(b, '\n'))
					mu.Unlock()
					r := <-ch
					switch {
					case r == nil:
						o.outcome = "unknown"
					case r.OK:
						o.outcome, o.resp = "ack", *r
					case r.Code == "outcome_unknown":
						o.outcome = "unknown"
						if px != nil {
							p.cmd.Process.Kill()
						}
					default:
						o.outcome, o.resp = "failed", *r
						t.Logf("%s: definite failure %s: %s", failpoint, r.Code, r.Err)
					}
					if o.outcome != "ack" {
						return
					}
				}
			})
		}
		wg.Wait()
	}
	if p.stop() {
		tl.killed = 1
	}
	<-done
	if px != nil {
		px.SetPlan(nil)
		if mode == "net.late" && reuse {
			release()
		}
	}
	if !reuse {
		cache = t.TempDir()
	}
	p = start(t, ep, cache, "")
	defer p.stop()
	call := func(r Req) Resp {
		r.Tag = ""
		resp, alive := p.call(t, r)
		if !alive {
			t.Fatalf("%s: restarted node died on %+v", failpoint, r)
		}
		return resp
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
		tl.unknown++
		if resp.Replayed {
			tl.replayed++
		}
		if r.Op == "create" {
			inc = resp.Inc
		} else {
			committed[r.ID] = true
		}
	}
	if mode == "net.late" && !reuse && inc != "" {
		release()
		r := Req{Op: "add", Key: "k", Inc: inc, ID: newID(), Input: "after-late"}
		if resp := call(r); !resp.OK {
			t.Fatalf("%s: add after the late apply: %s %s", failpoint, resp.Code, resp.Err)
		}
		committed[r.ID] = true
	}
	if inc == "" {
		if resp := call(Req{Op: "create", Key: "k", ID: newID()}); !resp.OK {
			t.Fatalf("%s: create after a definitely failed create: %s %s", failpoint, resp.Code, resp.Err)
		}
		return tl, batches
	}
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
	if resp := call(Req{Op: "create", Key: "k", ID: newID()}); resp.OK {
		t.Fatalf("%s: a second create of an existing key succeeded: %+v", failpoint, resp)
	}
	rd := call(Req{Op: "read", Key: "k", Inc: inc})
	if !rd.OK {
		t.Fatalf("%s: read: %s %s", failpoint, rd.Code, rd.Err)
	}
	seen := map[string]int{}
	for _, id := range rd.IDs {
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
	return tl, batches
}

func sum(b []uint64) (s uint64) {
	for _, v := range b {
		s += v
	}
	return s
}
