package main

import (
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/OrdalieTech/big-garden/internal/storetest"
)

// TestBatchKill is TestProcessKill with group commit (PROTOCOL §6 Batches):
// one entity, 8 concurrent clients (8 adds each, every other one 40 KiB so
// most publications upload) against one node process with BatchMax 16,
// SIGKILLed at each failpoint's nth pass (n random in 4..6; sql.member:
// 4..40; reply: the 10th..39th reply; net.*: the proxy fault armed after
// 10..39 replies).
// The restart alternates an empty cache and the killed one's, every unknown
// is retried with its ID, and the ledger is checked as in killRun. The
// "unpublished invisible" check of killRun does not apply: with concurrent
// clients a committed member may die before its reply is written. Every
// add reply carries the node's Stats.Batches, so the test reports the batch
// sizes committed before the kill and requires multi-member batches in most
// runs of each failpoint; for sql.member, a failpoint inside a member's
// transaction (reached only while a batch of at least two is being
// assembled), the killed batch counts too.
//
// Partial batches: some adds are held-outcome members (PROTOCOL §6
// Batches): clients 0-2 send a callback that inserts its row and then fails
// at their 3rd and 6th add, client 3 reuses its last acknowledged ID with
// another input (id_conflict) and client 4 replays it, at their 3rd and 6th
// add. A first answer must be the final outcome (the callback's error
// unchanged, id_conflict, or the acknowledged result replayed), or
// unavailable / outcome_unknown / no reply when the publication did not
// commit; after the restart every one that was not final is retried and
// must reach its final outcome, and the ledger checks hold (a failed
// callback's row never appears).
func TestBatchKill(t *testing.T) {
	base := storetest.MinIO(t)
	t.Setenv("GARDEN_TEST_BATCH", "16")
	runs := 20
	if s := os.Getenv("GARDEN_KILL_RUNS"); s != "" {
		runs, _ = strconv.Atoi(s)
	}
	for _, fp := range append(slices.Clone(failpoints), "sql.member") {
		t.Run(fp, func(t *testing.T) {
			var tot tally
			var sizes [16]uint64
			multi := 0
			partial := map[string]int{}
			for i := range runs {
				ep := base.Sub(fmt.Sprintf("batch-%s-%d", fp, i))
				n := 4 + rand.N(3)
				switch {
				case fp == "reply" || strings.HasPrefix(fp, "net."):
					n = 10 + rand.N(30)
				case fp == "sql.member": // ~7 member passes per batch: also land in batches with held-outcome members
					n = 4 + rand.N(37)
				}
				r, b := batchKillRun(t, ep, fp, n, i%2 == 1, partial)
				tot.killed, tot.acked, tot.unknown, tot.replayed = tot.killed+r.killed, tot.acked+r.acked, tot.unknown+r.unknown, tot.replayed+r.replayed
				m := uint64(0)
				for k := range b {
					sizes[k] += b[k]
					if k > 0 {
						m += b[k]
					}
				}
				if m > 0 || (fp == "sql.member" && r.killed > 0) {
					multi++
				}
				clean(t, ep)
				if t.Failed() {
					return
				}
			}
			t.Logf("%s: %d runs, %d killed at the failpoint, %d with a multi-member batch committed before the kill; "+
				"batch sizes before the kill (members:publications) %s; %d acknowledged ops, %d unknown retried (%d replayed a commit); "+
				"held-outcome members (kind:first answer, all final after the restart) %v",
				fp, runs, tot.killed, multi, batchSizes(sizes), tot.acked, tot.unknown, tot.replayed, partial)
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
func batchKillRun(t *testing.T, ep storetest.Endpoint, mode string, armAt int, reuse bool, partial map[string]int) (tl tally, batches []uint64) {
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
	var held []*special
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
				var last *op // this client's last acknowledged add
				for i := range 8 {
					o := &op{req: Req{Op: "add", Key: "k", Inc: inc, ID: newID(), Input: fmt.Sprintf("%d-%d", c, i), Big: i%2 == 1,
						Tag: fmt.Sprintf("%d-%d", c, i)}}
					var s *special
					switch {
					case c < 3 && i%3 == 2:
						o.req.Fail = true
						s = &special{kind: "fail", req: o.req}
					case (c == 3 || c == 4) && i%3 == 2:
						s = &special{kind: "replay", req: last.req, ack: last.resp}
						s.req.Tag = o.req.Tag
						if c == 3 {
							s.kind, s.req.Input = "conflict", s.req.Input+"-reuse"
						}
					}
					omu.Lock()
					if s != nil {
						held = append(held, s)
					} else {
						ops = append(ops, o)
					}
					omu.Unlock()
					ch := make(chan *Resp, 1)
					mu.Lock()
					if dead {
						mu.Unlock()
						o.outcome = "unknown"
						if s != nil {
							s.first = "unsent"
						}
						return
					}
					pending[o.req.Tag] = ch
					b, _ := json.Marshal(o.req)
					if s != nil {
						b, _ = json.Marshal(s.req)
					}
					p.in.Write(append(b, '\n'))
					mu.Unlock()
					r := <-ch
					if s != nil {
						switch {
						case r == nil:
							s.first = "none"
						case s.final(*r):
							s.first = "final"
							continue
						case r.Code == "outcome_unknown" || r.Code == "unavailable":
							s.first = r.Code
							if px != nil && r.Code == "outcome_unknown" {
								p.cmd.Process.Kill()
							}
						default:
							s.first = "bad"
							t.Errorf("%s: held-outcome %s member %s answered %+v", failpoint, s.kind, s.req.ID, r)
						}
						return
					}
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
					last = o
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
	for _, s := range held {
		partial[s.kind+":"+s.first]++
		if s.first == "final" {
			continue
		}
		r := s.req
		r.Inc = inc
		if resp := call(r); !s.final(resp) {
			t.Fatalf("%s: retry of held-outcome %s member %s (first answer %s): %+v", failpoint, s.kind, r.ID, s.first, resp)
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

// special is a held-outcome member of TestBatchKill: a failing callback
// ("fail"), or an acknowledged add's ID reused with another input
// ("conflict") or replayed ("replay"). first is how its first attempt ended:
// "final", "unavailable", "outcome_unknown", "none" (no reply) or "unsent".
type special struct {
	kind, first string
	req         Req
	ack         Resp // the acknowledged add whose ID it reuses
}

// final reports whether r is s's outcome once the publication that
// answers it committed (PROTOCOL §6 Batches).
func (s *special) final(r Resp) bool {
	switch s.kind {
	case "fail":
		return r.Code == "other" && strings.Contains(r.Err, "refused by callback")
	case "conflict":
		return r.Code == "id_conflict"
	}
	return r.OK && r.Replayed && r.Value == s.ack.Value && r.Seq == s.ack.Seq
}
