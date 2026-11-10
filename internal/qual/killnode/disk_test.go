package main

import (
	"fmt"
	"math/rand/v2"
	"slices"
	"strconv"
	"testing"
	"time"

	"github.com/OrdalieTech/big-garden/internal/storetest"
)

// TestDiskFull (PLAN §16 row 23) makes local writes fail for real: the node
// processes run under RLIMIT_FSIZE, so every write past the limit fails with
// EFBIG, whether it is the WAL, a staged object, a checkpoint or a restored
// image. Node a adds 40 KiB and inline rows until its cache hits the limit;
// node r, limited to half of it, then takes over on an empty cache (a replay
// past the limit) and adds once. Unpublished work must fail, never be
// acknowledged; no node may crash; a's strong read never shows a failed add;
// and a fresh unlimited node recovers from S3 alone every acknowledged add at
// the position it answered, resolves every outcome_unknown add retried with
// its ID, and holds no definitely failed add.
func TestDiskFull(t *testing.T) {
	base := storetest.Local(t)
	runs := killRuns()
	var tot [4]int // acked, failed, unknown, replays refused
	for i := range runs {
		ep := base.Sub(fmt.Sprintf("diskfull-%d", i))
		r := diskFullRun(t, ep, 160<<10+rand.N(uint64(256<<10)))
		for j := range tot {
			tot[j] += r[j]
		}
		clean(t, ep)
		if t.Failed() {
			return
		}
	}
	t.Logf("%d runs: %d adds acknowledged, %d failed definitely, %d outcome_unknown then resolved; replay past the limit refused in %d runs",
		runs, tot[0], tot[1], tot[2], tot[3])
	if runs > 0 && tot[1]+tot[2] == 0 {
		t.Fatal("the file size limit never failed a write")
	}
}

func diskFullRun(t *testing.T, ep storetest.Endpoint, limit uint64) (tot [4]int) {
	led := newLedger()
	issued := map[string]bool{}
	var unknowns []Req
	add := func(e *mnode, r Req) bool {
		issued[r.ID] = true
		resp, ok := e.do(r, 30*time.Second)
		switch {
		case !ok:
			t.Fatalf("node %s died on a full disk: %+v", e.name, r)
		case resp.OK:
			led.ack(t, r.ID, resp)
			tot[0]++
			return true
		case resp.Code == "outcome_unknown":
			unknowns = append(unknowns, r)
			tot[2]++
		default:
			led.absent[r.ID] = true
			tot[1]++
		}
		return false
	}
	a := startNode(t, ep, "a", "-fsize", strconv.FormatUint(limit, 10))
	defer a.kill()
	cr, ok := a.do(Req{Op: "create", Key: "k", ID: newID()}, 30*time.Second)
	if !ok || !cr.OK {
		t.Fatalf("create under a %d-byte limit: %+v", limit, cr)
	}
	inc := cr.Inc
	for j, fails := 0, 0; j < 80 && fails < 3; j++ {
		if add(a, Req{Op: "add", Key: "k", Inc: inc, ID: newID(), Input: strconv.Itoa(j), Big: j%3 != 2}) {
			fails = 0
		} else {
			fails++
		}
	}
	if rd, ok := a.do(Req{Op: "read", Key: "k", Inc: inc}, 30*time.Second); !ok {
		t.Fatal("node a died reading on a full disk")
	} else if rd.OK {
		for _, id := range rd.IDs {
			if led.absent[id] {
				t.Errorf("strong read on a full disk shows the failed add %s", id)
			}
		}
		for id := range led.committed {
			if !slices.Contains(rd.IDs, id) {
				t.Errorf("strong read on a full disk misses the acknowledged add %s", id)
			}
		}
	}
	r := startNode(t, ep, "r", "-fsize", strconv.FormatUint(limit/2, 10))
	defer r.kill()
	if tk, ok := r.do(Req{Op: "takeover", Key: "k"}, 30*time.Second); !ok {
		t.Fatal("node r died restoring past its limit")
	} else if !tk.OK {
		tot[3] = 1
	}
	add(r, Req{Op: "add", Key: "k", Inc: inc, ID: newID(), Input: "r"})

	u := startNode(t, ep, "u") // unlimited, empty cache: stable storage again
	defer u.kill()
	if tk, ok := u.do(Req{Op: "takeover", Key: "k"}, 30*time.Second); !ok || !tk.OK {
		t.Fatalf("takeover by an unlimited node: %+v", tk)
	}
	for _, q := range unknowns { // §18.1: resolvable once local storage works
		resp, ok := u.do(q, 30*time.Second)
		if !ok || !resp.OK {
			t.Fatalf("retry of unknown add %s: %+v", q.ID, resp)
		}
		led.ack(t, q.ID, resp)
	}
	led.check(t, ep, "k", inc, issued)
	return tot
}
