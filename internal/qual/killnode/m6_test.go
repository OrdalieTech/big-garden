package main

import (
	"context"
	"fmt"
	"os"
	"slices"
	"strconv"
	"syscall"
	"testing"
	"time"

	"github.com/OrdalieTech/big-garden/internal/head"
	"github.com/OrdalieTech/big-garden/internal/store"
	"github.com/OrdalieTech/big-garden/internal/storetest"
)

// TestStaleMaintenance (M6) SIGSTOPs owner a inside its first compaction:
// before the object PUT (maint.put), between PUT and head CAS (maint.cas) or
// after the CAS (maint.done). b takes over and commits, a is continued: a's
// step publishes only if it committed before the takeover (maint.done), its
// object is never referenced by a later head, a cannot write any more, and
// the ledger is intact through a third node c.
func TestStaleMaintenance(t *testing.T) {
	base := storetest.MinIO(t)
	runs := 20
	if s := os.Getenv("GARDEN_KILL_RUNS"); s != "" {
		runs, _ = strconv.Atoi(s)
	}
	for _, fp := range []string{"maint.put", "maint.cas", "maint.done"} {
		t.Run(fp, func(t *testing.T) {
			for i := range runs {
				ep := base.Sub(fmt.Sprintf("maint-%s-%d", fp, i))
				staleMaintenanceRun(t, ep, fp)
				clean(t, ep)
				if t.Failed() {
					return
				}
			}
			t.Logf("%s: %d runs, all stopped inside maintenance", fp, runs)
		})
	}
}

func staleMaintenanceRun(t *testing.T, ep storetest.Endpoint, fp string) {
	a := start(t, ep, t.TempDir(), fp+":1:stop", "-node", "a")
	defer a.stop()
	b := start(t, ep, t.TempDir(), "", "-node", "b")
	defer b.stop()
	must := func(p *proc, r Req) Resp {
		resp, alive := p.call(t, r)
		if !alive || !resp.OK {
			t.Fatalf("%s: %+v: %+v", fp, r, resp)
		}
		return resp
	}
	st := store.NewS3(ep.Client(nil), ep.Bucket, ep.Prefix, 5*time.Second)
	headOf := func() head.Head {
		o, err := st.Get(context.Background(), head.HeadKey("k"), "", 1<<20)
		if err != nil {
			t.Fatal(err)
		}
		h, err := head.Decode(o.Body)
		if err != nil {
			t.Fatal(err)
		}
		return h
	}
	inc := must(a, Req{Op: "create", Key: "k", ID: newID()}).Inc
	committed := map[string]bool{}
	for i := 0; ; i++ {
		if i == 30 {
			t.Fatalf("%s: maintenance never reached the failpoint", fp)
		}
		r := Req{Op: "add", Key: "k", Inc: inc, ID: newID(), Input: strconv.Itoa(i), Big: true}
		must(a, r)
		committed[r.ID] = true
		deadline := time.Now().Add(300 * time.Millisecond)
		for !a.stopped() && time.Now().Before(deadline) {
			time.Sleep(2 * time.Millisecond)
		}
		if a.stopped() {
			break
		}
	}
	stale := headOf() // a is stopped inside maintenance at this head
	must(b, Req{Op: "takeover", Key: "k"})
	for i := range 3 {
		r := Req{Op: "add", Key: "k", Inc: inc, ID: newID(), Input: "b" + strconv.Itoa(i), Big: i == 1}
		must(b, r)
		committed[r.ID] = true
	}
	a.cmd.Process.Signal(syscall.SIGCONT)
	time.Sleep(1500 * time.Millisecond) // a's maintenance finishes (CommitTimeout 1 s)
	if resp, alive := a.call(t, Req{Op: "add", Key: "k", Inc: inc, ID: newID(), Input: "stale"}); !alive || resp.OK {
		t.Fatalf("%s: stale owner wrote after the takeover: %+v alive %v", fp, resp, alive)
	}
	for i := range 10 { // b keeps committing and maintaining
		r := Req{Op: "add", Key: "k", Inc: inc, ID: newID(), Input: "c" + strconv.Itoa(i), Big: true}
		must(b, r)
		committed[r.ID] = true
		h := headOf()
		if fp != "maint.done" { // a's object is the only one at base stale.Revision
			for _, ref := range append([]head.Ref{h.Snapshot}, h.Deltas...) {
				if base, _, _, err := head.ParseObjectKey(head.ObjectPrefix("k", h.Incarnation), ref.Key); err == nil && base == stale.Revision {
					t.Fatalf("%s: stale maintenance object %s referenced at revision %d", fp, ref.Key, h.Revision)
				}
			}
		}
		if h.Owner == nil || h.Owner.Node != "b" {
			t.Fatalf("%s: owner %+v", fp, h.Owner)
		}
	}
	c := start(t, ep, t.TempDir(), "", "-node", "c")
	defer c.stop()
	must(c, Req{Op: "takeover", Key: "k"}) // recovered from S3 alone
	ids := must(c, Req{Op: "read", Key: "k", Inc: inc}).IDs
	for id := range committed {
		if !slices.Contains(ids, id) {
			t.Fatalf("%s: acknowledged ID %s lost", fp, id)
		}
	}
	if len(ids) != len(committed) {
		t.Fatalf("%s: %d IDs committed, %d acknowledged", fp, len(ids), len(committed))
	}
}
