package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"slices"
	"strconv"
	"strings"
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
	base := storetest.Local(t)
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
	pending := "" // an add a never answered before stopping: either outcome is allowed
	for i := 0; ; i++ {
		if i == 30 {
			t.Fatalf("%s: maintenance never reached the failpoint", fp)
		}
		r := Req{Op: "add", Key: "k", Inc: inc, ID: newID(), Input: strconv.Itoa(i), Big: true}
		if resp, answered := callUntilStopped(t, a, r); !answered {
			pending = r.ID // maintenance runs beside the add: the stop froze a before it replied
			break
		} else if !resp.OK {
			t.Fatalf("%s: %+v: %+v", fp, r, resp)
		}
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
	if pending != "" {
		select {
		case line, ok := <-a.lines:
			var resp Resp
			if ok && json.Unmarshal(line, &resp) == nil && resp.OK {
				committed[pending] = true
			}
		case <-time.After(60 * time.Second):
			t.Fatalf("%s: a never answered its pending add after SIGCONT", fp)
		}
	}
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
	for _, id := range ids {
		if !committed[id] && id != pending {
			t.Fatalf("%s: ID %s read but never sent or acknowledged", fp, id)
		}
	}
}

// callUntilStopped sends r to p and returns its reply, or answered=false once
// p is SIGSTOPped without having replied.
func callUntilStopped(t *testing.T, p *proc, r Req) (resp Resp, answered bool) {
	b, _ := json.Marshal(r)
	if _, err := p.in.Write(append(b, '\n')); err != nil {
		t.Fatalf("node died on %+v", r)
	}
	deadline := time.After(60 * time.Second)
	for {
		select {
		case line, ok := <-p.lines:
			if !ok {
				t.Fatalf("node died on %+v", r)
			}
			if err := json.Unmarshal(line, &resp); err != nil {
				t.Fatal(err)
			}
			return resp, true
		case <-deadline:
			t.Fatalf("node hung on %+v", r)
		case <-time.After(5 * time.Millisecond):
			if p.stopped() {
				return resp, false
			}
		}
	}
}

// TestWarmKill (D21): a node is SIGKILLed while parking its file at Close,
// after claiming it at activation, or while catching it up; the next process
// on the same cache must read exactly the committed rows. A run without a
// kill must have activated warm; "control" runs never kill.
func TestWarmKill(t *testing.T) {
	base := storetest.Local(t)
	runs := 20
	if s := os.Getenv("GARDEN_KILL_RUNS"); s != "" {
		runs, _ = strconv.Atoi(s)
	}
	for _, fp := range []string{"control", "warm.park", "warm.claim", "warm.apply.mid"} {
		t.Run(fp, func(t *testing.T) {
			killed := 0
			for i := range runs {
				ep := base.Sub(fmt.Sprintf("warm-%s-%d", fp, i))
				if warmKillRun(t, ep, fp) {
					killed++
				}
				clean(t, ep)
				if t.Failed() {
					return
				}
			}
			if want := map[bool]int{true: 0, false: runs}[fp == "control"]; killed != want {
				t.Fatalf("%s: killed in %d of %d runs", fp, killed, runs)
			}
			t.Logf("%s: %d runs, %d killed, every row recovered", fp, runs, killed)
		})
	}
}

func warmKillRun(t *testing.T, ep storetest.Endpoint, fp string) (killed bool) {
	cache := t.TempDir()
	must := func(p *proc, r Req) Resp {
		resp, alive := p.call(t, r)
		if !alive || !resp.OK {
			t.Fatalf("%s: %+v: %+v", fp, r, resp)
		}
		return resp
	}
	var ids []string
	add := func(p *proc, inc string, n int) {
		for i := range n {
			r := Req{Op: "add", Key: "k", Inc: inc, ID: newID(), Input: strconv.Itoa(len(ids)), Big: i%4 == 0}
			must(p, r)
			ids = append(ids, r.ID)
		}
	}
	parkFP := map[bool]string{true: fp + ":1", false: ""}[fp == "warm.park"]
	a := start(t, ep, cache, parkFP, "-node", "a")
	inc := must(a, Req{Op: "create", Key: "k", ID: newID()}).Inc
	add(a, inc, 12)
	killed = a.stop() // Close parks the file
	b := start(t, ep, t.TempDir(), "", "-node", "b")
	must(b, Req{Op: "takeover", Key: "k"})
	add(b, inc, 6) // a's file is now behind: activation catches up
	b.stop()
	activateFP := map[bool]string{true: "", false: fp + ":1"}[fp == "warm.park"]
	a2 := start(t, ep, cache, activateFP, "-node", "a")
	resp, alive := a2.call(t, Req{Op: "takeover", Key: "k"})
	if alive && resp.OK {
		resp, alive = a2.call(t, Req{Op: "read", Key: "k", Inc: inc})
	}
	if k := a2.stop(); k || !alive {
		killed = true
		a2 = start(t, ep, cache, "", "-node", "a")
		must(a2, Req{Op: "takeover", Key: "k"})
		resp = must(a2, Req{Op: "read", Key: "k", Inc: inc})
		defer a2.stop()
	} else if !resp.OK {
		t.Fatalf("%s: read: %+v", fp, resp)
	} else if resp.Warm == 0 && !killed {
		t.Fatalf("%s: a run without a kill activated cold", fp)
	}
	if !slices.Equal(resp.IDs, ids) {
		t.Fatalf("%s: read %d rows %v, committed %d %v", fp, len(resp.IDs), resp.IDs, len(ids), ids)
	}
	return killed
}

// TestDeleteKill (D25): node b takes over and deletes the entity and is
// SIGKILLed right after the tombstone CAS (cas.after) or inside the Collect
// that follows (before its second object deletion, collect.del). The
// tombstone is committed either way; the retry, from b restarted on the same
// cache, collects the rest and returns nil, as does a second retry; then
// only birth.json and head.json remain, every read is not_found, and a
// Create recreates an empty incarnation.
func TestDeleteKill(t *testing.T) {
	base := storetest.Local(t)
	runs := 10
	if s := os.Getenv("GARDEN_KILL_RUNS"); s != "" {
		runs, _ = strconv.Atoi(s)
	}
	for _, fp := range []string{"cas.after:2", "collect.del:2"} {
		t.Run(fp, func(t *testing.T) {
			for i := range runs {
				ep := base.Sub(fmt.Sprintf("delete-%s-%d", fp[:strings.IndexByte(fp, ':')], i))
				deleteKillRun(t, ep, fp)
				clean(t, ep)
				if t.Failed() {
					return
				}
			}
			t.Logf("%s: %d runs, all killed inside the delete", fp, runs)
		})
	}
}

func deleteKillRun(t *testing.T, ep storetest.Endpoint, fp string) {
	must := func(p *proc, r Req) Resp {
		t.Helper()
		resp, alive := p.call(t, r)
		if !alive || !resp.OK {
			t.Fatalf("%s: %+v: %+v", fp, r, resp)
		}
		return resp
	}
	a := start(t, ep, t.TempDir(), "", "-node", "a")
	inc := must(a, Req{Op: "create", Key: "k", ID: newID()}).Inc
	for i := range 6 { // four objects, then an inline tail
		must(a, Req{Op: "add", Key: "k", Inc: inc, ID: newID(), Input: strconv.Itoa(i), Big: i < 4})
	}
	a.stop()
	st := store.NewS3(ep.Client(nil), ep.Bucket, ep.Prefix, 5*time.Second)
	keys := func() (n int, tomb bool) {
		page, _, err := st.List(context.Background(), head.EntityDir("k")+"/", "", 1000)
		if err != nil {
			t.Fatal(err)
		}
		o, err := st.Get(context.Background(), head.HeadKey("k"), "", 1<<20)
		if err != nil {
			t.Fatal(err)
		}
		h, err := head.Decode(o.Body)
		if err != nil {
			t.Fatal(err)
		}
		return len(page), h.Deleted
	}
	before, _ := keys()
	cache := t.TempDir()
	b := start(t, ep, cache, fp, "-node", "b") // its takeover is the first CAS, the tombstone the second
	must(b, Req{Op: "takeover", Key: "k"})
	if resp, alive := b.call(t, Req{Op: "delete", Key: "k", Inc: inc}); alive {
		t.Fatalf("%s: delete answered %+v", fp, resp)
	}
	if !b.stop() {
		t.Fatalf("%s: b was not killed", fp)
	}
	n, tomb := keys()
	if !tomb || n >= before && fp != "cas.after:2" || n != before && fp == "cas.after:2" {
		t.Fatalf("%s: after the kill: tombstone %v, %d keys (%d before)", fp, tomb, n, before)
	}
	b = start(t, ep, cache, "", "-node", "b")
	defer b.stop()
	must(b, Req{Op: "delete", Key: "k", Inc: inc})
	must(b, Req{Op: "delete", Key: "k", Inc: inc})
	if n, tomb := keys(); n != 2 || !tomb {
		t.Fatalf("%s: after the retry: tombstone %v, %d keys", fp, tomb, n)
	}
	if resp, _ := b.call(t, Req{Op: "read", Key: "k", Inc: inc}); resp.Code != "not_found" {
		t.Fatalf("%s: read after delete: %+v", fp, resp)
	}
	inc2 := must(b, Req{Op: "create", Key: "k", ID: newID()}).Inc
	if ids := must(b, Req{Op: "read", Key: "k", Inc: inc2}).IDs; inc2 == inc || len(ids) != 0 {
		t.Fatalf("%s: recreated incarnation %s reads %v", fp, inc2, ids)
	}
}
