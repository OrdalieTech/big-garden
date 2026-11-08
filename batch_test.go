package garden

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/OrdalieTech/big-garden/internal/store"
	"github.com/OrdalieTech/big-garden/internal/storetest"
)

// Batch tests (PROTOCOL §6 Batches, PLAN §6.5 and M7): a leader blocked in
// its callback lets later Updates queue in a known FIFO order; releasing it
// makes it take them all into one publication.

type outcome struct {
	res      Result
	err      error
	panicked any
}

// enqueue starts an Update and returns once it waits on the entity.
func enqueue(t *testing.T, n *Node, ctx context.Context, ref Ref, req Request, fn func(*Tx) ([]byte, error)) chan outcome {
	t.Helper()
	before := n.Stats().QueuedRequests
	out := make(chan outcome, 1)
	go func() {
		var o outcome
		defer func() { o.panicked = recover(); out <- o }()
		o.res, o.err = n.Update(ctx, ref, req, fn)
	}()
	for deadline := time.Now().Add(5 * time.Second); n.Stats().QueuedRequests != before+1; {
		if time.Now().After(deadline) {
			t.Fatal("update never queued")
		}
		time.Sleep(time.Millisecond)
	}
	return out
}

// hold starts an Update of row l that blocks inside its callback until release.
func hold(t *testing.T, n *Node, ref Ref, req Request, release chan struct{}) chan outcome {
	t.Helper()
	inside, out := make(chan struct{}), make(chan outcome, 1)
	go func() {
		var o outcome
		o.res, o.err = n.Update(bg, ref, req, func(tx *Tx) ([]byte, error) {
			close(inside)
			<-release
			return put("l", "0")(tx)
		})
		out <- o
	}()
	<-inside
	return out
}

func getV(tx *Tx, k string) string {
	var v string
	tx.Query(bg, "SELECT v FROM kv WHERE k = ?", []any{k}, func(r *sql.Rows) error {
		for r.Next() {
			r.Scan(&v)
		}
		return r.Err()
	})
	return v
}

func batchNode(t *testing.T, st store.Store, mod func(*Limits)) *Node {
	return testNode(t, st, "a", func(l *Limits) {
		l.BatchMax = 16
		l.ResultBytes = 1 << 10
		if mod != nil {
			mod(l)
		}
	})
}

// TestBatchMembers: result ordering, reads and errors following speculative
// earlier members, partial callback failures (error, capacity, panic,
// admission verdicts), one publication, and each member's ID replayed after
// a restart.
func TestBatchMembers(t *testing.T) {
	mem := newStore(t)
	n := batchNode(t, mem, nil)
	ref := mustCreate(t, n, "k", nil)
	h0, _ := headAt(t, mem, "k")
	release := make(chan struct{})
	lreq := newReq(t, "leader", 1)
	leader := hold(t, n, ref, lreq, release)
	boom := errors.New("boom")
	r1, r2, r8 := newReq(t, "f1", 1), newReq(t, "f2", 1), newReq(t, "f8", 1)
	r3 := newReq(t, "f3", 1)
	conflict := Request{ID: r1.ID, Hash: newReq(t, "other", 1).Hash, Schema: 1}
	outs := []chan outcome{
		enqueue(t, n, bg, ref, r1, put("a", "1")),
		enqueue(t, n, bg, ref, r2, func(tx *Tx) ([]byte, error) { return []byte("saw " + getV(tx, "a")), nil }),
		enqueue(t, n, bg, ref, r3, func(tx *Tx) ([]byte, error) {
			if getV(tx, "a") != "1" {
				return nil, errors.New("did not see the earlier member")
			}
			put("c", "3")(tx)
			return nil, boom
		}),
		enqueue(t, n, bg, ref, r1, put("a", "1")),                // replay of an unpublished member
		enqueue(t, n, bg, ref, conflict, put("a", "x")),          // id_conflict with an unpublished member
		enqueue(t, n, bg, ref, newReq(t, "f6", 1), func(*Tx) ([]byte, error) { return make([]byte, 2<<10), nil }),
		enqueue(t, n, bg, ref, newReq(t, "f7", 2), put("z", "z")), // schema_mismatch
		enqueue(t, n, bg, ref, newReq(t, "f9", 1), func(tx *Tx) ([]byte, error) { put("p", "p")(tx); panic("member panic") }),
		enqueue(t, n, bg, ref, r8, put("b", "2")),
	}
	close(release)
	lo := <-leader
	var got []outcome
	for _, o := range outs {
		got = append(got, <-o)
	}
	if lo.err != nil || lo.res.Version.Sequence != 2 || string(lo.res.Value) != "put l" {
		t.Fatalf("leader %+v", lo)
	}
	want := func(i int, seq uint64, val string, replayed bool) {
		t.Helper()
		o := got[i]
		if o.err != nil || o.panicked != nil || o.res.Version.Sequence != seq || string(o.res.Value) != val || o.res.Replayed != replayed {
			t.Fatalf("member %d: %+v, want seq %d %q replayed=%v", i, o, seq, val, replayed)
		}
	}
	want(0, 3, "put a", false)
	want(1, 4, "saw 1", false)
	if !errors.Is(got[2].err, boom) {
		t.Fatalf("member 2: %+v, want its own error", got[2])
	}
	want(3, 3, "put a", true)
	wantCode(t, got[4].err, CodeIDConflict)
	wantCode(t, got[5].err, CodeCapacity)
	wantCode(t, got[6].err, CodeSchemaMismatch)
	if got[7].panicked != "member panic" {
		t.Fatalf("member 7: %+v, want its panic in its own goroutine", got[7])
	}
	want(8, 5, "put b", false)
	h, _ := headAt(t, mem, "k")
	if h.Sequence != 5 || h.Revision != h0.Revision+1 {
		t.Fatalf("head seq %d rev %d, want one publication of 2..5 after rev %d", h.Sequence, h.Revision, h0.Revision)
	}
	if b := n.Stats().Batches; b[3] != 1 {
		t.Fatalf("batch distribution %v, want one batch of 4", b)
	}
	if err := n.Close(bg); err != nil {
		t.Fatal(err)
	}
	n2 := batchNode(t, mem, nil) // new session of the same node: D4 takeover
	for i, c := range []struct {
		req Request
		seq uint64
		val string
	}{{lreq, 2, "put l"}, {r1, 3, "put a"}, {r2, 4, "saw 1"}, {r8, 5, "put b"}} {
		res, err := n2.Lookup(bg, ref, c.req.ID)
		if err != nil || res.Version.Sequence != c.seq || string(res.Value) != c.val {
			t.Fatalf("lookup %d after restart: %+v %v", i, res, err)
		}
		res, err = n2.Update(bg, ref, c.req, put("never", "applied"))
		if err != nil || !res.Replayed || res.Version.Sequence != c.seq {
			t.Fatalf("retry %d after restart: %+v %v", i, res, err)
		}
	}
	_, err := n2.Lookup(bg, ref, r3.ID)
	wantCode(t, err, CodeAbsentAtVersion)
	rows := readAll(t, n2, ref)
	if fmt.Sprint(rows) != "map[a:1 b:2 l:0]" {
		t.Fatalf("rows %v", rows)
	}
}

// TestBatchUnresolved: a batch whose head CAS has an unknown outcome gives
// every member outcome_unknown (held errors too), a retry of any member's ID
// stays outcome_unknown while unresolved, and resolution then settles every
// member exactly once. A definitely failed CAS gives every member
// unavailable and commits nothing.
func TestBatchUnresolved(t *testing.T) {
	for _, mode := range []string{"unknown", "failed"} {
		t.Run(mode, func(t *testing.T) {
			mem := newStore(t)
			n := batchNode(t, mem, func(l *Limits) { l.CommitTimeout = 300 * time.Millisecond })
			ref := mustCreate(t, n, "k", nil)
			mustPut(t, n, ref, "seed", "s", newLedger()) // the next ones may inline
			var armed atomic.Bool
			reject := store.Unknown
			if mode == "failed" {
				reject = store.Failed
			}
			mem.SetPlan(func(a storetest.Attempt) storetest.MemFault {
				if armed.Load() && a.Op == "cas" {
					return storetest.MemFault{Reject: reject}
				}
				return storetest.MemFault{}
			})
			release := make(chan struct{})
			lreq, r1, r2 := newReq(t, "leader", 1), newReq(t, "f1", 1), newReq(t, "f2", 1)
			leader := hold(t, n, ref, lreq, release)
			o1 := enqueue(t, n, bg, ref, r1, put("a", "1"))
			o2 := enqueue(t, n, bg, ref, r2, func(tx *Tx) ([]byte, error) { return nil, errors.New("saw " + getV(tx, "a")) })
			armed.Store(true)
			close(release)
			code := CodeOutcomeUnknown
			if mode == "failed" {
				code = CodeUnavailable
			}
			for i, o := range []outcome{<-leader, <-o1, <-o2} {
				e := wantCode(t, o.err, code)
				if id := []Request{lreq, r1, r2}[i].ID; mode == "unknown" && e.Operation != id {
					t.Fatalf("member %d: operation %x, want %x", i, e.Operation, id)
				}
			}
			if mode == "unknown" {
				_, err := n.Update(bg, ref, r1, put("a", "1")) // still unresolved: the member's own ID
				wantCode(t, err, CodeOutcomeUnknown)
			}
			armed.Store(false)
			res, err := n.Update(bg, ref, r1, put("a", "1"))
			if mode == "failed" { // nothing committed: the retry is a new mutation
				if err != nil || res.Replayed {
					t.Fatalf("retry after a failed batch: %+v %v", res, err)
				}
				_, err = n.Lookup(bg, ref, lreq.ID)
				wantCode(t, err, CodeAbsentAtVersion)
				return
			}
			if err != nil || !res.Replayed || res.Version.Sequence != 4 {
				t.Fatalf("retry of member 1: %+v %v", res, err)
			}
			res, err = n.Update(bg, ref, lreq, put("never", "applied"))
			if err != nil || !res.Replayed || res.Version.Sequence != 3 {
				t.Fatalf("retry of the leader: %+v %v", res, err)
			}
			_, err = n.Lookup(bg, ref, r2.ID)
			wantCode(t, err, CodeAbsentAtVersion)
		})
	}
}

// TestBatchCancellation: a member canceled while queued (not yet taken)
// leaves the batch with outcome_unknown and never commits; a member canceled
// while the batch runs its callback gets outcome_unknown at once and its
// commit resolves by Lookup; the leader's cancellation mid-batch does not
// abort the others' publication.
func TestBatchCancellation(t *testing.T) {
	mem := newStore(t)
	n := batchNode(t, mem, nil)
	ref := mustCreate(t, n, "k", nil)
	mustPut(t, n, ref, "seed", "s", newLedger())
	lctx, lcancel := context.WithCancel(bg)
	release, inside2, release2 := make(chan struct{}), make(chan struct{}), make(chan struct{})
	lreq, r1, r2, r3 := newReq(t, "leader", 1), newReq(t, "f1", 1), newReq(t, "f2", 1), newReq(t, "f3", 1)
	leaderOut := make(chan outcome, 1)
	linside := make(chan struct{})
	go func() {
		var o outcome
		o.res, o.err = n.Update(lctx, ref, lreq, func(tx *Tx) ([]byte, error) { close(linside); <-release; return put("l", "0")(tx) })
		leaderOut <- o
	}()
	<-linside
	ctx1, cancel1 := context.WithCancel(bg)
	o1 := enqueue(t, n, ctx1, ref, r1, put("a", "1"))
	ctx2, cancel2 := context.WithCancel(bg)
	o2 := enqueue(t, n, ctx2, ref, r2, func(tx *Tx) ([]byte, error) { close(inside2); <-release2; return put("b", "2")(tx) })
	o3 := enqueue(t, n, bg, ref, r3, put("c", "3"))
	cancel1() // queued, not taken
	wantCode(t, (<-o1).err, CodeOutcomeUnknown)
	close(release)
	<-inside2
	cancel2() // taken: its callback runs in the batch
	wantCode(t, (<-o2).err, CodeOutcomeUnknown)
	lcancel()
	close(release2)
	if o := <-leaderOut; o.err != nil || o.res.Version.Sequence != 3 {
		t.Fatalf("leader canceled mid-batch: %+v", o)
	}
	if o := <-o3; o.err != nil || o.res.Version.Sequence != 5 {
		t.Fatalf("member after the canceled one: %+v", o)
	}
	if res, err := n.Lookup(bg, ref, r2.ID); err != nil || res.Version.Sequence != 4 {
		t.Fatalf("canceled member committed in the batch: %+v %v", res, err)
	}
	_, err := n.Lookup(bg, ref, r1.ID)
	wantCode(t, err, CodeAbsentAtVersion)
	if rows := readAll(t, n, ref); fmt.Sprint(rows) != "map[b:2 c:3 l:0 seed:s]" {
		t.Fatalf("rows %v", rows)
	}
}

// TestBatchNeverPassesRead: a read queued among Updates stops the batch, and
// sees the leader's publication.
func TestBatchNeverPassesRead(t *testing.T) {
	mem := newStore(t)
	n := batchNode(t, mem, nil)
	ref := mustCreate(t, n, "k", nil)
	release := make(chan struct{})
	leader := hold(t, n, ref, newReq(t, "leader", 1), release)
	o1 := enqueue(t, n, bg, ref, newReq(t, "f1", 1), put("a", "1"))
	read := make(chan map[string]string, 1)
	before := n.Stats().QueuedRequests
	go func() { read <- readAll(t, n, ref) }()
	for n.Stats().QueuedRequests != before+1 {
		time.Sleep(time.Millisecond)
	}
	close(release)
	if o := <-leader; o.err != nil {
		t.Fatal(o.err)
	}
	if b := n.Stats().Batches; b[0] < 1 || b[1] != 0 {
		t.Fatalf("batch distribution %v: the leader passed a queued read", b)
	}
	if o := <-o1; o.err != nil {
		t.Fatal(o.err)
	}
	if rows := <-read; rows["l"] != "0" {
		t.Fatalf("read %v misses the leader", rows)
	}
}
