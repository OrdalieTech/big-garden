package garden

import (
	"bytes"
	"database/sql"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/OrdalieTech/big-garden/internal/head"
	"github.com/OrdalieTech/big-garden/internal/storetest"
)

// headGets counts the GETs of key's head in st's journal from entry from on.
func headGets(st faultStore, key string, from int) (c int) {
	for _, a := range st.Journal()[from:] {
		if a.Op == "get" && a.Key == head.HeadKey(key) {
			c++
		}
	}
	return c
}

// rows reads every row of ref's kv table.
func rows(n *Node, ref Ref) (map[string]string, error) {
	got := map[string]string{}
	_, err := n.Read(bg, ref, 1, func(v *View) ([]byte, error) {
		return nil, v.Query(bg, "SELECT k, v FROM kv", nil, func(r *sql.Rows) error {
			for r.Next() {
				var k, v string
				if err := r.Scan(&k, &v); err != nil {
					return err
				}
				got[k] = v
			}
			return r.Err()
		})
	})
	return got, err
}

// queueReads starts k Reads of ref and returns once all of them wait on the
// entity; each sends its rows, or nil on error.
func queueReads(t *testing.T, n *Node, ref Ref, k int) chan map[string]string {
	t.Helper()
	out, before := make(chan map[string]string, k), n.Stats().QueuedRequests
	for range k {
		go func() {
			got, err := rows(n, ref)
			if err != nil {
				t.Errorf("read: %v", err)
				got = nil
			}
			out <- got
		}()
	}
	for deadline := time.Now().Add(5 * time.Second); n.Stats().QueuedRequests != before+int64(k); time.Sleep(time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatal("reads never queued")
		}
	}
	return out
}

// TestSharedValidation (D23): Reads queued behind a Read whose head GET is in
// flight need one more GET between them, issued after they all began; Reads
// queued behind a publication need none, its CAS proved the head.
func TestSharedValidation(t *testing.T) {
	st := newStore(t)
	n := testNode(t, st, "a", func(l *Limits) { l.StorageAttemptTimeout, l.CommitTimeout = 2*time.Second, 4*time.Second })
	l := newLedger()
	ref := mustCreate(t, n, "k", l)
	mustPut(t, n, ref, "a", "1", l)

	gate, held := make(chan struct{}), atomic.Bool{}
	st.SetPlan(func(a storetest.Attempt) storetest.MemFault {
		if a.Op == "get" && a.Key == head.HeadKey("k") && held.CompareAndSwap(false, true) {
			return storetest.MemFault{Hold: gate}
		}
		return storetest.MemFault{}
	})
	from := len(st.Journal())
	first := make(chan map[string]string, 1)
	go func() { got, _ := rows(n, ref); first <- got }()
	for headGets(st, "k", from) == 0 {
		time.Sleep(time.Millisecond)
	}
	group := queueReads(t, n, ref, 15)
	close(gate)
	for i := range 16 {
		c := group
		if i == 0 {
			c = first
		}
		if got := <-c; got["a"] != "1" {
			t.Fatalf("read %v, want a=1", got)
		}
	}
	if g := headGets(st, "k", from); g != 2 {
		t.Fatalf("16 reads in two groups made %d head GETs, want 2", g)
	}
	st.SetPlan(nil)

	release := make(chan struct{})
	leader := hold(t, n, ref, newReq(t, "leader", 1), release)
	from = len(st.Journal())
	group = queueReads(t, n, ref, 16)
	close(release)
	if o := <-leader; o.err != nil {
		t.Fatal(o.err)
	}
	for range 16 {
		if got := <-group; got["l"] != "0" {
			t.Fatalf("read %v misses the publication it waited behind", got)
		}
	}
	if g := headGets(st, "k", from); g != 0 {
		t.Fatalf("16 reads behind a publication made %d head GETs, want 0", g)
	}
}

// counter returns the value of row n.
func counter(n *Node, ref Ref) (int64, Result, error) {
	var v int64
	res, err := n.Read(bg, ref, 1, func(vw *View) ([]byte, error) {
		return nil, vw.Query(bg, "SELECT CAST(v AS INTEGER) FROM kv WHERE k='n'", nil, func(r *sql.Rows) error {
			for r.Next() {
				if err := r.Scan(&v); err != nil {
					return err
				}
			}
			return r.Err()
		})
	})
	return v, res, err
}

func increment(tx *Tx) ([]byte, error) {
	_, err := tx.Exec(bg, "UPDATE kv SET v = CAST(v AS INTEGER) + 1 WHERE k='n'")
	return nil, err
}

// TestReadRidesCAS (D23): 8 writers and 8 readers on one entity whose head
// CAS takes 2 ms. Queued reads no longer stop a batch, so batches of several
// members form; every read is linearizable (acknowledged before it began <=
// value <= started before it ended) and sequences never go back.
func TestReadRidesCAS(t *testing.T) {
	st := newStore(t)
	n := batchNode(t, st, nil)
	l := newLedger()
	ref := mustCreate(t, n, "k", l)
	mustPut(t, n, ref, "n", "0", l)
	st.SetPlan(func(a storetest.Attempt) storetest.MemFault {
		if a.Op != "cas" {
			return storetest.MemFault{}
		}
		h := make(chan struct{})
		time.AfterFunc(2*time.Millisecond, func() { close(h) })
		return storetest.MemFault{Hold: h}
	})
	from := len(st.Journal())
	var started, acked, reads atomic.Int64
	var wg, rg sync.WaitGroup
	stop := make(chan struct{})
	for w := range 8 {
		wg.Go(func() {
			for i := range 40 {
				started.Add(1)
				if _, err := n.Update(bg, ref, newReq(t, fmt.Sprint(w, "-", i), 1), increment); err != nil {
					t.Errorf("update: %v", err)
					return
				}
				acked.Add(1)
			}
		})
	}
	for range 8 {
		rg.Go(func() {
			last := uint64(0)
			for {
				select {
				case <-stop:
					return
				default:
				}
				lo := acked.Load()
				v, res, err := counter(n, ref)
				hi := started.Load()
				switch {
				case err != nil:
					t.Errorf("read: %v", err)
					return
				case v < lo || v > hi:
					t.Errorf("read not linearizable: %d outside [%d, %d]", v, lo, hi)
					return
				case res.Version.Sequence < last:
					t.Errorf("read went back: sequence %d after %d", res.Version.Sequence, last)
					return
				}
				last = res.Version.Sequence
				reads.Add(1)
			}
		})
	}
	wg.Wait()
	close(stop)
	rg.Wait()
	b := n.Stats().Batches
	multi := uint64(0)
	for _, k := range b[1:] {
		multi += k
	}
	t.Logf("%d reads, %d head GETs; publications by member count %v", reads.Load(), headGets(st, "k", from), b)
	if v, _, err := counter(n, ref); err != nil || v != 320 {
		t.Fatalf("counter %d (%v), want 320", v, err)
	}
	if n.cfg.Limits.BatchMax > 1 && multi == 0 {
		t.Fatalf("no batch of several members formed beside the readers: %v", b)
	}
}

// TestFollowerRead (D24): a reader on a node that never owns the entity,
// while the owner writes and maintenance compacts, snapshots and collects
// under it. A counter read is linearizable iff acknowledged before it began
// <= value <= started before it ended, and sequences never go back. The
// follower never changes the owner and catches up from its own file.
func TestFollowerRead(t *testing.T) {
	st := newStore(t)
	a, b := testNode(t, st, "a", nil), testNode(t, st, "b", nil)
	l := newLedger()
	ref := mustCreate(t, a, "k", l)
	mustPut(t, a, ref, "n", "0", l)
	writes := 400
	if storetest.LocalOn() {
		writes = 100
	}
	var started, acked atomic.Int64
	done := make(chan struct{})
	var wg sync.WaitGroup
	for w := range 4 {
		wg.Go(func() {
			for i := range writes {
				started.Add(1)
				req := newReq(t, fmt.Sprint(w, "-", i), 1)
				var err error
				for deadline := time.Now().Add(time.Minute); ; { // an overloaded machine may time a batch out: retry its ID, as a client does
					_, err = a.Update(bg, ref, req, func(tx *Tx) ([]byte, error) {
						_, err := tx.Exec(bg, "UPDATE kv SET v = CAST(v AS INTEGER) + 1 WHERE k='n'")
						if err == nil { // grow and churn pages so compaction and snapshots run
							_, err = tx.Exec(bg, "INSERT OR REPLACE INTO kv VALUES(?, hex(randomblob(600)))", fmt.Sprint("f", i%40))
						}
						return nil, err
					})
					var e *Error
					if err == nil || !errors.As(err, &e) || e.Code != CodeOutcomeUnknown && e.Code != CodeUnavailable || time.Now().After(deadline) {
						break
					}
				}
				if err != nil {
					t.Errorf("update: %v", err)
					return
				}
				acked.Add(1)
			}
		})
	}
	go func() { wg.Wait(); close(done) }()
	reads, last := 0, uint64(0)
	for stop := false; !stop; reads++ {
		select {
		case <-done:
			stop = true // one last read after every write
		default:
		}
		lo := acked.Load()
		v, res, err := counter(b, ref)
		hi := started.Load()
		switch {
		case err != nil:
			t.Fatalf("follower read %d: %v", reads, err)
		case v < lo || v > hi:
			t.Fatalf("follower read %d not linearizable: %d outside [%d, %d]", reads, v, lo, hi)
		case res.Version.Sequence < last:
			t.Fatalf("follower read %d went back: sequence %d after %d", reads, res.Version.Sequence, last)
		}
		last = res.Version.Sequence
	}
	sa, sb := a.Stats(), b.Stats()
	t.Logf("%d follower reads; owner: %d commits, %d snapshots, %d compactions, %d objects collected; follower: %d activations, %d catch-ups, %d warm hits, %d owner changes",
		reads, sa.Commits, sa.Snapshots, sa.Compactions, sa.CollectedObjects, sb.Activations, sb.CatchUps, sb.WarmHits, sb.OwnerChanges)
	if v, _, err := counter(b, ref); err != nil || v != int64(4*writes) {
		t.Fatalf("final follower read %d (%v), want %d", v, err, 4*writes)
	}
	if h, _ := headAt(t, st, "k"); sb.OwnerChanges != 0 || *h.Owner != a.self {
		t.Fatalf("the follower changed the owner: %d owner changes, owner %+v", sb.OwnerChanges, h.Owner)
	}
	if sb.CatchUps == 0 || sb.Activations >= uint64(reads) {
		t.Fatalf("%d activations (%d catch-ups) for %d reads: the follower does not keep its file", sb.Activations, sb.CatchUps, reads)
	}
	req := newReq(t, "last", 1)
	if _, err := a.Update(bg, ref, req, increment); err != nil {
		t.Fatal(err)
	}
	if res, err := b.Lookup(bg, ref, req.ID); err != nil || !res.Replayed { // Lookup inherits the rule
		t.Fatalf("follower lookup %+v %v", res, err)
	}
	_, err := b.Update(bg, ref, newReq(t, "follower", 1), increment)
	wantCode(t, err, CodeNotOwner)
}

// TestExportFollower (D24): Export on a node that does not own the entity
// returns the owner's current state, as an archive Import accepts.
func TestExportFollower(t *testing.T) {
	st := newStore(t)
	a, b := testNode(t, st, "a", nil), testNode(t, st, "b", nil)
	l := newLedger()
	ref := mustCreate(t, a, "k", l)
	for i := range 5 {
		mustPut(t, a, ref, fmt.Sprint("k", i), randText(2000), l)
	}
	if _, err := rows(b, ref); err != nil { // the follower holds an image
		t.Fatal(err)
	}
	mustPut(t, a, ref, "last", "1", l) // and then falls behind
	var out bytes.Buffer
	v, err := b.Export(bg, ref, &out)
	h, _ := headAt(t, st, "k")
	if err != nil || v.Sequence != h.Sequence {
		t.Fatalf("follower export %+v %v, head at %d", v, err, h.Sequence)
	}
	ref2, _, err := b.Import(bg, "copy", newReq(t, "import", 1), &out)
	if err != nil {
		t.Fatal(err)
	}
	want, _ := rows(a, ref)
	if got, err := rows(b, ref2); err != nil || fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("imported %v (%v), want %v", got, err, want)
	}
	if h, _ := headAt(t, st, "k"); *h.Owner != a.self || b.Stats().OwnerChanges != 0 {
		t.Fatalf("the export changed the owner: %+v", h.Owner)
	}
}

// TestHundredReaders: 100 concurrent reads of one entity queue within the
// default PendingPerEntity (256), with no capacity rejection, and the
// publication they wait behind validates them all.
func TestHundredReaders(t *testing.T) {
	st := newStore(t)
	n := testNode(t, st, "a", func(l *Limits) { l.PendingPerEntity = 0 }) // the default
	ref := mustCreate(t, n, "k", newLedger())
	release := make(chan struct{})
	leader := hold(t, n, ref, newReq(t, "leader", 1), release)
	from := len(st.Journal())
	group := queueReads(t, n, ref, 100)
	close(release)
	if o := <-leader; o.err != nil {
		t.Fatal(o.err)
	}
	for range 100 {
		if got := <-group; got["l"] != "0" {
			t.Fatalf("read %v", got)
		}
	}
	if s := n.Stats(); s.ReadRejections != 0 || headGets(st, "k", from) != 0 {
		t.Fatalf("%d read rejections, %d head GETs", s.ReadRejections, headGets(st, "k", from))
	}
}
