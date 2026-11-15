package garden

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/OrdalieTech/big-garden/internal/head"
	"github.com/OrdalieTech/big-garden/internal/store"
)

// offGate sets up a's off-mutex snapshot (D35) on key k, paused once its
// object is uploaded: it returns the paused object key and the release.
func offGate(t *testing.T, st store.Store, ps *pauseStore, a *Node, l *ledger) (Ref, string, chan struct{}) {
	t.Helper()
	a.offGate = 1 // every maintenance snapshot goes off the mutex
	ref := mustCreate(t, a, "k", l)
	fillMaintenance(t, a, ref, true, l)
	paused, release := pauseOnce(ps, "put", "after", func(key string) bool {
		h, _ := headAt(t, st, "k")
		return h.Pending != nil && strings.Contains(key, "/"+h.Pending.Attempt.String()+"/")
	})
	startMaintenance(a, "k")
	return ref, waitPaused(t, paused), release
}

func objectExists(st store.Store, key string) bool {
	_, err := st.Get(bg, key, "", head.MaxObjectBytes)
	return err == nil
}

// While an off-mutex snapshot is uploaded, writes go on without waiting for
// it, a collection keeps its object, and the snapshot then replaces the
// deltas it covers while keeping those written meanwhile.
func TestSnapshotOffMutex(t *testing.T) {
	st := newStore(t)
	ps := &pauseStore{Store: st}
	a := testNode(t, ps, "a", nil)
	l := newLedger()
	ref, obj, release := offGate(t, st, ps, a, l)
	h0, _ := headAt(t, st, "k")
	for i := range 5 { // the entity mutex is free: these do not wait for the snapshot
		t0 := time.Now()
		mustPut(t, a, ref, fmt.Sprint("during", i), "x", l)
		if d := time.Since(t0); d > 5*time.Second {
			t.Fatalf("a write waited %v behind the snapshot", d)
		}
	}
	b := testNode(t, st, "b", nil)
	if err := b.Collect(bg, ref); err != nil {
		t.Fatal(err)
	}
	if !objectExists(st, obj) {
		t.Fatal("a collection deleted the pending snapshot")
	}
	close(release)
	a.wg.Wait()
	h, _ := headAt(t, st, "k")
	if h.Pending != nil || h.Snapshot.Key != obj || h.Snapshot.MaxSequence != h0.Sequence { // h0: the head that announced it
		t.Fatalf("head after the snapshot: pending %v, snapshot %s, want %s", h.Pending, h.Snapshot.Key, obj)
	}
	for _, d := range h.Deltas {
		if d.MinSequence <= h.Snapshot.MaxSequence {
			t.Fatalf("delta %d..%d kept under a snapshot to %d", d.MinSequence, d.MaxSequence, h.Snapshot.MaxSequence)
		}
	}
	if h.Sequence != h0.Sequence+5 {
		t.Fatalf("head at sequence %d, want %d", h.Sequence, h0.Sequence+5)
	}
	l.verify(t, st, "c")
}

// A takeover during the upload ends the snapshot: the old owner drops it,
// the new owner's maintenance drops the intent, nothing acknowledged is lost
// and collection removes the object.
func TestSnapshotOffMutexTakeover(t *testing.T) {
	st := newStore(t)
	ps := &pauseStore{Store: st}
	a := testNode(t, ps, "a", nil)
	l := newLedger()
	ref, obj, release := offGate(t, st, ps, a, l)
	b := testNode(t, st, "b", nil)
	route, err := b.Resolve(bg, "k")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := b.Takeover(bg, route); err != nil {
		t.Fatal(err)
	}
	mustPut(t, b, ref, "after", "takeover", l) // b's maintenance drops the intent it does not run
	b.wg.Wait()
	close(release)
	a.wg.Wait()
	h, _ := headAt(t, st, "k")
	if h.Pending != nil || h.Snapshot.Key == obj {
		t.Fatalf("pending %v, snapshot %s after a takeover", h.Pending, h.Snapshot.Key)
	}
	if err := b.Collect(bg, ref); err != nil {
		t.Fatal(err)
	}
	if objectExists(st, obj) {
		t.Fatal("the abandoned snapshot survived collection")
	}
	l.verify(t, st, "c")
}

// An explicit Snapshot during the upload publishes its own and ends the
// pending one, whose object goes: the erasure of D22 still holds.
func TestSnapshotOffMutexExplicit(t *testing.T) {
	st := newStore(t)
	ps := &pauseStore{Store: st}
	a := testNode(t, ps, "a", nil)
	l := newLedger()
	ref, obj, release := offGate(t, st, ps, a, l)
	if _, err := a.Snapshot(bg, ref); err != nil {
		t.Fatal(err)
	}
	close(release)
	a.wg.Wait()
	h, _ := headAt(t, st, "k")
	if h.Pending != nil || h.Snapshot.Key == obj || len(h.Deltas) != 0 {
		t.Fatalf("pending %v, snapshot %s, %d deltas", h.Pending, h.Snapshot.Key, len(h.Deltas))
	}
	if objectExists(st, obj) {
		t.Fatal("the abandoned snapshot outlived the explicit one")
	}
	l.verify(t, st, "c")
}
