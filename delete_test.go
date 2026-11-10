package garden

import (
	"bytes"
	"database/sql"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/OrdalieTech/big-garden/internal/head"
	"github.com/OrdalieTech/big-garden/internal/store"
)

// entityKeys lists every stored key of key's entity directory.
func entityKeys(t *testing.T, st store.Store, key string) []string {
	t.Helper()
	page, _, err := st.List(bg, head.EntityDir(key)+"/", "", 1000)
	if err != nil {
		t.Fatal(err)
	}
	var keys []string
	for _, o := range page {
		keys = append(keys, strings.TrimPrefix(o.Key, head.EntityDir(key)+"/"))
	}
	return keys
}

// TestDelete (D25): Delete leaves a tombstone head and collects every object
// of the incarnation; every operation on it is then not_found, on the owner
// and on a follower, whose resident image and warm file are dropped; Delete
// again is idempotent; Create recreates the key as a new incarnation with
// nothing of the old one, birth.json untouched, revision and epoch carried
// on; the old Ref gets incarnation_mismatch; Import recreates a tombstone too.
func TestDelete(t *testing.T) {
	st := newStore(t)
	a := testNode(t, st, "a", nil)
	b := testNode(t, st, "b", nil)
	ref := mustCreate(t, a, "k", nil)
	l := newLedger()
	for i := range 12 { // uploaded deltas, then an inline tail
		mustPut(t, a, ref, fmt.Sprint("k", i), strings.Repeat("v", 3000), l)
	}
	mustPut(t, a, ref, "small", "s", l)
	a.wg.Wait()
	if h, _ := headAt(t, st, "k"); h.Tail == nil || len(h.Deltas) == 0 {
		t.Fatalf("setup: want deltas and a tail, have %d deltas, tail %v", len(h.Deltas), h.Tail != nil)
	}
	if got := readAll(t, b, ref); len(got) != 13 { // b follows: a resident image
		t.Fatalf("follower read %d rows", len(got))
	}
	dir := t.TempDir() // w parks a warm file of the incarnation
	w := warmNode(t, st, "w", dir, nil)
	readAll(t, w, ref)
	w.Close(bg)
	w = warmNode(t, st, "w", dir, nil)
	if s := w.Stats(); s.WarmEntities != 1 {
		t.Fatalf("setup: %d warm files", s.WarmEntities)
	}
	birth0, _ := st.Get(bg, head.BirthKey("k"), "", head.MaxBirthBytes)
	route0, err := a.Resolve(bg, "k")
	if err != nil {
		t.Fatal(err)
	}
	wantCode(t, b.Delete(bg, ref), CodeNotOwner)

	if err := a.Delete(bg, ref); err != nil {
		t.Fatal(err)
	}
	tomb, _ := headAt(t, st, "k")
	if !tomb.Deleted || tomb.Owner != nil || tomb.Tail != nil || tomb.Revision != route0.Revision+1 || tomb.Epoch != route0.Epoch+1 {
		t.Fatalf("tombstone %+v", tomb)
	}
	if keys := entityKeys(t, st, "k"); !slices.Equal(keys, []string{"birth.json", "head.json"}) {
		t.Fatalf("after Delete the entity holds %v", keys)
	}
	notFound := func(n *Node) {
		t.Helper()
		var buf bytes.Buffer
		_, err := n.Read(bg, ref, 1, func(*View) ([]byte, error) { return nil, nil })
		wantCode(t, err, CodeNotFound)
		_, err = n.Lookup(bg, ref, newReq(t, "x", 1).ID)
		wantCode(t, err, CodeNotFound)
		_, err = n.Update(bg, ref, newReq(t, "u", 1), put("x", "y"))
		wantCode(t, err, CodeNotFound)
		_, err = n.Migrate(bg, ref, newReq(t, "m", 1), 2, func(*Tx) error { return nil })
		wantCode(t, err, CodeNotFound)
		_, err = n.Export(bg, ref, &buf)
		wantCode(t, err, CodeNotFound)
		_, err = n.Snapshot(bg, ref)
		wantCode(t, err, CodeNotFound)
		_, err = n.Resolve(bg, "k")
		wantCode(t, err, CodeNotFound)
		_, err = n.Takeover(bg, route0)
		wantCode(t, err, CodeNotFound)
		wantCode(t, n.Release(bg, ref), CodeNotFound)
		if err := n.Delete(bg, ref); err != nil { // idempotent
			t.Fatalf("Delete again: %v", err)
		}
		if err := n.Collect(bg, ref); err != nil {
			t.Fatalf("Collect of a tombstone: %v", err)
		}
	}
	notFound(a)
	notFound(b)
	if s := b.Stats(); s.ResidentEntities != 0 {
		t.Fatalf("the follower kept %d resident images", s.ResidentEntities)
	}
	notFound(w)
	if s := w.Stats(); s.WarmEntities != 0 || s.ResidentEntities != 0 {
		t.Fatalf("w kept %d warm files, %d resident", s.WarmEntities, s.ResidentEntities)
	}
	for r, err := range a.Scan(bg, "") {
		if err != nil || r.Ref.Key == "k" {
			t.Fatalf("scan yielded %+v %v", r, err)
		}
	}

	req := newReq(t, "again", 1)
	ref2, res, err := b.Create(bg, "k", req, createKV)
	if err != nil {
		t.Fatal(err)
	}
	if ref2.Incarnation == ref.Incarnation || res.Version.Sequence != 1 || string(res.Value) != "created" {
		t.Fatalf("recreation %+v %+v", ref2, res)
	}
	h2, _ := headAt(t, st, "k")
	if h2.Deleted || h2.Revision != tomb.Revision+1 || h2.Epoch != tomb.Epoch+1 || h2.Snapshot.BaseRevision != tomb.Revision ||
		h2.Owner == nil || h2.Owner.Node != "b" {
		t.Fatalf("recreated head %+v", h2)
	}
	if birth, _ := st.Get(bg, head.BirthKey("k"), "", head.MaxBirthBytes); !bytes.Equal(birth.Body, birth0.Body) {
		t.Fatal("recreation rewrote birth.json")
	}
	if _, res, err := a.Create(bg, "k", req, createKV); err != nil || !res.Replayed || res.Version != (Version{ref2.Incarnation, 1}) {
		t.Fatalf("retried recreation: %+v %v", res, err) // the receipt decides, not birth.json
	}
	_, _, err = a.Create(bg, "k", newReq(t, "other", 1), createKV)
	wantCode(t, err, CodeInvalidArgument)
	if got := readAll(t, a, ref2); len(got) != 0 { // nothing of the deleted incarnation
		t.Fatalf("recreated entity holds %v", got)
	}
	_, err = a.Read(bg, ref, 1, func(*View) ([]byte, error) { return nil, nil })
	wantCode(t, err, CodeIncarnationMismatch)
	_, err = b.Update(bg, ref, newReq(t, "old", 1), put("x", "y"))
	wantCode(t, err, CodeIncarnationMismatch)
	l2 := newLedger()
	mustPut(t, b, ref2, "new", "row", l2)
	if got := readAll(t, a, ref2); len(got) != 1 || got["new"] != "row" {
		t.Fatalf("recreated entity reads %v", got)
	}

	var archive bytes.Buffer // Import on a tombstone recreates too
	if _, err := b.Export(bg, ref2, &archive); err != nil {
		t.Fatal(err)
	}
	if err := b.Delete(bg, ref2); err != nil {
		t.Fatal(err)
	}
	ref3, _, err := a.Import(bg, "k", newReq(t, "import", 1), &archive)
	if err != nil {
		t.Fatal(err)
	}
	if ref3.Incarnation == ref2.Incarnation || ref3.Incarnation == ref.Incarnation {
		t.Fatalf("import reused incarnation %v", ref3)
	}
	if got := readAll(t, b, ref3); len(got) != 1 || got["new"] != "row" {
		t.Fatalf("imported entity reads %v", got)
	}
	if keys := entityKeys(t, st, "k"); len(keys) != 3 || keys[0] != "birth.json" || keys[1] != "head.json" {
		t.Fatalf("after Import the entity holds %v", keys) // the third is the import's snapshot
	}
}

// TestDeleteFollower: a node that never owns the entity reads it while the
// owner writes, then deletes it. Every read before the delete answers a state
// with no fewer rows than the previous one; once a read is not_found, every
// later read is.
func TestDeleteFollower(t *testing.T) {
	st := newStore(t)
	a := testNode(t, st, "a", nil)
	b := testNode(t, st, "b", nil)
	ref := mustCreate(t, a, "k", nil)
	var deleted atomic.Bool
	var wg sync.WaitGroup
	var reads, gone int
	var failure error
	wg.Go(func() {
		last := 0
		for {
			after := deleted.Load()
			n := 0
			_, err := b.Read(bg, ref, 1, func(v *View) ([]byte, error) {
				return nil, v.Query(bg, "SELECT count(*) FROM kv", nil, func(r *sql.Rows) error { r.Next(); return r.Scan(&n) })
			})
			var pe *Error
			switch {
			case err == nil && gone == 0 && n >= last:
				reads, last = reads+1, n
			case errors.As(err, &pe) && pe.Code == CodeNotFound:
				gone++
			default:
				failure = fmt.Errorf("read %d after %d rows and %d not_found: %v", n, last, gone, err)
				return
			}
			if after && gone > 0 {
				return
			}
		}
	})
	l := newLedger()
	for i := range 40 {
		mustPut(t, a, ref, fmt.Sprint("k", i), strings.Repeat("v", 1+i*100), l)
	}
	if err := a.Delete(bg, ref); err != nil {
		t.Fatal(err)
	}
	deleted.Store(true)
	wg.Wait()
	if failure != nil {
		t.Fatal(failure)
	}
	if reads == 0 || gone == 0 {
		t.Fatalf("%d reads, %d not_found", reads, gone)
	}
	t.Logf("%d reads during 40 writes and the delete, then not_found", reads)
}
