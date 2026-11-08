package garden

import (
	"archive/tar"
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/OrdalieTech/big-garden/internal/head"
	"github.com/OrdalieTech/big-garden/internal/store"
	"github.com/OrdalieTech/big-garden/internal/storetest"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

var bg = context.Background()

// ledger records every acknowledged write: key -> row -> value. Every entry
// must be recoverable from the store alone.
type ledger struct {
	mu   sync.Mutex
	rows map[string]map[string]string
	incs map[string][16]byte
}

func newLedger() *ledger {
	return &ledger{rows: map[string]map[string]string{}, incs: map[string][16]byte{}}
}

func (l *ledger) ack(ref Ref, k, v string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.rows[ref.Key] == nil {
		l.rows[ref.Key] = map[string]string{}
	}
	l.rows[ref.Key][k] = v
	l.incs[ref.Key] = ref.Incarnation
}

// verify reads every acknowledged row through a new Node with an empty cache.
func (l *ledger) verify(t *testing.T, st store.Store, nodeID string) {
	t.Helper()
	if fs, ok := st.(faultStore); ok {
		fs.SetPlan(nil)
	}
	n := testNode(t, st, nodeID, nil)
	defer n.Close(bg)
	for key, rows := range l.rows {
		got := readAll(t, n, Ref{key, l.incs[key]})
		for k, v := range rows {
			if got[k] != v {
				t.Fatalf("acknowledged write lost: %s/%s = %q, want %q", key, k, got[k], v)
			}
		}
	}
}

func testNode(t *testing.T, mem store.Store, nodeID string, mod func(*Limits)) *Node {
	t.Helper()
	cfg := Config{S3: s3.New(s3.Options{}), Bucket: "b", Prefix: "p", NodeID: nodeID, CacheDir: t.TempDir(),
		Limits: Limits{EntityBytes: 8 << 20, StorageAttemptTimeout: 200 * time.Millisecond, CommitTimeout: time.Second}}
	if b, err := strconv.Atoi(os.Getenv("GARDEN_TEST_BATCH")); err == nil { // =1: every core test again on the unbatched path
		cfg.Limits.BatchMax = b
	}
	if mod != nil {
		mod(&cfg.Limits)
	}
	cfg, err := cfg.validate()
	if err != nil {
		t.Fatal(err)
	}
	n, err := open(bg, cfg, mem)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { n.Close(bg) })
	return n
}

func newReq(t *testing.T, input string, schema uint32) Request {
	t.Helper()
	id, err := NewMutationID(time.Now())
	if err != nil {
		t.Fatal(err)
	}
	return Request{ID: id, Hash: [32]byte(append([]byte(input), make([]byte, 32)...)[:32]), Schema: schema}
}

func createKV(tx *Tx) ([]byte, error) {
	_, err := tx.Exec(bg, "CREATE TABLE kv(k TEXT PRIMARY KEY, v TEXT NOT NULL)")
	return []byte("created"), err
}

func put(k, v string) func(*Tx) ([]byte, error) {
	return func(tx *Tx) ([]byte, error) {
		_, err := tx.Exec(bg, "INSERT OR REPLACE INTO kv VALUES (?, ?)", k, v)
		return []byte("put " + k), err
	}
}

func readAll(t *testing.T, n *Node, ref Ref) map[string]string {
	t.Helper()
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
	if err != nil {
		t.Fatalf("read %s: %v", ref.Key, err)
	}
	return got
}

func wantCode(t *testing.T, err error, code Code) *Error {
	t.Helper()
	var e *Error
	if !errors.As(err, &e) || e.Code != code {
		t.Fatalf("got %v, want %s", err, code)
	}
	return e
}

func mustCreate(t *testing.T, n *Node, key string, l *ledger) Ref {
	t.Helper()
	ref, res, err := n.Create(bg, key, newReq(t, "create "+key, 1), createKV)
	if err != nil {
		t.Fatalf("create %s: %v", key, err)
	}
	if string(res.Value) != "created" || res.Version.Sequence != 1 || res.Replayed {
		t.Fatalf("create result %+v", res)
	}
	if l != nil {
		l.ack(ref, "", "")
		delete(l.rows[key], "")
	}
	return ref
}

func mustPut(t *testing.T, n *Node, ref Ref, k, v string, l *ledger) Result {
	t.Helper()
	res, err := n.Update(bg, ref, newReq(t, "put "+k+v, 1), put(k, v))
	if err != nil {
		t.Fatalf("update %s=%.20s: %v", k, v, err)
	}
	l.ack(ref, k, v)
	return res
}

func headAt(t *testing.T, st store.Store, key string) (*head.Head, string) {
	t.Helper()
	var o store.Object
	var err error
	if mem, ok := st.(*storetest.Mem); ok {
		if o, ok = mem.Peek(head.HeadKey(key)); !ok {
			err = errors.New("no head")
		}
	} else {
		o, err = st.Get(bg, head.HeadKey(key), "", head.MaxHeadBytes)
	}
	if err != nil {
		t.Fatalf("head of %s: %v", key, err)
	}
	h, err := head.Decode(o.Body)
	if err != nil {
		t.Fatal(err)
	}
	return &h, o.ETag
}

func randText(n int) string {
	b := make([]byte, n/2)
	rand.Read(b)
	return fmt.Sprintf("%x", b)
}

func TestOpenNamespace(t *testing.T) {
	mem := newStore(t)
	a := testNode(t, mem, "a", nil)
	b := testNode(t, mem, "b", nil)
	if a.ns != b.ns || a.ns == (head.ID{}) || a.self.Session == b.self.Session {
		t.Fatal("nodes disagree on the namespace or share a session")
	}
	// entities without namespace.json: fail closed
	mem2 := newStore(t)
	mem2.PutImmutable(bg, head.EntityDir("k")+"/birth.json", []byte("{}"))
	cfg, _ := Config{S3: s3.New(s3.Options{}), Bucket: "b", Prefix: "p", NodeID: "a", CacheDir: t.TempDir()}.validate()
	_, err := open(bg, cfg, mem2)
	wantCode(t, err, CodeStateMissing)
	// a store that ignores If-None-Match is unsupported
	mem3 := newStore(t)
	mem3.SetPlan(func(a storetest.Attempt) storetest.MemFault {
		if strings.HasPrefix(a.Key, "probe/") && a.Op == "cas" && a.Cond != "" && bytes.Equal(a.Body, []byte("3")) {
			return storetest.MemFault{Reject: store.Failed}
		}
		return storetest.MemFault{}
	})
	_, err = open(bg, cfg, mem3)
	wantCode(t, err, CodeUnsupported)
}

func TestSessionDirCleanup(t *testing.T) {
	mem := newStore(t)
	dir := t.TempDir()
	cfg, _ := Config{S3: s3.New(s3.Options{}), Bucket: "b", Prefix: "p", NodeID: "a", CacheDir: dir}.validate()
	a, err := open(bg, cfg, mem)
	if err != nil {
		t.Fatal(err)
	}
	b, err := open(bg, cfg, mem) // a still holds its lock: its dir survives
	if err != nil {
		t.Fatal(err)
	}
	for _, n := range []*Node{a, b} {
		if fi, err := statDir(n.dir); err != nil || !fi {
			t.Fatalf("session dir %s missing: %v", n.dir, err)
		}
	}
	adir := a.dir
	a.lock.Close() // simulate a dead process: the lock is released, the dir stays
	a.lock = nil
	c, err := open(bg, cfg, mem)
	if err != nil {
		t.Fatal(err)
	}
	if ok, _ := statDir(adir); ok {
		t.Fatal("stale session dir not removed")
	}
	b.Close(bg)
	c.Close(bg)
}

func TestCreateUpdateReadLookup(t *testing.T) {
	mem := newStore(t)
	n := testNode(t, mem, "a", nil)
	l := newLedger()
	ref := mustCreate(t, n, "doc", l)

	// first write after genesis always uploads (D16)
	r1 := mustPut(t, n, ref, "a", "1", l)
	if r1.Version.Sequence != 2 || string(r1.Value) != "put a" {
		t.Fatalf("update result %+v", r1)
	}
	h, _ := headAt(t, mem, "doc")
	if len(h.Deltas) != 1 || h.Tail != nil || h.Deltas[0].BaseRevision != 1 {
		t.Fatalf("first update did not upload: %d deltas, tail %v", len(h.Deltas), h.Tail != nil)
	}
	// small writes go inline: one CAS, no PUT
	journal := func() []storetest.Attempt { // Mem only; the head checks below hold on every store
		if mem, ok := mem.(*storetest.Mem); ok {
			return mem.Journal()
		}
		return nil
	}
	before := len(journal())
	mustPut(t, n, ref, "b", "2", l)
	mustPut(t, n, ref, "c", "3", l)
	for _, a := range journal()[before:] {
		if a.Op == "put" {
			t.Fatalf("inline write uploaded %s", a.Key)
		}
	}
	h, _ = headAt(t, mem, "doc")
	if len(h.Deltas) != 1 || h.Tail == nil || h.Tail.MinSequence != 3 || h.Tail.MaxSequence != 4 {
		t.Fatalf("inline tail missing or wrong: %+v", h.Tail)
	}
	// a delta above 31 KiB uploads and empties the tail
	big := randText(40 << 10)
	mustPut(t, n, ref, "big", big, l)
	h, _ = headAt(t, mem, "doc")
	if len(h.Deltas) != 2 || h.Tail != nil {
		t.Fatalf("large write did not upload: %d deltas, tail %v", len(h.Deltas), h.Tail != nil)
	}
	if got := readAll(t, n, ref); got["a"] != "1" || got["c"] != "3" || got["big"] != big {
		t.Fatalf("read %v", got)
	}

	// replay, conflict, lookup
	req := newReq(t, "x", 1)
	res, err := n.Update(bg, ref, req, put("x", "1"))
	if err != nil || res.Replayed {
		t.Fatal(err)
	}
	l.ack(ref, "x", "1")
	again, err := n.Update(bg, ref, req, put("x", "other"))
	if err != nil || !again.Replayed || again.Version != res.Version || string(again.Value) != "put x" {
		t.Fatalf("replay %+v %v", again, err)
	}
	other := req
	other.Hash[0] ^= 1
	_, err = n.Update(bg, ref, other, put("x", "2"))
	wantCode(t, err, CodeIDConflict)
	lk, err := n.Lookup(bg, ref, req.ID)
	if err != nil || !lk.Replayed || lk.Version != res.Version {
		t.Fatalf("lookup %+v %v", lk, err)
	}
	fresh := newReq(t, "never", 1)
	e := wantCode(t, func() error { _, err := n.Lookup(bg, ref, fresh.ID); return err }(), CodeAbsentAtVersion)
	if e.At.Sequence != res.Version.Sequence {
		t.Fatalf("absent at %+v", e.At)
	}

	// schema mismatch on update and read; incarnation mismatch
	_, err = n.Update(bg, ref, newReq(t, "s", 2), put("s", "s"))
	wantCode(t, err, CodeSchemaMismatch)
	_, err = n.Read(bg, ref, 2, func(*View) ([]byte, error) { return nil, nil })
	wantCode(t, err, CodeSchemaMismatch)
	_, err = n.Read(bg, Ref{"doc", [16]byte{1}}, 1, func(*View) ([]byte, error) { return nil, nil })
	wantCode(t, err, CodeIncarnationMismatch)

	// callback errors pass through unchanged and commit nothing
	boom := errors.New("boom")
	_, err = n.Update(bg, ref, newReq(t, "boom", 1), func(*Tx) ([]byte, error) { return nil, boom })
	if err != boom {
		t.Fatalf("callback error %v", err)
	}
	_, err = n.Read(bg, ref, 1, func(*View) ([]byte, error) { return nil, boom })
	if err != boom {
		t.Fatalf("read callback error %v", err)
	}
	_, err = n.Read(bg, ref, 1, func(*View) ([]byte, error) { return make([]byte, 17<<10), nil })
	wantCode(t, err, CodeCapacity)

	st := n.Stats()
	if st.Commits < 6 || st.Replays != 1 || st.Conflicts != 1 || st.Reads < 1 || st.ResidentEntities != 1 || st.Mutations < 6 {
		t.Fatalf("stats %+v", st)
	}
	route, err := n.Resolve(bg, "doc")
	if err != nil || route.Ref != ref || route.Owner != n.Owner() || route.Epoch != 1 {
		t.Fatalf("resolve %+v %v", route, err)
	}
	_, err = n.Resolve(bg, "nope")
	wantCode(t, err, CodeNotFound)
	l.verify(t, mem, "a")
}

func TestCreateIdempotentAndConflicts(t *testing.T) {
	mem := newStore(t)
	n := testNode(t, mem, "a", nil)
	req := newReq(t, "create", 1)
	ref, res, err := n.Create(bg, "k", req, createKV)
	if err != nil {
		t.Fatal(err)
	}
	ref2, res2, err := n.Create(bg, "k", req, createKV)
	if err != nil || ref2 != ref || !res2.Replayed || string(res2.Value) != string(res.Value) || res2.Version != res.Version {
		t.Fatalf("create replay %+v %+v %v", ref2, res2, err)
	}
	// replay from another node of the same NodeID (new process)
	n2 := testNode(t, mem, "a", nil)
	ref3, res3, err := n2.Create(bg, "k", req, createKV)
	if err != nil || ref3 != ref || !res3.Replayed {
		t.Fatalf("create replay on a new node %+v %v", res3, err)
	}
	conflict := req
	conflict.Hash[0] ^= 1
	_, _, err = n.Create(bg, "k", conflict, createKV)
	wantCode(t, err, CodeIDConflict)
	_, _, err = n.Create(bg, "k", newReq(t, "other", 1), createKV)
	if e := wantCode(t, err, CodeInvalidArgument); e.Route == nil || e.Route.Ref != ref {
		t.Fatalf("key exists without a route: %+v", e)
	}
	// a failing callback leaves no trace: the key stays creatable (D11)
	boom := errors.New("boom")
	_, _, err = n.Create(bg, "k2", newReq(t, "c2", 1), func(*Tx) ([]byte, error) { return nil, boom })
	if err != boom {
		t.Fatalf("callback error %v", err)
	}
	if page, _, _ := mem.List(bg, head.EntityDir("k2")+"/", "", 1); len(page) != 0 {
		t.Fatal("failed create left artifacts")
	}
	mustCreate(t, n, "k2", nil)
	// expired and future IDs
	old := newReq(t, "old", 1)
	id, _ := NewMutationID(time.Now().Add(-2 * time.Hour))
	old.ID = id
	_, _, err = n.Create(bg, "k3", old, createKV)
	wantCode(t, err, CodeIDExpired)
	future := newReq(t, "future", 1)
	future.ID, _ = NewMutationID(time.Now().Add(time.Hour))
	_, _, err = n.Create(bg, "k3", future, createKV)
	wantCode(t, err, CodeInvalidArgument)
}

func TestConcurrentCreates(t *testing.T) {
	mem := newStore(t)
	nodes := []*Node{testNode(t, mem, "a", nil), testNode(t, mem, "b", nil)}
	var wins atomic.Int32
	var wg sync.WaitGroup
	for i := range 8 {
		wg.Go(func() {
			_, _, err := nodes[i%2].Create(bg, "race", newReq(t, fmt.Sprint("c", i), 1), createKV)
			var e *Error
			switch {
			case err == nil:
				wins.Add(1)
			case errors.As(err, &e) && (e.Code == CodeStateMissing || e.Code == CodeInvalidArgument):
			default:
				t.Errorf("create %d: %v", i, err)
			}
		})
	}
	wg.Wait()
	if wins.Load() != 1 {
		t.Fatalf("%d creates won", wins.Load())
	}
}

// failHeadCAS rejects every head CAS with a definite failure until disarmed.
func failHeadCAS(mem faultStore, key string, armed *atomic.Bool) {
	mem.SetPlan(func(a storetest.Attempt) storetest.MemFault {
		if armed.Load() && a.Op == "cas" && a.Key == head.HeadKey(key) {
			return storetest.MemFault{Reject: store.Unknown}
		}
		return storetest.MemFault{}
	})
}

func TestCreateResumeD5(t *testing.T) {
	mem := newStore(t)
	n := testNode(t, mem, "a", func(l *Limits) { l.CommitTimeout = 300 * time.Millisecond })
	var armed atomic.Bool
	armed.Store(true)
	failHeadCAS(mem, "k", &armed)
	req := newReq(t, "create", 1)
	_, _, err := n.Create(bg, "k", req, createKV)
	if e := wantCode(t, err, CodeOutcomeUnknown); e.Operation != req.ID {
		t.Fatalf("operation %x", e.Operation)
	}
	_, _, err = n.Create(bg, "k", newReq(t, "other", 1), createKV)
	wantCode(t, err, CodeUnavailable) // the entity is blocked while unresolved
	armed.Store(false)
	// a new process resumes from birth.json with the same request (D5)
	n2 := testNode(t, mem, "a", nil)
	_, _, err = n2.Create(bg, "k", newReq(t, "other", 1), createKV)
	wantCode(t, err, CodeStateMissing)
	ref, res, err := n2.Create(bg, "k", req, createKV)
	if err != nil || res.Replayed || string(res.Value) != "created" {
		t.Fatalf("resume %+v %v", res, err)
	}
	l := newLedger()
	mustPut(t, n2, ref, "a", "1", l)
	l.verify(t, mem, "a")
	// a used head that disappears is never resumed
	mem.Delete(bg, head.HeadKey("k"))
	_, _, err = testNode(t, mem, "a", nil).Create(bg, "k", req, createKV)
	wantCode(t, err, CodeStateMissing)
}

// TestCreateAfterBirthIsUnknown: once birth.json may exist, only the same
// request can finish the create (D5), so every later failure is
// outcome_unknown and the same-request retry completes.
func TestCreateAfterBirthIsUnknown(t *testing.T) {
	for _, tc := range []struct {
		name  string
		fault func(a storetest.Attempt) bool
		fate  storetest.MemFault
	}{
		{"birth reply lost", func(a storetest.Attempt) bool { return a.Op == "put" && a.Key == head.BirthKey("k") },
			storetest.MemFault{LoseReply: true}},
		{"snapshot put refused", func(a storetest.Attempt) bool { return a.Op == "put" && strings.HasSuffix(a.Key, ".ltx") },
			storetest.MemFault{Reject: store.Failed}},
		{"genesis cas refused", func(a storetest.Attempt) bool { return a.Op == "cas" && a.Key == head.HeadKey("k") },
			storetest.MemFault{Reject: store.Failed}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mem := newStore(t)
			n := testNode(t, mem, "a", nil)
			var armed atomic.Bool
			armed.Store(true)
			mem.SetPlan(func(a storetest.Attempt) storetest.MemFault {
				if armed.Load() && tc.fault(a) {
					return tc.fate
				}
				return storetest.MemFault{}
			})
			req := newReq(t, "create", 1)
			_, _, err := n.Create(bg, "k", req, createKV)
			if e := wantCode(t, err, CodeOutcomeUnknown); e.Operation != req.ID {
				t.Fatalf("operation %x", e.Operation)
			}
			armed.Store(false)
			ref, res, err := n.Create(bg, "k", req, createKV)
			if err != nil || string(res.Value) != "created" {
				t.Fatalf("same-request retry %+v %v", res, err)
			}
			l := newLedger()
			mustPut(t, n, ref, "a", "1", l)
			l.verify(t, mem, "a")
		})
	}
}

func TestMissingHeadFailsClosed(t *testing.T) {
	mem := newStore(t)
	n := testNode(t, mem, "a", nil)
	l := newLedger()
	ref := mustCreate(t, n, "k", l)
	mustPut(t, n, ref, "a", "1", l)
	mem.Delete(bg, head.HeadKey("k"))
	_, err := n.Read(bg, ref, 1, func(*View) ([]byte, error) { return nil, nil })
	wantCode(t, err, CodeStateMissing)
	n2 := testNode(t, mem, "a", nil)
	_, err = n2.Update(bg, ref, newReq(t, "u", 1), put("b", "2"))
	wantCode(t, err, CodeStateMissing)
	_, err = n2.Resolve(bg, "k")
	wantCode(t, err, CodeStateMissing)
	_, _, err = n2.Create(bg, "k", newReq(t, "c", 1), createKV)
	wantCode(t, err, CodeStateMissing)
	_, err = n2.Read(bg, Ref{Key: "never"}, 1, func(*View) ([]byte, error) { return nil, nil })
	wantCode(t, err, CodeNotFound)
}

func TestRestartRecoversEverything(t *testing.T) {
	mem := newStore(t)
	n := testNode(t, mem, "a", nil)
	l := newLedger()
	refs := []Ref{mustCreate(t, n, "x", l), mustCreate(t, n, "y", l)}
	for i := range 20 {
		v := fmt.Sprint(i)
		if i%7 == 0 {
			v = randText(40 << 10)
		}
		mustPut(t, n, refs[i%2], fmt.Sprint("k", i%5), v, l)
	}
	// a new process of the same node takes over implicitly and sees everything
	l.verify(t, mem, "a")
	n2 := testNode(t, mem, "a", nil)
	mustPut(t, n2, refs[0], "after", "restart", l)
	h, _ := headAt(t, mem, "x")
	if *h.Owner != n2.self || h.Epoch != 3 { // a's verify node claimed it at epoch 2
		t.Fatalf("owner %+v epoch %d", h.Owner, h.Epoch)
	}
	// the old session is fenced: its cached head is stale, its CAS fails
	_, err := n.Update(bg, refs[0], newReq(t, "stale", 1), put("stale", "1"))
	wantCode(t, err, CodeNotOwner)
	// another node never takes over implicitly
	_, err = testNode(t, mem, "b", nil).Read(bg, refs[0], 1, func(*View) ([]byte, error) { return nil, nil })
	wantCode(t, err, CodeNotOwner)
	l.verify(t, mem, "a")
}

func TestSnapshotOnCap(t *testing.T) {
	mem := newStore(t)
	n := testNode(t, mem, "a", nil)
	n.skipMaintenance = true // §9 would compact long before the §6.7 cap
	l := newLedger()
	ref := mustCreate(t, n, "k", l)
	snapshots := 0
	for i := range 40 {
		mustPut(t, n, ref, "blob", randText(40<<10), l)
		h, _ := headAt(t, mem, "k")
		if len(h.Deltas) > head.MaxDeltas {
			t.Fatalf("%d deltas", len(h.Deltas))
		}
		if len(h.Deltas) == 0 {
			snapshots++
			if h.Snapshot.BaseRevision == 0 || h.Snapshot.MaxSequence != uint64(i+2) || h.Tail != nil {
				t.Fatalf("bad snapshot ref %+v", h.Snapshot)
			}
		}
	}
	if snapshots == 0 {
		t.Fatal("replay pages never triggered a snapshot")
	}
	// a large database reaches the 32-delta cap before the replay bound
	ref2 := mustCreate(t, n, "wide", l)
	if _, err := n.Update(bg, ref2, newReq(t, "fill", 1), func(tx *Tx) ([]byte, error) {
		for i := range 1000 {
			if _, err := tx.Exec(bg, "INSERT INTO kv VALUES (?, ?)", fmt.Sprint("fill", i), randText(4000)); err != nil {
				return nil, err
			}
		}
		return nil, nil
	}); err != nil {
		t.Fatal(err)
	}
	capped := false
	for prev := 1; !capped; {
		mustPut(t, n, ref2, "blob", randText(40<<10), l)
		h, _ := headAt(t, mem, "wide")
		switch {
		case prev == head.MaxDeltas:
			if len(h.Deltas) != 0 || h.Snapshot.BaseRevision == 0 {
				t.Fatalf("write after %d deltas did not snapshot: %d deltas", prev, len(h.Deltas))
			}
			capped = true
		case len(h.Deltas) != prev+1:
			t.Fatalf("snapshot before the cap at %d deltas", prev)
		}
		prev = len(h.Deltas)
	}
	l.verify(t, mem, "a")
}

func TestConcurrentSameCreate(t *testing.T) {
	mem := newStore(t)
	nodes := []*Node{testNode(t, mem, "a", nil), testNode(t, mem, "b", nil), testNode(t, mem, "c", nil)}
	req := newReq(t, "same", 1)
	refs := make([]Ref, len(nodes))
	errs := make([]error, len(nodes))
	var wg sync.WaitGroup
	for i, n := range nodes {
		wg.Go(func() { refs[i], _, errs[i] = n.Create(bg, "k", req, createKV) })
	}
	wg.Wait()
	for i, err := range errs {
		var e *Error
		switch {
		case err == nil:
			if refs[i] != refs[0] && errs[0] == nil {
				t.Fatalf("two incarnations acknowledged: %x %x", refs[i].Incarnation, refs[0].Incarnation)
			}
		case errors.As(err, &e) && e.Code == CodeNotOwner: // replay found another node's head
		default:
			t.Fatalf("node %d: %v", i, err)
		}
	}
}

func TestIDExpiredByFloor(t *testing.T) {
	mem := newStore(t)
	ahead := testNode(t, mem, "a", nil)
	ahead.now = func() time.Time { return time.Now().Add(2 * time.Hour) } // a skewed clock moves the floor
	l := newLedger()
	creq := newReq(t, "create", 1)
	creq.ID, _ = NewMutationID(time.Now().Add(2 * time.Hour))
	ref, _, err := ahead.Create(bg, "k", creq, createKV)
	if err != nil {
		t.Fatal(err)
	}
	reqAhead := newReq(t, "ahead", 1)
	reqAhead.ID, _ = NewMutationID(time.Now().Add(2 * time.Hour))
	if _, err := ahead.Update(bg, ref, reqAhead, put("a", "1")); err != nil {
		t.Fatal(err)
	}
	l.ack(ref, "a", "1")
	n := testNode(t, mem, "a", nil) // same NodeID, normal clock: implicit takeover
	req := newReq(t, "late", 1)
	_, err = n.Update(bg, ref, req, put("b", "2"))
	wantCode(t, err, CodeIDExpired)
	_, err = n.Lookup(bg, ref, req.ID)
	wantCode(t, err, CodeIDExpired)
	l.verify(t, mem, "a")
}

// TestResolveBranches drives each resolve() branch with scheduled faults.
func TestResolveBranches(t *testing.T) {
	t.Run("lost reply after apply", func(t *testing.T) {
		mem := newStore(t)
		n := testNode(t, mem, "a", nil)
		l := newLedger()
		ref := mustCreate(t, n, "k", l)
		mustPut(t, n, ref, "a", "1", l)
		var once atomic.Bool
		mem.SetPlan(func(a storetest.Attempt) storetest.MemFault {
			if a.Op == "cas" && a.Key == head.HeadKey("k") && once.CompareAndSwap(false, true) {
				return storetest.MemFault{LoseReply: true}
			}
			return storetest.MemFault{}
		})
		res := mustPut(t, n, ref, "b", "2", l) // inline: resolved by its nonce
		res2 := mustPut(t, n, ref, "c", randText(40<<10), l)
		if res.Version.Sequence != 3 || res2.Version.Sequence != 4 {
			t.Fatalf("versions %+v %+v", res.Version, res2.Version)
		}
		l.verify(t, mem, "a")
	})
	t.Run("apply after client cancel, retry same ID", func(t *testing.T) {
		mem := newStore(t)
		n := testNode(t, mem, "a", func(l *Limits) { l.CommitTimeout = 400 * time.Millisecond })
		l := newLedger()
		ref := mustCreate(t, n, "k", l)
		mustPut(t, n, ref, "a", "1", l)
		hold := make(chan struct{})
		var armed atomic.Bool
		armed.Store(true)
		mem.SetPlan(func(a storetest.Attempt) storetest.MemFault {
			if armed.Load() && a.Op == "cas" && a.Key == head.HeadKey("k") {
				return storetest.MemFault{Hold: hold}
			}
			return storetest.MemFault{}
		})
		req := newReq(t, "held", 1)
		_, err := n.Update(bg, ref, req, put("b", "2"))
		wantCode(t, err, CodeOutcomeUnknown)
		_, err = n.Update(bg, ref, newReq(t, "other", 1), put("c", "3"))
		wantCode(t, err, CodeUnavailable)
		_, err = n.Update(bg, ref, req, put("b", "2"))
		wantCode(t, err, CodeOutcomeUnknown)
		armed.Store(false)
		close(hold) // the held proposals apply now; exactly one wins
		time.Sleep(50 * time.Millisecond)
		res, err := n.Update(bg, ref, req, put("b", "2"))
		if err != nil || !res.Replayed || res.Version.Sequence != 3 {
			t.Fatalf("retry %+v %v", res, err)
		}
		l.ack(ref, "b", "2")
		mustPut(t, n, ref, "c", "3", l)
		l.verify(t, mem, "a")
	})
	t.Run("hedged duplicate", func(t *testing.T) {
		mem := newStore(t)
		n := testNode(t, mem, "a", nil)
		l := newLedger()
		ref := mustCreate(t, n, "k", l)
		mustPut(t, n, ref, "a", "1", l)
		hold := make(chan struct{})
		var first atomic.Bool
		mem.SetPlan(func(a storetest.Attempt) storetest.MemFault {
			if a.Op == "cas" && a.Key == head.HeadKey("k") && first.CompareAndSwap(false, true) {
				return storetest.MemFault{Hold: hold}
			}
			return storetest.MemFault{}
		})
		before := len(mem.Journal())
		mustPut(t, n, ref, "b", "2", l)
		var cas [][]byte
		for _, a := range mem.Journal()[before:] {
			if a.Op == "cas" {
				cas = append(cas, a.Body)
			}
		}
		if len(cas) != 2 || !bytes.Equal(cas[0], cas[1]) {
			t.Fatalf("want two identical CAS requests, got %d", len(cas))
		}
		close(hold) // the stalled original applies late: 412, no effect
		time.Sleep(20 * time.Millisecond)
		mustPut(t, n, ref, "c", "3", l)
		l.verify(t, mem, "a")
	})
	t.Run("412 receipt absent", func(t *testing.T) {
		mem := newStore(t)
		n := testNode(t, mem, "a", nil)
		l := newLedger()
		ref := mustCreate(t, n, "k", l)
		mustPut(t, n, ref, "a", "1", l)
		mustPut(t, n, ref, "b", "1", l)
		stealHead(t, mem, "k")
		_, err := n.Update(bg, ref, newReq(t, "lost", 1), put("c", "3"))
		if e := wantCode(t, err, CodeNotOwner); e.Route == nil || e.Route.Owner.NodeID != "thief" {
			t.Fatalf("route %+v", e.Route)
		}
	})
	t.Run("412 receipt present", func(t *testing.T) {
		mem := newStore(t)
		n := testNode(t, mem, "a", nil)
		l := newLedger()
		ref := mustCreate(t, n, "k", l)
		mustPut(t, n, ref, "a", "1", l)
		mustPut(t, n, ref, "b", "1", l)
		hold := make(chan struct{})
		var state atomic.Int32 // 0: arm CAS, 1: hold the next head GET, 2: done
		mem.SetPlan(func(a storetest.Attempt) storetest.MemFault {
			switch {
			case a.Op == "cas" && a.Key == head.HeadKey("k") && state.CompareAndSwap(0, 1):
				return storetest.MemFault{LoseReply: true}
			case a.Op == "get" && a.Key == head.HeadKey("k") && state.CompareAndSwap(1, 2):
				return storetest.MemFault{Hold: hold}
			}
			return storetest.MemFault{}
		})
		go func() {
			for state.Load() != 2 {
				time.Sleep(time.Millisecond)
			}
			stealHead(t, mem, "k") // the head moves past our candidate before resolve reads it
			close(hold)
		}()
		res, err := n.Update(bg, ref, newReq(t, "applied", 1), put("c", "3"))
		if err != nil || string(res.Value) != "put c" || res.Version.Sequence != 4 {
			t.Fatalf("receipt in the new head must prove the commit: %+v %v", res, err)
		}
	})
	t.Run("every proposal definitely failed", func(t *testing.T) {
		mem := newStore(t)
		n := testNode(t, mem, "a", nil)
		l := newLedger()
		ref := mustCreate(t, n, "k", l)
		mustPut(t, n, ref, "a", "1", l)
		var once atomic.Bool
		mem.SetPlan(func(a storetest.Attempt) storetest.MemFault {
			if a.Op == "cas" && a.Key == head.HeadKey("k") && once.CompareAndSwap(false, true) {
				return storetest.MemFault{Reject: store.Failed}
			}
			return storetest.MemFault{}
		})
		start := time.Now()
		_, err := n.Update(bg, ref, newReq(t, "refused", 1), put("b", "2"))
		wantCode(t, err, CodeUnavailable)
		if time.Since(start) >= n.cfg.Limits.CommitTimeout {
			t.Fatal("a definitely refused candidate waited for the commit deadline")
		}
		e, _ := n.entity("k")
		e.mu.Lock()
		if e.unresolved != nil {
			t.Fatal("candidate still unresolved")
		}
		e.mu.Unlock()
		if res := mustPut(t, n, ref, "c", "3", l); res.Version.Sequence != 3 {
			t.Fatalf("version %+v", res.Version)
		}
		l.verify(t, mem, "a")
	})
	t.Run("ambiguous then definite stays unknown", func(t *testing.T) {
		mem := newStore(t)
		n := testNode(t, mem, "a", func(l *Limits) { l.CommitTimeout = 400 * time.Millisecond })
		var logBuf syncBuffer
		n.cfg.Logger = slog.New(slog.NewTextHandler(&logBuf, nil))
		l := newLedger()
		ref := mustCreate(t, n, "k", l)
		mustPut(t, n, ref, "a", "1", l)
		var cas atomic.Int32
		mem.SetPlan(func(a storetest.Attempt) storetest.MemFault {
			if a.Op != "cas" || a.Key != head.HeadKey("k") {
				return storetest.MemFault{}
			}
			if cas.Add(1) == 1 {
				return storetest.MemFault{Reject: store.Unknown} // may still apply late
			}
			return storetest.MemFault{Reject: store.Failed}
		})
		before := len(mem.Journal())
		req := newReq(t, "late", 1)
		_, err := n.Update(bg, ref, req, put("b", "2"))
		wantCode(t, err, CodeOutcomeUnknown)
		if cas.Load() != 3 {
			t.Fatalf("want 3 proposals, got %d", cas.Load())
		}
		_, err = n.Update(bg, ref, newReq(t, "other", 1), put("c", "3"))
		wantCode(t, err, CodeUnavailable)
		if st := n.Stats(); st.OldestUnresolved < 400*time.Millisecond || st.UnknownOutcomes != 1 {
			t.Fatalf("stats %+v", st)
		}
		if logs := strings.Count(logBuf.String(), "code=outcome_unknown"); logs != 1 || strings.Contains(logBuf.String(), `"k"`) {
			t.Fatalf("log %q", logBuf.String())
		}
		mem.SetPlan(nil)
		var first storetest.Attempt
		for _, a := range mem.Journal()[before:] {
			if a.Op == "cas" && a.Key == head.HeadKey("k") {
				first = a
				break
			}
		}
		if _, err := mem.PutCAS(bg, head.HeadKey("k"), first.Body, first.Cond); err != nil { // the late apply
			t.Fatal(err)
		}
		res, err := n.Update(bg, ref, req, put("b", "2"))
		if err != nil || !res.Replayed || res.Version.Sequence != 3 {
			t.Fatalf("retry %+v %v", res, err)
		}
		l.ack(ref, "b", "2")
		mustPut(t, n, ref, "c", "3", l)
		if st := n.Stats(); st.OldestUnresolved != 0 {
			t.Fatalf("stats %+v", st)
		}
		l.verify(t, mem, "a")
	})
}

// TestResolvePrunedReceipt: an inline mutation applies, its reply is lost,
// and before resolve reads the head another owner advances the expiry floor
// past the ID, pruning the receipt. Resolve must not call it not committed.
func TestResolvePrunedReceipt(t *testing.T) {
	mem := newStore(t)
	n := testNode(t, mem, "a", nil)
	other := testNode(t, mem, "b", nil)
	other.now = func() time.Time { return time.Now().Add(2 * time.Hour) }
	l := newLedger()
	ref := mustCreate(t, n, "k", l)
	mustPut(t, n, ref, "a", "1", l)
	hold := make(chan struct{})
	var state atomic.Int32
	mem.SetPlan(func(a storetest.Attempt) storetest.MemFault {
		switch {
		case a.Op == "cas" && a.Key == head.HeadKey("k") && state.CompareAndSwap(0, 1):
			return storetest.MemFault{LoseReply: true}
		case a.Op == "get" && a.Key == head.HeadKey("k") && state.CompareAndSwap(1, 2):
			return storetest.MemFault{Hold: hold}
		}
		return storetest.MemFault{}
	})
	go func() {
		defer close(hold)
		for state.Load() != 2 {
			time.Sleep(time.Millisecond)
		}
		route, err := other.Resolve(bg, "k")
		if err == nil {
			_, err = other.Takeover(bg, route)
		}
		req := newReq(t, "advance the floor", 1)
		req.ID, _ = NewMutationID(time.Now().Add(2 * time.Hour))
		if err == nil {
			_, err = other.Update(bg, ref, req, put("c", "3"))
		}
		if err != nil {
			t.Error(err)
		}
	}()
	req := newReq(t, "applied", 1)
	_, err := n.Update(bg, ref, req, put("b", "2"))
	if e := wantCode(t, err, CodeOutcomeUnknown); e.Operation != req.ID {
		t.Fatalf("operation %x", e.Operation)
	}
	if got := readAll(t, other, ref); got["b"] != "2" { // it did commit
		t.Fatalf("rows %v", got)
	}
	_, err = other.Lookup(bg, ref, req.ID)
	wantCode(t, err, CodeIDExpired)
}

// TestReproposeAfterRecovery: the D12 proposal cap is per resolve call, so a
// candidate whose first proposals all got 503s commits once S3 recovers.
func TestReproposeAfterRecovery(t *testing.T) {
	mem := newStore(t)
	n := testNode(t, mem, "a", func(l *Limits) { l.CommitTimeout = 400 * time.Millisecond })
	l := newLedger()
	ref := mustCreate(t, n, "k", l)
	mustPut(t, n, ref, "a", "1", l)
	var cas atomic.Int32
	mem.SetPlan(func(a storetest.Attempt) storetest.MemFault {
		if a.Op == "cas" && a.Key == head.HeadKey("k") && cas.Add(1) <= 4 {
			return storetest.MemFault{Reject: store.Unknown}
		}
		return storetest.MemFault{}
	})
	req := newReq(t, "ambiguous", 1)
	_, err := n.Update(bg, ref, req, put("b", "2"))
	wantCode(t, err, CodeOutcomeUnknown)
	mustPut(t, n, ref, "c", "3", l) // settles the candidate first: it commits
	res, err := n.Lookup(bg, ref, req.ID)
	if err != nil || !res.Replayed || res.Version.Sequence != 3 {
		t.Fatalf("lookup %+v %v", res, err)
	}
	l.ack(ref, "b", "2")
	l.verify(t, mem, "a")
}

// TestDeadlineEndsWaiting: a caller's deadline bounds both its wait for the
// entity mutex and its settle of an unresolved candidate (PLAN §8.4).
func TestDeadlineEndsWaiting(t *testing.T) {
	mem := newStore(t)
	n := testNode(t, mem, "a", func(l *Limits) { l.CommitTimeout = 300 * time.Millisecond })
	l := newLedger()
	ref := mustCreate(t, n, "k", l)
	mustPut(t, n, ref, "a", "1", l)
	var armed atomic.Bool
	armed.Store(true)
	mem.SetPlan(func(a storetest.Attempt) storetest.MemFault {
		if armed.Load() && a.Key == head.HeadKey("k") && (a.Op == "cas" || a.Op == "get") {
			return storetest.MemFault{Reject: store.Unknown}
		}
		return storetest.MemFault{}
	})
	req := newReq(t, "stuck", 1)
	_, err := n.Update(bg, ref, req, put("b", "2"))
	wantCode(t, err, CodeOutcomeUnknown)
	n.cfg.Limits.CommitTimeout = 5 * time.Second
	short := func() (time.Duration, error) {
		ctx, cancel := context.WithTimeout(bg, 50*time.Millisecond)
		defer cancel()
		start := time.Now()
		_, err := n.Update(ctx, ref, newReq(t, "short", 1), put("c", "3"))
		return time.Since(start), err
	}
	if d, err := short(); d > time.Second { // the settle runs under the caller's deadline
		t.Fatalf("settle took %v: %v", d, err)
	}
	held := make(chan struct{})
	go func() { // a long settle holds the entity mutex
		ctx, cancel := context.WithTimeout(bg, 2*time.Second)
		defer cancel()
		close(held)
		n.Update(ctx, ref, req, put("b", "2"))
	}()
	<-held
	time.Sleep(20 * time.Millisecond)
	d, err := short()
	if d > time.Second {
		t.Fatalf("queued caller waited %v", d)
	}
	wantCode(t, err, CodeOutcomeUnknown) // queued: the candidate might have been its own
	armed.Store(false)
	if _, err := n.Update(bg, ref, req, put("b", "2")); err != nil {
		t.Fatal(err)
	}
	l.ack(ref, "b", "2")
	l.verify(t, mem, "a")
}

// TestCreateCallbackPanic: a panicking Create callback releases its cache
// reservation and file and is not counted as a success.
func TestCreateCallbackPanic(t *testing.T) {
	n := testNode(t, newStore(t), "a", nil)
	for range 5 {
		func() {
			defer func() {
				if recover() == nil {
					t.Fatal("no panic")
				}
			}()
			n.Create(bg, "k", newReq(t, "create", 1), func(*Tx) ([]byte, error) { panic("boom") })
		}()
	}
	n.mu.Lock()
	reserved := n.reserved
	n.mu.Unlock()
	if entries, _ := os.ReadDir(n.dir); reserved != 0 || len(entries) != 1 {
		t.Fatalf("reserved %d, cache dir %v", reserved, entries)
	}
	if st := n.Stats(); st.Mutations != 0 || st.CacheBytes != 0 {
		t.Fatalf("stats %+v", st)
	}
	mustCreate(t, n, "k", nil)
	_, _, err := n.Create(bg, "k2", newReq(t, "nil", 1), nil)
	wantCode(t, err, CodeInvalidArgument)
}

// blockingWriter blocks every Write until release is closed.
type blockingWriter struct{ entered, release chan struct{} }

func (w *blockingWriter) Write(p []byte) (int, error) {
	select {
	case w.entered <- struct{}{}:
	default:
	}
	<-w.release
	return len(p), nil
}

// TestExportWriterDoesNotBlockEntity: Export writes to the caller's writer
// without holding the entity mutex.
func TestExportWriterDoesNotBlockEntity(t *testing.T) {
	mem := newStore(t)
	n := testNode(t, mem, "a", nil)
	l := newLedger()
	ref := mustCreate(t, n, "k", l)
	w := &blockingWriter{make(chan struct{}, 1), make(chan struct{})}
	done := make(chan error, 1)
	go func() { _, err := n.Export(bg, ref, w); done <- err }()
	<-w.entered
	ctx, cancel := context.WithTimeout(bg, 2*time.Second)
	defer cancel()
	_, err := n.Update(ctx, ref, newReq(t, "during export", 1), put("a", "1"))
	close(w.release)
	if err != nil {
		t.Fatalf("update behind a blocked export: %v", err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

type syncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

func TestResolveObjectBranch(t *testing.T) {
	mem := newStore(t)
	n := testNode(t, mem, "a", nil)
	l := newLedger()
	ref := mustCreate(t, n, "k", l)
	mustPut(t, n, ref, "a", "1", l)
	hold := make(chan struct{})
	var state atomic.Int32
	mem.SetPlan(func(a storetest.Attempt) storetest.MemFault {
		switch {
		case a.Op == "cas" && a.Key == head.HeadKey("k") && state.CompareAndSwap(0, 1):
			return storetest.MemFault{LoseReply: true}
		case a.Op == "get" && a.Key == head.HeadKey("k") && state.CompareAndSwap(1, 2):
			return storetest.MemFault{Hold: hold}
		}
		return storetest.MemFault{}
	})
	go func() {
		for state.Load() != 2 {
			time.Sleep(time.Millisecond)
		}
		stealHead(t, mem, "k")
		close(hold)
	}()
	big := randText(40 << 10)
	res, err := n.Update(bg, ref, newReq(t, "upload", 1), put("big", big))
	if err != nil || res.Version.Sequence != 3 {
		t.Fatalf("an object referenced by the new head proves the commit: %+v %v", res, err)
	}
	h, _ := headAt(t, mem, "k")
	if h.Owner.Node != "thief" || h.Sequence != 3 || len(h.Deltas) != 2 {
		t.Fatalf("head %+v", h)
	}
}

func TestResolveHydrationFailureIsUnknown(t *testing.T) {
	mem := newStore(t)
	n := testNode(t, mem, "a", func(l *Limits) { l.CommitTimeout = 500 * time.Millisecond })
	l := newLedger()
	ref := mustCreate(t, n, "k", l)
	mustPut(t, n, ref, "a", "1", l)
	hold := make(chan struct{})
	var state atomic.Int32
	mem.SetPlan(func(a storetest.Attempt) storetest.MemFault {
		switch {
		case a.Op == "cas" && a.Key == head.HeadKey("k") && state.CompareAndSwap(0, 1):
			return storetest.MemFault{LoseReply: true}
		case a.Op == "get" && a.Key == head.HeadKey("k") && state.CompareAndSwap(1, 2):
			return storetest.MemFault{Hold: hold}
		case a.Op == "get" && strings.HasSuffix(a.Key, ".ltx") && state.Load() == 2:
			return storetest.MemFault{Reject: store.Unknown} // the receipt check cannot hydrate
		}
		return storetest.MemFault{}
	})
	go func() {
		for state.Load() != 2 {
			time.Sleep(time.Millisecond)
		}
		stealHead(t, mem, "k")
		close(hold)
	}()
	req := newReq(t, "applied", 1)
	_, err := n.Update(bg, ref, req, put("b", "2")) // inline, applied, then the head moved on
	if e := wantCode(t, err, CodeOutcomeUnknown); e.Operation != req.ID {
		t.Fatalf("operation %x", e.Operation)
	}
	state.Store(3)
	if res, err := n.Update(bg, ref, req, put("b", "2")); err == nil || !strings.Contains(err.Error(), "not_owner") {
		t.Fatalf("retry after resolution: %+v %v", res, err)
	}
	if res, err := testNode(t, mem, "thief-reader", nil).Resolve(bg, "k"); err != nil || res.Owner.NodeID != "thief" {
		t.Fatal(err)
	}
}

// stealHead publishes a takeover of key by another node directly.
func stealHead(t *testing.T, mem store.Store, key string) {
	h, etag := headAt(t, mem, key)
	h.Owner = &head.Owner{Node: "thief", Session: head.NewID()}
	h.Epoch, h.Revision, h.Nonce = h.Epoch+1, h.Revision+1, head.NewID()
	body, err := h.Encode()
	if err != nil {
		t.Error(err)
		return
	}
	if _, err := mem.PutCAS(bg, head.HeadKey(key), body, etag); err != nil {
		t.Error(err)
	}
}

func statDir(p string) (bool, error) {
	fi, err := os.Lstat(p)
	return err == nil && fi.IsDir(), err
}

func TestOwnershipAndMigrate(t *testing.T) {
	mem := newStore(t)
	a := testNode(t, mem, "a", nil)
	b := testNode(t, mem, "b", nil)
	l := newLedger()
	ref := mustCreate(t, a, "k", l)
	mustPut(t, a, ref, "a", "1", l)
	mustPut(t, a, ref, "b", "2", l) // inline tail carried by the takeover
	route, err := b.Resolve(bg, "k")
	if err != nil {
		t.Fatal(err)
	}
	stale := route
	stale.Epoch--
	_, err = b.Takeover(bg, stale)
	wantCode(t, err, CodeRouteChanged)
	got, err := b.Takeover(bg, route)
	if err != nil || got.Owner != b.Owner() || got.Epoch != route.Epoch+1 {
		t.Fatalf("takeover %+v %v", got, err)
	}
	if again, err := b.Takeover(bg, route); err != nil || again != got {
		t.Fatalf("takeover by the owner is a no-op: %+v %v", again, err)
	}
	mustPut(t, b, ref, "c", "3", l)
	_, err = a.Update(bg, ref, newReq(t, "fenced", 1), put("x", "x")) // cached head: the CAS fails
	if e := wantCode(t, err, CodeNotOwner); e.Route == nil || e.Route.Owner != b.Owner() {
		t.Fatalf("route %+v", e.Route)
	}
	_, err = a.Read(bg, ref, 1, func(*View) ([]byte, error) { return nil, nil })
	wantCode(t, err, CodeNotOwner)

	// release, then the unowned entity is claimable
	if err := b.Release(bg, ref); err != nil {
		t.Fatal(err)
	}
	if err := b.Release(bg, ref); err != nil {
		t.Fatalf("release by a non-owner is nil: %v", err)
	}
	_, err = b.Read(bg, ref, 1, func(*View) ([]byte, error) { return nil, nil })
	wantCode(t, err, CodeNotOwner)
	route, _ = a.Resolve(bg, "k")
	if route.Owner != (Owner{}) {
		t.Fatalf("released route %+v", route)
	}
	if _, err := a.Takeover(bg, route); err != nil {
		t.Fatal(err)
	}
	if got := readAll(t, a, ref); got["c"] != "3" || got["b"] != "2" {
		t.Fatalf("read after takeovers %v", got)
	}

	// evict keeps ownership; the next access rehydrates
	if err := a.Evict(bg, ref); err != nil {
		t.Fatal(err)
	}
	mustPut(t, a, ref, "d", "4", l)

	// migrate
	_, err = a.Migrate(bg, ref, newReq(t, "m0", 1), 1, func(*Tx) error { return nil })
	wantCode(t, err, CodeInvalidArgument)
	mreq := newReq(t, "m", 1)
	res, err := a.Migrate(bg, ref, mreq, 2, func(tx *Tx) error {
		_, err := tx.Exec(bg, "ALTER TABLE kv ADD COLUMN n INTEGER")
		return err
	})
	if err != nil || len(res.Value) != 0 {
		t.Fatalf("migrate %+v %v", res, err)
	}
	if again, err := a.Migrate(bg, ref, mreq, 2, func(*Tx) error { return nil }); err != nil || !again.Replayed {
		t.Fatalf("migrate replay %+v %v", again, err)
	}
	_, err = a.Update(bg, ref, newReq(t, "old schema", 1), put("e", "5"))
	wantCode(t, err, CodeSchemaMismatch)
	if _, err := a.Read(bg, ref, 2, func(*View) ([]byte, error) { return nil, nil }); err != nil {
		t.Fatal(err)
	}
	h, _ := headAt(t, mem, "k")
	if h.Schema != 2 {
		t.Fatalf("head schema %d", h.Schema)
	}
}

func TestCloseReportsUnresolved(t *testing.T) {
	mem := newStore(t)
	n := testNode(t, mem, "a", func(l *Limits) { l.CommitTimeout = 300 * time.Millisecond })
	l := newLedger()
	ref := mustCreate(t, n, "k", l)
	mustPut(t, n, ref, "a", "1", l)
	var armed atomic.Bool
	armed.Store(true)
	failHeadCAS(mem, "k", &armed)
	_, err := n.Update(bg, ref, newReq(t, "u", 1), put("b", "2"))
	wantCode(t, err, CodeOutcomeUnknown)
	err = n.Evict(bg, ref)
	wantCode(t, err, CodeOutcomeUnknown)
	wantCode(t, n.Close(bg), CodeOutcomeUnknown)
	_, err = n.Read(bg, ref, 1, func(*View) ([]byte, error) { return nil, nil })
	wantCode(t, err, CodeClosed)
	armed.Store(false)
	l.verify(t, mem, "a")
}

// TestCloseHonorsContext: Close stops waiting for a busy entity when its
// ctx ends, concurrent Closes are safe, and Resolve is closed afterwards.
func TestCloseHonorsContext(t *testing.T) {
	mem := newStore(t)
	n := testNode(t, mem, "a", func(l *Limits) { l.CommitTimeout = 300 * time.Millisecond })
	l := newLedger()
	ref := mustCreate(t, n, "k", l)
	mustPut(t, n, ref, "a", "1", l)
	var armed atomic.Bool
	armed.Store(true)
	mem.SetPlan(func(a storetest.Attempt) storetest.MemFault {
		if armed.Load() && a.Key == head.HeadKey("k") && (a.Op == "cas" || a.Op == "get") {
			return storetest.MemFault{Reject: store.Unknown}
		}
		return storetest.MemFault{}
	})
	req := newReq(t, "stuck", 1)
	_, err := n.Update(bg, ref, req, put("b", "2"))
	wantCode(t, err, CodeOutcomeUnknown)
	n.cfg.Limits.CommitTimeout = 5 * time.Second
	held := make(chan struct{})
	go func() { // a long settle holds the entity mutex
		ctx, cancel := context.WithTimeout(bg, 2*time.Second)
		defer cancel()
		close(held)
		n.Update(ctx, ref, req, put("b", "2"))
	}()
	<-held
	time.Sleep(20 * time.Millisecond)
	ctx, cancel := context.WithTimeout(bg, 100*time.Millisecond)
	defer cancel()
	start := time.Now()
	var wg sync.WaitGroup
	errs := make([]error, 2)
	for i := range errs {
		wg.Go(func() { errs[i] = n.Close(ctx) })
	}
	wg.Wait()
	if d := time.Since(start); d > time.Second {
		t.Fatalf("Close took %v", d)
	}
	wantCode(t, errs[0], CodeOutcomeUnknown)
	wantCode(t, errs[1], CodeOutcomeUnknown)
	_, err = n.Resolve(bg, "k")
	wantCode(t, err, CodeClosed)
	armed.Store(false)
}

// TestRecoveriesBound: at most Limits.Recoveries activations run at once.
func TestRecoveriesBound(t *testing.T) {
	mem := newStore(t)
	n := testNode(t, mem, "a", nil)
	var refs []Ref
	for i := range 6 {
		refs = append(refs, mustCreate(t, n, fmt.Sprint("k", i), nil))
	}
	var inflight, peak atomic.Int32
	mem.SetPlan(func(a storetest.Attempt) storetest.MemFault {
		if a.Op != "get" || !strings.HasSuffix(a.Key, ".ltx") {
			return storetest.MemFault{}
		}
		peak.Store(max(peak.Load(), inflight.Add(1)))
		hold := make(chan struct{})
		time.AfterFunc(50*time.Millisecond, func() { inflight.Add(-1); close(hold) })
		return storetest.MemFault{Hold: hold}
	})
	cold := testNode(t, mem, "a", func(l *Limits) { l.Recoveries = 2 })
	var wg sync.WaitGroup
	for _, ref := range refs {
		wg.Go(func() { readAll(t, cold, ref) })
	}
	wg.Wait()
	if p := peak.Load(); p < 1 || p > 2 {
		t.Fatalf("peak concurrent activations %d, want at most 2", p)
	}
}

// TestEntityMapBounded: operations on entities this node does not hold
// leave no in-memory entry behind (PLAN §11.2).
func TestEntityMapBounded(t *testing.T) {
	mem := newStore(t)
	owner := testNode(t, mem, "owner", nil)
	var refs []Ref
	for i := range 20 {
		refs = append(refs, mustCreate(t, owner, fmt.Sprint("k", i), nil))
	}
	n := testNode(t, mem, "a", nil)
	for i := range 1000 {
		_, err := n.Read(bg, refs[i%len(refs)], 1, func(*View) ([]byte, error) { return nil, nil })
		wantCode(t, err, CodeNotOwner)
	}
	_, err := n.Read(bg, Ref{Key: "missing"}, 1, func(*View) ([]byte, error) { return nil, nil })
	wantCode(t, err, CodeNotFound)
	entries := func() int {
		n.mu.Lock()
		defer n.mu.Unlock()
		return len(n.entities)
	}
	if got := entries(); got != 0 {
		t.Fatalf("%d entries after misrouted reads", got)
	}
	l := newLedger()
	ref := mustCreate(t, n, "mine", l)
	mustPut(t, n, ref, "a", "1", l)
	if got := entries(); got != 1 { // resident
		t.Fatalf("%d entries", got)
	}
	err = n.Evict(bg, ref)
	n.wg.Wait() // the maintenance step after the Update holds the entity too
	if err != nil || entries() != 0 {
		t.Fatalf("evict %v, %d entries", err, entries())
	}
	l.verify(t, mem, "a")
}

// TestBusyColdEntityNotResident: an entity busy without a cache file does
// not count against ResidentEntities.
func TestBusyColdEntityNotResident(t *testing.T) {
	mem := newStore(t)
	creator := testNode(t, mem, "a", nil)
	x, y := mustCreate(t, creator, "x", nil), mustCreate(t, creator, "y", nil)
	creator.Close(bg)
	n := testNode(t, mem, "a", func(l *Limits) { l.ResidentEntities = 1 })
	hold := make(chan struct{})
	var state atomic.Int32
	mem.SetPlan(func(a storetest.Attempt) storetest.MemFault {
		if a.Op == "get" && a.Key == head.HeadKey("x") && state.CompareAndSwap(0, 1) {
			return storetest.MemFault{Hold: hold}
		}
		return storetest.MemFault{}
	})
	done := make(chan struct{})
	go func() { defer close(done); readAll(t, n, x) }()
	for state.Load() != 1 {
		time.Sleep(time.Millisecond)
	}
	if st := n.Stats(); st.ResidentEntities != 0 {
		t.Errorf("busy cold entity counted resident: %+v", st)
	}
	readAll(t, n, y)
	close(hold)
	<-done
}

func TestMaintenance(t *testing.T) {
	mem := newStore(t)
	n := testNode(t, mem, "a", nil)
	l := newLedger()
	ref := mustCreate(t, n, "k", l)
	if _, err := n.Update(bg, ref, newReq(t, "fill", 1), func(tx *Tx) ([]byte, error) {
		for i := range 200 {
			if _, err := tx.Exec(bg, "INSERT INTO kv VALUES (?, ?)", fmt.Sprint("fill", i), randText(4000)); err != nil {
				return nil, err
			}
		}
		return nil, nil
	}); err != nil {
		t.Fatal(err)
	}
	compacted, snapshots := false, 0
	for i := range 60 {
		// rewrite 12 distinct fill rows: an uploaded delta of distinct pages
		res, err := n.Update(bg, ref, newReq(t, fmt.Sprint("rows", i), 1), func(tx *Tx) ([]byte, error) {
			for j := range 12 {
				k := fmt.Sprint("fill", (i*12+j)%200)
				v := randText(4000)
				if _, err := tx.Exec(bg, "UPDATE kv SET v = ? WHERE k = ?", v, k); err != nil {
					return nil, err
				}
				l.ack(ref, k, v) // acknowledged only if the update succeeds (checked below)
			}
			return nil, nil
		})
		if err != nil || res.Replayed {
			t.Fatal(err)
		}
		if i%5 == 0 {
			mustPut(t, n, ref, fmt.Sprint("small", i), "x", l) // inline tails ride along
		}
		n.wg.Wait()
		h, _ := headAt(t, mem, "k")
		if len(h.Deltas) > 2*fanout {
			t.Fatalf("%d deltas: compaction is not keeping up", len(h.Deltas))
		}
		for _, d := range h.Deltas {
			compacted = compacted || d.Level > 0
		}
		if len(h.Deltas) == 0 && h.Snapshot.BaseRevision > 0 {
			snapshots++
		}
	}
	if !compacted || snapshots == 0 {
		t.Fatalf("compacted %v, snapshots %d", compacted, snapshots)
	}
	if st := n.Stats(); st.Snapshots == 0 || st.SnapshotBytes < 200*4000/2 { // compressed random hex
		t.Fatalf("snapshot stats %+v", st)
	}
	l.verify(t, mem, "a")
}

func TestCollect(t *testing.T) {
	mem := newStore(t)
	n := testNode(t, mem, "a", nil)
	l := newLedger()
	ref := mustCreate(t, n, "k", l)
	for range 40 {
		mustPut(t, n, ref, "blob", randText(40<<10), l)
		n.wg.Wait()
	}
	h, _ := headAt(t, mem, "k")
	prefix := head.ObjectPrefix("k", h.Incarnation)
	inflight := head.ObjectKey("k", h.Incarnation, h.Revision, head.NewID(), head.Digest{1}) // an upload not yet published
	mem.PutImmutable(bg, inflight, []byte("x"))
	mem.PutImmutable(bg, prefix+"junk", []byte("x")) // unparseable: never deleted
	other := testNode(t, mem, "b", nil)              // any node may collect
	if err := other.Collect(bg, ref); err != nil {
		t.Fatal(err)
	}
	page, _, _ := mem.List(bg, prefix, "", 1000)
	want := map[string]bool{h.Snapshot.Key: true, inflight: true, prefix + "junk": true}
	for _, d := range h.Deltas {
		want[d.Key] = true
	}
	for _, o := range page {
		if !want[o.Key] {
			t.Fatalf("unreachable object survived: %s", o.Key)
		}
		delete(want, o.Key)
	}
	if len(want) != 0 {
		t.Fatalf("objects that must survive were deleted: %v", want)
	}
	if st := other.Stats(); st.Collections != 1 || st.CollectedObjects == 0 || st.CollectedBytes == 0 {
		t.Fatalf("collect stats %+v", st)
	}
	if st := n.Stats(); st.Compactions == 0 || st.Commits != 41 || st.CompactionInputBytes <= st.CompactionOutputBytes/2 ||
		st.CompactionOutputBytes == 0 || st.StagingBytes != 0 || st.OldestUnresolved != 0 {
		t.Fatalf("maintenance stats %+v", st)
	}
	err := other.Collect(bg, Ref{"k", [16]byte{9}})
	wantCode(t, err, CodeIncarnationMismatch)
	l.verify(t, mem, "a")
}

func TestExport(t *testing.T) {
	mem := newStore(t)
	n := testNode(t, mem, "a", nil)
	l := newLedger()
	ref := mustCreate(t, n, "k", l)
	mustPut(t, n, ref, "a", "1", l)
	mustPut(t, n, ref, "b", "2", l) // the export includes the inline tail
	var out bytes.Buffer
	v, err := n.Export(bg, ref, &out)
	if err != nil || v.Sequence != 3 || v.Incarnation != ref.Incarnation {
		t.Fatalf("export %+v %v", v, err)
	}
	tr := tar.NewReader(&out)
	var names []string
	var m manifest
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		body, _ := io.ReadAll(tr)
		names = append(names, hdr.Name)
		if hdr.Typeflag != tar.TypeReg || hdr.Mode != 0o600 || hdr.ModTime.Unix() != 0 {
			t.Fatalf("entry %+v", hdr)
		}
		switch hdr.Name {
		case "manifest.json":
			if err := json.Unmarshal(body, &m); err != nil {
				t.Fatal(err)
			}
		case "snapshot.ltx":
			if uint64(len(body)) != m.Bytes || sha256.Sum256(body) != m.SHA256 {
				t.Fatal("snapshot differs from its manifest")
			}
		}
	}
	h, _ := headAt(t, mem, "k")
	if strings.Join(names, ",") != "manifest.json,snapshot.ltx" || m.Sequence != 3 || m.DatabaseChecksum != h.DatabaseChecksum ||
		m.PageCount != h.PageCount || m.Key != "k" {
		t.Fatalf("archive %v manifest %+v", names, m)
	}
}

func TestImport(t *testing.T) {
	mem := newStore(t)
	n := testNode(t, mem, "a", nil)
	l := newLedger()
	src := mustCreate(t, n, "src", l)
	mustPut(t, n, src, "a", "1", l)
	mustPut(t, n, src, "b", "2", l)
	var archive bytes.Buffer
	if _, err := n.Export(bg, src, &archive); err != nil {
		t.Fatal(err)
	}
	other := newStore(t) // another namespace
	m := testNode(t, other, "z", nil)
	req := newReq(t, "import", 1)
	ref, res, err := m.Import(bg, "dst", req, bytes.NewReader(archive.Bytes()))
	if err != nil || res.Version.Sequence != 4 || ref.Incarnation == src.Incarnation {
		t.Fatalf("import %+v %+v %v", ref, res, err)
	}
	if got := readAll(t, m, ref); got["a"] != "1" || got["b"] != "2" {
		t.Fatalf("imported rows %v", got)
	}
	// the import itself is replayable
	again, err := m.Lookup(bg, ref, req.ID)
	if err != nil || !again.Replayed || again.Version.Sequence != 4 {
		t.Fatalf("import receipt %+v %v", again, err)
	}
	if _, res2, err := m.Import(bg, "dst", req, bytes.NewReader(archive.Bytes())); err != nil || !res2.Replayed {
		t.Fatalf("import replay %+v %v", res2, err)
	}
	mustPut(t, m, ref, "c", "3", l)
	// strictness: schema, trailing data, tampering
	_, _, err = m.Import(bg, "x", newReq(t, "s", 2), bytes.NewReader(archive.Bytes()))
	wantCode(t, err, CodeSchemaMismatch)
	_, _, err = m.Import(bg, "x", newReq(t, "t", 1), io.MultiReader(bytes.NewReader(archive.Bytes()), strings.NewReader("x")))
	wantCode(t, err, CodeCorrupt)
	bad := bytes.Clone(archive.Bytes())
	bad[1536+100] ^= 1 // inside snapshot.ltx data (after two 512-byte headers and the padded manifest)
	_, _, err = m.Import(bg, "x", newReq(t, "u", 1), bytes.NewReader(bad))
	wantCode(t, err, CodeCorrupt)
	if _, err := other.Get(bg, head.BirthKey("x"), "", 4<<10); store.OutcomeOf(err) != store.NotFound {
		t.Fatal("a rejected import wrote birth.json")
	}
}

func TestConcurrentUpdatesAndReads(t *testing.T) {
	mem := newStore(t)
	n := testNode(t, mem, "a", nil)
	l := newLedger()
	refs := []Ref{mustCreate(t, n, "p", l), mustCreate(t, n, "q", l)}
	var wg sync.WaitGroup
	for g := range 6 {
		wg.Go(func() {
			for i := range 15 {
				ref := refs[(g+i)%2]
				k, v := fmt.Sprint("g", g, "-", i), randText(16+(i%4)*12000)
				if _, err := n.Update(bg, ref, newReq(t, k+v, 1), put(k, v)); err != nil {
					t.Error(err)
					return
				}
				l.ack(ref, k, v)
				if _, err := n.Read(bg, ref, 1, func(*View) ([]byte, error) { return nil, nil }); err != nil {
					t.Error(err)
					return
				}
			}
		})
	}
	wg.Wait()
	n.wg.Wait()
	l.verify(t, mem, "a")
}

func TestCachePolicy(t *testing.T) {
	mem := newStore(t)
	n := testNode(t, mem, "a", func(l *Limits) { l.ResidentEntities = 2; l.IdleTimeout = 100 * time.Millisecond })
	l := newLedger()
	var refs []Ref
	for i := range 4 {
		refs = append(refs, mustCreate(t, n, fmt.Sprint("e", i), l))
		mustPut(t, n, refs[i], "a", fmt.Sprint(i), l)
		if r := n.Stats().ResidentEntities; r > 2 {
			t.Fatalf("%d resident entities, limit 2", r)
		}
	}
	for _, ref := range refs { // evicted entities rehydrate on access
		if got := readAll(t, n, ref); got["a"] == "" {
			t.Fatalf("%s lost its row", ref.Key)
		}
	}
	deadline := time.Now().Add(2 * time.Second)
	for n.Stats().ResidentEntities != 0 {
		if time.Now().After(deadline) {
			t.Fatal("idle caches were never evicted")
		}
		time.Sleep(20 * time.Millisecond)
	}
	mustPut(t, n, refs[0], "b", "after idle", l)
	l.verify(t, mem, "a")
}

func TestPendingPerEntity(t *testing.T) {
	mem := newStore(t)
	n := testNode(t, mem, "a", func(l *Limits) { l.PendingPerEntity = 1 })
	l := newLedger()
	ref := mustCreate(t, n, "k", l)
	inside, release := make(chan struct{}), make(chan struct{})
	go n.Update(bg, ref, newReq(t, "hold", 1), func(tx *Tx) ([]byte, error) {
		close(inside)
		<-release
		return nil, nil
	})
	<-inside
	queued := make(chan error, 1)
	go func() { _, err := n.Update(bg, ref, newReq(t, "queued", 1), put("q", "1")); queued <- err }()
	for n.Stats().QueuedRequests != 1 {
		time.Sleep(time.Millisecond)
	}
	_, err := n.Update(bg, ref, newReq(t, "over", 1), put("o", "1"))
	wantCode(t, err, CodeCapacity)
	close(release)
	if st := n.Stats(); st.InflightStorage < 0 || st.InflightStorage > 64 {
		t.Fatalf("inflight %d", st.InflightStorage)
	}
	if err := <-queued; err != nil {
		t.Fatal(err)
	}
}

func TestInflightStorageBound(t *testing.T) {
	mem := newStore(t)
	n := testNode(t, mem, "a", func(l *Limits) { l.InflightStorage = 1 })
	l := newLedger()
	refs := []Ref{mustCreate(t, n, "x", l), mustCreate(t, n, "y", l)}
	var wg sync.WaitGroup // one slot: concurrent uploads, activations and hedges must queue, never deadlock
	for i := range 8 {
		wg.Go(func() { mustPut(t, n, refs[i%2], fmt.Sprint("k", i), randText(40<<10), l) })
	}
	wg.Wait()
	n.wg.Wait()
	if st := n.Stats(); st.InflightStorage != 0 {
		t.Fatalf("%d storage slots leaked", st.InflightStorage)
	}
	l.verify(t, mem, "a")
}

func TestCacheBytes(t *testing.T) {
	mem := newStore(t)
	n := testNode(t, mem, "a", func(l *Limits) { l.Recoveries = 1 })
	l := newLedger()
	ref0 := mustCreate(t, n, "e0", l)
	mustPut(t, n, ref0, "a", randText(16<<10), l)
	size := n.Stats().CacheBytes
	if size <= 0 {
		t.Fatalf("a resident entity accounts %d cache bytes", size)
	}
	n.cfg.Limits.CacheBytes = n.buildBytes() + size/2 // one build, and no resident entity beside it
	ref1 := mustCreate(t, n, "e1", l)                 // genesis evicts e0
	mustPut(t, n, ref1, "a", randText(16<<10), l)
	if s := n.Stats(); s.ResidentEntities != 1 || s.CacheBytes > n.cfg.Limits.CacheBytes {
		t.Fatalf("stats %+v", s)
	}
	readAll(t, n, ref0) // activation evicts e1
	if s := n.Stats(); s.ResidentEntities != 1 {
		t.Fatalf("stats %+v", s)
	}
	started, release := make(chan struct{}), make(chan struct{})
	done := make(chan error)
	go func() {
		_, err := n.Update(bg, ref0, newReq(t, "pinned", 1), func(tx *Tx) ([]byte, error) {
			close(started)
			<-release
			return put("b", "pinned")(tx)
		})
		done <- err
	}()
	<-started // e0 is in use: its bytes are pinned
	_, err := n.Read(bg, ref1, 1, func(v *View) ([]byte, error) { return nil, nil })
	wantCode(t, err, CodeCapacity)
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	l.ack(ref0, "b", "pinned")
	if got := readAll(t, n, ref1); got["a"] == "" {
		t.Fatal("e1 lost its row")
	}
	l.verify(t, mem, "a")
}

// TestExportAfterActivation: Export of an entity the Node activated from the
// store (evicted, read, taken over, or after a restart) matches its head,
// with and without an inline tail, and the archive imports.
func TestExportAfterActivation(t *testing.T) {
	for _, writes := range []int{0, 2} { // 0: snapshot only; 2: deltas or an inline tail
		mem := newStore(t)
		a := testNode(t, mem, "a", nil)
		l := newLedger()
		ref := mustCreate(t, a, "k", l)
		for i := range writes {
			mustPut(t, a, ref, strconv.Itoa(i), "v", l)
		}
		export := func(step string, n *Node) {
			t.Helper()
			var out bytes.Buffer
			v, err := n.Export(bg, ref, &out)
			if err != nil || v.Sequence != uint64(1+writes) {
				t.Fatalf("writes=%d %s: export %+v %v", writes, step, v, err)
			}
			imp := testNode(t, newStore(t), "z", nil)
			dst, _, err := imp.Import(bg, "dst", newReq(t, "import", 1), &out)
			if err != nil {
				t.Fatalf("writes=%d %s: import %v", writes, step, err)
			}
			if got := readAll(t, imp, dst); len(got) != writes {
				t.Fatalf("writes=%d %s: imported rows %v", writes, step, got)
			}
		}
		if err := a.Evict(bg, ref); err != nil {
			t.Fatal(err)
		}
		export("evict", a)
		if err := a.Evict(bg, ref); err != nil {
			t.Fatal(err)
		}
		readAll(t, a, ref)
		export("evict-read", a)
		a.Close(bg)
		b := testNode(t, mem, "b", nil)
		route, err := b.Resolve(bg, "k")
		if err != nil {
			t.Fatal(err)
		}
		if _, err := b.Takeover(bg, route); err != nil {
			t.Fatal(err)
		}
		export("takeover", b)
		b.Close(bg)
		export("restart", testNode(t, mem, "b", nil)) // same NodeID: implicit takeover (D4)
	}
}

// TestSchemaMismatchReportsStored: a schema_mismatch Error carries the
// stored schema (the archive's for Import), so a rolling upgrade can choose
// between Migrate and deferring to a newer Node without guessing (D20).
func TestSchemaMismatchReportsStored(t *testing.T) {
	mem := newStore(t)
	n := testNode(t, mem, "a", nil)
	ref := mustCreate(t, n, "k", nil)
	if _, err := n.Migrate(bg, ref, newReq(t, "m", 1), 3, func(*Tx) error { return nil }); err != nil {
		t.Fatal(err)
	}
	_, err := n.Read(bg, ref, 1, func(*View) ([]byte, error) { return nil, nil })
	if e := wantCode(t, err, CodeSchemaMismatch); e.Schema != 3 {
		t.Fatalf("read: stored schema %d", e.Schema)
	}
	_, err = n.Update(bg, ref, newReq(t, "u", 4), put("a", "1"))
	if e := wantCode(t, err, CodeSchemaMismatch); e.Schema != 3 {
		t.Fatalf("update: stored schema %d", e.Schema)
	}
	var archive bytes.Buffer
	if _, err := n.Export(bg, ref, &archive); err != nil {
		t.Fatal(err)
	}
	_, _, err = n.Import(bg, "dst", newReq(t, "i", 1), &archive)
	if e := wantCode(t, err, CodeSchemaMismatch); e.Schema != 3 {
		t.Fatalf("import: archive schema %d", e.Schema)
	}
}
