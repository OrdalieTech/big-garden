package garden

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/OrdalieTech/big-garden/internal/head"
	"github.com/OrdalieTech/big-garden/internal/store"
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
					if !retryable(err) || time.Now().After(deadline) {
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

// TestReadStaleConcurrentAndCatchUp (D40): 8 concurrent ReadStale callers on a
// follower read without waiting on S3 while fresh, never observe failed or
// mixed reads across owner updates and snapshots, catch up in place by
// downloading only changed LTX deltas, and recover cleanly after cache loss.
func TestReadStaleConcurrentAndCatchUp(t *testing.T) {
	st := newStore(t)
	owner := testNode(t, st, "owner", nil)
	dir := t.TempDir()
	follower := warmNode(t, st, "follower", dir, func(l *Limits) {
		l.SQLiteCacheBytes = 4 << 20
		l.MmapBytes = 16 << 20
	})
	ref, _, err := owner.Create(bg, "big", newReq(t, "c0", 1), func(tx *Tx) ([]byte, error) {
		if _, err := tx.Exec(bg, "CREATE TABLE kv(k TEXT PRIMARY KEY, v TEXT, gen INTEGER NOT NULL)"); err != nil {
			return nil, err
		}
		stmt, err := tx.Prepare(bg, "INSERT INTO kv(k, v, gen) VALUES (?, ?, 0)")
		if err != nil {
			return nil, err
		}
		for i := range 400 {
			if _, err := stmt.Exec(bg, fmt.Sprintf("k%04d", i), randText(512)); err != nil {
				return nil, err
			}
		}
		return nil, stmt.Close()
	})
	if err != nil {
		t.Fatal(err)
	}
	readConsistent := func(n *Node, maxAge time.Duration) (int, uint64, error) {
		var minGen, maxGen int
		res, err := n.ReadStale(bg, ref, 1, maxAge, func(v *View) ([]byte, error) {
			return nil, v.Query(bg, "SELECT MIN(gen), MAX(gen) FROM kv WHERE k < 'k0020'", nil, func(r *sql.Rows) error {
				if r.Next() {
					return r.Scan(&minGen, &maxGen)
				}
				return r.Err()
			})
		})
		if err != nil {
			return 0, 0, err
		}
		if minGen != maxGen {
			return 0, res.Version.Sequence, fmt.Errorf("mixed read: minGen=%d maxGen=%d at seq %d", minGen, maxGen, res.Version.Sequence)
		}
		return minGen, res.Version.Sequence, nil
	}
	// Cold activation on follower via ReadStale returns CodeUnavailable immediately
	// while activating in the background.
	waitReady := func(n *Node, maxAge time.Duration) (int, uint64) {
		t.Helper()
		deadline := time.Now().Add(5 * time.Second)
		for {
			g, seq, err := readConsistent(n, maxAge)
			if err == nil {
				return g, seq
			}
			if _, ok := hasCode(err, CodeUnavailable); !ok {
				t.Fatalf("unexpected ReadStale error: %v", err)
			}
			if time.Now().After(deadline) {
				t.Fatal("timed out waiting for background activation")
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
	gen, seq0 := waitReady(follower, 200*time.Millisecond)
	if gen != 0 {
		t.Fatalf("initial ReadStale: gen=%d seq=%d", gen, seq0)
	}
	// Fresh reads make zero S3 requests.
	from := len(st.Journal())
	for range 16 {
		if g, _, err := readConsistent(follower, time.Hour); err != nil || g != 0 {
			t.Fatalf("fresh ReadStale: gen=%d err=%v", g, err)
		}
	}
	if gets := headGets(st, "big", from); gets != 0 {
		t.Fatalf("fresh ReadStale made %d head GETs, want 0", gets)
	}
	// 8 concurrent readers while owner performs 12 updates (including a snapshot).
	stop := make(chan struct{})
	var readCount atomic.Int64
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			var lastSeq uint64
			for {
				select {
				case <-stop:
					return
				default:
				}
				_, seq, err := readConsistent(follower, 40*time.Millisecond)
				if err != nil {
					t.Errorf("concurrent ReadStale: %v", err)
					return
				}
				if seq < lastSeq {
					t.Errorf("ReadStale sequence went backwards: %d < %d", seq, lastSeq)
					return
				}
				lastSeq = seq
				readCount.Add(1)
			}
		})
	}
	actBytesBefore := follower.Stats().ActivationBytes
	for g := 1; g <= 12; g++ {
		if _, err := owner.Update(bg, ref, newReq(t, fmt.Sprintf("u%d", g), 1), func(tx *Tx) ([]byte, error) {
			_, err := tx.Exec(bg, "UPDATE kv SET gen = ? WHERE k < 'k0020'", g)
			return nil, err
		}); err != nil {
			t.Fatal(err)
		}
		if g == 6 {
			if _, err := owner.Snapshot(bg, ref); err != nil {
				t.Fatal(err)
			}
		}
		time.Sleep(15 * time.Millisecond)
	}
	// Wait for follower to observe gen=12 within bounded staleness.
	deadline := time.Now().Add(5 * time.Second)
	for {
		g, _, err := readConsistent(follower, 40*time.Millisecond)
		if err != nil {
			t.Fatal(err)
		}
		if g == 12 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("follower stayed at gen=%d, want 12", g)
		}
		time.Sleep(10 * time.Millisecond)
	}
	close(stop)
	wg.Wait()
	if readCount.Load() == 0 {
		t.Fatal("no concurrent reads completed")
	}
	// In-place catch-up downloaded deltas, far less than 12 full snapshots.
	h, _ := headAt(t, st, "big")
	fullSize := uint64(h.PageCount) * head.PageSize
	deltaBytes := follower.Stats().ActivationBytes - actBytesBefore
	if deltaBytes >= 3*fullSize {
		t.Fatalf("catch-up downloaded %d bytes across 12 small updates (db size %d)", deltaBytes, fullSize)
	}
	if follower.Stats().CatchUps == 0 {
		t.Fatalf("want CatchUps > 0, got %+v", follower.Stats())
	}
	// Reopen on empty cache dir: follower recovers from S3 on its own.
	if err := follower.Close(bg); err != nil {
		t.Fatal(err)
	}
	fresh := warmNode(t, st, "follower2", t.TempDir(), nil)
	if g, _ := waitReady(fresh, time.Second); g != 12 {
		t.Fatalf("fresh follower after cache wipe: gen=%d", g)
	}
}

func TestLocalDir(t *testing.T) {
	storeDir := t.TempDir()
	openLocal := func(id string) *Node {
		n, err := Open(bg, Config{
			LocalDir: storeDir,
			Prefix:   "stores",
			NodeID:   id,
			CacheDir: t.TempDir(),
			Limits:   Limits{EntityBytes: 1 << 20, CacheBytes: 4 << 20, Recoveries: 1},
		})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { n.Close(bg) })
		return n
	}
	w := openLocal("writer")
	r := openLocal("reader")
	if _, err := w.Write(bg, "k1", 1, func(tx *Tx) error {
		if _, err := tx.Exec(bg, "CREATE TABLE t(k TEXT PRIMARY KEY, v INTEGER)"); err != nil {
			return err
		}
		if _, err := tx.Exec(bg, "INSERT INTO t VALUES('a', 42)"); err != nil {
			return err
		}
		var v int
		if err := tx.QueryRow(bg, "SELECT v FROM t WHERE k=?", []any{"a"}, &v); err != nil || v != 42 {
			t.Fatalf("Tx.QueryRow = %d, %v", v, err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	var got int
	for deadline := time.Now().Add(5 * time.Second); ; time.Sleep(10 * time.Millisecond) {
		_, err := r.ReadKey(bg, "k1", 1, time.Second, func(v *View) error {
			return v.QueryRow(bg, "SELECT v FROM t WHERE k=?", []any{"a"}, &got)
		})
		if err == nil {
			break
		}
		if _, ok := hasCode(err, CodeUnavailable); !ok || time.Now().After(deadline) {
			t.Fatalf("ReadKey = %d, %v", got, err)
		}
	}
	if got != 42 {
		t.Fatalf("ReadKey = %d, want 42", got)
	}
	// Another writer takes over via Write, then removes via Remove.
	w2 := openLocal("writer2")
	if _, err := w2.Write(bg, "k1", 1, func(tx *Tx) error {
		_, err := tx.Exec(bg, "UPDATE t SET v=99 WHERE k='a'")
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := r.ReadKey(bg, "k1", 1, 0, func(v *View) error {
		return v.QueryRow(bg, "SELECT v FROM t WHERE k=?", []any{"a"}, &got)
	}); err != nil || got != 99 {
		t.Fatalf("ReadKey after takeover = %d, %v", got, err)
	}
	if err := w2.Remove(bg, "k1"); err != nil {
		t.Fatal(err)
	}
	if _, err := r.ReadKey(bg, "k1", 1, 0, func(v *View) error { return nil }); func() bool { _, ok := hasCode(err, CodeNotFound); return !ok }() {
		t.Fatalf("ReadKey after Remove: %v", err)
	}
}

type throttledStore struct {
	store.Store
	delay     atomic.Int64 // nanoseconds per Read on .ltx objects
	inFlight  atomic.Int64
	maxActive atomic.Int64
}

func (s *throttledStore) Open(ctx context.Context, key string, off, n int64) (io.ReadCloser, error) {
	rc, err := s.Store.Open(ctx, key, off, n)
	if err != nil || !strings.HasSuffix(key, ".ltx") {
		return rc, err
	}
	cur := s.inFlight.Add(1)
	for {
		old := s.maxActive.Load()
		if cur <= old || s.maxActive.CompareAndSwap(old, cur) {
			break
		}
	}
	return &throttledReader{rc: rc, ctx: ctx, store: s}, nil
}

type throttledReader struct {
	rc     io.ReadCloser
	ctx    context.Context
	store  *throttledStore
	closed atomic.Bool
}

func (r *throttledReader) Read(p []byte) (int, error) {
	if d := time.Duration(r.store.delay.Load()); d > 0 {
		t := time.NewTimer(d)
		select {
		case <-r.ctx.Done():
			t.Stop()
			return 0, r.ctx.Err()
		case <-t.C:
		}
	}
	if len(p) > 4096 {
		p = p[:4096]
	}
	return r.rc.Read(p)
}

func (r *throttledReader) Close() error {
	if r.closed.CompareAndSwap(false, true) {
		r.store.inFlight.Add(-1)
	}
	return r.rc.Close()
}

// TestReadStaleSlowRepublishAndColdActivation reproduces the production bug
// where a republished snapshot or newly added entity takes longer to download
// than the per-request deadline: reads on an already-local entity must never
// fail or wait (serving stale data until background catch-up completes),
// requests for a not-yet-local entity must return immediately while activating
// in the background, and downloads must run one at a time when Recoveries=1.
func TestReadStaleSlowRepublishAndColdActivation(t *testing.T) {
	base := newStore(t)
	ts := &throttledStore{Store: base}
	owner := testNode(t, base, "owner", nil)
	follower := warmNode(t, ts, "follower", t.TempDir(), func(l *Limits) {
		l.Recoveries = 1
	})

	// Seed company-fr at gen 1 and warm it on the follower.
	ref, _, err := owner.Create(bg, "index/company-fr", newReq(t, "c0", 1), func(tx *Tx) ([]byte, error) {
		if _, err := tx.Exec(bg, "CREATE TABLE kv(k TEXT PRIMARY KEY, v TEXT, gen INTEGER NOT NULL)"); err != nil {
			return nil, err
		}
		for i := range 200 {
			if _, err := tx.Exec(bg, "INSERT INTO kv VALUES(?, ?, 1)", fmt.Sprintf("k%04d", i), randText(512)); err != nil {
				return nil, err
			}
		}
		return nil, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := follower.Read(bg, ref, 1, func(v *View) ([]byte, error) { return nil, nil }); err != nil {
		t.Fatal(err)
	}

	// Builder republishes a new snapshot of index/company-fr (gen 2) and a brand-new entity sources/fr.land.
	if _, err := owner.Update(bg, ref, newReq(t, "u2", 1), func(tx *Tx) ([]byte, error) {
		for i := range 200 {
			if _, err := tx.Exec(bg, "UPDATE kv SET v=?, gen=2 WHERE k=?", randText(512), fmt.Sprintf("k%04d", i)); err != nil {
				return nil, err
			}
		}
		return nil, nil
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := owner.Snapshot(bg, ref); err != nil {
		t.Fatal(err)
	}
	landRef, _, err := owner.Create(bg, "sources/fr.land", newReq(t, "land0", 1), func(tx *Tx) ([]byte, error) {
		if _, err := tx.Exec(bg, "CREATE TABLE land(k TEXT PRIMARY KEY, v TEXT)"); err != nil {
			return nil, err
		}
		for i := range 200 {
			if _, err := tx.Exec(bg, "INSERT INTO land VALUES(?, ?)", fmt.Sprintf("l%04d", i), randText(512)); err != nil {
				return nil, err
			}
		}
		return nil, nil
	})
	if err != nil {
		t.Fatal(err)
	}

	// Throttle .ltx reads so a snapshot download takes ~450ms, well beyond the 80ms request deadline.
	ts.delay.Store(int64(15 * time.Millisecond))
	const (
		maxAge     = 30 * time.Millisecond
		reqTimeout = 80 * time.Millisecond
	)
	// Let validTime age past maxAge before requests arrive (the quiet-gap trigger).
	time.Sleep(maxAge + 20*time.Millisecond)

	// Concurrently serve requests with an 80ms deadline while both downloads run.
	var failedReads, servedReads, sawStale atomic.Int64
	var sawGen2 atomic.Bool
	stop := make(chan struct{})
	var wg sync.WaitGroup
	for range 4 {
		wg.Go(func() {
			for {
				select {
				case <-stop:
					return
				default:
				}
				rctx, cancel := context.WithTimeout(bg, reqTimeout)
				var gen int
				res, err := follower.ReadStale(rctx, ref, 1, maxAge, func(v *View) ([]byte, error) {
					return nil, v.QueryRow(rctx, "SELECT MIN(gen) FROM kv", nil, &gen)
				})
				cancel()
				if err != nil {
					failedReads.Add(1)
					t.Errorf("ReadStale failed during slow republish: %v", err)
					return
				}
				servedReads.Add(1)
				if res.Stale > maxAge {
					sawStale.Add(1)
				}
				if gen == 2 {
					sawGen2.Store(true)
				}
				time.Sleep(5 * time.Millisecond)
			}
		})
	}

	// A request for not-yet-local sources/fr.land must answer immediately (< 40ms), not block until reqTimeout.
	lctx, lcancel := context.WithTimeout(bg, reqTimeout)
	t0 := time.Now()
	_, lerr := follower.ReadStale(lctx, landRef, 1, maxAge, func(v *View) ([]byte, error) { return nil, nil })
	lcancel()
	if elapsed := time.Since(t0); elapsed >= reqTimeout/2 {
		t.Errorf("cold ReadStale waited %v (want immediate error)", elapsed)
	}
	var ge *Error
	if !errors.As(lerr, &ge) || ge.Code != CodeUnavailable {
		t.Errorf("cold ReadStale err = %v, want CodeUnavailable", lerr)
	}

	// Wait for both background downloads to finish on their own (no request context cancellation).
	deadline := time.Now().Add(15 * time.Second)
	for !sawGen2.Load() {
		if time.Now().After(deadline) {
			close(stop)
			wg.Wait()
			t.Fatalf("follower never caught up to gen 2 (served=%d failed=%d)", servedReads.Load(), failedReads.Load())
		}
		time.Sleep(25 * time.Millisecond)
	}
	for {
		rctx, cancel := context.WithTimeout(bg, reqTimeout)
		_, err := follower.ReadStale(rctx, landRef, 1, maxAge, func(v *View) ([]byte, error) { return nil, nil })
		cancel()
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			close(stop)
			wg.Wait()
			t.Fatalf("sources/fr.land never activated in background: %v", err)
		}
		time.Sleep(25 * time.Millisecond)
	}

	close(stop)
	wg.Wait()

	if failedReads.Load() != 0 {
		t.Fatalf("failedReads = %d, want 0", failedReads.Load())
	}
	if servedReads.Load() == 0 {
		t.Fatal("expected reads to be served during slow republish")
	}
	if sawStale.Load() == 0 {
		t.Fatal("expected ReadStale to report Stale > maxAge while background download was running")
	}
	if max := ts.maxActive.Load(); max > 1 {
		t.Fatalf("concurrent .ltx downloads = %d, want 1 (Recoveries=1)", max)
	}
}
