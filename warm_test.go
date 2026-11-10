package garden

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/OrdalieTech/big-garden/internal/head"
	"github.com/OrdalieTech/big-garden/internal/store"
	"github.com/OrdalieTech/big-garden/internal/storetest"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// warmNode opens a node on mem with its cache in dir, so a later node can
// find the warm tier an earlier one left there (D21).
func warmNode(t *testing.T, mem *storetest.Mem, id, dir string, mod func(*Limits)) *Node {
	t.Helper()
	cfg := Config{S3: s3.New(s3.Options{}), Bucket: "b", Prefix: "p", NodeID: id, CacheDir: dir,
		Limits: Limits{EntityBytes: 8 << 20, StorageAttemptTimeout: 200 * time.Millisecond, CommitTimeout: time.Second}}
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

// objectGets counts the GETs of objects (not heads) in mem's journal after seq.
func objectGets(mem *storetest.Mem, seq int) (n int) {
	for _, a := range mem.Journal() {
		if a.Seq > seq && a.Op == "get" && strings.Contains(a.Key, "/objects/") {
			n++
		}
	}
	return n
}

func random(n int) []byte {
	b := make([]byte, n)
	rand.Read(b)
	return b
}

func lastSeq(mem *storetest.Mem) int {
	j := mem.Journal()
	return j[len(j)-1].Seq
}

func writeN(t *testing.T, n *Node, ref Ref, from, count int) {
	t.Helper()
	for i := from; i < from+count; i++ {
		if _, err := n.Update(bg, ref, newReq(t, fmt.Sprint("w", i), 1), put(fmt.Sprint("k", i), strings.Repeat("v", 3000))); err != nil {
			t.Fatal(err)
		}
	}
}

func wantRows(t *testing.T, n *Node, ref Ref, count int) {
	t.Helper()
	got := readAll(t, n, ref)
	for i := range count {
		if got[fmt.Sprint("k", i)] == "" {
			t.Fatalf("row k%d missing after activation (%d rows)", i, len(got))
		}
	}
}

// takeOver moves ref to n.
func takeOver(t *testing.T, n *Node, key string) {
	t.Helper()
	route, err := n.Resolve(bg, key)
	if err == nil {
		_, err = n.Takeover(bg, route)
	}
	if err != nil {
		t.Fatal(err)
	}
}

func TestWarmTier(t *testing.T) {
	mem := storetest.NewMem()
	dir := t.TempDir()
	a := warmNode(t, mem, "a", dir, nil)
	ref, _, err := a.Create(bg, "k", newReq(t, "c", 1), createKV)
	if err != nil {
		t.Fatal(err)
	}
	writeN(t, a, ref, 0, 40) // past the inline tail: uploaded deltas
	a.Close(bg)

	t.Run("hit after restart", func(t *testing.T) {
		a2 := warmNode(t, mem, "a", dir, nil)
		if s := a2.Stats(); s.WarmEntities != 1 || s.WarmBytes == 0 {
			t.Fatalf("restart lost the warm file: %+v", s)
		}
		takeOver(t, a2, "k") // a's session is gone: a2 is another owner
		seq := lastSeq(mem)
		wantRows(t, a2, ref, 40)
		if s := a2.Stats(); s.WarmHits+s.CatchUps != 1 || objectGets(mem, seq) != 0 {
			t.Fatalf("not a warm activation: hits %d, catch-ups %d, object GETs %d", s.WarmHits, s.CatchUps, objectGets(mem, seq))
		}
		a2.Close(bg)
	})

	t.Run("catch-up after writes elsewhere", func(t *testing.T) {
		b := warmNode(t, mem, "b", t.TempDir(), nil)
		takeOver(t, b, "k")
		writeN(t, b, ref, 40, 12)
		h := b.Stats()
		b.Close(bg)
		a3 := warmNode(t, mem, "a", dir, nil)
		takeOver(t, a3, "k")
		seq := lastSeq(mem)
		wantRows(t, a3, ref, 52)
		s := a3.Stats()
		if s.CatchUps != 1 || s.WarmHits != 0 {
			t.Fatalf("want one catch-up: %+v (writer %+v)", s, h)
		}
		cur, _ := mem.Peek(head.HeadKey("k"))
		hd, _ := head.Decode(cur.Body)
		if gets := objectGets(mem, seq); gets > len(hd.Deltas) {
			t.Fatalf("catch-up fetched %d objects, the head has %d deltas", gets, len(hd.Deltas))
		}
		a3.Close(bg)
	})

	t.Run("cold when the file is corrupt", func(t *testing.T) {
		files, _ := filepath.Glob(filepath.Join(dir, "warm", "*", "*.db"))
		if len(files) != 1 {
			t.Fatalf("warm files: %v", files)
		}
		f, err := os.OpenFile(files[0], os.O_RDWR, 0)
		if err != nil {
			t.Fatal(err)
		}
		f.WriteAt([]byte{0xff, 0x00, 0xff}, 3*4096+100)
		f.Close()
		a4 := warmNode(t, mem, "a", dir, nil)
		takeOver(t, a4, "k")
		wantRows(t, a4, ref, 52)
		if s := a4.Stats(); s.WarmHits+s.CatchUps != 0 || s.Activations != 1 {
			t.Fatalf("a corrupt warm file must activate cold: %+v", s)
		}
		a4.Close(bg)
	})

	t.Run("cold after a snapshot elsewhere", func(t *testing.T) {
		b := warmNode(t, mem, "b", t.TempDir(), nil)
		takeOver(t, b, "k")
		b.skipMaintenance = true // uploaded deltas pile up to MaxDeltas: the next write snapshots
		for i := range head.MaxDeltas + 4 {
			if _, err := b.Update(bg, ref, newReq(t, fmt.Sprint("big", i), 1), put("big", hex.EncodeToString(random(40<<10)))); err != nil {
				t.Fatal(err)
			}
		}
		b.Close(bg)
		cur, _ := mem.Peek(head.HeadKey("k"))
		if hd, _ := head.Decode(cur.Body); hd.Snapshot.MaxSequence <= 53 {
			t.Fatalf("no snapshot past the parked image: %+v", hd.Snapshot)
		}
		a5 := warmNode(t, mem, "a", dir, nil)
		takeOver(t, a5, "k")
		wantRows(t, a5, ref, 52)
		if s := a5.Stats(); s.WarmHits+s.CatchUps != 0 || s.Activations != 1 {
			t.Fatalf("want a cold activation: %+v", s)
		}
		a5.Close(bg)
	})

	t.Run("evict forces cold", func(t *testing.T) {
		a6 := warmNode(t, mem, "a", dir, nil)
		takeOver(t, a6, "k")
		readAll(t, a6, ref)
		if err := a6.Evict(bg, ref); err != nil {
			t.Fatal(err)
		}
		readAll(t, a6, ref)
		if s := a6.Stats(); s.Activations != 2 || s.WarmHits+s.CatchUps != 1 {
			t.Fatalf("Evict must leave nothing warm: %+v", s)
		}
	})
}

// TestWarmBudget: warm files count in CacheBytes and the least recently used
// go first.
func TestWarmBudget(t *testing.T) {
	mem := storetest.NewMem()
	n := warmNode(t, mem, "a", t.TempDir(), func(l *Limits) {
		l.EntityBytes, l.CacheBytes, l.ResidentEntities, l.Recoveries = 1<<20, 6<<20, 2, 1
	})
	var refs []Ref
	for i := range 20 {
		ref, _, err := n.Create(bg, fmt.Sprint("e", i), newReq(t, fmt.Sprint("c", i), 1), createKV)
		if err != nil {
			t.Fatal(err)
		}
		writeN(t, n, ref, 0, 20)
		refs = append(refs, ref)
	}
	s := n.Stats()
	if s.WarmEntities == 0 || s.WarmEvictions == 0 || s.CacheBytes > 6<<20 {
		t.Fatalf("want parked and evicted entities within the budget: %+v", s)
	}
	var lru Ref // the least recently used warm entity: fit must not evict its own file to activate it
	n.wmu.Lock()
	for _, ref := range refs {
		w, ok := n.warm[filepath.Base(head.EntityDir(ref.Key))]
		if old, had := n.warm[filepath.Base(head.EntityDir(lru.Key))]; ok && (!had || w.used.Before(old.used)) {
			lru = ref
		}
	}
	n.wmu.Unlock()
	wantRows(t, n, lru, 20)
	if s := n.Stats(); s.WarmHits != 1 {
		t.Fatalf("the least recently used warm entity did not activate warm: %+v", s)
	}
	for _, ref := range refs {
		wantRows(t, n, ref, 20)
	}
}

// TestWarmActivationCost (GARDEN_MEASURE=1, best with GARDEN_TEST_S3=1)
// logs median activation latency and object bytes of a warm hit, a catch-up
// after three updates elsewhere, and a cold restore, at 1 and 16 MiB (D21).
func TestWarmActivationCost(t *testing.T) {
	if os.Getenv("GARDEN_MEASURE") == "" {
		t.Skip("GARDEN_MEASURE is not set")
	}
	mem := newStore(t)
	a := testNode(t, mem, "a", func(l *Limits) {
		l.EntityBytes, l.StorageAttemptTimeout, l.CommitTimeout = 64<<20, 10*time.Second, 30*time.Second
	})
	b := testNode(t, mem, "b", func(l *Limits) {
		l.EntityBytes, l.StorageAttemptTimeout, l.CommitTimeout = 64<<20, 10*time.Second, 30*time.Second
	})
	for _, size := range []int{1 << 20, 16 << 20} {
		key := fmt.Sprint("m", size>>20)
		ref, _, err := a.Create(bg, key, newReq(t, key, 1), createKV)
		if err != nil {
			t.Fatal(err)
		}
		for i := range size / (256 << 10) {
			if _, err := a.Update(bg, ref, newReq(t, fmt.Sprint(key, i), 1), put(fmt.Sprint("f", i), hex.EncodeToString(random(128<<10)))); err != nil {
				t.Fatal(err)
			}
		}
		park := func(n *Node) {
			e, err := n.enter(bg, key, nil, nil)
			if err != nil {
				t.Fatal(err)
			}
			n.park(e)
			n.leave(e)
		}
		median := func(name string, prep func(), op func()) {
			var ds []time.Duration
			s0 := a.Stats()
			for range 7 {
				prep()
				t0 := time.Now()
				op()
				ds = append(ds, time.Since(t0))
			}
			slices.Sort(ds)
			s1 := a.Stats()
			t.Logf("%2d MiB %-9s p50 %8v  max %8v  object bytes/activation %d  (hits %d, catch-ups %d)", size>>20, name, ds[3].Round(100*time.Microsecond),
				ds[6].Round(100*time.Microsecond), (s1.ActivationBytes-s0.ActivationBytes)/7, s1.WarmHits-s0.WarmHits, s1.CatchUps-s0.CatchUps)
		}
		readA := func() { readAll(t, a, ref) }
		median("warm hit", func() { park(a) }, readA)
		i := 0
		median("catch-up", func() {
			park(a)
			takeOver(t, b, key)
			for range 3 {
				i++
				if _, err := b.Update(bg, ref, newReq(t, fmt.Sprint(key, "b", i), 1), put(fmt.Sprint("b", i%5), "x")); err != nil {
					t.Fatal(err)
				}
			}
		}, func() { takeOver(t, a, key); readA() })
		median("cold", func() { a.Evict(bg, ref); takeOver(t, b, key) }, func() { takeOver(t, a, key); readA() })
	}
}

// TestWarmKeepsFileOnStoreError: a store failure while fetching the links of
// a catch-up puts the warm file back; the next activation catches up (D21).
func TestWarmKeepsFileOnStoreError(t *testing.T) {
	mem := storetest.NewMem()
	dir := t.TempDir()
	a := warmNode(t, mem, "a", dir, nil)
	ref, _, err := a.Create(bg, "k", newReq(t, "c", 1), createKV)
	if err != nil {
		t.Fatal(err)
	}
	writeN(t, a, ref, 0, 12)
	a.Close(bg)
	b := warmNode(t, mem, "b", t.TempDir(), nil)
	takeOver(t, b, "k")
	for i := range 2 { // uploaded deltas after a's file: the catch-up must GET them
		if _, err := b.Update(bg, ref, newReq(t, fmt.Sprint("big", i), 1), put(fmt.Sprint("big", i), hex.EncodeToString(random(40<<10)))); err != nil {
			t.Fatal(err)
		}
	}
	b.Close(bg)
	var failing atomic.Bool
	failing.Store(true)
	mem.SetPlan(func(at storetest.Attempt) storetest.MemFault {
		if failing.Load() && at.Op == "get" && strings.Contains(at.Key, "/objects/") {
			return storetest.MemFault{Reject: store.Unknown}
		}
		return storetest.MemFault{}
	})
	a2 := warmNode(t, mem, "a", dir, func(l *Limits) { l.StorageAttemptTimeout = 50 * time.Millisecond })
	route, err := a2.Resolve(bg, "k")
	if err == nil {
		_, err = a2.Takeover(bg, route)
	}
	if err == nil {
		t.Fatal("activation succeeded while object GETs fail")
	}
	if s := a2.Stats(); s.WarmEntities != 1 {
		t.Fatalf("a store error deleted the warm file: %+v", s)
	}
	failing.Store(false)
	takeOver(t, a2, "k")
	readAll(t, a2, ref)
	if s := a2.Stats(); s.CatchUps != 1 {
		t.Fatalf("want a catch-up after the store recovered: %+v", s)
	}
}
