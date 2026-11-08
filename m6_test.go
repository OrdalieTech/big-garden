package garden

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/OrdalieTech/big-garden/internal/head"
	"github.com/OrdalieTech/big-garden/internal/store"
	"github.com/OrdalieTech/big-garden/internal/storetest"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// pauseStore calls hook before and after each store call of one Node: a
// process paused (SIGSTOP) around a PUT, CAS or GET. A call that was paused
// before it was sent goes out with its deadline removed, like a request a
// resumed process sends late.
type pauseStore struct {
	store.Store
	mu   sync.Mutex
	hook func(op, key, phase string) bool // true: the call was paused
}

func (p *pauseStore) at(op, key, phase string) bool {
	p.mu.Lock()
	h := p.hook
	p.mu.Unlock()
	return h != nil && h(op, key, phase)
}

func (p *pauseStore) setHook(h func(op, key, phase string) bool) {
	p.mu.Lock()
	p.hook = h
	p.mu.Unlock()
}

func late(ctx context.Context, paused bool) context.Context {
	if paused {
		return context.WithoutCancel(ctx)
	}
	return ctx
}

func (p *pauseStore) Get(ctx context.Context, key, inm string, max int64) (store.Object, error) {
	o, err := p.Store.Get(late(ctx, p.at("get", key, "before")), key, inm, max)
	p.at("get", key, "after")
	return o, err
}

func (p *pauseStore) PutImmutable(ctx context.Context, key string, body []byte) error {
	err := p.Store.PutImmutable(late(ctx, p.at("put", key, "before")), key, body)
	p.at("put", key, "after")
	return err
}

func (p *pauseStore) PutCAS(ctx context.Context, key string, body []byte, ifMatch string) (string, error) {
	etag, err := p.Store.PutCAS(late(ctx, p.at("cas", key, "before")), key, body, ifMatch)
	p.at("cas", key, "after")
	return etag, err
}

// pauseOnce arms ps to pause the first call matching op/phase (and key
// filter) until release is closed; paused receives the matched key.
func pauseOnce(ps *pauseStore, op, phase string, match func(key string) bool) (paused chan string, release chan struct{}) {
	paused, release = make(chan string, 1), make(chan struct{})
	var once sync.Once
	ps.setHook(func(o, key, ph string) bool {
		if o != op || ph != phase || (match != nil && !match(key)) {
			return false
		}
		hit := false
		once.Do(func() { hit = true })
		if hit {
			paused <- key
			<-release
		}
		return hit
	})
	return paused, release
}

func waitPaused(t *testing.T, paused chan string) string {
	t.Helper()
	select {
	case k := <-paused:
		return k
	case <-time.After(10 * time.Second):
		t.Fatal("the paused call never came")
		return ""
	}
}

func refKeys(h *head.Head) []string {
	keys := []string{h.Snapshot.Key}
	for _, d := range h.Deltas {
		keys = append(keys, d.Key)
	}
	return keys
}

// fillMaintenance writes on ref with maintenance off until the head needs a
// snapshot (replay > page_count+256) or a compaction (8 level-0 deltas).
func fillMaintenance(t *testing.T, n *Node, ref Ref, snapshot bool, l *ledger) {
	t.Helper()
	n.mu.Lock()
	n.skipMaintenance = true
	n.mu.Unlock()
	if snapshot {
		if _, err := n.Update(bg, ref, newReq(t, "fill", 1), func(tx *Tx) ([]byte, error) {
			for i := range 100 {
				if _, err := tx.Exec(bg, "INSERT OR REPLACE INTO kv VALUES (?, ?)", fmt.Sprint("fill", i), randText(4000)); err != nil {
					return nil, err
				}
			}
			return nil, nil
		}); err != nil {
			t.Fatal(err)
		}
		for i := 0; ; i++ {
			h, _ := headAt(t, n.st.(limited).Store, ref.Key)
			e := n.entities[ref.Key]
			if e.replay() > uint64(h.PageCount)+256 {
				return
			}
			mustPut(t, n, ref, fmt.Sprint("fill", i%100), randText(40<<10), l)
		}
	}
	for i := 0; ; i++ {
		h, _ := headAt(t, n.st.(limited).Store, ref.Key)
		if len(h.Deltas) >= fanout {
			if e := n.entities[ref.Key]; e.replay() > uint64(h.PageCount)+256 {
				t.Fatal("compaction fill reached the snapshot threshold")
			}
			return
		}
		mustPut(t, n, ref, fmt.Sprint("blob", i%3), randText(40<<10), l)
	}
}

// startMaintenance runs one maintenance step of key in the background.
func startMaintenance(n *Node, key string) {
	n.mu.Lock()
	n.skipMaintenance = false
	e := n.entities[key]
	n.mu.Unlock()
	n.maintainLater(e)
}

// TestStaleMaintenanceAcrossTakeover pauses a's compaction or snapshot
// before/after its object PUT and before/after its head CAS, lets b take
// over and write, then resumes a: a stale step never publishes, its object
// is never referenced by any later head, and every acknowledged write stays
// readable from S3 alone.
func TestStaleMaintenanceAcrossTakeover(t *testing.T) {
	for _, kind := range []string{"compaction", "snapshot"} {
		for _, at := range []string{"put.before", "put.after", "cas.before", "cas.after"} {
			t.Run(kind+"/"+at, func(t *testing.T) {
				st := newStore(t)
				ps := &pauseStore{Store: st}
				a := testNode(t, ps, "a", nil)
				l := newLedger()
				ref := mustCreate(t, a, "k", l)
				fillMaintenance(t, a, ref, kind == "snapshot", l)
				before, _ := headAt(t, st, "k")
				var obj string // a's maintenance object
				op, phase, _ := cut(at)
				var match func(string) bool
				if op == "cas" {
					match = func(k string) bool { return k == head.HeadKey("k") }
				}
				paused, release := pauseOnce(ps, op, phase, match)
				prev := ps.hook
				ps.setHook(func(o, key, ph string) bool {
					if o == "put" && ph == "before" && obj == "" {
						obj = key
					}
					return prev(o, key, ph)
				})
				startMaintenance(a, "k")
				waitPaused(t, paused)
				b := testNode(t, st, "b", nil)
				route, err := b.Resolve(bg, "k")
				if err != nil {
					t.Fatal(err)
				}
				route, err = b.Takeover(bg, route)
				if err != nil {
					t.Fatal(err)
				}
				for i := range 3 {
					mustPut(t, b, ref, fmt.Sprint("b", i), randText(100), l)
				}
				mustPut(t, b, ref, "bblob", randText(40<<10), l)
				b.wg.Wait()
				close(release)
				a.wg.Wait()
				ps.setHook(nil)
				h, _ := headAt(t, st, "k")
				if h.Owner == nil || h.Owner.Node != "b" || h.Revision <= before.Revision {
					t.Fatalf("head owner %+v revision %d", h.Owner, h.Revision)
				}
				st2 := a.Stats()
				published := st2.Compactions + st2.Snapshots
				if at == "cas.after" { // a's CAS applied before b's takeover: legitimate
					if published != 1 {
						t.Fatalf("maintenance that committed before the takeover not counted: %+v", st2)
					}
				} else if published != 0 {
					t.Fatalf("stale maintenance published: %+v", st2)
				}
				if obj == "" {
					t.Fatal("no maintenance object observed")
				}
				// b keeps writing and maintaining; a's object must never be referenced unless it committed
				for i := range 12 {
					mustPut(t, b, ref, fmt.Sprint("blob", i%3), randText(40<<10), l)
					b.wg.Wait()
					h, _ := headAt(t, st, "k")
					if at != "cas.after" && slices.Contains(refKeys(h), obj) {
						t.Fatalf("stale maintenance object %s retargeted into revision %d", obj, h.Revision)
					}
				}
				_, err = a.Update(bg, ref, newReq(t, "stale", 1), put("stale", "x"))
				if err == nil {
					t.Fatal("stale owner wrote after the takeover")
				}
				if err := b.Collect(bg, ref); err != nil {
					t.Fatal(err)
				}
				b.Close(bg)
				l.verify(t, st, "b") // fresh session of the owner's NodeID, empty cache: from S3 alone (D4)
			})
		}
	}
}

func cut(s string) (string, string, bool) {
	for i := range s {
		if s[i] == '.' {
			return s[:i], s[i+1:], true
		}
	}
	return s, "", false
}

// objectsOf lists the objects of ref's incarnation.
func objectsOf(t *testing.T, st store.Store, key string, inc head.ID) map[string]bool {
	t.Helper()
	out := map[string]bool{}
	page, _, err := st.List(bg, head.ObjectPrefix(key, inc), "", 1000)
	if err != nil {
		t.Fatal(err)
	}
	for _, o := range page {
		out[o.Key] = true
	}
	return out
}

// requireReferenced checks that every object of the current head exists.
func requireReferenced(t *testing.T, st store.Store, key string) *head.Head {
	t.Helper()
	h, _ := headAt(t, st, key)
	objs := objectsOf(t, st, key, h.Incarnation)
	for _, k := range refKeys(h) {
		if !objs[k] {
			t.Fatalf("revision %d references a deleted object %s", h.Revision, k)
		}
	}
	return h
}

// TestCollectPausedAfterHead pauses Collect right after its head GET while
// the owner uploads, writes inline tails, compacts, snapshots and loses the
// entity to another node: the resumed Collect works from the old head and
// must delete nothing a newer head references.
func TestCollectPausedAfterHead(t *testing.T) {
	for _, snapshot := range []bool{false, true} {
		t.Run(fmt.Sprint("snapshot=", snapshot), func(t *testing.T) {
			st := newStore(t)
			a := testNode(t, st, "a", nil)
			l := newLedger()
			ref := mustCreate(t, a, "k", l)
			for i := range 4 {
				mustPut(t, a, ref, fmt.Sprint("blob", i), randText(40<<10), l)
			}
			a.wg.Wait()
			h0, _ := headAt(t, st, "k")
			orphan := head.ObjectKey("k", h0.Incarnation, h0.Revision-1, head.NewID(), head.Digest{7}) // abandoned upload
			if err := st.PutImmutable(bg, orphan, []byte("x")); err != nil {
				t.Fatal(err)
			}
			ps := &pauseStore{Store: st}
			c := testNode(t, ps, "c", nil)
			paused, release := pauseOnce(ps, "get", "after", func(k string) bool { return k == head.HeadKey("k") })
			done := make(chan error, 1)
			go func() { done <- c.Collect(bg, ref) }()
			waitPaused(t, paused)
			fillMaintenance(t, a, ref, snapshot, l)
			startMaintenance(a, "k")
			a.wg.Wait()
			mustPut(t, a, ref, "inline", "x", l) // an inline tail in the head
			h, _ := headAt(t, st, "k")
			if h.Tail == nil {
				t.Fatal("no inline tail")
			}
			if s := a.Stats(); s.Snapshots+s.Compactions == 0 {
				t.Fatalf("no maintenance published: %+v", s)
			}
			b := testNode(t, st, "b", nil)
			route, _ := b.Resolve(bg, "k")
			if _, err := b.Takeover(bg, route); err != nil {
				t.Fatal(err)
			}
			mustPut(t, b, ref, "b", "1", l)
			mustPut(t, b, ref, "bblob", randText(40<<10), l)
			b.wg.Wait()
			close(release)
			if err := <-done; err != nil {
				t.Fatal(err)
			}
			if objectsOf(t, st, "k", ref.Incarnation)[orphan] {
				t.Fatal("the resumed Collect did not run its deletions")
			}
			requireReferenced(t, st, "k")
			if err := c.Collect(bg, ref); err != nil { // a fresh pass on the current head
				t.Fatal(err)
			}
			requireReferenced(t, st, "k")
			b.Close(bg)
			l.verify(t, st, "b")
		})
	}
}

// TestCollectedObjectReuploaded: a's upload is applied, its CAS paused; b
// takes over and Collect deletes a's orphan; a resumes (412, never acked),
// then a late copy of the same object is uploaded again. No head may ever
// reference it, and the next Collect reaps it again.
func TestCollectedObjectReuploaded(t *testing.T) {
	st := newStore(t)
	ps := &pauseStore{Store: st}
	a := testNode(t, ps, "a", nil)
	l := newLedger()
	ref := mustCreate(t, a, "k", l)
	mustPut(t, a, ref, "blob", randText(40<<10), l)
	a.wg.Wait()
	a.mu.Lock()
	a.skipMaintenance = true
	a.mu.Unlock()
	var obj string
	paused, release := pauseOnce(ps, "cas", "before", nil)
	prev := ps.hook
	ps.setHook(func(o, key, ph string) bool {
		if o == "put" && ph == "after" && obj == "" {
			obj = key
		}
		return prev(o, key, ph)
	})
	done := make(chan error, 1)
	go func() {
		_, err := a.Update(bg, ref, newReq(t, "stale", 1), put("stale", randText(40<<10)))
		done <- err
	}()
	waitPaused(t, paused)
	body, err := st.Get(bg, obj, "", 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	b := testNode(t, st, "b", nil)
	route, _ := b.Resolve(bg, "k")
	if _, err := b.Takeover(bg, route); err != nil {
		t.Fatal(err)
	}
	mustPut(t, b, ref, "b", "1", l)
	if err := b.Collect(bg, ref); err != nil {
		t.Fatal(err)
	}
	if objectsOf(t, st, "k", ref.Incarnation)[obj] {
		t.Fatal("Collect kept the stale orphan")
	}
	close(release)
	if err := <-done; err == nil {
		t.Fatal("stale owner acknowledged an update after the takeover")
	}
	ps.setHook(nil)
	if err := st.PutImmutable(bg, obj, body.Body); err != nil { // a late hedge or retry of the same PUT
		t.Fatal(err)
	}
	for i := range 10 {
		mustPut(t, b, ref, fmt.Sprint("blob", i%3), randText(40<<10), l)
		b.wg.Wait()
		if h, _ := headAt(t, st, "k"); slices.Contains(refKeys(h), obj) {
			t.Fatalf("re-uploaded orphan %s referenced by revision %d", obj, h.Revision)
		}
	}
	if err := b.Collect(bg, ref); err != nil {
		t.Fatal(err)
	}
	if objectsOf(t, st, "k", ref.Incarnation)[obj] {
		t.Fatal("Collect kept the re-uploaded orphan")
	}
	b.Close(bg)
	l.verify(t, st, "b")
}

// TestCollectDuringActivation: b's takeover activation is paused before its
// object GETs; c takes over from b, snapshots, and Collect deletes the
// objects b is about to read. b must restart on the new head (PROTOCOL §4
// step 2) and answer route_changed with c's Route, never serve or claim.
func TestCollectDuringActivation(t *testing.T) {
	st := newStore(t)
	a := testNode(t, st, "a", nil)
	l := newLedger()
	ref := mustCreate(t, a, "k", l)
	for i := range 3 {
		mustPut(t, a, ref, fmt.Sprint("blob", i), randText(40<<10), l)
	}
	a.wg.Wait()
	ps := &pauseStore{Store: st}
	b := testNode(t, ps, "b", nil)
	route, err := b.Resolve(bg, "k")
	if err != nil {
		t.Fatal(err)
	}
	paused, release := pauseOnce(ps, "get", "before", func(k string) bool { return k != head.HeadKey("k") })
	type reply struct {
		r   Route
		err error
	}
	done := make(chan reply, 1)
	go func() { r, err := b.Takeover(bg, route); done <- reply{r, err} }()
	waitPaused(t, paused)
	c := testNode(t, st, "c", nil)
	cr, err := c.Resolve(bg, "k")
	if err != nil || cr.Owner.NodeID != "b" {
		t.Fatalf("b's claim not visible: %+v %v", cr, err)
	}
	if _, err := c.Takeover(bg, cr); err != nil {
		t.Fatal(err)
	}
	fillMaintenance(t, c, ref, true, l)
	startMaintenance(c, "k")
	c.wg.Wait()
	if err := c.Collect(bg, ref); err != nil {
		t.Fatal(err)
	}
	close(release)
	got := <-done
	e := wantCode(t, got.err, CodeRouteChanged)
	if e.Route == nil || e.Route.Owner.NodeID != "c" {
		t.Fatalf("route_changed without c's Route: %+v", e.Route)
	}
	requireReferenced(t, st, "k")
	mustPut(t, c, ref, "after", "1", l)
	c.Close(bg)
	l.verify(t, st, "c")
}

// corruptFixture is an entity whose head references a genesis snapshot, a
// compacted (level 1) delta and level-0 deltas, and carries an inline tail.
func corruptFixture(t *testing.T) (*storetest.Mem, Ref, *ledger, *head.Head) {
	mem := storetest.NewMem()
	a := testNode(t, mem, "a", nil)
	l := newLedger()
	ref := mustCreate(t, a, "k", l)
	for i := range fanout + 1 {
		mustPut(t, a, ref, fmt.Sprint("blob", i), randText(40<<10), l)
		a.wg.Wait()
	}
	mustPut(t, a, ref, "inline", "x", l)
	a.wg.Wait()
	a.Close(bg)
	h, _ := headAt(t, mem, "k")
	if h.Tail == nil || len(h.Deltas) < 2 || h.Deltas[0].Level != 1 || h.Deltas[len(h.Deltas)-1].Level != 0 {
		t.Fatalf("fixture head: tail %v deltas %+v", h.Tail != nil, h.Deltas)
	}
	return mem, ref, l, h
}

// TestCorruptRetainedObjects flips a byte in, truncates or deletes each
// retained object type and rewrites the head's inline tail: a fresh node
// (empty cache) fails closed with corrupt or state_missing, never serves and
// never reinitializes; once the fault is gone everything acknowledged reads.
func TestCorruptRetainedObjects(t *testing.T) {
	type target struct {
		name string
		key  func(*head.Head) string
	}
	targets := []target{
		{"snapshot", func(h *head.Head) string { return h.Snapshot.Key }},
		{"compacted", func(h *head.Head) string { return h.Deltas[0].Key }},
		{"delta", func(h *head.Head) string { return h.Deltas[len(h.Deltas)-1].Key }},
	}
	faults := map[string]func([]byte) []byte{
		"flip":     func(b []byte) []byte { b = bytes.Clone(b); b[len(b)/2] ^= 0x10; return b },
		"truncate": func(b []byte) []byte { return b[:len(b)-1] },
	}
	for _, tg := range targets {
		for name, f := range faults {
			t.Run(tg.name+"/"+name, func(t *testing.T) {
				mem, ref, l, h := corruptFixture(t)
				key := tg.key(h)
				mem.SetPlan(func(a storetest.Attempt) storetest.MemFault {
					if a.Op == "get" && a.Key == key {
						return storetest.MemFault{Body: f}
					}
					return storetest.MemFault{}
				})
				expectFailClosed(t, mem, ref, CodeCorrupt)
				mem.SetPlan(nil)
				requireHeadUnchanged(t, mem, h)
				l.verify(t, mem, ownerOf(t, mem)) // b's failed takeover kept its claim (PROTOCOL §7 step 7)
			})
		}
		t.Run(tg.name+"/delete", func(t *testing.T) {
			mem, ref, _, h := corruptFixture(t)
			if err := mem.Delete(bg, tg.key(h)); err != nil {
				t.Fatal(err)
			}
			expectFailClosed(t, mem, ref, CodeStateMissing)
			requireHeadUnchanged(t, mem, h)
		})
	}
	for _, recompute := range []bool{false, true} {
		t.Run(fmt.Sprint("tail/sha-recomputed=", recompute), func(t *testing.T) {
			mem, ref, l, h := corruptFixture(t)
			o, _ := mem.Peek(head.HeadKey("k"))
			data, _ := json.Marshal(h.Tail.Data)
			bad := bytes.Clone(h.Tail.Data)
			bad[len(bad)/2] ^= 0x10
			badData, _ := json.Marshal(bad)
			body := bytes.Replace(o.Body, data, badData, 1)
			if recompute {
				oldSum, _ := json.Marshal(h.Tail.SHA256)
				newSum, _ := json.Marshal(head.Digest(sha256.Sum256(bad)))
				body = bytes.Replace(body, oldSum, newSum, 1)
			}
			if bytes.Equal(body, o.Body) {
				t.Fatal("tail not rewritten")
			}
			etag, err := mem.PutCAS(bg, head.HeadKey("k"), body, o.ETag)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := head.Decode(body); err == nil && !recompute {
				t.Fatal("strict decode accepted a tail whose SHA-256 differs")
			}
			expectFailClosed(t, mem, ref, CodeCorrupt)
			cur, _ := mem.Peek(head.HeadKey("k"))
			if !recompute && cur.ETag != etag {
				t.Fatal("a node rewrote a head that fails strict decode")
			}
			if recompute { // a claim may carry the tail byte-identical; it must stay the damaged one
				ch, _ := headAt(t, mem, "k")
				if !bytes.Equal(ch.Tail.Data, bad) || ch.Sequence != h.Sequence || ch.Incarnation != h.Incarnation {
					t.Fatal("the damaged tail was replaced")
				}
			}
			mem.PutCAS(bg, head.HeadKey("k"), o.Body, cur.ETag) // repair: the original head
			l.verify(t, mem, "a")
		})
	}
}

// expectFailClosed reads, updates, exports and takes over ref from fresh
// nodes with empty caches: each must fail with code.
func expectFailClosed(t *testing.T, mem *storetest.Mem, ref Ref, code Code) {
	t.Helper()
	n := testNode(t, mem, "a", nil) // D4: the owner's next session
	_, err := n.Read(bg, ref, 1, func(*View) ([]byte, error) { return []byte("served"), nil })
	wantCode(t, err, code)
	_, err = n.Update(bg, ref, newReq(t, "u", 1), put("u", "1"))
	wantCode(t, err, code)
	_, err = n.Export(bg, ref, io.Discard)
	wantCode(t, err, code)
	n.Close(bg)
	b := testNode(t, mem, "b", nil)
	if route, err := b.Resolve(bg, ref.Key); err == nil {
		_, err = b.Takeover(bg, route)
		wantCode(t, err, code)
	} else {
		wantCode(t, err, code)
	}
	_, _, err = b.Create(bg, ref.Key, newReq(t, "again", 1), createKV)
	if err == nil {
		t.Fatal("Create reinitialized a key whose state is damaged")
	}
	b.Close(bg)
}

func ownerOf(t *testing.T, st store.Store) string {
	t.Helper()
	h, _ := headAt(t, st, "k")
	return h.Owner.Node
}

func requireHeadUnchanged(t *testing.T, mem *storetest.Mem, h *head.Head) {
	t.Helper()
	cur, _ := headAt(t, mem, h.Key)
	if cur.Incarnation != h.Incarnation || cur.Sequence != h.Sequence || cur.DatabaseChecksum != h.DatabaseChecksum {
		t.Fatalf("head changed from seq %d to %d (incarnation %v)", h.Sequence, cur.Sequence, cur.Incarnation == h.Incarnation)
	}
}

// TestCorruptBirthAndNamespace: a damaged birth.json or namespace.json
// fails closed; nothing is reinitialized empty.
func TestCorruptBirthAndNamespace(t *testing.T) {
	flip := func(b []byte) []byte { b = bytes.Clone(b); b[len(b)/2] ^= 0x10; return b }
	trunc := func(b []byte) []byte { return b[:len(b)-1] }
	for name, f := range map[string]func([]byte) []byte{"flip": flip, "truncate": trunc} {
		t.Run("namespace/"+name, func(t *testing.T) {
			mem, _, _, _ := corruptFixture(t)
			mem.SetPlan(func(a storetest.Attempt) storetest.MemFault {
				if a.Op == "get" && a.Key == head.NamespaceKey {
					return storetest.MemFault{Body: f}
				}
				return storetest.MemFault{}
			})
			cfg, _ := Config{S3: s3.New(s3.Options{}), Bucket: "b", Prefix: "p", NodeID: "a", CacheDir: t.TempDir()}.validate()
			_, err := open(bg, cfg, mem)
			wantCode(t, err, CodeCorrupt)
		})
		t.Run("birth/"+name, func(t *testing.T) {
			mem, ref, l, h := corruptFixture(t)
			mem.SetPlan(func(a storetest.Attempt) storetest.MemFault {
				if a.Op == "get" && a.Key == head.BirthKey("k") {
					return storetest.MemFault{Body: f}
				}
				return storetest.MemFault{}
			})
			n := testNode(t, mem, "b", nil)
			_, _, err := n.Create(bg, "k", newReq(t, "other", 1), createKV)
			if err == nil {
				t.Fatal("Create over a damaged birth succeeded")
			}
			requireHeadUnchanged(t, mem, h)
			o, _ := mem.Peek(head.HeadKey("k"))
			mem.Delete(bg, head.HeadKey("k")) // head gone too: only the damaged birth remains
			_, _, err = n.Create(bg, "k", newReq(t, "other2", 1), createKV)
			wantCode(t, err, CodeCorrupt)
			_, err = n.Read(bg, ref, 1, func(*View) ([]byte, error) { return nil, nil })
			wantCode(t, err, CodeStateMissing)
			if _, ok := mem.Peek(head.HeadKey("k")); ok {
				t.Fatal("a head was created over a damaged birth")
			}
			mem.SetPlan(nil)
			mem.PutCAS(bg, head.HeadKey("k"), o.Body, "")
			l.verify(t, mem, "a")
		})
	}
	t.Run("namespace/delete", func(t *testing.T) {
		mem, _, _, _ := corruptFixture(t)
		mem.Delete(bg, head.NamespaceKey)
		cfg, _ := Config{S3: s3.New(s3.Options{}), Bucket: "b", Prefix: "p", NodeID: "a", CacheDir: t.TempDir()}.validate()
		_, err := open(bg, cfg, mem)
		wantCode(t, err, CodeStateMissing)
	})
	t.Run("birth/delete", func(t *testing.T) {
		mem, ref, l, h := corruptFixture(t)
		mem.Delete(bg, head.BirthKey("k"))
		n := testNode(t, mem, "b", nil)
		_, _, err := n.Create(bg, "k", newReq(t, "other", 1), createKV)
		if err == nil {
			t.Fatal("Create reinitialized a key whose birth.json is gone")
		}
		requireHeadUnchanged(t, mem, h)
		_ = ref
		l.verify(t, mem, "a")
	})
}
