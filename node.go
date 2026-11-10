package garden

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"iter"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/OrdalieTech/big-garden/internal/engine"
	"github.com/OrdalieTech/big-garden/internal/head"
	"github.com/OrdalieTech/big-garden/internal/store"
)

// Node is one process's handle on a namespace.
type Node struct {
	cfg  Config
	st   store.Store
	drv  *engine.Driver
	ns   head.ID
	self head.Owner
	dir  string   // session cache dir
	lock *os.File // holds the flock on dir
	now  func() time.Time

	mu         sync.Mutex
	entities   map[string]*entity
	wmu        sync.Mutex          // the warm tier below; after mu, never before
	warm       map[string]warmFile // by EntityDir hash (D21)
	warmDir    string
	reserved   int64         // cache bytes reserved by activations in progress
	recoveries chan struct{} // one token per activation in progress (Limits.Recoveries)
	staging    atomic.Int64  // bytes of objects being uploaded (Stats)
	logged     atomic.Int64  // unix nanos of the last log line (rate limit)
	closed     bool

	done            chan struct{} // closed by Close: stops the sweeper
	counters        counters
	wg              sync.WaitGroup // maintenance steps in flight
	skipMaintenance bool           // tests of the §6.7 cap only
}

// Open validates cfg and opens a Node on its namespace (PROTOCOL §8). It
// writes and deletes probe objects under <Prefix>/probe/ to check that the
// store enforces If-None-Match and If-Match on PUT; a store that does not
// fails Open with CodeUnsupported.
func Open(ctx context.Context, cfg Config) (*Node, error) {
	cfg, err := cfg.validate()
	if err != nil {
		return nil, err
	}
	return open(ctx, cfg, store.NewS3(cfg.S3, cfg.Bucket, cfg.Prefix, cfg.Limits.StorageAttemptTimeout))
}

// open runs PROTOCOL §8 steps 2–4 for a validated cfg on st.
func open(ctx context.Context, cfg Config, st store.Store) (*Node, error) {
	n := &Node{cfg: cfg, st: limited{st, make(chan struct{}, cfg.Limits.InflightStorage), &[4]atomic.Int64{}}, entities: map[string]*entity{}, now: time.Now,
		recoveries: make(chan struct{}, cfg.Limits.Recoveries)}
	var err error
	if n.drv, err = engine.NewDriver(cfg.Extend); err != nil {
		return nil, invalid("Config.Extend: " + err.Error())
	}
	if err := n.namespace(ctx); err != nil {
		return nil, err
	}
	if err := n.probe(ctx); err != nil {
		return nil, err
	}
	n.self = head.Owner{Node: cfg.NodeID, Session: head.NewID()}
	if err := n.sessionDir(); err != nil {
		return nil, err
	}
	if err := n.warmIndex(); err != nil {
		return nil, err
	}
	n.done = make(chan struct{})
	go n.sweeper()
	return n, nil
}

// sweeper discards the clean caches of entities idle for IdleTimeout.
func (n *Node) sweeper() {
	t := time.NewTicker(n.cfg.Limits.IdleTimeout / 2)
	defer t.Stop()
	for {
		select {
		case <-n.done:
			return
		case now := <-t.C:
			n.mu.Lock()
			n.scan(now.Add(-n.cfg.Limits.IdleTimeout), nil)
			n.mu.Unlock()
		}
	}
}

// room reports whether self can become resident under ResidentEntities,
// discarding the least recently admitted idle, settled entity if needed.
func (n *Node) room(self *entity) bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	resident, _, lru := n.scan(time.Time{}, self)
	self.building = resident < n.cfg.Limits.ResidentEntities || n.shed(lru) // counted resident until unreserve
	return self.building
}

// pause waits d or until ctx ends.
func pause(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// scan walks the entities other than self under n.mu, never blocking on an
// entity mutex: it discards the idle, settled resident ones last used before
// cutoff and forgets unneeded ones. It returns the number of resident
// entities (a busy one only with a file), their cache bytes (a busy one with
// its last seen size) and the least recently admitted idle, settled one.
func (n *Node) scan(cutoff time.Time, self *entity) (resident int, used int64, lru *entity) {
	var at time.Time
	for _, e := range n.entities {
		if e == self || !e.mu.TryLock() {
			if b := e.bytes.Load(); b > 0 || e.building && e != self {
				resident, used = resident+1, used+b
			}
			continue
		}
		if e.db != nil && e.unresolved == nil && e.used.Before(cutoff) {
			n.park(e)
		}
		if e.db != nil {
			e.bytes.Store(e.db.Bytes())
			resident, used = resident+1, used+e.bytes.Load()
			if e.unresolved == nil && (lru == nil || e.used.Before(at)) {
				lru, at = e, e.used
			}
		}
		n.forget(e)
		e.mu.Unlock()
	}
	return resident, used, lru
}

// shed parks lru if it is still resident, idle and settled.
func (n *Node) shed(lru *entity) bool {
	if lru == nil || !lru.mu.TryLock() {
		return false
	}
	defer lru.mu.Unlock()
	if lru.db == nil || lru.unresolved != nil {
		return false
	}
	n.park(lru)
	return true
}

// warmFile is a file of the warm tier (D21): <hash>.db holds Image, and
// <hash>.json this struct; Pages are the replay pages of the links it was
// parked with. Both are hints: activation trusts the file only after the
// checksum gate of a cold restore.
type warmFile struct {
	Image engine.Image
	Pages map[head.Digest]uint32
	bytes int64
	used  time.Time
}

// warmIndex lists this namespace's warm tier, left by earlier sessions:
// each <hash>.db with a readable <hash>.json; anything else is removed.
func (n *Node) warmIndex() error {
	n.warmDir, n.warm = filepath.Join(n.cfg.CacheDir, "warm", n.ns.String()), map[string]warmFile{}
	if err := os.MkdirAll(n.warmDir, 0o700); err != nil {
		return newError(CodeUnavailable, "warm dir", err)
	}
	entries, _ := os.ReadDir(n.warmDir)
	for _, d := range entries {
		path := filepath.Join(n.warmDir, d.Name())
		name, db := strings.TrimSuffix(strings.TrimSuffix(path, ".db"), ".json"), strings.HasSuffix(path, ".db")
		var w warmFile
		b, err := os.ReadFile(name + ".json")
		st, serr := os.Lstat(name + ".db")
		switch {
		case db && err == nil && serr == nil && st.Mode().IsRegular() && json.Unmarshal(b, &w) == nil:
			w.bytes, w.used = st.Size(), st.ModTime()
			n.warm[filepath.Base(name)] = w
		case db || !strings.HasSuffix(path, ".json") || serr != nil: // a .json is removed with its .db
			os.Remove(path)
		}
	}
	return nil
}

// park moves e's settled file to the warm tier instead of deleting it (D21);
// e keeps nothing. Anything unsettled, or any failure, deletes the file as
// discard does.
func (n *Node) park(e *entity) {
	if e.db == nil || e.h == nil || e.unresolved != nil || e.db.Published() != imageOf(e.h) || len(e.pages) != len(links(e.h)) {
		e.discard()
		return
	}
	refs := links(e.h)
	w := warmFile{Image: imageOf(e.h), Pages: map[head.Digest]uint32{}, used: e.used}
	for i, r := range refs {
		w.Pages[r.SHA256] = e.pages[i]
	}
	b, _ := json.Marshal(w)
	hash := filepath.Base(head.EntityDir(e.key))
	name := filepath.Join(n.warmDir, hash)
	n.wmu.Lock()
	defer n.wmu.Unlock()
	delete(n.warm, hash)
	size, err := e.db.Park(name + ".db")
	failpoint("warm.park")
	if err == nil {
		err = os.WriteFile(name+".json", b, 0o600)
	}
	if err == nil {
		w.bytes = size
		os.Chtimes(name+".db", w.used, w.used)
		n.warm[hash] = w
		e.clear()
		return
	}
	os.Remove(name + ".db")
	os.Remove(name + ".json")
	e.discard()
}

// claimWarm moves key's warm file, if any, to path and returns its hint;
// with path empty it deletes the file and its hint.
func (n *Node) claimWarm(key, path string) (warmFile, bool) {
	hash := filepath.Base(head.EntityDir(key))
	name := filepath.Join(n.warmDir, hash)
	n.wmu.Lock()
	defer n.wmu.Unlock()
	w, ok := n.warm[hash]
	delete(n.warm, hash)
	if path == "" {
		os.Remove(name + ".db")
		os.Remove(name + ".json")
		return w, false
	}
	return w, ok && os.Rename(name+".db", path) == nil
}

// unclaimWarm puts back a file claimWarm moved to path.
func (n *Node) unclaimWarm(key, path string, w warmFile) {
	hash := filepath.Base(head.EntityDir(key))
	n.wmu.Lock()
	defer n.wmu.Unlock()
	if os.Rename(path, filepath.Join(n.warmDir, hash)+".db") == nil {
		n.warm[hash] = w
	}
}

// evictWarm deletes the least recently used warm file other than keep's:
// S3 keeps the state.
func (n *Node) evictWarm(keep string) bool {
	keep = filepath.Base(head.EntityDir(keep))
	n.wmu.Lock()
	defer n.wmu.Unlock()
	lru := ""
	for h, w := range n.warm {
		if h != keep && (lru == "" || w.used.Before(n.warm[lru].used)) {
			lru = h
		}
	}
	if lru == "" {
		return false
	}
	delete(n.warm, lru)
	name := filepath.Join(n.warmDir, lru)
	os.Remove(name + ".db")
	os.Remove(name + ".json")
	n.counters.warmEvictions.Add(1)
	return true
}

// warmBytes sums the sizes of the warm files.
func (n *Node) warmBytes() (b int64) {
	n.wmu.Lock()
	defer n.wmu.Unlock()
	for _, w := range n.warm {
		b += w.bytes
	}
	return b
}

func (n *Node) namespace(ctx context.Context) error {
	for range 2 {
		if found, err := n.readNamespace(ctx); found || err != nil {
			return err
		}
		page, _, err := n.st.List(ctx, "entities/", "", 1)
		if err != nil {
			return storeError(err)
		}
		if len(page) > 0 {
			// namespace.json precedes every entity: another node may have
			// written both since the GET above.
			if found, err := n.readNamespace(ctx); found || err != nil {
				return err
			}
			return newError(CodeStateMissing, "entities exist without namespace.json", nil)
		}
		body, _ := head.Namespace{Format: head.Format, Namespace: head.NewID(), EntityFormat: head.Format}.Encode()
		err = n.st.PutImmutable(ctx, head.NamespaceKey, body)
		if o := store.OutcomeOf(err); o != store.OK && o != store.PreconditionFailed && o != store.Unknown {
			return storeError(err)
		}
	}
	return newError(CodeUnavailable, "namespace.json is not readable", nil)
}

// readNamespace sets n.ns from namespace.json and reports whether it exists.
func (n *Node) readNamespace(ctx context.Context) (bool, error) {
	obj, err := n.st.Get(ctx, head.NamespaceKey, "", head.MaxNamespaceBytes)
	if store.OutcomeOf(err) == store.NotFound {
		return false, nil
	} else if err != nil {
		return false, storeError(err)
	}
	ns, err := head.DecodeNamespace(obj.Body)
	if err != nil {
		return false, newError(CodeCorrupt, "namespace.json: "+err.Error(), nil)
	}
	n.ns = ns.Namespace
	return true, nil
}

// probe checks the conditional-write semantics the protocol relies on.
func (n *Node) probe(ctx context.Context) error {
	p := "probe/" + head.NewID().String() + "/"
	x, y := p+"x", p+"y"
	defer n.st.Delete(context.WithoutCancel(ctx), x)
	defer n.st.Delete(context.WithoutCancel(ctx), y)
	bad := func(what string, err error) error {
		if o := store.OutcomeOf(err); o == store.Unknown {
			return storeError(err)
		}
		return newError(CodeUnsupported, "storage probe: "+what, err)
	}
	if err := n.st.PutImmutable(ctx, x, []byte("x")); err != nil {
		return bad("create", err)
	}
	if err := n.st.PutImmutable(ctx, x, []byte("x")); store.OutcomeOf(err) != store.PreconditionFailed {
		return bad("If-None-Match ignored", err)
	}
	e1, err := n.st.PutCAS(ctx, y, []byte("1"), "")
	if err != nil {
		return bad("conditional create", err)
	}
	if _, err := n.st.PutCAS(ctx, y, []byte("2"), e1); err != nil {
		return bad("If-Match", err)
	}
	if _, err := n.st.PutCAS(ctx, y, []byte("3"), e1); store.OutcomeOf(err) != store.PreconditionFailed {
		return bad("stale If-Match accepted", err)
	}
	obj, err := n.st.Get(ctx, y, "", 1)
	if err != nil || !bytes.Equal(obj.Body, []byte("2")) {
		return bad("read after write", err)
	}
	return nil
}

func (n *Node) sessionDir() error {
	if err := os.MkdirAll(n.cfg.CacheDir, 0o700); err != nil {
		return newError(CodeUnavailable, "cache dir", err)
	}
	n.dir = filepath.Join(n.cfg.CacheDir, n.self.Session.String())
	if err := os.Mkdir(n.dir, 0o700); err != nil {
		return newError(CodeUnavailable, "session dir", err)
	}
	f, ok := lockDir(n.dir, os.O_CREATE|os.O_EXCL)
	if !ok {
		os.RemoveAll(n.dir)
		return newError(CodeUnavailable, "session lock", nil)
	}
	n.lock = f
	entries, _ := os.ReadDir(n.cfg.CacheDir)
	for _, d := range entries { // older processes' sessions: removable once unlocked
		var id head.ID
		if d.Type() != os.ModeDir || d.Name() == n.self.Session.String() || id.UnmarshalText([]byte(d.Name())) != nil {
			continue
		}
		path := filepath.Join(n.cfg.CacheDir, d.Name())
		if f, ok := lockDir(path, 0); ok {
			os.RemoveAll(path)
			f.Close()
		}
	}
	return nil
}

// lockDir opens dir/lock (never following symlinks) and takes an exclusive
// non-blocking flock on it.
func lockDir(dir string, flags int) (*os.File, bool) {
	f, err := os.OpenFile(filepath.Join(dir, "lock"), os.O_RDWR|syscall.O_NOFOLLOW|flags, 0o600)
	if err != nil {
		return nil, false
	}
	if syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB) != nil {
		f.Close()
		return nil, false
	}
	return f, true
}

// Owner returns this Node's publishing identity.
func (n *Node) Owner() Owner { return Owner{NodeID: n.self.Node, Session: n.self.Session} }

// entity returns the resident entity of key, creating it, or closed.
func (n *Node) entity(key string) (*entity, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.closed {
		return nil, newError(CodeClosed, "", nil)
	}
	e := n.entities[key]
	if e == nil {
		e = &entity{key: key, mu: make(gate, 1)}
		n.entities[key] = e
	}
	e.refs++
	return e, nil
}

// leave ends an operation on e: it unlocks e and drops the reference. A
// head this session does not own stays resident like an owned one: a strong
// read revalidates it anyway (D24).
func (n *Node) leave(e *entity) {
	e.mu.Unlock()
	n.drop(e)
}

// drop releases one reference to e and forgets e once no operation holds it
// and it is neither resident nor unresolved (PLAN §11.2).
func (n *Node) drop(e *entity) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if e.refs--; e.refs == 0 && e.mu.TryLock() {
		n.forget(e)
		e.mu.Unlock()
	}
}

// forget removes e from the map if nothing needs it; n.mu and e.mu are held.
func (n *Node) forget(e *entity) {
	if e.refs == 0 && e.db == nil && e.unresolved == nil && n.entities[e.key] == e {
		delete(n.entities, e.key)
	}
}

func (n *Node) isClosed() bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.closed
}

// buildBytes is the cache reserve of one activation or genesis build.
func (n *Node) buildBytes() int64 { return 4*n.cfg.Limits.EntityBytes + 1<<20 }

// cache makes room for e to become resident and reserves buildBytes for
// building its file (PROTOCOL §4 step 1, §7 cache policy); the caller
// releases the reserve with unreserve once the file is built.
func (n *Node) cache(ctx context.Context, e *entity) error {
	for i := 0; !n.room(e); i++ { // wait briefly while every resident entity is busy
		if i == 100 || pause(ctx, 10*time.Millisecond) != nil {
			return newError(CodeCapacity, "ResidentEntities entities are busy", ctx.Err())
		}
	}
	for i := 0; !n.fit(e); i++ { // wait briefly for pinned entities
		if i == 100 || pause(ctx, 10*time.Millisecond) != nil {
			n.mu.Lock()
			e.building = false
			n.mu.Unlock()
			return newError(CodeCapacity, "cache bytes held by entities in use", ctx.Err())
		}
	}
	return nil
}

// fit reserves buildBytes for self if it fits in CacheBytes beside the other
// reserves, the files of the other resident entities and the warm tier,
// deleting warm files then parking idle, settled entities, least recently
// used first, until it does. A busy
// entity is pinned and counts with its last seen size. It never blocks on an
// entity mutex.
func (n *Node) fit(self *entity) bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	for {
		_, used, lru := n.scan(time.Time{}, self)
		if n.reserved+used+n.warmBytes()+n.buildBytes() <= n.cfg.Limits.CacheBytes {
			n.reserved += n.buildBytes()
			return true
		}
		if !n.evictWarm(self.key) && !n.shed(lru) {
			return false
		}
	}
}

func (n *Node) unreserve(e *entity) {
	n.mu.Lock()
	n.reserved -= n.buildBytes()
	e.building = false
	n.mu.Unlock()
}

// Close stops admissions, settles and discards every resident entity, and
// deletes the session cache dir (PROTOCOL §7). It never closes the S3 client.
func (n *Node) Close(ctx context.Context) error {
	n.mu.Lock()
	if !n.closed && n.done != nil {
		close(n.done)
	}
	n.closed = true
	es := make([]*entity, 0, len(n.entities))
	for _, e := range n.entities {
		es = append(es, e)
	}
	n.mu.Unlock()
	ctx, cancel := context.WithTimeout(ctx, n.cfg.Limits.CommitTimeout)
	defer cancel()
	idle := make(chan struct{})
	go func() { n.wg.Wait(); close(idle) }()
	select {
	case <-idle:
	case <-ctx.Done():
	}
	unknown := false
	for _, e := range es {
		if e.mu.lock(ctx) != nil { // still busy: it may hold an unresolved candidate
			unknown = true
			continue
		}
		if e.unresolved != nil {
			n.resolve(ctx, e, e.unresolved)
			unknown = unknown || e.unresolved != nil
		}
		n.park(e)
		e.mu.Unlock()
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.lock != nil {
		os.RemoveAll(n.dir)
		n.lock.Close()
		n.lock = nil
	}
	if unknown {
		return newError(CodeOutcomeUnknown, "a publication stayed unresolved", nil)
	}
	return nil
}

// storeError maps a storage failure to an Error.
func storeError(err error) *Error {
	var e *Error
	if errors.As(err, &e) {
		return e
	}
	switch store.OutcomeOf(err) {
	case store.Failed:
		return newError(CodeUnavailable, "storage request failed", err)
	default:
		return newError(CodeUnavailable, "storage unreachable", err)
	}
}

// engineError maps a local SQLite failure to an Error.
func engineError(err error) *Error {
	switch {
	case errors.Is(err, engine.ErrCorrupt):
		return newError(CodeCorrupt, "local image failed validation", err)
	case engine.Full(err):
		return newError(CodeCapacity, "entity size limit reached", err)
	case errors.Is(err, engine.ErrReserved):
		return newError(CodeInvalidArgument, "reserved table name", err)
	}
	return newError(CodeUnavailable, "local database", err)
}

// limited bounds the storage requests in flight to Limits.InflightStorage.
type limited struct {
	store.Store
	sem  chan struct{}
	busy *[4]atomic.Int64 // nanoseconds in calls, by Stats.Storage class
}

// do runs f holding one in-flight slot; a request still waiting for a slot
// when ctx ends is never sent.
func (l limited) do(ctx context.Context, op, key string, f func() error) error {
	select {
	case l.sem <- struct{}{}:
	case <-ctx.Done():
		return &store.Error{Outcome: store.Unknown, Op: op, Key: key, Err: ctx.Err()} // never sent
	}
	defer func(t time.Time) {
		l.busy[map[string]int{"get": 0, "put": 1, "cas": 1, "list": 2, "delete": 3}[op]].Add(int64(time.Since(t)))
		<-l.sem
	}(time.Now())
	return f()
}

func (l limited) Get(ctx context.Context, key, ifNoneMatch string, max int64) (o store.Object, err error) {
	err = l.do(ctx, "get", key, func() error { o, err = l.Store.Get(ctx, key, ifNoneMatch, max); return err })
	return o, err
}

func (l limited) PutImmutable(ctx context.Context, key string, body []byte) error {
	return l.do(ctx, "put", key, func() error { return l.Store.PutImmutable(ctx, key, body) })
}

func (l limited) PutCAS(ctx context.Context, key string, body []byte, ifMatch string) (etag string, err error) {
	err = l.do(ctx, "cas", key, func() error { etag, err = l.Store.PutCAS(ctx, key, body, ifMatch); return err })
	return etag, err
}

func (l limited) List(ctx context.Context, prefix, after string, limit int) (es []store.Entry, more bool, err error) {
	err = l.do(ctx, "list", prefix, func() error { es, more, err = l.Store.List(ctx, prefix, after, limit); return err })
	return es, more, err
}

func (l limited) Delete(ctx context.Context, key string) error {
	return l.do(ctx, "delete", key, func() error { return l.Store.Delete(ctx, key) })
}

// Scan yields the Route of every entity of the namespace whose key hashes
// after afterKey's ("" from the start), in key-hash order, so a scan resumes
// from the last Route's key. Each Route is a point observation, as from
// Resolve; an unreadable head is yielded as an error and the scan goes on.
// Cost: one LIST per 1,000 stored objects and one GET per entity (D22).
func (n *Node) Scan(ctx context.Context, afterKey string) iter.Seq2[Route, error] {
	return func(yield func(Route, error) bool) {
		after := ""
		if afterKey != "" {
			after = head.EntityDir(afterKey) + "/head.json"
		}
		for {
			page, more, err := n.st.List(ctx, "entities/", after, 1000)
			if err != nil {
				yield(Route{}, storeError(err))
				return
			}
			var keys []string
			for _, o := range page {
				if after = o.Key; strings.HasSuffix(o.Key, "/head.json") {
					keys = append(keys, o.Key)
				}
			}
			routes, errs, gone := make([]Route, len(keys)), make([]error, len(keys)), make([]bool, len(keys))
			var wg sync.WaitGroup
			sem := make(chan struct{}, 4)
			for i, k := range keys {
				wg.Go(func() {
					sem <- struct{}{}
					defer func() { <-sem }()
					obj, err := n.st.Get(ctx, k, "", head.MaxHeadBytes)
					if gone[i] = store.OutcomeOf(err) == store.NotFound; err != nil { // deleted meanwhile: skipped
						errs[i] = storeError(err)
						return
					}
					h, err := head.Decode(obj.Body)
					switch {
					case err != nil:
						errs[i] = newError(CodeCorrupt, k+": "+err.Error(), nil)
					case h.Namespace != n.ns || head.EntityDir(h.Key)+"/head.json" != k:
						errs[i] = newError(CodeCorrupt, k+" names another namespace or key", nil)
					default:
						routes[i] = *routeOf(&h)
					}
				})
			}
			wg.Wait()
			for i := range keys {
				if !gone[i] && !yield(routes[i], errs[i]) {
					return
				}
			}
			if !more || len(page) == 0 {
				return
			}
		}
	}
}
