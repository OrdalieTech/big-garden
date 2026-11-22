package garden

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"io"
	"iter"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
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
	warmUsed   int64               // sum of the warm files' bytes, under wmu
	warmDir    string
	reserved   int64         // cache bytes reserved by activations in progress
	recoveries chan struct{} // one token per activation in progress (Limits.Recoveries)
	staging    atomic.Int64  // bytes of objects being uploaded (Stats)
	logged     atomic.Int64  // unix nanos of the last log line (rate limit)
	closed     bool
	uploads    map[string]upload      // blob uploads of this Node by full key, under mu (D33)
	unborn     map[string]spooledBlob // spools of blobs for a Create, under mu
	chunks     chunkCache

	done            chan struct{} // closed by Close: stops the sweeper
	counters        counters
	wg              sync.WaitGroup // maintenance steps in flight
	skipMaintenance bool           // tests of the §6.7 cap only
	offGate         uint32         // pages from which maintenance snapshots off the mutex (D35)
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
	var st store.Store
	if cfg.LocalDir != "" {
		st, err = store.NewLocal(cfg.LocalDir, cfg.Prefix)
		if err != nil {
			return nil, newError(CodeUnavailable, "local store", err)
		}
	} else {
		st = store.NewS3(cfg.S3, cfg.Bucket, cfg.Prefix, cfg.Limits.StorageAttemptTimeout)
	}
	return open(ctx, cfg, st)
}

// keyCheck is the namespace's check of a sealing key (D36): it proves the key
// without revealing it.
func keyCheck(key []byte, ns head.ID) *head.Digest {
	if key == nil {
		return nil
	}
	m := hmac.New(sha256.New, key)
	m.Write([]byte("big-garden key check " + ns.String()))
	d := head.Digest(m.Sum(nil))
	return &d
}

// open runs PROTOCOL §8 steps 2–4 for a validated cfg on st.
func open(ctx context.Context, cfg Config, st store.Store) (*Node, error) {
	if cfg.Key != nil {
		st, _ = store.Sealed(st, cfg.Key) // validated: 32 bytes
	}
	n := &Node{cfg: cfg, st: limited{st, make(chan struct{}, cfg.Limits.InflightStorage), &[4]atomic.Int64{}}, entities: map[string]*entity{}, uploads: map[string]upload{}, unborn: map[string]spooledBlob{}, offGate: 16 << 20 / head.PageSize, now: time.Now,
		recoveries: make(chan struct{}, cfg.Limits.Recoveries)}
	var err error
	if n.drv, err = engine.NewDriver(cfg.Extend); err != nil {
		return nil, invalid("Config.Extend: " + err.Error())
	}
	n.drv.SetOptions(engine.Options{CacheBytes: cfg.Limits.SQLiteCacheBytes, MmapBytes: cfg.Limits.MmapBytes})
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
	if err := n.openChunks(); err != nil {
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
			n.pruneUnborn(now)
			n.mu.Unlock()
		}
	}
}

// pruneUnborn removes the spools of blobs for a Create that no Create used
// in time; under n.mu.
func (n *Node) pruneUnborn(now time.Time) {
	for path, b := range n.unborn {
		if now.After(b.until) {
			n.dropUnborn(path)
		}
	}
}

// spooledBlob is the spool of a blob for a Create: kept until, size bytes
// counted in CacheBytes (D37).
type spooledBlob struct {
	until time.Time
	size  int64
}

// dropUnborn removes a blob spool for a Create; under n.mu.
func (n *Node) dropUnborn(path string) {
	if b, ok := n.unborn[path]; ok {
		os.Remove(path)
		n.staging.Add(-b.size)
		delete(n.unborn, path)
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
// entity mutex: it parks the idle, settled resident ones last used before
// cutoff and forgets unneeded ones. It returns the number of resident
// entities (a busy one with a file or being built), their cache bytes (a busy
// one with its last seen size) and the least recently admitted idle, settled
// one.
func (n *Node) scan(cutoff time.Time, self *entity) (resident int, used int64, lru *entity) {
	var at time.Time
	for _, e := range n.entities {
		if e == self || !e.mu.TryLock() {
			if b := e.bytes.Load(); b > 0 || e.building && e != self {
				resident, used = resident+1, used+b
			}
			continue
		}
		if e.refs == 0 && e.db != nil && e.unresolved == nil && e.lastUsed().Before(cutoff) {
			n.park(e)
		}
		if e.db != nil { // its bytes as of its last publication (entity.go): no stat per resident under n.mu
			resident, used = resident+1, used+e.bytes.Load()
			if e.refs == 0 && e.unresolved == nil && (lru == nil || e.lastUsed().Before(at)) {
				lru, at = e, e.lastUsed()
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
	if lru.refs != 0 || lru.db == nil || lru.unresolved != nil {
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
	n.warmDir, n.warm, n.warmUsed = filepath.Join(n.cfg.CacheDir, "warm", n.ns.String()), map[string]warmFile{}, 0
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
			w.bytes, w.used = st.Size()+int64(len(b)), st.ModTime() // the hint counts too
			n.warmPut(filepath.Base(name), w)
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
	e.rw.Lock()
	defer e.rw.Unlock()
	if e.db == nil || e.h == nil || e.unresolved != nil || e.db.Published() != imageOf(e.h) {
		e.discardLocked()
		return
	}
	refs := links(e.h)
	w := warmFile{Image: imageOf(e.h), Pages: map[head.Digest]uint32{}, used: e.lastUsed()}
	if len(e.pages) == len(refs) {
		for i, r := range refs {
			w.Pages[r.SHA256] = e.pages[i]
		}
	}
	b, _ := json.Marshal(w)
	hash := filepath.Base(head.EntityDir(e.key))
	name := filepath.Join(n.warmDir, hash)
	n.wmu.Lock()
	defer n.wmu.Unlock()
	n.warmDel(hash)
	size, err := e.db.Park(name + ".db")
	failpoint("warm.park")
	if err == nil {
		err = os.WriteFile(name+".json", b, 0o600)
	}
	if err == nil {
		w.bytes = size + int64(len(b)) // the hint counts too: 100,000 of them are megabytes
		os.Chtimes(name+".db", w.used, w.used)
		n.warmPut(hash, w)
		e.clearLocked()
		return
	}
	os.Remove(name + ".db")
	os.Remove(name + ".json")
	e.discardLocked()
}

// claimWarm moves key's warm file, if any, to path and returns its hint;
// with path empty it deletes the file and its hint.
func (n *Node) claimWarm(key, path string) (warmFile, bool) {
	hash := filepath.Base(head.EntityDir(key))
	name := filepath.Join(n.warmDir, hash)
	n.wmu.Lock()
	defer n.wmu.Unlock()
	w, ok := n.warm[hash]
	n.warmDel(hash)
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
		n.warmPut(hash, w)
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
	n.warmDel(lru)
	name := filepath.Join(n.warmDir, lru)
	os.Remove(name + ".db")
	os.Remove(name + ".json")
	n.counters.warmEvictions.Add(1)
	return true
}

// warmBytes is the size of the warm files, kept by warmPut and warmDel
// (under wmu): fit asks on every admission, and the tier may hold every
// entity the node ever touched.
func (n *Node) warmBytes() int64 {
	n.wmu.Lock()
	defer n.wmu.Unlock()
	return n.warmUsed
}

func (n *Node) warmPut(hash string, w warmFile) {
	n.warmDel(hash)
	n.warm[hash], n.warmUsed = w, n.warmUsed+w.bytes
}

func (n *Node) warmDel(hash string) {
	n.warmUsed -= n.warm[hash].bytes
	delete(n.warm, hash)
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
		id := head.NewID()
		body, _ := head.Namespace{Format: head.Format, Namespace: id, EntityFormat: head.Format, KeyCheck: keyCheck(n.cfg.Key, id)}.Encode()
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
	if want := keyCheck(n.cfg.Key, ns.Namespace); (want == nil) != (ns.KeyCheck == nil) || want != nil && !hmac.Equal(want[:], ns.KeyCheck[:]) {
		return false, invalid("Config.Key does not match the namespace's (sealed or not: DECISIONS D36)")
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
	f, err := os.OpenFile(filepath.Join(dir, "lock"), os.O_RDWR|engine.NoFollow|flags, 0o600)
	if err != nil {
		return nil, false
	}
	if engine.Lock(f) != nil {
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

// buildBytes is the cache reserve of one genesis build.
func (n *Node) buildBytes() int64 { return n.cfg.Limits.EntityBytes + 1<<20 }

// activationBytes is the cache reserve of activating h: its live database
// file plus 1 MiB headroom (cold restore streams LTX directly without spooling).
func (n *Node) activationBytes(h *head.Head) int64 {
	return int64(h.PageCount)*head.PageSize + 1<<20
}

// fitsWith reports whether adding need bytes alongside self's current file and
// all other resident/warm/reserved bytes fits within CacheBytes.
func (n *Node) fitsWith(self *entity, need int64) bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	_, used, _ := n.scan(time.Time{}, self)
	if self != nil {
		used += self.bytes.Load()
	}
	return n.reserved+used+n.warmBytes()+n.staging.Load()+need <= n.cfg.Limits.CacheBytes
}

// cache makes room for e to become resident and reserves buildBytes for
// building its file (PROTOCOL §4 step 1, §7 cache policy); the caller
// releases the reserve with unreserve once the file is built.
func (n *Node) cache(ctx context.Context, e *entity) error {
	return n.cacheFor(ctx, e, n.buildBytes())
}

func (n *Node) cacheFor(ctx context.Context, e *entity, need int64) error {
	for i := 0; !n.room(e); i++ { // wait briefly while every resident entity is busy
		if i == 100 || pause(ctx, 10*time.Millisecond) != nil {
			return newError(CodeCapacity, "ResidentEntities entities are busy", ctx.Err())
		}
	}
	for i := 0; !n.fitFor(e, need); i++ { // wait briefly for pinned entities
		if i == 100 || pause(ctx, 10*time.Millisecond) != nil {
			n.mu.Lock()
			e.building = false
			n.mu.Unlock()
			return newError(CodeCapacity, "cache bytes held by entities in use", ctx.Err())
		}
	}
	return nil
}

// fitFor reserves need bytes for self if it fits in CacheBytes beside the other
// reserves, the files of the other resident entities, the warm tier and the
// spools of objects and blobs in flight (D37),
// deleting warm files then parking idle, settled entities, least recently
// used first, until it does. A busy
// entity is pinned and counts with its last seen size. It never blocks on an
// entity mutex.
func (n *Node) fitFor(self *entity, need int64) bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	for {
		_, used, lru := n.scan(time.Time{}, self)
		if n.reserved+used+n.warmBytes()+n.staging.Load()+need <= n.cfg.Limits.CacheBytes {
			n.reserved += need
			return true
		}
		if !n.evictWarm(self.key) && !n.shed(lru) {
			return false
		}
	}
}

// trimCache evicts warm files and parks idle resident entities (other than
// self) until accounted cache bytes plus headroom fit within CacheBytes.
func (n *Node) trimCache(self *entity, headroom int64) {
	n.mu.Lock()
	defer n.mu.Unlock()
	for {
		_, used, lru := n.scan(time.Time{}, self)
		if self != nil {
			used += self.bytes.Load()
		}
		if n.reserved+used+n.warmBytes()+n.staging.Load()+headroom <= n.cfg.Limits.CacheBytes {
			return
		}
		if !n.evictWarm(selfKey(self)) && !n.shed(lru) {
			return
		}
	}
}

func selfKey(e *entity) string {
	if e == nil {
		return ""
	}
	return e.key
}

func (n *Node) unreserve(e *entity) { n.unreserveFor(e, n.buildBytes()) }

func (n *Node) unreserveFor(e *entity, need int64) {
	n.mu.Lock()
	n.reserved -= need
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
	case errors.Is(err, engine.ErrStaleBlob):
		return newError(CodeInvalidArgument, err.Error(), nil)
	case errors.Is(err, engine.ErrReserved):
		return newError(CodeInvalidArgument, "reserved table name", err)
	}
	return newError(CodeUnavailable, "local database", err)
}

// limited bounds the storage requests in flight to Limits.InflightStorage.
// storageClass is the Stats.Storage index of a limited call.
var storageClass = map[string]int{"get": 0, "put": 1, "cas": 1, "list": 2, "delete": 3}

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
		l.busy[storageClass[op]].Add(int64(time.Since(t)))
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

func (l limited) PutFile(ctx context.Context, key string, body io.ReaderAt, size int64, sum [32]byte) error {
	return l.do(ctx, "put", key, func() error { return l.Store.PutFile(ctx, key, body, size, sum) })
}

func (l limited) Open(ctx context.Context, key string, off, n int64) (r io.ReadCloser, err error) {
	err = l.do(ctx, "get", key, func() error { r, err = l.Store.Open(ctx, key, off, n); return err }) // the slot covers the request, not the body
	return r, err
}

// Scan yields the Route of every live entity of the namespace (tombstones
// are skipped, D25) whose key hashes after afterKey's ("" from the start), in
// key-hash order, so a scan resumes
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
					if gone[i] = store.OutcomeOf(err) == store.NotFound; err != nil { // a head vanished between LIST and GET (an operator's deletion): skipped
						errs[i] = storeError(err)
						return
					}
					h, err := head.Decode(obj.Body)
					switch {
					case err != nil:
						errs[i] = newError(CodeCorrupt, k+": "+err.Error(), nil)
					case h.Namespace != n.ns || head.EntityDir(h.Key)+"/head.json" != k:
						errs[i] = newError(CodeCorrupt, k+" names another namespace or key", nil)
					case h.Deleted: // a tombstone: no entity (D25)
						gone[i] = true
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
