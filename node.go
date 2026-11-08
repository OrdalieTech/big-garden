package garden

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
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
	ns   head.ID
	self head.Owner
	dir  string   // session cache dir
	lock *os.File // holds the flock on dir
	now  func() time.Time

	mu         sync.Mutex
	entities   map[string]*entity
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

// Open validates cfg and opens a Node on its namespace (PROTOCOL §8).
func Open(ctx context.Context, cfg Config) (*Node, error) {
	cfg, err := cfg.validate()
	if err != nil {
		return nil, err
	}
	return open(ctx, cfg, store.NewS3(cfg.S3, cfg.Bucket, cfg.Prefix, cfg.Limits.StorageAttemptTimeout))
}

// open runs PROTOCOL §8 steps 2–4 for a validated cfg on st.
func open(ctx context.Context, cfg Config, st store.Store) (*Node, error) {
	n := &Node{cfg: cfg, st: limited{st, make(chan struct{}, cfg.Limits.InflightStorage)}, entities: map[string]*entity{}, now: time.Now,
		recoveries: make(chan struct{}, cfg.Limits.Recoveries)}
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
			n.evict(now.Add(-n.cfg.Limits.IdleTimeout), 0, nil)
		}
	}
}

// room makes room for self to become resident under ResidentEntities,
// waiting briefly while every resident entity is busy.
func (n *Node) room(ctx context.Context, self *entity) bool {
	for i := 0; i < 100; i++ {
		if n.evict(time.Time{}, n.cfg.Limits.ResidentEntities, self) {
			return true
		}
		if pause(ctx) != nil {
			return false
		}
	}
	return false
}

// pause waits 10 ms or until ctx ends.
func pause(ctx context.Context) error {
	t := time.NewTimer(10 * time.Millisecond)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// evict discards resident, idle, settled entities last used before cutoff.
// With keep > 0 it instead discards the least recently used one if at
// least keep entities other than self are resident (busy ones count), and
// reports whether there is room for self. It never blocks on an entity mutex.
func (n *Node) evict(cutoff time.Time, keep int, self *entity) bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	resident, lru := 0, (*entity)(nil)
	var lruUsed time.Time
	for _, e := range n.entities {
		if e == self {
			continue
		}
		if !e.mu.TryLock() {
			if e.bytes.Load() > 0 { // busy: resident only with a file
				resident++
			}
			continue
		}
		if e.db != nil && e.unresolved == nil && keep == 0 && e.used.Before(cutoff) {
			e.discard()
		}
		n.forget(e)
		if e.db != nil {
			resident++
			if e.unresolved == nil && (lru == nil || e.used.Before(lruUsed)) {
				lru, lruUsed = e, e.used
			}
		}
		e.mu.Unlock()
	}
	if keep == 0 || resident < keep {
		return true
	}
	if lru == nil || !lru.mu.TryLock() {
		return false
	}
	defer lru.mu.Unlock()
	if lru.db == nil || lru.unresolved != nil {
		return false
	}
	lru.discard()
	return true
}

func (n *Node) namespace(ctx context.Context) error {
	for range 2 {
		obj, err := n.st.Get(ctx, head.NamespaceKey, "", 1<<10)
		if err == nil {
			ns, err := head.DecodeNamespace(obj.Body)
			if err != nil {
				return newError(CodeCorrupt, "namespace.json: "+err.Error(), nil)
			}
			n.ns = ns.Namespace
			return nil
		}
		if store.OutcomeOf(err) != store.NotFound {
			return storeError(err)
		}
		page, _, err := n.st.List(ctx, "entities/", "", 1)
		if err != nil {
			return storeError(err)
		}
		if len(page) > 0 {
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

// leave ends an operation on e: it drops a head this session does not own
// (the next access GETs it anyway), unlocks e and drops the reference.
func (n *Node) leave(e *entity) {
	if e.unresolved == nil && e.h != nil && !n.mine(e.h) {
		e.discard()
	}
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
	if !n.room(ctx, e) {
		return newError(CodeCapacity, "ResidentEntities entities are busy", ctx.Err())
	}
	for i := 0; !n.fit(e); i++ { // wait briefly for pinned entities
		if i == 100 || pause(ctx) != nil {
			return newError(CodeCapacity, "cache bytes held by entities in use", ctx.Err())
		}
	}
	return nil
}

// fit reserves buildBytes for self if it fits in CacheBytes beside the other
// reserves and the files of the other resident entities, discarding idle,
// settled entities least recently admitted first until it does. A busy
// entity is pinned and counts with its last seen size. It never blocks on an
// entity mutex.
func (n *Node) fit(self *entity) bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	for {
		used, lru := n.reserved, (*entity)(nil)
		var lruUsed time.Time
		for _, e := range n.entities {
			if e == self || !e.mu.TryLock() {
				used += e.bytes.Load()
				continue
			}
			if e.db != nil {
				e.bytes.Store(e.db.Bytes())
				if e.unresolved == nil && (lru == nil || e.used.Before(lruUsed)) {
					lru, lruUsed = e, e.used
				}
			}
			used += e.bytes.Load()
			e.mu.Unlock()
		}
		if used+n.buildBytes() <= n.cfg.Limits.CacheBytes {
			n.reserved += n.buildBytes()
			return true
		}
		if lru == nil || !lru.mu.TryLock() {
			return false
		}
		if lru.db == nil || lru.unresolved != nil {
			lru.mu.Unlock()
			return false
		}
		lru.discard()
		lru.mu.Unlock()
	}
}

func (n *Node) unreserve() {
	n.mu.Lock()
	n.reserved -= n.buildBytes()
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
		e.discard()
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
	}
	return newError(CodeUnavailable, "local database", err)
}

// limited bounds the storage requests in flight to Limits.InflightStorage.
type limited struct {
	store.Store
	sem chan struct{}
}

func (l limited) acquire(ctx context.Context, op, key string) error {
	select {
	case l.sem <- struct{}{}:
		return nil
	case <-ctx.Done():
		return &store.Error{Outcome: store.Unknown, Op: op, Key: key, Err: ctx.Err()} // never sent
	}
}

func (l limited) Get(ctx context.Context, key, ifNoneMatch string, max int64) (store.Object, error) {
	if err := l.acquire(ctx, "get", key); err != nil {
		return store.Object{}, err
	}
	defer func() { <-l.sem }()
	return l.Store.Get(ctx, key, ifNoneMatch, max)
}

func (l limited) PutImmutable(ctx context.Context, key string, body []byte) error {
	if err := l.acquire(ctx, "put", key); err != nil {
		return err
	}
	defer func() { <-l.sem }()
	return l.Store.PutImmutable(ctx, key, body)
}

func (l limited) PutCAS(ctx context.Context, key string, body []byte, ifMatch string) (string, error) {
	if err := l.acquire(ctx, "cas", key); err != nil {
		return "", err
	}
	defer func() { <-l.sem }()
	return l.Store.PutCAS(ctx, key, body, ifMatch)
}

func (l limited) List(ctx context.Context, prefix, after string, limit int) ([]store.Entry, bool, error) {
	if err := l.acquire(ctx, "list", prefix); err != nil {
		return nil, false, err
	}
	defer func() { <-l.sem }()
	return l.Store.List(ctx, prefix, after, limit)
}

func (l limited) Delete(ctx context.Context, key string) error {
	if err := l.acquire(ctx, "delete", key); err != nil {
		return err
	}
	defer func() { <-l.sem }()
	return l.Store.Delete(ctx, key)
}
