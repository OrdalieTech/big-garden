package garden

import (
	"bytes"
	"cmp"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"io"
	"math"
	"os"
	"path/filepath"
	"slices"
	"time"

	"github.com/OrdalieTech/big-garden/internal/engine"
	"github.com/OrdalieTech/big-garden/internal/head"
	"github.com/OrdalieTech/big-garden/internal/store"
)

const clockSkew = 5 * time.Minute

var errResultTooLarge = errors.New("result exceeds Limits.ResultBytes")

func tsOf(id MutationID) int64 { return int64(binary.BigEndian.Uint64(id[:8])) }

// checkRequest validates the key and the shape of a request.
func checkRequest(key string, req Request) error {
	if err := head.ValidKey(key); err != nil {
		return invalid(err.Error())
	}
	if req.ID == (MutationID{}) || req.Hash == [32]byte{} || req.Schema == 0 {
		return invalid("request needs a nonzero ID, hash and schema")
	}
	return nil
}

// checkClock is the clock admission of PROTOCOL §5 step 1 and §6 step 3.
// A node clock more than clockSkew from the store's (the Date of its latest
// reply, re-sampled once with a namespace GET before refusing) refuses every
// mutation: its IDs would move the receipt floor for every node (D22).
// anyAge admits an ID older than ReceiptRetention (Create, D31).
func (n *Node) checkClock(ctx context.Context, req Request, anyAge bool) error {
	s, sampled := n.st.(limited).Store.(interface{ Skew() time.Duration })
	far := func() bool { return sampled && (s.Skew() > clockSkew || s.Skew() < -clockSkew) }
	if far() {
		n.st.Get(ctx, head.NamespaceKey, "", head.MaxNamespaceBytes)
	}
	switch now := n.now(); {
	case far():
		return newError(CodeUnavailable, "node clock differs from storage by "+s.Skew().Round(time.Second).String(), nil)
	case tsOf(req.ID) > now.Add(clockSkew).UnixMilli():
		return invalid("mutation ID time is in the future")
	case !anyAge && n.expired(req.ID):
		return &Error{Code: CodeIDExpired, Operation: req.ID, Message: "mutation ID is older than ReceiptRetention"}
	}
	return nil
}

// expired reports whether id is older than ReceiptRetention.
func (n *Node) expired(id MutationID) bool {
	return tsOf(id) <= n.now().Add(-n.cfg.Limits.ReceiptRetention).UnixMilli()
}

// binding is the request binding of PROTOCOL §1.
func binding(kind byte, from, to uint32, hash [32]byte) head.Digest {
	b := append([]byte("state-request-v1\x00"), kind)
	b = binary.BigEndian.AppendUint32(binary.BigEndian.AppendUint32(b, from), to)
	return sha256.Sum256(append(b, hash[:]...))
}

// enter returns the entity of key with its mutex held and any unresolved
// candidate settled first (PROTOCOL §2 rule 5), both within ctx. If it stays
// unresolved, the same operation ID (or every caller, for a nil id) gets
// outcome_unknown and others unavailable; a mutation whose ctx ends while
// queued gets outcome_unknown, since the candidate may be its own. A read
// waits in readers too, so that it does not stop a batch (D23).
func (n *Node) enter(ctx context.Context, key string, id *MutationID, m *waiter, read bool) (*entity, error) {
	if err := head.ValidKey(key); err != nil {
		return nil, invalid(err.Error())
	}
	e, err := n.entity(key)
	if err != nil {
		return nil, err
	}
	if e.waiting.Add(1) > int64(n.cfg.Limits.PendingPerEntity) {
		e.waiting.Add(-1)
		n.drop(e)
		return nil, newError(CodeCapacity, "too many operations queued on the entity", nil)
	}
	if read {
		e.readers.Add(1)
	}
	served, err := n.wait(ctx, e, m)
	if served { // the leader that took m counted it out of waiting
		n.drop(e)
		select {
		case <-m.done:
			return nil, errServed
		default: // a batch runs m: it may commit
			return nil, &Error{Code: CodeOutcomeUnknown, Operation: *id, Message: "deadline while in a batch", cause: err}
		}
	}
	if read {
		e.readers.Add(-1)
	}
	e.waiting.Add(-1)
	if err != nil {
		n.drop(e)
		if id == nil || *id != (MutationID{}) {
			return nil, &Error{Code: CodeOutcomeUnknown, Operation: *cmp.Or(id, &MutationID{}), Message: "deadline while queued", cause: err}
		}
		return nil, newError(CodeUnavailable, "deadline while queued", err)
	}
	e.used = time.Now()
	if n.isClosed() {
		n.leave(e)
		return nil, newError(CodeClosed, "", nil)
	}
	if c := e.unresolved; c != nil {
		// The candidate's own retry may wait CommitTimeout for the store; any
		// other operation tries for one attempt, at most once per attempt
		// timeout, then fails fast (D22). Cut short, c stays unresolved.
		own := id != nil && (c.op != nil && *c.op == head.OpID(*id) || slices.Contains(c.more, head.OpID(*id)))
		if wait := n.cfg.Limits.StorageAttemptTimeout; own || time.Since(e.tried) >= wait {
			if own {
				wait = n.cfg.Limits.CommitTimeout
			}
			ctx, cancel := context.WithTimeout(ctx, wait)
			n.resolve(ctx, e, c)
			cancel()
			e.tried = time.Now()
		}
		if e.unresolved != nil {
			n.leave(e)
			if id == nil || (c.op != nil && *c.op == head.OpID(*id)) {
				return nil, &Error{Code: CodeOutcomeUnknown, Operation: opOf(c), Message: "earlier attempt still unresolved"}
			}
			if slices.Contains(c.more, head.OpID(*id)) {
				return nil, &Error{Code: CodeOutcomeUnknown, Operation: *id, Message: "earlier attempt still unresolved"}
			}
			return nil, newError(CodeUnavailable, "an earlier publication is unresolved", nil)
		}
	}
	return e, nil
}

// owned makes e's cached head current enough and activated (PROTOCOL §3
// steps 2–4, §6 step 2). A mutation (t0 zero) needs this session's
// ownership and uses a cached head it owns as is. A read invoked at t0 needs
// a head proven current after t0 (D23), else the conditional GET, and an
// image exactly that head, owned or not (D24): it publishes nothing.
func (n *Node) owned(ctx context.Context, e *entity, ref Ref, t0 time.Time) error {
	if err := n.ownedHead(ctx, e, ref, t0); err != nil {
		return err
	}
	if t0.IsZero() && e.db != nil && e.pages == nil { // follower image lacks replay counts for mutation maintenance
		n.park(e)
	}
	if e.db != nil {
		return nil
	}
	if err := n.activateMode(ctx, e, e.h, e.etag, !t0.IsZero()); err != nil {
		return err
	}
	if t0.IsZero() && !n.mine(e.h) {
		e.discard()
		return newError(CodeRouteChanged, "the head changed during activation", nil)
	}
	return nil
}

// ownedHead is owned without the activation: the current head, owned for a
// write (Delete needs nothing more, so it works on a cold or damaged entity).
func (n *Node) ownedHead(ctx context.Context, e *entity, ref Ref, t0 time.Time) error {
	write := t0.IsZero()
	if e.h == nil || e.db == nil || write && !n.mine(e.h) || !write && !e.current(t0) {
		if err := n.fetch(ctx, e, ref.Incarnation, e.etag); err != nil {
			return err
		}
	}
	if e.h.Incarnation != ref.Incarnation {
		return &Error{Code: CodeIncarnationMismatch, Route: routeOf(e.h), Message: "the key has another incarnation"}
	}
	if e.h.Deleted { // a cached tombstone (D25)
		return n.deleted(e)
	}
	if write && !n.mine(e.h) {
		if e.h.Owner == nil || e.h.Owner.Node != n.cfg.NodeID {
			return notOwner(e.h)
		}
		if err := n.claim(e); err != nil { // D4: an earlier session of this node
			return err
		}
	}
	return nil
}

// claim takes e.h over for this session (PROTOCOL §7 step 5).
func (n *Node) claim(e *entity) error {
	next := *e.h
	self := n.self
	next.Owner, next.Epoch, next.Revision, next.Nonce = &self, next.Epoch+1, next.Revision+1, head.NewID()
	_, err := n.publish(e, &candidate{base: e.h, baseETag: e.etag, next: next, pages: e.pages})
	return err
}

// barrier revalidates the cached head (PROTOCOL §0) for an operation
// invoked at t0, with no GET if a later request proved it (D23). changed
// means the caller redoes its operation from activation once.
func (n *Node) barrier(ctx context.Context, e *entity, t0 time.Time) (changed bool, err error) {
	if e.current(t0) {
		return false, nil
	}
	issued := time.Now()
	_, err = n.st.Get(ctx, head.HeadKey(e.key), e.etag, head.MaxHeadBytes)
	switch store.OutcomeOf(err) {
	case store.NotModified:
		e.setValid(issued)
		return false, nil
	case store.OK:
		e.discard()
		return true, nil
	case store.NotFound:
		e.discard()
		return false, n.missing(ctx, e.key)
	}
	return false, storeError(err)
}

// putObject writes an immutable object; an existing one must hold body.
func (n *Node) putObject(ctx context.Context, key string, body *spool, sum [32]byte) error {
	n.staging.Add(body.n)
	defer n.staging.Add(-body.n)
	failpoint("put.before")
	failpoint("put.mid")
	var err error
	if body.f == nil {
		err = n.st.PutImmutable(ctx, key, body.buf.Bytes())
	} else {
		err = n.st.PutFile(ctx, key, body.f, body.n, sum)
	}
	failpoint("put.after")
	if store.OutcomeOf(err) == store.PreconditionFailed { // a retry that found its object: it must hold these bytes
		r, gerr := n.st.Open(ctx, key, 0, body.n+1)
		if err = gerr; err == nil {
			h := sha256.New()
			k, cerr := io.Copy(h, r)
			r.Close()
			if err = cerr; errors.Is(err, store.ErrSealBroken) || err == nil && (k != body.n || [32]byte(h.Sum(nil)) != sum) {
				return newError(CodeCorrupt, "an object key holds other bytes", nil)
			}
		}
	}
	if err != nil {
		return storeError(err)
	}
	return nil
}

// spool holds an object being built: in memory up to spoolMemory, then in a
// temporary file of the session directory, so no object costs its size in
// memory (D34). Close removes the file.
type spool struct {
	dir string
	buf bytes.Buffer
	f   *os.File
	n   int64
}

const spoolMemory = 1 << 20

func (n *Node) spool() *spool { return &spool{dir: n.dir} }

func (s *spool) Write(p []byte) (int, error) {
	if s.f == nil && s.buf.Len()+len(p) > spoolMemory {
		f, err := os.CreateTemp(s.dir, "object-*")
		if err != nil {
			return 0, err
		}
		s.f = f
		if _, err := f.Write(s.buf.Bytes()); err != nil {
			return 0, err
		}
		s.buf = bytes.Buffer{}
	}
	s.n += int64(len(p))
	if s.f != nil {
		return s.f.Write(p)
	}
	return s.buf.Write(p)
}

func (s *spool) ReaderAt() io.ReaderAt {
	if s.f != nil {
		return s.f
	}
	return bytes.NewReader(s.buf.Bytes())
}

func (s *spool) Close() {
	if s.f != nil {
		s.f.Close()
		os.Remove(s.f.Name())
		s.f = nil
	}
}

// scoped wraps a callback to enforce ResultBytes and to tell its own error
// apart from library errors.
func scoped[T any](limit int, fn func(T) ([]byte, error), cb *error) func(T) ([]byte, error) {
	return func(t T) ([]byte, error) {
		r, err := fn(t)
		if err == nil && len(r) > limit {
			err = errResultTooLarge
		}
		*cb = err
		return r, err
	}
}

// callbackError returns the caller's error unchanged, or maps a library one.
func callbackError(err, cb error) error {
	switch {
	case cb == errResultTooLarge:
		return newError(CodeCapacity, cb.Error(), nil)
	case cb != nil && err == cb:
		return err
	}
	return engineError(err)
}

// Resolve returns the current Route of key from a fresh head GET (PROTOCOL
// §3). Each call costs that GET: cache Route.Ref per key.
func (n *Node) Resolve(ctx context.Context, key string) (Route, error) {
	h, err := n.current(ctx, key, head.ID{})
	if err != nil {
		return Route{}, err
	}
	if h.Deleted {
		return Route{}, newError(CodeNotFound, "the entity was deleted", nil)
	}
	return *routeOf(h), nil
}

// current decodes key's head (of incarnation inc unless zero) from a plain
// GET, with no entity or cache: Resolve and Collect. A tombstone is returned
// as such.
func (n *Node) current(ctx context.Context, key string, inc head.ID) (*head.Head, error) {
	if err := head.ValidKey(key); err != nil {
		return nil, invalid(err.Error())
	}
	if n.isClosed() {
		return nil, newError(CodeClosed, "", nil)
	}
	obj, err := n.st.Get(ctx, head.HeadKey(key), "", head.MaxHeadBytes)
	switch store.OutcomeOf(err) {
	case store.OK:
		return n.decodeHead(obj.Body, key, inc)
	case store.NotFound:
		return nil, n.missing(ctx, key)
	}
	return nil, storeError(err)
}

// Read runs fn on the state named by a head proven current by a request
// issued after the call began (PROTOCOL §3), on any Node: ownership is not
// needed, the image is checked against that head (D24).
func (n *Node) Read(ctx context.Context, ref Ref, schema uint32, fn func(*View) ([]byte, error)) (res Result, err error) {
	defer func() { n.observe(ref.Key, true, res, err) }()
	t0 := time.Now()
	e, err := n.enter(ctx, ref.Key, &MutationID{}, nil, true)
	if err != nil {
		return Result{}, err
	}
	defer n.leave(e)
	if err := n.owned(ctx, e, ref, t0); err != nil {
		return Result{}, err
	}
	if e.h.Schema != schema {
		return Result{}, &Error{Code: CodeSchemaMismatch, At: Version{e.h.Incarnation, e.h.Sequence}, Schema: e.h.Schema,
			Message: "stored schema differs"}
	}
	var cb error
	v, err := e.db.Read(scoped(math.MaxInt, func(v *engine.View) ([]byte, error) { return fn(&View{v}) }, &cb)) // stores nothing: unbounded (D22)
	if err != nil {
		return Result{}, callbackError(err, cb)
	}
	return Result{Value: v, Version: Version{e.h.Incarnation, e.h.Sequence}}, nil
}

// ReadStale runs fn on a local read-only snapshot of ref without waiting on S3
// or taking ownership. Concurrent ReadStale calls execute in parallel on a
// read-only SQLite connection pool. When half of maxAge has elapsed, a single
// background refresh checks S3 (under the Node's lifetime context, independent
// of any request deadline) and downloads only new deltas or a replacement
// snapshot one recovery at a time while reads keep serving the already-local
// revision. If the entity is not local yet, ReadStale starts background
// activation and returns CodeUnavailable immediately. A maxAge <= 0 runs a
// strong Read.
func (n *Node) ReadStale(ctx context.Context, ref Ref, schema uint32, maxAge time.Duration, fn func(*View) ([]byte, error)) (res Result, err error) {
	if maxAge <= 0 {
		return n.Read(ctx, ref, schema, fn)
	}
	defer func() { n.observe(ref.Key, true, res, err) }()
	if err := head.ValidKey(ref.Key); err != nil {
		return Result{}, invalid(err.Error())
	}
	e, err := n.entity(ref.Key)
	if err != nil {
		return Result{}, err
	}
	defer n.drop(e)
	e.rw.RLock()
	if !e.roReady(ref) {
		e.rw.RUnlock()
		if !n.hasWarmHead(ref.Key, ref.Incarnation) || !n.attachWarm(ctx, e, ref) {
			n.refreshAsync(e, ref, maxAge)
			return Result{}, newError(CodeUnavailable, "entity activation in progress", nil)
		}
		e.rw.RLock()
		if !e.roReady(ref) {
			e.rw.RUnlock()
			n.refreshAsync(e, ref, maxAge)
			return Result{}, newError(CodeUnavailable, "entity activation in progress", nil)
		}
	}
	valid := e.validTime()
	age := time.Duration(0)
	if !valid.IsZero() {
		age = time.Since(valid)
	}
	if valid.IsZero() || age >= maxAge/2 {
		n.refreshAsync(e, ref, maxAge)
	}
	if e.h.Schema != schema {
		ver, got := Version{e.h.Incarnation, e.h.Sequence}, e.h.Schema
		e.rw.RUnlock()
		return Result{}, &Error{Code: CodeSchemaMismatch, At: ver, Schema: got, Message: "stored schema differs"}
	}
	behind := e.behindSince.Load()
	syncing := behind != 0
	stale := age
	if syncing {
		stale = max(time.Since(time.Unix(0, behind)), 0)
	}
	db, ver := e.db, Version{e.h.Incarnation, e.h.Sequence}
	e.touch()
	var cb error
	v, err := db.ReadConcurrent(ctx, scoped(math.MaxInt, func(v *engine.View) ([]byte, error) { return fn(&View{v}) }, &cb))
	e.rw.RUnlock()
	if err != nil {
		return Result{}, callbackError(err, cb)
	}
	return Result{Value: v, Version: ver, Stale: stale, Syncing: syncing}, nil
}

// attachWarm opens key's warm-tier database file for immediate ReadStale
// serving without scanning the whole file or waiting on S3; background
// refreshAsync then verifies it against the current head.
func (n *Node) attachWarm(ctx context.Context, e *entity, ref Ref) bool {
	if e.mu.lock(ctx) != nil {
		return false
	}
	defer e.mu.Unlock()
	if n.isClosed() {
		return false
	}
	e.rw.RLock()
	ready := e.roReady(ref)
	unres := e.unresolved != nil
	e.rw.RUnlock()
	if ready {
		return true
	}
	if unres || !n.room(e) {
		return false
	}
	defer func() {
		n.mu.Lock()
		e.building = false
		n.mu.Unlock()
	}()
	path := filepath.Join(n.dir, head.NewID().String()+".db")
	w, ok := n.claimWarm(e.key, path)
	if !ok {
		return false
	}
	if w.Head == nil || w.Head.Namespace != n.ns || w.Head.Key != e.key || w.Head.Deleted || w.Head.Incarnation != ref.Incarnation {
		n.unclaimWarm(e.key, path, w)
		return false
	}
	db, err := engine.OpenWarm(ctx, n.drv, path, n.maxPages(), w.Head)
	if err != nil {
		os.Remove(path)
		n.claimWarm(e.key, "")
		return false
	}
	refs := links(w.Head)
	var pages []uint32
	pagesOK := true
	for _, r := range refs {
		p, known := w.Pages[r.SHA256]
		pagesOK = pagesOK && known
		pages = append(pages, p)
	}
	if !pagesOK {
		pages = nil
	}
	h := *w.Head
	e.rw.Lock()
	if e.db != nil {
		e.db.Discard()
	}
	e.db, e.h, e.etag, e.pages = db, &h, w.ETag, pages
	e.setValid(time.Time{})
	e.behindSince.Store(0)
	e.bytes.Store(db.Bytes())
	e.touch()
	e.rw.Unlock()
	n.writeSidecar(e)
	n.counters.warmHits.Add(1)
	n.claimWarm(e.key, "")
	return true
}

func (n *Node) refreshAsync(e *entity, ref Ref, maxAge time.Duration) {
	if retry := e.retryAfter.Load(); retry > 0 && time.Now().UnixNano() < retry {
		return
	}
	if !e.refreshing.CompareAndSwap(false, true) {
		return
	}
	n.mu.Lock()
	if n.closed {
		n.mu.Unlock()
		e.refreshing.Store(false)
		return
	}
	n.wg.Add(1)
	e.refs++
	n.mu.Unlock()
	go func() {
		defer n.wg.Done()
		defer e.refreshing.Store(false)
		ctx, cancel := n.lifeContext()
		defer cancel()
		if e.mu.lock(ctx) != nil {
			n.drop(e)
			return
		}
		defer n.leave(e)
		if n.isClosed() || e.unresolved != nil {
			return
		}
		err := n.syncFollower(ctx, e, ref)
		if ge, ok := hasCode(err, CodeIncarnationMismatch); ok && ge.Route != nil {
			e.rw.RLock()
			hasDB := e.db != nil
			e.rw.RUnlock()
			if hasDB {
				e.behindSince.CompareAndSwap(0, time.Now().UnixNano())
			}
			if err = n.syncFollower(ctx, e, ge.Route.Ref); err == nil {
				n.setRef(e.key, ge.Route.Ref)
			}
		} else if _, ok := hasCode(err, CodeNotFound); ok {
			n.forgetRef(e.key)
		}
		if err != nil {
			backoff := min(max(maxAge/2, 20*time.Millisecond), time.Second)
			e.retryAfter.Store(time.Now().Add(backoff).UnixNano())
		} else {
			e.retryAfter.Store(0)
		}
	}()
}

// syncFollower revalidates e's head and catches its local file up to cur
// while keeping e.rw unlocked during all S3 downloads and cold restores so
// concurrent ReadStale callers never wait on S3. Caller holds e.mu.
func (n *Node) syncFollower(ctx context.Context, e *entity, ref Ref) error {
	e.rw.RLock()
	oldH, oldTag, oldDB := e.h, e.etag, e.db
	e.rw.RUnlock()
	issued := time.Now()
	obj, err := n.st.Get(ctx, head.HeadKey(e.key), oldTag, head.MaxHeadBytes)
	switch store.OutcomeOf(err) {
	case store.NotModified:
		if oldDB != nil && oldH != nil && oldH.Incarnation == ref.Incarnation && !oldH.Deleted && oldDB.Published() == imageOf(oldH) {
			e.behindSince.Store(0)
			e.setValid(issued)
			return nil
		}
		// Cached head without a matching local DB: re-fetch full head.
		issued = time.Now()
		obj, err = n.st.Get(ctx, head.HeadKey(e.key), "", head.MaxHeadBytes)
		if err != nil {
			if store.OutcomeOf(err) == store.NotFound {
				e.discard()
				return n.missing(ctx, e.key)
			}
			return storeError(err)
		}
	case store.OK:
	case store.NotFound:
		e.discard()
		return n.missing(ctx, e.key)
	default:
		return storeError(err)
	}
	cur, err := n.decodeHead(obj.Body, e.key, ref.Incarnation)
	if err != nil {
		return err
	}
	if oldH != nil && cur.Revision < oldH.Revision {
		n.counters.staleHeads.Add(1)
		return newError(CodeUnavailable, "the store returned an older head than one already seen", nil)
	}
	if cur.Deleted {
		return n.deleted(e)
	}
	// Case A: data unchanged (only owner/compaction changed).
	if oldDB != nil && oldDB.Published() == imageOf(cur) {
		e.rw.Lock()
		if oldH != nil && !slices.Equal(links(oldH), links(cur)) {
			e.pages = nil
		}
		e.h, e.etag = cur, obj.ETag
		e.behindSince.Store(0)
		e.setValid(issued)
		e.rw.Unlock()
		n.writeSidecar(e)
		return nil
	}
	if oldDB != nil {
		e.behindSince.CompareAndSwap(0, time.Now().UnixNano())
	}
	// Case B: resident DB behind cur; download only the new delta suffix outside
	// e.rw (serialized by n.recoveries), then take e.rw briefly to apply pages in place.
	if oldDB != nil && oldDB.Published().Sequence < cur.Sequence {
		refs := links(cur)
		i := slices.IndexFunc(refs, func(r head.Ref) bool { return r.MaxSequence > oldDB.Published().Sequence })
		if i > 0 && i < len(refs) {
			select {
			case n.recoveries <- struct{}{}:
			case <-ctx.Done():
				return newError(CodeCapacity, "Recoveries activations in progress", ctx.Err())
			}
			objs, done, oerr := n.objects(ctx, refs[i:], cur, &n.counters.activationBytes)
			if oerr == nil {
				oerr = engine.CheckObjects(refs[i:], objs)
			}
			if oerr == nil {
				e.rw.Lock()
				if n.isClosed() || ctx.Err() != nil {
					e.rw.Unlock()
					done()
					<-n.recoveries
					return storeError(cmp.Or(ctx.Err(), context.Canceled))
				}
				n.removeSidecar(oldDB)
				// All delta bytes are already local and verified; finish in-place apply
				// without cancelling on shutdown so SIGTERM never destroys a resident DB.
				newPages, cerr := oldDB.CatchUp(context.Background(), n.maxPages(), cur, refs[i:], objs)
				if cerr == nil {
					if e.pages != nil && oldH != nil && len(links(oldH)) >= i && slices.Equal(links(oldH)[:i], refs[:i]) {
						e.pages = append(slices.Clone(e.pages[:i]), newPages...)
					} else {
						e.pages = nil
					}
					e.h, e.etag = cur, obj.ETag
					e.behindSince.Store(0)
					e.setValid(issued)
					e.bytes.Store(oldDB.Bytes())
					n.counters.activations.Add(1)
					n.counters.catchUps.Add(1)
					e.rw.Unlock()
					n.writeSidecar(e)
					done()
					<-n.recoveries
					return nil
				}
				// CatchUp already removed the dirty file.
				e.clearLocked()
				oldDB = nil
				e.rw.Unlock()
			}
			done()
			<-n.recoveries
			if oerr != nil && !miss(oerr) {
				return storeError(oerr)
			}
		}
	}
	// Case C: cold or warm restore into a fresh file outside e.rw, then swap.
	// restoreNew acquires n.recoveries and discards oldDB only if needed to fit CacheBytes.
	db, gotH, gotTag, gotIssued, pages, err := n.restoreNew(ctx, e, cur, obj.ETag, oldDB == nil, true)
	if err != nil {
		return err
	}
	if !gotIssued.IsZero() {
		issued = gotIssued
	}
	e.rw.Lock()
	if e.db != nil {
		e.db.Discard()
	}
	e.db, e.h, e.etag, e.pages = db, gotH, gotTag, pages
	e.behindSince.Store(0)
	e.setValid(issued)
	e.bytes.Store(db.Bytes())
	e.rw.Unlock()
	n.writeSidecar(e)
	return nil
}

// Lookup returns the stored outcome of id in freshly validated state, on
// any Node, like Read.
func (n *Node) Lookup(ctx context.Context, ref Ref, id MutationID) (res Result, err error) {
	defer func() { n.observe(ref.Key, true, res, err) }()
	t0 := time.Now()
	e, err := n.enter(ctx, ref.Key, &id, nil, false)
	if err != nil {
		return Result{}, err
	}
	defer n.leave(e)
	return n.lookup(ctx, e, ref, id, t0)
}

// lookup is Lookup invoked at t0.
func (n *Node) lookup(ctx context.Context, e *entity, ref Ref, id MutationID, t0 time.Time) (Result, error) {
	if err := n.owned(ctx, e, ref, t0); err != nil {
		return Result{}, err
	}
	r, ok, err := e.db.Receipt(head.OpID(id))
	m, merr := e.db.Meta()
	if err = errors.Join(err, merr); err != nil {
		e.discard()
		return Result{}, engineError(err)
	}
	at := Version{e.h.Incarnation, e.h.Sequence}
	switch {
	case ok:
		return Result{Value: r.Result, Version: Version{e.h.Incarnation, r.Sequence}, Replayed: true}, nil
	case tsOf(id) <= int64(m.FloorMs):
		return Result{}, &Error{Code: CodeIDExpired, Operation: id, At: at, Message: "receipt floor passed"}
	}
	return Result{}, &Error{Code: CodeAbsentAtVersion, Operation: id, At: at}
}

// Create makes a new entity at key (PROTOCOL §5).
func (n *Node) Create(ctx context.Context, key string, req Request, fn func(*Tx) ([]byte, error)) (Ref, Result, error) {
	if ctx == nil || fn == nil {
		return Ref{}, Result{}, invalid("nil context or callback")
	}
	if err := cmp.Or(checkRequest(key, req), n.checkClock(ctx, req, true)); err != nil {
		return Ref{}, Result{}, err
	}
	bound := binding(1, req.Schema, req.Schema, req.Hash)
	var born []Blob // the spooled blobs the callback names (D33)
	ref, res, err := n.create(ctx, key, req, bound, func(path string, inc head.ID) (*engine.DB, []byte, error) {
		born = born[:0]
		db, err := engine.Create(ctx, n.drv, path, n.maxPages(), n.ns, key, inc)
		if err != nil {
			return nil, nil, engineError(err)
		}
		var cb error
		built := false
		defer func() { // also on a callback panic
			if !built {
				db.Discard()
			}
		}()
		failpoint("sql.before")
		fresh := func(object string) bool {
			return slices.ContainsFunc(born, func(b Blob) bool { return b.f.Object == object })
		}
		result, _, err := db.Mutate(engine.Commit{ID: head.OpID(req.ID), Bound: bound, Schema: req.Schema, Fresh: fresh},
			scoped(n.cfg.Limits.ResultBytes, func(t *engine.Tx) ([]byte, error) { failpoint("sql.inside"); return fn(&Tx{t, &born}) }, &cb))
		if err != nil {
			return nil, nil, callbackError(err, cb)
		}
		failpoint("sql.after")
		built = true
		return db, result, nil
	}, func(ctx context.Context, inc head.ID) error { // after birth.json: only this Request resumes what it leaves
		for _, b := range born {
			if err := n.putSpooled(ctx, head.BlobPrefix(key, inc)+b.f.Object, b.spool, b.f); err != nil {
				return err
			}
		}
		return nil
	})
	if err == nil {
		n.mu.Lock()
		for _, b := range born {
			n.dropUnborn(b.spool)
		}
		n.mu.Unlock()
	}
	return ref, res, err
}

func (n *Node) maxPages() uint32 { return uint32(n.cfg.Limits.EntityBytes / head.PageSize) }

// builder builds the local genesis image of incarnation inc at path and
// returns it with the operation's result (PROTOCOL §5 step 4).
type builder func(path string, inc head.ID) (*engine.DB, []byte, error)

// create runs PROTOCOL §5 for Create (kind 1) and Import (kind 4).
func (n *Node) create(ctx context.Context, key string, req Request, bound head.Digest, build builder,
	upload func(context.Context, head.ID) error) (ref Ref, res Result, err error) {
	defer func() { n.observe(key, false, res, err) }()
	e, err := n.enter(ctx, key, &req.ID, nil, false)
	if err != nil {
		return Ref{}, Result{}, err
	}
	defer n.leave(e)
	t0 := time.Now()
	obj, err := n.st.Get(ctx, head.HeadKey(key), "", head.MaxHeadBytes)
	var tomb *head.Head // recreation (D25): genesis of a new incarnation by a CAS on the tombstone
	switch store.OutcomeOf(err) {
	case store.OK:
		if tomb, err = n.decodeHead(obj.Body, key, head.ID{}); err != nil || !tomb.Deleted {
			return n.createExists(ctx, e, req, bound, tomb, err, t0)
		}
		if n.expired(req.ID) { // it may have made the deleted incarnation: never recreate under it (D31)
			return Ref{}, Result{}, &Error{Code: CodeIDExpired, Operation: req.ID, Message: "mutation ID is older than ReceiptRetention"}
		}
		if err := n.reap(ctx, tomb); err != nil { // the deleted incarnation's objects, if its Delete was cut
			return Ref{}, Result{}, err
		}
	case store.NotFound:
	default:
		return Ref{}, Result{}, storeError(err)
	}
	e.discard()
	for {
		var birth *head.Birth
		var inc head.ID
		if tomb == nil {
			birth, err = n.birth(ctx, key)
		}
		switch {
		case tomb != nil: // birth.json is the first incarnation's and stays; no birth-without-head window (D5)
			inc = head.NewID()
		case err != nil:
			return Ref{}, Result{}, err
		case birth != nil && birth.OperationID == head.OpID(req.ID) && birth.InputHash == bound:
			if err := n.resumable(ctx, key, birth.Incarnation); err != nil {
				return Ref{}, Result{}, err
			}
			inc = birth.Incarnation
		case birth != nil && birth.OperationID == head.OpID(req.ID):
			return Ref{}, Result{}, &Error{Code: CodeIDConflict, Operation: req.ID, Message: "operation ID used with another input"}
		case birth != nil: // D27: an interrupted create that left no object is superseded, in a new incarnation
			if err := n.untouched(ctx, key); err != nil {
				return Ref{}, Result{}, err
			}
			inc = head.NewID()
		default:
			page, _, err := n.st.List(ctx, head.EntityDir(key)+"/", "", 1)
			if err != nil {
				return Ref{}, Result{}, storeError(err)
			}
			if len(page) > 0 {
				if b, err := n.birth(ctx, key); err == nil && b != nil {
					continue // a concurrent create wrote it after our GET: decide again on it
				}
				return Ref{}, Result{}, newError(CodeStateMissing, "entity artifacts exist without birth.json", nil)
			}
			inc = head.NewID()
		}
		// PROTOCOL §5 steps 4–8
		if err := n.cache(ctx, e); err != nil {
			return Ref{}, Result{}, err
		}
		db, result, err := func() (*engine.DB, []byte, error) {
			defer n.unreserve(e) // also on a callback panic
			db, result, err := build(filepath.Join(n.dir, head.NewID().String()+".db"), inc)
			if err == nil {
				e.bytes.Store(db.Bytes()) // resident from here on, for the other entities' room
			}
			return db, result, err
		}()
		if err != nil {
			return Ref{}, Result{}, err
		}
		e.rw.Lock()
		e.db = db
		e.mutating = true
		e.rw.Unlock()
		defer func() {
			e.rw.Lock()
			e.mutating = false
			e.rw.Unlock()
		}()
		op := head.OpID(req.ID)
		buf := n.spool()
		defer buf.Close()
		snap, err := db.Snapshot(buf)
		if err != nil {
			e.discard()
			return Ref{}, Result{}, engineError(err)
		}
		failpoint("capture")
		// From the birth attempt on, birth.json may exist, and only this request
		// can resume it (D5): a failure is outcome_unknown, never a code that
		// invites a retry under another ID (§5 step 5).
		unknown := func(err error) (Ref, Result, error) {
			var pe *Error
			if tomb == nil && errors.As(err, &pe) && (pe.Code == CodeUnavailable || pe.Code == CodeCorrupt || pe.Code == CodeCapacity) {
				err = &Error{Code: CodeOutcomeUnknown, Operation: req.ID, Message: "create outcome unknown: " + pe.Message, cause: pe}
			}
			return Ref{}, Result{}, withOp(err, req.ID)
		}
		if birth == nil && tomb == nil {
			attempt := head.NewID()
			body, err := head.Birth{Format: head.Format, Namespace: n.ns, Key: key, Incarnation: inc, OperationID: op,
				InputHash: bound, Schema: req.Schema, Attempt: attempt}.Encode()
			if err == nil {
				err = n.st.PutImmutable(ctx, head.BirthKey(key), body)
			}
			if store.OutcomeOf(err) == store.PreconditionFailed {
				b, berr := n.birth(ctx, key)
				switch {
				case berr != nil:
					err = berr
				case b == nil:
					err = newError(CodeUnavailable, "birth.json not readable after a conflict", nil)
				case b.Attempt == attempt:
					err = nil
				case b.OperationID == op && b.InputHash == bound:
					e.discard()
					continue // the same request won the birth: restart with it
				default:
					err = newError(CodeStateMissing, "another create won the key", nil)
				}
			}
			if err != nil {
				e.discard()
				return unknown(storeError(err))
			}
		}
		if upload != nil { // after birth.json: only this request can resume what it leaves
			if err := upload(ctx, inc); err != nil {
				e.discard()
				return unknown(err)
			}
		}
		ref := snap.Ref
		c := candidate{next: head.Head{Revision: 1, Epoch: 1}, object: &ref, op: &op, bound: bound, pages: []uint32{snap.Pages}}
		if tomb != nil { // revision and epoch continue across incarnations; the snapshot is produced against the tombstone
			c.base, c.baseETag, ref.BaseRevision = tomb, obj.ETag, tomb.Revision
			c.next.Revision, c.next.Epoch = tomb.Revision+1, tomb.Epoch+1
		}
		ref.Key = head.ObjectKey(key, inc, ref.BaseRevision, head.NewID(), ref.SHA256)
		if err := n.putObject(ctx, ref.Key, buf, ref.SHA256); err != nil {
			e.discard()
			return unknown(err)
		}
		buf.Close()
		self := n.self
		c.next = head.Head{Format: head.Format, Namespace: n.ns, Key: key, Incarnation: inc, Revision: c.next.Revision, Nonce: head.NewID(),
			Owner: &self, Epoch: c.next.Epoch, Sequence: snap.Ref.MaxSequence, Schema: req.Schema, Codec: head.Codec, PageSize: head.PageSize,
			PageCount: snap.PageCount, DatabaseChecksum: ref.PostChecksum, Snapshot: ref, Deltas: []head.Ref{}}
		rec, err := n.publish(e, &c)
		if tomb == nil && lostGenesis(err) { // another create published genesis first (D27)
			return n.answer(ctx, e, key, req, bound, t0)
		}
		if err != nil {
			return unknown(err)
		}
		if rec != nil { // another genesis carrying our receipt
			return Ref{key, e.h.Incarnation}, Result{Value: rec.Result, Version: Version{e.h.Incarnation, rec.Sequence}}, nil
		}
		return Ref{key, inc}, Result{Value: result, Version: Version{inc, c.next.Sequence}}, nil
	}
}

// createExists answers a Create (invoked at t0) whose key has a live head
// h, decoded with err (§5 step 2): the receipt of its ID in the current
// state, a replay or id_conflict by bound, else exists. The receipt,
// not birth.json, decides: a recreation's create is not in birth.json (D25).
func (n *Node) createExists(ctx context.Context, e *entity, req Request, bound head.Digest, h *head.Head, err error, t0 time.Time) (Ref, Result, error) {
	if err != nil {
		return Ref{}, Result{}, err
	}
	ref := Ref{e.key, h.Incarnation}
	if err := n.owned(ctx, e, ref, t0); err != nil {
		return Ref{}, Result{}, err
	}
	r, ok, err := e.db.Receipt(head.OpID(req.ID))
	switch {
	case err != nil:
		e.discard()
		return Ref{}, Result{}, engineError(err)
	case ok && r.InputHash == bound:
		return ref, Result{Value: r.Result, Version: Version{h.Incarnation, r.Sequence}, Replayed: true}, nil
	case ok:
		return Ref{}, Result{}, &Error{Code: CodeIDConflict, Operation: req.ID, Message: "operation ID used with another input"}
	}
	return Ref{}, Result{}, &Error{Code: CodeExists, Route: routeOf(e.h), Message: "key exists"}
}

// birth reads birth.json; nil if absent.
func (n *Node) birth(ctx context.Context, key string) (*head.Birth, error) {
	obj, err := n.st.Get(ctx, head.BirthKey(key), "", head.MaxBirthBytes)
	switch store.OutcomeOf(err) {
	case store.NotFound:
		return nil, nil
	case store.OK:
		b, err := head.DecodeBirth(obj.Body)
		if err != nil || b.Namespace != n.ns || b.Key != key {
			return nil, newError(CodeCorrupt, "birth.json failed validation", err)
		}
		return &b, nil
	}
	return nil, storeError(err)
}

// resumable is the D5 check: no object of a used head may exist.
func (n *Node) resumable(ctx context.Context, key string, inc head.ID) error {
	prefix := head.ObjectPrefix(key, inc)
	for after := ""; ; {
		page, more, err := n.st.List(ctx, head.EntityDir(key)+"/objects/", after, 1000)
		if err != nil {
			return storeError(err)
		}
		for _, o := range page {
			if base, _, _, err := head.ParseObjectKey(prefix, o.Key); err != nil || base > 0 {
				return newError(CodeStateMissing, "the entity was used before its head disappeared", nil)
			}
			after = o.Key
		}
		if !more || len(page) == 0 {
			return nil
		}
	}
}

// untouched is the D27 check: an interrupted create that left no object never
// published a head, so another request may take the key. A snapshot left may
// be a genesis whose head was published, then lost (D14): only its request
// resumes it (D5).
func (n *Node) untouched(ctx context.Context, key string) error {
	for _, dir := range []string{"/objects/", "/blobs/"} { // an import uploads its blobs before genesis (D33)
		page, _, err := n.st.List(ctx, head.EntityDir(key)+dir, "", 1)
		if err != nil {
			return storeError(err)
		}
		if len(page) > 0 {
			return newError(CodeStateMissing, "birth.json and objects exist without head.json", nil)
		}
	}
	return nil
}

// lostGenesis is a genesis publication that found another genesis.
func lostGenesis(err error) bool {
	var pe *Error
	return errors.As(err, &pe) && (pe.Code == CodeRouteChanged || pe.Code == CodeNotOwner)
}

// answer is a create's reply from the key's current head (§5 step 2).
func (n *Node) answer(ctx context.Context, e *entity, key string, req Request, bound head.Digest, t0 time.Time) (Ref, Result, error) {
	obj, err := n.st.Get(ctx, head.HeadKey(key), "", head.MaxHeadBytes)
	if err != nil {
		return Ref{}, Result{}, storeError(err)
	}
	h, err := n.decodeHead(obj.Body, key, head.ID{})
	return n.createExists(ctx, e, req, bound, h, err, t0)
}

func withOp(err error, id MutationID) error {
	var e *Error
	if errors.As(err, &e) && e.Operation == (MutationID{}) {
		e.Operation = id
	}
	return err
}

// Update runs fn as one mutation of ref (PROTOCOL §6), perhaps in a batch.
func (n *Node) Update(ctx context.Context, ref Ref, req Request, fn func(*Tx) ([]byte, error)) (Result, error) {
	return n.mutate(ctx, ref, req, 2, req.Schema, fn)
}

// Migrate moves the schema from Request.Schema to the uint32 argument.
func (n *Node) Migrate(ctx context.Context, ref Ref, req Request, to uint32, fn func(*Tx) error) (Result, error) {
	if to <= req.Schema {
		return Result{}, invalid("Migrate must raise the schema")
	}
	return n.mutate(ctx, ref, req, 3, to, func(tx *Tx) ([]byte, error) { return nil, fn(tx) })
}

// mutate is Update (kind 2) and Migrate (kind 3, schema req.Schema -> to).
func (n *Node) mutate(ctx context.Context, ref Ref, req Request, kind byte, to uint32, fn func(*Tx) ([]byte, error)) (res Result, err error) {
	defer func() { n.observe(ref.Key, false, res, err) }()
	if err := checkRequest(ref.Key, req); err != nil {
		return Result{}, err
	}
	bound, t0 := binding(kind, req.Schema, to, req.Hash), time.Now()
	var m *waiter
	if kind == 2 && n.cfg.Limits.BatchMax > 1 {
		m = &waiter{ctx: ctx, req: req, bound: bound, fn: fn, done: make(chan struct{})}
	}
	e, err := n.enter(ctx, ref.Key, &req.ID, m, false)
	if err == errServed {
		if m.panic != nil {
			panic(m.panic)
		}
		return m.res, m.err
	}
	if err != nil {
		return Result{}, err
	}
	defer n.leave(e)
	for redo := 0; redo < 2; redo++ {
		if err := n.owned(ctx, e, ref, time.Time{}); err != nil { // a redo follows a discard: it GETs
			return Result{}, withOp(err, req.ID)
		}
		if err := n.checkClock(ctx, req, false); err != nil { // admission starts with the clock (§6 step 3)
			return Result{}, err
		}
		res, verdict := n.admit(e, req, bound)
		var cb error
		var seq uint64
		var result []byte
		if verdict == nil && res == nil {
			e.rw.Lock()
			e.db.CloseReaders()
			e.mutating = true
			e.rw.Unlock()
			defer func() {
				e.rw.Lock()
				e.mutating = false
				e.rw.Unlock()
			}()
			n.removeSidecar(e.db)
			floor := n.now().Add(-n.cfg.Limits.ReceiptRetention).UnixMilli()
			failpoint("sql.before")
			result, seq, err = e.db.Mutate(engine.Commit{ID: head.OpID(req.ID), Bound: bound, Schema: to, FloorMs: uint64(floor), Fresh: n.fresh(e)},
				scoped(n.cfg.Limits.ResultBytes, func(t *engine.Tx) ([]byte, error) { failpoint("sql.inside"); return fn(&Tx{t: t}) }, &cb))
			if err != nil && (cb == nil || cb == errResultTooLarge) {
				e.discard()
				return Result{}, withOp(callbackError(err, cb), req.ID)
			}
			verdict = err
		}
		if verdict != nil || res != nil { // a state-dependent answer: barrier first
			changed, err := n.barrier(ctx, e, t0)
			if err != nil {
				return Result{}, withOp(err, req.ID)
			}
			if changed {
				continue
			}
			if res != nil {
				return *res, nil
			}
			return Result{}, verdict
		}
		failpoint("sql.after")
		var batch []*waiter
		if m != nil {
			if batch, err = n.collect(e); err != nil {
				e.discard()
				err = newError(CodeUnavailable, "a batch member failed locally", err)
				n.deliver(e, batch, nil, err)
				return Result{}, withOp(err, req.ID)
			}
		}
		return n.publishUpdate(ctx, e, req, bound, to, result, seq, batch)
	}
	return Result{}, &Error{Code: CodeRouteChanged, Operation: req.ID, Message: "the head changed twice during the operation"}
}

// admit runs the state-dependent admission checks of PROTOCOL §6 step 3.
func (n *Node) admit(e *entity, req Request, bound head.Digest) (*Result, error) {
	m, err := e.db.Meta()
	if err != nil {
		return nil, engineError(err)
	}
	if tsOf(req.ID) <= int64(m.FloorMs) {
		return nil, &Error{Code: CodeIDExpired, Operation: req.ID, Message: "receipt floor passed"}
	}
	r, ok, err := e.db.Receipt(head.OpID(req.ID))
	switch {
	case err != nil:
		return nil, engineError(err)
	case ok && r.InputHash == bound:
		return &Result{Value: r.Result, Version: Version{e.h.Incarnation, r.Sequence}, Replayed: true}, nil
	case ok:
		return nil, &Error{Code: CodeIDConflict, Operation: req.ID, Message: "operation ID used with another input"}
	case m.Schema != req.Schema:
		return nil, &Error{Code: CodeSchemaMismatch, Operation: req.ID, At: Version{e.h.Incarnation, e.h.Sequence}, Schema: m.Schema,
			Message: "stored schema differs"}
	}
	return nil, nil
}

// publishUpdate captures and publishes a locally committed mutation
// (PROTOCOL §6 steps 6–10).
func (n *Node) publishUpdate(ctx context.Context, e *entity, req Request, bound head.Digest, schema uint32, result []byte, seq uint64,
	batch []*waiter) (Result, error) {
	h, op, last := e.h, head.OpID(req.ID), seq
	c := &candidate{base: h, baseETag: e.etag, op: &op, bound: bound}
	for _, m := range batch {
		if m.commit {
			last, c.more = m.res.Version.Sequence, append(c.more, head.OpID(m.req.ID))
		}
	}
	if len(c.more) > 0 { // the shortest member's deadline must not abort the others (PLAN §6.5)
		var cancel context.CancelFunc
		ctx, cancel = n.commitContext()
		defer cancel()
	}
	fail := func(err error) (Result, error) {
		e.discard()
		err = withOp(err, req.ID)
		n.deliver(e, batch, nil, err)
		return Result{}, err
	}
	buf := n.spool()
	defer func() { buf.Close() }()
	delta, err := e.db.Delta(buf)
	if err != nil {
		return fail(engineError(err))
	}
	failpoint("capture")
	next := *h
	next.Revision, next.Nonce, next.Sequence, next.Schema = h.Revision+1, head.NewID(), last, schema
	next.PageCount, next.DatabaseChecksum = delta.PageCount, delta.Ref.PostChecksum
	if len(h.Deltas) == head.MaxDeltas || e.replay()+uint64(delta.Pages) > 2*uint64(delta.PageCount)+256 {
		buf.Close() // snapshot on cap (§6 step 7)
		buf = n.spool()
		snap, err := e.db.Snapshot(buf)
		if err != nil {
			return fail(engineError(err))
		}
		ref := placed(h, snap.Ref)
		next.Snapshot, next.Deltas, next.Tail, c.object, c.pages = ref, []head.Ref{}, nil, &ref, []uint32{snap.Pages}
		if err := n.putObject(ctx, ref.Key, buf, ref.SHA256); err != nil {
			return fail(err)
		}
		buf.Close()
	} else {
		t, tbuf := delta, buf
		if h.Tail != nil {
			out := n.spool()
			defer out.Close()
			t, err = engine.Compact(ctx, out, []head.Ref{h.Tail.Ref(), delta.Ref},
				[]io.ReaderAt{bytes.NewReader(h.Tail.Data), buf.ReaderAt()})
			if err != nil {
				return fail(engineError(err))
			}
			tbuf = out
		}
		c.pages = append(slices.Clone(e.pages[:1+len(h.Deltas)]), t.Pages) // the tail, or the new last delta
		uploaded := slices.ContainsFunc(links(h), func(r head.Ref) bool { return r.BaseRevision >= 1 })
		if t.Ref.Bytes <= head.InlineBytes && uploaded && n.cfg.Key == nil { // a head never carries sealed data (D36)
			r := t.Ref
			next.Tail = &head.Tail{SHA256: r.SHA256, Bytes: r.Bytes, MinSequence: r.MinSequence, MaxSequence: r.MaxSequence,
				PreChecksum: r.PreChecksum, PostChecksum: r.PostChecksum, Data: tbuf.buf.Bytes()} // in memory: within spoolMemory
		} else {
			ref := placed(h, t.Ref)
			next.Deltas, next.Tail, c.object = append(slices.Clip(h.Deltas), ref), nil, &ref
			if err := n.putObject(ctx, ref.Key, tbuf, ref.SHA256); err != nil {
				return fail(err)
			}
			tbuf.Close()
			buf.Close()
		}
	}
	c.next = next
	rec, err := n.publish(e, c)
	n.deliver(e, batch, rec, err)
	if err != nil {
		return Result{}, withOp(err, req.ID)
	}
	n.maintainLater(e)
	if rec != nil {
		return Result{Value: rec.Result, Version: Version{e.h.Incarnation, rec.Sequence}}, nil
	}
	return Result{Value: result, Version: Version{h.Incarnation, seq}}, nil
}

// waiter is an Update queued on a busy entity that a batch may take
// (PROTOCOL §6 Batches). The batch leader takes it off entity.queue under
// entity.qmu, runs the callback, and closes done once res, err (or panic)
// are final.
type waiter struct {
	ctx    context.Context
	req    Request
	bound  head.Digest
	fn     func(*Tx) ([]byte, error)
	commit bool // its transaction committed locally: it is in the publication
	done   chan struct{}
	res    Result
	err    error
	panic  any
}

var (
	errServed = errors.New("garden: served by a batch")
	errPanic  = errors.New("garden: callback panicked")
)

// wait locks e.mu within ctx. With m, a batch leader may take m instead:
// served then reports that m's outcome is (or will be) set by the leader.
func (n *Node) wait(ctx context.Context, e *entity, m *waiter) (served bool, err error) {
	if m == nil {
		return false, e.mu.lock(ctx)
	}
	e.qmu.Lock()
	e.queue = append(e.queue, m)
	e.qmu.Unlock()
	locked := false
	select {
	case e.mu <- struct{}{}:
		locked = true
	case <-m.done:
	case <-ctx.Done():
		err = ctx.Err()
	}
	e.qmu.Lock()
	defer e.qmu.Unlock()
	if i := slices.Index(e.queue, m); i >= 0 {
		e.queue = slices.Delete(e.queue, i, i+1)
		return false, err
	}
	if locked { // taken: the leader released mu only after closing done
		e.mu.Unlock()
	}
	return true, err
}

// collect runs, after the leader's own transaction, the transactions of the
// Updates queued on e in FIFO order, up to BatchMax members in all and only
// while every other waiting operation is a Read (PLAN §6.5: never pass a
// queued migration or release; a queued read rides the batch's CAS, D23).
// Each member keeps its outcome until the publication resolves; an error
// means a library failure: discard all.
func (n *Node) collect(e *entity) (batch []*waiter, err error) {
	for len(batch)+1 < n.cfg.Limits.BatchMax {
		var m *waiter
		e.qmu.Lock()
		if len(e.queue) > 0 && e.waiting.Load() <= int64(len(e.queue))+e.readers.Load() {
			m, e.queue = e.queue[0], e.queue[1:]
			e.waiting.Add(-1)
		}
		e.qmu.Unlock()
		if m == nil {
			return batch, nil
		}
		batch = append(batch, m)
		if m.err = m.ctx.Err(); m.err != nil { // its caller already got outcome_unknown
			continue
		}
		if m.err = n.checkClock(m.ctx, m.req, false); m.err != nil {
			continue
		}
		if res, verdict := n.admit(e, m.req, m.bound); res != nil || verdict != nil {
			m.res, m.err = *cmp.Or(res, &Result{}), verdict
			continue
		}
		n.removeSidecar(e.db)
		var cb error
		floor := n.now().Add(-n.cfg.Limits.ReceiptRetention).UnixMilli()
		result, seq, err := e.db.Mutate(engine.Commit{ID: head.OpID(m.req.ID), Bound: m.bound, Schema: m.req.Schema, FloorMs: uint64(floor), Fresh: n.fresh(e)},
			scoped(n.cfg.Limits.ResultBytes, func(t *engine.Tx) (r []byte, err error) {
				defer func() {
					if p := recover(); p != nil {
						m.panic, err = p, errPanic
					}
				}()
				failpoint("sql.member")
				return m.fn(&Tx{t: t})
			}, &cb))
		switch {
		case err == nil:
			m.res, m.commit = Result{Value: result, Version: Version{e.h.Incarnation, seq}}, true
		case cb != nil && err == cb: // rolled back alone; held until the publication resolves
			m.err = callbackError(err, cb)
		default:
			return batch, engineError(err)
		}
	}
	return batch, nil
}

// deliver gives each batch member its outcome once the publication resolved
// (PROTOCOL §6 Batches). rec non-nil means resolution proved the leader's
// receipt, perhaps committed by another publication: each member is then
// checked by its own receipt. Any outcome that observed unpublished state
// becomes outcome_unknown or unavailable unless the publication committed.
func (n *Node) deliver(e *entity, batch []*waiter, rec *engine.Receipt, perr error) {
	var pe *Error
	errors.As(perr, &pe)
	for _, m := range batch {
		id := m.req.ID
		switch {
		case perr == nil && rec != nil && m.err == nil:
			m.res, m.err = n.verify(e, m)
		case perr == nil && rec == nil:
		case pe != nil && pe.Code != CodeOutcomeUnknown && m.commit && pe.Code != CodeIDConflict:
			m.res, m.err = Result{}, &Error{Code: pe.Code, Operation: id, Route: pe.Route, Message: pe.Message, cause: perr}
		case perr == nil || (pe != nil && pe.Code != CodeOutcomeUnknown):
			m.res, m.err = Result{}, &Error{Code: CodeUnavailable, Operation: id, Message: "the batch was not published", cause: cmp.Or(perr, m.err)}
		default:
			m.res, m.err = Result{}, &Error{Code: CodeOutcomeUnknown, Operation: id, Message: "batch publication outcome unknown", cause: perr}
		}
		close(m.done)
	}
}

// verify checks a member by its receipt in e's current image (deliver).
func (n *Node) verify(e *entity, m *waiter) (Result, error) {
	id := m.req.ID
	if e.db == nil {
		return Result{}, &Error{Code: CodeOutcomeUnknown, Operation: id, Message: "batch member not verified"}
	}
	r, ok, err := e.db.Receipt(head.OpID(id))
	meta, merr := e.db.Meta()
	switch {
	case errors.Join(err, merr) != nil:
		return Result{}, &Error{Code: CodeOutcomeUnknown, Operation: id, Message: "batch member not verified", cause: errors.Join(err, merr)}
	case ok && r.InputHash == m.bound:
		return Result{Value: r.Result, Version: Version{e.h.Incarnation, r.Sequence}, Replayed: m.res.Replayed}, nil
	case ok:
		return Result{}, &Error{Code: CodeIDConflict, Operation: id, Message: "operation ID used with another input"}
	case tsOf(id) <= int64(meta.FloorMs):
		return Result{}, &Error{Code: CodeOutcomeUnknown, Operation: id, Message: "receipt floor passed during publication"}
	}
	return Result{}, &Error{Code: CodeRouteChanged, Operation: id, Route: routeOf(e.h), Message: "the batch was not published"}
}
