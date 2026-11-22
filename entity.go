package garden

import (
	"bytes"
	"cmp"
	"context"
	"crypto/sha256"
	"errors"
	"io"
	"math/rand/v2"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/OrdalieTech/big-garden/internal/engine"
	"github.com/OrdalieTech/big-garden/internal/head"
	"github.com/OrdalieTech/big-garden/internal/store"
)

// entity is the in-memory state of one key (DECISIONS D8): the cached head,
// its ETag, the connection and the unresolved candidate. pages caches the
// decoded page frames of each of h's snapshot, deltas and tail (the replay
// pages of PROTOCOL §6 step 7 and §9); it is derived from h and reset with it.
type entity struct {
	mu         gate
	rw         sync.RWMutex // separates concurrent ReadStale (RLock) from file/head swaps and mutations (Lock); always acquired inside mu
	key        string
	h          *head.Head
	etag       string
	db         *engine.DB
	unresolved *candidate
	pages      []uint32
	snap       *snapJob // an off-mutex snapshot in flight (D35), under mu
	mutating   bool     // under rw: local SQLite mutation in flight
	qmu        sync.Mutex
	queue      []*waiter    // Updates waiting on mu that a batch may take (PROTOCOL §6 Batches)
	used       time.Time    // last admission: cache policy only (ResidentEntities, IdleTimeout)
	usedUnix   atomic.Int64 // unix nanos of last ReadStale hit
	refreshing atomic.Bool  // background ReadStale revalidation in flight
	retryAfter atomic.Int64 // unix nanos before next background refresh after an error
	building   bool         // under Node.mu: between room and unreserve, counted as resident
	tried      time.Time    // end of the last settle attempt of unresolved by enter (D22 fail fast)
	validUnix  atomic.Int64 // unix nanos of issue time that last proved h current: a GET, 304 or CAS (D23)
	refs       int          // operations holding e, under Node.mu: e leaves Node.entities at 0
	waiting    atomic.Int64 // callers queued on mu (PendingPerEntity)
	readers    atomic.Int64 // the Reads among them: they do not stop a batch (D23)
	bytes      atomic.Int64 // db + wal size last seen (CacheBytes); 0 when not resident
	since      atomic.Int64 // unix nanos unresolved was set, 0 when nil (Stats.OldestUnresolved)
}

// gate is the entity mutex (D8); lock also gives up when ctx ends.
type gate chan struct{}

func (g gate) Lock()   { g <- struct{}{} }
func (g gate) Unlock() { <-g }

func (g gate) TryLock() bool {
	select {
	case g <- struct{}{}:
		return true
	default:
		return false
	}
}

func (g gate) lock(ctx context.Context) error {
	select {
	case g <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// candidate is one publication (PROTOCOL §2). pages goes with next.
type candidate struct {
	base     *head.Head
	baseETag string
	next     head.Head
	nonces   []head.ID
	object   *head.Ref
	op       *head.OpID
	more     []head.OpID // the later members of a batch (PROTOCOL §6 Batches)
	bound    head.Digest
	pages    []uint32
}

// discard closes and deletes the cache file and drops the cached head (D10).
func (e *entity) discard() {
	e.rw.Lock()
	defer e.rw.Unlock()
	e.discardLocked()
}

func (e *entity) discardLocked() {
	if e.db != nil {
		e.db.Discard()
	}
	e.clearLocked()
}

// clear drops the local image (closed) and the cached head.
func (e *entity) clear() {
	e.rw.Lock()
	defer e.rw.Unlock()
	e.clearLocked()
}

func (e *entity) clearLocked() {
	e.db, e.h, e.etag, e.pages = nil, nil, "", nil
	e.validUnix.Store(0)
	e.bytes.Store(0)
}

func (e *entity) setValid(t time.Time) {
	if t.IsZero() {
		e.validUnix.Store(0)
	} else {
		e.validUnix.Store(t.UnixNano())
	}
}

func (e *entity) validTime() time.Time {
	if u := e.validUnix.Load(); u != 0 {
		return time.Unix(0, u)
	}
	return time.Time{}
}

func (e *entity) touch() { e.usedUnix.Store(time.Now().UnixNano()) }

func (e *entity) lastUsed() time.Time {
	if u := e.usedUnix.Load(); u > 0 {
		if t := time.Unix(0, u); t.After(e.used) {
			return t
		}
	}
	return e.used
}

// roReady reports whether e holds a settled, verified local image matching
// ref that concurrent ReadStale calls can query directly; caller holds rw.RLock.
func (e *entity) roReady(ref Ref) bool {
	return !e.mutating && e.unresolved == nil && e.db != nil && e.h != nil && !e.h.Deleted &&
		e.h.Incarnation == ref.Incarnation && e.db.Published() == imageOf(e.h)
}

// current reports whether a request issued after t0 proved e.h current and
// the local image is exactly e.h (D23): a strong read or a barrier invoked
// at t0 then needs no GET of its own.
func (e *entity) current(t0 time.Time) bool {
	return e.validTime().After(t0) && e.db != nil && e.db.Published() == imageOf(e.h)
}

// links are h's snapshot, deltas and tail, in order.
func links(h *head.Head) []head.Ref {
	refs := append([]head.Ref{h.Snapshot}, h.Deltas...)
	if h.Tail != nil {
		refs = append(refs, h.Tail.Ref())
	}
	return refs
}

// resolved clears the unresolved candidate and its since.
func (e *entity) resolved() {
	e.rw.Lock()
	e.unresolved = nil
	e.rw.Unlock()
	e.since.Store(0)
}

func imageOf(h *head.Head) engine.Image {
	return engine.Image{Sequence: h.Sequence, PageCount: h.PageCount, Checksum: h.DatabaseChecksum}
}

// decodeHead strictly decodes a head of key in this namespace; inc, if not
// zero, must be its incarnation.
func (n *Node) decodeHead(b []byte, key string, inc head.ID) (*head.Head, error) {
	h, err := head.Decode(b)
	if err != nil {
		return nil, newError(CodeCorrupt, "head.json: "+err.Error(), nil)
	}
	if h.Namespace != n.ns || h.Key != key {
		return nil, newError(CodeCorrupt, "head.json names another namespace or key", nil)
	}
	if inc != (head.ID{}) && h.Incarnation != inc {
		return nil, &Error{Code: CodeIncarnationMismatch, Route: routeOf(&h), Message: "the key has another incarnation"}
	}
	return &h, nil
}

func routeOf(h *head.Head) *Route {
	r := &Route{Ref: Ref{Key: h.Key, Incarnation: h.Incarnation}, Epoch: h.Epoch, Revision: h.Revision, Bytes: int64(h.PageCount) * head.PageSize}
	if h.Owner != nil {
		r.Owner = Owner{NodeID: h.Owner.Node, Session: h.Owner.Session}
	}
	return r
}

// missing returns not_found if no head was ever published (no head.json,
// and nothing but birth.json, D27), else state_missing.
func (n *Node) missing(ctx context.Context, key string) error {
	_, err := n.st.Get(ctx, head.BirthKey(key), "", head.MaxBirthBytes)
	switch store.OutcomeOf(err) {
	case store.OK:
		if err := n.untouched(ctx, key); err != nil {
			return err
		}
		return newError(CodeNotFound, "", nil)
	case store.NotFound:
		return newError(CodeNotFound, "", nil)
	}
	return storeError(err)
}

func (n *Node) mine(h *head.Head) bool { return h.Owner != nil && *h.Owner == n.self }

func notOwner(h *head.Head) error {
	return &Error{Code: CodeNotOwner, Route: routeOf(h), Message: "another session owns the entity"}
}

// commitContext is the node-owned publication context: the caller's
// cancellation does not cancel it (PROTOCOL §2 step 3).
func (n *Node) commitContext() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), n.cfg.Limits.CommitTimeout)
}

// cas sends c's proposal as one hedged head CAS (D3). unsure is false if
// every request of it definitely did not apply (§2 step 5).
func (n *Node) cas(ctx context.Context, e *entity, c *candidate) (tag string, unsure bool, err error) {
	body, err := c.next.Encode()
	if err != nil {
		return "", false, newError(CodeCorrupt, "candidate head: "+err.Error(), nil)
	}
	failpoint("cas.before")
	tag, errs, err := store.Hedge(ctx, n.cfg.Limits.StorageAttemptTimeout/2, func(ctx context.Context) (string, error) {
		return n.st.PutCAS(ctx, head.HeadKey(e.key), body, c.baseETag)
	})
	if err == nil {
		failpoint("cas.after")
	}
	return tag, slices.ContainsFunc(errs, func(err error) bool { return store.OutcomeOf(err) != store.Failed }), err
}

// publish runs PROTOCOL §2: one CAS, then resolve on anything but OK. A nil
// error means committed; rec is the stored receipt when resolution proved
// the commit through it.
func (n *Node) publish(e *entity, c *candidate) (rec *engine.Receipt, err error) {
	if err = errors.Join(head.Successor(c.base, &c.next), c.next.Validate()); err != nil {
		e.discard()
		return nil, newError(CodeCorrupt, "candidate head: "+err.Error(), nil)
	}
	ctx, cancel := n.commitContext()
	defer cancel()
	e.rw.Lock()
	e.unresolved = c
	e.rw.Unlock()
	c.nonces = []head.ID{c.next.Nonce}
	issued := time.Now()
	e.since.Store(issued.UnixNano())
	etag, unsure, err := n.cas(ctx, e, c)
	if err == nil {
		n.settle(e, c, &c.next, etag, issued)
		if c.base != nil && c.next.Epoch != c.base.Epoch && e.db != nil { // a new owner epoch: blobs of earlier ones may be dead
			e.db.SweepDue = true
		}
		return nil, nil
	}
	if !unsure { // definitely not committed (§2 step 5)
		e.discard()
		e.resolved()
		return nil, storeError(err)
	}
	refused := store.OutcomeOf(err) != store.Unknown // every request was answered 412 or Failed: none applied
	rec, err = n.resolve(ctx, e, c)
	var pe *Error
	switch {
	case e.unresolved == nil || !errors.As(err, &pe) || pe.Code == CodeStateMissing:
	case refused && len(c.nonces) == 1 && c.op == nil:
		// the only proposal was refused and carries no operation another
		// node may have committed: definitely not committed, the reread
		// alone failed (PLAN §8.2, PROTOCOL §2 step 5)
		e.discard()
		e.resolved()
		if pe.Code == CodeOutcomeUnknown {
			err = newError(CodeUnavailable, "head unreadable after a refused publication", pe)
		}
	case pe.Code != CodeOutcomeUnknown:
		// still possibly committed: nothing but outcome_unknown may describe it (§2 rule 5)
		err = &Error{Code: CodeOutcomeUnknown, Operation: opOf(c), Message: "publication outcome unknown: " + pe.Message, cause: pe}
	}
	return rec, err
}

// settle records a committed candidate (PROTOCOL §2, after committed): the
// local image is checkpointed if it is the candidate's, and discarded if it
// is not cur's. issued is when the request that returned cur was sent.
func (n *Node) settle(e *entity, c *candidate, cur *head.Head, etag string, issued time.Time) {
	e.rw.Lock()
	e.unresolved = nil
	e.since.Store(0)
	n.countCommit(c)
	if e.db != nil && slices.Contains(c.nonces, cur.Nonce) {
		e.pages = c.pages
		if e.db.Published() != imageOf(cur) && e.db.Checkpoint(imageOf(cur)) != nil {
			e.discardLocked()
		}
	}
	if e.db != nil && e.db.Published() != imageOf(cur) {
		e.discardLocked()
	}
	h := *cur              // a copy: cur may be &c.next, and c.base chains every earlier head
	e.h, e.etag = &h, etag // kept even without a local image: the next access activates or revalidates it
	e.setValid(issued)
	if e.db != nil {
		e.bytes.Store(e.db.Bytes())
	}
	e.rw.Unlock()
	if e.db != nil {
		n.trimCache(e, min(n.cfg.Limits.EntityBytes, 1<<20))
	}
}

// resolve runs PROTOCOL §2 resolve(c) until ctx ends.
func (n *Node) resolve(ctx context.Context, e *entity, c *candidate) (*engine.Receipt, error) {
	limit := len(c.nonces) + 2 // D12: two re-proposals per call
	for i := 0; ; i++ {
		if i > 0 && pause(ctx, rand.N(10*time.Millisecond<<min(i, 7))) != nil { // full jitter, 10 ms doubling to 1 s
			return nil, &Error{Code: CodeOutcomeUnknown, Operation: opOf(c), Message: "publication outcome unknown", cause: ctx.Err()}
		}
		issued := time.Now()
		obj, err := n.st.Get(ctx, head.HeadKey(e.key), "", head.MaxHeadBytes)
		switch store.OutcomeOf(err) {
		case store.OK:
			cur, err := n.decodeHead(obj.Body, e.key, head.ID{})
			switch {
			case err != nil:
				return nil, err
			case slices.Contains(c.nonces, cur.Nonce):
				n.settle(e, c, cur, obj.ETag, issued)
				return nil, nil
			case c.base == nil || cur.Revision > c.base.Revision:
				return n.dead(ctx, e, c, cur, obj.ETag, issued)
			case cur.Nonce != c.base.Nonce:
				return nil, newError(CodeCorrupt, "head revision went backwards", nil)
			}
		case store.NotFound:
			if c.base != nil {
				return nil, newError(CodeStateMissing, "head.json disappeared during publication", nil)
			}
		default:
			continue
		}
		if len(c.nonces) < limit { // D12: re-propose with a fresh nonce under the same condition
			c.next.Nonce = head.NewID()
			c.nonces, issued = append(c.nonces, c.next.Nonce), time.Now()
			if etag, _, err := n.cas(ctx, e, c); err == nil {
				n.settle(e, c, &c.next, etag, issued)
				return nil, nil
			}
		}
	}
}

// dead settles a candidate whose every proposal can no longer apply
// (PROTOCOL §2 resolve step 3).
func (n *Node) dead(ctx context.Context, e *entity, c *candidate, cur *head.Head, etag string, issued time.Time) (*engine.Receipt, error) {
	committed := false
	var rec *engine.Receipt
	switch {
	case c.object != nil && slices.ContainsFunc(links(cur), func(r head.Ref) bool { return r.Key == c.object.Key }):
		committed = true
	case c.next.Deleted: // Delete: done iff the incarnation is gone, by this delete or another (D25)
		committed = cur.Deleted || cur.Incarnation != c.next.Incarnation
	case c.op != nil && cur.Deleted: // a tombstone holds no receipt (D25)
		if c.base == nil || cur.Revision != c.base.Revision+1 { // c may have committed, then been deleted
			e.discard()
			e.resolved()
			return nil, &Error{Code: CodeOutcomeUnknown, Operation: opOf(c), Message: "the entity was deleted during publication"}
		}
	case c.op != nil: // the tail is not an object: an inline mutation always lands here
		e.discard()
		if err := n.activate(ctx, e, cur, etag); err != nil {
			return nil, err // e.unresolved stays: a later operation resolves again
		}
		r, ok, err := e.db.Receipt(*c.op)
		m, merr := e.db.Meta()
		if err = errors.Join(err, merr); err != nil {
			e.discard()
			return nil, engineError(err)
		}
		if ok && r.InputHash != c.bound {
			e.resolved()
			return nil, &Error{Code: CodeIDConflict, Operation: opOf(c), Message: "operation ID used with another input"}
		}
		// Absent from another incarnation, c may have committed in a deleted
		// one, unless that incarnation began at the very revision c proposed.
		// A genesis of another incarnation at c's own revision lost a create race
		// with c (D27): no delete can have come between.
		deleted := cur.Incarnation != c.next.Incarnation && cur.Revision != c.next.Revision
		if !ok && (deleted || tsOf(opOf(c)) <= int64(m.FloorMs)) { // or a newer owner pruned our receipt
			e.discard()
			e.resolved()
			return nil, &Error{Code: CodeOutcomeUnknown, Operation: opOf(c), Message: "receipt floor passed or entity deleted during publication"}
		}
		committed, rec = ok, &r
	case c.object != nil: // maintenance: committed only if its object is referenced
	default: // takeover (owner set) or release (owner null)
		committed = n.mine(cur) == (c.next.Owner != nil)
	}
	if committed {
		n.settle(e, c, cur, etag, issued)
		return rec, nil
	}
	e.discard()
	e.resolved()
	switch {
	case cur.Deleted: // not committed, and the entity is gone (D25)
		return nil, n.deleted(e)
	case !n.mine(cur):
		return nil, notOwner(cur)
	}
	return nil, &Error{Code: CodeRouteChanged, Route: routeOf(cur), Message: "the head changed during publication"}
}

func opOf(c *candidate) MutationID {
	if c.op == nil {
		return MutationID{}
	}
	return MutationID(*c.op)
}

// activate restores exactly h into a new cache file (PROTOCOL §4), restarting
// with the current head if an object vanished or changed under it.
func (n *Node) activate(ctx context.Context, e *entity, h *head.Head, etag string) error {
	return n.activateMode(ctx, e, h, etag, false)
}

func (n *Node) activateMode(ctx context.Context, e *entity, h *head.Head, etag string, forRead bool) error {
	db, cur, curTag, curIssued, pages, err := n.restoreNew(ctx, e, h, etag, true, forRead)
	if err != nil {
		return err
	}
	e.rw.Lock()
	if e.db != nil {
		e.db.Discard()
	}
	e.db, e.h, e.etag, e.pages = db, cur, curTag, pages
	if curTag != etag {
		e.setValid(curIssued)
	}
	e.bytes.Store(db.Bytes())
	e.rw.Unlock()
	return nil
}

// restoreNew builds a verified *engine.DB for h (or a newer head if an object
// was collected meanwhile) in a fresh cache file, without holding e.rw so
// concurrent ReadStale calls on an existing image stay unblocked.
func (n *Node) restoreNew(ctx context.Context, e *entity, h *head.Head, etag string, tryWarm, forRead bool) (
	*engine.DB, *head.Head, string, time.Time, []uint32, error) {
	select {
	case n.recoveries <- struct{}{}:
		defer func() { <-n.recoveries }()
	case <-ctx.Done():
		return nil, nil, "", time.Time{}, nil, newError(CodeCapacity, "Recoveries activations in progress", ctx.Err())
	}
	need := n.activationBytes(h)
	// If replacing an existing resident image would exceed CacheBytes, discard
	// it only once this activation holds a recovery slot (never while queued).
	if e.db != nil && !n.fitsWith(e, need) {
		e.discard()
		tryWarm = true
	}
	if err := n.cacheFor(ctx, e, need); err != nil {
		return nil, nil, "", time.Time{}, nil, err
	}
	defer n.unreserveFor(e, need)
	if tryWarm {
		if db, pages, ok := n.warmRestore(ctx, e, h, forRead); ok {
			return db, h, etag, time.Time{}, pages, nil
		}
	}
	var issued time.Time
	for {
		path := filepath.Join(n.dir, head.NewID().String()+".db")
		db, pages, err := engine.RestoreStream(ctx, n.drv, path, n.maxPages(), h, func(r head.Ref) (io.ReadCloser, error) {
			if h.Tail != nil && r == h.Tail.Ref() {
				return io.NopCloser(bytes.NewReader(h.Tail.Data)), nil
			}
			body, err := n.st.Open(ctx, r.Key, 0, int64(r.Bytes))
			if err != nil {
				return nil, err
			}
			return &countedStream{rc: body, key: r.Key, downloaded: &n.counters.activationBytes}, nil
		})
		if err != nil && miss(err) { // collection race, or a missing or corrupt object
			issued = time.Now()
			obj, gerr := n.st.Get(ctx, head.HeadKey(e.key), etag, head.MaxHeadBytes)
			switch store.OutcomeOf(gerr) {
			case store.OK:
				if h, gerr = n.decodeHead(obj.Body, e.key, h.Incarnation); gerr != nil {
					return nil, nil, "", time.Time{}, nil, gerr
				}
				if h.Deleted { // deleted meanwhile: its refs are the dead incarnation's (D25)
					return nil, nil, "", time.Time{}, nil, n.deleted(e)
				}
				etag = obj.ETag
				continue
			case store.NotModified:
				n.counters.activationFailures.Add(1)
				if errors.Is(err, engine.ErrCorrupt) {
					return nil, nil, "", time.Time{}, nil, newError(CodeCorrupt, "object differs from its reference", err)
				}
				return nil, nil, "", time.Time{}, nil, newError(CodeStateMissing, "an object of the head is missing", nil)
			case store.NotFound:
				return nil, nil, "", time.Time{}, nil, n.missing(ctx, e.key)
			}
			return nil, nil, "", time.Time{}, nil, storeError(gerr)
		}
		if err != nil {
			var se *store.Error
			if errors.As(err, &se) {
				return nil, nil, "", time.Time{}, nil, storeError(err)
			}
			n.counters.activationFailures.Add(1)
			return nil, nil, "", time.Time{}, nil, engineError(err)
		}
		n.counters.activations.Add(1)
		return db, h, etag, issued, pages, nil
	}
}

type countedStream struct {
	rc         io.ReadCloser
	key        string
	downloaded *atomic.Uint64
}

func (c *countedStream) Read(p []byte) (int, error) {
	n, err := c.rc.Read(p)
	if n > 0 {
		c.downloaded.Add(uint64(n))
	}
	switch {
	case err == nil || err == io.EOF:
		return n, err
	case errors.Is(err, store.ErrSealBroken):
		return n, engine.ErrCorrupt
	default:
		return n, &store.Error{Outcome: store.Unknown, Op: "get", Key: c.key, Err: err}
	}
}

func (c *countedStream) Close() error { return c.rc.Close() }

// objects GETs the links refs of h, four at a time, each streamed into a
// spool (D34) and checked for its length and SHA-256. h's tail, if refs ends with it, is read from h.
// err is the first failure, a store error before NotFound or corruption;
// done removes the spools.
func (n *Node) objects(ctx context.Context, refs []head.Ref, h *head.Head, downloaded *atomic.Uint64) (_ []io.ReaderAt, done func(), _ error) {
	tail := h.Tail != nil && len(refs) > 0 && refs[len(refs)-1] == h.Tail.Ref()
	if tail {
		refs = refs[:len(refs)-1]
	}
	objs, errs, spools := make([]io.ReaderAt, len(refs), len(refs)+1), make([]error, len(refs)), make([]*spool, len(refs))
	done = func() {
		for _, s := range spools {
			s.Close()
		}
	}
	var wg sync.WaitGroup
	sem := make(chan struct{}, 4)
	for i, r := range refs {
		spools[i] = n.spool()
		wg.Go(func() {
			sem <- struct{}{}
			defer func() { <-sem }()
			body, err := n.st.Open(ctx, r.Key, 0, int64(r.Bytes))
			if err == nil {
				h := sha256.New()
				var k int64
				k, err = io.Copy(io.MultiWriter(spools[i], h), body)
				body.Close()
				switch {
				case errors.Is(err, store.ErrSealBroken):
					err = engine.ErrCorrupt
				case err != nil:
					err = &store.Error{Outcome: store.Unknown, Op: "get", Key: r.Key, Err: err}
				case uint64(k) != r.Bytes || [32]byte(h.Sum(nil)) != r.SHA256:
					err = engine.ErrCorrupt
				}
				downloaded.Add(uint64(k))
			}
			objs[i], errs[i] = spools[i].ReaderAt(), err
		})
	}
	wg.Wait()
	var err error
	for _, e := range errs {
		if e != nil && !miss(e) {
			return nil, done, e
		}
		err = cmp.Or(err, e)
	}
	if tail {
		objs = append(objs, bytes.NewReader(h.Tail.Data))
	}
	return objs, done, err
}

// warmRestore activates h from its warm file, if it has one (D21): an
// unchanged file only passes the checksum gate of a cold restore; one behind
// h gets the links after it, the first overlaid. The file leaves the warm
// tier either way; false means activate cold.
func (n *Node) warmRestore(ctx context.Context, e *entity, h *head.Head, forRead bool) (*engine.DB, []uint32, bool) {
	path := filepath.Join(n.dir, head.NewID().String()+".db")
	w, ok := n.claimWarm(e.key, path)
	if !ok {
		return nil, nil, false
	}
	refs := links(h)
	i := slices.IndexFunc(refs, func(r head.Ref) bool { return r.MaxSequence > w.Image.Sequence })
	if i < 0 {
		i = len(refs)
	}
	var pages []uint32
	pagesOK := true
	for _, r := range refs[:i] {
		p, known := w.Pages[r.SHA256]
		pagesOK = pagesOK && known
		pages = append(pages, p)
	}
	ok = pagesOK || forRead
	failpoint("warm.claim")
	if ok && i > 0 && w.Image.Sequence <= h.Sequence {
		objs, done, err := n.objects(ctx, refs[i:], h, &n.counters.activationBytes)
		defer done()
		failpoint("warm.apply.mid")   // the kill lands inside Restore's overlay
		if err != nil && !miss(err) { // the store, not the file: keep it for the next attempt
			n.unclaimWarm(e.key, path, w)
			return nil, nil, false
		}
		if err == nil {
			db, p, err := engine.Restore(ctx, n.drv, path, n.maxPages(), h, objs, &w.Image)
			if err == nil {
				n.counters.activations.Add(1)
				if i == len(refs) {
					n.counters.warmHits.Add(1)
				} else {
					n.counters.catchUps.Add(1)
				}
				var allPages []uint32
				if pagesOK {
					allPages = append(pages, p...)
				}
				n.claimWarm(e.key, "")
				return db, allPages, true
			}
		}
	}
	os.Remove(path)
	n.claimWarm(e.key, "")
	return nil, nil, false
}

// miss reports a missing or corrupt object, as opposed to a store failure.
func miss(err error) bool {
	return store.OutcomeOf(err) == store.NotFound || errors.Is(err, engine.ErrCorrupt)
}

// replay is the decoded page count of the deltas plus the tail.
func (e *entity) replay() uint64 {
	var r uint64
	for i, p := range e.pages {
		if i > 0 {
			r += uint64(p)
		}
	}
	return r
}
