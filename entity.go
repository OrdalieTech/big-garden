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
	key        string
	h          *head.Head
	etag       string
	db         *engine.DB
	unresolved *candidate
	pages      []uint32
	qmu        sync.Mutex
	queue      []*waiter    // Updates waiting on mu that a batch may take (PROTOCOL §6 Batches)
	used       time.Time    // last admission: cache policy only (ResidentEntities, IdleTimeout)
	building   bool         // under Node.mu: between room and unreserve, counted as resident
	tried      time.Time    // end of the last settle attempt of unresolved by enter (D22 fail fast)
	valid      time.Time    // issue time of the request that last proved h current: a GET, 304 or CAS (D23)
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
	if e.db != nil {
		e.db.Discard()
	}
	e.clear()
}

// clear drops the local image (closed) and the cached head.
func (e *entity) clear() {
	e.db, e.h, e.etag, e.pages, e.valid = nil, nil, "", nil, time.Time{}
	e.bytes.Store(0)
}

// current reports whether a request issued after t0 proved e.h current and
// the local image is exactly e.h (D23): a strong read or a barrier invoked
// at t0 then needs no GET of its own.
func (e *entity) current(t0 time.Time) bool {
	return e.valid.After(t0) && e.db != nil && e.db.Published() == imageOf(e.h)
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
func (e *entity) resolved() { e.unresolved = nil; e.since.Store(0) }

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
	r := &Route{Ref: Ref{Key: h.Key, Incarnation: h.Incarnation}, Epoch: h.Epoch, Revision: h.Revision}
	if h.Owner != nil {
		r.Owner = Owner{NodeID: h.Owner.Node, Session: h.Owner.Session}
	}
	return r
}

// missing returns state_missing if birth.json exists, else not_found.
func (n *Node) missing(ctx context.Context, key string) error {
	_, err := n.st.Get(ctx, head.BirthKey(key), "", head.MaxBirthBytes)
	switch store.OutcomeOf(err) {
	case store.OK:
		return newError(CodeStateMissing, "birth.json exists without head.json", nil)
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
	e.unresolved, c.nonces = c, []head.ID{c.next.Nonce}
	issued := time.Now()
	e.since.Store(issued.UnixNano())
	etag, unsure, err := n.cas(ctx, e, c)
	if err == nil {
		n.settle(e, c, &c.next, etag, issued)
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
	e.resolved()
	n.countCommit(c)
	if e.db != nil && slices.Contains(c.nonces, cur.Nonce) {
		e.pages = c.pages
		if e.db.Published() != imageOf(cur) && e.db.Checkpoint(imageOf(cur)) != nil {
			e.discard()
		}
	}
	if e.db != nil && e.db.Published() != imageOf(cur) {
		e.discard()
	}
	h := *cur                               // a copy: cur may be &c.next, and c.base chains every earlier head
	e.h, e.etag, e.valid = &h, etag, issued // kept even without a local image: the next access activates or revalidates it
	if e.db != nil {
		e.bytes.Store(e.db.Bytes())
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
	case c.object != nil && slices.ContainsFunc(append([]head.Ref{cur.Snapshot}, cur.Deltas...),
		func(r head.Ref) bool { return r.Key == c.object.Key }):
		committed = true
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
		if !ok && tsOf(opOf(c)) <= int64(m.FloorMs) { // a newer owner may have pruned our receipt
			e.discard()
			e.resolved()
			return nil, &Error{Code: CodeOutcomeUnknown, Operation: opOf(c), Message: "receipt floor passed during publication"}
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
	if !n.mine(cur) {
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
	select {
	case n.recoveries <- struct{}{}:
		defer func() { <-n.recoveries }()
	case <-ctx.Done():
		return newError(CodeCapacity, "Recoveries activations in progress", ctx.Err())
	}
	if err := n.cache(ctx, e); err != nil {
		return err
	}
	defer n.unreserve(e)
	if n.warmStart(ctx, e, h, etag) {
		return nil
	}
	for {
		objs, err := n.objects(ctx, links(h), h)
		if err != nil && !miss(err) {
			return storeError(err)
		}
		if err != nil { // collection race, or a missing or corrupt object
			obj, gerr := n.st.Get(ctx, head.HeadKey(e.key), etag, head.MaxHeadBytes)
			switch store.OutcomeOf(gerr) {
			case store.OK:
				if h, gerr = n.decodeHead(obj.Body, e.key, h.Incarnation); gerr != nil {
					return gerr
				}
				etag, e.valid = obj.ETag, time.Time{} // h is no longer the head e.valid proved
				continue
			case store.NotModified:
				if errors.Is(err, engine.ErrCorrupt) {
					return newError(CodeCorrupt, "object differs from its reference", nil)
				}
				return newError(CodeStateMissing, "an object of the head is missing", nil)
			case store.NotFound:
				return n.missing(ctx, e.key)
			}
			return storeError(gerr)
		}
		path := filepath.Join(n.dir, head.NewID().String()+".db")
		db, pages, err := engine.Restore(ctx, n.drv, path, n.maxPages(), h, objs, nil)
		if err != nil {
			n.counters.activationFailures.Add(1)
			return engineError(err)
		}
		n.counters.activations.Add(1)
		e.db, e.h, e.etag, e.pages = db, h, etag, pages
		e.bytes.Store(db.Bytes())
		return nil
	}
}

// objects GETs the links refs of h, four at a time, checking each one's
// length and SHA-256; h's tail, if refs ends with it, is read from h. err is
// the first failure, a store error before NotFound or corruption.
func (n *Node) objects(ctx context.Context, refs []head.Ref, h *head.Head) ([]io.ReaderAt, error) {
	tail := h.Tail != nil && len(refs) > 0 && refs[len(refs)-1] == h.Tail.Ref()
	if tail {
		refs = refs[:len(refs)-1]
	}
	objs, errs := make([]io.ReaderAt, len(refs), len(refs)+1), make([]error, len(refs))
	var wg sync.WaitGroup
	sem := make(chan struct{}, 4)
	for i, r := range refs {
		wg.Go(func() {
			sem <- struct{}{}
			defer func() { <-sem }()
			obj, err := n.st.Get(ctx, r.Key, "", int64(r.Bytes))
			if err == nil && (uint64(len(obj.Body)) != r.Bytes || sha256.Sum256(obj.Body) != r.SHA256) {
				err = engine.ErrCorrupt
			}
			objs[i], errs[i] = bytes.NewReader(obj.Body), err
			n.counters.activationBytes.Add(uint64(len(obj.Body)))
		})
	}
	wg.Wait()
	var err error
	for _, e := range errs {
		if e != nil && !miss(e) {
			return nil, e
		}
		err = cmp.Or(err, e)
	}
	if tail {
		objs = append(objs, bytes.NewReader(h.Tail.Data))
	}
	return objs, err
}

// warmStart activates e at h from its warm file, if it has one (D21): an
// unchanged file only passes the checksum gate of a cold restore; one behind
// h gets the links after it, the first overlaid. The file leaves the warm
// tier either way; false means activate cold.
func (n *Node) warmStart(ctx context.Context, e *entity, h *head.Head, etag string) bool {
	path := filepath.Join(n.dir, head.NewID().String()+".db")
	w, ok := n.claimWarm(e.key, path)
	if !ok {
		return false
	}
	refs := links(h)
	i := slices.IndexFunc(refs, func(r head.Ref) bool { return r.MaxSequence > w.Image.Sequence })
	if i < 0 {
		i = len(refs)
	}
	var pages []uint32
	for _, r := range refs[:i] {
		p, known := w.Pages[r.SHA256]
		ok = ok && known
		pages = append(pages, p)
	}
	failpoint("warm.claim")
	if ok && i > 0 && w.Image.Sequence <= h.Sequence {
		objs, err := n.objects(ctx, refs[i:], h)
		failpoint("warm.apply.mid")   // the kill lands inside Restore's overlay
		if err != nil && !miss(err) { // the store, not the file: keep it for the next attempt
			n.unclaimWarm(e.key, path, w)
			return false
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
				e.db, e.h, e.etag, e.pages = db, h, etag, append(pages, p...)
				e.bytes.Store(db.Bytes())
				n.claimWarm(e.key, "")
				return true
			}
		}
	}
	os.Remove(path)
	n.claimWarm(e.key, "")
	return false
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
