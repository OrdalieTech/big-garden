package garden

import (
	"bytes"
	"cmp"
	"context"
	"crypto/sha256"
	"errors"
	"io"
	"math/rand/v2"
	"path/filepath"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/OrdalieTech/big-garden/internal/engine"
	"github.com/OrdalieTech/big-garden/internal/head"
	"github.com/OrdalieTech/big-garden/internal/store"
)

const maxHeadBytes = 64 << 10

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
	refs       int          // operations holding e, under Node.mu: e leaves Node.entities at 0
	waiting    atomic.Int64 // callers queued on mu (PendingPerEntity)
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
	unsure   bool // some proposal got an answer other than a definite failure (§2 step 5)
}

// discard closes and deletes the cache file and drops the cached head (D10).
func (e *entity) discard() {
	if e.db != nil {
		e.db.Discard()
	}
	e.db, e.h, e.etag, e.pages = nil, nil, "", nil
	e.bytes.Store(0)
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
	r := &Route{Ref: Ref{Key: h.Key, Incarnation: h.Incarnation}, Epoch: h.Epoch, Revision: h.Revision}
	if h.Owner != nil {
		r.Owner = Owner{NodeID: h.Owner.Node, Session: h.Owner.Session}
	}
	return r
}

// missing returns state_missing if birth.json exists, else not_found.
func (n *Node) missing(ctx context.Context, key string) error {
	_, err := n.st.Get(ctx, head.BirthKey(key), "", 4<<10)
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

// cas sends c's proposal as one hedged head CAS (D3). c.unsure is set unless
// every request of it definitely did not apply.
func (n *Node) cas(ctx context.Context, e *entity, c *candidate) (string, error) {
	body, err := c.next.Encode()
	if err != nil {
		return "", newError(CodeCorrupt, "candidate head: "+err.Error(), nil)
	}
	failpoint("cas.before")
	tag, errs, err := store.Hedge(ctx, n.cfg.Limits.StorageAttemptTimeout/2, func(ctx context.Context) (string, error) {
		return n.st.PutCAS(ctx, head.HeadKey(e.key), body, c.baseETag)
	})
	if err == nil {
		failpoint("cas.after")
	}
	c.unsure = c.unsure || slices.ContainsFunc(errs, func(err error) bool { return store.OutcomeOf(err) != store.Failed })
	return tag, err
}

// publish runs PROTOCOL §2: one CAS, then resolve on anything but OK. A nil
// error means committed; rec is the stored receipt when resolution proved
// the commit through it.
func (n *Node) publish(e *entity, c *candidate) (rec *engine.Receipt, err error) {
	if c.base == nil {
		err = head.CheckGenesis(&c.next)
	} else {
		err = head.Successor(c.base, &c.next)
	}
	if err = errors.Join(err, c.next.Validate()); err != nil {
		e.discard()
		return nil, newError(CodeCorrupt, "candidate head: "+err.Error(), nil)
	}
	ctx, cancel := n.commitContext()
	defer cancel()
	e.unresolved, c.nonces = c, []head.ID{c.next.Nonce}
	e.since.Store(time.Now().UnixNano())
	etag, err := n.cas(ctx, e, c)
	if err == nil {
		n.settle(e, c, &c.next, etag)
		return nil, nil
	}
	if !c.unsure { // definitely not committed (§2 step 5)
		e.discard()
		e.unresolved = nil
		e.since.Store(0)
		return nil, storeError(err)
	}
	rec, err = n.resolve(ctx, e, c)
	var pe *Error
	if e.unresolved != nil && errors.As(err, &pe) && pe.Code != CodeOutcomeUnknown && pe.Code != CodeStateMissing {
		// still possibly committed: nothing but outcome_unknown may describe it (§2 rule 5)
		err = &Error{Code: CodeOutcomeUnknown, Operation: opOf(c), Message: "publication outcome unknown: " + pe.Message, cause: pe}
	}
	return rec, err
}

// settle records a committed candidate (PROTOCOL §2, after committed): the
// local image is checkpointed if it is the candidate's, and discarded if it
// is not cur's.
func (n *Node) settle(e *entity, c *candidate, cur *head.Head, etag string) {
	e.unresolved = nil
	e.since.Store(0)
	n.countCommit(c)
	if e.db != nil && slices.Contains(c.nonces, cur.Nonce) {
		e.pages = c.pages
		o := engine.Object{Ref: head.Ref{MaxSequence: cur.Sequence, PostChecksum: cur.DatabaseChecksum}, PageCount: cur.PageCount}
		if e.db.Published() != imageOf(cur) && e.db.Checkpoint(o) != nil {
			e.discard()
		}
	}
	if e.db != nil && e.db.Published() != imageOf(cur) {
		e.discard()
	}
	e.h, e.etag = cur, etag // kept even without a local image: the next access activates or revalidates it
	if e.db != nil {
		e.bytes.Store(e.db.Bytes())
	}
}

// resolve runs PROTOCOL §2 resolve(c) until ctx ends.
func (n *Node) resolve(ctx context.Context, e *entity, c *candidate) (*engine.Receipt, error) {
	limit := len(c.nonces) + 2 // D12: two re-proposals per call
	for i := 0; ; i++ {
		if i > 0 && sleep(ctx, i) != nil {
			return nil, &Error{Code: CodeOutcomeUnknown, Operation: opOf(c), Message: "publication outcome unknown", cause: ctx.Err()}
		}
		obj, err := n.st.Get(ctx, head.HeadKey(e.key), "", maxHeadBytes)
		var cur *head.Head
		switch store.OutcomeOf(err) {
		case store.OK:
			if cur, err = n.decodeHead(obj.Body, e.key, head.ID{}); err != nil {
				return nil, err
			}
		case store.NotFound:
			if c.base != nil {
				return nil, newError(CodeStateMissing, "head.json disappeared during publication", nil)
			}
		default:
			continue
		}
		if cur != nil && slices.Contains(c.nonces, cur.Nonce) {
			n.settle(e, c, cur, obj.ETag)
			return nil, nil
		}
		if cur != nil && (c.base == nil || cur.Revision > c.base.Revision) {
			return n.dead(ctx, e, c, cur, obj.ETag)
		}
		if cur != nil && cur.Nonce != c.base.Nonce {
			return nil, newError(CodeCorrupt, "head revision went backwards", nil)
		}
		if len(c.nonces) < limit { // D12: re-propose with a fresh nonce under the same condition
			c.next.Nonce = head.NewID()
			c.nonces = append(c.nonces, c.next.Nonce)
			if etag, err := n.cas(ctx, e, c); err == nil {
				n.settle(e, c, &c.next, etag)
				return nil, nil
			}
		}
	}
}

// dead settles a candidate whose every proposal can no longer apply
// (PROTOCOL §2 resolve step 3).
func (n *Node) dead(ctx context.Context, e *entity, c *candidate, cur *head.Head, etag string) (*engine.Receipt, error) {
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
			e.unresolved = nil
			e.since.Store(0)
			return nil, &Error{Code: CodeIDConflict, Operation: opOf(c), Message: "operation ID used with another input"}
		}
		if !ok && tsOf(opOf(c)) <= int64(m.FloorMs) { // a newer owner may have pruned our receipt
			e.discard()
			e.unresolved = nil
			e.since.Store(0)
			return nil, &Error{Code: CodeOutcomeUnknown, Operation: opOf(c), Message: "receipt floor passed during publication"}
		}
		committed, rec = ok, &r
	case c.object != nil: // maintenance: committed only if its object is referenced
	case c.next.Owner != nil: // takeover
		committed = n.mine(cur)
	default: // release
		committed = !n.mine(cur)
	}
	if committed {
		n.settle(e, c, cur, etag)
		return rec, nil
	}
	e.discard()
	e.unresolved = nil
	e.since.Store(0)
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

// sleep waits a full-jitter backoff (10 ms doubling to 1 s) or until ctx ends.
func sleep(ctx context.Context, i int) error {
	t := time.NewTimer(rand.N(10 * time.Millisecond << min(i, 7)))
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
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
	defer n.unreserve()
	for {
		refs := append([]head.Ref{h.Snapshot}, h.Deltas...)
		objs := make([]io.ReaderAt, len(refs), len(refs)+1)
		errs := make([]error, len(refs))
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
			})
		}
		wg.Wait()
		var err error
		for _, e := range errs {
			if e != nil && store.OutcomeOf(e) != store.NotFound && !errors.Is(e, engine.ErrCorrupt) {
				return storeError(e)
			}
			err = cmp.Or(err, e)
		}
		if err != nil { // collection race, or a missing or corrupt object
			obj, gerr := n.st.Get(ctx, head.HeadKey(e.key), etag, maxHeadBytes)
			switch store.OutcomeOf(gerr) {
			case store.OK:
				if h, gerr = n.decodeHead(obj.Body, e.key, h.Incarnation); gerr != nil {
					return gerr
				}
				etag = obj.ETag
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
		if h.Tail != nil {
			objs = append(objs, bytes.NewReader(h.Tail.Data))
		}
		path := filepath.Join(n.dir, head.NewID().String()+".db")
		db, err := engine.Restore(ctx, path, n.maxPages(), h, objs)
		if err != nil {
			n.counters.activationFailures.Add(1)
			return engineError(err)
		}
		n.counters.activations.Add(1)
		e.db, e.h, e.etag = db, h, etag
		e.pages = db.Pages
		e.bytes.Store(db.Bytes())
		return nil
	}
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
