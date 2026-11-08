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
	"time"

	"github.com/OrdalieTech/big-garden/internal/engine"
	"github.com/OrdalieTech/big-garden/internal/head"
	"github.com/OrdalieTech/big-garden/internal/store"
)

const maxHeadBytes = 64 << 10

// entity is the in-memory state of one key (DECISIONS D8): the cached head,
// its ETag, the connection and the unresolved candidate. replay and tail
// cache the decoded page counts of the head's deltas plus tail, and of the
// tail alone (PROTOCOL §6 step 7); they are derived from h and reset with it.
type entity struct {
	mu         sync.Mutex
	key        string
	h          *head.Head
	etag       string
	db         *engine.DB
	unresolved *candidate
	replay     uint32
	tail       uint32
}

// candidate is one publication (PROTOCOL §2). replay and tail are the page
// counts that go with next.
type candidate struct {
	base     *head.Head
	baseETag string
	next     head.Head
	nonces   []head.ID
	object   *head.Ref
	op       *head.OpID
	bound    head.Digest
	replay   uint32
	tail     uint32
}

// discard closes and deletes the cache file and drops the cached head (D10).
func (e *entity) discard() {
	if e.db != nil {
		e.db.Discard()
	}
	e.db, e.h, e.etag, e.replay, e.tail = nil, nil, "", 0, 0
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

// cas sends one hedged head CAS (D3).
func (n *Node) cas(ctx context.Context, e *entity, next *head.Head, etag string) (string, error) {
	body, err := next.Encode()
	if err != nil {
		return "", newError(CodeCorrupt, "candidate head: "+err.Error(), nil)
	}
	tag, _, err := store.Hedge(ctx, n.cfg.Limits.StorageAttemptTimeout/2, func(ctx context.Context) (string, error) {
		return n.st.PutCAS(ctx, head.HeadKey(e.key), body, etag)
	})
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
	etag, err := n.cas(ctx, e, &c.next, c.baseETag)
	if err == nil {
		n.settle(e, c, &c.next, etag)
		return nil, nil
	}
	return n.resolve(ctx, e, c)
}

// settle records a committed candidate (PROTOCOL §2, after committed): the
// local image is checkpointed if it is the candidate's, and discarded if it
// is not cur's.
func (n *Node) settle(e *entity, c *candidate, cur *head.Head, etag string) {
	e.h, e.etag, e.unresolved = cur, etag, nil
	if e.db == nil {
		return
	}
	if slices.Contains(c.nonces, cur.Nonce) {
		e.replay, e.tail = c.replay, c.tail
		o := engine.Object{Ref: head.Ref{MaxSequence: cur.Sequence, PostChecksum: cur.DatabaseChecksum}, PageCount: cur.PageCount}
		if e.db.Published() != imageOf(cur) && e.db.Checkpoint(o) != nil {
			e.discard()
			return
		}
	}
	if e.db.Published() != imageOf(cur) {
		e.discard()
	}
}

// resolve runs PROTOCOL §2 resolve(c) until ctx ends.
func (n *Node) resolve(ctx context.Context, e *entity, c *candidate) (*engine.Receipt, error) {
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
		if len(c.nonces) < 3 { // D12: re-propose with a fresh nonce under the same condition
			c.next.Nonce = head.NewID()
			c.nonces = append(c.nonces, c.next.Nonce)
			if etag, err := n.cas(ctx, e, &c.next, c.baseETag); err == nil {
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
	case c.op != nil:
		e.discard()
		if err := n.activate(ctx, e, cur, etag); err != nil {
			return nil, err // e.unresolved stays: a later operation resolves again
		}
		r, ok, err := e.db.Receipt(*c.op)
		if err != nil {
			e.discard()
			return nil, engineError(err)
		}
		if ok && r.InputHash != c.bound {
			e.unresolved = nil
			return nil, &Error{Code: CodeIDConflict, Operation: opOf(c), Message: "operation ID used with another input"}
		}
		committed, rec = ok, &r
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
	need := 4*n.cfg.Limits.EntityBytes + 1<<20
	if !n.reserve(need) {
		return newError(CodeCapacity, "cache bytes exhausted", nil)
	}
	defer n.unreserve(need)
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
		db, err := engine.Restore(ctx, path, uint32(n.cfg.Limits.EntityBytes/head.PageSize), h, objs)
		if err != nil {
			return engineError(err)
		}
		e.db, e.h, e.etag = db, h, etag
		e.replay, e.tail = 0, 0
		for _, p := range db.Pages[1:] {
			e.replay += p
		}
		if h.Tail != nil {
			e.tail = db.Pages[len(db.Pages)-1]
		}
		return nil
	}
}
