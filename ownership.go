package garden

import (
	"context"
	"errors"
	"slices"
	"time"

	"github.com/OrdalieTech/big-garden/internal/head"
	"github.com/OrdalieTech/big-garden/internal/store"
)

// fetch revalidates e's cached head with a GET (If-None-Match: etag, if not
// empty), parking a local image that is not exactly the new head's: another
// image, or the same one by other links, whose page counts it does not know.
// A tombstone is not_found, with the local image and warm file dropped (D25).
func (n *Node) fetch(ctx context.Context, e *entity, inc head.ID, etag string) error {
	issued := time.Now()
	obj, err := n.st.Get(ctx, head.HeadKey(e.key), etag, head.MaxHeadBytes)
	switch store.OutcomeOf(err) {
	case store.NotModified:
		e.valid = issued
		return nil
	case store.OK:
	case store.NotFound:
		e.discard()
		return n.missing(ctx, e.key)
	default:
		return storeError(err)
	}
	cur, err := n.decodeHead(obj.Body, e.key, inc)
	if err != nil {
		return err
	}
	if cur.Deleted {
		return n.deleted(e)
	}
	if e.db != nil && (e.db.Published() != imageOf(cur) || !slices.Equal(links(e.h), links(cur))) {
		n.park(e) // the file of the previous head: a base for catching up later
	}
	e.h, e.etag, e.valid = cur, obj.ETag, issued
	return nil
}

// Takeover claims the entity from the owner named by observed (PROTOCOL §7).
func (n *Node) Takeover(ctx context.Context, observed Route) (Route, error) {
	e, err := n.enter(ctx, observed.Ref.Key, &MutationID{}, nil, false)
	if err != nil {
		return Route{}, err
	}
	defer n.leave(e)
	for try := 0; ; try++ {
		if err := n.fetch(ctx, e, observed.Ref.Incarnation, ""); err != nil {
			return Route{}, err
		}
		cur := *routeOf(e.h)
		if !n.mine(e.h) {
			if cur.Owner != observed.Owner || cur.Epoch != observed.Epoch || try == 3 {
				return Route{}, &Error{Code: CodeRouteChanged, Route: &cur, Message: "the observed route is stale"}
			}
			if err := n.claim(e); err != nil {
				var pe *Error
				if e.unresolved != nil || !errors.As(err, &pe) || (pe.Code != CodeNotOwner && pe.Code != CodeRouteChanged) {
					return Route{}, err
				}
				continue // not committed: retry while the observed owner still holds it
			}
		}
		if e.db == nil {
			if err := n.activate(ctx, e, e.h, e.etag); err != nil {
				if pe, ok := err.(*Error); ok && pe.Route == nil { // fresh Errors only: never shared
					pe.Route = routeOf(e.h)
				}
				return Route{}, err
			}
		}
		if !n.mine(e.h) { // activation restarted on a newer head owned by another session
			r := routeOf(e.h)
			e.discard()
			return Route{}, &Error{Code: CodeRouteChanged, Route: r, Message: "the head changed during activation"}
		}
		return *routeOf(e.h), nil
	}
}

// Release gives up ownership: owner null, epoch+1 (PROTOCOL §7).
func (n *Node) Release(ctx context.Context, ref Ref) error {
	e, err := n.enter(ctx, ref.Key, nil, nil, false)
	if err != nil {
		return err
	}
	defer n.leave(e)
	if e.h == nil || !n.mine(e.h) {
		if err := n.fetch(ctx, e, ref.Incarnation, ""); err != nil {
			return err
		}
	}
	if e.h.Incarnation != ref.Incarnation {
		return &Error{Code: CodeIncarnationMismatch, Route: routeOf(e.h), Message: "the key has another incarnation"}
	}
	if !n.mine(e.h) {
		return nil
	}
	next := *e.h
	next.Owner, next.Epoch, next.Revision, next.Nonce = nil, next.Epoch+1, next.Revision+1, head.NewID()
	_, err = n.publish(e, &candidate{base: e.h, baseETag: e.etag, next: next})
	if err == nil {
		e.discard()
	}
	return err
}

// deleted answers not_found for e's tombstone (D25) and drops what this
// node keeps of the deleted incarnation: the local image and the warm file.
func (n *Node) deleted(e *entity) error {
	e.discard()
	n.claimWarm(e.key, "")
	return newError(CodeNotFound, "the entity was deleted", nil)
}

// Delete ends ref's incarnation (PROTOCOL §7, D25): the owner replaces the
// head with a tombstone (no owner, epoch+1, no tail), then collects every
// object of the incarnation. Every operation on ref is not_found afterwards;
// Create or Import recreates the key as a new incarnation. Deleting an
// already deleted incarnation collects it again and returns nil, so a retry
// after any failure finishes the erasure.
func (n *Node) Delete(ctx context.Context, ref Ref) error {
	e, err := n.enter(ctx, ref.Key, nil, nil, false)
	if err != nil {
		return err
	}
	var tomb *head.Head
	if err = n.ownedHead(ctx, e, ref, time.Time{}); err == nil { // a zero t0: a write, owner only; no activation
		next := *e.h
		next.Owner, next.Tail, next.Deleted = nil, nil, true
		next.Epoch, next.Revision, next.Nonce = next.Epoch+1, next.Revision+1, head.NewID()
		if _, err = n.publish(e, &candidate{base: e.h, baseETag: e.etag, next: next}); err == nil && e.h.Deleted && e.h.Incarnation == ref.Incarnation {
			tomb = e.h // the settled head: a later tombstone if resolution found one
		}
		if err == nil {
			n.deleted(e)
		}
	}
	n.leave(e)
	var pe *Error
	switch {
	case tomb != nil:
		return n.reap(ctx, tomb)
	case err == nil, errors.As(err, &pe) && (pe.Code == CodeNotFound || pe.Code == CodeIncarnationMismatch):
		return n.Collect(ctx, ref) // deleted already (maybe recreated since), or never created
	}
	return err
}

// Evict closes and deletes the local cache of ref; ownership is unchanged
// (PROTOCOL §7).
func (n *Node) Evict(ctx context.Context, ref Ref) error {
	e, err := n.enter(ctx, ref.Key, nil, nil, false)
	if err != nil {
		return err
	}
	defer n.leave(e)
	e.discard()
	n.claimWarm(ref.Key, "") // and nothing in the warm tier: the next activation is cold
	return nil
}
