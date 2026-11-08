package garden

import (
	"context"
	"errors"

	"github.com/OrdalieTech/big-garden/internal/head"
	"github.com/OrdalieTech/big-garden/internal/store"
)

// fetch replaces e's cached head with a fresh GET of it (no If-None-Match),
// discarding a local image that no longer matches or is no longer owned.
func (n *Node) fetch(ctx context.Context, e *entity, inc head.ID) error {
	obj, err := n.st.Get(ctx, head.HeadKey(e.key), "", maxHeadBytes)
	switch store.OutcomeOf(err) {
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
	if e.db != nil && (e.db.Published() != imageOf(cur) || !n.mine(cur)) {
		e.discard()
	}
	e.h, e.etag = cur, obj.ETag
	return nil
}

// Takeover claims the entity from the owner named by observed (PROTOCOL §7).
func (n *Node) Takeover(ctx context.Context, observed Route) (Route, error) {
	if err := head.ValidKey(observed.Ref.Key); err != nil {
		return Route{}, invalid(err.Error())
	}
	e, err := n.enter(observed.Ref.Key, &MutationID{})
	if err != nil {
		return Route{}, err
	}
	defer e.mu.Unlock()
	for try := 0; ; try++ {
		if err := n.fetch(ctx, e, observed.Ref.Incarnation); err != nil {
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
				return Route{}, withRoute(err, routeOf(e.h))
			}
		}
		return *routeOf(e.h), nil
	}
}

func withRoute(err error, r *Route) error {
	if e, ok := err.(*Error); ok && e.Route == nil { // fresh Errors only: never shared
		e.Route = r
	}
	return err
}

// Release gives up ownership: owner null, epoch+1 (PROTOCOL §7).
func (n *Node) Release(ctx context.Context, ref Ref) error {
	if err := head.ValidKey(ref.Key); err != nil {
		return invalid(err.Error())
	}
	e, err := n.enter(ref.Key, nil)
	if err != nil {
		return err
	}
	defer e.mu.Unlock()
	if e.h == nil || !n.mine(e.h) {
		if err := n.fetch(ctx, e, ref.Incarnation); err != nil {
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

// Evict closes and deletes the local cache of ref; ownership is unchanged
// (PROTOCOL §7). The entity struct stays registered, so its mutex keeps
// serializing every operation on the key.
func (n *Node) Evict(ctx context.Context, ref Ref) error {
	if err := head.ValidKey(ref.Key); err != nil {
		return invalid(err.Error())
	}
	e, err := n.enter(ref.Key, nil)
	if err != nil {
		return err
	}
	defer e.mu.Unlock()
	e.discard()
	return nil
}
