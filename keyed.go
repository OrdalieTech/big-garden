package garden

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"slices"
	"time"
)

func hasCode(err error, codes ...Code) (*Error, bool) {
	var ge *Error
	return ge, errors.As(err, &ge) && slices.Contains(codes, ge.Code)
}

func (n *Node) resolveCached(ctx context.Context, key string, fresh bool) (Ref, error) {
	if !fresh {
		n.refMu.RLock()
		ref, ok := n.refs[key]
		n.refMu.RUnlock()
		if ok {
			return ref, nil
		}
	}
	route, err := n.Resolve(ctx, key)
	if err != nil {
		return Ref{}, err
	}
	n.setRef(key, route.Ref)
	return route.Ref, nil
}

func (n *Node) setRef(key string, ref Ref) {
	n.refMu.Lock()
	n.refs[key] = ref
	n.refMu.Unlock()
}

func (n *Node) forgetRef(key string) {
	n.refMu.Lock()
	delete(n.refs, key)
	n.refMu.Unlock()
}

// ReadKey resolves key (using a per-node cached Ref) and runs fn under
// ReadStale when maxAge > 0, or linearizable Read when maxAge <= 0. On
// CodeNotFound or CodeIncarnationMismatch it refreshes the cached Ref once.
func (n *Node) ReadKey(ctx context.Context, key string, schema uint32, maxAge time.Duration, fn func(*View) error) (Result, error) {
	cb := func(v *View) ([]byte, error) { return nil, fn(v) }
	for attempt := range 2 {
		ref, err := n.resolveCached(ctx, key, attempt > 0)
		if err != nil {
			return Result{}, err
		}
		var res Result
		if maxAge > 0 {
			res, err = n.ReadStale(ctx, ref, schema, maxAge, cb)
		} else {
			res, err = n.Read(ctx, ref, schema, cb)
		}
		if err == nil {
			return res, nil
		}
		if _, ok := hasCode(err, CodeNotFound, CodeIncarnationMismatch); ok {
			n.forgetRef(key)
			if attempt == 0 {
				continue
			}
		}
		return Result{}, err
	}
	return Result{}, newError(CodeNotFound, "key not found", nil)
}

func (n *Node) mutateKey(ctx context.Context, key string, onMissing func() (Ref, Result, error), op func(Ref) (Result, error)) (Result, error) {
	for attempt := range 4 {
		route, err := n.Resolve(ctx, key)
		if _, ok := hasCode(err, CodeNotFound); ok {
			if onMissing == nil {
				n.forgetRef(key)
				return Result{}, nil
			}
			ref, res, cerr := onMissing()
			if cerr == nil {
				n.setRef(key, ref)
				return res, nil
			}
			if _, ok := hasCode(cerr, CodeExists); ok {
				continue
			}
			return Result{}, cerr
		}
		if err != nil {
			return Result{}, err
		}
		n.setRef(key, route.Ref)
		res, err := op(route.Ref)
		if err == nil {
			return res, nil
		}
		if ge, ok := hasCode(err, CodeNotOwner); ok && ge.Route != nil {
			if _, terr := n.Takeover(ctx, *ge.Route); terr != nil {
				if _, ok := hasCode(terr, CodeRouteChanged); !ok {
					return Result{}, terr
				}
			}
			continue
		}
		if _, ok := hasCode(err, CodeRouteChanged, CodeIncarnationMismatch, CodeNotFound); ok && attempt < 3 {
			n.forgetRef(key)
			continue
		}
		return Result{}, err
	}
	return Result{}, newError(CodeUnavailable, fmt.Sprintf("mutate %s: could not acquire ownership", key), nil)
}

// Write creates or updates key in a single durable transaction, acquiring
// ownership via Takeover if needed and retrying transient route races.
func (n *Node) Write(ctx context.Context, key string, schema uint32, fn func(*Tx) error) (Result, error) {
	id, err := NewMutationID(time.Now())
	if err != nil {
		return Result{}, err
	}
	var hash [32]byte
	rand.Read(hash[:])
	req := Request{ID: id, Hash: hash, Schema: schema}
	cb := func(tx *Tx) ([]byte, error) { return nil, fn(tx) }
	return n.mutateKey(ctx, key,
		func() (Ref, Result, error) { return n.Create(ctx, key, req, cb) },
		func(ref Ref) (Result, error) { return n.Update(ctx, ref, req, cb) },
	)
}

// Remove deletes key, taking over ownership if needed; an absent key is a no-op.
func (n *Node) Remove(ctx context.Context, key string) error {
	_, err := n.mutateKey(ctx, key, nil, func(ref Ref) (Result, error) {
		err := n.Delete(ctx, ref)
		if _, ok := hasCode(err, CodeNotFound); err == nil || ok {
			n.forgetRef(key)
			return Result{}, nil
		}
		return Result{}, err
	})
	return err
}
