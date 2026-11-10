package kit

import (
	"context"
	"errors"
	"math/rand/v2"
	"slices"
	"sync"
	"time"

	garden "github.com/OrdalieTech/big-garden"
)

// Membership names the live Nodes (their NodeIDs, this one included).
// Liveness is the caller's: a false suspicion costs a takeover, never a
// stale publication (the core fences it).
type Membership interface{ Members() []string }

// Transport carries a Call to another Node, whose side calls Router.Serve.
// Call and Reply are plain data: encoding/json round-trips both.
type Transport interface {
	Forward(ctx context.Context, node string, c Call) (Reply, error)
}

// Call is one mutation by name: a callback cannot travel, so the receiving
// Node runs the Handler registered under Op.
type Call struct {
	Op, Key string
	Req     garden.Request
	Input   []byte
	Create  bool // create the key (on the Node Preferred names) instead of updating it
}

// Reply is the outcome of a Call: Err for a library error, Callback for a
// handler's own error (its text).
type Reply struct {
	Result   garden.Result
	Ref      garden.Ref
	Err      *garden.Error
	Callback string
}

// Handler is a mutation callback registered by name.
type Handler func(tx *garden.Tx, input []byte) ([]byte, error)

// Router sends each mutation to its entity's owner, with the identical
// Request on every attempt: sticky to the owner it last saw (one Resolve on
// a miss), a takeover only by the Node Preferred names when the owner is
// absent from Members or the entity is unowned (no rebalancing of live
// owners), one hop (a forwarded Call is never forwarded again), backoff on
// route_changed, at most four attempts. Reads need no routing: any Node
// serves them (D24).
type Router struct {
	n      *garden.Node
	m      Membership
	t      Transport
	mu     sync.Mutex
	ops    map[string]Handler
	owners map[string]garden.Route // last seen, by key
}

const attempts = 4

// NewRouter routes n's mutations among m's members over t.
func NewRouter(n *garden.Node, m Membership, t Transport) *Router {
	return &Router{n: n, m: m, t: t, ops: map[string]Handler{}, owners: map[string]garden.Route{}}
}

// Handle registers h under op, on every Node.
func (r *Router) Handle(op string, h Handler) {
	r.mu.Lock()
	r.ops[op] = h
	r.mu.Unlock()
}

// Mutate runs op on key's owner and returns its result. An error after a
// transport failure on the last attempt is outcome_unknown for req.ID: the
// remote owner may have committed it, so retry with the same Request.
func (r *Router) Mutate(ctx context.Context, c Call) (garden.Result, garden.Ref, error) {
	var err error
	for try := range attempts {
		if try > 0 {
			select { // full jitter, 50 ms doubling
			case <-time.After(rand.N(50 * time.Millisecond << try)):
			case <-ctx.Done():
				return garden.Result{}, garden.Ref{}, ctx.Err()
			}
		}
		target, rt, rerr := r.target(ctx, c)
		if rerr != nil {
			return garden.Result{}, garden.Ref{}, rerr
		}
		var rep Reply
		if target == r.n.Owner().NodeID {
			rep = r.run(ctx, c, rt)
		} else if rep, err = r.t.Forward(ctx, target, c); err != nil {
			r.forget(c.Key)
			err = &garden.Error{Code: garden.CodeOutcomeUnknown, Operation: c.Req.ID, Message: "kit: forwarding failed: " + err.Error()}
			continue
		}
		switch {
		case rep.Callback != "":
			return rep.Result, rep.Ref, errors.New(rep.Callback)
		case rep.Err == nil:
			r.remember(c.Key, garden.Route{Ref: rep.Ref, Owner: garden.Owner{NodeID: target}})
			return rep.Result, rep.Ref, nil
		case rep.Err.Route != nil && (rep.Err.Code == garden.CodeNotOwner || rep.Err.Code == garden.CodeRouteChanged):
			r.remember(c.Key, *rep.Err.Route)
		case rep.Err.Code != garden.CodeOutcomeUnknown && rep.Err.Code != garden.CodeUnavailable:
			return garden.Result{}, garden.Ref{}, rep.Err
		}
		err = rep.Err
	}
	return garden.Result{}, garden.Ref{}, err
}

// Serve runs a Call that another Node forwarded, without forwarding it
// again: a Node that is not the owner answers not_owner with the Route.
func (r *Router) Serve(ctx context.Context, c Call) Reply {
	target, rt, err := r.target(ctx, c)
	var ge *garden.Error
	switch {
	case errors.As(err, &ge):
		return Reply{Err: ge}
	case err != nil:
		return Reply{Err: &garden.Error{Code: garden.CodeUnavailable, Message: err.Error()}}
	case target != r.n.Owner().NodeID: // not this Node's to run (or to take over): the origin re-routes
		return Reply{Err: &garden.Error{Code: garden.CodeNotOwner, Route: &rt, Message: "kit: owned elsewhere"}}
	}
	return r.run(ctx, c, rt)
}

// target picks the Node for c: the owner last seen, or the Preferred member
// when the owner is gone, unknown or absent (and always for a Create).
func (r *Router) target(ctx context.Context, c Call) (string, garden.Route, error) {
	members := r.m.Members()
	r.mu.Lock()
	rt, ok := r.owners[c.Key]
	r.mu.Unlock()
	if !ok && !c.Create {
		var err error
		if rt, err = r.n.Resolve(ctx, c.Key); err != nil {
			return "", rt, err
		}
		r.remember(c.Key, rt)
	}
	if c.Create || rt.Owner.NodeID == "" || !slices.Contains(members, rt.Owner.NodeID) {
		p, err := garden.Preferred(c.Key, members)
		return p, rt, err
	}
	return rt.Owner.NodeID, rt, nil
}

// run executes c on this Node, taking over first if the entity is unowned or
// its owner is no member (this Node being the one Preferred names).
func (r *Router) run(ctx context.Context, c Call, rt garden.Route) Reply {
	r.mu.Lock()
	h, ok := r.ops[c.Op]
	r.mu.Unlock()
	if !ok {
		return Reply{Err: &garden.Error{Code: garden.CodeUnsupported, Message: "kit: no handler for " + c.Op}}
	}
	var cb error
	fn := func(tx *garden.Tx) ([]byte, error) { v, err := h(tx, c.Input); cb = err; return v, err }
	var rep Reply
	var err error
	if c.Create {
		rep.Ref, rep.Result, err = r.n.Create(ctx, c.Key, c.Req, fn)
	} else {
		self := r.n.Owner().NodeID
		if rt.Owner.NodeID != self && (rt.Owner.NodeID == "" || !slices.Contains(r.m.Members(), rt.Owner.NodeID)) {
			if rt, err = r.n.Takeover(ctx, rt); err != nil {
				cb = nil
			}
		}
		if err == nil {
			rep.Ref = rt.Ref
			rep.Result, err = r.n.Update(ctx, rt.Ref, c.Req, fn)
		}
	}
	var ge *garden.Error
	switch {
	case err == nil:
	case cb != nil && errors.Is(err, cb):
		rep.Callback = cb.Error()
	case errors.As(err, &ge):
		rep.Err = ge
	default:
		rep.Err = &garden.Error{Code: garden.CodeUnavailable, Message: err.Error()}
	}
	return rep
}

func (r *Router) remember(key string, rt garden.Route) {
	r.mu.Lock()
	if len(r.owners) >= 1<<17 { // bounded: forget everything, Resolve again
		clear(r.owners)
	}
	r.owners[key] = rt
	r.mu.Unlock()
}

func (r *Router) forget(key string) {
	r.mu.Lock()
	delete(r.owners, key)
	r.mu.Unlock()
}
