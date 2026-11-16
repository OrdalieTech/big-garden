package kit

import (
	"context"
	"errors"
	"io"
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
	// ForwardFile is Forward of a Call carrying a file (Router.MutateFile):
	// the receiving side calls Router.ServeFile with c and body. It must not
	// close body, which the next attempt reads again from its start.
	ForwardFile(ctx context.Context, node string, c Call, body io.Reader) (Reply, error)
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

// FileHandler is a Handler that may name a file: b is the file the Call
// carried (Router.MutateFile), already stored on this Node for tx.SetFile,
// or nil for a Call without one.
type FileHandler func(tx *garden.Tx, input []byte, b *garden.Blob) ([]byte, error)

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
	ops    map[string]FileHandler
	owners map[string]garden.Route // last seen, by key
}

const attempts = 4

// NewRouter routes n's mutations among m's members over t.
func NewRouter(n *garden.Node, m Membership, t Transport) *Router {
	return &Router{n: n, m: m, t: t, ops: map[string]FileHandler{}, owners: map[string]garden.Route{}}
}

// Handle registers h under op, on every Node.
func (r *Router) Handle(op string, h Handler) {
	r.HandleFile(op, func(tx *garden.Tx, input []byte, _ *garden.Blob) ([]byte, error) { return h(tx, input) })
}

// HandleFile registers h under op, on every Node.
func (r *Router) HandleFile(op string, h FileHandler) {
	r.mu.Lock()
	r.ops[op] = h
	r.mu.Unlock()
}

// Mutate runs op on key's owner and returns its result. An error after a
// transport failure on the last attempt is outcome_unknown for req.ID: the
// remote owner may have committed it, so retry with the same Request.
func (r *Router) Mutate(ctx context.Context, c Call) (garden.Result, garden.Ref, error) {
	return r.mutate(ctx, c, nil)
}

// MutateFile is Mutate with a file: the owner stores body with PutBlob (a
// Create, on the Node it runs on, before the key exists), then runs the
// FileHandler of op with the Blob, which a Blob cannot leave since only its
// uploader may link it (DECISIONS D38). Each attempt reads body from its
// start. A retry of a committed Request stores nothing new: PutBlob finds
// the bytes in the image.
func (r *Router) MutateFile(ctx context.Context, c Call, body io.ReadSeeker) (garden.Result, garden.Ref, error) {
	return r.mutate(ctx, c, body)
}

func (r *Router) mutate(ctx context.Context, c Call, body io.ReadSeeker) (garden.Result, garden.Ref, error) {
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
		var in io.Reader
		if body != nil {
			if _, serr := body.Seek(0, io.SeekStart); serr != nil { // keeps an earlier outcome_unknown
				return garden.Result{}, garden.Ref{}, errors.Join(err, serr)
			}
			in = body
		}
		switch {
		case target == r.n.Owner().NodeID:
			rep = r.run(ctx, c, rt, in)
		case body == nil:
			rep, err = r.t.Forward(ctx, target, c)
		default:
			rep, err = r.t.ForwardFile(ctx, target, c, body)
		}
		if err != nil {
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
func (r *Router) Serve(ctx context.Context, c Call) Reply { return r.serve(ctx, c, nil) }

// ServeFile is Serve of a Call ForwardFile carried, with its file.
func (r *Router) ServeFile(ctx context.Context, c Call, body io.Reader) Reply {
	return r.serve(ctx, c, body)
}

func (r *Router) serve(ctx context.Context, c Call, body io.Reader) Reply {
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
	return r.run(ctx, c, rt, body)
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
// its owner is no member (this Node being the one Preferred names), with
// the file body stored first if there is one.
func (r *Router) run(ctx context.Context, c Call, rt garden.Route, body io.Reader) Reply {
	r.mu.Lock()
	h, ok := r.ops[c.Op]
	r.mu.Unlock()
	if !ok {
		return Reply{Err: &garden.Error{Code: garden.CodeUnsupported, Message: "kit: no handler for " + c.Op}}
	}
	var cb error
	var b *garden.Blob
	fn := func(tx *garden.Tx) ([]byte, error) { v, err := h(tx, c.Input, b); cb = err; return v, err }
	put := func(ref garden.Ref) error {
		if body == nil {
			return nil
		}
		blob, err := r.n.PutBlob(ctx, ref, body)
		b = &blob
		return err
	}
	var rep Reply
	var err error
	self := r.n.Owner().NodeID
	gone := func(rt garden.Route) bool {
		return rt.Owner.NodeID != self && (rt.Owner.NodeID == "" || !slices.Contains(r.m.Members(), rt.Owner.NodeID))
	}
	if c.Create {
		if err = put(garden.Ref{Key: c.Key}); err == nil {
			if rep.Ref, rep.Result, err = r.n.Create(ctx, c.Key, c.Req, fn); err == nil {
				r.remember(c.Key, garden.Route{Ref: rep.Ref, Owner: r.n.Owner()})
			}
		}
	} else {
		if gone(rt) && rt.Epoch == 0 { // a route remembered from a reply names only its owner: observe the head first
			rt, err = r.n.Resolve(ctx, c.Key)
		}
		switch {
		case err != nil:
		case gone(rt):
			if rt, err = r.n.Takeover(ctx, rt); err == nil {
				r.remember(c.Key, rt)
			}
		case rt.Owner.NodeID != self: // a live member owns it after all: the origin re-routes
			return Reply{Err: &garden.Error{Code: garden.CodeNotOwner, Route: &rt, Message: "kit: owned elsewhere"}}
		}
		if err == nil {
			err = put(rt.Ref)
		}
		if err != nil {
			cb = nil
		} else {
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
