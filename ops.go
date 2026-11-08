package garden

import (
	"bytes"
	"cmp"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"io"
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
func (n *Node) checkClock(req Request) error {
	switch now := n.now(); {
	case tsOf(req.ID) > now.Add(clockSkew).UnixMilli():
		return invalid("mutation ID time is in the future")
	case tsOf(req.ID) <= now.Add(-n.cfg.Limits.ReceiptRetention).UnixMilli():
		return &Error{Code: CodeIDExpired, Operation: req.ID, Message: "mutation ID is older than ReceiptRetention"}
	}
	return nil
}

// binding is the request binding of PROTOCOL §1.
func binding(kind byte, from, to uint32, hash [32]byte) head.Digest {
	b := append([]byte("state-request-v1\x00"), kind)
	b = binary.BigEndian.AppendUint32(binary.BigEndian.AppendUint32(b, from), to)
	return sha256.Sum256(append(b, hash[:]...))
}

// enter returns the entity of key with its mutex held and any unresolved
// candidate settled first (PROTOCOL §2 rule 5). If it stays unresolved, the
// same operation ID (or every caller, for a nil id) gets outcome_unknown and
// others unavailable.
func (n *Node) enter(key string, id *MutationID) (*entity, error) {
	e, err := n.entity(key)
	if err != nil {
		return nil, err
	}
	if e.waiting.Add(1) > int64(n.cfg.Limits.PendingPerEntity) {
		e.waiting.Add(-1)
		return nil, newError(CodeCapacity, "too many operations queued on the entity", nil)
	}
	e.mu.Lock()
	e.waiting.Add(-1)
	e.used = time.Now()
	if n.isClosed() {
		e.mu.Unlock()
		return nil, newError(CodeClosed, "", nil)
	}
	if c := e.unresolved; c != nil {
		ctx, cancel := n.commitContext()
		n.resolve(ctx, e, c)
		cancel()
		if e.unresolved != nil {
			e.mu.Unlock()
			if id == nil || (c.op != nil && *c.op == head.OpID(*id)) {
				return nil, &Error{Code: CodeOutcomeUnknown, Operation: opOf(c), Message: "earlier attempt still unresolved"}
			}
			return nil, newError(CodeUnavailable, "an earlier publication is unresolved", nil)
		}
	}
	return e, nil
}

// owned makes e's cached head current enough, owned by this session and
// activated (PROTOCOL §3 steps 2–4, §6 step 2). fresh forces the conditional
// GET; otherwise a cached head owned by this session is used as is.
func (n *Node) owned(ctx context.Context, e *entity, ref Ref, fresh bool) error {
	if fresh || e.h == nil || !n.mine(e.h) || e.db == nil {
		obj, err := n.st.Get(ctx, head.HeadKey(e.key), e.etag, maxHeadBytes)
		switch store.OutcomeOf(err) {
		case store.NotModified:
		case store.OK:
			cur, err := n.decodeHead(obj.Body, e.key, ref.Incarnation)
			if err != nil {
				return err
			}
			if e.db != nil && (e.db.Published() != imageOf(cur) || !n.mine(cur)) {
				e.discard()
			}
			e.h, e.etag = cur, obj.ETag
		case store.NotFound:
			e.discard()
			return n.missing(ctx, e.key)
		default:
			return storeError(err)
		}
	}
	if e.h.Incarnation != ref.Incarnation {
		return &Error{Code: CodeIncarnationMismatch, Route: routeOf(e.h), Message: "the key has another incarnation"}
	}
	if !n.mine(e.h) {
		if e.h.Owner == nil || e.h.Owner.Node != n.cfg.NodeID {
			return notOwner(e.h)
		}
		if err := n.claim(e); err != nil { // D4: an earlier session of this node
			return err
		}
	}
	if e.db == nil {
		if err := n.activate(ctx, e, e.h, e.etag); err != nil {
			return err
		}
		if !n.mine(e.h) {
			e.discard()
			return newError(CodeRouteChanged, "the head changed during activation", nil)
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

// barrier revalidates the cached head (PROTOCOL §0). changed means the
// caller redoes its operation from activation once.
func (n *Node) barrier(ctx context.Context, e *entity) (changed bool, err error) {
	_, err = n.st.Get(ctx, head.HeadKey(e.key), e.etag, maxHeadBytes)
	switch store.OutcomeOf(err) {
	case store.NotModified:
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
func (n *Node) putObject(ctx context.Context, key string, body []byte) error {
	n.staging.Add(int64(len(body)))
	defer n.staging.Add(-int64(len(body)))
	failpoint("put.before")
	failpoint("put.mid")
	err := n.st.PutImmutable(ctx, key, body)
	failpoint("put.after")
	if store.OutcomeOf(err) == store.PreconditionFailed {
		obj, gerr := n.st.Get(ctx, key, "", int64(len(body)))
		if gerr == nil && !bytes.Equal(obj.Body, body) {
			return newError(CodeCorrupt, "an object key holds other bytes", nil)
		}
		err = gerr
	}
	if err != nil {
		return storeError(err)
	}
	return nil
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

// Resolve returns the current Route of key from a fresh head GET (PROTOCOL §3).
func (n *Node) Resolve(ctx context.Context, key string) (Route, error) {
	if err := head.ValidKey(key); err != nil {
		return Route{}, invalid(err.Error())
	}
	obj, err := n.st.Get(ctx, head.HeadKey(key), "", maxHeadBytes)
	switch store.OutcomeOf(err) {
	case store.OK:
		h, err := n.decodeHead(obj.Body, key, head.ID{})
		if err != nil {
			return Route{}, err
		}
		return *routeOf(h), nil
	case store.NotFound:
		return Route{}, n.missing(ctx, key)
	}
	return Route{}, storeError(err)
}

// Read runs fn on the state validated by a head GET issued after the call
// began (PROTOCOL §3).
func (n *Node) Read(ctx context.Context, ref Ref, schema uint32, fn func(*View) ([]byte, error)) (res Result, err error) {
	defer func() { n.observe(ref.Key, true, res, err) }()
	if err := head.ValidKey(ref.Key); err != nil {
		return Result{}, invalid(err.Error())
	}
	e, err := n.enter(ref.Key, &MutationID{})
	if err != nil {
		return Result{}, err
	}
	defer e.mu.Unlock()
	if err := n.owned(ctx, e, ref, true); err != nil {
		return Result{}, err
	}
	if e.h.Schema != schema {
		return Result{}, &Error{Code: CodeSchemaMismatch, At: Version{e.h.Incarnation, e.h.Sequence}, Message: "stored schema differs"}
	}
	var cb error
	v, err := e.db.Read(scoped(n.cfg.Limits.ResultBytes, func(v *engine.View) ([]byte, error) { return fn(&View{v}) }, &cb))
	if err != nil {
		return Result{}, callbackError(err, cb)
	}
	return Result{Value: v, Version: Version{e.h.Incarnation, e.h.Sequence}}, nil
}

// Lookup returns the stored outcome of id in freshly validated state.
func (n *Node) Lookup(ctx context.Context, ref Ref, id MutationID) (res Result, err error) {
	defer func() { n.observe(ref.Key, true, res, err) }()
	if err := head.ValidKey(ref.Key); err != nil {
		return Result{}, invalid(err.Error())
	}
	e, err := n.enter(ref.Key, &id)
	if err != nil {
		return Result{}, err
	}
	defer e.mu.Unlock()
	return n.lookup(ctx, e, ref, id)
}

func (n *Node) lookup(ctx context.Context, e *entity, ref Ref, id MutationID) (Result, error) {
	if err := n.owned(ctx, e, ref, true); err != nil {
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
	if err := cmp.Or(checkRequest(key, req), n.checkClock(req)); err != nil {
		return Ref{}, Result{}, err
	}
	bound := binding(1, req.Schema, req.Schema, req.Hash)
	return n.create(ctx, key, req, bound, func(path string, inc head.ID) (*engine.DB, []byte, error) {
		db, err := engine.Create(ctx, path, n.maxPages(), n.ns, key, inc)
		if err != nil {
			return nil, nil, engineError(err)
		}
		var cb error
		failpoint("sql.before")
		result, _, err := db.Mutate(engine.Commit{ID: head.OpID(req.ID), Bound: bound, Schema: req.Schema},
			scoped(n.cfg.Limits.ResultBytes, func(t *engine.Tx) ([]byte, error) { failpoint("sql.inside"); return fn(&Tx{t}) }, &cb))
		if err != nil {
			db.Discard()
			return nil, nil, callbackError(err, cb)
		}
		failpoint("sql.after")
		return db, result, nil
	})
}

func (n *Node) maxPages() uint32 { return uint32(n.cfg.Limits.EntityBytes / head.PageSize) }

// builder builds the local genesis image of incarnation inc at path and
// returns it with the operation's result (PROTOCOL §5 step 4).
type builder func(path string, inc head.ID) (*engine.DB, []byte, error)

// create runs PROTOCOL §5 for Create (kind 1) and Import (kind 4).
func (n *Node) create(ctx context.Context, key string, req Request, bound head.Digest, build builder) (ref Ref, res Result, err error) {
	defer func() { n.observe(key, false, res, err) }()
	e, err := n.enter(key, &req.ID)
	if err != nil {
		return Ref{}, Result{}, err
	}
	defer e.mu.Unlock()
	obj, err := n.st.Get(ctx, head.HeadKey(key), "", maxHeadBytes)
	switch store.OutcomeOf(err) {
	case store.OK:
		return n.createExists(ctx, e, req, bound, obj.Body)
	case store.NotFound:
	default:
		return Ref{}, Result{}, storeError(err)
	}
	e.discard()
	for {
		birth, err := n.birth(ctx, key)
		var inc head.ID
		switch {
		case err != nil:
			return Ref{}, Result{}, err
		case birth != nil && birth.OperationID == head.OpID(req.ID) && birth.InputHash == bound:
			if err := n.resumable(ctx, key, birth.Incarnation); err != nil {
				return Ref{}, Result{}, err
			}
			inc = birth.Incarnation
		case birth != nil && birth.OperationID == head.OpID(req.ID):
			return Ref{}, Result{}, &Error{Code: CodeIDConflict, Operation: req.ID, Message: "operation ID used with another input"}
		case birth != nil:
			return Ref{}, Result{}, newError(CodeStateMissing, "birth.json exists without head.json", nil)
		default:
			page, _, err := n.st.List(ctx, head.EntityDir(key)+"/", "", 1)
			if err != nil {
				return Ref{}, Result{}, storeError(err)
			}
			if len(page) > 0 {
				return Ref{}, Result{}, newError(CodeStateMissing, "entity artifacts exist without birth.json", nil)
			}
			inc = head.NewID()
		}
		ref, res, err := n.genesis(ctx, e, key, req, bound, inc, birth == nil, build)
		if err != errRestart {
			return ref, res, err
		}
	}
}

var errRestart = errors.New("restart create")

// createExists answers a Create whose key already has a head (§5 step 2).
func (n *Node) createExists(ctx context.Context, e *entity, req Request, bound head.Digest, body []byte) (Ref, Result, error) {
	h, err := n.decodeHead(body, e.key, head.ID{})
	if err != nil {
		return Ref{}, Result{}, err
	}
	birth, err := n.birth(ctx, e.key)
	if err != nil {
		return Ref{}, Result{}, err
	}
	switch {
	case birth != nil && birth.OperationID == head.OpID(req.ID) && birth.InputHash == bound:
		ref := Ref{e.key, birth.Incarnation}
		res, err := n.lookup(ctx, e, ref, req.ID)
		return ref, res, err
	case birth != nil && birth.OperationID == head.OpID(req.ID):
		return Ref{}, Result{}, &Error{Code: CodeIDConflict, Operation: req.ID, Message: "operation ID used with another input"}
	}
	return Ref{}, Result{}, &Error{Code: CodeInvalidArgument, Route: routeOf(h), Message: "key exists"}
}

// birth reads birth.json; nil if absent.
func (n *Node) birth(ctx context.Context, key string) (*head.Birth, error) {
	obj, err := n.st.Get(ctx, head.BirthKey(key), "", 4<<10)
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

// genesis runs PROTOCOL §5 steps 4–8.
func (n *Node) genesis(ctx context.Context, e *entity, key string, req Request, bound head.Digest, inc head.ID, writeBirth bool,
	build builder) (Ref, Result, error) {
	if err := n.cache(e); err != nil {
		return Ref{}, Result{}, err
	}
	db, result, err := build(filepath.Join(n.dir, head.NewID().String()+".db"), inc)
	n.unreserve()
	if err != nil {
		return Ref{}, Result{}, err
	}
	e.db = db
	e.bytes.Store(db.Bytes())
	op := head.OpID(req.ID)
	var buf bytes.Buffer
	snap, err := db.Snapshot(&buf)
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
		if errors.As(err, &pe) && (pe.Code == CodeUnavailable || pe.Code == CodeCorrupt || pe.Code == CodeCapacity) {
			err = &Error{Code: CodeOutcomeUnknown, Operation: req.ID, Message: "create outcome unknown: " + pe.Message, cause: pe}
		}
		return Ref{}, Result{}, withOp(err, req.ID)
	}
	if writeBirth {
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
				return Ref{}, Result{}, errRestart
			default:
				err = newError(CodeStateMissing, "another create won the key", nil)
			}
		}
		if err != nil {
			e.discard()
			return unknown(storeError(err))
		}
	}
	ref := snap.Ref
	ref.Key = head.ObjectKey(key, inc, 0, head.NewID(), ref.SHA256)
	if err := n.putObject(ctx, ref.Key, buf.Bytes()); err != nil {
		e.discard()
		return unknown(err)
	}
	self := n.self
	next := head.Head{Format: head.Format, Namespace: n.ns, Key: key, Incarnation: inc, Revision: 1, Nonce: head.NewID(),
		Owner: &self, Epoch: 1, Sequence: snap.Ref.MaxSequence, Schema: req.Schema, Codec: head.Codec, PageSize: head.PageSize,
		PageCount: snap.PageCount, DatabaseChecksum: ref.PostChecksum, Snapshot: ref, Deltas: []head.Ref{}}
	rec, err := n.publish(e, &candidate{next: next, object: &ref, op: &op, bound: bound, pages: []uint32{snap.Pages}})
	if err != nil {
		return unknown(err)
	}
	if rec != nil { // another genesis carrying our receipt
		return Ref{key, e.h.Incarnation}, Result{Value: rec.Result, Version: Version{e.h.Incarnation, rec.Sequence}}, nil
	}
	return Ref{key, inc}, Result{Value: result, Version: Version{inc, next.Sequence}}, nil
}

func withOp(err error, id MutationID) error {
	var e *Error
	if errors.As(err, &e) && e.Operation == (MutationID{}) {
		e.Operation = id
	}
	return err
}

// Update runs fn as one mutation of ref (PROTOCOL §6, B = 1).
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
	bound := binding(kind, req.Schema, to, req.Hash)
	e, err := n.enter(ref.Key, &req.ID)
	if err != nil {
		return Result{}, err
	}
	defer e.mu.Unlock()
	for redo := 0; redo < 2; redo++ {
		if err := n.owned(ctx, e, ref, redo > 0); err != nil {
			return Result{}, withOp(err, req.ID)
		}
		if err := n.checkClock(req); err != nil { // admission starts with the clock (§6 step 3)
			return Result{}, err
		}
		res, verdict := n.admit(e, req, bound)
		var cb error
		var seq uint64
		var result []byte
		if verdict == nil && res == nil {
			floor := n.now().Add(-n.cfg.Limits.ReceiptRetention).UnixMilli()
			failpoint("sql.before")
			result, seq, err = e.db.Mutate(engine.Commit{ID: head.OpID(req.ID), Bound: bound, Schema: to, FloorMs: uint64(floor)},
				scoped(n.cfg.Limits.ResultBytes, func(t *engine.Tx) ([]byte, error) { failpoint("sql.inside"); return fn(&Tx{t}) }, &cb))
			if err != nil && (cb == nil || cb == errResultTooLarge) {
				e.discard()
				return Result{}, withOp(callbackError(err, cb), req.ID)
			}
			verdict = err
		}
		if verdict != nil || res != nil { // a state-dependent answer: barrier first
			changed, err := n.barrier(ctx, e)
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
		return n.publishUpdate(ctx, e, req, bound, to, result, seq)
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
		return nil, &Error{Code: CodeSchemaMismatch, Operation: req.ID, At: Version{e.h.Incarnation, e.h.Sequence}, Message: "stored schema differs"}
	}
	return nil, nil
}

// publishUpdate captures and publishes a locally committed mutation
// (PROTOCOL §6 steps 6–10).
func (n *Node) publishUpdate(ctx context.Context, e *entity, req Request, bound head.Digest, schema uint32, result []byte, seq uint64) (Result, error) {
	h, op := e.h, head.OpID(req.ID)
	fail := func(err error) (Result, error) { e.discard(); return Result{}, withOp(err, req.ID) }
	var buf bytes.Buffer
	delta, err := e.db.Delta(&buf)
	if err != nil {
		return fail(engineError(err))
	}
	failpoint("capture")
	next := *h
	next.Revision, next.Nonce, next.Sequence, next.Schema = h.Revision+1, head.NewID(), seq, schema
	next.PageCount, next.DatabaseChecksum = delta.PageCount, delta.Ref.PostChecksum
	c := &candidate{base: h, baseETag: e.etag, op: &op, bound: bound}
	if len(h.Deltas) == head.MaxDeltas || e.replay()+uint64(delta.Pages) > 2*uint64(delta.PageCount)+256 {
		buf.Reset() // snapshot on cap (§6 step 7)
		snap, err := e.db.Snapshot(&buf)
		if err != nil {
			return fail(engineError(err))
		}
		ref := snap.Ref
		ref.Key, ref.BaseRevision = head.ObjectKey(h.Key, h.Incarnation, h.Revision, head.NewID(), ref.SHA256), h.Revision
		next.Snapshot, next.Deltas, next.Tail, c.object, c.pages = ref, []head.Ref{}, nil, &ref, []uint32{snap.Pages}
		if err := n.putObject(ctx, ref.Key, buf.Bytes()); err != nil {
			return fail(err)
		}
	} else {
		t, tbytes := delta, buf.Bytes()
		if h.Tail != nil {
			var out bytes.Buffer
			t, err = engine.Compact(ctx, &out, []head.Ref{h.Tail.Ref(), delta.Ref},
				[]io.ReaderAt{bytes.NewReader(h.Tail.Data), bytes.NewReader(buf.Bytes())})
			if err != nil {
				return fail(engineError(err))
			}
			tbytes = out.Bytes()
		}
		c.pages = append(slices.Clone(e.pages[:1+len(h.Deltas)]), t.Pages) // the tail, or the new last delta
		uploaded := slices.ContainsFunc(append([]head.Ref{h.Snapshot}, h.Deltas...), func(r head.Ref) bool { return r.BaseRevision >= 1 })
		if t.Ref.Bytes <= head.InlineBytes && uploaded {
			r := t.Ref
			next.Tail = &head.Tail{SHA256: r.SHA256, Bytes: r.Bytes, MinSequence: r.MinSequence, MaxSequence: r.MaxSequence,
				PreChecksum: r.PreChecksum, PostChecksum: r.PostChecksum, Data: tbytes}
		} else {
			ref := t.Ref
			ref.Key, ref.BaseRevision = head.ObjectKey(h.Key, h.Incarnation, h.Revision, head.NewID(), ref.SHA256), h.Revision
			next.Deltas, next.Tail, c.object = append(slices.Clip(h.Deltas), ref), nil, &ref
			if err := n.putObject(ctx, ref.Key, tbytes); err != nil {
				return fail(err)
			}
		}
	}
	c.next = next
	rec, err := n.publish(e, c)
	if err != nil {
		return Result{}, withOp(err, req.ID)
	}
	n.maintainLater(e)
	if rec != nil {
		return Result{Value: rec.Result, Version: Version{e.h.Incarnation, rec.Sequence}}, nil
	}
	return Result{Value: result, Version: Version{h.Incarnation, seq}}, nil
}
