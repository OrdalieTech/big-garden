package storetest

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"slices"
	"sort"
	"strings"
	"sync"

	"github.com/OrdalieTech/big-garden/internal/store"
)

// MemFault is the fate of one Mem attempt. The zero MemFault applies the
// call atomically and replies truthfully.
type MemFault struct {
	// Reject, when not OK, replies with this outcome without applying:
	// Unknown is a 5xx, Failed a 403, PreconditionFailed an injected 412.
	Reject store.Outcome
	// Hold applies the call only once closed, whether or not the caller is
	// still waiting: delayed application, or application after the caller's
	// context ended (the caller then sees Unknown).
	Hold <-chan struct{}
	// LoseReply applies the call, then replies Unknown.
	LoseReply bool
	// Body rewrites the body of an OK Get (truncation, corruption): a
	// provider or transport that returns wrong bytes as success.
	Body func([]byte) []byte
}

// Attempt is one journal entry: one physical call.
type Attempt struct {
	Seq      int
	Op       string // "get", "put", "cas", "list", "delete"
	Key      string // key, or prefix for list
	Cond     string // ifNoneMatch for get, ifMatch for cas, after for list
	Body     []byte // put and cas only
	Fault    MemFault
	Applied  bool          // the call took effect (a read observed the state)
	Result   store.Outcome // outcome of the application, if Applied
	ETag     string        // ETag created by an applied write
	Returned bool          // the caller got a reply (false while held)
	Outcome  store.Outcome // what the caller was told, if Returned
}

// Mem is a linearizable in-memory Store: every call applies atomically at one
// instant under a mutex. ETags are opaque, quoted, and unique per write. Plan
// chooses each attempt's fault; the journal records every attempt.
type Mem struct {
	Plan func(Attempt) MemFault // nil: no faults; set before use; runs under the lock, must not call Mem

	mu      sync.Mutex
	objects map[string]memObject
	writes  int
	journal []Attempt
}

type memObject struct {
	body []byte
	etag string
}

func NewMem() *Mem { return &Mem{objects: map[string]memObject{}} }

// Journal returns a copy of every attempt so far, in arrival order.
func (m *Mem) Journal() []Attempt {
	m.mu.Lock()
	defer m.mu.Unlock()
	return slices.Clone(m.journal)
}

// Peek returns the current object at key, for test assertions.
func (m *Mem) Peek(key string) (store.Object, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	o, ok := m.objects[key]
	return store.Object{Body: bytes.Clone(o.body), ETag: o.etag}, ok
}

type memReply struct {
	obj  store.Object
	page []store.Entry
	more bool
	err  error
}

// call journals one attempt, applies it as its fault says, and replies.
func (m *Mem) call(ctx context.Context, a Attempt, max int64, limit int) memReply {
	m.mu.Lock()
	a.Seq = len(m.journal)
	a.Body = bytes.Clone(a.Body)
	if m.Plan != nil {
		a.Fault = m.Plan(a)
	}
	m.journal = append(m.journal, a)
	m.mu.Unlock()
	f := a.Fault
	if ctx.Err() != nil { // never sent
		return m.reply(a.Seq, memReply{err: memErr(a, store.Unknown, ctx.Err())})
	}
	if f.Reject != store.OK {
		return m.reply(a.Seq, memReply{err: memErr(a, f.Reject, errors.New("injected"))})
	}
	if f.Hold == nil {
		r := m.apply(a, max, limit)
		if f.LoseReply {
			r = memReply{err: memErr(a, store.Unknown, errors.New("reply lost"))}
		}
		return m.reply(a.Seq, r)
	}
	applied := make(chan memReply, 1)
	go func() { <-f.Hold; applied <- m.apply(a, max, limit) }()
	select {
	case r := <-applied:
		if f.LoseReply {
			r = memReply{err: memErr(a, store.Unknown, errors.New("reply lost"))}
		}
		return m.reply(a.Seq, r)
	case <-ctx.Done():
		return m.reply(a.Seq, memReply{err: memErr(a, store.Unknown, ctx.Err())})
	}
}

func (m *Mem) reply(seq int, r memReply) memReply {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.journal[seq].Returned, m.journal[seq].Outcome = true, store.OutcomeOf(r.err)
	return r
}

// apply performs the attempt atomically.
func (m *Mem) apply(a Attempt, max int64, limit int) memReply {
	m.mu.Lock()
	defer m.mu.Unlock()
	cur, exists := m.objects[a.Key]
	var r memReply
	fail := func(o store.Outcome, msg string) { r.err = memErr(a, o, errors.New(msg)) }
	switch a.Op {
	case "get":
		switch {
		case !exists:
			fail(store.NotFound, "no such key")
		case a.Cond != "" && a.Cond == cur.etag:
			fail(store.NotModified, "not modified")
		case int64(len(cur.body)) > max:
			fail(store.Failed, "object exceeds max")
		default:
			r.obj = store.Object{Body: bytes.Clone(cur.body), ETag: cur.etag}
			if a.Fault.Body != nil {
				r.obj.Body = a.Fault.Body(r.obj.Body)
			}
		}
	case "put", "cas":
		switch {
		case a.Cond == "" && exists, a.Cond != "" && (!exists || a.Cond != cur.etag):
			fail(store.PreconditionFailed, "precondition failed")
		default:
			m.writes++
			etag := fmt.Sprintf("\"m%d-%x\"", m.writes, len(a.Body))
			m.objects[a.Key] = memObject{body: a.Body, etag: etag}
			r.obj.ETag = etag
			m.journal[a.Seq].ETag = etag
		}
	case "list":
		var keys []string
		for k := range m.objects {
			if strings.HasPrefix(k, a.Key) && k > a.Cond {
				keys = append(keys, k)
			}
		}
		sort.Strings(keys)
		r.more = len(keys) > limit
		for _, k := range keys[:min(limit, len(keys))] {
			r.page = append(r.page, store.Entry{Key: k, Size: int64(len(m.objects[k].body))})
		}
	case "delete":
		delete(m.objects, a.Key)
	}
	m.journal[a.Seq].Applied, m.journal[a.Seq].Result = true, store.OutcomeOf(r.err)
	return r
}

func memErr(a Attempt, o store.Outcome, err error) error {
	return &store.Error{Outcome: o, Op: a.Op, Key: a.Key, Err: err}
}

func (m *Mem) Get(ctx context.Context, key, ifNoneMatch string, max int64) (store.Object, error) {
	r := m.call(ctx, Attempt{Op: "get", Key: key, Cond: ifNoneMatch}, max, 0)
	return r.obj, r.err
}

func (m *Mem) PutImmutable(ctx context.Context, key string, body []byte) error {
	return m.call(ctx, Attempt{Op: "put", Key: key, Body: body}, 0, 0).err
}

func (m *Mem) PutCAS(ctx context.Context, key string, body []byte, ifMatch string) (string, error) {
	r := m.call(ctx, Attempt{Op: "cas", Key: key, Cond: ifMatch, Body: body}, 0, 0)
	return r.obj.ETag, r.err
}

func (m *Mem) List(ctx context.Context, prefix, after string, limit int) ([]store.Entry, bool, error) {
	if limit < 1 || limit > 1000 {
		return nil, false, &store.Error{Outcome: store.Failed, Op: "list", Key: prefix, Err: errors.New("limit outside 1..1000")}
	}
	r := m.call(ctx, Attempt{Op: "list", Key: prefix, Cond: after}, 0, limit)
	return r.page, r.more, r.err
}

func (m *Mem) Delete(ctx context.Context, key string) error {
	return m.call(ctx, Attempt{Op: "delete", Key: key}, 0, 0).err
}

var _ store.Store = (*Mem)(nil)
