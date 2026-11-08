package garden

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"github.com/OrdalieTech/big-garden/internal/engine"
	"github.com/OrdalieTech/big-garden/internal/head"
	"github.com/OrdalieTech/big-garden/internal/store"
)

// Node is one process's handle on a namespace.
type Node struct {
	cfg  Config
	st   store.Store
	ns   head.ID
	self head.Owner
	dir  string   // session cache dir
	lock *os.File // holds the flock on dir
	now  func() time.Time

	mu       sync.Mutex
	entities map[string]*entity
	reserved int64 // cache bytes reserved by activations in progress
	closed   bool
}

// Open validates cfg and opens a Node on its namespace (PROTOCOL §8).
func Open(ctx context.Context, cfg Config) (*Node, error) {
	cfg, err := cfg.validate()
	if err != nil {
		return nil, err
	}
	return open(ctx, cfg, store.NewS3(cfg.S3, cfg.Bucket, cfg.Prefix, cfg.Limits.StorageAttemptTimeout))
}

// open runs PROTOCOL §8 steps 2–4 for a validated cfg on st.
func open(ctx context.Context, cfg Config, st store.Store) (*Node, error) {
	n := &Node{cfg: cfg, st: st, entities: map[string]*entity{}, now: time.Now}
	if err := n.namespace(ctx); err != nil {
		return nil, err
	}
	if err := n.probe(ctx); err != nil {
		return nil, err
	}
	n.self = head.Owner{Node: cfg.NodeID, Session: head.NewID()}
	if err := n.sessionDir(); err != nil {
		return nil, err
	}
	return n, nil
}

func (n *Node) namespace(ctx context.Context) error {
	for range 2 {
		obj, err := n.st.Get(ctx, head.NamespaceKey, "", 1<<10)
		if err == nil {
			ns, err := head.DecodeNamespace(obj.Body)
			if err != nil {
				return newError(CodeCorrupt, "namespace.json: "+err.Error(), nil)
			}
			n.ns = ns.Namespace
			return nil
		}
		if store.OutcomeOf(err) != store.NotFound {
			return storeError(err)
		}
		page, _, err := n.st.List(ctx, "entities/", "", 1)
		if err != nil {
			return storeError(err)
		}
		if len(page) > 0 {
			return newError(CodeStateMissing, "entities exist without namespace.json", nil)
		}
		body, _ := head.Namespace{Format: head.Format, Namespace: head.NewID(), EntityFormat: head.Format}.Encode()
		err = n.st.PutImmutable(ctx, head.NamespaceKey, body)
		if o := store.OutcomeOf(err); o != store.OK && o != store.PreconditionFailed && o != store.Unknown {
			return storeError(err)
		}
	}
	return newError(CodeUnavailable, "namespace.json is not readable", nil)
}

// probe checks the conditional-write semantics the protocol relies on.
func (n *Node) probe(ctx context.Context) error {
	p := "probe/" + head.NewID().String() + "/"
	x, y := p+"x", p+"y"
	defer n.st.Delete(context.WithoutCancel(ctx), x)
	defer n.st.Delete(context.WithoutCancel(ctx), y)
	bad := func(what string, err error) error {
		if o := store.OutcomeOf(err); o == store.Unknown {
			return storeError(err)
		}
		return newError(CodeUnsupported, "storage probe: "+what, err)
	}
	if err := n.st.PutImmutable(ctx, x, []byte("x")); err != nil {
		return bad("create", err)
	}
	if err := n.st.PutImmutable(ctx, x, []byte("x")); store.OutcomeOf(err) != store.PreconditionFailed {
		return bad("If-None-Match ignored", err)
	}
	e1, err := n.st.PutCAS(ctx, y, []byte("1"), "")
	if err != nil {
		return bad("conditional create", err)
	}
	if _, err := n.st.PutCAS(ctx, y, []byte("2"), e1); err != nil {
		return bad("If-Match", err)
	}
	if _, err := n.st.PutCAS(ctx, y, []byte("3"), e1); store.OutcomeOf(err) != store.PreconditionFailed {
		return bad("stale If-Match accepted", err)
	}
	obj, err := n.st.Get(ctx, y, "", 1)
	if err != nil || !bytes.Equal(obj.Body, []byte("2")) {
		return bad("read after write", err)
	}
	return nil
}

func (n *Node) sessionDir() error {
	if err := os.MkdirAll(n.cfg.CacheDir, 0o700); err != nil {
		return newError(CodeUnavailable, "cache dir", err)
	}
	n.dir = filepath.Join(n.cfg.CacheDir, n.self.Session.String())
	if err := os.Mkdir(n.dir, 0o700); err != nil {
		return newError(CodeUnavailable, "session dir", err)
	}
	f, ok := lockDir(n.dir, os.O_CREATE|os.O_EXCL)
	if !ok {
		os.RemoveAll(n.dir)
		return newError(CodeUnavailable, "session lock", nil)
	}
	n.lock = f
	entries, _ := os.ReadDir(n.cfg.CacheDir)
	for _, d := range entries { // older processes' sessions: removable once unlocked
		var id head.ID
		if d.Type() != os.ModeDir || d.Name() == n.self.Session.String() || id.UnmarshalText([]byte(d.Name())) != nil {
			continue
		}
		path := filepath.Join(n.cfg.CacheDir, d.Name())
		if f, ok := lockDir(path, 0); ok {
			os.RemoveAll(path)
			f.Close()
		}
	}
	return nil
}

// lockDir opens dir/lock (never following symlinks) and takes an exclusive
// non-blocking flock on it.
func lockDir(dir string, flags int) (*os.File, bool) {
	f, err := os.OpenFile(filepath.Join(dir, "lock"), os.O_RDWR|syscall.O_NOFOLLOW|flags, 0o600)
	if err != nil {
		return nil, false
	}
	if syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB) != nil {
		f.Close()
		return nil, false
	}
	return f, true
}

// Owner returns this Node's publishing identity.
func (n *Node) Owner() Owner { return Owner{NodeID: n.self.Node, Session: n.self.Session} }

// entity returns the resident entity of key, creating it, or closed.
func (n *Node) entity(key string) (*entity, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.closed {
		return nil, newError(CodeClosed, "", nil)
	}
	e := n.entities[key]
	if e == nil {
		e = &entity{key: key}
		n.entities[key] = e
	}
	return e, nil
}

// reserve claims cache bytes for one activation (PROTOCOL §4 step 1).
func (n *Node) reserve(b int64) bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.reserved+b > n.cfg.Limits.CacheBytes {
		return false
	}
	n.reserved += b
	return true
}

func (n *Node) unreserve(b int64) {
	n.mu.Lock()
	n.reserved -= b
	n.mu.Unlock()
}

// Close stops admissions, settles and discards every resident entity, and
// deletes the session cache dir (PROTOCOL §7). It never closes the S3 client.
func (n *Node) Close(ctx context.Context) error {
	n.mu.Lock()
	n.closed = true
	es := make([]*entity, 0, len(n.entities))
	for _, e := range n.entities {
		es = append(es, e)
	}
	n.mu.Unlock()
	ctx, cancel := context.WithTimeout(ctx, n.cfg.Limits.CommitTimeout)
	defer cancel()
	unknown := false
	for _, e := range es {
		e.mu.Lock()
		if e.unresolved != nil {
			n.resolve(ctx, e, e.unresolved)
			unknown = unknown || e.unresolved != nil
		}
		e.discard()
		e.mu.Unlock()
	}
	if n.lock != nil {
		os.RemoveAll(n.dir)
		n.lock.Close()
		n.lock = nil
	}
	if unknown {
		return newError(CodeOutcomeUnknown, "a publication stayed unresolved", nil)
	}
	return nil
}

// storeError maps a storage failure to an Error.
func storeError(err error) *Error {
	var e *Error
	if errors.As(err, &e) {
		return e
	}
	switch store.OutcomeOf(err) {
	case store.Failed:
		return newError(CodeUnavailable, "storage request failed", err)
	default:
		return newError(CodeUnavailable, "storage unreachable", err)
	}
}

// engineError maps a local SQLite failure to an Error.
func engineError(err error) *Error {
	switch {
	case errors.Is(err, engine.ErrCorrupt):
		return newError(CodeCorrupt, "local image failed validation", err)
	case engine.Full(err):
		return newError(CodeCapacity, "entity size limit reached", err)
	}
	return newError(CodeUnavailable, "local database", err)
}
