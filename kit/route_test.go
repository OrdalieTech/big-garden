package kit

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"io"
	"slices"
	"strconv"
	"sync"
	"testing"

	garden "github.com/OrdalieTech/big-garden"
	"github.com/OrdalieTech/big-garden/internal/storetest"
)

// cluster is n Routers on one namespace, a channel-free in-process
// transport, and a mutable membership.
type cluster struct {
	mu      sync.Mutex
	members []string
	routers map[string]*Router
	down    map[string]bool // the transport cannot reach these
}

func (c *cluster) Members() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return slices.Clone(c.members)
}

func (c *cluster) Forward(ctx context.Context, node string, call Call) (Reply, error) {
	c.mu.Lock()
	r, down := c.routers[node], c.down[node]
	c.mu.Unlock()
	if down || r == nil {
		return Reply{}, errors.New("unreachable")
	}
	return r.Serve(ctx, call), nil
}

func (c *cluster) ForwardFile(ctx context.Context, node string, call Call, body io.Reader) (Reply, error) {
	c.mu.Lock()
	r, down := c.routers[node], c.down[node]
	c.mu.Unlock()
	if down || r == nil {
		return Reply{}, errors.New("unreachable")
	}
	return r.ServeFile(ctx, call, body), nil
}

func newCluster(t *testing.T, ids ...string) *cluster {
	ep := storetest.Local(t).Sub("route")
	c := &cluster{members: ids, routers: map[string]*Router{}, down: map[string]bool{}}
	for _, id := range ids {
		r := NewRouter(open(t, ep, id), c, c)
		r.Handle("create", func(tx *garden.Tx, _ []byte) ([]byte, error) {
			_, err := tx.Exec(bg, "CREATE TABLE counter(n INTEGER NOT NULL); INSERT INTO counter VALUES (0)")
			return nil, err
		})
		r.Handle("incr", func(tx *garden.Tx, _ []byte) ([]byte, error) {
			if _, err := tx.Exec(bg, "UPDATE counter SET n = n + 1"); err != nil {
				return nil, err
			}
			var n int
			err := tx.Query(bg, "SELECT n FROM counter", nil, func(r *sql.Rows) error { r.Next(); return r.Scan(&n) })
			return []byte(strconv.Itoa(n)), err
		})
		c.routers[id] = r
	}
	return c
}

func (c *cluster) incr(t *testing.T, via, key string) int {
	t.Helper()
	res, _, err := c.routers[via].Mutate(bg, Call{Op: "incr", Key: key, Req: req(t, "incr")})
	if err != nil {
		t.Fatalf("incr %s via %s: %v", key, via, err)
	}
	n, _ := strconv.Atoi(string(res.Value))
	return n
}

// TestRouter: creates land on the Preferred node; mutations from any node
// reach the owner with one hop and no takeover; when the owner leaves the
// membership the Preferred member takes over once; a counter stays exact.
func TestRouter(t *testing.T) {
	c := newCluster(t, "a", "b", "c")
	key := "k"
	if _, _, err := c.routers["a"].Mutate(bg, Call{Op: "create", Key: key, Req: req(t, "create"), Create: true}); err != nil {
		t.Fatal(err)
	}
	pref, _ := garden.Preferred(key, c.Members())
	want := 0
	for i := range 30 {
		want++
		if got := c.incr(t, []string{"a", "b", "c"}[i%3], key); got != want {
			t.Fatalf("counter %d, want %d", got, want)
		}
	}
	for id, r := range c.routers { // sticky: only the preferred node ever owned it
		if id != pref && r.n.Stats().OwnerChanges != 0 {
			t.Fatalf("%s took the entity over while %s owned it", id, pref)
		}
	}
	c.mu.Lock() // the owner leaves: the next mutation moves the entity once
	c.members = slices.DeleteFunc(c.members, func(m string) bool { return m == pref })
	c.down[pref] = true
	c.mu.Unlock()
	var via string
	for _, m := range c.Members() {
		via = m
	}
	for range 10 {
		want++
		if got := c.incr(t, via, key); got != want {
			t.Fatalf("counter %d after the owner left, want %d", got, want)
		}
	}
	rt, _ := c.routers[via].n.Resolve(bg, key)
	if next, _ := garden.Preferred(key, c.Members()); rt.Owner.NodeID != next {
		t.Fatalf("owner %s after the owner left, want the preferred member %s", rt.Owner.NodeID, next)
	}
}

// TestRouterTransportLossIsUnknown: a mutation whose every forward fails is
// outcome_unknown for its ID, never an error that invites a fresh ID.
func TestRouterTransportLossIsUnknown(t *testing.T) {
	c := newCluster(t, "a", "b")
	key := "k"
	if _, _, err := c.routers["a"].Mutate(bg, Call{Op: "create", Key: key, Req: req(t, "create"), Create: true}); err != nil {
		t.Fatal(err)
	}
	pref, _ := garden.Preferred(key, c.Members())
	other := map[string]string{"a": "b", "b": "a"}[pref]
	c.mu.Lock()
	c.down[pref] = true // unreachable, yet still a member
	c.mu.Unlock()
	r := req(t, "lost")
	_, _, err := c.routers[other].Mutate(bg, Call{Op: "incr", Key: key, Req: r})
	var ge *garden.Error
	if !errors.As(err, &ge) || ge.Code != garden.CodeOutcomeUnknown || ge.Operation != r.ID {
		t.Fatalf("got %v, want outcome_unknown for the request", err)
	}
}

// TestRouterTakeoverRoutes: the Node that took an entity over remembers it
// owns it, so later forwarded mutations need no takeover, and a route
// remembered from a reply (owner only, no epoch) is observed before a
// takeover instead of failing it as stale.
func TestRouterTakeoverRoutes(t *testing.T) {
	c := newCluster(t, "a", "b", "c")
	key := "k"
	if _, _, err := c.routers["a"].Mutate(bg, Call{Op: "create", Key: key, Req: req(t, "create"), Create: true}); err != nil {
		t.Fatal(err)
	}
	pref, _ := garden.Preferred(key, c.Members())
	var via string
	for _, m := range c.Members() {
		if m != pref {
			via = m
		}
	}
	c.incr(t, via, key) // via remembers the owner from the reply: no epoch
	c.mu.Lock()
	c.members = slices.DeleteFunc(c.members, func(m string) bool { return m == pref })
	c.down[pref] = true
	c.mu.Unlock()
	next, _ := garden.Preferred(key, c.Members())
	r := c.routers[next]
	r.remember(key, garden.Route{Ref: garden.Ref{Key: key}, Owner: garden.Owner{NodeID: pref}})
	rt, _ := c.routers[via].n.Resolve(bg, key)
	if rep := r.run(bg, Call{Op: "incr", Key: key, Req: req(t, "local")}, garden.Route{Ref: rt.Ref, Owner: garden.Owner{NodeID: pref}}, nil); rep.Err != nil {
		t.Fatalf("takeover from an owner-only route: %v", rep.Err)
	}
	if got := r.owners[key].Owner.NodeID; got != next {
		t.Fatalf("%s remembers %q as the owner after taking over", next, got)
	}
	for range 3 {
		c.incr(t, via, key)
	}
}

// TestRouterFiles: a file sent from a Node that does not own the entity is
// stored on the owner, which alone may link it, and named in the same
// mutation; a retried Request replays; a Create carries its file from any
// Node; a FileHandler called without a file gets nil.
func TestRouterFiles(t *testing.T) {
	c := newCluster(t, "a", "b")
	for _, r := range c.routers {
		r.HandleFile("attach", func(tx *garden.Tx, input []byte, b *garden.Blob) ([]byte, error) {
			if b == nil {
				return []byte("none"), nil
			}
			if string(input) == "create" {
				if _, err := tx.Exec(bg, "CREATE TABLE t(x INTEGER)"); err != nil {
					return nil, err
				}
			}
			return []byte(strconv.FormatInt(b.Size, 10)), tx.SetFile("doc", b)
		})
	}
	key := "k"
	if _, _, err := c.routers["a"].Mutate(bg, Call{Op: "create", Key: key, Req: req(t, "create"), Create: true}); err != nil {
		t.Fatal(err)
	}
	pref, _ := garden.Preferred(key, c.Members())
	other := map[string]string{"a": "b", "b": "a"}[pref]
	body := bytes.Repeat([]byte("file "), 20000) // past the inline size: an object
	read := func(via, key string) ([]byte, int) {
		t.Helper()
		n := c.routers[via].n
		rt, err := n.Resolve(bg, key)
		if err != nil {
			t.Fatal(err)
		}
		var f garden.Blob
		var files int
		if _, err := n.Read(bg, rt.Ref, 1, func(v *garden.View) ([]byte, error) {
			var err error
			if f, _, err = v.File(bg, "doc"); err != nil {
				return nil, err
			}
			return nil, v.Query(bg, "SELECT count(DISTINCT object) FROM _state_blobs", nil, func(r *sql.Rows) error { r.Next(); return r.Scan(&files) })
		}); err != nil {
			t.Fatal(err)
		}
		rc, err := n.OpenBlob(bg, rt.Ref, f)
		if err != nil {
			t.Fatal(err)
		}
		defer rc.Close()
		got, err := io.ReadAll(rc)
		if err != nil {
			t.Fatal(err)
		}
		return got, files
	}
	r := req(t, "attach")
	for range 2 { // the retry replays
		res, _, err := c.routers[other].MutateFile(bg, Call{Op: "attach", Key: key, Req: r}, bytes.NewReader(body))
		if err != nil || string(res.Value) != strconv.Itoa(len(body)) {
			t.Fatalf("attach from %s: %q, %v", other, res.Value, err)
		}
	}
	if got, files := read(other, key); !bytes.Equal(got, body) || files != 1 {
		t.Fatalf("read back %d bytes in %d objects, want %d in 1", len(got), files, len(body))
	}
	if c.routers[other].n.Stats().OwnerChanges != 0 {
		t.Fatal("the sender took the entity over")
	}
	for _, via := range []string{"a", "b"} {
		k := "new-" + via
		if _, _, err := c.routers[via].MutateFile(bg, Call{Op: "attach", Key: k, Req: req(t, k), Input: []byte("create"), Create: true}, bytes.NewReader(body)); err != nil {
			t.Fatalf("create with a file from %s: %v", via, err)
		}
		if got, _ := read(via, k); !bytes.Equal(got, body) {
			t.Fatalf("created file read back %d bytes", len(got))
		}
	}
	if res, _, err := c.routers[other].Mutate(bg, Call{Op: "attach", Key: key, Req: req(t, "plain")}); err != nil || string(res.Value) != "none" {
		t.Fatalf("a file handler without a file: %q, %v", res.Value, err)
	}
}
