package kit

import (
	"context"
	"database/sql"
	"errors"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
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
	hops    atomic.Int64
}

func (c *cluster) Members() []string { c.mu.Lock(); defer c.mu.Unlock(); return slices.Clone(c.members) }

func (c *cluster) Forward(ctx context.Context, node string, call Call) (Reply, error) {
	c.mu.Lock()
	r, down := c.routers[node], c.down[node]
	c.mu.Unlock()
	if down || r == nil {
		return Reply{}, errors.New("unreachable")
	}
	c.hops.Add(1)
	return r.Serve(ctx, call), nil
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
