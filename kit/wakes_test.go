package kit

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	garden "github.com/OrdalieTech/big-garden"
	"github.com/OrdalieTech/big-garden/internal/storetest"
)

// kept reports whether key's wake row survived its last wake, due again
// near its arm plus grace (it was armed within grace: the business mutation
// might not have committed yet).
func kept(t *testing.T, c *cluster, key string) bool {
	n := c.routers["a"].n
	rt, err := n.Resolve(bg, "kit/wakes/0")
	if err != nil {
		t.Fatal(err)
	}
	var due int64
	n.Read(bg, rt.Ref, 1, func(v *garden.View) ([]byte, error) {
		return nil, v.Query(bg, "SELECT due - armed FROM kit_wakes WHERE key = ?", []any{key}, func(r *sql.Rows) error {
			if r.Next() {
				return r.Scan(&due)
			}
			return nil
		})
	})
	return due == grace.Milliseconds()
}

// TestWakes: an entity armed for two due times is woken for both, from
// whichever Node runs its shard, through a missing entity on the same shard;
// what the handler does is exact (each due row once), and the row armed
// within grace stays, due at its arm plus grace.
func TestWakes(t *testing.T) {
	c := newCluster(t, "a", "b")
	var ws []*Wakes
	for _, id := range []string{"a", "b"} {
		r := c.routers[id]
		r.Handle("todo", func(tx *garden.Tx, in []byte) ([]byte, error) {
			if _, err := tx.Exec(bg, "CREATE TABLE IF NOT EXISTS todo(at INTEGER NOT NULL); CREATE TABLE IF NOT EXISTS done(n INTEGER NOT NULL); INSERT INTO done SELECT 0 WHERE NOT EXISTS (SELECT 1 FROM done)"); err != nil {
				return nil, err
			}
			_, err := tx.Exec(bg, "INSERT INTO todo VALUES (?), (?)", ms(in), ms(in[8:]))
			return nil, err
		})
		ws = append(ws, NewWakes(r, 1, func(tx *garden.Tx, now time.Time) (time.Time, []byte, error) {
			if _, err := tx.Exec(bg, "UPDATE done SET n = n + (SELECT count(*) FROM todo WHERE at <= ?)", now.UnixMilli()); err != nil {
				return time.Time{}, nil, err
			}
			if _, err := tx.Exec(bg, "DELETE FROM todo WHERE at <= ?", now.UnixMilli()); err != nil {
				return time.Time{}, nil, err
			}
			var next sql.NullInt64
			err := tx.Query(bg, "SELECT min(at) FROM todo", nil, func(r *sql.Rows) error { r.Next(); return r.Scan(&next) })
			if !next.Valid {
				return time.Time{}, nil, err
			}
			return time.UnixMilli(next.Int64), nil, err
		}, nil))
	}
	ctx, cancel := context.WithCancel(bg)
	defer cancel()
	key := "e"
	if _, _, err := c.routers["a"].Mutate(bg, Call{Op: "create", Key: key, Req: req(t, "create"), Create: true}); err != nil {
		t.Fatal(err)
	}
	if err := ws[1].Arm(bg, "gone", time.Now()); err != nil { // a deleted or never created entity
		t.Fatal(err)
	}
	t1, t2 := time.Now().Add(300*time.Millisecond), time.Now().Add(900*time.Millisecond)
	if err := ws[0].Arm(bg, key, t1); err != nil {
		t.Fatal(err)
	}
	if _, _, err := c.routers["b"].Mutate(bg, Call{Op: "todo", Key: key, Req: req(t, "todo"), Input: stamp(t1.UnixMilli(), t2.UnixMilli())}); err != nil {
		t.Fatal(err)
	}
	for _, w := range ws {
		go w.Run(ctx, 50*time.Millisecond)
	}
	rt, err := c.routers["a"].n.Resolve(bg, key)
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(10 * time.Second)
	for {
		var n, left int
		if _, err := c.routers["a"].n.Read(bg, rt.Ref, 1, func(v *garden.View) ([]byte, error) {
			return nil, v.Query(bg, "SELECT (SELECT n FROM done), (SELECT count(*) FROM todo)", nil, func(r *sql.Rows) error { r.Next(); return r.Scan(&n, &left) })
		}); err != nil {
			t.Fatal(err)
		}
		if n == 2 && left == 0 && kept(t, c, key) {
			return
		}
		if n > 2 || time.Now().After(deadline) {
			t.Fatalf("done %d, %d left", n, left)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// TestWakesArmRacesShardCreation: the first arms of a shard, from two Nodes
// at once, race to create it; every arm still lands, whichever Node won and
// whether its create was still between birth and head.
func TestWakesArmRacesShardCreation(t *testing.T) {
	c := newCluster(t, "a", "b")
	ws := map[string]*Wakes{}
	for _, id := range []string{"a", "b"} {
		ws[id] = NewWakes(c.routers[id], 1, func(*garden.Tx, time.Time) (time.Time, []byte, error) { return time.Time{}, nil, nil }, nil)
	}
	errs := make(chan error, 8)
	for i := range 8 {
		go func() { errs <- ws[[]string{"a", "b"}[i%2]].Arm(bg, fmt.Sprint("key", i), time.Now().Add(time.Hour)) }()
	}
	for range 8 {
		if err := <-errs; err != nil {
			t.Fatal(err)
		}
	}
	n := c.routers["a"].n
	rt, err := n.Resolve(bg, "kit/wakes/0")
	if err != nil {
		t.Fatal(err)
	}
	var rows int
	n.Read(bg, rt.Ref, 1, func(v *garden.View) ([]byte, error) {
		return nil, v.Query(bg, "SELECT count(*) FROM kit_wakes", nil, func(r *sql.Rows) error { r.Next(); return r.Scan(&rows) })
	})
	if rows != 8 {
		t.Fatalf("%d of 8 arms landed", rows)
	}
}

// cutHeads refuses every head.json PUT: a Node that dies between its
// snapshot and its head.
type cutHeads struct{ http.RoundTripper }

func (c cutHeads) RoundTrip(r *http.Request) (*http.Response, error) {
	if r.Method == http.MethodPut && strings.HasSuffix(r.URL.Path, "/head.json") {
		return nil, errors.New("cut")
	}
	return c.RoundTripper.RoundTrip(r)
}

// TestWakesResumesACutShardCreate: the first arm's Node dies after the
// shard's snapshot, before its head; the next arm, on another Node, resumes
// that create instead of finding the shard state_missing.
func TestWakesResumesACutShardCreate(t *testing.T) {
	ep := storetest.Local(t).Sub("cut")
	a, err := garden.Open(bg, garden.Config{S3: ep.Client(&http.Client{Transport: cutHeads{http.DefaultTransport.(*http.Transport).Clone()}}),
		Bucket: ep.Bucket, Prefix: ep.Prefix, NodeID: "a", CacheDir: t.TempDir(),
		Limits: garden.Limits{StorageAttemptTimeout: 200 * time.Millisecond, CommitTimeout: time.Second}})
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close(bg)
	c := &cluster{members: []string{"a"}, routers: map[string]*Router{}, down: map[string]bool{}}
	c.routers["a"], c.routers["b"] = NewRouter(a, c, c), NewRouter(open(t, ep, "b"), c, c)
	nothing := func(*garden.Tx, time.Time) (time.Time, []byte, error) { return time.Time{}, nil, nil }
	if err := NewWakes(c.routers["a"], 1, nothing, nil).Arm(bg, "x", time.Now()); err == nil {
		t.Fatal("arm without a head succeeded")
	}
	c.members = []string{"b"}
	if err := NewWakes(c.routers["b"], 1, nothing, nil).Arm(bg, "y", time.Now()); err != nil {
		t.Fatal(err)
	}
}

// TestWakesResultAndBackoff: a wake's result reaches done once the wake is
// recorded, and a failing handler backs off instead of firing every tick.
func TestWakesResultAndBackoff(t *testing.T) {
	c := newCluster(t, "a")
	r := c.routers["a"]
	var calls atomic.Int32
	got := make(chan string, 4)
	w := NewWakes(r, 1, func(tx *garden.Tx, _ time.Time) (time.Time, []byte, error) {
		var key string
		tx.Query(bg, "SELECT name FROM who", nil, func(rows *sql.Rows) error { rows.Next(); return rows.Scan(&key) })
		if key == "fails" {
			calls.Add(1)
			return time.Time{}, nil, errors.New("handler failed")
		}
		return time.Time{}, []byte("claimed " + key), nil
	}, func(key string, result []byte) { got <- key + ": " + string(result) })
	r.Handle("named", func(tx *garden.Tx, in []byte) ([]byte, error) {
		_, err := tx.Exec(bg, "CREATE TABLE who(name TEXT); INSERT INTO who VALUES (?)", string(in))
		return nil, err
	})
	for _, key := range []string{"fails", "works"} {
		if _, _, err := r.Mutate(bg, Call{Op: "named", Key: key, Req: req(t, key), Input: []byte(key), Create: true}); err != nil {
			t.Fatal(err)
		}
		if err := w.Arm(bg, key, time.Now()); err != nil {
			t.Fatal(err)
		}
	}
	ctx, cancel := context.WithCancel(bg)
	defer cancel()
	go w.Run(ctx, 20*time.Millisecond)
	select {
	case s := <-got:
		if s != "works: claimed works" {
			t.Fatal(s)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("no result")
	}
	time.Sleep(500 * time.Millisecond) // 25 ticks
	if n := calls.Load(); n != 1 {
		t.Fatalf("the failing handler ran %d times in 25 ticks", n)
	}
}
