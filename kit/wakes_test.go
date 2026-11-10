package kit

import (
	"context"
	"database/sql"
	"testing"
	"time"

	garden "github.com/OrdalieTech/big-garden"
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
		ws = append(ws, NewWakes(r, 1, func(tx *garden.Tx, now time.Time) (time.Time, error) {
			if _, err := tx.Exec(bg, "UPDATE done SET n = n + (SELECT count(*) FROM todo WHERE at <= ?)", now.UnixMilli()); err != nil {
				return time.Time{}, err
			}
			if _, err := tx.Exec(bg, "DELETE FROM todo WHERE at <= ?", now.UnixMilli()); err != nil {
				return time.Time{}, err
			}
			var next sql.NullInt64
			err := tx.Query(bg, "SELECT min(at) FROM todo", nil, func(r *sql.Rows) error { r.Next(); return r.Scan(&next) })
			if !next.Valid {
				return time.Time{}, err
			}
			return time.UnixMilli(next.Int64), err
		}))
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
