package engine

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/OrdalieTech/big-garden/internal/head"
	"modernc.org/sqlite"
	sqlite3 "modernc.org/sqlite/lib"
)

var bg = context.Background()

func opID(n uint64) head.OpID {
	var id head.OpID
	binary.BigEndian.PutUint64(id[:], n)
	binary.BigEndian.PutUint64(id[8:], n*0x9e3779b97f4a7c15)
	return id
}

func commit(n uint64) Commit {
	c := Commit{ID: opID(n), Schema: 1}
	c.Bound[0] = byte(n)
	return c
}

// fixture is a created database plus its published objects, so tests can
// restore it.
type fixture struct {
	t    *testing.T
	d    *DB
	dir  string
	n    uint64 // operation IDs used
	head head.Head
	objs [][]byte
}

func newFixture(t testing.TB, maxPages uint32) *fixture {
	t.Helper()
	dir := t.TempDir()
	f := &fixture{dir: dir}
	if tt, ok := t.(*testing.T); ok {
		f.t = tt
	}
	ns, inc := head.NewID(), head.NewID()
	d, err := Create(bg, filepath.Join(dir, "db"), maxPages, ns, "key", inc)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { d.Discard() })
	f.d = d
	f.head = head.Head{Namespace: ns, Key: "key", Incarnation: inc, Schema: 1}
	return f
}

// mutate runs one mutation and fails the test on error.
func (f *fixture) mutate(t testing.TB, sql string, args ...any) uint64 {
	t.Helper()
	f.n++
	_, seq, err := f.d.Mutate(commit(f.n), func(tx *Tx) ([]byte, error) {
		_, err := tx.Exec(bg, sql, args...)
		return nil, err
	})
	if err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
	return seq
}

// publish captures the pending mutations as a delta (or the first one as a
// snapshot), checkpoints, and records it in the fixture's head.
func (f *fixture) publish(t testing.TB, snapshot bool) Object {
	t.Helper()
	var b bytes.Buffer
	var o Object
	var err error
	if snapshot || len(f.objs) == 0 {
		o, err = f.d.Snapshot(&b)
		f.objs, f.head.Deltas = nil, []head.Ref{}
	} else {
		o, err = f.d.Delta(&b)
	}
	if err != nil {
		t.Fatal(err)
	}
	if err := f.d.Checkpoint(o); err != nil {
		t.Fatal(err)
	}
	if len(f.objs) == 0 {
		f.head.Snapshot = o.Ref
	} else {
		f.head.Deltas = append(f.head.Deltas, o.Ref)
	}
	f.objs = append(f.objs, b.Bytes())
	f.head.Sequence, f.head.PageCount, f.head.DatabaseChecksum = o.Ref.MaxSequence, o.PageCount, o.Ref.PostChecksum
	return o
}

func readers(objs [][]byte) []io.ReaderAt {
	r := make([]io.ReaderAt, len(objs))
	for i, b := range objs {
		r[i] = bytes.NewReader(b)
	}
	return r
}

// restore rebuilds the fixture's published head into a new file.
func (f *fixture) restore(t testing.TB) *DB {
	t.Helper()
	d, err := Restore(bg, filepath.Join(t.TempDir(), "restored"), head.MaxPages, &f.head, readers(f.objs))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { d.Discard() })
	return d
}

func scalar(t testing.TB, c interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}, q string) string {
	t.Helper()
	var v string
	if err := c.QueryRowContext(bg, q).Scan(&v); err != nil {
		t.Fatalf("%s: %v", q, err)
	}
	return v
}

func TestCreateSettings(t *testing.T) {
	f := newFixture(t, 1000)
	for q, want := range map[string]string{"PRAGMA page_size": "4096", "PRAGMA journal_mode": "wal",
		"PRAGMA wal_autocheckpoint": "0", "PRAGMA synchronous": "0", "PRAGMA trusted_schema": "0",
		"PRAGMA auto_vacuum": "1", "PRAGMA max_page_count": "1000", "PRAGMA foreign_keys": "1"} {
		if got := scalar(t, f.d.conn, q); got != want {
			t.Errorf("%s = %s, want %s", q, got, want)
		}
	}
	if v, _ := sqlite.Limit(f.d.conn, sqlite3.SQLITE_LIMIT_ATTACHED, -1); v != 0 {
		t.Errorf("attached limit %d", v)
	}
	st, err := os.Stat(f.d.path)
	if err != nil || st.Mode().Perm() != 0o600 {
		t.Fatalf("db file mode: %v %v", st.Mode(), err)
	}
	m, err := f.d.Meta()
	if err != nil || m.Sequence != 0 || m.Key != "key" || m.Incarnation != f.head.Incarnation {
		t.Fatalf("meta %+v %v", m, err)
	}
	// Genesis never reuses a file; an existing path is refused for both
	// creation and restoration, and a missing file is never created by open.
	if _, err := Create(bg, f.d.path, 1000, head.ID{}, "k", head.ID{}); !errors.Is(err, os.ErrExist) {
		t.Fatalf("Create over an existing file: %v", err)
	}
	if _, err := Restore(bg, f.d.path, 1000, &f.head, []io.ReaderAt{bytes.NewReader(nil)}); !errors.Is(err, os.ErrExist) {
		t.Fatal("Restore over an existing file")
	}
	missing := filepath.Join(f.dir, "missing")
	if _, err := open(bg, missing, 1000); err == nil {
		t.Fatal("open created a missing file")
	}
	if _, err := os.Stat(missing); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("open left a file: %v", err)
	}
	if _, err := Create(bg, "relative", 1000, head.ID{}, "k", head.ID{}); err == nil {
		os.Remove("relative")
		t.Fatal("relative path accepted")
	}
	// Discard deletes every file (DECISIONS D10).
	f.mutate(t, "CREATE TABLE t(a)")
	if err := f.d.Discard(); err != nil {
		t.Fatal(err)
	}
	for _, s := range []string{"", "-wal", "-shm"} {
		if _, err := os.Stat(f.d.path + s); !errors.Is(err, os.ErrNotExist) {
			t.Errorf("%s survived Discard: %v", s, err)
		}
	}
}

func TestMutateReceiptsAndRollback(t *testing.T) {
	f := newFixture(t, 1000)
	f.mutate(t, "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")
	f.publish(t, true)

	// A committed mutation stores its receipt with the result.
	res, seq, err := f.d.Mutate(commit(10), func(tx *Tx) ([]byte, error) {
		_, err := tx.Exec(bg, "INSERT INTO t(v) VALUES ('a')")
		return []byte("ok"), err
	})
	if err != nil || string(res) != "ok" || seq != 2 {
		t.Fatalf("mutate: %q %d %v", res, seq, err)
	}
	r, ok, err := f.d.Receipt(opID(10))
	if err != nil || !ok || string(r.Result) != "ok" || r.Sequence != 2 || r.InputHash != commit(10).Bound {
		t.Fatalf("receipt %+v %v %v", r, ok, err)
	}
	if _, ok, _ := f.d.Receipt(opID(11)); ok {
		t.Fatal("phantom receipt")
	}
	// A callback error is returned unchanged and rolls everything back,
	// including the receipt and the sequence.
	boom := errors.New("boom")
	_, _, err = f.d.Mutate(commit(11), func(tx *Tx) ([]byte, error) {
		tx.Exec(bg, "INSERT INTO t(v) VALUES ('b')")
		return nil, boom
	})
	if err != boom {
		t.Fatalf("callback error changed: %v", err)
	}
	// A panicking callback also rolls back and leaves the connection usable.
	func() {
		defer func() { recover() }()
		f.d.Mutate(commit(12), func(tx *Tx) ([]byte, error) {
			tx.Exec(bg, "INSERT INTO t(v) VALUES ('c')")
			panic("callback panic")
		})
	}()
	// A duplicate operation ID is refused by the receipt key.
	if _, _, err := f.d.Mutate(commit(10), func(*Tx) ([]byte, error) { return nil, nil }); err == nil {
		t.Fatal("duplicate operation ID committed")
	}
	if got := scalar(t, f.d.conn, "SELECT group_concat(v) FROM t"); got != "a" {
		t.Fatalf("rows after rollbacks: %s", got)
	}
	if m, _ := f.d.Meta(); m.Sequence != 2 {
		t.Fatalf("sequence %d after rollbacks", m.Sequence)
	}
	if _, ok, _ := f.d.Receipt(opID(11)); ok {
		t.Fatal("receipt of a failed callback survived")
	}

	// The handle is dead after its callback, also from a leaked goroutine.
	var leaked *Tx
	f.d.Mutate(commit(13), func(tx *Tx) ([]byte, error) { leaked = tx; return nil, nil })
	if _, err := leaked.Exec(bg, "INSERT INTO t(v) VALUES ('late')"); err != ErrScope {
		t.Fatalf("leaked Tx: %v", err)
	}
	if err := leaked.Query(bg, "SELECT 1", nil, nil); err != ErrScope {
		t.Fatalf("leaked Tx query: %v", err)
	}
	var view *View
	got, err := f.d.Read(func(v *View) ([]byte, error) {
		view = v
		var s string
		err := v.Query(bg, "SELECT group_concat(v) FROM t", nil, func(r *sql.Rows) error {
			r.Next()
			return r.Scan(&s)
		})
		return []byte(s), err
	})
	if err != nil || string(got) != "a" {
		t.Fatalf("read: %q %v", got, err)
	}
	if err := view.Query(bg, "SELECT 1", nil, nil); err != ErrScope {
		t.Fatalf("leaked View: %v", err)
	}

	// Receipt floor: receipts at or below the floor are pruned, at most
	// 1000 per mutation, and the stored floor never retreats.
	for i := uint64(100); i < 1300; i++ {
		c := commit(i)
		binary.BigEndian.PutUint64(c.ID[:], i) // timestamp i
		if _, _, err := f.d.Mutate(c, func(*Tx) ([]byte, error) { return nil, nil }); err != nil {
			t.Fatal(err)
		}
	}
	count := func() string { return scalar(t, f.d.conn, "SELECT count(*) FROM _state_receipts") }
	before := count()
	c := commit(5000)
	binary.BigEndian.PutUint64(c.ID[:], 5000)
	c.FloorMs = 1199 // receipts 100..1199 qualify: 1100 rows, 1000 deleted
	f.d.Mutate(c, func(*Tx) ([]byte, error) { return nil, nil })
	if b, a := before, count(); fmt.Sprint(mustInt(b)-1000+1) != a {
		t.Fatalf("pruned %s -> %s, want 1000 fewer plus the new receipt", b, a)
	}
	c = commit(5001)
	binary.BigEndian.PutUint64(c.ID[:], 5001)
	c.FloorMs = 3 // behind the stored floor
	f.d.Mutate(c, func(*Tx) ([]byte, error) { return nil, nil })
	if m, _ := f.d.Meta(); m.FloorMs != 1199 {
		t.Fatalf("floor moved back to %d", m.FloorMs)
	}
	if _, ok, _ := f.d.Receipt(opID(1300)); ok {
		t.Fatal("unexpected receipt")
	}
	if got := scalar(t, f.d.conn, "SELECT count(*) FROM _state_receipts WHERE operation_id < x'00000000000004b0'"); got != "0" {
		t.Fatalf("%s receipts at or below the floor remain after a second prune", got)
	}
}

func mustInt(s string) int {
	var n int
	fmt.Sscan(s, &n)
	return n
}

// TestAutoRollbackCannotCommit: SQLite rolls the whole library transaction
// back by itself on SQLITE_FULL without a statement journal and on an
// interrupted write. A callback that swallows the error and keeps writing
// must not commit: its later statements run in autocommit, where the commit
// hook refuses them, and the receipt update finds no row.
func TestAutoRollbackCannotCommit(t *testing.T) {
	f := newFixture(t, 40)
	f.mutate(t, "CREATE TABLE t(v BLOB)")
	f.publish(t, true)
	cases := map[string]func(*Tx) error{
		"full": func(tx *Tx) error {
			_, err := tx.Exec(bg, "INSERT INTO t VALUES (zeroblob(500000))")
			return err
		},
		"interrupt": func(tx *Tx) error {
			ctx, cancel := context.WithTimeout(bg, 20*time.Millisecond)
			defer cancel()
			_, err := tx.Exec(ctx, `INSERT INTO t SELECT x FROM (WITH RECURSIVE r(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM r)
				SELECT x FROM r) WHERE x < 0`)
			return err
		},
	}
	for name, fail := range cases {
		f.n++
		var later error
		_, _, err := f.d.Mutate(commit(f.n), func(tx *Tx) ([]byte, error) {
			if err := fail(tx); err == nil {
				t.Errorf("%s: statement succeeded", name)
			} else {
				t.Logf("%s: %v", name, err)
			}
			_, later = tx.Exec(bg, "INSERT INTO t VALUES (x'01')")
			return nil, nil
		})
		t.Logf("%s: write after the rollback: %v; mutation: %v", name, later, err)
		if err == nil || later == nil {
			t.Errorf("%s: committed after SQLite rolled the transaction back", name)
		}
		if m, _ := f.d.Meta(); m.Sequence != 1 {
			t.Fatalf("%s: sequence %d", name, m.Sequence)
		}
		if got := scalar(t, f.d.conn, "SELECT count(*) FROM t"); got != "0" {
			t.Fatalf("%s: %s rows escaped the rolled-back transaction", name, got)
		}
	}
	// The WAL holds nothing committed beyond the published image.
	f.mutate(t, "INSERT INTO t VALUES (1)")
	f.publish(t, false)
	f.restore(t)
}

func TestReturningAndBlobLimit(t *testing.T) {
	f := newFixture(t, 2000)
	f.mutate(t, "CREATE TABLE t(id INTEGER PRIMARY KEY, v BLOB)")
	_, _, err := f.d.Mutate(commit(20), func(tx *Tx) ([]byte, error) {
		// Reading one row of three still applies all three inserts.
		var first int
		err := tx.Query(bg, "INSERT INTO t(v) VALUES (x'01'), (x'02'), (x'03') RETURNING id", nil, func(r *sql.Rows) error {
			r.Next()
			return r.Scan(&first)
		})
		if err != nil || first != 1 {
			return nil, fmt.Errorf("returning: %d %v", first, err)
		}
		// The 1 MiB limit bounds values and whole rows (SQLITE_LIMIT_LENGTH).
		if _, err := tx.Exec(bg, "INSERT INTO t(v) VALUES (?)", make([]byte, valueBytes-64)); err != nil {
			return nil, fmt.Errorf("value just below 1 MiB refused: %v", err)
		}
		if _, err := tx.Exec(bg, "INSERT INTO t(v) VALUES (?)", make([]byte, valueBytes+1)); err == nil {
			return nil, errors.New("value above 1 MiB accepted")
		}
		if _, err := tx.Exec(bg, "INSERT INTO t(v) VALUES (zeroblob(?))", valueBytes+1); err == nil {
			return nil, errors.New("zeroblob above 1 MiB accepted")
		}
		if _, err := tx.Exec(bg, "SELECT '"+strings.Repeat("x", valueBytes)+"'"); err == nil {
			return nil, errors.New("SQL text above 1 MiB accepted")
		}
		return nil, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := scalar(t, f.d.conn, "SELECT count(*) FROM t"); got != "4" {
		t.Fatalf("%s rows", got)
	}
	f.publish(t, true)
	f.restore(t)
}

// TestScopeRace runs a leaked handle against the end of its callback: every
// call either completes inside the scope or fails with ErrScope; none runs
// in library mode.
func TestScopeRace(t *testing.T) {
	f := newFixture(t, 1000)
	f.mutate(t, "CREATE TABLE t(v)")
	for i := uint64(0); i < 50; i++ {
		var wg sync.WaitGroup
		f.d.Mutate(commit(100+i), func(tx *Tx) ([]byte, error) {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for {
					_, err := tx.Exec(bg, "INSERT INTO _state_meta SELECT * FROM _state_meta")
					if err == ErrScope {
						return
					}
					if err == nil {
						t.Error("leaked handle wrote library metadata")
						return
					}
				}
			}()
			time.Sleep(time.Millisecond)
			return nil, nil
		})
		wg.Wait()
	}
	if got := scalar(t, f.d.conn, "SELECT count(*) FROM _state_meta"); got != "1" {
		t.Fatalf("%s meta rows", got)
	}
}
