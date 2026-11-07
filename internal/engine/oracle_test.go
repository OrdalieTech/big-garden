package engine

import (
	"bytes"
	"database/sql"
	"errors"
	"flag"
	"fmt"
	"io"
	"math/rand/v2"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/OrdalieTech/big-garden/internal/head"
)

var (
	long = flag.Bool("long", false, "run the long randomized capture/replay oracle (>=100k transactions)")
	seed = flag.Uint64("seed", 0, "oracle seed (0 = time based)")
)

// profile is one database size class of the randomized oracle.
type profile struct {
	name                  string
	short, long           int    // transactions
	maxPages              uint32 // max_page_count
	target                int64  // bytes above which deletes dominate
	blob, big             int    // typical and occasional value sizes
	compareEvery, restore int    // batches between oracle comparisons / full restores
}

var profiles = []profile{
	{"small", 600, 50000, 300, 900 << 10, 2 << 10, 300 << 10, 1, 10},
	{"medium", 250, 35000, 4096, 8 << 20, 8 << 10, 1<<20 - 100, 4, 40},
	{"large", 120, 20000, 16384, 48 << 20, 32 << 10, 1<<20 - 100, 20, 200},
}

// TestOracleRandomized drives random transactions through Mutate, publishes
// them as deltas (and sometimes as snapshots), and checks every published
// state against SQLite's own image, read through the pager of a separate
// connection (sqlite3_serialize) and through the checkpointed main file:
// neither path shares code with the capture. The replayed image is kept by
// applying each published object to a separate file; full restores from the
// published chain (including compacted objects) run periodically with
// integrity_check. A counter row incremented by every callback must equal
// the number of committed mutations: rolled-back transactions leave nothing.
func TestOracleRandomized(t *testing.T) {
	s := *seed
	if s == 0 {
		s = uint64(time.Now().UnixNano())
	}
	total := 0
	for _, p := range profiles {
		n := p.short
		if *long {
			n = p.long
		}
		t.Run(p.name, func(t *testing.T) {
			t.Logf("seed %d", s)
			total += runOracle(t, p, n, rand.New(rand.NewPCG(s, uint64(len(p.name)))))
		})
	}
	t.Logf("%d transactions attempted", total)
}

type oracle struct {
	t        *testing.T
	p        profile
	rng      *rand.Rand
	f        *fixture
	replay   *os.File
	img      Image
	commits  int // committed mutations
	failed   int // callbacks that failed or were rolled back
	maxBytes int64
	compacts int
	restores int
}

func runOracle(t *testing.T, p profile, txs int, rng *rand.Rand) int {
	o := &oracle{t: t, p: p, rng: rng, f: newFixture(t, p.maxPages)}
	o.f.mutate(t, `CREATE TABLE counter(n INTEGER NOT NULL); INSERT INTO counter VALUES (0);
		CREATE TABLE t0(id INTEGER PRIMARY KEY, k INTEGER, v BLOB, s TEXT);
		CREATE TABLE log(id INTEGER PRIMARY KEY, msg TEXT)`)
	o.publish(true)
	start := time.Now()
	batches, done := 0, 0
	for ; done < txs; batches++ {
		b := 1 + o.rng.IntN(8)
		committed := 0
		for range b {
			if o.mutation() {
				committed++
			}
			done++
		}
		if committed > 0 {
			o.publish(o.rng.IntN(25) == 0)
		}
		if batches%p.compareEvery == 0 {
			o.compare()
		}
		if batches%p.restore == 0 {
			o.fullRestore()
		}
	}
	o.compare()
	o.fullRestore()
	t.Logf("%s: %d transactions (%d committed, %d rolled back) in %d batches, %v; max %d KiB; %d compactions, %d restores",
		p.name, done, o.commits, o.failed, batches, time.Since(start).Round(time.Millisecond), o.maxBytes>>10, o.compacts, o.restores)
	return done
}

func (o *oracle) pageCount() int64 {
	var n int64
	if err := o.f.d.conn.QueryRowContext(bg, "PRAGMA page_count").Scan(&n); err != nil {
		o.t.Fatal(err)
	}
	o.maxBytes = max(o.maxBytes, n*pageSize)
	return n * pageSize
}

// mutation runs one random transaction and reports whether it committed.
func (o *oracle) mutation() bool {
	r := o.rng
	full := o.pageCount() > o.p.target
	abort := r.IntN(12) == 0
	o.f.n++
	c := commit(o.f.n) // its ID's timestamp is n: keep about 1000 receipts
	c.FloorMs = max(o.f.n, 1000) - 1000
	_, _, err := o.f.d.Mutate(c, func(tx *Tx) ([]byte, error) {
		ex := func(q string, args ...any) error { _, err := tx.Exec(bg, q, args...); return err }
		if err := ex("UPDATE counter SET n = n + 1"); err != nil {
			return nil, err
		}
		for range 1 + r.IntN(5) {
			tbl := fmt.Sprintf("t%d", r.IntN(4))
			size := r.IntN(o.p.blob + 1)
			if r.IntN(30) == 0 {
				size = o.p.big
			}
			blob := []string{"randomblob(?)", "zeroblob(?)", "(randomblob(64) || zeroblob(?))"}[r.IntN(3)]
			var err error
			switch op := r.IntN(20); {
			case full && op < 8:
				err = ex(fmt.Sprintf("DELETE FROM %s WHERE id %% ? <> 0", tbl), 2+r.IntN(4))
			case op < 6:
				err = ex(fmt.Sprintf("INSERT INTO %s(k, v, s) VALUES (?, %s, ?)", tbl, blob), r.IntN(1000), size, fmt.Sprint(r.Int()))
			case op < 8:
				n := 1 + r.IntN(50)
				err = ex(fmt.Sprintf(`INSERT INTO %s(k, v, s) WITH RECURSIVE c(i) AS (SELECT 1 UNION ALL SELECT i+1 FROM c WHERE i < %d)
					SELECT i, %s, 'bulk' FROM c`, tbl, n, blob), size/8)
			case op < 11:
				err = ex(fmt.Sprintf("UPDATE %s SET v = %s, k = k + 1 WHERE id %% ? = ?", tbl, blob), size, 2+r.IntN(20), r.IntN(3))
			case op < 13:
				err = ex(fmt.Sprintf("DELETE FROM %s WHERE id %% ? = ?", tbl), 2+r.IntN(10), r.IntN(3))
			case op == 13:
				err = ex(fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s(id INTEGER PRIMARY KEY, k INTEGER, v BLOB, s TEXT)", tbl))
			case op == 14 && tbl != "t0":
				err = ex(fmt.Sprintf("DROP TABLE IF EXISTS %s", tbl))
			case op == 15:
				err = ex(fmt.Sprintf([]string{"CREATE INDEX IF NOT EXISTS %[1]s_k ON %[1]s(k)", "DROP INDEX IF EXISTS %[1]s_k",
					"CREATE UNIQUE INDEX IF NOT EXISTS %[1]s_s ON %[1]s(s)", "DROP INDEX IF EXISTS %[1]s_s"}[r.IntN(4)], tbl))
			case op == 16:
				err = ex([]string{
					fmt.Sprintf("CREATE TRIGGER IF NOT EXISTS %[1]s_log AFTER INSERT ON %[1]s BEGIN INSERT INTO log(msg) VALUES (new.s); END", tbl),
					fmt.Sprintf("DROP TRIGGER IF EXISTS %s_log", tbl), "DELETE FROM log WHERE id % 2 = 0"}[r.IntN(3)])
			case op == 17: // RETURNING, partly consumed
				err = tx.Query(bg, fmt.Sprintf("INSERT INTO %s(k, v, s) VALUES (1, %s, 'r'), (2, x'01', 'r') RETURNING id, length(v)", tbl, blob),
					[]any{size}, func(rows *sql.Rows) error { rows.Next(); return nil })
			case op == 18: // failed constraint: duplicate primary key, statement-level rollback only
				err = ex(fmt.Sprintf("INSERT INTO %[1]s(id, k, v) SELECT id, k, v FROM %[1]s LIMIT 1", tbl))
			default:
				err = ex(fmt.Sprintf("UPDATE %s SET s = NULL WHERE rowid IN (SELECT rowid FROM %[1]s ORDER BY random() LIMIT 3)", tbl))
			}
			_ = err // statement errors (missing tables, constraints, SQLITE_FULL) are part of the workload
		}
		if abort {
			return nil, errors.New("abort")
		}
		return nil, nil
	})
	if err == nil {
		o.commits++
		return true
	}
	// Only the callback's own abort, SQLite's own rollback and the page
	// limit are expected; anything else (I/O errors) invalidates the run.
	if err.Error() != "abort" && !strings.Contains(err.Error(), "rolled back during the callback") && !Full(err) {
		o.t.Fatalf("mutation %d: %v", o.f.n, err)
	}
	o.failed++
	return false
}

// publish captures the pending batch (as a snapshot when asked or when the
// chain is long), checkpoints, applies the object to the replay file, and
// compacts the published chain now and then.
func (o *oracle) publish(snapshot bool) {
	f, t := o.f, o.t
	var before Image
	if !snapshot {
		before = f.d.Published()
	}
	obj := f.publish(t, snapshot)
	if snapshot {
		o.resetReplay()
	} else {
		var err error
		if o.img != before {
			t.Fatalf("replay at %+v, database published %+v", o.img, before)
		}
		if o.img, err = apply(bg, o.replay, o.img, obj.Ref, bytes.NewReader(f.objs[len(f.objs)-1])); err != nil {
			t.Fatalf("replaying delta %d..%d: %v", obj.Ref.MinSequence, obj.Ref.MaxSequence, err)
		}
	}
	if o.img != f.d.Published() {
		t.Fatalf("replay %+v, published %+v", o.img, f.d.Published())
	}
	switch {
	case len(f.head.Deltas) >= 16: // merge the oldest eight deltas
		o.compact(1, 9)
	case len(f.objs) > 1 && o.rng.IntN(40) == 0: // fold everything into a new snapshot
		o.compact(0, len(f.objs))
		o.resetReplay()
	}
}

// compact replaces objects [i, j) of the chain (0 = snapshot) by one object
// made by Compact.
func (o *oracle) compact(i, j int) {
	f, t := o.f, o.t
	refs := append([]head.Ref{f.head.Snapshot}, f.head.Deltas...)
	var b bytes.Buffer
	c, err := Compact(bg, &b, refs[i:j], readers(f.objs[i:j]))
	if err != nil {
		t.Fatalf("compact %d..%d: %v", i, j, err)
	}
	refs = append(append(refs[:i:i], c.Ref), refs[j:]...)
	f.objs = append(append(f.objs[:i:i], b.Bytes()), f.objs[j:]...)
	f.head.Snapshot, f.head.Deltas = refs[0], refs[1:]
	if i == 0 && (c.Pages != c.PageCount || c.PageCount != f.head.PageCount) {
		t.Fatalf("folded snapshot has %d pages, commit %d, head %d", c.Pages, c.PageCount, f.head.PageCount)
	}
	o.compacts++
}

// resetReplay rebuilds the replay file from the chain's snapshot alone.
func (o *oracle) resetReplay() {
	f, t := o.f, o.t
	if o.replay != nil { // tmpfs pages count against the test's memory limit
		o.replay.Close()
		os.Remove(o.replay.Name())
	}
	var err error
	o.replay, err = os.CreateTemp(t.TempDir(), "replay")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { o.replay.Close() })
	o.img = Image{}
	if o.img, err = apply(bg, o.replay, Image{}, f.head.Snapshot, bytes.NewReader(f.objs[0])); err != nil {
		t.Fatal(err)
	}
	for i, d := range f.head.Deltas {
		if o.img, err = apply(bg, o.replay, o.img, d, bytes.NewReader(f.objs[i+1])); err != nil {
			t.Fatal(err)
		}
	}
}

// compare checks the replay file against SQLite's own image of the
// published state, and the counter against the committed mutations.
func (o *oracle) compare() {
	t := o.t
	want := serialize(t, o.f.d.path)
	main, err := os.ReadFile(o.f.d.path)
	if err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(io.NewSectionReader(o.replay, 0, 1<<40))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(want, main) {
		t.Fatalf("oracle paths disagree: serialized %d bytes, main file %d bytes", len(want), len(main))
	}
	if !bytes.Equal(got, want) {
		samePages(t, got, want)
	}
	m, err := o.f.d.Meta()
	if err != nil || m.Sequence != uint64(1+o.commits) {
		t.Fatalf("meta sequence %d, want %d: %v", m.Sequence, 1+o.commits, err)
	}
	if n := scalar(t, o.f.d.conn, "SELECT n FROM counter"); mustInt(n) != o.commits {
		t.Fatalf("counter %s, want %d committed mutations", n, o.commits)
	}
}

// fullRestore rebuilds the published chain into a new database (with
// integrity_check) and compares it with SQLite's image.
func (o *oracle) fullRestore() {
	t := o.t
	d := o.f.restore(t)
	got, err := os.ReadFile(d.path)
	if err != nil {
		t.Fatal(err)
	}
	samePages(t, got, serialize(t, o.f.d.path))
	if err := d.Discard(); err != nil {
		t.Fatal(err)
	}
	o.restores++
}

func samePages(t *testing.T, got, want []byte) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("replayed image has %d pages, SQLite's %d", len(got)/pageSize, len(want)/pageSize)
	}
	for p := 0; p < len(want); p += pageSize {
		if !bytes.Equal(got[p:p+pageSize], want[p:p+pageSize]) {
			t.Fatalf("page %d differs from SQLite's image", p/pageSize+1)
		}
	}
}

// serialize returns SQLite's image of the database at path through
// sqlite3_serialize on a separate connection (the pager, not sqlite_dbpage
// or the WAL frame list).
func serialize(t testing.TB, path string) []byte {
	t.Helper()
	db, err := sql.Open("sqlite", filepath.Clean(path))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	c, err := db.Conn(bg)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	var img []byte
	err = c.Raw(func(dc any) error {
		s, ok := dc.(interface{ Serialize() ([]byte, error) })
		if !ok {
			return errors.New("no Serialize")
		}
		img, err = s.Serialize()
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	return img
}
