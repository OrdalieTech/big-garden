package m0

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"modernc.org/sqlite"
)

const pageSize = 4096

func open(t *testing.T, path string) (*sql.DB, *sql.Conn) {
	t.Helper()
	db, err := sql.Open("sqlite", "file:"+path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	c, err := db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	return db, c
}

func exec(t *testing.T, c *sql.Conn, q string, args ...any) {
	t.Helper()
	if _, err := c.ExecContext(context.Background(), q, args...); err != nil {
		t.Fatalf("%s: %v", q, err)
	}
}

func pragma(t *testing.T, c *sql.Conn, q string) string {
	t.Helper()
	var v string
	if err := c.QueryRowContext(context.Background(), q).Scan(&v); err != nil {
		t.Fatalf("%s: %v", q, err)
	}
	return v
}

// setup applies and verifies the engine settings of PLAN §6.1 + DECISIONS D1.
func setup(t *testing.T, c *sql.Conn) {
	t.Helper()
	for _, q := range []string{"PRAGMA page_size=4096", "PRAGMA journal_mode=WAL", "PRAGMA wal_autocheckpoint=0",
		"PRAGMA synchronous=OFF", "PRAGMA trusted_schema=OFF"} {
		rows, err := c.QueryContext(context.Background(), q)
		if err != nil {
			t.Fatalf("%s: %v", q, err)
		}
		rows.Close()
	}
	for q, want := range map[string]string{"PRAGMA page_size": "4096", "PRAGMA journal_mode": "wal",
		"PRAGMA wal_autocheckpoint": "0", "PRAGMA synchronous": "0", "PRAGMA trusted_schema": "0"} {
		if got := pragma(t, c, q); got != want {
			t.Fatalf("%s = %q, want %q (set and verify, PLAN §6.1)", q, got, want)
		}
	}
}

// series is an INSERT source of n rows of expr (no generate_series needed).
func series(n int, expr string) string {
	return fmt.Sprintf("WITH RECURSIVE s(i) AS (SELECT 1 UNION ALL SELECT i+1 FROM s WHERE i<%d) SELECT %s FROM s", n, expr)
}

func TestSQLiteBuild(t *testing.T) {
	_, c := open(t, filepath.Join(t.TempDir(), "db"))
	v := pragma(t, c, "SELECT sqlite_version()")
	var a, b, p int
	fmt.Sscanf(v, "%d.%d.%d", &a, &b, &p)
	// WAL-reset bug fixed in 3.51.3 (PLAN §6.2, sqlite.org/wal.html).
	if a != 3 || b < 51 || (b == 51 && p < 3) {
		t.Fatalf("sqlite_version %s lacks the WAL-reset fix (>= 3.51.3)", v)
	}
	rows, err := c.QueryContext(context.Background(), "PRAGMA compile_options")
	if err != nil {
		t.Fatal(err)
	}
	var opts []string
	for rows.Next() {
		var o string
		rows.Scan(&o)
		opts = append(opts, o)
	}
	rows.Close()
	if !slices.Contains(opts, "ENABLE_DBPAGE_VTAB") {
		t.Fatalf("sqlite_dbpage not compiled in (D2): %v", opts)
	}
	t.Logf("sqlite_version=%s", v)
	t.Logf("compile_options=%s", strings.Join(opts, ","))
}

// policy is the three-mode authorizer of PLAN §6.1, reduced to the checks M0
// must prove. Switching mode re-registers the callback, which expires every
// prepared statement so nothing prepared under one mode runs under another.
type mode int

const (
	library mode = iota
	mutation
	read
)

var storagePragmas = []string{"journal_mode", "synchronous", "wal_autocheckpoint", "page_size", "locking_mode",
	"wal_checkpoint", "writable_schema", "trusted_schema", "cache_size", "mmap_size", "auto_vacuum", "temp_store",
	"journal_size_limit", "schema_version", "user_version", "foreign_keys", "query_only", "busy_timeout"}

func policy(m mode) sqlite.AuthorizerFn {
	return func(a sqlite.AuthorizerActionCode, arg1, arg2, db, _ string) sqlite.AuthorizerReturnCode {
		if m == library {
			return sqlite.AuthorizerOK
		}
		// arg1 or arg2 may name the table (e.g. CREATE TRIGGER/INDEX pass
		// the table as arg2): a trigger on a reserved table would run app
		// SQL inside library-mode statements.
		reserved := strings.HasPrefix(strings.ToLower(arg1), "_state_") || strings.HasPrefix(strings.ToLower(arg2), "_state_") ||
			strings.EqualFold(arg1, "sqlite_dbpage")
		switch a {
		case sqlite.AuthTransaction, sqlite.AuthSavepoint, sqlite.AuthAttach, sqlite.AuthDetach,
			sqlite.AuthCreateVTable, sqlite.AuthDropVTable:
			return sqlite.AuthorizerDeny
		case sqlite.AuthPragma:
			if slices.Contains(storagePragmas, strings.ToLower(arg1)) {
				return sqlite.AuthorizerDeny
			}
		case sqlite.AuthFunction:
			if strings.EqualFold(arg2, "load_extension") {
				return sqlite.AuthorizerDeny
			}
		case sqlite.AuthRead, sqlite.AuthSelect, sqlite.AuthRecursive:
		case sqlite.AuthInsert, sqlite.AuthUpdate, sqlite.AuthDelete, sqlite.AuthCreateTable, sqlite.AuthCreateIndex,
			sqlite.AuthDropTable, sqlite.AuthDropIndex, sqlite.AuthAlterTable, sqlite.AuthCreateTrigger,
			sqlite.AuthDropTrigger, sqlite.AuthCreateView, sqlite.AuthDropView, sqlite.AuthReindex, sqlite.AuthAnalyze:
			if m == read {
				return sqlite.AuthorizerDeny
			}
		default:
			return sqlite.AuthorizerDeny // unknown or temp-object actions
		}
		if reserved || (db != "" && db != "main") {
			return sqlite.AuthorizerDeny
		}
		return sqlite.AuthorizerOK
	}
}

func setMode(t *testing.T, c *sql.Conn, m mode) {
	t.Helper()
	err := c.Raw(func(dc any) error {
		r, ok := dc.(sqlite.AuthorizerRegisterer)
		if !ok {
			return errors.New("driver connection has no RegisterAuthorizer")
		}
		return r.RegisterAuthorizer(policy(m))
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestAuthorizerPerConnection(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "db")
	db, c := open(t, path)
	setup(t, c)
	exec(t, c, "CREATE TABLE _state_meta(k TEXT PRIMARY KEY, v)")
	exec(t, c, "CREATE TABLE app(id INTEGER PRIMARY KEY, v TEXT)")

	// Library-controlled transaction around application SQL (PLAN §6.4):
	// BEGIN in library mode, callback SQL in mutation mode, receipt + COMMIT
	// back in library mode.
	setMode(t, c, library)
	exec(t, c, "BEGIN IMMEDIATE")
	setMode(t, c, mutation)
	exec(t, c, "INSERT INTO app(v) VALUES ('a')")
	exec(t, c, "CREATE INDEX app_v ON app(v)")
	denied := []string{
		"COMMIT", "ROLLBACK", "BEGIN", "SAVEPOINT s", "RELEASE s", "END",
		"/* comment */ COMMIT", "INSERT INTO app(v) VALUES ('b'); COMMIT",
		"ATTACH DATABASE ':memory:' AS x", "DETACH DATABASE main",
		"PRAGMA journal_mode=DELETE", "PRAGMA synchronous=FULL", "PRAGMA wal_autocheckpoint=1000",
		"PRAGMA locking_mode=NORMAL", "PRAGMA writable_schema=ON", "PRAGMA wal_checkpoint(TRUNCATE)",
		"PRAGMA main.journal_mode", "PRAGMA page_size=8192", "PRAGMA cache_size=-100000",
		"SELECT load_extension('/tmp/x.so')",
		"SELECT * FROM _state_meta", "INSERT INTO _state_meta VALUES ('k', 1)", "DROP TABLE _state_meta",
		"CREATE TABLE _STATE_x(a)", "CREATE INDEX i ON _state_meta(v)",
		"CREATE TRIGGER tm AFTER INSERT ON _state_meta BEGIN SELECT 1; END", "CREATE TEMP TABLE t(a)", "SELECT data FROM sqlite_dbpage WHERE pgno=1",
		"WITH x AS (SELECT * FROM _state_meta) SELECT * FROM x",
	}
	for _, q := range denied {
		_, err := c.ExecContext(ctx, q)
		if err == nil || !(strings.Contains(err.Error(), "authorization denied") || strings.Contains(err.Error(), "not authorized")) {
			t.Errorf("mutation mode allowed %q: %v", q, err)
		}
	}
	// A trigger or view cannot smuggle reserved access: it is refused when
	// created or when it would run.
	for _, q := range [][2]string{
		{"CREATE TRIGGER tr AFTER INSERT ON app BEGIN INSERT INTO _state_meta VALUES ('x', 1); END", "INSERT INTO app(v) VALUES ('t')"},
		{"CREATE VIEW vw AS SELECT * FROM _state_meta", "SELECT * FROM vw"},
	} {
		_, err1 := c.ExecContext(ctx, q[0])
		_, err2 := c.ExecContext(ctx, q[1])
		if err1 == nil && err2 == nil {
			t.Errorf("reserved table reached through %q", q[0])
		}
		t.Logf("%.20s...: create=%v use=%v", q[0], err1, err2)
		c.ExecContext(ctx, "DROP TRIGGER IF EXISTS tr")
		c.ExecContext(ctx, "DROP VIEW IF EXISTS vw")
	}
	setMode(t, c, read)
	for _, q := range []string{"INSERT INTO app(v) VALUES ('c')", "DELETE FROM app", "CREATE TABLE z(a)", "COMMIT"} {
		if _, err := c.ExecContext(ctx, q); err == nil {
			t.Errorf("read mode allowed %q", q)
		}
	}
	if pragma(t, c, "SELECT count(*) FROM app") == "" {
		t.Fatal("read mode denied a plain read")
	}
	setMode(t, c, library)
	exec(t, c, "INSERT OR REPLACE INTO _state_meta VALUES ('sequence', 1)")
	exec(t, c, "COMMIT")

	// The policy lives on that one physical connection only: a second
	// connection of the same pool and an unrelated pool are unaffected.
	setMode(t, c, mutation)
	other, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()
	_, unrelated := open(t, filepath.Join(t.TempDir(), "other"))
	for _, o := range []*sql.Conn{other, unrelated} {
		for _, q := range []string{"PRAGMA synchronous=FULL", "BEGIN", "SELECT 1", "COMMIT",
			"ATTACH DATABASE ':memory:' AS x", "DETACH DATABASE x"} {
			if _, err := o.ExecContext(ctx, q); err != nil {
				t.Errorf("unrestricted connection refused %q: %v", q, err)
			}
		}
	}
	if pragma(t, other, "SELECT count(*) FROM _state_meta") != "1" {
		t.Fatal("other connection cannot read library metadata")
	}
	if _, err := c.ExecContext(ctx, "COMMIT"); err == nil {
		t.Fatal("policy did not survive use of other connections")
	}
}

func TestInsertReturning(t *testing.T) {
	ctx := context.Background()
	_, c := open(t, filepath.Join(t.TempDir(), "db"))
	setup(t, c)
	exec(t, c, "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT NOT NULL)")
	setMode(t, c, mutation)
	rows, err := c.QueryContext(ctx, "INSERT INTO t(v) VALUES (?), (?) RETURNING id, upper(v)", "a", "b")
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for rows.Next() {
		var id int
		var v string
		if err := rows.Scan(&id, &v); err != nil {
			t.Fatal(err)
		}
		got = append(got, fmt.Sprintf("%d %s", id, v))
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	rows.Close()
	if strings.Join(got, ",") != "1 A,2 B" {
		t.Fatalf("RETURNING rows %v", got)
	}
	if pragma(t, c, "SELECT group_concat(v) FROM t") != "a,b" {
		t.Fatal("RETURNING insert not applied")
	}
}

func TestContextInterruptsQuery(t *testing.T) {
	_, c := open(t, filepath.Join(t.TempDir(), "db"))
	setup(t, c)
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	start := time.Now()
	var n int64
	err := c.QueryRowContext(ctx,
		"WITH RECURSIVE r(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM r) SELECT count(*) FROM r").Scan(&n)
	elapsed := time.Since(start)
	if err == nil || elapsed > 5*time.Second {
		t.Fatalf("infinite query not interrupted: err=%v after %v", err, elapsed)
	}
	t.Logf("interrupted after %v: %v (errors.Is deadline: %v)", elapsed, err, errors.Is(err, context.DeadlineExceeded))
	// Report, do not assume, whether the pinned connection stays usable: the
	// engine must quarantine an entity whose connection went bad.
	var one int
	err = c.QueryRowContext(context.Background(), "SELECT 1").Scan(&one)
	t.Logf("pinned connection after interrupt: SELECT 1 -> %d, %v", one, err)
}

// walPages returns the page numbers named by the frame headers of a WAL file
// (D2). A frame is 24 header bytes + one page; the page number is the first
// big-endian uint32 of its header.
func walPages(t *testing.T, path string) []uint32 {
	t.Helper()
	b, err := os.ReadFile(path + "-wal")
	if err != nil {
		t.Fatal(err)
	}
	if len(b) < 32 || (len(b)-32)%(24+pageSize) != 0 {
		t.Fatalf("WAL size %d is not header + whole frames", len(b))
	}
	var pages []uint32
	for off := 32; off < len(b); off += 24 + pageSize {
		pages = append(pages, binary.BigEndian.Uint32(b[off:]))
	}
	slices.Sort(pages)
	return slices.Compact(pages)
}

func fileBytes(t *testing.T, path string) []byte {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil && !os.IsNotExist(err) {
		t.Fatal(err)
	}
	return b
}

// capture applies D2 to the published image base: for every page named in
// the WAL, read its committed content through sqlite_dbpage, then size the
// image to page_count.
func capture(t *testing.T, c *sql.Conn, path string, base []byte) ([]byte, []uint32) {
	t.Helper()
	var count uint32
	if err := c.QueryRowContext(context.Background(), "PRAGMA page_count").Scan(&count); err != nil {
		t.Fatal(err)
	}
	img := make([]byte, int(count)*pageSize)
	copy(img, base)
	var changed []uint32
	for _, p := range walPages(t, path) {
		if p > count {
			continue
		}
		var data []byte
		if err := c.QueryRowContext(context.Background(), "SELECT data FROM sqlite_dbpage WHERE pgno=?", p).Scan(&data); err != nil {
			t.Fatal(err)
		}
		if len(data) != pageSize {
			t.Fatalf("page %d has %d bytes", p, len(data))
		}
		copy(img[(p-1)*pageSize:], data)
		changed = append(changed, p)
	}
	return img, changed
}

func checkpoint(t *testing.T, c *sql.Conn, path string) []byte {
	t.Helper()
	var busy, log, done int
	if err := c.QueryRowContext(context.Background(), "PRAGMA wal_checkpoint(TRUNCATE)").Scan(&busy, &log, &done); err != nil {
		t.Fatal(err)
	}
	if busy != 0 || log != done {
		t.Fatalf("checkpoint incomplete: busy=%d log=%d checkpointed=%d", busy, log, done)
	}
	if st, err := os.Stat(path + "-wal"); err != nil || st.Size() != 0 {
		t.Fatalf("WAL not truncated: %v", err)
	}
	return fileBytes(t, path)
}

func integrity(t *testing.T, img []byte) {
	t.Helper()
	p := filepath.Join(t.TempDir(), "check")
	if err := os.WriteFile(p, img, 0o600); err != nil {
		t.Fatal(err)
	}
	_, c := open(t, p)
	if got := pragma(t, c, "PRAGMA integrity_check"); got != "ok" {
		t.Fatalf("integrity_check: %s", got)
	}
}

// TestDBPageCapture proves D2 against SQLite's own checkpoint as the
// independent oracle: with wal_autocheckpoint=0 the main file stays the
// published image, sqlite_dbpage returns committed pages still held in the
// WAL, pages of rolled-back (spilled) frames read back unchanged, and the
// captured image equals the checkpointed file byte for byte, through growth
// and shrinkage.
func TestDBPageCapture(t *testing.T) {
	path := filepath.Join(t.TempDir(), "db")
	_, c := open(t, path)
	setup(t, c)
	exec(t, c, "CREATE TABLE t(id INTEGER PRIMARY KEY, v BLOB)")
	exec(t, c, "CREATE TABLE big(id INTEGER PRIMARY KEY, v BLOB)")
	exec(t, c, "INSERT INTO t(v) "+series(200, "randomblob(300)"))
	published := checkpoint(t, c, path)

	rounds := []struct {
		name  string
		batch []string
	}{
		{"grow beyond 1000 pages", []string{
			"INSERT INTO big(v) " + series(1500, "randomblob(3000)"),
			"UPDATE t SET v = randomblob(200) WHERE id % 3 = 0",
			"DELETE FROM t WHERE id % 7 = 0",
		}},
		{"spilled rollback", []string{"PRAGMA cache_size=8", "ROLLBACKTEST", "UPDATE t SET v = x'00' WHERE id = 1"}},
		{"shrink by vacuum", []string{"DELETE FROM big", "VACUUM"}},
	}
	for _, r := range rounds {
		for _, q := range r.batch {
			if q == "ROLLBACKTEST" {
				exec(t, c, "BEGIN")
				exec(t, c, "INSERT INTO big(v) "+series(300, "randomblob(4000)"))
				exec(t, c, "ROLLBACK")
				continue
			}
			exec(t, c, q)
		}
		if !bytes.Equal(fileBytes(t, path), published) {
			t.Fatalf("%s: main file changed before checkpoint (wal_autocheckpoint=0 not honored)", r.name)
		}
		img, changed := capture(t, c, path, published)
		walN := len(walPages(t, path))
		oracle := checkpoint(t, c, path)
		if !bytes.Equal(img, oracle) {
			t.Fatalf("%s: captured image differs from SQLite's checkpointed file", r.name)
		}
		if bytes.Equal(oracle, published) {
			t.Fatalf("%s: batch changed nothing", r.name)
		}
		integrity(t, img)
		diff := 0
		for p := 0; p < len(oracle)/pageSize; p++ {
			if p >= len(published)/pageSize || !bytes.Equal(oracle[p*pageSize:(p+1)*pageSize], published[p*pageSize:(p+1)*pageSize]) {
				diff++
			}
		}
		if r.name == "spilled rollback" && walN <= diff {
			t.Fatalf("rolled-back transaction did not spill frames into the WAL (%d WAL pages, %d changed)", walN, diff)
		}
		t.Logf("%s: WAL names %d pages, %d captured, %d differ; %d -> %d pages",
			r.name, walN, len(changed), diff, len(published)/pageSize, len(img)/pageSize)
		published = oracle
	}
}

// TestCloseCheckpointsWAL records an engine hazard: closing the last
// connection checkpoints committed-but-unpublished WAL frames into the main
// file even with wal_autocheckpoint=0. Discarding speculative state therefore
// cannot be "close, delete -wal/-shm, reopen" (DECISIONS D10): the whole
// cache file must be discarded and rehydrated from the published head.
func TestCloseCheckpointsWAL(t *testing.T) {
	path := filepath.Join(t.TempDir(), "db")
	db, c := open(t, path)
	setup(t, c)
	exec(t, c, "CREATE TABLE t(v)")
	published := checkpoint(t, c, path)
	exec(t, c, "INSERT INTO t VALUES ('speculative')")
	c.Close()
	db.Close()
	if _, err := os.Stat(path + "-wal"); !os.IsNotExist(err) {
		t.Fatalf("WAL survived close: %v", err)
	}
	if bytes.Equal(fileBytes(t, path), published) {
		t.Fatal("close did not checkpoint: the published-main-file shortcut would be safe")
	}
	t.Log("close checkpointed unpublished frames into the main file")
}
