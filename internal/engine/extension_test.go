package engine

import (
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/OrdalieTech/big-garden/internal/head"
	"modernc.org/sqlite"
	"modernc.org/sqlite/vtab"
)

// readString runs q in read mode and returns its single text value.
func readString(t *testing.T, d *DB, q string, args ...any) string {
	t.Helper()
	got, err := d.Read(func(v *View) ([]byte, error) {
		var s string
		err := v.Query(bg, q, args, func(r *sql.Rows) error {
			if !r.Next() {
				return errors.New("no row")
			}
			return r.Scan(&s)
		})
		return []byte(s), err
	})
	if err != nil {
		t.Fatalf("%s: %v", q, err)
	}
	return string(got)
}

// TestBuiltinVirtualTables creates, writes and queries the allowed built-in
// modules in mutation and read mode, checks that their shadow tables stay
// read-only to application SQL, and restores the image (capture exactness
// and integrity_check with fts5 and rtree present, DECISIONS D19).
func TestBuiltinVirtualTables(t *testing.T) {
	f := newFixture(t, 4000)
	f.mutate(t, `CREATE VIRTUAL TABLE docs USING fts5(body);
		INSERT INTO docs(body) VALUES ('the quick brown fox'), ('lazy dogs sleep'), ('a quick test');
		CREATE VIRTUAL TABLE docs_terms USING fts5vocab(docs, row);
		CREATE VIRTUAL TABLE box USING rtree(id, x0, x1, y0, y1);
		INSERT INTO box VALUES (1, 0, 1, 0, 1), (2, 5, 6, 5, 6), (3, 0.5, 2, 0.5, 2);
		CREATE VIRTUAL TABLE ibox USING rtree_i32(id, a, b);
		INSERT INTO ibox VALUES (1, 10, 20);
		CREATE VIRTUAL TABLE shapes USING geopoly(name);
		INSERT INTO shapes(_shape, name) VALUES ('[[0,0],[1,0],[1,1],[0,1],[0,0]]', 'square');
		CREATE TABLE app(id INTEGER PRIMARY KEY, v TEXT)`)
	f.publish(t, true)
	// fts5 commands are INSERTs into the table itself.
	f.mutate(t, `WITH RECURSIVE c(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM c WHERE n < 300)
		INSERT INTO docs(body) SELECT 'row ' || n FROM c;
		INSERT INTO docs(docs, rank) VALUES ('merge', 16); INSERT INTO docs(docs) VALUES ('optimize');
		INSERT INTO docs(docs) VALUES ('integrity-check'); DELETE FROM box WHERE id = 2; DROP TABLE ibox;
		CREATE VIRTUAL TABLE tmp USING fts5(a); INSERT INTO tmp VALUES ('x'); DROP TABLE tmp`)
	check := func(d *DB) {
		t.Helper()
		for q, want := range map[string]string{
			"SELECT group_concat(rowid) FROM docs WHERE docs MATCH 'quick' ORDER BY rank":                  "1,3",
			"SELECT count(*) FROM docs WHERE docs MATCH 'row'":                                             "300",
			"SELECT doc FROM docs_terms WHERE term = 'quick'":                                              "2",
			"SELECT group_concat(id) FROM box WHERE x1 >= 0.8 AND x0 <= 1.5 ORDER BY id":                   "1,3",
			"SELECT name FROM shapes WHERE geopoly_contains_point(_shape, 0.5, 0.5)":                       "square",
			"SELECT count(*) FROM sqlite_schema WHERE name LIKE 'ibox%' OR name LIKE 'tmp%'":               "0",
			"SELECT json_extract('{\"a\":[1,2]}', '$.a[1]') || sqrt(16) || (box.id) FROM box WHERE id = 3": "24.03",
		} {
			if got := readString(t, d, q); got != want {
				t.Errorf("%s = %q, want %q", q, got, want)
			}
		}
	}
	check(f.d)

	// Shadow tables: the authorizer lets the modules' own statements through,
	// so it cannot refuse these; defensive mode does.
	for _, q := range []string{
		"INSERT INTO docs_data VALUES (999, x'00')", "UPDATE docs_content SET c0 = 'x'", "DELETE FROM docs_idx",
		"DELETE FROM docs_docsize", "UPDATE docs_config SET v = 1", "DELETE FROM box_node", "UPDATE box_rowid SET nodeno = 9",
		"INSERT INTO box_parent VALUES (99, 1)", "DELETE FROM shapes_node", "DROP TABLE docs_data", "DROP TABLE box_node",
		"ALTER TABLE docs_data RENAME TO x", "ALTER TABLE box_node ADD COLUMN z",
		"CREATE TRIGGER st AFTER INSERT ON app BEGIN DELETE FROM docs_data; END; INSERT INTO app(v) VALUES ('t')",
		"CREATE TRIGGER st2 AFTER DELETE ON docs_data BEGIN SELECT 1; END",
		"CREATE VIRTUAL TABLE _state_x USING fts5(a)", "CREATE VIRTUAL TABLE _STATE_r USING rtree(id, a, b)",
		"CREATE VIRTUAL TABLE _state USING fts5(a)", // its shadow tables would be _state_data, ...
		"CREATE VIRTUAL TABLE s USING dbstat", "CREATE VIRTUAL TABLE s USING sqlite_dbpage",
		"CREATE VIRTUAL TABLE s USING nosuchmodule(a)", "CREATE VIRTUAL TABLE s USING fts4(a)",
		"CREATE VIRTUAL TABLE s USING json_each", "CREATE VIRTUAL TABLE s USING pragma_table_info",
		"CREATE VIRTUAL TABLE temp.s USING fts5(a)", "CREATE VIRTUAL TABLE s USING fts5vocab(main, _state_meta, row)",
		"CREATE VIRTUAL TABLE s USING fts5vocab(temp, docs, row)",
		"CREATE VIRTUAL TABLE s1 USING fts5(key, content=_state_meta)",
		"CREATE VIRTUAL TABLE s2 USING fts5(result, content=_state_receipts, content_rowid=sequence)",
		"SELECT load_extension('x')",
	} {
		f.n++
		var stmtErr error
		_, _, err := f.d.Mutate(commit(f.n), func(tx *Tx) ([]byte, error) {
			_, stmtErr = tx.Exec(bg, q)
			if stmtErr == nil && strings.Contains(q, "content=") {
				// external content is read when queried or rebuilt
				_, stmtErr = tx.Exec(bg, "SELECT * FROM "+q[21:23])
				_, err := tx.Exec(bg, "INSERT INTO "+q[21:23]+"("+q[21:23]+") VALUES ('rebuild')")
				stmtErr = errors.Join(stmtErr, err)
			}
			return nil, nil
		})
		if err != nil {
			t.Fatal(err)
		}
		if stmtErr == nil {
			t.Errorf("allowed: %q", q)
		}
		t.Logf("%.60s: %v", q, stmtErr)
	}
	// Read mode refuses writes and module creation; query_only=ON refuses
	// writes to an ordinary table named like an rtree shadow table, which
	// the read-mode authorizer lets through (rtree prepares its writes when
	// it connects).
	f.mutate(t, "CREATE TABLE x_node(a); CREATE TABLE y_rowid(a); CREATE TABLE z_parent(a)")
	for _, q := range []string{"INSERT INTO docs(body) VALUES ('r')", "INSERT INTO docs(docs) VALUES ('optimize')",
		"DELETE FROM box", "CREATE VIRTUAL TABLE r USING fts5(a)", "DROP TABLE docs", "INSERT INTO x_node VALUES (1)",
		"INSERT INTO y_rowid VALUES (1) RETURNING a", "DELETE FROM z_parent", "UPDATE box_node SET data = x''",
		"INSERT INTO box_rowid VALUES (9, 9)", "PRAGMA query_only=0"} {
		if _, err := f.d.Read(func(v *View) ([]byte, error) { return nil, v.Query(bg, q, nil, nil) }); err == nil {
			t.Errorf("read mode allowed %q", q)
		}
	}
	check(f.d)
	f.publish(t, false)
	r := f.restore(t) // Restore verifies the checksum and runs integrity_check
	check(r)
	if got := scalar(t, r.conn, "PRAGMA integrity_check"); got != "ok" {
		t.Fatalf("integrity_check: %s", got)
	}
}

// TestTrustedSchemaVirtualTables records how trusted_schema=OFF treats
// virtual tables used from schema objects: an external-content fts5 kept in
// sync by triggers (the usual pattern) and a view over rtree.
func TestTrustedSchemaVirtualTables(t *testing.T) {
	f := newFixture(t, 4000)
	f.mutate(t, `CREATE TABLE notes(id INTEGER PRIMARY KEY, body TEXT);
		CREATE VIRTUAL TABLE notes_fts USING fts5(body, content=notes, content_rowid=id);
		CREATE TRIGGER notes_ai AFTER INSERT ON notes BEGIN
			INSERT INTO notes_fts(rowid, body) VALUES (new.id, new.body); END;
		CREATE VIRTUAL TABLE box USING rtree(id, x0, x1);
		CREATE VIEW near AS SELECT id FROM box WHERE x0 <= 1`)
	f.n++
	var trig, view error
	if _, _, err := f.d.Mutate(commit(f.n), func(tx *Tx) ([]byte, error) {
		_, trig = tx.Exec(bg, "INSERT INTO notes(body) VALUES ('hello world'); INSERT INTO box VALUES (1, 0, 1)")
		_, view = tx.Exec(bg, "SELECT * FROM near")
		return nil, nil
	}); err != nil {
		t.Fatal(err)
	}
	if trig != nil || view != nil {
		t.Fatalf("fts5 from a trigger: %v; rtree from a view: %v", trig, view)
	}
	if got := readString(t, f.d, "SELECT group_concat(rowid) FROM notes_fts WHERE notes_fts MATCH 'hello'"); got != "1" {
		t.Fatalf("external content match %q", got)
	}
}

// series is a stateless Go module: CREATE VIRTUAL TABLE t USING series(n)
// yields the integers 1..n. Its only state is its arguments, kept in the
// schema, so it replicates with the file.
type series struct{}
type seriesTable struct{ n int64 }
type seriesCursor struct{ i, n int64 }

func (series) Create(c vtab.Context, args []string) (vtab.Table, error) {
	return seriesConnect(c, args)
}
func (series) Connect(c vtab.Context, args []string) (vtab.Table, error) {
	return seriesConnect(c, args)
}
func seriesConnect(c vtab.Context, args []string) (vtab.Table, error) {
	if len(args) != 4 {
		return nil, errors.New("series(n)")
	}
	n, err := strconv.ParseInt(args[3], 10, 64)
	if err != nil {
		return nil, err
	}
	return &seriesTable{n}, c.Declare("CREATE TABLE x(value INTEGER)")
}
func (t *seriesTable) BestIndex(*vtab.IndexInfo) error { return nil }
func (t *seriesTable) Open() (vtab.Cursor, error)      { return &seriesCursor{n: t.n}, nil }
func (t *seriesTable) Disconnect() error               { return nil }
func (t *seriesTable) Destroy() error                  { return nil }
func (c *seriesCursor) Filter(int, string, []vtab.Value) error {
	c.i = 1
	return nil
}
func (c *seriesCursor) Next() error                    { c.i++; return nil }
func (c *seriesCursor) Eof() bool                      { return c.i > c.n }
func (c *seriesCursor) Column(int) (vtab.Value, error) { return c.i, nil }
func (c *seriesCursor) Rowid() (int64, error)          { return c.i, nil }
func (c *seriesCursor) Close() error                   { return nil }

func double(_ *sqlite.FunctionContext, a []driver.Value) (driver.Value, error) {
	if n, ok := a[0].(int64); ok {
		return 2 * n, nil
	}
	return nil, fmt.Errorf("double: want an integer")
}

// TestGoExtensions registers a function and a module on a private driver:
// both work in statements, neither reaches another driver, the module is
// allowed by the authorizer, functions are refused in schema objects by
// trusted_schema=OFF, and an image using the module restores without it.
func TestGoExtensions(t *testing.T) {
	ext, err := NewDriver(func(d *sqlite.Driver) error {
		if err := d.RegisterDeterministicScalarFunction("double", 1, double); err != nil {
			return err
		}
		return d.RegisterModule("series", series{})
	})
	if err != nil {
		t.Fatal(err)
	}
	if !ext.modules["series"] || plain.modules["series"] || ext.modules["dbstat"] {
		t.Fatalf("modules %v / %v", ext.modules, plain.modules)
	}
	f := newFixture(t, 4000)
	f.d.Discard()
	ns, inc := head.NewID(), head.NewID()
	if f.d, err = Create(bg, ext, filepath.Join(t.TempDir(), "db"), 4000, ns, "key", inc); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { f.d.Discard() })
	f.head = head.Head{Namespace: ns, Key: "key", Incarnation: inc, Schema: 1}
	f.mutate(t, `CREATE TABLE app(n INTEGER); CREATE VIRTUAL TABLE s USING series(4);
		INSERT INTO app SELECT double(value) FROM s`)
	if got := readString(t, f.d, "SELECT group_concat(n) || '|' || double(21) FROM app"); got != "2,4,6,8|42" {
		t.Fatalf("got %q", got)
	}
	// Functions in schema objects: refused when created or when used.
	for _, p := range [][2]string{
		{"CREATE INDEX di ON app(double(n))", ""},
		{"CREATE TABLE g(n INTEGER, d INTEGER AS (double(n)))", "INSERT INTO g(n) VALUES (1)"},
		{"CREATE TABLE c(n INTEGER CHECK (double(n) > 0))", "INSERT INTO c VALUES (1)"},
		{"CREATE VIEW dv AS SELECT double(n) FROM app", "SELECT * FROM dv"},
		{"CREATE TRIGGER dt AFTER INSERT ON app BEGIN SELECT double(new.n); END", "INSERT INTO app VALUES (1)"},
		{"CREATE VIEW sv AS SELECT * FROM s", "SELECT * FROM sv"}, // a Go module is not innocuous either
	} {
		f.n++
		var createErr, useErr error
		_, _, err := f.d.Mutate(commit(f.n), func(tx *Tx) ([]byte, error) {
			if _, createErr = tx.Exec(bg, p[0]); createErr == nil && p[1] != "" {
				_, useErr = tx.Exec(bg, p[1])
			}
			return nil, nil
		})
		if err != nil {
			t.Fatal(err)
		}
		if createErr == nil && useErr == nil {
			t.Errorf("%q then %q allowed", p[0], p[1])
		}
		if e := errors.Join(createErr, useErr); !strings.Contains(e.Error(), "unsafe use of") {
			t.Errorf("%q: unclear error %v", p[0], e)
		}
		t.Logf("%.50s: create=%v use=%v", p[0], createErr, useErr)
	}
	f.mutate(t, "DROP VIEW IF EXISTS dv; DROP VIEW IF EXISTS sv; DROP TRIGGER IF EXISTS dt; DROP TABLE IF EXISTS g; DROP TABLE IF EXISTS c")
	f.publish(t, true)

	// The private registrations reach no other driver.
	other := filepath.Join(t.TempDir(), "other.db")
	for _, db := range []*sql.DB{mustOpen(t, "sqlite", other), sql.OpenDB(connector{plain.sqlite, other})} {
		for _, q := range []string{"SELECT double(1)", "CREATE VIRTUAL TABLE s USING series(1)"} {
			if _, err := db.Exec(q); err == nil {
				t.Errorf("%q works on another driver", q)
			}
		}
		db.Close()
	}
	// Activation never runs caller code: the image restores on a driver
	// without the module or function; using them fails with SQLite's error.
	r := f.restore(t)
	if got := scalar(t, r.conn, "SELECT group_concat(n) FROM app"); got != "2,4,6,8" {
		t.Fatalf("restored %q", got)
	}
	if _, err := r.Read(func(v *View) ([]byte, error) { return nil, v.Query(bg, "SELECT * FROM s", nil, nil) }); err == nil ||
		errors.Is(err, ErrCorrupt) || !strings.Contains(err.Error(), "no such module: series") {
		t.Fatalf("missing module: %v", err)
	}
	if _, err := r.Read(func(v *View) ([]byte, error) { return nil, v.Query(bg, "SELECT double(1)", nil, nil) }); err == nil ||
		!strings.Contains(err.Error(), "no such function: double") {
		t.Fatalf("missing function: %v", err)
	}

	// Extend errors and collations are refused.
	if _, err := NewDriver(func(*sqlite.Driver) error { return errors.New("boom") }); err == nil {
		t.Fatal("extend error accepted")
	}
	if _, err := NewDriver(func(d *sqlite.Driver) error {
		return d.RegisterCollationUtf8("rev", func(a, b string) int { return strings.Compare(b, a) })
	}); err == nil || !strings.Contains(err.Error(), "collation") {
		t.Fatalf("collation: %v", err)
	}
}

func mustOpen(t *testing.T, name, dsn string) *sql.DB {
	t.Helper()
	db, err := sql.Open(name, dsn)
	if err != nil {
		t.Fatal(err)
	}
	return db
}

// TestFTS5OutOfSync makes external-content and contentless fts5 indexes
// disagree with their content, through the 'delete' command with wrong
// values, then restores: activation's integrity_check must accept the
// published image (it is exactly what was committed).
func TestFTS5OutOfSync(t *testing.T) {
	f := newFixture(t, 4000)
	f.mutate(t, `CREATE TABLE notes(id INTEGER PRIMARY KEY, body TEXT);
		CREATE VIRTUAL TABLE nf USING fts5(body, content=notes, content_rowid=id);
		INSERT INTO notes VALUES (1, 'alpha beta'), (2, 'gamma');
		INSERT INTO nf(rowid, body) VALUES (1, 'alpha beta');
		CREATE VIRTUAL TABLE cl USING fts5(body, content='');
		INSERT INTO cl(rowid, body) VALUES (1, 'x y')`)
	for _, q := range []string{
		"INSERT INTO nf(nf, rowid, body) VALUES ('delete', 1, 'zeta omega')",
		"INSERT INTO nf(nf, rowid, body) VALUES ('delete', 7, 'gamma')",
		"INSERT INTO nf(nf, rowid, body) VALUES ('delete', 1, 'alpha')",
		"INSERT INTO cl(cl, rowid, body) VALUES ('delete', 1, 'q r s')",
		"INSERT INTO cl(cl, rowid, body) VALUES ('delete', 1, 'x')",
		"INSERT INTO cl(cl, rowid, body) VALUES ('delete', 9, 'x y')",
		"INSERT INTO nf(rowid, body) VALUES (1, 'alpha beta')",
		"DELETE FROM notes",
		"INSERT INTO nf(nf) VALUES ('optimize')",
	} {
		f.n++
		var stmtErr error
		if _, _, err := f.d.Mutate(commit(f.n), func(tx *Tx) ([]byte, error) {
			_, stmtErr = tx.Exec(bg, q)
			return nil, nil
		}); err != nil {
			t.Fatal(err)
		}
		t.Logf("%s: %v", q, stmtErr)
	}
	f.publish(t, true)
	r := f.restore(t)
	if got := scalar(t, r.conn, "PRAGMA integrity_check"); got != "ok" {
		t.Fatalf("integrity_check: %s", got)
	}
}

// TestRenameIntoReserved: ALTER TABLE ... RENAME TO passes the old name to
// the authorizer only, so Mutate checks the schema after the callback and
// rolls back a mutation that moved a table (and a module's shadow tables)
// into a reserved name.
func TestRenameIntoReserved(t *testing.T) {
	f := newFixture(t, 4000)
	f.mutate(t, "CREATE TABLE app(a); CREATE VIRTUAL TABLE docs USING fts5(b); CREATE VIRTUAL TABLE box USING rtree(id, a, b)")
	schema := scalar(t, f.d.conn, "SELECT group_concat(name) FROM sqlite_schema")
	for _, q := range []string{"ALTER TABLE app RENAME TO _state_new", "ALTER TABLE app RENAME TO _STATE_x",
		"ALTER TABLE docs RENAME TO _state_y", "ALTER TABLE box RENAME TO _state_z", "ALTER TABLE app RENAME TO dbstat",
		"ALTER TABLE app RENAME TO DBSTAT"} {
		f.n++
		if _, _, err := f.d.Mutate(commit(f.n), func(tx *Tx) ([]byte, error) {
			_, err := tx.Exec(bg, q)
			return nil, err
		}); !errors.Is(err, ErrReserved) {
			t.Errorf("%s: %v", q, err)
		}
	}
	if got := scalar(t, f.d.conn, "SELECT group_concat(name) FROM sqlite_schema"); got != schema {
		t.Fatalf("schema %s, want %s", got, schema)
	}
	f.mutate(t, "ALTER TABLE docs RENAME TO docs2; ALTER TABLE box RENAME TO box2; ALTER TABLE app RENAME TO state_app")
	if got := readString(t, f.d, "SELECT count(*) FROM sqlite_schema WHERE name IN ('docs2_data', 'box2_node', 'state_app')"); got != "3" {
		t.Fatalf("renamed shadow tables: %s", got)
	}
}

// TestForeignKeyToReserved: the authorizer never sees REFERENCES targets, so
// a foreign key to a library table would commit and make every later
// mutation fail in the library's own receipt prune or meta update ("foreign
// key mismatch", or a cascade into application rows). Mutate rolls it back.
func TestForeignKeyToReserved(t *testing.T) {
	f := newFixture(t, 4000)
	f.mutate(t, "CREATE TABLE pin(a)")
	schema := scalar(t, f.d.conn, "SELECT group_concat(sql) FROM sqlite_schema")
	for _, q := range []string{
		"CREATE TABLE p1(x REFERENCES _state_meta(sequence))",
		"ALTER TABLE pin ADD COLUMN y REFERENCES _state_receipts(input_hash)",
		"CREATE TABLE p2(x BLOB REFERENCES _state_receipts(operation_id) ON DELETE CASCADE)",
		"CREATE TABLE p3(x, FOREIGN KEY (x) REFERENCES _STATE_META)",
		"CREATE TABLE p4(x REFERENCES _state_none(a))",
	} {
		f.n++
		if _, _, err := f.d.Mutate(commit(f.n), func(tx *Tx) ([]byte, error) {
			_, err := tx.Exec(bg, q)
			return nil, err
		}); !errors.Is(err, ErrReserved) {
			t.Errorf("%s: %v", q, err)
		}
		f.mutate(t, "INSERT INTO pin(a) VALUES (1)") // the entity stays writable
	}
	if got := scalar(t, f.d.conn, "SELECT group_concat(sql) FROM sqlite_schema"); got != schema {
		t.Fatalf("schema %s, want %s", got, schema)
	}
	f.mutate(t, "CREATE TABLE parent(id INTEGER PRIMARY KEY); CREATE TABLE child(p REFERENCES parent(id))")
	f.publish(t, true)
	f.restore(t)

	// Activation refuses an image holding such a key (an import of an image
	// the library would not have published).
	if err := f.d.inTx(func() error {
		return f.d.exec("UPDATE _state_meta SET sequence = sequence + 1; CREATE TABLE p5(x REFERENCES _state_meta(sequence))")
	}); err != nil {
		t.Fatal(err)
	}
	f.publish(t, false)
	if _, err := Restore(bg, plain, filepath.Join(t.TempDir(), "r"), head.MaxPages, &f.head, readers(f.objs)); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("restore with a key to a reserved table: %v", err)
	}
}

// TestExtendReplacesBuiltin: Extend must not replace what a fresh driver
// has. A replaced nocase or binary collation corrupts index lookups and
// fails activation's integrity_check, a replaced upper() breaks activation
// of images using it in a CHECK or an index, a Go module named fts5 runs
// caller code at activation, and replaced like() or lower() would skew the
// engine's own introspection. Collations and connection hooks are refused.
func TestExtendReplacesBuiltin(t *testing.T) {
	fn := func(name string, n int32) func(*sqlite.Driver) error {
		return func(d *sqlite.Driver) error {
			return d.RegisterScalarFunction(name, n, func(*sqlite.FunctionContext, []driver.Value) (driver.Value, error) { return 1, nil })
		}
	}
	coll := func(name string) func(*sqlite.Driver) error {
		return func(d *sqlite.Driver) error { return d.RegisterCollationUtf8(name, strings.Compare) }
	}
	for name, extend := range map[string]func(*sqlite.Driver) error{
		"upper": fn("upper", 1), "UPPER/2": fn("UPPER", 2), "like": fn("like", 2), "like/3": fn("like", 3),
		"lower": fn("lower", 1), "json_extract": fn("json_extract", 2), "fts5 function": fn("fts5", 1),
		"nocase": coll("nocase"), "binary": coll("BINARY"), "rtrim": coll("rtrim"),
		"fts5":              func(d *sqlite.Driver) error { return d.RegisterModule("fts5", series{}) },
		"RTREE":             func(d *sqlite.Driver) error { return d.RegisterModule("RTREE", series{}) },
		"json_each":         func(d *sqlite.Driver) error { return d.RegisterModule("json_each", series{}) },
		"pragma_table_info": func(d *sqlite.Driver) error { return d.RegisterModule("pragma_table_info", series{}) },
		"_state_m":          func(d *sqlite.Driver) error { return d.RegisterModule("_state_m", series{}) },
		"hook": func(d *sqlite.Driver) error {
			d.RegisterConnectionHook(func(sqlite.ExecQuerierContext, string) error { return nil })
			return nil
		},
	} {
		if _, err := NewDriver(extend); err == nil {
			t.Errorf("%s: Extend accepted", name)
		} else {
			t.Logf("%s: %v", name, err)
		}
	}
}

// brokenConnect is series whose Connect fails and counts its calls.
type brokenConnect struct{ series }

var connects int

func (brokenConnect) Connect(vtab.Context, []string) (vtab.Table, error) {
	connects++
	return nil, errors.New("connect refused")
}

// TestActivationRunsNoModuleCode: integrity_check connects every virtual
// table whose module is registered, so activation runs it on a driver
// without the Node's extensions: a module whose Connect fails (or would run
// arbitrary caller code) must not make activation fail.
func TestActivationRunsNoModuleCode(t *testing.T) {
	ext, err := NewDriver(func(d *sqlite.Driver) error { return d.RegisterModule("broken", brokenConnect{}) })
	if err != nil {
		t.Fatal(err)
	}
	f := newFixture(t, 4000)
	f.d.Discard()
	ns, inc := head.NewID(), head.NewID()
	if f.d, err = Create(bg, ext, filepath.Join(t.TempDir(), "db"), 4000, ns, "key", inc); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { f.d.Discard() })
	f.head = head.Head{Namespace: ns, Key: "key", Incarnation: inc, Schema: 1}
	f.mutate(t, "CREATE TABLE app(a); CREATE VIRTUAL TABLE b USING broken(3)")
	f.publish(t, true)
	connects = 0
	r, err := Restore(bg, ext, filepath.Join(t.TempDir(), "r"), head.MaxPages, &f.head, readers(f.objs))
	if err != nil {
		t.Fatalf("activation with a failing module: %v", err)
	}
	defer r.Discard()
	if connects != 0 {
		t.Fatalf("activation connected the module %d times", connects)
	}
	if _, _, err := r.Mutate(commit(99), func(tx *Tx) ([]byte, error) {
		_, err := tx.Exec(bg, "INSERT INTO app VALUES (1)")
		return nil, err
	}); err != nil || connects != 0 {
		t.Fatalf("mutation: %v, %d connects", err, connects)
	}
	if _, err := r.Read(func(v *View) ([]byte, error) { return nil, v.Query(bg, "SELECT * FROM b", nil, nil) }); err == nil ||
		!strings.Contains(err.Error(), "connect refused") {
		t.Fatalf("query of the module: %v", err)
	}
}
