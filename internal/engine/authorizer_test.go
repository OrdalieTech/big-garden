package engine

import (
	"database/sql"
	"os"
	"path/filepath"
	"testing"
)

// TestAuthorizerBypass tries to reach library state, transaction control,
// storage settings, other files and extensions from application SQL. Every
// attempt runs in its own mutation whose callback ignores the error and
// returns success, so a statement that slipped through would commit. After
// all of them the metadata row, the receipts and the published image must
// be exactly what the successful no-op mutations produced, and no file may
// have appeared next to the database.
func TestAuthorizerBypass(t *testing.T) {
	f := newFixture(t, 4000)
	f.mutate(t, `CREATE TABLE app(id INTEGER PRIMARY KEY, v TEXT);
		CREATE TABLE log(v TEXT)`)
	f.publish(t, true)
	outside := filepath.Join(f.dir, "outside.db")

	denied := []string{
		// transaction control, also hidden behind comments, case and other statements
		"COMMIT", "ROLLBACK", "BEGIN", "END", "SAVEPOINT s", "RELEASE s", "ROLLBACK TO s",
		"/* x */ COMMIT", "-- x\nCOMMIT", "commit transaction", "INSERT INTO app(v) VALUES ('m'); COMMIT",
		"SELECT 1; ROLLBACK; INSERT INTO app(v) VALUES ('after')",
		// other databases and files
		"ATTACH DATABASE '" + outside + "' AS x", "ATTACH ':memory:' AS x", "DETACH DATABASE main",
		"SELECT sqlite_attach('" + outside + "', 'x', NULL)", "VACUUM", "VACUUM INTO '" + outside + "'",
		"SELECT load_extension('/nonexistent/x.so')", "SELECT LOAD_EXTENSION('x')",
		// storage settings
		"PRAGMA journal_mode=DELETE", "PRAGMA main.journal_mode=OFF", "PRAGMA synchronous=FULL",
		"PRAGMA wal_autocheckpoint=1", "PRAGMA wal_checkpoint(TRUNCATE)", "PRAGMA writable_schema=ON",
		"PRAGMA locking_mode=EXCLUSIVE", "PRAGMA page_size=8192", "PRAGMA max_page_count=100000",
		"PRAGMA cache_size=-1000000", "PRAGMA trusted_schema=ON", "PRAGMA foreign_keys=OFF",
		"PRAGMA user_version=7", "PRAGMA schema_version=1", "PRAGMA query_only=1", "PRAGMA auto_vacuum=NONE",
		"PRAGMA temp_store=MEMORY", "PRAGMA mmap_size=1000000", "PRAGMA integrity_check", "PRAGMA table_info(_state_meta)",
		"PRAGMA journal_mode",
		// reserved objects, directly and through CTEs, views, triggers, indexes
		"SELECT * FROM _state_meta", "SELECT count(*) FROM _STATE_RECEIPTS", "SELECT * FROM main._state_meta",
		`SELECT * FROM "_state_meta"`, "UPDATE _state_meta SET sequence = 99", "DELETE FROM _state_receipts",
		"INSERT INTO _state_receipts VALUES (x'00', x'00', x'', 1)", "DROP TABLE _state_receipts",
		"ALTER TABLE _state_meta RENAME TO m", "ALTER TABLE _state_meta ADD COLUMN x", "CREATE TABLE _state_x(a)",
		"CREATE INDEX i ON _state_meta(sequence)", "CREATE INDEX _state_i ON app(v)", "CREATE VIEW _state_v AS SELECT 1",
		"CREATE TRIGGER tm AFTER UPDATE ON _state_meta BEGIN SELECT 1; END",
		"WITH x AS (SELECT * FROM _state_meta) SELECT * FROM x",
		"WITH RECURSIVE r(n) AS (SELECT sequence FROM _state_meta UNION ALL SELECT n FROM r) SELECT * FROM r LIMIT 1",
		"SELECT (SELECT sequence FROM _state_meta)", "INSERT INTO log SELECT key FROM _state_meta",
		"ANALYZE", "ANALYZE app", "ALTER TABLE app RENAME TO _state_meta",
		// raw pages
		"SELECT data FROM sqlite_dbpage WHERE pgno = 1", "SELECT * FROM sqlite_dbpage('main')",
		"UPDATE sqlite_dbpage SET data = zeroblob(4096) WHERE pgno = 1", "INSERT INTO sqlite_dbpage VALUES (1, zeroblob(4096))",
		"DELETE FROM sqlite_dbpage", "SELECT * FROM dbstat",
		// connection-wide objects
		"CREATE TEMP TABLE t(a)", "CREATE TEMP VIEW v AS SELECT 1", "CREATE TEMP TRIGGER tt AFTER INSERT ON app BEGIN SELECT 1; END",
		"CREATE TABLE temp.t(a)", "CREATE VIRTUAL TABLE temp.f USING fts5(x)",
		// virtual tables of modules outside the allowlist or with reserved names (D19)
		"CREATE VIRTUAL TABLE f USING dbstat", "CREATE VIRTUAL TABLE f USING sqlite_dbpage",
		"CREATE VIRTUAL TABLE f USING nosuchmodule(x)", "CREATE VIRTUAL TABLE _state_f USING fts5(x)",
		"CREATE VIRTUAL TABLE _state_r USING rtree(id, a, b)",
		// schema table writes
		"INSERT INTO sqlite_master VALUES ('table', 'x', 'x', 0, 'CREATE TABLE x(a)')",
		"UPDATE sqlite_master SET sql = 'x'", "DELETE FROM sqlite_master",
	}
	ok := 0
	for _, q := range denied {
		f.n++
		var stmtErr error
		_, _, err := f.d.Mutate(commit(f.n), func(tx *Tx) ([]byte, error) {
			_, stmtErr = tx.Exec(bg, q)
			if stmtErr == nil {
				stmtErr = tx.Query(bg, q, nil, func(r *sql.Rows) error { return nil })
			}
			return nil, nil
		})
		if stmtErr == nil {
			t.Errorf("allowed: %q", q)
		}
		if err == nil {
			ok++
		}
	}
	// Triggers and views that reach reserved tables are refused when created
	// or when they would run. The triggers are dropped again, the views kept.
	for _, p := range [][2]string{
		{"CREATE TRIGGER tr AFTER INSERT ON app BEGIN UPDATE _state_meta SET sequence = 99; END", "INSERT INTO app(v) VALUES ('t')"},
		{"CREATE TRIGGER tr2 AFTER INSERT ON app BEGIN INSERT INTO log SELECT data FROM sqlite_dbpage; END", "INSERT INTO app(v) VALUES ('t')"},
		{"CREATE VIEW vw AS SELECT * FROM _state_receipts", "SELECT * FROM vw"},
		{"CREATE VIEW vw2 AS SELECT data FROM sqlite_dbpage", "SELECT * FROM vw2"},
	} {
		f.n++
		var createErr, useErr error
		_, _, err := f.d.Mutate(commit(f.n), func(tx *Tx) ([]byte, error) {
			_, createErr = tx.Exec(bg, p[0])
			_, useErr = tx.Exec(bg, p[1])
			_, err := tx.Exec(bg, "DROP TRIGGER IF EXISTS tr; DROP TRIGGER IF EXISTS tr2")
			return nil, err
		})
		if err != nil {
			t.Fatal(err)
		}
		ok++
		if createErr == nil && useErr == nil {
			t.Errorf("%q then %q allowed", p[0], p[1])
		}
		t.Logf("%.30s: create=%v use=%v", p[0], createErr, useErr)
	}

	// Read mode refuses every write, schema change and transaction control.
	for _, q := range []string{"INSERT INTO app(v) VALUES ('r')", "UPDATE app SET v = 'r'", "DELETE FROM app",
		"CREATE TABLE z(a)", "DROP TABLE app", "CREATE INDEX zi ON app(v)", "ALTER TABLE app ADD COLUMN z",
		"COMMIT", "ROLLBACK", "BEGIN", "INSERT INTO app(v) VALUES ('r') RETURNING id", "SELECT * FROM _state_meta",
		"PRAGMA user_version=3", "SELECT data FROM sqlite_dbpage", "SELECT * FROM vw", "SELECT * FROM vw2"} {
		if _, err := f.d.Read(func(v *View) ([]byte, error) { return nil, v.Query(bg, q, nil, nil) }); err == nil {
			t.Errorf("read mode allowed %q", q)
		}
	}
	// Ordinary application SQL still works in both modes. (A view that
	// cannot be used would also block ALTER TABLE ... RENAME.)
	f.mutate(t, `DROP VIEW vw; DROP VIEW vw2;
		INSERT INTO app(v) VALUES ('x');
		CREATE INDEX app_v ON app(v); CREATE VIEW av AS SELECT v FROM app;
		CREATE TRIGGER lg AFTER INSERT ON app BEGIN INSERT INTO log VALUES (new.v); END;
		INSERT INTO app(v) VALUES ('y') RETURNING id;
		ALTER TABLE app ADD COLUMN w INTEGER DEFAULT 1; DROP INDEX app_v; REINDEX;
		ALTER TABLE app ADD COLUMN z; ALTER TABLE app RENAME COLUMN z TO z2; ALTER TABLE app DROP COLUMN z2;
		ALTER TABLE log RENAME TO log2; ALTER TABLE log2 RENAME TO log`)
	if got, err := f.d.Read(func(v *View) ([]byte, error) {
		var s string
		err := v.Query(bg, `WITH RECURSIVE c(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM c WHERE n < 3)
			SELECT (SELECT count(*) FROM av WHERE v IN ('x', 'y')) || '|' || (SELECT count(*) FROM c) || '|' ||
			(SELECT group_concat(v) FROM log) || '|' || (SELECT count(*) FROM pragma_table_info('app')) || '|' ||
			(SELECT count(*) > 0 FROM sqlite_schema) || abs(-1) || (random() * 0)`,
			nil, func(r *sql.Rows) error { r.Next(); return r.Scan(&s) })
		return []byte(s), err
	}); err != nil || string(got) != "2|3|y|3|110" {
		t.Fatalf("read: %q %v", got, err)
	}

	// No statement reached the library state or another file.
	m, err := f.d.Meta()
	if err != nil || m.Sequence != uint64(2+ok) || m.Key != "key" {
		t.Fatalf("meta %+v (want sequence %d): %v", m, 2+ok, err)
	}
	if got := scalar(t, f.d.conn, "SELECT count(*) FROM _state_receipts"); mustInt(got) != 2+ok {
		t.Fatalf("%s receipts, want %d", got, 2+ok)
	}
	if got := scalar(t, f.d.conn, "SELECT count(*) FROM app WHERE v IN ('after', 'r')"); got != "0" {
		t.Fatalf("%s rows from statements after a denied one or from read mode", got)
	}
	entries, _ := os.ReadDir(f.dir)
	for _, e := range entries {
		if n := e.Name(); n != "db" && n != "db-wal" && n != "db-shm" {
			t.Errorf("unexpected file %s", n)
		}
	}
	f.publish(t, false)
	r := f.restore(t)
	if got := scalar(t, r.conn, "SELECT group_concat(v) FROM log"); got != "y" {
		t.Fatalf("restored log %q", got)
	}
}
