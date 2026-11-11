// Package engine owns one entity's SQLite cache file: the pinned connection,
// the authorizer scopes, capture of committed pages as LTX objects, and
// reconstruction of an image from LTX objects (PLAN §5.3–5.4, §6.1–6.2, §9.3).
//
// A DB is used by one goroutine at a time, under the entity mutex. Every
// method is a straight-line function; the only state a DB keeps besides its
// handles is the published image of its main file.
package engine

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"strings"

	"github.com/OrdalieTech/big-garden/internal/head"
	"github.com/superfly/ltx"
	"modernc.org/sqlite"
	sqlite3 "modernc.org/sqlite/lib"
)

// ErrCorrupt marks an image, object or file that fails validation.
var ErrCorrupt = errors.New("engine: corrupt")

// ErrReserved is returned by Mutate when application SQL gave a table a
// reserved name by renaming it or declared a foreign key to a reserved
// table; the transaction is rolled back.
var ErrReserved = errors.New("engine: application SQL used a reserved table name")

const (
	pageSize   = head.PageSize
	valueBytes = 1 << 20 // SQL value, row and statement text limit (PLAN §11.2)
	pruneBatch = 1000    // receipts deleted per mutation at most (PLAN §8.3)
)

// Image is the logical state of a database file: its last sequence, page
// count and LTX rolling checksum.
type Image struct {
	Sequence  uint64
	PageCount uint32
	Checksum  head.Checksum
}

// Meta is the library-owned row of _state_meta (PROTOCOL §1).
type Meta struct {
	Namespace   head.ID
	Key         string
	Incarnation head.ID
	Schema      uint32
	Sequence    uint64
	FloorMs     uint64
}

// Receipt is the stored outcome of one operation ID.
type Receipt struct {
	InputHash head.Digest
	Result    []byte
	Sequence  uint64
}

// Commit is the library metadata a mutation writes with its application
// changes: the receipt identity, the schema after the mutation, and the
// proposed receipt floor (the stored floor never moves backward).
type Commit struct {
	ID      head.OpID
	Bound   head.Digest
	Schema  uint32
	FloorMs uint64
}

// DB is one exclusively owned SQLite cache file.
type DB struct {
	path    string
	db      *sql.DB
	conn    *sql.Conn
	modules map[string]bool // virtual table modules application SQL may use
	main    *os.File        // read-only; holds the published image between checkpoints
	pub     Image           // image of the main file (the last checkpointed, published state)
}

// The library's tables (PROTOCOL §1), compared verbatim with sqlite_schema at
// activation. Receipts are a rowid table keyed by sequence: a table b-tree
// leaf holds up to 4,061 bytes of a row, where a WITHOUT ROWID (index) leaf
// spilled every result above ~940 bytes to a 4 KiB overflow page.
var reservedSQL = [2]string{
	`CREATE TABLE _state_meta(format INTEGER NOT NULL, namespace BLOB NOT NULL, key TEXT NOT NULL,
  incarnation BLOB NOT NULL, schema INTEGER NOT NULL, sequence INTEGER NOT NULL,
  expiry_floor_ms INTEGER NOT NULL) STRICT`,
	`CREATE TABLE _state_receipts(sequence INTEGER PRIMARY KEY, operation_id BLOB NOT NULL UNIQUE,
  input_hash BLOB NOT NULL, result BLOB NOT NULL) STRICT`,
}

var schemaSQL = reservedSQL[0] + ";\n" + reservedSQL[1] + ";"

// Driver opens the SQLite connections of one Node through a private modernc
// driver, so the functions and modules registered on it reach no other
// connection in the process (PLAN §6.1).
type Driver struct {
	sqlite  *sqlite.Driver
	modules map[string]bool
}

// NewDriver returns a Driver on which extend (if not nil) has registered
// functions and virtual table modules. The modules it added are allowed to
// application SQL besides the built-in ones. It must not replace a function,
// collation or module a fresh driver has (an index, a CHECK constraint or
// the library's own SQL may use it), nor register a collation (a schema can
// use it whatever trusted_schema says) or a connection hook (caller code on
// every connection, activation included).
func NewDriver(extend func(*sqlite.Driver) error) (*Driver, error) {
	// The compiled-in modules keep their state in ordinary shadow tables (D19).
	d := &Driver{sqlite: &sqlite.Driver{}, modules: map[string]bool{"fts5": true, "fts5vocab": true, "rtree": true,
		"rtree_i32": true, "geopoly": true}}
	if extend == nil {
		return d, nil
	}
	if err := extend(d.sqlite); err != nil {
		return nil, err
	}
	base, err := listed(&sqlite.Driver{})
	if err != nil {
		return nil, err
	}
	// modernc keeps a driver's registrations in unexported fields and
	// replaces a built-in silently: reading them is the only way to see it.
	v := reflect.ValueOf(d.sqlite).Elem()
	funcs, colls, hooks, mods := v.FieldByName("udfs"), v.FieldByName("collations"), v.FieldByName("connectionHooks"), v.FieldByName("modules")
	if !funcs.IsValid() || !colls.IsValid() || !hooks.IsValid() || !mods.IsValid() {
		return nil, errors.New("engine: unsupported modernc.org/sqlite driver layout")
	}
	if colls.Len() != 0 || hooks.Len() != 0 {
		return nil, errors.New("engine: collations and connection hooks are not supported")
	}
	for _, k := range funcs.MapKeys() {
		if n := strings.ToLower(k.String()); base["function "+n] {
			return nil, fmt.Errorf("engine: registered function %s replaces a built-in", n)
		}
	}
	for _, k := range mods.MapKeys() {
		n := strings.ToLower(k.String())
		// SQLite registers json_* and pragma_* modules when first used.
		if base["module "+n] || reserved(n) || strings.HasPrefix(n, "json") || strings.HasPrefix(n, "pragma_") {
			return nil, fmt.Errorf("engine: registered module %s replaces a built-in or is reserved", n)
		}
		d.modules[n] = true
	}
	return d, nil
}

// listed returns the functions and modules of a connection opened by drv,
// lowercased in Go.
func listed(drv *sqlite.Driver) (map[string]bool, error) {
	db := sql.OpenDB(connector{drv, ":memory:"})
	defer db.Close()
	rows, err := db.Query(`SELECT 'function ', name FROM pragma_function_list
		UNION ALL SELECT 'module ', name FROM pragma_module_list`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	names := map[string]bool{}
	for rows.Next() {
		var kind, n string
		if err := rows.Scan(&kind, &n); err != nil {
			return nil, err
		}
		names[kind+strings.ToLower(n)] = true
	}
	return names, rows.Err()
}

// connector opens dsn through a private driver (database/sql registers it
// nowhere).
type connector struct {
	d   *sqlite.Driver
	dsn string
}

func (c connector) Connect(context.Context) (driver.Conn, error) { return c.d.Open(c.dsn) }
func (c connector) Driver() driver.Driver                        { return c.d }

// Create makes a new database for genesis at path, which must not exist, with
// the reserved tables and a meta row at sequence 0 (PROTOCOL §5 step 4). The
// first Mutate then produces sequence 1.
func Create(ctx context.Context, drv *Driver, path string, maxPages uint32, ns head.ID, key string, inc head.ID) (*DB, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_EXCL|NoFollow, 0o600)
	if err != nil {
		return nil, err
	}
	f.Close()
	d, err := open(ctx, drv, path, maxPages)
	if err != nil {
		removeFiles(path)
		return nil, err
	}
	err = d.inTx(func() error {
		if err := d.exec(schemaSQL); err != nil {
			return err
		}
		return d.exec("INSERT INTO _state_meta VALUES (1, ?, ?, ?, 0, 0, 0)", ns[:], key, inc[:])
	})
	if err == nil {
		err = d.checkpoint()
	}
	if err == nil {
		d.pub, err = fileImage(d.main, 0)
	}
	if err != nil {
		return nil, errors.Join(err, d.Discard())
	}
	return d, nil
}

// open opens an existing file read-write (never creating it), applies and
// verifies the engine settings, and installs the library-mode authorizer.
func open(ctx context.Context, drv *Driver, path string, maxPages uint32) (*DB, error) {
	if !filepath.IsAbs(path) || maxPages == 0 || maxPages > head.MaxPages {
		return nil, fmt.Errorf("engine: invalid path or page limit")
	}
	for _, s := range []string{"-wal", "-shm"} { // never attach residue
		if err := os.Remove(path + s); err != nil && !errors.Is(err, os.ErrNotExist) {
			return nil, err
		}
	}
	db := sql.OpenDB(connector{drv.sqlite, fileDSN(path, "rw")})
	db.SetMaxOpenConns(1)
	d := &DB{path: path, db: db, modules: drv.modules}
	var err error
	if d.conn, err = db.Conn(ctx); err == nil {
		err = d.setup(maxPages)
	}
	if err == nil {
		d.main, err = os.OpenFile(path, os.O_RDONLY|NoFollow, 0)
	}
	if err != nil {
		return nil, errors.Join(err, d.close())
	}
	return d, nil
}

func fileDSN(path, mode string) string {
	return (&url.URL{Scheme: "file", OmitHost: true, Path: path, RawQuery: "mode=" + mode + "&_defensive=1"}).String()
}

func (d *DB) setup(maxPages uint32) error {
	settings := []struct{ set, get, want string }{
		{"PRAGMA page_size=4096", "PRAGMA page_size", "4096"},
		{"PRAGMA auto_vacuum=FULL", "PRAGMA auto_vacuum", "1"},
		{"PRAGMA journal_mode=WAL", "PRAGMA journal_mode", "wal"},
		{"PRAGMA wal_autocheckpoint=0", "PRAGMA wal_autocheckpoint", "0"},
		{"PRAGMA synchronous=OFF", "PRAGMA synchronous", "0"}, // D1
		{"PRAGMA trusted_schema=OFF", "PRAGMA trusted_schema", "0"},
		{"PRAGMA foreign_keys=ON", "PRAGMA foreign_keys", "1"},
		{"PRAGMA secure_delete=FAST", "PRAGMA secure_delete", "2"}, // deleted bytes leave the image at once (with auto_vacuum=FULL)
		{"PRAGMA cache_size=-1024", "PRAGMA cache_size", "-1024"},
		{"PRAGMA busy_timeout=1000", "PRAGMA busy_timeout", "1000"},
		{fmt.Sprintf("PRAGMA max_page_count=%d", maxPages), "PRAGMA max_page_count", fmt.Sprint(maxPages)},
	}
	for _, s := range settings {
		var got string
		get := func() error { return d.conn.QueryRowContext(context.Background(), s.get).Scan(&got) }
		if err := get(); err != nil {
			return err
		}
		// set only what differs: even a no-op auto_vacuum=FULL commits a
		// write to page 1, and an opened image must stay its head's (M9)
		if got != s.want {
			if err := d.exec(s.set); err != nil {
				return err
			}
			if err := get(); err != nil {
				return err
			}
		}
		if got != s.want {
			return fmt.Errorf("engine: %s = %q, want %q", s.get, got, s.want)
		}
	}
	for id, v := range map[int]int{sqlite3.SQLITE_LIMIT_LENGTH: valueBytes,
		sqlite3.SQLITE_LIMIT_SQL_LENGTH: valueBytes, sqlite3.SQLITE_LIMIT_ATTACHED: 0} {
		sqlite.Limit(d.conn, id, v)
		if got, err := sqlite.Limit(d.conn, id, -1); err != nil || got != v {
			return fmt.Errorf("engine: sqlite limit %d = %d, want %d: %v", id, got, v, err)
		}
	}
	return d.setMode(library)
}

// Published returns the image of the main file: the last checkpointed state.
func (d *DB) Published() Image { return d.pub }

// exec runs library SQL. Library statements are short and bounded; they are
// not cancelled by a caller's context, so a rollback can always run.
func (d *DB) exec(q string, args ...any) error {
	_, err := d.conn.ExecContext(context.Background(), q, args...)
	return err
}

// inTx runs fn in a library transaction (BEGIN IMMEDIATE ... COMMIT). On an
// error or a panic of fn it restores library mode and rolls back.
func (d *DB) inTx(fn func() error) (err error) {
	if err := d.exec("BEGIN IMMEDIATE"); err != nil {
		return err
	}
	committed := false
	defer func() { // also runs on a callback panic
		if !committed {
			err = withCleanup(err, d.setMode(library), d.rollback())
		}
	}()
	if err = fn(); err == nil {
		err = d.exec("COMMIT")
	}
	committed = err == nil
	return err
}

// rollback ends the library transaction. SQLite may already have rolled it
// back by itself (SQLITE_FULL, interrupt), which is the wanted state too:
// a failed ROLLBACK is accepted when an empty transaction can then start.
func (d *DB) rollback() error {
	if d.exec("ROLLBACK") == nil {
		return nil
	}
	if err := d.exec("BEGIN"); err != nil {
		return err
	}
	return d.exec("ROLLBACK")
}

// Meta reads the library metadata row.
func (d *DB) Meta() (Meta, error) {
	var m Meta
	var format int
	var ns, inc []byte
	rows, err := d.conn.QueryContext(context.Background(),
		"SELECT format, namespace, key, incarnation, schema, sequence, expiry_floor_ms FROM _state_meta")
	if err != nil {
		return m, err
	}
	defer rows.Close()
	n := 0
	for ; rows.Next(); n++ {
		if err := rows.Scan(&format, &ns, &m.Key, &inc, &m.Schema, &m.Sequence, &m.FloorMs); err != nil {
			return m, err
		}
	}
	if err := rows.Err(); err != nil {
		return m, err
	}
	if n != 1 || format != head.Format || len(ns) != 16 || len(inc) != 16 || m.Sequence > head.MaxInt || m.FloorMs > head.MaxInt {
		return m, fmt.Errorf("%w: invalid _state_meta", ErrCorrupt)
	}
	copy(m.Namespace[:], ns)
	copy(m.Incarnation[:], inc)
	return m, nil
}

// Receipt returns the stored receipt of id, if any.
func (d *DB) Receipt(id head.OpID) (Receipt, bool, error) {
	var r Receipt
	var bound []byte
	err := d.conn.QueryRowContext(context.Background(),
		"SELECT input_hash, result, sequence FROM _state_receipts WHERE operation_id = ?", id[:]).Scan(&bound, &r.Result, &r.Sequence)
	if errors.Is(err, sql.ErrNoRows) {
		return r, false, nil
	}
	if err != nil {
		return r, false, err
	}
	if len(bound) != 32 {
		return r, false, fmt.Errorf("%w: invalid receipt", ErrCorrupt)
	}
	copy(r.InputHash[:], bound)
	return r, true, nil
}

// Mutate runs fn as one local SQLite transaction together with the receipt
// and metadata of c, and commits it locally (PROTOCOL §6 steps 4–5). It
// returns fn's result and the new sequence. An error from fn is returned
// unchanged after rollback. Nothing is published: the caller captures,
// publishes, then checkpoints or discards.
//
// The receipt row is written before fn runs and its result filled in after:
// if SQLite rolled the transaction back on its own (SQLITE_FULL, interrupt),
// the final update finds no row and the mutation fails instead of committing
// library metadata without the application's changes.
func (d *DB) Mutate(c Commit, fn func(*Tx) ([]byte, error)) (result []byte, seq uint64, err error) {
	err = d.inTx(func() error {
		m, err := d.Meta()
		if err != nil {
			return err
		}
		floor := max(m.FloorMs, c.FloorMs)
		if seq = m.Sequence + 1; seq > head.MaxInt || floor >= head.MaxInt || c.Schema == 0 {
			return errors.New("engine: sequence, floor or schema out of range")
		}
		var below [8]byte
		binary.BigEndian.PutUint64(below[:], floor+1)
		if err := d.exec(`DELETE FROM _state_receipts WHERE operation_id IN (SELECT operation_id
		FROM _state_receipts WHERE operation_id < ? ORDER BY operation_id LIMIT ?)`, below[:], pruneBatch); err != nil {
			return err
		}
		if err := d.exec("INSERT INTO _state_receipts VALUES (?, ?, ?, x'')", seq, c.ID[:], c.Bound[:]); err != nil {
			return err
		}
		if err := d.exec("UPDATE _state_meta SET schema = ?, sequence = ?, expiry_floor_ms = ?", c.Schema, seq, floor); err != nil {
			return err
		}
		if err := d.setMode(mutation); err != nil {
			return err
		}
		tx := &Tx{scope{conn: d.conn}}
		if result, err = runScoped(&tx.scope, func() ([]byte, error) { return fn(tx) }); err != nil {
			return err
		}
		if err := d.setMode(library); err != nil {
			return err
		}
		if err := d.schemaOwned(); err != nil {
			return err
		}
		res, err := d.conn.ExecContext(context.Background(), "UPDATE _state_receipts SET result = ? WHERE operation_id = ?",
			append([]byte{}, result...), c.ID[:])
		if err != nil {
			return err
		}
		if n, err := res.RowsAffected(); err != nil || n != 1 {
			return fmt.Errorf("engine: transaction rolled back during the callback (%d rows, %v)", n, err)
		}
		return nil
	})
	if err != nil {
		return nil, 0, err
	}
	return result, seq, nil
}

// schemaOwned returns ErrReserved if application SQL renamed an object into
// a reserved name (ALTER TABLE ... RENAME TO: the authorizer never sees the
// new name) or declared a foreign key to a reserved table (the authorizer
// never sees REFERENCES targets), which would make the library's own
// statements on that table fail. Names are compared in Go.
func (d *DB) schemaOwned() error {
	rows, err := d.conn.QueryContext(context.Background(), `SELECT 0, name FROM sqlite_schema UNION ALL
		SELECT 1, f."table" FROM sqlite_schema s, pragma_foreign_key_list(s.name) f WHERE s.type = 'table'`)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var fk bool
		var n string
		if err := rows.Scan(&fk, &n); err != nil {
			return err
		}
		if reserved(n) && (fk || n != "_state_meta" && n != "_state_receipts" && n != "sqlite_autoindex__state_receipts_1") {
			return ErrReserved
		}
	}
	return rows.Err()
}

// Read runs fn in a read-only scope inside a library read transaction, with
// query_only=ON besides the read-mode authorizer.
func (d *DB) Read(fn func(*View) ([]byte, error)) (result []byte, err error) {
	if err := d.exec("BEGIN"); err != nil {
		return nil, err
	}
	defer func() { err = withCleanup(err, d.setMode(library), d.exec("PRAGMA query_only=0"), d.rollback()) }()
	if err := d.exec("PRAGMA query_only=1"); err != nil {
		return nil, err
	}
	if err := d.setMode(read); err != nil {
		return nil, err
	}
	v := &View{scope{conn: d.conn}}
	return runScoped(&v.scope, func() ([]byte, error) { return fn(v) })
}

// checkpoint runs wal_checkpoint(TRUNCATE) and verifies an empty WAL.
func (d *DB) checkpoint() error {
	var busy, log, done int
	if err := d.conn.QueryRowContext(context.Background(), "PRAGMA wal_checkpoint(TRUNCATE)").Scan(&busy, &log, &done); err != nil {
		return err
	}
	if busy != 0 || log != done {
		return fmt.Errorf("engine: checkpoint incomplete (busy=%d log=%d done=%d)", busy, log, done)
	}
	if st, err := os.Stat(d.path + "-wal"); err != nil || st.Size() != 0 {
		return fmt.Errorf("engine: WAL not empty after checkpoint: %v", err)
	}
	return nil
}

// Checkpoint installs the published image img into the main file (PROTOCOL
// §2, after committed): TRUNCATE checkpoint, empty WAL, and a main file whose
// sequence and size match img. On error the caller discards the cache.
func (d *DB) Checkpoint(img Image) error {
	if err := d.checkpoint(); err != nil {
		return err
	}
	m, err := d.Meta()
	if err != nil {
		return err
	}
	st, err := d.main.Stat()
	if err != nil {
		return err
	}
	if m.Sequence != img.Sequence || st.Size() != int64(img.PageCount)*pageSize {
		return fmt.Errorf("engine: checkpointed image (sequence %d, %d bytes) is not %+v", m.Sequence, st.Size(), img)
	}
	d.pub = img
	return nil
}

// Park checkpoints the published image into the main file, closes the
// database, moves the file to dst (D21: the warm tier) and returns its size.
// On error the caller discards.
func (d *DB) Park(dst string) (int64, error) {
	if err := errors.Join(d.Checkpoint(d.pub), d.close()); err != nil {
		return 0, err
	}
	return int64(d.pub.PageCount) * pageSize, errors.Join(os.Rename(d.path, dst), removeFiles(d.path))
}

// Bytes is the size of the database and -wal files (0 for a missing file).
func (d *DB) Bytes() int64 {
	var n int64
	for _, s := range []string{"", "-wal", "-shm"} { // all three sit in the cache directory
		if st, err := os.Stat(d.path + s); err == nil {
			n += st.Size()
		}
	}
	return n
}

// Discard closes the connection and deletes the database, -wal and -shm
// files (DECISIONS D10). The DB is unusable afterwards.
func (d *DB) Discard() error {
	return errors.Join(d.close(), removeFiles(d.path))
}

func (d *DB) close() error {
	var errs []error
	if d.conn != nil {
		errs = append(errs, d.conn.Close())
	}
	errs = append(errs, d.db.Close())
	if d.main != nil {
		errs = append(errs, d.main.Close())
	}
	return errors.Join(errs...)
}

// Full reports whether err is SQLite's SQLITE_FULL: the database reached
// its page limit (a capacity outcome, nothing committed).
func Full(err error) bool {
	var se *sqlite.Error
	return errors.As(err, &se) && se.Code()&0xff == sqlite3.SQLITE_FULL
}

// withCleanup returns err itself when every cleanup succeeded, so a callback
// error passes through unchanged.
func withCleanup(err error, cleanup ...error) error {
	if errors.Join(cleanup...) == nil {
		return err
	}
	return errors.Join(append([]error{err}, cleanup...)...)
}

func removeFiles(path string) error {
	var errs []error
	for _, s := range []string{"", "-wal", "-shm"} {
		if err := os.Remove(path + s); err != nil && !errors.Is(err, os.ErrNotExist) {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// fileImage computes the image of a whole database file (activation and
// genesis only: a full scan).
func fileImage(f *os.File, seq uint64) (Image, error) {
	st, err := f.Stat()
	if err != nil {
		return Image{}, err
	}
	if st.Size()%pageSize != 0 || st.Size()/pageSize > head.MaxPages {
		return Image{}, fmt.Errorf("%w: file size %d", ErrCorrupt, st.Size())
	}
	sum, err := ltx.ChecksumReader(io.NewSectionReader(f, 0, st.Size()), pageSize)
	return Image{seq, uint32(st.Size() / pageSize), head.Checksum(sum)}, err
}
