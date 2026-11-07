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
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"syscall"

	"github.com/OrdalieTech/big-garden/internal/head"
	"github.com/superfly/ltx"
	"modernc.org/sqlite"
	sqlite3 "modernc.org/sqlite/lib"
)

// ErrCorrupt marks an image, object or file that fails validation.
var ErrCorrupt = errors.New("engine: corrupt")

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
	path string
	db   *sql.DB
	conn *sql.Conn
	main *os.File // read-only; holds the published image between checkpoints
	pub  Image    // image of the main file (the last checkpointed, published state)
}

const schemaSQL = `
CREATE TABLE _state_meta(format INTEGER NOT NULL, namespace BLOB NOT NULL, key TEXT NOT NULL,
  incarnation BLOB NOT NULL, schema INTEGER NOT NULL, sequence INTEGER NOT NULL,
  expiry_floor_ms INTEGER NOT NULL) STRICT;
CREATE TABLE _state_receipts(operation_id BLOB PRIMARY KEY, input_hash BLOB NOT NULL,
  result BLOB NOT NULL, sequence INTEGER NOT NULL) STRICT, WITHOUT ROWID;`

// Create makes a new database for genesis at path, which must not exist, with
// the reserved tables and a meta row at sequence 0 (PROTOCOL §5 step 4). The
// first Mutate then produces sequence 1.
func Create(ctx context.Context, path string, maxPages uint32, ns head.ID, key string, inc head.ID) (*DB, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_EXCL|syscall.O_NOFOLLOW, 0o600)
	if err != nil {
		return nil, err
	}
	f.Close()
	d, err := open(ctx, path, maxPages)
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
func open(ctx context.Context, path string, maxPages uint32) (*DB, error) {
	if !filepath.IsAbs(path) || maxPages == 0 || maxPages > head.MaxPages {
		return nil, fmt.Errorf("engine: invalid path or page limit")
	}
	for _, s := range []string{"-wal", "-shm"} { // never attach residue
		if err := os.Remove(path + s); err != nil && !errors.Is(err, os.ErrNotExist) {
			return nil, err
		}
	}
	dsn := (&url.URL{Scheme: "file", OmitHost: true, Path: path, RawQuery: "mode=rw&_defensive=1"}).String()
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	d := &DB{path: path, db: db}
	if d.conn, err = db.Conn(ctx); err == nil {
		err = d.setup(maxPages)
	}
	if err == nil {
		d.main, err = os.OpenFile(path, os.O_RDONLY|syscall.O_NOFOLLOW, 0)
	}
	if err != nil {
		return nil, errors.Join(err, d.close())
	}
	return d, nil
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
		{"PRAGMA cache_size=-1024", "PRAGMA cache_size", "-1024"},
		{"PRAGMA busy_timeout=1000", "PRAGMA busy_timeout", "1000"},
		{fmt.Sprintf("PRAGMA max_page_count=%d", maxPages), "PRAGMA max_page_count", fmt.Sprint(maxPages)},
	}
	for _, s := range settings {
		var got string
		if err := d.exec(s.set); err != nil {
			return err
		}
		if err := d.conn.QueryRowContext(context.Background(), s.get).Scan(&got); err != nil {
			return err
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

// inTx runs fn in a library transaction (BEGIN IMMEDIATE ... COMMIT).
func (d *DB) inTx(fn func() error) error {
	if err := d.exec("BEGIN IMMEDIATE"); err != nil {
		return err
	}
	if err := fn(); err != nil {
		return errors.Join(err, d.exec("ROLLBACK"))
	}
	if err := d.exec("COMMIT"); err != nil {
		return errors.Join(err, d.exec("ROLLBACK"))
	}
	return nil
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
	if err := d.exec("BEGIN IMMEDIATE"); err != nil {
		return nil, 0, err
	}
	committed := false
	defer func() { // also runs on a callback panic
		if !committed {
			err = withCleanup(err, d.setMode(library), d.exec("ROLLBACK"))
		}
	}()
	m, err := d.Meta()
	if err != nil {
		return nil, 0, err
	}
	seq, floor := m.Sequence+1, max(m.FloorMs, c.FloorMs)
	if seq > head.MaxInt || floor >= head.MaxInt || c.Schema == 0 {
		return nil, 0, errors.New("engine: sequence, floor or schema out of range")
	}
	var below [8]byte
	binary.BigEndian.PutUint64(below[:], floor+1)
	if err := d.exec(`DELETE FROM _state_receipts WHERE operation_id IN (SELECT operation_id
		FROM _state_receipts WHERE operation_id < ? ORDER BY operation_id LIMIT ?)`, below[:], pruneBatch); err != nil {
		return nil, 0, err
	}
	if err := d.exec("INSERT INTO _state_receipts VALUES (?, ?, x'', ?)", c.ID[:], c.Bound[:], seq); err != nil {
		return nil, 0, err
	}
	if err := d.exec("UPDATE _state_meta SET schema = ?, sequence = ?, expiry_floor_ms = ?", c.Schema, seq, floor); err != nil {
		return nil, 0, err
	}
	if err := d.setMode(mutation); err != nil {
		return nil, 0, err
	}
	tx := &Tx{scope{conn: d.conn}}
	if result, err = runScoped(&tx.scope, func() ([]byte, error) { return fn(tx) }); err != nil {
		return nil, 0, err
	}
	if err := d.setMode(library); err != nil {
		return nil, 0, err
	}
	res, err := d.conn.ExecContext(context.Background(), "UPDATE _state_receipts SET result = ? WHERE operation_id = ?",
		append([]byte{}, result...), c.ID[:])
	if err != nil {
		return nil, 0, err
	}
	if n, err := res.RowsAffected(); err != nil || n != 1 {
		return nil, 0, fmt.Errorf("engine: transaction rolled back during the callback (%d rows, %v)", n, err)
	}
	if err := d.exec("COMMIT"); err != nil {
		return nil, 0, err
	}
	committed = true
	return result, seq, nil
}

// Read runs fn in a read-only scope inside a library read transaction.
func (d *DB) Read(fn func(*View) ([]byte, error)) (result []byte, err error) {
	if err := d.exec("BEGIN"); err != nil {
		return nil, err
	}
	defer func() { err = withCleanup(err, d.setMode(library), d.exec("ROLLBACK")) }()
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

// Checkpoint installs the published object o into the main file (PROTOCOL
// §2, after committed): TRUNCATE checkpoint, empty WAL, and a main file whose
// sequence and size match o. On error the caller discards the cache.
func (d *DB) Checkpoint(o Object) error {
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
	if m.Sequence != o.Ref.MaxSequence || st.Size() != int64(o.PageCount)*pageSize {
		return fmt.Errorf("engine: checkpointed image (sequence %d, %d bytes) is not object %d..%d with %d pages",
			m.Sequence, st.Size(), o.Ref.MinSequence, o.Ref.MaxSequence, o.PageCount)
	}
	d.pub = Image{o.Ref.MaxSequence, o.PageCount, o.Ref.PostChecksum}
	return nil
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
