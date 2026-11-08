package engine

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"sync"

	"modernc.org/sqlite"
)

// ErrScope is returned when a Tx or View is used after its callback returned.
var ErrScope = errors.New("engine: SQL handle used outside its callback")

// mode selects the authorizer policy of the connection (PLAN §6.1).
type mode int

const (
	library  mode = iota // library-issued SQL: everything
	mutation             // application DML and DDL on application objects
	read                 // application reads only
)

// setMode installs the authorizer of m and, in application modes, a commit
// hook that turns any commit into a rollback, so application SQL can never
// commit on its own (for instance in autocommit after SQLite rolled back the
// library transaction). Re-registering the authorizer expires every prepared
// statement, so nothing prepared under one mode runs under another.
func (d *DB) setMode(m mode) error {
	return d.conn.Raw(func(dc any) error {
		c, ok := dc.(interface {
			sqlite.AuthorizerRegisterer
			sqlite.HookRegisterer
		})
		if !ok {
			return errors.New("engine: driver connection lacks authorizer or hooks")
		}
		if m == library {
			c.RegisterCommitHook(nil)
		} else {
			c.RegisterCommitHook(func() int32 { return 1 })
		}
		return c.RegisterAuthorizer(policy(m, d.modules))
	})
}

// introspection lists the read-only pragmas application SQL may run.
var introspection = map[string]bool{"table_info": true, "table_xinfo": true, "index_list": true,
	"index_info": true, "index_xinfo": true, "foreign_key_list": true}

// moduleReads lists the pragmas the built-in modules query, without an
// argument: rtree reads page_size when it creates a table, fts5 reads
// data_version to validate its cached structure.
var moduleReads = map[string]bool{"page_size": true, "data_version": true}

// rtreeShadow reports whether name may be a shadow table of rtree or
// geopoly. Both prepare their writes when they connect, which may happen
// first in read mode: those statements are authorized there, and Read's
// query_only=ON keeps them (and any table so named) from writing.
func rtreeShadow(name string) bool {
	n := strings.ToLower(name)
	return strings.HasSuffix(n, "_node") || strings.HasSuffix(n, "_rowid") || strings.HasSuffix(n, "_parent")
}

// reserved reports whether name is a library-owned object (with the index
// name REINDEX passes for a library table's key) or a virtual table exposing
// raw pages.
func reserved(name string) bool {
	n := strings.ToLower(name)
	return strings.HasPrefix(n, "_state_") || strings.HasPrefix(n, "sqlite_autoindex__state_") || n == "sqlite_dbpage" || n == "dbstat"
}

// policy is the authorizer of mode m. Application modes allow an explicit
// list of actions on the main database and deny everything else: transaction
// control, ATTACH/DETACH, pragmas other than introspection, temporary
// objects, virtual tables of modules outside modules, extension loading,
// reserved objects and unknown actions. Statements a virtual table runs on
// its shadow tables are authorized like application statements; defensive
// mode keeps the shadow tables read-only to application SQL (DECISIONS D19).
func policy(m mode, modules map[string]bool) sqlite.AuthorizerFn {
	if m == library {
		return func(sqlite.AuthorizerActionCode, string, string, string, string) sqlite.AuthorizerReturnCode {
			return sqlite.AuthorizerOK
		}
	}
	return func(a sqlite.AuthorizerActionCode, arg1, arg2, db, _ string) sqlite.AuthorizerReturnCode {
		// ALTER TABLE ... RENAME rewrites the temp schema too, which stays
		// empty: temporary objects are denied.
		tempSchema := db == "temp" && arg1 == "sqlite_temp_master" && (a == sqlite.AuthRead || a == sqlite.AuthUpdate)
		if a == sqlite.AuthAlterTable { // arg1 is the database; DROP COLUMN passes the column as db
			db = arg1
		}
		if a == sqlite.AuthReindex && reserved(arg1) { // arg1 is the index: skip it, so a bare REINDEX still runs
			return sqlite.AuthorizerIgnore
		}
		if (db != "" && db != "main" && !tempSchema) || reserved(arg1) {
			return sqlite.AuthorizerDeny
		}
		switch a {
		case sqlite.AuthSelect, sqlite.AuthRead, sqlite.AuthRecursive:
			return sqlite.AuthorizerOK
		case sqlite.AuthFunction:
			if !strings.EqualFold(arg2, "load_extension") {
				return sqlite.AuthorizerOK
			}
		case sqlite.AuthPragma:
			if introspection[strings.ToLower(arg1)] && !reserved(arg2) || moduleReads[strings.ToLower(arg1)] && arg2 == "" {
				return sqlite.AuthorizerOK
			}
		case sqlite.AuthInsert, sqlite.AuthUpdate, sqlite.AuthDelete:
			if m == mutation || rtreeShadow(arg1) { // read mode runs with query_only=ON
				return sqlite.AuthorizerOK
			}
		case sqlite.AuthCreateTable, sqlite.AuthCreateView,
			sqlite.AuthDropTable, sqlite.AuthDropView, sqlite.AuthReindex: // CREATE INDEX authorizes a REINDEX
			if m == mutation {
				return sqlite.AuthorizerOK
			}
		case sqlite.AuthCreateVTable, sqlite.AuthDropVTable: // arg2 is the module
			if m == mutation && modules[strings.ToLower(arg2)] {
				return sqlite.AuthorizerOK
			}
		case sqlite.AuthCreateIndex, sqlite.AuthCreateTrigger, sqlite.AuthDropIndex, sqlite.AuthDropTrigger,
			sqlite.AuthAlterTable: // arg2 is the table
			if m == mutation && !reserved(arg2) {
				return sqlite.AuthorizerOK
			}
		}
		return sqlite.AuthorizerDeny
	}
}

// scope gates a callback's SQL handle: usable only until the callback
// returns. A call racing with the end of the callback either completes
// before the scope closes or fails with ErrScope.
type scope struct {
	mu   sync.RWMutex
	conn *sql.Conn
	done bool
}

func runScoped(s *scope, fn func() ([]byte, error)) ([]byte, error) {
	defer func() { s.mu.Lock(); s.done = true; s.mu.Unlock() }()
	return fn()
}

func (s *scope) enter() error {
	s.mu.RLock()
	if s.done {
		s.mu.RUnlock()
		return ErrScope
	}
	return nil
}

// query runs q, passes the rows to fn, then closes them and reports the
// first of fn's error, the iteration error and the close error.
func (s *scope) query(ctx context.Context, q string, args []any, fn func(*sql.Rows) error) (err error) {
	if err := s.enter(); err != nil {
		return err
	}
	defer s.mu.RUnlock()
	rows, err := s.conn.QueryContext(ctx, q, args...)
	if err != nil {
		return err
	}
	defer func() {
		if cerr := rows.Close(); err == nil {
			err = cerr
		}
	}()
	if fn != nil {
		if err := fn(rows); err != nil {
			return err
		}
	}
	return rows.Err()
}

// Tx is the scoped SQL handle of a mutation callback.
type Tx struct{ scope }

// Query runs a statement and hands its rows to fn; the rows are closed when
// Query returns.
func (t *Tx) Query(ctx context.Context, q string, args []any, fn func(*sql.Rows) error) error {
	return t.query(ctx, q, args, fn)
}

// Exec runs one or more statements, each authorized separately.
func (t *Tx) Exec(ctx context.Context, q string, args ...any) (sql.Result, error) {
	if err := t.enter(); err != nil {
		return nil, err
	}
	defer t.mu.RUnlock()
	return t.conn.ExecContext(ctx, q, args...)
}

// View is the scoped read-only SQL handle of a read callback.
type View struct{ scope }

// Query runs a read-only statement and hands its rows to fn; the rows are
// closed when Query returns.
func (v *View) Query(ctx context.Context, q string, args []any, fn func(*sql.Rows) error) error {
	return v.query(ctx, q, args, fn)
}
