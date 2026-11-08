package garden

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/binary"
	"log/slog"
	"time"

	"github.com/OrdalieTech/big-garden/internal/engine"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"modernc.org/sqlite"
)

// Ref prevents a retry for an old/imported incarnation from reaching a new one.
type Ref struct {
	Key         string
	Incarnation [16]byte
}

// Owner is the publishing identity of one Node process.
type Owner struct {
	NodeID  string
	Session [16]byte
}

// Route is an authoritative ownership observation of one entity.
type Route struct {
	Ref      Ref
	Owner    Owner // zero Owner means unowned, not nonexistent
	Epoch    uint64
	Revision uint64
}

// MutationID is 8-byte big-endian Unix ms + 16 cryptographic random bytes.
type MutationID [24]byte

// Request is the stable identity of one mutation; reuse it unchanged to retry.
type Request struct {
	ID     MutationID
	Hash   [32]byte // caller hashes canonical operation name, input, and schema contract
	Schema uint32
}

// Version names one published logical state of an entity.
type Version struct {
	Incarnation [16]byte
	Sequence    uint64
}

// Result is the copied, bounded outcome of a committed or replayed operation.
type Result struct {
	Value    []byte // copied, bounded; never an open cursor or streaming body
	Version  Version
	Replayed bool
}

// Config configures a Node. The caller owns S3 and Logger.
type Config struct {
	S3       *s3.Client // official SDK client, supplied by the caller
	Bucket   string
	Prefix   string
	NodeID   string
	CacheDir string
	Limits   Limits
	Logger   *slog.Logger // nil: no logs; else outcome_unknown and corrupt outcomes, rate-limited

	// Extend, if not nil, is called once by Open with the Node's private
	// SQLite driver, before any connection, to register SQL functions and
	// virtual table modules (DECISIONS D19). They reach only this Node's
	// connections; its modules may be used by CREATE VIRTUAL TABLE besides
	// the built-in fts5, fts5vocab, rtree, rtree_i32 and geopoly. Durable
	// state must live in the SQLite file only: a module keeps none
	// elsewhere. Functions may be called from statements, not from schema
	// objects (index expressions, triggers, views, CHECK, generated
	// columns), so activation never runs caller code. Collations,
	// connection hooks and names replacing a built-in function or module
	// are refused. An error fails Open with invalid_argument.
	Extend func(*sqlite.Driver) error
}

// Tx is the scoped SQL handle of a mutation callback. It is invalid after the
// callback returns.
type Tx struct{ t *engine.Tx }

// View is the scoped read-only SQL handle of a Read callback. It is invalid
// after the callback returns.
type View struct{ v *engine.View }

// NewMutationID returns a fresh MutationID stamped with t.
func NewMutationID(t time.Time) (MutationID, error) {
	var id MutationID
	ms := t.UnixMilli()
	if ms <= 0 {
		return id, invalid("mutation ID time must be after the Unix epoch")
	}
	binary.BigEndian.PutUint64(id[:8], uint64(ms))
	rand.Read(id[8:]) // never returns an error since Go 1.24
	return id, nil
}

func (v *View) Query(ctx context.Context, q string, args []any, fn func(*sql.Rows) error) error {
	return v.v.Query(ctx, q, args, fn)
}

func (t *Tx) Query(ctx context.Context, q string, args []any, fn func(*sql.Rows) error) error {
	return t.t.Query(ctx, q, args, fn)
}

func (t *Tx) Exec(ctx context.Context, q string, args ...any) (sql.Result, error) {
	return t.t.Exec(ctx, q, args...)
}
