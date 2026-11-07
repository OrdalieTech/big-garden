package state

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/binary"
	"io"
	"log/slog"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
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
	Logger   *slog.Logger
}

// Node is one process's handle on a namespace.
type Node struct{}

// Tx is the scoped SQL handle of a mutation callback. It is invalid after the
// callback returns.
type Tx struct{}

// View is the scoped read-only SQL handle of a Read callback. It is invalid
// after the callback returns.
type View struct{}

// Open validates cfg and opens a Node on its namespace.
func Open(ctx context.Context, cfg Config) (*Node, error) {
	if _, err := cfg.validate(); err != nil {
		return nil, err
	}
	return nil, unsupported()
}

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

func (*Node) Owner() Owner { return Owner{} }

func (*Node) Resolve(context.Context, string) (Route, error) { return Route{}, unsupported() }

func (*Node) Create(context.Context, string, Request, func(*Tx) ([]byte, error)) (Ref, Result, error) {
	return Ref{}, Result{}, unsupported()
}

func (*Node) Takeover(context.Context, Route) (Route, error) { return Route{}, unsupported() }

func (*Node) Release(context.Context, Ref) error { return unsupported() }

func (*Node) Update(context.Context, Ref, Request, func(*Tx) ([]byte, error)) (Result, error) {
	return Result{}, unsupported()
}

func (*Node) Read(context.Context, Ref, uint32, func(*View) ([]byte, error)) (Result, error) {
	return Result{}, unsupported()
}

func (*Node) Lookup(context.Context, Ref, MutationID) (Result, error) { return Result{}, unsupported() }

// Migrate moves the schema from Request.Schema to the uint32 argument.
func (*Node) Migrate(context.Context, Ref, Request, uint32, func(*Tx) error) (Result, error) {
	return Result{}, unsupported()
}

func (*Node) Evict(context.Context, Ref) error { return unsupported() }

func (*Node) Export(context.Context, Ref, io.Writer) (Version, error) {
	return Version{}, unsupported()
}

func (*Node) Import(context.Context, string, Request, io.Reader) (Ref, Result, error) {
	return Ref{}, Result{}, unsupported()
}

// Collect deletes unreachable immutable artifacts of one entity (PLAN §9.2).
func (*Node) Collect(context.Context, Ref) error { return unsupported() }

func (*Node) Stats() Stats { return Stats{} }

func (*Node) Close(context.Context) error { return unsupported() }

func (*View) Query(context.Context, string, []any, func(*sql.Rows) error) error { return unsupported() }

func (*Tx) Query(context.Context, string, []any, func(*sql.Rows) error) error { return unsupported() }

func (*Tx) Exec(context.Context, string, ...any) (sql.Result, error) { return nil, unsupported() }

func unsupported() *Error { return newError(CodeUnsupported, "not implemented yet", nil) }
