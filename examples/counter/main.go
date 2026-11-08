// Command counter keeps named counters in S3, one big-garden entity per
// counter. Each invocation opens a Node, runs one command and closes it, so
// running it under two NodeIDs shows ownership moving between processes.
//
//	counter [-node ID] [-cache DIR] create NAME
//	counter incr [-by N] [-op HEX] NAME   # -op: retry an earlier request
//	counter get NAME
//	counter lookup NAME OP
//	counter migrate NAME
//	counter export NAME FILE
//	counter import NAME FILE              # creates NAME from an export
//
// Every command prints one JSON line. S3 comes from GARDEN_S3_ENDPOINT,
// GARDEN_S3_REGION, GARDEN_S3_BUCKET, GARDEN_S3_PREFIX, AWS_ACCESS_KEY_ID and
// AWS_SECRET_ACCESS_KEY.
package main

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"time"

	garden "github.com/OrdalieTech/big-garden"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// Schema versions. Version 1 is what create builds; version 2 adds the time
// of the last change. Commands run against version 2 and upgrade a version 1
// counter on first use.
const (
	schemaV1 = 1
	schemaV2 = 2
)

type output struct {
	Command  string `json:"command"`
	Name     string `json:"name"`
	Value    int64  `json:"value"`
	Sequence uint64 `json:"sequence,omitempty"`
	Replayed bool   `json:"replayed,omitempty"`
	Op       string `json:"op,omitempty"`
	TookOver bool   `json:"took_over,omitempty"`
	Owner    string `json:"owner,omitempty"`
}

func main() {
	node := flag.String("node", "node-a", "NodeID of this process")
	cache := flag.String("cache", "", "cache dir (default: a dir per NodeID under the user cache)")
	flag.Parse()
	if *cache == "" {
		d, _ := os.UserCacheDir()
		*cache = filepath.Join(d, "garden-counter", *node)
	}
	if err := run(context.Background(), *node, *cache, flag.Args()); err != nil {
		fmt.Fprintln(os.Stderr, "counter:", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, nodeID, cacheDir string, args []string) error {
	if len(args) < 2 {
		return errors.New("usage: counter [-node ID] COMMAND [ARGS] NAME")
	}
	cmd := args[0]
	fs := flag.NewFlagSet(cmd, flag.ExitOnError)
	by := fs.Int64("by", 1, "increment")
	opHex := fs.String("op", "", "operation ID (hex) to retry")
	fs.Parse(args[1:])
	if fs.NArg() < 1 {
		return errors.New("missing counter name")
	}
	name := fs.Arg(0)

	n, err := open(ctx, nodeID, cacheDir)
	if err != nil {
		return err
	}
	defer n.Close(context.Background())
	c := &counters{n: n}
	key := "counter/" + name

	var out output
	switch cmd {
	case "create":
		out, err = c.create(ctx, key)
	case "incr":
		var id garden.MutationID
		if id, err = parseOrNewID(*opHex); err == nil {
			out, err = c.incr(ctx, key, *by, id)
		}
	case "get":
		out, err = c.get(ctx, key)
	case "lookup":
		var id garden.MutationID
		if fs.NArg() < 2 {
			return errors.New("lookup NAME OP")
		}
		if id, err = parseOrNewID(fs.Arg(1)); err == nil {
			out, err = c.lookup(ctx, key, id)
		}
	case "migrate":
		out, err = c.migrate(ctx, key)
	case "export", "import":
		if fs.NArg() < 2 {
			return fmt.Errorf("%s NAME FILE", cmd)
		}
		if cmd == "export" {
			out, err = c.export(ctx, key, fs.Arg(1))
		} else {
			out, err = c.importFile(ctx, key, fs.Arg(1))
		}
	default:
		return fmt.Errorf("unknown command %q", cmd)
	}
	if err != nil {
		return err
	}
	out.Command, out.Name = cmd, name
	return json.NewEncoder(os.Stdout).Encode(out)
}

func open(ctx context.Context, nodeID, cacheDir string) (*garden.Node, error) {
	env := func(k, def string) string {
		if v := os.Getenv(k); v != "" {
			return v
		}
		return def
	}
	creds := aws.Credentials{AccessKeyID: os.Getenv("AWS_ACCESS_KEY_ID"), SecretAccessKey: os.Getenv("AWS_SECRET_ACCESS_KEY")}
	client := s3.New(s3.Options{
		Region:       env("GARDEN_S3_REGION", "us-east-1"),
		BaseEndpoint: aws.String(env("GARDEN_S3_ENDPOINT", "http://127.0.0.1:9100")),
		UsePathStyle: true,
		Credentials:  aws.CredentialsProviderFunc(func(context.Context) (aws.Credentials, error) { return creds, nil }),
	})
	if err := os.MkdirAll(cacheDir, 0o700); err != nil {
		return nil, err
	}
	return garden.Open(ctx, garden.Config{
		S3:       client,
		Bucket:   env("GARDEN_S3_BUCKET", "state-test"),
		Prefix:   env("GARDEN_S3_PREFIX", "examples/counter"),
		NodeID:   nodeID,
		CacheDir: cacheDir,
	})
}

type counters struct {
	n        *garden.Node
	tookOver bool
}

// request returns the stable identity of one operation: the hash covers the
// operation name, its input and the schema it expects, so retrying the same
// ID with other input is refused as id_conflict.
func request(id garden.MutationID, schema uint32, op string, input ...any) garden.Request {
	h := sha256.New()
	fmt.Fprintf(h, "counter/%s/schema=%d", op, schema)
	for _, v := range input {
		fmt.Fprintf(h, "\x00%v", v)
	}
	r := garden.Request{ID: id, Schema: schema}
	h.Sum(r.Hash[:0])
	return r
}

func (c *counters) create(ctx context.Context, key string) (output, error) {
	id, err := garden.NewMutationID(time.Now())
	if err != nil {
		return output{}, err
	}
	_, res, err := c.n.Create(ctx, key, request(id, schemaV1, "create", key), func(tx *garden.Tx) ([]byte, error) {
		_, err := tx.Exec(ctx, `CREATE TABLE counter(value INTEGER NOT NULL)`)
		if err == nil {
			_, err = tx.Exec(ctx, `INSERT INTO counter VALUES (0)`)
		}
		return nil, err
	})
	if err != nil {
		return output{}, err
	}
	return output{Sequence: res.Version.Sequence, Op: hex.EncodeToString(id[:])}, nil
}

// do runs op on the counter at key: it resolves the key, follows a not_owner
// Route with a Takeover, and upgrades a version 1 counter on schema_mismatch.
// op must be safe to call again; mutations reuse their Request unchanged.
func (c *counters) do(ctx context.Context, key string, op func(garden.Ref) error) error {
	route, err := c.n.Resolve(ctx, key)
	if err != nil {
		return err
	}
	migrated := false
	for attempt := 0; attempt < 4; attempt++ {
		err = op(route.Ref)
		var ge *garden.Error
		if !errors.As(err, &ge) {
			return err
		}
		switch {
		case ge.Code == garden.CodeNotOwner && ge.Route != nil:
			if _, err = c.n.Takeover(ctx, *ge.Route); err != nil && !isCode(err, garden.CodeRouteChanged) {
				return err
			}
			c.tookOver = true
		case ge.Code == garden.CodeSchemaMismatch && !migrated:
			if err = c.upgrade(ctx, route.Ref); err != nil && !isCode(err, garden.CodeSchemaMismatch) {
				return err
			}
			migrated = true
		case ge.Code == garden.CodeRouteChanged && ge.Route != nil:
			route = *ge.Route
		default:
			return err
		}
	}
	return err
}

// upgrade migrates a version 1 counter to version 2. A counter already at
// version 2 answers schema_mismatch, which the caller ignores.
func (c *counters) upgrade(ctx context.Context, ref garden.Ref) error {
	id, err := garden.NewMutationID(time.Now())
	if err != nil {
		return err
	}
	_, err = c.n.Migrate(ctx, ref, request(id, schemaV1, "migrate-v2"), schemaV2, func(tx *garden.Tx) error {
		_, err := tx.Exec(ctx, `ALTER TABLE counter ADD COLUMN updated_ms INTEGER NOT NULL DEFAULT 0`)
		return err
	})
	return err
}

func (c *counters) incr(ctx context.Context, key string, by int64, id garden.MutationID) (output, error) {
	req := request(id, schemaV2, "incr", key, by)
	var res garden.Result
	err := c.do(ctx, key, func(ref garden.Ref) (err error) {
		res, err = c.n.Update(ctx, ref, req, func(tx *garden.Tx) ([]byte, error) {
			// The callback may run again after an unacknowledged attempt, so
			// it reads the time from the operation ID, not the clock.
			ms := int64(binary.BigEndian.Uint64(id[:8]))
			if _, err := tx.Exec(ctx, `UPDATE counter SET value = value + ?, updated_ms = ?`, by, ms); err != nil {
				return nil, err
			}
			var v int64
			err := tx.Query(ctx, `SELECT value FROM counter`, nil, func(r *Rows) error { return scanOne(r, &v) })
			return []byte(strconv.FormatInt(v, 10)), err
		})
		return err
	})
	if err != nil {
		return output{}, err
	}
	return c.result(res, id), nil
}

func (c *counters) get(ctx context.Context, key string) (output, error) {
	var res garden.Result
	err := c.do(ctx, key, func(ref garden.Ref) (err error) {
		res, err = c.n.Read(ctx, ref, schemaV2, func(v *garden.View) ([]byte, error) {
			var n int64
			err := v.Query(ctx, `SELECT value FROM counter`, nil, func(r *Rows) error { return scanOne(r, &n) })
			return []byte(strconv.FormatInt(n, 10)), err
		})
		return err
	})
	if err != nil {
		return output{}, err
	}
	return c.result(res, garden.MutationID{}), nil
}

func (c *counters) lookup(ctx context.Context, key string, id garden.MutationID) (output, error) {
	var res garden.Result
	err := c.do(ctx, key, func(ref garden.Ref) (err error) {
		res, err = c.n.Lookup(ctx, ref, id)
		return err
	})
	if err != nil {
		return output{}, err
	}
	return c.result(res, id), nil
}

func (c *counters) migrate(ctx context.Context, key string) (output, error) {
	err := c.do(ctx, key, func(ref garden.Ref) error { return c.upgrade(ctx, ref) })
	if isCode(err, garden.CodeSchemaMismatch) {
		err = nil // already at version 2
	}
	if err != nil {
		return output{}, err
	}
	return c.get(ctx, key)
}

func (c *counters) export(ctx context.Context, key, file string) (output, error) {
	f, err := os.Create(file)
	if err != nil {
		return output{}, err
	}
	defer f.Close()
	var v garden.Version
	err = c.do(ctx, key, func(ref garden.Ref) (err error) {
		if _, err = f.Seek(0, 0); err == nil {
			if err = f.Truncate(0); err == nil {
				v, err = c.n.Export(ctx, ref, f)
			}
		}
		return err
	})
	if err == nil {
		err = f.Close()
	}
	if err != nil {
		return output{}, err
	}
	return output{Sequence: v.Sequence, TookOver: c.tookOver}, nil
}

func (c *counters) importFile(ctx context.Context, key, file string) (output, error) {
	f, err := os.Open(file)
	if err != nil {
		return output{}, err
	}
	defer f.Close()
	id, err := garden.NewMutationID(time.Now())
	if err != nil {
		return output{}, err
	}
	// The archive carries its schema; Import refuses a Request for another.
	_, res, err := c.n.Import(ctx, key, request(id, schemaV2, "import", key), f)
	if err != nil {
		return output{}, err
	}
	out, err := c.get(ctx, key)
	out.Sequence = res.Version.Sequence
	return out, err
}

func (c *counters) result(res garden.Result, id garden.MutationID) output {
	v, _ := strconv.ParseInt(string(res.Value), 10, 64)
	out := output{Value: v, Sequence: res.Version.Sequence, Replayed: res.Replayed, TookOver: c.tookOver,
		Owner: c.n.Owner().NodeID}
	if id != (garden.MutationID{}) {
		out.Op = hex.EncodeToString(id[:])
	}
	return out
}

func parseOrNewID(s string) (garden.MutationID, error) {
	var id garden.MutationID
	if s == "" {
		return garden.NewMutationID(time.Now())
	}
	b, err := hex.DecodeString(s)
	if err != nil || len(b) != len(id) {
		return id, fmt.Errorf("operation ID %q: want %d hex bytes", s, len(id))
	}
	copy(id[:], b)
	return id, nil
}

func isCode(err error, code garden.Code) bool {
	var ge *garden.Error
	return errors.As(err, &ge) && ge.Code == code
}

// Rows is the cursor a Query callback scans.
type Rows = sql.Rows

// scanOne scans the single row of r into dst.
func scanOne(r *Rows, dst any) error {
	if !r.Next() {
		if err := r.Err(); err != nil {
			return err
		}
		return sql.ErrNoRows
	}
	return r.Scan(dst)
}
