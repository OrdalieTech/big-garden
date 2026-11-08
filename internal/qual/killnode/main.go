// Command killnode is the test-only node process of the M4 process-kill
// harness (kill_test.go): one garden Node serving a ledger entity over
// stdin/stdout JSON lines. Build it with -tags gardenfail and set
// GARDEN_FAILPOINT=name:n to SIGKILL it at a commit boundary; name "reply"
// kills it right after its nth response is written.
package main

import (
	"bufio"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
	"syscall"
	"time"

	garden "github.com/OrdalieTech/big-garden"
	"github.com/OrdalieTech/big-garden/internal/storetest"
)

// Req is one operation: create, add (insert a log row named by the ID) or read.
type Req struct {
	Op, Key, Inc, ID, Input string
	Big                     bool // add a 40 KiB pad so the delta is an object, not inline
}

// Resp answers one Req.
type Resp struct {
	OK       bool
	Code     string
	Err      string
	Value    string
	Seq      uint64
	Replayed bool
	Inc      string
	IDs      []string
}

func main() {
	var ep storetest.Endpoint
	var cache, node string
	flag.StringVar(&ep.URL, "url", "", "S3 endpoint")
	flag.StringVar(&ep.Bucket, "bucket", "", "bucket")
	flag.StringVar(&ep.Prefix, "prefix", "", "prefix")
	flag.StringVar(&cache, "cache", "", "cache directory")
	flag.StringVar(&node, "node", "node", "node ID")
	commit := flag.Duration("commit-timeout", 0, "Limits.CommitTimeout (0: default)")
	flag.Parse()
	ep.Region, ep.AccessKey, ep.SecretKey = "us-east-1", os.Getenv("AWS_ACCESS_KEY_ID"), os.Getenv("AWS_SECRET_ACCESS_KEY")
	ctx := context.Background()
	n, err := garden.Open(ctx, garden.Config{S3: ep.Client(nil), Bucket: ep.Bucket, Prefix: ep.Prefix, NodeID: node, CacheDir: cache,
		Limits: garden.Limits{CommitTimeout: *commit, StorageAttemptTimeout: min(*commit, 2*time.Second)}})
	if err != nil {
		fmt.Fprintln(os.Stderr, "open:", err)
		os.Exit(2)
	}
	replyKill := 0
	if f := strings.Split(os.Getenv("GARDEN_FAILPOINT")+"::", ":"); f[0] == "reply" {
		replyKill, _ = strconv.Atoi(f[1])
	}
	in := bufio.NewScanner(os.Stdin)
	out := json.NewEncoder(os.Stdout)
	for replies := 1; in.Scan(); replies++ {
		var r Req
		if err := json.Unmarshal(in.Bytes(), &r); err != nil {
			fmt.Fprintln(os.Stderr, "request:", err)
			os.Exit(2)
		}
		out.Encode(serve(ctx, n, r))
		if replies == replyKill {
			syscall.Kill(os.Getpid(), syscall.SIGKILL)
		}
	}
	n.Close(ctx)
}

func serve(ctx context.Context, n *garden.Node, r Req) Resp {
	var id garden.MutationID
	hex.Decode(id[:], []byte(r.ID))
	var ref garden.Ref
	ref.Key = r.Key
	hex.Decode(ref.Incarnation[:], []byte(r.Inc))
	req := garden.Request{ID: id, Hash: sha256.Sum256([]byte(r.Op + "\x00" + r.Input)), Schema: 1}
	var res garden.Result
	var err error
	var ids []string
	switch r.Op {
	case "create":
		ref, res, err = n.Create(ctx, r.Key, req, func(tx *garden.Tx) ([]byte, error) {
			_, err := tx.Exec(ctx, "CREATE TABLE log(id TEXT NOT NULL, pad BLOB NOT NULL)")
			return []byte("created"), err
		})
	case "add":
		pad := []byte{}
		if r.Big {
			pad = make([]byte, 40<<10)
			rand.Read(pad)
		}
		res, err = n.Update(ctx, ref, req, func(tx *garden.Tx) ([]byte, error) {
			if _, err := tx.Exec(ctx, "INSERT INTO log VALUES (?, ?)", r.ID, pad); err != nil {
				return nil, err
			}
			var c int
			err := tx.Query(ctx, "SELECT count(*) FROM log", nil, func(rows *sql.Rows) error {
				rows.Next()
				return rows.Scan(&c)
			})
			return []byte(strconv.Itoa(c)), err
		})
	case "takeover":
		var route garden.Route
		if route, err = n.Resolve(ctx, r.Key); err == nil {
			route, err = n.Takeover(ctx, route)
			ref = route.Ref
		}
	case "read":
		res, err = n.Read(ctx, ref, 1, func(v *garden.View) ([]byte, error) {
			return nil, v.Query(ctx, "SELECT id FROM log", nil, func(rows *sql.Rows) error {
				for rows.Next() {
					var s string
					if err := rows.Scan(&s); err != nil {
						return err
					}
					ids = append(ids, s)
				}
				return rows.Err()
			})
		})
	}
	if err != nil {
		var ge *garden.Error
		code := "other"
		if errors.As(err, &ge) {
			code = string(ge.Code)
		}
		return Resp{Code: code, Err: err.Error()}
	}
	return Resp{OK: true, Value: string(res.Value), Seq: res.Version.Sequence, Replayed: res.Replayed,
		Inc: hex.EncodeToString(ref.Incarnation[:]), IDs: ids}
}
