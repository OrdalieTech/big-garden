// Command kvregistry keeps a key/value registry per tenant, one big-garden
// entity per tenant, with full-text search over values (FTS5) and vector
// similarity over an embedding of each value (package vec).
//
//	kvregistry [-node ID] [-cache DIR] TENANT create
//	kvregistry TENANT put KEY VALUE
//	kvregistry TENANT get KEY
//	kvregistry TENANT search QUERY     # FTS5 MATCH syntax
//	kvregistry TENANT near TEXT [K]    # nearest values by embedding
//
// S3 comes from GARDEN_S3_ENDPOINT, GARDEN_S3_REGION, GARDEN_S3_BUCKET,
// GARDEN_S3_PREFIX, AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY. The
// embeddings are a deterministic stand-in for a real model.
package main

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/binary"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"hash/fnv"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	garden "github.com/OrdalieTech/big-garden"
	"github.com/OrdalieTech/big-garden/vec"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

const schema = 1

// The schema of a tenant. Triggers keep the FTS5 index in step with kv; they
// may not call vec functions (Config.Extend functions are refused in schema
// objects), so the embedding is computed by the statement that writes it.
var ddl = []string{
	`CREATE TABLE kv(k TEXT PRIMARY KEY, v TEXT NOT NULL, emb BLOB NOT NULL)`,
	`CREATE VIRTUAL TABLE kv_fts USING fts5(k UNINDEXED, v)`,
	`CREATE TRIGGER kv_ai AFTER INSERT ON kv BEGIN
		INSERT INTO kv_fts(rowid, k, v) VALUES (new.rowid, new.k, new.v); END`,
	`CREATE TRIGGER kv_au AFTER UPDATE ON kv BEGIN
		DELETE FROM kv_fts WHERE rowid = old.rowid;
		INSERT INTO kv_fts(rowid, k, v) VALUES (new.rowid, new.k, new.v); END`,
	`CREATE TRIGGER kv_ad AFTER DELETE ON kv BEGIN DELETE FROM kv_fts WHERE rowid = old.rowid; END`,
}

// Registry is the per-tenant registry on one Node.
type Registry struct{ n *garden.Node }

// Hit is one search or similarity result.
type Hit struct {
	Key      string  `json:"key"`
	Value    string  `json:"value"`
	Distance float64 `json:"distance,omitempty"`
}

// Open opens a Node with the vec functions registered.
func Open(ctx context.Context, client *s3.Client, bucket, prefix, nodeID, cacheDir string) (*Registry, error) {
	n, err := garden.Open(ctx, garden.Config{S3: client, Bucket: bucket, Prefix: prefix, NodeID: nodeID,
		CacheDir: cacheDir, Extend: vec.Register})
	if err != nil {
		return nil, err
	}
	return &Registry{n: n}, nil
}

func (r *Registry) Close(ctx context.Context) error { return r.n.Close(ctx) }

func key(tenant string) string { return "tenant/" + tenant }

func request(op string, input ...string) (garden.Request, error) {
	id, err := garden.NewMutationID(time.Now())
	if err != nil {
		return garden.Request{}, err
	}
	return garden.Request{ID: id, Schema: schema,
		Hash: sha256.Sum256([]byte(fmt.Sprintf("kv/%s/schema=%d\x00%s", op, schema, strings.Join(input, "\x00"))))}, nil
}

// CreateTenant creates the entity of tenant.
func (r *Registry) CreateTenant(ctx context.Context, tenant string) error {
	req, err := request("create", tenant)
	if err != nil {
		return err
	}
	for {
		_, _, err = r.n.Create(ctx, key(tenant), req, func(tx *garden.Tx) ([]byte, error) {
			for _, q := range ddl {
				if _, err := tx.Exec(ctx, q); err != nil {
					return nil, err
				}
			}
			return nil, nil
		})
		if !isCode(err, garden.CodeOutcomeUnknown) {
			return err
		}
		// May have committed: retry the identical Request, never a new one.
	}
}

// Put sets key to value in tenant's registry.
func (r *Registry) Put(ctx context.Context, tenant, k, v string) error {
	req, err := request("put", tenant, k, v)
	if err != nil {
		return err
	}
	emb := embedJSON(v)
	return r.do(ctx, tenant, func(ref garden.Ref) error {
		_, err := r.n.Update(ctx, ref, req, func(tx *garden.Tx) ([]byte, error) {
			_, err := tx.Exec(ctx, `INSERT INTO kv(k, v, emb) VALUES (?, ?, vec_f32(?))
				ON CONFLICT(k) DO UPDATE SET v = excluded.v, emb = excluded.emb`, k, v, emb)
			return nil, err
		})
		return err
	})
}

// Get returns the value of key, and whether it exists.
func (r *Registry) Get(ctx context.Context, tenant, k string) (string, bool, error) {
	hits, err := r.query(ctx, tenant, `SELECT k, v, 0 FROM kv WHERE k = ?`, k)
	if err != nil || len(hits) == 0 {
		return "", false, err
	}
	return hits[0].Value, true, nil
}

// Search returns the values matching an FTS5 query, best first.
func (r *Registry) Search(ctx context.Context, tenant, query string, limit int) ([]Hit, error) {
	return r.query(ctx, tenant, `SELECT k, v, 0 FROM kv_fts WHERE kv_fts MATCH ? ORDER BY rank LIMIT ?`, query, limit)
}

// Near returns the k values whose embedding is closest to that of text.
func (r *Registry) Near(ctx context.Context, tenant, text string, k int) ([]Hit, error) {
	return r.query(ctx, tenant, `SELECT k, v, vec_distance_cosine(emb, ?) AS d FROM kv ORDER BY d, k LIMIT ?`,
		embedBlob(text), k)
}

// query runs a strong read returning rows of (key, value, distance). The
// rows travel as the Read's Result value, which is bounded by
// Limits.ResultBytes.
func (r *Registry) query(ctx context.Context, tenant, q string, args ...any) ([]Hit, error) {
	var res garden.Result
	err := r.do(ctx, tenant, func(ref garden.Ref) (err error) {
		res, err = r.n.Read(ctx, ref, schema, func(v *garden.View) ([]byte, error) {
			hits := []Hit{}
			err := v.Query(ctx, q, args, func(rows *sql.Rows) error {
				for rows.Next() {
					var h Hit
					if err := rows.Scan(&h.Key, &h.Value, &h.Distance); err != nil {
						return err
					}
					hits = append(hits, h)
				}
				return rows.Err()
			})
			if err != nil {
				return nil, err
			}
			return json.Marshal(hits)
		})
		return err
	})
	if err != nil {
		return nil, err
	}
	var hits []Hit
	return hits, json.Unmarshal(res.Value, &hits)
}

// do runs op on tenant's entity, taking it over when another node owns it.
// op must be safe to call again; mutations reuse their Request unchanged.
func (r *Registry) do(ctx context.Context, tenant string, op func(garden.Ref) error) error {
	route, err := r.n.Resolve(ctx, key(tenant))
	if err != nil {
		return err
	}
	for attempt := 0; ; attempt++ {
		err = op(route.Ref)
		var ge *garden.Error
		if attempt == 3 || !errors.As(err, &ge) {
			return err
		}
		switch ge.Code {
		case garden.CodeNotOwner, garden.CodeRouteChanged:
			if ge.Route == nil {
				return err
			}
			// A lost race (route_changed) is retried with the next Route.
			if _, err := r.n.Takeover(ctx, *ge.Route); err != nil && !isCode(err, garden.CodeRouteChanged) {
				return err
			}
		case garden.CodeOutcomeUnknown, garden.CodeUnavailable:
			time.Sleep(ge.RetryAfter)
		default:
			return err
		}
	}
}

// embed is a deterministic stand-in for an embedding model: a normalized
// bag of hashed words in 16 dimensions.
func embed(text string) []float32 {
	v := make([]float32, 16)
	for _, w := range strings.Fields(strings.ToLower(text)) {
		h := fnv.New32a()
		h.Write([]byte(strings.Trim(w, ".,;:!?")))
		v[h.Sum32()%16]++
	}
	var norm float64
	for _, x := range v {
		norm += float64(x * x)
	}
	for i := range v {
		if norm > 0 {
			v[i] = float32(float64(v[i]) / math.Sqrt(norm))
		}
	}
	return v
}

// embedJSON is the embedding as the JSON text vec_f32 accepts.
func embedJSON(text string) string {
	parts := make([]string, 0, 16)
	for _, x := range embed(text) {
		parts = append(parts, strconv.FormatFloat(float64(x), 'g', -1, 32))
	}
	return "[" + strings.Join(parts, ",") + "]"
}

// embedBlob is the embedding as the little-endian float32 BLOB vec stores.
func embedBlob(text string) []byte {
	v := embed(text)
	b := make([]byte, 4*len(v))
	for i, x := range v {
		binary.LittleEndian.PutUint32(b[4*i:], math.Float32bits(x))
	}
	return b
}

func isCode(err error, code garden.Code) bool {
	var ge *garden.Error
	return errors.As(err, &ge) && ge.Code == code
}

// s3Client builds an S3 client from the environment.
func s3Client() *s3.Client {
	creds := aws.Credentials{AccessKeyID: os.Getenv("AWS_ACCESS_KEY_ID"), SecretAccessKey: os.Getenv("AWS_SECRET_ACCESS_KEY")}
	return s3.New(s3.Options{
		Region:       env("GARDEN_S3_REGION", "us-east-1"),
		BaseEndpoint: aws.String(env("GARDEN_S3_ENDPOINT", "http://127.0.0.1:9100")),
		UsePathStyle: true,
		Credentials:  aws.CredentialsProviderFunc(func(context.Context) (aws.Credentials, error) { return creds, nil }),
	})
}

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func main() {
	node := flag.String("node", "kv-a", "NodeID of this process")
	cache := flag.String("cache", "", "cache dir (default: a dir per NodeID under the user cache)")
	flag.Parse()
	if *cache == "" {
		d, _ := os.UserCacheDir()
		*cache = filepath.Join(d, "garden-kvregistry", *node)
	}
	if err := run(context.Background(), *node, *cache, flag.Args()); err != nil {
		fmt.Fprintln(os.Stderr, "kvregistry:", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, node, cache string, args []string) error {
	if len(args) < 2 {
		return errors.New("usage: kvregistry [-node ID] TENANT COMMAND [ARGS]")
	}
	if err := os.MkdirAll(cache, 0o700); err != nil {
		return err
	}
	r, err := Open(ctx, s3Client(), env("GARDEN_S3_BUCKET", "state-test"), env("GARDEN_S3_PREFIX", "examples/kvregistry"), node, cache)
	if err != nil {
		return err
	}
	defer r.Close(context.Background())
	tenant, cmd, rest := args[0], args[1], args[2:]
	need := map[string]int{"create": 0, "put": 2, "get": 1, "search": 1, "near": 1}
	if n, ok := need[cmd]; !ok || len(rest) < n {
		return fmt.Errorf("bad command %q", strings.Join(args[1:], " "))
	}
	var out any
	switch cmd {
	case "create":
		err = r.CreateTenant(ctx, tenant)
	case "put":
		err = r.Put(ctx, tenant, rest[0], rest[1])
	case "get":
		var v string
		var ok bool
		v, ok, err = r.Get(ctx, tenant, rest[0])
		out = map[string]any{"key": rest[0], "value": v, "found": ok}
	case "search":
		out, err = r.Search(ctx, tenant, rest[0], 10)
	case "near":
		k := 3
		if len(rest) > 1 {
			if k, err = strconv.Atoi(rest[1]); err != nil {
				return err
			}
		}
		out, err = r.Near(ctx, tenant, rest[0], k)
	}
	if err != nil || out == nil {
		return err
	}
	return json.NewEncoder(os.Stdout).Encode(out)
}
