package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io/fs"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	garden "github.com/OrdalieTech/big-garden"
	"github.com/OrdalieTech/big-garden/internal/storetest"
)

// soakNode is one node process of the soak (qual soak starts three or more
// of them): a garden Node serving ledger entities over stdin/stdout JSON
// lines, like internal/qual/killnode, with a bounded schema. Each entity
// holds a counter, a ring of the last -ring ledger rows (slot = n mod ring,
// written in the transaction that increments the counter) and a 16-slot
// payload table, so its size stays bounded however long the soak runs.
func soakNode(args []string) error {
	fs := flag.NewFlagSet("soak-node", flag.ContinueOnError)
	var ep storetest.Endpoint
	var cache, node string
	fs.StringVar(&ep.URL, "url", "", "S3 endpoint")
	fs.StringVar(&ep.Region, "region", "us-east-1", "S3 region")
	fs.StringVar(&ep.Bucket, "bucket", "", "bucket")
	fs.StringVar(&ep.Prefix, "prefix", "", "prefix")
	fs.StringVar(&cache, "cache", "", "cache directory")
	fs.StringVar(&node, "node", "node", "node ID")
	ring := fs.Int("ring", 1024, "ledger ring size")
	entityBytes := fs.Int64("entity-bytes", 8<<20, "Limits.EntityBytes")
	cacheBytes := fs.Int64("cache-bytes", 512<<20, "Limits.CacheBytes")
	batch := fs.Int("batch", 0, "Limits.BatchMax (0: default)")
	delay := fs.Duration("delay", 0, "hold every S3 reply this long (injected latency)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	ep.AccessKey, ep.SecretKey = os.Getenv("AWS_ACCESS_KEY_ID"), os.Getenv("AWS_SECRET_ACCESS_KEY")
	ctx := context.Background()
	w := &wire{base: http.DefaultTransport.(*http.Transport).Clone(), inflight: map[string]int{}, delay: *delay}
	n, err := garden.Open(ctx, garden.Config{S3: ep.Client(&http.Client{Transport: w}), Bucket: ep.Bucket, Prefix: ep.Prefix, NodeID: node,
		CacheDir: cache, Limits: garden.Limits{EntityBytes: *entityBytes, CacheBytes: *cacheBytes, BatchMax: *batch}})
	if err != nil {
		return fmt.Errorf("open: %w", err)
	}
	in := bufio.NewScanner(os.Stdin)
	in.Buffer(make([]byte, 1<<20), 1<<20)
	out := json.NewEncoder(os.Stdout)
	var mu sync.Mutex
	var wg sync.WaitGroup
	for in.Scan() {
		var r sReq
		if err := json.Unmarshal(in.Bytes(), &r); err != nil {
			return fmt.Errorf("request: %w", err)
		}
		if r.Op == "outage" { // every storage request fails for the next Size ms (qual stress Q7)
			w.down.Store(time.Now().Add(time.Duration(r.Size) * time.Millisecond).UnixNano())
			mu.Lock()
			out.Encode(sResp{OK: true, Tag: r.Tag})
			mu.Unlock()
			continue
		}
		wg.Go(func() {
			resp := soakServe(ctx, n, r, *ring, cache)
			resp.Tag = r.Tag
			mu.Lock()
			out.Encode(resp)
			mu.Unlock()
		})
	}
	wg.Wait()
	return n.Close(ctx)
}

// sReq is one soak request; Size is the payload of a write or fill,
// incompressible unless Compressible.
type sReq struct {
	Op, Key, Inc, ID, Tag string
	Size                  int
	Compressible          bool `json:",omitempty"`
	Route                 *sRt
}

// sRt is a garden.Route in JSON.
type sRt struct {
	Inc, Node, Session string
	Epoch, Revision    uint64
}

func toSRt(r garden.Route) *sRt {
	return &sRt{Inc: hex.EncodeToString(r.Ref.Incarnation[:]), Node: r.Owner.NodeID,
		Session: hex.EncodeToString(r.Owner.Session[:]), Epoch: r.Epoch, Revision: r.Revision}
}

func fromSRt(key string, r *sRt) garden.Route {
	g := garden.Route{Ref: garden.Ref{Key: key}, Owner: garden.Owner{NodeID: r.Node}, Epoch: r.Epoch, Revision: r.Revision}
	hex.Decode(g.Ref.Incarnation[:], []byte(r.Inc))
	hex.Decode(g.Owner.Session[:], []byte(r.Session))
	return g
}

// sResp answers one sReq. A ring answers Ns/IDs in counter order; a read
// answers the counter and the last 8 IDs.
type sResp struct {
	OK       bool
	Code     string `json:",omitempty"`
	Err      string `json:",omitempty"`
	Value    string `json:",omitempty"`
	Seq      uint64 `json:",omitempty"`
	Replayed bool   `json:",omitempty"`
	Inc      string `json:",omitempty"`
	IDs      []string
	Ns       []int64
	Tag      string
	Route    *sRt
	Proc     *procStats `json:",omitempty"`
}

// procStats is a node's resource sample.
type procStats struct {
	RSS, HeapAlloc, HeapInuse, HeapSys int64
	Goroutines, FDs                    int
	CacheDirBytes                      int64
	CPUSeconds                         float64 // user + system since the process started
	Stats                              garden.Stats
}

func soakServe(ctx context.Context, n *garden.Node, r sReq, ring int, cache string) sResp {
	var id garden.MutationID
	hex.Decode(id[:], []byte(r.ID))
	ref := garden.Ref{Key: r.Key}
	hex.Decode(ref.Incarnation[:], []byte(r.Inc))
	req := garden.Request{ID: id, Hash: sha256.Sum256([]byte(r.Op + "\x00" + r.ID + "\x00" + strconv.Itoa(r.Size))), Schema: 1}
	var res garden.Result
	var err error
	var ids []string
	var ns []int64
	var route garden.Route
	switch r.Op {
	case "create":
		ref, res, err = n.Create(ctx, r.Key, req, func(tx *garden.Tx) ([]byte, error) {
			for _, s := range []string{"CREATE TABLE meta(c INTEGER NOT NULL)", "INSERT INTO meta VALUES (0)",
				"CREATE TABLE ledger(slot INTEGER PRIMARY KEY, n INTEGER NOT NULL, id TEXT NOT NULL)",
				"CREATE TABLE kv(k INTEGER PRIMARY KEY, v BLOB NOT NULL)"} {
				if _, err := tx.Exec(ctx, s); err != nil {
					return nil, err
				}
			}
			return []byte("0"), nil
		})
	case "write":
		pad := payload(r.Size, r.Compressible)
		res, err = n.Update(ctx, ref, req, func(tx *garden.Tx) ([]byte, error) {
			var c int64
			if err := tx.Query(ctx, "SELECT c FROM meta", nil, func(rows *sql.Rows) error {
				rows.Next()
				return rows.Scan(&c)
			}); err != nil {
				return nil, err
			}
			c++
			for _, s := range []struct {
				q    string
				args []any
			}{{"UPDATE meta SET c = ?", []any{c}}, {"INSERT OR REPLACE INTO ledger VALUES (?, ?, ?)", []any{c % int64(ring), c, r.ID}},
				{"INSERT OR REPLACE INTO kv VALUES (?, ?)", []any{c % 16, pad}}} {
				if _, err := tx.Exec(ctx, s.q, s.args...); err != nil {
					return nil, err
				}
			}
			return strconv.AppendInt(nil, c, 10), nil
		})
	case "read", "ring":
		q := "SELECT n, id FROM ledger ORDER BY n"
		if r.Op == "read" {
			q = "SELECT n, id FROM (SELECT n, id FROM ledger ORDER BY n DESC LIMIT 8) ORDER BY n"
		}
		res, err = n.Read(ctx, ref, 1, func(v *garden.View) ([]byte, error) {
			var c int64
			if err := v.Query(ctx, "SELECT c FROM meta", nil, func(rows *sql.Rows) error {
				rows.Next()
				return rows.Scan(&c)
			}); err != nil {
				return nil, err
			}
			return strconv.AppendInt(nil, c, 10), v.Query(ctx, q, nil, func(rows *sql.Rows) error {
				for rows.Next() {
					var k int64
					var s string
					if err := rows.Scan(&k, &s); err != nil {
						return err
					}
					ns, ids = append(ns, k), append(ids, s)
				}
				return rows.Err()
			})
		})
	case "fill": // Size more bytes in 512 KiB rows of kv after the 16 write slots (values are limited to 1 MiB), one transaction
		res, err = n.Update(ctx, ref, req, func(tx *garden.Tx) ([]byte, error) {
			var k int64
			if err := tx.Query(ctx, "SELECT coalesce(max(k), 15) FROM kv", nil, func(rows *sql.Rows) error {
				rows.Next()
				return rows.Scan(&k)
			}); err != nil {
				return nil, err
			}
			for left := r.Size; left > 0; left -= 512 << 10 {
				k++
				if _, err := tx.Exec(ctx, "INSERT INTO kv VALUES (?, ?)", k, payload(min(left, 512<<10), r.Compressible)); err != nil {
					return nil, err
				}
			}
			return []byte("0"), nil
		})
	case "evict":
		err = n.Evict(ctx, ref)
	case "lookup":
		res, err = n.Lookup(ctx, ref, id)
	case "collect":
		err = n.Collect(ctx, ref)
	case "delete":
		err = n.Delete(ctx, ref)
	case "resolve":
		route, err = n.Resolve(ctx, r.Key)
		ref = route.Ref
	case "takeover":
		if r.Route != nil {
			route, err = n.Takeover(ctx, fromSRt(r.Key, r.Route))
		} else if route, err = n.Resolve(ctx, r.Key); err == nil {
			route, err = n.Takeover(ctx, route)
		}
		ref = route.Ref
	case "stats":
		return sResp{OK: true, Proc: sample(n, cache)}
	case "heap": // write a heap profile to the path in Key
		f, ferr := os.Create(r.Key)
		if err = ferr; err == nil {
			err = errors.Join(pprof.Lookup("heap").WriteTo(f, 0), f.Close())
		}
		if err == nil {
			return sResp{OK: true}
		}
	default:
		err = fmt.Errorf("unknown op %q", r.Op)
	}
	if err != nil {
		var ge *garden.Error
		code := "other"
		var rt *sRt
		if errors.As(err, &ge) {
			code = string(ge.Code)
			if ge.Route != nil {
				rt = toSRt(*ge.Route)
			}
		}
		msg := err.Error()
		if c := errors.Unwrap(err); c != nil { // the driver logs the redacted cause too
			msg += " (cause: " + c.Error() + ")"
		}
		return sResp{Code: code, Err: msg, Route: rt}
	}
	var rt *sRt
	if r.Op == "resolve" || r.Op == "takeover" {
		rt = toSRt(route)
	}
	return sResp{OK: true, Value: string(res.Value), Seq: res.Version.Sequence, Replayed: res.Replayed,
		Inc: hex.EncodeToString(ref.Incarnation[:]), IDs: ids, Ns: ns, Route: rt}
}

func sample(n *garden.Node, cache string) *procStats {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	p := &procStats{RSS: rssOf(os.Getpid()), HeapAlloc: int64(m.HeapAlloc), HeapInuse: int64(m.HeapInuse), HeapSys: int64(m.HeapSys),
		Goroutines: runtime.NumGoroutine(), Stats: n.Stats()}
	var ru syscall.Rusage
	if syscall.Getrusage(syscall.RUSAGE_SELF, &ru) == nil {
		p.CPUSeconds = time.Duration(ru.Utime.Nano() + ru.Stime.Nano()).Seconds()
	}
	if fds, err := os.ReadDir("/proc/self/fd"); err == nil {
		p.FDs = len(fds)
	}
	filepath.WalkDir(cache, func(_ string, d fs.DirEntry, err error) error {
		if err == nil && !d.IsDir() {
			if i, err := d.Info(); err == nil {
				p.CacheDirBytes += i.Size()
			}
		}
		return nil
	})
	return p
}

// payload is n random bytes, or n bytes of a repeated sentence when
// compressible.
func payload(n int, compressible bool) []byte {
	if compressible {
		return bytes.Repeat([]byte("the same compressible sentence, again. "), n/39+1)[:n]
	}
	b := make([]byte, n)
	rand.Read(b)
	return b
}

// rssOf reads VmRSS of a process in bytes (0 if unreadable).
func rssOf(pid int) int64 {
	b, err := os.ReadFile(fmt.Sprintf("/proc/%d/status", pid))
	if err != nil {
		return 0
	}
	for l := range strings.Lines(string(b)) {
		if v, ok := strings.CutPrefix(l, "VmRSS:"); ok {
			var kb int64
			fmt.Sscan(v, &kb)
			return kb << 10
		}
	}
	return 0
}
