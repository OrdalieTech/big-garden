package main

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	garden "github.com/OrdalieTech/big-garden"
	"github.com/OrdalieTech/big-garden/internal/store"
	"github.com/OrdalieTech/big-garden/internal/storetest"
)

// stress is a scaled-down PLAN §17 Q0/Q1 latency run through the library on
// one node: sequential and concurrent inline (1 KiB) updates, upload-path
// updates, strong reads, and cold activations (Evict, then Read) of a 1 MiB
// and a 16 MiB entity. Every physical request is classified on the wire;
// errors and outcome_unknown are counted, never dropped. Everything is under
// a fresh random prefix and deleted afterwards.
func stress(args []string) error {
	fs := flag.NewFlagSet("stress", flag.ContinueOnError)
	local := fs.Bool("local", false, "use the pinned local MinIO fixture")
	remote := fs.Bool("remote", false, "use QUAL_S3_* and AWS_* (missing: blocked_missing_remote_configuration)")
	ops := fs.Int("ops", 200, "operations per latency phase")
	workers := fs.Int("workers", 8, "concurrent clients of the concurrent phase (one entity each)")
	cold := fs.Int("cold", 5, "cold activations per size")
	out := fs.String("out", "", "directory for stress.json")
	if err := fs.Parse(args); err != nil {
		return err
	}
	var ep storetest.Endpoint
	var err error
	switch {
	case *remote:
		ep, err = storetest.RemoteFromEnv()
	case *local:
		ep, err = storetest.StartMinIO(context.Background())
	default:
		return errors.New("choose --local or --remote")
	}
	if err != nil {
		return err
	}
	ep = ep.Sub("stress")
	ctx := context.Background()
	wire := &wire{base: http.DefaultTransport.(*http.Transport).Clone(), inflight: map[string]int{}}
	dir, err := os.MkdirTemp("", "stress-cache-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(dir)
	n, err := garden.Open(ctx, garden.Config{S3: ep.Client(&http.Client{Transport: wire}), Bucket: ep.Bucket,
		Prefix: ep.Prefix, NodeID: "stress", CacheDir: dir, Limits: garden.Limits{EntityBytes: 32 << 20, CacheBytes: 2 << 30}})
	if err != nil {
		return err
	}
	rep := stressReport{Endpoint: ep.URL, Prefix: ep.Prefix, Started: time.Now().UTC(), Ops: *ops, Workers: *workers}
	defer func() {
		cerr := n.Close(ctx)
		k, err := cleanup(ctx, store.NewS3(ep.Client(nil), ep.Bucket, ep.Prefix, time.Minute))
		fmt.Printf("close: %v; cleanup: %d objects deleted under %s (%v)\n", cerr, k, ep.Prefix, err)
	}()

	refs := make([]garden.Ref, max(*workers, 1))
	rep.phase("create", wire, n, len(refs), 1, func(i int) error {
		var err error
		refs[i], _, err = n.Create(ctx, fmt.Sprintf("e%03d", i), req(), func(tx *garden.Tx) ([]byte, error) {
			_, err := tx.Exec(ctx, "CREATE TABLE kv(k INTEGER PRIMARY KEY, v BLOB NOT NULL)")
			return nil, err
		})
		return err
	})
	small := func(i int) error { return update(ctx, n, refs[i%len(refs)], i%64, 1<<10) }
	rep.phase("inline-seq", wire, n, *ops, 1, func(i int) error { return update(ctx, n, refs[0], i%64, 1<<10) })
	rep.phase("inline-conc", wire, n, *ops, *workers, small)
	rep.phase("upload-seq", wire, n, *ops/4, 1, func(i int) error { return update(ctx, n, refs[0], 1000+i%8, 48<<10) })
	rep.phase("read-seq", wire, n, *ops, 1, func(int) error { return read(ctx, n, refs[0]) })
	rep.phase("read-conc", wire, n, *ops, *workers, func(i int) error { return read(ctx, n, refs[i%len(refs)]) })
	for _, size := range []int{1 << 20, 16 << 20} {
		label := fmt.Sprintf("%dm", size>>20)
		var ref garden.Ref
		rep.phase("fill-"+label, wire, n, 1+size/(512<<10), 1, func(i int) error {
			if i == 0 {
				var err error
				ref, _, err = n.Create(ctx, "cold-"+label, req(), func(tx *garden.Tx) ([]byte, error) {
					_, err := tx.Exec(ctx, "CREATE TABLE kv(k INTEGER PRIMARY KEY, v BLOB NOT NULL)")
					return nil, err
				})
				return err
			}
			return update(ctx, n, ref, i, 512<<10)
		})
		time.Sleep(2 * time.Second) // let background maintenance settle before evicting
		rep.phase("cold-"+label, wire, n, *cold, 1, func(int) error {
			if err := n.Evict(ctx, ref); err != nil {
				return err
			}
			return read(ctx, n, ref)
		})
	}
	rep.Finished = time.Now().UTC()
	st := n.Stats()
	rep.Stats = &st
	for _, p := range rep.Phases {
		fmt.Printf("%-12s n=%-4d p50=%-8s p95=%-8s p99=%-8s max=%-8s err=%d unknown=%d req=%v up=%dB hedges=%d\n",
			p.Name, p.N, p.P50, p.P95, p.P99, p.Max, p.Errors, p.Unknown, p.Requests, p.Uploaded, p.Hedges)
	}
	if *out != "" {
		if err := os.MkdirAll(*out, 0o755); err != nil {
			return err
		}
		b, _ := json.MarshalIndent(rep, "", "  ")
		if err := os.WriteFile(filepath.Join(*out, "stress.json"), append(b, '\n'), 0o644); err != nil {
			return err
		}
	}
	return nil
}

func req() garden.Request {
	id, err := garden.NewMutationID(time.Now())
	if err != nil {
		panic(err)
	}
	r := garden.Request{ID: id, Schema: 1}
	rand.Read(r.Hash[:])
	return r
}

func update(ctx context.Context, n *garden.Node, ref garden.Ref, k, size int) error {
	v := make([]byte, size)
	rand.Read(v) // incompressible
	_, err := n.Update(ctx, ref, req(), func(tx *garden.Tx) ([]byte, error) {
		_, err := tx.Exec(ctx, "INSERT OR REPLACE INTO kv VALUES (?, ?)", k, v)
		return nil, err
	})
	return err
}

func read(ctx context.Context, n *garden.Node, ref garden.Ref) error {
	_, err := n.Read(ctx, ref, 1, func(v *garden.View) ([]byte, error) {
		var c int
		err := v.Query(ctx, "SELECT count(*) FROM kv", nil, func(r *sql.Rows) error {
			for r.Next() {
				if err := r.Scan(&c); err != nil {
					return err
				}
			}
			return r.Err()
		})
		return strconv.AppendInt(nil, int64(c), 10), err
	})
	return err
}

type stressReport struct {
	Endpoint, Prefix  string
	Started, Finished time.Time
	Ops, Workers      int
	Phases            []phase
	Stats             *garden.Stats
}

type phase struct {
	Name                     string
	N, Workers               int
	Errors, Unknown          int
	ErrorCodes               map[string]int `json:",omitempty"`
	FirstError               string         `json:",omitempty"`
	P50, P95, P99, Max       string
	Requests                 map[string]int // physical requests by class during the phase
	Uploaded                 int64          // request body bytes (decoded) during the phase
	Hedges                   int            // head CAS requests repeating an earlier (key, condition)
	Commits, Snapshots, Comp uint64
}

// phase runs f(0..n-1) on workers goroutines and records latency, outcomes
// and the wire and library counter deltas.
func (r *stressReport) phase(name string, w *wire, node *garden.Node, n, workers int, f func(int) error) {
	before, s0 := w.snapshot(), node.Stats()
	durs := make([]time.Duration, n)
	errs := make([]error, n)
	next := make(chan int)
	var wg sync.WaitGroup
	for range workers {
		wg.Go(func() {
			for i := range next {
				t := time.Now()
				errs[i] = f(i)
				durs[i] = time.Since(t)
			}
		})
	}
	for i := range n {
		next <- i
	}
	close(next)
	wg.Wait()
	after, s1 := w.snapshot(), node.Stats()
	p := phase{Name: name, N: n, Workers: workers, Requests: map[string]int{}, ErrorCodes: map[string]int{},
		Uploaded: after.uploaded - before.uploaded, Hedges: after.hedges - before.hedges,
		Commits: s1.Commits - s0.Commits, Snapshots: s1.Snapshots - s0.Snapshots, Comp: s1.Compactions - s0.Compactions}
	for k, v := range after.requests {
		if d := v - before.requests[k]; d > 0 {
			p.Requests[k] = d
		}
	}
	for _, err := range errs {
		if err == nil {
			continue
		}
		p.Errors++
		var ge *garden.Error
		code := err.Error()
		if errors.As(err, &ge) {
			code = string(ge.Code)
		}
		if p.ErrorCodes[code]++; p.FirstError == "" {
			p.FirstError = err.Error()
		}
		if code == string(garden.CodeOutcomeUnknown) {
			p.Unknown++
		}
	}
	slices.Sort(durs)
	q := func(x float64) string { return durs[min(n-1, int(x*float64(n)))].Round(100 * time.Microsecond).String() }
	p.P50, p.P95, p.P99, p.Max = q(0.50), q(0.95), q(0.99), q(1)
	r.Phases = append(r.Phases, p)
}

// wire counts physical requests by class (head CAS, head GET, object PUT,
// object GET, list, delete), request body bytes, and hedges: head CAS
// requests sent while one with the same key and condition awaits its reply
// (a D12 re-proposal is sent only after the previous reply, so it is not
// counted).
type wire struct {
	base *http.Transport
	mu   sync.Mutex
	c    wireCounts
	inflight map[string]int // head key + condition of head CAS requests awaiting a reply
}

type wireCounts struct {
	requests map[string]int
	uploaded int64
	hedges   int
}

func (w *wire) snapshot() wireCounts {
	w.mu.Lock()
	defer w.mu.Unlock()
	c := w.c
	c.requests = map[string]int{}
	for k, v := range w.c.requests {
		c.requests[k] = v
	}
	return c
}

func (w *wire) RoundTrip(r *http.Request) (*http.Response, error) {
	head := strings.HasSuffix(r.URL.Path, "/head.json")
	class := strings.ToLower(r.Method)
	switch {
	case r.Method == "GET" && r.URL.Query().Has("list-type"):
		class = "list"
	case head:
		class += "-head"
	default:
		class += "-object"
	}
	n := r.ContentLength
	if d, err := strconv.ParseInt(r.Header.Get("X-Amz-Decoded-Content-Length"), 10, 64); err == nil {
		n = d
	}
	w.mu.Lock()
	if w.c.requests == nil {
		w.c.requests = map[string]int{}
	}
	w.c.requests[class]++
	if r.Method == "PUT" {
		w.c.uploaded += max(n, 0)
	}
	k := ""
	if r.Method == "PUT" && head {
		k = r.URL.Path + "|" + r.Header.Get("If-Match") + "|" + r.Header.Get("If-None-Match")
		if w.inflight[k] > 0 {
			w.c.hedges++
		}
		w.inflight[k]++
	}
	w.mu.Unlock()
	resp, err := w.base.RoundTrip(r)
	if k != "" {
		w.mu.Lock()
		w.inflight[k]--
		w.mu.Unlock()
	}
	return resp, err
}
