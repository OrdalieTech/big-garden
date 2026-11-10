package main

import (
	"bytes"
	"cmp"
	"compress/gzip"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"maps"
	"math"
	mrand "math/rand/v2"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	garden "github.com/OrdalieTech/big-garden"
	"github.com/OrdalieTech/big-garden/internal/engine"
	"github.com/OrdalieTech/big-garden/internal/head"
	"github.com/OrdalieTech/big-garden/internal/store"
	"github.com/OrdalieTech/big-garden/internal/storetest"
	"github.com/superfly/ltx"
)

// stress is a scaled-down PLAN §17 Q0/Q1 latency run through the library on
// one node: sequential and concurrent inline (1 KiB) updates, upload-path
// updates, strong reads, and cold activations (Evict, then Read) of a 1 MiB
// and a 16 MiB entity. Every physical request is classified on the wire;
// errors and outcome_unknown are counted, never dropped. Everything is under
// a fresh random prefix and deleted afterwards.
func stress(args []string) error {
	fs := flag.NewFlagSet("stress", flag.ContinueOnError)
	local := fs.Bool("local", false, "use the pinned local S3 fixture (GARDEN_TEST_S3_SERVER: rustfs or minio)")
	remote := fs.Bool("remote", false, "use QUAL_S3_* and AWS_* (missing: blocked_missing_remote_configuration)")
	ops := fs.Int("ops", 200, "operations per latency phase")
	workers := fs.Int("workers", 8, "concurrent clients of the concurrent phase (one entity each)")
	cold := fs.Int("cold", 5, "cold activations per size")
	out := fs.String("out", "", "directory for stress.json")
	hot := fs.String("hot", "", "comma-separated client counts of hot-entity phases (all on one entity, 1 KiB inline updates)")
	hotOnly := fs.Bool("hot-only", false, "run only the create and hot-entity phases")
	delay := fs.Duration("delay", 0, "hold every S3 reply this long before relaying it (injected latency)")
	batch := fs.Int("batch", 1, "Limits.BatchMax")
	var c stressCfg
	profiles := fs.String("profiles", "", "comma-separated PLAN §17.2 profiles (Q0..Q7); empty: the single-node phases above")
	requireRemote := fs.Bool("require-remote", false, "remote, and missing remote configuration (incl. three node hosts) is blocked_missing_remote_configuration")
	nodesCfg := fs.String("nodes-config", os.Getenv("QUAL_NODES_CONFIG"), "comma-separated ssh destinations of three node hosts, each with ~/qual and ~/qual.env (see qual soak -hosts)")
	fs.IntVar(&c.runs, "runs", 1, "runs (seeds) of every profile")
	fs.DurationVar(&c.warmup, "warmup", 5*time.Minute, "warmup of steady profiles (not measured)")
	fs.DurationVar(&c.measure, "measure", 30*time.Minute, "measured duration of every steady phase")
	fs.Int64Var(&c.minOps, "min-operations", 0, "Q3/Q6: keep measuring until this many operations (0: the profile's own minimum)")
	fs.Float64Var(&c.scale, "scale", 1, "multiply every dataset size and operation minimum (below 1: an indicative smoke run, never a qualification)")
	fs.IntVar(&c.maxImage, "max-image", 256<<20, "Q5: largest image size run; larger ones are reported as skipped")
	fs.Uint64Var(&c.seed, "seed", 1, "base seed; run r uses seed+r")
	fs.Uint64Var(&c.maxRequests, "max-requests", 0, "budget: stop (blocked_budget) after this many physical S3 requests; required with --remote")
	fs.DurationVar(&c.outage, "outage", time.Minute, "Q7 storage outage length")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *profiles != "" {
		set := map[string]bool{}
		fs.Visit(func(f *flag.Flag) { set[f.Name] = true })
		for _, f := range []string{"ops", "workers", "hot", "hot-only", "batch"} {
			if set[f] {
				return fmt.Errorf("--%s belongs to the single-node phases, not to --profiles", f)
			}
		}
		c.local, c.remote, c.delay, c.cold = *local, *remote || *requireRemote, *delay, 20
		if set["cold"] {
			c.cold = *cold
		}
		return stressProfiles(&c, strings.Split(*profiles, ","), *out, *nodesCfg, *requireRemote)
	}
	var ep storetest.Endpoint
	var err error
	switch {
	case *remote:
		ep, err = storetest.RemoteFromEnv()
	case *local:
		ep, err = storetest.StartLocal(context.Background())
	default:
		return errors.New("choose --local or --remote")
	}
	if err != nil {
		return err
	}
	ep = ep.Sub("stress")
	ctx := context.Background()
	wire := &wire{base: http.DefaultTransport.(*http.Transport).Clone(), inflight: map[string]int{}, delay: *delay}
	dir, err := os.MkdirTemp("", "stress-cache-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(dir)
	n, err := garden.Open(ctx, garden.Config{S3: ep.Client(&http.Client{Transport: wire}), Bucket: ep.Bucket,
		Prefix: ep.Prefix, NodeID: "stress", CacheDir: dir, Limits: garden.Limits{EntityBytes: 32 << 20, CacheBytes: 2 << 30, BatchMax: *batch}})
	if err != nil {
		return err
	}
	rep := stressReport{Endpoint: ep.URL, Prefix: ep.Prefix, Started: time.Now().UTC(), Ops: *ops, Workers: *workers,
		Delay: delay.String(), BatchMax: *batch}
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
	if *hot != "" {
		hotRef := refs[0]
		for _, f := range strings.Split(*hot, ",") {
			c, err := strconv.Atoi(f)
			if err != nil || c < 1 {
				return fmt.Errorf("bad -hot entry %q", f)
			}
			rep.phase(fmt.Sprintf("hot-%d", c), wire, n, *ops, c, func(i int) error { return update(ctx, n, hotRef, i%64, 1<<10) })
		}
	}
	small := func(i int) error { return update(ctx, n, refs[i%len(refs)], i%64, 1<<10) }
	if *hotOnly {
		goto done
	}
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
done:
	rep.Finished = time.Now().UTC()
	st := n.Stats()
	rep.Stats = &st
	for _, p := range rep.Phases {
		fmt.Printf("%-12s n=%-4d %7.1f/s p50=%-8s p95=%-8s p99=%-8s max=%-8s err=%d unknown=%d req=%v up=%dB down=%dB hedges=%d batches=%v\n",
			p.Name, p.N, p.PerSecond, p.P50, p.P95, p.P99, p.Max, p.Errors, p.Unknown, p.Requests, p.Uploaded, p.Downloaded, p.Hedges, p.Batches)
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
	Delay             string
	BatchMax          int
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
	ObjectUploaded           int64          // the part of Uploaded sent by immutable object PUTs
	Downloaded               int64          // response body bytes (Content-Length) during the phase
	Hedges                   int            // head CAS requests repeating an earlier (key, condition)
	Commits, Snapshots, Comp uint64
	PerSecond                float64        // successful operations per second of wall time
	Batches                  map[int]uint64 // committed mutation publications by member count
}

// phase runs f(0..n-1) on workers goroutines and records latency, outcomes
// and the wire and library counter deltas.
func (r *stressReport) phase(name string, w *wire, node *garden.Node, n, workers int, f func(int) error) {
	before, s0, start := w.snapshot(), node.Stats(), time.Now()
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
	wall := time.Since(start)
	after, s1 := w.snapshot(), node.Stats()
	p := phase{Name: name, N: n, Workers: workers, Requests: map[string]int{}, ErrorCodes: map[string]int{},
		Uploaded: after.uploaded - before.uploaded, Downloaded: after.downloaded - before.downloaded,
		ObjectUploaded: after.objUploaded - before.objUploaded, Hedges: after.hedges - before.hedges,
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
	p.PerSecond = float64(n-p.Errors) / wall.Seconds()
	p.Batches = map[int]uint64{}
	for i := range s1.Batches {
		if d := s1.Batches[i] - s0.Batches[i]; d > 0 {
			p.Batches[i+1] = d
		}
	}
	slices.Sort(durs)
	q := func(x float64) string {
		return durs[min(n-1, int(x*float64(n)))].Round(100 * time.Microsecond).String()
	}
	p.P50, p.P95, p.P99, p.Max = q(0.50), q(0.95), q(0.99), q(1)
	r.Phases = append(r.Phases, p)
}

// wire counts physical requests by class (head CAS, head GET, object PUT,
// object GET, list, delete; a head GET answered 304 Not Modified is
// "get-head-304"), request and response body bytes, and hedges: head CAS
// requests sent while one with the same key and condition awaits its reply
// (a D12 re-proposal is sent only after the previous reply, so it is not
// counted).
type wire struct {
	base     *http.Transport
	mu       sync.Mutex
	c        wireCounts
	inflight map[string]int // head key + condition of head CAS requests awaiting a reply
	delay    time.Duration  // held after every reply, like storetest.Fault.Delay
	down     atomic.Int64   // until this UnixNano every request fails (an injected storage outage)
}

type wireCounts struct {
	requests    map[string]int
	uploaded    int64
	objUploaded int64
	downloaded  int64
	hedges      int
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
	if time.Now().UnixNano() < w.down.Load() {
		if r.Body != nil {
			r.Body.Close()
		}
		return nil, errors.New("injected storage outage")
	}
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
		if !head {
			w.c.objUploaded += max(n, 0)
		}
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
	if err == nil {
		w.mu.Lock()
		if resp.StatusCode == http.StatusNotModified && class == "get-head" {
			w.c.requests[class]--
			w.c.requests["get-head-304"]++
		}
		w.c.downloaded += max(resp.ContentLength, 0)
		w.mu.Unlock()
	}
	if w.delay > 0 {
		time.Sleep(w.delay)
	}
	if k != "" {
		w.mu.Lock()
		if w.inflight[k]--; w.inflight[k] == 0 {
			delete(w.inflight, k) // every CAS has its own If-Match: keep only those in flight
		}
		w.mu.Unlock()
	}
	return resp, err
}

// The PLAN §17.2 profiles (`qual stress --profiles=Q0,...`). Q1-Q5 and Q7
// drive qual soak-node processes (local, or on -nodes-config hosts over ssh)
// through the soak's caller-side router; Q0 and the Q2 amplification run in
// this process; Q6 is qual soak. Every profile and run writes
// run-<r>/<profile>.json; summary.json aggregates the gates over runs and
// env.json is the §17.1 environment lock. Each phase records latency
// distributions by operation (from the intended start, so open-loop queueing
// counts), outcomes (acknowledged, unknown, failed, shed by an exhausted
// open-loop generator: never dropped silently), a per-second timeline, the
// library counters summed over the nodes (Stats: physical S3 attempts,
// failures, bytes and time by class), PUTs per mutation, peak node
// resources, CPU seconds, and a concurrent direct-provider GET/PUT probe
// (§18.2). A run with --scale below 1, fewer than three runs, or shorter
// warmup/measure than §17.2 is a smoke run: indicative, never a pass.

var stressNames = []string{"Q0", "Q1", "Q2", "Q3", "Q4", "Q5", "Q6", "Q7"}

// openFraction is the open-loop offered load as a fraction of the
// closed-loop throughput just measured: healthy load below saturation.
const openFraction = 0.5

type stressCfg struct {
	ep                      storetest.Endpoint
	out                     string   // the current run's directory
	local, remote           bool     //
	hosts                   []string // ssh node hosts; [""]: local processes
	runs, cold, maxImage    int
	scale                   float64
	warmup, measure, outage time.Duration
	delay                   time.Duration
	minOps                  int64
	seed, maxRequests, used uint64 // used: physical S3 requests so far
}

var errBudget = errors.New("blocked_budget: --max-requests reached")

type gate struct {
	ID, Status string // pass, fail, provider_blocked, info
	Value      float64
	Target     string
	Detail     string    `json:",omitempty"`
	Values     []float64 `json:",omitempty"` // summary: one per run
	Evidence   []string  `json:",omitempty"` // summary: profile files
}

type stressSummary struct {
	Verdict           string // pass, fail, blocked_missing_remote_configuration, blocked_budget
	Smoke             bool
	Profiles          []string
	Runs              int
	Scale             float64
	Remote            bool
	Delay             string
	Warmup, Measure   string
	Started, Finished time.Time
	EstimatedRequests uint64
	PhysicalRequests  uint64
	Gates             []*gate
	Files             []string
	Failures          []string `json:",omitempty"`
	Error             string   `json:",omitempty"`
}

type profileOut struct {
	Profile           string
	Run               int
	Seed              uint64
	Scale             float64
	Remote            bool
	Delay             string
	Nodes             int
	Started, Finished time.Time
	Params            map[string]any
	Phases            []*mphase      `json:",omitempty"`
	InProcess         []phase        `json:",omitempty"` // single-node phases on the wire counter (Q2 amplification)
	Detail            map[string]any `json:",omitempty"`
	Gates             []gate
	Failures          []string `json:",omitempty"`
}

type mphase struct {
	Name                  string
	Measured              bool
	Seconds               float64
	Offered               float64 `json:",omitempty"` // open loop: operations per second
	Ops                   map[string]*lstat
	Throughput, Mutations float64 // acknowledged operations and writes per second
	ErrorRate             float64 // (failed + shed) / issued; unknown outcomes are separate
	Unknown, Shed         int64
	MaxFailedMs           float64 // longest failed or unknown operation: admission stays bounded
	Codes                 map[string]int64
	S3                    map[string]s3class // get, put (incl. CAS), list, delete
	PutsPerMutation       float64            // successful PUTs incl. CAS and maintenance / committed mutations
	FailedPutsPerMutation float64
	UploadedPerMutation   float64
	MeanBatch             float64 // committed mutations per publication
	CPUSeconds            float64 // all node processes
	Peak                  peak
	Provider              map[string]*lstat // concurrent direct-provider 4 KiB PUT and GET
	Library               garden.Stats      // counter deltas summed over the nodes (gauges zero)
	Timeline              []tick            // per second of the phase
	unk                   []shop            // unknown writes, for resolution
}

type lstat struct {
	N, OK, Unknown, Failed, Shed int64
	P50, P95, P99, Max           float64 // ms, acknowledged operations
}

type s3class struct {
	Attempts, Failures, UploadedBytes, DownloadedBytes uint64
	Seconds                                            float64
}

type peak struct {
	Samples                                                 int
	RSS, HeapAlloc, CacheDirBytes, CacheBytes, StagingBytes int64
	ResidentEntities, WarmBytes                             int64
	Goroutines, FDs                                         int
}

type tick struct{ OK, Unknown, Failed int64 }

func stressProfiles(c *stressCfg, names []string, out, nodesCfg string, requireRemote bool) error {
	if out == "" {
		return errors.New("--out is required with --profiles")
	}
	for _, n := range names {
		if !slices.Contains(stressNames, n) {
			return fmt.Errorf("unknown profile %q", n)
		}
	}
	out, _ = filepath.Abs(out)
	if err := os.MkdirAll(out, 0o755); err != nil {
		return err
	}
	sum := stressSummary{Profiles: names, Runs: c.runs, Scale: c.scale, Remote: c.remote, Delay: c.delay.String(),
		Warmup: c.warmup.String(), Measure: c.measure.String(), Started: time.Now().UTC(),
		Smoke: c.scale < 1 || c.runs < 3 || c.warmup < 5*time.Minute || c.measure < 30*time.Minute}
	write := func(verdict string, err error) error {
		sum.Verdict, sum.Finished, sum.PhysicalRequests = verdict, time.Now().UTC(), c.used
		if err != nil {
			sum.Error = err.Error()
		}
		b, _ := json.MarshalIndent(sum, "", "  ")
		os.WriteFile(filepath.Join(out, "summary.json"), append(b, '\n'), 0o644)
		fmt.Printf("stress: %s, smoke %v, %d gates, %d physical requests\n", verdict, sum.Smoke, len(sum.Gates), c.used)
		if verdict != "pass" {
			return fmt.Errorf("%s %s", verdict, strings.Join(append(sum.Failures, sum.Error), "; "))
		}
		return nil
	}
	var err error
	switch {
	case c.remote:
		c.ep, err = storetest.RemoteFromEnv()
		if nodesCfg != "" {
			c.hosts = strings.Split(nodesCfg, ",")
		}
		if err == nil && requireRemote && len(slices.Compact(slices.Sorted(slices.Values(c.hosts)))) < 3 {
			err = fmt.Errorf("%w: --nodes-config needs three distinct node hosts", storetest.ErrBlockedRemote)
		}
		if err == nil && c.maxRequests == 0 {
			return write("blocked_budget", errors.New("--max-requests is required with --remote (PLAN §20.4 budget preflight)"))
		}
	case c.local:
		c.ep, err = storetest.StartLocal(context.Background())
	default:
		return errors.New("choose --local or --remote")
	}
	if errors.Is(err, storetest.ErrBlockedRemote) {
		return write(storetest.ErrBlockedRemote.Error(), err)
	}
	if err != nil {
		return err
	}
	if len(c.hosts) == 0 {
		c.hosts = []string{""}
	} else if len(slices.Compact(slices.Sorted(slices.Values(c.hosts)))) < 3 {
		return errors.New("--nodes-config needs three distinct hosts: a multi-host run on fewer is not one")
	}
	c.ep = c.ep.Sub("stress")
	env := envLock(c, out)
	b, _ := json.MarshalIndent(env, "", "  ")
	if err := os.WriteFile(filepath.Join(out, "env.json"), append(b, '\n'), 0o644); err != nil {
		return err
	}
	sum.EstimatedRequests = c.estimate(names)
	fmt.Printf("budget preflight: about %d physical S3 requests (limit %d; 0: none)\n", sum.EstimatedRequests, c.maxRequests)
	byID := map[string]*gate{}
	for r := 1; r <= c.runs; r++ {
		c.out = filepath.Join(out, fmt.Sprintf("run-%d", r))
		for _, name := range names {
			o := &profileOut{Profile: name, Run: r, Seed: c.seed + uint64(r), Scale: c.scale, Remote: c.remote,
				Delay: c.delay.String(), Started: time.Now().UTC()}
			fmt.Printf("== %s run %d\n", name, r)
			err := c.profile(o)
			if err != nil {
				o.Failures = append(o.Failures, err.Error())
			}
			o.Finished = time.Now().UTC()
			if c.delay > 0 { // the targets are for the unmodified fixture: the delayed profile is reported, not gated
				for i := range o.Gates {
					if !strings.HasPrefix(o.Gates[i].ID, "18.1/") {
						o.Gates[i].Status = "info"
					}
				}
			}
			file := filepath.Join(c.out, name+".json")
			os.MkdirAll(c.out, 0o755)
			b, werr := json.MarshalIndent(o, "", "  ")
			if werr == nil {
				werr = os.WriteFile(file, append(b, '\n'), 0o644)
			}
			if werr != nil {
				return werr
			}
			rel, _ := filepath.Rel(out, file)
			sum.Files = append(sum.Files, rel)
			for _, f := range o.Failures {
				sum.Failures = append(sum.Failures, fmt.Sprintf("%s run %d: %s", name, r, f))
			}
			for _, g := range o.Gates {
				fmt.Printf("   %-40s %-16s %s (%s)\n", g.ID, g.Status, g.Detail, g.Target)
				a := byID[g.ID]
				if a == nil {
					a = &gate{ID: g.ID, Status: g.Status, Target: g.Target, Detail: g.Detail}
					byID[g.ID] = a
					sum.Gates = append(sum.Gates, a)
				}
				a.Values, a.Evidence = append(a.Values, g.Value), append(a.Evidence, rel)
				a.Value, a.Detail = g.Value, g.Detail
				if rank(g.Status) > rank(a.Status) {
					a.Status = g.Status
				}
			}
			if errors.Is(err, errBudget) {
				return write("blocked_budget", err)
			}
		}
	}
	verdict := "pass"
	for _, g := range sum.Gates {
		if g.Status == "fail" {
			verdict = "fail"
		}
	}
	if len(sum.Failures) > 0 {
		verdict = "fail"
	}
	return write(verdict, nil)
}

// rank orders gate statuses: an aggregate over runs takes the worst.
func rank(s string) int { return slices.Index([]string{"info", "pass", "provider_blocked", "fail"}, s) }

func (c *stressCfg) profile(o *profileOut) error {
	switch o.Profile {
	case "Q0":
		return c.q0(o)
	case "Q1":
		return c.q1(o)
	case "Q2":
		return c.q2(o)
	case "Q3":
		return c.q3(o)
	case "Q4":
		return c.q4(o)
	case "Q5":
		return c.q5(o)
	case "Q6":
		return c.q6(o)
	}
	return c.q7(o)
}

// count scales a dataset size (at least 8).
func (c *stressCfg) count(n int) int { return max(8, int(float64(n)*c.scale)) }

// estimate is the budget preflight: physical requests at the remote
// throughput targets (about three per logical operation) for every
// measured and warmup phase, plus creation.
func (c *stressCfg) estimate(names []string) uint64 {
	phases := map[string]float64{"Q1": 4, "Q2": 7, "Q3": 2, "Q4": 3, "Q7": 2}
	var ops float64
	for _, n := range names {
		ops += phases[n] * (c.warmup + c.measure).Seconds() * 2000
		ops += float64(map[string]int{"Q1": 2 * c.count(10000), "Q2": c.count(10000), "Q3": c.count(100000), "Q4": c.count(1000), "Q7": c.count(10000)}[n]) * 3
	}
	return uint64(ops*3) * uint64(c.runs)
}

func (c *stressCfg) tgt(local, remote float64) float64 {
	if c.remote {
		return remote
	}
	return local
}

// latGate checks label's p50/p95/p99 in ph (ms). On the remote store a
// failing concurrent provider baseline makes it provider_blocked (§18.2).
func (c *stressCfg) latGate(id string, ph *mphase, label string, local, remote [3]float64) gate {
	t := local
	if c.remote {
		t = remote
	}
	g := gate{ID: id, Target: fmt.Sprintf("p50/p95/p99 <= %g/%g/%g ms", t[0], t[1], t[2]), Status: "fail", Detail: "no samples"}
	if t[0] == 0 { // no p50 target
		t[0], g.Target = math.Inf(1), fmt.Sprintf("p95/p99 <= %g/%g ms", t[1], t[2])
	}
	if ph == nil || ph.Ops[label] == nil || ph.Ops[label].OK == 0 {
		return g
	}
	l := ph.Ops[label]
	g.Value, g.Detail = l.P99, fmt.Sprintf("%s %s: p50 %.1f p95 %.1f p99 %.1f ms (n=%d, failed %d, unknown %d)", ph.Name, label, l.P50, l.P95, l.P99, l.OK, l.Failed, l.Unknown)
	if l.P50 <= t[0] && l.P95 <= t[1] && l.P99 <= t[2] {
		g.Status = "pass"
	}
	if c.remote {
		if ok, why := providerOK(ph.Provider); !ok {
			g.Status, g.Detail = "provider_blocked", g.Detail+"; "+why
		}
	}
	return g
}

// providerOK is the §18.2 healthy-provider condition on the concurrent
// direct baseline: small GET p95 <= 100 / p99 <= 250 ms, PUT p95 <= 150 /
// p99 <= 500 ms.
func providerOK(p map[string]*lstat) (bool, string) {
	g, u := p["get-4k"], p["put-4k"]
	if g == nil || u == nil || g.OK == 0 || u.OK == 0 {
		return false, "no provider baseline samples"
	}
	why := fmt.Sprintf("provider GET p95/p99 %.0f/%.0f ms, PUT %.0f/%.0f ms", g.P95, g.P99, u.P95, u.P99)
	return g.P95 <= 100 && g.P99 <= 250 && u.P95 <= 150 && u.P99 <= 500, why
}

func cmpGate(id string, v, target float64, atLeast bool, unit, detail string) gate {
	g := gate{ID: id, Value: v, Detail: detail, Status: "fail"}
	if atLeast {
		g.Target = fmt.Sprintf(">= %g %s", target, unit)
		if v >= target {
			g.Status = "pass"
		}
	} else {
		g.Target = fmt.Sprintf("<= %g %s", target, unit)
		if v <= target {
			g.Status = "pass"
		}
	}
	if math.IsNaN(v) || math.IsInf(v, 0) { // JSON has neither
		g.Value, g.Status, g.Detail = -1, "fail", detail+" (no measurement)"
	}
	return g
}

// load is one generator: its clients pick keys (uniform or Zipf s=1.1) and
// do a strong read with probability read, else a write of size bytes.
type load struct {
	label        string // operation label prefix
	keys         []string
	read         float64
	size         int
	zipf         bool
	compressible bool
	clients      int
	rate         float64 // > 0: open loop at this many operations per second
}

func (l *load) next(p *prun, rng *mrand.Rand, z *mrand.Zipf) (string, sReq, string) {
	i := 0
	if z != nil {
		i = int(z.Uint64())
	} else {
		i = rng.IntN(len(l.keys))
	}
	key := l.keys[i]
	r := sReq{Inc: p.s.refs[key]}
	if rng.Float64() < l.read {
		r.Op = "read"
		return l.label + "read", r, key
	}
	r.Op, r.ID, r.Size, r.Compressible = "write", newID(), l.size, l.compressible
	return l.label + "write", r, key
}

func (l *load) rng(seed uint64, client int) (*mrand.Rand, *mrand.Zipf) {
	rng := mrand.New(mrand.NewPCG(seed, uint64(client)))
	if l.zipf {
		return rng, mrand.NewZipf(rng, 1.1, 1, uint64(len(l.keys)-1))
	}
	return rng, nil
}

// prun is one profile's set of node processes on one fresh prefix.
type prun struct {
	c       *stressCfg
	o       *profileOut
	s       *soakRun
	hf      *os.File
	probe   *store.S3
	mu      sync.Mutex
	gone    garden.Stats // counters of node processes that ended
	goneCPU float64
	ledger  bool  // Q7: record history and acknowledged writes for the ledger check
	err     error // sticky: the budget is exhausted
}

func (c *stressCfg) start(o *profileOut, name string, nodes int, extra ...string) (*prun, error) {
	dir := filepath.Join(c.out, name)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	ep := c.ep.Sub(name)
	s := &soakRun{ep: ep, out: dir, ring: 1024, t0: time.Now(), nodes: map[string]*snode{}, hosts: c.hosts, refs: map[string]string{},
		lat: map[string][]time.Duration{}, counts: map[string]map[string]int64{}, codes: map[string]int64{}, pending: map[string][]ack{},
		recent: map[string][]ack{}, unknown: map[string][]unk{}, gates: map[string]*sync.RWMutex{}, windows: map[string]*swindow{},
		halt: func() {}, routeTimeout: time.Minute, extra: append([]string{"-delay", c.delay.String()}, extra...)}
	s.self, _ = os.Executable()
	var err error
	if s.logf, err = os.OpenFile(filepath.Join(dir, "stress.log"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644); err != nil {
		return nil, err
	}
	p := &prun{c: c, o: o, s: s, probe: store.NewS3(ep.Client(nil), ep.Bucket, ep.Prefix+"/probe", time.Minute)}
	if p.hf, err = os.Create(filepath.Join(dir, "history.jsonl.gz")); err != nil {
		return nil, err
	}
	s.hz = gzip.NewWriter(p.hf)
	s.hist = json.NewEncoder(s.hz)
	o.Nodes = max(o.Nodes, nodes)
	for i := range nodes {
		if err := p.add(fmt.Sprintf("n%d", i)); err != nil {
			p.close()
			return nil, err
		}
	}
	return p, nil
}

// add starts a node process and waits until it answers (Open succeeded).
func (p *prun) add(name string) error {
	if err := p.s.start(name, true); err != nil {
		return err
	}
	if _, ok := p.s.get(name).do(sReq{Op: "stats"}, 2*time.Minute); !ok {
		return fmt.Errorf("node %s did not start: see %s", name, filepath.Join(p.s.out, "node-"+name+".stderr"))
	}
	return nil
}

// kill SIGKILLs a node after saving its counters, optionally wiping its cache.
func (p *prun) kill(name string, wipe bool) {
	m := p.s.get(name)
	if m == nil {
		return
	}
	if r, ok := m.do(sReq{Op: "stats"}, 10*time.Second); ok && r.Proc != nil {
		p.mu.Lock()
		addCounters(&p.gone, r.Proc.Stats, false)
		p.goneCPU += r.Proc.CPUSeconds
		p.mu.Unlock()
	}
	m.kill()
	if wipe {
		m.wipe()
	}
	p.s.mu.Lock()
	delete(p.s.nodes, name)
	p.s.mu.Unlock()
}

func (p *prun) names() []string {
	p.s.mu.Lock()
	defer p.s.mu.Unlock()
	return slices.Sorted(maps.Keys(p.s.nodes))
}

func (p *prun) close() {
	for _, n := range p.names() {
		p.kill(n, false)
	}
	k, err := cleanup(context.Background(), store.NewS3(p.s.ep.Client(nil), p.s.ep.Bucket, p.s.ep.Prefix, time.Minute))
	p.s.logx("cleanup: %d objects deleted under %s (%v)", k, p.s.ep.Prefix, err)
	os.RemoveAll(filepath.Join(p.s.out, "cache"))
	for _, h := range p.c.hosts {
		if h != "" {
			exec.Command("ssh", h, "--", "rm -rf $HOME/soak-cache").Run()
		}
	}
	p.s.hz.Close()
	p.hf.Close()
	p.s.logf.Close()
}

// total is the counters and CPU seconds of every node process so far, and
// the live processes' samples.
func (p *prun) total() (garden.Stats, float64, []procStats) {
	p.mu.Lock()
	t, cpu := p.gone, p.goneCPU
	p.mu.Unlock()
	var live []procStats
	for _, n := range p.names() {
		if m := p.s.get(n); m != nil {
			if r, ok := m.do(sReq{Op: "stats"}, 30*time.Second); ok && r.Proc != nil {
				addCounters(&t, r.Proc.Stats, false)
				cpu += r.Proc.CPUSeconds
				live = append(live, *r.Proc)
			}
		}
	}
	return t, cpu, live
}

// addCounters adds (or subtracts) the cumulative counters of s to d:
// uint64 fields and the storage call times; gauges are left alone.
func addCounters(d *garden.Stats, s garden.Stats, sub bool) {
	var walk func(d, s reflect.Value, storage bool)
	walk = func(d, s reflect.Value, storage bool) {
		switch d.Kind() {
		case reflect.Struct:
			for i := range d.NumField() {
				walk(d.Field(i), s.Field(i), storage || d.Type().Field(i).Name == "Storage")
			}
		case reflect.Array:
			for i := range d.Len() {
				walk(d.Index(i), s.Index(i), storage)
			}
		case reflect.Uint64:
			if sub {
				d.SetUint(d.Uint() - s.Uint())
			} else {
				d.SetUint(d.Uint() + s.Uint())
			}
		case reflect.Int64:
			if storage { // Storage[i].Time
				if sub {
					d.SetInt(d.Int() - s.Int())
				} else {
					d.SetInt(d.Int() + s.Int())
				}
			}
		}
	}
	walk(reflect.ValueOf(d).Elem(), reflect.ValueOf(s), false)
}

// meter collects one phase's operations.
type meter struct {
	mu      sync.Mutex
	t0      time.Time
	lat     map[string][]time.Duration
	cnt     map[string]*[4]int64 // ack, unknown, failed, shed
	codes   map[string]int64
	tl      []tick
	unk     []shop // unknown writes, for resolution
	maxFail time.Duration
}

func (m *meter) add(label string, o shop, d time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	c := m.cnt[label]
	if c == nil {
		c = new([4]int64)
		m.cnt[label] = c
	}
	sec := int(time.Since(m.t0) / time.Second)
	for len(m.tl) <= sec {
		m.tl = append(m.tl, tick{})
	}
	switch o.Class {
	case "ack":
		c[0]++
		m.lat[label] = append(m.lat[label], d)
		m.tl[sec].OK++
	case "unknown":
		c[1]++
		m.tl[sec].Unknown++
		m.maxFail = max(m.maxFail, d)
		if o.Op == "write" {
			m.unk = append(m.unk, o)
		}
	default:
		c[2]++
		m.tl[sec].Failed++
		m.maxFail = max(m.maxFail, d)
	}
	if o.Code != "" {
		m.codes[label+":"+o.Code]++
	}
}

// direct records an operation sent straight to a node (no router).
func (m *meter) direct(label string, err error, d time.Duration) {
	o := shop{Class: "ack"}
	if err != nil {
		o.Class, o.Code = "failed", err.Error()
	}
	m.add(label, o, d)
}

func (m *meter) shed(label string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.cnt[label] == nil {
		m.cnt[label] = new([4]int64)
	}
	m.cnt[label][3]++
}

// quant summarizes acknowledged latencies.
func quant(d []time.Duration) *lstat {
	l := &lstat{N: int64(len(d)), OK: int64(len(d))}
	if len(d) == 0 {
		return l
	}
	slices.Sort(d)
	q := func(x float64) float64 { return float64(d[min(len(d)-1, int(x*float64(len(d))))]) / 1e6 }
	l.P50, l.P95, l.P99, l.Max = q(.5), q(.95), q(.99), q(1)
	return l
}

// phase runs body as one measured interval: counters before and after,
// node samples every 5 s, and the concurrent provider probe.
func (p *prun) phase(name string, measured bool, body func(m *meter)) *mphase {
	m := &meter{t0: time.Now(), lat: map[string][]time.Duration{}, cnt: map[string]*[4]int64{}, codes: map[string]int64{}}
	ph := &mphase{Name: name, Measured: measured, Ops: map[string]*lstat{}}
	if p.err != nil {
		return ph
	}
	s0, cpu0, _ := p.total()
	stop := make(chan struct{})
	var bg sync.WaitGroup
	bg.Go(func() { p.s.metrics(stop, 5*time.Second) })
	bg.Go(func() { ph.Provider = p.probeLoop(stop) })
	body(m)
	el := time.Since(m.t0)
	close(stop)
	bg.Wait()
	s1, cpu1, live := p.total()
	ph.Seconds, ph.Codes, ph.Timeline, ph.MaxFailedMs, ph.CPUSeconds = el.Seconds(), m.codes, m.tl, float64(m.maxFail)/1e6, cpu1-cpu0
	ph.unk = m.unk
	var issued, failed int64
	for label, c := range m.cnt {
		l := quant(m.lat[label])
		l.N, l.Unknown, l.Failed, l.Shed = c[0]+c[1]+c[2]+c[3], c[1], c[2], c[3]
		ph.Ops[label] = l
		issued, failed, ph.Unknown, ph.Shed = issued+l.N, failed+c[2], ph.Unknown+c[1], ph.Shed+c[3]
		ph.Throughput += float64(c[0]) / el.Seconds()
		if strings.HasSuffix(label, "write") {
			ph.Mutations += float64(c[0]) / el.Seconds()
		}
	}
	if issued > 0 {
		ph.ErrorRate = float64(failed+ph.Shed) / float64(issued)
	}
	var d garden.Stats
	addCounters(&d, s1, false)
	addCounters(&d, s0, true)
	ph.Library, ph.S3 = d, map[string]s3class{}
	var reqs uint64
	for i, n := range []string{"get", "put", "list", "delete"} {
		st := d.Storage[i]
		ph.S3[n] = s3class{st.Attempts, st.Failures, st.UploadedBytes, st.DownloadedBytes, st.Time.Seconds()}
		reqs += st.Attempts
	}
	if d.Mutations > 0 {
		put := d.Storage[1]
		ph.PutsPerMutation = float64(put.Attempts-put.Failures) / float64(d.Mutations)
		ph.FailedPutsPerMutation = float64(put.Failures) / float64(d.Mutations)
		ph.UploadedPerMutation = float64(put.UploadedBytes) / float64(d.Mutations)
	}
	if d.Commits > 0 {
		ph.MeanBatch = float64(d.Mutations) / float64(d.Commits)
	}
	ph.Peak = p.peak(m.t0, time.Now(), live)
	p.o.Phases = append(p.o.Phases, ph)
	fmt.Printf("   %-28s %6.1fs %8.1f ops/s %8.1f writes/s err %.4f unk %d shed %d puts/mut %.2f rss %dMiB |%s\n", name, ph.Seconds,
		ph.Throughput, ph.Mutations, ph.ErrorRate, ph.Unknown, ph.Shed, ph.PutsPerMutation, ph.Peak.RSS>>20, opsLine(ph.Ops))
	if p.c.used += reqs; p.c.maxRequests > 0 && p.c.used > p.c.maxRequests {
		p.err = errBudget
	}
	return ph
}

func opsLine(ops map[string]*lstat) string {
	var b strings.Builder
	for _, k := range slices.Sorted(maps.Keys(ops)) {
		l := ops[k]
		fmt.Fprintf(&b, " %s n=%d p50/p99=%.1f/%.1fms", k, l.N, l.P50, l.P99)
	}
	return b.String()
}

func (p *prun) peak(t0, t1 time.Time, live []procStats) peak {
	lo, hi := t0.Sub(p.s.t0).Seconds(), t1.Sub(p.s.t0).Seconds()
	var k peak
	p.s.smu.Lock()
	pts := slices.Clone(p.s.series)
	p.s.smu.Unlock()
	for _, x := range pts {
		if x.Alive && x.T >= lo && x.T <= hi {
			live = append(live, x.Proc)
		}
	}
	for _, x := range live {
		k.Samples++
		k.RSS, k.HeapAlloc, k.CacheDirBytes = max(k.RSS, x.RSS), max(k.HeapAlloc, x.HeapAlloc), max(k.CacheDirBytes, x.CacheDirBytes)
		k.CacheBytes, k.StagingBytes = max(k.CacheBytes, x.Stats.CacheBytes), max(k.StagingBytes, x.Stats.StagingBytes)
		k.ResidentEntities, k.WarmBytes = max(k.ResidentEntities, x.Stats.ResidentEntities), max(k.WarmBytes, x.Stats.WarmBytes)
		k.Goroutines, k.FDs = max(k.Goroutines, x.Goroutines), max(k.FDs, x.FDs)
	}
	return k
}

// probeLoop is the concurrent direct-provider baseline: a single-attempt
// 4 KiB create PUT and a GET of it every 250 ms on its own client.
func (p *prun) probeLoop(stop chan struct{}) map[string]*lstat {
	ctx := context.Background()
	body := make([]byte, 4<<10)
	rand.Read(body)
	var put, get []time.Duration
	var perr, gerr int64
	t := time.NewTicker(250 * time.Millisecond)
	defer t.Stop()
	for i := 0; ; i++ {
		select {
		case <-stop:
			pl, gl := quant(put), quant(get)
			pl.Failed, gl.Failed = perr, gerr
			pl.N, gl.N = pl.OK+perr, gl.OK+gerr
			return map[string]*lstat{"put-4k": pl, "get-4k": gl}
		case <-t.C:
		}
		k := fmt.Sprintf("%d-%d", time.Now().UnixNano(), i)
		t0 := time.Now()
		if _, err := p.probe.PutCAS(ctx, k, body, ""); err != nil {
			perr++
			continue
		}
		put = append(put, time.Since(t0))
		t0 = time.Now()
		if _, err := p.probe.Get(ctx, k, "", 8<<10); err != nil {
			gerr++
		} else {
			get = append(get, time.Since(t0))
		}
	}
}

// do runs one routed operation and records it.
func (p *prun) do(m *meter, c *sclient, label string, r sReq, key string, start time.Time) {
	o := c.run(r, key)
	d := time.Since(start)
	m.add(label, o, d)
	if !p.ledger {
		return
	}
	s := p.s
	s.record(o)
	s.smu.Lock()
	defer s.smu.Unlock()
	s.ops++
	if s.counts[label] == nil {
		s.counts[label] = map[string]int64{}
	}
	s.counts[label][o.Class]++
	if o.Class == "ack" {
		s.lat[label] = append(s.lat[label], d)
	}
	if r.Op == "write" && o.Class == "ack" {
		n, err := strconv.ParseInt(o.Value, 10, 64)
		if err != nil {
			s.failures = append(s.failures, fmt.Sprintf("write %s on %s: bad value %q", o.ID, key, o.Value))
			return
		}
		s.pending[key] = append(s.pending[key], ack{o.ID, n, time.Now(), o.Replayed})
	}
}

func (p *prun) client(id int) *sclient {
	return &sclient{id: id, s: p.s, owner: map[string]string{}, suspect: map[string]time.Time{}}
}

// closed runs bounded-concurrency generators until d has passed and at
// least minOps operations were issued.
func (p *prun) closed(m *meter, d time.Duration, minOps int64, loads ...load) {
	until := time.Now().Add(d)
	seed := p.o.Seed*1000 + uint64(len(p.o.Phases))
	var issued atomic.Int64
	var wg sync.WaitGroup
	for li, l := range loads {
		for ci := range l.clients {
			wg.Go(func() {
				id := li*10000 + ci
				c := p.client(id)
				rng, z := l.rng(seed, id)
				for p.err == nil && (time.Now().Before(until) || issued.Load() < minOps) {
					issued.Add(1)
					label, r, key := l.next(p, rng, z)
					p.do(m, c, label, r, key, time.Now())
				}
			})
		}
	}
	wg.Wait()
}

// open runs paced open-loop generators for d: an operation is due every
// 1/rate; it is shed (and counted) when all of the load's clients are busy.
// Latency counts from the due time, so queueing is never omitted.
func (p *prun) open(m *meter, d time.Duration, loads ...load) {
	until := time.Now().Add(d)
	seed := p.o.Seed*1000 + uint64(len(p.o.Phases))
	var wg sync.WaitGroup
	var lw sync.WaitGroup
	for li, l := range loads {
		lw.Go(func() {
			pool := make(chan *sclient, l.clients)
			for ci := range l.clients {
				pool <- p.client(li*10000 + ci)
			}
			rng, z := l.rng(seed, li*10000+9999)
			every := time.Duration(float64(time.Second) / max(l.rate, 1e-3))
			for due := time.Now(); due.Before(until); due = due.Add(every) {
				time.Sleep(time.Until(due))
				label, r, key := l.next(p, rng, z)
				select {
				case c := <-pool:
					wg.Go(func() {
						p.do(m, c, label, r, key, due)
						pool <- c
					})
				default:
					m.shed(label)
				}
			}
		})
	}
	lw.Wait()
	wg.Wait()
}

// create creates keys through the router (64 at a time) and fills each with
// fill bytes; the phase is the initial creation cost.
func (p *prun) create(keys []string, fill int) error {
	var mu sync.Mutex
	var first error
	p.phase("create", false, func(m *meter) {
		next := make(chan string)
		var wg sync.WaitGroup
		for w := range 64 {
			wg.Go(func() {
				c := p.client(-100 - w)
				for k := range next {
					var o shop
					t := time.Now()
					for try := 0; try < 5 && o.Class != "ack"; try++ {
						o = c.run(sReq{Op: "create", ID: newID()}, k)
					}
					m.add("create", o, time.Since(t))
					if o.Class == "ack" {
						mu.Lock()
						p.s.refs[k] = o.Inc
						mu.Unlock()
					}
					if o.Class == "ack" && fill > 0 {
						t = time.Now()
						o = c.run(sReq{Op: "fill", ID: newID(), Inc: o.Inc, Size: fill}, k)
						m.add("fill", o, time.Since(t))
					}
					if o.Class != "ack" {
						mu.Lock()
						first = cmp.Or(first, fmt.Errorf("create %s: %s %s %s", k, o.Class, o.Code, o.Err))
						mu.Unlock()
					}
				}
			})
		}
		for _, k := range keys {
			next <- k
		}
		close(next)
		wg.Wait()
	})
	return cmp.Or(p.err, first)
}

// setup creates and fills keys, then runs the warmup on warm; it returns
// the warmup phase (nil without warmup).
func (p *prun) setup(keys []string, fill int, warm ...load) (*mphase, error) {
	if err := p.create(keys, fill); err != nil {
		return nil, err
	}
	if p.c.warmup <= 0 {
		return nil, nil
	}
	return p.phase("warmup", false, func(m *meter) { p.closed(m, p.c.warmup, 0, warm...) }), p.err
}

func keyset(prefix string, n int) []string {
	k := make([]string, n)
	for i := range k {
		k[i] = fmt.Sprintf("%s%06d", prefix, i)
	}
	return k
}

func nodeArgs(entity, cache int64, batch int) []string {
	return []string{"-entity-bytes", strconv.FormatInt(entity, 10), "-cache-bytes", strconv.FormatInt(cache, 10), "-batch", strconv.Itoa(batch)}
}

// rssGate is §18.3: peak consumer RSS per node at most 512 MiB (review
// ceiling 1 GiB) with 256 resident entities.
func rssGate(phs ...*mphase) gate {
	var rss int64
	for _, ph := range phs {
		if ph != nil {
			rss = max(rss, ph.Peak.RSS)
		}
	}
	return cmpGate("18.3/rss_q1_q2", float64(rss)/(1<<20), 512, false, "MiB", fmt.Sprintf("peak node RSS %d MiB (review ceiling 1024)", rss>>20))
}

// Q1: mixed state on three nodes, then on one for the scaling gate.
func (c *stressCfg) q1(o *profileOut) error {
	n := c.count(10000)
	o.Params = map[string]any{"entities": n, "entity_bytes": 64 << 10, "read_fraction": 0.8, "update_bytes": 1 << 10,
		"clients": 256, "popularity": "uniform", "open_loop_fraction": openFraction, "batch_max": "default"}
	var closed [2]*mphase
	var open *mphase
	for i, nodes := range []int{3, 1} {
		p, err := c.start(o, fmt.Sprintf("q1-%dn", nodes), nodes, nodeArgs(8<<20, 8<<30, 0)...)
		if err != nil {
			return err
		}
		keys := keyset("e", n)
		l := load{keys: keys, read: .8, size: 1 << 10, clients: 256}
		if _, err := p.setup(keys, 60<<10, l); err != nil {
			p.close()
			return err
		}
		closed[i] = p.phase(fmt.Sprintf("mixed-closed-%dn", nodes), true, func(m *meter) { p.closed(m, c.measure, 0, l) })
		if nodes == 3 {
			l.rate = openFraction * closed[0].Throughput
			open = p.phase("mixed-open-3n", true, func(m *meter) { p.open(m, c.measure, l) })
			open.Offered = l.rate
		}
		p.close()
		if p.err != nil {
			return p.err
		}
	}
	o.Gates = append(o.Gates,
		cmpGate("18.2/q1_throughput_3n", closed[0].Throughput, c.tgt(5000, 2000), true, "ops/s",
			fmt.Sprintf("%.0f ops/s, error rate %.4f, unknown %d", closed[0].Throughput, closed[0].ErrorRate, closed[0].Unknown)),
		cmpGate("18.2/scale_3_vs_1", closed[0].Throughput/closed[1].Throughput, 2.2, true, "x",
			fmt.Sprintf("%.0f vs %.0f ops/s on independent entities", closed[0].Throughput, closed[1].Throughput)),
		c.latGate("18.2/warm_read", open, "read", [3]float64{5, 15, 30}, [3]float64{80, 180, 400}),
		cmpGate("18.2/error_rate_q1", open.ErrorRate*100, 0.1, false, "%", fmt.Sprintf("open loop at %.0f ops/s: unknown %d reported separately", open.Offered, open.Unknown)),
		rssGate(closed[0], open))
	return nil
}

// Q2: write-heavy, B=1, three nodes; then the in-process amplification run.
func (c *stressCfg) q2(o *profileOut) error {
	n := c.count(10000)
	o.Params = map[string]any{"entities": n, "entity_bytes": 64 << 10, "read_fraction": 0.2, "update_bytes": []int{1 << 10, 16 << 10},
		"clients": 256, "popularity": []string{"uniform", "zipf s=1.1"}, "batch_max": 1, "open_loop_fraction": openFraction}
	p, err := c.start(o, "q2", 3, nodeArgs(8<<20, 8<<30, 1)...)
	if err != nil {
		return err
	}
	keys := keyset("e", n)
	base := load{keys: keys, read: .2, size: 1 << 10, clients: 256}
	if _, err := p.setup(keys, 60<<10, base); err != nil {
		p.close()
		return err
	}
	u1 := p.phase("1k-uniform-closed", true, func(m *meter) { p.closed(m, c.measure, 0, base) })
	ol := base
	ol.rate = openFraction * u1.Throughput
	open := p.phase("1k-uniform-open", true, func(m *meter) { p.open(m, c.measure, ol) })
	open.Offered = ol.rate
	var others []*mphase
	for _, v := range []struct {
		name               string
		size               int
		zipf, compressible bool
	}{{"1k-zipf-closed", 1 << 10, true, false}, {"16k-uniform-closed", 16 << 10, false, false},
		{"16k-zipf-closed", 16 << 10, true, false}, {"1k-uniform-compressible-closed", 1 << 10, false, true}} {
		l := base
		l.size, l.zipf, l.compressible = v.size, v.zipf, v.compressible
		others = append(others, p.phase(v.name, true, func(m *meter) { p.closed(m, c.measure, 0, l) }))
	}
	p.close()
	if p.err != nil {
		return p.err
	}
	o.Gates = append(o.Gates,
		cmpGate("18.2/q2_b1_mutations_3n", u1.Mutations, c.tgt(1500, 1000), true, "mutations/s",
			fmt.Sprintf("%.0f successful 1 KiB B=1 mutations/s, error rate %.4f, unknown %d", u1.Mutations, u1.ErrorRate, u1.Unknown)),
		c.latGate("18.2/warm_mutation_b1", open, "write", [3]float64{15, 40, 100}, [3]float64{180, 450, 1000}),
		cmpGate("18.2/error_rate_q2", open.ErrorRate*100, 0.1, false, "%", fmt.Sprintf("open loop at %.0f ops/s: unknown %d reported separately", open.Offered, open.Unknown)),
		cmpGate("18.3/puts_per_b1_mutation", u1.PutsPerMutation, 2.5, false, "PUTs",
			fmt.Sprintf("successful PUTs incl. CAS and maintenance per 1 KiB B=1 mutation (failed %.3f separately, mean batch %.2f)", u1.FailedPutsPerMutation, u1.MeanBatch)),
		rssGate(append(others, u1, open)...))
	return c.amplify(o, max(40, int(400*c.scale)))
}

// Q3: cold churn on one node whose cache is smaller than the working set.
func (c *stressCfg) q3(o *profileOut) error {
	n := c.count(100000)
	cache := max(int64(float64(8<<30)*c.scale), 24<<20) // >= 8 Recoveries x (4 x 512 KiB + 1 MiB)
	minOps := c.minOps
	if minOps == 0 {
		minOps = int64(1e6 * c.scale)
	}
	o.Params = map[string]any{"entities": n, "entity_bytes": 128 << 10, "working_set_bytes": n * (128 << 10), "cache_bytes": cache,
		"read_fraction": 0.8, "update_bytes": 1 << 10, "clients": 64, "min_operations": minOps}
	p, err := c.start(o, "q3", 1, nodeArgs(512<<10, cache, 0)...)
	if err != nil {
		return err
	}
	keys := keyset("e", n)
	l := load{keys: keys, read: .8, size: 1 << 10, clients: 64}
	if _, err := p.setup(keys, 124<<10, l); err != nil {
		p.close()
		return err
	}
	ph := p.phase("churn-closed", true, func(m *meter) { p.closed(m, c.measure, minOps, l) })
	p.close()
	lib := ph.Library
	o.Detail = map[string]any{"activations": lib.Activations, "warm_hits": lib.WarmHits, "catch_ups": lib.CatchUps,
		"warm_evictions": lib.WarmEvictions, "activation_bytes": lib.ActivationBytes, "cache_overshoot_bytes": max(0, ph.Peak.CacheDirBytes-cache)}
	var issued int64
	for _, l := range ph.Ops {
		issued += l.N
	}
	o.Gates = append(o.Gates,
		cmpGate("18.3/cache_budget_q3", float64(ph.Peak.CacheDirBytes)/float64(cache), 1, false, "x budget",
			fmt.Sprintf("peak cache directory %d MiB of %d MiB budget (sampled every 5 s), %d activations, %d warm hits", ph.Peak.CacheDirBytes>>20, cache>>20, lib.Activations, lib.WarmHits)),
		cmpGate("18.3/rss_q3", float64(ph.Peak.RSS)/(1<<20), 1024, false, "MiB",
			fmt.Sprintf("peak RSS %d MiB with %d entities (> cache): namespace-scale memory independence", ph.Peak.RSS>>20, n)),
		cmpGate("18.2/error_rate_q3", ph.ErrorRate*100, 0.1, false, "%", fmt.Sprintf("%d operations, unknown %d", issued, ph.Unknown)))
	if issued < minOps {
		o.Failures = append(o.Failures, fmt.Sprintf("only %d of %d operations", issued, minOps))
	}
	return p.err
}

// Q4: one hot entity plus independent ones.
func (c *stressCfg) q4(o *profileOut) error {
	n := c.count(999)
	o.Params = map[string]any{"independent_entities": n, "hot_clients": 64, "independent_clients": 64, "hot_read_fraction": 0,
		"independent_read_fraction": 0.8, "update_bytes": 1 << 10, "batch_max": "default"}
	p, err := c.start(o, "q4", 3, nodeArgs(8<<20, 8<<30, 0)...)
	if err != nil {
		return err
	}
	keys := append([]string{"hot"}, keyset("e", n)...)
	ind := load{label: "ind-", keys: keys[1:], read: .8, size: 1 << 10, clients: 64}
	hot := load{label: "hot-", keys: keys[:1], size: 1 << 10, clients: 64} // writes only: a queued read ends a batch (PLAN §6.5)
	if _, err := p.setup(keys, 60<<10, ind); err != nil {
		p.close()
		return err
	}
	a := p.phase("independent-only", true, func(m *meter) { p.closed(m, c.measure, 0, ind) })
	b := p.phase("hot+independent", true, func(m *meter) { p.closed(m, c.measure, 0, hot, ind) })
	p.close()
	ratio := func(k string) float64 {
		if a.Ops[k] == nil || b.Ops[k] == nil || a.Ops[k].P99 == 0 {
			return -1
		}
		return b.Ops[k].P99 / a.Ops[k].P99
	}
	hw := b.Ops["hot-write"]
	put := b.Provider["put-4k"]
	o.Detail = map[string]any{"isolation_read_p99_ratio": ratio("ind-read"), "isolation_write_p99_ratio": ratio("ind-write"),
		"hot_writes_per_s": float64(hw.OK) / b.Seconds, "mean_batch": b.MeanBatch,
		"hot_bound_estimate_per_s": b.MeanBatch / max(2*put.P50/1000, 1e-6)} // B / (PUT + CAS), §18.2
	o.Gates = append(o.Gates, gate{ID: "18.2/hotspot_q4", Status: "info", Value: float64(hw.OK) / b.Seconds,
		Target: "B / (PUT + CAS latency) upper-bound estimate", Detail: fmt.Sprintf("hot %.0f writes/s, mean batch %.1f, estimate %.0f/s; independent read p99 x%.2f, write p99 x%.2f under the hotspot",
			float64(hw.OK)/b.Seconds, b.MeanBatch, o.Detail["hot_bound_estimate_per_s"], ratio("ind-read"), ratio("ind-write"))})
	return p.err
}

// call sends r straight to node n0 (single-node profiles, long operations).
func (p *prun) call(r sReq, timeout time.Duration) (sResp, error) {
	m := p.s.get("n0")
	if m == nil {
		return sResp{}, errors.New("n0 is not running")
	}
	resp, ok := m.do(r, timeout)
	switch {
	case !ok:
		return resp, fmt.Errorf("%s %s: no reply within %v", r.Op, r.Key, timeout)
	case !resp.OK:
		return resp, fmt.Errorf("%s %s: %s: %s", r.Op, r.Key, resp.Code, resp.Err)
	}
	return resp, nil
}

func (p *prun) nodeStats() garden.Stats {
	if r, err := p.call(sReq{Op: "stats"}, time.Minute); err == nil && r.Proc != nil {
		return r.Proc.Stats
	}
	return garden.Stats{}
}

func sizeLabel(n int) string {
	if n >= 1<<20 {
		return fmt.Sprintf("%dm", n>>20)
	}
	return fmt.Sprintf("%dk", n>>10)
}

// Q5: images of 64 KiB to 256 MiB with 0/8/32 deltas requested; cold
// activation in a warm process (Evict, then a strong read: the §18.3 read
// bound) and in a fresh process with an empty cache (SIGKILL, wipe,
// restart, Takeover, strong read: the §18.2 cold activation and
// takeover-to-usable latencies); then capacity exhaustion.
func (c *stressCfg) q5(o *profileOut) error {
	var sizes, skipped []int
	for _, s := range []int{64 << 10, 1 << 20, 64 << 20, 256 << 20} {
		if s <= c.maxImage {
			sizes = append(sizes, s)
		} else {
			skipped = append(skipped, s)
		}
	}
	if len(sizes) == 0 {
		return errors.New("--max-image excludes every Q5 size")
	}
	top := sizes[len(sizes)-1]
	eb := int64(4 << 20)
	if top > 2<<20 {
		eb = min(256<<20, 2*int64(top))
	}
	cache := max(int64(8<<30), 8*(4*eb+1<<20))
	o.Params = map[string]any{"sizes": sizes, "skipped_sizes": skipped, "deltas": []int{0, 8, 32}, "delta_write_bytes": 48 << 10,
		"samples": c.cold, "entity_bytes": eb, "cache_bytes": cache}
	p, err := c.start(o, "q5", 1, nodeArgs(eb, cache, 0)...)
	if err != nil {
		return err
	}
	defer p.close()
	ctx := context.Background()
	st := store.NewS3(p.s.ep.Client(nil), p.s.ep.Bucket, p.s.ep.Prefix, time.Minute)
	type bound struct {
		Phase                 string
		HeadDeltas, MaxGETs   int
		Bound                 int
		WarmHits, Activations uint64
	}
	var bounds []bound
	cold := map[string]*mphase{}
	det := map[string]any{"read_bounds": &bounds}
	o.Detail = det
	keyOf := func(size int) string { return "img-" + sizeLabel(size) }
	incs := map[string]string{}
	for _, size := range sizes {
		label, key := sizeLabel(size), keyOf(size)
		p.phase("fill-"+label, false, func(m *meter) {
			t := time.Now()
			r, err := p.call(sReq{Op: "create", Key: key, ID: newID()}, 2*time.Minute)
			if m.direct("create", err, time.Since(t)); err != nil {
				o.Failures = append(o.Failures, err.Error())
				return
			}
			incs[key] = r.Inc
			for left := size * 31 / 32; left > 0; left -= 16 << 20 {
				t := time.Now()
				_, err := p.call(sReq{Op: "fill", Key: key, Inc: r.Inc, ID: newID(), Size: min(left, 16<<20)}, 10*time.Minute)
				if m.direct("fill", err, time.Since(t)); err != nil {
					o.Failures = append(o.Failures, err.Error())
					return
				}
			}
		})
		inc := incs[key]
		if inc == "" {
			continue
		}
		have := 0
		for _, nd := range []int{0, 8, 32} {
			for ; have < nd; have++ {
				if _, err := p.call(sReq{Op: "write", Key: key, Inc: inc, ID: newID(), Size: 48 << 10}, 5*time.Minute); err != nil {
					o.Failures = append(o.Failures, err.Error())
				}
			}
			time.Sleep(2 * time.Second) // background maintenance settles
			var h head.Head
			if ob, err := st.Get(ctx, head.HeadKey(key), "", 1<<20); err == nil {
				h, _ = head.Decode(ob.Body)
			}
			name := fmt.Sprintf("%s-%dd", label, nd)
			b := bound{Phase: "evict-" + name, HeadDeltas: len(h.Deltas), Bound: 1 + 1 + len(h.Deltas) + 1}
			p.phase(b.Phase, true, func(m *meter) {
				for range c.cold {
					if _, err := p.call(sReq{Op: "evict", Key: key, Inc: inc}, time.Minute); err != nil {
						m.direct("activation", err, 0)
						continue
					}
					s0 := p.nodeStats()
					t := time.Now()
					_, err := p.call(sReq{Op: "read", Key: key, Inc: inc}, 10*time.Minute)
					m.direct("activation", err, time.Since(t))
					s1 := p.nodeStats()
					b.MaxGETs = max(b.MaxGETs, int(s1.Storage[0].Attempts-s0.Storage[0].Attempts))
					b.WarmHits += s1.WarmHits - s0.WarmHits
					b.Activations += s1.Activations - s0.Activations
				}
			})
			bounds = append(bounds, b)
			cold[name] = p.phase("cold-"+name, true, func(m *meter) {
				for range c.cold {
					p.kill("n0", true)
					if err := p.add("n0"); err != nil {
						m.direct("takeover-to-usable", err, 0)
						continue
					}
					t := time.Now()
					_, err := p.call(sReq{Op: "takeover", Key: key}, 10*time.Minute)
					m.direct("takeover", err, time.Since(t))
					if err == nil {
						_, err = p.call(sReq{Op: "read", Key: key, Inc: inc}, 10*time.Minute)
					}
					m.direct("takeover-to-usable", err, time.Since(t))
				}
			})
		}
	}
	// Capacity exhaustion: grow the largest image past EntityBytes; the
	// refusal must be capacity and the entity must stay readable.
	key := keyOf(top)
	capOK, capDetail := false, "largest image was not created"
	if inc := incs[key]; inc != "" {
		chunk := int(min(16<<20, eb/4))
		capDetail = fmt.Sprintf("no refusal after %d MiB more", (int(eb/int64(chunk))+2)*chunk>>20)
		for range int(eb/int64(chunk)) + 2 {
			r, err := p.call(sReq{Op: "fill", Key: key, Inc: inc, ID: newID(), Size: chunk}, 10*time.Minute)
			if err == nil {
				continue
			}
			_, rerr := p.call(sReq{Op: "read", Key: key, Inc: inc}, 10*time.Minute)
			// The limit surfaces as SQLITE_FULL inside the callback, which passes
			// callback errors through, or as capacity if SQLite rolled back alone.
			capOK = (r.Code == string(garden.CodeCapacity) || strings.Contains(r.Err, "database or disk is full")) && rerr == nil
			capDetail = fmt.Sprintf("refused %q (%s); read afterwards: %v", r.Code, r.Err, rerr)
			break
		}
	}
	ok := len(bounds) > 0
	worst := ""
	for _, b := range bounds {
		if b.MaxGETs > b.Bound || b.MaxGETs == 0 {
			ok = false
		}
		worst += fmt.Sprintf(" %s %d/%d", b.Phase, b.MaxGETs, b.Bound)
	}
	o.Gates = append(o.Gates,
		gate{ID: "18.3/cold_activation_reads", Status: map[bool]string{true: "pass", false: "fail"}[ok], Target: "GETs <= head + snapshot + deltas + birth validation",
			Detail: "max GETs per activation / bound:" + worst},
		gate{ID: "18.1/capacity_refusal_q5", Status: map[bool]string{true: "pass", false: "fail"}[capOK], Target: "capacity refusal, entity still readable", Detail: capDetail},
		c.latGate("18.2/takeover_small", cold["64k-0d"], "takeover-to-usable", [3]float64{0, 2000, 5000}, [3]float64{0, 10000, 20000}),
		c.latGate("18.2/cold_activation_1mib_8d", cold["1m-8d"], "takeover-to-usable", [3]float64{0, 500, 1500}, [3]float64{0, 2000, 5000}))
	if top >= 64<<20 {
		o.Gates = append(o.Gates, c.latGate("18.2/cold_activation_64mib", cold["64m-0d"], "takeover-to-usable", [3]float64{0, 3000, 8000}, [3]float64{0, 15000, 30000}))
	}
	return p.err
}

// Q6 is the three-node soak (qual soak), run with this campaign's
// duration and operation minimum; its summary.json is the evidence.
func (c *stressCfg) q6(o *profileOut) error {
	dir := filepath.Join(c.out, "Q6")
	minOps := c.minOps
	if minOps == 0 {
		minOps = int64(1e7 * c.scale)
	}
	args := []string{"-duration", c.measure.String(), "-min-ops", strconv.FormatInt(minOps, 10), "-out", dir, "-local"}
	if c.remote {
		args[len(args)-1] = "-remote"
		if c.hosts[0] != "" {
			args = append(args, "-hosts", strings.Join(c.hosts, ","))
		}
	}
	o.Params = map[string]any{"soak_args": args}
	err := soak(args)
	var sum soakSummary
	b, rerr := os.ReadFile(filepath.Join(dir, "summary.json"))
	if rerr == nil {
		rerr = json.Unmarshal(b, &sum)
	}
	g := gate{ID: "18.3/q6_soak", Status: "fail", Target: "soak verdict pass (growth, goroutines, descriptors, ledger, windows)", Detail: fmt.Sprint(rerr)}
	if rerr == nil {
		g.Value, g.Detail = float64(sum.Ops), fmt.Sprintf("%s: %d operations in %s, %d windows ok, %d failed", sum.Verdict, sum.Ops, sum.Elapsed, sum.WindowsOK, sum.WindowsFailed)
		if sum.Verdict == "pass" {
			g.Status = "pass"
		}
	}
	o.Gates = append(o.Gates, g)
	return err
}

// Q7: Q1 load paced open-loop at half the warmup throughput through a
// SIGKILL+restart with a wiped cache, a 6 s SIGSTOP of an owner while
// clients take over elsewhere, a storage outage on every node and a
// restore storm (every node killed with its cache, all restarted). Each
// wave's recovery is the time from restored health to the first 5 s window
// at 90% of the offered rate without failures (§18.2: two minutes).
// Unknown writes are resolved by receipt lookup, and the ledger of every
// acknowledged write is checked from a fresh node at the end.
func (c *stressCfg) q7(o *profileOut) error {
	n := c.count(10000)
	d := max(c.measure, 8*c.outage)
	o.Params = map[string]any{"entities": n, "entity_bytes": 64 << 10, "read_fraction": 0.8, "update_bytes": 1 << 10, "clients": 256,
		"duration": d.String(), "outage": c.outage.String(), "route_timeout": sRouteTimeout.String()}
	p, err := c.start(o, "q7", 3, nodeArgs(8<<20, 8<<30, 0)...)
	if err != nil {
		return err
	}
	defer p.close()
	p.ledger, p.s.routeTimeout = true, sRouteTimeout
	keys := keyset("e", n)
	l := load{keys: keys, read: .8, size: 1 << 10, clients: 256}
	warm, err := p.setup(keys, 60<<10, l)
	if err != nil {
		return err
	}
	if warm == nil {
		warm = p.phase("capacity", false, func(m *meter) { p.closed(m, 30*time.Second, 0, l) })
	}
	l.rate = openFraction * warm.Throughput
	type wave struct {
		Kind       string
		Start, End float64 // seconds into the phase; End: health restored
		RecoveryS  float64 // -1: never recovered
	}
	var waves []wave
	ph := p.phase("waves", true, func(m *meter) {
		var wg sync.WaitGroup
		wg.Go(func() { p.open(m, d, l) })
		stop := make(chan struct{})
		wg.Go(func() { // ledger checks often enough that no acknowledged write ages out of the ring
			t := time.NewTicker(2 * time.Minute)
			defer t.Stop()
			for {
				select {
				case <-stop:
					return
				case <-t.C:
					p.s.checkpoint(keys, false)
				}
			}
		})
		at := func(f float64) time.Time {
			time.Sleep(time.Until(m.t0.Add(time.Duration(f * float64(d)))))
			return time.Now()
		}
		mark := func(kind string, t time.Time) {
			waves = append(waves, wave{Kind: kind, Start: t.Sub(m.t0).Seconds(), End: time.Since(m.t0).Seconds()})
			p.s.logx("wave %s done", kind)
		}
		t := at(1. / 8)
		p.kill("n1", true)
		if err := p.add("n1"); err != nil {
			o.Failures = append(o.Failures, err.Error())
		}
		mark("kill", t)
		t = at(2. / 8)
		if rr, ok := p.s.get("n0").do(sReq{Op: "resolve", Key: keys[0]}, 10*time.Second); ok && rr.Route != nil && p.s.get(rr.Route.Node) != nil {
			owner := p.s.get(rr.Route.Node)
			owner.signal(syscall.SIGSTOP)
			time.Sleep(6 * time.Second)
			owner.signal(syscall.SIGCONT)
			mark("stale-owner-"+rr.Route.Node, t)
		}
		t = at(3. / 8)
		for _, nm := range p.names() {
			p.s.get(nm).do(sReq{Op: "outage", Size: int(c.outage / time.Millisecond)}, 10*time.Second)
		}
		time.Sleep(time.Until(t.Add(c.outage)))
		mark("outage", t)
		t = at(5.5 / 8)
		for _, nm := range p.names() {
			p.kill(nm, true)
		}
		for i := range 3 {
			if err := p.add(fmt.Sprintf("n%d", i)); err != nil {
				o.Failures = append(o.Failures, err.Error())
			}
		}
		mark("restore-storm", t)
		close(stop)
		wg.Wait()
	})
	ph.Offered = l.rate
	// Recovery: 5 s windows after each wave against the offered rate.
	worst := 0.0
	for i := range waves {
		w := &waves[i]
		w.RecoveryS = -1
		for s := int(math.Ceil(w.End)); s+5 <= len(ph.Timeline); s++ {
			var ok, bad int64
			for _, x := range ph.Timeline[s : s+5] {
				ok, bad = ok+x.OK, bad+x.Failed+x.Unknown
			}
			if float64(ok) >= 0.9*5*l.rate && bad == 0 {
				w.RecoveryS = float64(s) - w.End
				break
			}
		}
		if w.RecoveryS < 0 {
			worst = 1e9 // never recovered within the run
		}
		worst = max(worst, w.RecoveryS)
	}
	// Unknown writes resolve by receipt once connectivity is stable.
	res := map[string]int{}
	rc := p.client(-7)
	for _, u := range ph.unk {
		lo := rc.run(sReq{Op: "lookup", ID: u.ID, Inc: p.s.refs[u.Key]}, u.Key)
		switch {
		case lo.Class == "ack":
			res["committed"]++
		case lo.Code == string(garden.CodeAbsentAtVersion):
			res["not_committed"]++
		case lo.Code == string(garden.CodeIDExpired):
			res["expired"]++
		default:
			res["unresolved"]++
		}
	}
	// Final ledger from a fresh node with no cache.
	for _, nm := range p.names() {
		p.kill(nm, true)
	}
	p.s.mu.Lock()
	p.s.members = nil
	p.s.mu.Unlock()
	if err := p.add("final"); err != nil {
		o.Failures = append(o.Failures, err.Error())
	} else {
		p.s.checkpoint(keys, true)
	}
	p.s.smu.Lock()
	fails, checked := slices.Clone(p.s.failures), p.s.checkedAcks
	p.s.smu.Unlock()
	o.Detail = map[string]any{"waves": waves, "unknown_resolution": res, "ledger_checked": checked, "ledger_failures": fails}
	o.Gates = append(o.Gates,
		cmpGate("18.2/backlog_recovery_q7", worst, 120, false, "s", fmt.Sprintf("worst wave recovery %.0f s; waves %v", worst, waves)),
		cmpGate("18.1/q7_unknown_resolved", float64(res["unresolved"]), 0, false, "unresolved", fmt.Sprintf("unknown writes after stable connectivity: %v", res)),
		cmpGate("18.1/q7_acknowledged_ledger", float64(len(fails)), 0, false, "failures", fmt.Sprintf("%d acknowledged writes checked from a fresh node; first failures %v", checked, fails[:min(len(fails), 3)])),
		gate{ID: "18.2/bounded_admission_q7", Status: "info", Value: ph.MaxFailedMs, Target: "failures explicit within deadlines",
			Detail: fmt.Sprintf("longest failed/unknown operation %.0f ms, shed %d, unknown %d", ph.MaxFailedMs, ph.Shed, ph.Unknown)})
	return p.err
}

// Q0: component baselines in this process. A bare modernc SQLite
// transaction; the engine's local transaction (receipts, WAL) and its
// capture (Delta) in a 64 KiB and a 64 MiB database (the §18.3 capture CPU
// gate, thread CPU time); the full-snapshot reference and a plain LTX-delta
// reference behind the same immutable PUT + head CAS acknowledgment
// (§17.3); and direct store primitives at 4 KiB, 32 KiB, 1 MiB and 64 MiB.
func (c *stressCfg) q0(o *profileOut) error {
	ctx := context.Background()
	n := max(20, int(200*c.scale))
	if err := os.MkdirAll(c.out, 0o755); err != nil {
		return err
	}
	dir, err := os.MkdirTemp(c.out, "q0-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(dir)
	det := map[string]any{}
	o.Detail, o.Params = det, map[string]any{"samples": n, "update": "one row: 1 KiB (bare, reference) or 3000 B (capture, one page)"}
	if det["sqlite_bare_1k_update"], err = bareSQLite(ctx, filepath.Join(dir, "bare.db"), n); err != nil {
		return err
	}
	var cpu [2]float64
	for i, size := range []int64{64 << 10, 64<<20 - 1<<20} {
		r, err := engineCapture(ctx, filepath.Join(dir, fmt.Sprintf("cap%d.db", i)), size, n)
		if err != nil {
			return err
		}
		det[[]string{"capture_64k", "capture_64m"}[i]] = r
		cpu[i] = r["capture_cpu_us_per_op"].(float64)
	}
	ep := c.ep.Sub("q0")
	st := store.NewS3(ep.Client(nil), ep.Bucket, ep.Prefix, time.Minute)
	defer cleanup(ctx, st)
	for _, size := range []int64{64 << 10, 1 << 20} {
		for _, full := range []bool{true, false} {
			r, err := refCAS(ctx, st, filepath.Join(dir, fmt.Sprintf("ref-%d-%v.db", size, full)), size, n, full)
			if err != nil {
				return err
			}
			det[fmt.Sprintf("reference_%s_%s", map[bool]string{true: "full_snapshot", false: "ltx_delta"}[full], sizeLabel(int(size)))] = r
		}
	}
	det["primitives"] = primitives(ctx, st, n)
	o.Gates = append(o.Gates, cmpGate("18.3/capture_cpu_64mib_vs_64kib", cpu[1]/cpu[0], 3, false, "x",
		fmt.Sprintf("thread CPU per one-page capture: %.1f us (64 MiB) vs %.1f us (64 KiB)", cpu[1], cpu[0])))
	return nil
}

func bareSQLite(ctx context.Context, path string, n int) (*lstat, error) {
	db, err := sql.Open("sqlite", "file:"+path+"?_pragma=journal_mode(wal)&_pragma=synchronous(full)")
	if err != nil {
		return nil, err
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	if _, err := db.ExecContext(ctx, "CREATE TABLE kv(k INTEGER PRIMARY KEY, v BLOB NOT NULL)"); err != nil {
		return nil, err
	}
	for i := range 16 {
		if _, err := db.ExecContext(ctx, "INSERT INTO kv VALUES (?, ?)", 100+i, payload(4<<10, false)); err != nil {
			return nil, err
		}
	}
	var lat []time.Duration
	for i := range n {
		v := payload(1<<10, false)
		t := time.Now()
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			return nil, err
		}
		if _, err := tx.ExecContext(ctx, "INSERT OR REPLACE INTO kv VALUES (?, ?)", i%16, v); err != nil {
			return nil, err
		}
		if err := tx.Commit(); err != nil {
			return nil, err
		}
		lat = append(lat, time.Since(t))
	}
	return quant(lat), nil
}

// edb is an engine database for Q0: one 3000-byte row per page.
type edb struct {
	d    *engine.DB
	seq  uint64
	rows int
}

var q0Driver = sync.OnceValues(func() (*engine.Driver, error) { return engine.NewDriver(nil) })

func newEDB(ctx context.Context, path string, size int64) (*edb, error) {
	drv, err := q0Driver()
	if err != nil {
		return nil, err
	}
	d, err := engine.Create(ctx, drv, path, head.MaxPages, head.NewID(), "q0", head.NewID())
	if err != nil {
		return nil, err
	}
	e := &edb{d: d}
	if err := e.mutate(ctx, "CREATE TABLE t(id INTEGER PRIMARY KEY, v BLOB)"); err != nil {
		return nil, err
	}
	for int64(e.rows)*head.PageSize < size {
		k := min(1000, size/head.PageSize-int64(e.rows)+1)
		if err := e.mutate(ctx, `INSERT INTO t(v) SELECT randomblob(3000) FROM (WITH RECURSIVE c(i) AS
			(SELECT 1 UNION ALL SELECT i+1 FROM c WHERE i < 1000) SELECT i FROM c) LIMIT ?`, k); err != nil {
			return nil, err
		}
		e.rows += int(k)
	}
	o, err := d.Snapshot(io.Discard)
	if err != nil {
		return nil, err
	}
	return e, d.Checkpoint(engine.Image{Sequence: o.Ref.MaxSequence, PageCount: o.PageCount, Checksum: o.Ref.PostChecksum})
}

func (e *edb) mutate(ctx context.Context, q string, args ...any) error {
	e.seq++
	c := engine.Commit{Schema: 1}
	binary.BigEndian.PutUint64(c.ID[:], e.seq)
	binary.BigEndian.PutUint64(c.ID[8:], e.seq*0x9e3779b97f4a7c15)
	c.Bound[0] = byte(e.seq)
	_, _, err := e.d.Mutate(c, func(tx *engine.Tx) ([]byte, error) {
		_, err := tx.Exec(ctx, q, args...)
		return nil, err
	})
	return err
}

// threadCPU is this OS thread's user + system time (RUSAGE_THREAD, Linux).
func threadCPU() time.Duration {
	var ru syscall.Rusage
	syscall.Getrusage(1, &ru)
	return time.Duration(ru.Utime.Nano() + ru.Stime.Nano())
}

func engineCapture(ctx context.Context, path string, size int64, n int) (map[string]any, error) {
	e, err := newEDB(ctx, path, size)
	if err != nil {
		return nil, err
	}
	defer e.d.Discard()
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	var txl, capl []time.Duration
	var cpu time.Duration
	pages := 0
	for i := range n + 5 { // the first five warm up and are not counted
		t := time.Now()
		if err := e.mutate(ctx, "UPDATE t SET v = randomblob(3000) WHERE id = ?", i%e.rows+1); err != nil {
			return nil, err
		}
		tx := time.Since(t)
		c0, t := threadCPU(), time.Now()
		o, err := e.d.Delta(io.Discard)
		if err != nil {
			return nil, err
		}
		if i >= 5 {
			txl, capl, cpu, pages = append(txl, tx), append(capl, time.Since(t)), cpu+threadCPU()-c0, pages+int(o.Pages)
		}
		if err := e.d.Checkpoint(engine.Image{Sequence: o.Ref.MaxSequence, PageCount: o.PageCount, Checksum: o.Ref.PostChecksum}); err != nil {
			return nil, err
		}
	}
	return map[string]any{"db_pages": e.d.Published().PageCount, "engine_tx": quant(txl), "capture": quant(capl),
		"capture_cpu_us_per_op": float64(cpu) / 1e3 / float64(n), "pages_per_op": float64(pages) / float64(n)}, nil
}

// refCAS is a reference publication behind the library's acknowledgment:
// local transaction, encode (the whole image, or the delta), immutable PUT,
// head CAS, checkpoint; acknowledged after the CAS.
func refCAS(ctx context.Context, st *store.S3, path string, size int64, n int, full bool) (map[string]any, error) {
	e, err := newEDB(ctx, path, size)
	if err != nil {
		return nil, err
	}
	defer e.d.Discard()
	dir := fmt.Sprintf("ref-%v-%d", full, size)
	etag, err := st.PutCAS(ctx, dir+"/head.json", []byte("{}"), "")
	if err != nil {
		return nil, err
	}
	var lat []time.Duration
	var up int64
	var buf bytes.Buffer
	for i := range n {
		t := time.Now()
		if err := e.mutate(ctx, "UPDATE t SET v = randomblob(1024) WHERE id = ?", i%e.rows+1); err != nil {
			return nil, err
		}
		buf.Reset()
		var o engine.Object
		if full {
			o, err = e.d.Snapshot(&buf)
		} else {
			o, err = e.d.Delta(&buf)
		}
		if err != nil {
			return nil, err
		}
		key := fmt.Sprintf("%s/%06d", dir, i)
		if err := st.PutImmutable(ctx, key, buf.Bytes()); err != nil {
			return nil, err
		}
		hb, _ := json.Marshal(map[string]any{"object": key, "sequence": o.Ref.MaxSequence, "checksum": o.Ref.PostChecksum})
		if etag, err = st.PutCAS(ctx, dir+"/head.json", hb, etag); err != nil {
			return nil, err
		}
		if err := e.d.Checkpoint(engine.Image{Sequence: o.Ref.MaxSequence, PageCount: o.PageCount, Checksum: o.Ref.PostChecksum}); err != nil {
			return nil, err
		}
		lat, up = append(lat, time.Since(t)), up+int64(buf.Len()+len(hb))
	}
	return map[string]any{"latency": quant(lat), "uploaded_per_op": up / int64(n), "db_pages": e.d.Published().PageCount}, nil
}

// primitives: single-attempt create PUT and GET by size, chained 4 KiB CAS.
// Objects of 1 MiB and more are deleted at once (the local fixture is small).
func primitives(ctx context.Context, st *store.S3, n int) map[string]*lstat {
	res := map[string]*lstat{}
	for _, size := range []int{4 << 10, 32 << 10, 1 << 20, 64 << 20} {
		k := n
		if size >= 1<<20 {
			k = max(3, n/10)
		}
		body := payload(size, false)
		var put, get []time.Duration
		var pe, ge int64
		for i := range k {
			key := fmt.Sprintf("prim/%s/%d", sizeLabel(size), i)
			t := time.Now()
			if _, err := st.PutCAS(ctx, key, body, ""); err != nil {
				pe++
				continue
			}
			put, t = append(put, time.Since(t)), time.Now()
			if _, err := st.Get(ctx, key, "", int64(size)); err != nil {
				ge++
			} else {
				get = append(get, time.Since(t))
			}
			if size >= 1<<20 {
				st.Delete(ctx, key)
			}
		}
		pl, gl := quant(put), quant(get)
		pl.Failed, gl.Failed, pl.N, gl.N = pe, ge, int64(k), int64(k)-pe
		res["put-"+sizeLabel(size)], res["get-"+sizeLabel(size)] = pl, gl
	}
	body := payload(4<<10, false)
	etag, err := st.PutCAS(ctx, "prim/cas", body, "")
	var cas []time.Duration
	var ce int64
	for range n {
		if err != nil {
			break
		}
		t := time.Now()
		e, cerr := st.PutCAS(ctx, "prim/cas", body, etag)
		if cerr != nil {
			ce++
			continue
		}
		etag, cas = e, append(cas, time.Since(t))
	}
	res["cas-4k"] = quant(cas)
	res["cas-4k"].Failed, res["cas-4k"].N = ce, int64(n)
	return res
}

// amplify is the §18.3 amplification run on the fixed 1 KiB fixture: one
// in-process node (B=1) on the wire counter, one 64 KiB entity, sequential
// 1 KiB updates whose fresh delta (a single-commit inline tail, or a
// single-commit level-0 delta object) is read back from each new head and
// decoded for its page count; then strong reads and 48 KiB upload-path
// updates for the per-publication request counts.
func (c *stressCfg) amplify(o *profileOut, n int) error {
	ctx := context.Background()
	ep := c.ep.Sub("q2-amp")
	w := &wire{base: http.DefaultTransport.(*http.Transport).Clone(), inflight: map[string]int{}, delay: c.delay}
	dir, err := os.MkdirTemp(c.out, "amp-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(dir)
	node, err := garden.Open(ctx, garden.Config{S3: ep.Client(&http.Client{Transport: w}), Bucket: ep.Bucket, Prefix: ep.Prefix,
		NodeID: "amp", CacheDir: dir, Limits: garden.Limits{EntityBytes: 8 << 20, CacheBytes: 1 << 30, BatchMax: 1}})
	if err != nil {
		return err
	}
	st := store.NewS3(ep.Client(nil), ep.Bucket, ep.Prefix, time.Minute)
	defer func() {
		node.Close(ctx)
		cleanup(ctx, st)
	}()
	ref, _, err := node.Create(ctx, "amp", req(), func(tx *garden.Tx) ([]byte, error) {
		_, err := tx.Exec(ctx, "CREATE TABLE kv(k INTEGER PRIMARY KEY, v BLOB NOT NULL)")
		return nil, err
	})
	if err != nil {
		return err
	}
	for i := range 16 {
		if err := update(ctx, node, ref, 100+i, 4<<10); err != nil {
			return err
		}
	}
	time.Sleep(2 * time.Second)
	var enc, pages []float64
	sample := func() {
		ob, err := st.Get(ctx, head.HeadKey("amp"), "", 1<<20)
		if err != nil {
			return
		}
		h, err := head.Decode(ob.Body)
		if err != nil {
			return
		}
		if t := h.Tail; t != nil && t.MinSequence == t.MaxSequence && t.MaxSequence == h.Sequence {
			enc, pages = append(enc, float64(t.Bytes)), append(pages, ltxPages(t.Data))
		} else if k := len(h.Deltas); t == nil && k > 0 && h.Deltas[k-1].MinSequence == h.Deltas[k-1].MaxSequence && h.Deltas[k-1].MaxSequence == h.Sequence {
			if ob, err := st.Get(ctx, h.Deltas[k-1].Key, "", 64<<20); err == nil {
				enc, pages = append(enc, float64(len(ob.Body))), append(pages, ltxPages(ob.Body))
			}
		}
	}
	rep := &stressReport{}
	rep.phase("amp-1k-seq", w, node, n, 1, func(i int) error {
		if err := update(ctx, node, ref, i%16, 1<<10); err != nil {
			return err
		}
		sample()
		return nil
	})
	rep.phase("amp-read-seq", w, node, n, 1, func(int) error { return read(ctx, node, ref) })
	nu := max(8, n/4)
	rep.phase("amp-upload-seq", w, node, nu, 1, func(i int) error { return update(ctx, node, ref, 200+i%8, 48<<10) })
	o.InProcess = rep.Phases
	mean := func(v []float64) float64 {
		s := 0.0
		for _, x := range v {
			s += x
		}
		return s / float64(max(len(v), 1))
	}
	u, r, up := rep.Phases[0], rep.Phases[1], rep.Phases[2]
	freshB, dirtyB := mean(enc), mean(pages)*head.PageSize
	upPer := float64(u.Uploaded) / float64(n)
	maint := func(p phase) float64 { return float64(p.Snapshots+p.Comp) / float64(p.N) }
	casPer := float64(u.Requests["put-head"]-u.Hedges) / float64(n)
	objPer := float64(up.Requests["put-object"]) / float64(nu)
	upCAS := float64(up.Requests["put-head"]-up.Hedges) / float64(nu)
	gets := float64(r.Requests["get-head"]+r.Requests["get-head-304"]) / float64(n)
	lists := 0 // beyond one per maintenance step (its collection may list superseded objects)
	for _, p := range rep.Phases {
		lists += max(0, p.Requests["list"]-int(p.Snapshots+p.Comp))
	}
	errs := u.Errors + r.Errors + up.Errors
	o.Detail = map[string]any{"fresh_delta_samples": len(enc), "fresh_delta_bytes_avg": freshB, "dirty_page_bytes_avg": dirtyB,
		"compression_ratio": dirtyB / max(freshB, 1), "receipt_bytes": "not separable: receipts are rows inside the changed pages",
		"uploaded_per_mutation": upPer, "upload_vs_changed_pages": upPer / max(dirtyB, 1), "inline_head_cas_per_mutation": casPer,
		"upload_object_puts_per_mutation": objPer, "upload_head_cas_per_mutation": upCAS, "head_gets_per_read": gets, "ordinary_lists": lists,
		"maintenance_per_mutation": map[string]float64{"inline": maint(u), "upload": maint(up)}}
	pub := casPer >= 1 && casPer <= 1+maint(u)+1e-9 && objPer >= 1 && objPer <= 1+maint(up)+1e-9 && upCAS >= 1 && upCAS <= 1+maint(up)+1e-9 &&
		gets == 1 && lists == 0 && errs == 0
	if len(enc) == 0 || dirtyB == 0 {
		freshB, dirtyB = math.NaN(), math.NaN() // no single-commit sample: inconclusive, never a pass
	}
	o.Gates = append(o.Gates,
		cmpGate("18.3/fresh_delta_1kib", freshB/1024, 32, false, "KiB", fmt.Sprintf("%d single-commit samples, %.0f B encoded, %.0f B dirty pages (ratio %.2f)", len(enc), freshB, dirtyB, dirtyB/freshB)),
		cmpGate("18.3/upload_vs_changed_pages", upPer/dirtyB, 4, false, "x", fmt.Sprintf("%.0f B uploaded per 1 KiB mutation incl. maintenance vs %.0f B changed pages", upPer, dirtyB)),
		gate{ID: "18.3/requests_per_publication", Status: map[bool]string{true: "pass", false: "fail"}[pub],
			Target: "inline: 1 head CAS; upload: 1 object PUT + 1 head CAS (+ recorded maintenance); read: 1 head GET; no LIST outside maintenance",
			Detail: fmt.Sprintf("inline CAS %.3f, upload PUT %.3f + CAS %.3f, maintenance %.3f/%.3f, head GETs per read %.3f, LIST %d, errors %d, hedges %d",
				casPer, objPer, upCAS, maint(u), maint(up), gets, lists, errs, u.Hedges+up.Hedges)})
	return nil
}

func ltxPages(b []byte) float64 {
	dec := ltx.NewDecoder(bytes.NewReader(b))
	if dec.DecodeHeader() != nil {
		return 0
	}
	buf := make([]byte, dec.Header().PageSize)
	var ph ltx.PageHeader
	n := 0.0
	for dec.DecodePage(&ph, buf) == nil {
		n++
	}
	return n
}

// envLock is the §17.1 environment record of the driver host and, over ssh,
// of every node host.
func envLock(c *stressCfg, out string) map[string]any {
	e := map[string]any{"go": runtime.Version(), "goos": runtime.GOOS, "goarch": runtime.GOARCH, "gomaxprocs": runtime.GOMAXPROCS(0),
		"num_cpu": runtime.NumCPU(), "gogc": os.Getenv("GOGC"), "gomemlimit": os.Getenv("GOMEMLIMIT"), "endpoint": c.ep.URL,
		"region": c.ep.Region, "bucket": c.ep.Bucket, "prefix": c.ep.Prefix, "remote": c.remote, "delay": c.delay.String(),
		"fixture": cmp.Or(os.Getenv(storetest.ServerEnv), "rustfs"), "encryption": "provider default (none requested)",
		"sdk_retries": "SDK retryer off; the store's own bounded retries and attempt deadlines (internal/store)", "started": time.Now().UTC()}
	if c.remote {
		delete(e, "fixture")
	}
	run := func(name string, args ...string) string {
		b, err := exec.Command(name, args...).Output()
		if err != nil {
			return "unavailable: " + err.Error()
		}
		return strings.TrimSpace(string(b))
	}
	e["commit"], e["dirty_tree"] = run("git", "rev-parse", "HEAD"), run("git", "status", "--porcelain") != ""
	for _, f := range []string{"go.sum", "research.lock.json"} {
		if b, err := os.ReadFile(f); err == nil {
			h := sha256.Sum256(b)
			e[f+"_sha256"] = hex.EncodeToString(h[:])
		}
	}
	e["kernel"] = run("uname", "-srm")
	e["cpu"] = run("sh", "-c", "grep -m1 'model name' /proc/cpuinfo | cut -d: -f2")
	e["mem"] = run("sh", "-c", "grep MemTotal /proc/meminfo")
	e["filesystem"] = run("df", "-T", out)
	var rl syscall.Rlimit
	if syscall.Getrlimit(syscall.RLIMIT_NOFILE, &rl) == nil {
		e["nofile"] = rl.Cur
	}
	nodes := map[string]string{}
	for _, h := range c.hosts {
		if h != "" {
			nodes[h] = run("ssh", h, "--", "sha256sum ~/qual; uname -srm; nproc; grep MemTotal /proc/meminfo; df -T $HOME | tail -1")
		}
	}
	if len(nodes) > 0 {
		e["node_hosts"] = nodes
	}
	return e
}
