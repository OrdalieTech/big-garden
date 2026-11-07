package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"sync"
	"time"

	"github.com/OrdalieTech/big-garden/internal/store"
	"github.com/OrdalieTech/big-garden/internal/storetest"
)

// doctor qualifies one S3 endpoint for the store contract (PLAN §3.1, M3):
// conditional create and CAS races with exactly one winner per round, stale
// ETag rejection, overwrite visibility across connections, single physical
// CAS attempts, late application after client cancellation, and detection
// of truncated or corrupted bodies. It writes everything under a fresh
// random prefix and deletes it afterwards.
func doctor(args []string) error {
	fs := flag.NewFlagSet("doctor", flag.ContinueOnError)
	local := fs.Bool("local", false, "use the pinned local MinIO fixture (started if needed)")
	remote := fs.Bool("remote", false, "use QUAL_S3_ENDPOINT/REGION/BUCKET/PREFIX and AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY")
	requireRemote := fs.Bool("require-remote", false, "missing remote configuration is blocked_missing_remote_configuration")
	var cfg doctorConfig
	fs.IntVar(&cfg.Rounds, "cas-rounds", 1000, "contested rounds for each of create and CAS")
	fs.IntVar(&cfg.Contenders, "contenders", 32, "concurrent writers per round")
	fs.IntVar(&cfg.LatencySamples, "latency-samples", 0, "PUT and GET latency samples per object size (0: skip)")
	fs.DurationVar(&cfg.AttemptTimeout, "attempt-timeout", time.Minute, "deadline of one physical request")
	out := fs.String("out", "", "directory for doctor.json")
	if err := fs.Parse(args); err != nil {
		return err
	}
	var ep storetest.Endpoint
	var err error
	switch {
	case *remote || *requireRemote:
		ep, err = storetest.RemoteFromEnv()
	case *local:
		ep, err = storetest.StartMinIO(context.Background())
	default:
		return errors.New("choose --local or --remote")
	}
	if err != nil {
		return err
	}
	rep, err := runDoctor(context.Background(), ep.Sub("doctor"), cfg)
	if *out != "" {
		if err := os.MkdirAll(*out, 0o755); err != nil {
			return err
		}
		b, _ := json.MarshalIndent(rep, "", "  ")
		if err := os.WriteFile(filepath.Join(*out, "doctor.json"), append(b, '\n'), 0o644); err != nil {
			return err
		}
	}
	return err
}

type doctorConfig struct {
	Rounds, Contenders, LatencySamples int
	AttemptTimeout                     time.Duration
}

type doctorCheck struct {
	Name   string `json:"name"`
	Status string `json:"status"` // pass, fail, info
	Detail string `json:"detail,omitempty"`
}

type race struct {
	Rounds, Contenders int
	OneWinner          int      // rounds with exactly one OK, the stored state being the winner's
	Unknown            int      // contender replies without a definite answer
	Violations         []string `json:",omitempty"` // first few rounds that failed
}

type dist struct {
	N, Errors          int
	P50, P95, P99, Max string
}

type doctorReport struct {
	Endpoint, Bucket, Prefix string
	Go, OS                   string
	Started, Finished        time.Time
	Config                   doctorConfig
	Checks                   []doctorCheck
	CreateRace, CASRace      race
	Attempts                 store.Counts    // direct clients
	ProxyAttempts            store.Counts    // client behind the fault proxy
	ProxyRequests            int             // requests the proxy saw
	Latency                  map[string]dist `json:",omitempty"`
	Pass                     bool
}

// runDoctor runs every check against ep and returns a nil error only if all
// passed. Nothing outside ep.Prefix is written, listed or deleted.
func runDoctor(ctx context.Context, ep storetest.Endpoint, cfg doctorConfig) (rep doctorReport, err error) {
	rep = doctorReport{Endpoint: ep.URL, Bucket: ep.Bucket, Prefix: ep.Prefix, Go: runtime.Version(),
		OS: runtime.GOOS + "/" + runtime.GOARCH, Started: time.Now().UTC(), Config: cfg}
	// Two independent clients (separate connection pools) and one behind the proxy.
	a := store.NewS3(ep.Client(nil), ep.Bucket, ep.Prefix, cfg.AttemptTimeout)
	b := store.NewS3(ep.Client(nil), ep.Bucket, ep.Prefix, cfg.AttemptTimeout)
	proxy, err := storetest.NewProxy(ep.URL)
	if err != nil {
		return rep, err
	}
	defer proxy.Close()
	p := store.NewS3(ep.Client(proxy.HTTPClient()), ep.Bucket, ep.Prefix, 5*time.Second)

	add := func(name string, ok bool, format string, v ...any) {
		st := map[bool]string{true: "pass", false: "fail"}[ok]
		rep.Checks = append(rep.Checks, doctorCheck{name, st, fmt.Sprintf(format, v...)})
		fmt.Printf("%-28s %s %s\n", name, st, fmt.Sprintf(format, v...))
	}
	info := func(name, format string, v ...any) {
		rep.Checks = append(rep.Checks, doctorCheck{name, "info", fmt.Sprintf(format, v...)})
		fmt.Printf("%-28s info %s\n", name, fmt.Sprintf(format, v...))
	}
	defer func() {
		n, cerr := cleanup(ctx, a)
		add("cleanup", cerr == nil, "%d objects deleted under %s (%v)", n, ep.Prefix, cerr)
		rep.Attempts, rep.ProxyAttempts = sum(a.Counts(), b.Counts()), p.Counts()
		rep.ProxyRequests = len(proxy.Journal())
		var total uint64
		for _, n := range rep.ProxyAttempts.Attempts {
			total += n
		}
		add("physical-attempt-count", total == uint64(rep.ProxyRequests), "adapter counted %d, proxy saw %d", total, rep.ProxyRequests)
		rep.Finished, rep.Pass = time.Now().UTC(), true
		for _, c := range rep.Checks {
			rep.Pass = rep.Pass && c.Status != "fail"
		}
		if !rep.Pass && err == nil {
			err = errors.New("endpoint failed qualification")
		}
	}()

	basics(ctx, a, b, add)
	rep.CreateRace = createRace(ctx, []*store.S3{a, b}, cfg)
	add("create-race", rep.CreateRace.OneWinner == cfg.Rounds, "%d/%d rounds with one winner, %d unknown replies %v",
		rep.CreateRace.OneWinner, cfg.Rounds, rep.CreateRace.Unknown, rep.CreateRace.Violations)
	rep.CASRace = casRace(ctx, []*store.S3{a, b}, cfg)
	add("cas-race", rep.CASRace.OneWinner == cfg.Rounds, "%d/%d rounds with one winner, stale token rejected and winner visible on the other connection pool; %d unknown replies %v",
		rep.CASRace.OneWinner, cfg.Rounds, rep.CASRace.Unknown, rep.CASRace.Violations)
	faultChecks(ctx, p, a, proxy, add, info)
	if cfg.LatencySamples > 0 {
		rep.Latency = latency(ctx, a, cfg.LatencySamples)
		for _, k := range slices.Sorted(func(yield func(string) bool) {
			for k := range rep.Latency {
				if !yield(k) {
					return
				}
			}
		}) {
			d := rep.Latency[k]
			info("latency-"+k, "n=%d p50=%s p95=%s p99=%s max=%s errors=%d", d.N, d.P50, d.P95, d.P99, d.Max, d.Errors)
		}
	}
	return rep, nil
}

type addFunc func(name string, ok bool, format string, v ...any)

func is(err error, o store.Outcome) bool { return store.OutcomeOf(err) == o }

// basics is the Open probe (PROTOCOL §8.3) plus the read paths.
func basics(ctx context.Context, a, b *store.S3, add addFunc) {
	err1 := a.PutImmutable(ctx, "probe/x", []byte("x"))
	err2 := a.PutImmutable(ctx, "probe/x", []byte("x"))
	add("immutable-create", is(err1, store.OK) && is(err2, store.PreconditionFailed), "first %v, second %v", err1, err2)
	_, err := a.Get(ctx, "probe/absent", "", 1<<10)
	add("get-absent", is(err, store.NotFound), "%v", err)
	add("delete-absent", is(a.Delete(ctx, "probe/absent"), store.OK), "")

	e1, err1 := a.PutCAS(ctx, "probe/y", []byte("y1"), "")
	_, err2 = a.PutCAS(ctx, "probe/y", []byte("y1"), "")
	e2, err3 := a.PutCAS(ctx, "probe/y", []byte("y2"), e1)
	_, err4 := b.PutCAS(ctx, "probe/y", []byte("stale"), e1)
	add("cas-create-update-stale", is(err1, store.OK) && is(err2, store.PreconditionFailed) && is(err3, store.OK) && is(err4, store.PreconditionFailed),
		"create %v, recreate %v, update %v, stale %v", err1, err2, err3, err4)
	_, err5 := b.PutCAS(ctx, "probe/none", []byte("z"), e2)
	add("cas-missing-key", is(err5, store.PreconditionFailed), "If-Match on a missing key: %v", err5)
	_, err1 = b.Get(ctx, "probe/y", e2, 1<<10)
	o, err2 := b.Get(ctx, "probe/y", e1, 1<<10)
	add("conditional-get", is(err1, store.NotModified) && err2 == nil && string(o.Body) == "y2" && o.ETag == e2,
		"current token %v; old token %v body %q etag match %v", err1, err2, o.Body, o.ETag == e2)
	_, err = b.Get(ctx, "probe/y", "", 1)
	add("get-size-limit", is(err, store.Failed), "%v", err)

	for i := range 5 {
		a.PutImmutable(ctx, fmt.Sprintf("probe/list/%d", i), []byte{byte(i)})
	}
	keys := []string{}
	p1, more1, err1 := b.List(ctx, "probe/list/", "", 2)
	for _, e := range p1 {
		keys = append(keys, e.Key)
	}
	after := "probe/list/~" // past everything if the first page was empty
	if len(keys) > 0 {
		after = keys[len(keys)-1]
	}
	p2, more2, err2 := b.List(ctx, "probe/list/", after, 3)
	for _, e := range p2 {
		keys = append(keys, e.Key)
	}
	want := []string{"probe/list/0", "probe/list/1", "probe/list/2", "probe/list/3", "probe/list/4"}
	add("list-pages", err1 == nil && err2 == nil && more1 && !more2 && slices.Equal(keys, want), "%v more=%v,%v %v %v", keys, more1, more2, err1, err2)
}

// createRace: per round, contenders race If-None-Match: * on a fresh key.
func createRace(ctx context.Context, stores []*store.S3, cfg doctorConfig) race {
	r := race{Rounds: cfg.Rounds, Contenders: cfg.Contenders}
	for round := range cfg.Rounds {
		key := fmt.Sprintf("create/%06d", round)
		etags, errs := contend(ctx, stores, cfg.Contenders, func(s *store.S3, i int) (string, error) {
			return s.PutCAS(ctx, key, body(round, i), "")
		})
		judge(ctx, &r, stores, round, key, etags, errs)
	}
	return r
}

// casRace: per round, contenders race If-Match on the same current token.
// Then the winner must be visible on the other connection pool with the
// winner's token, the old token must be rejected, and a conditional GET with
// the new token must answer 304.
func casRace(ctx context.Context, stores []*store.S3, cfg doctorConfig) race {
	r := race{Rounds: cfg.Rounds, Contenders: cfg.Contenders}
	etag, err := stores[0].PutCAS(ctx, "cas", body(-1, 0), "")
	if err != nil {
		r.Violations = append(r.Violations, "setup: "+err.Error())
		return r
	}
	for round := range cfg.Rounds {
		prev := etag
		etags, errs := contend(ctx, stores, cfg.Contenders, func(s *store.S3, i int) (string, error) {
			return s.PutCAS(ctx, "cas", body(round, i), prev)
		})
		winner := judge(ctx, &r, stores, round, "cas", etags, errs)
		if winner < 0 {
			o, err := stores[1].Get(ctx, "cas", "", 1<<16) // continue from whatever is stored
			if err != nil {
				return r
			}
			etag = o.ETag
			continue
		}
		etag = etags[winner]
		_, stale := stores[1].PutCAS(ctx, "cas", []byte("stale"), prev)
		_, notMod := stores[1].Get(ctx, "cas", etag, 1<<16)
		if !is(stale, store.PreconditionFailed) || !is(notMod, store.NotModified) {
			r.OneWinner--
			r.violate(fmt.Sprintf("round %d: stale token %v, conditional get %v", round, stale, notMod))
		}
	}
	return r
}

func body(round, i int) []byte {
	return fmt.Appendf(bytes.Repeat([]byte{'.'}, 64), "round=%d contender=%d", round, i)
}

func contend(ctx context.Context, stores []*store.S3, n int, put func(*store.S3, int) (string, error)) ([]string, []error) {
	etags, errs := make([]string, n), make([]error, n)
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := range n {
		wg.Go(func() { <-start; etags[i], errs[i] = put(stores[i%len(stores)], i) })
	}
	close(start)
	wg.Wait()
	return etags, errs
}

// judge checks one round: exactly one OK, every other reply a definite
// PreconditionFailed or an Unknown, and the stored object (read through
// the second connection pool) equal to the winner's body and token.
func judge(ctx context.Context, r *race, stores []*store.S3, round int, key string, etags []string, errs []error) int {
	winner, others := -1, 0
	for i, err := range errs {
		switch store.OutcomeOf(err) {
		case store.OK:
			if winner >= 0 {
				r.violate(fmt.Sprintf("round %d: contenders %d and %d both won", round, winner, i))
				return -1
			}
			winner = i
		case store.PreconditionFailed:
			others++
		case store.Unknown:
			r.Unknown++
		default:
			r.violate(fmt.Sprintf("round %d: contender %d: %v", round, i, err))
			return -1
		}
	}
	o, err := stores[1].Get(ctx, key, "", 1<<16)
	switch {
	case winner < 0:
		r.violate(fmt.Sprintf("round %d: no definite winner (%d unknown)", round, len(errs)-others))
	case err != nil || !bytes.Equal(o.Body, body(round, winner)) || o.ETag != etags[winner]:
		r.violate(fmt.Sprintf("round %d: stored state is not the winner's (%v)", round, err))
		return -1
	default:
		r.OneWinner++
	}
	return winner
}

func (r *race) violate(s string) {
	if len(r.Violations) < 10 {
		r.Violations = append(r.Violations, s)
	}
}

// faultChecks drives the adapter through the proxy against the real endpoint.
func faultChecks(ctx context.Context, p, a *store.S3, proxy *storetest.Proxy, add addFunc, info func(string, string, ...any)) {
	plan := func(f func(storetest.Request) storetest.Fault) int {
		proxy.SetPlan(f)
		return len(proxy.Journal())
	}
	sent := func(from int) int { return len(proxy.Journal()) - from }
	e, err := p.PutCAS(ctx, "fault/h", body(0, 0), "")
	if err != nil {
		add("fault-setup", false, "%v", err)
		return
	}

	n := plan(func(storetest.Request) storetest.Fault { return storetest.Fault{Status: 503, Code: "SlowDown"} })
	_, err = p.PutCAS(ctx, "fault/h", body(1, 0), e)
	add("cas-5xx-one-attempt", is(err, store.Unknown) && sent(n) == 1, "%v after %d request(s)", err, sent(n))
	n = plan(func(storetest.Request) storetest.Fault { return storetest.Fault{Status: 503, Code: "SlowDown"} })
	_, err = p.Get(ctx, "fault/h", "", 1<<16)
	add("get-5xx-bounded-retry", is(err, store.Unknown) && sent(n) == 4, "%v after %d requests", err, sent(n))

	// Applied, reply lost: one request, Unknown, then visible.
	n = plan(func(storetest.Request) storetest.Fault { return storetest.Fault{Drop: true} })
	_, err = p.PutCAS(ctx, "fault/h", body(2, 0), e)
	o, gerr := a.Get(ctx, "fault/h", "", 1<<16)
	add("cas-lost-reply", is(err, store.Unknown) && sent(n) == 1 && gerr == nil && bytes.Equal(o.Body, body(2, 0)),
		"%v after %d request(s); applied: %v", err, sent(n), bytes.Equal(o.Body, body(2, 0)))
	e = o.ETag

	// Held until the client gave up, then applied: a timeout is not a no.
	hold := make(chan struct{})
	n = plan(func(storetest.Request) storetest.Fault { return storetest.Fault{Hold: hold} })
	short, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
	_, err = p.PutCAS(short, "fault/h", body(3, 0), e)
	cancel()
	before, _ := a.Get(ctx, "fault/h", "", 1<<16)
	close(hold)
	proxy.Wait()
	o, gerr = a.Get(ctx, "fault/h", "", 1<<16)
	add("cas-late-apply-after-cancel", is(err, store.Unknown) && bytes.Equal(before.Body, body(2, 0)) && gerr == nil && bytes.Equal(o.Body, body(3, 0)),
		"client saw %v; applied after the client left: %v", err, bytes.Equal(o.Body, body(3, 0)))
	e = o.ETag

	// Hedge: the first request stalls before reaching the store, the hedge
	// wins, the stalled one applies later and must fail its condition.
	hold = make(chan struct{})
	first := true // read and written only by the plan, under the proxy's lock
	n = plan(func(storetest.Request) storetest.Fault {
		if first {
			first = false
			return storetest.Fault{Hold: hold}
		}
		return storetest.Fault{}
	})
	_, errs, err := store.Hedge(ctx, 100*time.Millisecond, func(c context.Context) (string, error) {
		return p.PutCAS(c, "fault/h", body(4, 0), e)
	})
	close(hold)
	proxy.Wait()
	j := proxy.Journal()[n:]
	o, gerr = a.Get(ctx, "fault/h", "", 1<<16)
	add("cas-hedge", err == nil && len(errs) == 2 && len(j) == 2 && j[0].Upstream == 412 && j[1].Upstream == 200 && gerr == nil && bytes.Equal(o.Body, body(4, 0)),
		"hedge %v %v; stalled request answered %d after release", err, errs, j[0].Upstream)

	// Bodies broken on the way back are never returned as success.
	n = plan(func(storetest.Request) storetest.Fault { return storetest.Fault{Truncate: true} })
	_, err = p.Get(ctx, "fault/h", "", 1<<16)
	add("get-truncated-body", is(err, store.Unknown) && sent(n) == 4, "%v after %d requests", err, sent(n))
	n = plan(func(storetest.Request) storetest.Fault { return storetest.Fault{Corrupt: true} })
	o, err = p.Get(ctx, "fault/h", "", 1<<16)
	if is(err, store.Unknown) {
		add("get-corrupted-body", true, "rejected by the response checksum after %d requests", sent(n))
	} else {
		info("get-corrupted-body", "not detected in transport (%v): the endpoint returned no checksum; content hashes are the only guard", err)
	}
	// A request body altered after signing must be rejected, not stored.
	big := bytes.Repeat([]byte("payload."), 512)
	plan(func(storetest.Request) storetest.Fault { return storetest.Fault{CorruptRequest: true} })
	err = p.PutImmutable(ctx, "fault/corrupt", big)
	plan(nil)
	_, gerr = a.Get(ctx, "fault/corrupt", "", 1<<16)
	add("put-corrupted-request", is(err, store.Failed) && is(gerr, store.NotFound), "put %v; object %v", err, gerr)
}

// latency measures single-attempt create PUTs, GETs and chained CAS updates
// of 4 KiB and 32 KiB objects, eight at a time.
func latency(ctx context.Context, s *store.S3, n int) map[string]dist {
	res := map[string]dist{}
	for _, size := range []int{4 << 10, 32 << 10} {
		payload := bytes.Repeat([]byte{'l'}, size)
		label := fmt.Sprintf("%dk", size>>10)
		res["put-"+label] = measure(n, 8, func(i int) error {
			return ignoreEtag(s.PutCAS(ctx, fmt.Sprintf("lat/%s/%06d", label, i), payload, ""))
		})
		res["get-"+label] = measure(n, 8, func(i int) error {
			_, err := s.Get(ctx, fmt.Sprintf("lat/%s/%06d", label, i), "", int64(size))
			return err
		})
	}
	payload := bytes.Repeat([]byte{'c'}, 4<<10)
	etag, _ := s.PutCAS(ctx, "lat/cas", payload, "")
	res["cas-4k"] = measure(n, 1, func(int) error {
		e, err := s.PutCAS(ctx, "lat/cas", payload, etag)
		if err == nil {
			etag = e
		}
		return err
	})
	return res
}

func ignoreEtag(_ string, err error) error { return err }

func measure(n, workers int, f func(int) error) dist {
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
	d := dist{N: n}
	for _, err := range errs {
		if err != nil {
			d.Errors++
		}
	}
	slices.Sort(durs)
	q := func(p float64) string {
		return durs[min(n-1, int(p*float64(n)))].Round(100 * time.Microsecond).String()
	}
	d.P50, d.P95, d.P99, d.Max = q(0.50), q(0.95), q(0.99), q(1)
	return d
}

// cleanup deletes every object under the store's prefix and checks that
// nothing is left.
func cleanup(ctx context.Context, s *store.S3) (int, error) {
	deleted := 0
	for {
		page, _, err := s.List(ctx, "", "", 1000)
		if err != nil {
			return deleted, err
		}
		if len(page) == 0 {
			return deleted, nil
		}
		for _, e := range page {
			if err := s.Delete(ctx, e.Key); err != nil {
				return deleted, err
			}
			deleted++
		}
	}
}

func sum(x, y store.Counts) store.Counts {
	for i := range x.Attempts {
		x.Attempts[i] += y.Attempts[i]
		x.Failures[i] += y.Failures[i]
		x.Uploaded[i] += y.Uploaded[i]
		x.Downloaded[i] += y.Downloaded[i]
	}
	x.CASAttempts += y.CASAttempts
	x.CASConflicts += y.CASConflicts
	x.AmbiguousPuts += y.AmbiguousPuts
	return x
}
