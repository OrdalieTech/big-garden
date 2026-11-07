package store_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/OrdalieTech/big-garden/internal/store"
	"github.com/OrdalieTech/big-garden/internal/storetest"
)

const attempt = 2 * time.Second

// rig is an S3 adapter whose every request crosses a fault proxy to either
// an HTTP front-end over storetest.Mem or the local MinIO fixture.
type rig struct {
	s     *store.S3
	proxy *storetest.Proxy
	minio bool
}

func upstreams(t *testing.T, f func(t *testing.T, newRig func(opts ...func(*s3.Options)) *rig)) {
	for _, kind := range []string{"mem", "minio"} {
		t.Run(kind, func(t *testing.T) {
			var ep storetest.Endpoint
			if kind == "minio" {
				ep = storetest.MinIO(t)
			} else {
				srv := httptest.NewServer(storetest.Handler(storetest.NewMem()))
				t.Cleanup(srv.Close)
				ep = storetest.Endpoint{URL: srv.URL, Region: "us-east-1", Bucket: "b", Prefix: "ns", AccessKey: "a", SecretKey: "s"}
			}
			f(t, func(opts ...func(*s3.Options)) *rig {
				p, err := storetest.NewProxy(ep.URL)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(p.Close)
				c := s3.New(ep.Client(p.HTTPClient()).Options(), opts...)
				return &rig{s: store.NewS3(c, ep.Bucket, ep.Prefix, attempt), proxy: p, minio: kind == "minio"}
			})
		})
	}
}

func want(t *testing.T, err error, o store.Outcome) {
	t.Helper()
	if got := store.OutcomeOf(err); got != o {
		t.Fatalf("outcome %s, want %s (%v)", got, o, err)
	}
}

func ctx(t *testing.T) context.Context {
	c, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	return c
}

func TestS3Semantics(t *testing.T) {
	upstreams(t, func(t *testing.T, newRig func(...func(*s3.Options)) *rig) {
		s, c := newRig().s, ctx(t)
		_, err := s.Get(c, "absent", "", 100)
		want(t, err, store.NotFound)
		want(t, s.Delete(c, "absent"), store.OK)
		want(t, s.PutImmutable(c, "obj", []byte("one")), store.OK)
		want(t, s.PutImmutable(c, "obj", []byte("one")), store.PreconditionFailed)

		e1, err := s.PutCAS(c, "head", []byte("v1"), "")
		want(t, err, store.OK)
		_, err = s.PutCAS(c, "head", []byte("v1"), "")
		want(t, err, store.PreconditionFailed)
		e2, err := s.PutCAS(c, "head", []byte("v2"), e1)
		want(t, err, store.OK)
		_, err = s.PutCAS(c, "head", []byte("v3"), e1)
		want(t, err, store.PreconditionFailed) // stale token
		_, err = s.PutCAS(c, "nohead", []byte("v"), e2)
		want(t, err, store.PreconditionFailed) // If-Match on a missing key
		_, err = s.Get(c, "head", e2, 100)
		want(t, err, store.NotModified)
		o, err := s.Get(c, "head", e1, 100)
		want(t, err, store.OK)
		if string(o.Body) != "v2" || o.ETag != e2 {
			t.Fatalf("get %q %s, want v2 %s", o.Body, o.ETag, e2)
		}
		_, err = s.Get(c, "head", "", 1)
		want(t, err, store.Failed) // above max

		for i := range 5 {
			want(t, s.PutImmutable(c, fmt.Sprintf("dir/%d", i), []byte("x")), store.OK)
		}
		page, more, err := s.List(c, "dir/", "dir/1", 2)
		want(t, err, store.OK)
		if len(page) != 2 || page[0] != (store.Entry{Key: "dir/2", Size: 1}) || page[1].Key != "dir/3" || !more {
			t.Fatalf("list %v more=%v", page, more)
		}
		page, more, err = s.List(c, "dir/", "dir/3", 2)
		if err != nil || len(page) != 1 || more {
			t.Fatalf("last page %v more=%v %v", page, more, err)
		}
		_, _, err = s.List(c, "dir/", "", 1001)
		want(t, err, store.Failed)
		want(t, s.Delete(c, "dir/4"), store.OK)
		_, err = s.Get(c, "dir/4", "", 100)
		want(t, err, store.NotFound)
	})
}

// TestS3Classification injects every HTTP status class without forwarding,
// and checks the outcome and the number of physical requests: CAS is never
// retried; idempotent calls retry Unknown only, up to 4 attempts.
func TestS3Classification(t *testing.T) {
	cases := []struct {
		op       string
		status   int
		code     string
		outcome  store.Outcome
		requests int
	}{
		{"get", 304, "", store.NotModified, 1},
		{"get", 404, "NoSuchKey", store.NotFound, 1},
		{"get", 404, "NoSuchBucket", store.Failed, 1},
		{"get", 403, "AccessDenied", store.Failed, 1}, // never "not found"
		{"get", 400, "InvalidArgument", store.Failed, 1},
		{"get", 301, "PermanentRedirect", store.Failed, 1},
		{"get", 412, "PreconditionFailed", store.Failed, 1},
		{"get", 408, "RequestTimeout", store.Unknown, 4},
		{"get", 429, "SlowDown", store.Unknown, 4},
		{"get", 500, "InternalError", store.Unknown, 4},
		{"get", 503, "SlowDown", store.Unknown, 4},
		{"put", 412, "PreconditionFailed", store.PreconditionFailed, 1},
		{"put", 409, "ConditionalRequestConflict", store.Unknown, 4},
		{"put", 403, "AccessDenied", store.Failed, 1},
		{"put", 404, "NoSuchBucket", store.Failed, 1},
		{"put", 500, "InternalError", store.Unknown, 4},
		{"put", 200, "", store.OK, 1},
		{"put", 200, "InternalError", store.Unknown, 4},
		{"cas", 412, "PreconditionFailed", store.PreconditionFailed, 1},
		{"cas", 409, "ConditionalRequestConflict", store.PreconditionFailed, 1},
		{"cas", 404, "NoSuchKey", store.PreconditionFailed, 1},
		{"cas", 404, "NoSuchBucket", store.Failed, 1},
		{"cas", 403, "AccessDenied", store.Failed, 1},
		{"cas", 400, "BadDigest", store.Failed, 1},
		{"cas", 500, "InternalError", store.Unknown, 1},
		{"cas", 503, "SlowDown", store.Unknown, 1},
		{"cas", 200, "", store.Unknown, 1},              // success without ETag: applied, token unknown
		{"cas", 200, "InternalError", store.Unknown, 1}, // S3's 200 with an error document
		{"list", 500, "InternalError", store.Unknown, 4},
		{"list", 403, "AccessDenied", store.Failed, 1},
		{"delete", 404, "NoSuchKey", store.OK, 1},
		{"delete", 404, "NoSuchBucket", store.Failed, 1},
		{"delete", 403, "AccessDenied", store.Failed, 1},
		{"delete", 503, "SlowDown", store.Unknown, 4},
	}
	op := map[string]int{"get": 0, "put": 1, "cas": 1, "list": 2, "delete": 3}
	upstreams(t, func(t *testing.T, newRig func(...func(*s3.Options)) *rig) {
		for _, tc := range cases {
			// A caller's client that would retry 10 times must not matter.
			r := newRig(func(o *s3.Options) { o.RetryMaxAttempts = 10 })
			r.proxy.SetPlan(func(storetest.Request) storetest.Fault { return storetest.Fault{Status: tc.status, Code: tc.code} })
			c := ctx(t)
			var err error
			switch tc.op {
			case "get":
				_, err = r.s.Get(c, "k", `"etag"`, 100)
			case "put":
				err = r.s.PutImmutable(c, "k", []byte("body"))
			case "cas":
				_, err = r.s.PutCAS(c, "k", []byte("body"), `"etag"`)
			case "list":
				_, _, err = r.s.List(c, "k", "", 10)
			case "delete":
				err = r.s.Delete(c, "k")
			}
			name := fmt.Sprintf("%s %d %s", tc.op, tc.status, tc.code)
			if got := store.OutcomeOf(err); got != tc.outcome {
				t.Errorf("%s: outcome %s, want %s (%v)", name, got, tc.outcome, err)
			}
			if err != nil && strings.Contains(err.Error(), "injected") {
				t.Errorf("%s: provider message leaked into %q", name, err)
			}
			n := r.s.Counts()
			if len(r.proxy.Journal()) != tc.requests || n.Attempts[op[tc.op]] != uint64(tc.requests) {
				t.Errorf("%s: %d requests, %d counted, want %d", name, len(r.proxy.Journal()), n.Attempts[op[tc.op]], tc.requests)
			}
			if tc.op == "cas" && (n.CASAttempts != 1 || (n.CASConflicts == 1) != (tc.outcome == store.PreconditionFailed) ||
				(n.AmbiguousPuts == 1) != (tc.outcome == store.Unknown)) {
				t.Errorf("%s: CAS counters %+v", name, n)
			}
		}
	})
}

// TestS3TransportFaults forwards real requests and breaks the reply path.
func TestS3TransportFaults(t *testing.T) {
	upstreams(t, func(t *testing.T, newRig func(...func(*s3.Options)) *rig) {
		c := ctx(t)

		// Applied, reply dropped: one physical attempt, Unknown, visible.
		r := newRig()
		e1, err := r.s.PutCAS(c, "h", []byte("v1"), "")
		want(t, err, store.OK)
		r.proxy.SetPlan(func(q storetest.Request) storetest.Fault { return storetest.Fault{Drop: q.Seq == 1} })
		_, err = r.s.PutCAS(c, "h", []byte("v2"), e1)
		want(t, err, store.Unknown)
		o, err := r.s.Get(c, "h", "", 100)
		if err != nil || string(o.Body) != "v2" {
			t.Fatalf("dropped-reply CAS did not apply: %q %v", o.Body, err)
		}
		if j := r.proxy.Journal(); len(j) != 3 || j[1].Upstream != 200 || j[1].Relayed {
			t.Fatalf("journal %+v", j)
		}

		// Held past the client's deadline, then applied with nobody waiting.
		hold := make(chan struct{})
		release := sync.OnceFunc(func() { close(hold) })
		t.Cleanup(release) // before the proxy's Close, which waits for it
		r.proxy.SetPlan(func(q storetest.Request) storetest.Fault {
			if q.Seq == 3 {
				return storetest.Fault{Hold: hold}
			}
			return storetest.Fault{}
		})
		short, cancel := context.WithTimeout(c, 100*time.Millisecond)
		_, err = r.s.PutCAS(short, "h", []byte("v3"), o.ETag)
		cancel()
		want(t, err, store.Unknown)
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("deadline identity lost: %v", err)
		}
		cur, err := r.s.Get(c, "h", "", 100)
		if err != nil || string(cur.Body) != "v2" {
			t.Fatalf("held CAS visible before release: %q %v", cur.Body, err)
		}
		release()
		r.proxy.Wait()
		cur, err = r.s.Get(c, "h", "", 100)
		if err != nil || string(cur.Body) != "v3" {
			t.Fatalf("held CAS did not apply after the client left: %q %v", cur.Body, err)
		}

		// Reply delayed past the per-attempt deadline: applied, Unknown.
		r = newRig()
		r.proxy.SetPlan(func(storetest.Request) storetest.Fault { return storetest.Fault{Delay: attempt + time.Second} })
		_, err = r.s.PutCAS(c, "h", []byte("v4"), cur.ETag)
		want(t, err, store.Unknown)
		r.proxy.SetPlan(nil)
		cur, err = r.s.Get(c, "h", "", 100)
		if err != nil || string(cur.Body) != "v4" {
			t.Fatalf("delayed CAS did not apply: %q %v", cur.Body, err)
		}

		// Truncated body on the first attempt: retried, then whole.
		r = newRig()
		r.proxy.SetPlan(func(q storetest.Request) storetest.Fault { return storetest.Fault{Truncate: q.Seq == 0} })
		o, err = r.s.Get(c, "h", "", 100)
		if err != nil || string(o.Body) != "v4" || r.s.Counts().Attempts[0] != 2 || r.s.Counts().Failures[0] != 1 {
			t.Fatalf("truncation not retried: %q %v %+v", o.Body, err, r.s.Counts())
		}
		// Truncated every time: Unknown, never a short OK.
		r.proxy.SetPlan(func(storetest.Request) storetest.Fault { return storetest.Fault{Truncate: true} })
		_, err = r.s.Get(c, "h", "", 100)
		want(t, err, store.Unknown)

		// A corrupted body is caught by the SDK's response checksum when the
		// provider returns one; otherwise only content hashes can catch it.
		r.proxy.SetPlan(func(storetest.Request) storetest.Fault { return storetest.Fault{Corrupt: true} })
		o, err = r.s.Get(c, "h", "", 100)
		if r.minio {
			want(t, err, store.Unknown)
		} else if err != nil || bytes.Equal(o.Body, []byte("v4")) {
			t.Fatalf("corruption without checksum: %q %v", o.Body, err)
		}

		// A request body altered after signing is rejected by a real server
		// (payload hash/checksum) and never applied.
		if r.minio {
			r.proxy.SetPlan(func(storetest.Request) storetest.Fault { return storetest.Fault{CorruptRequest: true} })
			_, err = r.s.PutCAS(c, "h", []byte("v5"), cur.ETag)
			want(t, err, store.Failed)
			r.proxy.SetPlan(nil)
			o, err = r.s.Get(c, "h", "", 100)
			if err != nil || string(o.Body) != "v4" {
				t.Fatalf("corrupted request applied: %q %v", o.Body, err)
			}
		}
	})
}

func TestS3RetryStopsAtDeadline(t *testing.T) {
	upstreams(t, func(t *testing.T, newRig func(...func(*s3.Options)) *rig) {
		r := newRig()
		r.proxy.SetPlan(func(storetest.Request) storetest.Fault { return storetest.Fault{Status: 503, Code: "SlowDown"} })
		c, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
		defer cancel()
		start := time.Now()
		_, err := r.s.Get(c, "k", "", 10)
		want(t, err, store.Unknown)
		if time.Since(start) > time.Second || len(r.proxy.Journal()) > 3 {
			t.Fatalf("retries outlived the deadline: %v, %d requests", time.Since(start), len(r.proxy.Journal()))
		}
	})
}

// TestS3Hedge: the first CAS stalls before reaching the server; the hedge
// applies; the stalled request applies later and must fail its condition.
func TestS3Hedge(t *testing.T) {
	upstreams(t, func(t *testing.T, newRig func(...func(*s3.Options)) *rig) {
		r, c := newRig(), ctx(t)
		e1, err := r.s.PutCAS(c, "h", []byte("v1"), "")
		want(t, err, store.OK)
		hold := make(chan struct{})
		release := sync.OnceFunc(func() { close(hold) })
		t.Cleanup(release)
		r.proxy.SetPlan(func(q storetest.Request) storetest.Fault {
			if q.Seq == 1 {
				return storetest.Fault{Hold: hold}
			}
			return storetest.Fault{}
		})
		etag, errs, err := store.Hedge(c, 50*time.Millisecond, func(c context.Context) (string, error) {
			return r.s.PutCAS(c, "h", []byte("v2"), e1)
		})
		want(t, err, store.OK)
		if len(errs) != 2 || errs[1] != nil || store.OutcomeOf(errs[0]) != store.Unknown {
			t.Fatalf("hedge errs %v", errs)
		}
		release()
		r.proxy.Wait()
		j := r.proxy.Journal()
		if len(j) != 3 || j[2].Upstream != 200 || j[1].Upstream != 412 {
			t.Fatalf("journal %+v", j)
		}
		o, err := r.s.Get(c, "h", "", 100)
		if err != nil || string(o.Body) != "v2" || o.ETag != etag {
			t.Fatalf("after hedge %q %s %v, want v2 %s", o.Body, o.ETag, err, etag)
		}
	})
}

func TestHedge(t *testing.T) {
	unknown := &store.Error{Outcome: store.Unknown}
	conflict := &store.Error{Outcome: store.PreconditionFailed}
	c := context.Background()

	var calls atomic.Int32
	fast := func(context.Context) (string, error) { calls.Add(1); return "e", nil }
	if etag, errs, err := store.Hedge(c, time.Hour, fast); err != nil || etag != "e" || len(errs) != 1 || calls.Load() != 1 {
		t.Fatalf("fast: %s %v %v", etag, errs, err)
	}
	calls.Store(0)
	if _, errs, err := store.Hedge(c, 0, func(context.Context) (string, error) { calls.Add(1); return "", conflict }); err != conflict || len(errs) != 1 || calls.Load() != 1 {
		t.Fatalf("no hedge on a returned failure: %v %v", errs, err)
	}

	// Both fail: Unknown dominates a definite answer.
	calls.Store(0)
	slowFail := func(ctx context.Context) (string, error) {
		if calls.Add(1) == 1 {
			time.Sleep(50 * time.Millisecond)
			return "", conflict
		}
		return "", unknown
	}
	if _, errs, err := store.Hedge(c, 10*time.Millisecond, slowFail); err != unknown || len(errs) != 2 || errs[0] != conflict {
		t.Fatalf("both fail: %v %v", errs, err)
	}

	// The first OK wins and cancels the stalled request.
	calls.Store(0)
	stall := func(ctx context.Context) (string, error) {
		if calls.Add(1) == 1 {
			<-ctx.Done()
			return "", &store.Error{Outcome: store.Unknown, Err: ctx.Err()}
		}
		return "second", nil
	}
	etag, errs, err := store.Hedge(c, 10*time.Millisecond, stall)
	if err != nil || etag != "second" || len(errs) != 2 || store.OutcomeOf(errs[0]) != store.Unknown || errs[1] != nil {
		t.Fatalf("stall: %s %v %v", etag, errs, err)
	}
}
