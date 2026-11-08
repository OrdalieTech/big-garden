package garden

import (
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/OrdalieTech/big-garden/internal/store"
	"github.com/OrdalieTech/big-garden/internal/storetest"
)

// faultStore is the store of a fault-scripted core test: a Mem, or with
// GARDEN_TEST_MINIO set the S3 adapter on MinIO through a storetest.Proxy
// that translates the same plans into HTTP faults on signed requests.
type faultStore interface {
	store.Store
	SetPlan(func(storetest.Attempt) storetest.MemFault)
	Journal() []storetest.Attempt
}

// proxied is the S3 adapter through a fault proxy. Plans see Attempts with
// store keys: Op "cas" is a PUT with If-Match, or with If-None-Match on a
// head; Cond is If-Match for a PUT and If-None-Match for a GET.
type proxied struct {
	store.Store
	p    *storetest.Proxy
	pfx  string // "prefix/"
	root string // "/bucket/prefix/"
}

// newStore is the store of a core test (see faultStore).
func newStore(t *testing.T) faultStore {
	if os.Getenv(storetest.MinIOEnv) == "" {
		return storetest.NewMem()
	}
	ep := storetest.MinIO(t)
	p, err := storetest.NewProxy(ep.URL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(p.Close) // after the object cleanup below
	st := &proxied{store.NewS3(ep.Client(p.HTTPClient()), ep.Bucket, ep.Prefix, 2*time.Second), p, ep.Prefix + "/", "/" + ep.Bucket + "/" + ep.Prefix + "/"}
	t.Cleanup(func() { // after the test's Nodes closed (cleanups run last-in first-out)
		st.SetPlan(nil)
		for range 100 {
			page, _, err := st.List(bg, "", "", 1000)
			if err != nil || len(page) == 0 {
				return
			}
			for _, o := range page {
				st.Delete(bg, o.Key)
			}
		}
	})
	return st
}

func (s *proxied) attempt(r storetest.Request) storetest.Attempt {
	a := storetest.Attempt{Seq: r.Seq, Key: strings.TrimPrefix(r.Path, s.root), Body: r.Body}
	q, _ := url.ParseQuery(r.RawQuery)
	switch {
	case r.Method == "GET" && q.Has("list-type"):
		a.Op, a.Key, a.Cond = "list", strings.TrimPrefix(q.Get("prefix"), s.pfx), strings.TrimPrefix(q.Get("start-after"), s.pfx)
	case r.Method == "GET":
		a.Op, a.Cond = "get", r.IfNoneMatch
	case r.Method == "PUT" && (r.IfMatch != "" || strings.HasSuffix(a.Key, "/head.json")):
		a.Op, a.Cond = "cas", r.IfMatch
	case r.Method == "PUT":
		a.Op = "put"
	case r.Method == "DELETE":
		a.Op = "delete"
	}
	return a
}

func (s *proxied) SetPlan(plan func(storetest.Attempt) storetest.MemFault) {
	if plan == nil {
		s.p.SetPlan(nil)
		return
	}
	s.p.SetPlan(func(r storetest.Request) storetest.Fault {
		f := plan(s.attempt(r))
		switch {
		case f.Body != nil:
			panic("proxied: MemFault.Body is Mem-only")
		case f.Reject == store.Unknown:
			return storetest.Fault{Status: 503, Code: "SlowDown"}
		case f.Reject == store.Failed:
			return storetest.Fault{Status: 403, Code: "AccessDenied"}
		case f.Reject == store.PreconditionFailed:
			return storetest.Fault{Status: 412, Code: "PreconditionFailed"}
		}
		return storetest.Fault{Hold: f.Hold, Drop: f.LoseReply}
	})
}

func (s *proxied) Journal() []storetest.Attempt {
	var out []storetest.Attempt
	for _, r := range s.p.Journal() {
		out = append(out, s.attempt(r))
	}
	return out
}
