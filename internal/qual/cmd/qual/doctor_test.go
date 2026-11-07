package main

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/OrdalieTech/big-garden/internal/storetest"
)

var quick = doctorConfig{Rounds: 20, Contenders: 8, LatencySamples: 4, AttemptTimeout: 10 * time.Second}

func quiet(t *testing.T) {
	stdout := os.Stdout
	os.Stdout, _ = os.Open(os.DevNull)
	t.Cleanup(func() { os.Stdout = stdout })
}

func statuses(rep doctorReport) map[string]string {
	m := map[string]string{}
	for _, c := range rep.Checks {
		m[c.Name] = c.Status
	}
	return m
}

func memEndpoint(t *testing.T, h http.Handler) storetest.Endpoint {
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	return storetest.Endpoint{URL: srv.URL, Region: "us-east-1", Bucket: "b", Prefix: "doctor", AccessKey: "a", SecretKey: "s"}
}

func TestDoctorPassesLinearizableStore(t *testing.T) {
	quiet(t)
	rep, err := runDoctor(context.Background(), memEndpoint(t, storetest.Handler(storetest.NewMem())), quick)
	if err != nil || !rep.Pass {
		t.Fatalf("%v %+v", err, rep.Checks)
	}
	st := statuses(rep)
	for _, name := range []string{"create-race", "cas-race", "cas-5xx-one-attempt", "cas-lost-reply", "cas-late-apply-after-cancel",
		"cas-hedge", "get-truncated-body", "get-corrupted-body", "put-corrupted-request", "physical-attempt-count", "cleanup"} {
		if st[name] != "pass" {
			t.Errorf("%s: %q", name, st[name])
		}
	}
	if rep.CASRace.OneWinner != quick.Rounds || rep.CreateRace.OneWinner != quick.Rounds || len(rep.Latency) != 5 {
		t.Fatalf("%+v %+v %v", rep.CreateRace, rep.CASRace, rep.Latency)
	}
}

// Negative control: a store that ignores If-Match / If-None-Match must fail
// both races and the stale-token check.
func TestDoctorRejectsStoreIgnoringConditions(t *testing.T) {
	quiet(t)
	m := storetest.NewMem()
	h := storetest.Handler(m)
	broken := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPut {
			_, key, _ := strings.Cut(strings.TrimPrefix(r.URL.Path, "/"), "/")
			body, _ := io.ReadAll(r.Body)
			r.Body = io.NopCloser(strings.NewReader(string(body)))
			if cur, ok := m.Peek(key); ok && r.Header.Get("If-Match") != "" {
				r.Header.Set("If-Match", cur.ETag) // condition ignored
			} else if ok {
				m.Delete(context.Background(), key)
			}
		}
		h.ServeHTTP(w, r)
	})
	rep, err := runDoctor(context.Background(), memEndpoint(t, broken), quick)
	st := statuses(rep)
	if err == nil || rep.Pass || st["create-race"] != "fail" || st["cas-race"] != "fail" || st["cas-create-update-stale"] != "fail" {
		t.Fatalf("broken store accepted: %v %v", err, st)
	}
}

func TestDoctorMinIO(t *testing.T) {
	ep := storetest.MinIO(t)
	quiet(t)
	rep, err := runDoctor(context.Background(), ep, quick)
	if err != nil || !rep.Pass {
		t.Fatalf("%v %+v", err, rep.Checks)
	}
}
