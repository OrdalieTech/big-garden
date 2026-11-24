package store

import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

func TestSizedDurationNoOverflow(t *testing.T) {
	s := &S3{attempt: 30 * time.Second}
	// Exact size of a ~9.6 GB snapshot (larger than math.MaxInt64 / 1e9 = 9,223,372,036 bytes)
	// previously overflowed time.Duration(n)*time.Second to a negative timeout.
	for _, n := range []int64{0, 1, 64 << 20, 9_614_813_180, 32 << 30, math.MaxInt64} {
		d := s.sizedDuration(n)
		if d < s.attempt {
			t.Fatalf("sizedDuration(%d) = %v < %v", n, d, s.attempt)
		}
	}
	got := s.sizedDuration(9_614_813_180)
	want := 30*time.Second + time.Duration(9_614_813_180/minRate)*time.Second + time.Duration(9_614_813_180%minRate)*time.Second/minRate
	if got != want {
		t.Fatalf("sizedDuration(9.6GB) = %v, want %v", got, want)
	}
}

func TestS3OpenChunkedAndResumesMidStream(t *testing.T) {
	origChunk := openChunkSize
	openChunkSize = 16 << 10 // 16 KiB chunks so a 100 KiB payload exercises 7 chunks
	t.Cleanup(func() { openChunkSize = origChunk })

	payload := make([]byte, 100<<10+123)
	_, _ = rand.Read(payload)

	var reqs atomic.Int32
	var droppedOnce atomic.Bool
	var resumedAt40K atomic.Bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := reqs.Add(1)
		rng := strings.TrimPrefix(r.Header.Get("Range"), "bytes=")
		parts := strings.Split(rng, "-")
		if len(parts) != 2 {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		start, err1 := strconv.ParseInt(parts[0], 10, 64)
		end, err2 := strconv.ParseInt(parts[1], 10, 64)
		if err1 != nil || err2 != nil || start < 0 || start >= int64(len(payload)) {
			w.WriteHeader(http.StatusRequestedRangeNotSatisfiable)
			return
		}
		if start == 40<<10 {
			resumedAt40K.Store(true)
		}
		if end >= int64(len(payload)) {
			end = int64(len(payload)) - 1
		}
		chunk := payload[start : end+1]
		w.Header().Set("Content-Length", strconv.Itoa(len(chunk)))
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(payload)))
		w.WriteHeader(http.StatusPartialContent)
		// Drop the TCP connection mid-body on chunk #3 (once) to verify exact-offset resume.
		if n == 3 && droppedOnce.CompareAndSwap(false, true) {
			half := len(chunk) / 2
			_, _ = w.Write(chunk[:half])
			if fl, ok := w.(http.Flusher); ok {
				fl.Flush()
			}
			if hj, ok := w.(http.Hijacker); ok {
				if conn, _, err := hj.Hijack(); err == nil {
					_ = conn.Close()
					return
				}
			}
			return
		}
		_, _ = w.Write(chunk)
	}))
	defer srv.Close()

	client := s3.New(s3.Options{
		Region: "us-east-1", UsePathStyle: true, BaseEndpoint: aws.String(srv.URL),
		Credentials: aws.AnonymousCredentials{},
		HTTPClient:  srv.Client(),
	})
	st := NewS3(client, "b", "ns", 5*time.Second)

	// 1. Full object streamed across multiple chunks with a mid-stream connection drop.
	rc, err := st.Open(context.Background(), "big.ltx", 0, int64(len(payload)))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	got, err := io.ReadAll(rc)
	_ = rc.Close()
	if err != nil {
		t.Fatalf("ReadAll after mid-stream drop: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("payload mismatch: got %d bytes, want %d", len(got), len(payload))
	}
	if !droppedOnce.Load() || !resumedAt40K.Load() {
		t.Fatalf("expected injected mid-stream drop and exact resume at 40 KiB (dropped=%v resumed=%v)", droppedOnce.Load(), resumedAt40K.Load())
	}
	if reqs.Load() < 7 {
		t.Fatalf("expected >=7 chunk/resume requests, got %d", reqs.Load())
	}

	// 2. Requesting more bytes than the object length (as putObject's body.n+1 check does) clamps cleanly.
	rc2, err := st.Open(context.Background(), "big.ltx", int64(len(payload))-500, 1000)
	if err != nil {
		t.Fatalf("Open short tail: %v", err)
	}
	tail, err := io.ReadAll(rc2)
	_ = rc2.Close()
	if err != nil || !bytes.Equal(tail, payload[len(payload)-500:]) {
		t.Fatalf("short tail mismatch: len=%d err=%v", len(tail), err)
	}
}
