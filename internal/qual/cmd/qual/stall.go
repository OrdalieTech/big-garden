package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"

	"github.com/OrdalieTech/big-garden/internal/store"
	"github.com/OrdalieTech/big-garden/internal/storetest"
)

// stall measures whether a PUT whose body stalls after its headers blocks
// GETs of the same key, per condition (If-Match on the current ETag, none,
// If-None-Match: * on an absent key). A raw signed PUT declares
// Content-Length, sends half the body, stalls, then sends the rest; GETs of
// the key start one second into the stall and are timed. Everything is under
// a fresh random prefix and deleted afterwards.
func stall(args []string) error {
	fs := flag.NewFlagSet("stall", flag.ContinueOnError)
	local := fs.Bool("local", false, "use the pinned local MinIO fixture")
	remote := fs.Bool("remote", false, "use QUAL_S3_* and AWS_* (missing: blocked_missing_remote_configuration)")
	hold := fs.Duration("stall", 40*time.Second, "how long the PUT body stalls")
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
	ep = ep.Sub("stall")
	ctx := context.Background()
	s := store.NewS3(ep.Client(nil), ep.Bucket, ep.Prefix, time.Minute)
	defer func() {
		n, err := cleanup(ctx, s)
		fmt.Printf("cleanup: %d objects deleted under %s (%v)\n", n, ep.Prefix, err)
	}()
	for _, c := range []struct {
		name, key, ifMatch, ifNoneMatch string
		race                            bool
	}{
		{"if-match", "if-match/head.json", "current", "", false},
		{"unconditional", "plain/head.json", "", "", false},
		{"if-none-match", "if-none-match/head.json", "", "*", false},
		{"if-match+race", "if-match-race/head.json", "current", "", true},
		{"if-none+race", "if-none-match-race/head.json", "", "*", true},
	} {
		etag := ""
		if c.ifNoneMatch == "" { // the key exists, as a head being updated does
			if etag, err = s.PutCAS(ctx, c.key, []byte(`{"v":0}`), ""); err != nil {
				return err
			}
		}
		if c.ifMatch != "" {
			c.ifMatch = etag
		}
		stallOne(ctx, ep, s, c.name, c.key, c.ifMatch, c.ifNoneMatch, c.race, *hold)
	}
	return nil
}

func stallOne(ctx context.Context, ep storetest.Endpoint, s *store.S3, name, key, ifMatch, ifNoneMatch string, race bool, hold time.Duration) {
	body := []byte(strings.Repeat("x", 2048))
	url := strings.TrimSuffix(ep.URL, "/") + "/" + ep.Bucket + "/" + ep.Prefix + "/" + key
	pr, pw := io.Pipe()
	req, _ := http.NewRequestWithContext(ctx, "PUT", url, pr)
	req.ContentLength = int64(len(body))
	if ifMatch != "" {
		req.Header.Set("If-Match", ifMatch)
	}
	if ifNoneMatch != "" {
		req.Header.Set("If-None-Match", ifNoneMatch)
	}
	sum := sha256.Sum256(body)
	hash := hex.EncodeToString(sum[:])
	req.Header.Set("X-Amz-Content-Sha256", hash)
	err := v4.NewSigner().SignHTTP(ctx, aws.Credentials{AccessKeyID: ep.AccessKey, SecretAccessKey: ep.SecretKey},
		req, hash, "s3", ep.Region, time.Now())
	if err != nil {
		fmt.Printf("%-14s sign: %v\n", name, err)
		return
	}
	type result struct {
		status int
		err    error
		at     time.Duration
	}
	start := time.Now()
	put := make(chan result, 1)
	go func() {
		resp, err := (&http.Client{Transport: http.DefaultTransport.(*http.Transport).Clone()}).Do(req)
		r := result{err: err, at: time.Since(start)}
		if resp != nil {
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			r.status = resp.StatusCode
		}
		put <- r
	}()
	go func() {
		pw.Write(body[:len(body)/2]) // headers and half the body are on the wire
		time.Sleep(hold)
		pw.Write(body[len(body)/2:])
		pw.Close()
	}()
	time.Sleep(time.Second)
	var gets []string
	for i := range 3 { // a fresh connection pool per GET, as another node would have
		r := store.NewS3(ep.Client(nil), ep.Bucket, ep.Prefix, time.Minute+hold)
		t := time.Now()
		o, err := r.Get(ctx, key, "", 1<<16)
		gets = append(gets, fmt.Sprintf("get%d=%s(%s,%q)", i, time.Since(t).Round(time.Millisecond), outcome(err), short(o.Body)))
	}
	if race { // a competing CAS on the same condition while the stalled one is open: one winner at most
		r := store.NewS3(ep.Client(nil), ep.Bucket, ep.Prefix, time.Minute+hold)
		t := time.Now()
		_, err := r.PutCAS(ctx, key, []byte(`{"v":1}`), ifMatch)
		gets = append(gets, fmt.Sprintf("racer-cas=%s(%s)", time.Since(t).Round(time.Millisecond), outcome(err)))
	}
	pr0 := <-put
	o, err := s.Get(ctx, key, "", 1<<16)
	fmt.Printf("%-14s stall=%s %s put=%d(%v)@%s final=(%s,%q)\n", name, hold, strings.Join(gets, " "), pr0.status, pr0.err,
		pr0.at.Round(time.Millisecond), outcome(err), short(o.Body))
}

func outcome(err error) string {
	if err == nil {
		return "ok"
	}
	return fmt.Sprint(store.OutcomeOf(err))
}

func short(b []byte) string {
	if len(b) > 12 {
		return string(b[:12]) + "..."
	}
	return string(b)
}
