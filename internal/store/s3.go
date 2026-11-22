package store

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go/middleware"
	smithyhttp "github.com/aws/smithy-go/transport/http"
)

// Retry budget of the idempotent calls (Get, PutImmutable, List, Delete).
// PutCAS never retries (D3).
const (
	maxAttempts = 4
	backoffBase = 50 * time.Millisecond
	backoffCap  = 2 * time.Second
	maxListKeys = 1000
)

// Indexes of Counts arrays, in Stats.Storage order.
const (
	opGet = iota
	opPut // PutImmutable and PutCAS
	opList
	opDelete
)

// Counts are physical request counters: one per SDK attempt, which is one
// HTTP request because every call is forced to a single SDK attempt.
// Failures are attempts that ended Unknown or Failed.
type Counts struct {
	Attempts, Failures, Uploaded, Downloaded [4]uint64 // GET, PUT (incl. CAS), LIST, DELETE
	CASAttempts, CASConflicts, AmbiguousPuts uint64
}

// S3 is the Store over a caller-supplied SDK client. The caller's client keeps
// its own transport, credentials and endpoint; S3 overrides only retries.
type S3 struct {
	client  *s3.Client
	bucket  string
	prefix  string        // "" or ending in "/"
	attempt time.Duration // deadline of each physical attempt, body read included
	n       [4]struct{ attempts, failures, up, down atomic.Uint64 }
	cas     [3]atomic.Uint64 // attempts, conflicts, ambiguous PUTs
	skew    atomic.Int64     // nanoseconds, see Skew
}

// NewS3 returns a Store on bucket under prefix (joined with "/"; "" for the
// bucket root). Every physical attempt is bounded by attemptTimeout.
func NewS3(client *s3.Client, bucket, prefix string, attemptTimeout time.Duration) *S3 {
	if prefix != "" {
		prefix += "/"
	}
	return &S3{client: client, bucket: bucket, prefix: prefix, attempt: attemptTimeout}
}

// Counts returns a copy of the physical request counters.
func (s *S3) Counts() Counts {
	var c Counts
	for i := range s.n {
		c.Attempts[i], c.Failures[i] = s.n[i].attempts.Load(), s.n[i].failures.Load()
		c.Uploaded[i], c.Downloaded[i] = s.n[i].up.Load(), s.n[i].down.Load()
	}
	c.CASAttempts, c.CASConflicts, c.AmbiguousPuts = s.cas[0].Load(), s.cas[1].Load(), s.cas[2].Load()
	return c
}

// once forces exactly one SDK attempt whatever the client's retry settings:
// an applied write whose reply is lost must not come back as a 412 (§8.1).
// It also samples the store's clock from the Date header of every reply.
func (s *S3) once(o *s3.Options) {
	o.Retryer, o.RetryMaxAttempts = aws.NopRetryer{}, 0
	o.APIOptions = append(o.APIOptions, func(st *middleware.Stack) error {
		return st.Deserialize.Add(middleware.DeserializeMiddlewareFunc("garden-date", func(ctx context.Context, in middleware.DeserializeInput,
			next middleware.DeserializeHandler) (middleware.DeserializeOutput, middleware.Metadata, error) {
			out, md, err := next.HandleDeserialize(ctx, in)
			if r, ok := out.RawResponse.(*smithyhttp.Response); ok {
				if t, perr := http.ParseTime(r.Header.Get("Date")); perr == nil {
					s.skew.Store(int64(time.Since(t)))
				}
			}
			return out, md, err
		}), middleware.Before)
	})
}

// Skew is the local clock minus the store's, from the latest reply with a
// Date header (second resolution; 0 before any).
func (s *S3) Skew() time.Duration { return time.Duration(s.skew.Load()) }

func (s *S3) Get(ctx context.Context, key, ifNoneMatch string, max int64) (Object, error) {
	var obj Object
	err := retry(ctx, func() (err error) { obj, err = s.get(ctx, key, ifNoneMatch, max); return err })
	return obj, err
}

func (s *S3) get(ctx context.Context, key, ifNoneMatch string, max int64) (Object, error) {
	ctx, cancel := context.WithTimeout(ctx, s.attempt)
	defer cancel()
	in := &s3.GetObjectInput{Bucket: &s.bucket, Key: aws.String(s.prefix + key), ChecksumMode: types.ChecksumModeEnabled}
	if ifNoneMatch != "" {
		in.IfNoneMatch = &ifNoneMatch
	}
	out, err := s.client.GetObject(ctx, in, s.once)
	if err != nil {
		return Object{}, s.done(opGet, 0, 0, classify("get", key, err))
	}
	defer out.Body.Close()
	if out.ContentLength == nil || out.ETag == nil || *out.ETag == "" {
		return Object{}, s.done(opGet, 0, 0, failed("get", key, Failed, "response without Content-Length or ETag"))
	}
	if *out.ContentLength > max {
		return Object{}, s.done(opGet, 0, 0, failed("get", key, Failed, fmt.Sprintf("object of %d bytes exceeds %d", *out.ContentLength, max)))
	}
	body, err := io.ReadAll(io.LimitReader(out.Body, max+1))
	switch {
	case err != nil: // truncated body, checksum mismatch, deadline
		return Object{}, s.done(opGet, 0, int64(len(body)), &Error{Outcome: Unknown, Op: "get", Key: key, Err: err})
	case int64(len(body)) != *out.ContentLength:
		return Object{}, s.done(opGet, 0, int64(len(body)), failed("get", key, Unknown, "body length differs from Content-Length"))
	}
	return Object{Body: body, ETag: *out.ETag}, s.done(opGet, 0, int64(len(body)), nil)
}

func (s *S3) PutImmutable(ctx context.Context, key string, body []byte) error {
	return retry(ctx, func() error { _, err := s.put(ctx, "put", key, body, ""); return err })
}

func (s *S3) PutCAS(ctx context.Context, key string, body []byte, ifMatch string) (string, error) {
	s.cas[0].Add(1)
	etag, err := s.put(ctx, "cas", key, body, ifMatch)
	if OutcomeOf(err) == PreconditionFailed {
		s.cas[1].Add(1)
	}
	return etag, err
}

// put makes one physical conditional PUT: If-Match ifMatch, or
// If-None-Match: * when ifMatch is "".
func (s *S3) put(ctx context.Context, op, key string, body []byte, ifMatch string) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, s.attempt)
	defer cancel()
	// The CRC lets the server reject a body altered in transit and lets Get
	// verify the stored bytes end to end.
	in := &s3.PutObjectInput{Bucket: &s.bucket, Key: aws.String(s.prefix + key), ChecksumAlgorithm: types.ChecksumAlgorithmCrc32,
		Body: bytes.NewReader(body), ContentLength: aws.Int64(int64(len(body)))}
	if ifMatch == "" {
		in.IfNoneMatch = aws.String("*")
	} else {
		in.IfMatch = &ifMatch
	}
	out, err := s.client.PutObject(ctx, in, s.once)
	if err == nil && op == "cas" && (out.ETag == nil || *out.ETag == "") {
		// Applied, but the next CAS needs the token: let resolve read it.
		err = failed(op, key, Unknown, "success without ETag")
	} else if err != nil {
		err = classify(op, key, err)
	}
	if OutcomeOf(err) == Unknown {
		s.cas[2].Add(1)
	}
	if err != nil {
		return "", s.done(opPut, int64(len(body)), 0, err)
	}
	return aws.ToString(out.ETag), s.done(opPut, int64(len(body)), 0, nil)
}

func (s *S3) List(ctx context.Context, prefix, after string, limit int) ([]Entry, bool, error) {
	if limit < 1 || limit > maxListKeys {
		return nil, false, failed("list", prefix, Failed, "limit outside 1..1000")
	}
	var page []Entry
	var more bool
	err := retry(ctx, func() (err error) { page, more, err = s.list(ctx, prefix, after, limit); return err })
	return page, more, err
}

func (s *S3) list(ctx context.Context, prefix, after string, limit int) ([]Entry, bool, error) {
	ctx, cancel := context.WithTimeout(ctx, s.attempt)
	defer cancel()
	in := &s3.ListObjectsV2Input{Bucket: &s.bucket, Prefix: aws.String(s.prefix + prefix), MaxKeys: aws.Int32(int32(limit))}
	if after != "" {
		in.StartAfter = aws.String(s.prefix + after)
	}
	out, err := s.client.ListObjectsV2(ctx, in, s.once)
	if err != nil {
		return nil, false, s.done(opList, 0, 0, classify("list", prefix, err))
	}
	page := make([]Entry, 0, len(out.Contents))
	last := after
	for _, o := range out.Contents {
		k := aws.ToString(o.Key)
		if len(page) == limit || !strings.HasPrefix(k, s.prefix+prefix) || k[len(s.prefix):] <= last || o.Size == nil {
			return nil, false, s.done(opList, 0, 0, failed("list", prefix, Failed, "malformed listing"))
		}
		last = k[len(s.prefix):]
		page = append(page, Entry{Key: last, Size: *o.Size})
	}
	return page, aws.ToBool(out.IsTruncated), s.done(opList, 0, 0, nil)
}

func (s *S3) Delete(ctx context.Context, key string) error {
	return retry(ctx, func() error {
		ctx, cancel := context.WithTimeout(ctx, s.attempt)
		defer cancel()
		_, err := s.client.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: &s.bucket, Key: aws.String(s.prefix + key)}, s.once)
		if err = classify("delete", key, err); OutcomeOf(err) == NotFound {
			err = nil
		}
		return s.done(opDelete, 0, 0, err)
	})
}

// done counts one physical attempt and returns err.
func (s *S3) done(op int, up, down int64, err error) error {
	n := &s.n[op]
	n.attempts.Add(1)
	n.up.Add(uint64(up))
	n.down.Add(uint64(down))
	if o := OutcomeOf(err); o == Unknown || o == Failed {
		n.failures.Add(1)
	}
	return err
}

// retry runs f until it returns anything but Unknown, at most maxAttempts
// times, sleeping a full-jitter backoff in between and never past ctx.
func retry(ctx context.Context, f func() error) error {
	for i := 0; ; i++ {
		err := f()
		if OutcomeOf(err) != Unknown || i+1 == maxAttempts {
			return err
		}
		t := time.NewTimer(rand.N(min(backoffCap, backoffBase<<i)))
		select {
		case <-ctx.Done():
			t.Stop()
			return err
		case <-t.C:
		}
	}
}

// classify maps an SDK error to an *Error. Only an HTTP status proves
// anything: no response at all (transport error, deadline, cancellation
// after sending) is Unknown, as are 2xx with an unreadable reply, 408, 429
// and 5xx. 403 is Failed, never NotFound; a 404 is NotFound only with
// NoSuchKey, so a missing bucket is never an empty namespace.
func classify(op, key string, err error) error {
	if err == nil {
		return nil
	}
	var re interface{ HTTPStatusCode() int }
	if !errors.As(err, &re) || re.HTTPStatusCode() == 0 { // no response
		return &Error{Outcome: Unknown, Op: op, Key: key, Err: err}
	}
	code := ""
	if ae := (interface{ ErrorCode() string })(nil); errors.As(err, &ae) {
		code = ae.ErrorCode()
	}
	o := Failed
	switch st := re.HTTPStatusCode(); {
	case st == 304 && op == "get":
		o = NotModified
	case st == 404 && code == "NoSuchKey" && op == "cas":
		o = PreconditionFailed // If-Match on a missing key
	case st == 404 && code == "NoSuchKey" && (op == "get" || op == "delete"):
		o = NotFound
	case st == 412 && (op == "put" || op == "cas"), st == 409 && op == "cas":
		o = PreconditionFailed
	case st < 300, st == 408, st == 409, st == 429, st >= 500:
		o = Unknown
	}
	// Sanitized: status and code only, never the provider's message body.
	return &Error{Outcome: o, Op: op, Key: key, Err: fmt.Errorf("http %d %s", re.HTTPStatusCode(), code)}
}

func failed(op, key string, o Outcome, msg string) error {
	return &Error{Outcome: o, Op: op, Key: key, Err: errors.New(msg)}
}

// sized bounds one attempt moving n bytes: the attempt timeout plus n at
// minRate (PLAN §11.2: larger objects get bounded, size-based timeouts).
func (s *S3) sized(ctx context.Context, n int64) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, s.attempt+time.Duration(n)*time.Second/minRate)
}

// minRate is the slowest transfer an attempt tolerates, in bytes per second.
const minRate = 256 << 10

// partSizeFor picks a multipart part size at least PartBytes that keeps the
// part count at or below 500 (well under S3 providers' 1,000-part limit for
// multi-GiB objects).
func partSizeFor(size int64) int64 {
	const maxParts = 500
	part := int64(PartBytes)
	if minPart := (size + maxParts - 1) / maxParts; minPart > part {
		part = (minPart + (1 << 20) - 1) &^ ((1 << 20) - 1)
	}
	return part
}

func (s *S3) PutFile(ctx context.Context, key string, body io.ReaderAt, size int64, sum [32]byte) error {
	k := aws.String(s.prefix + key)
	if size <= PartBytes {
		return retry(ctx, func() error {
			ctx, cancel := s.sized(ctx, size)
			defer cancel()
			_, err := s.client.PutObject(ctx, &s3.PutObjectInput{Bucket: &s.bucket, Key: k, Body: io.NewSectionReader(body, 0, size),
				ContentLength: aws.Int64(size), ChecksumSHA256: b64(sum[:]), IfNoneMatch: aws.String("*")}, s.once)
			return s.done(opPut, size, 0, classify("put", key, err))
		})
	}
	var up *s3.CreateMultipartUploadOutput
	if err := retry(ctx, func() (err error) {
		up, err = s.client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{Bucket: &s.bucket, Key: k,
			ChecksumAlgorithm: types.ChecksumAlgorithmSha256}, s.once)
		return s.done(opPut, 0, 0, classify("put", key, err))
	}); err != nil {
		return err
	}
	complete := false
	defer func() {
		if !complete { // parts are billed until aborted
			s.client.AbortMultipartUpload(context.WithoutCancel(ctx), &s3.AbortMultipartUploadInput{Bucket: &s.bucket, Key: k, UploadId: up.UploadId})
		}
	}()
	partSize := partSizeFor(size)
	parts := make([]types.CompletedPart, (size+partSize-1)/partSize)
	errs := make([]error, len(parts))
	sem, wg := make(chan struct{}, 4), sync.WaitGroup{}
	for i := range parts {
		sem <- struct{}{}
		wg.Go(func() {
			defer func() { <-sem }()
			off := int64(i) * partSize
			part := io.NewSectionReader(body, off, min(partSize, size-off))
			h := sha256.New()
			if _, errs[i] = io.Copy(h, part); errs[i] != nil {
				return
			}
			errs[i] = retry(ctx, func() error {
				ctx, cancel := s.sized(ctx, part.Size())
				defer cancel()
				part.Seek(0, io.SeekStart)
				out, err := s.client.UploadPart(ctx, &s3.UploadPartInput{Bucket: &s.bucket, Key: k, UploadId: up.UploadId,
					PartNumber: aws.Int32(int32(i + 1)), Body: part, ContentLength: aws.Int64(part.Size()), ChecksumSHA256: b64(h.Sum(nil))}, s.once)
				if err == nil {
					parts[i] = types.CompletedPart{ETag: out.ETag, PartNumber: aws.Int32(int32(i + 1)), ChecksumSHA256: out.ChecksumSHA256}
				}
				return s.done(opPut, part.Size(), 0, classify("put", key, err))
			})
		})
	}
	wg.Wait()
	if err := errors.Join(errs...); err != nil {
		return err
	}
	_, err := s.client.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{Bucket: &s.bucket, Key: k, UploadId: up.UploadId,
		MultipartUpload: &types.CompletedMultipartUpload{Parts: parts}}, s.once) // not retried: a lost reply is Unknown
	complete = err == nil
	return s.done(opPut, 0, 0, classify("put", key, err))
}

func (s *S3) Copy(ctx context.Context, from, to string) error {
	return retry(ctx, func() error {
		src := strings.Split(s.bucket+"/"+s.prefix+from, "/")
		for i := range src {
			src[i] = url.PathEscape(src[i])
		}
		_, err := s.client.CopyObject(ctx, &s3.CopyObjectInput{Bucket: &s.bucket, Key: aws.String(s.prefix + to),
			CopySource: aws.String(strings.Join(src, "/"))}, s.once)
		return s.done(opPut, 0, 0, classify("get", to, err)) // "get": a missing source is NotFound
	})
}

func b64(b []byte) *string { return aws.String(base64.StdEncoding.EncodeToString(b)) }

func (s *S3) Open(ctx context.Context, key string, off, n int64) (io.ReadCloser, error) {
	var out *s3.GetObjectOutput
	var cancel context.CancelFunc
	err := retry(ctx, func() (err error) {
		var actx context.Context
		actx, cancel = s.sized(ctx, n) // the body is read after Open returns: Close ends the attempt
		out, err = s.client.GetObject(actx, &s3.GetObjectInput{Bucket: &s.bucket, Key: aws.String(s.prefix + key),
			Range: aws.String(fmt.Sprintf("bytes=%d-%d", off, off+n-1))}, s.once)
		if err != nil {
			cancel()
		}
		return s.done(opGet, 0, n, classify("get", key, err))
	})
	if err != nil {
		return nil, err
	}
	return body{io.LimitReader(out.Body, n), out.Body, cancel}, nil
}

type body struct {
	io.Reader
	c      io.Closer
	cancel context.CancelFunc
}

func (b body) Close() error {
	defer b.cancel()
	return b.c.Close()
}
