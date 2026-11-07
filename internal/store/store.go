// Package store is the private storage seam between the library and S3
// (PLAN §8.1). It is a test seam, not a provider plug-in API: the only
// production implementation is the S3 adapter; tests use internal/storetest.
//
// Every method makes the requests it documents and nothing more: no hidden
// retries of conditional writes, no LIST-based discovery. Keys are relative
// to the namespace prefix; the implementation joins them to it.
package store

import (
	"context"
	"errors"
	"time"
)

// Outcome classifies a storage call. Every error a Store returns is an
// *Error carrying one of the non-OK outcomes.
type Outcome int

const (
	// OK: the call took effect exactly as requested (nil error).
	OK Outcome = iota
	// NotModified: Get with ifNoneMatch, and the current ETag equals it.
	NotModified
	// NotFound: the key definitely does not exist (HTTP 404).
	NotFound
	// PreconditionFailed: a conditional write definitely did not apply
	// because its condition was false (HTTP 412, or 409 for a conflicting
	// concurrent conditional write). For PutCAS this does NOT prove that an
	// earlier ambiguous attempt with the same condition did not apply.
	PreconditionFailed
	// Unknown: no definite answer. A write may have applied, now or later
	// (timeout, cancellation, transport error, 5xx); a read returned nothing
	// usable. Resolve writes by reading; never assume either way.
	Unknown
	// Failed: definitely not applied and not worth retrying unchanged
	// (403, 400, a body above the caller's bound, a malformed response).
	Failed
)

func (o Outcome) String() string {
	return [...]string{"ok", "not_modified", "not_found", "precondition_failed", "unknown", "failed"}[o]
}

// Error is the error type of every Store method.
type Error struct {
	Outcome Outcome
	Op      string // "get", "put", "cas", "list", "delete"
	Key     string
	Err     error // sanitized cause; never a raw provider response body
}

func (e *Error) Error() string {
	s := "store " + e.Op + " " + e.Key + ": " + e.Outcome.String()
	if e.Err != nil {
		s += ": " + e.Err.Error()
	}
	return s
}

func (e *Error) Unwrap() error { return e.Err }

// OutcomeOf classifies err. nil is OK. An error that is not a store *Error
// is Unknown: anything unclassified must be treated as possibly applied.
func OutcomeOf(err error) Outcome {
	if err == nil {
		return OK
	}
	var e *Error
	if errors.As(err, &e) {
		return e.Outcome
	}
	return Unknown
}

// Hedge sends put, and sends it once more if the first request has not
// returned after delay (D3; no hedge when delay <= 0). Both requests carry the
// same body and the same condition, so at most one can apply. Hedge returns at
// the first OK, after canceling the other request, or once every request has
// returned. errs holds each request's own result in send order; err is nil
// after an OK, else the first Unknown, else the first error.
func Hedge(ctx context.Context, delay time.Duration, put func(context.Context) (string, error)) (etag string, errs []error, err error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	type reply struct {
		i    int
		etag string
		err  error
	}
	replies := make(chan reply, 2)
	send := func(i int) {
		errs = append(errs, nil)
		go func() { e, err := put(ctx); replies <- reply{i, e, err} }()
	}
	var timer <-chan time.Time
	if delay > 0 {
		t := time.NewTimer(delay)
		defer t.Stop()
		timer = t.C
	}
	send(0)
	ok := false
	for pending := 1; pending > 0; {
		select {
		case <-timer:
			send(1)
			pending++
		case r := <-replies:
			pending--
			errs[r.i] = r.err
			if r.err == nil && !ok {
				ok, etag = true, r.etag
				cancel()
			}
		}
	}
	if ok {
		return etag, errs, nil
	}
	for _, e := range errs {
		if OutcomeOf(e) == Unknown {
			return "", errs, e
		}
	}
	return "", errs, errs[0]
}

// Object is a whole object body and its opaque ETag.
type Object struct {
	Body []byte
	ETag string // opaque: compare and send back byte for byte, never parse
}

// Entry is one LIST result.
type Entry struct {
	Key  string
	Size int64
}

// Store is the complete set of storage operations the library uses.
type Store interface {
	// Get reads the whole object at key. If ifNoneMatch is non-empty and is
	// the current ETag, it returns NotModified with an empty Object (D7: the
	// 304 instant is a valid linearization point for the cached head). A body
	// longer than max is Failed, never truncated.
	// Outcomes: OK, NotModified, NotFound, Unknown, Failed.
	Get(ctx context.Context, key, ifNoneMatch string, max int64) (Object, error)

	// PutImmutable creates key with body using If-None-Match: *. It may be
	// retried freely with the same key and bytes. PreconditionFailed means
	// some object already exists at key: the caller must Get it and verify
	// its bytes before referencing it.
	// Outcomes: OK, PreconditionFailed, Unknown, Failed.
	PutImmutable(ctx context.Context, key string, body []byte) error

	// PutCAS replaces key with body if its current ETag equals ifMatch, or
	// creates it if ifMatch is "" (If-None-Match: *). The SDK makes exactly
	// one physical attempt (D3); any outcome but OK goes to resolve().
	// Returns the new ETag on OK.
	// Outcomes: OK, PreconditionFailed, Unknown, Failed.
	PutCAS(ctx context.Context, key string, body []byte, ifMatch string) (etag string, err error)

	// List returns at most limit entries with keys under prefix, in
	// ascending key order, strictly after the key after. more reports that
	// further entries may exist. Used only by maintenance and diagnostics,
	// never to discover committed state.
	// Outcomes: OK, Unknown, Failed.
	List(ctx context.Context, prefix, after string, limit int) (page []Entry, more bool, err error)

	// Delete removes key; deleting an absent key is OK.
	// Outcomes: OK, Unknown, Failed.
	Delete(ctx context.Context, key string) error
}
