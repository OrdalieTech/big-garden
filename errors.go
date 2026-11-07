package garden

import (
	"strings"
	"time"
)

// Code is a stable error classification. Its values never change meaning.
type Code string

// The sixteen error codes (PLAN §4.4).
const (
	CodeNotFound            Code = "not_found"            // no such entity
	CodeNotOwner            Code = "not_owner"            // another session owns it; Error.Route says which
	CodeRouteChanged        Code = "route_changed"        // the observed Route is stale
	CodeIncarnationMismatch Code = "incarnation_mismatch" // the Ref names another incarnation of the key
	CodeSchemaMismatch      Code = "schema_mismatch"      // stored schema differs from the expected one
	CodeIDConflict          Code = "id_conflict"          // operation ID reused with another input hash
	CodeIDExpired           Code = "id_expired"           // operation ID at or below the receipt floor
	CodeAbsentAtVersion     Code = "absent_at_version"    // no receipt in the state validated at Error.At
	CodeOutcomeUnknown      Code = "outcome_unknown"      // may have committed: retry with the same Request
	CodeUnavailable         Code = "unavailable"          // storage unreachable or too slow; retryable
	CodeCapacity            Code = "capacity"             // a bounded resource is exhausted; retryable
	CodeUnsupported         Code = "unsupported"          // format, version or operation not supported
	CodeCorrupt             Code = "corrupt"              // a stored artifact failed validation
	CodeStateMissing        Code = "state_missing"        // existing state is incomplete; never recreated
	CodeClosed              Code = "closed"               // the Node is closed
	CodeInvalidArgument     Code = "invalid_argument"     // the call itself is malformed
)

// Error is the only error type returned by this package, apart from caller
// callback errors that pass through unchanged. Use errors.As to inspect it.
type Error struct {
	Code       Code
	Operation  MutationID
	Route      *Route        // nil if unavailable/not relevant
	At         Version       // validated observation point, if available
	RetryAfter time.Duration // hint, never permission to change operation identity
	Message    string        // bounded, sanitized explanation
	cause      error         // private; context cause or sanitized lower-layer error
}

// Error returns the code and message. The cause is reachable only through
// Unwrap, so logging an Error never prints a lower layer's text.
func (e *Error) Error() string {
	if e.Message == "" {
		return "state: " + string(e.Code)
	}
	return "state: " + string(e.Code) + ": " + e.Message
}

// Unwrap returns the cause, preserving context.Canceled and
// context.DeadlineExceeded identity for errors.Is.
func (e *Error) Unwrap() error { return e.cause }

const maxMessage = 256

// newError builds an Error with a message bounded to maxMessage bytes.
func newError(code Code, msg string, cause error) *Error {
	if len(msg) > maxMessage {
		msg = strings.ToValidUTF8(msg[:maxMessage], "")
	}
	return &Error{Code: code, Message: msg, cause: cause}
}
