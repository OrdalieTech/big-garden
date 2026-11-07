package store

import (
	"errors"
	"fmt"
	"testing"
)

func TestOutcomeOf(t *testing.T) {
	if OutcomeOf(nil) != OK {
		t.Fatal("nil is not OK")
	}
	e := &Error{Outcome: PreconditionFailed, Op: "cas", Key: "k"}
	if OutcomeOf(fmt.Errorf("wrapped: %w", e)) != PreconditionFailed {
		t.Fatal("wrapped outcome lost")
	}
	// Anything unclassified may have applied.
	if OutcomeOf(errors.New("connection reset")) != Unknown {
		t.Fatal("foreign error is not Unknown")
	}
	for o, s := range []string{"ok", "not_modified", "not_found", "precondition_failed", "unknown", "failed"} {
		if Outcome(o).String() != s {
			t.Fatalf("%d: %s", o, Outcome(o))
		}
	}
}
