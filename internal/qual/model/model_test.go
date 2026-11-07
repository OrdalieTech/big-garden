package model

import (
	"testing"
	"time"
)

func allCases() []*Case { return append(Cases(), CaseR(3, 2, 3)) }

// TestCorrectRandom: the correct protocol shows no violation on random schedules
// of every case.
func TestCorrectRandom(t *testing.T) {
	for _, c := range allCases() {
		r := Random(c, Correct, 20261107, 3000, 2)
		if r.Status != Pass {
			lines, _, _ := Replay(c, Correct, r.Trace)
			t.Fatalf("case %s: %s: %s\n%s", c.Name, r.Status, r.Violation, join(lines))
		}
		t.Logf("case %s: %d traces, %d transitions, max depth %d, %d bounded, %.1fs", c.Name, r.Traces, r.Steps, r.MaxDepth, r.Bounded, r.Seconds)
	}
}

func TestShortestSmoke(t *testing.T) {
	r := Shortest(caseP(), UncondHead, 20, Budget{States: 5e6, Time: time.Minute})
	lines, viol, err := Replay(caseP(), UncondHead, r.Trace)
	if r.Status != Violation || err != nil {
		t.Fatalf("%s %v %s", r.Status, err, viol)
	}
	t.Logf("%s\n%s", viol, join(lines))
}

func join(lines []string) string {
	s := ""
	for _, l := range lines {
		s += l + "\n"
	}
	return s
}
