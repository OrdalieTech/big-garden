package model

import (
	"path/filepath"
	"testing"
	"time"
)

// withinContract are the cases where the correct protocol must show no
// violation: no administrative head deletion, or one within DECISIONS D14's
// scope (no create of the key in flight). Case C is finding F1.
func withinContract() []*Case {
	var cs []*Case
	for _, c := range append(Cases(), CaseR(3, 2, 3)) {
		if c.Losses == 0 || c.LossAtRest {
			cs = append(cs, c)
		}
	}
	return cs
}

// TestCorrectRandom: the correct protocol shows no violation on random schedules
// of every case within the store contract.
func TestCorrectRandom(t *testing.T) {
	for _, c := range withinContract() {
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

// TestCounterexampleFixtures: every broken variant has a permanent minimal
// counterexample (written by qual model --negative-controls=all) that still
// ends in the same violation, at its last step and not before.
func TestCounterexampleFixtures(t *testing.T) {
	for _, v := range Negatives() {
		path := filepath.Join("testdata", "counterexamples", v.String()+".json")
		f, err := ReadFixture(path)
		if err != nil {
			t.Errorf("%s: %v", v, err)
			continue
		}
		if f.Variant != v.String() {
			t.Errorf("%s: fixture is for %s", path, f.Variant)
		}
		if err := f.Check(); err != nil {
			t.Errorf("%s: %v", path, err)
		}
	}
}

// TestFindingF1 keeps protocol finding F1 (.work/model-findings.md) visible: a
// genesis CAS that outlives an administrative head deletion republishes
// genesis over acknowledged data (G6). Delete this test, and the fixture, only
// together with a protocol change or an explicit G6 amendment.
func TestFindingF1(t *testing.T) {
	f, err := ReadFixture(filepath.Join("testdata", "findings", "f1-genesis-after-head-deletion.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := f.Check(); err != nil {
		t.Fatal(err)
	}
	if f.Variant != Correct.String() {
		t.Fatalf("F1 must reproduce on the correct protocol, fixture is %s", f.Variant)
	}
}
