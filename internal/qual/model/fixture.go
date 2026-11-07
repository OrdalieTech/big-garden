package model

import (
	"encoding/json"
	"fmt"
	"os"
)

// Fixture is a permanent counterexample: a broken variant, the case it runs in,
// the exact transitions from the initial state and the violation they end with.
type Fixture struct {
	Case      string        `json:"case"`
	Variant   string        `json:"variant"`
	Violation string        `json:"violation"`
	Steps     []FixtureStep `json:"steps"`
}

// FixtureStep is one transition of a fixture with its human-readable rendering.
type FixtureStep struct {
	T    [3]uint8 `json:"t"`
	Desc string   `json:"desc"`
}

// NewFixture replays trace and records it.
func NewFixture(c *Case, v Variant, trace []Tr) (Fixture, error) {
	lines, viol, err := Replay(c, v, trace)
	if err != nil {
		return Fixture{}, err
	}
	if viol == "" {
		return Fixture{}, fmt.Errorf("trace ends without a violation")
	}
	f := Fixture{Case: c.Name, Variant: v.String(), Violation: viol}
	for i, t := range trace {
		f.Steps = append(f.Steps, FixtureStep{T: [3]uint8{t.K, t.A, t.B}, Desc: lines[i]})
	}
	return f, nil
}

// Check replays a fixture and requires the same violation at its last step.
func (f Fixture) Check() error {
	c, err := CaseByName(f.Case)
	if err != nil {
		return err
	}
	v, ok := ParseVariant(f.Variant)
	if !ok {
		return fmt.Errorf("unknown variant %q", f.Variant)
	}
	var trace []Tr
	for _, s := range f.Steps {
		trace = append(trace, Tr{s.T[0], s.T[1], s.T[2]})
	}
	_, viol, err := Replay(c, v, trace)
	if err != nil {
		return err
	}
	if viol != f.Violation {
		return fmt.Errorf("%s: violation %q, fixture says %q", f.Variant, viol, f.Violation)
	}
	return nil
}

// WriteFixture writes f as indented JSON.
func WriteFixture(path string, f Fixture) error {
	b, err := json.MarshalIndent(f, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(b, '\n'), 0o644)
}

// ReadFixture reads one fixture.
func ReadFixture(path string) (Fixture, error) {
	var f Fixture
	b, err := os.ReadFile(path)
	if err == nil {
		err = json.Unmarshal(b, &f)
	}
	return f, err
}
