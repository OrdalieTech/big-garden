package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	pm "github.com/OrdalieTech/big-garden/internal/qual/model"
)

// model runs the M1 protocol model (PLAN §14, §20.2): the bounded exhaustive
// cases, N seeded random traces and the broken-variant controls. It exits
// nonzero on any violation of the correct model, any undetected control and any
// inconclusive (budget-limited) search.
func model(args []string) error {
	fs := flag.NewFlagSet("model", flag.ContinueOnError)
	nodes := fs.Int("nodes", 3, "nodes in the random-trace configuration (1-3)")
	ents := fs.Int("entities", 2, "entities in the random-trace configuration (1-2)")
	conc := fs.Int("concurrent-operations", 3, "concurrent operations in the random-trace configuration")
	exhaustive := fs.Bool("exhaustive-bounded", false, "explore the bounded cases exhaustively")
	cases := fs.String("cases", "P,O,G,I,C", "bounded cases to explore")
	traces := fs.Uint64("random-traces", 0, "seeded random traces of the random configuration")
	seed := fs.Uint64("seed", 20261107, "random seed")
	negatives := fs.String("negative-controls", "", `"all", or a comma-separated list of broken variants`)
	out := fs.String("out", "", "output directory (model.json and counterexamples/)")
	maxStates := fs.Uint64("max-states", 40_000_000, "state budget per exhaustive search")
	budget := fs.Duration("time-budget", 30*time.Minute, "time budget per exhaustive search")
	workers := fs.Int("workers", min(4, runtime.NumCPU()), "random-trace workers")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *out == "" {
		return errors.New("--out is required")
	}
	if *nodes < 1 || *nodes > 3 || *ents < 1 || *ents > 2 || *conc < 1 {
		return errors.New("--nodes must be 1-3, --entities 1-2, --concurrent-operations >= 1")
	}
	if err := os.MkdirAll(filepath.Join(*out, "counterexamples"), 0o755); err != nil {
		return err
	}
	b := pm.Budget{States: *maxStates, Time: *budget, Progress: os.Stderr}
	var results []pm.Result
	var failures []string
	save := func(c *pm.Case, v pm.Variant, trace []pm.Tr, name string) []string {
		f, err := pm.NewFixture(c, v, trace)
		if err != nil {
			failures = append(failures, fmt.Sprintf("%s %s: trace does not replay: %v", c.Name, v, err))
			return nil
		}
		if err := pm.WriteFixture(filepath.Join(*out, "counterexamples", name), f); err != nil {
			failures = append(failures, err.Error())
		}
		var lines []string
		for _, s := range f.Steps {
			lines = append(lines, s.Desc)
		}
		return lines
	}
	record := func(r pm.Result, c *pm.Case, v pm.Variant) {
		switch {
		case v != pm.Correct && r.Trace != nil:
			r.Lines = save(c, v, r.Trace, v.String()+".json") // each control runs in one case
		case len(r.Kinds) > 0:
			for i, k := range r.Kinds {
				lines := save(c, v, k.Trace, fmt.Sprintf("correct-%s-kind%d.json", c.Name, i))
				if i == 0 {
					r.Lines = lines
				}
			}
		case r.Trace != nil:
			r.Lines = save(c, v, r.Trace, fmt.Sprintf("correct-%s.json", c.Name))
		}
		results = append(results, r)
		fmt.Printf("%-10s %-28s %-12s %-12s states=%d dup=%d bounded=%d depth-cut=%d traces=%d transitions=%d %.1fs %s\n",
			r.Mode, r.Variant, r.Case, r.Status, r.States, r.Duplicates, r.Bounded, r.DepthCut, r.Traces, r.Steps, r.Seconds, r.Note)
		for _, k := range r.Kinds {
			fmt.Printf("    %d traces (first %d): %s\n", k.Count, k.First, k.Violation)
		}
		if r.Violation != "" && len(r.Kinds) == 0 {
			fmt.Printf("    %s\n", r.Violation)
		}
	}
	if *exhaustive {
		for _, name := range strings.Split(*cases, ",") {
			c, err := pm.CaseByName(strings.TrimSpace(name))
			if err != nil {
				return err
			}
			r := pm.Exhaustive(c, pm.Correct, b)
			record(r, c, pm.Correct)
			if r.Status != pm.Pass {
				failures = append(failures, fmt.Sprintf("exhaustive %s: %s %s", c.Name, r.Status, r.Violation))
			}
		}
	}
	if *traces > 0 {
		c := pm.CaseR(*nodes, *ents, *conc)
		r := pm.Random(c, pm.Correct, *seed, *traces, *workers)
		record(r, c, pm.Correct)
		if r.Status != pm.Pass || r.Traces != *traces {
			failures = append(failures, fmt.Sprintf("random: %s after %d traces %s", r.Status, r.Traces, r.Violation))
		}
	}
	var vs []pm.Variant
	switch *negatives {
	case "", "none":
	case "all":
		vs = pm.Negatives()
	default:
		for _, n := range strings.Split(*negatives, ",") {
			v, ok := pm.ParseVariant(strings.TrimSpace(n))
			if !ok || v == pm.Correct {
				return fmt.Errorf("unknown negative control %q", n)
			}
			vs = append(vs, v)
		}
	}
	for _, v := range vs {
		c := pm.ControlCase(v)
		r := pm.Shortest(c, v, c.Depth, b)
		record(r, c, v)
		if r.Status != pm.Violation {
			failures = append(failures, fmt.Sprintf("negative control %s not detected: %s", v, r.Status))
		}
	}
	report := struct {
		Results  []pm.Result `json:"results"`
		Failures []string    `json:"failures"`
		Passed   bool        `json:"passed"`
	}{results, failures, len(failures) == 0}
	j, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(*out, "model.json"), append(j, '\n'), 0o644); err != nil {
		return err
	}
	if len(failures) > 0 {
		return errors.New(strings.Join(failures, "; "))
	}
	return nil
}
