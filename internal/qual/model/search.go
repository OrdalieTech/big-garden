package model

import (
	"fmt"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"
)

// Status of a search. Hitting a state or time budget is Inconclusive, never Pass.
const (
	Pass         = "pass"
	Violation    = "violation"
	Inconclusive = "inconclusive"
)

// Result reports one search.
type Result struct {
	Case       string   `json:"case"`
	Variant    string   `json:"variant"`
	Mode       string   `json:"mode"`
	Status     string   `json:"status"`
	States     uint64   `json:"states"`      // distinct canonical states expanded
	Duplicates uint64   `json:"duplicates"`  // successors pruned as already seen
	Bounded    uint64   `json:"bounded"`     // successors discarded by a model bound
	DepthCut   uint64   `json:"depth_cut"`   // successors not expanded at the depth limit
	Steps      uint64   `json:"transitions"` // transitions executed
	Traces     uint64   `json:"traces,omitempty"`
	Depth      int      `json:"depth"`
	MaxDepth   int      `json:"max_depth_reached"`
	Seed       uint64   `json:"seed,omitempty"`
	Violation  string   `json:"violation,omitempty"`
	Trace      []Tr     `json:"-"`
	Note       string   `json:"note,omitempty"`
	Seconds    float64  `json:"seconds"`
	Lines      []string `json:"trace,omitempty"`
}

// Budget bounds an exhaustive search.
type Budget struct {
	States uint64
	Time   time.Duration
}

type dfs struct {
	c      *Case
	v      Variant
	k      *canon
	seen   map[[2]uint64]uint8
	limit  int
	budget Budget
	start  time.Time
	res    *Result
	stack  []Tr
	stop   bool
}

func (d *dfs) walk(s *State, depth int) {
	var buf [96]Tr
	if depth > d.res.MaxDepth {
		d.res.MaxDepth = depth
	}
	for _, t := range Enabled(d.c, s, buf[:0]) {
		if d.stop {
			return
		}
		ns := *s
		ok, viol := Step(d.c, d.v, &ns, t)
		d.res.Steps++
		if !ok {
			d.res.Bounded++
			continue
		}
		if viol != "" {
			d.res.Status, d.res.Violation = Violation, viol
			d.res.Trace = append(append([]Tr(nil), d.stack...), t)
			d.stop = true
			return
		}
		if depth+1 >= d.limit {
			d.res.DepthCut++
			continue
		}
		h := d.k.Hash(d.c, &ns)
		if old, ok := d.seen[h]; ok && int(old) <= depth+1 {
			d.res.Duplicates++
			continue
		}
		d.seen[h] = uint8(depth + 1)
		d.res.States++
		if d.res.States >= d.budget.States || (d.res.States%4096 == 0 && time.Since(d.start) > d.budget.Time) {
			d.res.Status, d.stop = Inconclusive, true
			d.res.Note = fmt.Sprintf("budget exhausted after %d states", d.res.States)
			return
		}
		d.stack = append(d.stack, t)
		d.walk(&ns, depth+1)
		d.stack = d.stack[:len(d.stack)-1]
	}
}

// Exhaustive explores every schedule of c up to its depth with canonical-state
// deduplication. A state reached again at a smaller depth is expanded again, so
// the search is complete within the depth bound.
func Exhaustive(c *Case, v Variant, b Budget) Result {
	return search(c, v, c.Depth, b)
}

func search(c *Case, v Variant, limit int, b Budget) Result {
	res := Result{Case: c.Name, Variant: v.String(), Mode: "exhaustive", Status: Pass, Depth: limit}
	d := &dfs{c: c, v: v, k: newCanon(), seen: map[[2]uint64]uint8{}, limit: limit, budget: b, start: time.Now(), res: &res}
	s := c.Init
	d.seen[d.k.Hash(c, &s)] = 0
	res.States = 1
	d.walk(&s, 0)
	res.Seconds = time.Since(d.start).Seconds()
	if res.Status == Pass && res.DepthCut > 0 {
		res.Note = fmt.Sprintf("complete up to depth %d; %d successors lie beyond it", limit, res.DepthCut)
	} else if res.Status == Pass {
		res.Note = "complete: the reachable state graph closed within the depth bound"
	}
	return res
}

// Shortest finds a minimal-length violating schedule by iterative deepening.
func Shortest(c *Case, v Variant, maxDepth int, b Budget) Result {
	start := time.Now()
	var total Result
	for limit := 1; limit <= maxDepth; limit++ {
		r := search(c, v, limit, b)
		total.States += r.States
		total.Steps += r.Steps
		if r.Status != Pass {
			r.Mode = "shortest"
			r.States, r.Steps = total.States, total.Steps
			r.Seconds = time.Since(start).Seconds()
			return r
		}
		if r.DepthCut == 0 { // graph closed: no violation at any depth
			r.Mode = "shortest"
			r.Seconds = time.Since(start).Seconds()
			return r
		}
	}
	return Result{Case: c.Name, Variant: v.String(), Mode: "shortest", Status: Inconclusive, Depth: maxDepth,
		States: total.States, Steps: total.Steps, Seconds: time.Since(start).Seconds(),
		Note: fmt.Sprintf("no violation within depth %d", maxDepth)}
}

// Random runs n seeded random schedules of c, each up to c.Depth transitions.
// Trace i uses the PCG stream (seed, i), so any violation is reproducible.
func Random(c *Case, v Variant, seed uint64, n uint64, workers int) Result {
	start := time.Now()
	res := Result{Case: c.Name, Variant: v.String(), Mode: "random", Status: Pass, Depth: c.Depth, Seed: seed}
	var next, steps, bounded atomic.Uint64
	var mu sync.Mutex
	var wg sync.WaitGroup
	maxDepth := 0
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var buf [96]Tr
			for {
				i := next.Add(1) - 1
				if i >= n {
					return
				}
				mu.Lock()
				done := res.Status != Pass
				mu.Unlock()
				if done {
					return
				}
				trace, viol, depth, st, bd := randomTrace(c, v, seed, i, buf[:0])
				steps.Add(st)
				bounded.Add(bd)
				mu.Lock()
				if depth > maxDepth {
					maxDepth = depth
				}
				if viol != "" && res.Status == Pass {
					res.Status, res.Violation, res.Trace = Violation, viol, trace
					res.Note = fmt.Sprintf("trace %d", i)
				}
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	res.Traces = min(next.Load(), n)
	res.Steps, res.Bounded, res.MaxDepth = steps.Load(), bounded.Load(), maxDepth
	res.Seconds = time.Since(start).Seconds()
	return res
}

func randomTrace(c *Case, v Variant, seed, i uint64, buf []Tr) (trace []Tr, viol string, depth int, steps, bounded uint64) {
	rng := rand.New(rand.NewPCG(seed, i))
	s := c.Init
	var picked []Tr
	for depth < c.Depth {
		trs := Enabled(c, &s, buf[:0])
		if len(trs) == 0 {
			break
		}
		moved := false
		for tries := 0; tries < len(trs) && !moved; tries++ {
			t := trs[rng.IntN(len(trs))]
			ns := s
			ok, vi := Step(c, v, &ns, t)
			steps++
			if !ok {
				bounded++
				continue
			}
			picked = append(picked, t)
			if vi != "" {
				return picked, vi, depth + 1, steps, bounded
			}
			s, moved = ns, true
		}
		if !moved {
			break
		}
		depth++
	}
	return nil, "", depth, steps, bounded
}

// Replay runs a trace from the initial state and returns its descriptions and
// the violation it ends with. It fails if a transition is not enabled, exceeds
// a bound, or a violation occurs before the last step.
func Replay(c *Case, v Variant, trace []Tr) (lines []string, viol string, err error) {
	s := c.Init
	var buf [96]Tr
	for i, t := range trace {
		enabled := false
		for _, e := range Enabled(c, &s, buf[:0]) {
			enabled = enabled || e == t
		}
		if !enabled {
			return lines, "", fmt.Errorf("step %d (%v) is not enabled", i, t)
		}
		lines = append(lines, fmt.Sprintf("%2d. %s", i+1, Describe(c, &s, t)))
		ok, vi := Step(c, v, &s, t)
		if !ok {
			return lines, "", fmt.Errorf("step %d exceeds a model bound", i)
		}
		if vi != "" {
			if i != len(trace)-1 {
				return lines, vi, fmt.Errorf("violation at step %d before the end: %s", i, vi)
			}
			return lines, vi, nil
		}
	}
	return lines, "", nil
}
