package model

import (
	"fmt"
	"io"
	"math"
	"math/rand/v2"
	"sort"
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
	Case       string            `json:"case"`
	Variant    string            `json:"variant"`
	Mode       string            `json:"mode"`
	Status     string            `json:"status"`
	States     uint64            `json:"states"`     // distinct canonical states expanded
	Duplicates uint64            `json:"duplicates"` // successors pruned as already seen
	Bounded    uint64            `json:"bounded"`    // successors discarded by a model bound
	BoundBy    map[string]uint64 `json:"bounded_by,omitempty"`
	DepthCut   uint64            `json:"depth_cut"`   // successors not expanded at the depth limit
	Steps      uint64            `json:"transitions"` // transitions executed
	Traces     uint64            `json:"traces,omitempty"`
	Depth      int               `json:"depth"`
	MaxDepth   int               `json:"max_depth_reached"`
	Seed       uint64            `json:"seed,omitempty"`
	Violation  string            `json:"violation,omitempty"`
	Trace      []Tr              `json:"-"`
	Note       string            `json:"note,omitempty"`
	Seconds    float64           `json:"seconds"`
	Collision  float64           `json:"fingerprint_collision_bound,omitempty"`
	Lines      []string          `json:"trace,omitempty"`
	Kinds      []Kind            `json:"violation_kinds,omitempty"` // random: every distinct violation
}

// Kind groups random-trace violations by their message with numbers and
// images elided; it keeps the first trace of each.
type Kind struct {
	Signature string `json:"signature"`
	Count     uint64 `json:"count"`
	First     uint64 `json:"first_trace"`
	Violation string `json:"violation"`
	Trace     []Tr   `json:"-"`
}

func signature(v string) string {
	var b []byte
	depth := 0
	for i := 0; i < len(v); i++ {
		switch ch := v[i]; {
		case ch == '[':
			depth++
		case ch == ']':
			depth--
			if depth == 0 {
				b = append(b, "[..]"...)
			}
		case depth > 0:
		case ch >= '0' && ch <= '9':
			if len(b) == 0 || b[len(b)-1] != 'N' {
				b = append(b, 'N')
			}
		default:
			b = append(b, ch)
		}
	}
	return string(b)
}

// Budget bounds an exhaustive search.
type Budget struct {
	States   uint64
	Time     time.Duration
	Progress io.Writer // optional: a line every 5M states
}

type dfs struct {
	c      *Case
	v      Variant
	k      *canon
	fps    *fpset           // full reachability: each state once
	depth  map[uint64]uint8 // depth-bounded: minimal depth seen
	limit  int
	budget Budget
	start  time.Time
	res    *Result
	stack  []Tr
	frames []*State
	bufs   [][]Tr
	stop   bool
}

func (d *dfs) walk(s *State, depth int) {
	if depth > d.res.MaxDepth {
		d.res.MaxDepth = depth
	}
	for len(d.frames) <= depth+1 {
		d.frames = append(d.frames, new(State))
		d.bufs = append(d.bufs, make([]Tr, 0, 96))
	}
	ns := d.frames[depth+1]
	trs := Enabled(d.c, s, d.bufs[depth][:0])
	d.bufs[depth] = trs
	for _, t := range trs {
		if d.stop {
			return
		}
		*ns = *s
		b, viol := Step(d.c, d.v, ns, t)
		d.res.Steps++
		if b != 0 {
			d.res.bounded(b)
			continue
		}
		if viol != "" {
			d.res.Status, d.res.Violation = Violation, viol
			d.res.Trace = append(append([]Tr(nil), d.stack...), t)
			d.stop = true
			return
		}
		if d.limit > 0 && depth+1 >= d.limit {
			d.res.DepthCut++
			continue
		}
		h := d.k.Hash(d.c, ns)
		if d.limit == 0 {
			if !d.fps.add(h) {
				if d.fps.full {
					d.res.Status, d.stop = Inconclusive, true
					d.res.Note = fmt.Sprintf("state table full after %d states", d.res.States)
					return
				}
				d.res.Duplicates++
				continue
			}
		} else {
			if old, ok := d.depth[h]; ok && int(old) <= depth+1 {
				d.res.Duplicates++
				continue
			}
			d.depth[h] = uint8(min(depth+1, 255))
		}
		d.res.States++
		if d.budget.Progress != nil && d.res.States%5_000_000 == 0 {
			fmt.Fprintf(d.budget.Progress, "  %s %s: %d states, %d duplicates, %d bounded, path %d, %.0fs\n", d.c.Name, d.v,
				d.res.States, d.res.Duplicates, d.res.Bounded, d.res.MaxDepth, time.Since(d.start).Seconds())
		}
		if d.res.States >= d.budget.States || (d.res.States%4096 == 0 && time.Since(d.start) > d.budget.Time) {
			d.res.Status, d.stop = Inconclusive, true
			d.res.Note = fmt.Sprintf("budget exhausted after %d states", d.res.States)
			return
		}
		d.stack = append(d.stack, t)
		d.walk(ns, depth+1)
		d.stack = d.stack[:len(d.stack)-1]
	}
}

// Exhaustive explores the complete reachable state graph of c: every canonical
// state is expanded once, with no depth bound. Every budget of a case is finite
// (invocations, restarts, clock jumps, hedges, three proposals per candidate,
// message slots), and fresh IDs are canonicalised, so the graph is finite and
// this covers every schedule of every length, which subsumes the plan's
// schedule-depth bounds.
func Exhaustive(c *Case, v Variant, b Budget) Result {
	return search(c, v, 0, b)
}

// search is a depth-first search; limit > 0 bounds the schedule length, and a
// state reached again at a smaller depth is then expanded again, so the search
// is complete within that bound.
func search(c *Case, v Variant, limit int, b Budget) Result {
	res := Result{Case: c.Name, Variant: v.String(), Mode: "exhaustive", Status: Pass, Depth: limit}
	d := &dfs{c: c, v: v, k: newCanon(), limit: limit, budget: b, start: time.Now(), res: &res}
	s := c.Init
	if limit == 0 {
		d.fps = newFPSet(b.States)
		d.fps.add(d.k.Hash(c, &s))
	} else {
		d.depth = map[uint64]uint8{d.k.Hash(c, &s): 0}
	}
	res.States = 1
	d.walk(&s, 0)
	res.Seconds = time.Since(d.start).Seconds()
	n := float64(res.States)
	res.Collision = n * n / math.Exp2(65) // hash compaction omission bound
	switch {
	case res.Status == Pass && res.DepthCut > 0:
		res.Note = fmt.Sprintf("complete up to depth %d; %d successors lie beyond it", limit, res.DepthCut)
	case res.Status == Pass:
		res.Note = fmt.Sprintf("complete: the whole reachable graph, longest DFS path %d", res.MaxDepth)
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
// Trace i uses the PCG stream (seed, i), so any violation is reproducible. All n
// traces run even after a violation; violations are grouped by kind.
func Random(c *Case, v Variant, seed uint64, n uint64, workers int) Result {
	start := time.Now()
	res := Result{Case: c.Name, Variant: v.String(), Mode: "random", Status: Pass, Depth: c.Depth, Seed: seed}
	var next, steps atomic.Uint64
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
				trace, viol, depth, st, bd := randomTrace(c, v, seed, i, buf[:0])
				steps.Add(st)
				mu.Lock()
				for b, n := range bd {
					for ; n > 0; n-- {
						res.bounded(uint8(b))
					}
				}
				if depth > maxDepth {
					maxDepth = depth
				}
				if viol != "" {
					res.addKind(viol, i, trace)
				}
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	res.Traces = min(next.Load(), n)
	sort.Slice(res.Kinds, func(i, j int) bool { return res.Kinds[i].First < res.Kinds[j].First })
	if len(res.Kinds) > 0 {
		k := res.Kinds[0]
		res.Status, res.Violation, res.Trace = Violation, k.Violation, k.Trace
		res.Note = fmt.Sprintf("%d violation kinds; first in trace %d", len(res.Kinds), k.First)
	}
	res.Steps, res.MaxDepth = steps.Load(), maxDepth
	res.Seconds = time.Since(start).Seconds()
	return res
}

func (r *Result) bounded(b uint8) {
	r.Bounded++
	if r.BoundBy == nil {
		r.BoundBy = map[string]uint64{}
	}
	r.BoundBy[boundNames[b]]++
}

func (r *Result) addKind(viol string, i uint64, trace []Tr) {
	sig := signature(viol)
	for k := range r.Kinds {
		if r.Kinds[k].Signature == sig {
			r.Kinds[k].Count++
			if i < r.Kinds[k].First {
				r.Kinds[k].First, r.Kinds[k].Violation, r.Kinds[k].Trace = i, viol, trace
			}
			return
		}
	}
	r.Kinds = append(r.Kinds, Kind{Signature: sig, Count: 1, First: i, Violation: viol, Trace: trace})
}

func randomTrace(c *Case, v Variant, seed, i uint64, buf []Tr) (trace []Tr, viol string, depth int, steps uint64, bounded [numBounds]uint64) {
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
			b, vi := Step(c, v, &ns, t)
			steps++
			if b != 0 {
				bounded[b]++
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
		b, vi := Step(c, v, &s, t)
		if b != 0 {
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
