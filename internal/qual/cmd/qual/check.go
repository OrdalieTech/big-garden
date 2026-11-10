package main

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"maps"
	"math"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"time"

	"github.com/anishathalye/porcupine"
)

// check re-verifies retained soak histories (soak.go: history.jsonl.gz, one
// driver-observed client operation per line) from the raw history alone,
// without trusting the soak's own verdicts (PLAN §15.2, §18.1, §20.5). Each
// entity is a ledger: a write commits at counter n = previous counter + 1
// and answers n; a strong read answers the counter c and the IDs at
// c-len+1..c (the last 8). The file is streamed once:
//
//   - slots: every observation (acknowledged write, read, receipt lookup)
//     of an ID at a counter value must agree: one ID per counter value (an
//     acknowledged write overwritten by another is lost) and one counter
//     value per ID (no duplicate effect of one operation ID);
//   - a definitely failed write is never observed, an unknown write at most
//     once (counted as applied or unobserved), and every counter value no
//     acknowledged write explains is matched by an unknown write;
//   - linearizability, complete: write n commits inside its interval (an
//     unknown one after its start), commits are ordered by n, and a read
//     answering c lies after commit c and before commit c+1. These are
//     difference constraints on a chain, so one forward and one backward
//     pass over the counter values of each entity decide them exactly;
//   - linearizability, sampled: porcupine checks every entity's operations
//     in periodic windows against a counter model, independently of the
//     pass above. A timeout (inconclusive) is a failure.
func check(args []string) error {
	fl := flag.NewFlagSet("check", flag.ContinueOnError)
	histories := fl.String("histories", "artifacts", "a history.jsonl.gz file, or a directory searched for them")
	out := fl.String("out", "", "checks.json path (empty: stdout only)")
	every := fl.Duration("sample-every", 10*time.Minute, "porcupine window period")
	length := fl.Duration("window", time.Minute, "porcupine window length")
	fl.Bool("require-ack-recovery", true, "always on: acknowledged writes are checked")
	fl.Bool("require-linearizability", true, "always on: strong reads are checked")
	negative := fl.Bool("require-negative-controls", false, "first check that seeded violations are detected")
	if err := fl.Parse(args); err != nil {
		return err
	}
	if *negative {
		if err := negativeControls(); err != nil {
			return err
		}
	}
	var files []string
	err := filepath.WalkDir(*histories, func(p string, d fs.DirEntry, err error) error {
		if err == nil && !d.IsDir() && filepath.Base(p) == "history.jsonl.gz" {
			files = append(files, p)
		}
		return err
	})
	if err != nil {
		return err
	}
	if len(files) == 0 {
		return fmt.Errorf("no history.jsonl.gz under %s", *histories)
	}
	var reps []*checkReport
	failed := false
	for _, f := range files {
		r, err := checkFile(f, *every, *length)
		if err != nil {
			return err
		}
		b, _ := json.Marshal(r)
		fmt.Printf("%s\n", b)
		failed = failed || r.Verdict != "pass"
		reps = append(reps, r)
	}
	if *out != "" {
		b, _ := json.MarshalIndent(reps, "", "  ")
		if err := os.WriteFile(*out, append(b, '\n'), 0o644); err != nil {
			return err
		}
	}
	if failed {
		return errors.New("a history failed its check")
	}
	return nil
}

// cop is one history line (soak.go shop).
type cop struct {
	Client             int
	Op, Key, ID, Class string
	Code, Value        string
	Replayed           bool
	IDs                []string
	Start, End         int64
}

type checkReport struct {
	File       string
	Truncated  string `json:",omitempty"` // the stream ended early: only the prefix was checked
	Lines      int64
	Ops        map[string]int64 // op:class
	Entities   int
	Slots      int64 // counter values committed over all entities
	Acked      int64 // acknowledged writes
	AckedSeen  int64 // ... whose counter value a read also showed
	ReadsAcked int64
	Lookups    int64
	Unknown    struct{ Applied, Unobserved int64 }
	Failed     int64 // definitely failed writes (none observed)
	Gaps       int64 // counter values never shown: committed by unobserved unknown writes
	Phantoms   int64 // counter values holding an ID no write issued
	Windows    struct {
		Checked, OK, Illegal, Inconclusive int
		Ops                                int64
	}
	Violations []string `json:",omitempty"`
	Verdict    string
}

func (r *checkReport) fail(format string, a ...any) {
	if len(r.Violations) < 100 {
		r.Violations = append(r.Violations, fmt.Sprintf(format, a...))
	}
	r.Verdict = "fail"
}

type mid [24]byte

// ledgerState is what the history shows of one entity.
type ledgerState struct {
	ids     []mid // counter value n-1 -> ID (zero: never observed)
	lo, hi  []int64
	flags   []uint8 // 1 acknowledged write, 2 shown by a read
	at      map[mid]int64
	unknown map[mid]int64 // unknown write -> its start
	failed  map[mid]bool
	lost    map[mid]string                // lookups answered absent_at_version or id_expired
	ops     map[int][]porcupine.Operation // sampled window -> operations
}

func (s *ledgerState) grow(n int64) {
	for int64(len(s.ids)) < n {
		s.ids, s.flags = append(s.ids, mid{}), append(s.flags, 0)
		s.lo, s.hi = append(s.lo, math.MinInt64), append(s.hi, math.MaxInt64)
	}
}

// observe records that id sits at counter value n.
func (s *ledgerState) observe(r *checkReport, key string, id mid, n int64, flag uint8) {
	s.grow(n)
	switch cur := s.ids[n-1]; {
	case cur == mid{}:
		s.ids[n-1] = id
	case cur != id:
		r.fail("%s: counter %d holds %x and %x (a lost write)", key, n, cur[:4], id[:4])
	}
	if m, ok := s.at[id]; ok && m != n {
		r.fail("%s: ID %x at counters %d and %d (a duplicate effect)", key, id[:4], m, n)
	}
	s.at[id] = n
	s.flags[n-1] |= flag
}

func parseID(s string) (id mid, ok bool) {
	_, err := hex.Decode(id[:], []byte(s))
	return id, err == nil && len(s) == 2*len(id)
}

func checkFile(path string, every, length time.Duration) (*checkReport, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	zr, err := gzip.NewReader(bufio.NewReaderSize(f, 1<<20))
	if err != nil {
		return nil, err
	}
	return checkStream(path, zr, every, length)
}

func checkStream(path string, zr io.Reader, every, length time.Duration) (*checkReport, error) {
	r := &checkReport{File: path, Ops: map[string]int64{}, Verdict: "pass"}
	keys := map[string]*ledgerState{}
	sc := bufio.NewScanner(zr)
	sc.Buffer(make([]byte, 1<<20), 16<<20)
	var bad error // the latest line that did not decode
	for sc.Scan() {
		if bad != nil {
			r.fail("line %d: %v", r.Lines, bad)
		}
		r.Lines++
		var o cop
		if bad = json.Unmarshal(sc.Bytes(), &o); bad != nil {
			continue
		}
		r.Ops[o.Op+":"+o.Class]++
		s := keys[o.Key]
		if s == nil {
			s = &ledgerState{at: map[mid]int64{}, unknown: map[mid]int64{}, failed: map[mid]bool{}, lost: map[mid]string{}, ops: map[int][]porcupine.Operation{}}
			keys[o.Key] = s
		}
		checkOp(r, s, o, int64(every), int64(length))
	}
	if err := sc.Err(); err != nil {
		if !errors.Is(err, io.ErrUnexpectedEOF) {
			return nil, err
		}
		r.Truncated = fmt.Sprintf("%v after %d lines", err, r.Lines)
		bad = nil // the cut line
	}
	if bad != nil {
		r.fail("line %d: %v", r.Lines, bad)
	}
	r.Entities = len(keys)
	for _, k := range slices.Sorted(maps.Keys(keys)) {
		finishKey(r, k, keys[k], int64(every), int64(length))
	}
	return r, nil
}

func checkOp(r *checkReport, s *ledgerState, o cop, every, length int64) {
	if o.Op == "lookup" && (o.Code == "absent_at_version" || o.Code == "id_expired") {
		if id, ok := parseID(o.ID); ok {
			s.lost[id] = o.Code // a receipt of an acknowledged write must stay (soak lookups are of recent ones)
		}
	}
	if o.Class != "ack" && o.Op != "write" {
		return // other failed reads and lookups prove nothing
	}
	var id mid
	if o.Op == "write" || o.Op == "lookup" {
		var ok bool
		if id, ok = parseID(o.ID); !ok {
			r.fail("%s: bad ID %q", o.Key, o.ID)
			return
		}
	}
	n, err := strconv.ParseInt(o.Value, 10, 64)
	if o.Class == "ack" && (err != nil || n < 0 || (o.Op != "read" && n == 0)) {
		r.fail("%s: %s %s answered %q", o.Key, o.Op, o.ID, o.Value)
		return
	}
	pin := cin{}
	switch {
	case o.Op == "write" && o.Class == "ack":
		r.Acked++
		s.observe(r, o.Key, id, n, 1)
		s.lo[n-1], s.hi[n-1] = max(s.lo[n-1], o.Start), min(s.hi[n-1], o.End)
		pin = cin{write: true}
	case o.Op == "write" && o.Class == "unknown":
		s.unknown[id] = o.Start
		return // added to the windows it may fall in once its fate is known
	case o.Op == "write":
		r.Failed++
		s.failed[id] = true
		return
	case o.Op == "lookup":
		r.Lookups++
		s.observe(r, o.Key, id, n, 0)
		s.hi[n-1] = min(s.hi[n-1], o.End)
		return
	case o.Op == "read":
		r.ReadsAcked++
		if want := min(n, 8); int64(len(o.IDs)) != want {
			r.fail("%s: read at counter %d shows %d IDs, not %d", o.Key, n, len(o.IDs), want)
			return
		}
		for i, sid := range o.IDs {
			rid, ok := parseID(sid)
			if !ok {
				r.fail("%s: read shows bad ID %q", o.Key, sid)
				return
			}
			s.observe(r, o.Key, rid, n-int64(len(o.IDs))+1+int64(i), 2)
		}
		if n > 0 {
			s.hi[n-1] = min(s.hi[n-1], o.End)
		}
		s.grow(n + 1)
		s.lo[n] = max(s.lo[n], o.Start)
	default:
		return
	}
	for w := (o.Start - length) / every; w <= o.End/every; w++ { // the sampled windows o overlaps
		if w0 := w * every; o.Start < w0+length && o.End > w0 && w >= 0 {
			s.ops[int(w)] = append(s.ops[int(w)], porcupine.Operation{ClientId: o.Client, Input: pin, Output: n, Call: o.Start, Return: o.End})
		}
	}
}

func finishKey(r *checkReport, key string, s *ledgerState, every, length int64) {
	unobserved := int64(0)
	for id, start := range s.unknown {
		n, ok := s.at[id]
		if !ok {
			unobserved++
			continue
		}
		r.Unknown.Applied++
		s.lo[n-1] = max(s.lo[n-1], start)
	}
	r.Unknown.Unobserved += unobserved
	for id, code := range s.lost {
		if n, ok := s.at[id]; ok && s.flags[n-1]&1 != 0 {
			r.fail("%s: lookup of the acknowledged write %x at counter %d answered %s", key, id[:4], n, code)
		}
	}
	for id := range s.failed {
		if n, ok := s.at[id]; ok {
			r.fail("%s: definitely failed write %x committed at counter %d", key, id[:4], n)
		}
	}
	// the last counter value is committed only if something showed it: a
	// read's lower bound on the next value does not create one
	last := int64(len(s.ids))
	for last > 0 && s.flags[last-1] == 0 && s.ids[last-1] == (mid{}) {
		last--
	}
	gaps := int64(0)
	for n := int64(1); n <= last; n++ {
		r.Slots++
		switch f := s.flags[n-1]; {
		case f&1 != 0:
			if f&2 != 0 {
				r.AckedSeen++
			}
		case s.ids[n-1] == (mid{}):
			gaps++
		default:
			if _, ok := s.unknown[s.ids[n-1]]; !ok {
				r.Phantoms++
				r.fail("%s: counter %d holds %x, which no write of the history issued", key, n, s.ids[n-1][:4])
			}
		}
	}
	r.Gaps += gaps
	if gaps > unobserved {
		r.fail("%s: %d counter values committed by no acknowledged write, only %d unobserved unknown writes", key, gaps, unobserved)
	}
	// complete check: earliest and latest commit times of each counter value
	lo, hi := slices.Clone(s.lo[:last]), slices.Clone(s.hi[:last])
	for n := 1; n < len(lo); n++ {
		lo[n] = max(lo[n], lo[n-1]+1)
	}
	for n := len(hi) - 2; n >= 0; n-- {
		hi[n] = min(hi[n], hi[n+1]-1)
	}
	for n := range lo {
		if lo[n] > hi[n] {
			r.fail("%s: no linearization: counter %d must commit after %d ns and before %d ns", key, n+1, lo[n], hi[n])
			break
		}
	}
	// sampled check: porcupine per window; the unknown writes started in the
	// 15 minutes before its end are optional increments (leaving out one
	// that applied inside the window can only fail the check, never pass it)
	for w, ops := range s.ops {
		end := int64(w)*every + length
		for _, start := range s.unknown {
			if start < end && start > end-int64(15*time.Minute) {
				ops = append(ops, porcupine.Operation{ClientId: -1, Input: cin{write: true}, Output: int64(-1), Call: start, Return: math.MaxInt64})
			}
		}
		r.Windows.Checked++
		r.Windows.Ops += int64(len(ops))
		switch porcupine.CheckOperationsTimeout(counterModel, ops, 10*time.Second) {
		case porcupine.Ok:
			r.Windows.OK++
		case porcupine.Illegal:
			r.Windows.Illegal++
			r.fail("%s: porcupine: window %d (%d ops) is not linearizable", key, w, len(ops))
		default:
			r.Windows.Inconclusive++
			r.fail("%s: porcupine: window %d (%d ops) inconclusive", key, w, len(ops))
		}
	}
}

type cin struct{ write bool }

// counterModel is one entity's counter; the state at a window's start is
// unknown (-1) until the first operation fixes it. An unknown write (output
// -1) increments it; porcupine may also order it last, as if it never applied.
var counterModel = porcupine.Model{
	Init: func() any { return int64(-1) },
	Step: func(st, in, out any) (bool, any) {
		s, w, n := st.(int64), in.(cin).write, out.(int64)
		switch {
		case !w && s < 0:
			return true, n
		case !w:
			return n == s, s
		case n < 0 && s < 0:
			return true, s
		case n < 0:
			return true, s + 1
		case s < 0:
			return true, n
		default:
			return n == s+1, n
		}
	},
}

// negativeControls checks that the checker passes a correct synthetic
// history and fails each seeded violation of it (PLAN §20.5).
func negativeControls() error {
	id := func(i int) string { return fmt.Sprintf("%048x", i+1) }
	clean := func() []cop {
		var h []cop
		var ids []string
		for i := int64(1); i <= 20; i++ {
			ids = append(ids, id(int(i)))
			h = append(h, cop{Client: 1, Op: "write", Key: "k", ID: id(int(i)), Class: "ack", Value: strconv.FormatInt(i, 10), Start: 10 * i, End: 10*i + 5},
				cop{Client: 2, Op: "read", Key: "k", Class: "ack", Value: strconv.FormatInt(i, 10), IDs: slices.Clone(ids[max(0, len(ids)-8):]), Start: 10*i + 6, End: 10*i + 8})
		}
		return h
	}
	cases := []struct {
		name string
		mut  func([]cop) []cop
	}{
		{"clean", func(h []cop) []cop { return h }},
		{"stale read", func(h []cop) []cop { h[21].Value, h[21].IDs = "10", h[19].IDs; return h }},
		{"overwritten write", func(h []cop) []cop { h[21].IDs[7] = id(99); return h }},
		{"duplicate effect", func(h []cop) []cop { h[21].IDs[6] = h[21].IDs[7]; return h }},
		{"failed write applied", func(h []cop) []cop { h[20].Class = "failed"; return h }},
		{"phantom commit", func(h []cop) []cop { return slices.Delete(h, 20, 21) }},
		{"read missing rows", func(h []cop) []cop { h[21].IDs = h[21].IDs[1:]; return h }},
		{"unknown write applied", func(h []cop) []cop { h[20].Class, h[20].End = "unknown", math.MaxInt64; return h }},
	}
	for _, c := range cases {
		var b []byte
		for _, o := range c.mut(clean()) {
			l, _ := json.Marshal(o)
			b = append(append(b, l...), '\n')
		}
		r, err := checkStream(c.name, bytes.NewReader(b), 100, 1000)
		if err != nil {
			return err
		}
		if want := c.name == "clean" || c.name == "unknown write applied"; (r.Verdict == "pass") != want {
			return fmt.Errorf("negative control %q: verdict %s, violations %q", c.name, r.Verdict, r.Violations)
		}
	}
	return nil
}
