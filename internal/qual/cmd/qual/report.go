package main

// report assembles the PLAN §18 gate table and the §22 evidence manifest
// from an artifacts directory. It never runs a test: every status comes
// from an artifact file, which the row names as its evidence. A missing
// artifact is not_run; missing remote configuration (no remote artifact
// and no QUAL_S3_* / AWS_* environment, or a remote run that recorded it)
// is blocked_missing_remote_configuration. A smoke stress run (reduced
// --scale, runs, warmup or measure) or a soak shorter than 24 h / 10
// million operations is indicative only: not_run, with its value shown.
//
// Fields read, by artifact (found anywhere under --artifacts by name):
//
//	doctor.json        (qual doctor)       Pass, Go
//	footprint.json     (qual footprint)    verdict
//	reproducible.json  (qual reproducible) verdict
//	model.json         (qual model)        passed
//	summary.json with Profiles  (qual stress --profiles) Verdict, Smoke,
//	                   Remote, Delay, Finished, Gates[].{ID, Status, Value,
//	                   Target, Detail, Evidence} (evidence files relative to
//	                   the summary's directory; each must exist)
//	summary.json with WindowsOK (qual soak) Verdict, Failures, Elapsed, Ops,
//	                   WindowsOK, WindowsFailed, MaxGoroutines, MaxFDs,
//	                   LedgerChecked; report.jsonl beside it is hashed only
//	env.json           (qual stress)       commit, go (consistency check)
//	*.json under a directory named *faults* (qual faults) and under check/
//	                   or named checks.json (qual check): only row, verdict
//	                   (pass/fail) and reps, from an object, an array of
//	                   objects, or an object whose rows array holds them
//	*.json under a directory named *cost* (qual cost --json): presence only
//	*.json under race/ and milestones/<M>.json: verdict
//
// Directories under a path containing "remote" are the remote scope.
// Outputs: --out (markdown), qualification.json (the rows) and
// manifest.json (SHA-256 of every artifact, research.lock.json, toolchain,
// commit) beside it. Exit status is nonzero unless every required row
// passes: local rows always, remote rows with --require-remote, soak rows
// with --require-soak.

import (
	"bytes"
	"cmp"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"time"

	"github.com/OrdalieTech/big-garden/internal/storetest"
)

type reportRow struct {
	Section, Gate, Scope string
	Status               string // pass, fail, provider_blocked, blocked_missing_remote_configuration, not_run, info
	Detail               string
	Evidence             []string
	Required             bool
}

type verdictRow struct {
	Row     json.RawMessage `json:"row"`
	Verdict string          `json:"verdict"`
	Status  string          `json:"status"` // qual faults rows say status`
	Reps    int             `json:"reps"`
	File    string          `json:"-"`
}

type reportRun struct {
	dir           string
	files         []string // every artifact, relative to dir
	stress, soaks map[string][]string
	faults        map[string][]verdictRow
	checks        []verdictRow
	blockedRemote bool // no remote configuration in this environment
	rows          []reportRow
}

func report(args []string) error {
	fl := flag.NewFlagSet("report", flag.ContinueOnError)
	dir := fl.String("artifacts", "artifacts", "artifacts directory")
	out := fl.String("out", "", "markdown report (default <artifacts>/qualification.md)")
	requireRemote := fl.Bool("require-remote", false, "remote rows must pass")
	requireSoak := fl.Bool("require-soak", false, "soak rows must pass")
	milestones := fl.String("require-milestones", "", "comma-separated milestones (artifacts/milestones/<M>.json verdict)")
	if err := fl.Parse(args); err != nil {
		return err
	}
	r := &reportRun{dir: *dir, stress: map[string][]string{}, soaks: map[string][]string{}, faults: map[string][]verdictRow{}}
	if *out == "" {
		*out = filepath.Join(*dir, "qualification.md")
	}
	outDir := filepath.Dir(*out)
	own := map[string]bool{}
	for _, f := range []string{*out, filepath.Join(outDir, "qualification.json"), filepath.Join(outDir, "manifest.json")} {
		if rel, err := filepath.Rel(*dir, f); err == nil {
			own[rel] = true
		}
	}
	err := filepath.WalkDir(*dir, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(*dir, p)
		if d.IsDir() {
			if d.Name() == "cache" { // node caches are not evidence
				return filepath.SkipDir
			}
			return nil
		}
		if !own[rel] {
			r.files = append(r.files, rel)
		}
		return nil
	})
	if err != nil && !errors.Is(err, fs.ErrNotExist) {
		return err
	}
	_, rerr := storetest.RemoteFromEnv()
	r.blockedRemote = errors.Is(rerr, storetest.ErrBlockedRemote)
	r.classify()
	r.safety()
	r.stressRows(*requireRemote)
	r.soakRows(*requireSoak, *requireRemote)
	r.release(*requireRemote, *milestones)
	man := r.manifest()
	if bad, _ := man["inconsistent"].([]string); len(bad) > 0 {
		r.add(reportRow{Section: "§20.5", Gate: "environment consistency", Scope: "-", Status: "fail", Detail: strings.Join(bad, "; "), Required: true})
	}
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		return err
	}
	b, _ := json.MarshalIndent(r.rows, "", "  ")
	if err := os.WriteFile(filepath.Join(outDir, "qualification.json"), append(b, '\n'), 0o644); err != nil {
		return err
	}
	b, _ = json.MarshalIndent(man, "", "  ")
	if err := os.WriteFile(filepath.Join(outDir, "manifest.json"), append(b, '\n'), 0o644); err != nil {
		return err
	}
	if err := os.WriteFile(*out, r.markdown(man), 0o644); err != nil {
		return err
	}
	count := map[string]int{}
	var failed []string
	for _, row := range r.rows {
		count[row.Status]++
		if row.Required && row.Status != "pass" {
			failed = append(failed, fmt.Sprintf("%s %s (%s): %s", row.Section, row.Gate, row.Scope, row.Status))
		}
	}
	fmt.Printf("qual report: %d rows %v; %d required rows not passing; wrote %s\n", len(r.rows), count, len(failed), *out)
	if len(failed) > 0 {
		return fmt.Errorf("release not permitted: %d required rows not passing (first: %s)", len(failed), failed[0])
	}
	return nil
}

func (r *reportRun) add(row reportRow) { r.rows = append(r.rows, row) }

func scopeOf(rel string) string {
	if strings.Contains(rel, "remote") {
		return "remote"
	}
	return "local"
}

func (r *reportRun) read(rel string, v any) error {
	b, err := os.ReadFile(filepath.Join(r.dir, rel))
	if err == nil {
		err = json.Unmarshal(b, v)
	}
	return err
}

// classify sorts the JSON artifacts by kind.
func (r *reportRun) classify() {
	for _, rel := range r.files {
		if !strings.HasSuffix(rel, ".json") {
			continue
		}
		parts := strings.Split(filepath.ToSlash(filepath.Dir(rel)), "/")
		in := func(f func(string) bool) bool { return slices.ContainsFunc(parts, f) }
		switch {
		case filepath.Base(rel) == "summary.json":
			var probe map[string]json.RawMessage
			if r.read(rel, &probe) != nil {
				continue
			}
			if _, ok := probe["Profiles"]; ok {
				r.stress[scopeOf(rel)] = append(r.stress[scopeOf(rel)], rel)
			} else if _, ok := probe["WindowsOK"]; ok {
				r.soaks[scopeOf(rel)] = append(r.soaks[scopeOf(rel)], rel)
			}
		case in(func(s string) bool { return strings.Contains(s, "faults") }) && filepath.Base(rel) == "faults.json": // not its lanes' own artifacts
			r.faults[scopeOf(rel)] = append(r.faults[scopeOf(rel)], r.verdicts(rel)...)
		case in(func(s string) bool { return s == "check" }) || filepath.Base(rel) == "checks.json":
			r.checks = append(r.checks, r.verdicts(rel)...)
		}
	}
}

// verdicts reads row/verdict/reps from an object, an array, or an object's
// rows array; an unreadable file is one failing entry.
func (r *reportRun) verdicts(rel string) []verdictRow {
	b, err := os.ReadFile(filepath.Join(r.dir, rel))
	var list []verdictRow
	var one struct {
		verdictRow
		Rows []verdictRow `json:"rows"`
	}
	switch {
	case err != nil:
	case json.Unmarshal(b, &list) == nil:
	case json.Unmarshal(b, &one) == nil && len(one.Rows) > 0:
		list = one.Rows
	case json.Unmarshal(b, &one) == nil:
		list = []verdictRow{one.verdictRow}
	default:
		err = errors.New("not a verdict file")
	}
	if err != nil {
		return []verdictRow{{Row: json.RawMessage(`"unreadable"`), Verdict: "fail: " + err.Error(), File: rel}}
	}
	for i := range list {
		list[i].File, list[i].Verdict = rel, cmp.Or(list[i].Verdict, list[i].Status)
		if len(list[i].Row) == 0 {
			list[i].Row, _ = json.Marshal(fmt.Sprintf("%s#%d", filepath.Base(rel), i))
		}
	}
	return list
}

// aggregate is pass when every entry passed with reps > 0 (reps 0 counts
// as a single checked history for qual check), fail when one failed.
func aggregate(rows []verdictRow) (status, detail string, files []string) {
	if len(rows) == 0 {
		return "not_run", "no artifact", nil
	}
	status = "pass"
	bad := 0
	for _, v := range rows {
		if !slices.Contains(files, v.File) {
			files = append(files, v.File)
		}
		if v.Verdict != "pass" {
			status, bad = "fail", bad+1
			if detail == "" {
				detail = fmt.Sprintf("first non-pass: row %s %q", v.Row, v.Verdict)
			}
		}
	}
	return status, fmt.Sprintf("%d of %d entries pass. %s", len(rows)-bad, len(rows), detail), files
}

// safety is §18.1: the independent history check (qual check) is the
// required evidence of each history property, the fault matrix (qual
// faults) of the fault properties; the stress Q7 and soak verdicts are
// supporting evidence that can only turn a row to fail.
func (r *reportRun) safety() {
	cs, cd, cf := aggregate(r.checks)
	for _, g := range []string{"zero lost driver-observed acknowledged mutations", "zero duplicated effects for an unexpired operation identity",
		"zero receipt/input-hash conflicts accepted", "zero invalid current-root chains",
		"zero strong-read linearizability violations in checked histories", "unknown outcomes resolvable after stable connectivity"} {
		r.add(reportRow{Section: "§18.1", Gate: g, Scope: "-", Status: cs, Detail: "qual check: " + cd, Evidence: cf, Required: true})
	}
	for _, scope := range []string{"local", "remote"} {
		st, fd, ff := aggregate(r.faults[scope])
		if st == "not_run" && scope == "remote" && r.blockedRemote {
			st = storetest.ErrBlockedRemote.Error()
		}
		for _, g := range []string{"zero stale-writer publications past a successful takeover fence",
			"zero successful initialization from ambiguous missing existing state",
			"every scheduled fault completes validly or fails explicitly within bounds"} {
			r.add(reportRow{Section: "§18.1", Gate: g, Scope: scope, Status: st, Detail: "qual faults: " + fd, Evidence: ff, Required: scope == "local"})
		}
	}
	var race []verdictRow
	for _, f := range r.files {
		if strings.HasPrefix(filepath.ToSlash(f), "race/") && strings.HasSuffix(f, ".json") {
			race = append(race, r.verdicts(f)...)
		}
	}
	rs, rd, rf := aggregate(race)
	r.add(reportRow{Section: "§18.1", Gate: "zero races in the required race campaign", Scope: "-", Status: rs, Detail: rd, Evidence: rf, Required: true})
}

// stressGates are the stress gate IDs in §18 order.
var stressGates = []string{
	"18.1/q7_acknowledged_ledger", "18.1/q7_unknown_resolved", "18.1/capacity_refusal_q5",
	"18.2/warm_read", "18.2/warm_mutation_b1", "18.2/cold_activation_1mib_8d", "18.2/cold_activation_64mib", "18.2/takeover_small",
	"18.2/q1_throughput_3n", "18.2/q2_b1_mutations_3n", "18.2/scale_3_vs_1", "18.2/error_rate_q1", "18.2/error_rate_q2", "18.2/error_rate_q3",
	"18.2/backlog_recovery_q7", "18.2/bounded_admission_q7", "18.2/hotspot_q4",
	"18.3/rss_q1_q2", "18.3/rss_q3", "18.3/cache_budget_q3", "18.3/requests_per_publication", "18.3/puts_per_b1_mutation",
	"18.3/fresh_delta_1kib", "18.3/upload_vs_changed_pages", "18.3/capture_cpu_64mib_vs_64kib", "18.3/cold_activation_reads",
}

// stressRows takes each gate from the best stress summary of its scope
// that has it: a full (non-smoke) undelayed run first, then the latest.
func (r *reportRun) stressRows(requireRemote bool) {
	type sum struct {
		rel string
		s   stressSummary
	}
	for _, scope := range []string{"local", "remote"} {
		var sums []sum
		for _, rel := range r.stress[scope] {
			var s stressSummary
			if r.read(rel, &s) == nil {
				sums = append(sums, sum{rel, s})
			}
		}
		slices.SortFunc(sums, func(a, b sum) int { // best first
			ka := fmt.Sprint(!a.s.Smoke, a.s.Delay == "0s", a.s.Finished.Format(time.RFC3339))
			kb := fmt.Sprint(!b.s.Smoke, b.s.Delay == "0s", b.s.Finished.Format(time.RFC3339))
			return strings.Compare(kb, ka)
		})
		for _, id := range stressGates {
			row := reportRow{Section: "§" + id[:4], Gate: id[5:], Scope: scope, Status: "not_run", Detail: "no stress artifact with this gate",
				Required: (scope == "local" || requireRemote) && !strings.HasSuffix(id, "_q4") && !strings.HasPrefix(id[5:], "bounded")}
			if scope == "remote" && r.blockedRemote && len(sums) == 0 {
				row.Status = storetest.ErrBlockedRemote.Error()
			}
			for _, sm := range sums {
				if sm.s.Verdict == storetest.ErrBlockedRemote.Error() {
					row.Status, row.Detail, row.Evidence = sm.s.Verdict, sm.s.Error, []string{sm.rel}
					break
				}
				i := slices.IndexFunc(sm.s.Gates, func(g *gate) bool { return g.ID == id })
				if i < 0 {
					continue
				}
				g := sm.s.Gates[i]
				row.Evidence, row.Status = []string{sm.rel}, g.Status
				row.Detail = fmt.Sprintf("%s; target %s; per run %v", g.Detail, g.Target, g.Values)
				for _, e := range g.Evidence {
					p := filepath.Join(filepath.Dir(sm.rel), e)
					if _, err := os.Stat(filepath.Join(r.dir, p)); err != nil {
						row.Status, row.Detail = "not_run", "evidence file missing: "+p
					}
					row.Evidence = append(row.Evidence, p)
				}
				if len(g.Evidence) == 0 {
					row.Status, row.Detail = "not_run", "gate without an evidence file"
				}
				switch {
				case row.Status == "not_run":
				case sm.s.Smoke:
					row.Status, row.Detail = "not_run", "smoke run only, indicative: "+g.Status+": "+row.Detail
				case sm.s.Delay != "0s":
					row.Status, row.Detail = "info", "delayed-store profile ("+sm.s.Delay+"): "+row.Detail
				}
				if row.Status == "info" {
					row.Required = false
				}
				break
			}
			r.add(row)
		}
	}
}

// soakRows are the Q6 rows (§18.3, and the soak's ledger and porcupine
// windows as supporting §18.1 evidence).
func (r *reportRun) soakRows(requireSoak, requireRemote bool) {
	for _, scope := range []string{"local", "remote"} {
		var best string
		var s soakSummary
		for _, rel := range r.soaks[scope] {
			var x soakSummary
			if r.read(rel, &x) == nil && (best == "" || x.Ops > s.Ops) {
				best, s = rel, x
			}
		}
		req := requireSoak && (scope == "local" || requireRemote)
		for _, g := range []struct{ name, fail string }{
			{"Q6 memory growth at most 5% in the final six hours", "grew"},
			{"goroutines <= 1,024 and descriptors <= 2,500, no growth with history", "goroutines|descriptors|fds"},
			{"Q6 ledger: no lost or duplicated acknowledged write", "ledger|committed twice|lookup"},
			{"Q6 porcupine windows linearizable", "window|porcupine"},
		} {
			row := reportRow{Section: "§18.3", Gate: g.name, Scope: scope, Status: "not_run", Detail: "no soak artifact", Required: req}
			if strings.HasPrefix(g.name, "Q6 ledger") || strings.HasPrefix(g.name, "Q6 porcupine") {
				row.Section = "§18.1"
			}
			if best == "" {
				if scope == "remote" && r.blockedRemote {
					row.Status = storetest.ErrBlockedRemote.Error()
				}
				r.add(row)
				continue
			}
			row.Evidence = []string{best}
			if rj := filepath.Join(filepath.Dir(best), "report.jsonl"); slices.Contains(r.files, rj) {
				row.Evidence = append(row.Evidence, rj)
			}
			row.Status = "pass"
			var hits []string
			for _, f := range s.Failures {
				for _, k := range strings.Split(g.fail, "|") {
					if strings.Contains(f, k) {
						hits = append(hits, f)
						break
					}
				}
			}
			if len(hits) > 0 || (strings.Contains(g.name, "porcupine") && (s.WindowsFailed > 0 || s.WindowsOK == 0)) {
				row.Status = "fail"
			}
			el, _ := time.ParseDuration(s.Elapsed)
			row.Detail = fmt.Sprintf("soak %s: %d operations in %s, ledger %d checked, windows %d ok / %d failed, max goroutines %d, max FDs %d; %s",
				s.Verdict, s.Ops, s.Elapsed, s.LedgerChecked, s.WindowsOK, s.WindowsFailed, s.MaxGoroutines, s.MaxFDs, strings.Join(hits, "; "))
			inconclusive := slices.ContainsFunc(s.Failures, func(f string) bool { return strings.Contains(f, "inconclusive") })
			switch {
			case row.Status == "fail": // a violation counts however short the soak
			case strings.HasPrefix(g.name, "Q6 memory") && inconclusive:
				row.Status, row.Detail = "not_run", "growth inconclusive: "+row.Detail
			case el < 24*time.Hour || s.Ops < 10_000_000:
				row.Status, row.Detail = "not_run", "short soak, indicative (Q6 needs 24 h and 10 million operations): "+row.Detail
			}
			r.add(row)
		}
	}
}

// release adds the non-§18 evidence §22 requires: doctor, model,
// footprint, reproducibility, cost, milestones.
func (r *reportRun) release(requireRemote bool, milestones string) {
	find := func(base, scope string) []string {
		var out []string
		for _, f := range r.files {
			if filepath.Base(f) == base && (scope == "" || scopeOf(f) == scope) {
				out = append(out, f)
			}
		}
		return out
	}
	one := func(section, gate, scope string, files []string, ok func(string) (bool, string), req bool) {
		row := reportRow{Section: section, Gate: gate, Scope: scope, Status: "not_run", Detail: "no artifact", Required: req}
		if len(files) == 0 && scope == "remote" && r.blockedRemote {
			row.Status = storetest.ErrBlockedRemote.Error()
		}
		for _, f := range files {
			good, d := ok(f)
			row.Evidence, row.Detail, row.Status = append(row.Evidence, f), d, "pass"
			if !good {
				row.Status = "fail"
				break
			}
		}
		r.add(row)
	}
	verdict := func(field string) func(string) (bool, string) {
		return func(f string) (bool, string) {
			var m map[string]any
			if err := r.read(f, &m); err != nil {
				return false, err.Error()
			}
			v := fmt.Sprint(m[field])
			return v == "pass" || v == "true", field + " " + v
		}
	}
	for _, scope := range []string{"local", "remote"} {
		one("§20.3/4", "store doctor: conditional writes, single CAS winner", scope, find("doctor.json", scope), verdict("Pass"), scope == "local" || requireRemote)
	}
	one("§20.2", "model exploration and counterexamples", "-", find("model.json", ""), verdict("passed"), true)
	one("§12", "footprint budgets", "-", find("footprint.json", ""), verdict("verdict"), true)
	one("§20.1", "reproducible pure-Go build", "-", find("reproducible.json", ""), verdict("verdict"), true)
	var cost []string
	for _, f := range r.files {
		if strings.Contains(filepath.ToSlash(filepath.Dir(f)), "cost") && strings.HasSuffix(f, ".json") {
			cost = append(cost, f)
		}
	}
	one("§19", "measured cost model output present", "-", cost, func(string) (bool, string) { return true, "present (no pass/fail)" }, false)
	if milestones == "" {
		return
	}
	for _, m := range strings.Split(milestones, ",") {
		one("§22", "milestone "+m+" record", "-", find(m+".json", ""), verdict("verdict"), true)
	}
}

// manifest is §22's evidence manifest.
func (r *reportRun) manifest() map[string]any {
	type art struct {
		Path   string
		Bytes  int64
		SHA256 string
	}
	hash := func(p string) (string, int64) {
		b, err := os.ReadFile(p)
		if err != nil {
			return "unreadable: " + err.Error(), 0
		}
		h := sha256.Sum256(b)
		return hex.EncodeToString(h[:]), int64(len(b))
	}
	var arts []art
	for _, f := range r.files {
		h, n := hash(filepath.Join(r.dir, f))
		arts = append(arts, art{f, n, h})
	}
	git := func(a ...string) string {
		b, err := exec.Command("git", a...).Output()
		if err != nil {
			return "unavailable"
		}
		return strings.TrimSpace(string(b))
	}
	gov, _ := exec.Command("go", "version").Output()
	m := map[string]any{"generated": time.Now().UTC(), "commit": git("rev-parse", "HEAD"), "dirty_tree": git("status", "--porcelain") != "",
		"toolchain": map[string]string{"report_binary": runtime.Version(), "go_version": strings.TrimSpace(string(gov)), "goos": runtime.GOOS, "goarch": runtime.GOARCH},
		"artifacts": arts}
	for _, f := range []string{"go.sum", "research.lock.json"} {
		h, _ := hash(f)
		m[f+"_sha256"] = h
	}
	var lock map[string]json.RawMessage
	if b, err := os.ReadFile("research.lock.json"); err == nil && json.Unmarshal(b, &lock) == nil {
		m["research_lock_toolchain"] = lock["toolchain"]
	}
	var bad []string
	for _, f := range r.files {
		if filepath.Base(f) != "env.json" {
			continue
		}
		var e map[string]any
		if r.read(f, &e) != nil {
			continue
		}
		if c := fmt.Sprint(e["commit"]); c != m["commit"] {
			bad = append(bad, fmt.Sprintf("%s ran at commit %s, not %s", f, c, m["commit"]))
		}
		if g := fmt.Sprint(e["go"]); g != runtime.Version() {
			bad = append(bad, fmt.Sprintf("%s ran with %s, not %s", f, g, runtime.Version()))
		}
	}
	m["inconsistent"] = bad
	return m
}

func (r *reportRun) markdown(man map[string]any) []byte {
	var b bytes.Buffer
	count := map[string]int{}
	for _, row := range r.rows {
		count[row.Status]++
	}
	fmt.Fprintf(&b, "# Qualification report\n\nGenerated %s from `%s` at commit `%s` (dirty tree: %v), toolchain %v.\n\n",
		man["generated"].(time.Time).Format(time.RFC3339), r.dir, man["commit"], man["dirty_tree"], man["toolchain"])
	fmt.Fprintf(&b, "Rows by status: %v. Release is permitted only when every required row passes (PLAN §22).\n\n", count)
	b.WriteString("| Section | Gate | Scope | Status | Required | Detail | Evidence |\n|---|---|---|---|---|---|---|\n")
	cell := func(s string) string { return strings.NewReplacer("|", "\\|", "\n", " ").Replace(s) }
	for _, row := range r.rows {
		fmt.Fprintf(&b, "| %s | %s | %s | **%s** | %v | %s | %s |\n", row.Section, cell(row.Gate), row.Scope, row.Status, row.Required,
			cell(row.Detail), cell(strings.Join(row.Evidence, ", ")))
	}
	b.WriteString("\n## Fault matrix rows (qual faults)\n\n| Scope | Row | Verdict | Reps | File |\n|---|---|---|---|---|\n")
	for _, scope := range []string{"local", "remote"} {
		for _, v := range r.faults[scope] {
			fmt.Fprintf(&b, "| %s | %s | %s | %d | %s |\n", scope, cell(string(v.Row)), cell(v.Verdict), v.Reps, v.File)
		}
	}
	b.WriteString("\n## Independent checks (qual check)\n\n| Row | Verdict | Reps | File |\n|---|---|---|---|\n")
	for _, v := range r.checks {
		fmt.Fprintf(&b, "| %s | %s | %d | %s |\n", cell(string(v.Row)), cell(v.Verdict), v.Reps, v.File)
	}
	fmt.Fprintf(&b, "\n## Evidence manifest\n\n%d artifacts hashed in manifest.json; go.sum %s; research.lock.json %s.\n",
		len(r.files), man["go.sum_sha256"], man["research.lock.json_sha256"])
	if bad, _ := man["inconsistent"].([]string); len(bad) > 0 {
		fmt.Fprintf(&b, "\nEnvironment inconsistencies: %s\n", strings.Join(bad, "; "))
	}
	return b.Bytes()
}
