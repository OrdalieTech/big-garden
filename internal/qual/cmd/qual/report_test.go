package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

// TestReportStatuses: a gate passes only with its evidence file; a missing
// evidence file or artifact is not_run; a smoke run never passes; a failing
// fault row fails its rows; missing remote configuration is blocked.
func TestReportStatuses(t *testing.T) {
	dir := t.TempDir()
	put := func(rel string, v any) {
		p := filepath.Join(dir, rel)
		os.MkdirAll(filepath.Dir(p), 0o755)
		b, _ := json.Marshal(v)
		if err := os.WriteFile(p, b, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	put("local-stress/summary.json", stressSummary{Verdict: "pass", Profiles: []string{"Q1"}, Delay: "0s", Gates: []*gate{
		{ID: "18.2/q1_throughput_3n", Status: "pass", Evidence: []string{"run-1/Q1.json"}},
		{ID: "18.2/warm_read", Status: "pass", Evidence: []string{"run-1/missing.json"}}}})
	put("local-stress/run-1/Q1.json", map[string]any{})
	put("smoke-stress/summary.json", stressSummary{Verdict: "pass", Smoke: true, Profiles: []string{"Q2"}, Delay: "0s", Gates: []*gate{
		{ID: "18.2/q2_b1_mutations_3n", Status: "pass", Evidence: []string{"run-1/Q2.json"}}}})
	put("smoke-stress/run-1/Q2.json", map[string]any{})
	put("local-faults/faults.json", map[string]any{"rows": []map[string]any{{"row": 1, "verdict": "pass", "reps": 100}, {"row": 2, "verdict": "fail", "reps": 100}}})
	put("check/checks.json", []map[string]any{{"verdict": "pass"}})
	for _, k := range []string{"QUAL_S3_ENDPOINT", "QUAL_S3_REGION", "QUAL_S3_BUCKET", "QUAL_S3_PREFIX"} {
		t.Setenv(k, "")
	}
	if err := report([]string{"--artifacts", dir}); err == nil {
		t.Fatal("report passed with missing evidence")
	}
	b, err := os.ReadFile(filepath.Join(dir, "qualification.json"))
	if err != nil {
		t.Fatal(err)
	}
	var rows []reportRow
	json.Unmarshal(b, &rows)
	status := map[string]string{}
	for _, r := range rows {
		status[r.Gate+"/"+r.Scope] = r.Status
	}
	for k, want := range map[string]string{
		"q1_throughput_3n/local":             "pass",
		"warm_read/local":                    "not_run",
		"q2_b1_mutations_3n/local":           "not_run",
		"zero invalid current-root chains/-": "pass",
		"zero stale-writer publications past a successful takeover fence/local":  "fail",
		"zero stale-writer publications past a successful takeover fence/remote": "blocked_missing_remote_configuration",
		"q1_throughput_3n/remote":                    "blocked_missing_remote_configuration",
		"zero races in the required race campaign/-": "not_run",
		"footprint budgets/-":                        "not_run",
	} {
		if status[k] != want {
			t.Errorf("%s: %q, want %q", k, status[k], want)
		}
	}
	if _, err := os.Stat(filepath.Join(dir, "manifest.json")); err != nil {
		t.Fatal(err)
	}
}
