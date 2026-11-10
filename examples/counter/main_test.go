package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/OrdalieTech/big-garden/internal/storetest"
)

// s3Env returns the environment of a counter process: the caller's S3
// variables, defaulting to the local RustFS fixture, and a fresh prefix.
func s3Env(t *testing.T) []string {
	if os.Getenv("GARDEN_TEST_S3") != "1" {
		t.Skip("set GARDEN_TEST_S3=1 to run against the local S3 fixture")
	}
	if os.Getenv("GARDEN_S3_ENDPOINT") == "" { // start the fixture if no other test did
		if _, err := storetest.StartLocal(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	b := make([]byte, 6)
	rand.Read(b)
	env := append(os.Environ(), "GARDEN_S3_PREFIX=examples/counter-"+hex.EncodeToString(b))
	for k, v := range map[string]string{"AWS_ACCESS_KEY_ID": "staterustfs", "AWS_SECRET_ACCESS_KEY": "staterustfs-secret"} {
		if os.Getenv(k) == "" {
			env = append(env, k+"="+v)
		}
	}
	return env
}

type proc struct {
	t        *testing.T
	bin, dir string
	env      []string
}

// run runs the counter binary as node and decodes its JSON line, or returns
// its stderr as an error.
func (p proc) run(node string, args ...string) (output, string) {
	p.t.Helper()
	cmd := exec.Command(p.bin, append([]string{"-node", node, "-cache", filepath.Join(p.dir, node)}, args...)...)
	cmd.Env = p.env
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	if err := cmd.Run(); err != nil {
		return output{}, strings.TrimSpace(stderr.String())
	}
	var out output
	if err := json.Unmarshal(stdout.Bytes(), &out); err != nil {
		p.t.Fatalf("%s %v: %v in %q", node, args, err, stdout.String())
	}
	return out, ""
}

func (p proc) ok(node string, args ...string) output {
	p.t.Helper()
	out, errText := p.run(node, args...)
	if errText != "" {
		p.t.Fatalf("%s %v: %s", node, args, errText)
	}
	return out
}

func TestCounter(t *testing.T) {
	env := s3Env(t)
	dir := t.TempDir()
	bin := filepath.Join(dir, "counter")
	build := exec.Command("go", "build", "-o", bin, ".")
	build.Env = append(os.Environ(), "CGO_ENABLED=0")
	if b, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build: %v\n%s", err, b)
	}
	p := proc{t: t, bin: bin, dir: dir, env: env}

	// Create at schema 1, then an explicit migration to schema 2.
	if out := p.ok("a", "create", "foo"); out.Sequence != 1 {
		t.Fatalf("create: %+v", out)
	}
	if out := p.ok("a", "migrate", "foo"); out.Sequence != 2 || out.Value != 0 {
		t.Fatalf("migrate: %+v", out)
	}
	if out := p.ok("a", "migrate", "foo"); out.Sequence != 2 {
		t.Fatalf("second migrate must be a no-op: %+v", out)
	}
	// A second counter is upgraded lazily by its first increment.
	p.ok("a", "create", "bar")
	if out := p.ok("a", "incr", "bar"); out.Value != 1 || out.Sequence != 3 {
		t.Fatalf("incr with lazy migration: %+v", out)
	}

	// Update, then a retry of the same request replays its stored result.
	p.ok("a", "incr", "-by", "5", "foo")
	first := p.ok("a", "incr", "-by", "2", "foo")
	if first.Value != 7 || first.Replayed {
		t.Fatalf("incr: %+v", first)
	}
	retry := p.ok("a", "incr", "-by", "2", "-op", first.Op, "foo")
	if !retry.Replayed || retry.Value != 7 || retry.Sequence != first.Sequence {
		t.Fatalf("retry: %+v, first %+v", retry, first)
	}
	if _, errText := p.run("a", "incr", "-by", "3", "-op", first.Op, "foo"); !strings.Contains(errText, "id_conflict") {
		t.Fatalf("same ID, other input: %q", errText)
	}
	if out := p.ok("a", "get", "foo"); out.Value != 7 || out.Sequence != first.Sequence {
		t.Fatalf("get: %+v", out)
	}
	if out := p.ok("a", "lookup", "foo", first.Op); !out.Replayed || out.Value != 7 || out.Sequence != first.Sequence {
		t.Fatalf("lookup: %+v", out)
	}

	// Node b finds the counter owned by node a and takes it over.
	if out := p.ok("b", "incr", "foo"); !out.TookOver || out.Value != 8 || out.Owner != "b" {
		t.Fatalf("takeover by b: %+v", out)
	}
	// A strong read from node a sees b's write and leaves b the owner (D24).
	if out := p.ok("a", "get", "foo"); out.TookOver || out.Value != 8 {
		t.Fatalf("read by a: %+v", out)
	}

	// Export from a fresh process, then Import under another key.
	t.Run("export-import", func(t *testing.T) {
		p := p
		p.t = t
		exportImport(p, filepath.Join(dir, "foo.tar"), first.Sequence+1)
	})
}

func exportImport(p proc, archive string, seq uint64) {
	t := p.t
	if out := p.ok("a", "export", "foo", archive); out.Sequence != seq {
		t.Fatalf("export: %+v", out)
	}
	if out := p.ok("b", "import", "foo-copy", archive); out.Value != 8 {
		t.Fatalf("import: %+v", out)
	}
	if out := p.ok("a", "get", "foo-copy"); out.Value != 8 {
		t.Fatalf("get copy: %+v", out)
	}
}
