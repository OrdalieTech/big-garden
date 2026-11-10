package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
)

// reproducible (PLAN §12.3, §20.1): `go mod verify`, then the minimal
// consumer built from N clean `git archive HEAD` trees with the pinned flags;
// every binary must hash the same. With -offline-second-build every build
// after the first runs with GOPROXY=off and GOFLAGS=-mod=readonly, from the
// module cache the first one verified.
func reproducible(args []string) error {
	fs := flag.NewFlagSet("reproducible", flag.ContinueOnError)
	n := fs.Int("clean-builds", 2, "")
	offline := fs.Bool("offline-second-build", true, "")
	out := fs.String("out", "artifacts/reproducible", "")
	if err := fs.Parse(args); err != nil {
		return err
	}
	run := func(dir string, env []string, name string, a ...string) (string, error) {
		cmd := exec.Command(name, a...)
		cmd.Dir, cmd.Env = dir, append(os.Environ(), env...)
		b, err := cmd.CombinedOutput()
		if err != nil {
			return "", fmt.Errorf("%s %s: %v: %s", name, strings.Join(a, " "), err, b)
		}
		return strings.TrimSpace(string(b)), nil
	}
	rev, err := run("", nil, "git", "rev-parse", "HEAD")
	if err != nil {
		return err
	}
	if _, err := run("", nil, "go", "mod", "verify"); err != nil {
		return err
	}
	flags := []string{"build", "-trimpath", "-buildvcs=false", "-ldflags=-s -w -buildid=", "-o", "consumer", "./internal/qual/consumer"}
	r := map[string]any{"commit": rev, "go": runtime.Version(), "goos": runtime.GOOS, "goarch": runtime.GOARCH,
		"flags": flags, "env": "GOTOOLCHAIN=local CGO_ENABLED=0", "mod_verify": "all modules verified"}
	var hashes []string
	for i := range *n {
		dir, err := os.MkdirTemp("", "qual-repro-")
		if err != nil {
			return err
		}
		defer os.RemoveAll(dir)
		if _, err := run("", nil, "sh", "-c", "git archive HEAD | tar -x -C "+dir); err != nil {
			return err
		}
		env := []string{"GOTOOLCHAIN=local", "CGO_ENABLED=0"}
		if i > 0 && *offline {
			env = append(env, "GOPROXY=off", "GOFLAGS=-mod=readonly")
		}
		if _, err := run(dir, env, "go", flags...); err != nil {
			return err
		}
		b, err := os.ReadFile(filepath.Join(dir, "consumer"))
		if err != nil {
			return err
		}
		h := sha256.Sum256(b)
		hashes = append(hashes, hex.EncodeToString(h[:]))
	}
	r["sha256"], r["verdict"] = hashes, "pass"
	for _, h := range hashes[1:] {
		if h != hashes[0] {
			r["verdict"] = "fail"
		}
	}
	os.MkdirAll(*out, 0o755)
	b, _ := json.MarshalIndent(r, "", "  ")
	if err := os.WriteFile(filepath.Join(*out, "reproducible.json"), append(b, '\n'), 0o644); err != nil {
		return err
	}
	fmt.Printf("%s\n", b)
	if r["verdict"] != "pass" {
		return fmt.Errorf("clean builds differ: %v", hashes)
	}
	return nil
}
