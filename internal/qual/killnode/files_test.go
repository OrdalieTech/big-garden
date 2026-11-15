package main

import (
	"fmt"
	"math/rand/v2"
	"os"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/OrdalieTech/big-garden/internal/storetest"
)

// TestProcessKillFiles is the kill matrix of D33 files: one node attaches
// and detaches files (PutBlob, then an Update linking it with a log row)
// and is SIGKILLed at the nth pass of a failpoint of the upload, of the
// publication or of the sweep maintenance runs after each file mutation.
// A new process retries the unknown operation with the same ID, then every
// file the image names must read back whole with its bytes, every
// acknowledged attach not detached since must be there, and after more
// mutations and their sweeps the files must still read back.
func TestProcessKillFiles(t *testing.T) {
	base := storetest.Local(t)
	runs := 10
	if s := os.Getenv("GARDEN_KILL_RUNS"); s != "" {
		runs, _ = strconv.Atoi(s)
	}
	for _, fp := range []string{"blob.put", "blob.after", "sql.after", "cas.before", "cas.after", "sweep.del", "reply"} {
		t.Run(fp, func(t *testing.T) {
			killed := 0
			for i := range runs {
				ep := base.Sub(fmt.Sprintf("files-%s-%d", fp, i))
				if filesRun(t, ep, fmt.Sprintf("%s:%d", fp, 1+rand.N(3)), i%2 == 1) {
					killed++
				}
				clean(t, ep)
				if t.Failed() {
					return
				}
			}
			t.Logf("%s: %d runs, %d killed at the failpoint", fp, runs, killed)
			if killed == 0 {
				t.Fatalf("failpoint %s never fired", fp)
			}
		})
	}
}

func filesRun(t *testing.T, ep storetest.Endpoint, failpoint string, reuse bool) (killed bool) {
	t.Helper()
	var dirs []string
	defer removeAll(&dirs)
	cache := runDir(t, &dirs)
	p := start(t, ep, cache, failpoint)
	want := map[string]bool{} // name -> attached (true) or detached (false), acknowledged
	var unknown *Req
	resp, alive := p.call(t, Req{Op: "create", Key: "k", ID: newID()})
	if !alive || !resp.OK {
		p.stop()
		return true // killed before the entity existed: nothing to check
	}
	inc := resp.Inc
	for i := range 8 {
		r := Req{Op: "attach", Key: "k", Inc: inc, ID: newID(), Input: fmt.Sprintf("f%d", i)}
		if i%3 == 2 { // detach an earlier file: its blob becomes the sweep's
			r.Op, r.Input = "detach", fmt.Sprintf("f%d", i-2)
		}
		resp, alive := p.call(t, r)
		if !alive || resp.Code == "outcome_unknown" {
			unknown = &r
			break
		}
		if !resp.OK {
			t.Fatalf("%s: %s %s: %s %s", failpoint, r.Op, r.Input, resp.Code, resp.Err)
		}
		want[r.Input] = r.Op == "attach"
		time.Sleep(20 * time.Millisecond) // let maintenance sweep
	}
	killed = p.stop()
	if !reuse {
		cache = runDir(t, &dirs)
	}
	p = start(t, ep, cache, "")
	defer p.stop()
	call := func(r Req) Resp {
		resp, alive := p.call(t, r)
		if !alive {
			t.Fatalf("%s: restarted node died on %+v", failpoint, r)
		}
		return resp
	}
	if resp := call(Req{Op: "takeover", Key: "k"}); !resp.OK {
		t.Fatalf("%s: takeover: %s %s", failpoint, resp.Code, resp.Err)
	}
	if unknown != nil { // the same ID and input: it commits once, or replays
		if resp := call(*unknown); !resp.OK {
			t.Fatalf("%s: retry of %s %s: %s %s", failpoint, unknown.Op, unknown.Input, resp.Code, resp.Err)
		}
		want[unknown.Input] = unknown.Op == "attach"
	}
	check := func(when string) {
		resp := call(Req{Op: "files", Key: "k", Inc: inc})
		if !resp.OK {
			t.Fatalf("%s: %s: a named file does not read back: %s %s", failpoint, when, resp.Code, resp.Err)
		}
		for name, attached := range want {
			if slices.Contains(resp.IDs, name) != attached {
				t.Fatalf("%s: %s: file %s present %v, want %v (files %v)", failpoint, when, name, !attached, attached, resp.IDs)
			}
		}
	}
	check("after the restart")
	for i := range 3 { // more file mutations: the new owner's sweeps run on the earlier residue
		r := Req{Op: "attach", Key: "k", Inc: inc, ID: newID(), Input: "g" + strconv.Itoa(i)}
		if i == 2 {
			r.Op, r.Input = "detach", "g0"
		}
		if resp := call(r); !resp.OK && !strings.Contains(resp.Err, "not_found") {
			t.Fatalf("%s: %s %s after the restart: %s %s", failpoint, r.Op, r.Input, resp.Code, resp.Err)
		}
		want[r.Input] = r.Op == "attach"
		time.Sleep(50 * time.Millisecond)
	}
	check("after the sweeps")
	return killed
}
