package storetest

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/OrdalieTech/big-garden/internal/store"
)

func want(t *testing.T, err error, o store.Outcome) {
	t.Helper()
	if got := store.OutcomeOf(err); got != o {
		t.Fatalf("outcome %s, want %s (%v)", got, o, err)
	}
}

func TestMemSemantics(t *testing.T) {
	ctx := context.Background()
	m := NewMem()
	want(t, m.PutImmutable(ctx, "a", []byte("1")), store.OK)
	want(t, m.PutImmutable(ctx, "a", []byte("1")), store.PreconditionFailed)
	_, err := m.Get(ctx, "missing", "", 10)
	want(t, err, store.NotFound)

	e1, err := m.PutCAS(ctx, "h", []byte("v1"), "")
	want(t, err, store.OK)
	_, err = m.PutCAS(ctx, "h", []byte("v1"), "")
	want(t, err, store.PreconditionFailed)
	e2, err := m.PutCAS(ctx, "h", []byte("v1"), e1) // same bytes, new token
	want(t, err, store.OK)
	if e1 == e2 || e1[0] != '"' {
		t.Fatalf("etags %s %s not opaque-unique", e1, e2)
	}
	_, err = m.PutCAS(ctx, "h", []byte("v2"), e1)
	want(t, err, store.PreconditionFailed) // stale
	_, err = m.PutCAS(ctx, "absent", []byte("v"), e2)
	want(t, err, store.PreconditionFailed)
	_, err = m.Get(ctx, "h", e2, 10)
	want(t, err, store.NotModified)
	o, err := m.Get(ctx, "h", e1, 10)
	want(t, err, store.OK)
	if string(o.Body) != "v1" || o.ETag != e2 {
		t.Fatalf("get %q %s", o.Body, o.ETag)
	}
	_, err = m.Get(ctx, "h", "", 1)
	want(t, err, store.Failed) // above max, never truncated

	for i := range 5 {
		want(t, m.PutImmutable(ctx, fmt.Sprintf("p/%d", i), []byte("x")), store.OK)
	}
	page, more, err := m.List(ctx, "p/", "p/1", 2)
	want(t, err, store.OK)
	if len(page) != 2 || page[0].Key != "p/2" || page[1].Key != "p/3" || !more {
		t.Fatalf("list %v %v", page, more)
	}
	page, more, _ = m.List(ctx, "p/", "p/3", 2)
	if len(page) != 1 || more {
		t.Fatalf("last page %v %v", page, more)
	}
	want(t, m.Delete(ctx, "p/0"), store.OK)
	want(t, m.Delete(ctx, "p/0"), store.OK)
	if _, ok := m.Peek("p/0"); ok {
		t.Fatal("deleted object still present")
	}
}

func TestMemFaults(t *testing.T) {
	ctx := context.Background()
	m := NewMem()
	hold := make(chan struct{})
	m.SetPlan(func(a Attempt) MemFault {
		switch a.Seq {
		case 0:
			return MemFault{Reject: store.Unknown} // 5xx, not applied
		case 1:
			return MemFault{Reject: store.Failed} // 403
		case 2:
			return MemFault{Reject: store.PreconditionFailed} // injected 412
		case 3:
			return MemFault{LoseReply: true} // applied, reply lost
		case 4:
			return MemFault{Hold: hold} // applied after the caller gave up
		case 6:
			return MemFault{Body: func(b []byte) []byte { return b[:len(b)/2] }}
		}
		return MemFault{}
	})
	for _, o := range []store.Outcome{store.Unknown, store.Failed, store.PreconditionFailed} {
		_, err := m.PutCAS(ctx, "h", []byte("x"), "")
		want(t, err, o)
		if _, ok := m.Peek("h"); ok {
			t.Fatal("rejected attempt applied")
		}
	}
	_, err := m.PutCAS(ctx, "h", []byte("v1"), "")
	want(t, err, store.Unknown)
	cur, ok := m.Peek("h")
	if !ok || string(cur.Body) != "v1" {
		t.Fatal("lost-reply attempt did not apply")
	}

	cctx, cancel := context.WithTimeout(ctx, 20*time.Millisecond)
	_, err = m.PutCAS(cctx, "h", []byte("v2"), cur.ETag)
	cancel()
	want(t, err, store.Unknown)
	if o, _ := m.Peek("h"); string(o.Body) != "v1" {
		t.Fatal("held attempt applied before release")
	}
	close(hold)
	deadline := time.Now().Add(5 * time.Second)
	for {
		if o, _ := m.Peek("h"); string(o.Body) == "v2" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("held attempt never applied after release")
		}
		time.Sleep(time.Millisecond)
	}
	_, err = m.PutCAS(ctx, "h", []byte("v3"), cur.ETag) // seq 5: stale after the late apply
	want(t, err, store.PreconditionFailed)
	o, err := m.Get(ctx, "h", "", 10) // seq 6: truncated body as OK
	want(t, err, store.OK)
	if string(o.Body) != "v" {
		t.Fatalf("body rewrite not applied: %q", o.Body)
	}

	j := m.Journal()
	if len(j) != 7 {
		t.Fatalf("journal has %d attempts", len(j))
	}
	for i, wantApplied := range []bool{false, false, false, true, true, true, true} {
		if j[i].Applied != wantApplied || !j[i].Returned {
			t.Fatalf("attempt %d: applied %v returned %v", i, j[i].Applied, j[i].Returned)
		}
	}
	if j[4].Outcome != store.Unknown || j[4].Result != store.OK || j[4].ETag == "" {
		t.Fatalf("late apply journaled as %+v", j[4])
	}
}

func TestMemCanceledBeforeSend(t *testing.T) {
	m := NewMem()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := m.PutCAS(ctx, "h", []byte("x"), "")
	want(t, err, store.Unknown)
	if _, ok := m.Peek("h"); ok || m.Journal()[0].Applied {
		t.Fatal("a call with a canceled context applied")
	}
}

func TestMemCASRace(t *testing.T) {
	ctx := context.Background()
	m := NewMem()
	etag := ""
	for round := range 200 {
		var wg sync.WaitGroup
		etags := make([]string, 32)
		errs := make([]error, 32)
		for i := range 32 {
			wg.Go(func() { etags[i], errs[i] = m.PutCAS(ctx, "h", []byte{byte(round), byte(i)}, etag) })
		}
		wg.Wait()
		winner := -1
		for i, err := range errs {
			switch store.OutcomeOf(err) {
			case store.OK:
				if winner >= 0 {
					t.Fatalf("round %d: two winners", round)
				}
				winner = i
			case store.PreconditionFailed:
			default:
				t.Fatalf("round %d: %v", round, err)
			}
		}
		if winner < 0 {
			t.Fatalf("round %d: no winner", round)
		}
		o, _ := m.Peek("h")
		if !bytes.Equal(o.Body, []byte{byte(round), byte(winner)}) || o.ETag != etags[winner] {
			t.Fatalf("round %d: stored state is not the winner's", round)
		}
		etag = etags[winner]
	}
}

// TestUnchunk: the proxy journal records the payload of aws-chunked PUTs,
// signed or unsigned, with or without trailers.
func TestUnchunk(t *testing.T) {
	for _, c := range []struct{ in, want string }{
		{"3\r\nabc\r\n0\r\nx-amz-checksum-crc32:AAAAAA==\r\n\r\n", "abc"},
		{"2;chunk-signature=ff\r\nab\r\n1;chunk-signature=ee\r\nc\r\n0;chunk-signature=dd\r\n\r\n", "abc"},
		{"a\r\n0123456789\r\n0\r\n\r\n", "0123456789"},
	} {
		if got := string(unchunk([]byte(c.in))); got != c.want {
			t.Errorf("unchunk(%q) = %q, want %q", c.in, got, c.want)
		}
	}
}
