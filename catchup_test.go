package garden

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestReadStaleRespectsContextOnWriteLock verifies that ReadStale honors
// ctx.Done() instead of blocking uninterruptibly when a writer holds e.rw.Lock().
func TestReadStaleRespectsContextOnWriteLock(t *testing.T) {
	st := newStore(t)
	owner := testNode(t, st, "owner", nil)
	follower := testNode(t, st, "follower", nil)
	ref := mustCreate(t, owner, "k", newLedger())
	mustPut(t, owner, ref, "a", "1", newLedger())

	// Warm follower so entity is roReady.
	deadline := time.Now().Add(5 * time.Second)
	for {
		_, err := follower.ReadStale(bg, ref, 1, time.Hour, func(v *View) ([]byte, error) { return nil, nil })
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("follower never activated: %v", err)
		}
		time.Sleep(10 * time.Millisecond)
	}

	e, err := follower.entity("k")
	if err != nil {
		t.Fatal(err)
	}
	defer follower.drop(e)

	// Hold the write lock to simulate an in-place CatchUp or mutation.
	e.rw.Lock()

	tctx, cancel := context.WithTimeout(bg, 30*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, err = follower.ReadStale(tctx, ref, 1, time.Hour, func(v *View) ([]byte, error) { return nil, nil })
	elapsed := time.Since(start)
	if !errors.Is(err, context.DeadlineExceeded) {
		e.rw.Unlock()
		t.Fatalf("ReadStale with timeout under write lock = %v, want DeadlineExceeded", err)
	}
	wantCode(t, err, CodeUnavailable)
	if elapsed > time.Second {
		e.rw.Unlock()
		t.Fatalf("ReadStale blocked %v past 30ms deadline", elapsed)
	}

	cctx, ccancel := context.WithCancel(bg)
	ccancel()
	_, err = follower.ReadStale(cctx, ref, 1, time.Hour, func(v *View) ([]byte, error) { return nil, nil })
	if !errors.Is(err, context.Canceled) {
		e.rw.Unlock()
		t.Fatalf("ReadStale with canceled ctx under write lock = %v, want Canceled", err)
	}
	wantCode(t, err, CodeUnavailable)

	// Releasing the write lock immediately unblocks ReadStale.
	e.rw.Unlock()
	var got string
	if _, err := follower.ReadStale(bg, ref, 1, time.Hour, func(v *View) ([]byte, error) {
		return nil, v.QueryRow(bg, "SELECT v FROM kv WHERE k='a'", nil, &got)
	}); err != nil || got != "1" {
		t.Fatalf("ReadStale after unlock = %q, err=%v", got, err)
	}
}

// TestReadStaleStagedCatchUpAndTightFallback verifies that follower delta
// catch-up stages onto a copy outside e.rw.Lock() (swapping pointers sub-ms
// and cleaning up the old file) when CacheBytes headroom permits, and cleanly
// falls back to in-place CatchUp when CacheBytes headroom is tight.
func TestReadStaleStagedCatchUpAndTightFallback(t *testing.T) {
	st := newStore(t)
	owner := testNode(t, st, "owner", nil)
	follower := testNode(t, st, "follower", func(l *Limits) {
		l.EntityBytes = 4 << 20
		l.CacheBytes = 16 << 20
		l.Recoveries = 1
	})
	ref, _, err := owner.Create(bg, "staged", newReq(t, "c0", 1), func(tx *Tx) ([]byte, error) {
		if _, err := tx.Exec(bg, "CREATE TABLE kv(k TEXT PRIMARY KEY, v TEXT, gen INTEGER NOT NULL)"); err != nil {
			return nil, err
		}
		for i := range 200 {
			if _, err := tx.Exec(bg, "INSERT INTO kv VALUES(?, ?, 0)", fmt.Sprintf("k%04d", i), randText(512)); err != nil {
				return nil, err
			}
		}
		return nil, nil
	})
	if err != nil {
		t.Fatal(err)
	}

	readGen := func(ctx context.Context, maxAge time.Duration) (int, error) {
		var g int
		_, err := follower.ReadStale(ctx, ref, 1, maxAge, func(v *View) ([]byte, error) {
			return nil, v.Query(ctx, "SELECT MIN(gen) FROM kv WHERE k='k0000'", nil, func(r *sql.Rows) error {
				if r.Next() {
					return r.Scan(&g)
				}
				return r.Err()
			})
		})
		return g, err
	}

	deadline := time.Now().Add(5 * time.Second)
	for {
		g, err := readGen(bg, 200*time.Millisecond)
		if err == nil && g == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("initial activation failed: g=%d err=%v", g, err)
		}
		time.Sleep(10 * time.Millisecond)
	}

	e, err := follower.entity("staged")
	if err != nil {
		t.Fatal(err)
	}
	defer follower.drop(e)

	e.rw.RLock()
	pathBefore := e.db.Path()
	e.rw.RUnlock()

	// Publish delta updates while 8 concurrent readers hammer ReadStale.
	stop := make(chan struct{})
	var readCount atomic.Int64
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			for {
				select {
				case <-stop:
					return
				default:
				}
				if _, err := readGen(bg, 20*time.Millisecond); err != nil {
					t.Errorf("concurrent ReadStale during staged CatchUp: %v", err)
					return
				}
				readCount.Add(1)
			}
		})
	}

	for g := 1; g <= 4; g++ {
		if _, err := owner.Update(bg, ref, newReq(t, fmt.Sprintf("u%d", g), 1), func(tx *Tx) ([]byte, error) {
			_, err := tx.Exec(bg, "UPDATE kv SET gen=? WHERE k < 'k0020'", g)
			return nil, err
		}); err != nil {
			t.Fatal(err)
		}
		time.Sleep(20 * time.Millisecond)
	}

	deadline = time.Now().Add(5 * time.Second)
	for {
		g, err := readGen(bg, 20*time.Millisecond)
		if err != nil {
			t.Fatal(err)
		}
		if g == 4 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("staged catch-up stalled at gen=%d, want 4", g)
		}
		time.Sleep(10 * time.Millisecond)
	}
	close(stop)
	wg.Wait()

	if readCount.Load() == 0 {
		t.Fatal("expected concurrent reads during staged catch-up")
	}
	e.rw.RLock()
	pathAfterStaged := e.db.Path()
	e.rw.RUnlock()
	if pathAfterStaged == pathBefore {
		t.Fatalf("expected staged CatchUp to swap to a new file path, stayed at %s", pathBefore)
	}
	if _, err := os.Stat(pathBefore); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("old db file %s should be removed after staged swap, stat err=%v", pathBefore, err)
	}
	catchUpsAfterStaged := follower.Stats().CatchUps
	if catchUpsAfterStaged == 0 {
		t.Fatalf("expected CatchUps > 0 after staged catch-up")
	}

	// Tight CacheBytes headroom: cannot fit 2 copies, must cleanly fall back to in-place CatchUp.
	follower.mu.Lock()
	follower.cfg.Limits.CacheBytes = e.bytes.Load() + (1 << 19) // < db + activationBytes(cur)
	follower.mu.Unlock()

	if _, err := owner.Update(bg, ref, newReq(t, "u5", 1), func(tx *Tx) ([]byte, error) {
		_, err := tx.Exec(bg, "UPDATE kv SET gen=5 WHERE k < 'k0020'")
		return nil, err
	}); err != nil {
		t.Fatal(err)
	}

	deadline = time.Now().Add(5 * time.Second)
	for {
		g, err := readGen(bg, 10*time.Millisecond)
		if err != nil {
			t.Fatal(err)
		}
		if g == 5 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("in-place fallback catch-up stalled at gen=%d, want 5", g)
		}
		time.Sleep(10 * time.Millisecond)
	}
	e.rw.RLock()
	pathAfterFallback := e.db.Path()
	e.rw.RUnlock()
	if pathAfterFallback != pathAfterStaged {
		t.Fatalf("expected tight-headroom fallback to update in place (%s), got %s", pathAfterStaged, pathAfterFallback)
	}
	if follower.Stats().CatchUps <= catchUpsAfterStaged {
		t.Fatalf("expected CatchUps to increment on in-place fallback: before=%d after=%d", catchUpsAfterStaged, follower.Stats().CatchUps)
	}
}
