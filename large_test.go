package garden

import (
	"bufio"
	"bytes"
	"database/sql"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/OrdalieTech/big-garden/internal/storetest"
)

// TestEntityAcrossLockPage grows an entity past 1 GiB, where SQLite's lock
// page (page 262,145 at 4 KiB) is never written, shrinks it back below, and
// checks every path that sees the whole image: capture, the snapshot of
// maintenance, a cold activation on another Node, export and import. The
// rows are zero blobs, so the objects stay small; the database files do not
// (several GiB under TMPDIR). Peak RSS stays far below the image (D34).
// GARDEN_TEST_LARGE=1 runs it.
func TestEntityAcrossLockPage(t *testing.T) {
	if os.Getenv("GARDEN_TEST_LARGE") == "" {
		t.Skip("GARDEN_TEST_LARGE is not set")
	}
	mem := storetest.NewMem()
	big := func(l *Limits) {
		l.EntityBytes, l.CacheBytes, l.Recoveries = 2<<30, 16<<30, 1 // room for a resident image and a rebuild
		l.CommitTimeout, l.StorageAttemptTimeout = 10*time.Minute, time.Minute
	}
	a, b := testNode(t, mem, "a", big), testNode(t, mem, "b", big)
	ref, _, err := a.Create(bg, "big", newReq(t, "create", 1), func(tx *Tx) ([]byte, error) {
		_, err := tx.Exec(bg, "CREATE TABLE t(id INTEGER PRIMARY KEY, v BLOB NOT NULL)")
		return nil, err
	})
	if err != nil {
		t.Fatal(err)
	}
	fill := func(input string, from, to int) {
		t.Helper()
		if _, err := a.Update(bg, ref, newReq(t, input, 1), func(tx *Tx) ([]byte, error) {
			_, err := tx.Exec(bg, `WITH RECURSIVE n(i) AS (SELECT ? UNION ALL SELECT i + 1 FROM n WHERE i < ?)
				INSERT INTO t SELECT i, zeroblob(1 << 19) FROM n`, from, to)
			return nil, err
		}); err != nil {
			t.Fatalf("%s: %v", input, err)
		}
	}
	count := func(n *Node, r Ref) (rows, bytes int64) {
		t.Helper()
		if _, err := n.Read(bg, r, 1, func(v *View) ([]byte, error) {
			return nil, v.Query(bg, "SELECT count(*), coalesce(sum(length(v)), 0) FROM t", nil, func(rs *sql.Rows) error {
				rs.Next()
				return rs.Scan(&rows, &bytes)
			})
		}); err != nil {
			t.Fatal(err)
		}
		return rows, bytes
	}
	fill("below", 1, 2000) // about 1,004 MiB: below the lock page
	fill("across", 2001, 2200)
	if _, err := a.Update(bg, ref, newReq(t, "rewrite", 1), func(tx *Tx) ([]byte, error) {
		_, err := tx.Exec(bg, "UPDATE t SET v = zeroblob(1 << 18) WHERE id <= 300") // replay above the image: a snapshot is due
		return nil, err
	}); err != nil {
		t.Fatalf("rewrite: %v", err)
	}
	a.wg.Wait() // maintenance: an off-mutex snapshot from the image copy, across the lock page (D35)
	if h, _ := headAt(t, mem, "big"); h.Pending != nil || h.Snapshot.MaxSequence < 3 {
		t.Fatalf("maintenance left pending %v, snapshot to %d", h.Pending, h.Snapshot.MaxSequence)
	}
	if _, err := a.Snapshot(bg, ref); err != nil { // the whole image as one object
		t.Fatalf("snapshot above 1 GiB: %v", err)
	}
	if rows, n := count(b, ref); rows != 2200 || n != 1900<<19+300<<18 { // cold activation of the snapshot
		t.Fatalf("b reads %d rows, %d bytes", rows, n)
	}
	if _, err := a.Update(bg, ref, newReq(t, "shrink", 1), func(tx *Tx) ([]byte, error) {
		_, err := tx.Exec(bg, "DELETE FROM t WHERE id > 1800") // auto_vacuum truncates below the lock page
		return nil, err
	}); err != nil {
		t.Fatalf("shrink below 1 GiB: %v", err)
	}
	if rows, _ := count(b, ref); rows != 1800 {
		t.Fatalf("b reads %d rows after the shrink", rows)
	}
	fill("again", 1801, 2200) // across once more, as a delta on a follower's image
	if rows, _ := count(b, ref); rows != 2200 {
		t.Fatalf("b reads %d rows after growing again", rows)
	}
	var archive bytes.Buffer
	if _, err := b.Export(bg, ref, &archive); err != nil {
		t.Fatalf("export: %v", err)
	}
	dst, _, err := b.Import(bg, "copy", newReq(t, "import", 1), bytes.NewReader(archive.Bytes()))
	if err != nil {
		t.Fatalf("import: %v", err)
	}
	if rows, n := count(a, dst); rows != 2200 || n != 1900<<19+300<<18 {
		t.Fatalf("the import reads %d rows, %d bytes", rows, n)
	}
	t.Logf("peak RSS %s for a 1.1 GiB entity; archive %d bytes", peakRSS(), archive.Len())
}

func peakRSS() string {
	f, err := os.Open("/proc/self/status")
	if err != nil {
		return "?"
	}
	defer f.Close()
	for s := bufio.NewScanner(f); s.Scan(); {
		if v, ok := strings.CutPrefix(s.Text(), "VmHWM:"); ok {
			return strings.TrimSpace(v)
		}
	}
	return "?"
}
