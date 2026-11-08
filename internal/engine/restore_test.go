package engine

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/OrdalieTech/big-garden/internal/head"
)

// TestRestoreRejects tampers with every input Restore and Compact trust and
// requires an ErrCorrupt failure that leaves no file behind.
func TestRestoreRejects(t *testing.T) {
	f := newFixture(t, 1000)
	f.mutate(t, "CREATE TABLE t(id INTEGER PRIMARY KEY, v BLOB)")
	f.mutate(t, "INSERT INTO t(v) SELECT randomblob(900) FROM (WITH RECURSIVE c(i) AS (SELECT 1 UNION ALL SELECT i+1 FROM c WHERE i < 50) SELECT i FROM c)")
	f.publish(t, true)
	f.mutate(t, "UPDATE t SET v = zeroblob(5) WHERE id % 4 = 0")
	f.publish(t, false)
	f.mutate(t, "DELETE FROM t WHERE id > 20") // shrinks (auto_vacuum)
	f.publish(t, false)
	f.restore(t)

	type tamper func(h *head.Head, objs [][]byte)
	resha := func(h *head.Head, objs [][]byte, i int) { // keep length/SHA consistent with tampered bytes
		ref := &h.Snapshot
		if i > 0 {
			ref = &h.Deltas[i-1]
		}
		ref.SHA256, ref.Bytes = sha256.Sum256(objs[i]), uint64(len(objs[i]))
	}
	cases := map[string]tamper{
		"snapshot byte":       func(h *head.Head, o [][]byte) { o[0][200] ^= 1 },
		"delta byte":          func(h *head.Head, o [][]byte) { o[1][150] ^= 1 },
		"delta byte, resha":   func(h *head.Head, o [][]byte) { o[1][150] ^= 1; resha(h, o, 1) },
		"trailer post, resha": func(h *head.Head, o [][]byte) { o[2][len(o[2])-10] ^= 1; resha(h, o, 2) },
		"truncated, resha":    func(h *head.Head, o [][]byte) { o[2] = o[2][:len(o[2])-1]; resha(h, o, 2) },
		"trailing, resha":     func(h *head.Head, o [][]byte) { o[2] = append(o[2], 0); resha(h, o, 2) },
		"frame size, resha":   func(h *head.Head, o [][]byte) { o[1][100+6] = 0xff; resha(h, o, 1) },
		"length":              func(h *head.Head, o [][]byte) { h.Deltas[0].Bytes++ },
		"missing delta":       func(h *head.Head, o [][]byte) { h.Deltas = h.Deltas[1:]; copy(o[1:], o[2:]) },
		"swapped deltas": func(h *head.Head, o [][]byte) {
			h.Deltas[0], h.Deltas[1] = h.Deltas[1], h.Deltas[0]
			o[1], o[2] = o[2], o[1]
		},
		"head checksum":     func(h *head.Head, o [][]byte) { h.DatabaseChecksum ^= 1 },
		"ref post checksum": func(h *head.Head, o [][]byte) { h.Deltas[1].PostChecksum ^= 1 },
		"ref pre checksum":  func(h *head.Head, o [][]byte) { h.Deltas[0].PreChecksum ^= 1 },
		"ref range":         func(h *head.Head, o [][]byte) { h.Deltas[1].MaxSequence++ },
		"head page count":   func(h *head.Head, o [][]byte) { h.PageCount++ },
		"head sequence":     func(h *head.Head, o [][]byte) { h.Sequence++ },
		"head schema":       func(h *head.Head, o [][]byte) { h.Schema++ },
		"head key":          func(h *head.Head, o [][]byte) { h.Key = "other" },
		"head incarnation":  func(h *head.Head, o [][]byte) { h.Incarnation[0] ^= 1 },
		"delta as snapshot": func(h *head.Head, o [][]byte) { h.Snapshot, o[0] = h.Deltas[0], o[1] },
	}
	for name, fn := range cases {
		h := f.head
		h.Deltas = append([]head.Ref{}, f.head.Deltas...)
		objs := make([][]byte, len(f.objs))
		for i, b := range f.objs {
			objs[i] = bytes.Clone(b)
		}
		fn(&h, objs)
		path := filepath.Join(t.TempDir(), "db")
		d, err := Restore(bg, plain, path, 1000, &h, readers(objs[:len(h.Deltas)+1]))
		if err == nil {
			d.Discard()
			t.Errorf("%s: restored", name)
			continue
		}
		if !errors.Is(err, ErrCorrupt) {
			t.Errorf("%s: %v is not ErrCorrupt", name, err)
		}
		if entries, _ := os.ReadDir(filepath.Dir(path)); len(entries) != 0 {
			t.Errorf("%s: left %d files", name, len(entries))
		}
	}

	// Compact verifies the chain itself: the ltx compactor does not check
	// that a pre-checksum equals the previous post-checksum.
	refs := append([]head.Ref{f.head.Snapshot}, f.head.Deltas...)
	for name, mod := range map[string]func([]head.Ref){
		"broken checksum chain": func(r []head.Ref) { r[2].PreChecksum = r[0].PostChecksum },
		"gap":                   func(r []head.Ref) { r[2].MinSequence++ },
		"wrong sha":             func(r []head.Ref) { r[1].SHA256[0] ^= 1 },
	} {
		r := append([]head.Ref{}, refs...)
		mod(r)
		if _, err := Compact(bg, io.Discard, r, readers(f.objs)); !errors.Is(err, ErrCorrupt) {
			t.Errorf("compact with %s: %v", name, err)
		}
	}
	var b bytes.Buffer
	c, err := Compact(bg, &b, refs[1:], readers(f.objs[1:]))
	if err != nil || c.Ref.MinSequence != refs[1].MinSequence || c.Ref.PostChecksum != refs[2].PostChecksum {
		t.Fatalf("compact deltas: %+v %v", c, err)
	}
	h := f.head
	h.Deltas = []head.Ref{c.Ref}
	d, err := Restore(bg, plain, filepath.Join(t.TempDir(), "db"), 1000, &h, readers([][]byte{f.objs[0], b.Bytes()}))
	if err != nil {
		t.Fatalf("restore from compacted delta: %v", err)
	}
	d.Discard()
}

// TestRestoreOpensWithoutWriting: opening a restored image commits nothing,
// so its WAL stays empty and a Snapshot reproduces the head's checksum (an
// Export after activation, M9).
func TestRestoreOpensWithoutWriting(t *testing.T) {
	f := newFixture(t, 1000)
	f.mutate(t, "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")
	f.publish(t, true)
	f.mutate(t, "INSERT INTO t(v) VALUES ('a')")
	f.publish(t, false)
	d := f.restore(t)
	if st, err := os.Stat(d.path + "-wal"); err != nil || st.Size() != 0 {
		t.Fatalf("WAL after restore: %v %v", st.Size(), err)
	}
	o, err := d.Snapshot(io.Discard)
	if err != nil || o.Ref.PostChecksum != f.head.DatabaseChecksum || o.Ref.MaxSequence != f.head.Sequence || o.PageCount != f.head.PageCount {
		t.Fatalf("snapshot after restore %+v %v, head %+v", o, err, f.head)
	}
}
