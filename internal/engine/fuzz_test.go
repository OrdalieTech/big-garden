package engine

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"

	"github.com/OrdalieTech/big-garden/internal/head"
	"github.com/superfly/ltx"
)

// fuzzRef derives the reference a head would carry for b: its length and
// SHA-256 (so the fuzzer reaches the decoder), and the sequence range and
// checksums its own header and trailer claim.
func fuzzRef(b []byte) head.Ref {
	ref := head.Ref{Bytes: uint64(len(b)), SHA256: sha256.Sum256(b)}
	if len(b) >= ltx.HeaderSize+ltx.TrailerSize {
		var h ltx.Header
		h.UnmarshalBinary(b)
		ref.MinSequence, ref.MaxSequence = uint64(h.MinTXID), uint64(h.MaxTXID)
		ref.PreChecksum = head.Checksum(h.PreApplyChecksum)
		ref.PostChecksum = head.Checksum(binary.BigEndian.Uint64(b[len(b)-ltx.TrailerSize:]))
	}
	return ref
}

// FuzzLTX feeds arbitrary bytes to the restore path, as a snapshot onto an
// empty file and as a delta onto a real image, and to the compactor. Nothing
// may panic or allocate by declared sizes; whatever is accepted must also be
// accepted by ltx's own decoder, must leave a file whose full checksum and
// size equal the claimed image, and must compact to a snapshot that restores
// to the same bytes.
func FuzzLTX(f *testing.F) {
	fx := newFixture(f, 1000)
	fx.mutate(f, "CREATE TABLE t(id INTEGER PRIMARY KEY, v BLOB)")
	fx.mutate(f, "INSERT INTO t(v) SELECT randomblob(700) FROM (WITH RECURSIVE c(i) AS (SELECT 1 UNION ALL SELECT i+1 FROM c WHERE i < 40) SELECT i FROM c)")
	fx.publish(f, true)
	fx.mutate(f, "UPDATE t SET v = zeroblob(10) WHERE id % 3 = 0")
	fx.mutate(f, "DELETE FROM t WHERE id > 30")
	fx.publish(f, false)
	snap, base := fx.objs[0], fx.head.Snapshot
	tf, err := os.Create(filepath.Join(f.TempDir(), "base"))
	if err != nil {
		f.Fatal(err)
	}
	defer tf.Close()
	baseImg, _, err := apply(bg, tf, Image{}, base, bytes.NewReader(snap))
	if err != nil {
		f.Fatal(err)
	}
	baseFile, err := os.ReadFile(tf.Name())
	if err != nil {
		f.Fatal(err)
	}
	f.Add(snap, false)
	f.Add(fx.objs[1], true)
	f.Add(fx.objs[1][:len(fx.objs[1])/2], true)
	f.Fuzz(func(t *testing.T, b []byte, delta bool) {
		path := filepath.Join(t.TempDir(), "img")
		file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o600)
		if err != nil {
			t.Fatal(err)
		}
		defer file.Close()
		img, ref := Image{}, fuzzRef(b)
		if delta {
			img = baseImg
			if _, err := file.WriteAt(baseFile, 0); err != nil {
				t.Fatal(err)
			}
		}
		got, _, err := apply(bg, file, img, ref, bytes.NewReader(b))
		if delta {
			var out bytes.Buffer
			c, cerr := Compact(bg, &out, []head.Ref{base, ref}, readers([][]byte{snap, b}))
			if (cerr == nil) != (err == nil) {
				t.Fatalf("apply: %v; compact: %v", err, cerr)
			}
			if cerr == nil {
				p2 := filepath.Join(t.TempDir(), "compacted")
				f2, _ := os.OpenFile(p2, os.O_RDWR|os.O_CREATE, 0o600)
				defer f2.Close()
				got2, _, err := apply(bg, f2, Image{}, c.Ref, bytes.NewReader(out.Bytes()))
				a, _ := os.ReadFile(path)
				b2, _ := os.ReadFile(p2)
				if err != nil || got2 != got || !bytes.Equal(a, b2) || c.Pages != got.PageCount {
					t.Fatalf("compacted snapshot restores to %+v (%v), delta to %+v", got2, err, got)
				}
			}
		}
		if err != nil {
			return
		}
		if err := ltx.NewDecoder(bytes.NewReader(b)).Verify(); err != nil {
			t.Fatalf("accepted bytes that ltx rejects: %v", err)
		}
		full, err := fileImage(file, got.Sequence)
		if err != nil || full != got {
			t.Fatalf("file image %+v (%v), claimed %+v", full, err, got)
		}
	})
}
