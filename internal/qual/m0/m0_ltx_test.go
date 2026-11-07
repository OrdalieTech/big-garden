package m0

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/anishathalye/porcupine"
	"github.com/superfly/ltx"
)

func checksum(t *testing.T, img []byte) ltx.Checksum {
	t.Helper()
	c, err := ltx.ChecksumReader(bytes.NewReader(img), pageSize)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

// encode writes an LTX v3 file holding the given pages of img.
func encode(t *testing.T, hdr ltx.Header, img []byte, pages []uint32, post ltx.Checksum) []byte {
	t.Helper()
	var b bytes.Buffer
	enc, err := ltx.NewEncoder(&b)
	if err != nil {
		t.Fatal(err)
	}
	hdr.Version, hdr.PageSize, hdr.Commit = ltx.Version, pageSize, uint32(len(img)/pageSize)
	if err := enc.EncodeHeader(hdr); err != nil {
		t.Fatal(err)
	}
	for _, p := range pages {
		if err := enc.EncodePage(ltx.PageHeader{Pgno: p}, img[(p-1)*pageSize:p*pageSize]); err != nil {
			t.Fatal(err)
		}
	}
	enc.SetPostApplyChecksum(post)
	if err := enc.Close(); err != nil {
		t.Fatal(err)
	}
	return b.Bytes()
}

func allPages(img []byte) []uint32 {
	var p []uint32
	for i := uint32(1); i <= uint32(len(img)/pageSize); i++ {
		p = append(p, i)
	}
	return p
}

// strictApply is the M0 prototype of the adapter's checks (PLAN §5.3): it
// rejects NoChecksum files, wrong page size, an unexpected sequence range or
// pre-checksum, any decoder error, and a post-checksum that does not match
// the image recomputed independently after applying the pages.
func strictApply(base, file []byte, min, max ltx.TXID, pre ltx.Checksum) ([]byte, error) {
	dec := ltx.NewDecoder(bytes.NewReader(file))
	if err := dec.DecodeHeader(); err != nil {
		return nil, err
	}
	h := dec.Header()
	switch {
	case h.NoChecksum():
		return nil, errors.New("NoChecksum LTX rejected")
	case h.PageSize != pageSize || h.Commit == 0:
		return nil, errors.New("bad page size or commit")
	case h.MinTXID != min || h.MaxTXID != max || h.PreApplyChecksum != pre:
		return nil, fmt.Errorf("unexpected range %d..%d or pre-checksum %s", h.MinTXID, h.MaxTXID, h.PreApplyChecksum)
	}
	img := make([]byte, int(h.Commit)*pageSize)
	copy(img, base)
	var ph ltx.PageHeader
	data := make([]byte, pageSize)
	for {
		err := dec.DecodePage(&ph, data)
		if err == io.EOF {
			break
		} else if err != nil {
			return nil, err
		}
		copy(img[(ph.Pgno-1)*pageSize:], data)
	}
	if err := dec.Close(); err != nil {
		return nil, err
	}
	c, err := ltx.ChecksumReader(bytes.NewReader(img), pageSize)
	if err != nil {
		return nil, err
	}
	if c != dec.Trailer().PostApplyChecksum {
		return nil, errors.New("post-apply checksum does not match the applied image")
	}
	return img, nil
}

// images returns a published SQLite image and its successor, made by SQLite
// and captured with D2 (pages read through sqlite_dbpage).
func images(t *testing.T) (p0, p1 []byte, changed []uint32) {
	path := filepath.Join(t.TempDir(), "db")
	_, c := open(t, path)
	setup(t, c)
	exec(t, c, "CREATE TABLE t(id INTEGER PRIMARY KEY, v BLOB)")
	exec(t, c, "INSERT INTO t(v) "+series(300, "randomblob(500)"))
	p0 = checkpoint(t, c, path)
	exec(t, c, "UPDATE t SET v = zeroblob(10) WHERE id % 5 = 0")
	exec(t, c, "INSERT INTO t(v) "+series(100, "randomblob(900)"))
	p1, changed = capture(t, c, path, p0)
	if oracle := checkpoint(t, c, path); !bytes.Equal(p1, oracle) {
		t.Fatal("capture differs from checkpoint")
	}
	return p0, p1, changed
}

func TestLTXRoundTrip(t *testing.T) {
	p0, p1, changed := images(t)
	c0, c1 := checksum(t, p0), checksum(t, p1)

	snap := encode(t, ltx.Header{MinTXID: 1, MaxTXID: 1}, p0, allPages(p0), c0)
	got, err := strictApply(nil, snap, 1, 1, 0)
	if err != nil || !bytes.Equal(got, p0) {
		t.Fatalf("snapshot round trip: %v", err)
	}
	var whole bytes.Buffer
	if err := ltx.NewDecoder(bytes.NewReader(snap)).DecodeDatabaseTo(&whole); err != nil || !bytes.Equal(whole.Bytes(), p0) {
		t.Fatalf("DecodeDatabaseTo: %v", err)
	}

	delta := encode(t, ltx.Header{MinTXID: 2, MaxTXID: 2, PreApplyChecksum: c0}, p1, changed, c1)
	if got, err := strictApply(p0, delta, 2, 2, c0); err != nil || !bytes.Equal(got, p1) {
		t.Fatalf("delta apply: %v", err)
	}
	t.Logf("snapshot %d pages -> %d bytes; delta %d pages -> %d bytes", len(p0)/pageSize, len(snap), len(changed), len(delta))

	// Compaction (AllowNonContiguousTXIDs=false, checksums kept) folds the
	// delta into a new snapshot equal to p1.
	var out bytes.Buffer
	cmp, err := ltx.NewCompactor(&out, []io.Reader{bytes.NewReader(snap), bytes.NewReader(delta)})
	if err != nil {
		t.Fatal(err)
	}
	if err := cmp.Compact(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got, err := strictApply(nil, out.Bytes(), 1, 2, 0); err != nil || !bytes.Equal(got, p1) {
		t.Fatalf("compacted snapshot: %v", err)
	}
	if cmp.Trailer().PostApplyChecksum != c1 {
		t.Fatal("compactor post checksum differs")
	}
}

func TestLTXRejects(t *testing.T) {
	p0, p1, changed := images(t)
	c0, c1 := checksum(t, p0), checksum(t, p1)
	snap := encode(t, ltx.Header{MinTXID: 1, MaxTXID: 1}, p0, allPages(p0), c0)
	delta := encode(t, ltx.Header{MinTXID: 2, MaxTXID: 2, PreApplyChecksum: c0}, p1, changed, c1)

	// The codec itself accepts NoChecksum files; only the adapter's flag
	// check keeps them out (PLAN §5.3).
	nock := encode(t, ltx.Header{MinTXID: 2, MaxTXID: 2, Flags: ltx.HeaderFlagNoChecksum}, p1, changed, 0)
	if err := ltx.NewDecoder(bytes.NewReader(nock)).Verify(); err != nil {
		t.Fatalf("ltx unexpectedly rejects NoChecksum itself: %v", err)
	}
	if _, err := strictApply(p0, nock, 2, 2, 0); err == nil {
		t.Fatal("NoChecksum accepted")
	}

	flip := func(b []byte, off int) []byte { b = bytes.Clone(b); b[off] ^= 0x40; return b }
	bad := map[string][]byte{
		"page byte flipped":   flip(delta, ltx.HeaderSize+ltx.PageHeaderSize+4+20),
		"header byte flipped": flip(delta, 40),
		"index byte flipped":  flip(delta, len(delta)-ltx.TrailerSize-10),
		"trailer flipped":     flip(delta, len(delta)-3),
		"trailing byte":       append(bytes.Clone(delta), 0),
		"truncated":           delta[:len(delta)-1],
		"wrong post checksum": encode(t, ltx.Header{MinTXID: 2, MaxTXID: 2, PreApplyChecksum: c0}, p1, changed, c0),
		"missing page":        encode(t, ltx.Header{MinTXID: 2, MaxTXID: 2, PreApplyChecksum: c0}, p1, changed[1:], c1),
	}
	for name, b := range bad {
		if _, err := strictApply(p0, b, 2, 2, c0); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
	if _, err := strictApply(p0, delta, 3, 3, c0); err == nil {
		t.Error("unexpected sequence range accepted")
	}
	if _, err := strictApply(p0, delta, 2, 2, c1); err == nil {
		t.Error("wrong pre-checksum accepted")
	}
	if _, err := strictApply(nil, encode(t, ltx.Header{MinTXID: 1, MaxTXID: 1}, p0, allPages(p0), c1), 1, 1, 0); err == nil {
		t.Error("snapshot with wrong post checksum accepted")
	}

	// The compactor refuses a sequence gap, but it does NOT check that each
	// input's pre-checksum equals its predecessor's post-checksum: the
	// adapter must verify the chain itself (PLAN §9.1).
	gap := encode(t, ltx.Header{MinTXID: 3, MaxTXID: 3, PreApplyChecksum: c0}, p1, changed, c1)
	cmp, _ := ltx.NewCompactor(io.Discard, []io.Reader{bytes.NewReader(snap), bytes.NewReader(gap)})
	if err := cmp.Compact(context.Background()); err == nil {
		t.Error("compactor accepted a sequence gap")
	}
	broken := encode(t, ltx.Header{MinTXID: 2, MaxTXID: 2, PreApplyChecksum: c1}, p1, changed, c1)
	cmp, _ = ltx.NewCompactor(io.Discard, []io.Reader{bytes.NewReader(snap), bytes.NewReader(broken)})
	if err := cmp.Compact(context.Background()); err != nil {
		t.Fatalf("compactor now checks checksum chains (update PROTOCOL.md): %v", err)
	}
	t.Log("compactor accepts a broken pre/post checksum chain: the adapter must check it")

	// A snapshot header with a pre-checksum is invalid at the codec level.
	enc, _ := ltx.NewEncoder(io.Discard)
	if err := enc.EncodeHeader(ltx.Header{Version: ltx.Version, PageSize: pageSize, Commit: 1, MinTXID: 1, MaxTXID: 1, PreApplyChecksum: c0}); err == nil {
		t.Error("snapshot with pre-checksum encoded")
	}
}

// TestLTXDeclaredSizeAllocation records a decoder hazard: v0.5.3 allocates a
// page's declared compressed size before reading it. Untrusted LTX bytes
// (anything not already matched to a head's SHA-256 and length, e.g. an
// Import archive) must have their frame sizes bounded before decoding.
func TestLTXDeclaredSizeAllocation(t *testing.T) {
	p0, _, _ := images(t)
	snap := encode(t, ltx.Header{MinTXID: 1, MaxTXID: 1}, p0, allPages(p0), checksum(t, p0))
	bomb := bytes.Clone(snap)
	binary.BigEndian.PutUint32(bomb[ltx.HeaderSize+ltx.PageHeaderSize:], 64<<20)
	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	err := ltx.NewDecoder(bytes.NewReader(bomb)).Verify()
	runtime.ReadMemStats(&after)
	if err == nil {
		t.Fatal("oversized frame accepted")
	}
	t.Logf("declared 64 MiB frame: %v; allocated %d MiB while failing", err, (after.TotalAlloc-before.TotalAlloc)>>20)
}

// TestPorcupineOracle pins the test-only linearizability checker and shows
// it is not vacuous: a stale read after a completed write is caught.
func TestPorcupineOracle(t *testing.T) {
	type op struct {
		write bool
		v     int
	}
	model := porcupine.Model{
		Init: func() any { return 0 },
		Step: func(s, in, out any) (bool, any) {
			o := in.(op)
			if o.write {
				return true, o.v
			}
			return out.(int) == s.(int), s
		},
	}
	ok := []porcupine.Operation{
		{ClientId: 0, Input: op{true, 1}, Call: 0, Output: 0, Return: 10},
		{ClientId: 1, Input: op{false, 0}, Call: 5, Output: 0, Return: 15}, // concurrent: may see 0
		{ClientId: 1, Input: op{false, 0}, Call: 20, Output: 1, Return: 30},
	}
	if !porcupine.CheckOperations(model, ok) {
		t.Fatal("linearizable history rejected")
	}
	stale := append(ok[:2:2], porcupine.Operation{ClientId: 1, Input: op{false, 0}, Call: 20, Output: 0, Return: 30})
	if porcupine.CheckOperations(model, stale) {
		t.Fatal("stale read after a completed write accepted")
	}
}
