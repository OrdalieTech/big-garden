package engine

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"syscall"

	"github.com/OrdalieTech/big-garden/internal/head"
	"github.com/superfly/ltx"
	"modernc.org/sqlite"
)

// maxFrame bounds a page frame's declared compressed size: lz4's
// CompressBlockBound(4096). ltx v0.5.3 allocates the declared size before
// reading it (M0), so no frame is decoded before this bound is checked.
const maxFrame = pageSize + pageSize/255 + 16

// Restore reconstructs the image named by h into a new file at path, which
// must not exist, then opens and checks it (PROTOCOL §4 steps 4–7). objs
// holds the snapshot followed by every delta of h, in order. With from set
// (D21), path is an existing file believed to hold the image from, and objs
// holds only the last len(objs) links of h: the first one overlays from (its
// range contains from.Sequence) and the gates below are unchanged, so a file
// that is not what from says fails them. Each object's
// length, SHA-256, frame sizes, header, sequence range and checksums are
// verified against h before and while it is applied; the final page count
// and checksum must equal h's, the metadata row must match h and
// integrity_check must pass. Every validation failure wraps ErrCorrupt. pages
// holds the page frames of each restored object (snapshot, deltas, tail):
// the replay pages of PROTOCOL §6 step 7.
func Restore(ctx context.Context, drv *Driver, path string, maxPages uint32, h *head.Head, objs []io.ReaderAt, from *Image) (
	d *DB, pages []uint32, err error) {
	refs := append([]head.Ref{h.Snapshot}, h.Deltas...)
	if h.Tail != nil {
		refs = append(refs, h.Tail.Ref())
	}
	flag, img := os.O_CREATE|os.O_EXCL, Image{}
	if from != nil {
		flag, img, refs = 0, *from, refs[len(refs)-min(len(objs), len(refs)):]
	}
	if len(objs) != len(refs) || h.PageCount > maxPages {
		return nil, nil, errors.New("engine: objects do not match the head or exceed the page limit")
	}
	f, err := os.OpenFile(path, os.O_RDWR|flag|syscall.O_NOFOLLOW, 0o600)
	if err != nil {
		return nil, nil, err
	}
	pages = make([]uint32, len(refs))
	for i, ref := range refs {
		if img, pages[i], err = apply(ctx, f, img, ref, objs[i], i == 0 && from != nil); err != nil {
			break
		}
	}
	var full Image
	if err == nil {
		full, err = fileImage(f, img.Sequence)
	}
	if err == nil && (img != Image{h.Sequence, h.PageCount, h.DatabaseChecksum} || full != img) {
		err = fmt.Errorf("%w: restored image %+v (file %+v) differs from the head", ErrCorrupt, img, full)
	}
	if err = errors.Join(err, f.Close()); err != nil {
		return nil, nil, errors.Join(err, removeFiles(path))
	}
	if d, err = open(ctx, drv, path, maxPages); err != nil {
		return nil, nil, errors.Join(err, removeFiles(path))
	}
	d.pub = img
	// integrity_check connects every virtual table whose module is
	// registered: it runs on a second connection, from a driver without the
	// Node's extensions, so activation never runs caller code.
	c := &DB{db: sql.OpenDB(connector{&sqlite.Driver{}, fileDSN(path, "ro")})}
	if c.conn, err = c.db.Conn(ctx); err == nil {
		err = c.check(h)
	}
	if err = errors.Join(err, c.close()); err != nil {
		return nil, nil, errors.Join(err, d.Discard())
	}
	return d, pages, nil
}

// Rebase rewrites the identity in the metadata row of an imported image
// (PROTOCOL §11 Import step 3); the caller's next Mutate then commits the
// import receipt at the following sequence.
func (d *DB) Rebase(ns head.ID, key string, inc head.ID) error {
	return d.inTx(func() error {
		return d.exec("UPDATE _state_meta SET namespace = ?, key = ?, incarnation = ?", ns[:], key, inc[:])
	})
}

// check verifies the metadata row against h and the schema's use of
// reserved names, and runs integrity_check.
func (d *DB) check(h *head.Head) error {
	m, err := d.Meta()
	if err != nil {
		return err
	}
	if m.Namespace != h.Namespace || m.Key != h.Key || m.Incarnation != h.Incarnation || m.Schema != h.Schema || m.Sequence != h.Sequence {
		return fmt.Errorf("%w: _state_meta does not match the head", ErrCorrupt)
	}
	if err := d.schemaOwned(); err != nil { // an image the library never publishes (an import)
		return fmt.Errorf("%w: %v", ErrCorrupt, err)
	}
	for i, name := range []string{"_state_meta", "_state_receipts"} { // another layout of the library's tables
		var sql string
		if err := d.conn.QueryRowContext(context.Background(), "SELECT sql FROM sqlite_schema WHERE name = ?", name).Scan(&sql); err != nil || sql != reservedSQL[i] {
			return fmt.Errorf("%w: %s is not the format 1 table", ErrCorrupt, name)
		}
	}
	var res string
	if err := d.conn.QueryRowContext(context.Background(), "PRAGMA integrity_check(1)").Scan(&res); err != nil {
		return err
	}
	if res != "ok" {
		return fmt.Errorf("%w: integrity_check: %.200s", ErrCorrupt, res)
	}
	return nil
}

// apply applies one verified object to the image in f: a snapshot onto an
// empty file, a delta onto img. It updates the rolling checksum from the old
// contents of the pages it replaces or truncates and returns the new image
// and the number of page frames the object carried. An overlay delta [a..b]
// applies onto an image at any s in [a-1, b-1]: it carries the final content
// of every page written in [a..b] and every page it grows by, so the result
// is the image at b (D21).
func apply(ctx context.Context, f *os.File, img Image, ref head.Ref, r io.ReaderAt, overlay bool) (Image, uint32, error) {
	dec, err := openObject(r, ref)
	if err != nil {
		return img, 0, err
	}
	snapshot, hd := ref.MinSequence == 1, dec.Header()
	follows := ref.MinSequence == img.Sequence+1 && ref.PreChecksum == img.Checksum
	if overlay {
		follows = ref.MinSequence <= img.Sequence+1 && img.Sequence < ref.MaxSequence
	}
	if snapshot != (img == Image{}) || (!snapshot && !follows) {
		return img, 0, fmt.Errorf("%w: object %d..%d does not follow image at %d", ErrCorrupt, ref.MinSequence, ref.MaxSequence, img.Sequence)
	}
	sum, count := ltx.Checksum(img.Checksum), img.PageCount
	var ph ltx.PageHeader
	data, old := make([]byte, pageSize), make([]byte, pageSize)
	prev, grown, carried := uint32(0), uint32(0), uint32(0)
	for {
		err := dec.DecodePage(&ph, data)
		if err == io.EOF {
			break
		}
		if err != nil { // the bytes matched their SHA-256: what fails to decode is corrupt
			err = fmt.Errorf("%w: %v", ErrCorrupt, err)
		} else {
			err = ctx.Err()
		}
		if err == nil && (ph.Pgno <= prev || ph.Pgno > hd.Commit || (snapshot && ph.Pgno != prev+1)) {
			err = fmt.Errorf("%w: page %d after %d (commit %d)", ErrCorrupt, ph.Pgno, prev, hd.Commit)
		}
		if err == nil && ph.Pgno <= count {
			err = xorPage(&sum, f, ph.Pgno, old)
		}
		if err == nil {
			_, err = f.WriteAt(data, int64(ph.Pgno-1)*pageSize)
		}
		if err != nil {
			return img, 0, err
		}
		sum ^= ltx.ChecksumPage(ph.Pgno, data)
		prev, carried = ph.Pgno, carried+1
		if ph.Pgno > count {
			grown++
		}
	}
	if hd.Commit > count && grown != hd.Commit-count {
		return img, 0, fmt.Errorf("%w: object grows to %d pages but carries %d of %d new pages", ErrCorrupt, hd.Commit, grown, hd.Commit-count)
	}
	for p := hd.Commit + 1; p <= count; p++ {
		if err := xorPage(&sum, f, p, old); err != nil {
			return img, 0, err
		}
	}
	if err := f.Truncate(int64(hd.Commit) * pageSize); err != nil {
		return img, 0, err
	}
	if err := dec.Close(); err != nil {
		return img, 0, fmt.Errorf("%w: %v", ErrCorrupt, err)
	}
	post := head.Checksum(ltx.ChecksumFlag | sum)
	if t := dec.Trailer(); head.Checksum(t.PostApplyChecksum) != post || post != ref.PostChecksum {
		return img, 0, fmt.Errorf("%w: post-apply checksum %s, computed %s, head %s", ErrCorrupt, t.PostApplyChecksum, ltx.Checksum(post), ltx.Checksum(ref.PostChecksum))
	}
	return Image{ref.MaxSequence, hd.Commit, post}, carried, nil
}

// openObject verifies r against ref (checkObject), then decodes its LTX
// header and checks it against ref. Callers only stream the pages, so the
// decoder does not retain the page index (Close still validates it).
func openObject(r io.ReaderAt, ref head.Ref) (*ltx.Decoder, error) {
	if err := checkObject(r, ref); err != nil {
		return nil, err
	}
	dec := ltx.NewDecoder(io.NewSectionReader(r, 0, int64(ref.Bytes)))
	dec.SetRetainPageIndex(false)
	if err := dec.DecodeHeader(); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrCorrupt, err)
	}
	if hd := dec.Header(); hd.NoChecksum() || hd.PageSize != pageSize || hd.Commit == 0 || hd.Commit > head.MaxPages ||
		uint64(hd.MinTXID) != ref.MinSequence || uint64(hd.MaxTXID) != ref.MaxSequence ||
		head.Checksum(hd.PreApplyChecksum) != ref.PreChecksum {
		return nil, fmt.Errorf("%w: LTX header %+v does not match its reference", ErrCorrupt, hd)
	}
	return dec, nil
}

// checkObject verifies r's exact length and SHA-256 against ref and bounds
// every page frame's declared size, in one streaming pass that allocates
// nothing proportional to declared sizes.
func checkObject(r io.ReaderAt, ref head.Ref) error {
	if ref.Bytes > head.MaxObjectBytes {
		return fmt.Errorf("%w: object of %d bytes", ErrCorrupt, ref.Bytes)
	}
	hw := hashWriter{w: io.Discard, h: sha256.New()}
	br := bufio.NewReader(io.TeeReader(io.NewSectionReader(r, 0, int64(ref.Bytes)+1), &hw))
	err := frames(br)
	if err == nil {
		_, err = io.Copy(io.Discard, br)
	}
	if err != nil {
		return fmt.Errorf("%w: %v", ErrCorrupt, err)
	}
	if uint64(hw.n) != ref.Bytes || !bytes.Equal(hw.h.Sum(nil), ref.SHA256[:]) {
		return fmt.Errorf("%w: object length or SHA-256 differs from its reference", ErrCorrupt)
	}
	return nil
}

// frames walks the header and page frames of an LTX v3 stream, requiring
// block-format frames whose sizes are within maxFrame.
func frames(br *bufio.Reader) error {
	var zero [ltx.PageHeaderSize]byte
	b := make([]byte, ltx.HeaderSize)
	if _, err := io.ReadFull(br, b); err != nil {
		return err
	}
	var hd ltx.Header
	if err := hd.UnmarshalBinary(b); err != nil {
		return err
	}
	for n := 0; ; n++ {
		if _, err := io.ReadFull(br, b[:ltx.PageHeaderSize]); err != nil {
			return err
		}
		if bytes.Equal(b[:ltx.PageHeaderSize], zero[:]) {
			return nil
		}
		if n >= head.MaxPages || binary.BigEndian.Uint16(b[4:]) != ltx.PageHeaderFlagSize {
			return fmt.Errorf("unexpected page frame %d", n)
		}
		if _, err := io.ReadFull(br, b[:4]); err != nil {
			return err
		}
		size := binary.BigEndian.Uint32(b[:4])
		if size == 0 || size > maxFrame {
			return fmt.Errorf("page frame of %d bytes", size)
		}
		if _, err := br.Discard(int(size)); err != nil {
			return err
		}
	}
}

// Compact merges a contiguous run of objects into one with the pinned ltx
// compactor (AllowNonContiguousTXIDs=false), after verifying every input's
// length, SHA-256, frames, header and trailer against its reference and the
// whole chain: contiguous sequences and pre-checksum = previous post-checksum
// (the compactor checks neither). Readers are bounded by their reference's
// length and stop on ctx. A run starting with a snapshot yields a snapshot.
func Compact(ctx context.Context, w io.Writer, refs []head.Ref, objs []io.ReaderAt) (Object, error) {
	if len(refs) < 2 || len(objs) != len(refs) {
		return Object{}, errors.New("engine: compaction needs two or more objects")
	}
	var used [head.MaxPages/64 + 1]uint64 // pages carried by any input
	var commit uint32
	inputs := make([]io.Reader, len(refs))
	for i, ref := range refs {
		if i > 0 && (ref.MinSequence != refs[i-1].MaxSequence+1 || ref.PreChecksum != refs[i-1].PostChecksum) {
			return Object{}, fmt.Errorf("%w: objects %d and %d are not a chain", ErrCorrupt, i-1, i)
		}
		dec, err := openObject(objs[i], ref)
		if err != nil {
			return Object{}, err
		}
		var ph ltx.PageHeader
		data := make([]byte, pageSize)
		for {
			err := dec.DecodePage(&ph, data)
			if err == io.EOF {
				break
			}
			if err == nil {
				err = ctx.Err()
			}
			if err == nil && ph.Pgno > dec.Header().Commit {
				err = fmt.Errorf("%w: page %d beyond commit", ErrCorrupt, ph.Pgno)
			}
			if err != nil {
				return Object{}, err
			}
			used[ph.Pgno/64] |= 1 << (ph.Pgno % 64)
		}
		if err := dec.Close(); err != nil {
			return Object{}, fmt.Errorf("%w: %v", ErrCorrupt, err)
		}
		if head.Checksum(dec.Trailer().PostApplyChecksum) != ref.PostChecksum {
			return Object{}, fmt.Errorf("%w: object %d trailer differs from its reference", ErrCorrupt, i)
		}
		commit = dec.Header().Commit
		inputs[i] = ctxReader{ctx, io.NewSectionReader(objs[i], 0, int64(ref.Bytes))}
	}
	out := hashWriter{w: w, h: sha256.New()}
	c, err := ltx.NewCompactor(&out, inputs)
	if err != nil {
		return Object{}, err
	}
	c.AllowNonContiguousTXIDs = false
	if err := c.Compact(ctx); err != nil {
		return Object{}, err
	}
	first, last := refs[0], refs[len(refs)-1]
	hd, tr := c.Header(), c.Trailer()
	if uint64(hd.MinTXID) != first.MinSequence || uint64(hd.MaxTXID) != last.MaxSequence || hd.Commit != commit ||
		head.Checksum(hd.PreApplyChecksum) != first.PreChecksum || head.Checksum(tr.PostApplyChecksum) != last.PostChecksum {
		return Object{}, fmt.Errorf("%w: compacted object does not span its inputs", ErrCorrupt)
	}
	o := Object{Ref: head.Ref{MinSequence: first.MinSequence, MaxSequence: last.MaxSequence, PreChecksum: first.PreChecksum,
		PostChecksum: last.PostChecksum, Bytes: uint64(out.n)}, PageCount: commit}
	copy(o.Ref.SHA256[:], out.h.Sum(nil))
	for p := uint32(1); p <= commit; p++ {
		if used[p/64]&(1<<(p%64)) != 0 {
			o.Pages++
		}
	}
	return o, nil
}

// ctxReader stops reading once ctx is done: the compactor's page loop does
// not check its context.
type ctxReader struct {
	ctx context.Context
	r   io.Reader
}

func (c ctxReader) Read(b []byte) (int, error) {
	if err := c.ctx.Err(); err != nil {
		return 0, err
	}
	return c.r.Read(b)
}
