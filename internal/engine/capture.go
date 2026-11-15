package engine

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/binary"
	"errors"
	"fmt"
	"hash"
	"io"
	"os"
	"slices"

	"github.com/OrdalieTech/big-garden/internal/head"
	"github.com/superfly/ltx"
)

// Object describes one encoded LTX object. The caller sets Ref's Key,
// BaseRevision and Level; the engine fills the rest.
type Object struct {
	Ref       head.Ref
	PageCount uint32 // database pages after applying it (the LTX commit)
	Pages     uint32 // page frames it carries
}

const (
	walHeader = 32
	walFrame  = 24 + pageSize
)

// Delta encodes the pages committed since the last checkpoint as one LTX
// object covering sequences published+1 .. current (DECISIONS D2, PROTOCOL §6
// step 6). Page numbers come from the WAL frame headers, page contents from
// sqlite_dbpage, the final size from page_count. The rolling checksum is
// updated from the old contents of the changed and truncated pages in the
// main file, which still holds the published image: no full scan.
func (d *DB) Delta(w io.Writer) (Object, error) {
	frames, err := d.walPages()
	if err != nil {
		return Object{}, err
	}
	var o Object
	err = d.readTx(func(m Meta, n uint32) error {
		if m.Sequence <= d.pub.Sequence {
			return errors.New("engine: no mutation to capture")
		}
		changed := frames[:0:0]
		grown := uint32(0)
		for _, p := range frames {
			if p <= n {
				changed = append(changed, p)
			}
			if p > d.pub.PageCount && p <= n {
				grown++
			}
		}
		if lock := ltx.LockPgno(pageSize); d.pub.PageCount < lock && lock <= n {
			grown++ // SQLite never writes the lock page (1 GiB offset); ltx excludes it
		}
		if n > d.pub.PageCount && grown != n-d.pub.PageCount {
			return fmt.Errorf("%w: WAL lacks %d grown pages", ErrCorrupt, n-d.pub.PageCount-grown)
		}
		stmt, err := d.conn.PrepareContext(context.Background(), "SELECT data FROM sqlite_dbpage WHERE pgno = ?")
		if err != nil {
			return err
		}
		defer stmt.Close()
		e, err := newEncoder(w, n, d.pub.Sequence+1, m.Sequence, d.pub.Checksum)
		if err != nil {
			return err
		}
		old := make([]byte, pageSize)
		var data []byte
		for _, p := range changed {
			if err := stmt.QueryRow(p).Scan(&data); err != nil {
				return err
			}
			if p <= d.pub.PageCount {
				if err := xorPage(&e.sum, d.main, p, old); err != nil {
					return err
				}
			}
			if err := e.page(p, data); err != nil {
				return err
			}
		}
		for p := n + 1; p <= d.pub.PageCount; p++ { // truncated pages
			if p == ltx.LockPgno(pageSize) {
				continue
			}
			if err := xorPage(&e.sum, d.main, p, old); err != nil {
				return err
			}
		}
		o, err = e.close()
		return err
	})
	return o, err
}

// Snapshot encodes the current committed image as a full LTX snapshot
// covering 1..current sequence. With an empty WAL it is the published image
// and must match its checksum.
func (d *DB) Snapshot(w io.Writer) (Object, error) {
	st, err := os.Stat(d.path + "-wal")
	if err != nil {
		return Object{}, err
	}
	var o Object
	err = d.readTx(func(m Meta, n uint32) error {
		rows, err := d.conn.QueryContext(context.Background(), "SELECT pgno, data FROM sqlite_dbpage")
		if err != nil {
			return err
		}
		defer rows.Close()
		e, err := newEncoder(w, n, 1, m.Sequence, 0)
		if err != nil {
			return err
		}
		var p uint32
		var data sql.RawBytes
		for i := uint32(1); rows.Next(); i++ {
			if err := rows.Scan(&p, &data); err != nil {
				return err
			}
			if p == ltx.LockPgno(pageSize) && p == i {
				o.Pages++ // counted below as present; never encoded
				continue
			}
			if p != i {
				return fmt.Errorf("engine: sqlite_dbpage returned page %d at position %d", p, i)
			}
			if err := e.page(p, data); err != nil {
				return err
			}
		}
		if err := rows.Err(); err != nil {
			return err
		}
		skipped := o.Pages
		if o, err = e.close(); err != nil {
			return err
		}
		o.Pages += skipped
		if o.Pages != n {
			return fmt.Errorf("engine: snapshot has %d of %d pages", o.Pages, n)
		}
		if st.Size() == 0 && (Image{m.Sequence, n, o.Ref.PostChecksum}) != d.pub {
			return fmt.Errorf("%w: main file differs from its published image", ErrCorrupt)
		}
		return nil
	})
	return o, err
}

// readTx runs fn inside a library read transaction with the current meta
// row and page count, so every page read sees one committed state.
func (d *DB) readTx(fn func(Meta, uint32) error) (err error) {
	if err := d.exec("BEGIN"); err != nil {
		return err
	}
	defer func() { err = withCleanup(err, d.rollback()) }()
	m, err := d.Meta()
	if err != nil {
		return err
	}
	var n uint32
	if err := d.conn.QueryRowContext(context.Background(), "PRAGMA page_count").Scan(&n); err != nil {
		return err
	}
	if n == 0 || n > head.MaxPages {
		return fmt.Errorf("%w: page count %d", ErrCorrupt, n)
	}
	return fn(m, n)
}

// walPages returns the sorted distinct page numbers named by the frame
// headers of the WAL. Since the last TRUNCATE checkpoint every page write
// landed in one of these frames, so the set covers every changed page;
// frames of rolled-back transactions only add pages whose content is
// unchanged (D2).
func (d *DB) walPages() ([]uint32, error) {
	f, err := os.Open(d.path + "-wal")
	if err != nil {
		return nil, err
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil || st.Size() == 0 {
		return nil, err
	}
	var b [walHeader]byte
	if _, err := f.ReadAt(b[:], 0); err != nil {
		return nil, err
	}
	be := binary.BigEndian
	if be.Uint32(b[0:])&^1 != 0x377f0682 || be.Uint32(b[4:]) != 3007000 || be.Uint32(b[8:]) != pageSize ||
		(st.Size()-walHeader)%walFrame != 0 {
		return nil, fmt.Errorf("%w: unexpected WAL header or size %d", ErrCorrupt, st.Size())
	}
	var pages []uint32
	for off := int64(walHeader); off < st.Size(); off += walFrame {
		if _, err := f.ReadAt(b[:4], off); err != nil {
			return nil, err
		}
		p := be.Uint32(b[:4])
		if p == 0 || p > head.MaxPages {
			return nil, fmt.Errorf("%w: WAL frame names page %d", ErrCorrupt, p)
		}
		pages = append(pages, p)
	}
	slices.Sort(pages)
	return slices.Compact(pages), nil
}

// xorPage reads page p of f into buf and XORs its checksum into sum.
func xorPage(sum *ltx.Checksum, f io.ReaderAt, p uint32, buf []byte) error {
	if _, err := f.ReadAt(buf, int64(p-1)*pageSize); err != nil {
		return err
	}
	*sum ^= ltx.ChecksumPage(p, buf)
	return nil
}

// encoder writes one LTX object while hashing its bytes and keeping the
// rolling checksum. sum holds the XOR of the page checksums; e.page adds a
// page, and the caller XORs out pages it replaces or truncates.
type encoder struct {
	enc  *ltx.Encoder
	out  hashWriter
	sum  ltx.Checksum
	obj  Object
	prev uint32
}

func newEncoder(w io.Writer, commit uint32, min, max uint64, pre head.Checksum) (*encoder, error) {
	e := &encoder{out: hashWriter{w: w, h: sha256.New()}, sum: ltx.Checksum(pre)}
	e.enc, _ = ltx.NewEncoder(&e.out) // never fails
	e.obj = Object{Ref: head.Ref{MinSequence: min, MaxSequence: max, PreChecksum: pre}, PageCount: commit}
	if min == 0 || max < min || max > head.MaxInt {
		return nil, fmt.Errorf("engine: invalid sequence range %d..%d", min, max)
	}
	return e, e.enc.EncodeHeader(ltx.Header{Version: ltx.Version, PageSize: pageSize, Commit: commit,
		MinTXID: ltx.TXID(min), MaxTXID: ltx.TXID(max), PreApplyChecksum: ltx.Checksum(pre)})
}

func (e *encoder) page(p uint32, data []byte) error {
	if len(data) != pageSize || p <= e.prev {
		return fmt.Errorf("engine: page %d of %d bytes after page %d", p, len(data), e.prev)
	}
	e.prev = p
	e.sum ^= ltx.ChecksumPage(p, data)
	e.obj.Pages++
	return e.enc.EncodePage(ltx.PageHeader{Pgno: p}, data)
}

func (e *encoder) close() (Object, error) {
	post := ltx.ChecksumFlag | e.sum
	e.enc.SetPostApplyChecksum(post)
	if err := e.enc.Close(); err != nil {
		return Object{}, err
	}
	e.obj.Ref.PostChecksum = head.Checksum(post)
	e.obj.Ref.Bytes = uint64(e.out.n)
	copy(e.obj.Ref.SHA256[:], e.out.h.Sum(nil))
	return e.obj, nil
}

// hashWriter counts and hashes what it writes to w.
type hashWriter struct {
	w io.Writer
	h hash.Hash
	n int64
}

func (hw *hashWriter) Write(b []byte) (int, error) {
	n, err := hw.w.Write(b)
	hw.h.Write(b[:n])
	hw.n += int64(n)
	return n, err
}

// CopyImage copies the published image (the main file) to dst. The caller
// excludes checkpoints (the entity mutex); copy_file_range lets btrfs and
// xfs share the extents instead of copying them (D35).
func (d *DB) CopyImage(dst *os.File) (Image, error) {
	src, err := os.OpenFile(d.path, os.O_RDONLY|NoFollow, 0)
	if err != nil {
		return Image{}, err
	}
	defer src.Close()
	n := int64(d.pub.PageCount) * pageSize
	if k, err := io.Copy(dst, io.LimitReader(src, n)); err != nil || k != n {
		return Image{}, fmt.Errorf("engine: image copy of %d of %d bytes: %v", k, n, err)
	}
	return d.pub, nil
}

// SnapshotFile encodes the image img held by f (a CopyImage) as a snapshot
// covering 1..img.Sequence, and checks it ends at img's checksum.
func SnapshotFile(w io.Writer, f io.ReaderAt, img Image) (Object, error) {
	e, err := newEncoder(w, img.PageCount, 1, img.Sequence, 0)
	if err != nil {
		return Object{}, err
	}
	buf, skipped := make([]byte, pageSize), uint32(0)
	for p := uint32(1); p <= img.PageCount; p++ {
		if p == ltx.LockPgno(pageSize) {
			skipped++ // never written by SQLite, never encoded
			continue
		}
		if _, err := f.ReadAt(buf, int64(p-1)*pageSize); err != nil {
			return Object{}, err
		}
		if err := e.page(p, buf); err != nil {
			return Object{}, err
		}
	}
	o, err := e.close()
	if err != nil {
		return Object{}, err
	}
	if o.Pages += skipped; o.Ref.PostChecksum != img.Checksum {
		return Object{}, fmt.Errorf("%w: the image copy does not end at its checksum", ErrCorrupt)
	}
	return o, nil
}
