package garden

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"hash"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/OrdalieTech/big-garden/internal/engine"
	"github.com/OrdalieTech/big-garden/internal/head"
	"github.com/OrdalieTech/big-garden/internal/store"
)

// Files (DECISIONS D33). An entity holds named files beside its tables:
// small ones inline in its image, others as immutable blob objects under
// its own prefix, each named by the ownership epoch that uploaded it. The
// image's _state_blobs table is the only record of which blobs are live,
// so a file commits with the rows that use it and dies with the entity.
//
// Safety, without clocks across nodes:
//  1. Bytes before row: a mutation may link a blob only if the image links
//     it already, or if this owner finished uploading it in its current
//     epoch less than blobLinkWindow ago (Node.fresh).
//  2. So a blob is linked during one contiguous interval: once unlinked and
//     past its window, nothing can link it again.
//  3. The sweep lists first, then judges under the entity mutex: it deletes
//     only a blob of an epoch at most its own, unlinked in its image, and
//     neither uploading nor fresh. A stale owner at epoch E never touches a
//     blob of a later epoch, and by 1 and 2 every blob of an epoch up to E
//     that is linked now was linked in its image too.

const (
	inlineFile     = 16 << 10 // up to this size a file lives in the image: it fits the head's inline tail (D16)
	blobLinkWindow = 30 * time.Minute
	maxWindow      = 64 // chunks per GET of a sequential read
)

// Blob is the content of a file: what PutBlob returns, Tx.SetFile names
// and View.File finds. Size and SHA256 describe it; changing them changes
// nothing stored.
type Blob struct {
	Size   int64
	SHA256 [32]byte
	f      engine.File
}

func blobOf(f engine.File) Blob { return Blob{f.Size, f.SHA256, f} }

type upload struct {
	epoch uint64
	until time.Time // zero while the PUT is in flight
}

// chunker hashes a stream whole and per engine.FileChunk.
type chunker struct {
	whole, part hash.Hash
	in          int64
	f           *engine.File
}

func (c *chunker) Write(p []byte) (int, error) {
	for q := p; len(q) > 0; {
		k := min(int64(len(q)), engine.FileChunk-c.in)
		c.whole.Write(q[:k])
		c.part.Write(q[:k])
		if c.in += k; c.in == engine.FileChunk {
			c.f.Chunks, c.in = c.part.Sum(c.f.Chunks), 0
			c.part.Reset()
		}
		q = q[k:]
	}
	c.f.Size += int64(len(p))
	return len(p), nil
}

func (c *chunker) close() {
	if c.in > 0 {
		c.f.Chunks = c.part.Sum(c.f.Chunks)
	}
	c.f.SHA256 = [32]byte(c.whole.Sum(nil))
}

// PutBlob stores the bytes of r for entity ref, outside the entity mutex,
// and returns them for Tx.SetFile on this Node within blobLinkWindow (after
// which an unlinked blob is collected). It requires ownership. Up to 16 KiB
// stays in memory and enters the image inline; bytes the image already
// holds are not stored again; up to 8 MiB is buffered, more is spooled to
// the cache directory and sent in parallel parts. A reader from OpenBlob,
// unread, is copied inside the storage service.
func (n *Node) PutBlob(ctx context.Context, ref Ref, r io.Reader) (Blob, error) {
	src, copied := r.(*blobReader)
	copied = copied && src.n == n && src.off == 0 && src.f.Size > inlineFile && src.f.Size <= 5<<30
	var f engine.File
	var body io.ReaderAt
	if copied {
		f = src.f
	} else {
		c := &chunker{whole: sha256.New(), part: sha256.New(), f: &f}
		var buf bytes.Buffer
		if _, err := io.Copy(io.MultiWriter(&buf, c), io.LimitReader(r, store.PartBytes+1)); err != nil {
			return Blob{}, newError(CodeInvalidArgument, "reading the blob", err)
		}
		body = bytes.NewReader(buf.Bytes())
		if f.Size > store.PartBytes {
			spool, err := os.CreateTemp(n.dir, "blob-*")
			if err != nil {
				return Blob{}, newError(CodeUnavailable, "blob spool", err)
			}
			defer os.Remove(spool.Name())
			defer spool.Close()
			if _, err := spool.Write(buf.Bytes()); err != nil {
				return Blob{}, newError(CodeUnavailable, "blob spool", err)
			}
			if _, err := io.Copy(io.MultiWriter(spool, c), r); err != nil {
				return Blob{}, newError(CodeInvalidArgument, "reading the blob", err)
			}
			body = spool
		}
		c.close()
		if f.Size <= inlineFile {
			f.Inline = append([]byte{}, buf.Bytes()...) // non-nil, even empty: inline
			return blobOf(f), nil
		}
	}
	e, err := n.enter(ctx, ref.Key, nil, nil, false)
	if err != nil {
		return Blob{}, err
	}
	if err := n.owned(ctx, e, ref, time.Time{}); err != nil {
		n.leave(e)
		return Blob{}, err
	}
	_, same, err := e.db.Files(&f.SHA256)
	epoch := e.h.Epoch
	n.leave(e)
	if err != nil {
		return Blob{}, engineError(err)
	}
	if same != nil { // linked now, so linkable now (rule 1)
		return blobOf(*same), nil
	}
	f.Object = head.BlobName(epoch, head.NewID())
	key := head.BlobPrefix(ref.Key, ref.Incarnation) + f.Object
	n.mu.Lock()
	n.uploads[key] = upload{epoch: epoch}
	n.mu.Unlock()
	failpoint("blob.put")
	if copied {
		err = n.st.Copy(ctx, src.key, key)
	} else {
		err = n.st.PutFile(ctx, key, body, f.Size, f.SHA256)
	}
	n.mu.Lock()
	n.uploads[key] = upload{epoch, n.now()} // never fresh: the sweep takes it if it landed
	if err == nil {
		n.uploads[key] = upload{epoch, n.now().Add(blobLinkWindow)}
	}
	n.mu.Unlock()
	if err != nil {
		return Blob{}, storeError(err)
	}
	return blobOf(f), nil
}

// fresh reports whether e's owner may link the blob named object now.
func (n *Node) fresh(e *entity) func(string) bool {
	return func(object string) bool {
		n.mu.Lock()
		defer n.mu.Unlock()
		u, ok := n.uploads[head.BlobPrefix(e.key, e.h.Incarnation)+object]
		return ok && u.epoch == e.h.Epoch && n.now().Before(u.until)
	}
}

// SetFile names b in the entity as name, replacing any file of that name,
// when the mutation commits; a nil b deletes the file. Several names may
// hold the same Blob. A blob neither in the image nor fresh from PutBlob on
// this Node is refused (invalid_argument): upload it again.
func (t *Tx) SetFile(name string, b *Blob) error {
	if b == nil {
		return t.t.SetFile(name, nil)
	}
	return t.t.SetFile(name, &b.f)
}

// File returns the file name of the image read. Application SQL may also
// read _state_blobs (name, object, size, sha256, chunks, inline) and join
// it with its own tables; only SetFile writes it.
func (v *View) File(ctx context.Context, name string) (b Blob, ok bool, err error) {
	err = v.Query(ctx, "SELECT coalesce(object, ''), size, sha256, chunks, inline FROM _state_blobs WHERE name = ?", []any{name},
		func(rows *sql.Rows) error {
			if !rows.Next() {
				return nil
			}
			var sum []byte
			ok = true
			if err := rows.Scan(&b.f.Object, &b.f.Size, &sum, &b.f.Chunks, &b.f.Inline); err != nil {
				return err
			}
			b.f.SHA256 = [32]byte(sum)
			return nil
		})
	if err != nil && strings.Contains(err.Error(), "no such table: _state_blobs") {
		err = nil
	}
	return blobOf(b.f), ok, err
}

// OpenBlob opens b, a file of ref read by View.File, on any Node and with
// no entity state: a blob never changes. The reader is also an io.ReaderAt
// safe for concurrent use, so http.ServeContent, archive/zip and other
// random-access readers work on it in place. It fetches only the 1 MiB
// chunks it touches, a growing run at a time while reads are sequential,
// checks each against the SHA-256 the image recorded and keeps it in the
// Node's chunk cache. Reads use ctx; a blob unlinked and collected since
// the read answers not_found.
func (n *Node) OpenBlob(ctx context.Context, ref Ref, b Blob) (io.ReadSeekCloser, error) {
	if len(b.f.Chunks) != int((b.f.Size+engine.FileChunk-1)/engine.FileChunk)*32 && b.f.Inline == nil {
		return nil, invalid("not a Blob of View.File or PutBlob")
	}
	return &blobReader{n: n, ctx: ctx, key: head.BlobPrefix(ref.Key, ref.Incarnation) + b.f.Object, f: b.f, last: -2}, nil
}

type blobReader struct {
	n    *Node
	ctx  context.Context
	key  string
	f    engine.File
	mu   sync.Mutex
	off  int64         // Read and Seek position
	body io.ReadCloser // an open GET serving chunks next..end-1
	next, end, last int64
	window, prev    int64
	cur             []byte // chunk last
}

func (r *blobReader) chunk(i int64) ([]byte, error) {
	if r.cur != nil && i == r.last {
		return r.cur, nil
	}
	sum := [32]byte(r.f.Chunks[i*32 : i*32+32])
	r.cur = nil
	if b := r.n.cached(sum); b != nil {
		r.last, r.cur = i, b
		return b, nil
	}
	if r.body == nil || r.next != i {
		if r.body != nil {
			r.body.Close()
		}
		r.window = 1
		if i == r.last+1 { // sequential: read further ahead each time
			r.window = min(2*r.prev, maxWindow)
		}
		r.prev = r.window
		count := (r.f.Size + engine.FileChunk - 1) / engine.FileChunk
		r.next, r.end = i, min(count, i+r.window)
		end := min(r.f.Size, r.end*engine.FileChunk)
		body, err := r.n.st.Open(r.ctx, r.key, i*engine.FileChunk, end-i*engine.FileChunk)
		if store.OutcomeOf(err) == store.NotFound {
			return nil, newError(CodeNotFound, "the blob was unlinked and collected", err)
		}
		if err != nil {
			return nil, storeError(err)
		}
		r.body = body
	}
	b := make([]byte, min(engine.FileChunk, r.f.Size-i*engine.FileChunk))
	if _, err := io.ReadFull(r.body, b); err != nil {
		r.body.Close()
		r.body = nil
		return nil, newError(CodeUnavailable, "blob read", err)
	}
	if r.next, r.last = r.next+1, i; r.next == r.end {
		r.body.Close()
		r.body = nil
	}
	if sha256.Sum256(b) != sum {
		return nil, newError(CodeCorrupt, "a blob chunk does not match its SHA-256", nil)
	}
	r.n.keep(sum, b)
	r.cur = b
	return b, nil
}

func (r *blobReader) ReadAt(p []byte, off int64) (int, error) {
	if off < 0 {
		return 0, invalid("negative offset")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	k := 0
	for k < len(p) && off < r.f.Size {
		var b []byte
		if r.f.Object == "" {
			b = r.f.Inline[off:]
		} else {
			c, err := r.chunk(off / engine.FileChunk)
			if err != nil {
				return k, err
			}
			b = c[off%engine.FileChunk:]
		}
		m := copy(p[k:], b)
		k, off = k+m, off+int64(m)
	}
	if k < len(p) {
		return k, io.EOF
	}
	return k, nil
}

func (r *blobReader) Read(p []byte) (int, error) {
	k, err := r.ReadAt(p, r.off)
	r.off += int64(k)
	if err == io.EOF && k > 0 {
		err = nil
	}
	return k, err
}

func (r *blobReader) Seek(off int64, whence int) (int64, error) {
	switch whence {
	case io.SeekCurrent:
		off += r.off
	case io.SeekEnd:
		off += r.f.Size
	}
	if off < 0 {
		return r.off, invalid("negative position")
	}
	r.off = off
	return off, nil
}

func (r *blobReader) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.body != nil {
		r.body.Close()
		r.body = nil
	}
	return nil
}

// The chunk cache: verified chunks by SHA-256 under CacheDir/chunks, shared
// by the Node's sessions, within a quarter of CacheBytes. A chunk is checked
// again on every hit, so a damaged file is dropped, never served.
type chunkCache struct {
	mu    sync.Mutex
	dir   string
	used  map[[32]byte]time.Time
	bytes int64
}

func (n *Node) cached(sum [32]byte) []byte {
	c := &n.chunks
	path := filepath.Join(c.dir, hex.EncodeToString(sum[:]))
	b, err := os.ReadFile(path)
	if err != nil {
		return nil
	}
	if sha256.Sum256(b) != sum {
		os.Remove(path)
		return nil
	}
	c.mu.Lock()
	c.used[sum] = time.Now()
	c.mu.Unlock()
	return b
}

func (n *Node) keep(sum [32]byte, b []byte) {
	c := &n.chunks
	tmp, err := os.CreateTemp(c.dir, "tmp-*")
	if err != nil {
		return
	}
	_, err = tmp.Write(b)
	if err = errors.Join(err, tmp.Close()); err != nil || os.Rename(tmp.Name(), filepath.Join(c.dir, hex.EncodeToString(sum[:]))) != nil {
		os.Remove(tmp.Name())
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.used[sum]; !ok {
		c.bytes += int64(len(b))
	}
	c.used[sum] = time.Now()
	for c.bytes > n.cfg.Limits.CacheBytes/4 && len(c.used) > 1 { // least recently used first
		var old [32]byte
		var at time.Time
		for s, t := range c.used {
			if at.IsZero() || t.Before(at) {
				old, at = s, t
			}
		}
		os.Remove(filepath.Join(c.dir, hex.EncodeToString(old[:])))
		delete(c.used, old)
		c.bytes -= engine.FileChunk // at most: a short last chunk is undercounted, never over
	}
}

// openChunks indexes the chunks earlier sessions left.
func (n *Node) openChunks() error {
	c := &n.chunks
	c.dir, c.used = filepath.Join(n.cfg.CacheDir, "chunks"), map[[32]byte]time.Time{}
	if err := os.MkdirAll(c.dir, 0o700); err != nil {
		return newError(CodeUnavailable, "chunk cache dir", err)
	}
	entries, _ := os.ReadDir(c.dir)
	for _, d := range entries {
		var sum [32]byte
		info, err := d.Info()
		if _, derr := hex.Decode(sum[:], []byte(d.Name())); err != nil || derr != nil || len(d.Name()) != 64 {
			os.Remove(filepath.Join(c.dir, d.Name()))
			continue
		}
		c.used[sum], c.bytes = info.ModTime(), c.bytes+info.Size()
	}
	return nil
}

// sweep deletes the blobs of ref that nothing can use (rules 1-3). It lists
// first, judges under the entity mutex, and deletes after it: a blob judged
// dead stays dead.
func (n *Node) sweep(ctx context.Context, ref Ref) error {
	prefix := head.BlobPrefix(ref.Key, ref.Incarnation)
	var listed []string
	for after := ""; ; {
		page, more, err := n.st.List(ctx, prefix, after, 1000)
		if err != nil {
			return storeError(err)
		}
		for _, o := range page {
			listed, after = append(listed, o.Key), o.Key
		}
		if !more || len(page) == 0 {
			break
		}
	}
	e, err := n.enter(ctx, ref.Key, nil, nil, false)
	if err != nil {
		return err
	}
	if err := n.owned(ctx, e, ref, time.Time{}); err != nil {
		n.leave(e)
		return err
	}
	if e.unresolved != nil { // the image may not be the committed one
		n.leave(e)
		return newError(CodeUnavailable, "a publication is unresolved", nil)
	}
	linked, _, err := e.db.Files(nil)
	if err != nil {
		n.leave(e)
		return engineError(err)
	}
	epoch, fresh := e.h.Epoch, n.fresh(e)
	e.db.SweepDue = false
	n.mu.Lock()
	inflight := map[string]bool{}
	for k, u := range n.uploads { // a committed link protects its blob from now on: unlinking it frees it
		if _, ok := linked[strings.TrimPrefix(k, prefix)]; strings.HasPrefix(k, prefix) && !u.until.IsZero() && (ok || !n.now().Before(u.until)) {
			delete(n.uploads, k)
		}
		inflight[k] = u.until.IsZero()
	}
	n.mu.Unlock()
	var dead []string
	for _, k := range listed {
		name := strings.TrimPrefix(k, prefix)
		_, live := linked[name]
		if ep, ok := head.BlobEpoch(name); ok && ep <= epoch && !live && !inflight[k] && !fresh(name) {
			dead = append(dead, k)
		}
	}
	n.leave(e)
	var errs []error
	for _, k := range dead {
		if err := n.st.Delete(ctx, k); store.OutcomeOf(err) != store.OK {
			errs = append(errs, err)
		}
	}
	if len(errs) > 0 {
		return storeError(errors.Join(errs...))
	}
	return nil
}
