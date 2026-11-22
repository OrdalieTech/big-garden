package garden

import (
	"archive/tar"
	"bytes"
	"cmp"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"os"
	"slices"
	"strings"
	"time"

	"github.com/OrdalieTech/big-garden/internal/engine"
	"github.com/OrdalieTech/big-garden/internal/head"
	"github.com/OrdalieTech/big-garden/internal/store"
)

// manifest is manifest.json of an export archive (PROTOCOL §11), in
// canonical field order.
type manifest struct {
	Format           uint32        `json:"format"`
	Namespace        head.ID       `json:"namespace"`
	Key              string        `json:"key"`
	Incarnation      head.ID       `json:"incarnation"`
	Sequence         uint64        `json:"sequence"`
	Schema           uint32        `json:"schema"`
	Codec            string        `json:"codec"`
	PageSize         uint32        `json:"page_size"`
	PageCount        uint32        `json:"page_count"`
	DatabaseChecksum head.Checksum `json:"database_checksum"`
	SHA256           head.Digest   `json:"sha256"`
	Bytes            uint64        `json:"bytes"`
}

// Export writes a snapshot of ref's validated state as a tar archive of
// manifest.json, snapshot.ltx and blobs/<object> for each blob the image
// links, in object order (PROTOCOL §11, D33), on any Node, like Read. The
// blobs stream from storage after the entity mutex, each chunk verified; a
// blob collected meanwhile fails the export (not_found: export again). A
// write error is returned as is: the partial output is not a backup.
func (n *Node) Export(ctx context.Context, ref Ref, w io.Writer) (Version, error) {
	h, m, snap, blobs, err := n.exportFiles(ctx, ref) // spooled: w never blocks the entity
	if err != nil {
		return Version{}, err
	}
	defer snap.Close()
	tw := tar.NewWriter(w)
	entry := func(name string, size int64, r io.Reader) error {
		hdr := &tar.Header{Name: name, Typeflag: tar.TypeReg, Mode: 0o600, Size: size, ModTime: time.Unix(0, 0), Format: tar.FormatPAX}
		if size < 1<<33 && len(name) <= 100 {
			hdr.Format = tar.FormatUSTAR
		}
		if err := tw.WriteHeader(hdr); err != nil {
			return err
		}
		_, err := io.Copy(tw, r)
		return err
	}
	if err := entry("manifest.json", int64(len(m)), bytes.NewReader(m)); err != nil {
		return Version{}, err
	}
	if err := entry("snapshot.ltx", snap.n, io.NewSectionReader(snap.ReaderAt(), 0, snap.n)); err != nil {
		return Version{}, err
	}
	for _, f := range blobs {
		r, _ := n.OpenBlob(ctx, ref, blobOf(f))
		err := entry("blobs/"+f.Object, f.Size, r)
		r.Close()
		if e := (*Error)(nil); errors.As(err, &e) && e.Code == CodeNotFound { // not the entity: retryable
			err = newError(CodeUnavailable, "a blob was collected during the export: export again", e)
		}
		if err != nil {
			return Version{}, err
		}
	}
	if err := tw.Close(); err != nil {
		return Version{}, err
	}
	return Version{h.Incarnation, h.Sequence}, nil
}

// exportFiles returns the validated head of ref, the manifest and spooled
// snapshot of its image and the blobs it links, under the entity mutex, for
// an Export invoked at t0.
func (n *Node) exportFiles(ctx context.Context, ref Ref) (*head.Head, []byte, *spool, []engine.File, error) {
	t0 := time.Now()
	e, err := n.enter(ctx, ref.Key, &MutationID{}, nil, false)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	defer n.leave(e)
	if err := n.owned(ctx, e, ref, t0); err != nil {
		return nil, nil, nil, nil, err
	}
	h, snap := e.h, n.spool()
	fail := func(err error) (*head.Head, []byte, *spool, []engine.File, error) {
		snap.Close()
		return nil, nil, nil, nil, err
	}
	o, err := e.db.Snapshot(snap)
	if err != nil {
		e.discard()
		return fail(engineError(err))
	}
	if o.Ref.PostChecksum != h.DatabaseChecksum || o.Ref.MaxSequence != h.Sequence {
		e.discard()
		return fail(newError(CodeCorrupt, "snapshot differs from the head", nil))
	}
	m, err := json.Marshal(manifest{head.Format, h.Namespace, h.Key, h.Incarnation, h.Sequence, h.Schema, head.Codec,
		head.PageSize, h.PageCount, h.DatabaseChecksum, o.Ref.SHA256, o.Ref.Bytes})
	if err != nil || len(m) > head.MaxHeadBytes {
		return fail(newError(CodeCorrupt, "manifest", err))
	}
	linked, _, err := e.db.Files(nil)
	if err != nil {
		return fail(engineError(err))
	}
	blobs := slices.SortedFunc(maps.Values(linked), func(a, b engine.File) int { return strings.Compare(a.Object, b.Object) })
	return h, m, snap, blobs, nil
}

// Import creates key from an Export archive (PROTOCOL §11): the archive is
// read and validated strictly, then Create runs with kind 4 and a genesis
// image rebased to (namespace, key, new incarnation) at manifest.sequence+1
// with the receipts and floor preserved. req.Schema must equal the archive's
// schema, else CodeSchemaMismatch with Error.Schema set to it.
func (n *Node) Import(ctx context.Context, key string, req Request, r io.Reader) (Ref, Result, error) {
	if err := cmp.Or(checkRequest(key, req), n.checkClock(ctx, req, false)); err != nil {
		return Ref{}, Result{}, err
	}
	m, snap, spools, err := n.readArchive(r)
	staged := snap.n // counted in CacheBytes until the import returns (D37)
	for _, sp := range spools {
		staged += sp.f.Size
	}
	n.staging.Add(staged)
	defer n.staging.Add(-staged)
	defer snap.Close()
	defer func() {
		for _, sp := range spools {
			sp.file.Close()
			os.Remove(sp.file.Name())
		}
	}()
	if err != nil {
		return Ref{}, Result{}, err
	}
	if req.Schema != m.Schema {
		return Ref{}, Result{}, &Error{Code: CodeSchemaMismatch, Operation: req.ID, Schema: m.Schema, Message: "archive schema differs"}
	}
	src := head.Head{Namespace: m.Namespace, Key: m.Key, Incarnation: m.Incarnation, Sequence: m.Sequence, Schema: m.Schema,
		PageCount: m.PageCount, DatabaseChecksum: m.DatabaseChecksum, Snapshot: head.Ref{SHA256: m.SHA256, Bytes: m.Bytes,
			MinSequence: 1, MaxSequence: m.Sequence, PostChecksum: m.DatabaseChecksum}}
	bound := binding(4, req.Schema, req.Schema, req.Hash)
	return n.create(ctx, key, req, bound, func(path string, inc head.ID) (*engine.DB, []byte, error) {
		db, _, err := engine.RestoreChecked(ctx, n.drv, path, n.maxPages(), &src, []io.ReaderAt{snap.ReaderAt()})
		if err != nil {
			return nil, nil, engineError(err)
		}
		linked, _, err := db.Files(nil)
		if err == nil && !maps.EqualFunc(linked, spools, func(f engine.File, sp spooled) bool {
			return f.Size == sp.f.Size && f.SHA256 == sp.f.SHA256 && bytes.Equal(f.Chunks, sp.f.Chunks)
		}) {
			err = fmt.Errorf("%w: the archive's blobs differ from the files of its image", engine.ErrCorrupt)
		}
		if err == nil {
			err = db.Rebase(n.ns, key, inc)
		}
		if err == nil {
			_, _, err = db.Mutate(engine.Commit{ID: head.OpID(req.ID), Bound: bound, Schema: req.Schema},
				func(*engine.Tx) ([]byte, error) { return nil, nil })
		}
		if err != nil {
			db.Discard()
			return nil, nil, engineError(err)
		}
		return db, nil, nil
	}, func(ctx context.Context, inc head.ID) error { // the blobs, renamed to epoch 0 by Rebase
		for object, sp := range spools {
			if err := n.putSpooled(ctx, head.BlobPrefix(key, inc)+"0000000000000000"+object[16:], sp.file.Name(), sp.f); err != nil {
				return err
			}
		}
		return nil
	})
}

// spooled is a blob of an archive being imported.
type spooled struct {
	file *os.File
	f    engine.File
}

// putSpooled uploads the blob f from the file at path to key before a
// genesis; a resumed create that finds the key keeps it only if it verifies.
func (n *Node) putSpooled(ctx context.Context, key, path string, f engine.File) error {
	file, err := os.Open(path)
	if err != nil {
		return newError(CodeUnavailable, "blob spool", err)
	}
	defer file.Close()
	err = n.st.PutFile(ctx, key, file, f.Size, f.SHA256)
	if store.OutcomeOf(err) == store.PreconditionFailed {
		r := &blobReader{n: n, ctx: ctx, key: key, f: f, last: -2}
		_, err = io.Copy(io.Discard, r)
		r.Close()
	}
	if err != nil {
		return storeError(err)
	}
	return nil
}

// readArchive reads exactly manifest.json then snapshot.ltx, regular files
// within their bounds, then the end of the archive and only zero padding.
// The snapshot is spooled (D34): the caller closes it, also on an error.
func (n *Node) readArchive(r io.Reader) (manifest, *spool, map[string]spooled, error) {
	var m manifest
	spools, snap := map[string]spooled{}, n.spool()
	bad := func(msg string) (manifest, *spool, map[string]spooled, error) {
		return m, snap, spools, newError(CodeCorrupt, "archive: "+msg, nil)
	}
	cr := &counted{Reader: r}
	tr := tar.NewReader(cr)
	var mj []byte
	sum := sha256.New()
	for i, name := range []string{"manifest.json", "snapshot.ltx"} {
		limit := int64(head.MaxHeadBytes)
		if i == 1 {
			limit = n.cfg.Limits.EntityBytes + n.cfg.Limits.EntityBytes/head.PageSize*64 + 1<<20
		}
		hdr, err := tr.Next()
		if err != nil || hdr.Name != name || hdr.Typeflag != tar.TypeReg || hdr.Size < 0 || hdr.Size > limit {
			return bad("expected the regular file " + name)
		}
		var k int64
		if i == 0 {
			mj, err = io.ReadAll(io.LimitReader(tr, hdr.Size))
			k = int64(len(mj))
		} else {
			k, err = io.Copy(io.MultiWriter(snap, sum), io.LimitReader(tr, hdr.Size))
		}
		if err != nil || k != hdr.Size {
			return bad("truncated " + name)
		}
	}
	// archive/tar also ends at a bare EOF or one zero block: require both end blocks
	blocks := func(size int64) int64 { return 512 + (size+511)/512*512 }
	want, last := blocks(int64(len(mj)))+blocks(snap.n)+1024, ""
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		object, ok := strings.CutPrefix(hdr.Name, "blobs/")
		if _, valid := head.BlobEpoch(object); err != nil || !ok || !valid || object <= last || hdr.Typeflag != tar.TypeReg || hdr.Size <= inlineFile {
			return bad("expected blobs/<object> in increasing order")
		}
		if hdr.Format == tar.FormatPAX {
			want += 1024 // its extended header block and record
		}
		f, err := os.CreateTemp(n.dir, "import-*")
		if err != nil {
			return m, nil, spools, newError(CodeUnavailable, "import spool", err)
		}
		sp := spooled{file: f}
		c := &chunker{whole: sha256.New(), part: sha256.New(), f: &sp.f}
		spools[object], last = sp, object
		if k, err := io.Copy(io.MultiWriter(f, c), io.LimitReader(tr, hdr.Size)); err != nil || k != hdr.Size {
			return bad("truncated blobs/" + object)
		}
		c.close()
		sp.f.Object, spools[object] = object, sp
		want += blocks(hdr.Size)
	}
	if cr.n != want {
		return bad("no end-of-archive blocks")
	}
	rest, err := io.ReadAll(io.LimitReader(r, 1<<20+1))
	if err != nil || len(rest) > 1<<20 || bytes.ContainsFunc(rest, func(c rune) bool { return c != 0 }) {
		return bad("data after the end of the archive")
	}
	d := json.NewDecoder(bytes.NewReader(mj))
	d.DisallowUnknownFields()
	if err := d.Decode(&m); err != nil {
		return bad("manifest.json does not decode")
	}
	canon, _ := json.Marshal(m)
	switch {
	case !bytes.Equal(canon, mj):
		return bad("manifest.json is not canonical")
	case m.Format != head.Format || m.Codec != head.Codec || m.PageSize != head.PageSize || m.Namespace == head.ID{} ||
		m.Incarnation == head.ID{} || head.ValidKey(m.Key) != nil || m.Sequence == 0 || m.Sequence >= head.MaxInt ||
		m.Schema == 0 || m.PageCount == 0 || m.PageCount > n.maxPages() || m.DatabaseChecksum&head.ChecksumFlag == 0:
		return bad("unsupported or invalid manifest")
	case uint64(snap.n) != m.Bytes || [32]byte(sum.Sum(nil)) != m.SHA256:
		return bad("snapshot.ltx differs from the manifest")
	}
	return m, snap, spools, nil
}

// counted counts the bytes read through it.
type counted struct {
	io.Reader
	n int64
}

func (c *counted) Read(p []byte) (int, error) {
	k, err := c.Reader.Read(p)
	c.n += int64(k)
	return k, err
}
