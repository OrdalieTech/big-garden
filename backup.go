package garden

import (
	"archive/tar"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"io"
	"time"

	"github.com/OrdalieTech/big-garden/internal/engine"
	"github.com/OrdalieTech/big-garden/internal/head"
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
// manifest.json and snapshot.ltx (PROTOCOL §11). A write error is returned
// as is: the partial output is not a backup.
func (n *Node) Export(ctx context.Context, ref Ref, w io.Writer) (Version, error) {
	if err := head.ValidKey(ref.Key); err != nil {
		return Version{}, invalid(err.Error())
	}
	e, err := n.enter(ref.Key, &MutationID{})
	if err != nil {
		return Version{}, err
	}
	defer e.mu.Unlock()
	if err := n.owned(ctx, e, ref, true); err != nil {
		return Version{}, err
	}
	h := e.h
	var snap bytes.Buffer
	o, err := e.db.Snapshot(&snap)
	if err != nil {
		e.discard()
		return Version{}, engineError(err)
	}
	if o.Ref.PostChecksum != h.DatabaseChecksum || o.Ref.MaxSequence != h.Sequence {
		e.discard()
		return Version{}, newError(CodeCorrupt, "snapshot differs from the head", nil)
	}
	m, err := json.Marshal(manifest{head.Format, h.Namespace, h.Key, h.Incarnation, h.Sequence, h.Schema, head.Codec,
		head.PageSize, h.PageCount, h.DatabaseChecksum, o.Ref.SHA256, o.Ref.Bytes})
	if err != nil || len(m) > maxHeadBytes {
		return Version{}, newError(CodeCorrupt, "manifest", err)
	}
	tw := tar.NewWriter(w)
	for _, f := range []struct {
		name string
		body []byte
	}{{"manifest.json", m}, {"snapshot.ltx", snap.Bytes()}} {
		hdr := &tar.Header{Name: f.name, Typeflag: tar.TypeReg, Mode: 0o600, Size: int64(len(f.body)),
			ModTime: time.Unix(0, 0), Format: tar.FormatUSTAR}
		if err := tw.WriteHeader(hdr); err != nil {
			return Version{}, err
		}
		if _, err := tw.Write(f.body); err != nil {
			return Version{}, err
		}
	}
	if err := tw.Close(); err != nil {
		return Version{}, err
	}
	return Version{h.Incarnation, h.Sequence}, nil
}

// Import creates key from an Export archive (PROTOCOL §11): the archive is
// read and validated strictly, then Create runs with kind 4 and a genesis
// image rebased to (namespace, key, new incarnation) at manifest.sequence+1
// with the receipts and floor preserved.
func (n *Node) Import(ctx context.Context, key string, req Request, r io.Reader) (Ref, Result, error) {
	if err := n.checkRequest(key, req); err != nil {
		return Ref{}, Result{}, err
	}
	m, snap, err := n.readArchive(r)
	if err != nil {
		return Ref{}, Result{}, err
	}
	if req.Schema != m.Schema {
		return Ref{}, Result{}, &Error{Code: CodeSchemaMismatch, Operation: req.ID, Message: "archive schema differs"}
	}
	src := head.Head{Namespace: m.Namespace, Key: m.Key, Incarnation: m.Incarnation, Sequence: m.Sequence, Schema: m.Schema,
		PageCount: m.PageCount, DatabaseChecksum: m.DatabaseChecksum, Snapshot: head.Ref{SHA256: m.SHA256, Bytes: m.Bytes,
			MinSequence: 1, MaxSequence: m.Sequence, PostChecksum: m.DatabaseChecksum}}
	bound := binding(4, req.Schema, req.Schema, req.Hash)
	return n.create(ctx, key, req, bound, func(path string, inc head.ID) (*engine.DB, []byte, error) {
		db, err := engine.Restore(ctx, path, n.maxPages(), &src, []io.ReaderAt{bytes.NewReader(snap)})
		if err != nil {
			return nil, nil, engineError(err)
		}
		if err = db.Rebase(n.ns, key, inc); err == nil {
			_, _, err = db.Mutate(engine.Commit{ID: head.OpID(req.ID), Bound: bound, Schema: req.Schema},
				func(*engine.Tx) ([]byte, error) { return nil, nil })
		}
		if err != nil {
			db.Discard()
			return nil, nil, engineError(err)
		}
		return db, nil, nil
	})
}

// readArchive reads exactly manifest.json then snapshot.ltx, regular files
// within their bounds, then the end of the archive and only zero padding.
func (n *Node) readArchive(r io.Reader) (manifest, []byte, error) {
	var m manifest
	bad := func(msg string) (manifest, []byte, error) {
		return m, nil, newError(CodeCorrupt, "archive: "+msg, nil)
	}
	tr := tar.NewReader(r)
	var files [2][]byte
	for i, name := range []string{"manifest.json", "snapshot.ltx"} {
		limit := int64(maxHeadBytes)
		if i == 1 {
			limit = n.cfg.Limits.EntityBytes + n.cfg.Limits.EntityBytes/head.PageSize*64 + 1<<20
		}
		hdr, err := tr.Next()
		if err != nil || hdr.Name != name || hdr.Typeflag != tar.TypeReg || hdr.Size < 0 || hdr.Size > limit {
			return bad("expected the regular file " + name)
		}
		if files[i], err = io.ReadAll(io.LimitReader(tr, hdr.Size)); err != nil || int64(len(files[i])) != hdr.Size {
			return bad("truncated " + name)
		}
	}
	if _, err := tr.Next(); err != io.EOF {
		return bad("entries after snapshot.ltx")
	}
	rest, err := io.ReadAll(io.LimitReader(r, 1<<20+1))
	if err != nil || len(rest) > 1<<20 || bytes.ContainsFunc(rest, func(c rune) bool { return c != 0 }) {
		return bad("data after the end of the archive")
	}
	d := json.NewDecoder(bytes.NewReader(files[0]))
	d.DisallowUnknownFields()
	if err := d.Decode(&m); err != nil {
		return bad("manifest.json does not decode")
	}
	canon, _ := json.Marshal(m)
	switch {
	case !bytes.Equal(canon, files[0]):
		return bad("manifest.json is not canonical")
	case m.Format != head.Format || m.Codec != head.Codec || m.PageSize != head.PageSize || m.Namespace == head.ID{} ||
		m.Incarnation == head.ID{} || head.ValidKey(m.Key) != nil || m.Sequence == 0 || m.Sequence >= head.MaxInt ||
		m.Schema == 0 || m.PageCount == 0 || m.PageCount > n.maxPages() || m.DatabaseChecksum&head.ChecksumFlag == 0:
		return bad("unsupported or invalid manifest")
	case uint64(len(files[1])) != m.Bytes || sha256.Sum256(files[1]) != m.SHA256:
		return bad("snapshot.ltx differs from the manifest")
	}
	return m, files[1], nil
}
