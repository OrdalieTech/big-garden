package garden

import (
	"archive/tar"
	"bytes"
	"context"
	"encoding/json"
	"io"
	"time"

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
