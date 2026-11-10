// Package kit holds tools built on the public API of package garden only:
// nothing here is needed to use an entity, and the core needs nothing from
// it.
package kit

import (
	"archive/tar"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"time"

	garden "github.com/OrdalieTech/big-garden"
)

// Backup writes every live entity of n's namespace to w, as a tar of one
// Export archive per entity named by the hex of its key. Each member is a
// validated state of its entity; the entities are not a single cut (there is
// no cross-entity atomicity). It runs on any Node. It returns the number of
// entities written; on error the output is not a backup.
func Backup(ctx context.Context, n *garden.Node, w io.Writer) (int, error) {
	tw := tar.NewWriter(w)
	count := 0
	for route, err := range n.Scan(ctx, "") {
		if err != nil {
			return count, err
		}
		var b bytes.Buffer
		if _, err := n.Export(ctx, route.Ref, &b); err != nil {
			var ge *garden.Error
			if errors.As(err, &ge) && ge.Code == garden.CodeNotFound { // deleted since the scan
				continue
			}
			return count, err
		}
		hdr := &tar.Header{Name: hex.EncodeToString([]byte(route.Ref.Key)) + ".tar", Typeflag: tar.TypeReg, Mode: 0o600,
			Size: int64(b.Len()), ModTime: time.Unix(0, 0), Format: tar.FormatPAX}
		if err := tw.WriteHeader(hdr); err != nil {
			return count, err
		}
		if _, err := tw.Write(b.Bytes()); err != nil {
			return count, err
		}
		count++
	}
	return count, tw.Close()
}

// Restore imports every archive of a Backup into n, which should be a fresh
// namespace (another prefix: PLAN §10.3), under the same keys. A key that
// already exists there is skipped, so a Restore retried after a failure
// goes on where it stopped. It returns the number of entities imported.
func Restore(ctx context.Context, n *garden.Node, r io.Reader) (int, error) {
	tr := tar.NewReader(r)
	count := 0
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			return count, nil
		}
		if err != nil {
			return count, err
		}
		name, ok := strings.CutSuffix(hdr.Name, ".tar")
		key, err := hex.DecodeString(name)
		if !ok || err != nil {
			return count, errors.New("kit: not a Backup member: " + hdr.Name)
		}
		archive, err := io.ReadAll(tr)
		if err != nil {
			return count, err
		}
		var ge *garden.Error
		if _, err := n.Resolve(ctx, string(key)); err == nil {
			continue // imported by an earlier attempt
		} else if !errors.As(err, &ge) || ge.Code != garden.CodeNotFound {
			return count, err
		}
		req := garden.Request{Hash: sha256.Sum256(archive)}
		if req.Schema, err = archiveSchema(archive); err != nil {
			return count, err
		}
		if req.ID, err = garden.NewMutationID(time.Now()); err != nil {
			return count, err
		}
		if _, _, err = n.Import(ctx, string(key), req, bytes.NewReader(archive)); err != nil {
			return count, err
		}
		count++
	}
}

// archiveSchema reads the schema from an Export archive's manifest.json
// (PROTOCOL §11), which Import checks against the request.
func archiveSchema(archive []byte) (uint32, error) {
	tr := tar.NewReader(bytes.NewReader(archive))
	hdr, err := tr.Next()
	if err != nil || hdr.Name != "manifest.json" {
		return 0, errors.New("kit: an Export archive starts with manifest.json")
	}
	var m struct{ Schema uint32 }
	return m.Schema, json.NewDecoder(tr).Decode(&m)
}
