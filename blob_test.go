package garden

import (
	"archive/tar"
	"bytes"
	"context"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/OrdalieTech/big-garden/internal/head"
	"github.com/OrdalieTech/big-garden/internal/storetest"
)

func blobKeys(t *testing.T, mem *storetest.Mem, ref Ref) []string {
	t.Helper()
	page, _, err := mem.List(bg, head.BlobPrefix(ref.Key, ref.Incarnation), "", 1000)
	if err != nil {
		t.Fatal(err)
	}
	var keys []string
	for _, e := range page {
		keys = append(keys, e.Key)
	}
	return keys
}

func fileOf(t *testing.T, n *Node, ref Ref, name string) (Blob, bool) {
	t.Helper()
	var b Blob
	var ok bool
	_, err := n.Read(bg, ref, 1, func(v *View) (_ []byte, err error) { b, ok, err = v.File(bg, name); return nil, err })
	if err != nil {
		t.Fatal(err)
	}
	return b, ok
}

func readBlob(t *testing.T, n *Node, ref Ref, b Blob, off, length int64) ([]byte, error) {
	t.Helper()
	rc, err := n.OpenBlob(bg, ref, b)
	if err != nil {
		return nil, err
	}
	defer rc.Close()
	p := make([]byte, length)
	k, err := rc.(io.ReaderAt).ReadAt(p, off)
	if err == io.EOF && int64(k) == length {
		err = nil
	}
	return p[:k], err
}

func setFile(name string, b *Blob) func(*Tx) ([]byte, error) {
	return func(tx *Tx) ([]byte, error) { return nil, tx.SetFile(name, b) }
}

func TestFiles(t *testing.T) {
	mem := storetest.NewMem()
	a, b := testNode(t, mem, "a", nil), testNode(t, mem, "b", nil)
	ref, _, err := a.Create(bg, "doc", newReq(t, "create", 1), createKV)
	if err != nil {
		t.Fatal(err)
	}
	pdf := random(3<<20 + 12345)
	blob, err := a.PutBlob(bg, ref, bytes.NewReader(pdf))
	if err != nil {
		t.Fatal(err)
	}
	small, err := a.PutBlob(bg, ref, bytes.NewReader([]byte("a note")))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := a.Update(bg, ref, newReq(t, "attach", 1), func(tx *Tx) ([]byte, error) {
		if _, err := tx.Exec(bg, "INSERT INTO kv VALUES ('title', 'Contract')"); err != nil {
			return nil, err
		}
		return nil, errors.Join(tx.SetFile("contract.pdf", &blob), tx.SetFile("copy.pdf", &blob), tx.SetFile("note.txt", &small))
	}); err != nil {
		t.Fatal(err)
	}
	if got := blobKeys(t, mem, ref); len(got) != 1 {
		t.Fatalf("blob objects %v, want the PDF once (two names, the note inline)", got)
	}
	if err := a.sweep(bg, ref); err != nil { // what maintenance runs after the commit
		t.Fatal(err)
	}

	// Any node reads it, whole or by range, each chunk verified.
	got, ok := fileOf(t, b, ref, "contract.pdf")
	if !ok || got.Size != int64(len(pdf)) || got.SHA256 != blob.SHA256 {
		t.Fatalf("file %+v %v", got, ok)
	}
	rc, err := b.OpenBlob(bg, ref, got)
	if err != nil {
		t.Fatal(err)
	}
	if whole, err := io.ReadAll(rc); err != nil || !bytes.Equal(whole, pdf) {
		t.Fatalf("whole read: %v", err)
	}
	rc.Close()
	off := int64(1<<20 - 10)
	if part, err := readBlob(t, b, ref, got, off, 100); err != nil || !bytes.Equal(part, pdf[off:off+100]) {
		t.Fatalf("range read across a chunk boundary: %v", err)
	}
	note, _ := fileOf(t, b, ref, "note.txt")
	if s, err := readBlob(t, b, ref, note, 2, 4); err != nil || string(s) != "note" {
		t.Fatalf("inline read %q %v", s, err)
	}

	// http.ServeContent serves ranges straight from the reader.
	rc, _ = b.OpenBlob(bg, ref, got)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.ServeContent(w, r, "contract.pdf", time.Time{}, rc)
	}))
	req, _ := http.NewRequest("GET", srv.URL, nil)
	req.Header.Set("Range", "bytes=2097150-2097159")
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(res.Body)
	res.Body.Close()
	srv.Close()
	rc.Close()
	if res.StatusCode != http.StatusPartialContent || !bytes.Equal(body, pdf[2097150:2097160]) {
		t.Fatalf("ServeContent: %d %d bytes", res.StatusCode, len(body))
	}

	// A chunk read once is served from the chunk cache, and a damaged cached chunk is refetched.
	gets := len(mem.Journal())
	if _, err := readBlob(t, b, ref, got, 0, 10); err != nil {
		t.Fatal(err)
	}
	if len(mem.Journal()) != gets {
		t.Fatal("a cached chunk went to storage")
	}
	sum := got.f.Chunks[:32]
	os.WriteFile(filepath.Join(b.chunks.dir, hex.EncodeToString(sum)), []byte("damaged"), 0o600)
	if part, err := readBlob(t, b, ref, got, 0, 10); err != nil || !bytes.Equal(part, pdf[:10]) {
		t.Fatalf("damaged cache entry: %v", err)
	}

	// Application SQL joins the files table but cannot write it.
	_, err = b.Read(bg, ref, 1, func(v *View) ([]byte, error) {
		return nil, v.Query(bg, "SELECT k.v, f.size FROM kv k, _state_blobs f WHERE f.name = 'contract.pdf'", nil, func(r *sql.Rows) error {
			var title string
			var size int64
			if !r.Next() {
				t.Fatal("no join row")
			}
			if err := r.Scan(&title, &size); err != nil || title != "Contract" || size != int64(len(pdf)) {
				t.Fatalf("join %q %d %v", title, size, err)
			}
			return nil
		})
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = a.Update(bg, ref, newReq(t, "forge", 1), func(tx *Tx) ([]byte, error) {
		_, err := tx.Exec(bg, "UPDATE _state_blobs SET size = 1")
		return nil, err
	})
	if err == nil {
		t.Fatal("application SQL wrote _state_blobs")
	}

	// Same bytes again: no upload.
	again, err := a.PutBlob(bg, ref, bytes.NewReader(pdf))
	if err != nil || again.f.Object != blob.f.Object {
		t.Fatalf("dedup: %v %q %q", err, again.f.Object, blob.f.Object)
	}

	// Removing one of two names keeps the blob; replacing the other makes it
	// dead; an unlinked upload dies once its window passes.
	if _, err := a.Update(bg, ref, newReq(t, "unname", 1), setFile("copy.pdf", nil)); err != nil {
		t.Fatal(err)
	}
	pdf2 := append(bytes.Clone(pdf), 1)
	blob2, err := a.PutBlob(bg, ref, bytes.NewReader(pdf2))
	if err != nil {
		t.Fatal(err)
	}
	orphan, err := a.PutBlob(bg, ref, bytes.NewReader(pdf[:2<<20]))
	if err != nil {
		t.Fatal(err)
	}
	if err := a.sweep(bg, ref); err != nil {
		t.Fatal(err)
	}
	if got := blobKeys(t, mem, ref); len(got) != 3 {
		t.Fatalf("after unnaming one of two names: %v", got)
	}
	if _, err := a.Update(bg, ref, newReq(t, "replace", 1), setFile("contract.pdf", &blob2)); err != nil {
		t.Fatal(err)
	}
	if err := a.sweep(bg, ref); err != nil {
		t.Fatal(err)
	}
	if got := blobKeys(t, mem, ref); len(got) != 2 { // blob2, and orphan still fresh
		t.Fatalf("after replace: %v", got)
	}
	if _, err := readBlob(t, a, ref, blob, 0, 10); err == nil {
		t.Fatal("read a collected blob") // a's cache never held it: a's reads went through b
	} else {
		wantCode(t, err, CodeNotFound)
	}
	a.now = func() time.Time { return time.Now().Add(blobLinkWindow + time.Minute) }
	if _, err := a.Update(bg, ref, newReq(t, "late", 1), setFile("late.pdf", &orphan)); err == nil {
		t.Fatal("linked a blob past its window")
	} else {
		wantCode(t, err, CodeInvalidArgument)
	}
	if err := a.sweep(bg, ref); err != nil {
		t.Fatal(err)
	}
	if got := blobKeys(t, mem, ref); len(got) != 1 || got[0] != head.BlobPrefix(ref.Key, ref.Incarnation)+blob2.f.Object {
		t.Fatalf("after window: %v", got)
	}
	a.now = time.Now

	// Corrupt bytes are refused, not served.
	key := head.BlobPrefix(ref.Key, ref.Incarnation) + blob2.f.Object
	o, _ := mem.Peek(key)
	bad := bytes.Clone(o.Body)
	bad[len(bad)-1] ^= 1
	mem.Delete(bg, key)
	mem.PutImmutable(bg, key, bad)
	cur, _ := fileOf(t, b, ref, "contract.pdf")
	if _, err := readBlob(t, b, ref, cur, cur.Size-10, 10); err == nil {
		t.Fatal("served a corrupt chunk")
	} else {
		wantCode(t, err, CodeCorrupt)
	}

	// The tombstone takes the blobs with it.
	if err := a.Delete(bg, ref); err != nil {
		t.Fatal(err)
	}
	if err := a.Collect(bg, ref); err != nil {
		t.Fatal(err)
	}
	if got := blobKeys(t, mem, ref); len(got) != 0 {
		t.Fatalf("after delete: %v", got)
	}
}

// A file of one entity enters another as a server-side copy.
func TestFilesCopy(t *testing.T) {
	mem := storetest.NewMem()
	cs := &copyStore{Mem: mem}
	n := testNode(t, cs, "a", nil)
	src, _, err := n.Create(bg, "template", newReq(t, "t", 1), createKV)
	if err != nil {
		t.Fatal(err)
	}
	dst, _, err := n.Create(bg, "envelope", newReq(t, "e", 1), createKV)
	if err != nil {
		t.Fatal(err)
	}
	data := random(2<<20 + 7)
	b, err := n.PutBlob(bg, src, bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := n.Update(bg, src, newReq(t, "link", 1), setFile("form.pdf", &b)); err != nil {
		t.Fatal(err)
	}
	f, _ := fileOf(t, n, src, "form.pdf")
	rc, _ := n.OpenBlob(bg, src, f)
	c, err := n.PutBlob(bg, dst, rc)
	if err != nil || cs.copies != 1 {
		t.Fatalf("copy: %v, %d server-side copies", err, cs.copies)
	}
	if _, err := n.Update(bg, dst, newReq(t, "link", 1), setFile("form.pdf", &c)); err != nil {
		t.Fatal(err)
	}
	g, _ := fileOf(t, n, dst, "form.pdf")
	if got, err := readBlob(t, n, dst, g, 0, g.Size); err != nil || !bytes.Equal(got, data) || len(blobKeys(t, mem, dst)) != 1 {
		t.Fatalf("copy: %v", err)
	}
}

type copyStore struct {
	*storetest.Mem
	copies int
}

func (c *copyStore) Copy(ctx context.Context, from, to string) error {
	c.copies++
	return c.Mem.Copy(ctx, from, to)
}

// An upload of an earlier epoch can never be linked, and the new owner's
// sweep collects it.
func TestFilesEpochFence(t *testing.T) {
	mem := storetest.NewMem()
	a, b := testNode(t, mem, "a", nil), testNode(t, mem, "b", nil)
	ref, _, err := a.Create(bg, "doc", newReq(t, "create", 1), createKV)
	if err != nil {
		t.Fatal(err)
	}
	old, err := a.PutBlob(bg, ref, bytes.NewReader(random(200<<10)))
	if err != nil {
		t.Fatal(err)
	}
	route, err := b.Resolve(bg, ref.Key)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := b.Takeover(bg, route); err != nil {
		t.Fatal(err)
	}
	if _, err := a.Update(bg, ref, newReq(t, "stale", 1), setFile("x", &old)); err == nil {
		t.Fatal("the old owner linked a blob")
	}
	if _, err := b.Update(bg, ref, newReq(t, "steal", 1), setFile("x", &old)); err == nil {
		t.Fatal("the new owner linked another epoch's blob")
	}
	if err := b.sweep(bg, ref); err != nil {
		t.Fatal(err)
	}
	if got := blobKeys(t, mem, ref); len(got) != 0 {
		t.Fatalf("old epoch blob survived: %v", got)
	}
	if err := a.sweep(bg, ref); err == nil {
		t.Fatal("a non-owner swept")
	}
}

// Export carries the files and Import recreates them under the new key;
// an archive whose blobs differ from its image is refused.
func TestFilesExportImport(t *testing.T) {
	mem := storetest.NewMem()
	a, b := testNode(t, mem, "a", nil), testNode(t, mem, "b", nil)
	ref, _, err := a.Create(bg, "doc", newReq(t, "create", 1), createKV)
	if err != nil {
		t.Fatal(err)
	}
	big, mid := random(2<<20+5), random(100<<10)
	var blobs []Blob
	for _, data := range [][]byte{big, mid, []byte("inline")} {
		bl, err := a.PutBlob(bg, ref, bytes.NewReader(data))
		if err != nil {
			t.Fatal(err)
		}
		blobs = append(blobs, bl)
	}
	if _, err := a.Update(bg, ref, newReq(t, "attach", 1), func(tx *Tx) ([]byte, error) {
		return nil, errors.Join(tx.SetFile("big", &blobs[0]), tx.SetFile("big-again", &blobs[0]), tx.SetFile("mid", &blobs[1]), tx.SetFile("note", &blobs[2]))
	}); err != nil {
		t.Fatal(err)
	}
	var archive bytes.Buffer
	if _, err := b.Export(bg, ref, &archive); err != nil {
		t.Fatal(err)
	}
	dst, _, err := b.Import(bg, "copy", newReq(t, "import", 1), bytes.NewReader(archive.Bytes()))
	if err != nil {
		t.Fatal(err)
	}
	if got := blobKeys(t, mem, dst); len(got) != 2 {
		t.Fatalf("imported blob objects %v, want 2", got)
	}
	if err := b.sweep(bg, dst); err != nil {
		t.Fatal(err)
	}
	for name, want := range map[string][]byte{"big": big, "big-again": big, "mid": mid, "note": []byte("inline")} {
		f, ok := fileOf(t, a, dst, name)
		if got, err := readBlob(t, a, dst, f, 0, f.Size); !ok || err != nil || !bytes.Equal(got, want) {
			t.Fatalf("imported %s: %v %v", name, ok, err)
		}
	}

	// A flipped byte in an archived blob, or a missing blob, is refused.
	raw := archive.Bytes()
	at := bytes.Index(raw, mid[:64])
	bad := bytes.Clone(raw)
	bad[at+10] ^= 1
	_, _, err = b.Import(bg, "bad", newReq(t, "bad", 1), bytes.NewReader(bad))
	wantCode(t, err, CodeCorrupt)
	tr, short := tar.NewReader(bytes.NewReader(raw)), &bytes.Buffer{}
	tw := tar.NewWriter(short)
	for {
		hdr, err := tr.Next()
		if err != nil {
			break
		}
		if strings.HasPrefix(hdr.Name, "blobs/") && hdr.Size == int64(len(mid)) {
			continue
		}
		tw.WriteHeader(hdr)
		io.Copy(tw, tr)
	}
	tw.Close()
	_, _, err = b.Import(bg, "short", newReq(t, "short", 1), bytes.NewReader(short.Bytes()))
	wantCode(t, err, CodeCorrupt)
}

// A callback copies a file under another name without moving bytes, and
// the copy outlives the original name.
func TestFilesRename(t *testing.T) {
	mem := storetest.NewMem()
	n := testNode(t, mem, "a", nil)
	ref, _, err := n.Create(bg, "doc", newReq(t, "create", 1), createKV)
	if err != nil {
		t.Fatal(err)
	}
	data := random(300 << 10)
	b, err := n.PutBlob(bg, ref, bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := n.Update(bg, ref, newReq(t, "current", 1), setFile("crdt/current", &b)); err != nil {
		t.Fatal(err)
	}
	if _, err := n.Update(bg, ref, newReq(t, "checkpoint", 1), func(tx *Tx) ([]byte, error) {
		f, ok, err := tx.File(bg, "crdt/current")
		if err != nil || !ok {
			return nil, fmt.Errorf("file: %v %v", ok, err)
		}
		return nil, errors.Join(tx.SetFile("crdt/trail/1", &f), tx.SetFile("crdt/current", nil))
	}); err != nil {
		t.Fatal(err)
	}
	n.now = func() time.Time { return time.Now().Add(blobLinkWindow + time.Minute) } // only the image protects it now
	if err := n.sweep(bg, ref); err != nil {
		t.Fatal(err)
	}
	f, ok := fileOf(t, n, ref, "crdt/trail/1")
	if got, err := readBlob(t, n, ref, f, 0, f.Size); !ok || err != nil || !bytes.Equal(got, data) || len(blobKeys(t, mem, ref)) != 1 {
		t.Fatalf("renamed file: %v %v", ok, err)
	}
	if _, ok := fileOf(t, n, ref, "crdt/current"); ok {
		t.Fatal("the old name survived")
	}
}
