package garden

import (
	"bytes"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/OrdalieTech/big-garden/internal/head"
	"github.com/OrdalieTech/big-garden/internal/storetest"
)

var updateFormat1 = flag.Bool("update-format1", false, "rewrite testdata/format1 (only when format 1 itself changes, which it must not)")

// format1Rows is the state the frozen entity and archive hold.
func format1Rows() map[string]string {
	rows := map[string]string{}
	for i := range 6 {
		rows[fmt.Sprint("k", i)] = strings.Repeat(fmt.Sprint(i), 9000) // past the inline tail: uploaded deltas
	}
	delete(rows, "k3")
	return rows
}

// TestFormat1Frozen: the stored objects and the export archive written by
// v0.1.0 (testdata/format1, never regenerated) still activate, read and
// import with this binary. A change that breaks them breaks every deployed
// namespace and every backup.
func TestFormat1Frozen(t *testing.T) {
	dir := filepath.Join("testdata", "format1")
	if *updateFormat1 {
		mem := storetest.NewMem()
		n := testNode(t, mem, "a", nil)
		ref, _, err := n.Create(bg, "frozen", newReq(t, "create", 1), createKV)
		if err != nil {
			t.Fatal(err)
		}
		for i := range 6 {
			if _, err := n.Update(bg, ref, newReq(t, fmt.Sprint("w", i), 1), put(fmt.Sprint("k", i), strings.Repeat(fmt.Sprint(i), 9000))); err != nil {
				t.Fatal(err)
			}
		}
		if _, err := n.Update(bg, ref, newReq(t, "del", 1), func(tx *Tx) ([]byte, error) {
			_, err := tx.Exec(bg, "DELETE FROM kv WHERE k = 'k3'")
			return nil, err
		}); err != nil {
			t.Fatal(err)
		}
		n.wg.Wait()
		os.RemoveAll(dir)
		page, _, _ := mem.List(bg, "", "", 1000)
		for _, o := range page {
			if strings.HasPrefix(o.Key, "probe/") {
				continue
			}
			obj, _ := mem.Peek(o.Key)
			path := filepath.Join(dir, "store", filepath.FromSlash(o.Key))
			os.MkdirAll(filepath.Dir(path), 0o755)
			os.WriteFile(path, obj.Body, 0o644)
		}
		var b bytes.Buffer
		if _, err := n.Export(bg, ref, &b); err != nil {
			t.Fatal(err)
		}
		os.WriteFile(filepath.Join(dir, "export.tar"), b.Bytes(), 0o644)
	}
	mem := storetest.NewMem()
	root := filepath.Join(dir, "store")
	err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		body, err := os.ReadFile(path)
		key, _ := filepath.Rel(root, path)
		if err == nil {
			_, err = mem.PutCAS(bg, filepath.ToSlash(key), body, "")
		}
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	n := testNode(t, mem, "b", nil)
	route, err := n.Resolve(bg, "frozen")
	if err != nil {
		t.Fatal(err)
	}
	want := format1Rows()
	check := func(what string, ref Ref) {
		got := readAll(t, n, ref)
		if len(got) != len(want) {
			t.Fatalf("%s: %d rows, want %d", what, len(got), len(want))
		}
		for k, v := range want {
			if got[k] != v {
				t.Fatalf("%s: row %s differs", what, k)
			}
		}
	}
	check("frozen entity", route.Ref)
	archive, err := os.ReadFile(filepath.Join(dir, "export.tar"))
	if err != nil {
		t.Fatal(err)
	}
	ref, _, err := n.Import(bg, "imported", newReq(t, "import", 1), bytes.NewReader(archive))
	if err != nil {
		t.Fatal(err)
	}
	check("imported archive", ref)
	if _, err := head.Decode(mustRead(t, filepath.Join(root, filepath.FromSlash(head.HeadKey("frozen"))))); err != nil {
		t.Fatal(err)
	}
}

func mustRead(t *testing.T, path string) []byte {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return b
}
