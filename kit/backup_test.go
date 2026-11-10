package kit

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"fmt"
	"testing"
	"time"

	garden "github.com/OrdalieTech/big-garden"
	"github.com/OrdalieTech/big-garden/internal/storetest"
)

var bg = context.Background()

func open(t *testing.T, ep storetest.Endpoint, id string) *garden.Node {
	t.Helper()
	n, err := garden.Open(bg, garden.Config{S3: ep.Client(nil), Bucket: ep.Bucket, Prefix: ep.Prefix, NodeID: id, CacheDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { n.Close(bg) })
	return n
}

func req(t *testing.T, s string) garden.Request {
	id, err := garden.NewMutationID(time.Now())
	if err != nil {
		t.Fatal(err)
	}
	return garden.Request{ID: id, Hash: sha256.Sum256([]byte(s)), Schema: 1}
}

func value(t *testing.T, n *garden.Node, ref garden.Ref) string {
	t.Helper()
	var v string
	if _, err := n.Read(bg, ref, 1, func(view *garden.View) ([]byte, error) {
		return nil, view.Query(bg, "SELECT v FROM kv WHERE k = 'a'", nil, func(r *sql.Rows) error { r.Next(); return r.Scan(&v) })
	}); err != nil {
		t.Fatalf("read %s: %v", ref.Key, err)
	}
	return v
}

// TestBackupRestoreDrill: a namespace backed up and restored into another
// prefix holds the same live entities with the same state; a deleted entity
// is not restored, and a second Restore imports nothing.
func TestBackupRestoreDrill(t *testing.T) {
	base := storetest.Local(t)
	src := open(t, base.Sub("src"), "a")
	refs := map[string]garden.Ref{}
	for i := range 20 {
		key := fmt.Sprint("e", i)
		ref, _, err := src.Create(bg, key, req(t, key), func(tx *garden.Tx) ([]byte, error) {
			_, err := tx.Exec(bg, "CREATE TABLE kv(k TEXT PRIMARY KEY, v TEXT)")
			return nil, err
		})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := src.Update(bg, ref, req(t, key+"v"), func(tx *garden.Tx) ([]byte, error) {
			_, err := tx.Exec(bg, "INSERT INTO kv VALUES ('a', ?)", key+"-state")
			return nil, err
		}); err != nil {
			t.Fatal(err)
		}
		refs[key] = ref
	}
	if err := src.Delete(bg, refs["e7"]); err != nil {
		t.Fatal(err)
	}
	var b bytes.Buffer
	if n, err := Backup(bg, src, &b); err != nil || n != 19 {
		t.Fatalf("Backup: %d entities, %v", n, err)
	}
	dst := open(t, base.Sub("dst"), "b")
	if n, err := Restore(bg, dst, bytes.NewReader(b.Bytes())); err != nil || n != 19 {
		t.Fatalf("Restore: %d entities, %v", n, err)
	}
	if n, err := Restore(bg, dst, bytes.NewReader(b.Bytes())); err != nil || n != 0 {
		t.Fatalf("second Restore: %d entities, %v", n, err)
	}
	for key := range refs {
		route, err := dst.Resolve(bg, key)
		if key == "e7" {
			if err == nil {
				t.Fatal("the deleted entity was restored")
			}
			continue
		}
		if err != nil {
			t.Fatal(err)
		}
		if v := value(t, dst, route.Ref); v != key+"-state" {
			t.Fatalf("%s restored as %q", key, v)
		}
	}
}
