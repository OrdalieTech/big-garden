package garden

import (
	"bytes"
	"crypto/rand"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/OrdalieTech/big-garden/internal/head"
	"github.com/OrdalieTech/big-garden/internal/store"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

func keyedNode(t *testing.T, st store.Store, id string, key []byte) (*Node, error) {
	t.Helper()
	cfg := Config{S3: s3.New(s3.Options{}), Bucket: "b", Prefix: "p", NodeID: id, CacheDir: t.TempDir(), Key: key,
		Limits: Limits{EntityBytes: 8 << 20, StorageAttemptTimeout: 200 * time.Millisecond, CommitTimeout: time.Second}}
	cfg, err := cfg.validate()
	if err != nil {
		return nil, err
	}
	n, err := open(bg, cfg, st)
	if err == nil {
		t.Cleanup(func() { n.Close(bg) })
	}
	return n, err
}

// everything lists every stored object's body.
func everything(t *testing.T, st store.Store) map[string][]byte {
	t.Helper()
	all := map[string][]byte{}
	for after := ""; ; {
		page, more, err := st.List(bg, "", after, 1000)
		if err != nil {
			t.Fatal(err)
		}
		for _, e := range page {
			o, err := st.Get(bg, e.Key, "", 1<<30)
			if err != nil {
				t.Fatal(err)
			}
			all[e.Key], after = o.Body, e.Key
		}
		if !more || len(page) == 0 {
			return all
		}
	}
}

// TestSealedNamespace runs an entity through rows, files, maintenance, a
// cold activation on another Node, export and import in a sealed namespace:
// it all reads back, no stored byte holds the plaintext, no head carries a
// tail, and a Node with another key, or none, is refused at Open.
func TestSealedNamespace(t *testing.T) {
	st := newStore(t)
	key := make([]byte, 32)
	rand.Read(key)
	a, err := keyedNode(t, st, "a", key)
	if err != nil {
		t.Fatal(err)
	}
	marker := "secret-marker-" + strings.Repeat("Z", 40)
	ref, _, err := a.Create(bg, "doc", newReq(t, "create", 1), createKV)
	if err != nil {
		t.Fatal(err)
	}
	file := bytes.Repeat([]byte(marker), 70000) // 3.8 MiB: four sealed segments
	b, err := a.PutBlob(bg, ref, bytes.NewReader(file))
	if err != nil {
		t.Fatal(err)
	}
	for i := range 40 {
		if _, err := a.Update(bg, ref, newReq(t, "row"+string(rune('a'+i%26))+string(rune('a'+i/26)), 1), func(tx *Tx) ([]byte, error) {
			_, err := tx.Exec(bg, "INSERT OR REPLACE INTO kv VALUES (?, ?)", "k", strings.Repeat(marker, 200))
			if err == nil && i == 0 {
				err = tx.SetFile("f", &b)
			}
			return nil, err
		}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := a.Snapshot(bg, ref); err != nil {
		t.Fatal(err)
	}
	for k, body := range everything(t, st) {
		if bytes.Contains(body, []byte(marker)) {
			t.Fatalf("%s holds the plaintext", k)
		}
		if strings.HasSuffix(k, "/head.json") {
			if h, err := head.Decode(body); err != nil || h.Tail != nil {
				t.Fatalf("%s: a sealed namespace's head carries a tail (%v)", k, err)
			}
		}
	}
	c, err := keyedNode(t, st, "c", key)
	if err != nil {
		t.Fatal(err)
	}
	if got := readAll(t, c, ref); got["k"] != strings.Repeat(marker, 200) {
		t.Fatal("the rows do not read back on another Node")
	}
	f, _ := fileOf(t, c, ref, "f")
	if got, err := readBlob(t, c, ref, f, 1<<20-7, 100); err != nil || !bytes.Equal(got, file[1<<20-7:1<<20+93]) {
		t.Fatalf("a range of the file: %v (%v)", err, errors.Unwrap(err))
	}
	var archive bytes.Buffer
	if _, err := c.Export(bg, ref, &archive); err != nil {
		t.Fatal(err)
	}
	dst, _, err := c.Import(bg, "copy", newReq(t, "import", 1), bytes.NewReader(archive.Bytes()))
	if err != nil {
		t.Fatal(err)
	}
	g, _ := fileOf(t, a, dst, "f")
	if got, err := readBlob(t, a, dst, g, 0, g.Size); err != nil || !bytes.Equal(got, file) {
		t.Fatalf("the imported file: %v", err)
	}
	for name, k := range map[string][]byte{"no key": nil, "another key": bytes.Repeat([]byte{1}, 32)} {
		_, err := keyedNode(t, st, "x", k)
		var e *Error
		if !errors.As(err, &e) || e.Code != CodeInvalidArgument {
			t.Fatalf("%s: Open gave %v", name, err)
		}
	}
	plain := newStore(t)
	if _, err := keyedNode(t, plain, "p", nil); err != nil {
		t.Fatal(err)
	}
	if _, err := keyedNode(t, plain, "q", key); err == nil {
		t.Fatal("a key opened a plaintext namespace")
	}
}
