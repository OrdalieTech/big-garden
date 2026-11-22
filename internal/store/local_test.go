package store

import (
	"bytes"
	"context"
	"crypto/sha256"
	"io"
	"testing"
)

func TestLocalStore(t *testing.T) {
	ctx := context.Background()
	st, err := NewLocal(t.TempDir(), "ns")
	if err != nil {
		t.Fatal(err)
	}
	// Missing key
	if _, err := st.Get(ctx, "a/b", "", 1024); OutcomeOf(err) != NotFound {
		t.Fatalf("missing Get: %v", err)
	}
	// PutCAS create + conflict + replace
	etag1, err := st.PutCAS(ctx, "a/b", []byte("v1"), "")
	if err != nil || etag1 == "" {
		t.Fatalf("PutCAS create: %q %v", etag1, err)
	}
	if _, err := st.PutCAS(ctx, "a/b", []byte("v2"), ""); OutcomeOf(err) != PreconditionFailed {
		t.Fatalf("duplicate create: %v", err)
	}
	if _, err := st.Get(ctx, "a/b", etag1, 1024); OutcomeOf(err) != NotModified {
		t.Fatalf("IfNoneMatch: %v", err)
	}
	etag2, err := st.PutCAS(ctx, "a/b", []byte("v2"), etag1)
	if err != nil || etag2 == etag1 {
		t.Fatalf("PutCAS replace: %q %v", etag2, err)
	}
	obj, err := st.Get(ctx, "a/b", "", 1024)
	if err != nil || string(obj.Body) != "v2" || obj.ETag != etag2 {
		t.Fatalf("Get: %+v %v", obj, err)
	}
	// PutImmutable + PutFile + Open + Copy + List + Delete
	if err := st.PutImmutable(ctx, "a/imm", []byte("one")); err != nil {
		t.Fatal(err)
	}
	if err := st.PutImmutable(ctx, "a/imm", []byte("two")); OutcomeOf(err) != PreconditionFailed {
		t.Fatalf("PutImmutable duplicate: %v", err)
	}
	data := []byte("hello streaming local store")
	sum := sha256.Sum256(data)
	if err := st.PutFile(ctx, "a/file", bytes.NewReader(data), int64(len(data)), sum); err != nil {
		t.Fatal(err)
	}
	rc, err := st.Open(ctx, "a/file", 6, 9)
	if err != nil {
		t.Fatal(err)
	}
	got, _ := io.ReadAll(rc)
	rc.Close()
	if string(got) != "streaming" {
		t.Fatalf("Open slice = %q", got)
	}
	if err := st.Copy(ctx, "a/file", "a/copy"); err != nil {
		t.Fatal(err)
	}
	page, more, err := st.List(ctx, "a/", "", 10)
	if err != nil || more || len(page) != 4 {
		t.Fatalf("List: len=%d more=%v err=%v", len(page), more, err)
	}
	if err := st.Delete(ctx, "a/copy"); err != nil {
		t.Fatal(err)
	}
	if err := st.Delete(ctx, "a/copy"); err != nil {
		t.Fatalf("delete absent: %v", err)
	}
}
