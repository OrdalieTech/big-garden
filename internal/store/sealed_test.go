package store_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"io"
	mrand "math/rand/v2"
	"strconv"
	"testing"

	"github.com/OrdalieTech/big-garden/internal/store"
	"github.com/OrdalieTech/big-garden/internal/storetest"
)

func sealedPair(t *testing.T) (*storetest.Mem, store.Store, []byte) {
	t.Helper()
	key := make([]byte, 32)
	rand.Read(key)
	mem := storetest.NewMem()
	s, err := store.Sealed(mem, key)
	if err != nil {
		t.Fatal(err)
	}
	return mem, s, key
}

func readRange(t *testing.T, s store.Store, key string, off, n int64) ([]byte, error) {
	t.Helper()
	r, err := s.Open(context.Background(), key, off, n)
	if err != nil {
		return nil, err
	}
	defer r.Close()
	return io.ReadAll(r)
}

// TestSealedRoundTrip writes bodies around the segment size through both
// puts, reads them whole and by random ranges, and finds no plaintext stored.
func TestSealedRoundTrip(t *testing.T) {
	mem, s, _ := sealedPair(t)
	r := mrand.New(mrand.NewPCG(1, 2))
	for _, size := range []int{0, 1, store.Segment - 1, store.Segment, store.Segment + 1, 3*store.Segment + 5, store.PartBytes + 7} {
		body := make([]byte, size)
		rand.Read(body)
		for _, put := range []string{"immutable", "file"} {
			key := "entities/x/objects/" + put + "/" + strconv.Itoa(size)
			var err error
			if put == "immutable" {
				err = s.PutImmutable(context.Background(), key, body)
			} else {
				err = s.PutFile(context.Background(), key, bytes.NewReader(body), int64(size), sha256.Sum256(body))
			}
			if err != nil {
				t.Fatalf("%s %d: %v", put, size, err)
			}
			stored, _ := mem.Peek(key)
			if int64(len(stored.Body)) != store.SealedSize(int64(size)) || size >= 64 && bytes.Contains(stored.Body, body[:64]) {
				t.Fatalf("%s %d: stored %d bytes, or plaintext in them", put, size, len(stored.Body))
			}
			if o, err := s.Get(context.Background(), key, "", int64(size)); err != nil || !bytes.Equal(o.Body, body) {
				t.Fatalf("%s %d: get: %v", put, size, err)
			}
			for range 20 {
				off := r.Int64N(int64(size) + 1)
				n := r.Int64N(int64(size)-off+2) + 1
				got, err := readRange(t, s, key, off, n)
				if want := body[off:min(int64(size), off+n)]; err != nil || !bytes.Equal(got, want) {
					t.Fatalf("%s %d: range %d+%d: %d bytes, %v", put, size, off, n, len(got), err)
				}
			}
		}
	}
	// other keys pass through
	if err := s.PutImmutable(context.Background(), "entities/x/head.json", []byte("clear")); err != nil {
		t.Fatal(err)
	}
	if o, _ := mem.Peek("entities/x/head.json"); string(o.Body) != "clear" {
		t.Fatal("a head was sealed")
	}
}

// TestSealedRefusesForgery: a flipped byte, swapped segments, a truncated
// object, an edited length and another key all fail to read.
func TestSealedRefusesForgery(t *testing.T) {
	mem, s, _ := sealedPair(t)
	body := make([]byte, 3*store.Segment)
	rand.Read(body)
	const key = "entities/x/blobs/i/0000000000000001-00000000000000000000000000000001"
	if err := s.PutImmutable(context.Background(), key, body); err != nil {
		t.Fatal(err)
	}
	good, _ := mem.Peek(key)
	forge := func(name string, edit func([]byte) []byte) {
		t.Helper()
		k := key + "-" + name
		mem.PutImmutable(context.Background(), k, edit(bytes.Clone(good.Body)))
		if got, err := readRange(t, s, k, 0, int64(len(body))); err == nil && bytes.Equal(got, body) {
			t.Fatalf("%s: read back as genuine", name)
		} else if err == nil {
			t.Fatalf("%s: read %d bytes without an error", name, len(got))
		}
	}
	const seg = store.Segment + 16
	forge("flip", func(b []byte) []byte { b[len(b)/2] ^= 1; return b })
	forge("swap", func(b []byte) []byte {
		h := 28
		a := bytes.Clone(b[h : h+seg])
		copy(b[h:h+seg], b[h+seg:h+2*seg])
		copy(b[h+seg:h+2*seg], a)
		return b
	})
	forge("truncate", func(b []byte) []byte { return b[:len(b)-100] })
	forge("length", func(b []byte) []byte { b[27]--; return b })
	other, err := store.Sealed(mem, bytes.Repeat([]byte{7}, 32))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := readRange(t, other, key, 0, 10); err == nil {
		t.Fatal("another key read the object")
	}
	if err := s.Copy(context.Background(), key, key+"-copy"); err != nil {
		t.Fatal(err)
	}
	if got, err := readRange(t, s, key+"-copy", store.Segment-5, 10); err != nil || !bytes.Equal(got, body[store.Segment-5:store.Segment+5]) {
		t.Fatalf("a copy does not read back: %v", err)
	}
}

// BenchmarkSealed: sealing and opening 16 MiB against the in-memory store.
func BenchmarkSealed(b *testing.B) {
	key := make([]byte, 32)
	mem := storetest.NewMem()
	s, _ := store.Sealed(mem, key)
	body := make([]byte, 16<<20)
	rand.Read(body)
	b.SetBytes(int64(len(body)))
	for i := 0; b.Loop(); i++ {
		k := "entities/x/objects/b/" + strconv.Itoa(i)
		if err := s.PutFile(context.Background(), k, bytes.NewReader(body), int64(len(body)), sha256.Sum256(body)); err != nil {
			b.Fatal(err)
		}
		r, _ := s.Open(context.Background(), k, 0, int64(len(body)))
		io.Copy(io.Discard, r)
		r.Close()
		mem.Delete(context.Background(), k)
	}
}
