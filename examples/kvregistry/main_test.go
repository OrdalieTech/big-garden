package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"reflect"
	"testing"
	"time"

	garden "github.com/OrdalieTech/big-garden"
)

func keys(hits []Hit) []string {
	ks := []string{}
	for _, h := range hits {
		ks = append(ks, h.Key)
	}
	return ks
}

func TestRegistry(t *testing.T) {
	if os.Getenv("GARDEN_TEST_S3") != "1" {
		t.Skip("set GARDEN_TEST_S3=1 to run against the local S3 fixture")
	}
	for k, v := range map[string]string{"AWS_ACCESS_KEY_ID": "staterustfs", "AWS_SECRET_ACCESS_KEY": "staterustfs-secret"} {
		if os.Getenv(k) == "" {
			t.Setenv(k, v)
		}
	}
	ctx := context.Background()
	b := make([]byte, 6)
	rand.Read(b)
	prefix := "examples/kvregistry-" + hex.EncodeToString(b)
	open := func(node string) *Registry {
		t.Helper()
		r, err := Open(ctx, s3Client(), env("GARDEN_S3_BUCKET", "state-test"), prefix, node, t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		return r
	}
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}

	// check verifies the registry's contents through every read path.
	check := func(r *Registry, tenant string) {
		t.Helper()
		if v, ok, err := r.Get(ctx, tenant, "fruit"); err != nil || !ok || v != "bananas and apples" {
			t.Fatalf("get fruit: %q %v %v", v, ok, err)
		}
		if _, ok, err := r.Get(ctx, tenant, "nothing"); err != nil || ok {
			t.Fatalf("get missing: %v %v", ok, err)
		}
		for q, want := range map[string][]string{
			"apples":          {"fruit"},
			"oranges":         {}, // replaced by the update
			"car OR world":    {"car", "greeting"},
			"fast AND sport*": {"car"},
		} {
			hits, err := r.Search(ctx, tenant, q, 10)
			must(err)
			got := keys(hits)
			if len(want) > 1 { // rank order is not part of the check
				if len(got) == 2 && got[0] > got[1] {
					got[0], got[1] = got[1], got[0]
				}
			}
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("search %q: %v, want %v", q, got, want)
			}
		}
		hits, err := r.Near(ctx, tenant, "apples, bananas", 2)
		must(err)
		if len(hits) != 2 || hits[0].Key != "fruit" || hits[0].Distance > hits[1].Distance {
			t.Fatalf("near: %+v", hits)
		}
		if hits[0].Distance > 0.2 {
			t.Fatalf("near: fruit too far: %+v", hits)
		}
	}

	r1 := open("kv-1")
	must(r1.CreateTenant(ctx, "acme"))
	must(r1.Put(ctx, "acme", "greeting", "hello brave new world"))
	must(r1.Put(ctx, "acme", "fruit", "apples and oranges"))
	must(r1.Put(ctx, "acme", "car", "a red sports car, fast"))
	must(r1.Put(ctx, "acme", "fruit", "bananas and apples"))
	check(r1, "acme")

	// The FTS5 index and the embeddings are pages of the entity, so an
	// exported and imported copy searches the same way.
	var archive bytes.Buffer
	route, err := r1.n.Resolve(ctx, key("acme"))
	must(err)
	_, err = r1.n.Export(ctx, route.Ref, &archive)
	must(err)
	id, err := garden.NewMutationID(time.Now())
	must(err)
	_, _, err = r1.n.Import(ctx, key("acme-copy"), garden.Request{ID: id, Schema: schema,
		Hash: sha256.Sum256([]byte("kv/import/acme-copy"))}, &archive)
	must(err)
	check(r1, "acme-copy")
	must(r1.Close(ctx))

	// Restart as another node with an empty cache dir: everything is
	// restored from S3 alone, after a takeover from kv-1.
	r2 := open("kv-2")
	check(r2, "acme")
	must(r2.Put(ctx, "acme", "boat", "a slow sail boat"))
	if hits, err := r2.Search(ctx, "acme", "boat", 10); err != nil || len(hits) != 1 {
		t.Fatalf("search after restart: %v %v", hits, err)
	}
	must(r2.Close(ctx))

	// Restart as kv-1 again, also with an empty cache dir.
	r3 := open("kv-1")
	defer r3.Close(ctx)
	check(r3, "acme")
	check(r3, "acme-copy")
	if v, ok, err := r3.Get(ctx, "acme", "boat"); err != nil || !ok || v != "a slow sail boat" {
		t.Fatalf("get boat: %q %v %v", v, ok, err)
	}
}
