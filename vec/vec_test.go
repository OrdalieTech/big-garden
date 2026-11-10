package vec_test

import (
	"database/sql"
	"encoding/binary"
	"fmt"
	"math"
	"math/rand/v2"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/OrdalieTech/big-garden/vec"
	"modernc.org/sqlite"
)

var drivers atomic.Int64

func open(t *testing.T) *sql.DB {
	t.Helper()
	d := &sqlite.Driver{}
	if err := vec.Register(d); err != nil {
		t.Fatal(err)
	}
	name := fmt.Sprint("vec-test-", drivers.Add(1)) // -count=N opens again
	sql.Register(name, d)
	db, err := sql.Open(name, ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { db.Close() })
	return db
}

func blob(v []float32) []byte {
	b := make([]byte, 4*len(v))
	for i, f := range v {
		binary.LittleEndian.PutUint32(b[4*i:], math.Float32bits(f))
	}
	return b
}

// TestAgainstReference compares every function with a Go reference over
// random vectors of several dimensions.
func TestAgainstReference(t *testing.T) {
	db := open(t)
	r := rand.New(rand.NewPCG(3, 4))
	for _, dims := range []int{1, 3, 16, 384, 768} {
		for range 20 {
			a, b := make([]float32, dims), make([]float32, dims)
			for i := range dims {
				a[i], b[i] = r.Float32()*4-2, r.Float32()*4-2
			}
			var dot, aa, bb, l2 float64
			for i := range dims {
				x, y := float64(a[i]), float64(b[i])
				dot, aa, bb, l2 = dot+x*y, aa+x*x, bb+y*y, l2+(x-y)*(x-y)
			}
			var gl2, gcos, gdot float64
			var n int64
			if err := db.QueryRow("SELECT vec_distance_l2(?1, ?2), vec_distance_cosine(?1, ?2), vec_dot(?1, ?2), vec_length(?1)",
				blob(a), blob(b)).Scan(&gl2, &gcos, &gdot, &n); err != nil {
				t.Fatal(err)
			}
			for _, c := range [][2]float64{{gl2, math.Sqrt(l2)}, {gcos, 1 - dot/math.Sqrt(aa*bb)}, {gdot, dot}} {
				if math.Abs(c[0]-c[1]) > 1e-9*max(1, math.Abs(c[1])) {
					t.Fatalf("dims %d: got %v, want %v", dims, c[0], c[1])
				}
			}
			if n != int64(dims) {
				t.Fatalf("vec_length %d, want %d", n, dims)
			}
		}
	}
}

func TestConversionNullAndErrors(t *testing.T) {
	db := open(t)
	var b []byte
	if err := db.QueryRow("SELECT vec_f32('[1, -2.5, 3e2]')").Scan(&b); err != nil || string(b) != string(blob([]float32{1, -2.5, 300})) {
		t.Fatalf("vec_f32 json: %x %v", b, err)
	}
	if err := db.QueryRow("SELECT vec_f32(vec_f32('[0.5]'))").Scan(&b); err != nil || string(b) != string(blob([]float32{0.5})) {
		t.Fatalf("vec_f32 blob: %x %v", b, err)
	}
	var null sql.NullFloat64
	for _, q := range []string{"SELECT vec_f32(NULL)", "SELECT vec_length(NULL)", "SELECT vec_distance_l2(NULL, x'00000000')",
		"SELECT vec_distance_cosine(x'00000000', NULL)", "SELECT vec_dot(NULL, NULL)",
		"SELECT vec_distance_cosine(x'00000000', x'0000803f')"} { // zero norm
		if err := db.QueryRow(q).Scan(&null); err != nil || null.Valid {
			t.Errorf("%s: %v %v", q, null, err)
		}
	}
	for q, msg := range map[string]string{
		"SELECT vec_distance_l2(vec_f32('[1, 2]'), vec_f32('[1, 2, 3]'))": "dimension mismatch",
		"SELECT vec_dot(x'000000', x'000000')":                            "float32 blob",
		"SELECT vec_length(x'')":                                          "float32 blob",
		"SELECT vec_distance_cosine(1, x'00000000')":                      "float32 blob",
		"SELECT vec_f32('[1, \"a\"]')":                                    "JSON array",
		"SELECT vec_f32('[]')":                                            "JSON array",
		"SELECT vec_f32('[1e300]')":                                       "JSON array",
		"SELECT vec_f32(3)":                                               "float32 blob",
		"SELECT vec_dot(vec_f32('[1]'))":                                  "wrong number of arguments",
	} {
		if err := db.QueryRow(q).Scan(&null); err == nil || !strings.Contains(err.Error(), msg) {
			t.Errorf("%s: %v, want %q", q, err, msg)
		}
	}
}

// TestVolatileArgs checks that results do not alias SQLite's argument
// buffers when many rows are scanned.
func TestVolatileArgs(t *testing.T) {
	db := open(t)
	if _, err := db.Exec("CREATE TABLE v(id INTEGER PRIMARY KEY, e BLOB)"); err != nil {
		t.Fatal(err)
	}
	for i := range 100 {
		if _, err := db.Exec("INSERT INTO v VALUES (?, ?)", i, blob([]float32{float32(i), 1})); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := db.Exec("CREATE TABLE c AS SELECT id, vec_f32(e) AS e FROM v"); err != nil {
		t.Fatal(err)
	}
	var bad int
	if err := db.QueryRow("SELECT count(*) FROM v JOIN c USING (id) WHERE v.e != c.e").Scan(&bad); err != nil || bad != 0 {
		t.Fatalf("%d copies differ: %v", bad, err)
	}
	var id int
	if err := db.QueryRow("SELECT id FROM v ORDER BY vec_distance_l2(e, vec_f32('[41.2, 1]')) LIMIT 1").Scan(&id); err != nil || id != 41 {
		t.Fatalf("nearest %d: %v", id, err)
	}
}
