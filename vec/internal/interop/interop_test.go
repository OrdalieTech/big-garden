// Package interop checks that package vec's vectors are sqlite-vec's: it
// loads the sqlite-vec that modernc bundles (registered process-wide, so in
// a test binary of its own) and compares its outputs with vec's.
package interop

import (
	"bytes"
	"database/sql"
	"encoding/binary"
	"math"
	"math/rand/v2"
	"testing"

	"github.com/OrdalieTech/big-garden/vec"
	_ "modernc.org/sqlite"
	_ "modernc.org/sqlite/vec"
)

func TestSqliteVecLayout(t *testing.T) {
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	r := rand.New(rand.NewPCG(9, 10))
	for _, dims := range []int{8, 384, 768} {
		a, b := make([]byte, 4*dims), make([]byte, 4*dims)
		for i := range dims {
			binary.LittleEndian.PutUint32(a[4*i:], math.Float32bits(r.Float32()*2-1))
			binary.LittleEndian.PutUint32(b[4*i:], math.Float32bits(r.Float32()*2-1))
		}
		var qa, qb []byte
		var d int64
		if err := db.QueryRow("SELECT vec_quantize_binary(?1), vec_quantize_binary(?2), vec_distance_hamming(vec_quantize_binary(?1), vec_quantize_binary(?2))",
			a, b).Scan(&qa, &qb, &d); err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(qa, vec.Quantize(a)) || !bytes.Equal(qb, vec.Quantize(b)) {
			t.Fatalf("dims %d: sqlite-vec quantizes otherwise", dims)
		}
		if got := vec.Nearest(qa, qb, 1); len(got) != 1 {
			t.Fatal("Nearest")
		}
		diff := 0
		for i := range qa {
			for x := qa[i] ^ qb[i]; x != 0; x &= x - 1 {
				diff++
			}
		}
		if int(d) != diff {
			t.Fatalf("dims %d: sqlite-vec hamming %d, vec %d", dims, d, diff)
		}
	}
}
