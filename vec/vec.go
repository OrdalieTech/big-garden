// Package vec registers vector search functions on a SQLite driver, for use
// with garden's Config.Extend (DECISIONS D19).
//
// A vector is a BLOB of little-endian float32 values, the float32 format of
// sqlite-vec, so vectors stored in an entity are also readable by
// sqlite-vec. The functions are deterministic; a NULL argument gives NULL,
// a malformed blob or a dimension mismatch is an SQL error:
//
//	vec_f32(v)                 blob from a JSON array text ('[0.1, 2]') or a valid blob
//	vec_distance_l2(a, b)      Euclidean distance
//	vec_distance_cosine(a, b)  1 - cosine similarity (NULL if either norm is 0)
//	vec_dot(a, b)              dot product
//	vec_length(v)              number of dimensions
//	vec_quantize_binary(v)     binary vector: one bit per element above zero, sqlite-vec's layout
//	vec_distance_hamming(a, b) differing bits of two binary vectors
//
// K-nearest-neighbor search is an ordinary query, a scan as sqlite-vec's
// vec0 tables do:
//
//	SELECT id FROM docs ORDER BY vec_distance_cosine(embedding, vec_f32(?)) LIMIT 10
//
// It is linear in rows. For large tables, search in two stages (D19): keep
// each vector's binary quantization, packed many to a chunk row, scan the
// chunks with Nearest inside a Read to get a few hundred candidates, then
// rerank those exactly with vec_distance_cosine. This reads 32 times fewer
// bytes and replaces a SQL call per row by a Go loop per chunk.
package vec

import (
	"database/sql/driver"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/bits"

	"modernc.org/sqlite"
)

// Register registers the vec functions on d.
func Register(d *sqlite.Driver) error {
	for name, f := range map[string]func([]driver.Value) (driver.Value, error){
		"vec_f32":              f32,
		"vec_length":           length,
		"vec_distance_l2":      pair(l2),
		"vec_distance_cosine":  pair(cosine),
		"vec_dot":              pair(dot),
		"vec_quantize_binary":  quantize,
		"vec_distance_hamming": hamming,
	} {
		n := int32(2)
		if name == "vec_f32" || name == "vec_length" || name == "vec_quantize_binary" {
			n = 1
		}
		scalar := func(_ *sqlite.FunctionContext, args []driver.Value) (driver.Value, error) { return f(args) }
		// VolatileArgs: blobs are read in place, never retained.
		if err := d.RegisterFunction(name, &sqlite.FunctionImpl{NArgs: n, Deterministic: true, Scalar: scalar, VolatileArgs: true}); err != nil {
			return err
		}
	}
	return nil
}

// vector returns v as a float32 blob, or an error.
func vector(v driver.Value) ([]byte, error) {
	b, ok := v.([]byte)
	if !ok || len(b) == 0 || len(b)%4 != 0 {
		return nil, fmt.Errorf("vec: want a non-empty float32 blob, got %T of %d bytes", v, len(b))
	}
	return b, nil
}

// at decodes the float32 at the start of b.
func at(b []byte) float64 {
	return float64(math.Float32frombits(binary.LittleEndian.Uint32(b)))
}

func f32(args []driver.Value) (driver.Value, error) {
	switch v := args[0].(type) {
	case nil:
		return nil, nil
	case string:
		var fs []float32
		if err := json.Unmarshal([]byte(v), &fs); err != nil || len(fs) == 0 {
			return nil, fmt.Errorf("vec: want a JSON array of numbers: %v", err)
		}
		b := make([]byte, 4*len(fs))
		for i, f := range fs {
			binary.LittleEndian.PutUint32(b[4*i:], math.Float32bits(f))
		}
		return b, nil
	default:
		b, err := vector(v)
		return append([]byte(nil), b...), err // the argument is volatile
	}
}

func length(args []driver.Value) (driver.Value, error) {
	if args[0] == nil {
		return nil, nil
	}
	b, err := vector(args[0])
	return int64(len(b) / 4), err
}

// pair adapts a function of two vectors of the same dimension.
func pair(f func(a, b []byte) driver.Value) func([]driver.Value) (driver.Value, error) {
	return func(args []driver.Value) (driver.Value, error) {
		if args[0] == nil || args[1] == nil {
			return nil, nil
		}
		a, err := vector(args[0])
		if err != nil {
			return nil, err
		}
		b, err := vector(args[1])
		if err != nil {
			return nil, err
		}
		if len(a) != len(b) {
			return nil, errors.New("vec: dimension mismatch")
		}
		return f(a, b), nil
	}
}

// The loops advance both slices together, four values at a time into
// separate sums; pair checked equal lengths.
func l2(a, b []byte) driver.Value {
	var s [4]float64
	for ; len(a) >= 16 && len(b) >= 16; a, b = a[16:], b[16:] {
		for j := range 4 {
			d := at(a[4*j:]) - at(b[4*j:])
			s[j] += d * d
		}
	}
	for ; len(a) >= 4 && len(b) >= 4; a, b = a[4:], b[4:] {
		d := at(a) - at(b)
		s[0] += d * d
	}
	return math.Sqrt(s[0] + s[1] + s[2] + s[3])
}

func dot(a, b []byte) driver.Value { return dotf(a, b) }

func dotf(a, b []byte) float64 {
	var s [4]float64
	for ; len(a) >= 16 && len(b) >= 16; a, b = a[16:], b[16:] {
		for j := range 4 {
			s[j] += at(a[4*j:]) * at(b[4*j:])
		}
	}
	for ; len(a) >= 4 && len(b) >= 4; a, b = a[4:], b[4:] {
		s[0] += at(a) * at(b)
	}
	return s[0] + s[1] + s[2] + s[3]
}

func cosine(a, b []byte) driver.Value {
	var ab, aa, bb [4]float64
	for ; len(a) >= 16 && len(b) >= 16; a, b = a[16:], b[16:] {
		for j := range 4 {
			x, y := at(a[4*j:]), at(b[4*j:])
			ab[j], aa[j], bb[j] = ab[j]+x*y, aa[j]+x*x, bb[j]+y*y
		}
	}
	for ; len(a) >= 4 && len(b) >= 4; a, b = a[4:], b[4:] {
		x, y := at(a), at(b)
		ab[0], aa[0], bb[0] = ab[0]+x*y, aa[0]+x*x, bb[0]+y*y
	}
	sab, saa, sbb := ab[0]+ab[1]+ab[2]+ab[3], aa[0]+aa[1]+aa[2]+aa[3], bb[0]+bb[1]+bb[2]+bb[3]
	if saa == 0 || sbb == 0 {
		return nil
	}
	return 1 - sab/math.Sqrt(saa*sbb)
}

// quantize is vec_quantize_binary: bit i of the result (byte i/8, least
// significant bit first) is set when element i is above zero, the layout of
// sqlite-vec's bit vectors. The dimension must be a multiple of 8.
func quantize(args []driver.Value) (driver.Value, error) {
	if args[0] == nil {
		return nil, nil
	}
	v, err := vector(args[0])
	if err != nil {
		return nil, err
	}
	if len(v)/4%8 != 0 {
		return nil, errors.New("vec: a binary vector needs a dimension that is a multiple of 8")
	}
	return Quantize(v), nil
}

// Quantize returns the binary vector of the float32 vector v (see
// vec_quantize_binary), for code that builds packed bit chunks itself.
func Quantize(v []byte) []byte {
	out := make([]byte, len(v)/32)
	for i := 0; 4*i+4 <= len(v); i++ {
		if at(v[4*i:]) > 0 {
			out[i/8] |= 1 << (i % 8)
		}
	}
	return out
}

// hamming is vec_distance_hamming: the number of differing bits of two
// binary vectors of the same length.
func hamming(args []driver.Value) (driver.Value, error) {
	if args[0] == nil || args[1] == nil {
		return nil, nil
	}
	a, ok := args[0].([]byte)
	b, ok2 := args[1].([]byte)
	if !ok || !ok2 || len(a) == 0 || len(a) != len(b) {
		return nil, errors.New("vec: want two binary vectors of the same length")
	}
	return int64(distance(a, b)), nil
}

func distance(a, b []byte) int {
	d := 0
	for ; len(a) >= 8; a, b = a[8:], b[8:] {
		d += bits.OnesCount64(binary.LittleEndian.Uint64(a) ^ binary.LittleEndian.Uint64(b))
	}
	for i := range a {
		d += bits.OnesCount8(a[i] ^ b[i])
	}
	return d
}

// Nearest returns the slots of the k binary vectors of packed closest to q
// in Hamming distance, closest first (ties by slot). packed holds
// len(packed)/len(q) vectors back to back, the layout of a chunk BLOB that
// keeps many vectors in one row. It is the first stage of a two-stage
// search: rerank the candidates' float32 vectors exactly (DECISIONS D19).
func Nearest(packed, q []byte, k int) []int {
	if len(q) == 0 || k <= 0 {
		return nil
	}
	type hit struct{ slot, d int }
	top := make([]hit, 0, k+1) // sorted by (d, slot); at most k
	for s := 0; (s+1)*len(q) <= len(packed); s++ {
		d := distance(packed[s*len(q):(s+1)*len(q)], q)
		if len(top) == k && d >= top[k-1].d {
			continue
		}
		i := len(top)
		top = append(top, hit{})
		for ; i > 0 && top[i-1].d > d; i-- {
			top[i] = top[i-1]
		}
		top[i] = hit{s, d}
		top = top[:min(len(top), k)]
	}
	slots := make([]int, len(top))
	for i, h := range top {
		slots[i] = h.slot
	}
	return slots
}
