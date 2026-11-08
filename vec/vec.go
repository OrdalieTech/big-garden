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
//
// K-nearest-neighbor search is an ordinary query, a scan as sqlite-vec's
// vec0 tables do:
//
//	SELECT id FROM docs ORDER BY vec_distance_cosine(embedding, vec_f32(?)) LIMIT 10
package vec

import (
	"database/sql/driver"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"math"

	"modernc.org/sqlite"
)

// Register registers the vec functions on d.
func Register(d *sqlite.Driver) error {
	for name, f := range map[string]func([]driver.Value) (driver.Value, error){
		"vec_f32":             f32,
		"vec_length":          length,
		"vec_distance_l2":     pair(l2),
		"vec_distance_cosine": pair(cosine),
		"vec_dot":             pair(dot),
	} {
		n := int32(2)
		if name == "vec_f32" || name == "vec_length" {
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

// The loops advance both slices together; pair checked equal lengths.
func l2(a, b []byte) driver.Value {
	var s float64
	for ; len(a) >= 4 && len(b) >= 4; a, b = a[4:], b[4:] {
		d := at(a) - at(b)
		s += d * d
	}
	return math.Sqrt(s)
}

func dot(a, b []byte) driver.Value {
	var s float64
	for ; len(a) >= 4 && len(b) >= 4; a, b = a[4:], b[4:] {
		s += at(a) * at(b)
	}
	return s
}

func cosine(a, b []byte) driver.Value {
	var ab, aa, bb float64
	for ; len(a) >= 4 && len(b) >= 4; a, b = a[4:], b[4:] {
		x, y := at(a), at(b)
		ab, aa, bb = ab+x*y, aa+x*x, bb+y*y
	}
	if aa == 0 || bb == 0 {
		return nil
	}
	return 1 - ab/math.Sqrt(aa*bb)
}
