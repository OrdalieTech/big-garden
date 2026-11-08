package garden

import (
	"bytes"
	"database/sql"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"math/rand/v2"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/OrdalieTech/big-garden/internal/store"
	"github.com/OrdalieTech/big-garden/internal/storetest"
	"github.com/OrdalieTech/big-garden/vec"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"modernc.org/sqlite"
)

// extNode is testNode with vec registered through Config.Extend.
func extNode(t *testing.T, st store.Store, nodeID string) *Node {
	t.Helper()
	cfg := Config{S3: s3.New(s3.Options{}), Bucket: "b", Prefix: "p", NodeID: nodeID, CacheDir: t.TempDir(),
		Limits: Limits{EntityBytes: 8 << 20, CommitTimeout: 2 * time.Second}, Extend: vec.Register}
	cfg, err := cfg.validate()
	if err != nil {
		t.Fatal(err)
	}
	n, err := open(bg, cfg, st)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { n.Close(bg) })
	return n
}

func f32blob(v []float32) []byte {
	b := make([]byte, 4*len(v))
	for i, f := range v {
		binary.LittleEndian.PutUint32(b[4*i:], math.Float32bits(f))
	}
	return b
}

// extEntity is the expected content of an entity using fts5, rtree and vec.
type extEntity struct {
	vecs [][]float32
	docs int
}

const extDims = 16

func (x *extEntity) create(tx *Tx) ([]byte, error) {
	_, err := tx.Exec(bg, `CREATE VIRTUAL TABLE docs USING fts5(body, pad UNINDEXED);
		CREATE VIRTUAL TABLE box USING rtree(id, x0, x1, y0, y1);
		CREATE TABLE items(id INTEGER PRIMARY KEY, emb BLOB NOT NULL)`)
	return nil, err
}

// add inserts a document of about 40 KiB (so the delta is uploaded, not
// inlined), a box and 10 vectors.
func (x *extEntity) add(r *rand.Rand) func(*Tx) ([]byte, error) {
	i := x.docs
	x.docs++
	var vs [][]float32
	for range 10 {
		v := make([]float32, extDims)
		for d := range v {
			v[d] = r.Float32()*2 - 1
		}
		vs = append(vs, v)
	}
	x.vecs = append(x.vecs, vs...)
	base := len(x.vecs) - len(vs)
	return func(tx *Tx) ([]byte, error) {
		if _, err := tx.Exec(bg, "INSERT INTO docs VALUES (?, ?)", fmt.Sprintf("document %d word%d", i, i%5), randText(40<<10)); err != nil {
			return nil, err
		}
		if _, err := tx.Exec(bg, "INSERT INTO box VALUES (?, ?, ?, ?, ?)", i, i, i+1, i, i+1); err != nil {
			return nil, err
		}
		for j, v := range vs {
			if _, err := tx.Exec(bg, "INSERT INTO items VALUES (?, ?)", base+j, f32blob(v)); err != nil {
				return nil, err
			}
		}
		return nil, nil
	}
}

// knn is the Go reference: the k nearest vectors to q by cosine distance.
func (x *extEntity) knn(q []float32, k int) []int64 {
	type hit struct {
		id int64
		d  float64
	}
	var hits []hit
	for i, v := range x.vecs {
		var ab, aa, bb float64
		for d := range v {
			ab, aa, bb = ab+float64(v[d])*float64(q[d]), aa+float64(v[d])*float64(v[d]), bb+float64(q[d])*float64(q[d])
		}
		hits = append(hits, hit{int64(i), 1 - ab/math.Sqrt(aa*bb)})
	}
	slices.SortFunc(hits, func(a, b hit) int {
		if a.d != b.d {
			if a.d < b.d {
				return -1
			}
			return 1
		}
		return int(a.id - b.id)
	})
	var ids []int64
	for _, h := range hits[:k] {
		ids = append(ids, h.id)
	}
	return ids
}

func ints(v *View, q string, args ...any) ([]int64, error) {
	var out []int64
	err := v.Query(bg, q, args, func(r *sql.Rows) error {
		for r.Next() {
			var i int64
			if err := r.Scan(&i); err != nil {
				return err
			}
			out = append(out, i)
		}
		return nil
	})
	return out, err
}

// verify checks fts5, rtree and vec KNN on n against the expected content.
func (x *extEntity) verify(t *testing.T, n *Node, ref Ref) {
	t.Helper()
	q := make([]float32, extDims)
	for d := range q {
		q[d] = float32(d%3) - 1
	}
	want := x.knn(q, 10)
	if _, err := n.Read(bg, ref, 1, func(v *View) ([]byte, error) {
		m, err := ints(v, "SELECT rowid FROM docs WHERE docs MATCH 'word3' ORDER BY rowid")
		if err != nil || len(m) != (x.docs+1)/5 {
			return nil, fmt.Errorf("match %v: %v", m, err)
		}
		b, err := ints(v, "SELECT id FROM box WHERE x1 >= 2.5 AND x0 <= 4.5 ORDER BY id")
		if err != nil || !slices.Equal(b, []int64{2, 3, 4}) {
			return nil, fmt.Errorf("rtree %v: %v", b, err)
		}
		got, err := ints(v, "SELECT id FROM items ORDER BY vec_distance_cosine(emb, ?), id LIMIT 10", f32blob(q))
		if err != nil || !slices.Equal(got, want) {
			return nil, fmt.Errorf("knn %v, want %v: %v", got, want, err)
		}
		return nil, nil
	}); err != nil {
		t.Fatalf("%s: %v", ref.Key, err)
	}
}

// TestExtensionsDurable writes fts5, rtree and vec tables through
// compaction or snapshot, then reads them back after a restart with an empty
// cache, a takeover by another node, Export and Import; a node without the
// extension activates the entity and gets SQLite's error from vec only.
func TestExtensionsDurable(t *testing.T) {
	st := newStore(t)
	a := extNode(t, st, "a")
	x := &extEntity{}
	ref, _, err := a.Create(bg, "x", newReq(t, "create", 1), x.create)
	if err != nil {
		t.Fatal(err)
	}
	r := rand.New(rand.NewPCG(1, 2))
	for i := range 20 {
		if _, err := a.Update(bg, ref, newReq(t, fmt.Sprint("add", i), 1), x.add(r)); err != nil {
			t.Fatal(err)
		}
		a.wg.Wait()
	}
	if s := a.Stats(); s.Compactions+s.Snapshots == 0 {
		t.Fatalf("no compaction or snapshot: %+v", s)
	}
	x.verify(t, a, ref)

	a2 := extNode(t, st, "a") // a's next process: empty cache, implicit takeover (D4)
	x.verify(t, a2, ref)

	b := extNode(t, st, "b")
	route, err := b.Resolve(bg, "x")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := b.Takeover(bg, route); err != nil {
		t.Fatal(err)
	}
	x.verify(t, b, ref)
	if _, err := b.Update(bg, ref, newReq(t, "add-b", 1), x.add(r)); err != nil {
		t.Fatal(err)
	}
	x.verify(t, b, ref)

	var archive bytes.Buffer
	if _, err := b.Export(bg, ref, &archive); err != nil {
		t.Fatal(err)
	}
	y, _, err := b.Import(bg, "y", newReq(t, "import", 1), &archive)
	if err != nil {
		t.Fatal(err)
	}
	x.verify(t, b, y)

	// Without Extend: activation succeeds (it never runs caller code),
	// fts5 and rtree work, vec fails with SQLite's error, never corrupt.
	c := testNode(t, st, "c", nil)
	if route, err = c.Resolve(bg, "y"); err == nil {
		_, err = c.Takeover(bg, route)
	}
	if err != nil {
		t.Fatal(err)
	}
	_, err = c.Read(bg, y, 1, func(v *View) ([]byte, error) {
		if m, err := ints(v, "SELECT rowid FROM docs WHERE docs MATCH 'word1'"); err != nil || len(m) == 0 {
			return nil, fmt.Errorf("match %v: %v", m, err)
		}
		_, err := ints(v, "SELECT id FROM items ORDER BY vec_distance_l2(emb, emb) LIMIT 1")
		return nil, err
	})
	var e *Error
	if err == nil || errors.As(err, &e) || !strings.Contains(err.Error(), "no such function: vec_distance_l2") {
		t.Fatalf("vec without Extend: %v", err)
	}
	if s := c.Stats(); s.Corruptions != 0 || s.ActivationFailures != 0 {
		t.Fatalf("stats %+v", s)
	}
}

// TestExtendBoundary checks Config.Extend's isolation and errors, and that
// the SQL boundary still holds with extensions.
func TestExtendBoundary(t *testing.T) {
	st := newStore(t)
	n := extNode(t, st, "a")
	// The registrations reach no connection of the default driver.
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Exec("SELECT vec_length(x'00000000')"); err == nil || !strings.Contains(err.Error(), "no such function") {
		t.Fatalf("vec visible to the default driver: %v", err)
	}
	// An Extend error fails Open with invalid_argument.
	cfg := Config{S3: s3.New(s3.Options{}), Bucket: "b", Prefix: "p", NodeID: "z", CacheDir: t.TempDir(),
		Extend: func(*sqlite.Driver) error { return errors.New("boom") }}
	if cfg, err = cfg.validate(); err != nil {
		t.Fatal(err)
	}
	_, err = open(bg, cfg, st)
	if e := wantCode(t, err, CodeInvalidArgument); !strings.Contains(e.Error(), "boom") {
		t.Fatalf("error %v", e)
	}

	ref, _, err := n.Create(bg, "k", newReq(t, "create", 1), func(tx *Tx) ([]byte, error) {
		_, err := tx.Exec(bg, "CREATE TABLE items(id INTEGER PRIMARY KEY, emb BLOB); INSERT INTO items VALUES (1, vec_f32('[1, 2]'))")
		return nil, err
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, q := range []string{
		"CREATE INDEX il ON items(vec_length(emb))",
		"CREATE TABLE g(emb BLOB, n AS (vec_length(emb)))",
		"CREATE VIRTUAL TABLE s USING dbstat", "CREATE VIRTUAL TABLE s USING sqlite_dbpage",
		"CREATE VIRTUAL TABLE s USING nosuchmodule", "CREATE VIRTUAL TABLE _state_x USING fts5(a)",
		"SELECT load_extension('/nonexistent/vec0.so')", "SELECT * FROM sqlite_dbpage", "SELECT * FROM dbstat",
	} {
		_, err := n.Update(bg, ref, newReq(t, q, 1), func(tx *Tx) ([]byte, error) {
			_, err := tx.Exec(bg, q)
			return nil, err
		})
		var e *Error
		if err == nil || errors.As(err, &e) {
			t.Errorf("%q: %v", q, err)
		}
		if strings.Contains(q, "vec_length") && !strings.Contains(fmt.Sprint(err), "unsafe use of vec_length()") {
			t.Errorf("%q: unclear error %v", q, err)
		}
	}
	if _, err := n.Read(bg, ref, 1, func(v *View) ([]byte, error) {
		got, err := ints(v, "SELECT vec_length(emb) FROM items")
		if err != nil || !slices.Equal(got, []int64{2}) {
			return nil, fmt.Errorf("%v: %v", got, err)
		}
		return nil, nil
	}); err != nil {
		t.Fatal(err)
	}
}

// BenchmarkKNN measures a brute-force KNN query (k = 10, cosine) inside an
// entity: vectors of 384 and 768 float32 dimensions, 10k and 50k rows.
func BenchmarkKNN(b *testing.B) {
	for _, dims := range []int{384, 768} {
		for _, rows := range []int{10_000, 50_000} {
			b.Run(fmt.Sprintf("d%d/n%d", dims, rows), func(b *testing.B) {
				cfg := Config{S3: s3.New(s3.Options{}), Bucket: "b", Prefix: "p", NodeID: "a", CacheDir: b.TempDir(), Extend: vec.Register,
					Limits: Limits{EntityBytes: maxEntityBytes, Recoveries: 1, CommitTimeout: time.Minute, StorageAttemptTimeout: time.Minute}}
				cfg, err := cfg.validate()
				if err != nil {
					b.Fatal(err)
				}
				n, err := open(bg, cfg, storetest.NewMem())
				if err != nil {
					b.Fatal(err)
				}
				defer n.Close(bg)
				id, _ := NewMutationID(time.Now())
				ref, _, err := n.Create(bg, "v", Request{ID: id, Hash: [32]byte{1}, Schema: 1}, func(tx *Tx) ([]byte, error) {
					_, err := tx.Exec(bg, "CREATE TABLE items(id INTEGER PRIMARY KEY, emb BLOB NOT NULL)")
					return nil, err
				})
				if err != nil {
					b.Fatal(err)
				}
				r := rand.New(rand.NewPCG(5, 6))
				x := &extEntity{}
				for start := 0; start < rows; start += 5000 {
					id, _ := NewMutationID(time.Now())
					if _, err := n.Update(bg, ref, Request{ID: id, Hash: [32]byte{2}, Schema: 1}, func(tx *Tx) ([]byte, error) {
						for i := start; i < start+5000; i++ {
							v := make([]float32, dims)
							for d := range v {
								v[d] = r.Float32()*2 - 1
							}
							x.vecs = append(x.vecs, v)
							if _, err := tx.Exec(bg, "INSERT INTO items VALUES (?, ?)", i, f32blob(v)); err != nil {
								return nil, err
							}
						}
						return nil, nil
					}); err != nil {
						b.Fatal(err)
					}
				}
				n.wg.Wait()
				q := x.vecs[7]
				knn := func() []int64 {
					var got []int64
					if _, err := n.Read(bg, ref, 1, func(v *View) ([]byte, error) {
						got, err = ints(v, "SELECT id FROM items ORDER BY vec_distance_cosine(emb, ?) LIMIT 10", f32blob(q))
						return nil, err
					}); err != nil {
						b.Fatal(err)
					}
					return got
				}
				if got, want := knn(), x.knn(q, 10); !slices.Equal(got, want) {
					b.Fatalf("knn %v, want %v", got, want)
				}
				mib := float64(n.Stats().CacheBytes) / (1 << 20)
				for b.Loop() {
					knn()
				}
				b.ReportMetric(mib, "entity-MiB")
				b.ReportMetric(float64(b.Elapsed().Microseconds())/1000/float64(b.N), "ms/query")
			})
		}
	}
}
