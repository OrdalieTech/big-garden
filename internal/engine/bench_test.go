package engine

import (
	"fmt"
	"io"
	"testing"
)

// benchDelta measures Delta (capture plus encoding) for a one-row update
// that rewrites one application page, in a database of about size bytes.
func benchDelta(b *testing.B, size int64) {
	fx := newFixture(b, 65536)
	fx.mutate(b, "CREATE TABLE t(id INTEGER PRIMARY KEY, v BLOB)")
	rows := 0
	for int64(rows)*pageSize < size { // one 3000-byte row per page
		n := min(1000, size/pageSize-int64(rows)+1)
		fx.mutate(b, `INSERT INTO t(v) SELECT randomblob(3000) FROM (WITH RECURSIVE c(i) AS
			(SELECT 1 UNION ALL SELECT i+1 FROM c WHERE i < 1000) SELECT i FROM c) LIMIT ?`, n)
		rows += int(n)
	}
	fx.publish(b, true)
	pages := fx.d.Published().PageCount
	captured := 0
	b.ResetTimer()
	for i := range b.N {
		b.StopTimer()
		fx.mutate(b, "UPDATE t SET v = randomblob(3000) WHERE id = ?", i%rows+1)
		b.StartTimer()
		o, err := fx.d.Delta(io.Discard)
		if err != nil {
			b.Fatal(err)
		}
		b.StopTimer()
		if o.Pages > 16 {
			b.Fatalf("one-row update captured %d pages", o.Pages)
		}
		captured += int(o.Pages)
		if err := fx.d.Checkpoint(o); err != nil {
			b.Fatal(err)
		}
		b.StartTimer()
	}
	b.ReportMetric(float64(pages), "db-pages")
	b.ReportMetric(float64(captured)/float64(b.N), "pages/op")
}

func BenchmarkDelta64KiB(b *testing.B) { benchDelta(b, 64<<10) }
func BenchmarkDelta64MiB(b *testing.B) { benchDelta(b, 64<<20-(1<<20)) }

// TestCaptureCostIndependentOfSize is the PLAN §18.3 gate: capturing the
// same one-page update costs at most 3x more CPU in a 64 MiB database than
// in a 64 KiB one (no full-database work per commit). Runs with -long.
func TestCaptureCostIndependentOfSize(t *testing.T) {
	if !*long {
		t.Skip("needs -long")
	}
	small := testing.Benchmark(func(b *testing.B) { benchDelta(b, 64<<10) })
	large := testing.Benchmark(func(b *testing.B) { benchDelta(b, 64<<20-(1<<20)) })
	ratio := float64(large.NsPerOp()) / float64(small.NsPerOp())
	t.Logf("64 KiB: %v (%s)\n64 MiB: %v (%s)\nratio %.2f", small, small.MemString(), large, large.MemString(), ratio)
	if ratio > 3 {
		t.Fatal(fmt.Sprintf("capture in 64 MiB costs %.2fx the 64 KiB capture (> 3x)", ratio))
	}
}
