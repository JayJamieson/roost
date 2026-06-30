package roost_test

import (
	"context"
	"math"
	"testing"
	"time"

	"github.com/jayjamieson/roost"
	"github.com/jayjamieson/roost/examples/codegen"
)

// metricRow is a representative hot-path row: a time.Time (which the reflection
// path must box via reflect.Value.Interface), a string, a float, and a non-nil
// nullable pointer - plus a partition column.
func metricRow() codegen.Metric {
	v := 3.14
	return codegen.Metric{TS: time.Now(), Host: "host-01", CPU: 0.42, Value: &v, Region: "us-east-1"}
}

// noRoll isolates the append hot path: roll and row-group sizes are large enough
// that neither the encoder nor a roll fires during the benchmark, so only
// struct->builder work (and its allocations) is measured.
var noRoll = []roost.Option{roost.WithRollRows(1 << 30), roost.WithRowGroupRows(1 << 30)}

// BenchmarkAppendReflection measures the default reflection appender.
func BenchmarkAppendReflection(b *testing.B) {
	w, err := roost.NewWriter[codegen.Metric](context.Background(), nopSink{}, noRoll...)
	if err != nil {
		b.Fatal(err)
	}
	defer w.Close()
	row := metricRow()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		row.CPU = float64(i)
		if err := w.Append(row); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkAppendGenerated measures the roostgen appender (zero reflection).
func BenchmarkAppendGenerated(b *testing.B) {
	w, err := roost.NewWriterFor[codegen.Metric](context.Background(), nopSink{}, codegen.MetricRoostAppender{}, noRoll...)
	if err != nil {
		b.Fatal(err)
	}
	defer w.Close()
	row := metricRow()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		row.CPU = float64(i)
		if err := w.Append(row); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkAppendGeneratedPtr measures the generated appender via AppendPtr with
// a reused row buffer: the &v escape is hoisted out of the loop, and the
// generated PartitionInto routes the row through a reused key buffer, so a
// steady stream into an open partition appends with zero allocations.
func BenchmarkAppendGeneratedPtr(b *testing.B) {
	w, err := roost.NewWriterFor[codegen.Metric](context.Background(), nopSink{}, codegen.MetricRoostAppender{}, noRoll...)
	if err != nil {
		b.Fatal(err)
	}
	defer w.Close()
	row := metricRow() // one buffer, reused across calls
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		row.CPU = float64(i)
		if err := w.AppendPtr(&row); err != nil {
			b.Fatal(err)
		}
	}
}

// TestGeneratedReducesAllocs turns SPEC §7.4's acceptance criterion into a hard
// gate: the generated appender must allocate <= 2 times per row and strictly
// fewer than reflection. Unlike a benchmark, this fails CI on regression.
func TestGeneratedReducesAllocs(t *testing.T) {
	row := metricRow()

	wr, err := roost.NewWriter[codegen.Metric](context.Background(), nopSink{}, noRoll...)
	if err != nil {
		t.Fatal(err)
	}
	defer wr.Close()
	refAllocs := testing.AllocsPerRun(2000, func() { _ = wr.Append(row) })

	wg, err := roost.NewWriterFor[codegen.Metric](context.Background(), nopSink{}, codegen.MetricRoostAppender{}, noRoll...)
	if err != nil {
		t.Fatal(err)
	}
	defer wg.Close()
	genAllocs := testing.AllocsPerRun(2000, func() { _ = wg.Append(row) })

	t.Logf("allocs/op: reflection=%.2f generated=%.2f", refAllocs, genAllocs)
	// Allocs/op is an integer count per row; round to ignore the sub-alloc noise
	// from amortized Arrow builder growth.
	if math.Round(genAllocs) > 2 {
		t.Errorf("generated allocs/op = %.2f, want <= 2", genAllocs)
	}
	if genAllocs >= refAllocs {
		t.Errorf("generated allocs/op (%.2f) is not lower than reflection (%.2f)", genAllocs, refAllocs)
	}
}

// TestGeneratedPtrZeroAllocPartition asserts the full optimization: AppendPtr
// with a reused buffer into an already-open partition allocates nothing, because
// PartitionInto routes the row through the Writer's reused key buffer.
func TestGeneratedPtrZeroAllocPartition(t *testing.T) {
	w, err := roost.NewWriterFor[codegen.Metric](context.Background(), nopSink{}, codegen.MetricRoostAppender{}, noRoll...)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	row := metricRow()
	allocs := testing.AllocsPerRun(2000, func() { _ = w.AppendPtr(&row) })
	t.Logf("AppendPtr allocs/op (steady-state, partition open) = %.2f", allocs)
	if math.Round(allocs) != 0 {
		t.Errorf("AppendPtr allocs/op = %.2f, want 0", allocs)
	}
}
