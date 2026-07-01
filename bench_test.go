package roost_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/jayjamieson/roost"
)

// BenchmarkAppendAndRoll includes encode + sink write at a realistic roll size.
func BenchmarkAppendAndRoll(b *testing.B) {
	w, _ := roost.NewWriter[Row](context.Background(), nopSink{},
		roost.WithRollRows(50000), roost.WithRowGroupRows(50000), roost.WithCodec("zstd"))
	defer w.Close()
	row := Row{RSN: 1, Time: time.Now(), Key: "abc123", Value: 3.14, Payload: make([]byte, 64)}
	b.SetBytes(rowBytes)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		row.RSN = int64(i)
		_ = w.Append(&row)
	}
}

// BenchmarkRollConcurrency compares synchronous rolls against worker pools
// against a latency-bearing sink. Close() is timed so the async drain counts.
func BenchmarkRollConcurrency(b *testing.B) {
	const rollRows = 50_000
	base := Row{RSN: 1, Time: time.Now(), Key: "abc123", Value: 3.14, Payload: make([]byte, 64)}

	for _, workers := range []int{1, 4, 16, 32} {
		b.Run(fmt.Sprintf("workers=%d", workers), func(b *testing.B) {
			opts := []roost.Option{
				roost.WithRollRows(rollRows),
				roost.WithRowGroupRows(rollRows),
				roost.WithCodec("zstd"),
			}
			if workers > 1 {
				opts = append(opts, roost.WithEncodeConcurrency(workers))
			}
			w, err := roost.NewWriter[Row](context.Background(),
				slowSink{delay: 30 * time.Millisecond}, opts...)
			if err != nil {
				b.Fatal(err)
			}
			row := base
			b.SetBytes(rowBytes)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				row.RSN = int64(i)
				_ = w.Append(&row)
			}
			_ = w.Close() // drains the pool; counted in elapsed for fair MB/s
			b.StopTimer()
		})
	}
}

// BenchmarkRollConcurrencyMem tracks peak heap as encode concurrency rises: with
// a latency-bearing sink, more workers keep more objects in flight at once.
func BenchmarkRollConcurrencyMem(b *testing.B) {
	const rollRows = 50_000
	base := Row{RSN: 1, Time: time.Now(), Key: "abc123", Value: 3.14, Payload: make([]byte, 64)}

	for _, workers := range []int{1, 4, 16, 32} {
		b.Run(fmt.Sprintf("workers=%d", workers), func(b *testing.B) {
			opts := []roost.Option{
				roost.WithRollRows(rollRows),
				roost.WithRowGroupRows(rollRows),
				roost.WithCodec("zstd"),
			}
			if workers > 1 {
				opts = append(opts, roost.WithEncodeConcurrency(workers))
			}
			w, err := roost.NewWriter[Row](context.Background(),
				slowSink{delay: 30 * time.Millisecond}, opts...) // big delay => more in flight
			if err != nil {
				b.Fatal(err)
			}
			row := base
			reportPeakHeap(b, func() {
				for i := 0; i < b.N; i++ {
					row.RSN = int64(i)
					_ = w.Append(&row)
				}
				_ = w.Close()
			})
		})
	}
}
