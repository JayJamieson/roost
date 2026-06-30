package roost_test

import (
	"context"
	"fmt"
	"io"
	"runtime" // add to the bench_test.go imports
	"testing"
	"time"

	"github.com/jayjamieson/roost"
)

// nopSink discards encoded bytes so benchmarks isolate CPU/alloc cost.
type nopSink struct{}

func (nopSink) Create(context.Context, string) (io.WriteCloser, error) { return nopWC{}, nil }

type nopWC struct{}

func (nopWC) Write(p []byte) (int, error) { return len(p), nil }
func (nopWC) Close() error                { return nil }

// slowSink models per-object upload latency (the R2 PutObject on Close),
// so the concurrency benchmark has I/O to overlap.
type slowSink struct{ delay time.Duration }

func (s slowSink) Create(context.Context, string) (io.WriteCloser, error) {
	return slowWC{delay: s.delay}, nil
}

type slowWC struct{ delay time.Duration }

func (slowWC) Write(p []byte) (int, error) { return len(p), nil }
func (w slowWC) Close() error              { time.Sleep(w.delay); return nil }

type Row struct {
	RSN     int64 `roost:"name=rsn"`
	Time    time.Time
	Key     string
	Value   float64
	Payload []byte
}

// BenchmarkAppend measures the hot path with no roll (pure struct->builders).
func BenchmarkAppend(b *testing.B) {
	w, err := roost.NewWriter[Row](context.Background(), nopSink{}, roost.WithRollRows(1<<30))
	if err != nil {
		b.Fatal(err)
	}
	defer w.Close()
	row := Row{RSN: 1, Time: time.Now(), Key: "abc123", Value: 3.14, Payload: make([]byte, 64)}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		row.RSN = int64(i)
		if err := w.Append(row); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkAppendAndRoll includes encode + sink write at a realistic roll size.
func BenchmarkAppendAndRoll(b *testing.B) {
	w, _ := roost.NewWriter[Row](context.Background(), nopSink{},
		roost.WithRollRows(50000), roost.WithRowGroupRows(50000), roost.WithCodec("zstd"))
	defer w.Close()
	row := Row{RSN: 1, Time: time.Now(), Key: "abc123", Value: 3.14, Payload: make([]byte, 64)}
	b.SetBytes(8 + 8 + 16 + 8 + 64)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		row.RSN = int64(i)
		_ = w.Append(row)
	}
}

// BenchmarkRollConcurrency compares synchronous rolls against a 4-worker pool
// against a latency-bearing sink. Close() is timed so the async drain counts.
func BenchmarkRollConcurrency(b *testing.B) {
	const rollRows = 50_000
	rowBytes := int64(8 + 24 + 8 + 64)
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
				_ = w.Append(row)
			}
			_ = w.Close() // drains the pool; counted in elapsed for fair MB/s
			b.StopTimer()
		})
	}
}

// samplePeakHeap polls live heap until stop is closed and returns the max
// HeapAlloc seen. ReadMemStats briefly stops the world, so run this as a
// dedicated memory pass - its ns/op is NOT meaningful, only the peak metrics.
func samplePeakHeap(stop <-chan struct{}) uint64 {
	var peak uint64
	var ms runtime.MemStats
	read := func() {
		runtime.ReadMemStats(&ms)
		if ms.HeapAlloc > peak {
			peak = ms.HeapAlloc
		}
	}
	t := time.NewTicker(500 * time.Microsecond)
	defer t.Stop()
	for {
		select {
		case <-stop:
			read()
			return peak
		case <-t.C:
			read()
		}
	}
}

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

			runtime.GC()
			var baseline runtime.MemStats
			runtime.ReadMemStats(&baseline)

			stop := make(chan struct{})
			result := make(chan uint64, 1)
			go func() { result <- samplePeakHeap(stop) }()

			row := base
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				row.RSN = int64(i)
				_ = w.Append(row)
			}
			_ = w.Close()
			b.StopTimer()

			close(stop)
			peak := <-result
			b.ReportMetric(float64(peak)/(1<<20), "peakMB")
			b.ReportMetric(float64(peak-baseline.HeapAlloc)/(1<<20), "deltaMB")
		})
	}
}
