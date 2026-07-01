package roost_test

import (
	"context"
	"io"
	"io/fs"
	"math/rand"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/parquet/file"
)

// Row is the shared row type for the benchmarks and throughput tests: a couple
// of scalars, a string, a float, and a binary payload.
type Row struct {
	RSN     int64 `roost:"name=rsn"`
	Time    time.Time
	Key     string
	Value   float64
	Payload []byte
}

// rowBytes is Row's approximate logical size, used for throughput MB/s.
const rowBytes = int64(8 + 8 + 16 + 8 + 64)

// corpusSize must exceed the row-group size so no row repeats within a group
// (keeps zstd honest). Power of two so we can index with a cheap mask.
const corpusSize = 1 << 15 // 32768

// buildCorpus generates varied, incompressible rows up front so a timed loop
// measures roost, not data generation.
func buildCorpus() []Row {
	rng := rand.New(rand.NewSource(1))
	keys := make([]string, 256)
	for i := range keys {
		b := make([]byte, 12)
		for j := range b {
			b[j] = byte('a' + rng.Intn(26))
		}
		keys[i] = string(b)
	}
	rows := make([]Row, corpusSize)
	for i := range rows {
		p := make([]byte, 64)
		rng.Read(p) // incompressible payload
		rows[i] = Row{
			RSN:     int64(i),
			Time:    time.Unix(0, rng.Int63()),
			Key:     keys[rng.Intn(len(keys))],
			Value:   rng.Float64(),
			Payload: p,
		}
	}
	return rows
}

// nopSink discards encoded bytes so benchmarks isolate CPU/alloc cost.
type nopSink struct{}

func (nopSink) Create(context.Context, string) (io.WriteCloser, error) { return nopWC{}, nil }

type nopWC struct{}

func (nopWC) Write(p []byte) (int, error) { return len(p), nil }
func (nopWC) Close() error                { return nil }

// slowSink models per-object upload latency (the R2 PutObject on Close), so the
// concurrency benchmark has I/O to overlap.
type slowSink struct{ delay time.Duration }

func (s slowSink) Create(context.Context, string) (io.WriteCloser, error) {
	return slowWC{delay: s.delay}, nil
}

type slowWC struct{ delay time.Duration }

func (slowWC) Write(p []byte) (int, error) { return len(p), nil }
func (w slowWC) Close() error              { time.Sleep(w.delay); return nil }

// samplePeakHeap polls in-use heap until stop is closed and returns the max
// HeapInuse seen. HeapInuse (in-use spans) is steadier than HeapAlloc and a
// better proxy for peak footprint, so all memory benchmarks share it.
// ReadMemStats briefly stops the world, so a benchmark using this is a dedicated
// memory pass - its ns/op is NOT meaningful, only the peak metrics.
func samplePeakHeap(stop <-chan struct{}) uint64 {
	var peak uint64
	var ms runtime.MemStats
	read := func() {
		runtime.ReadMemStats(&ms)
		if ms.HeapInuse > peak {
			peak = ms.HeapInuse
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

// reportPeakHeap runs fn (the timed append+close loop) while polling peak heap,
// and reports peakMB plus deltaMB over the pre-run baseline.
func reportPeakHeap(b *testing.B, fn func()) {
	runtime.GC()
	var baseline runtime.MemStats
	runtime.ReadMemStats(&baseline)

	stop := make(chan struct{})
	result := make(chan uint64, 1)
	go func() { result <- samplePeakHeap(stop) }()

	b.ResetTimer()
	fn()
	b.StopTimer()

	close(stop)
	peak := <-result
	b.ReportMetric(float64(peak)/(1<<20), "peakMB")
	b.ReportMetric(float64(peak-baseline.HeapInuse)/(1<<20), "deltaMB")
}

// eachParquet opens every *.parquet file under dir (sorted by path) and calls fn
// with the open reader; the reader is closed after fn returns.
func eachParquet(t *testing.T, dir string, fn func(path string, rdr *file.Reader)) {
	t.Helper()
	var paths []string
	_ = filepath.WalkDir(dir, func(p string, d fs.DirEntry, err error) error {
		if err == nil && !d.IsDir() && strings.HasSuffix(p, ".parquet") {
			paths = append(paths, p)
		}
		return nil
	})
	sort.Strings(paths)
	for _, p := range paths {
		rdr, err := file.OpenParquetFile(p, false)
		if err != nil {
			t.Fatalf("open %s: %v", p, err)
		}
		fn(p, rdr)
		rdr.Close()
	}
}

// countParquetRows returns the total row count and the file paths under dir.
func countParquetRows(t *testing.T, dir string) (int64, []string) {
	t.Helper()
	var rows int64
	var files []string
	eachParquet(t, dir, func(p string, rdr *file.Reader) {
		files = append(files, p)
		rows += rdr.NumRows()
	})
	return rows, files
}
