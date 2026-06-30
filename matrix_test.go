package roost_test

import (
	"context"
	"fmt"
	"os"
	"runtime"
	"strconv"
	"testing"
	"time"

	"github.com/jayjamieson/roost"
)

func envInt(k string, def int) int {
	if v := os.Getenv(k); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

// samplePeakInuse polls HeapInuse (in-use heap spans - steadier than HeapAlloc)
// until stop is closed and returns the max seen.
func samplePeakInuse(stop <-chan struct{}) uint64 {
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

// TestMemThroughputMatrix runs ONE config (from env) so an external
// /usr/bin/time -l wrapper can attribute RSS to it. It prints a CSV line:
//
//	CSV,conc,level,recPerSec,inputMBps,diskMBps,peakHeapMB
//
// Config: ROOST_CONC, ROOST_LEVEL (0=codec default), ROOST_N, ROOST_CODEC (none, snappy, zstd). Roll == row group
// (one row group per object) so objects turn over and encode concurrency - which
// parallelizes across objects, not within one object - is actually exercised.
// Incompressible payload (buildCorpus) makes zstd do real work.
func TestMemThroughputMatrix(t *testing.T) {
	if os.Getenv("ROOST_MATRIX") == "" {
		t.Skip("set ROOST_MATRIX=1 to run the tuning matrix")
	}
	conc := envInt("ROOST_CONC", 1)
	codec := os.Getenv("ROOST_CODEC")
	level := envInt("ROOST_LEVEL", 0)
	n := envInt("ROOST_N", 2_000_000)

	corpus := buildCorpus()
	opts := []roost.Option{
		roost.WithCodec(codec),
		roost.WithRowGroupRows(25_000),
		roost.WithRollRows(25_000),
	}
	if conc > 1 {
		opts = append(opts, roost.WithEncodeConcurrency(conc))
	}
	if level != 0 {
		opts = append(opts, roost.WithCompressionLevel(level))
	}
	w, err := roost.NewWriter[LocalRow](context.Background(), nopSink{}, opts...)
	if err != nil {
		t.Fatal(err)
	}

	stop := make(chan struct{})
	peakCh := make(chan uint64, 1)
	go func() { peakCh <- samplePeakInuse(stop) }()

	start := time.Now()
	for i := 0; i < n; i++ {
		if err := w.Append(corpus[i&(corpusSize-1)]); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	elapsed := time.Since(start)
	close(stop)
	peak := <-peakCh

	st := w.Stats()
	secs := elapsed.Seconds()
	fmt.Printf("CSV,%d,%d,%.0f,%.1f,%.1f,%.1f\n",
		conc, level,
		float64(n)/secs,
		float64(n)*float64(localRowBytes)/secs/1e6,
		float64(st.Bytes)/secs/1e6,
		float64(peak)/(1<<20))
}
