package roost_test

import (
	"context"
	"testing"
	"time"

	"github.com/jayjamieson/roost"
	"github.com/jayjamieson/roost/sink/local"
)

func benchLocalThroughput(b *testing.B, fsync bool) {
	dir := b.TempDir()
	sink, err := local.New(dir, local.WithFsync(fsync))
	if err != nil {
		b.Fatal(err)
	}
	// Smaller rolls => more objects => more fsyncs, widening the fsync gap.
	w, err := roost.NewWriter[Row](context.Background(), sink,
		roost.WithCompressionLevel(1),
		roost.WithCodec("snappy"),
		roost.WithRowGroupRows(25_000),
		roost.WithRollRows(100_000),
	)
	if err != nil {
		b.Fatal(err)
	}

	corpus := buildCorpus() // built BEFORE the timer

	b.SetBytes(rowBytes) // framework MB/s = logical input rate
	b.ReportAllocs()
	b.ResetTimer()

	start := time.Now() // measure across all records: first append .. Close
	for i := 0; i < b.N; i++ {
		if err := w.Append(&corpus[i&(corpusSize-1)]); err != nil {
			b.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		b.Fatal(err)
	}
	elapsed := time.Since(start)
	b.StopTimer()

	st := w.Stats()
	if secs := elapsed.Seconds(); secs > 0 {
		b.ReportMetric(float64(b.N)/secs, "rec/s")
		b.ReportMetric(float64(st.Bytes)/secs/1e6, "diskMB/s") // real post-zstd bytes
	}
}

// go test -bench LocalSinkThroughput -benchmem -benchtime=2000000x
func BenchmarkLocalSinkThroughput(b *testing.B) {
	b.Run("fsync=off", func(b *testing.B) { benchLocalThroughput(b, false) })
	b.Run("fsync=on", func(b *testing.B) { benchLocalThroughput(b, true) })
}
