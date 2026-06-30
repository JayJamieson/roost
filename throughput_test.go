package roost_test

import (
	"context"
	"math/rand"
	"testing"
	"time"

	"github.com/jayjamieson/roost"
	"github.com/jayjamieson/roost/sink/local"
)

type LocalRow struct {
	RSN     int64 `roost:"name=rsn"`
	Time    time.Time
	Key     string
	Value   float64
	Payload []byte
}

const localRowBytes = int64(8 + 8 + 16 + 8 + 64)

// corpusSize must exceed the row-group size so no row repeats within a group
// (keeps zstd honest). Power of two so we can index with a cheap mask.
const corpusSize = 1 << 15 // 32768

// buildCorpus generates varied, incompressible rows up front so the timed
// loop measures roost, not data generation.
func buildCorpus() []LocalRow {
	rng := rand.New(rand.NewSource(1))
	keys := make([]string, 256)
	for i := range keys {
		b := make([]byte, 12)
		for j := range b {
			b[j] = byte('a' + rng.Intn(26))
		}
		keys[i] = string(b)
	}
	rows := make([]LocalRow, corpusSize)
	for i := range rows {
		p := make([]byte, 64)
		rng.Read(p) // incompressible payload
		rows[i] = LocalRow{
			RSN:     int64(i),
			Time:    time.Unix(0, rng.Int63()),
			Key:     keys[rng.Intn(len(keys))],
			Value:   rng.Float64(),
			Payload: p,
		}
	}
	return rows
}

func benchLocalThroughput(b *testing.B, fsync bool) {
	dir := b.TempDir()
	sink, err := local.New(dir, local.WithFsync(fsync))
	if err != nil {
		b.Fatal(err)
	}
	// Smaller rolls => more objects => more fsyncs, widening the fsync gap.
	w, err := roost.NewWriter[LocalRow](context.Background(), sink,
		roost.WithCompressionLevel(1),
		roost.WithCodec("snappy"),
		roost.WithRowGroupRows(25_000),
		roost.WithRollRows(100_000),
	)
	if err != nil {
		b.Fatal(err)
	}

	corpus := buildCorpus() // built BEFORE the timer

	b.SetBytes(localRowBytes) // framework MB/s = logical input rate
	b.ReportAllocs()
	b.ResetTimer()

	start := time.Now() // measure across all records: first append .. Close
	for i := 0; i < b.N; i++ {
		if err := w.Append(corpus[i&(corpusSize-1)]); err != nil {
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
