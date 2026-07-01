package roost_test

import (
	"context"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/jayjamieson/roost"
)

// Ev is a log/event-shaped row used to compare codecs on realistic data.
type Ev struct {
	ID      int64     `roost:"name=id"`
	TS      time.Time `roost:"name=ts"`
	Level   string    `roost:"name=level"`
	Service string    `roost:"name=service"`
	Msg     string    `roost:"name=msg"`
	Latency float64   `roost:"name=latency"`
	Payload []byte    `roost:"name=payload"`
}

// buildEv generates n deterministic rows for a profile and returns them plus the
// logical (pre-encode) byte size, so a benchmark can report a compression ratio.
//
//	"logs"  - repetitive templates, low-cardinality labels, no payload: compressible
//	"blobs" - random message + random 64B payload: high entropy, barely compressible
func buildEv(profile string, n int) ([]Ev, int64) {
	rng := rand.New(rand.NewSource(1))
	levels := []string{"INFO", "WARN", "ERROR", "DEBUG"}
	svcs := []string{"api", "auth", "billing", "cache", "db", "ingest", "search", "worker"}
	tmpl := []string{
		"request completed in %dms", "user %d authenticated",
		"cache miss for key shard-%d", "retrying upstream attempt %d",
		"flushed %d rows to storage", "connection pool size now %d",
	}
	const fixed = 8 + 8 + 5 + 8 + 8 // id + ts + level + service + latency (approx)
	rows := make([]Ev, n)
	var logical int64
	base := time.Unix(1700000000, 0)
	for i := range rows {
		r := Ev{
			ID:      int64(i),
			TS:      base.Add(time.Duration(i) * time.Millisecond),
			Level:   levels[rng.Intn(len(levels))],
			Service: svcs[rng.Intn(len(svcs))],
			Latency: rng.Float64() * 1000,
		}
		if profile == "blobs" {
			p := make([]byte, 64)
			rng.Read(p)
			r.Payload = p
			m := make([]byte, 24)
			rng.Read(m)
			r.Msg = string(m)
			logical += fixed + 64 + 24
		} else { // "logs"
			r.Msg = fmt.Sprintf(tmpl[rng.Intn(len(tmpl))], rng.Intn(500))
			logical += fixed + int64(len(r.Msg))
		}
		rows[i] = r
	}
	return rows, logical
}

// BenchmarkCodec compares Parquet codecs on compressible (logs) vs incompressible
// (blobs) data. Each sub-benchmark re-encodes the same corpus and reports:
//
//	ratio  - logical input bytes / encoded bytes (higher = smaller files)
//	outMB  - encoded object size
//	peakMB - peak in-use heap (encoder window dominates; corpus is a constant baseline)
//	MB/s   - input throughput (b.SetBytes), plus rec/s
//
// Run:  go test -bench Codec -benchmem -benchtime=10x
func BenchmarkCodec(b *testing.B) {
	type variant struct {
		name string
		opts []roost.Option
	}
	variants := []variant{
		{"none", []roost.Option{roost.WithCodec("none")}},
		{"snappy", []roost.Option{roost.WithCodec("snappy")}},
		{"zstd", []roost.Option{roost.WithCodec("zstd")}},
		{"zstd-lvl1", []roost.Option{roost.WithCodec("zstd"), roost.WithCompressionLevel(1)}},
	}
	const rows = 200_000
	for _, profile := range []string{"logs", "blobs"} {
		corpus, logical := buildEv(profile, rows)
		for _, v := range variants {
			b.Run(profile+"/"+v.name, func(b *testing.B) {
				b.SetBytes(logical) // -> framework MB/s is input (logical) throughput
				b.ReportAllocs()

				stop := make(chan struct{})
				pk := make(chan uint64, 1)
				go func() { pk <- samplePeakHeap(stop) }()

				var outBytes int64
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					opts := append([]roost.Option{
						roost.WithRowGroupRows(100_000), roost.WithRollRows(rows),
					}, v.opts...)
					w, err := roost.NewWriter[Ev](context.Background(), nopSink{}, opts...)
					if err != nil {
						b.Fatal(err)
					}
					for j := range corpus {
						if err := w.Append(&corpus[j]); err != nil {
							b.Fatal(err)
						}
					}
					if err := w.Close(); err != nil {
						b.Fatal(err)
					}
					outBytes = w.Stats().Bytes
				}
				b.StopTimer()
				close(stop)
				peak := <-pk

				b.ReportMetric(float64(logical)/float64(outBytes), "ratio")
				b.ReportMetric(float64(outBytes)/1e6, "outMB")
				b.ReportMetric(float64(peak)/(1<<20), "peakMB")
				b.ReportMetric(float64(rows)*float64(b.N)/b.Elapsed().Seconds(), "rec/s")
			})
		}
	}
}
