package roost_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/jayjamieson/roost"
	"github.com/jayjamieson/roost/sink/local"
)

// BenchmarkStreamingMem fixes a small row group and varies the roll size by
// 128_000x. Because the Writer streams each filled row group out and releases
// it, peak heap should stay ~flat across roll sizes (bounded by one row group),
// not scale with rollRows. codec=none isolates the Arrow record memory from the
// compressor window (a separate axis; see WithCompressionLevel). Run with a
// fixed iteration count so the peaks are comparable:
//
//	go test -bench StreamingMem -benchtime=2000000x
func BenchmarkStreamingMem(b *testing.B) {
	base := Row{RSN: 1, Time: time.Now(), Key: "abc123", Value: 3.14, Payload: make([]byte, 64)}
	const rowGroup = 8192
	for _, rollRows := range []int{8192, 1 << 20, 1 << 30} {
		b.Run(fmt.Sprintf("roll=%d", rollRows), func(b *testing.B) {
			w, err := roost.NewWriter[Row](context.Background(), nopSink{},
				roost.WithRollRows(rollRows), roost.WithRowGroupRows(rowGroup), roost.WithCodec("none"))
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

// TestStreamingHugeRoll exercises the streaming path that the benchmark measures:
// one object spanning many row groups (rollRows far larger than rowGroupRows).
// All rows must be present in a single file.
func TestStreamingHugeRoll(t *testing.T) {
	dir := t.TempDir()
	sink, err := local.New(dir)
	if err != nil {
		t.Fatal(err)
	}
	w, err := roost.NewWriter[Row](context.Background(), sink,
		roost.WithRollRows(1<<30), roost.WithRowGroupRows(1000), roost.WithCodec("snappy"))
	if err != nil {
		t.Fatal(err)
	}
	const n = 25_000 // -> 25 row groups in a single object
	row := Row{Key: "k", Payload: []byte("p"), Time: time.Now()}
	for i := 0; i < n; i++ {
		row.RSN = int64(i)
		if err := w.Append(&row); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	var files int
	var rows int64
	eachParquet(t, dir, func(p string, rdr *file.Reader) {
		files++
		rows += rdr.NumRows()
		if got := rdr.NumRowGroups(); got < 20 {
			t.Errorf("expected many row groups in one object, got %d", got)
		}
	})
	if files != 1 {
		t.Fatalf("expected exactly 1 object, got %d", files)
	}
	if rows != n {
		t.Fatalf("rows = %d, want %d", rows, n)
	}
}
