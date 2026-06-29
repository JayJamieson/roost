package roost_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/jayjamieson/roost"
	"github.com/jayjamieson/roost/sink/local"
)

// TestConcurrentManyPartitionsStress hammers the per-partition encode pipeline:
// more partitions than maxOpenPartitions (forcing eviction+finalize churn),
// high concurrency, and multi-row-group objects. Run under -race to catch data
// races/deadlocks in the goroutine handoff. All rows must survive.
func TestConcurrentManyPartitionsStress(t *testing.T) {
	dir := t.TempDir()
	sink, err := local.New(dir)
	if err != nil {
		t.Fatal(err)
	}
	w, err := roost.NewWriter[Event](context.Background(), sink,
		roost.WithRowGroupRows(100), // small row groups -> many handoffs
		roost.WithRollRows(500),     // multiple row groups per object
		roost.WithMaxOpenPartitions(8),
		roost.WithEncodeConcurrency(8),
		roost.WithCodec("zstd"))
	if err != nil {
		t.Fatal(err)
	}

	regions := make([]string, 20) // > maxOpenPartitions => constant eviction
	for i := range regions {
		regions[i] = fmt.Sprintf("r%02d", i)
	}

	const n = 24_000
	for i := 0; i < n; i++ {
		row := Event{RSN: int64(i), Time: time.Now(), Region: regions[i%len(regions)], Body: []byte("payload")}
		if err := w.Append(row); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	if rows, _ := countParquetRows(t, dir); rows != n {
		t.Fatalf("rows = %d, want %d", rows, n)
	}
}
