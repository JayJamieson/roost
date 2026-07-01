package roost_test

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/jayjamieson/roost"
	"github.com/jayjamieson/roost/sink/local"
)

type Metric struct {
	TS     time.Time `roost:"name=ts"`
	Host   string    `roost:"name=host"`
	CPU    float64   `roost:"name=cpu"`
	Region string    `roost:"name=region,partition"` // -> region=<v>/...
}

// Basic write to the local filesystem with zstd and a 500k-row roll.
func ExampleWriter() {
	dir, _ := os.MkdirTemp("", "roost-ex")
	defer os.RemoveAll(dir)

	sink, err := local.New(dir)
	if err != nil {
		log.Fatal(err)
	}
	w, err := roost.NewWriter[Metric](context.Background(), sink,
		roost.WithCodec("zstd"),
		roost.WithRollRows(500_000),
	)
	if err != nil {
		log.Fatal(err)
	}
	for i := 0; i < 1000; i++ {
		if err := w.Append(&Metric{TS: time.Now(), Host: "h1", CPU: 0.42, Region: "us-east-1"}); err != nil {
			log.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		log.Fatal(err)
	}
	fmt.Println(w.Stats().Rows)
	// Output: 1000
}

// Hive partitioning: the `region` column lands in the path, one object per
// region here. Read it back in DuckDB with:
//
//	SELECT * FROM read_parquet('<dir>/**/*.parquet', hive_partitioning=true)
//
// and `region` reappears as a derived column.
func ExampleWriter_partitioned() {
	dir, _ := os.MkdirTemp("", "roost-part")
	defer os.RemoveAll(dir)

	sink, _ := local.New(dir)
	w, _ := roost.NewWriter[Metric](context.Background(), sink, roost.WithRollRows(1_000_000))
	for _, r := range []string{"us-east-1", "eu-west-1", "ap-southeast-2"} {
		_ = w.Append(&Metric{TS: time.Now(), Host: "h", CPU: 1, Region: r})
	}
	_ = w.Close()
	fmt.Println(w.Stats().Objects) // one finalized object per region
	// Output: 3
}

// Overlap encode + upload with a 4-worker pool. Append never blocks on
// encoding; Close() drains the workers.
func ExampleWriter_withEncodeConcurrency() {
	dir, _ := os.MkdirTemp("", "roost-conc")
	defer os.RemoveAll(dir)

	sink, _ := local.New(dir)
	w, _ := roost.NewWriter[Metric](context.Background(), sink,
		roost.WithRollRows(10_000),
		roost.WithEncodeConcurrency(4),
	)
	for i := 0; i < 40_000; i++ {
		_ = w.Append(&Metric{TS: time.Now(), Host: "h", CPU: 1, Region: "r"})
	}
	_ = w.Close()
	fmt.Println(w.Stats().Rows)
	// Output: 40000
}
