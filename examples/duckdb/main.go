//go:build duckdb

// Command duckdb-example uses the DuckDB encoder (CGO + libduckdb).
// Run: go run -tags duckdb ./examples/duckdb
package main

import (
	"context"
	"log"
	"time"

	"github.com/jayjamieson/roost"
	"github.com/jayjamieson/roost/sink/local"
)

type Row struct {
	ID   int64     `roost:"name=id"`
	Time time.Time `roost:"name=ts"`
	Body []byte
}

func main() {
	sink, err := local.New("./out")
	if err != nil {
		log.Fatal(err)
	}

	w, err := roost.NewWriter[Row](context.Background(), sink,
		roost.WithEncoder(roost.NewDuckDBEncoder("zstd", 122_880)),
		roost.WithRollRows(1_000_000),
	)

	if err != nil {
		log.Fatal(err)
	}
	for i := 0; i < 100_000; i++ {
		_ = w.Append(Row{ID: int64(i), Time: time.Now(), Body: []byte("x")})
	}
	if err := w.Close(); err != nil {
		log.Fatal(err)
	}
}
