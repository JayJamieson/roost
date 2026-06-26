//go:build duckdb

// DuckDB encoder: registers the Arrow records as a view and COPYs straight to
// a temp Parquet file using DuckDB's C++ writer, then streams it to dst.
// Build with -tags duckdb; needs CGO + github.com/marcboeker/go-duckdb/v2.
package roost

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	duckdb "github.com/duckdb/duckdb-go/v2"
)

type duckEncoder struct {
	codec        string
	rowGroupRows int64
}

// NewDuckDBEncoder returns an encoder backed by DuckDB's Parquet writer.
func NewDuckDBEncoder(codec string, rowGroupRows int64) Encoder {
	return &duckEncoder{codec: codec, rowGroupRows: rowGroupRows}
}

func (e *duckEncoder) EncodeObject(ctx context.Context, dst io.Writer, schema *arrow.Schema, recs []arrow.RecordBatch) error {
	dir, err := os.MkdirTemp("", "roost-duck")
	if err != nil {
		return err
	}
	defer os.RemoveAll(dir)
	out := filepath.Join(dir, "o.parquet")

	db, err := sql.Open("duckdb", filepath.Join(dir, "d.duckdb"))
	if err != nil {
		return err
	}
	defer db.Close()
	conn, err := db.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()

	rr, err := array.NewRecordReader(schema, recs)
	if err != nil {
		return err
	}
	defer rr.Release()

	q := fmt.Sprintf("COPY (SELECT * FROM v) TO '%s' (FORMAT parquet, COMPRESSION %s, ROW_GROUP_SIZE %d)",
		out, duckCodec(e.codec), e.rowGroupRows)

	if err := conn.Raw(func(dc any) error {
		ar, err := duckdb.NewArrowFromConn(dc.(driver.Conn))
		if err != nil {
			return err
		}
		release, err := ar.RegisterView(rr, "v")
		if err != nil {
			return err
		}
		defer release()
		ec, ok := dc.(driver.ExecerContext)
		if !ok {
			return fmt.Errorf("roost: duckdb conn is not an ExecerContext")
		}
		_, err = ec.ExecContext(ctx, q, nil)
		return err
	}); err != nil {
		return err
	}

	f, err := os.Open(out)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = io.Copy(dst, f)
	return err
}

func duckCodec(name string) string {
	switch name {
	case "none":
		return "uncompressed"
	case "snappy":
		return "snappy"
	default:
		return "zstd"
	}
}
