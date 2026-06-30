//go:build duckdb

// DuckDB encoder: buffers an object's records (DuckDB's COPY consumes the whole
// set at once), then registers them as a view and COPYs to a temp Parquet file
// using DuckDB's C++ writer, streaming the result to dst on Close. Unlike the
// pqarrow encoder this holds the records until Close — that buffering is a
// DuckDB constraint. Build with -tags duckdb; needs CGO + go-duckdb/v2.
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

func (e *duckEncoder) Open(ctx context.Context, dst io.Writer, schema *arrow.Schema) (ObjectEncoder, error) {
	return &duckObject{e: e, ctx: ctx, dst: dst, schema: schema}, nil
}

type duckObject struct {
	e      *duckEncoder
	ctx    context.Context
	dst    io.Writer
	schema *arrow.Schema
	recs   []arrow.RecordBatch
}

// Write retains rec because the COPY at Close needs the full record set.
func (o *duckObject) Write(rec arrow.RecordBatch) error {
	rec.Retain()
	o.recs = append(o.recs, rec)
	return nil
}

// Close runs the register-view + COPY and streams the result to dst, then
// releases the retained records. When the Writer uses an encode pool this runs
// on a worker, so the DuckDB encode stays off the Append goroutine.
func (o *duckObject) Close() error {
	defer func() {
		for _, r := range o.recs {
			r.Release()
		}
		o.recs = nil
	}()

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
	conn, err := db.Conn(o.ctx)
	if err != nil {
		return err
	}
	defer conn.Close()

	rr, err := array.NewRecordReader(o.schema, o.recs)
	if err != nil {
		return err
	}
	defer rr.Release()

	q := fmt.Sprintf("COPY (SELECT * FROM v) TO '%s' (FORMAT parquet, COMPRESSION %s, ROW_GROUP_SIZE %d)",
		out, duckCodec(o.e.codec), o.e.rowGroupRows)

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
		_, err = ec.ExecContext(o.ctx, q, nil)
		return err
	}); err != nil {
		return err
	}

	f, err := os.Open(out)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = io.Copy(o.dst, f)
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
