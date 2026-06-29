package roost

import (
	"context"
	"io"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
)

type pqEncoder struct{ props *parquet.WriterProperties }

// NewPqarrowEncoder returns the pure-Go default encoder. It streams: each
// record is flushed as its own row group and may be released immediately, so
// the Writer never holds more than the current row group per partition.
//
// Dictionary encoding is off by default and enabled only for the columns in
// dictCols (see WithDictionaryColumns and the `dict` struct tag), so
// high-cardinality columns don't pay for a dictionary that never compresses.
func NewPqarrowEncoder(codec string, rowGroupRows int64, opts ...PqarrowOption) Encoder {
	var c pqConfig
	for _, o := range opts {
		o(&c)
	}
	props := []parquet.WriterProperty{
		parquet.WithCompression(pqCodec(codec)),
		parquet.WithMaxRowGroupLength(rowGroupRows),
		parquet.WithDictionaryDefault(false),
	}
	if c.level != 0 {
		props = append(props, parquet.WithCompressionLevel(c.level))
	}
	for _, col := range c.dictCols {
		props = append(props, parquet.WithDictionaryFor(col, true))
	}
	return &pqEncoder{props: parquet.NewWriterProperties(props...)}
}

// PqarrowOption configures the default pqarrow encoder.
type PqarrowOption func(*pqConfig)

type pqConfig struct {
	dictCols []string
	level    int
}

// WithDictionaryFor enables dictionary encoding for the named columns.
func PqarrowDictionaryColumns(cols ...string) PqarrowOption {
	return func(c *pqConfig) { c.dictCols = append(c.dictCols, cols...) }
}

// PqarrowCompressionLevel sets the codec compression level (0 = codec default).
func PqarrowCompressionLevel(level int) PqarrowOption {
	return func(c *pqConfig) { c.level = level }
}

func (e *pqEncoder) Open(_ context.Context, dst io.Writer, schema *arrow.Schema) (ObjectEncoder, error) {
	fw, err := pqarrow.NewFileWriter(schema, writerOnly{dst}, e.props, pqarrow.DefaultWriterProps())
	if err != nil {
		return nil, err
	}
	return &pqObject{fw: fw}, nil
}

type pqObject struct{ fw *pqarrow.FileWriter }

// Write flushes rec as a row group immediately (Write, not WriteBuffered), so
// nothing accumulates on the heap between row groups; the Writer releases rec
// right after this returns.
func (o *pqObject) Write(rec arrow.RecordBatch) error { return o.fw.Write(rec) }

// Close writes the footer only; writerOnly keeps it from closing dst.
func (o *pqObject) Close() error { return o.fw.Close() }

func pqCodec(name string) compress.Compression {
	switch name {
	case "snappy":
		return compress.Codecs.Snappy
	case "zstd":
		return compress.Codecs.Zstd
	default:
		return compress.Codecs.Uncompressed
	}
}
