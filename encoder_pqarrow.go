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

// NewPqarrowEncoder returns the pure-Go default encoder. Dictionary encoding is
// off by default and enabled only for the columns in dictCols (see
// WithDictionaryColumns and the `dict` struct tag), so high-cardinality columns
// don't pay for a dictionary that never compresses.
func NewPqarrowEncoder(codec string, rowGroupRows int64, dictCols ...string) Encoder {
	props := []parquet.WriterProperty{
		parquet.WithCompression(pqCodec(codec)),
		parquet.WithMaxRowGroupLength(rowGroupRows),
		parquet.WithDictionaryDefault(false),
	}
	for _, c := range dictCols {
		props = append(props, parquet.WithDictionaryFor(c, true))
	}
	return &pqEncoder{props: parquet.NewWriterProperties(props...)}
}

func (e *pqEncoder) EncodeObject(_ context.Context, dst io.Writer, schema *arrow.Schema, recs []arrow.RecordBatch) error {
	fw, err := pqarrow.NewFileWriter(schema, writerOnly{dst}, e.props, pqarrow.DefaultWriterProps())
	if err != nil {
		return err
	}
	for _, r := range recs {
		if err := fw.WriteBuffered(r); err != nil {
			fw.Close()
			return err
		}
	}
	return fw.Close()
}

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
