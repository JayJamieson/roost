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

// NewPqarrowEncoder returns the pure-Go default encoder.
func NewPqarrowEncoder(codec string, rowGroupRows int64) Encoder {
	return &pqEncoder{props: parquet.NewWriterProperties(
		parquet.WithCompression(pqCodec(codec)),
		parquet.WithMaxRowGroupLength(rowGroupRows),
		parquet.WithDictionaryDefault(true),
	)}
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
