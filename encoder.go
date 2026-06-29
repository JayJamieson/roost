package roost

import (
	"context"
	"io"

	"github.com/apache/arrow-go/v18/arrow"
)

// Encoder opens a per-object encoder. The streaming shape lets the Writer hand
// over one row group at a time and release it immediately, instead of holding
// every record for an object until the roll boundary.
type Encoder interface {
	// Open begins one Parquet object written to dst. The encoder must NOT
	// close dst — the Writer owns the sink WriteCloser's lifecycle.
	Open(ctx context.Context, dst io.Writer, schema *arrow.Schema) (ObjectEncoder, error)
}

// ObjectEncoder encodes a single Parquet object.
type ObjectEncoder interface {
	// Write encodes one record as a row group. After Write returns the Writer
	// may release the record, so an encoder that needs it longer must Retain.
	Write(rec arrow.RecordBatch) error
	// Close finalizes the object (writes the Parquet footer). It must NOT
	// close the underlying dst.
	Close() error
}

// writerOnly hides Close/Seek from an encoder's downstream library so it
// cannot close or seek the sink's WriteCloser out from under the Writer.
type writerOnly struct{ w io.Writer }

func (x writerOnly) Write(p []byte) (int, error) { return x.w.Write(p) }
