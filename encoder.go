package roost

import (
	"context"
	"io"

	"github.com/apache/arrow-go/v18/arrow"
)

// Encoder writes a set of Arrow records as one Parquet object to dst. It must
// NOT close dst — the Writer owns the sink WriteCloser's lifecycle.
type Encoder interface {
	EncodeObject(ctx context.Context, dst io.Writer, schema *arrow.Schema, recs []arrow.RecordBatch) error
}

// writerOnly hides Close/Seek from an encoder's downstream library so it
// cannot close or seek the sink's WriteCloser out from under the Writer.
type writerOnly struct{ w io.Writer }

func (x writerOnly) Write(p []byte) (int, error) { return x.w.Write(p) }
