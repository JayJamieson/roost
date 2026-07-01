package roost

import (
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// recordBuf is one partition's Arrow record builder plus a row counter. How a
// row becomes columns now lives behind RowAppender (reflection or generated);
// this is just the buffer the Writer fills and snapshots into row groups.
type recordBuf struct {
	b    *array.RecordBuilder
	rows int
}

func newRecordBuf(mem memory.Allocator, schema *arrow.Schema) *recordBuf {
	return &recordBuf{b: array.NewRecordBuilder(mem, schema)}
}

// newRecord snapshots the buffered rows into an immutable record and resets.
func (r *recordBuf) newRecord() arrow.RecordBatch { r.rows = 0; return r.b.NewRecord() }

func (r *recordBuf) release() { r.b.Release() }
