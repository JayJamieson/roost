package roost

import (
	"reflect"
	"strings"

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

// partitionPath builds the Hive path segment ("k1=v1/k2=v2") for a row, also
// used as the partition map key. Empty when there are no partition columns.
// This backs the reflection RowAppender; the generated one inlines the same
// logic with direct field access.
func partitionPath(rv reflect.Value, parts []fieldPlan) string {
	if len(parts) == 0 {
		return ""
	}
	var sb strings.Builder
	for i := range parts {
		if i > 0 {
			sb.WriteByte('/')
		}
		sb.WriteString(parts[i].name)
		sb.WriteByte('=')
		sb.WriteString(SanitizeSegment(parts[i].format(rv.Field(parts[i].structIndex))))
	}
	return sb.String()
}
