package roost

import (
	"reflect"
	"strings"
	"unsafe"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// appender turns struct values into Arrow rows using a precomputed plan.
// Reflection happens per row but the per-field plan (offsets, typed append
// closures) is built once, so there is no per-call type parsing.
type appender struct {
	b    *array.RecordBuilder
	cols []fieldPlan
	rows int
}

func newAppender(mem memory.Allocator, schema *arrow.Schema, cols []fieldPlan) *appender {
	return &appender{b: array.NewRecordBuilder(mem, schema), cols: cols}
}

// appendRow appends one struct (already dereferenced to a struct Value) using
// the reflect accessors.
func (a *appender) appendRow(rv reflect.Value) {
	for i := range a.cols {
		a.cols[i].appendTo(a.b.Field(i), rv.Field(a.cols[i].structIndex))
	}
	a.rows++
}

// appendRowUnsafe appends one struct addressed by base using the offset-based
// accessors. base must point at a live struct of the planned type for the whole
// call; each accessor reads its field and copies the value into the builder, so
// nothing aliases base afterward.
func (a *appender) appendRowUnsafe(base unsafe.Pointer) {
	for i := range a.cols {
		a.cols[i].appendUnsafe(a.b.Field(i), unsafe.Add(base, a.cols[i].offset))
	}
	a.rows++
}

// newRecord snapshots the buffered rows into an immutable record and resets.
func (a *appender) newRecord() arrow.RecordBatch {
	a.rows = 0
	return a.b.NewRecordBatch()
}

func (a *appender) release() { a.b.Release() }

// partitionPath builds the Hive path segment ("k1=v1/k2=v2") for a row, also
// used as the partition map key. Empty when there are no partition columns.
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
		sb.WriteString(sanitizeSegment(parts[i].format(rv.Field(parts[i].structIndex))))
	}
	return sb.String()
}

// partitionPathUnsafe is the offset-based analogue of partitionPath.
func partitionPathUnsafe(base unsafe.Pointer, parts []fieldPlan) string {
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
		sb.WriteString(sanitizeSegment(parts[i].formatUnsafe(unsafe.Add(base, parts[i].offset))))
	}
	return sb.String()
}

// sanitizeSegment keeps partition values safe for paths and object keys.
func sanitizeSegment(s string) string {
	if s == "" {
		return "__empty__"
	}
	return strings.Map(func(r rune) rune {
		switch r {
		case '/', '\\', '=', ' ', '\t', '\n', '\r', '"', '\'':
			return '_'
		}
		return r
	}, s)
}
