package roost

import (
	"reflect"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

// RowAppender turns values of T into Arrow rows and Hive partition paths. The
// reflection implementation is the zero-setup default (see NewWriter); roostgen
// emits a faster, allocation-free one used via NewWriterFor.
//
// The Writer pipeline (per-partition builders, row-group flushing, rolling,
// encoder, sink, stats) consumes only this interface, so both paths are
// otherwise identical.
type RowAppender[T any] interface {
	// Schema is the Parquet file schema, partition columns excluded (Hive
	// convention). It is built once and must be stable for the appender's life.
	Schema() *arrow.Schema
	// Partition returns the sanitized Hive path segment for v
	// ("region=us-east-1/dt=2026-06-29"), or "" when there are no partition
	// columns.
	Partition(v *T) string
	// Append appends one row's data columns into b, in Schema() field order.
	Append(v *T, b *array.RecordBuilder)
}

// reflectAppender is the default RowAppender: it interprets the reflected plan
// per row. The per-field plan (offsets, typed closures) is built once by
// buildPlan, but the boxing reflect.ValueOf/Interface forces remain per row —
// which is exactly what roostgen removes.
type reflectAppender[T any] struct{ pl *plan }

func (a reflectAppender[T]) Schema() *arrow.Schema { return a.pl.fileSchema }

func (a reflectAppender[T]) Partition(v *T) string {
	if len(a.pl.partCols) == 0 {
		return ""
	}
	return partitionPath(structOf(v), a.pl.partCols)
}

func (a reflectAppender[T]) Append(v *T, b *array.RecordBuilder) {
	rv := structOf(v)
	for i := range a.pl.dataCols {
		a.pl.dataCols[i].appendTo(b.Field(i), rv.Field(a.pl.dataCols[i].structIndex))
	}
}

// structOf dereferences *T to the underlying struct value, unwrapping any extra
// pointer indirection when T is itself a pointer type. Callers guarantee the
// chain is non-nil (Writer.Append rejects nil pointers up front).
func structOf[T any](v *T) reflect.Value {
	rv := reflect.ValueOf(v).Elem()
	for rv.Kind() == reflect.Pointer {
		rv = rv.Elem()
	}
	return rv
}
