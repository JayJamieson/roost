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
	// PartitionInto appends v's sanitized Hive key ("region=us-east-1/dt=...")
	// to dst and returns the extended slice (append-style), appending nothing
	// when there are no partition columns. The Writer builds keys into a reused
	// buffer through this, so a steady stream into already-open partitions
	// appends with zero allocations. roostgen's generated keys must stay
	// byte-identical to the reflection path (the equivalence test enforces it).
	PartitionInto(v *T, dst []byte) []byte
	// Append appends one row's data columns into b, in Schema() field order.
	Append(v *T, b *array.RecordBuilder)
}

// reflectAppender is the default RowAppender: it interprets the reflected plan
// per row. The per-field plan (offsets, typed closures) is built once by
// buildPlan, but the boxing reflect.ValueOf/Interface forces remain per row -
// which is exactly what roostgen removes.
type reflectAppender[T any] struct{ pl *plan }

func (a reflectAppender[T]) Schema() *arrow.Schema { return a.pl.fileSchema }

func (a reflectAppender[T]) PartitionInto(v *T, dst []byte) []byte {
	parts := a.pl.partCols
	if len(parts) == 0 {
		return dst
	}
	rv := structOf(v)
	for i := range parts {
		if i > 0 {
			dst = append(dst, '/')
		}
		dst = append(dst, parts[i].name...)
		dst = append(dst, '=')
		dst = AppendSanitized(dst, parts[i].format(rv.Field(parts[i].structIndex)))
	}
	return dst
}

func (a reflectAppender[T]) Append(v *T, b *array.RecordBuilder) {
	rv := structOf(v)
	for i := range a.pl.dataCols {
		a.pl.dataCols[i].appendTo(b.Field(i), rv.Field(a.pl.dataCols[i].structIndex))
	}
}

// structOf dereferences *T to the underlying struct value. Callers guarantee v
// is non-nil (Writer.Append rejects nil pointers up front).
func structOf[T any](v *T) reflect.Value {
	return reflect.ValueOf(v).Elem()
}
