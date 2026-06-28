package roost

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"
	"unsafe"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

var timeType = reflect.TypeOf(time.Time{})

// fieldPlan is the precomputed instruction for one struct field. It carries two
// parallel field accessors: reflect-based (appendTo/format, used by Append and
// AppendPtr) and offset-based (appendUnsafe/formatUnsafe, used by AppendUnsafe).
type fieldPlan struct {
	structIndex  int
	offset       uintptr // byte offset of the field within the struct (unsafe path)
	name         string
	dtype        arrow.DataType
	nullable     bool
	partition    bool
	appendTo     func(b array.Builder, fv reflect.Value)
	appendUnsafe func(b array.Builder, fieldPtr unsafe.Pointer)
	format       func(fv reflect.Value) string        // partition value -> path segment
	formatUnsafe func(fieldPtr unsafe.Pointer) string // partition value -> path segment
}

// fieldCodec bundles the arrow type and the four accessors mapField produces
// for one Go field, so the function isn't returning a seven-value tuple.
type fieldCodec struct {
	dtype        arrow.DataType
	nullable     bool
	appendTo     func(array.Builder, reflect.Value)
	appendUnsafe func(array.Builder, unsafe.Pointer)
	format       func(reflect.Value) string  // nil if the type can't be a partition column
	formatUnsafe func(unsafe.Pointer) string // nil if the type can't be a partition column
}

// plan is the reflected layout for a struct type T.
type plan struct {
	fileSchema *arrow.Schema // excludes partition columns (Hive convention)
	dataCols   []fieldPlan   // order matches fileSchema fields
	partCols   []fieldPlan
	dictCols   []string // data column names tagged for dictionary encoding
}

func buildPlan(t reflect.Type) (*plan, error) {
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	if t.Kind() != reflect.Struct {
		return nil, fmt.Errorf("roost: type %s is not a struct", t)
	}
	var data, parts []fieldPlan
	var fields []arrow.Field
	var dictCols []string
	for i := 0; i < t.NumField(); i++ {
		sf := t.Field(i)
		if sf.PkgPath != "" {
			continue // unexported
		}
		name, partition, omit, dict := parseTag(sf)
		if omit {
			continue
		}
		fc, err := mapField(sf.Type)
		if err != nil {
			return nil, fmt.Errorf("roost: field %s: %w", sf.Name, err)
		}
		fp := fieldPlan{
			structIndex: i, offset: sf.Offset, name: name,
			dtype: fc.dtype, nullable: fc.nullable, partition: partition,
			appendTo: fc.appendTo, appendUnsafe: fc.appendUnsafe,
			format: fc.format, formatUnsafe: fc.formatUnsafe,
		}
		if partition {
			if fc.format == nil {
				return nil, fmt.Errorf("roost: field %s type %s cannot be a partition column", sf.Name, sf.Type)
			}
			parts = append(parts, fp)
			continue
		}
		data = append(data, fp)
		fields = append(fields, arrow.Field{Name: name, Type: fc.dtype, Nullable: fc.nullable})
		if dict {
			dictCols = append(dictCols, name)
		}
	}
	if len(data) == 0 {
		return nil, fmt.Errorf("roost: struct %s has no data columns", t)
	}
	return &plan{fileSchema: arrow.NewSchema(fields, nil), dataCols: data, partCols: parts, dictCols: dictCols}, nil
}

func parseTag(sf reflect.StructField) (name string, partition, omit, dict bool) {
	name = sf.Name
	tag := sf.Tag.Get("roost")
	if tag == "" {
		return name, false, false, false
	}
	for _, p := range strings.Split(tag, ",") {
		p = strings.TrimSpace(p)
		switch {
		case p == "-" || p == "omit":
			omit = true
		case p == "partition":
			partition = true
		case p == "dict":
			dict = true
		case strings.HasPrefix(p, "name="):
			name = strings.TrimPrefix(p, "name=")
		}
	}
	return name, partition, omit, dict
}

// elem dereferences a pointer field, reporting ok=false for a nil pointer.
func elem(fv reflect.Value) (reflect.Value, bool) {
	if fv.Kind() == reflect.Pointer {
		if fv.IsNil() {
			return reflect.Value{}, false
		}
		return fv.Elem(), true
	}
	return fv, true
}

// deref reads the pointer stored in a nullable (pointer-typed) field, reporting
// ok=false when it is nil.
func deref(fieldPtr unsafe.Pointer) (unsafe.Pointer, bool) {
	p := *(*unsafe.Pointer)(fieldPtr)
	return p, p != nil
}

// reflectAppender wraps a per-element setter with nil handling. set receives the
// already-dereferenced element value.
func reflectAppender(set func(array.Builder, reflect.Value)) func(array.Builder, reflect.Value) {
	return func(b array.Builder, fv reflect.Value) {
		if e, ok := elem(fv); ok {
			set(b, e)
		} else {
			b.AppendNull()
		}
	}
}

// reflectFormatter is the partition-path analogue of reflectAppender.
func reflectFormatter(format func(reflect.Value) string) func(reflect.Value) string {
	return func(fv reflect.Value) string {
		if e, ok := elem(fv); ok {
			return format(e)
		}
		return "null"
	}
}

// unsafeAppender builds the offset-based appender for a field of concrete type
// V. For a nullable field the field memory holds a *V, so it derefs first;
// otherwise it reinterprets the field memory as V directly. set copies the value
// into the builder, so the source struct need not outlive the call.
func unsafeAppender[V any](nullable bool, set func(array.Builder, V)) func(array.Builder, unsafe.Pointer) {
	if nullable {
		return func(b array.Builder, fieldPtr unsafe.Pointer) {
			if p, ok := deref(fieldPtr); ok {
				set(b, *(*V)(p))
			} else {
				b.AppendNull()
			}
		}
	}
	return func(b array.Builder, fieldPtr unsafe.Pointer) {
		set(b, *(*V)(fieldPtr))
	}
}

// unsafeFormatter is the partition-path analogue of unsafeAppender.
func unsafeFormatter[V any](nullable bool, format func(V) string) func(unsafe.Pointer) string {
	if nullable {
		return func(fieldPtr unsafe.Pointer) string {
			if p, ok := deref(fieldPtr); ok {
				return format(*(*V)(p))
			}
			return "null"
		}
	}
	return func(fieldPtr unsafe.Pointer) string {
		return format(*(*V)(fieldPtr))
	}
}

// mapField maps a Go field type to its arrow type plus the reflect- and
// offset-based accessors used by the append strategies.
func mapField(t reflect.Type) (fieldCodec, error) {
	nullable := false
	if t.Kind() == reflect.Pointer {
		nullable = true
		t = t.Elem()
	}

	switch {
	case t == timeType:
		return fieldCodec{
			dtype:    &arrow.TimestampType{Unit: arrow.Microsecond},
			nullable: nullable,
			appendTo: reflectAppender(func(b array.Builder, e reflect.Value) {
				b.(*array.TimestampBuilder).Append(arrow.Timestamp(e.Interface().(time.Time).UnixMicro()))
			}),
			appendUnsafe: unsafeAppender(nullable, func(b array.Builder, v time.Time) {
				b.(*array.TimestampBuilder).Append(arrow.Timestamp(v.UnixMicro()))
			}),
			format: reflectFormatter(func(e reflect.Value) string {
				return e.Interface().(time.Time).UTC().Format("2006-01-02")
			}),
			formatUnsafe: unsafeFormatter(nullable, func(v time.Time) string {
				return v.UTC().Format("2006-01-02")
			}),
		}, nil

	case t.Kind() == reflect.Slice && t.Elem().Kind() == reflect.Uint8: // []byte -> Binary
		return fieldCodec{
			dtype:    arrow.BinaryTypes.Binary,
			nullable: nullable,
			appendTo: reflectAppender(func(b array.Builder, e reflect.Value) {
				b.(*array.BinaryBuilder).Append(e.Bytes())
			}),
			appendUnsafe: unsafeAppender(nullable, func(b array.Builder, v []byte) {
				b.(*array.BinaryBuilder).Append(v)
			}),
		}, nil
	}

	switch t.Kind() {
	case reflect.String:
		return fieldCodec{
			dtype:    arrow.BinaryTypes.String,
			nullable: nullable,
			appendTo: reflectAppender(func(b array.Builder, e reflect.Value) {
				b.(*array.StringBuilder).Append(e.String())
			}),
			appendUnsafe: unsafeAppender(nullable, func(b array.Builder, v string) {
				b.(*array.StringBuilder).Append(v)
			}),
			format:       reflectFormatter(func(e reflect.Value) string { return e.String() }),
			formatUnsafe: unsafeFormatter(nullable, func(v string) string { return v }),
		}, nil
	case reflect.Bool:
		return fieldCodec{
			dtype:    arrow.FixedWidthTypes.Boolean,
			nullable: nullable,
			appendTo: reflectAppender(func(b array.Builder, e reflect.Value) {
				b.(*array.BooleanBuilder).Append(e.Bool())
			}),
			appendUnsafe: unsafeAppender(nullable, func(b array.Builder, v bool) {
				b.(*array.BooleanBuilder).Append(v)
			}),
		}, nil
	case reflect.Int:
		return intCodec[int](nullable), nil
	case reflect.Int64:
		return intCodec[int64](nullable), nil
	case reflect.Int32:
		return fieldCodec{
			dtype:    arrow.PrimitiveTypes.Int32,
			nullable: nullable,
			appendTo: reflectAppender(func(b array.Builder, e reflect.Value) {
				b.(*array.Int32Builder).Append(int32(e.Int()))
			}),
			appendUnsafe: unsafeAppender(nullable, func(b array.Builder, v int32) {
				b.(*array.Int32Builder).Append(v)
			}),
			format:       reflectFormatter(func(e reflect.Value) string { return strconv.FormatInt(e.Int(), 10) }),
			formatUnsafe: unsafeFormatter(nullable, func(v int32) string { return strconv.FormatInt(int64(v), 10) }),
		}, nil
	case reflect.Uint:
		return uintCodec[uint](nullable), nil
	case reflect.Uint64:
		return uintCodec[uint64](nullable), nil
	case reflect.Uint32:
		return fieldCodec{
			dtype:    arrow.PrimitiveTypes.Uint32,
			nullable: nullable,
			appendTo: reflectAppender(func(b array.Builder, e reflect.Value) {
				b.(*array.Uint32Builder).Append(uint32(e.Uint()))
			}),
			appendUnsafe: unsafeAppender(nullable, func(b array.Builder, v uint32) {
				b.(*array.Uint32Builder).Append(v)
			}),
			format:       reflectFormatter(func(e reflect.Value) string { return strconv.FormatUint(e.Uint(), 10) }),
			formatUnsafe: unsafeFormatter(nullable, func(v uint32) string { return strconv.FormatUint(uint64(v), 10) }),
		}, nil
	case reflect.Float64:
		return fieldCodec{
			dtype:    arrow.PrimitiveTypes.Float64,
			nullable: nullable,
			appendTo: reflectAppender(func(b array.Builder, e reflect.Value) {
				b.(*array.Float64Builder).Append(e.Float())
			}),
			appendUnsafe: unsafeAppender(nullable, func(b array.Builder, v float64) {
				b.(*array.Float64Builder).Append(v)
			}),
		}, nil
	case reflect.Float32:
		return fieldCodec{
			dtype:    arrow.PrimitiveTypes.Float32,
			nullable: nullable,
			appendTo: reflectAppender(func(b array.Builder, e reflect.Value) {
				b.(*array.Float32Builder).Append(float32(e.Float()))
			}),
			appendUnsafe: unsafeAppender(nullable, func(b array.Builder, v float32) {
				b.(*array.Float32Builder).Append(v)
			}),
		}, nil
	}
	return fieldCodec{}, fmt.Errorf("unsupported type %s", t)
}

// intCodec builds the codec for a signed-integer field of Go width V, mapped to
// arrow Int64 (covers Go int and int64).
func intCodec[V int | int64](nullable bool) fieldCodec {
	return fieldCodec{
		dtype:    arrow.PrimitiveTypes.Int64,
		nullable: nullable,
		appendTo: reflectAppender(func(b array.Builder, e reflect.Value) {
			b.(*array.Int64Builder).Append(e.Int())
		}),
		appendUnsafe: unsafeAppender(nullable, func(b array.Builder, v V) {
			b.(*array.Int64Builder).Append(int64(v))
		}),
		format:       reflectFormatter(func(e reflect.Value) string { return strconv.FormatInt(e.Int(), 10) }),
		formatUnsafe: unsafeFormatter(nullable, func(v V) string { return strconv.FormatInt(int64(v), 10) }),
	}
}

// uintCodec builds the codec for an unsigned-integer field of Go width V, mapped
// to arrow Uint64 (covers Go uint and uint64).
func uintCodec[V uint | uint64](nullable bool) fieldCodec {
	return fieldCodec{
		dtype:    arrow.PrimitiveTypes.Uint64,
		nullable: nullable,
		appendTo: reflectAppender(func(b array.Builder, e reflect.Value) {
			b.(*array.Uint64Builder).Append(e.Uint())
		}),
		appendUnsafe: unsafeAppender(nullable, func(b array.Builder, v V) {
			b.(*array.Uint64Builder).Append(uint64(v))
		}),
		format:       reflectFormatter(func(e reflect.Value) string { return strconv.FormatUint(e.Uint(), 10) }),
		formatUnsafe: unsafeFormatter(nullable, func(v V) string { return strconv.FormatUint(uint64(v), 10) }),
	}
}
