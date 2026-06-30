package roost

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/jayjamieson/roost/internal/roosttag"
)

var timeType = reflect.TypeOf(time.Time{})

// fieldPlan is the precomputed instruction for one struct field.
type fieldPlan struct {
	structIndex int
	name        string
	dtype       arrow.DataType
	nullable    bool
	partition   bool
	appendTo    func(b array.Builder, fv reflect.Value)
	format      func(fv reflect.Value) string // partition value -> path segment
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
		dt, nullable, app, fmtFn, err := mapField(sf.Type)
		if err != nil {
			return nil, fmt.Errorf("roost: field %s: %w", sf.Name, err)
		}
		fp := fieldPlan{structIndex: i, name: name, dtype: dt, nullable: nullable, partition: partition, appendTo: app, format: fmtFn}
		if partition {
			if fmtFn == nil {
				return nil, fmt.Errorf("roost: field %s type %s cannot be a partition column", sf.Name, sf.Type)
			}
			parts = append(parts, fp)
			continue
		}
		data = append(data, fp)
		fields = append(fields, arrow.Field{Name: name, Type: dt, Nullable: nullable})
		if dict {
			dictCols = append(dictCols, name)
		}
	}
	if len(data) == 0 {
		return nil, fmt.Errorf("roost: struct %s has no data columns", t)
	}
	return &plan{fileSchema: arrow.NewSchema(fields, nil), dataCols: data, partCols: parts, dictCols: dictCols}, nil
}

// parseTag is a thin adapter over the shared roosttag parser, which the
// generator (cmd/roostgen) uses too so the two paths can't drift.
func parseTag(sf reflect.StructField) (name string, partition, omit, dict bool) {
	t := roosttag.Parse(sf.Tag.Get("roost"), sf.Name)
	return t.Name, t.Partition, t.Omit, t.Dict
}

// SanitizeSegment keeps partition values safe for paths and object keys. It is
// exported so roostgen-emitted code (which lives in the caller's package) builds
// byte-identical Hive paths to the reflection path.
func SanitizeSegment(s string) string {
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

// AppendSanitized appends SanitizeSegment(s) to dst and returns the extended
// slice, without allocating an intermediate string. Generated PartitionInto
// methods use it to build partition keys into a reused buffer. All sanitized
// characters are single-byte ASCII and every other byte is copied verbatim, so
// the result is byte-identical to SanitizeSegment(s) (asserted by test).
func AppendSanitized(dst []byte, s string) []byte {
	if s == "" {
		return append(dst, "__empty__"...)
	}
	for i := 0; i < len(s); i++ {
		switch s[i] {
		case '/', '\\', '=', ' ', '\t', '\n', '\r', '"', '\'':
			dst = append(dst, '_')
		default:
			dst = append(dst, s[i])
		}
	}
	return dst
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

// mapField maps a Go type to (arrow type, nullable, appender, partition formatter).
func mapField(t reflect.Type) (arrow.DataType, bool, func(array.Builder, reflect.Value), func(reflect.Value) string, error) {
	nullable := false
	if t.Kind() == reflect.Pointer {
		nullable = true
		t = t.Elem()
	}

	if t == timeType {
		app := func(b array.Builder, fv reflect.Value) {
			e, ok := elem(fv)
			if !ok {
				b.AppendNull()
				return
			}
			b.(*array.TimestampBuilder).Append(arrow.Timestamp(e.Interface().(time.Time).UnixMicro()))
		}
		fmtFn := func(fv reflect.Value) string {
			e, ok := elem(fv)
			if !ok {
				return "null"
			}
			return e.Interface().(time.Time).UTC().Format("2006-01-02")
		}
		return &arrow.TimestampType{Unit: arrow.Microsecond}, nullable, app, fmtFn, nil
	}

	// []byte -> Binary (must precede the Slice/Kind switch).
	if t.Kind() == reflect.Slice && t.Elem().Kind() == reflect.Uint8 {
		app := func(b array.Builder, fv reflect.Value) {
			e, ok := elem(fv)
			if !ok {
				b.AppendNull()
				return
			}
			b.(*array.BinaryBuilder).Append(e.Bytes())
		}
		return arrow.BinaryTypes.Binary, nullable, app, nil, nil
	}

	switch t.Kind() {
	case reflect.String:
		app := func(b array.Builder, fv reflect.Value) {
			if e, ok := elem(fv); ok {
				b.(*array.StringBuilder).Append(e.String())
			} else {
				b.AppendNull()
			}
		}
		return arrow.BinaryTypes.String, nullable, app, strFmt, nil
	case reflect.Bool:
		app := func(b array.Builder, fv reflect.Value) {
			if e, ok := elem(fv); ok {
				b.(*array.BooleanBuilder).Append(e.Bool())
			} else {
				b.AppendNull()
			}
		}
		return arrow.FixedWidthTypes.Boolean, nullable, app, nil, nil
	case reflect.Int, reflect.Int64:
		return intCol(arrow.PrimitiveTypes.Int64, nullable, func(b array.Builder, v int64) { b.(*array.Int64Builder).Append(v) })
	case reflect.Int32:
		return intCol(arrow.PrimitiveTypes.Int32, nullable, func(b array.Builder, v int64) { b.(*array.Int32Builder).Append(int32(v)) })
	case reflect.Uint, reflect.Uint64:
		dt := arrow.PrimitiveTypes.Uint64
		app := func(b array.Builder, fv reflect.Value) {
			if e, ok := elem(fv); ok {
				b.(*array.Uint64Builder).Append(e.Uint())
			} else {
				b.AppendNull()
			}
		}
		return dt, nullable, app, uintFmt, nil
	case reflect.Uint32:
		dt := arrow.PrimitiveTypes.Uint32
		app := func(b array.Builder, fv reflect.Value) {
			if e, ok := elem(fv); ok {
				b.(*array.Uint32Builder).Append(uint32(e.Uint()))
			} else {
				b.AppendNull()
			}
		}
		return dt, nullable, app, uintFmt, nil
	case reflect.Float64:
		app := func(b array.Builder, fv reflect.Value) {
			if e, ok := elem(fv); ok {
				b.(*array.Float64Builder).Append(e.Float())
			} else {
				b.AppendNull()
			}
		}
		return arrow.PrimitiveTypes.Float64, nullable, app, nil, nil
	case reflect.Float32:
		app := func(b array.Builder, fv reflect.Value) {
			if e, ok := elem(fv); ok {
				b.(*array.Float32Builder).Append(float32(e.Float()))
			} else {
				b.AppendNull()
			}
		}
		return arrow.PrimitiveTypes.Float32, nullable, app, nil, nil
	}
	return nil, false, nil, nil, fmt.Errorf("unsupported type %s", t)
}

func intCol(dt arrow.DataType, nullable bool, set func(array.Builder, int64)) (arrow.DataType, bool, func(array.Builder, reflect.Value), func(reflect.Value) string, error) {
	app := func(b array.Builder, fv reflect.Value) {
		if e, ok := elem(fv); ok {
			set(b, e.Int())
		} else {
			b.AppendNull()
		}
	}
	return dt, nullable, app, intFmt, nil
}

func strFmt(fv reflect.Value) string {
	if e, ok := elem(fv); ok {
		return e.String()
	}
	return "null"
}
func intFmt(fv reflect.Value) string {
	if e, ok := elem(fv); ok {
		return strconv.FormatInt(e.Int(), 10)
	}
	return "null"
}
func uintFmt(fv reflect.Value) string {
	if e, ok := elem(fv); ok {
		return strconv.FormatUint(e.Uint(), 10)
	}
	return "null"
}
