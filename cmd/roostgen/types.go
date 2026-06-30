package main

import (
	"fmt"
	"go/types"
)

// colKind enumerates the supported column shapes. It mirrors schema.go's
// mapField exactly (SPEC §5.3); anything outside this set is a generate-time
// error, never a silent skip.
type colKind int

const (
	kindBool colKind = iota
	kindInt64
	kindInt32
	kindUint64
	kindUint32
	kindFloat64
	kindFloat32
	kindString
	kindBinary
	kindTime
)

// mapping is the resolved Arrow column for a Go field type.
type mapping struct {
	kind      colKind
	arrowType string // expression for the arrow.Field Type
	builder   string // builder cast, e.g. "*array.Int64Builder"
}

// resolve maps a Go field type to its Arrow column, returning nullable=true for
// a pointer field. It rejects anything not in SPEC §5.3 (named scalars, nested
// structs, non-byte slices, etc.) with a clear "unsupported type" error,
// matching mapField's behavior in schema.go.
func resolve(t types.Type) (mapping, bool, error) {
	nullable := false
	if ptr, ok := t.(*types.Pointer); ok {
		nullable = true
		t = ptr.Elem()
	}

	// time.Time (a named type) - check before the generic *types.Named reject.
	if named, ok := t.(*types.Named); ok {
		o := named.Obj()
		if o.Pkg() != nil && o.Pkg().Path() == "time" && o.Name() == "Time" {
			return mapping{kindTime, "&arrow.TimestampType{Unit: arrow.Microsecond}", "*array.TimestampBuilder"}, nullable, nil
		}
		return mapping{}, nullable, fmt.Errorf("unsupported type %s", t)
	}

	// []byte -> Binary (must precede the basic-kind switch).
	if sl, ok := t.(*types.Slice); ok {
		if b, ok := sl.Elem().(*types.Basic); ok && b.Kind() == types.Uint8 {
			return mapping{kindBinary, "arrow.BinaryTypes.Binary", "*array.BinaryBuilder"}, nullable, nil
		}
		return mapping{}, nullable, fmt.Errorf("unsupported type %s", t)
	}

	b, ok := t.(*types.Basic)
	if !ok {
		return mapping{}, nullable, fmt.Errorf("unsupported type %s", t)
	}
	switch b.Kind() {
	case types.Bool:
		return mapping{kindBool, "arrow.FixedWidthTypes.Boolean", "*array.BooleanBuilder"}, nullable, nil
	case types.Int, types.Int64:
		return mapping{kindInt64, "arrow.PrimitiveTypes.Int64", "*array.Int64Builder"}, nullable, nil
	case types.Int32:
		return mapping{kindInt32, "arrow.PrimitiveTypes.Int32", "*array.Int32Builder"}, nullable, nil
	case types.Uint, types.Uint64:
		return mapping{kindUint64, "arrow.PrimitiveTypes.Uint64", "*array.Uint64Builder"}, nullable, nil
	case types.Uint32:
		return mapping{kindUint32, "arrow.PrimitiveTypes.Uint32", "*array.Uint32Builder"}, nullable, nil
	case types.Float64:
		return mapping{kindFloat64, "arrow.PrimitiveTypes.Float64", "*array.Float64Builder"}, nullable, nil
	case types.Float32:
		return mapping{kindFloat32, "arrow.PrimitiveTypes.Float32", "*array.Float32Builder"}, nullable, nil
	case types.String:
		return mapping{kindString, "arrow.BinaryTypes.String", "*array.StringBuilder"}, nullable, nil
	}
	return mapping{}, nullable, fmt.Errorf("unsupported type %s", t)
}

// appendArg returns the argument to the typed builder's Append for a value
// expression val (already dereferenced for pointer fields). isPtr reports
// whether val is a pointer deref, so method-receiver expressions get parens.
func (m mapping) appendArg(val string, isPtr bool) string {
	switch m.kind {
	case kindInt64:
		return "int64(" + val + ")"
	case kindUint64:
		return "uint64(" + val + ")"
	case kindTime:
		return "arrow.Timestamp(" + recv(val, isPtr) + ".UnixMicro())"
	default:
		return val
	}
}

// partExpr returns a string-typed Go expression for the column's partition path
// value, or ok=false when the type cannot be a partition column (bool, float,
// []byte). It matches schema.go's partition formatters.
func (m mapping) partExpr(val string, isPtr bool) (string, bool) {
	switch m.kind {
	case kindString:
		return val, true
	case kindInt64, kindInt32:
		return "strconv.FormatInt(int64(" + val + "), 10)", true
	case kindUint64, kindUint32:
		return "strconv.FormatUint(uint64(" + val + "), 10)", true
	case kindTime:
		return recv(val, isPtr) + `.UTC().Format("2006-01-02")`, true
	default:
		return "", false
	}
}

// usesStrconvForPartition reports whether this column's partition formatter
// needs the strconv import.
func (m mapping) usesStrconvForPartition() bool {
	switch m.kind {
	case kindInt64, kindInt32, kindUint64, kindUint32:
		return true
	}
	return false
}

// recv parenthesizes a pointer-deref value expression so a following method
// call binds to the dereferenced value: (*v.F).UnixMicro(), not *v.F.UnixMicro().
func recv(val string, isPtr bool) string {
	if isPtr {
		return "(" + val + ")"
	}
	return val
}
