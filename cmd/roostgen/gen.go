package main

import (
	"bytes"
	"fmt"
	"go/format"
	"go/types"
	"reflect"
	"strings"
	"text/template"

	"github.com/jayjamieson/roost/internal/roosttag"
	"golang.org/x/tools/go/packages"
)

// generate loads the package in dir and emits the roostgen file body for the
// named struct types. The result is gofmt'd and deterministic (struct field
// order), so it is safe to compare against a golden file. It never writes to
// disk — that is the caller's job — which keeps it trivially testable.
func generate(dir string, typeNames []string) ([]byte, error) {
	pkg, err := loadPackage(dir)
	if err != nil {
		return nil, err
	}

	file := genFile{Package: pkg.Name}
	for _, name := range typeNames {
		gt, needStrconv, err := genForType(pkg, name)
		if err != nil {
			return nil, err
		}
		file.NeedStrconv = file.NeedStrconv || needStrconv
		file.Types = append(file.Types, gt)
	}

	var buf bytes.Buffer
	if err := fileTemplate.Execute(&buf, file); err != nil {
		return nil, fmt.Errorf("rendering template: %w", err)
	}
	out, err := format.Source(buf.Bytes())
	if err != nil {
		return nil, fmt.Errorf("formatting generated source: %w\n--- raw ---\n%s", err, buf.String())
	}
	return out, nil
}

func loadPackage(dir string) (*packages.Package, error) {
	cfg := &packages.Config{
		Mode: packages.NeedName | packages.NeedTypes | packages.NeedTypesInfo | packages.NeedSyntax,
		Dir:  dir,
	}
	pkgs, err := packages.Load(cfg, ".")
	if err != nil {
		return nil, fmt.Errorf("loading package in %s: %w", dir, err)
	}
	var errs []string
	for _, p := range pkgs {
		for _, e := range p.Errors {
			errs = append(errs, e.Error())
		}
	}
	if len(errs) > 0 {
		return nil, fmt.Errorf("package in %s has errors: %s", dir, strings.Join(errs, "; "))
	}
	if len(pkgs) != 1 {
		return nil, fmt.Errorf("expected exactly one package in %s, found %d", dir, len(pkgs))
	}
	return pkgs[0], nil
}

// genForType resolves one struct type into its generated schema, append
// statements, and partition body. It returns needStrconv so the file-level
// import list only pulls in strconv when an int/uint partition column exists.
func genForType(pkg *packages.Package, name string) (genType, bool, error) {
	st, err := findStruct(pkg, name)
	if err != nil {
		return genType{}, false, err
	}

	gt := genType{
		GoName:       name,
		SchemaVar:    lowerFirst(name) + "RoostSchema",
		AppenderType: name + "RoostAppender",
	}
	var parts []partColumn
	needStrconv := false
	dataIdx := 0

	for i := 0; i < st.NumFields(); i++ {
		f := st.Field(i)
		if !f.Exported() {
			continue
		}
		tag := roosttag.Parse(reflect.StructTag(st.Tag(i)).Get("roost"), f.Name())
		if tag.Omit {
			continue
		}
		m, nullable, err := resolve(f.Type())
		if err != nil {
			return genType{}, false, fmt.Errorf("field %s: %w", f.Name(), err)
		}

		if tag.Partition {
			val := "v." + f.Name()
			if nullable {
				val = "*v." + f.Name()
			}
			expr, ok := m.partExpr(val, nullable)
			if !ok {
				return genType{}, false, fmt.Errorf("field %s type %s cannot be a partition column", f.Name(), f.Type())
			}
			if m.usesStrconvForPartition() {
				needStrconv = true
			}
			parts = append(parts, partColumn{name: tag.Name, goField: f.Name(), nullable: nullable, expr: expr})
			continue
		}

		gt.SchemaFields = append(gt.SchemaFields,
			fmt.Sprintf("{Name: %q, Type: %s, Nullable: %t},", tag.Name, m.arrowType, nullable))
		gt.AppendStmts = append(gt.AppendStmts, buildAppendStmt(dataIdx, f.Name(), m, nullable))
		dataIdx++
	}

	if dataIdx == 0 {
		return genType{}, false, fmt.Errorf("struct %s has no data columns", name)
	}
	gt.PartitionBody = buildPartitionBody(parts)
	return gt, needStrconv, nil
}

func findStruct(pkg *packages.Package, name string) (*types.Struct, error) {
	obj := pkg.Types.Scope().Lookup(name)
	if obj == nil {
		return nil, fmt.Errorf("type %s not found in package %s", name, pkg.Name)
	}
	named, ok := obj.Type().(*types.Named)
	if !ok {
		return nil, fmt.Errorf("%s is not a named type", name)
	}
	st, ok := named.Underlying().(*types.Struct)
	if !ok {
		return nil, fmt.Errorf("%s is not a struct type", name)
	}
	return st, nil
}

// buildAppendStmt renders the Append call(s) for one data column at builder
// index idx. Pointer fields get a nil guard that appends a null instead.
func buildAppendStmt(idx int, goField string, m mapping, nullable bool) string {
	if nullable {
		arg := m.appendArg("*v."+goField, true)
		return fmt.Sprintf("if v.%s == nil {\nb.Field(%d).AppendNull()\n} else {\nb.Field(%d).(%s).Append(%s)\n}",
			goField, idx, idx, m.builder, arg)
	}
	arg := m.appendArg("v."+goField, false)
	return fmt.Sprintf("b.Field(%d).(%s).Append(%s)", idx, m.builder, arg)
}

// buildPartitionBody renders the body of the Partition method. Non-pointer
// columns concatenate inline ("region=" + roost.SanitizeSegment(...)); pointer
// columns compute their segment into a local first so nil maps to "null".
func buildPartitionBody(parts []partColumn) string {
	if len(parts) == 0 {
		return `return ""`
	}
	var pre strings.Builder
	terms := make([]string, 0, len(parts))
	for i, p := range parts {
		prefix := p.name + "="
		if i > 0 {
			prefix = "/" + p.name + "="
		}
		var seg string
		if p.nullable {
			seg = fmt.Sprintf("seg%d", i)
			pre.WriteString(fmt.Sprintf("%s := \"null\"\nif v.%s != nil {\n%s = roost.SanitizeSegment(%s)\n}\n",
				seg, p.goField, seg, p.expr))
		} else {
			seg = "roost.SanitizeSegment(" + p.expr + ")"
		}
		terms = append(terms, fmt.Sprintf("%q + %s", prefix, seg))
	}
	return pre.String() + "return " + strings.Join(terms, " + ")
}

// lowerFirst lowercases the first rune, turning an exported type name into the
// unexported package-level schema var name (Metric -> metricRoostSchema).
func lowerFirst(s string) string {
	if s == "" {
		return s
	}
	r := []rune(s)
	if r[0] >= 'A' && r[0] <= 'Z' {
		r[0] = r[0] - 'A' + 'a'
	}
	return string(r)
}

// genType is the per-type data the template renders.
type genType struct {
	GoName        string
	SchemaVar     string
	AppenderType  string
	SchemaFields  []string
	AppendStmts   []string
	PartitionBody string
}

// genFile is the whole emitted file's data.
type genFile struct {
	Package     string
	NeedStrconv bool
	Types       []genType
}

// partColumn carries the resolved partition column data for buildPartitionBody.
type partColumn struct {
	name     string // column name (tag name)
	goField  string // Go field name
	nullable bool   // pointer field
	expr     string // string-typed Go expression yielding the (unsanitized) value
}

var fileTemplate = template.Must(template.New("roostgen").Parse(`// Code generated by roostgen. DO NOT EDIT.

package {{.Package}}

import (
{{- if .NeedStrconv}}
	"strconv"
{{- end}}

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/jayjamieson/roost"
)
{{range .Types}}
var {{.SchemaVar}} = arrow.NewSchema([]arrow.Field{
{{- range .SchemaFields}}
	{{.}}
{{- end}}
}, nil)

// {{.AppenderType}} is the generated zero-reflection RowAppender for {{.GoName}}.
type {{.AppenderType}} struct{}

var _ roost.RowAppender[{{.GoName}}] = {{.AppenderType}}{}

func ({{.AppenderType}}) Schema() *arrow.Schema { return {{.SchemaVar}} }

func ({{.AppenderType}}) Partition(v *{{.GoName}}) string {
{{.PartitionBody}}
}

func ({{.AppenderType}}) Append(v *{{.GoName}}, b *array.RecordBuilder) {
{{- range .AppendStmts}}
	{{.}}
{{- end}}
}
{{end}}`))
