package roost_test

import (
	"context"
	"fmt"
	"io/fs"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"

	"github.com/jayjamieson/roost"
	"github.com/jayjamieson/roost/examples/codegen"
	"github.com/jayjamieson/roost/sink/local"
)

// fieldInfo is a schema field reduced to the bits that must match between the
// reflection and generated paths.
type fieldInfo struct {
	Name     string
	Type     string
	Nullable bool
}

// decodedRow is one Parquet row decoded to comparable Go values, plus the Hive
// partition it was read from.
type decodedRow struct {
	Region string
	TS     int64 // micros
	Host   string
	CPU    float64
	HasVal bool
	Value  float64
}

// buildMetricCorpus mixes all of Metric's field shapes: several partitions,
// repeated hosts, and nil pointers (every 5th row) so the nullable column is
// exercised.
func buildMetricCorpus() []codegen.Metric {
	regions := []string{"us-east-1", "eu-west-1", "ap-southeast-2"}
	base := time.Date(2026, 6, 29, 12, 0, 0, 0, time.UTC)
	rows := make([]codegen.Metric, 0, 300)
	for i := 0; i < 300; i++ {
		m := codegen.Metric{
			TS:     base.Add(time.Duration(i) * time.Second),
			Host:   fmt.Sprintf("host-%02d", i%13),
			CPU:    float64(i) * 0.25,
			Region: regions[i%len(regions)],
		}
		if i%5 != 0 {
			v := float64(i) / 3.0
			m.Value = &v
		}
		rows = append(rows, m)
	}
	return rows
}

// TestReflectionVsGeneratedEquivalence writes the same corpus through both
// constructors and asserts the readback (schema, row count, Hive layout, and
// decoded column values) is identical. This is the contract that lets callers
// switch constructors freely.
func TestReflectionVsGeneratedEquivalence(t *testing.T) {
	rows := buildMetricCorpus()

	refDir := writeCorpus(t, rows, false)
	genDir := writeCorpus(t, rows, true)

	refSchema, refRows, refParts := readAllParquet(t, refDir)
	genSchema, genRows, genParts := readAllParquet(t, genDir)

	if !reflect.DeepEqual(refSchema, genSchema) {
		t.Fatalf("schema mismatch:\n reflection=%+v\n generated =%+v", refSchema, genSchema)
	}
	if len(refRows) != len(rows) || len(genRows) != len(rows) {
		t.Fatalf("row count mismatch: corpus=%d reflection=%d generated=%d", len(rows), len(refRows), len(genRows))
	}
	if !reflect.DeepEqual(refParts, genParts) {
		t.Fatalf("partition layout mismatch:\n reflection=%v\n generated =%v", refParts, genParts)
	}

	sortRows(refRows)
	sortRows(genRows)
	if !reflect.DeepEqual(refRows, genRows) {
		// Surface the first divergence to make failures debuggable.
		for i := range refRows {
			if refRows[i] != genRows[i] {
				t.Fatalf("row %d differs:\n reflection=%+v\n generated =%+v", i, refRows[i], genRows[i])
			}
		}
		t.Fatal("decoded rows differ")
	}
}

// TestGeneratedSmoke is the compile-time-plus-roundtrip check from SPEC §7.3:
// NewWriterFor with the generated appender writes N rows that read back as N.
func TestGeneratedSmoke(t *testing.T) {
	const n = 5000
	rows := make([]codegen.Metric, n)
	base := time.Unix(0, 0)
	for i := range rows {
		rows[i] = codegen.Metric{TS: base.Add(time.Duration(i) * time.Millisecond), Host: "h", CPU: 1, Region: "r"}
	}
	dir := writeCorpus(t, rows, true)
	_, decoded, _ := readAllParquet(t, dir)
	if len(decoded) != n {
		t.Fatalf("read back %d rows, want %d", len(decoded), n)
	}
}

// writeCorpus writes rows via the generated appender (gen) or reflection, with
// a fixed codec and a roll size large enough that each partition is a single
// deterministic object.
func writeCorpus(t *testing.T, rows []codegen.Metric, gen bool) string {
	t.Helper()
	dir := t.TempDir()
	sink, err := local.New(dir)
	if err != nil {
		t.Fatal(err)
	}
	opts := []roost.Option{
		roost.WithCodec("snappy"),
		roost.WithRowGroupRows(500),
		roost.WithRollRows(1 << 20),
	}
	ctx := context.Background()
	var w *roost.Writer[codegen.Metric]
	if gen {
		w, err = roost.NewWriterFor[codegen.Metric](ctx, sink, codegen.MetricRoostAppender{}, opts...)
	} else {
		w, err = roost.NewWriter[codegen.Metric](ctx, sink, opts...)
	}
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range rows {
		if err := w.Append(r); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return dir
}

// readAllParquet reads every .parquet file under dir, returning the common file
// schema (reduced), all decoded rows, and the sorted set of Hive partition dirs.
func readAllParquet(t *testing.T, dir string) ([]fieldInfo, []decodedRow, []string) {
	t.Helper()
	var paths []string
	_ = filepath.WalkDir(dir, func(p string, d fs.DirEntry, err error) error {
		if err == nil && !d.IsDir() && strings.HasSuffix(p, ".parquet") {
			paths = append(paths, p)
		}
		return nil
	})
	sort.Strings(paths)

	var schema []fieldInfo
	var rows []decodedRow
	partSet := map[string]struct{}{}

	for _, p := range paths {
		rel, _ := filepath.Rel(dir, p)
		partDir := filepath.ToSlash(filepath.Dir(rel))
		partSet[partDir] = struct{}{}
		region := regionFromPath(partDir)

		f, err := file.OpenParquetFile(p, false)
		if err != nil {
			t.Fatalf("open %s: %v", p, err)
		}
		rdr, err := pqarrow.NewFileReader(f, pqarrow.ArrowReadProperties{}, memory.DefaultAllocator)
		if err != nil {
			t.Fatalf("reader %s: %v", p, err)
		}
		tbl, err := rdr.ReadTable(context.Background())
		if err != nil {
			t.Fatalf("read table %s: %v", p, err)
		}
		sc := tbl.Schema()
		if schema == nil {
			for _, fld := range sc.Fields() {
				schema = append(schema, fieldInfo{Name: fld.Name, Type: fld.Type.String(), Nullable: fld.Nullable})
			}
		}
		ts := colInt64(t, tbl, "ts")
		host := colString(t, tbl, "host")
		cpu := colFloat64(t, tbl, "cpu")
		val, valNull := colFloat64Nullable(t, tbl, "value")
		for i := range ts {
			rows = append(rows, decodedRow{
				Region: region, TS: ts[i], Host: host[i], CPU: cpu[i],
				HasVal: !valNull[i], Value: val[i],
			})
		}
		tbl.Release()
		f.Close()
	}

	parts := make([]string, 0, len(partSet))
	for k := range partSet {
		parts = append(parts, k)
	}
	sort.Strings(parts)
	return schema, rows, parts
}

func regionFromPath(p string) string {
	for _, seg := range strings.Split(p, "/") {
		if strings.HasPrefix(seg, "region=") {
			return strings.TrimPrefix(seg, "region=")
		}
	}
	return ""
}

func colIndex(t *testing.T, tbl arrow.Table, name string) int {
	for i, f := range tbl.Schema().Fields() {
		if f.Name == name {
			return i
		}
	}
	t.Fatalf("column %q not found", name)
	return -1
}

func colInt64(t *testing.T, tbl arrow.Table, name string) []int64 {
	var out []int64
	for _, ch := range tbl.Column(colIndex(t, tbl, name)).Data().Chunks() {
		a := ch.(*array.Timestamp)
		for i := 0; i < a.Len(); i++ {
			out = append(out, int64(a.Value(i)))
		}
	}
	return out
}

func colString(t *testing.T, tbl arrow.Table, name string) []string {
	var out []string
	for _, ch := range tbl.Column(colIndex(t, tbl, name)).Data().Chunks() {
		a := ch.(*array.String)
		for i := 0; i < a.Len(); i++ {
			out = append(out, a.Value(i))
		}
	}
	return out
}

func colFloat64(t *testing.T, tbl arrow.Table, name string) []float64 {
	var out []float64
	for _, ch := range tbl.Column(colIndex(t, tbl, name)).Data().Chunks() {
		a := ch.(*array.Float64)
		for i := 0; i < a.Len(); i++ {
			out = append(out, a.Value(i))
		}
	}
	return out
}

func colFloat64Nullable(t *testing.T, tbl arrow.Table, name string) ([]float64, []bool) {
	var vals []float64
	var nulls []bool
	for _, ch := range tbl.Column(colIndex(t, tbl, name)).Data().Chunks() {
		a := ch.(*array.Float64)
		for i := 0; i < a.Len(); i++ {
			if a.IsNull(i) {
				vals = append(vals, 0)
				nulls = append(nulls, true)
			} else {
				vals = append(vals, a.Value(i))
				nulls = append(nulls, false)
			}
		}
	}
	return vals, nulls
}

func sortRows(rows []decodedRow) {
	sort.Slice(rows, func(i, j int) bool {
		a, b := rows[i], rows[j]
		switch {
		case a.Region != b.Region:
			return a.Region < b.Region
		case a.TS != b.TS:
			return a.TS < b.TS
		case a.Host != b.Host:
			return a.Host < b.Host
		default:
			return a.CPU < b.CPU
		}
	})
}
