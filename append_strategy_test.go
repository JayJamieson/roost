package roost_test

import (
	"context"
	"fmt"
	"io/fs"
	"math/rand"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"

	"github.com/jayjamieson/roost"
	"github.com/jayjamieson/roost/sink/local"
)

// Wide exercises every supported field type, including nullable pointers and a
// partition column, so the strategy-equivalence tests cover the full plan.
type Wide struct {
	I      int        `roost:"name=i"`
	I64    int64      `roost:"name=i64"`
	I32    int32      `roost:"name=i32"`
	U      uint       `roost:"name=u"`
	U64    uint64     `roost:"name=u64"`
	U32    uint32     `roost:"name=u32"`
	F64    float64    `roost:"name=f64"`
	F32    float32    `roost:"name=f32"`
	B      bool       `roost:"name=b"`
	S      string     `roost:"name=s"`
	Bin    []byte     `roost:"name=bin"`
	T      time.Time  `roost:"name=t"`
	PI64   *int64     `roost:"name=pi64"`
	PS     *string    `roost:"name=ps"`
	PT     *time.Time `roost:"name=pt"`
	Region string     `roost:"name=region,partition"`
}

type appendStrategy int

const (
	stratValue appendStrategy = iota
	stratPtr
	stratUnsafe
)

// wideRows builds deterministic, varied rows with a sprinkling of nulls.
func wideRows(n int) []Wide {
	rng := rand.New(rand.NewSource(7))
	rows := make([]Wide, n)
	for i := range rows {
		v := rng.Int63()
		s := fmt.Sprintf("s-%d", i)
		var pi *int64
		var ps *string
		var pt *time.Time
		if i%3 != 0 { // leave every third row's pointers nil
			tt := time.Unix(0, v).UTC()
			pi, ps, pt = &v, &s, &tt
		}
		rows[i] = Wide{
			I: i, I64: int64(i) * 7, I32: int32(i), U: uint(i), U64: uint64(i) * 3, U32: uint32(i),
			F64: float64(i) / 3, F32: float32(i) / 7, B: i%2 == 0, S: s,
			Bin: []byte(fmt.Sprintf("b%d", i)), T: time.Unix(int64(i), 0).UTC(),
			PI64: pi, PS: ps, PT: pt, Region: "r1",
		}
	}
	return rows
}

func writeWide(t *testing.T, dir string, rows []Wide, s appendStrategy) {
	t.Helper()
	sink, err := local.New(dir)
	if err != nil {
		t.Fatal(err)
	}
	// One big object/row-group so read-back order is deterministic.
	w, err := roost.NewWriter[Wide](context.Background(), sink,
		roost.WithRollRows(1_000_000), roost.WithRowGroupRows(1_000_000), roost.WithCodec("snappy"))
	if err != nil {
		t.Fatal(err)
	}
	for i := range rows {
		switch s {
		case stratValue:
			err = w.Append(rows[i])
		case stratPtr:
			err = w.AppendPtr(&rows[i])
		case stratUnsafe:
			err = w.AppendUnsafe(rows[i])
		}
		if err != nil {
			t.Fatalf("append row %d: %v", i, err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
}

// readColumns reads every parquet file under dir (lexical = write order for a
// single partition) and returns column name -> ordered stringified values.
func readColumns(t *testing.T, dir string) map[string][]string {
	t.Helper()
	out := map[string][]string{}
	err := filepath.WalkDir(dir, func(p string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() || !strings.HasSuffix(p, ".parquet") {
			return err
		}
		f, e := os.Open(p)
		if e != nil {
			t.Fatal(e)
		}
		defer f.Close()
		tbl, e := pqarrow.ReadTable(context.Background(), f,
			parquet.NewReaderProperties(memory.DefaultAllocator), pqarrow.ArrowReadProperties{}, memory.DefaultAllocator)
		if e != nil {
			t.Fatalf("read %s: %v", p, e)
		}
		defer tbl.Release()
		sch := tbl.Schema()
		for ci := 0; ci < int(tbl.NumCols()); ci++ {
			name := sch.Field(ci).Name
			for _, chunk := range tbl.Column(ci).Data().Chunks() {
				for i := 0; i < chunk.Len(); i++ {
					out[name] = append(out[name], chunk.ValueStr(i))
				}
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return out
}

// parquetRelDirs returns the sorted, root-relative directories that contain
// parquet files — i.e. the Hive partition paths, independent of file names.
func parquetRelDirs(t *testing.T, root string) []string {
	t.Helper()
	set := map[string]struct{}{}
	err := filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() || !strings.HasSuffix(p, ".parquet") {
			return err
		}
		rel, _ := filepath.Rel(root, p)
		set[filepath.ToSlash(filepath.Dir(rel))] = struct{}{}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	out := make([]string, 0, len(set))
	for k := range set {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// BenchmarkAppendStrategies isolates the per-row cost of the three append
// strategies. Roll/row-group thresholds are set huge so no encoding happens
// inside the timed loop, and the timer is stopped before Close, so the reported
// allocs/op is purely the append path: value boxes the struct for reflect (1
// alloc), ptr avoids that box but still boxes time.Time via reflect, and unsafe
// reads every field by offset.
//
//	go test -bench AppendStrategies -benchmem -benchtime=1000000x
func BenchmarkAppendStrategies(b *testing.B) {
	corpus := buildCorpus()
	run := func(b *testing.B, s appendStrategy) {
		dir := b.TempDir()
		sink, err := local.New(dir)
		if err != nil {
			b.Fatal(err)
		}
		w, err := roost.NewWriter[LocalRow](context.Background(), sink,
			roost.WithCodec("zstd"),
			roost.WithRowGroupRows(1<<30), roost.WithRollRows(1<<30))
		if err != nil {
			b.Fatal(err)
		}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			row := &corpus[i&(corpusSize-1)]
			switch s {
			case stratValue:
				err = w.Append(*row)
			case stratPtr:
				err = w.AppendPtr(row)
			case stratUnsafe:
				err = w.AppendUnsafe(*row)
			}
			if err != nil {
				b.Fatal(err)
			}
		}
		b.StopTimer() // exclude the one-time encode at Close from the measurement
		if err := w.Close(); err != nil {
			b.Fatal(err)
		}
	}
	b.Run("value", func(b *testing.B) { run(b, stratValue) })
	b.Run("ptr", func(b *testing.B) { run(b, stratPtr) })
	b.Run("unsafe", func(b *testing.B) { run(b, stratUnsafe) })
}

func TestAppendPtrMatchesAppend(t *testing.T) {
	rows := wideRows(2000)
	dirV, dirP := t.TempDir(), t.TempDir()
	writeWide(t, dirV, rows, stratValue)
	writeWide(t, dirP, rows, stratPtr)

	got, want := readColumns(t, dirP), readColumns(t, dirV)
	if len(want["i"]) != len(rows) {
		t.Fatalf("value-strategy wrote %d rows, want %d", len(want["i"]), len(rows))
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatal("AppendPtr output differs from Append output")
	}
}

func TestAppendUnsafeMatchesAppend(t *testing.T) {
	rows := wideRows(2000)
	dirV, dirU := t.TempDir(), t.TempDir()
	writeWide(t, dirV, rows, stratValue)
	writeWide(t, dirU, rows, stratUnsafe)

	got, want := readColumns(t, dirU), readColumns(t, dirV)
	if len(want["i"]) != len(rows) {
		t.Fatalf("value-strategy wrote %d rows, want %d", len(want["i"]), len(rows))
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatal("AppendUnsafe data differs from Append data")
	}
	// formatUnsafe must build the same Hive partition path as the reflect path.
	if u, v := parquetRelDirs(t, dirU), parquetRelDirs(t, dirV); !reflect.DeepEqual(u, v) {
		t.Fatalf("partition dirs differ: unsafe=%v value=%v", u, v)
	}
}

func TestAppendPtrNil(t *testing.T) {
	dir := t.TempDir()
	sink, err := local.New(dir)
	if err != nil {
		t.Fatal(err)
	}
	w, err := roost.NewWriter[Wide](context.Background(), sink)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	if err := w.AppendPtr(nil); err == nil {
		t.Fatal("AppendPtr(nil) should return an error")
	}
}

// TestAppendUnsafeReadsCorrectValues checks the unsafe offset reads against
// known inputs directly (not just against the reflect path), covering an
// integer, string, bool, and a nil pointer (null).
func TestAppendUnsafeReadsCorrectValues(t *testing.T) {
	rows := []Wide{
		{I64: 11, S: "hello", B: true, PI64: nil, Region: "r1"},
		{I64: 22, S: "world", B: false, PI64: func() *int64 { x := int64(99); return &x }(), Region: "r1"},
	}
	dir := t.TempDir()
	writeWide(t, dir, rows, stratUnsafe)
	cols := readColumns(t, dir)

	checks := map[string][]string{
		"i64":  {"11", "22"},
		"s":    {"hello", "world"},
		"b":    {"true", "false"},
		"pi64": {"(null)", "99"},
	}
	for col, want := range checks {
		if got := cols[col]; !reflect.DeepEqual(got, want) {
			t.Errorf("column %s = %v, want %v", col, got, want)
		}
	}
}
