package roost_test

import (
	"context"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/file"

	"github.com/jayjamieson/roost"
	"github.com/jayjamieson/roost/sink/local"
)

// DictRow has one column opted into dictionary encoding via tag, one opted in
// via the option, and one left at the default (plain).
type DictRow struct {
	Tagged   string `roost:"name=tagged,dict"`
	Optioned string `roost:"name=optioned"`
	Plain    string `roost:"name=plain"`
}

// columnEncodings returns column name -> the encodings recorded in its chunk
// metadata, across every parquet file under dir.
func columnEncodings(t *testing.T, dir string) map[string][]parquet.Encoding {
	t.Helper()
	out := map[string][]parquet.Encoding{}
	eachParquet(t, dir, func(p string, rdr *file.Reader) {
		md := rdr.MetaData()
		for rg := 0; rg < md.NumRowGroups(); rg++ {
			rgMeta := md.RowGroup(rg)
			for c := 0; c < rgMeta.NumColumns(); c++ {
				cc, e := rgMeta.ColumnChunk(c)
				if e != nil {
					t.Fatal(e)
				}
				name := cc.PathInSchema()[0]
				out[name] = append(out[name], cc.Encodings()...)
			}
		}
	})
	return out
}

func hasDict(encs []parquet.Encoding) bool {
	for _, e := range encs {
		if e == parquet.Encodings.RLEDict || e == parquet.Encodings.PlainDict {
			return true
		}
	}
	return false
}

func TestDictionaryOptIn(t *testing.T) {
	dir := t.TempDir()
	sink, err := local.New(dir)
	if err != nil {
		t.Fatal(err)
	}
	w, err := roost.NewWriter[DictRow](context.Background(), sink,
		roost.WithCodec("none"),
		roost.WithRollRows(1_000_000), roost.WithRowGroupRows(1_000_000),
		roost.WithDictionaryColumns("optioned"))
	if err != nil {
		t.Fatal(err)
	}
	cats := []string{"a", "b", "c"} // low cardinality so dictionary engages
	for i := 0; i < 3000; i++ {
		v := cats[i%3]
		if err := w.Append(&DictRow{Tagged: v, Optioned: v, Plain: v}); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	enc := columnEncodings(t, dir)
	if !hasDict(enc["tagged"]) {
		t.Errorf("tagged column should be dictionary-encoded (via tag); got %v", enc["tagged"])
	}
	if !hasDict(enc["optioned"]) {
		t.Errorf("optioned column should be dictionary-encoded (via option); got %v", enc["optioned"])
	}
	if hasDict(enc["plain"]) {
		t.Errorf("plain column should NOT be dictionary-encoded by default; got %v", enc["plain"])
	}
}

// TestDictionaryTagAndOptionUnion exercises the dedup path: a column named by
// both the tag and the option must still be dictionary-encoded (and not error).
func TestDictionaryTagAndOptionUnion(t *testing.T) {
	dir := t.TempDir()
	sink, err := local.New(dir)
	if err != nil {
		t.Fatal(err)
	}
	w, err := roost.NewWriter[DictRow](context.Background(), sink,
		roost.WithCodec("none"),
		roost.WithRollRows(1_000_000), roost.WithRowGroupRows(1_000_000),
		roost.WithDictionaryColumns("tagged")) // "tagged" is also dict-tagged
	if err != nil {
		t.Fatal(err)
	}
	cats := []string{"a", "b", "c"}
	for i := 0; i < 1500; i++ {
		if err := w.Append(&DictRow{Tagged: cats[i%3], Optioned: cats[i%3], Plain: cats[i%3]}); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if enc := columnEncodings(t, dir); !hasDict(enc["tagged"]) {
		t.Errorf("tagged column (tag+option) should be dictionary-encoded; got %v", enc["tagged"])
	}
}

// TestCompressionLevelHonored proves WithCompressionLevel reaches the encoder:
// the same compressible data must encode smaller at a higher zstd level. If the
// level were ignored, the two objects would be byte-identical in size.
func TestCompressionLevelHonored(t *testing.T) {
	write := func(level int) int64 {
		dir := t.TempDir()
		sink, err := local.New(dir)
		if err != nil {
			t.Fatal(err)
		}
		w, err := roost.NewWriter[DictRow](context.Background(), sink,
			roost.WithCodec("zstd"), roost.WithCompressionLevel(level),
			roost.WithRollRows(1_000_000), roost.WithRowGroupRows(1_000_000))
		if err != nil {
			t.Fatal(err)
		}
		// Repetitive-but-not-trivial data so a higher level wins on ratio.
		for i := 0; i < 50_000; i++ {
			v := strings.Repeat("abcdefgh", (i%7)+1)
			if err := w.Append(&DictRow{Tagged: v, Optioned: v, Plain: v}); err != nil {
				t.Fatal(err)
			}
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
		return w.Stats().Bytes
	}
	low, high := write(1), write(19)
	if high >= low {
		t.Fatalf("expected level 19 (%d bytes) < level 1 (%d bytes)", high, low)
	}
	t.Logf("zstd level 1 = %d bytes, level 19 = %d bytes (%.1f%% smaller)", low, high, 100*float64(low-high)/float64(low))
}
