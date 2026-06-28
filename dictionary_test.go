package roost_test

import (
	"context"
	"io/fs"
	"path/filepath"
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
	err := filepath.WalkDir(dir, func(p string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() || !strings.HasSuffix(p, ".parquet") {
			return err
		}
		rdr, e := file.OpenParquetFile(p, false)
		if e != nil {
			t.Fatal(e)
		}
		defer rdr.Close()
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
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
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
		if err := w.Append(DictRow{Tagged: v, Optioned: v, Plain: v}); err != nil {
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
		if err := w.Append(DictRow{Tagged: cats[i%3], Optioned: cats[i%3], Plain: cats[i%3]}); err != nil {
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
