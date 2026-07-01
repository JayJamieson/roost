package roost_test

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jayjamieson/roost"
	"github.com/jayjamieson/roost/sink/local"
)

type Event struct {
	RSN    int64     `roost:"name=rsn"`
	Time   time.Time `roost:"name=event_time"`
	Region string    `roost:"name=region,partition"`
	Body   []byte
}

func TestWriteRoundTrip(t *testing.T) {
	dir := t.TempDir()
	sink, err := local.New(dir)
	if err != nil {
		t.Fatal(err)
	}
	w, err := roost.NewWriter[Event](context.Background(), sink,
		roost.WithRowGroupRows(1000), roost.WithRollRows(5000), roost.WithCodec("snappy"))
	if err != nil {
		t.Fatal(err)
	}
	const n = 12000
	for i := 0; i < n; i++ {
		if err := w.Append(&Event{RSN: int64(i), Time: time.Now(), Region: "us-east-1", Body: []byte("x")}); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	rows, files := countParquetRows(t, dir)
	if rows != n {
		t.Fatalf("rows = %d, want %d", rows, n)
	}
	if len(files) < 2 {
		t.Fatalf("expected multiple rolled objects, got %d", len(files))
	}
	if st := w.Stats(); st.Rows != n {
		t.Fatalf("stats rows = %d, want %d", st.Rows, n)
	}
}

func TestHivePartitionLayout(t *testing.T) {
	dir := t.TempDir()
	sink, _ := local.New(dir)
	w, _ := roost.NewWriter[Event](context.Background(), sink, roost.WithRollRows(100000))
	regions := []string{"us-east-1", "eu-west-1", "ap-southeast-2"}
	for i := 0; i < 3000; i++ {
		_ = w.Append(&Event{RSN: int64(i), Time: time.Now(), Region: regions[i%3], Body: []byte("y")})
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	_, files := countParquetRows(t, dir)
	for _, r := range regions {
		seg := "region=" + r
		found := false
		for _, f := range files {
			if strings.Contains(filepath.ToSlash(f), seg+"/") {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("missing partition dir %s", seg)
		}
	}
}

func TestConcurrentEncode(t *testing.T) {
	dir := t.TempDir()
	sink, _ := local.New(dir)
	w, _ := roost.NewWriter[Event](context.Background(), sink,
		roost.WithRollRows(1000), roost.WithEncodeConcurrency(4))
	const n = 10000
	for i := 0; i < n; i++ {
		_ = w.Append(&Event{RSN: int64(i), Time: time.Now(), Region: "r", Body: []byte("z")})
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if rows, _ := countParquetRows(t, dir); rows != n {
		t.Fatalf("rows = %d, want %d", rows, n)
	}
}
