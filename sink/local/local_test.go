package local

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

func TestLocalCreateWritesNestedFile(t *testing.T) {
	dir := t.TempDir()
	s, err := New(dir)
	if err != nil {
		t.Fatal(err)
	}
	wc, err := s.Create(context.Background(), "region=x/dt=2026-06-26/part-0.parquet")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := wc.Write([]byte("hello")); err != nil {
		t.Fatal(err)
	}
	if err := wc.Close(); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(filepath.Join(dir, "region=x", "dt=2026-06-26", "part-0.parquet"))
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "hello" {
		t.Fatalf("got %q", got)
	}
}

func TestLocalFsyncOption(t *testing.T) {
	dir := t.TempDir()
	s, _ := New(dir, WithFsync(true))
	wc, _ := s.Create(context.Background(), "a.parquet")
	_, _ = wc.Write([]byte("durable"))
	if err := wc.Close(); err != nil {
		t.Fatalf("fsync close: %v", err)
	}
}
