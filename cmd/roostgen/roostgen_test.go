package main

import (
	"bytes"
	"flag"
	"go/format"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

var update = flag.Bool("update", false, "rewrite golden files")

// TestGenerateGolden runs the generator on the comprehensive fixture and
// compares it to the checked-in golden, exercising every supported type plus
// pointers, partitions, rename and omit. With -update it regenerates the golden.
func TestGenerateGolden(t *testing.T) {
	got, err := generate("testdata/in", []string{"Metric"})
	if err != nil {
		t.Fatalf("generate: %v", err)
	}

	golden := filepath.Join("testdata", "golden", "metric_roost.go.golden")
	if *update {
		if err := os.MkdirAll(filepath.Dir(golden), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(golden, got, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	want, err := os.ReadFile(golden)
	if err != nil {
		t.Fatalf("reading golden (run with -update to create): %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Errorf("generated output differs from golden.\n--- got ---\n%s", got)
	}

	// Output must be gofmt-stable and deterministic across runs.
	if formatted, err := format.Source(got); err != nil {
		t.Errorf("output is not valid Go: %v", err)
	} else if !bytes.Equal(formatted, got) {
		t.Error("output is not gofmt-stable")
	}
	got2, err := generate("testdata/in", []string{"Metric"})
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, got2) {
		t.Error("output is non-deterministic across runs")
	}
}

// TestGenerateErrors asserts the generator fails clearly on out-of-scope types,
// rather than silently skipping them (SPEC §11).
func TestGenerateErrors(t *testing.T) {
	cases := []struct {
		name string
		dir  string
		typ  string
		want string
	}{
		{"unsupported type", "testdata/badtype", "Bad", "unsupported type"},
		{"float partition", "testdata/badpartition", "Bad", "cannot be a partition column"},
		{"missing type", "testdata/in", "Nope", "not found"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := generate(tc.dir, []string{tc.typ})
			if err == nil {
				t.Fatalf("expected error, got nil")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error %q does not contain %q", err.Error(), tc.want)
			}
		})
	}
}
