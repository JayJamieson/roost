// Command roostgen generates a zero-reflection roost.RowAppender[T] for one or
// more struct types, in the easyjson style: the roost:"..." tags are read at
// generate time and baked into typed field access, instead of being
// interpreted by reflection on every row.
//
// Usage:
//
//	roostgen -type Metric [-type Metric,Other] [-output file.go]
//
// Typically invoked via go:generate next to the type definition:
//
//	//go:generate go run github.com/jayjamieson/roost/cmd/roostgen -type Metric
//
// Then `go generate ./...` writes <type>_roost.go next to the struct, and you
// swap roost.NewWriter[T] for roost.NewWriterFor[T](..., TRoostAppender{}).
//
// Supported field types match roost's reflection path exactly (see SPEC §5.3):
// bool, int/int32/int64, uint/uint32/uint64, float32/float64, string, []byte,
// time.Time, and pointers to any of those (nullable). Anything else is a
// generate-time error rather than a silent skip.
package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintf(os.Stderr, "roostgen: %v\n", err)
		os.Exit(1)
	}
}

func run(argv []string) error {
	fs := flag.NewFlagSet("roostgen", flag.ContinueOnError)
	typeFlag := fs.String("type", "", "struct type name(s), comma-separated (required)")
	output := fs.String("output", "", "output file (default <lowercased-first-type>_roost.go)")
	if err := fs.Parse(argv); err != nil {
		return err
	}
	if *typeFlag == "" {
		return fmt.Errorf("-type is required")
	}
	types := splitTypes(*typeFlag)
	if len(types) == 0 {
		return fmt.Errorf("-type is required")
	}

	dir := "." // go:generate runs in the package directory
	src, err := generate(dir, types)
	if err != nil {
		return err
	}

	out := *output
	if out == "" {
		out = strings.ToLower(types[0]) + "_roost.go"
	}
	if err := os.WriteFile(filepath.Join(dir, out), src, 0o644); err != nil {
		return fmt.Errorf("writing %s: %w", out, err)
	}
	return nil
}

// splitTypes parses the comma-separated -type value, trimming spaces and
// dropping empties.
func splitTypes(s string) []string {
	var out []string
	for _, p := range strings.Split(s, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}
