// Package roosttag parses the `roost:"..."` struct tag.
//
// It is shared by the reflection path (schema.go's parseTag) and the roostgen
// code generator (cmd/roostgen) so the two can never disagree about what a tag
// means. It depends only on the standard library, so importing it into the
// runtime package roost adds nothing of consequence to the import graph.
package roosttag

import "strings"

// Tag is the parsed form of a single `roost:"..."` struct tag.
type Tag struct {
	// Name is the Parquet column name (the name= value, or the field name).
	Name string
	// Partition marks the field as a Hive partition column (excluded from the
	// file schema, emitted into the partition path instead).
	Partition bool
	// Omit drops the field entirely ("-" or "omit").
	Omit bool
	// Dict requests Parquet dictionary encoding for the column.
	Dict bool
}

// Parse parses the value of a `roost` struct tag, defaulting the column name to
// fieldName when no name= is present. The accepted options are:
//
//	name=<col>   rename the column
//	partition    treat as a Hive partition column
//	dict         dictionary-encode the column
//	-, omit      skip the field
func Parse(raw, fieldName string) Tag {
	t := Tag{Name: fieldName}
	if raw == "" {
		return t
	}
	for _, p := range strings.Split(raw, ",") {
		p = strings.TrimSpace(p)
		switch {
		case p == "-" || p == "omit":
			t.Omit = true
		case p == "partition":
			t.Partition = true
		case p == "dict":
			t.Dict = true
		case strings.HasPrefix(p, "name="):
			t.Name = strings.TrimPrefix(p, "name=")
		}
	}
	return t
}
