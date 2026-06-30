package roost_test

import (
	"testing"

	"github.com/jayjamieson/roost"
	"github.com/jayjamieson/roost/examples/codegen"
)

// TestAppendSanitizedMatchesSanitizeSegment locks the buffer-appending helper to
// the string form, so generated PartitionInto keys are byte-identical to the
// reflection path's SanitizeSegment.
func TestAppendSanitizedMatchesSanitizeSegment(t *testing.T) {
	cases := []string{
		"", "us-east-1", "plain", "null", "2026-06-29",
		"a/b\\c=d e\tf\ng\rh\"i'j", "ünïcödé/x", "  ", "=",
	}
	for _, s := range cases {
		want := roost.SanitizeSegment(s)
		if got := string(roost.AppendSanitized(nil, s)); got != want {
			t.Errorf("AppendSanitized(%q) = %q, want %q", s, got, want)
		}
		if got := string(roost.AppendSanitized([]byte("PRE/"), s)); got != "PRE/"+want {
			t.Errorf("AppendSanitized onto prefix for %q = %q, want %q", s, got, "PRE/"+want)
		}
	}
}

// TestPartitionIntoMatchesPartition asserts the generated appender's two
// partition representations agree, so the Writer's zero-alloc path produces the
// same Hive layout as Partition.
func TestPartitionIntoMatchesPartition(t *testing.T) {
	app := codegen.MetricRoostAppender{}
	pa, ok := any(app).(roost.PartitionAppender[codegen.Metric])
	if !ok {
		t.Fatal("MetricRoostAppender does not implement roost.PartitionAppender")
	}
	regions := []string{"us-east-1", "a/b=c d", "", "ünïcödé", `q"o'te`}
	for _, r := range regions {
		m := codegen.Metric{Region: r}
		want := app.Partition(&m)
		if got := string(pa.PartitionInto(&m, nil)); got != want {
			t.Errorf("PartitionInto for region %q = %q, want %q", r, got, want)
		}
	}
}
