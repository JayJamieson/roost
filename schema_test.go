package roost

import (
	"reflect"
	"testing"
	"time"
)

type Event struct {
	RSN     int64     `roost:"name=rsn"`
	Time    time.Time `roost:"name=event_time"`
	Region  string    `roost:"name=region,partition"`
	Value   *float64  // nullable
	Payload []byte
	Secret  string `roost:"-"`
	hidden  int    // unexported, ignored
}

func TestBuildPlan(t *testing.T) {
	pl, err := buildPlan(reflect.TypeOf(Event{}))
	if err != nil {
		t.Fatal(err)
	}
	gotData := []string{}
	for _, c := range pl.dataCols {
		gotData = append(gotData, c.name)
	}
	want := []string{"rsn", "event_time", "Value", "Payload"}
	if !reflect.DeepEqual(gotData, want) {
		t.Fatalf("data cols = %v, want %v", gotData, want)
	}
	if len(pl.partCols) != 1 || pl.partCols[0].name != "region" {
		t.Fatalf("partition cols = %+v, want [region]", pl.partCols)
	}
	// Partition column must be projected out of the file schema.
	for _, f := range pl.fileSchema.Fields() {
		if f.Name == "region" {
			t.Fatal("region must not be in the file schema")
		}
	}
	// Pointer field is nullable.
	for _, c := range pl.dataCols {
		if c.name == "Value" && !c.nullable {
			t.Fatal("Value should be nullable")
		}
	}
}

func TestBuildPlanRejectsNonStruct(t *testing.T) {
	if _, err := buildPlan(reflect.TypeOf(42)); err == nil {
		t.Fatal("expected error for non-struct")
	}
}

func TestPartitionPath(t *testing.T) {
	pl, _ := buildPlan(reflect.TypeOf(Event{}))
	a := reflectAppender[Event]{pl: pl}
	e := Event{Region: "us east/1"}
	got := string(a.PartitionInto(&e, nil))
	if got != "region=us_east_1" {
		t.Fatalf("path = %q, want region=us_east_1 (sanitized)", got)
	}
}
