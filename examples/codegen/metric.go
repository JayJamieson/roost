// Package codegen demonstrates roost's zero-reflection code-generation path.
//
// Metric carries the roost:"..." tags; roostgen reads them at generate time and
// emits metric_roost.go (checked in, as easyjson does). Regenerate with:
//
//	go generate ./...
//
// It is an importable package (not package main) so the equivalence and
// allocation benchmarks in the repo root can write the same type through both
// the reflection and generated paths; the runnable demo lives in Example below.
package codegen

import "time"

// Metric is the worked example from the spec: a timestamp, a couple of scalar
// columns, a nullable pointer, a Hive partition column, and an omitted field.
type Metric struct {
	TS     time.Time `roost:"name=ts"`
	Host   string    `roost:"name=host"`
	CPU    float64   `roost:"name=cpu"`
	Value  *float64  `roost:"name=value"`            // nullable
	Region string    `roost:"name=region,partition"` // Hive partition column
	Secret string    `roost:"-"`                     // omitted
}

//go:generate go run github.com/jayjamieson/roost/cmd/roostgen -type Metric
