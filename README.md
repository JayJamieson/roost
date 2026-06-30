# roost

Streaming Go structs land and settle into DuckDB-friendly Parquet.

`roost` reflects a Go struct once into a fast appender, accumulates rows into
Arrow record batches, and at a roll boundary encodes one Parquet object via a
pluggable `Encoder`, written through a pluggable `Sink`. Output reads back with
`SELECT * FROM read_parquet('./data/**/*.parquet', hive_partitioning=true)`.

## Quick start

```go
type Event struct {
    RSN     int64     `roost:"name=rsn"`
    Time    time.Time `roost:"name=event_time"`
    Region  string    `roost:"name=region,partition"` // -> region=us-east-1/...
    Payload []byte
}

sink, _ := local.New("./data")
w, _ := roost.NewWriter[Event](ctx, sink,
    roost.WithCodec("zstd"),
    roost.WithRollRows(1_000_000),
    roost.WithEncodeConcurrency(4),
)
for ev := range stream {
    w.Append(ev)
}
w.Close()
```

## Layout

- `roost` - `Writer[T]`, options, `Sink`/`Encoder` interfaces, pqarrow encoder.
- `roost/sink/local` - filesystem sink (optional fsync).
- `roost/sink/s3` - S3 / R2 sink: single PutObject per object, spill-to-file,
  optional bandwidth limit. No multipart by design.
- `roost/limit` - byte-rate token bucket + io wrappers.
- DuckDB encoder lives in the core package behind build tag `duckdb`.

## Two encoders - pick per environment

| | pqarrow (default) | duckdb (`-tags duckdb`) |
|---|---|---|
| Dependency | pure Go, cross-compiles | CGO + libduckdb |
| Raw single-file speed | good | ~40% faster |
| Concurrency | file/segment fan-out (`WithEncodeConcurrency`) | internal C++ threads |
| Memory | on-heap, measurable | off-heap (C++) |
| In-write transforms | none (passthrough) | SQL (sort/cluster/aggregate) |
| Use when | embedded, serverless, no-CGO, objwal | DuckDB already in stack, need transforms |

Same `Writer[T]` surface either way; swap via `WithEncoder(roost.NewDuckDBEncoder(...))`.

## Code generation (optional, zero-reflection)

`NewWriter[T]` uses reflection: no setup, works on any struct immediately. For
hot ingest paths where allocations matter, `roostgen` emits a typed appender
that removes the per-row reflection, in the easyjson style - your `roost:"..."`
tags are read at *generate* time instead of being interpreted on every row.

```go
//go:generate go run github.com/jayjamieson/roost/cmd/roostgen -type Metric
```

```sh
go generate ./...   # writes metric_roost.go next to your type
```

Then swap the constructor - everything else (options, sinks, encoders,
partitioning) is identical:

```go
// reflection (default)
w, _ := roost.NewWriter[Metric](ctx, sink, opts...)
// generated (zero-reflection)
w, _ := roost.NewWriterFor[Metric](ctx, sink, MetricRoostAppender{}, opts...)
```

Both produce byte-equivalent Parquet (guaranteed by the equivalence test), so
switching is just changing the constructor. Regenerate when the struct changes
(same discipline as easyjson/sqlc). See `examples/codegen` for a worked setup
with a checked-in generated file.

| | Reflection - `NewWriter` | Codegen - `NewWriterFor` + `roostgen` |
|---|---|---|
| Setup | none; works on any struct immediately | `go generate`; regenerate on struct change |
| Allocs/row | higher (struct + `time.Time` boxing) | 1 via `Append`, 0 via `AppendPtr` |
| Hot-path ns/row | reflect overhead | direct field access |
| Type safety | runtime errors from the plan | compile-time (`var _ RowAppender[T]`) |
| Schema visibility | implicit | explicit in generated file |
| Moving parts | one code path | extra generated file + build-time generator dep |
| Supported types | bool, ints, uints, floats, string, `[]byte`, `time.Time`, pointers | the same set |
| Best for | prototyping, moderate throughput, changing schemas | hot ingest on stable schemas |

### Squeezing out the last allocations

`Append(v T)` copies its argument to the heap (one alloc/row) because it takes
the address of the value parameter. To remove it, reuse a row buffer and call
`AppendPtr` - the escape then hoists out of your loop:

```go
row := Metric{Region: "us-east-1"}
for ev := range stream {
    row.TS, row.Host, row.CPU = ev.TS, ev.Host, ev.CPU // refill in place
    w.AppendPtr(&row)
}
```

For partitioned types, `roostgen` also emits `PartitionInto`, which the Writer
uses to build the partition key into a reused buffer instead of allocating a
fresh string per row. Combined with `AppendPtr`, a steady stream into open
partitions appends with **zero allocations**.

Append hot path, partitioned `Metric` with a `time.Time`, no roll (Apple M5):

```
BenchmarkAppendReflection-10     186 ns/op    4 allocs/op   // NewWriter, reflection
BenchmarkAppendGenerated-10       99 ns/op    1 allocs/op   // NewWriterFor, Append(v T)
BenchmarkAppendGeneratedPtr-10    80 ns/op    0 allocs/op   // NewWriterFor, AppendPtr(&row) reused
```

## Performance

```
goos: darwin
goarch: arm64
pkg: github.com/jayjamieson/roost
cpu: Apple M5
BenchmarkAppend-10                               4722384               251.0 ns/op          2019 B/op          8 allocs/op
BenchmarkAppendAndRoll-10                        3871932               317.8 ns/op        327.22 MB/s        3867 B/op          8 allocs/op
BenchmarkRollConcurrency/workers=1-10            1147006               1031 ns/op         100.87 MB/s        3855 B/op          8 allocs/op
BenchmarkRollConcurrency/workers=4-10            4402430               247.5 ns/op        420.29 MB/s        3874 B/op          8 allocs/op
BenchmarkRollConcurrency/workers=16-10          11206383               104.8 ns/op        991.92 MB/s        3855 B/op          8 allocs/op
BenchmarkRollConcurrency/workers=32-10          11164981               110.3 ns/op        942.71 MB/s        3853 B/op          8 allocs/op
BenchmarkRollConcurrencyMem/workers=1-10         1180226               996.5 ns/op              89.01 deltaMB           90.00 peakMB        3893 B/op        8 allocs/op
BenchmarkRollConcurrencyMem/workers=4-10         4608975               231.9 ns/op             346.7 deltaMB           347.7 peakMB         3869 B/op        8 allocs/op
BenchmarkRollConcurrencyMem/workers=16-10        9727112               123.9 ns/op             205.2 deltaMB           206.2 peakMB         3852 B/op        8 allocs/op
BenchmarkRollConcurrencyMem/workers=32-10        9677396               126.5 ns/op             224.7 deltaMB           225.8 peakMB         3852 B/op        8 allocs/op
```

## S3/R2 sink: single PUT, no multipart

The sink buffers each object to a spill temp file, then issues one
`PutObject` on `Close()` streaming from the seekable file with a known
`Content-Length`. This bounds memory (one object, on disk not heap), needs no
multipart state machine, and lets the SDK retry by seeking. Consumers who want
multipart implement their own `Sink` - it's a 2-method interface.

## Bandwidth limiting

`WithRateLimit(bytesPerSec, burst)` wraps the upload body in a shared token
bucket so concurrent object uploads can't saturate a NIC shared with the
ingest path. The limiter preserves `Seek` so SDK retries still work, and
exposes `Stats()` for throughput observability.
