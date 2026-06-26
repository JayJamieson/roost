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
