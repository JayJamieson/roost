// Package roost turns a stream of Go structs into Parquet objects laid out
// for DuckDB read_parquet + Hive partitioning.
//
// A Writer[T] reflects T once into a fast per-field appender, accumulates
// rows into Arrow record batches, and at a roll boundary encodes them to one
// Parquet object via a pluggable Encoder, written through a pluggable Sink.
//
// Field tags:
//
//	type Event struct {
//	    RSN     int64     `roost:"name=rsn"`
//	    Time    time.Time `roost:"name=event_time"`
//	    Region  string    `roost:"name=region,partition"` // -> region=us-east-1/...
//	    Status  string    `roost:"name=status,dict"`      // low-cardinality -> dictionary
//	    Payload []byte                                     // high-cardinality -> plain
//	    Skip    string    `roost:"-"`                     // omitted
//	}
//
// Partition columns live in the object path (Hive convention) and are
// projected out of the file body, so a reader recovers them with
// read_parquet('root/**/*.parquet', hive_partitioning=true).
//
// # Reflection vs. code generation
//
// There are two constructors. NewWriter[T] is the zero-setup default: it
// reflects T once and interprets the plan per row. NewWriterFor[T] takes a
// caller-supplied RowAppender[T] and does no reflection at all - typically a
// type emitted by the roostgen command (see cmd/roostgen), which reads the
// roost:"..." tags at generate time and bakes them into typed field access.
// Both produce byte-equivalent Parquet; the generated path trades a build step
// for fewer per-row allocations on hot ingest paths. See examples/codegen.
//
// Append takes *T so a hot path can reuse one row buffer across calls and pay no
// per-row heap copy. For partitioned generated types roostgen also emits a
// PartitionInto method that lets the Writer build partition keys into a reused
// buffer, so appending into already-open partitions allocates nothing.
//
// # Dictionary encoding
//
// Dictionary encoding is OFF by default. Opt a column in with the `dict` tag or
// WithDictionaryColumns when its values are low-cardinality and repetitive
// (enums, status codes, region names): the dictionary shrinks the column and
// usually speeds it up. Leave it off for high-cardinality or unique columns
// (IDs, timestamps, random or binary blobs) - there a dictionary only burns
// memory and CPU on a memo table that never compresses.
//
// # Memory
//
// The Writer streams: each filled row group is encoded and released immediately,
// so resident memory is one row group per open partition, independent of the
// (much larger) roll size. Under high WithEncodeConcurrency the dominant cost is
// the compressor's per-encoder window; WithCompressionLevel trades ratio for a
// smaller window, and the concurrency bound caps how many encoders run at once.
package roost
