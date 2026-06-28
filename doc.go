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
// # Dictionary encoding
//
// Dictionary encoding is OFF by default. Opt a column in with the `dict` tag or
// WithDictionaryColumns when its values are low-cardinality and repetitive
// (enums, status codes, region names): the dictionary shrinks the column and
// usually speeds it up. Leave it off for high-cardinality or unique columns
// (IDs, timestamps, random or binary blobs) — there a dictionary only burns
// memory and CPU on a memo table that never compresses. The two mechanisms are
// unioned, and only the default pqarrow encoder honors them.
//
// # Append strategies
//
// Append accepts T by value, which boxes it for reflection (one heap allocation
// per row). AppendPtr takes *T and avoids that box; for a caller holding
// addressable data (for example &slice[i]) it is the cheapest option by bytes
// allocated and needs no unsafe. AppendUnsafe reads fields by precomputed offset
// and is the fastest in CPU, but taking &v escapes the struct to the heap, so it
// does not reduce the allocation count. Prefer AppendPtr unless a profile shows
// the per-row reflect time is the bottleneck. See BenchmarkAppendStrategies.
package roost
