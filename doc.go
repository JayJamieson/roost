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
//	    Payload []byte
//	    Skip    string    `roost:"-"`                     // omitted
//	}
//
// Partition columns live in the object path (Hive convention) and are
// projected out of the file body, so a reader recovers them with
// read_parquet('root/**/*.parquet', hive_partitioning=true).
package roost
