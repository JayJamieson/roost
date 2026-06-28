package roost

// config holds resolved Writer settings.
type config struct {
	codec             string
	rowGroupRows      int64
	rollRows          int
	rollBytes         int64 // 0 = disabled; rows-based roll still applies
	maxOpenPartitions int
	concurrency       int
	encoder           Encoder
	dictColumns       []string // columns to dictionary-encode (default: none)
}

func defaultConfig() config {
	return config{
		codec:             "zstd",
		rowGroupRows:      122_880,
		rollRows:          1_000_000,
		maxOpenPartitions: 64,
		concurrency:       1,
	}
}

// Option configures a Writer.
type Option func(*config)

// WithCodec sets the Parquet compression codec for the default encoder
// ("zstd", "snappy", "none"). Ignored if WithEncoder is supplied.
func WithCodec(c string) Option { return func(o *config) { o.codec = c } }

// WithRowGroupRows sets the target Parquet row group size in rows.
func WithRowGroupRows(n int64) Option { return func(o *config) { o.rowGroupRows = n } }

// WithRollRows finalizes the current object once it reaches n rows.
func WithRollRows(n int) Option { return func(o *config) { o.rollRows = n } }

// WithRollBytes finalizes the current object once its buffered uncompressed
// size estimate reaches n bytes (in addition to the row-based roll).
func WithRollBytes(n int64) Option { return func(o *config) { o.rollBytes = n } }

// WithMaxOpenPartitions bounds concurrently-open partitions; the
// least-recently-used is finalized and evicted when the bound is exceeded.
func WithMaxOpenPartitions(n int) Option { return func(o *config) { o.maxOpenPartitions = n } }

// WithEncodeConcurrency hands rolled objects to a pool of n encode/upload
// workers so Append never blocks on encoding or I/O. n<=1 is synchronous.
func WithEncodeConcurrency(n int) Option { return func(o *config) { o.concurrency = n } }

// WithEncoder overrides the encoder (e.g. the DuckDB encoder).
func WithEncoder(e Encoder) Option { return func(o *config) { o.encoder = e } }

// WithDictionaryColumns enables Parquet dictionary encoding for the named
// columns (by their Parquet name, i.e. the name= tag value or field name). It is
// unioned with any columns carrying the `dict` struct tag.
//
// Dictionary encoding shrinks low-cardinality, repetitive columns (enums, status
// codes, region names) and often speeds them up. Leave it off — the default —
// for high-cardinality or unique columns (IDs, timestamps, random/binary blobs):
// there it only burns memory and CPU building a dictionary that never pays off.
//
// Only the default pqarrow encoder honors this; a custom WithEncoder is
// responsible for its own encoding choices.
func WithDictionaryColumns(cols ...string) Option {
	return func(o *config) { o.dictColumns = append(o.dictColumns, cols...) }
}
