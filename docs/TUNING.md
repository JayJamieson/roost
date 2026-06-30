# Tuning roost: memory vs throughput

roost streams each filled row group straight to its encoder and releases it, so
resident memory is one row group per open partition plus the active compressor
state - not a whole object, and not the whole stream. Within that model a few
knobs trade memory, throughput, and file size against each other. This doc
explains each knob and gives measured numbers so you can pick a point on the
curve.

## The knobs

| Option / tag | Effect | Memory | Throughput | File size |
|---|---|---|---|---|
| `WithCompressionLevel(n)` | zstd level -> window size | dominant lever | dominant lever | minor on high-entropy data, large on compressible data |
| `WithEncodeConcurrency(n)` | parallel encode workers | +N compressor windows | helps only when compression/upload is the bottleneck | none |
| `WithCodec("zstd"\|"snappy"\|"none")` | compressor | snappy/none ≪ zstd | none/snappy faster | zstd smallest |
| `WithRowGroupRows(n)` | rows buffered before a row group flushes | linear in n | larger = better ratio, fewer flushes | larger = better ratio |
| `WithRollRows` / `WithRollBytes` | rows/bytes per object | independent of roll (streaming) | larger = fewer objects/uploads | - |
| `WithMaxOpenPartitions(n)` | concurrently open partitions | linear in n (one row group each) | - | - |
| `dict` tag / `WithDictionaryColumns` | per-column dictionary | + memo table per dict column | helps low-cardinality | smaller for low-cardinality |

Two facts that drive everything below:

1. Encode concurrency parallelizes _across objects_, not within one object.
   Each object is pinned to one worker so its row groups stay ordered. You only
   benefit from `WithEncodeConcurrency` when several objects are in flight at
   once - i.e. many partitions, or a roll size near the row-group size. A single
   partition rolling at 1M rows with 128k row groups keeps one object open at a
   time and won't use extra workers.

2. Peak resident memory is dominated by the zstd compressor window. Each
   active encoder holds a history/window buffer; the default zstd level uses a
   large one. So peak ≈ (compressor window) × (concurrent encoders). Both factors
   are tunable: the window via `WithCompressionLevel`, the count via
   `WithEncodeConcurrency`.

## Measured: throughput vs memory

Apple M5, `go test` matrix (`matrix_test.go`), 2,000,000 rows of a 104-byte
record with an incompressible 64-byte payload, single partition,
`RowGroupRows == RollRows == 50_000` (one row group per object, so encode
concurrency is exercised), `nopSink` (discards bytes - this is the encode
ceiling, not a disk/network-bound rate). Max RSS is the real process resident
size from `/usr/bin/time -l`.

| codec / level | concurrency | records/s | input MB/s | input Mbps | disk MB/s | max RSS |
|---|---:|---:|---:|---:|---:|---:|
| zstd (default) | 1 | 2.97 M | 309 | 2 470 | 231 | 974 MB |
| zstd (default) | 2 | 5.06 M | 526 | 4 210 | 394 | 1 311 MB |
| zstd (default) | 4 | 7.27 M | 756 | 6 050 | 566 | 1 490 MB |
| zstd (default) | 8 | 7.36 M | 765 | 6 120 | 573 | 1 521 MB |
| zstd level 1 | 1 | 9.43 M | 980 | 7 840 | 758 | 496 MB |
| zstd level 1 | 2 | 9.78 M | 1 017 | 8 140 | 787 | 665 MB |
| zstd level 1 | 4 | 9.80 M | 1 019 | 8 155 | 789 | 723 MB |
| zstd level 1 | 8 | 9.79 M | 1 018 | 8 140 | 788 | 643 MB |

### What this says

- Compression level is the biggest lever, by far. `WithCompressionLevel(1)`
  at concurrency 1 is 3.2× the throughput and half the memory of the default
  level. The default zstd level spends a lot of CPU and a large window; on
  high-entropy data it buys almost no ratio (disk MB/s is similar), so it's pure
  cost here.
- Concurrency helps only when compression is the bottleneck. At the
  expensive default level, 1->4 workers nearly triples throughput - but adds ~50%
  RSS (more concurrent windows). At level 1, compression is cheap enough that one
  worker nearly saturates the pipeline, so extra workers add memory for almost no
  gain.
- Diminishing returns past `GOMAXPROCS`-ish. 4->8 workers barely moves
  throughput but keeps costing memory.

### Caveats when reading these numbers

- `nopSink` removes I/O. Against a real object store the upload latency, not
  compression, is usually the bottleneck - there `WithEncodeConcurrency`
  overlaps uploads and helps even at cheap compression levels. Benchmark with
  your sink.
- The payload here is incompressible, so level barely changes file size. On
  compressible data (logs, JSON), higher levels meaningfully shrink files at the
  CPU/memory cost shown - that's the real trade.
- RSS includes the Go runtime, the binary, and GC headroom (`GOGC=100` lets the
  heap float to ~2× live). `GOMEMLIMIT` caps it at the cost of more frequent GC.

## Recommendations

- High-entropy payloads (blobs, random IDs, pre-compressed data): low level
  (`WithCompressionLevel(1)`) or `WithCodec("snappy")`. Concurrency 1 is usually
  enough. Lowest memory, highest throughput, negligible size penalty.
- Compressible data, throughput-priority: `WithCompressionLevel(1..3)` and
  `WithEncodeConcurrency(GOMAXPROCS)` if you have many partitions/objects.
- Compressible data, size-priority: default or higher level; accept the
  CPU/memory cost. Raise `WithRowGroupRows` for better ratio.
- Memory-constrained: keep concurrency low, pick a low level (smaller
  window), cap `WithMaxOpenPartitions`, and set `GOMEMLIMIT`.
- Object stores (upload-bound): raise `WithEncodeConcurrency` to overlap
  uploads regardless of level; memory grows with in-flight objects, so pair with
  a low level if constrained.

## Reproduce

```sh
go test -c -o roost.test .
for level in 0 1; do for conc in 1 2 4 8; do
  ROOST_MATRIX=1 ROOST_CONC=$conc ROOST_LEVEL=$level ROOST_CODEC="zstd" ROOST_N=2000000 \
    /usr/bin/time -l ./roost.test -test.run TestMemThroughputMatrix
done; done
# prints: CSV,conc,level,recPerSec,inputMBps,diskMBps,peakHeapMB  + RSS from time
```
