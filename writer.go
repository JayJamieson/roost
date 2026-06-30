package roost

import (
	"context"
	"errors"
	"fmt"
	"path"
	"reflect"
	"sync"
	"time"

	"github.com/apache/arrow-go/v18/arrow/memory"
)

var (
	errClosed = errors.New("roost: writer is closed")
	errNil    = errors.New("roost: appended a nil pointer")
)

// Writer encodes a stream of T into Parquet objects via a Sink.
type Writer[T any] struct {
	ctx   context.Context
	sink  Sink
	enc   Encoder
	mem   memory.Allocator
	pl    *plan
	cfg   config
	runID int64

	mu     sync.Mutex
	parts  map[string]*partState
	order  []string // LRU of partition keys
	stats  statCounter
	closed bool

	// Fixed pool of encode workers: compression + upload run off w.mu, on a
	// bounded set of goroutines. Each object is pinned to one worker (round-robin
	// at open) so its row groups stay ordered. nextWorker/objSeq are guarded by mu.
	workers    []chan encodeOp
	nextWorker int
	objSeq     int // monotonic across all objects so names never collide on eviction
	wg         sync.WaitGroup
	encMu      sync.Mutex
	encErr     error
}

// partState holds one partition's open object. Instead of buffering every
// record until the roll boundary, it streams each filled row group to its
// assigned encode worker, so resident memory is one row group rather than a
// whole object's worth of records.
type partState struct {
	pathSeg  string
	app      *appender
	os       *objState     // nil until the first row group is flushed
	ch       chan encodeOp // the worker this object is pinned to
	objRows  int
	objBytes int64
}

// NewWriter reflects T, validates it, and returns a ready Writer.
func NewWriter[T any](ctx context.Context, sink Sink, opts ...Option) (*Writer[T], error) {
	if sink == nil {
		return nil, errors.New("roost: nil sink")
	}
	pl, err := buildPlan(reflect.TypeOf((*T)(nil)).Elem())
	if err != nil {
		return nil, err
	}
	cfg := defaultConfig()
	for _, o := range opts {
		o(&cfg)
	}
	w := &Writer[T]{
		ctx: ctx, sink: sink, mem: memory.DefaultAllocator, pl: pl, cfg: cfg,
		runID: time.Now().UnixNano(), parts: make(map[string]*partState),
	}
	if cfg.encoder != nil {
		w.enc = cfg.encoder
	} else {
		w.enc = NewPqarrowEncoder(cfg.codec, cfg.rowGroupRows,
			PqarrowDictionaryColumns(unionDictCols(pl.dictCols, cfg.dictColumns)...),
			PqarrowCompressionLevel(cfg.codecLevel))
	}
	// A fixed pool of n encode workers runs compression + upload off w.mu. Even
	// at concurrency 1 the single worker pipelines encoding with Append. The
	// goroutine count is exactly n regardless of partition/object churn.
	n := cfg.concurrency
	if n < 1 {
		n = 1
	}
	w.workers = make([]chan encodeOp, n)
	w.wg.Add(n)
	for i := range w.workers {
		ch := make(chan encodeOp, 2) // small buffer lets Append run ~1 row group ahead of the compressor
		w.workers[i] = ch
		go w.encodeWorker(ch)
	}
	return w, nil
}

// Append writes one record. Cheap path: reflect, route, append to builders,
// and flush a row group through the open encoder when one fills.
func (w *Writer[T]) Append(v T) error {
	rv := reflect.ValueOf(v)
	for rv.Kind() == reflect.Pointer {
		if rv.IsNil() {
			return errNil
		}
		rv = rv.Elem()
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return errClosed
	}

	key := partitionPath(rv, w.pl.partCols)
	ps := w.parts[key]
	if ps == nil {
		if err := w.evictIfNeeded(); err != nil {
			return err
		}
		ps = &partState{pathSeg: key, app: newAppender(w.mem, w.pl.fileSchema, w.pl.dataCols)}
		w.parts[key] = ps
	}
	w.touch(key)

	ps.app.appendRow(rv)
	ps.objRows++
	ps.objBytes += w.rowBytesEstimate()
	w.stats.addRows(1)

	if int64(ps.app.rows) >= w.cfg.rowGroupRows {
		if err := w.flushRowGroup(ps); err != nil {
			return err
		}
	}
	if ps.objRows >= w.cfg.rollRows || (w.cfg.rollBytes > 0 && ps.objBytes >= w.cfg.rollBytes) {
		// fmt.Printf("ps.objRows: %d >= w.cfg.rollRows: %d || w.cfg.rollBytes: %d > ps.objBytes: %d > w.cfg.rollBytes: %d \n", ps.objRows, w.cfg.rollRows, w.cfg.rollBytes, ps.objBytes, w.cfg.rollBytes)
		// fmt.Printf("%v, %v\n", ps.objRows >= w.cfg.rollRows, w.cfg.rollBytes > 0 && ps.objBytes >= w.cfg.rollBytes)
		return w.finalizeLocked(ps)
	}
	return nil
}

// Flush finalizes every open object, starting fresh objects afterward.
func (w *Writer[T]) Flush() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return errClosed
	}
	var firstErr error
	for _, ps := range w.parts {
		if err := w.finalizeLocked(ps); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// Close finalizes all objects, waits for the encode goroutines to drain, and
// releases builders.
func (w *Writer[T]) Close() error {
	w.mu.Lock()
	if w.closed {
		w.mu.Unlock()
		return nil
	}
	w.closed = true
	var firstErr error
	for _, ps := range w.parts {
		if err := w.finalizeLocked(ps); err != nil && firstErr == nil {
			firstErr = err
		}
		ps.app.release()
	}
	w.mu.Unlock()

	for _, ch := range w.workers {
		close(ch) // no more ops will be sent (closed == true)
	}
	w.wg.Wait() // workers drain queued ops (footer + upload) and exit
	if ee := w.encErrLoad(); ee != nil && firstErr == nil {
		firstErr = ee
	}
	return firstErr
}

// Stats returns a snapshot of progress.
func (w *Writer[T]) Stats() Stats { return w.stats.snapshot() }

// startObject opens the sink object + encoder for ps and pins it to one encode
// worker (round-robin). Caller holds w.mu; called lazily on the first row group
// flush so a partition that never fills a row group costs nothing until it
// finalizes.
func (w *Writer[T]) startObject(ps *partState) error {
	name := w.objectName(ps.pathSeg, w.objSeq)
	w.objSeq++
	wc, err := w.sink.Create(w.ctx, name)
	if err != nil {
		return err
	}
	cw := &countWriter{w: wc}
	obj, err := w.enc.Open(w.ctx, cw, w.pl.fileSchema)
	if err != nil {
		wc.Close()
		return err
	}
	ps.os = &objState{obj: obj, wc: wc, cw: cw}
	ps.ch = w.workers[w.nextWorker%len(w.workers)]
	w.nextWorker++
	return nil
}

// flushRowGroup snapshots the buffered rows as one record and hands it to the
// object's assigned worker (in order). The worker's bounded channel applies
// backpressure so Append runs at most ~one row group ahead of the compressor.
// The send may briefly block on the buffer but never on compression itself.
func (w *Writer[T]) flushRowGroup(ps *partState) error {
	if ps.app.rows == 0 {
		return nil
	}
	if ps.os == nil {
		if err := w.startObject(ps); err != nil {
			return err
		}
	}
	ps.ch <- encodeOp{os: ps.os, rec: ps.app.newRecord()} // worker compresses + releases
	return w.encErrLoad()
}

// finalizeLocked queues the footer + upload for ps's object on its worker and
// resets ps for the next object. Caller holds w.mu. Finalization is
// asynchronous; errors surface at a later call or at Close.
func (w *Writer[T]) finalizeLocked(ps *partState) error {
	if err := w.flushRowGroup(ps); err != nil { // any leftover rows < a row group
		return err
	}
	if ps.os == nil {
		return nil // nothing was ever written for this object
	}
	ps.ch <- encodeOp{os: ps.os} // rec == nil: worker writes footer + upload
	ps.os = nil
	ps.ch = nil
	ps.objRows = 0
	ps.objBytes = 0
	return w.encErrLoad()
}

// objectName builds a unique object path. seq is monotonic across the whole
// Writer, so names never collide even when a partition is evicted and reopened.
func (w *Writer[T]) objectName(pathSeg string, seq int) string {
	file := fmt.Sprintf("part-%016x-%05d.parquet", w.runID, seq)
	if pathSeg == "" {
		return file
	}
	return path.Join(pathSeg, file)
}

// rowBytesEstimate is a cheap fixed estimate for byte-based rolling. It is
// intentionally approximate; exact sizing requires the encoded footer.
func (w *Writer[T]) rowBytesEstimate() int64 { return 256 }

// LRU helpers ---------------------------------------------------------------

func (w *Writer[T]) touch(key string) {
	for i, k := range w.order {
		if k == key {
			w.order = append(w.order[:i], w.order[i+1:]...)
			break
		}
	}
	w.order = append(w.order, key)
}

func (w *Writer[T]) evictIfNeeded() error {
	if w.cfg.maxOpenPartitions <= 0 || len(w.parts) < w.cfg.maxOpenPartitions {
		return nil
	}
	victim := w.order[0]
	w.order = w.order[1:]
	ps := w.parts[victim]
	delete(w.parts, victim)
	if ps == nil {
		return nil
	}
	err := w.finalizeLocked(ps)
	ps.app.release()
	return err
}

// unionDictCols merges the tag-derived and option-derived dictionary column
// lists, de-duplicating while keeping a stable order (tag columns first).
func unionDictCols(tag, opt []string) []string {
	if len(tag) == 0 && len(opt) == 0 {
		return nil
	}
	seen := make(map[string]struct{}, len(tag)+len(opt))
	out := make([]string, 0, len(tag)+len(opt))
	for _, c := range append(append([]string(nil), tag...), opt...) {
		if _, ok := seen[c]; !ok {
			seen[c] = struct{}{}
			out = append(out, c)
		}
	}
	return out
}

// countWriter counts bytes written for Stats.
type countWriter struct {
	w interface{ Write([]byte) (int, error) }
	n int64
}

func (c *countWriter) Write(p []byte) (int, error) {
	n, err := c.w.Write(p)
	c.n += int64(n)
	return n, err
}
