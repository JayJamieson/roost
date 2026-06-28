package roost

import (
	"context"
	"errors"
	"fmt"
	"path"
	"reflect"
	"sync"
	"time"
	"unsafe"

	"github.com/apache/arrow-go/v18/arrow"
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
	pool   *encodePool
	stats  statCounter
	closed bool
}

type partState struct {
	pathSeg  string
	app      *appender
	recs     []arrow.RecordBatch
	objRows  int
	objBytes int64
	seq      int
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
		w.enc = NewPqarrowEncoder(cfg.codec, cfg.rowGroupRows, unionDictCols(pl.dictCols, cfg.dictColumns)...)
	}
	if cfg.concurrency > 1 {
		w.pool = newEncodePool(cfg.concurrency, w.runJob)
	}
	return w, nil
}

// Append writes one record via the value-reflection strategy: it boxes v into
// an interface for reflect, which costs one heap allocation per row. See
// AppendPtr and AppendUnsafe for allocation-free alternatives.
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
	ps, err := w.getPartLocked(partitionPath(rv, w.pl.partCols))
	if err != nil {
		return err
	}
	ps.app.appendRow(rv)
	return w.postAppendLocked(ps)
}

// AppendPtr is like Append but takes a pointer. Boxing a pointer into reflect's
// interface does not allocate, and .Elem() yields an addressable Value into the
// caller's storage, so a caller holding addressable data (e.g. &slice[i]) pays
// no per-row allocation. Field reads still go through reflection.
func (w *Writer[T]) AppendPtr(v *T) error {
	if v == nil {
		return errNil
	}
	rv := reflect.ValueOf(v).Elem()
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return errClosed
	}
	ps, err := w.getPartLocked(partitionPath(rv, w.pl.partCols))
	if err != nil {
		return err
	}
	ps.app.appendRow(rv)
	return w.postAppendLocked(ps)
}

// AppendUnsafe is like Append but reads fields through precomputed byte offsets
// (computed once from the compiler's struct layout) instead of reflection on the
// hot path. It is the fastest strategy in CPU terms because it does no reflect
// work per field and never boxes time.Time.
//
// It does NOT, however, eliminate the per-row allocation: &v is taken and the
// resulting pointer flows into appendRowUnsafe, whose per-field accessor is an
// indirect closure call, so escape analysis conservatively moves v to the heap.
// In other words it trades the value strategy's reflect-boxing allocation for an
// argument-escape allocation of the same struct. If allocation count is the
// priority, AppendPtr allocates the fewest bytes (see BenchmarkAppendStrategies).
//
// Safety: offsets come from reflect.StructField.Offset for this exact T; field
// pointers are derived with unsafe.Add (never uintptr arithmetic) so the GC
// tracks them; every read is immediately copied into the builder, so nothing
// aliases v after the call. The equivalence and value tests run under
// `go test -race`, which enables checkptr to validate the pointer arithmetic.
func (w *Writer[T]) AppendUnsafe(v T) error {
	base := unsafe.Pointer(&v)
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return errClosed
	}
	ps, err := w.getPartLocked(partitionPathUnsafe(base, w.pl.partCols))
	if err != nil {
		return err
	}
	ps.app.appendRowUnsafe(base)
	return w.postAppendLocked(ps)
}

// getPartLocked returns the partState for key, creating and (if necessary)
// evicting to make room. Caller holds w.mu. Takes no closures so all three
// Append variants share routing without incurring an allocation.
func (w *Writer[T]) getPartLocked(key string) (*partState, error) {
	ps := w.parts[key]
	if ps == nil {
		if err := w.evictIfNeeded(); err != nil {
			return nil, err
		}
		ps = &partState{pathSeg: key, app: newAppender(w.mem, w.pl.fileSchema, w.pl.dataCols)}
		w.parts[key] = ps
	}
	w.touch(key)
	return ps, nil
}

// postAppendLocked records one buffered row, snapshots a record at the row-group
// boundary, and rolls the object when it reaches its size limit. Caller holds
// w.mu and has already appended the row to ps.app.
func (w *Writer[T]) postAppendLocked(ps *partState) error {
	ps.objRows++
	ps.objBytes += w.rowBytesEstimate()
	w.stats.addRows(1)
	if int64(ps.app.rows) >= w.cfg.rowGroupRows {
		ps.recs = append(ps.recs, ps.app.newRecord())
	}
	if ps.objRows >= w.cfg.rollRows || (w.cfg.rollBytes > 0 && ps.objBytes >= w.cfg.rollBytes) {
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

// Close finalizes all objects, drains the encode pool, and releases builders.
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

	if w.pool != nil {
		if err := w.pool.close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// Stats returns a snapshot of progress.
func (w *Writer[T]) Stats() Stats { return w.stats.snapshot() }

// finalizeLocked emits the current object for ps. Caller holds w.mu.
func (w *Writer[T]) finalizeLocked(ps *partState) error {
	if ps.app.rows > 0 {
		ps.recs = append(ps.recs, ps.app.newRecord())
	}
	if len(ps.recs) == 0 {
		return nil
	}
	job := encodeJob{name: w.objectName(ps), recs: ps.recs}
	ps.recs = nil
	ps.objRows = 0
	ps.objBytes = 0
	ps.seq++
	if w.pool != nil {
		w.pool.submit(job) // async; errors surface at Close
		return nil
	}
	return w.runJob(job)
}

// runJob encodes one object and writes it through the sink. Safe for pool
// workers: it touches only the sink, the encoder, and the (locked) stats.
func (w *Writer[T]) runJob(j encodeJob) error {
	defer releaseRecs(j.recs)
	wc, err := w.sink.Create(w.ctx, j.name)
	if err != nil {
		return err
	}
	cw := &countWriter{w: wc}
	encErr := w.enc.EncodeObject(w.ctx, cw, w.pl.fileSchema, j.recs)
	closeErr := wc.Close()
	if encErr != nil {
		return encErr
	}
	if closeErr != nil {
		return closeErr
	}
	w.stats.addObject(cw.n)
	return nil
}

func (w *Writer[T]) objectName(ps *partState) string {
	file := fmt.Sprintf("part-%016x-%05d.parquet", w.runID, ps.seq)
	if ps.pathSeg == "" {
		return file
	}
	return path.Join(ps.pathSeg, file)
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
	for _, c := range tag {
		if _, ok := seen[c]; !ok {
			seen[c] = struct{}{}
			out = append(out, c)
		}
	}
	for _, c := range opt {
		if _, ok := seen[c]; !ok {
			seen[c] = struct{}{}
			out = append(out, c)
		}
	}
	return out
}

func releaseRecs(recs []arrow.RecordBatch) {
	for _, r := range recs {
		r.Release()
	}
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
