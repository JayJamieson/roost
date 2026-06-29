package roost

import (
	"io"

	"github.com/apache/arrow-go/v18/arrow"
)

// objState is the per-object encode state shared by all of an object's ops. All
// ops for one object are routed to a single worker (affinity), so failed is only
// ever touched by that one goroutine — no synchronization needed.
type objState struct {
	obj    ObjectEncoder
	wc     io.WriteCloser
	cw     *countWriter
	failed bool
}

// encodeOp is one unit of work for an encode worker: write rec as a row group,
// or, when rec is nil, finalize the object (footer + upload).
type encodeOp struct {
	os  *objState
	rec arrow.RecordBatch
}

// encodeWorker drains one worker channel. A fixed set of these (sized by
// WithEncodeConcurrency) does all compression and upload off the Writer's lock,
// so the goroutine count is bounded regardless of how many objects churn. Each
// object is pinned to one worker, so its row groups are written in order.
func (w *Writer[T]) encodeWorker(ch <-chan encodeOp) {
	defer w.wg.Done()
	for op := range ch {
		os := op.os
		if op.rec != nil {
			if !os.failed {
				if err := os.obj.Write(op.rec); err != nil {
					os.failed = true
					w.setEncErr(err)
				}
			}
			op.rec.Release()
			continue
		}
		// rec == nil: finalize.
		if err := os.obj.Close(); err != nil && !os.failed {
			os.failed = true
			w.setEncErr(err)
		}
		if !os.failed {
			w.stats.addObject(os.cw.n)
		}
		if err := os.wc.Close(); err != nil {
			w.setEncErr(err)
		}
	}
}

// setEncErr records the first error seen by any encode worker.
func (w *Writer[T]) setEncErr(err error) {
	w.encMu.Lock()
	if w.encErr == nil {
		w.encErr = err
	}
	w.encMu.Unlock()
}

// encErrLoad returns the first encode error seen so far, if any.
func (w *Writer[T]) encErrLoad() error {
	w.encMu.Lock()
	defer w.encMu.Unlock()
	return w.encErr
}
