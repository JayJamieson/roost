// Package limit provides a byte-rate token bucket and rate-limited io
// wrappers, plus simple throughput accounting. It exists so an upload sink
// can cap bandwidth and avoid saturating a NIC shared with an ingest path.
package limit

import (
	"io"
	"sync"
	"time"
)

// Bucket is a thread-safe byte token bucket. A non-positive rate means
// unlimited (the wrappers still account bytes for Stats).
type Bucket struct {
	mu     sync.Mutex
	rate   float64 // bytes/sec; <=0 == unlimited
	burst  float64
	tokens float64
	last   time.Time
	total  int64
	now    func() time.Time // injectable for tests
	sleep  func(time.Duration)
}

// NewBucket creates a bucket limiting to bytesPerSec with the given burst (in
// bytes). burst<=0 defaults to one second of rate.
func NewBucket(bytesPerSec float64, burst int) *Bucket {
	b := &Bucket{rate: bytesPerSec, burst: float64(burst), now: time.Now, sleep: time.Sleep}
	if b.burst <= 0 {
		b.burst = bytesPerSec
	}
	b.tokens = b.burst
	b.last = b.now()
	return b
}

// reserve consumes n tokens and returns how long the caller must wait first.
func (b *Bucket) reserve(n int) time.Duration {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.total += int64(n)
	if b.rate <= 0 {
		return 0
	}
	now := b.now()
	b.tokens += now.Sub(b.last).Seconds() * b.rate
	if b.tokens > b.burst {
		b.tokens = b.burst
	}
	b.last = now
	b.tokens -= float64(n)
	if b.tokens >= 0 {
		return 0
	}
	return time.Duration(-b.tokens / b.rate * float64(time.Second))
}

func (b *Bucket) take(n int) {
	if d := b.reserve(n); d > 0 {
		b.sleep(d)
	}
}

// Stats returns total bytes accounted and the configured rate.
func (b *Bucket) Stats() (total int64, rate float64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.total, b.rate
}

// Reader wraps r so reads are throttled and counted.
func (b *Bucket) Reader(r io.Reader) io.Reader { return &limReader{b: b, r: r} }

// Writer wraps w so writes are throttled and counted.
func (b *Bucket) Writer(w io.Writer) io.Writer { return &limWriter{b: b, w: w} }

// ReadSeeker wraps rs preserving Seek (so an AWS SDK body can rewind for
// retries) while throttling Read.
func (b *Bucket) ReadSeeker(rs io.ReadSeeker) io.ReadSeeker { return &limReadSeeker{b: b, rs: rs} }

type limReader struct {
	b *Bucket
	r io.Reader
}

func (l *limReader) Read(p []byte) (int, error) {
	n, err := l.r.Read(p)
	if n > 0 {
		l.b.take(n)
	}
	return n, err
}

type limWriter struct {
	b *Bucket
	w io.Writer
}

func (l *limWriter) Write(p []byte) (int, error) {
	l.b.take(len(p))
	return l.w.Write(p)
}

type limReadSeeker struct {
	b  *Bucket
	rs io.ReadSeeker
}

func (l *limReadSeeker) Read(p []byte) (int, error) {
	n, err := l.rs.Read(p)
	if n > 0 {
		l.b.take(n)
	}
	return n, err
}

func (l *limReadSeeker) Seek(off int64, whence int) (int64, error) { return l.rs.Seek(off, whence) }
