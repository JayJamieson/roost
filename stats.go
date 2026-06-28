package roost

import (
	"sync"
	"time"
)

// Stats is a snapshot of a Writer's progress. The counters are cumulative;
// the rates are computed at snapshot time over the window since the first row.
type Stats struct {
	Rows    int64 // rows appended
	Objects int64 // parquet objects finalized
	Bytes   int64 // bytes written to sinks (post-encode)

	Elapsed     time.Duration // wall time since the first appended row
	RowsPerSec  float64       // average rows/sec since the first row
	BytesPerSec float64       // average bytes/sec since the first row
}

// RatesSince returns the rows/sec and bytes/sec observed between a previous
// snapshot and this one. A monitor polling Stats() on an interval (e.g. every
// second) uses this to get an instantaneous rate instead of the average:
//
//	prev := w.Stats()
//	for range time.Tick(time.Second) {
//	    cur := w.Stats()
//	    rps, bps := cur.RatesSince(prev)
//	    log.Printf("%.0f rec/s, %.1f MB/s", rps, bps/1e6)
//	    prev = cur
//	}
func (s Stats) RatesSince(prev Stats) (rowsPerSec, bytesPerSec float64) {
	d := (s.Elapsed - prev.Elapsed).Seconds()
	if d <= 0 {
		return 0, 0
	}
	return float64(s.Rows-prev.Rows) / d, float64(s.Bytes-prev.Bytes) / d
}

type statCounter struct {
	mu    sync.Mutex
	s     Stats
	start time.Time // set on the first row; clock is never read on the hot path otherwise
}

func (c *statCounter) addRows(n int64) {
	c.mu.Lock()
	if c.start.IsZero() {
		c.start = time.Now()
	}
	c.s.Rows += n
	c.mu.Unlock()
}

func (c *statCounter) addObject(b int64) { c.mu.Lock(); c.s.Objects++; c.s.Bytes += b; c.mu.Unlock() }

func (c *statCounter) snapshot() Stats {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := c.s
	if !c.start.IsZero() {
		out.Elapsed = time.Since(c.start)
		if sec := out.Elapsed.Seconds(); sec > 0 {
			out.RowsPerSec = float64(out.Rows) / sec
			out.BytesPerSec = float64(out.Bytes) / sec
		}
	}
	return out
}
