package roost

import "sync"

// Stats is a snapshot of a Writer's progress.
type Stats struct {
	Rows    int64 // rows appended
	Objects int64 // parquet objects finalized
	Bytes   int64 // bytes written to sinks (post-encode)
}

type statCounter struct {
	mu sync.Mutex
	s  Stats
}

func (c *statCounter) addRows(n int64)   { c.mu.Lock(); c.s.Rows += n; c.mu.Unlock() }
func (c *statCounter) addObject(b int64) { c.mu.Lock(); c.s.Objects++; c.s.Bytes += b; c.mu.Unlock() }
func (c *statCounter) snapshot() Stats   { c.mu.Lock(); defer c.mu.Unlock(); return c.s }
