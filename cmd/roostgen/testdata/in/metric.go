package in

import "time"

// Metric is the golden fixture: it covers every supported field type, a
// nullable pointer (data and partition-adjacent), a renamed field, an omitted
// field, an unexported field, and both a string and an integer partition
// column (the latter forcing the strconv import).
type Metric struct {
	TS       time.Time  `roost:"name=ts"`
	Host     string     `roost:"name=host"`
	CPU      float64    `roost:"name=cpu"`
	Mem      float32    `roost:"name=mem"`
	Count    int        `roost:"name=count"`
	Seq      int64      `roost:"name=seq"`
	Small    int32      `roost:"name=small"`
	UCount   uint       `roost:"name=ucount"`
	UBig     uint64     `roost:"name=ubig"`
	USmall   uint32     `roost:"name=usmall"`
	OK       bool       `roost:"name=ok"`
	Blob     []byte     `roost:"name=blob"`
	Value    *float64   `roost:"name=value"`     // nullable data column
	LastSeen *time.Time `roost:"name=last_seen"` // nullable time (paren path)
	NID      *int64     `roost:"name=nid"`       // nullable int (conversion path)
	Region   string     `roost:"name=region,partition"`
	Shard    int        `roost:"name=shard,partition"`
	Secret   string     `roost:"-"` // omitted
	internal string     // unexported, skipped
}
