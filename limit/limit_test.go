package limit

import (
	"bytes"
	"io"
	"testing"
	"time"
)

func TestBucketReserveMath(t *testing.T) {
	b := NewBucket(1000, 1000) // 1000 B/s, 1000 B burst
	clock := time.Unix(0, 0)
	b.now = func() time.Time { return clock }
	b.last = clock
	b.tokens = b.burst

	if d := b.reserve(1000); d != 0 {
		t.Fatalf("first 1000 within burst should not wait, got %v", d)
	}
	// No time advances; next 1000 needs a full second.
	d := b.reserve(1000)
	if d < 900*time.Millisecond || d > 1100*time.Millisecond {
		t.Fatalf("expected ~1s wait, got %v", d)
	}
	if total, _ := b.Stats(); total != 2000 {
		t.Fatalf("total = %d, want 2000", total)
	}
}

func TestBucketUnlimitedAccounts(t *testing.T) {
	b := NewBucket(0, 0) // unlimited
	if d := b.reserve(1 << 20); d != 0 {
		t.Fatalf("unlimited should never wait, got %v", d)
	}
	if total, _ := b.Stats(); total != 1<<20 {
		t.Fatalf("total = %d", total)
	}
}

func TestReaderPassthrough(t *testing.T) {
	b := NewBucket(0, 0)
	in := bytes.Repeat([]byte("roost"), 1000)
	got, err := io.ReadAll(b.Reader(bytes.NewReader(in)))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, in) {
		t.Fatal("reader mangled data")
	}
	if total, _ := b.Stats(); total != int64(len(in)) {
		t.Fatalf("accounted %d, want %d", total, len(in))
	}
}

func TestReadSeekerSeeks(t *testing.T) {
	b := NewBucket(0, 0)
	rs := b.ReadSeeker(bytes.NewReader([]byte("0123456789")))
	if _, err := rs.Seek(5, io.SeekStart); err != nil {
		t.Fatal(err)
	}
	got, _ := io.ReadAll(rs)
	if string(got) != "56789" {
		t.Fatalf("got %q after seek", got)
	}
}
