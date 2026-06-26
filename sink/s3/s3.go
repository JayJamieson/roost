// Package s3 is a roost.Sink for S3 / Cloudflare R2. Each object is written
// to a spill temp file and uploaded with a single PutObject on Close — no
// multipart (consumers who need multipart implement their own sink). The
// upload streams from the seekable spill file with a known Content-Length and
// can be bandwidth-limited by a shared token bucket.
package s3

import (
	"context"
	"io"
	"os"
	"path"

	"github.com/aws/aws-sdk-go-v2/aws"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/jayjamieson/roost/limit"
)

// ObjectPutter is the subset of *awss3.Client this sink uses (so tests can
// supply a fake).
type ObjectPutter interface {
	PutObject(context.Context, *awss3.PutObjectInput, ...func(*awss3.Options)) (*awss3.PutObjectOutput, error)
}

// Sink uploads objects to a bucket under an optional key prefix.
type Sink struct {
	p           ObjectPutter
	bucket      string
	prefix      string
	contentType string
	spillDir    string
	bucketLimit *limit.Bucket
}

// Option configures the sink.
type Option func(*Sink)

// WithPrefix prepends a key prefix to every object name.
func WithPrefix(p string) Option { return func(s *Sink) { s.prefix = p } }

// WithContentType sets the object Content-Type (default application/vnd.apache.parquet).
func WithContentType(ct string) Option { return func(s *Sink) { s.contentType = ct } }

// WithSpillDir sets where temp files are written (default os.TempDir).
func WithSpillDir(dir string) Option { return func(s *Sink) { s.spillDir = dir } }

// WithRateLimit caps total upload bandwidth across all objects to
// bytesPerSec (burst in bytes); <=0 disables limiting.
func WithRateLimit(bytesPerSec float64, burst int) Option {
	return func(s *Sink) {
		if bytesPerSec > 0 {
			s.bucketLimit = limit.NewBucket(bytesPerSec, burst)
		}
	}
}

// New returns a sink. p is typically an *awss3.Client (point its endpoint at
// R2 for Cloudflare).
func New(p ObjectPutter, bucket string, opts ...Option) (*Sink, error) {
	s := &Sink{p: p, bucket: bucket, contentType: "application/vnd.apache.parquet"}
	for _, o := range opts {
		o(s)
	}
	return s, nil
}

// Stats exposes upload bytes/rate when a rate limit is configured.
func (s *Sink) Stats() (total int64, rate float64) {
	if s.bucketLimit == nil {
		return 0, 0
	}
	return s.bucketLimit.Stats()
}

// Create returns a WriteCloser that buffers to a spill file; the single PUT
// happens on Close.
func (s *Sink) Create(ctx context.Context, name string) (io.WriteCloser, error) {
	f, err := os.CreateTemp(s.spillDir, "roost-*.parquet")
	if err != nil {
		return nil, err
	}
	return &spillWC{ctx: ctx, s: s, f: f, key: path.Join(s.prefix, name)}, nil
}

type spillWC struct {
	ctx context.Context
	s   *Sink
	f   *os.File
	key string
	n   int64
}

func (w *spillWC) Write(p []byte) (int, error) {
	n, err := w.f.Write(p)
	w.n += int64(n)
	return n, err
}

func (w *spillWC) Close() error {
	tmp := w.f.Name()
	defer os.Remove(tmp)
	if _, err := w.f.Seek(0, io.SeekStart); err != nil {
		w.f.Close()
		return err
	}
	var body io.Reader = w.f
	if w.s.bucketLimit != nil {
		body = w.s.bucketLimit.ReadSeeker(w.f) // seekable for SDK retries
	}
	_, err := w.s.p.PutObject(w.ctx, &awss3.PutObjectInput{
		Bucket:        aws.String(w.s.bucket),
		Key:           aws.String(w.key),
		Body:          body,
		ContentLength: aws.Int64(w.n),
		ContentType:   aws.String(w.s.contentType),
	})
	if cerr := w.f.Close(); cerr != nil && err == nil {
		err = cerr
	}
	return err
}
