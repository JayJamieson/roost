package s3

import (
	"context"
	"io"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
)

type capture struct {
	calls  int
	key    string
	bucket string
	body   []byte
	length int64
}

func (c *capture) PutObject(_ context.Context, in *awss3.PutObjectInput, _ ...func(*awss3.Options)) (*awss3.PutObjectOutput, error) {
	c.calls++
	c.bucket = aws.ToString(in.Bucket)
	c.key = aws.ToString(in.Key)
	c.length = aws.ToInt64(in.ContentLength)
	b, _ := io.ReadAll(in.Body)
	c.body = b
	return &awss3.PutObjectOutput{}, nil
}

func TestS3SinglePutOnClose(t *testing.T) {
	cap := &capture{}
	s, err := New(cap, "mybucket", WithPrefix("data"))
	if err != nil {
		t.Fatal(err)
	}
	wc, err := s.Create(context.Background(), "region=x/part-0.parquet")
	if err != nil {
		t.Fatal(err)
	}
	payload := []byte("PAR1...parquet bytes...PAR1")
	if _, err := wc.Write(payload); err != nil {
		t.Fatal(err)
	}
	if cap.calls != 0 {
		t.Fatal("must not upload before Close")
	}
	if err := wc.Close(); err != nil {
		t.Fatal(err)
	}
	if cap.calls != 1 {
		t.Fatalf("calls = %d, want 1 (no multipart)", cap.calls)
	}
	if cap.bucket != "mybucket" || cap.key != "data/region=x/part-0.parquet" {
		t.Fatalf("bucket/key = %s/%s", cap.bucket, cap.key)
	}
	if cap.length != int64(len(payload)) || string(cap.body) != string(payload) {
		t.Fatalf("len=%d body=%q", cap.length, cap.body)
	}
}

func TestS3RateLimitWired(t *testing.T) {
	cap := &capture{}
	s, _ := New(cap, "b", WithRateLimit(1<<20, 1<<20))
	wc, _ := s.Create(context.Background(), "o.parquet")
	_, _ = wc.Write([]byte("abcdef"))
	if err := wc.Close(); err != nil {
		t.Fatal(err)
	}
	if total, rate := s.Stats(); total != 6 || rate != float64(1<<20) {
		t.Fatalf("stats total=%d rate=%v", total, rate)
	}
}
