// Command r2-example writes Parquet to Cloudflare R2 (or any S3-compatible
// store): one PutObject per object, no multipart, with a bandwidth cap.
package main

import (
	"context"
	"log"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/jayjamieson/roost"
	s3sink "github.com/jayjamieson/roost/sink/s3"
)

type Event struct {
	RSN     int64     `roost:"name=rsn"`
	Time    time.Time `roost:"name=event_time"`
	Region  string    `roost:"name=region,partition"`
	Payload []byte
}

func main() {
	ctx := context.Background()
	const accountID = "<your-account-id>"

	cfg, err := awsconfig.LoadDefaultConfig(ctx,
		awsconfig.WithRegion("auto"),
		awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
			"<access-key-id>", "<secret-access-key>", "")),
	)

	if err != nil {
		log.Fatal(err)
	}

	client := awss3.NewFromConfig(cfg, func(o *awss3.Options) {
		o.BaseEndpoint = aws.String("https://" + accountID + ".r2.cloudflarestorage.com")
		o.UsePathStyle = true // safest for R2
	})

	sink, err := s3sink.New(client, "my-bucket",
		s3sink.WithPrefix("wal"),
		s3sink.WithRateLimit(50<<20, 8<<20), // cap uploads at 50 MB/s, 8 MB burst
	)

	if err != nil {
		log.Fatal(err)
	}

	w, err := roost.NewWriter[Event](ctx, sink,
		roost.WithCodec("zstd"),
		roost.WithRollBytes(128<<20),   // ~128 MB objects (approx; uses row estimate)
		roost.WithEncodeConcurrency(4), // overlap encode + upload
	)

	if err != nil {
		log.Fatal(err)
	}

	for i := 0; i < 1_000_000; i++ {
		if err := w.Append(Event{
			RSN: int64(i), Time: time.Now(), Region: "us-east-1", Payload: []byte("…"),
		}); err != nil {
			log.Fatal(err)
		}
	}

	if err := w.Close(); err != nil {
		log.Fatal(err)
	}

	total, rate := sink.Stats()
	log.Printf("uploaded %d bytes (cap %.0f MB/s)", total, rate/(1<<20))
}
