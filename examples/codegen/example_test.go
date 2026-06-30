package codegen_test

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/jayjamieson/roost"
	"github.com/jayjamieson/roost/examples/codegen"
	"github.com/jayjamieson/roost/sink/local"
)

// Example shows the two constructors side by side: NewWriter uses reflection
// (zero setup), NewWriterFor uses the roostgen-emitted appender (zero
// reflection). Everything else — options, sinks, encoders, partitioning — is
// identical, so switching is just changing the constructor.
func Example() {
	ctx := context.Background()
	dir, _ := os.MkdirTemp("", "roost-codegen")
	defer os.RemoveAll(dir)
	sink, _ := local.New(dir)

	// Reflection: zero setup, works on any struct immediately.
	wr, _ := roost.NewWriter[codegen.Metric](ctx, sink, roost.WithCodec("zstd"))
	_ = wr.Append(codegen.Metric{TS: time.Unix(0, 0), Host: "h1", CPU: 0.4, Region: "us-east-1"})
	_ = wr.Close()

	// Generated: zero reflection, identical surface.
	wg, _ := roost.NewWriterFor[codegen.Metric](ctx, sink, codegen.MetricRoostAppender{}, roost.WithCodec("zstd"))
	_ = wg.Append(codegen.Metric{TS: time.Unix(0, 0), Host: "h1", CPU: 0.4, Region: "us-east-1"})
	_ = wg.Close()

	fmt.Println("ok")
	// Output: ok
}
