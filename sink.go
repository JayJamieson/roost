package roost

import (
	"context"
	"io"
)

// Sink decides where encoded objects go. Create returns a writer for one
// object at a forward-slash relative path (e.g. "region=x/dt=y/part-..parquet").
// The returned Close() is the commit point: a sink may fsync (local) or issue
// a single upload (s3/r2) there. Create may be called concurrently.
type Sink interface {
	Create(ctx context.Context, name string) (io.WriteCloser, error)
}
