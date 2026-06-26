// Package local is a roost.Sink that writes objects to the local filesystem.
package local

import (
	"context"
	"io"
	"os"
	"path/filepath"
)

// Sink writes each object under a root directory, creating parent dirs.
type Sink struct {
	dir     string
	fsync   bool
	dirPerm os.FileMode
}

// Option configures a local sink.
type Option func(*Sink)

// WithFsync fsyncs each file before close (durable vs page-cache).
func WithFsync(on bool) Option { return func(s *Sink) { s.fsync = on } }

// WithDirPerm sets the permission used for created directories.
func WithDirPerm(p os.FileMode) Option { return func(s *Sink) { s.dirPerm = p } }

// New creates the root directory and returns a sink.
func New(dir string, opts ...Option) (*Sink, error) {
	s := &Sink{dir: dir, dirPerm: 0o755}
	for _, o := range opts {
		o(s)
	}
	if err := os.MkdirAll(dir, s.dirPerm); err != nil {
		return nil, err
	}
	return s, nil
}

// Create opens (and truncates) the object file, creating parent dirs.
func (s *Sink) Create(_ context.Context, name string) (io.WriteCloser, error) {
	full := filepath.Join(s.dir, filepath.FromSlash(name))
	if err := os.MkdirAll(filepath.Dir(full), s.dirPerm); err != nil {
		return nil, err
	}
	f, err := os.Create(full)
	if err != nil {
		return nil, err
	}
	return &fileWC{f: f, fsync: s.fsync}, nil
}

type fileWC struct {
	f     *os.File
	fsync bool
}

func (w *fileWC) Write(p []byte) (int, error) { return w.f.Write(p) }

func (w *fileWC) Close() error {
	if w.fsync {
		if err := w.f.Sync(); err != nil {
			w.f.Close()
			return err
		}
	}
	return w.f.Close()
}
