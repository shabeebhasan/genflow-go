// Package storage keeps input faces and generated images. Local disk here; the interface is
// small so S3 can replace it without touching the workflow.
package storage

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

type Store interface {
	Put(ctx context.Context, key string, data []byte) error
	Get(ctx context.Context, key string) ([]byte, error)
}

type Local struct{ Dir string }

func (l Local) path(key string) (string, error) {
	clean := filepath.Clean("/" + key)
	if strings.Contains(key, "..") {
		return "", fmt.Errorf("bad key %q", key)
	}
	return filepath.Join(l.Dir, clean), nil
}

// Put writes atomically (temp file, then rename), so a crash never leaves half an image.
func (l Local) Put(_ context.Context, key string, data []byte) error {
	p, err := l.path(key)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		return err
	}
	tmp := p + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, p)
}

func (l Local) Get(_ context.Context, key string) ([]byte, error) {
	p, err := l.path(key)
	if err != nil {
		return nil, err
	}
	return os.ReadFile(p)
}
