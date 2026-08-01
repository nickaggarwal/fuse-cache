package cache

import (
	"context"
	"os"
	"path/filepath"
	"strings"
)

// NVMeStorage implements TierStorage for NVME storage
type NVMeStorage struct {
	basePath string
}

// NewNVMeStorage creates a new NVME storage instance
func NewNVMeStorage(basePath string) (*NVMeStorage, error) {
	if err := os.MkdirAll(basePath, 0755); err != nil {
		return nil, err
	}

	return &NVMeStorage{
		basePath: basePath,
	}, nil
}

func (ns *NVMeStorage) Read(ctx context.Context, path string) ([]byte, error) {
	fullPath := filepath.Join(ns.basePath, strings.TrimPrefix(path, "/"))
	return os.ReadFile(fullPath)
}

func (ns *NVMeStorage) Write(ctx context.Context, path string, data []byte) error {
	fullPath := filepath.Join(ns.basePath, strings.TrimPrefix(path, "/"))

	dir := filepath.Dir(fullPath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}

	// Write to a temp file in the same directory, then rename atomically so a
	// crash mid-write never leaves a partial file at the final path.
	tmp, err := os.CreateTemp(dir, ".nvme-write-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		os.Remove(tmpName)
		return err
	}
	if err := tmp.Close(); err != nil {
		os.Remove(tmpName)
		return err
	}
	if err := os.Rename(tmpName, fullPath); err != nil {
		os.Remove(tmpName)
		return err
	}
	return nil
}

// BeginStream starts a streaming write of path. Bytes are staged in a temp
// file in the destination directory; Commit renames it into place atomically
// (same crash guarantee as Write), Abort discards it. This lets a network
// receive path write bytes to disk as they arrive instead of materializing
// the whole object in memory first.
func (ns *NVMeStorage) BeginStream(path string) (*NVMeStreamWriter, error) {
	fullPath := filepath.Join(ns.basePath, strings.TrimPrefix(path, "/"))
	dir := filepath.Dir(fullPath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}
	tmp, err := os.CreateTemp(dir, ".nvme-stream-*")
	if err != nil {
		return nil, err
	}
	return &NVMeStreamWriter{f: tmp, tmpName: tmp.Name(), finalPath: fullPath}, nil
}

// NVMeStreamWriter stages a streaming write; exactly one of Commit or Abort
// must be called.
type NVMeStreamWriter struct {
	f         *os.File
	tmpName   string
	finalPath string
	n         int64
}

func (w *NVMeStreamWriter) Write(p []byte) (int, error) {
	n, err := w.f.Write(p)
	w.n += int64(n)
	return n, err
}

// BytesWritten returns the total bytes written so far.
func (w *NVMeStreamWriter) BytesWritten() int64 { return w.n }

// Commit closes the temp file and renames it to the final path.
func (w *NVMeStreamWriter) Commit() error {
	if err := w.f.Close(); err != nil {
		os.Remove(w.tmpName)
		return err
	}
	if err := os.Rename(w.tmpName, w.finalPath); err != nil {
		os.Remove(w.tmpName)
		return err
	}
	return nil
}

// Abort discards the staged bytes.
func (w *NVMeStreamWriter) Abort() {
	w.f.Close()
	os.Remove(w.tmpName)
}

func (ns *NVMeStorage) Delete(ctx context.Context, path string) error {
	fullPath := filepath.Join(ns.basePath, strings.TrimPrefix(path, "/"))
	return os.Remove(fullPath)
}

func (ns *NVMeStorage) Exists(ctx context.Context, path string) bool {
	fullPath := filepath.Join(ns.basePath, strings.TrimPrefix(path, "/"))
	_, err := os.Stat(fullPath)
	return err == nil
}

func (ns *NVMeStorage) Size(ctx context.Context, path string) (int64, error) {
	fullPath := filepath.Join(ns.basePath, strings.TrimPrefix(path, "/"))
	info, err := os.Stat(fullPath)
	if err != nil {
		return 0, err
	}
	return info.Size(), nil
}
