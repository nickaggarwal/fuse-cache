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
