package cache

import (
	"context"
	"os"
	"path/filepath"
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
	fullPath := filepath.Join(ns.basePath, path)
	return os.ReadFile(fullPath)
}

func (ns *NVMeStorage) Write(ctx context.Context, path string, data []byte) error {
	fullPath := filepath.Join(ns.basePath, path)

	dir := filepath.Dir(fullPath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}

	return os.WriteFile(fullPath, data, 0644)
}

func (ns *NVMeStorage) Delete(ctx context.Context, path string) error {
	fullPath := filepath.Join(ns.basePath, path)
	return os.Remove(fullPath)
}

func (ns *NVMeStorage) Exists(ctx context.Context, path string) bool {
	fullPath := filepath.Join(ns.basePath, path)
	_, err := os.Stat(fullPath)
	return err == nil
}

func (ns *NVMeStorage) Size(ctx context.Context, path string) (int64, error) {
	fullPath := filepath.Join(ns.basePath, path)
	info, err := os.Stat(fullPath)
	if err != nil {
		return 0, err
	}
	return info.Size(), nil
}
