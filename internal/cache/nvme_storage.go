package cache

import (
	"context"
	"io/ioutil"
	"os"
	"path/filepath"
)

// NVMeStorage implements TierStorage for NVME storage
type NVMeStorage struct {
	basePath string
}

// NewNVMeStorage creates a new NVME storage instance
func NewNVMeStorage(basePath string) (*NVMeStorage, error) {
	// Ensure the base path exists
	if err := os.MkdirAll(basePath, 0755); err != nil {
		return nil, err
	}

	return &NVMeStorage{
		basePath: basePath,
	}, nil
}

// Read reads a file from NVME storage
func (ns *NVMeStorage) Read(ctx context.Context, path string) ([]byte, error) {
	fullPath := filepath.Join(ns.basePath, path)
	return ioutil.ReadFile(fullPath)
}

// Write writes a file to NVME storage
func (ns *NVMeStorage) Write(ctx context.Context, path string, data []byte) error {
	fullPath := filepath.Join(ns.basePath, path)

	// Create directory if it doesn't exist
	dir := filepath.Dir(fullPath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}

	return ioutil.WriteFile(fullPath, data, 0644)
}

// Delete removes a file from NVME storage
func (ns *NVMeStorage) Delete(ctx context.Context, path string) error {
	fullPath := filepath.Join(ns.basePath, path)
	return os.Remove(fullPath)
}

// Exists checks if a file exists in NVME storage
func (ns *NVMeStorage) Exists(ctx context.Context, path string) bool {
	fullPath := filepath.Join(ns.basePath, path)
	_, err := os.Stat(fullPath)
	return err == nil
}

// Size returns the size of a file in NVME storage
func (ns *NVMeStorage) Size(ctx context.Context, path string) (int64, error) {
	fullPath := filepath.Join(ns.basePath, path)
	info, err := os.Stat(fullPath)
	if err != nil {
		return 0, err
	}
	return info.Size(), nil
}
