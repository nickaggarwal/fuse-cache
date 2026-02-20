package cache

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

func TestNVMeStorage_WriteAndRead(t *testing.T) {
	dir := t.TempDir()
	ns, err := NewNVMeStorage(dir)
	if err != nil {
		t.Fatalf("NewNVMeStorage: %v", err)
	}

	ctx := context.Background()
	data := []byte("hello world")

	if err := ns.Write(ctx, "/test.txt", data); err != nil {
		t.Fatalf("Write: %v", err)
	}

	got, err := ns.Read(ctx, "/test.txt")
	if err != nil {
		t.Fatalf("Read: %v", err)
	}

	if string(got) != string(data) {
		t.Errorf("Read got %q, want %q", got, data)
	}
}

func TestNVMeStorage_Exists(t *testing.T) {
	dir := t.TempDir()
	ns, _ := NewNVMeStorage(dir)
	ctx := context.Background()

	if ns.Exists(ctx, "/nonexistent.txt") {
		t.Error("Exists returned true for nonexistent file")
	}

	ns.Write(ctx, "/exists.txt", []byte("data"))
	if !ns.Exists(ctx, "/exists.txt") {
		t.Error("Exists returned false for existing file")
	}
}

func TestNVMeStorage_Size(t *testing.T) {
	dir := t.TempDir()
	ns, _ := NewNVMeStorage(dir)
	ctx := context.Background()

	data := []byte("twelve chars")
	ns.Write(ctx, "/size.txt", data)

	size, err := ns.Size(ctx, "/size.txt")
	if err != nil {
		t.Fatalf("Size: %v", err)
	}
	if size != int64(len(data)) {
		t.Errorf("Size = %d, want %d", size, len(data))
	}
}

func TestNVMeStorage_Delete(t *testing.T) {
	dir := t.TempDir()
	ns, _ := NewNVMeStorage(dir)
	ctx := context.Background()

	ns.Write(ctx, "/delete.txt", []byte("data"))
	if err := ns.Delete(ctx, "/delete.txt"); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	if ns.Exists(ctx, "/delete.txt") {
		t.Error("File still exists after delete")
	}
}

func TestNVMeStorage_WriteCreatesSubdirectories(t *testing.T) {
	dir := t.TempDir()
	ns, _ := NewNVMeStorage(dir)
	ctx := context.Background()

	if err := ns.Write(ctx, "/a/b/c/deep.txt", []byte("nested")); err != nil {
		t.Fatalf("Write nested: %v", err)
	}

	fullPath := filepath.Join(dir, "/a/b/c/deep.txt")
	if _, err := os.Stat(fullPath); err != nil {
		t.Errorf("Nested file not found on disk: %v", err)
	}
}
