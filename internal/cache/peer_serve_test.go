package cache

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

// TestGetLocal_ServesChunkFromWholeFile covers the peer-serving path for
// FUSE-written files, which are stored whole on NVMe (only the parent has an
// entry; no per-chunk files exist). A cross-node peer requests
// "<parent>_chunk_N"; GetLocal must synthesize that chunk from the whole
// parent file instead of missing and forcing a cloud fallback.
func TestGetLocal_ServesChunkFromWholeFile(t *testing.T) {
	cm := newTestCacheManager()
	dir := t.TempDir()
	cm.config.NVMePath = dir
	cm.config.ChunkSize = 4

	parent := "/whole.bin"
	content := []byte("ABCDEFGHIJ") // 10 bytes, chunk size 4 => ABCD | EFGH | IJ
	if err := os.WriteFile(filepath.Join(dir, "whole.bin"), content, 0o644); err != nil {
		t.Fatalf("write whole file: %v", err)
	}
	cm.entries[parent] = &CacheEntry{
		FilePath:  parent,
		Size:      int64(len(content)),
		IsChunked: true,
		NumChunks: 3,
	}

	cases := []struct {
		chunk int
		want  string
	}{
		{0, "ABCD"},
		{1, "EFGH"},
		{2, "IJ"},
	}
	for _, tc := range cases {
		chunkPath := parent + "_chunk_" + itoa(tc.chunk)
		got, err := cm.GetLocal(context.Background(), chunkPath)
		if err != nil {
			t.Fatalf("GetLocal(%s): %v", chunkPath, err)
		}
		if string(got.Data) != tc.want {
			t.Fatalf("GetLocal(%s) = %q, want %q", chunkPath, string(got.Data), tc.want)
		}
	}

	// A node that does not hold the parent must still cleanly miss (so the peer
	// read fails over to another peer / cloud rather than returning garbage).
	if _, err := cm.GetLocal(context.Background(), "/absent.bin_chunk_0"); err == nil {
		t.Fatal("GetLocal for a chunk of an absent parent should error")
	}
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var b []byte
	neg := i < 0
	if neg {
		i = -i
	}
	for i > 0 {
		b = append([]byte{byte('0' + i%10)}, b...)
		i /= 10
	}
	if neg {
		b = append([]byte{'-'}, b...)
	}
	return string(b)
}
