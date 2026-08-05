package cache

import (
	"bytes"
	"context"
	"fmt"
	"testing"
	"time"

	"fuse-client/internal/coordinator"
)

// A file is split with the stride in effect when it was written. A reader
// configured with a different -chunk-size must still address it with the
// original stride: computing chunk offsets from the wrong one silently
// mis-slices every boundary, and — when the wrong offset lands past the end of
// a chunk — panics inside the FUSE read goroutine, unmounting the node.

const strideTestChunk = 4 // bytes; stands in for the 4 MiB production stride

func strideTestChunks() [][]byte {
	return [][]byte{
		[]byte("aaaa"), []byte("bbbb"), []byte("cccc"),
		[]byte("dddd"), []byte("eeee"), []byte("ffff"),
	}
}

// newStrideTestManager writes a file chunked at strideTestChunk while the
// manager itself is configured for a larger stride.
func newStrideTestManager(t *testing.T, path string) (*DefaultCacheManager, []byte) {
	t.Helper()
	cm := newTestCacheManager()
	ctx := context.Background()
	cm.config.ChunkSize = 2 * strideTestChunk // deliberately wrong for this file
	cm.config.RangePrefetchChunks = 0
	cm.config.PeerTimeout = 2 * time.Second

	var whole []byte
	for i, data := range strideTestChunks() {
		chunkPath := fmt.Sprintf("%s_chunk_%d", path, i)
		if err := cm.nvmeStorage.Write(ctx, chunkPath, data); err != nil {
			t.Fatalf("nvme write %s: %v", chunkPath, err)
		}
		whole = append(whole, data...)
	}
	return cm, whole
}

func TestReadRange_UsesPublishedChunkStride(t *testing.T) {
	ctx := context.Background()
	path := "/stride-published.bin"
	cm, whole := newStrideTestManager(t, path)

	coord := coordinator.NewCoordinatorService()
	cm.config.Coordinator = coord
	if err := coord.UpdateFileLocation(ctx, &coordinator.FileLocation{
		FilePath:    path,
		PeerID:      "peer-1",
		StorageTier: "nvme",
		StoragePath: path,
		FileSize:    int64(len(whole)),
		IsChunked:   true,
		ChunkSize:   strideTestChunk,
	}); err != nil {
		t.Fatalf("UpdateFileLocation: %v", err)
	}

	entry, ok := cm.resolveChunkedEntry(ctx, path)
	if !ok {
		t.Fatalf("resolveChunkedEntry: not resolved")
	}
	if entry.ChunkSize != strideTestChunk {
		t.Fatalf("resolved ChunkSize = %d, want %d", entry.ChunkSize, strideTestChunk)
	}
	if want := int64(len(strideTestChunks())); entry.NumChunks != want {
		t.Fatalf("resolved NumChunks = %d, want %d", entry.NumChunks, want)
	}

	// Whole file, and a range starting mid-chunk at an offset that the
	// configured (wrong) stride would place inside chunk 0.
	for _, tc := range []struct{ offset, size int }{
		{0, len(whole)},
		{5, 2},
		{strideTestChunk, strideTestChunk},
		{len(whole) - 3, 3},
	} {
		got, err := cm.ReadRange(ctx, path, int64(tc.offset), tc.size)
		if err != nil {
			t.Fatalf("ReadRange(%d,%d): %v", tc.offset, tc.size, err)
		}
		want := whole[tc.offset : tc.offset+tc.size]
		if !bytes.Equal(got, want) {
			t.Fatalf("ReadRange(%d,%d) = %q, want %q", tc.offset, tc.size, got, want)
		}
	}
}

// Legacy metadata carries no stride at all. The read is allowed to come back
// short — the stride is genuinely unknown — but it must never panic, because
// the panic happens in the FUSE read goroutine and takes down the mount.
func TestReadRange_LegacyStrideMismatchDoesNotPanic(t *testing.T) {
	ctx := context.Background()
	path := "/stride-legacy.bin"
	cm, whole := newStrideTestManager(t, path)

	cm.mu.Lock()
	cm.entries[path] = &CacheEntry{
		FilePath:  path,
		Size:      int64(len(whole)),
		IsChunked: true,
		NumChunks: int64(len(strideTestChunks())),
		// ChunkSize deliberately unset: metadata written before the field existed.
	}
	cm.mu.Unlock()

	for offset := 0; offset <= len(whole); offset++ {
		for size := 1; size <= len(whole); size++ {
			got, err := cm.ReadRange(ctx, path, int64(offset), size)
			if err != nil {
				t.Fatalf("ReadRange(%d,%d): %v", offset, size, err)
			}
			if len(got) > size {
				t.Fatalf("ReadRange(%d,%d) returned %d bytes, want <= %d", offset, size, len(got), size)
			}
		}
	}
}

// WriteTo assembles chunk-by-chunk; a wrong chunk count produces a short body
// with no error, which an HTTP caller sees as a successful truncated download.
func TestWriteTo_ShortAssemblyIsAnError(t *testing.T) {
	ctx := context.Background()
	path := "/stride-short.bin"
	cm, whole := newStrideTestManager(t, path)

	cm.mu.Lock()
	cm.entries[path] = &CacheEntry{
		FilePath:  path,
		Size:      int64(len(whole)),
		IsChunked: true,
		NumChunks: int64(len(strideTestChunks())) / 2, // e.g. count derived from the wrong stride
		ChunkSize: strideTestChunk,
	}
	cm.mu.Unlock()

	var buf bytes.Buffer
	n, err := cm.WriteTo(ctx, path, &buf)
	if err == nil {
		t.Fatalf("WriteTo returned %d bytes and no error, want a short-read error", n)
	}
	if n >= int64(len(whole)) {
		t.Fatalf("WriteTo wrote %d bytes, expected a short assembly (< %d)", n, len(whole))
	}
}

func TestEffectiveChunkSizeAndChunkCount(t *testing.T) {
	cm := newTestCacheManager()
	cm.config.ChunkSize = 8

	if got := cm.effectiveChunkSize(nil); got != 8 {
		t.Fatalf("effectiveChunkSize(nil) = %d, want 8", got)
	}
	if got := cm.effectiveChunkSize(&CacheEntry{}); got != 8 {
		t.Fatalf("effectiveChunkSize(zero) = %d, want 8", got)
	}
	if got := cm.effectiveChunkSize(&CacheEntry{ChunkSize: 4}); got != 4 {
		t.Fatalf("effectiveChunkSize(4) = %d, want 4", got)
	}

	for _, tc := range []struct{ size, stride, want int64 }{
		{0, 4, 0}, {1, 4, 1}, {4, 4, 1}, {5, 4, 2}, {24, 4, 6}, {24, 0, 0}, {-1, 4, 0},
	} {
		if got := chunkCountFor(tc.size, tc.stride); got != tc.want {
			t.Fatalf("chunkCountFor(%d,%d) = %d, want %d", tc.size, tc.stride, got, tc.want)
		}
	}

	for _, tc := range []struct{ from, to, n, wf, wt int64 }{
		{0, 4, 4, 0, 4},
		{5, 2, 4, 4, 4},  // from past end
		{2, 9, 4, 2, 4},  // to past end
		{-1, 3, 4, 0, 3}, // negative from
		{3, 1, 4, 3, 3},  // inverted
	} {
		f, to := clampChunkSlice(tc.from, tc.to, tc.n)
		if f != tc.wf || to != tc.wt {
			t.Fatalf("clampChunkSlice(%d,%d,%d) = (%d,%d), want (%d,%d)", tc.from, tc.to, tc.n, f, to, tc.wf, tc.wt)
		}
	}
}
