package cache

import (
	"context"
	"testing"
)

// TestReadRangeStrideSmallerThanChunks characterizes ReadRange when the
// published stride is SMALLER than the chunk files actually on disk (a file
// written at -chunk-size 16 and described by metadata saying 8).
//
// clampChunkSlice keeps each span inside its chunk, so nothing panics, but
// each span clamps against the real (longer) chunk and the pieces are
// appended: a window that crosses a stride boundary comes back LONGER than
// requested, with bytes from the wrong offsets. The go-fuse backend truncates
// to len(dest), which is why this has not crashed anything; the data it
// returns is still wrong. Found while porting the read path to Rust, where the
// same arithmetic writing into a caller-sized buffer overran it.
//
// This pins today's behaviour. If ReadRange is changed to bound the result to
// the requested size (or to reject a stride/disk mismatch), update the test.
func TestReadRangeStrideSmallerThanChunks(t *testing.T) {
	cm := newTestCacheManager()
	cm.config.ChunkSize = 4
	ctx := context.Background()
	// On disk: two 8-byte chunks (written at stride 8).
	cm.nvmeStorage.Write(ctx, "/f_chunk_0", []byte("AAAAAAAA"))
	cm.nvmeStorage.Write(ctx, "/f_chunk_1", []byte("BBBBBBBB"))
	// Metadata claims stride 4, size 16.
	cm.entries.put(&CacheEntry{FilePath: "/f", Size: 16, IsChunked: true, NumChunks: 4, ChunkSize: 4, Tier: TierNVMe})

	// [2,6) at stride 4 spans chunk 0 [2,4) and chunk 1 [0,2). Chunk 0 on
	// disk is 8 bytes, so its span clamps to [2,8) instead of [2,4).
	got, err := cm.ReadRange(ctx, "/f", 2, 4)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "AAAAAABB" && string(got) != "AAAABB" {
		t.Fatalf("ReadRange(2,4) = %q; characterization changed, re-check the stride-mismatch handling", got)
	}
	if len(got) <= 4 {
		t.Fatalf("ReadRange now bounds its result (%d bytes); update this test", len(got))
	}
}
