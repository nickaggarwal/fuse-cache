package cache

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"
)

// A chunked file that is whole on local NVMe (after a manifest warm, chunk
// assembly or a FUSE write) must be served by reading just the requested
// window from that file. Before this fix every FUSE window (~128 KiB-1 MiB)
// went through readChunkData -> readChunkFromWholeLocalFile, which opens the
// parent and copies a full 16 MiB chunk into the range cache, then slices
// it: ~16-128x the bytes per window, plus range-cache churn. Measured on an
// A100 node: 15 GB of fully local weights loaded through FUSE in 46 s
// (0.31 GB/s) against 2.2 s from a plain hostPath.

// Windows read from a whole local chunked file return exactly the file's
// bytes, across chunk boundaries and at the tail.
func TestReadRange_WholeLocalChunkedFile(t *testing.T) {
	const chunkSize = 1 << 20
	cm, parent, numChunks := benchCacheManagerWithWholeFile(t, chunkSize, 4)
	want, err := os.ReadFile(filepath.Join(cm.config.NVMePath, "bench-whole.bin"))
	if err != nil {
		t.Fatal(err)
	}
	total := chunkSize * numChunks
	for _, c := range []struct{ off, size int64 }{
		{0, 128 << 10},                   // first window
		{chunkSize - 4096, 8192},         // straddles chunk 0/1
		{3*chunkSize + 100, 1 << 20},     // runs past EOF: clamp
		{total - 1, 10},                  // last byte
		{total, 10},                      // at EOF: empty
		{chunkSize, 2*chunkSize + 12345}, // multi-chunk window
	} {
		got, err := cm.ReadRange(context.Background(), parent, c.off, int(c.size))
		if err != nil {
			t.Fatalf("ReadRange(%d,%d): %v", c.off, c.size, err)
		}
		end := min(c.off+c.size, total)
		if c.off >= total {
			end = c.off
		}
		if !bytes.Equal(got, want[min(c.off, total):end]) {
			t.Errorf("ReadRange(%d,%d): %d bytes, mismatch", c.off, c.size, len(got))
		}
	}
	// The window path must not populate the 16 MiB-per-chunk range cache.
	if n, _ := cm.rangeCacheStats(parent); n != 0 {
		t.Errorf("range cache holds %d chunks after whole-file reads; want 0", n)
	}
}

// A file whose local copy is shorter than its recorded size (a torn or
// still-assembling write) is not served from disk: the chunk path decides.
func TestReadRange_WholeLocalShortFileNotServed(t *testing.T) {
	const chunkSize = 1 << 20
	cm, parent, _ := benchCacheManagerWithWholeFile(t, chunkSize, 2)
	p := filepath.Join(cm.config.NVMePath, "bench-whole.bin")
	if err := os.Truncate(p, chunkSize); err != nil { // half the recorded size
		t.Fatal(err)
	}
	if _, ok := cm.readWholeChunkedRange(context.Background(), parent, 2*chunkSize, 0, 4096); ok {
		t.Fatal("served a window from a short local file")
	}
}

// The switch takes effect. Disabled, each window takes the per-chunk path and
// synthesizes a full chunk from the whole file (a 200-byte window spanning a
// boundary reads two 1 MiB chunks); enabled, it reads exactly the window and
// the counter records it. Both return the same bytes.
func TestReadRange_WholeFileSwitch(t *testing.T) {
	const chunkSize = 1 << 20
	for _, disabled := range []bool{true, false} {
		cm, parent, _ := benchCacheManagerWithWholeFile(t, chunkSize, 2)
		cm.config.DisableWholeFileRangeReads = disabled
		want, _ := os.ReadFile(filepath.Join(cm.config.NVMePath, "bench-whole.bin"))
		got, err := cm.ReadRange(context.Background(), parent, chunkSize-100, 200)
		if err != nil || !bytes.Equal(got, want[chunkSize-100:chunkSize+100]) {
			t.Fatalf("disabled=%v: %d bytes, err %v", disabled, len(got), err)
		}
		n := cm.metrics.WholeFileRangeReads.Load()
		switch {
		case disabled && n != 0:
			t.Errorf("disabled: %d whole-file reads, want 0", n)
		case !disabled && n != 1:
			t.Errorf("enabled: %d whole-file reads, want 1", n)
		case !disabled && cm.metrics.WholeFileRangeReadBytes.Load() != 200:
			t.Errorf("enabled: read %d bytes for a 200-byte window", cm.metrics.WholeFileRangeReadBytes.Load())
		}
	}
}

// BenchmarkReadRange_WholeLocalChunked reads a 64 MiB whole local chunked
// file end to end in 1 MiB FUSE windows (vLLM's safetensors loader pattern).
func BenchmarkReadRange_WholeLocalChunked(b *testing.B) {
	const chunkSize = 16 << 20
	const window = 1 << 20
	cm, parent, numChunks := benchCacheManagerWithWholeFile(b, chunkSize, 4)
	total := chunkSize * numChunks
	ctx := context.Background()
	b.SetBytes(total)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for off := int64(0); off < total; off += window {
			if _, err := cm.ReadRange(ctx, parent, off, window); err != nil {
				b.Fatal(err)
			}
		}
	}
}
