package cache

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"testing"
)

// chunkWindowManager stores a chunked file as _chunk_N files only (no whole
// copy), the layout a peer or cloud fill leaves behind.
func chunkWindowManager(tb testing.TB, chunkSize, numChunks int64) (*DefaultCacheManager, string, []byte) {
	tb.Helper()
	dir := tb.TempDir()
	nvme, err := NewNVMeStorage(dir)
	if err != nil {
		tb.Fatal(err)
	}
	parent := "/chunked.bin"
	want := make([]byte, chunkSize*numChunks)
	for i := range want {
		want[i] = byte(i * 7 % 251)
	}
	for c := int64(0); c < numChunks; c++ {
		name := filepath.Join(dir, fmt.Sprintf("chunked.bin_chunk_%d", c))
		if err := os.WriteFile(name, want[c*chunkSize:(c+1)*chunkSize], 0o644); err != nil {
			tb.Fatal(err)
		}
	}
	cm := &DefaultCacheManager{
		config:       &CacheConfig{NVMePath: dir, ChunkSize: chunkSize, RangePrefetchChunks: 2, RangePrefetchMaxChunks: 4},
		nvmeStorage:  nvme,
		peerStorage:  newMockStorage(),
		cloudStorage: newMockStorage(),
		entries:      newEntryTable(),
		logger:       log.New(io.Discard, "", 0),
		metrics:      NewCacheMetrics(),
		rangeChunks:  make(map[string]*chunkFileCache),
		hybridHints:  make(map[string]hybridReadHint),
		tierPerf:     newTierPerfTracker(),
	}
	cm.entries.put(&CacheEntry{FilePath: parent, Size: int64(len(want)), IsChunked: true, NumChunks: numChunks, ChunkSize: chunkSize, Tier: TierNVMe})
	return cm, parent, want
}

// A random window inside one chunk is read from that chunk's file without
// loading the chunk: exact bytes, counted, and the range cache stays empty.
func TestReadRange_ChunkWindowRandom(t *testing.T) {
	const chunkSize = 1 << 20
	cm, parent, want := chunkWindowManager(t, chunkSize, 4)
	for _, c := range []struct{ off, size int64 }{
		{2*chunkSize + 4096, 128 << 10},  // inside chunk 2
		{chunkSize + 17, 1000},           // inside chunk 1, unaligned
		{3*chunkSize + 900_000, 1 << 20}, // runs past EOF: clamp
		{5*chunkSize/2 + 1, 4096},
	} {
		got, err := cm.ReadRange(context.Background(), parent, c.off, int(c.size))
		if err != nil {
			t.Fatalf("ReadRange(%d,%d): %v", c.off, c.size, err)
		}
		end := min(c.off+c.size, int64(len(want)))
		if !bytes.Equal(got, want[c.off:end]) {
			t.Fatalf("ReadRange(%d,%d): %d bytes, mismatch", c.off, c.size, len(got))
		}
	}
	if n := cm.metrics.ChunkWindowReads.Load(); n == 0 {
		t.Fatal("no window reads counted: random windows took the whole-chunk path")
	}
	if n, _ := cm.rangeCacheStats(parent); n != 0 {
		t.Errorf("range cache holds %d chunks after random window reads; want 0", n)
	}
}

// A sequential stream keeps the per-chunk path, whose range cache and
// read-ahead serve the following windows.
func TestReadRange_ChunkWindowSequentialUsesChunkPath(t *testing.T) {
	const chunkSize = 1 << 20
	cm, parent, want := chunkWindowManager(t, chunkSize, 4)
	const win = 128 << 10
	for off := int64(0); off < 2*chunkSize; off += win {
		got, err := cm.ReadRange(context.Background(), parent, off, win)
		if err != nil || !bytes.Equal(got, want[off:off+win]) {
			t.Fatalf("sequential ReadRange(%d): err=%v mismatch=%t", off, err, !bytes.Equal(got, want[off:off+win]))
		}
	}
	if n := cm.metrics.ChunkWindowReads.Load(); n != 0 {
		t.Errorf("%d sequential windows took the window path; want 0", n)
	}
}

// The switch takes effect, and both paths return the same bytes.
func TestReadRange_ChunkWindowSwitch(t *testing.T) {
	const chunkSize = 1 << 20
	for _, disabled := range []bool{true, false} {
		cm, parent, want := chunkWindowManager(t, chunkSize, 3)
		cm.config.DisableChunkWindowReads = disabled
		off := int64(chunkSize + 5000)
		got, err := cm.ReadRange(context.Background(), parent, off, 4096)
		if err != nil || !bytes.Equal(got, want[off:off+4096]) {
			t.Fatalf("disabled=%t: err=%v mismatch", disabled, err)
		}
		if n := cm.metrics.ChunkWindowReads.Load(); (n > 0) == disabled {
			t.Errorf("disabled=%t: window reads=%d", disabled, n)
		}
	}
}

// A chunk file larger than the stride (written with a bigger chunk size than
// the entry records) is not served by the window path; the per-chunk path and
// its clamp decide.
func TestReadRange_ChunkWindowStrideMismatchNotServed(t *testing.T) {
	const chunkSize = 1 << 20
	cm, parent, _ := chunkWindowManager(t, chunkSize, 2)
	big := make([]byte, 2*chunkSize)
	if err := os.WriteFile(filepath.Join(cm.config.NVMePath, "chunked.bin_chunk_1"), big, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, ok := cm.readChunkWindow(context.Background(), parent, 1, chunkSize, chunkSize+10, chunkSize+20); ok {
		t.Fatal("served a window from an oversized chunk file")
	}
}

// 128 KiB random windows over 16 MiB chunks: the case the read profile found
// at ~110 MB/s with 128 bytes allocated per byte returned.
func BenchmarkReadRange_RandomWindow(b *testing.B) {
	const chunkSize = 16 << 20
	for _, disabled := range []bool{true, false} {
		name := "window"
		if disabled {
			name = "whole-chunk"
		}
		b.Run(name, func(b *testing.B) {
			cm, parent, _ := chunkWindowManager(b, chunkSize, 4)
			cm.config.DisableChunkWindowReads = disabled
			const win = 128 << 10
			offs := []int64{3*chunkSize + 7*win, chunkSize + 61*win, 2*chunkSize + 19*win, 101 * win, 3*chunkSize + 113*win, chunkSize + 3*win}
			b.SetBytes(win)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				cm.rangeMu.Lock()
				delete(cm.rangeChunks, parent) // measure the miss, as random access does
				cm.rangeMu.Unlock()
				if _, err := cm.ReadRange(context.Background(), parent, offs[i%len(offs)], win); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
