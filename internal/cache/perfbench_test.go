package cache

// Performance comparison benchmarks. This file is intentionally self-contained
// and identical between the baseline worktree and the branch so results are
// directly comparable. It reuses newTestCacheManager/newLargeFileCacheManager
// and mockStorage from cache_test.go.

import (
	"context"
	"fmt"
	"io"
	"log"
	"testing"
	"time"
)

// benchCacheManager returns a large-file cache manager with logging silenced
// so benchmark output stays parseable.
func benchCacheManager(maxNVMe int64) *DefaultCacheManager {
	cm := newLargeFileCacheManager(maxNVMe)
	cm.logger = log.New(io.Discard, "", 0)
	return cm
}

func benchPut(b *testing.B, size int64) {
	cm := benchCacheManager(1 << 40) // effectively unlimited NVMe
	ctx := context.Background()
	data := make([]byte, size)
	b.SetBytes(size)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		entry := &CacheEntry{
			FilePath:     fmt.Sprintf("/bench/put-%d-%d.bin", size, i),
			Size:         size,
			LastAccessed: time.Now(),
			Data:         data,
		}
		if err := cm.Put(ctx, entry); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkPut256KB(b *testing.B)  { benchPut(b, 256<<10) }
func BenchmarkPut2MB(b *testing.B)    { benchPut(b, 2<<20) }
func BenchmarkPut64MBChunked(b *testing.B) { benchPut(b, 64<<20) }

func BenchmarkGetNVMeHit(b *testing.B) {
	cm := benchCacheManager(1 << 40)
	ctx := context.Background()
	data := make([]byte, 2<<20)
	entry := &CacheEntry{
		FilePath:     "/bench/get-hit.bin",
		Size:         int64(len(data)),
		LastAccessed: time.Now(),
		Data:         data,
	}
	if err := cm.Put(ctx, entry); err != nil {
		b.Fatal(err)
	}
	b.SetBytes(int64(len(data)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := cm.Get(ctx, "/bench/get-hit.bin"); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkGetChunked64MB(b *testing.B) {
	cm := benchCacheManager(1 << 40)
	ctx := context.Background()
	data := make([]byte, 64<<20)
	entry := &CacheEntry{
		FilePath:     "/bench/get-chunked.bin",
		Size:         int64(len(data)),
		LastAccessed: time.Now(),
		Data:         data,
	}
	if err := cm.Put(ctx, entry); err != nil {
		b.Fatal(err)
	}
	b.SetBytes(int64(len(data)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := cm.Get(ctx, "/bench/get-chunked.bin"); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkReadRangeSequential(b *testing.B) {
	cm := benchCacheManager(1 << 40)
	ctx := context.Background()
	const fileSize = 64 << 20
	const rangeSize = 1 << 20
	data := make([]byte, fileSize)
	entry := &CacheEntry{
		FilePath:     "/bench/range.bin",
		Size:         fileSize,
		LastAccessed: time.Now(),
		Data:         data,
	}
	if err := cm.Put(ctx, entry); err != nil {
		b.Fatal(err)
	}
	b.SetBytes(rangeSize)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		off := int64(i%(fileSize/rangeSize)) * rangeSize
		if _, err := cm.ReadRange(ctx, "/bench/range.bin", off, rangeSize); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkPutWithEvictionPressure(b *testing.B) {
	// Cap NVMe at 32MB with 1MB objects so eviction runs continuously.
	cm := benchCacheManager(32 << 20)
	ctx := context.Background()
	const size = 1 << 20
	data := make([]byte, size)
	b.SetBytes(size)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		entry := &CacheEntry{
			FilePath:     fmt.Sprintf("/bench/evict-%d.bin", i),
			Size:         size,
			LastAccessed: time.Now(),
			Data:         data,
		}
		if err := cm.Put(ctx, entry); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkGetParallel(b *testing.B) {
	cm := benchCacheManager(1 << 40)
	ctx := context.Background()
	data := make([]byte, 1<<20)
	for i := 0; i < 16; i++ {
		entry := &CacheEntry{
			FilePath:     fmt.Sprintf("/bench/par-%d.bin", i),
			Size:         int64(len(data)),
			LastAccessed: time.Now(),
			Data:         data,
		}
		if err := cm.Put(ctx, entry); err != nil {
			b.Fatal(err)
		}
	}
	b.SetBytes(int64(len(data)))
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			path := fmt.Sprintf("/bench/par-%d.bin", i%16)
			if _, err := cm.Get(ctx, path); err != nil {
				b.Fatal(err)
			}
			i++
		}
	})
}
