package cache

import (
	"context"
	"fmt"
	"testing"
)

// benchEntryTable seeds one directory of whole files plus one large chunked
// file, the shape that made the old scan expensive: a single 100GB object at
// an 8MB stride contributes ~12800 chunk entries that every FUSE lookup,
// readdir, and getattr had to copy and walk.
func benchEntryTable(b *testing.B, chunks int) *DefaultCacheManager {
	b.Helper()
	cm := newTestCacheManager()
	cm.config.Coordinator = nil
	for i := 0; i < 64; i++ {
		p := fmt.Sprintf("/data/file%03d.bin", i)
		cm.entries.put(&CacheEntry{FilePath: p, Size: 1024, Tier: TierNVMe})
	}
	cm.entries.put(&CacheEntry{
		FilePath: "/data/big.bin", Size: int64(chunks) * 8 << 20,
		Tier: TierNVMe, IsChunked: true, NumChunks: int64(chunks),
	})
	for i := 0; i < chunks; i++ {
		cp := chunkPathFor("/data/big.bin", int64(i))
		cm.entries.put(&CacheEntry{FilePath: cp, Size: 8 << 20, Tier: TierNVMe})
	}
	return cm
}

func BenchmarkLookupEntry(b *testing.B) {
	cm := benchEntryTable(b, 12800)
	ctx := context.Background()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, ok := cm.LookupEntry(ctx, "/data/file042.bin", false); !ok {
			b.Fatal("miss")
		}
	}
}

// The pre-index equivalent of one lookup: snapshot everything, then scan.
func BenchmarkLookupViaList(b *testing.B) {
	cm := benchEntryTable(b, 12800)
	ctx := context.Background()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		entries, err := cm.List(ctx)
		if err != nil {
			b.Fatal(err)
		}
		found := false
		for _, e := range entries {
			if e.FilePath == "/data/file042.bin" {
				found = true
				break
			}
		}
		if !found {
			b.Fatal("miss")
		}
	}
}

func BenchmarkReadDir(b *testing.B) {
	cm := benchEntryTable(b, 12800)
	ctx := context.Background()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := cm.ReadDir(ctx, "/data"); err != nil {
			b.Fatal(err)
		}
	}
}

// collectLocalDeleteTargets runs on every overwrite; before the index it
// scanned every cached object to find one file's chunks.
func BenchmarkCollectLocalDeleteTargets(b *testing.B) {
	cm := benchEntryTable(b, 12800)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cm.collectLocalDeleteTargets("/data/file042.bin", true)
	}
}
