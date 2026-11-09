package cache

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// countingCloud counts Size/Exists probes on top of the mock cloud tier.
type countingCloud struct {
	*mockStorage
	probes int
}

func (c *countingCloud) Exists(ctx context.Context, p string) bool {
	c.probes++
	return c.mockStorage.Exists(ctx, p)
}

func (c *countingCloud) Size(ctx context.Context, p string) (int64, error) {
	c.probes++
	return c.mockStorage.Size(ctx, p)
}

// A file marked durable before a restart is adopted as persisted from the
// ledger and evicted without a cloud probe; one rewritten since (size or mtime
// changed) is not trusted and keeps the probe.
func TestPersistedLedgerSurvivesRestart(t *testing.T) {
	ctx := context.Background()
	prev := evictTestManager(t, 1<<30)
	prev.rehydrateNVMeAccounting() // opens the (empty) ledger

	for _, p := range []string{"/durable.bin", "/rewritten.bin"} {
		seedNVMe(t, prev, p, 3000, true, 0)
		prev.mu.Lock()
		mustEntry(t, prev, p).PersistedToCloud = false
		prev.mu.Unlock()
		prev.markPersistedToCloud(p)
	}
	prev.persisted.close()

	// The file changes after it was persisted: same path, new bytes.
	full := filepath.Join(prev.config.NVMePath, "rewritten.bin")
	if err := os.WriteFile(full, make([]byte, 3100), 0o644); err != nil {
		t.Fatal(err)
	}
	later := time.Now().Add(time.Minute)
	_ = os.Chtimes(full, later, later)

	// The next process, over the same directory.
	next := evictTestManager(t, 1<<30)
	next.config.NVMePath = prev.config.NVMePath
	next.nvmeStorage = prev.nvmeStorage
	cloud := &countingCloud{mockStorage: newMockStorage()}
	next.cloudStorage = cloud
	next.rehydrateNVMeAccounting()
	defer next.persisted.close()

	next.mu.RLock()
	durable := mustEntry(t, next, "/durable.bin").PersistedToCloud
	rewritten := mustEntry(t, next, "/rewritten.bin").PersistedToCloud
	next.mu.RUnlock()
	if !durable {
		t.Fatal("ledgered file not adopted as persisted")
	}
	if rewritten {
		t.Fatal("file rewritten after it was persisted was trusted from the ledger")
	}
	if got := next.metrics.RehydratePersistedTrusted.Load(); got != 1 {
		t.Fatalf("RehydratePersistedTrusted = %d, want 1", got)
	}

	if !next.evictionSafe(ctx, "/durable.bin", mustEntryRead(t, next, "/durable.bin")) {
		t.Fatal("ledgered file not eviction-safe")
	}
	if cloud.probes != 0 {
		t.Fatalf("eviction of a ledgered file probed the cloud %d times", cloud.probes)
	}

	// The ledger was compacted to the one trusted line.
	raw, err := os.ReadFile(filepath.Join(prev.config.NVMePath, persistedLedgerDirName, persistedLedgerName))
	if err != nil {
		t.Fatal(err)
	}
	if n := len(splitLines(string(raw))); n != 1 {
		t.Fatalf("ledger has %d lines after compaction, want 1:\n%s", n, raw)
	}
}

func mustEntryRead(t *testing.T, cm *DefaultCacheManager, p string) *CacheEntry {
	t.Helper()
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	return mustEntry(t, cm, p)
}

func splitLines(s string) []string {
	var out []string
	start := 0
	for i := 0; i < len(s); i++ {
		if s[i] == '\n' {
			if i > start {
				out = append(out, s[start:i])
			}
			start = i + 1
		}
	}
	if start < len(s) {
		out = append(out, s[start:])
	}
	return out
}
