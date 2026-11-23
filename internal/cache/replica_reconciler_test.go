package cache

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

// hotLocalObjects must not take cm.mu twice: with a writer constantly
// queued, a nested RLock inside the entries scan blocks forever (the
// 48-node Azure freeze). Large recent entries force the holdsWhole check.
func TestHotLocalObjectsNoNestedLockWithWriter(t *testing.T) {
	cm := evictTestManager(t, 1<<40)
	now := time.Now()
	cm.mu.Lock()
	for i := 0; i < 200; i++ {
		p := fmt.Sprintf("/m/big-%d.bin", i)
		cm.entries.put(&CacheEntry{FilePath: p, StoragePath: p, Size: 200 << 20, IsChunked: true, ChunkSize: 16 << 20,
			NumChunks: 13, LastAccessed: now, Tier: TierNVMe})
	}
	cm.mu.Unlock()

	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() { // a writer that is almost always queued
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			cm.mu.Lock()
			cm.mu.Unlock()
		}
	}()
	done := make(chan struct{})
	go func() {
		for i := 0; i < 200; i++ {
			cm.hotLocalObjects(now)
		}
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("hotLocalObjects deadlocked against a queued writer (nested cm.mu.RLock)")
	}
	close(stop)
	wg.Wait()
}
