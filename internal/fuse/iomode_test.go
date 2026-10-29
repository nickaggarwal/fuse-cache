package fuse

import (
	"sync"
	"testing"
)

// The first open decides the mode; concurrent opens join it whatever the
// local copy says, until all are released.
func TestIOModeTrackerJoinsHeldMode(t *testing.T) {
	var tr ioModeTracker
	local := false
	can := func() bool { return local }

	// First open: no local copy -> cached.
	if m := tr.open(true, can); m != ioModeCached {
		t.Fatalf("first open: %v", m)
	}
	// The read lands the copy; a concurrent open must stay cached.
	local = true
	if m := tr.open(true, can); m != ioModeCached {
		t.Fatalf("join while cached held: %v (kernel would EIO a passthrough open)", m)
	}
	tr.release()
	tr.release()
	// All released: the next open may use passthrough now.
	if m := tr.open(true, can); m != ioModePassthrough {
		t.Fatalf("after release: %v", m)
	}
	// Concurrent opens join passthrough even if the probe would now fail.
	local = false
	if m := tr.open(true, can); m != ioModePassthrough {
		t.Fatalf("join while passthrough held: %v", m)
	}
	tr.release()
	tr.release()
	if tr.count != 0 || tr.mode != ioModeNone {
		t.Fatalf("not reset: count=%d mode=%v", tr.count, tr.mode)
	}
}

func TestIOModeTrackerWriteOpenAndBalance(t *testing.T) {
	var tr ioModeTracker
	if m := tr.open(false, func() bool { return true }); m != ioModeCached {
		t.Fatalf("write-only first open: %v", m)
	}
	tr.release()
	tr.release() // an extra release must not underflow
	if tr.count != 0 {
		t.Fatalf("count %d", tr.count)
	}
}

// Many opens racing on one inode never end up in two modes at once.
func TestIOModeTrackerConcurrent(t *testing.T) {
	var tr ioModeTracker
	var mu sync.Mutex
	seen := map[ioMode]int{}
	var wg sync.WaitGroup
	var start sync.WaitGroup
	start.Add(1)
	for i := 0; i < 64; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			start.Wait()
			m := tr.open(true, func() bool { return i%2 == 0 })
			mu.Lock()
			seen[m]++
			mu.Unlock()
		}(i)
	}
	start.Done()
	wg.Wait()
	if len(seen) != 1 {
		t.Fatalf("opens held in more than one mode at once: %v", seen)
	}
}
