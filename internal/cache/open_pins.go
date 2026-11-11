package cache

import "sync"

// Open-file pins: never evict what is being read right now.
//
// LRU recency only moves when the daemon sees a read, and a model served
// through kernel passthrough (or out of the page cache) is read by the kernel
// without the daemon seeing anything after open(). A model loaded hours ago
// and serving traffic since looks exactly like a file nobody wants, and is the
// first thing the evictor picks. Evicting it does not break the open fd (the
// inode lives until close), but the next process to open it (a restart, a
// second replica on the node) pays a full cold pull for a file that was never
// idle.
//
// So the FUSE layer reports opens and releases, and eviction skips any file
// (or any chunk of a file) with an open handle. Pins are in-memory and
// per-process, which is the right lifetime: the handles die with the process.
type openPins struct {
	mu sync.Mutex
	n  map[string]int
}

// NoteOpen records an open handle on filePath.
func (cm *DefaultCacheManager) NoteOpen(filePath string) {
	cm.sched.noteForeground()
	cm.openPins.mu.Lock()
	defer cm.openPins.mu.Unlock()
	if cm.openPins.n == nil {
		cm.openPins.n = make(map[string]int)
	}
	cm.openPins.n[filePath]++
}

// NoteRelease drops one open handle on filePath.
func (cm *DefaultCacheManager) NoteRelease(filePath string) {
	cm.openPins.mu.Lock()
	defer cm.openPins.mu.Unlock()
	if c := cm.openPins.n[filePath]; c > 1 {
		cm.openPins.n[filePath] = c - 1
	} else {
		delete(cm.openPins.n, filePath)
	}
}

// isOpen reports whether filePath, or the parent of a chunk path, has an
// open handle.
func (cm *DefaultCacheManager) isOpen(key string) bool {
	cm.openPins.mu.Lock()
	defer cm.openPins.mu.Unlock()
	if len(cm.openPins.n) == 0 {
		return false
	}
	if cm.openPins.n[key] > 0 {
		return true
	}
	if parent, ok := parentFilePathFromChunkPath(key); ok {
		return cm.openPins.n[parent] > 0
	}
	return false
}

// OpenFileCount reports how many distinct files have open handles.
func (cm *DefaultCacheManager) OpenFileCount() int {
	cm.openPins.mu.Lock()
	defer cm.openPins.mu.Unlock()
	return len(cm.openPins.n)
}
