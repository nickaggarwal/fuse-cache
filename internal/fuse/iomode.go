package fuse

import "sync"

// ioMode is the kernel io mode an open of an inode is granted.
type ioMode int

const (
	ioModeNone ioMode = iota
	ioModeCached
	ioModePassthrough
)

// ioModeTracker keeps every open of one inode in one kernel io mode.
//
// Linux 6.9+ FUSE refuses to hold an inode open in passthrough and cached
// mode at once: an open() whose mode conflicts with the opens already held
// fails with EIO inside the kernel ("Using incorrect open mode is a server
// mistake, which results in user visible failure of open() with EIO",
// fs/fuse/iomode.c). With a cold cache that is the common case under load:
// the first open of a small file finds no local copy and is served cached,
// its read lands the copy, and a concurrent open then finds the copy and
// asks for passthrough. Eight threads reading tokenizer.json on cpu-w3 hit
// it on every run; with passthrough off, never.
//
// So the first open decides, and later opens join its mode until every open
// of the inode is released. A file that became local while cached opens are
// held is served cached until they close; the next open after that gets
// passthrough.
type ioModeTracker struct {
	mu    sync.Mutex
	count int
	mode  ioMode
}

// open grants the mode for a new open. wantPassthrough says whether this
// open could use passthrough at all (read-only, enabled); canPassthrough is
// asked only when the open decides the mode (no opens held).
func (t *ioModeTracker) open(wantPassthrough bool, canPassthrough func() bool) ioMode {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.count > 0 {
		t.count++
		if t.mode == ioModePassthrough && !wantPassthrough {
			// A write open while passthrough opens are held cannot join
			// them; the kernel decides (it refuses the cached open). Rare
			// on the read-mostly mount; counted so release stays balanced.
			return ioModeCached
		}
		return t.mode
	}
	t.count = 1
	if wantPassthrough && canPassthrough() {
		t.mode = ioModePassthrough
	} else {
		t.mode = ioModeCached
	}
	return t.mode
}

// release records that one open of the inode was released.
func (t *ioModeTracker) release() {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.count > 0 {
		t.count--
	}
	if t.count == 0 {
		t.mode = ioModeNone
	}
}
