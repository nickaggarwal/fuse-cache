package cache

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Persisted ledger: remembers across restarts which local files are already
// durable in the cloud tier.
//
// PersistedToCloud lives only in memory. After a restart rehydration adopts
// every file as unpersisted, so the first eviction pass sends one cloud HEAD
// per candidate (evictionSafe) before it can free anything; under real
// pressure, on a node holding thousands of chunk files, eviction stalls behind
// the probes exactly when it is needed.
//
// The ledger is one append-only file, <NVMePath>/.fuse-ledger/persisted, a
// line per object marked durable: "<size> <mtime-unix-nano> <path>". Its own
// reserved directory, skipped by rehydration like the durability journal (the
// journal directory holds exactly the not-yet-persisted records).
// Rehydration trusts a line only if the file on disk still has that exact size
// and mtime, so a file rewritten after it was persisted is never mistaken for
// durable; anything not trusted falls back to the cloud probe, as before.
//
// Advisory: a lost or truncated ledger only costs the probes it saves. It is
// compacted at startup to the lines that were still trusted.

const (
	persistedLedgerName = "persisted"
	// persistedLedgerDirName is the reserved NVMe subdirectory for the ledger.
	persistedLedgerDirName = ".fuse-ledger"
)

type persistedLedger struct {
	mu   sync.Mutex
	path string
	f    *os.File
}

type ledgerKey struct {
	size  int64
	mtime int64
}

// openPersistedLedger reads the ledger and returns what it recorded, keyed by
// cache path. A missing or unreadable ledger yields an empty map.
func openPersistedLedger(dir string) (*persistedLedger, map[string]ledgerKey) {
	if dir == "" {
		return nil, nil
	}
	l := &persistedLedger{path: filepath.Join(dir, persistedLedgerName)}
	seen := map[string]ledgerKey{}
	if f, err := os.Open(l.path); err == nil {
		sc := bufio.NewScanner(f)
		sc.Buffer(make([]byte, 64*1024), 1024*1024)
		for sc.Scan() {
			parts := strings.SplitN(sc.Text(), " ", 3)
			if len(parts) != 3 {
				continue
			}
			size, err1 := strconv.ParseInt(parts[0], 10, 64)
			mtime, err2 := strconv.ParseInt(parts[1], 10, 64)
			if err1 != nil || err2 != nil || parts[2] == "" {
				continue
			}
			seen[parts[2]] = ledgerKey{size: size, mtime: mtime}
		}
		f.Close()
	}
	return l, seen
}

// compact rewrites the ledger with only the given entries and opens it for
// appending.
func (l *persistedLedger) compact(keep map[string]ledgerKey) {
	if l == nil {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if err := os.MkdirAll(filepath.Dir(l.path), 0o755); err != nil {
		return
	}
	tmp := l.path + ".tmp"
	if f, err := os.Create(tmp); err == nil {
		w := bufio.NewWriter(f)
		for p, k := range keep {
			fmt.Fprintf(w, "%d %d %s\n", k.size, k.mtime, p)
		}
		_ = w.Flush()
		f.Close()
		_ = os.Rename(tmp, l.path)
	}
	l.f, _ = os.OpenFile(l.path, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0o644)
}

// record appends one durable object. Missing file or ledger: no-op.
func (l *persistedLedger) record(path string, full string) {
	if l == nil {
		return
	}
	info, err := os.Stat(full)
	if err != nil {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.f == nil {
		return
	}
	fmt.Fprintf(l.f, "%d %d %s\n", info.Size(), info.ModTime().UnixNano(), path)
}

func (l *persistedLedger) close() {
	if l == nil {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.f != nil {
		l.f.Close()
		l.f = nil
	}
}

// trustedPersisted reports whether the ledger's line for path still matches
// the file on disk (same size and mtime).
func trustedPersisted(seen map[string]ledgerKey, path string, size int64, mtime time.Time) bool {
	k, ok := seen[path]
	return ok && k.size == size && k.mtime == mtime.UnixNano()
}
