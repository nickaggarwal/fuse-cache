package nodeinit

import (
	"fmt"
	"os"
	"path/filepath"
	"time"
)

const benchBlockSize = 4 << 20 // 4 MiB blocks, matching large sequential cache IO

// benchmarkDir measures sequential write and read throughput (MB/s) in dir
// using a temp file of size totalBytes. O_SYNC-free but with one Sync() at the
// end of the write phase so page-cache-only writes don't report memory speed.
// The read phase reopens the file; on Linux the page cache may still serve it,
// which is acceptable — the score formula weights write speed equally and the
// benchmark is a tie-breaker, not the primary signal.
func benchmarkDir(dir string, totalBytes int64) (writeMBps, readMBps float64, err error) {
	if totalBytes <= 0 {
		totalBytes = 64 << 20
	}
	f, err := os.CreateTemp(dir, ".fuse-nodeinit-bench-*")
	if err != nil {
		return 0, 0, err
	}
	path := f.Name()
	defer os.Remove(path)

	block := make([]byte, benchBlockSize)
	for i := range block {
		block[i] = byte(i * 31)
	}

	writeStart := time.Now()
	var written int64
	for written < totalBytes {
		n := int64(len(block))
		if written+n > totalBytes {
			n = totalBytes - written
		}
		if _, werr := f.Write(block[:n]); werr != nil {
			f.Close()
			return 0, 0, werr
		}
		written += n
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return 0, 0, err
	}
	if err := f.Close(); err != nil {
		return 0, 0, err
	}
	writeDur := time.Since(writeStart)

	rf, err := os.Open(path)
	if err != nil {
		return 0, 0, err
	}
	defer rf.Close()
	readStart := time.Now()
	var read int64
	for {
		n, rerr := rf.Read(block)
		read += int64(n)
		if rerr != nil {
			break
		}
	}
	readDur := time.Since(readStart)
	if read < written {
		return 0, 0, fmt.Errorf("benchmark read %d of %d bytes", read, written)
	}

	toMBps := func(bytes int64, dur time.Duration) float64 {
		if dur <= 0 {
			return 0
		}
		return float64(bytes) / (1 << 20) / dur.Seconds()
	}
	return toMBps(written, writeDur), toMBps(read, readDur), nil
}

// RefreshFreeSpace re-runs statfs on the chosen cache dir and updates the
// config's free bytes and budget. Used by the daemon mode so the client's
// heartbeats can report real headroom as the disk fills.
func RefreshFreeSpace(cfg *Config, hostRoot string, opts Options) error {
	hostDir := filepath.Join(hostRoot, cfg.CacheDir)
	total, free, err := statfsFunc(hostDir)
	if err != nil {
		return err
	}
	cfg.TotalBytes = total
	cfg.FreeBytes = free
	cfg.CacheBytes = cacheBudget(free, opts)
	cfg.GeneratedAt = time.Now()
	return nil
}
