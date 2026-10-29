package main

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"sync"
)

// cmdBenchFile runs the shared workload as pread(2) against a plain path. It
// is the one client used for every mount (Go go-fuse, Rust fuser) and for the
// raw cache file, so FUSE numbers differ only in the server.
func cmdBenchFile(args []string) error {
	fs := flag.NewFlagSet("bench-file", flag.ExitOnError)
	file := fs.String("file", "", "file to read")
	impl := fs.String("impl", "?", "label for the serving implementation")
	layer := fs.String("layer", "fuse", "label for the measured layer")
	windowKiB := fs.Int64("window-kib", 128, "read size")
	pattern := fs.String("pattern", "seq", "seq|rand")
	threads := fs.Int("threads", 1, "concurrent readers")
	totalMB := fs.Int64("total-mb", 1024, "bytes to read")
	verify := fs.Bool("verify", false, "check content")
	fs.Parse(args)
	if *file == "" {
		return errors.New("-file required")
	}
	f, err := os.Open(*file)
	if err != nil {
		return err
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		return err
	}
	w := workload{size: st.Size(), window: *windowKiB << 10, pattern: *pattern, threads: *threads, totalBytes: *totalMB << 20, seed: 42}
	bufs := sync.Pool{New: func() any { b := make([]byte, w.window); return &b }}
	res, err := runWorkload(w, *impl, *layer, func(off int64, n int) (int, error) {
		bp := bufs.Get().(*[]byte)
		defer bufs.Put(bp)
		b := (*bp)[:n]
		got, err := f.ReadAt(b, off)
		if err != nil && got != n {
			return 0, fmt.Errorf("pread off=%d: %w", off, err)
		}
		if *verify {
			if err := checkPattern(b[:got], off); err != nil {
				return 0, err
			}
		}
		return got, nil
	})
	if err != nil {
		return err
	}
	emit(res)
	return nil
}
