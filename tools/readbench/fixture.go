package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"fuse-client/internal/cache"
)

// fixtureFile describes one logical file in a generated fixture, so the Rust
// layout test can check it without hard-coding Go behaviour.
type fixtureFile struct {
	Path      string `json:"path"`
	Size      int64  `json:"size"`
	Layout    string `json:"layout"` // chunked | whole | small
	ChunkSize int64  `json:"chunk_size"`
	Sidecar   bool   `json:"sidecar"`
	// Corrupt marks a file whose bytes were altered after the sidecar was
	// written; readers that verify must refuse it.
	Corrupt bool `json:"corrupt,omitempty"`
}

// cmdFixture writes a small cache dir exercising every layout the Go write
// paths produce, at a 1 MiB stride so it stays a few MB: per-chunk files with
// a short last chunk, a chunked file of exactly N strides, a whole-layout
// chunked file, small files with .sha256 sidecars (one corrupted), and a
// nested directory. A manifest.json lists what was written.
func cmdFixture(args []string) error {
	fs := flag.NewFlagSet("fixture", flag.ExitOnError)
	dir := fs.String("dir", "", "output cache dir (must not exist)")
	fs.Parse(args)
	if *dir == "" {
		return fmt.Errorf("-dir required")
	}
	const mb = 1 << 20
	cm, err := newCacheManager(cmOpts{dir: *dir, chunkMB: 1})
	if err != nil {
		return err
	}
	ctx := context.Background()
	var files []fixtureFile
	add := func(path string, size int64, layout string) error {
		var err error
		switch layout {
		case "chunked":
			err = cm.PutFromReader(ctx, path, &patternReader{end: size}, size, time.Now())
		case "whole":
			var f *os.File
			var staged string
			if f, staged, err = cm.CreateWriteFile(ctx, path); err != nil {
				return err
			}
			buf := make([]byte, size)
			fillPattern(buf, 0)
			if _, err = f.Write(buf); err != nil {
				return err
			}
			f.Close()
			err = cm.PutFromFile(ctx, path, staged, size, time.Now())
		case "small":
			data := make([]byte, size)
			fillPattern(data, 0)
			err = cm.Put(ctx, &cache.CacheEntry{FilePath: path, Data: data})
		}
		if err != nil {
			return fmt.Errorf("%s %s: %w", layout, path, err)
		}
		ff := fixtureFile{Path: path, Size: size, Layout: layout}
		if layout != "small" && size > mb {
			ff.ChunkSize = mb
		}
		_, serr := os.Stat(filepath.Join(*dir, path+".sha256"))
		ff.Sidecar = serr == nil
		files = append(files, ff)
		return nil
	}
	for _, f := range []struct {
		path   string
		size   int64
		layout string
	}{
		{"/models/a/chunked-short-tail.bin", 5*mb + 12345, "chunked"},
		{"/models/a/chunked-exact.bin", 4 * mb, "chunked"},
		{"/models/b/whole-chunked.bin", 3*mb + 7, "whole"},
		{"/models/b/whole-tiny.bin", 1000, "whole"},
		{"/models/small.bin", 777 * 1024, "small"},
		{"/models/corrupt.bin", 64 * 1024, "small"},
		{"/models/dir with space/x_chunk_y.bin", 2*mb + 1, "chunked"},
	} {
		if err := add(f.path, f.size, f.layout); err != nil {
			return err
		}
	}
	cm.Shutdown()

	// Corrupt one byte of a sidecar-protected file after the fact.
	p := filepath.Join(*dir, "models/corrupt.bin")
	b, err := os.ReadFile(p)
	if err != nil {
		return err
	}
	b[100] ^= 0xff
	if err := os.WriteFile(p, b, 0o600); err != nil {
		return err
	}
	for i := range files {
		if files[i].Path == "/models/corrupt.bin" {
			files[i].Corrupt = true
		}
	}
	// A staged temp file left by an interrupted write must never be served.
	os.WriteFile(filepath.Join(*dir, "models/a/.nvme-write-123456"), []byte("partial"), 0o600)

	m, _ := json.MarshalIndent(map[string]any{"chunk_size": mb, "files": files}, "", "  ")
	if err := os.WriteFile(filepath.Join(*dir, "manifest.json"), append(m, '\n'), 0o644); err != nil {
		return err
	}
	log.Printf("fixture written to %s (%d files)", *dir, len(files))
	return nil
}
