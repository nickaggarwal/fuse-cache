//go:build linux

package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	"fuse-client/internal/coordinator"
	fusefs "fuse-client/internal/fuse"
)

// cmdMount mounts the production go-fuse backend over a cache dir with the
// same CacheManager settings bench uses, and serves until SIGINT/SIGTERM.
func cmdMount(args []string) error {
	fs := flag.NewFlagSet("mount", flag.ExitOnError)
	dir := fs.String("dir", "", "cache dir")
	mnt := fs.String("mnt", "", "mount point")
	chunkMB := fs.Int("chunk-mb", 16, "configured -chunk-size")
	whole := fs.Bool("whole-file-reads", false, "-range-whole-file-reads")
	var pubs pubFlag
	fs.Var(&pubs, "publish", "path:size:strideMiB (repeatable)")
	fs.Parse(args)
	restore := hideJournal(*dir)
	defer restore()
	coord := coordinator.NewCoordinatorService()
	if err := parsePub(coord, pubs); err != nil {
		return err
	}
	cm, err := newCacheManager(cmOpts{dir: *dir, chunkMB: *chunkMB, wholeFile: *whole, coord: coord, localPeerID: "bench-local"})
	if err != nil {
		return err
	}
	defer cm.Shutdown()
	gfs := fusefs.NewGoFuseFileSystem(cm)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := gfs.Mount(ctx, *mnt); err != nil {
		return err
	}
	log.Printf("go-fuse mounted %s at %s", *dir, *mnt)
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
	go func() { <-sig; cancel() }()
	gfs.Serve(ctx)
	return gfs.Unmount(*mnt)
}
