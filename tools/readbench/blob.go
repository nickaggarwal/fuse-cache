package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"
)

// cmdGenBlob writes a pattern blob into a cache dir at the path the OCI blob
// endpoint serves it from, /oci/sha256/<hex>, through the same write path as
// gen (PutFromReader -> per-chunk files). The digest is the real SHA-256 of
// the content, so the blob is a valid content-addressed object. Prints the
// hex digest.
func cmdGenBlob(args []string) error {
	fs := flag.NewFlagSet("gen-blob", flag.ExitOnError)
	dir := fs.String("dir", "", "cache dir")
	sizeMB := fs.Int64("size-mb", 4096, "blob size in MiB")
	extra := fs.Int64("extra-bytes", 0, "bytes added to size")
	chunkMB := fs.Int("chunk-mb", 16, "chunk size")
	digestOut := fs.String("digest-out", "", "write the hex digest here")
	fs.Parse(args)
	if *dir == "" {
		return errors.New("-dir required")
	}
	size := *sizeMB<<20 + *extra
	h := sha256.New()
	if _, err := io.Copy(h, &patternReader{end: size}); err != nil {
		return err
	}
	digest := hex.EncodeToString(h.Sum(nil))
	cm, err := newCacheManager(cmOpts{dir: *dir, chunkMB: *chunkMB})
	if err != nil {
		return err
	}
	defer cm.Shutdown()
	path := "/oci/sha256/" + digest
	if err := cm.PutFromReader(context.Background(), path, &patternReader{end: size}, size, time.Now()); err != nil {
		return err
	}
	log.Printf("gen-blob %s size=%d chunk=%dMiB", path, size, *chunkMB)
	fmt.Println(digest)
	if *digestOut != "" {
		return os.WriteFile(*digestOut, []byte(digest+"\n"), 0o644)
	}
	return nil
}

// cmdBenchBlob drives a blob endpoint the way nydusd's registry/http-proxy
// backend does: HEAD for the size, then Range GETs of -window-kib at
// window-aligned offsets from -streams concurrent connections. Offsets come
// from the shared workload engine: "rand" scatters them (nydusd's 4 prefetch
// threads walking different regions of a blob), "seq" gives each stream its
// own sequential region.
func cmdBenchBlob(args []string) error {
	fs := flag.NewFlagSet("bench-blob", flag.ExitOnError)
	base := fs.String("url", "http://127.0.0.1:8081", "server base URL")
	route := fs.String("route", "v2", "v2 (/v2/<repo>/blobs/sha256:<hex>) | oci (/oci/blobs/<hex>)")
	digest := fs.String("digest", "", "blob hex digest")
	impl := fs.String("impl", "?", "label")
	layer := fs.String("layer", "blob", "label")
	windowKiB := fs.Int64("window-kib", 1024, "range size")
	pattern := fs.String("pattern", "rand", "rand|seq")
	streams := fs.Int("streams", 4, "concurrent connections")
	totalMB := fs.Int64("total-mb", 2048, "bytes to fetch")
	verify := fs.Bool("verify", false, "check content")
	fs.Parse(args)
	if *digest == "" {
		return errors.New("-digest required")
	}
	u := *base + "/v2/bench/model/blobs/sha256:" + *digest
	if *route == "oci" {
		u = *base + "/oci/blobs/" + *digest
	}
	tr := &http.Transport{
		DialContext:         (&net.Dialer{Timeout: 5 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
		MaxIdleConnsPerHost: *streams * 2,
		DisableCompression:  true,
		ReadBufferSize:      256 << 10,
	}
	client := &http.Client{Transport: tr, Timeout: 60 * time.Second}
	resp, err := client.Head(u)
	if err != nil {
		return err
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("HEAD %s: %s", u, resp.Status)
	}
	size := resp.ContentLength
	w := workload{size: size, window: *windowKiB << 10, pattern: *pattern, threads: *streams, totalBytes: *totalMB << 20, seed: 42}
	bufs := sync.Pool{New: func() any { b := make([]byte, w.window); return &b }}
	res, err := runWorkload(w, *impl, *layer, func(off int64, n int) (int, error) {
		req, _ := http.NewRequest(http.MethodGet, u, nil)
		req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", off, off+int64(n)-1))
		resp, err := client.Do(req)
		if err != nil {
			return 0, err
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusPartialContent {
			b, _ := io.ReadAll(io.LimitReader(resp.Body, 256))
			return 0, fmt.Errorf("GET range %d+%d: %s %s", off, n, resp.Status, strings.TrimSpace(string(b)))
		}
		bp := bufs.Get().(*[]byte)
		defer bufs.Put(bp)
		got, err := io.ReadFull(resp.Body, (*bp)[:n])
		if err != nil && !errors.Is(err, io.ErrUnexpectedEOF) {
			return got, err
		}
		if *verify {
			if err := checkPattern((*bp)[:got], off); err != nil {
				return 0, err
			}
		}
		return got, nil
	})
	if err != nil {
		return err
	}
	res.Note = "url=" + u
	emit(res)
	return nil
}
