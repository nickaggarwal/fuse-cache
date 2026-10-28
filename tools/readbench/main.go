// Command readbench measures the Go read data path of fuse-client so a
// rewrite can be compared against it on identical workloads.
//
// It drives only exported APIs of internal/cache, internal/api and
// internal/fuse (nothing under internal/api is modified), so what it measures
// is the code path the client daemon runs:
//
//	gen        write a cache dir through the real CacheManager write paths
//	           (PutFromReader = per-chunk layout, PutFromFile = whole-file
//	           layout, Put = small file with .sha256 sidecar)
//	bench      in-process DefaultCacheManager.ReadRange workloads + pprof
//	serve-peer the real api.Handler (/api/peer/read, sendfile) over a dir
//	bench-peer the real PeerStorage raw-HTTP client against any peer server
//	coord      a minimal coordinator HTTP endpoint (location + peers)
//	mount      the real go-fuse backend over a dir (Linux only)
//	bench-file pread workloads against any path (e.g. a file inside a FUSE
//	           mount), so Go and Rust mounts are driven by one client
//
// Azure ranged reads are benchmarked from internal/cache (azure_bench_test.go)
// because AzureStorage's fields are unexported and its constructor pins the
// public endpoint.
//
// File content is a pure function of the byte offset (see fillPattern), so
// any range read by any implementation can be verified without a reference
// copy.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"math"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"fuse-client/internal/api"
	"fuse-client/internal/cache"
	"fuse-client/internal/coordinator"
)

func main() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	if len(os.Args) < 2 {
		usage()
	}
	cmd, args := os.Args[1], os.Args[2:]
	var err error
	switch cmd {
	case "gen":
		err = cmdGen(args)
	case "verify":
		err = cmdVerify(args)
	case "bench":
		err = cmdBench(args)
	case "serve-peer":
		err = cmdServePeer(args)
	case "bench-peer":
		err = cmdBenchPeer(args)
	case "coord":
		err = cmdCoord(args)
	case "mount":
		err = cmdMount(args)
	case "bench-file":
		err = cmdBenchFile(args)
	case "azure-vector":
		err = cmdAzureVector(args)
	case "fixture":
		err = cmdFixture(args)
	case "azure-put":
		err = cmdAzurePut(args)
	default:
		usage()
	}
	if err != nil {
		log.Fatalf("%s: %v", cmd, err)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage: readbench gen|verify|bench|serve-peer|bench-peer|coord|mount|bench-file [flags]")
	os.Exit(2)
}

// ---------------------------------------------------------------------------
// Deterministic content. Byte at absolute offset o is byte (o%8) of
// splitmix64(o/8). The Rust crate implements the identical function.

func splitmix64(z uint64) uint64 {
	z += 0x9e3779b97f4a7c15
	z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9
	z = (z ^ (z >> 27)) * 0x94d049bb133111eb
	return z ^ (z >> 31)
}

func fillPattern(buf []byte, off int64) {
	i := 0
	for i < len(buf) {
		o := off + int64(i)
		v := splitmix64(uint64(o >> 3))
		for k := int(o & 7); k < 8 && i < len(buf); k++ {
			buf[i] = byte(v >> (8 * uint(k)))
			i++
		}
	}
}

func checkPattern(buf []byte, off int64) error {
	want := make([]byte, len(buf))
	fillPattern(want, off)
	for i := range buf {
		if buf[i] != want[i] {
			return fmt.Errorf("content mismatch at offset %d: got %#x want %#x", off+int64(i), buf[i], want[i])
		}
	}
	return nil
}

type patternReader struct{ off, end int64 }

func (r *patternReader) Read(p []byte) (int, error) {
	if r.off >= r.end {
		return 0, io.EOF
	}
	if rem := r.end - r.off; int64(len(p)) > rem {
		p = p[:rem]
	}
	fillPattern(p, r.off)
	r.off += int64(len(p))
	return len(p), nil
}

// ---------------------------------------------------------------------------
// Cache manager construction with the production client's read-path defaults
// (cmd/client/main.go flag defaults, -chunk-size 16 as on the clusters).

type cmOpts struct {
	dir          string
	chunkMB      int
	wholeFile    bool
	localPeerID  string
	coord        coordinator.Coordinator
	parallel     int
	prefetch     int
	prefetchMax  int
	rangeChunks  int
	rangeCacheMB int
}

func newCacheManager(o cmOpts) (*cache.DefaultCacheManager, error) {
	if o.parallel == 0 {
		o.parallel = 8
	}
	if o.prefetch == 0 {
		o.prefetch = 2
	}
	if o.prefetchMax == 0 {
		o.prefetchMax = 16
	}
	if o.rangeChunks == 0 {
		o.rangeChunks = 16
	}
	if o.rangeCacheMB == 0 {
		o.rangeCacheMB = 512
	}
	cfg := &cache.CacheConfig{
		NVMePath:                   o.dir,
		MaxNVMeSize:                1 << 40,
		MaxPeerSize:                1 << 40,
		PeerTimeout:                30 * time.Second,
		CloudTimeout:               60 * time.Second,
		Coordinator:                o.coord,
		ChunkSize:                  int64(o.chunkMB) << 20,
		ParallelRangeReads:         o.parallel,
		RangePrefetchChunks:        o.prefetch,
		RangePrefetchMaxChunks:     o.prefetchMax,
		DisableWholeFileRangeReads: !o.wholeFile,
		RangeChunkCacheSize:        o.rangeChunks,
		RangeChunkCacheMaxBytes:    int64(o.rangeCacheMB) << 20,
		RangePrefetchMaxBytes:      128 << 20,
		PeerReadSortByNetwork:      true,
		PeerReadParallelFanout:     true,
		AdaptiveRemoteRead:         true,
		PeerRawTransport:           true,
		LocalPeerID:                o.localPeerID,
		// The benchmark must not rewrite the layout under itself: completion
		// assembles chunk files into a whole file once a reader goes idle.
		ChunkCompletionDisabled: true,
		WatermarkEvictDisabled:  true,
		WarmReconcileDisabled:   true,
		// No real cloud: S3 at a closed port fails fast instead of probing
		// instance metadata for credentials.
		CloudProvider:      "s3",
		S3Bucket:           "readbench",
		S3Region:           "us-east-1",
		S3Endpoint:         "http://127.0.0.1:1",
		S3ForcePathStyle:   true,
		CloudRetryCount:    1,
		CloudRetryBaseWait: time.Millisecond,
	}
	return cache.NewCacheManager(cfg)
}

// ---------------------------------------------------------------------------
// gen

func cmdGen(args []string) error {
	fs := flag.NewFlagSet("gen", flag.ExitOnError)
	dir := fs.String("dir", "", "cache dir to write")
	path := fs.String("path", "/bench/model.bin", "cache path")
	sizeMB := fs.Int64("size-mb", 4096, "file size in MiB")
	extra := fs.Int64("extra-bytes", 0, "bytes added to size (exercise a short last chunk)")
	chunkMB := fs.Int("chunk-mb", 16, "chunk size in MiB (-chunk-size)")
	layout := fs.String("layout", "chunked", "chunked (PutFromReader) | whole (PutFromFile) | small (Put, writes .sha256)")
	fs.Parse(args)
	if *dir == "" {
		return errors.New("-dir required")
	}
	size := *sizeMB<<20 + *extra
	cm, err := newCacheManager(cmOpts{dir: *dir, chunkMB: *chunkMB})
	if err != nil {
		return err
	}
	defer cm.Shutdown()
	ctx := context.Background()
	start := time.Now()
	switch *layout {
	case "chunked":
		err = cm.PutFromReader(ctx, *path, &patternReader{end: size}, size, time.Now())
	case "whole":
		var f *os.File
		var staged string
		f, staged, err = cm.CreateWriteFile(ctx, *path)
		if err != nil {
			return err
		}
		buf := make([]byte, 4<<20)
		for off := int64(0); off < size; {
			n := int64(len(buf))
			if size-off < n {
				n = size - off
			}
			fillPattern(buf[:n], off)
			if _, err = f.Write(buf[:n]); err != nil {
				f.Close()
				return err
			}
			off += n
		}
		if err = f.Close(); err != nil {
			return err
		}
		err = cm.PutFromFile(ctx, *path, staged, size, time.Now())
	case "small":
		data := make([]byte, size)
		fillPattern(data, 0)
		err = cm.Put(ctx, &cache.CacheEntry{FilePath: *path, Data: data})
	default:
		return fmt.Errorf("unknown layout %q", *layout)
	}
	if err != nil {
		return err
	}
	log.Printf("gen layout=%s path=%s size=%d chunk=%dMiB in %v", *layout, *path, size, *chunkMB, time.Since(start))
	return nil
}

// verify reads the whole file back through ReadRange and checks content.
func cmdVerify(args []string) error {
	fs := flag.NewFlagSet("verify", flag.ExitOnError)
	dir := fs.String("dir", "", "cache dir")
	path := fs.String("path", "/bench/model.bin", "cache path")
	size := fs.Int64("size", 0, "file size in bytes")
	chunkMB := fs.Int("chunk-mb", 16, "configured chunk size")
	strideMB := fs.Int("stride-mb", 0, "published chunk stride (0 = whole file)")
	fs.Parse(args)
	coord := coordinator.NewCoordinatorService()
	publish(coord, *path, *size, int64(*strideMB)<<20)
	cm, err := newCacheManager(cmOpts{dir: *dir, chunkMB: *chunkMB, coord: coord, localPeerID: "bench-local"})
	if err != nil {
		return err
	}
	defer cm.Shutdown()
	for off := int64(0); off < *size; off += 1 << 20 {
		b, err := cm.ReadRange(context.Background(), *path, off, 1<<20)
		if err != nil {
			return err
		}
		if err := checkPattern(b, off); err != nil {
			return err
		}
	}
	log.Printf("verify ok path=%s size=%d", *path, *size)
	return nil
}

func publish(coord coordinator.Coordinator, path string, size, stride int64) {
	loc := &coordinator.FileLocation{
		FilePath: path, PeerID: "bench-local", StorageTier: "nvme", StoragePath: path,
		FileSize: size, LastAccessed: time.Now(),
	}
	if stride > 0 {
		loc.IsChunked = true
		loc.ChunkSize = stride
	}
	_ = coord.UpdateFileLocation(context.Background(), loc)
}

// ---------------------------------------------------------------------------
// Workload engine (identical semantics in the Rust crate).

type workload struct {
	size, window int64
	pattern      string
	threads      int
	totalBytes   int64
	seed         uint64
}

type result struct {
	Impl       string  `json:"impl"`
	Layer      string  `json:"layer"`
	Pattern    string  `json:"pattern"`
	WindowKiB  int64   `json:"window_kib"`
	Threads    int     `json:"threads"`
	Bytes      int64   `json:"bytes"`
	Ops        int     `json:"ops"`
	Seconds    float64 `json:"seconds"`
	MBps       float64 `json:"mb_per_s"`
	P50us      float64 `json:"p50_us"`
	P99us      float64 `json:"p99_us"`
	CPUSecPerG float64 `json:"cpu_s_per_gib"`
	Note       string  `json:"note,omitempty"`
}

// offsets returns each thread's op offsets.
func (w workload) offsets() [][]int64 {
	out := make([][]int64, w.threads)
	perThread := w.totalBytes / int64(w.threads)
	ops := perThread / w.window
	if ops < 1 {
		ops = 1
	}
	for t := 0; t < w.threads; t++ {
		switch w.pattern {
		case "seq":
			region := w.size / int64(w.threads)
			start := int64(t) * region
			start -= start % w.window
			for i := int64(0); i < ops; i++ {
				off := start + i*w.window
				if off+w.window > start+region || off+w.window > w.size {
					break
				}
				out[t] = append(out[t], off)
			}
		default:
			slots := (w.size - w.window) / w.window
			s := w.seed + uint64(t)*0x9e3779b97f4a7c15
			for i := int64(0); i < ops; i++ {
				s = splitmix64(s)
				out[t] = append(out[t], int64(s%uint64(slots+1))*w.window)
			}
		}
	}
	return out
}

func cpuSeconds() float64 {
	var ru syscall.Rusage
	_ = syscall.Getrusage(syscall.RUSAGE_SELF, &ru)
	return tv(ru.Utime) + tv(ru.Stime)
}

func tv(t syscall.Timeval) float64 { return float64(t.Sec) + float64(t.Usec)/1e6 }

func runWorkload(w workload, impl, layer string, read func(off int64, n int) (int, error)) (result, error) {
	plan := w.offsets()
	lat := make([][]time.Duration, w.threads)
	var total atomic.Int64
	var firstErr atomic.Value
	cpu0 := cpuSeconds()
	start := time.Now()
	var wg sync.WaitGroup
	for t := 0; t < w.threads; t++ {
		wg.Add(1)
		go func(t int) {
			defer wg.Done()
			lat[t] = make([]time.Duration, 0, len(plan[t]))
			for _, off := range plan[t] {
				t0 := time.Now()
				n, err := read(off, int(w.window))
				if err != nil {
					firstErr.CompareAndSwap(nil, err)
					return
				}
				lat[t] = append(lat[t], time.Since(t0))
				total.Add(int64(n))
			}
		}(t)
	}
	wg.Wait()
	secs := time.Since(start).Seconds()
	cpu := cpuSeconds() - cpu0
	if e := firstErr.Load(); e != nil {
		return result{}, e.(error)
	}
	var all []time.Duration
	for _, l := range lat {
		all = append(all, l...)
	}
	sort.Slice(all, func(i, j int) bool { return all[i] < all[j] })
	pct := func(p float64) float64 {
		if len(all) == 0 {
			return 0
		}
		idx := int(math.Ceil(p*float64(len(all)))) - 1
		if idx < 0 {
			idx = 0
		}
		return float64(all[idx].Nanoseconds()) / 1e3
	}
	b := total.Load()
	return result{
		Impl: impl, Layer: layer, Pattern: w.pattern, WindowKiB: w.window >> 10, Threads: w.threads,
		Bytes: b, Ops: len(all), Seconds: secs, MBps: float64(b) / (1 << 20) / secs,
		P50us: pct(0.50), P99us: pct(0.99), CPUSecPerG: cpu / (float64(b) / (1 << 30)),
	}, nil
}

func emit(r result) {
	b, _ := json.Marshal(r)
	fmt.Println(string(b))
}

// ---------------------------------------------------------------------------
// bench: in-process ReadRange

func cmdBench(args []string) error {
	fs := flag.NewFlagSet("bench", flag.ExitOnError)
	dir := fs.String("dir", "", "cache dir")
	path := fs.String("path", "/bench/model.bin", "cache path")
	size := fs.Int64("size", 0, "file size in bytes")
	strideMB := fs.Int("stride-mb", 16, "published chunk stride (0 = unchunked file)")
	chunkMB := fs.Int("chunk-mb", 16, "configured -chunk-size")
	windowKiB := fs.Int64("window-kib", 128, "read window")
	pattern := fs.String("pattern", "seq", "seq|rand")
	threads := fs.Int("threads", 1, "concurrent readers")
	totalMB := fs.Int64("total-mb", 1024, "bytes to read per run")
	whole := fs.Bool("whole-file-reads", false, "-range-whole-file-reads")
	verify := fs.Bool("verify", false, "check content of every read")
	cpuProf := fs.String("cpuprofile", "", "write CPU profile")
	memProf := fs.String("memprofile", "", "write alloc profile")
	fs.Parse(args)
	restore := hideJournal(*dir)
	defer restore()

	coord := coordinator.NewCoordinatorService()
	publish(coord, *path, *size, int64(*strideMB)<<20)
	cm, err := newCacheManager(cmOpts{dir: *dir, chunkMB: *chunkMB, wholeFile: *whole, coord: coord, localPeerID: "bench-local"})
	if err != nil {
		return err
	}
	defer cm.Shutdown()
	ctx := context.Background()
	w := workload{size: *size, window: *windowKiB << 10, pattern: *pattern, threads: *threads, totalBytes: *totalMB << 20, seed: 42}

	if *memProf != "" {
		runtime.MemProfileRate = 64 * 1024
	}
	if *cpuProf != "" {
		f, err := os.Create(*cpuProf)
		if err != nil {
			return err
		}
		pprof.StartCPUProfile(f)
		defer func() { pprof.StopCPUProfile(); f.Close() }()
	}
	var gcBefore runtime.MemStats
	runtime.ReadMemStats(&gcBefore)
	res, err := runWorkload(w, "go", "library", func(off int64, n int) (int, error) {
		b, err := cm.ReadRange(ctx, *path, off, n)
		if err != nil {
			return 0, err
		}
		if *verify {
			if err := checkPattern(b, off); err != nil {
				return 0, err
			}
		}
		return len(b), nil
	})
	if err != nil {
		return err
	}
	var gcAfter runtime.MemStats
	runtime.ReadMemStats(&gcAfter)
	res.Note = fmt.Sprintf("alloc_bytes_per_byte=%.2f gc_cycles=%d whole_file_reads=%v",
		float64(gcAfter.TotalAlloc-gcBefore.TotalAlloc)/float64(res.Bytes), gcAfter.NumGC-gcBefore.NumGC, *whole)
	emit(res)
	if *memProf != "" {
		f, err := os.Create(*memProf)
		if err != nil {
			return err
		}
		pprof.Lookup("allocs").WriteTo(f, 0)
		f.Close()
	}
	return nil
}

// hideJournal moves the durability journal aside for the run. On startup the
// cache manager resumes journalled uploads, which here would mean re-reading
// chunk files to push them at a dead endpoint in the background, polluting
// the measurement. Shape comes from the coordinator instead (as in prod).
func hideJournal(dir string) func() {
	j := filepath.Join(dir, ".fuse-durability")
	hidden := j + ".readbench-hidden"
	if err := os.Rename(j, hidden); err != nil {
		return func() {}
	}
	done := make(chan struct{})
	restore := func() {
		select {
		case <-done:
			return
		default:
			close(done)
		}
		os.RemoveAll(j) // anything the run itself journalled
		os.Rename(hidden, j)
	}
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
	go func() {
		select {
		case <-sig:
			restore()
			os.Exit(130)
		case <-done:
		}
	}()
	return restore
}

// ---------------------------------------------------------------------------
// coordinator (in-process service + minimal HTTP surface)

type pubFlag []string

func (p *pubFlag) String() string     { return strings.Join(*p, ",") }
func (p *pubFlag) Set(v string) error { *p = append(*p, v); return nil }

// parsePub parses path:size:strideMiB[:peerID].
func parsePub(coord coordinator.Coordinator, specs []string) error {
	for _, s := range specs {
		parts := strings.Split(s, ":")
		if len(parts) < 3 {
			return fmt.Errorf("bad -publish %q (want path:size:strideMiB[:peer])", s)
		}
		var size, stride int64
		fmt.Sscan(parts[1], &size)
		fmt.Sscan(parts[2], &stride)
		peer := "bench-local"
		if len(parts) > 3 {
			peer = parts[3]
		}
		loc := &coordinator.FileLocation{FilePath: parts[0], PeerID: peer, StorageTier: "nvme",
			StoragePath: parts[0], FileSize: size, LastAccessed: time.Now()}
		if stride > 0 {
			loc.IsChunked = true
			loc.ChunkSize = stride << 20
		}
		if err := coord.UpdateFileLocation(context.Background(), loc); err != nil {
			return err
		}
	}
	return nil
}

func coordHandler(coord coordinator.Coordinator) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/files/location", func(w http.ResponseWriter, r *http.Request) {
		locs, err := coord.GetFileLocation(r.Context(), r.URL.Query().Get("path"))
		if err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(locs)
	})
	mux.HandleFunc("/api/peers", func(w http.ResponseWriter, r *http.Request) {
		peers, err := coord.GetPeers(r.Context(), r.URL.Query().Get("requester_id"))
		if err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(peers)
	})
	return mux
}

func cmdCoord(args []string) error {
	fs := flag.NewFlagSet("coord", flag.ExitOnError)
	listen := fs.String("listen", "127.0.0.1:18080", "listen address")
	var pubs, peers pubFlag
	fs.Var(&pubs, "publish", "path:size:strideMiB[:peer] (repeatable)")
	fs.Var(&peers, "peer", "id=httpAddr (repeatable)")
	fs.Parse(args)
	coord := coordinator.NewCoordinatorService()
	if err := parsePub(coord, pubs); err != nil {
		return err
	}
	for _, p := range peers {
		id, addr, _ := strings.Cut(p, "=")
		coord.RegisterPeer(context.Background(), &coordinator.PeerInfo{ID: id, Address: addr, GRPCAddress: addr, Status: "active"})
	}
	log.Printf("coord listening on %s", *listen)
	return http.ListenAndServe(*listen, coordHandler(coord))
}

// ---------------------------------------------------------------------------
// serve-peer: the real API handler (incl. /api/peer/read)

func cmdServePeer(args []string) error {
	fs := flag.NewFlagSet("serve-peer", flag.ExitOnError)
	dir := fs.String("dir", "", "cache dir")
	listen := fs.String("listen", "127.0.0.1:18081", "listen address")
	chunkMB := fs.Int("chunk-mb", 16, "configured -chunk-size")
	apiKey := fs.String("api-key", "", "X-API-Key")
	var pubs pubFlag
	fs.Var(&pubs, "publish", "path:size:strideMiB (repeatable) so LocalChunkFile knows chunked parents")
	fs.Parse(args)
	restore := hideJournal(*dir)
	defer restore()
	coord := coordinator.NewCoordinatorService()
	if err := parsePub(coord, pubs); err != nil {
		return err
	}
	cm, err := newCacheManager(cmOpts{dir: *dir, chunkMB: *chunkMB, coord: coord, localPeerID: "bench-local"})
	if err != nil {
		return err
	}
	defer cm.Shutdown()
	// LocalChunkFile needs the parent row to say IsChunked. On a restarted
	// node that comes from resolveChunkedEntry (coordinator); prime it the
	// same way a first read would.
	for _, p := range pubs {
		path := strings.Split(p, ":")[0]
		cm.ReadRange(context.Background(), path, 0, 1)
	}
	h := api.NewHandler(cm, coord, "bench-local", *apiKey)
	srv := &http.Server{Addr: *listen, Handler: h.SetupRoutes()}
	log.Printf("go peer server on %s dir=%s", *listen, *dir)
	return srv.ListenAndServe()
}

// ---------------------------------------------------------------------------
// bench-peer: real PeerStorage (raw HTTP transport) fetching chunk objects

func cmdBenchPeer(args []string) error {
	fs := flag.NewFlagSet("bench-peer", flag.ExitOnError)
	addr := fs.String("addr", "127.0.0.1:18081", "peer HTTP address")
	path := fs.String("path", "/bench/model.bin", "parent path")
	size := fs.Int64("size", 0, "file size")
	strideMB := fs.Int64("stride-mb", 16, "chunk stride")
	threads := fs.Int("threads", 1, "concurrent chunk fetches")
	totalMB := fs.Int64("total-mb", 2048, "bytes to fetch")
	verify := fs.Bool("verify", false, "check content")
	apiKey := fs.String("api-key", "", "X-API-Key")
	label := fs.String("server", "?", "server impl label")
	fs.Parse(args)
	coord := coordinator.NewCoordinatorService()
	coord.RegisterPeer(context.Background(), &coordinator.PeerInfo{ID: "srv", Address: *addr, GRPCAddress: *addr, Status: "active"})
	publish(coord, *path, *size, *strideMB<<20)
	// The location row must name the serving peer for partitioning.
	coord.UpdateFileLocation(context.Background(), &coordinator.FileLocation{FilePath: *path, PeerID: "srv", StorageTier: "nvme",
		StoragePath: *path, FileSize: *size, IsChunked: true, ChunkSize: *strideMB << 20, LastAccessed: time.Now()})
	ps, err := cache.NewPeerStorage(coord, 30*time.Second, "bench-client", true, true, true, *apiKey)
	if err != nil {
		return err
	}
	stride := *strideMB << 20
	nChunks := (*size + stride - 1) / stride
	w := workload{size: nChunks * stride, window: stride, pattern: "seq", threads: *threads, totalBytes: *totalMB << 20}
	ctx := context.Background()
	res, err := runWorkload(w, "go", "peer-client", func(off int64, n int) (int, error) {
		idx := off / stride
		b, err := ps.Read(ctx, fmt.Sprintf("%s_chunk_%d", *path, idx))
		if err != nil {
			return 0, err
		}
		if *verify {
			if err := checkPattern(b, idx*stride); err != nil {
				return 0, err
			}
		}
		return len(b), nil
	})
	if err != nil {
		return err
	}
	res.Pattern = "chunk"
	res.Note = "server=" + *label
	emit(res)
	return nil
}
