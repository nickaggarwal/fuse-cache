package cache

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
)

// TestAzureReadBench measures AzureStorage.Read on chunk objects (the
// first-range-then-parallel path cold reads take) against a configurable
// endpoint, for comparison with the Rust reader in rust/fusedata. Opt-in:
//
//	FUSE_AZBENCH_ENDPOINT=http://127.0.0.1:10000/devstoreaccount1/ \
//	FUSE_AZBENCH_ACCOUNT=devstoreaccount1 FUSE_AZBENCH_KEY=... \
//	FUSE_AZBENCH_PATH=/bench/az.bin FUSE_AZBENCH_SIZE=268435456 \
//	go test ./internal/cache -run TestAzureReadBench -v
//
// The struct is built in-package because NewAzureStorage pins the public
// *.blob.core.windows.net endpoint; every other field matches the client's
// defaults (-azure-download-concurrency 8, -azure-download-block-size-mb 4).
// Emits one JSON row in the same shape as tools/readbench.
func TestAzureReadBench(t *testing.T) {
	endpoint := os.Getenv("FUSE_AZBENCH_ENDPOINT")
	if endpoint == "" {
		t.Skip("FUSE_AZBENCH_ENDPOINT not set")
	}
	account, key := os.Getenv("FUSE_AZBENCH_ACCOUNT"), os.Getenv("FUSE_AZBENCH_KEY")
	container := envOr("FUSE_AZBENCH_CONTAINER", "fuse-cache")
	path := envOr("FUSE_AZBENCH_PATH", "/bench/az.bin")
	size, _ := strconv.ParseInt(os.Getenv("FUSE_AZBENCH_SIZE"), 10, 64)
	stride := int64(16 << 20)
	threads, _ := strconv.Atoi(envOr("FUSE_AZBENCH_THREADS", "1"))
	verify := os.Getenv("FUSE_AZBENCH_VERIFY") != ""

	cred, err := azblob.NewSharedKeyCredential(account, key)
	if err != nil {
		t.Fatal(err)
	}
	client, err := azblob.NewClientWithSharedKeyCredential(endpoint, cred, nil)
	if err != nil {
		t.Fatal(err)
	}
	as := &AzureStorage{
		client: client, containerName: container, timeout: 60 * time.Second,
		downloadConcurrency: 8, downloadBlockSize: 4 << 20, parallelDownloadMinBytes: 8 << 20,
	}
	n := (size + stride - 1) / stride
	jobs := make(chan int64, n)
	for i := int64(0); i < n; i++ {
		jobs <- i
	}
	close(jobs)
	var total atomic.Int64
	var mu sync.Mutex
	var lats []time.Duration
	var firstErr error
	cpu0 := rusageCPU()
	start := time.Now()
	var wg sync.WaitGroup
	for w := 0; w < threads; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range jobs {
				t0 := time.Now()
				b, err := as.Read(context.Background(), fmt.Sprintf("%s_chunk_%d", path, i))
				d := time.Since(t0)
				mu.Lock()
				if err != nil && firstErr == nil {
					firstErr = err
				}
				lats = append(lats, d)
				mu.Unlock()
				if err == nil && verify {
					if bad := firstMismatch(b, i*stride); bad >= 0 {
						mu.Lock()
						firstErr = fmt.Errorf("content mismatch at %d", bad)
						mu.Unlock()
					}
				}
				total.Add(int64(len(b)))
			}
		}()
	}
	wg.Wait()
	secs := time.Since(start).Seconds()
	cpu := rusageCPU() - cpu0
	if firstErr != nil {
		t.Fatal(firstErr)
	}
	sort.Slice(lats, func(i, j int) bool { return lats[i] < lats[j] })
	pct := func(p float64) float64 {
		idx := int(math.Ceil(p*float64(len(lats)))) - 1
		if idx < 0 {
			idx = 0
		}
		return float64(lats[idx].Nanoseconds()) / 1e3
	}
	b := total.Load()
	row, _ := json.Marshal(map[string]any{
		"impl": "go", "layer": "azure", "pattern": "chunk", "window_kib": stride >> 10, "threads": threads,
		"bytes": b, "ops": len(lats), "seconds": secs, "mb_per_s": float64(b) / (1 << 20) / secs,
		"p50_us": pct(0.5), "p99_us": pct(0.99), "cpu_s_per_gib": cpu / (float64(b) / (1 << 30)),
	})
	fmt.Println(string(row))
}

func envOr(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}

func rusageCPU() float64 {
	var ru syscall.Rusage
	_ = syscall.Getrusage(syscall.RUSAGE_SELF, &ru)
	return float64(ru.Utime.Sec+ru.Stime.Sec) + float64(ru.Utime.Usec+ru.Stime.Usec)/1e6
}

// firstMismatch checks readbench's deterministic pattern (byte o is byte
// o%8 of splitmix64(o/8)); -1 when buf matches.
func firstMismatch(buf []byte, off int64) int64 {
	mix := func(z uint64) uint64 {
		z += 0x9e3779b97f4a7c15
		z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9
		z = (z ^ (z >> 27)) * 0x94d049bb133111eb
		return z ^ (z >> 31)
	}
	for i := range buf {
		o := uint64(off) + uint64(i)
		if buf[i] != byte(mix(o>>3)>>(8*(o&7))) {
			return int64(o)
		}
	}
	return -1
}
