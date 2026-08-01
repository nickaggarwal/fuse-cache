package cache

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"
)

// startBenchPeerHTTP serves `size` deterministic bytes at /api/peer/read the
// way a real peer does (Content-Length set, sendfile-eligible shape).
func startBenchPeerHTTP(tb testing.TB, size int) (addr string, cleanup func()) {
	tb.Helper()
	payload := make([]byte, size)
	for i := range payload {
		payload[i] = byte(i)
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", strconv.Itoa(len(payload)))
		w.WriteHeader(http.StatusOK)
		w.Write(payload)
	}))
	return strings.TrimPrefix(srv.URL, "http://"), srv.Close
}

// BenchmarkPeerReceive_BufferedThenPromote measures the OLD receive path:
// read the full body into memory, then promote via a separate full-object
// NVMe write (what the background promoteToNVMe/promoteChunkAndAdvertise
// goroutine did before streaming promotion). Timing includes the promote
// write because that work still happens per fetched chunk, just off the
// caller's critical path.
func BenchmarkPeerReceive_BufferedThenPromote(b *testing.B) {
	const size = 16 * 1024 * 1024
	addr, cleanup := startBenchPeerHTTP(b, size)
	defer cleanup()
	cm, _ := newStreamPromoteManager(b, 1<<40)
	ps := newRawPeerStorage()
	// promoteSink nil => plain buffered read.

	ctx := context.Background()
	b.SetBytes(size)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		path := "/bench.bin_chunk_" + itoa(i%16)
		data, err := ps.readFromPeerRaw(ctx, addr, path)
		if err != nil {
			b.Fatalf("read: %v", err)
		}
		// Old-path promotion: second full write of the same bytes.
		entry := &CacheEntry{
			FilePath: path, StoragePath: path,
			Size: int64(len(data)), LastAccessed: time.Now(),
			Tier: TierNVMe, Data: data,
		}
		if err := cm.putToNVMeWithEviction(ctx, entry); err != nil {
			b.Fatalf("promote: %v", err)
		}
	}
}

// BenchmarkPeerReceive_StreamingTee measures the NEW receive path: the tee
// writes bytes to NVMe as they arrive; when the read returns, promotion is
// already durable — no second write.
func BenchmarkPeerReceive_StreamingTee(b *testing.B) {
	const size = 16 * 1024 * 1024
	addr, cleanup := startBenchPeerHTTP(b, size)
	defer cleanup()
	cm, _ := newStreamPromoteManager(b, 1<<40)
	ps := newRawPeerStorage()
	ps.promoteSink = cm

	ctx := context.Background()
	b.SetBytes(size)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		path := "/bench.bin_chunk_" + itoa(i%16)
		// Delete the previous round's landing so BeginPromotion admits again
		// (steady-state fetches are of chunks not yet local). Cheap unlink,
		// same for both benchmarks' fairness since the old path overwrites.
		cm.mu.Lock()
		if e := cm.entries[path]; e != nil {
			cm.nvmeUsed -= e.Size
			delete(cm.entries, path)
		}
		cm.mu.Unlock()
		cm.nvmeStorage.Delete(ctx, path)

		data, err := ps.readFromPeerRaw(ctx, addr, path)
		if err != nil {
			b.Fatalf("read: %v", err)
		}
		if len(data) != size {
			b.Fatalf("short read: %d", len(data))
		}
		// Promotion already landed by the tee; the background path is a no-op.
		if !cm.alreadyLocalNVMe(ctx, path, int64(len(data))) {
			b.Fatal("tee did not land the object")
		}
	}
}
