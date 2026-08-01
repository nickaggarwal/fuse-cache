package cache

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
)

// fakeBlobServer emulates just enough of the Blob REST surface for ranged
// GETs: parses Range headers, returns 206 with Content-Range, counts calls.
type fakeBlobServer struct {
	data     []byte
	getCalls atomic.Int64
	headers  atomic.Int64
}

func (f *fakeBlobServer) handler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodHead {
			f.headers.Add(1)
			w.Header().Set("Content-Length", strconv.Itoa(len(f.data)))
			w.WriteHeader(http.StatusOK)
			return
		}
		if r.Method != http.MethodGet {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		f.getCalls.Add(1)
		start, end := int64(0), int64(len(f.data))-1
		if rng := r.Header.Get("x-ms-range"); rng != "" {
			var s, e int64
			if n, _ := fmt.Sscanf(rng, "bytes=%d-%d", &s, &e); n == 2 {
				start, end = s, e
			} else if n, _ := fmt.Sscanf(rng, "bytes=%d-", &s); n == 1 {
				start = s
			}
		}
		if end >= int64(len(f.data)) {
			end = int64(len(f.data)) - 1
		}
		body := f.data[start : end+1]
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(f.data)))
		w.Header().Set("Content-Length", strconv.Itoa(len(body)))
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Header().Set("Accept-Ranges", "bytes")
		w.Header().Set("ETag", `"fake"`)
		w.Header().Set("Last-Modified", time.Now().UTC().Format(http.TimeFormat))
		w.WriteHeader(http.StatusPartialContent)
		w.Write(body)
	}
}

func newFakeAzure(t *testing.T, payload []byte, blockSize int64, concurrency int) (*AzureStorage, *fakeBlobServer, func()) {
	t.Helper()
	fake := &fakeBlobServer{data: payload}
	srv := httptest.NewServer(fake.handler())
	client, err := azblob.NewClientWithNoCredential(srv.URL+"/", nil)
	if err != nil {
		srv.Close()
		t.Fatalf("NewClientWithNoCredential: %v", err)
	}
	as := &AzureStorage{
		client:                   client,
		containerName:            "c",
		timeout:                  30 * time.Second,
		downloadConcurrency:      uint16(concurrency),
		downloadBlockSize:        blockSize,
		parallelDownloadMinBytes: 0,
	}
	return as, fake, srv.Close
}

// TestAzureChunkRead_SingleRoundTripForSmallBlob: a chunk blob at or under
// one block downloads with exactly one GET and no HEAD.
func TestAzureChunkRead_SingleRoundTripForSmallBlob(t *testing.T) {
	payload := []byte(strings.Repeat("a", 3*1024*1024)) // 3MB < 4MB block
	as, fake, done := newFakeAzure(t, payload, 4*1024*1024, 8)
	defer done()

	got, err := as.Read(context.Background(), "/f_chunk_0")
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if string(got) != string(payload) {
		t.Fatalf("payload mismatch: %d vs %d bytes", len(got), len(payload))
	}
	if calls := fake.getCalls.Load(); calls != 1 {
		t.Fatalf("GET calls = %d, want exactly 1", calls)
	}
	if heads := fake.headers.Load(); heads != 0 {
		t.Fatalf("HEAD calls = %d, want 0", heads)
	}
}

// TestAzureChunkRead_ParallelForLargeBlob: an 8MB chunk with 4MB blocks uses
// the first-range GET plus parallel ranged GETs for the remainder, and
// reassembles byte-exactly.
func TestAzureChunkRead_ParallelForLargeBlob(t *testing.T) {
	payload := make([]byte, 8*1024*1024)
	for i := range payload {
		payload[i] = byte(i % 249)
	}
	as, fake, done := newFakeAzure(t, payload, 2*1024*1024, 4)
	defer done()

	got, err := as.Read(context.Background(), "/f_chunk_3")
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if string(got) != string(payload) {
		t.Fatal("reassembled payload differs from source")
	}
	// First range + ceil(6MB/2MB)=3 remainder blocks = 4 GETs.
	if calls := fake.getCalls.Load(); calls != 4 {
		t.Fatalf("GET calls = %d, want 4 (1 first-range + 3 parallel)", calls)
	}
	if heads := fake.headers.Load(); heads != 0 {
		t.Fatalf("HEAD calls = %d, want 0 (size from Content-Range)", heads)
	}
}

// TestAzureChunkRead_ExactBlockBoundary: blob exactly one block long must not
// issue a remainder download.
func TestAzureChunkRead_ExactBlockBoundary(t *testing.T) {
	payload := []byte(strings.Repeat("b", 4*1024*1024))
	as, fake, done := newFakeAzure(t, payload, 4*1024*1024, 8)
	defer done()

	got, err := as.Read(context.Background(), "/f_chunk_1")
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if len(got) != len(payload) {
		t.Fatalf("len = %d, want %d", len(got), len(payload))
	}
	if calls := fake.getCalls.Load(); calls != 1 {
		t.Fatalf("GET calls = %d, want 1 for exact-block blob", calls)
	}
}
