package cache

import (
	"context"
	"io"
	"log"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	pb "fuse-client/internal/pb"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

// benchCacheManagerWithWholeFile builds a DefaultCacheManager backed by a real
// on-disk NVMe store holding a single whole file (the FUSE-write storage
// model), with a parent chunked entry but no per-chunk files. Returns the
// manager, the parent path, and the number of chunks.
func benchCacheManagerWithWholeFile(tb testing.TB, chunkSize int64, numChunks int64) (*DefaultCacheManager, string, int64) {
	tb.Helper()
	dir := tb.TempDir()
	nvme, err := NewNVMeStorage(dir)
	if err != nil {
		tb.Fatalf("NewNVMeStorage: %v", err)
	}

	parent := "/bench-whole.bin"
	total := chunkSize * numChunks
	// Write a whole file directly to the NVMe base path.
	f, err := os.Create(filepath.Join(dir, "bench-whole.bin"))
	if err != nil {
		tb.Fatalf("create whole file: %v", err)
	}
	block := make([]byte, chunkSize)
	for i := range block {
		block[i] = byte(i)
	}
	for i := int64(0); i < numChunks; i++ {
		if _, err := f.Write(block); err != nil {
			tb.Fatalf("write block: %v", err)
		}
	}
	f.Close()

	cm := &DefaultCacheManager{
		config: &CacheConfig{
			NVMePath:  dir,
			ChunkSize: chunkSize,
		},
		nvmeStorage:  nvme,
		peerStorage:  newMockStorage(),
		cloudStorage: newMockStorage(),
		entries:      make(map[string]*CacheEntry),
		logger:       log.New(io.Discard, "", 0),
		metrics:      NewCacheMetrics(),
		rangeChunks:  make(map[string]*chunkFileCache),
		hybridHints:  make(map[string]hybridReadHint),
		tierPerf:     newTierPerfTracker(),
	}
	cm.entries[parent] = &CacheEntry{
		FilePath:  parent,
		Size:      total,
		IsChunked: true,
		NumChunks: numChunks,
	}
	return cm, parent, numChunks
}

// BenchmarkPeerServe_Synthesis measures only the server-side chunk synthesis
// cost (GetLocal -> readChunkFromWholeLocalFile), i.e. the allocation/CPU of
// producing one chunk's bytes from the whole parent file.
func BenchmarkPeerServe_Synthesis(b *testing.B) {
	const chunkSize = 16 * 1024 * 1024
	cm, parent, numChunks := benchCacheManagerWithWholeFile(b, chunkSize, 8)
	ctx := context.Background()

	b.SetBytes(chunkSize)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		chunkPath := parent + "_chunk_" + itoa(int(int64(i)%numChunks))
		entry, err := cm.GetLocal(ctx, chunkPath)
		if err != nil {
			b.Fatalf("GetLocal: %v", err)
		}
		if int64(len(entry.Data)) != chunkSize {
			b.Fatalf("chunk len = %d, want %d", len(entry.Data), chunkSize)
		}
	}
}

// startBufconnPeerServer spins up an in-process PeerService over bufconn (no
// network) and returns a connected client plus a cleanup func.
func startBufconnPeerServer(tb testing.TB, cm CacheManager) (pb.PeerServiceClient, func()) {
	tb.Helper()
	lis := bufconn.Listen(64 * 1024 * 1024)
	srv := grpc.NewServer(
		grpc.MaxRecvMsgSize(peerGRPCMaxMessageBytes),
		grpc.MaxSendMsgSize(peerGRPCMaxMessageBytes),
	)
	pb.RegisterPeerServiceServer(srv, NewPeerGRPCServer(cm))
	go func() { _ = srv.Serve(lis) }()

	conn, err := grpc.DialContext(
		context.Background(),
		"bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(peerGRPCMaxMessageBytes),
			grpc.MaxCallSendMsgSize(peerGRPCMaxMessageBytes),
		),
	)
	if err != nil {
		tb.Fatalf("grpc.DialContext: %v", err)
	}
	return pb.NewPeerServiceClient(conn), func() {
		conn.Close()
		srv.Stop()
		lis.Close()
	}
}

// BenchmarkPeerServe_GRPCReadFile measures the full server path a peer chunk
// read takes: ReadFile -> synthesis -> protobuf marshal -> stream, plus client
// receive, all in-process (no network) so the numbers isolate CPU/alloc/copy
// overhead from VNet latency.
func BenchmarkPeerServe_GRPCReadFile(b *testing.B) {
	const chunkSize = 16 * 1024 * 1024
	cm, parent, numChunks := benchCacheManagerWithWholeFile(b, chunkSize, 8)
	client, cleanup := startBufconnPeerServer(b, cm)
	defer cleanup()

	b.SetBytes(chunkSize)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		chunkPath := parent + "_chunk_" + itoa(int(int64(i)%numChunks))
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		stream, err := client.ReadFile(ctx, &pb.ReadFileRequest{Path: chunkPath})
		if err != nil {
			cancel()
			b.Fatalf("ReadFile: %v", err)
		}
		var got int
		for {
			msg, err := stream.Recv()
			if err == io.EOF {
				break
			}
			if err != nil {
				cancel()
				b.Fatalf("Recv: %v", err)
			}
			got += len(msg.Data)
		}
		cancel()
		if got != chunkSize {
			b.Fatalf("received %d bytes, want %d", got, chunkSize)
		}
	}
}

// TestPeerServe_GRPCReadFile_CorrectBytes verifies the pooled-buffer range
// streaming path serves the exact bytes of each chunk from the whole parent
// file (not just the right length), across the full-size and short final chunk.
func TestPeerServe_GRPCReadFile_CorrectBytes(t *testing.T) {
	const chunkSize = 3
	dir := t.TempDir()
	nvme, err := NewNVMeStorage(dir)
	if err != nil {
		t.Fatalf("NewNVMeStorage: %v", err)
	}
	parent := "/bytes.bin"
	content := []byte("ABCDEFGHIJ") // 10 bytes, chunk size 3 => ABC DEF GHI J
	if err := os.WriteFile(filepath.Join(dir, "bytes.bin"), content, 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}
	cm := &DefaultCacheManager{
		config:       &CacheConfig{NVMePath: dir, ChunkSize: chunkSize},
		nvmeStorage:  nvme,
		peerStorage:  newMockStorage(),
		cloudStorage: newMockStorage(),
		entries:      make(map[string]*CacheEntry),
		logger:       log.New(io.Discard, "", 0),
		metrics:      NewCacheMetrics(),
		rangeChunks:  make(map[string]*chunkFileCache),
		hybridHints:  make(map[string]hybridReadHint),
		tierPerf:     newTierPerfTracker(),
	}
	cm.entries[parent] = &CacheEntry{FilePath: parent, Size: int64(len(content)), IsChunked: true, NumChunks: 4}

	client, cleanup := startBufconnPeerServer(t, cm)
	defer cleanup()

	want := []string{"ABC", "DEF", "GHI", "J"}
	for i, w := range want {
		stream, err := client.ReadFile(context.Background(), &pb.ReadFileRequest{Path: parent + "_chunk_" + itoa(i)})
		if err != nil {
			t.Fatalf("ReadFile chunk %d: %v", i, err)
		}
		var got []byte
		for {
			msg, err := stream.Recv()
			if err == io.EOF {
				break
			}
			if err != nil {
				t.Fatalf("Recv chunk %d: %v", i, err)
			}
			got = append(got, msg.Data...)
		}
		if string(got) != w {
			t.Fatalf("chunk %d = %q, want %q", i, string(got), w)
		}
	}
}
