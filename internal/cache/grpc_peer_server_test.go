package cache

import (
	"bytes"
	"context"
	"io"
	"log"
	"net"
	"testing"
	"time"

	pb "fuse-client/internal/pb"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func startTestPeerGRPCServer(t *testing.T) (pb.PeerServiceClient, *DefaultCacheManager, func()) {
	t.Helper()

	cm := &DefaultCacheManager{
		config: &CacheConfig{
			NVMePath:           "/tmp/test-grpc",
			MaxNVMeSize:        10 * 1024 * 1024,
			ChunkSize:          4 * 1024 * 1024,
			CloudRetryCount:    1,
			CloudRetryBaseWait: time.Millisecond,
			MinPeerReplicas:    3,
			CloudTimeout:       5 * time.Second,
		},
		nvmeStorage:  newMockStorage(),
		peerStorage:  newMockStorage(),
		cloudStorage: newMockStorage(),
		entries:      make(map[string]*CacheEntry),
		logger:       log.New(log.Writer(), "[GRPC-TEST] ", log.LstdFlags),
		metrics:      NewCacheMetrics(),
	}

	srv := grpc.NewServer()
	pb.RegisterPeerServiceServer(srv, NewPeerGRPCServer(cm))

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}
	go srv.Serve(lis)

	conn, err := grpc.Dial(lis.Addr().String(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		srv.Stop()
		t.Fatalf("failed to dial: %v", err)
	}

	client := pb.NewPeerServiceClient(conn)
	cleanup := func() {
		conn.Close()
		srv.Stop()
	}
	return client, cm, cleanup
}

func TestPeerGRPC_WriteAndReadFile(t *testing.T) {
	client, _, cleanup := startTestPeerGRPCServer(t)
	defer cleanup()

	ctx := context.Background()
	data := []byte("hello gRPC world, this is test data for streaming")

	// Write file via client streaming
	stream, err := client.WriteFile(ctx)
	if err != nil {
		t.Fatalf("WriteFile stream: %v", err)
	}

	// Send metadata + first chunk
	if err := stream.Send(&pb.WriteFileRequest{
		Path:      "/grpc-test.txt",
		TotalSize: int64(len(data)),
		Data:      data[:20],
	}); err != nil {
		t.Fatalf("Send metadata: %v", err)
	}

	// Send remaining data
	if err := stream.Send(&pb.WriteFileRequest{
		Data: data[20:],
	}); err != nil {
		t.Fatalf("Send data: %v", err)
	}

	resp, err := stream.CloseAndRecv()
	if err != nil {
		t.Fatalf("CloseAndRecv: %v", err)
	}
	if resp.BytesWritten != int64(len(data)) {
		t.Errorf("bytes_written = %d, want %d", resp.BytesWritten, len(data))
	}

	// Read file via server streaming
	readStream, err := client.ReadFile(ctx, &pb.ReadFileRequest{Path: "/grpc-test.txt"})
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}

	var buf bytes.Buffer
	for {
		chunk, err := readStream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("Recv: %v", err)
		}
		buf.Write(chunk.Data)
	}

	if !bytes.Equal(buf.Bytes(), data) {
		t.Errorf("read data = %q, want %q", buf.String(), string(data))
	}
}

func TestPeerGRPC_FileExists(t *testing.T) {
	client, cm, cleanup := startTestPeerGRPCServer(t)
	defer cleanup()

	ctx := context.Background()

	// File doesn't exist yet
	resp, err := client.FileExists(ctx, &pb.FileExistsRequest{Path: "/missing.txt"})
	if err != nil {
		t.Fatalf("FileExists: %v", err)
	}
	if resp.Exists {
		t.Error("file should not exist")
	}

	// Put a file directly
	cm.Put(ctx, &CacheEntry{
		FilePath:     "/exists.txt",
		Size:         5,
		LastAccessed: time.Now(),
		Data:         []byte("hello"),
	})

	resp, err = client.FileExists(ctx, &pb.FileExistsRequest{Path: "/exists.txt"})
	if err != nil {
		t.Fatalf("FileExists: %v", err)
	}
	if !resp.Exists {
		t.Error("file should exist")
	}
}

func TestPeerGRPC_FileSize(t *testing.T) {
	client, cm, cleanup := startTestPeerGRPCServer(t)
	defer cleanup()

	ctx := context.Background()

	cm.Put(ctx, &CacheEntry{
		FilePath:     "/sized.txt",
		Size:         42,
		LastAccessed: time.Now(),
		Data:         make([]byte, 42),
	})

	resp, err := client.FileSize(ctx, &pb.FileSizeRequest{Path: "/sized.txt"})
	if err != nil {
		t.Fatalf("FileSize: %v", err)
	}
	if resp.Size != 42 {
		t.Errorf("size = %d, want 42", resp.Size)
	}
}

func TestPeerGRPC_DeleteFile(t *testing.T) {
	client, cm, cleanup := startTestPeerGRPCServer(t)
	defer cleanup()

	ctx := context.Background()

	// Write directly to NVMe only (no background cloud persist)
	cm.nvmeStorage.Write(ctx, "/delete-me.txt", []byte("hello"))
	cm.mu.Lock()
	cm.entries["/delete-me.txt"] = &CacheEntry{
		FilePath:     "/delete-me.txt",
		Size:         5,
		LastAccessed: time.Now(),
		Tier:         TierNVMe,
		Data:         []byte("hello"),
	}
	cm.mu.Unlock()

	_, err := client.DeleteFile(ctx, &pb.DeleteFileRequest{Path: "/delete-me.txt"})
	if err != nil {
		t.Fatalf("DeleteFile: %v", err)
	}

	// Verify it's gone from the cache entries
	cm.mu.RLock()
	_, exists := cm.entries["/delete-me.txt"]
	cm.mu.RUnlock()
	if exists {
		t.Error("file should be deleted from cache entries")
	}
}

func TestPeerGRPC_ReadLargeFile(t *testing.T) {
	client, cm, cleanup := startTestPeerGRPCServer(t)
	defer cleanup()

	ctx := context.Background()

	// Create data larger than 64KB chunk size
	data := make([]byte, 256*1024) // 256KB
	for i := range data {
		data[i] = byte(i % 256)
	}

	cm.Put(ctx, &CacheEntry{
		FilePath:     "/large.bin",
		Size:         int64(len(data)),
		LastAccessed: time.Now(),
		Data:         data,
	})

	// Read via streaming
	stream, err := client.ReadFile(ctx, &pb.ReadFileRequest{Path: "/large.bin"})
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}

	var buf bytes.Buffer
	chunkCount := 0
	for {
		chunk, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("Recv: %v", err)
		}
		buf.Write(chunk.Data)
		chunkCount++
	}

	if !bytes.Equal(buf.Bytes(), data) {
		t.Error("large file data mismatch")
	}

	// Should have received multiple chunks (256KB / 64KB = 4 chunks)
	if chunkCount < 4 {
		t.Errorf("expected >= 4 chunks, got %d", chunkCount)
	}
}
