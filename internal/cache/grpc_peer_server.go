package cache

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"time"

	pb "fuse-client/internal/pb"
)

const grpcChunkSize = 64 * 1024 // 64KB streaming chunks

// PeerGRPCServer implements the PeerService gRPC service.
// Each client node runs this so other peers can read/write files via gRPC.
type PeerGRPCServer struct {
	pb.UnimplementedPeerServiceServer
	cacheManager CacheManager
}

// NewPeerGRPCServer creates a new peer gRPC server.
func NewPeerGRPCServer(cm CacheManager) *PeerGRPCServer {
	return &PeerGRPCServer{cacheManager: cm}
}

// ReadFile streams file data in 64KB chunks.
func (s *PeerGRPCServer) ReadFile(req *pb.ReadFileRequest, stream pb.PeerService_ReadFileServer) error {
	entry, err := s.cacheManager.Get(stream.Context(), req.Path)
	if err != nil {
		return fmt.Errorf("file not found: %v", err)
	}

	buf := entry.Data
	for offset := 0; offset < len(buf); offset += grpcChunkSize {
		end := offset + grpcChunkSize
		if end > len(buf) {
			end = len(buf)
		}
		if err := stream.Send(&pb.FileChunk{Data: buf[offset:end]}); err != nil {
			return err
		}
	}
	return nil
}

// WriteFile receives metadata + data chunks via client streaming.
func (s *PeerGRPCServer) WriteFile(stream pb.PeerService_WriteFileServer) error {
	// First message must contain metadata
	first, err := stream.Recv()
	if err != nil {
		return fmt.Errorf("failed to receive metadata: %v", err)
	}
	path := first.Path
	if path == "" {
		return fmt.Errorf("path is required in first message")
	}

	var buf bytes.Buffer
	if len(first.Data) > 0 {
		buf.Write(first.Data)
	}

	// Receive remaining data chunks
	for {
		msg, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("receive error: %v", err)
		}
		buf.Write(msg.Data)
	}

	data := buf.Bytes()
	entry := &CacheEntry{
		FilePath:     path,
		StoragePath:  path,
		Size:         int64(len(data)),
		LastAccessed: time.Now(),
		Data:         data,
	}

	if err := s.cacheManager.Put(context.Background(), entry); err != nil {
		return fmt.Errorf("failed to store file: %v", err)
	}

	return stream.SendAndClose(&pb.WriteFileResponse{
		BytesWritten: int64(len(data)),
	})
}

// DeleteFile removes a file from local storage.
func (s *PeerGRPCServer) DeleteFile(ctx context.Context, req *pb.DeleteFileRequest) (*pb.DeleteFileResponse, error) {
	if err := s.cacheManager.Delete(ctx, req.Path); err != nil {
		return nil, err
	}
	return &pb.DeleteFileResponse{}, nil
}

// FileExists checks if a file exists.
func (s *PeerGRPCServer) FileExists(ctx context.Context, req *pb.FileExistsRequest) (*pb.FileExistsResponse, error) {
	_, err := s.cacheManager.Get(ctx, req.Path)
	return &pb.FileExistsResponse{Exists: err == nil}, nil
}

// FileSize returns the size of a file.
func (s *PeerGRPCServer) FileSize(ctx context.Context, req *pb.FileSizeRequest) (*pb.FileSizeResponse, error) {
	entry, err := s.cacheManager.Get(ctx, req.Path)
	if err != nil {
		return nil, fmt.Errorf("file not found: %v", err)
	}
	return &pb.FileSizeResponse{Size: entry.Size}, nil
}
