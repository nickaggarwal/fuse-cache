package cache

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	pb "fuse-client/internal/pb"

	"google.golang.org/grpc/codes"
	grpcpeer "google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

const grpcChunkSize = 24 * 1024 * 1024 // 24MiB streaming chunks

var grpcReadBufPool = sync.Pool{
	New: func() interface{} {
		b := make([]byte, grpcChunkSize)
		return &b
	},
}

// PeerGRPCServer implements the PeerService gRPC service.
// Each client node runs this so other peers can read/write files via gRPC.
type PeerGRPCServer struct {
	pb.UnimplementedPeerServiceServer
	cacheManager CacheManager
}

type localGetter interface {
	GetLocal(ctx context.Context, filePath string) (*CacheEntry, error)
}

type localPathResolver interface {
	LocalFilePath(ctx context.Context, filePath string) (string, bool)
}

// localChunkResolver resolves a "<parent>_chunk_N" request to a byte range in a
// whole parent file held on this node (the FUSE-write storage model), so the
// chunk can be streamed directly from disk with a pooled buffer instead of
// synthesizing the whole chunk into a fresh heap allocation per request.
type localChunkResolver interface {
	LocalChunkFile(ctx context.Context, chunkPath string) (localPath string, offset, length int64, ok bool)
}

// NewPeerGRPCServer creates a new peer gRPC server.
func NewPeerGRPCServer(cm CacheManager) *PeerGRPCServer {
	return &PeerGRPCServer{cacheManager: cm}
}

// errPeerServeBusy is returned when serve-side admission control rejects a
// peer request. RESOURCE_EXHAUSTED tells the requester to fail over to another
// holder and retry this node only after jitter (thundering-herd Phase 1).
var errPeerServeBusy = status.Error(codes.ResourceExhausted, "peer serve capacity exhausted")

// admitPeerServe consults the cache manager's serve gate, if it has one.
// Managers without a gate admit everything (nil release func is never
// returned; ok=true always pairs with a callable release).
func (s *PeerGRPCServer) admitPeerServe() (release func(), ok bool) {
	if admitter, hasGate := s.cacheManager.(PeerServeAdmitter); hasGate {
		return admitter.TryAcquirePeerServe()
	}
	return func() {}, true
}

// ReadFile streams file data in fixed-size chunks.
func (s *PeerGRPCServer) ReadFile(req *pb.ReadFileRequest, stream pb.PeerService_ReadFileServer) error {
	release, ok := s.admitPeerServe()
	if !ok {
		return errPeerServeBusy
	}
	defer release()

	// Heat tracking: the requester's address identifies a distinct remote
	// reader; the reconciler scales the file's replica target on this.
	if observer, ok := s.cacheManager.(RemoteReadObserver); ok {
		if p, pok := grpcpeer.FromContext(stream.Context()); pok && p.Addr != nil {
			observer.NoteRemoteReader(req.Path, p.Addr.String())
		}
	}

	if resolver, ok := s.cacheManager.(localPathResolver); ok {
		if localPath, ok := resolver.LocalFilePath(stream.Context(), req.Path); ok {
			if err := streamLocalFile(localPath, stream); err == nil {
				return nil
			}
			// Fall through to cache-entry path for robustness on transient local I/O errors.
		}
	}

	// Chunk request served from a whole parent file: stream the byte range
	// directly with a pooled buffer, avoiding a per-chunk full-size heap
	// allocation (the dominant allocation on the peer serve path under high
	// parallelism).
	if resolver, ok := s.cacheManager.(localChunkResolver); ok {
		if localPath, offset, length, ok := resolver.LocalChunkFile(stream.Context(), req.Path); ok {
			if err := streamLocalFileRange(localPath, offset, length, stream); err == nil {
				return nil
			}
			// Fall through to cache-entry path on transient local I/O errors.
		}
	}

	entry, err := s.getForPeerRPC(stream.Context(), req.Path)
	if err != nil {
		// NOT_FOUND, not a bare error: the requester distinguishes "this
		// holder doesn't have it" from "the transfer failed". A miss is
		// routing information, not a network fault, so it must not poison
		// the requester's pairwise success EWMA.
		return status.Errorf(codes.NotFound, "file not found: %v", err)
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

func streamLocalFile(localPath string, stream pb.PeerService_ReadFileServer) error {
	f, err := os.Open(localPath)
	if err != nil {
		return err
	}
	defer f.Close()

	bufPtr := grpcReadBufPool.Get().(*[]byte)
	buf := *bufPtr
	defer grpcReadBufPool.Put(bufPtr)

	for {
		n, readErr := f.Read(buf)
		if n > 0 {
			if err := stream.Send(&pb.FileChunk{Data: buf[:n]}); err != nil {
				return err
			}
		}
		if readErr == io.EOF {
			return nil
		}
		if readErr != nil {
			return readErr
		}
	}
}

// streamLocalFileRange streams [offset, offset+length) of localPath using a
// pooled buffer, so serving one chunk out of a whole parent file allocates no
// per-request payload buffer. gRPC marshals each Send synchronously, so the
// pooled buffer is safe to reuse across iterations.
func streamLocalFileRange(localPath string, offset, length int64, stream pb.PeerService_ReadFileServer) error {
	if length <= 0 {
		return nil
	}
	f, err := os.Open(localPath)
	if err != nil {
		return err
	}
	defer f.Close()

	bufPtr := grpcReadBufPool.Get().(*[]byte)
	buf := *bufPtr
	defer grpcReadBufPool.Put(bufPtr)

	pos := offset
	remaining := length
	for remaining > 0 {
		n := int64(len(buf))
		if n > remaining {
			n = remaining
		}
		read, readErr := f.ReadAt(buf[:n], pos)
		if read > 0 {
			if err := stream.Send(&pb.FileChunk{Data: buf[:read]}); err != nil {
				return err
			}
			pos += int64(read)
			remaining -= int64(read)
		}
		if readErr == io.EOF {
			return nil
		}
		if readErr != nil {
			return readErr
		}
	}
	return nil
}

// WriteFile receives metadata + data chunks via client streaming.
func (s *PeerGRPCServer) WriteFile(stream pb.PeerService_WriteFileServer) error {
	release, ok := s.admitPeerServe()
	if !ok {
		return errPeerServeBusy
	}
	defer release()

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
	_, err := s.getForPeerRPC(ctx, req.Path)
	return &pb.FileExistsResponse{Exists: err == nil}, nil
}

// FileSize returns the size of a file.
func (s *PeerGRPCServer) FileSize(ctx context.Context, req *pb.FileSizeRequest) (*pb.FileSizeResponse, error) {
	entry, err := s.getForPeerRPC(ctx, req.Path)
	if err != nil {
		return nil, fmt.Errorf("file not found: %v", err)
	}
	return &pb.FileSizeResponse{Size: entry.Size}, nil
}

func (s *PeerGRPCServer) getForPeerRPC(ctx context.Context, path string) (*CacheEntry, error) {
	if local, ok := s.cacheManager.(localGetter); ok {
		return local.GetLocal(ctx, path)
	}
	return s.cacheManager.Get(ctx, path)
}
