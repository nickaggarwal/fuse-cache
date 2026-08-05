package agentserver

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"path/filepath"

	pb "fuse-client/internal/pb"
	"fuse-client/internal/session"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Server is the agent gRPC server that runs inside the fuse-client DaemonSet.
// The CSI node plugin connects to this over a Unix socket.
type Server struct {
	pb.UnimplementedAgentServiceServer
	sessMgr    *session.Manager
	socketPath string
	grpcServer *grpc.Server
	logger     *log.Logger
}

// New creates a new agent server.
func New(sessMgr *session.Manager, socketPath string) *Server {
	return &Server{
		sessMgr:    sessMgr,
		socketPath: socketPath,
		logger:     log.New(log.Writer(), "[AGENT] ", log.LstdFlags),
	}
}

// Serve starts the gRPC server on the Unix socket. Blocks until ctx is cancelled.
func (s *Server) Serve(ctx context.Context) error {
	// Ensure parent directory exists.
	if err := os.MkdirAll(filepath.Dir(s.socketPath), 0755); err != nil {
		return fmt.Errorf("create socket dir: %w", err)
	}

	// Remove stale socket.
	os.Remove(s.socketPath)

	lis, err := net.Listen("unix", s.socketPath)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", s.socketPath, err)
	}

	s.grpcServer = grpc.NewServer()
	pb.RegisterAgentServiceServer(s.grpcServer, s)

	go func() {
		<-ctx.Done()
		s.grpcServer.GracefulStop()
	}()

	s.logger.Printf("Agent gRPC server listening on %s", s.socketPath)
	return s.grpcServer.Serve(lis)
}

func (s *Server) CreateSession(ctx context.Context, req *pb.CreateSessionRequest) (*pb.CreateSessionResponse, error) {
	if req.VolumeId == "" {
		return nil, status.Error(codes.InvalidArgument, "volume_id is required")
	}

	policy := session.CachePolicy{}
	if req.Policy != nil {
		policy.CacheMode = req.Policy.CacheMode
		policy.Warmup = req.Policy.Warmup
		policy.WarmupBandwidth = req.Policy.WarmupBandwidth
		policy.Pinned = req.Policy.Pinned
		policy.SourcePolicy = req.Policy.SourcePolicy
	}

	sess, err := s.sessMgr.Create(ctx, req.VolumeId, req.RootPath, req.ReadOnly, policy)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "create session: %v", err)
	}

	return &pb.CreateSessionResponse{
		HostPath: sess.HostPath,
		VolumeId: sess.VolumeID,
	}, nil
}

func (s *Server) DeleteSession(ctx context.Context, req *pb.DeleteSessionRequest) (*pb.DeleteSessionResponse, error) {
	if req.VolumeId == "" {
		return nil, status.Error(codes.InvalidArgument, "volume_id is required")
	}

	_, err := s.sessMgr.Delete(ctx, req.VolumeId)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "delete session: %v", err)
	}

	return &pb.DeleteSessionResponse{}, nil
}

func (s *Server) GetSession(ctx context.Context, req *pb.GetSessionRequest) (*pb.SessionInfo, error) {
	if req.VolumeId == "" {
		return nil, status.Error(codes.InvalidArgument, "volume_id is required")
	}

	sess := s.sessMgr.Get(ctx, req.VolumeId)
	if sess == nil {
		return nil, status.Error(codes.NotFound, "session not found")
	}

	return sessionToProto(sess), nil
}

func (s *Server) ListSessions(ctx context.Context, _ *pb.ListSessionsRequest) (*pb.ListSessionsResponse, error) {
	sessions := s.sessMgr.List(ctx)
	infos := make([]*pb.SessionInfo, len(sessions))
	for i, sess := range sessions {
		infos[i] = sessionToProto(sess)
	}
	return &pb.ListSessionsResponse{Sessions: infos}, nil
}

func sessionToProto(sess *session.Session) *pb.SessionInfo {
	return &pb.SessionInfo{
		VolumeId: sess.VolumeID,
		RootPath: sess.RootPath,
		HostPath: sess.HostPath,
		ReadOnly: sess.ReadOnly,
		Policy: &pb.CachePolicy{
			CacheMode:       sess.Policy.CacheMode,
			Warmup:          sess.Policy.Warmup,
			WarmupBandwidth: sess.Policy.WarmupBandwidth,
			Pinned:          sess.Policy.Pinned,
			SourcePolicy:    sess.Policy.SourcePolicy,
		},
		RefCount:      sess.RefCount,
		CreatedAtUnix: sess.CreatedAt.Unix(),
	}
}
