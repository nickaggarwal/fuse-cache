package coordinator

import (
	"context"
	"time"

	pb "fuse-client/internal/pb"

	"google.golang.org/protobuf/types/known/timestamppb"
)

// GRPCServer implements the CoordinatorService gRPC service.
type GRPCServer struct {
	pb.UnimplementedCoordinatorServiceServer
	service *CoordinatorService
}

// NewGRPCServer creates a new gRPC server wrapping the given CoordinatorService.
func NewGRPCServer(service *CoordinatorService) *GRPCServer {
	return &GRPCServer{service: service}
}

func (s *GRPCServer) RegisterPeer(ctx context.Context, req *pb.RegisterPeerRequest) (*pb.RegisterPeerResponse, error) {
	peer := peerInfoFromProto(req.Peer)
	if err := s.service.RegisterPeer(ctx, peer); err != nil {
		return nil, err
	}
	return &pb.RegisterPeerResponse{}, nil
}

func (s *GRPCServer) GetPeers(ctx context.Context, req *pb.GetPeersRequest) (*pb.GetPeersResponse, error) {
	peers, err := s.service.GetPeers(ctx, req.RequesterId)
	if err != nil {
		return nil, err
	}
	pbPeers := make([]*pb.PeerInfo, len(peers))
	for i, p := range peers {
		pbPeers[i] = peerInfoToProto(p)
	}
	return &pb.GetPeersResponse{Peers: pbPeers}, nil
}

func (s *GRPCServer) UpdatePeerStatus(ctx context.Context, req *pb.UpdatePeerStatusRequest) (*pb.UpdatePeerStatusResponse, error) {
	err := s.service.UpdatePeerStatus(ctx, req.PeerId, req.Status, req.AvailableSpace, req.UsedSpace)
	if err != nil {
		return nil, err
	}
	return &pb.UpdatePeerStatusResponse{}, nil
}

func (s *GRPCServer) GetFileLocation(ctx context.Context, req *pb.GetFileLocationRequest) (*pb.GetFileLocationResponse, error) {
	locations, err := s.service.GetFileLocation(ctx, req.FilePath)
	if err != nil {
		return nil, err
	}
	pbLocs := make([]*pb.FileLocation, len(locations))
	for i, loc := range locations {
		pbLocs[i] = fileLocationToProto(loc)
	}
	return &pb.GetFileLocationResponse{Locations: pbLocs}, nil
}

func (s *GRPCServer) UpdateFileLocation(ctx context.Context, req *pb.UpdateFileLocationRequest) (*pb.UpdateFileLocationResponse, error) {
	loc := fileLocationFromProto(req.Location)
	if err := s.service.UpdateFileLocation(ctx, loc); err != nil {
		return nil, err
	}
	return &pb.UpdateFileLocationResponse{}, nil
}

func (s *GRPCServer) GetPeerStats(ctx context.Context, req *pb.GetPeerStatsRequest) (*pb.GetPeerStatsResponse, error) {
	stats := s.service.GetPeerStats()
	return &pb.GetPeerStatsResponse{
		ActivePeers:    toInt64(stats["active_peers"]),
		TotalPeers:     toInt64(stats["total_peers"]),
		TotalSpace:     toInt64(stats["total_space"]),
		UsedSpace:      toInt64(stats["used_space"]),
		AvailableSpace: toInt64(stats["available_space"]),
		FileCount:      toInt64(stats["file_count"]),
	}, nil
}

// Converters

func peerInfoToProto(p *PeerInfo) *pb.PeerInfo {
	return &pb.PeerInfo{
		Id:             p.ID,
		Address:        p.Address,
		GrpcAddress:    p.GRPCAddress,
		NvmePath:       p.NVMePath,
		AvailableSpace: p.AvailableSpace,
		UsedSpace:      p.UsedSpace,
		Status:         p.Status,
		LastHeartbeat:  timestamppb.New(p.LastHeartbeat),
	}
}

func peerInfoFromProto(p *pb.PeerInfo) *PeerInfo {
	var lastHB time.Time
	if p.LastHeartbeat != nil {
		lastHB = p.LastHeartbeat.AsTime()
	}
	return &PeerInfo{
		ID:             p.Id,
		Address:        p.Address,
		GRPCAddress:    p.GrpcAddress,
		NVMePath:       p.NvmePath,
		AvailableSpace: p.AvailableSpace,
		UsedSpace:      p.UsedSpace,
		Status:         p.Status,
		LastHeartbeat:  lastHB,
	}
}

func fileLocationToProto(fl *FileLocation) *pb.FileLocation {
	chunks := make([]*pb.ChunkInfo, len(fl.Chunks))
	for i, c := range fl.Chunks {
		chunks[i] = &pb.ChunkInfo{
			Index:   int32(c.Index),
			PeerId:  c.PeerID,
			ChunkId: c.ChunkID,
		}
	}
	return &pb.FileLocation{
		FilePath:     fl.FilePath,
		PeerId:       fl.PeerID,
		StorageTier:  fl.StorageTier,
		StoragePath:  fl.StoragePath,
		FileSize:     fl.FileSize,
		LastAccessed: timestamppb.New(fl.LastAccessed),
		IsChunked:    fl.IsChunked,
		Chunks:       chunks,
	}
}

func fileLocationFromProto(fl *pb.FileLocation) *FileLocation {
	chunks := make([]ChunkInfo, len(fl.Chunks))
	for i, c := range fl.Chunks {
		chunks[i] = ChunkInfo{
			Index:   int(c.Index),
			PeerID:  c.PeerId,
			ChunkID: c.ChunkId,
		}
	}
	var lastAccessed time.Time
	if fl.LastAccessed != nil {
		lastAccessed = fl.LastAccessed.AsTime()
	}
	return &FileLocation{
		FilePath:     fl.FilePath,
		PeerID:       fl.PeerId,
		StorageTier:  fl.StorageTier,
		StoragePath:  fl.StoragePath,
		FileSize:     fl.FileSize,
		LastAccessed: lastAccessed,
		IsChunked:    fl.IsChunked,
		Chunks:       chunks,
	}
}

func toInt64(v interface{}) int64 {
	switch val := v.(type) {
	case int:
		return int64(val)
	case int64:
		return val
	default:
		return 0
	}
}
