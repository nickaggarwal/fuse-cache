package coordinator

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"time"

	pb "fuse-client/internal/pb"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// GRPCCoordinatorClient implements the Coordinator interface via gRPC.
type GRPCCoordinatorClient struct {
	conn   *grpc.ClientConn
	client pb.CoordinatorServiceClient

	httpAddr   string
	httpClient *http.Client
}

// NewGRPCCoordinatorClient creates a new gRPC-based coordinator client.
func NewGRPCCoordinatorClient(addr string) (*GRPCCoordinatorClient, error) {
	conn, err := grpc.Dial(addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return nil, err
	}
	return &GRPCCoordinatorClient{
		conn:     conn,
		client:   pb.NewCoordinatorServiceClient(conn),
		httpAddr: deriveCoordinatorHTTPAddr(addr),
		httpClient: &http.Client{
			Timeout: 10 * time.Second,
		},
	}, nil
}

func (c *GRPCCoordinatorClient) RegisterPeer(ctx context.Context, peer *PeerInfo) error {
	_, err := c.client.RegisterPeer(ctx, &pb.RegisterPeerRequest{
		Peer: peerInfoToProto(peer),
	})
	return err
}

func (c *GRPCCoordinatorClient) GetPeers(ctx context.Context, requesterID string) ([]*PeerInfo, error) {
	resp, err := c.client.GetPeers(ctx, &pb.GetPeersRequest{RequesterId: requesterID})
	if err != nil {
		return nil, err
	}
	peers := make([]*PeerInfo, len(resp.Peers))
	for i, p := range resp.Peers {
		peers[i] = peerInfoFromProto(p)
	}
	return peers, nil
}

func (c *GRPCCoordinatorClient) UpdatePeerStatus(ctx context.Context, peerID string, status string, availableSpace, usedSpace int64) error {
	_, err := c.client.UpdatePeerStatus(ctx, &pb.UpdatePeerStatusRequest{
		PeerId:         peerID,
		Status:         status,
		AvailableSpace: availableSpace,
		UsedSpace:      usedSpace,
	})
	return err
}

func (c *GRPCCoordinatorClient) GetFileLocation(ctx context.Context, filePath string) ([]*FileLocation, error) {
	resp, err := c.client.GetFileLocation(ctx, &pb.GetFileLocationRequest{FilePath: filePath})
	if err != nil {
		return nil, err
	}
	locations := make([]*FileLocation, len(resp.Locations))
	for i, loc := range resp.Locations {
		locations[i] = fileLocationFromProto(loc)
	}
	return locations, nil
}

func (c *GRPCCoordinatorClient) ListFileLocations(ctx context.Context, prefix string) ([]*FileLocation, error) {
	url := fmt.Sprintf("http://%s/api/files/locations", c.httpAddr)
	if prefix != "" {
		url += "?prefix=" + prefix
	}
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, err
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var locations []*FileLocation
	if err := json.NewDecoder(resp.Body).Decode(&locations); err != nil {
		return nil, err
	}
	return locations, nil
}

func (c *GRPCCoordinatorClient) UpdateFileLocation(ctx context.Context, location *FileLocation) error {
	_, err := c.client.UpdateFileLocation(ctx, &pb.UpdateFileLocationRequest{
		Location: fileLocationToProto(location),
	})
	return err
}

func (c *GRPCCoordinatorClient) GetPeerStats() map[string]interface{} {
	resp, err := c.client.GetPeerStats(context.Background(), &pb.GetPeerStatsRequest{})
	if err != nil {
		return map[string]interface{}{"error": err.Error()}
	}
	return map[string]interface{}{
		"active_peers":    resp.ActivePeers,
		"total_peers":     resp.TotalPeers,
		"total_space":     resp.TotalSpace,
		"used_space":      resp.UsedSpace,
		"available_space": resp.AvailableSpace,
		"file_count":      resp.FileCount,
	}
}

// Close closes the underlying gRPC connection.
func (c *GRPCCoordinatorClient) Close() error {
	return c.conn.Close()
}

func deriveCoordinatorHTTPAddr(grpcAddr string) string {
	host, _, err := net.SplitHostPort(grpcAddr)
	if err != nil {
		return grpcAddr
	}
	return net.JoinHostPort(host, "8080")
}
