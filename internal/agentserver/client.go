package agentserver

import (
	"context"
	"fmt"
	"time"

	pb "fuse-client/internal/pb"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Client connects to the agent gRPC server over a Unix socket.
// Used by the CSI node plugin.
type Client struct {
	conn   *grpc.ClientConn
	client pb.AgentServiceClient
}

// NewClient creates a new agent client connected to the given Unix socket path.
func NewClient(socketPath string) (*Client, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, err := grpc.DialContext(ctx, "unix://"+socketPath,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)
	if err != nil {
		return nil, fmt.Errorf("connect to agent at %s: %w", socketPath, err)
	}

	return &Client{
		conn:   conn,
		client: pb.NewAgentServiceClient(conn),
	}, nil
}

// CreateSession asks the agent to prepare a cache-backed host path.
func (c *Client) CreateSession(ctx context.Context, volumeID, rootPath string, readOnly bool, policy *pb.CachePolicy) (*pb.CreateSessionResponse, error) {
	return c.client.CreateSession(ctx, &pb.CreateSessionRequest{
		VolumeId: volumeID,
		RootPath: rootPath,
		ReadOnly: readOnly,
		Policy:   policy,
	})
}

// DeleteSession asks the agent to release a session.
func (c *Client) DeleteSession(ctx context.Context, volumeID string) error {
	_, err := c.client.DeleteSession(ctx, &pb.DeleteSessionRequest{
		VolumeId: volumeID,
	})
	return err
}

// GetSession returns session info.
func (c *Client) GetSession(ctx context.Context, volumeID string) (*pb.SessionInfo, error) {
	return c.client.GetSession(ctx, &pb.GetSessionRequest{
		VolumeId: volumeID,
	})
}

// ListSessions returns all active sessions.
func (c *Client) ListSessions(ctx context.Context) ([]*pb.SessionInfo, error) {
	resp, err := c.client.ListSessions(ctx, &pb.ListSessionsRequest{})
	if err != nil {
		return nil, err
	}
	return resp.Sessions, nil
}

// Close closes the connection.
func (c *Client) Close() error {
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}
