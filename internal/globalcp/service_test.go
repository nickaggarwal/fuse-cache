package globalcp

import (
	"context"
	"io"
	"log"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	"fuse-client/internal/pb"
)

// startTestService runs the service over a real in-process gRPC server.
// A stream's behavior lives in the gRPC plumbing as much as in the handler
// -- ordering, flow control, the send/recv split -- so calling the methods
// directly would test something other than what ships.
func startTestService(t *testing.T) (*Service, pb.GlobalControlPlaneClient) {
	t.Helper()
	svc := NewService(Config{
		Logger:                log.New(io.Discard, "", 0),
		AllowInsecureIdentity: true,
	})
	lis := bufconn.Listen(1 << 20)
	srv := grpc.NewServer()
	pb.RegisterGlobalControlPlaneServer(srv, svc)
	go func() { _ = srv.Serve(lis) }()

	conn, err := grpc.Dial("bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() {
		_ = conn.Close()
		srv.Stop()
		_ = lis.Close()
	})
	return svc, pb.NewGlobalControlPlaneClient(conn)
}

func asCluster(ctx context.Context, id string) context.Context {
	return metadata.AppendToOutgoingContext(ctx, ClusterIDHeader, id)
}

func registerCluster(t *testing.T, client pb.GlobalControlPlaneClient, id string, publish, subscribe []string) {
	t.Helper()
	_, err := client.RegisterCluster(asCluster(context.Background(), id), &pb.RegisterClusterRequest{
		ClusterId:         id,
		Object:            &pb.ObjectRef{Provider: "s3", Bucket: "shared", KeyPrefix: id},
		PublishPrefixes:   publish,
		SubscribePrefixes: subscribe,
	})
	if err != nil {
		t.Fatalf("register %s: %v", id, err)
	}
}

func loc(path string, size int64) *pb.GlobalLocation {
	return &pb.GlobalLocation{FilePath: path, FileSize: size}
}

// recvWithin reads one message, failing rather than hanging the suite.
func recvWithin(t *testing.T, stream pb.GlobalControlPlane_SyncClient, what string) *pb.SyncResponse {
	t.Helper()
	type result struct {
		msg *pb.SyncResponse
		err error
	}
	ch := make(chan result, 1)
	go func() {
		m, err := stream.Recv()
		ch <- result{m, err}
	}()
	select {
	case r := <-ch:
		if r.err != nil {
			t.Fatalf("recv %s: %v", what, r.err)
		}
		return r.msg
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for %s", what)
		return nil
	}
}

// awaitSnapshot drains the snapshot batches and returns their rows. Every
// stream begins with one, terminated by final=true, so a subscriber knows
// its mirror is complete instead of guessing from a quiet stream.
func awaitSnapshot(t *testing.T, stream pb.GlobalControlPlane_SyncClient) []*pb.GlobalLocation {
	t.Helper()
	var out []*pb.GlobalLocation
	for {
		msg := recvWithin(t, stream, "snapshot")
		snap := msg.GetSnapshot()
		if snap == nil {
			t.Fatalf("expected snapshot, got %T", msg.GetBody())
		}
		out = append(out, snap.GetLocations()...)
		if snap.GetFinal() {
			return out
		}
	}
}

func TestSyncRequiresRegistrationFirst(t *testing.T) {
	_, client := startTestService(t)
	stream, err := client.Sync(asCluster(context.Background(), "unregistered"))
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}
	// The allowlist arrives with registration; without it there is no basis
	// on which to accept a publish or scope a subscription.
	if _, err := stream.Recv(); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("got %v (code %s), want FailedPrecondition", err, status.Code(err))
	}
}

// TestPublishCrossesToSubscriber is the whole point of the service: a path
// written in cluster A becomes discoverable in cluster B.
func TestPublishCrossesToSubscriber(t *testing.T) {
	_, client := startTestService(t)
	registerCluster(t, client, "a", []string{"/models"}, nil)
	registerCluster(t, client, "b", nil, []string{"/models"})

	bStream, err := client.Sync(asCluster(context.Background(), "b"))
	if err != nil {
		t.Fatalf("b sync: %v", err)
	}
	if rows := awaitSnapshot(t, bStream); len(rows) != 0 {
		t.Fatalf("expected empty initial snapshot, got %d rows", len(rows))
	}

	aStream, err := client.Sync(asCluster(context.Background(), "a"))
	if err != nil {
		t.Fatalf("a sync: %v", err)
	}
	awaitSnapshot(t, aStream)

	if err := aStream.Send(&pb.SyncRequest{Body: &pb.SyncRequest_Publish{
		Publish: &pb.PublishBatch{Locations: []*pb.GlobalLocation{loc("/models/qwen.safetensors", 3<<30)}},
	}}); err != nil {
		t.Fatalf("publish: %v", err)
	}

	ev := recvWithin(t, bStream, "upsert event").GetEvent()
	if ev == nil || ev.GetKind() != pb.GlobalEvent_UPSERT {
		t.Fatalf("expected UPSERT, got %+v", ev)
	}
	got := ev.GetLocation()
	if got.GetFilePath() != "/models/qwen.safetensors" {
		t.Errorf("path = %q", got.GetFilePath())
	}
	// Identity is stamped by the server from the authenticated connection,
	// never taken from the row the client sent.
	if got.GetClusterId() != "a" {
		t.Errorf("cluster_id = %q, want %q", got.GetClusterId(), "a")
	}
	// A row with no ObjectRef falls back to the cluster's registered store,
	// rather than being stored pointing nowhere.
	if got.GetObject().GetBucket() != "shared" || got.GetObject().GetKeyPrefix() != "a" {
		t.Errorf("object = %+v, want the publisher's registered store", got.GetObject())
	}
	if got.GetPublishedUnix() == 0 {
		t.Error("published_unix not stamped")
	}
}

// TestPublishOutsideAllowlistIsRejected: a misconfigured agent must not be
// able to widen its own scope past what was registered.
func TestPublishOutsideAllowlistIsRejected(t *testing.T) {
	_, client := startTestService(t)
	registerCluster(t, client, "a", []string{"/models"}, nil)

	stream, err := client.Sync(asCluster(context.Background(), "a"))
	if err != nil {
		t.Fatalf("sync: %v", err)
	}
	awaitSnapshot(t, stream)

	// Near-miss, not an obviously-unrelated path: "/models-v2" is what a
	// substring check would wrongly accept.
	if err := stream.Send(&pb.SyncRequest{Body: &pb.SyncRequest_Publish{
		Publish: &pb.PublishBatch{Locations: []*pb.GlobalLocation{loc("/models-v2/secret.bin", 1)}},
	}}); err != nil {
		t.Fatalf("send: %v", err)
	}
	if _, err := stream.Recv(); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("got %v (code %s), want PermissionDenied", err, status.Code(err))
	}
}

// TestPublisherDoesNotSeeItsOwnRows: echoing a cluster's own publishes back
// would have its mirror report "another cluster has this" for its own files.
func TestPublisherDoesNotSeeItsOwnRows(t *testing.T) {
	svc, client := startTestService(t)
	registerCluster(t, client, "a", []string{"/models"}, []string{"/models"})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stream, err := client.Sync(asCluster(ctx, "a"))
	if err != nil {
		t.Fatalf("sync: %v", err)
	}
	awaitSnapshot(t, stream)

	if err := stream.Send(&pb.SyncRequest{Body: &pb.SyncRequest_Publish{
		Publish: &pb.PublishBatch{Locations: []*pb.GlobalLocation{loc("/models/a.bin", 1)}},
	}}); err != nil {
		t.Fatalf("send: %v", err)
	}

	// Wait on observed state -- the row landing in the store -- rather than
	// racing a sleep against the server's handling of the publish.
	deadline := time.Now().Add(5 * time.Second)
	for {
		rows, _ := svc.Store().GetLocations(context.Background(), "/models/a.bin")
		if len(rows) == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("publish never reached the store")
		}
		time.Sleep(5 * time.Millisecond)
	}

	// A second stream for the same cluster gets a snapshot, and its own row
	// must not be in it.
	stream2, err := client.Sync(asCluster(ctx, "a"))
	if err != nil {
		t.Fatalf("sync 2: %v", err)
	}
	if rows := awaitSnapshot(t, stream2); len(rows) != 0 {
		t.Fatalf("snapshot echoed the cluster's own %d row(s) back to it", len(rows))
	}
}

// TestSnapshotCarriesExistingRows: a cluster connecting after the fact
// converges on the current index without replaying history.
func TestSnapshotCarriesExistingRows(t *testing.T) {
	svc, client := startTestService(t)
	registerCluster(t, client, "a", []string{"/models"}, nil)
	registerCluster(t, client, "b", nil, []string{"/models"})

	if err := svc.Store().PutLocations(context.Background(), []*pb.GlobalLocation{
		{ClusterId: "a", FilePath: "/models/a.bin", FileSize: 10},
		{ClusterId: "a", FilePath: "/other/b.bin", FileSize: 20},
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	stream, err := client.Sync(asCluster(context.Background(), "b"))
	if err != nil {
		t.Fatalf("sync: %v", err)
	}
	rows := awaitSnapshot(t, stream)
	if len(rows) != 1 {
		t.Fatalf("snapshot has %d rows, want 1 (subscription is scoped to /models)", len(rows))
	}
	if rows[0].GetFilePath() != "/models/a.bin" {
		t.Errorf("snapshot row = %q", rows[0].GetFilePath())
	}
}

func TestWithdrawReachesSubscribers(t *testing.T) {
	svc, client := startTestService(t)
	registerCluster(t, client, "a", []string{"/models"}, nil)
	registerCluster(t, client, "b", nil, []string{"/models"})

	if err := svc.Store().PutLocations(context.Background(), []*pb.GlobalLocation{
		{ClusterId: "a", FilePath: "/models/a.bin", FileSize: 10},
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	bStream, err := client.Sync(asCluster(context.Background(), "b"))
	if err != nil {
		t.Fatalf("b sync: %v", err)
	}
	awaitSnapshot(t, bStream)

	aStream, err := client.Sync(asCluster(context.Background(), "a"))
	if err != nil {
		t.Fatalf("a sync: %v", err)
	}
	awaitSnapshot(t, aStream)

	if err := aStream.Send(&pb.SyncRequest{Body: &pb.SyncRequest_Withdraw{
		Withdraw: &pb.WithdrawBatch{FilePaths: []string{"/models/a.bin"}},
	}}); err != nil {
		t.Fatalf("withdraw: %v", err)
	}

	ev := recvWithin(t, bStream, "withdraw event").GetEvent()
	if ev == nil || ev.GetKind() != pb.GlobalEvent_WITHDRAW {
		t.Fatalf("expected WITHDRAW, got %+v", ev)
	}
	if ev.GetFilePath() != "/models/a.bin" || ev.GetClusterId() != "a" {
		t.Errorf("event = %+v", ev)
	}

	// Withdraw must be idempotent: a reconnecting agent cannot know what the
	// server still holds, so retracting an absent row is not an error.
	if err := aStream.Send(&pb.SyncRequest{Body: &pb.SyncRequest_Withdraw{
		Withdraw: &pb.WithdrawBatch{FilePaths: []string{"/models/a.bin", "/models/never-existed"}},
	}}); err != nil {
		t.Fatalf("repeat withdraw: %v", err)
	}
	if ev := recvWithin(t, bStream, "repeat withdraw event").GetEvent(); ev.GetKind() != pb.GlobalEvent_WITHDRAW {
		t.Fatalf("expected a second WITHDRAW, got %+v", ev)
	}
}

func TestResolveReturnsEveryClustersCopy(t *testing.T) {
	svc, client := startTestService(t)
	if err := svc.Store().PutLocations(context.Background(), []*pb.GlobalLocation{
		{ClusterId: "a", FilePath: "/models/a.bin", FileSize: 10},
		{ClusterId: "b", FilePath: "/models/a.bin", FileSize: 10},
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}
	resp, err := client.Resolve(asCluster(context.Background(), "c"), &pb.ResolveRequest{FilePath: "/models/a.bin"})
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	// Several clusters may hold the same path; collapsing them would discard
	// a copy the caller might prefer.
	if len(resp.GetLocations()) != 2 {
		t.Fatalf("got %d locations, want 2", len(resp.GetLocations()))
	}
	resp, err = client.Resolve(asCluster(context.Background(), "c"), &pb.ResolveRequest{FilePath: "/nope"})
	if err != nil {
		t.Fatalf("resolve miss: %v", err)
	}
	if len(resp.GetLocations()) != 0 {
		t.Errorf("miss returned %d locations", len(resp.GetLocations()))
	}
}

// TestIdentityRequiresCertWhenInsecureDisabled pins the Stage 4 gate: with
// the dev fallback off, a header-declared identity is rejected outright, so
// a cluster cannot claim to be another.
func TestIdentityRequiresCertWhenInsecureDisabled(t *testing.T) {
	svc := NewService(Config{Logger: log.New(io.Discard, "", 0)})
	if svc.AllowInsecureIdentity {
		t.Fatal("insecure identity must be off by default")
	}
	ctx := metadata.NewIncomingContext(context.Background(),
		metadata.Pairs(ClusterIDHeader, "impostor"))
	if _, err := svc.identify(ctx, "impostor"); status.Code(err) != codes.Unauthenticated {
		t.Fatalf("got %v (code %s), want Unauthenticated", err, status.Code(err))
	}
}

func TestRegisterCapsPrefixCount(t *testing.T) {
	_, client := startTestService(t)
	many := make([]string, 0, 100)
	for i := 0; i < 100; i++ {
		many = append(many, "/p"+string(rune('a'+i%26))+string(rune('a'+i/26)))
	}
	_, err := client.RegisterCluster(asCluster(context.Background(), "a"), &pb.RegisterClusterRequest{
		ClusterId:       "a",
		PublishPrefixes: many,
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("got %v (code %s), want InvalidArgument", err, status.Code(err))
	}
}
