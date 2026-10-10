package globalcp

import (
	"context"
	"log"
	"net"
	"os"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"fuse-client/internal/mtls"
	"fuse-client/internal/pb"
)

// These tests run the control plane over a real TLS listener rather than
// over an in-process stub, because the thing being tested lives in the
// transport: identity comes out of the verified certificate chain, and a
// fake peer.AuthInfo would let the test agree with itself about what the
// chain contained.

type tlsFixture struct {
	ca   *mtls.TestCA
	dir  string
	addr string
	svc  *Service
}

func startTLSControlPlane(t *testing.T) *tlsFixture {
	t.Helper()
	ca, err := mtls.NewTestCA()
	if err != nil {
		t.Fatalf("NewTestCA: %v", err)
	}
	dir := t.TempDir()
	serverCfg, err := ca.WriteTo(dir, "globalcp")
	if err != nil {
		t.Fatalf("WriteTo: %v", err)
	}
	creds, err := serverCfg.ServerCredentials()
	if err != nil {
		t.Fatalf("ServerCredentials: %v", err)
	}

	svc := NewService(Config{
		Store:  NewMemStore(),
		Logger: log.New(os.Stderr, "[TEST-CP] ", 0),
		// The production default. If a test needed this true to pass, the
		// test would be measuring the fallback and not the certificate.
		AllowInsecureIdentity: false,
	})
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := grpc.NewServer(grpc.Creds(creds))
	pb.RegisterGlobalControlPlaneServer(srv, svc)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)

	return &tlsFixture{ca: ca, dir: dir, addr: lis.Addr().String(), svc: svc}
}

// dialAs connects presenting a certificate issued to certName. The
// server's name is always "globalcp" -- only the CLIENT identity varies,
// which is the axis these tests move along.
func (f *tlsFixture) dialAs(t *testing.T, certName string) pb.GlobalControlPlaneClient {
	t.Helper()
	cfg, err := f.ca.WriteTo(t.TempDir(), certName)
	if err != nil {
		t.Fatalf("WriteTo: %v", err)
	}
	cfg.CAFile = f.dir + "/ca.crt"
	cfg.ServerName = "globalcp"
	opt, err := cfg.DialOption()
	if err != nil {
		t.Fatalf("DialOption: %v", err)
	}
	return f.connect(t, opt)
}

func (f *tlsFixture) connect(t *testing.T, opt grpc.DialOption) pb.GlobalControlPlaneClient {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, err := grpc.DialContext(ctx, f.addr, opt, grpc.WithBlock())
	if err != nil {
		t.Fatalf("dial %s: %v", f.addr, err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return pb.NewGlobalControlPlaneClient(conn)
}

func register(t *testing.T, c pb.GlobalControlPlaneClient, claimedID string, publish, subscribe []string) *pb.RegisterClusterResponse {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	resp, err := c.RegisterCluster(ctx, &pb.RegisterClusterRequest{
		ClusterId:         claimedID,
		PublishPrefixes:   publish,
		SubscribePrefixes: subscribe,
		Object: &pb.ObjectRef{
			Provider: "s3", Bucket: "shared", KeyPrefix: claimedID,
		},
	})
	if err != nil {
		t.Fatalf("RegisterCluster: %v", err)
	}
	return resp
}

func codeOf(err error) codes.Code { return status.Code(err) }

// The headline property: a certificate is an identity, and a request field
// is not. cluster-b presents its own certificate while claiming to be
// cluster-a in every place the wire lets it.
func TestCertificateOverridesTheClaimedClusterID(t *testing.T) {
	f := startTLSControlPlane(t)
	client := f.dialAs(t, "cluster-b")

	resp := register(t, client, "cluster-a", []string{"/models/"}, []string{"/models/"})
	if resp.GetClusterId() != "cluster-b" {
		t.Fatalf("registration accepted a claimed identity: got %q, want cluster-b", resp.GetClusterId())
	}

	// And the header is no better than the field.
	ctx, cancel := context.WithTimeout(
		metadata.AppendToOutgoingContext(context.Background(), ClusterIDHeader, "cluster-a"),
		5*time.Second)
	defer cancel()
	hdrResp, err := client.RegisterCluster(ctx, &pb.RegisterClusterRequest{
		ClusterId:       "cluster-a",
		PublishPrefixes: []string{"/models/"},
	})
	if err != nil {
		t.Fatalf("RegisterCluster: %v", err)
	}
	if hdrResp.GetClusterId() != "cluster-b" {
		t.Fatalf("header overrode the certificate: got %q, want cluster-b", hdrResp.GetClusterId())
	}
}

// The same question one level down: having registered honestly, can
// cluster-b push a ROW stamped cluster-a? The stream is where this would
// matter, because that is the only path that writes to the index.
func TestPublishedRowsAreStampedWithTheCertificateIdentity(t *testing.T) {
	f := startTLSControlPlane(t)
	client := f.dialAs(t, "cluster-b")
	register(t, client, "", []string{"/models/"}, []string{"/models/"})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stream, err := client.Sync(ctx)
	if err != nil {
		t.Fatalf("Sync: %v", err)
	}
	if err := stream.Send(&pb.SyncRequest{Body: &pb.SyncRequest_Publish{
		Publish: &pb.PublishBatch{Locations: []*pb.GlobalLocation{{
			ClusterId: "cluster-a", // the lie
			FilePath:  "/models/x.bin",
			FileSize:  10,
			Object:    &pb.ObjectRef{Provider: "s3", Bucket: "shared", KeyPrefix: "cluster-a"},
		}}},
	}}); err != nil {
		t.Fatalf("Send: %v", err)
	}

	rows := waitForRows(t, f.svc, "/models/x.bin", 1)
	if got := rows[0].GetClusterId(); got != "cluster-b" {
		t.Fatalf("row stored under a forged cluster: got %q, want cluster-b", got)
	}
	// The key prefix rides along in the ObjectRef, and the row now says
	// cluster-b holds bytes under cluster-a's prefix. That is a real
	// mis-advertisement, but it is one a reader discovers as a miss, not a
	// way to overwrite another cluster's row -- which is what the ID
	// stamping prevents.
	if got := rows[0].GetObject().GetKeyPrefix(); got != "cluster-a" {
		t.Logf("note: object prefix is caller-supplied (%q); identity is not", got)
	}
}

func TestPublishOutsideTheAllowlistIsDenied(t *testing.T) {
	f := startTLSControlPlane(t)
	client := f.dialAs(t, "cluster-b")
	register(t, client, "", []string{"/models/"}, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stream, err := client.Sync(ctx)
	if err != nil {
		t.Fatalf("Sync: %v", err)
	}
	// /models-v2/ is the near miss that a naive strings.HasPrefix on
	// "/models" would let through.
	if err := stream.Send(&pb.SyncRequest{Body: &pb.SyncRequest_Publish{
		Publish: &pb.PublishBatch{Locations: []*pb.GlobalLocation{{
			FilePath: "/models-v2/x.bin",
			Object:   &pb.ObjectRef{Provider: "s3", Bucket: "shared"},
		}}},
	}}); err != nil {
		t.Fatalf("Send: %v", err)
	}
	// Drain past the snapshot and any heartbeat: the denial terminates the
	// stream, so it surfaces as the first Recv ERROR, not the first Recv.
	if code := recvUntilError(t, stream); code != codes.PermissionDenied {
		t.Fatalf("expected PermissionDenied, got %v", code)
	}
}

// recvUntilError reads until the stream fails and returns the status code.
func recvUntilError(t *testing.T, stream pb.GlobalControlPlane_SyncClient) codes.Code {
	t.Helper()
	for i := 0; i < 32; i++ {
		if _, err := stream.Recv(); err != nil {
			return status.Code(err)
		}
	}
	t.Fatal("stream delivered 32 messages without the expected error")
	return codes.OK
}

func TestResolveOutsideTheSubscriptionIsDenied(t *testing.T) {
	f := startTLSControlPlane(t)

	// cluster-a publishes a checkpoint.
	a := f.dialAs(t, "cluster-a")
	register(t, a, "", []string{"/checkpoints/"}, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stream, err := a.Sync(ctx)
	if err != nil {
		t.Fatalf("Sync: %v", err)
	}
	if err := stream.Send(&pb.SyncRequest{Body: &pb.SyncRequest_Publish{
		Publish: &pb.PublishBatch{Locations: []*pb.GlobalLocation{{
			FilePath: "/checkpoints/step-1",
			Object:   &pb.ObjectRef{Provider: "s3", Bucket: "shared", KeyPrefix: "cluster-a"},
		}}},
	}}); err != nil {
		t.Fatalf("Send: %v", err)
	}
	waitForRows(t, f.svc, "/checkpoints/step-1", 1)

	// cluster-b subscribes only to /models/. Its stream would never be
	// sent that row; Resolve must not be the back door that hands it over.
	b := f.dialAs(t, "cluster-b")
	register(t, b, "", nil, []string{"/models/"})
	rctx, rcancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer rcancel()
	_, err = b.Resolve(rctx, &pb.ResolveRequest{FilePath: "/checkpoints/step-1"})
	if codeOf(err) != codes.PermissionDenied {
		t.Fatalf("expected PermissionDenied for an unsubscribed path, got %v", err)
	}

	// Control: inside its subscription the same call works, so the denial
	// above is about scope and not about Resolve being broken.
	c := f.dialAs(t, "cluster-c")
	register(t, c, "", nil, []string{"/checkpoints/"})
	cctx, ccancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer ccancel()
	resp, err := c.Resolve(cctx, &pb.ResolveRequest{FilePath: "/checkpoints/step-1"})
	if err != nil {
		t.Fatalf("Resolve inside the subscription failed: %v", err)
	}
	if len(resp.GetLocations()) != 1 {
		t.Fatalf("expected 1 location, got %d", len(resp.GetLocations()))
	}
}

func TestUnregisteredClusterCannotResolve(t *testing.T) {
	f := startTLSControlPlane(t)
	client := f.dialAs(t, "cluster-z")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := client.Resolve(ctx, &pb.ResolveRequest{FilePath: "/models/x.bin"})
	if codeOf(err) != codes.FailedPrecondition {
		t.Fatalf("expected FailedPrecondition before registration, got %v", err)
	}
}

// A plaintext client against a TLS listener must not reach the service at
// all. This is what makes AllowInsecureIdentity=false meaningful: the
// header fallback is unreachable because the transport refuses first.
func TestPlaintextClientCannotReachATLSControlPlane(t *testing.T) {
	f := startTLSControlPlane(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	conn, err := grpc.DialContext(ctx, f.addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()
	client := pb.NewGlobalControlPlaneClient(conn)
	cctx, ccancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer ccancel()
	_, err = client.RegisterCluster(cctx, &pb.RegisterClusterRequest{
		ClusterId: "cluster-a", PublishPrefixes: []string{"/models/"},
	})
	if err == nil {
		t.Fatal("a plaintext client registered against a TLS control plane")
	}
	if f.svc.store == nil {
		return
	}
	c, _ := f.svc.store.GetCluster(context.Background(), "cluster-a")
	if c != nil {
		t.Fatal("a plaintext client created a cluster record")
	}
}

// A certificate signed by a different CA is not merely a weaker channel:
// since the cluster ID is read out of the certificate, trusting it would
// let anyone with any CA mint any identity.
func TestCertificateFromAnUntrustedCAIsRejected(t *testing.T) {
	f := startTLSControlPlane(t)
	other, err := mtls.NewTestCA()
	if err != nil {
		t.Fatalf("NewTestCA: %v", err)
	}
	// Client material from the stranger CA, but trusting the real server
	// CA so the failure is the SERVER rejecting the client, not the
	// client rejecting the server.
	cfg, err := other.WriteTo(t.TempDir(), "cluster-a")
	if err != nil {
		t.Fatalf("WriteTo: %v", err)
	}
	cfg.CAFile = f.dir + "/ca.crt"
	cfg.ServerName = "globalcp"
	opt, err := cfg.DialOption()
	if err != nil {
		t.Fatalf("DialOption: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	conn, err := grpc.DialContext(ctx, f.addr, opt)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()
	cctx, ccancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer ccancel()
	_, err = pb.NewGlobalControlPlaneClient(conn).RegisterCluster(cctx,
		&pb.RegisterClusterRequest{ClusterId: "cluster-a"})
	if err == nil {
		t.Fatal("a certificate from an untrusted CA was accepted")
	}
}

// waitForRows polls the store rather than sleeping: a publish rides the
// stream asynchronously, so there is no return value to wait on, and a
// fixed sleep would race the server's recv loop under -race.
func waitForRows(t *testing.T, svc *Service, path string, want int) []*pb.GlobalLocation {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		rows, err := svc.store.GetLocations(context.Background(), path)
		if err == nil && len(rows) >= want {
			return rows
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %d row(s) at %s", want, path)
	return nil
}
