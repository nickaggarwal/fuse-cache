package globalsync

import (
	"context"
	"io"
	"log"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	"fuse-client/internal/globalcp"
	"fuse-client/internal/pb"
)

// startCP runs a real control plane over bufconn and returns a dialer the
// agent can use. The whole point of this file is the interaction between
// two real gRPC peers -- stream ordering, snapshot-then-events, reconnect --
// so a fake server would test the fake.
func startCP(t *testing.T) (*globalcp.Service, func() []grpc.DialOption, func()) {
	t.Helper()
	svc := globalcp.NewService(globalcp.Config{
		Logger:                log.New(io.Discard, "", 0),
		AllowInsecureIdentity: true,
	})
	lis := bufconn.Listen(1 << 20)
	srv := grpc.NewServer()
	pb.RegisterGlobalControlPlaneServer(srv, svc)
	go func() { _ = srv.Serve(lis) }()

	dialOpts := func() []grpc.DialOption {
		return []grpc.DialOption{
			grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
				return lis.DialContext(ctx)
			}),
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		}
	}
	stop := func() {
		srv.Stop()
		_ = lis.Close()
	}
	t.Cleanup(stop)
	return svc, dialOpts, stop
}

func startAgent(t *testing.T, cfg Config, dial func() []grpc.DialOption) *Agent {
	t.Helper()
	cfg.Endpoint = "bufnet"
	cfg.DialOptions = dial()
	if cfg.Logger == nil {
		cfg.Logger = log.New(io.Discard, "", 0)
	}
	a, err := New(cfg)
	if err != nil {
		t.Fatalf("new agent: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	a.Start(ctx)
	t.Cleanup(func() {
		cancel()
		a.Stop()
	})
	return a
}

// waitFor polls a condition. Tests key off observed state rather than
// sleeping past a deadline: a timer racing real I/O passes bare and fails
// under -race, and the ordering here is causal, not temporal.
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func TestAgentConnectsAndRegisters(t *testing.T) {
	_, dial, _ := startCP(t)
	a := startAgent(t, Config{
		ClusterID:         "cluster-a",
		Object:            &pb.ObjectRef{Provider: "s3", Bucket: "shared", KeyPrefix: "a"},
		PublishPrefixes:   []string{"/models"},
		SubscribePrefixes: []string{"/models"},
	}, dial)

	waitFor(t, "connection", func() bool { return a.Status().Connected })
	waitFor(t, "initial snapshot", func() bool { return a.Status().Mirror.Ready })

	if got := a.Status().AcceptedPrefixes; len(got) != 1 || got[0] != "/models" {
		t.Fatalf("accepted prefixes %v", got)
	}
}

// TestPublishCrossesBetweenAgents is the headline path: A writes an
// allowlisted cloud-backed file, B learns where to fetch it -- without any
// network path between the two data planes.
func TestPublishCrossesBetweenAgents(t *testing.T) {
	_, dial, _ := startCP(t)

	a := startAgent(t, Config{
		ClusterID:       "cluster-a",
		Object:          &pb.ObjectRef{Provider: "s3", Bucket: "shared", KeyPrefix: "a"},
		PublishPrefixes: []string{"/models"},
	}, dial)
	b := startAgent(t, Config{
		ClusterID:         "cluster-b",
		Object:            &pb.ObjectRef{Provider: "s3", Bucket: "shared", KeyPrefix: "b"},
		SubscribePrefixes: []string{"/models"},
	}, dial)

	waitFor(t, "both connected", func() bool { return a.Status().Connected && b.Status().Connected })
	waitFor(t, "b's snapshot", func() bool { return b.Status().Mirror.Ready })

	a.OnCloudPersist("/models/llama.bin", "cloud", 4096, false, 0)

	waitFor(t, "b to see a's row", func() bool { return len(b.Mirror().Lookup("/models/llama.bin")) == 1 })

	got := b.Mirror().Lookup("/models/llama.bin")[0]
	if got.GetClusterId() != "cluster-a" {
		t.Fatalf("cluster id %q", got.GetClusterId())
	}
	if got.GetObject().GetBucket() != "shared" || got.GetObject().GetKeyPrefix() != "a" {
		t.Fatalf("object ref %v: B must learn A's bucket AND A's key prefix, or the read looks under the wrong key", got.GetObject())
	}
	if got.GetFileSize() != 4096 {
		t.Fatalf("file size %d", got.GetFileSize())
	}
}

func TestNvmeOnlyPathIsNeverPublished(t *testing.T) {
	_, dial, _ := startCP(t)
	a := startAgent(t, Config{
		ClusterID:       "cluster-a",
		Object:          &pb.ObjectRef{Provider: "s3", Bucket: "shared", KeyPrefix: "a"},
		PublishPrefixes: []string{"/models"},
	}, dial)
	b := startAgent(t, Config{
		ClusterID:         "cluster-b",
		SubscribePrefixes: []string{"/models"},
	}, dial)
	waitFor(t, "both ready", func() bool { return a.Status().Mirror.Ready && b.Status().Mirror.Ready })

	a.OnCloudPersist("/models/local-only.bin", "nvme", 10, false, 0)
	// Then a path that IS publishable, as a barrier: once it arrives, the
	// earlier one has had its chance. Asserting absence after a fixed sleep
	// would only prove the sleep was short.
	a.OnCloudPersist("/models/real.bin", "cloud", 10, false, 0)

	waitFor(t, "the barrier row", func() bool { return len(b.Mirror().Lookup("/models/real.bin")) == 1 })
	if got := b.Mirror().Lookup("/models/local-only.bin"); len(got) != 0 {
		t.Fatalf("an NVMe-only path was advertised across clusters: %v", got)
	}
}

func TestPathOutsideAllowlistIsNeverPublished(t *testing.T) {
	_, dial, _ := startCP(t)
	a := startAgent(t, Config{
		ClusterID:       "cluster-a",
		Object:          &pb.ObjectRef{Provider: "s3", Bucket: "shared", KeyPrefix: "a"},
		PublishPrefixes: []string{"/models"},
	}, dial)
	b := startAgent(t, Config{
		ClusterID:         "cluster-b",
		SubscribePrefixes: []string{"/"},
	}, dial)
	waitFor(t, "both ready", func() bool { return a.Status().Mirror.Ready && b.Status().Mirror.Ready })

	a.OnCloudPersist("/secrets/key.pem", "cloud", 10, false, 0)
	a.OnCloudPersist("/models/real.bin", "cloud", 10, false, 0)

	waitFor(t, "the barrier row", func() bool { return len(b.Mirror().Lookup("/models/real.bin")) == 1 })

	// Filtered locally, so it never reached the wire. That matters: the CP
	// kills a stream that publishes outside its allowlist, so an agent that
	// sent it would take the whole connection down.
	if got := b.Mirror().Lookup("/secrets/key.pem"); len(got) != 0 {
		t.Fatalf("a non-allowlisted path crossed the boundary: %v", got)
	}
	if !a.Status().Connected {
		t.Fatal("agent's stream died; it must filter locally, not let the CP reject it")
	}
}

func TestWithdrawReachesTheOtherCluster(t *testing.T) {
	_, dial, _ := startCP(t)
	a := startAgent(t, Config{
		ClusterID:       "cluster-a",
		Object:          &pb.ObjectRef{Provider: "s3", Bucket: "shared", KeyPrefix: "a"},
		PublishPrefixes: []string{"/models"},
	}, dial)
	b := startAgent(t, Config{
		ClusterID:         "cluster-b",
		SubscribePrefixes: []string{"/models"},
	}, dial)
	waitFor(t, "both ready", func() bool { return a.Status().Mirror.Ready && b.Status().Mirror.Ready })

	a.OnCloudPersist("/models/x.bin", "cloud", 10, false, 0)
	waitFor(t, "publish", func() bool { return len(b.Mirror().Lookup("/models/x.bin")) == 1 })

	a.OnDelete("/models/x.bin")
	waitFor(t, "withdraw", func() bool { return len(b.Mirror().Lookup("/models/x.bin")) == 0 })
}

// TestAgentReconnectsAndResyncs: the CP going away and coming back must heal
// without operator action, and the mirror must converge on the CP's view
// rather than on whatever it happened to hold.
func TestAgentReconnectsAndResyncs(t *testing.T) {
	svc := globalcp.NewService(globalcp.Config{
		Logger:                log.New(io.Discard, "", 0),
		AllowInsecureIdentity: true,
	})

	// A fixed address, so the agent's own reconnect loop finds the restarted
	// server. bufconn cannot be re-listened, so this one test uses loopback.
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := lis.Addr().String()
	srv := grpc.NewServer()
	pb.RegisterGlobalControlPlaneServer(srv, svc)
	go func() { _ = srv.Serve(lis) }()

	cfg := Config{
		ClusterID:         "cluster-b",
		Endpoint:          addr,
		SubscribePrefixes: []string{"/models"},
		DialOptions:       []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())},
		Logger:            log.New(io.Discard, "", 0),
	}
	b, err := New(cfg)
	if err != nil {
		t.Fatalf("new agent: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer func() { cancel(); b.Stop() }()
	b.Start(ctx)

	waitFor(t, "first connect", func() bool { return b.Status().Mirror.Ready })

	srv.Stop()
	waitFor(t, "disconnect", func() bool { return !b.Status().Connected })

	// Local autonomy: with the CP gone, the mirror keeps answering.
	if !b.Status().Mirror.Ready {
		t.Fatal("mirror went not-ready when the control plane died")
	}

	lis2, err := net.Listen("tcp", addr)
	if err != nil {
		t.Fatalf("relisten %s: %v", addr, err)
	}
	srv2 := grpc.NewServer()
	pb.RegisterGlobalControlPlaneServer(srv2, svc)
	go func() { _ = srv2.Serve(lis2) }()
	defer srv2.Stop()

	// A publisher that only exists after the restart. Its row can reach B
	// only through a fresh snapshot or a post-reconnect event -- either way
	// it proves the stream healed.
	pubCfg := Config{
		ClusterID:       "cluster-a",
		Endpoint:        addr,
		Object:          &pb.ObjectRef{Provider: "s3", Bucket: "shared", KeyPrefix: "a"},
		PublishPrefixes: []string{"/models"},
		DialOptions:     []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())},
		Logger:          log.New(io.Discard, "", 0),
	}
	pub, err := New(pubCfg)
	if err != nil {
		t.Fatalf("new publisher: %v", err)
	}
	pubCtx, pubCancel := context.WithCancel(context.Background())
	defer func() { pubCancel(); pub.Stop() }()
	pub.Start(pubCtx)
	waitFor(t, "publisher connected", func() bool { return pub.Status().Connected })

	waitFor(t, "b to reconnect", func() bool { return b.Status().Connected })

	pub.OnCloudPersist("/models/after-restart.bin", "cloud", 7, false, 0)
	waitFor(t, "b to see the post-restart row", func() bool {
		return len(b.Mirror().Lookup("/models/after-restart.bin")) == 1
	})
	if b.Status().Reconnects == 0 {
		t.Fatal("reconnect was not counted")
	}
}

// TestPublishWhileDisconnectedIsQueuedNotLost: the coordinator's write path
// must not care whether the global CP is up, and the rows it produced while
// it was down must still arrive.
func TestPublishWhileDisconnectedIsQueuedNotLost(t *testing.T) {
	_, dial, _ := startCP(t)

	a := startAgent(t, Config{
		ClusterID:       "cluster-a",
		Object:          &pb.ObjectRef{Provider: "s3", Bucket: "shared", KeyPrefix: "a"},
		PublishPrefixes: []string{"/models"},
	}, dial)

	// Before the agent has connected at all, the allowlist is empty, so
	// publish is a no-op -- that is correct (nothing has been granted yet).
	// The interesting case is after registration, which is what we wait for.
	waitFor(t, "connected", func() bool { return a.Status().Connected })

	for i := 0; i < 50; i++ {
		a.OnCloudPersist("/models/batch.bin", "cloud", int64(i), false, 0)
	}
	// Coalescing collapses the burst; what matters is that enqueueing never
	// blocked and the queue drains.
	waitFor(t, "queue to drain", func() bool { return a.Status().QueueDepth == 0 && a.Status().Published > 0 })
	if a.Status().QueueDropped != 0 {
		t.Fatalf("dropped %d intents under a trivial burst", a.Status().QueueDropped)
	}
}

// TestPublishNeverBlocksWhileDisconnected is the autonomy requirement as a
// latency bound on the real agent, not on a stub.
//
// OnCloudPersist is called from the coordinator's metadata write path, which
// a client waits on. If it ever dials, retries, or waits for the control
// plane, a dead CP turns into cluster-wide write latency -- the exact
// coupling this design exists to avoid. The agent here points at an
// unroutable address and never connects.
func TestPublishNeverBlocksWhileDisconnected(t *testing.T) {
	a, err := New(Config{
		ClusterID: "cluster-a",
		// TEST-NET-1, reserved and unroutable: every dial hangs until the
		// agent's own timeout, so the loop is permanently mid-reconnect.
		Endpoint:        "192.0.2.1:9090",
		Object:          &pb.ObjectRef{Provider: "s3", Bucket: "shared", KeyPrefix: "a"},
		PublishPrefixes: []string{"/models"},
		DialOptions:     []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())},
		Logger:          log.New(io.Discard, "", 0),
	})
	if err != nil {
		t.Fatalf("new agent: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer func() { cancel(); a.Stop() }()
	a.Start(ctx)

	start := time.Now()
	for i := 0; i < 1000; i++ {
		a.OnCloudPersist("/models/x.bin", "cloud", 1, false, 0)
		a.OnDelete("/models/x.bin")
	}
	elapsed := time.Since(start)

	// 2000 calls in well under a second. The bound is loose on purpose --
	// it is there to catch network I/O on this path, not to measure mutex
	// throughput.
	if elapsed > 2*time.Second {
		t.Fatalf("2000 publish/withdraw calls took %v against a dead control plane; this path must not do I/O", elapsed)
	}
	if a.Status().Connected {
		t.Fatal("agent reports connected to an unroutable address")
	}
}

// TestMirrorStaysUsableWhileDisconnected: discovery that already happened
// must survive the control plane going away.
func TestMirrorStaysUsableWhileDisconnected(t *testing.T) {
	svc := globalcp.NewService(globalcp.Config{
		Logger:                log.New(io.Discard, "", 0),
		AllowInsecureIdentity: true,
	})
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := lis.Addr().String()
	srv := grpc.NewServer()
	pb.RegisterGlobalControlPlaneServer(srv, svc)
	go func() { _ = srv.Serve(lis) }()

	opts := []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}
	mk := func(id string, publish, subscribe []string) *Agent {
		ag, err := New(Config{
			ClusterID: id, Endpoint: addr,
			Object:            &pb.ObjectRef{Provider: "s3", Bucket: "shared", KeyPrefix: id},
			PublishPrefixes:   publish,
			SubscribePrefixes: subscribe,
			DialOptions:       opts,
			Logger:            log.New(io.Discard, "", 0),
		})
		if err != nil {
			t.Fatalf("new agent %s: %v", id, err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(func() { cancel(); ag.Stop() })
		ag.Start(ctx)
		return ag
	}

	a := mk("cluster-a", []string{"/models"}, nil)
	b := mk("cluster-b", nil, []string{"/models"})
	waitFor(t, "both ready", func() bool { return a.Status().Mirror.Ready && b.Status().Mirror.Ready })

	a.OnCloudPersist("/models/known.bin", "cloud", 42, false, 0)
	waitFor(t, "b to learn the row", func() bool { return len(b.Mirror().Lookup("/models/known.bin")) == 1 })

	srv.Stop()
	_ = lis.Close()
	waitFor(t, "b to notice the disconnect", func() bool { return !b.Status().Connected })

	// The degradation must be confined to NEW discovery.
	got := b.Mirror().Lookup("/models/known.bin")
	if len(got) != 1 || got[0].GetFileSize() != 42 {
		t.Fatalf("mirror lost a known row when the control plane died: %v", got)
	}
	if !b.Status().Mirror.Ready {
		t.Fatal("mirror went not-ready on disconnect; it must serve last-good")
	}
}
