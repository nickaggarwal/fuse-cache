package globalcpe2e

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

	"fuse-client/internal/cache"
	"fuse-client/internal/coordinator"
	"fuse-client/internal/coordinator/globalsync"
	"fuse-client/internal/globalcp"
	"fuse-client/internal/pb"
)

// controlPlane is a real globalcp.Service behind a real gRPC server.
//
// bufconn rather than a loopback port so the suite cannot flake on a port
// collision, and stop/restart is deterministic: the autonomy test kills the
// control plane and must observe the consequence, which a TIME_WAIT race
// would make intermittent.
type controlPlane struct {
	svc  *globalcp.Service
	lis  *bufconn.Listener
	grpc *grpc.Server
}

func startControlPlane(t *testing.T) *controlPlane {
	t.Helper()
	cp := &controlPlane{
		svc: globalcp.NewService(globalcp.Config{
			Logger: log.New(io.Discard, "", 0),
			// No certificates on bufconn, so identity comes from the
			// header -- the same embedded/dev posture cmd/coordinator uses
			// on loopback. The authorization tests that need a forged
			// identity live in internal/globalcp, where a TLS listener is
			// available; this suite is about bytes, not about authz.
			AllowInsecureIdentity: true,
		}),
		lis: bufconn.Listen(1 << 20),
	}
	cp.grpc = grpc.NewServer()
	pb.RegisterGlobalControlPlaneServer(cp.grpc, cp.svc)
	go func() { _ = cp.grpc.Serve(cp.lis) }()
	t.Cleanup(cp.stop)
	return cp
}

func (cp *controlPlane) dialOptions() []grpc.DialOption {
	return []grpc.DialOption{
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return cp.lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}
}

func (cp *controlPlane) stop() {
	cp.grpc.Stop()
	_ = cp.lis.Close()
}

// cluster is one complete, self-sufficient coordinated cache: its own
// coordinator, its own sync agent, its own cloud backend scoped to its own
// key prefix, and its own cache manager.
//
// Everything here is the production type. The only substitution in the whole
// federation is the object store the cloud backends point at.
type cluster struct {
	id    string
	coord *coordinator.CoordinatorService
	agent *globalsync.Agent
	cm    *cache.DefaultCacheManager

	// prefix is this cluster's object-key namespace inside the shared
	// bucket. Two clusters sharing a bucket MUST differ here; the layout
	// test is what makes that visible rather than assumed.
	prefix string
}

type clusterConfig struct {
	id        string
	prefix    string
	publish   []string
	subscribe []string
	// globalRead is the data-plane switch. Off by default, exactly as the
	// flag is, so the suite can assert the feature is inert when unset.
	globalRead bool
}

const sharedBucket = "shared-federation-bucket"

// newCluster wires one cluster against the shared object store and joins it
// to the control plane.
func newCluster(t *testing.T, cp *controlPlane, store *s3Server, cfg clusterConfig) *cluster {
	t.Helper()

	coord := coordinator.NewCoordinatorService()

	agent, err := globalsync.New(globalsync.Config{
		ClusterID: cfg.id,
		Endpoint:  "bufnet",
		Object: &pb.ObjectRef{
			Provider:  "s3",
			Bucket:    sharedBucket,
			KeyPrefix: cfg.prefix,
		},
		PublishPrefixes:   cfg.publish,
		SubscribePrefixes: cfg.subscribe,
		DialOptions:       cp.dialOptions(),
		Logger:            log.New(io.Discard, "", 0),
	})
	if err != nil {
		t.Fatalf("%s: sync agent: %v", cfg.id, err)
	}
	coord.SetGlobalPublisher(agent)
	coord.SetGlobalMirror(agent.Mirror())

	ctx, cancel := context.WithCancel(context.Background())
	agent.Start(ctx)
	t.Cleanup(func() {
		cancel()
		agent.Stop()
	})

	// The real constructor, not a hand-assembled struct. It is what builds
	// the cloud backend from config, applies ScopeCloudKeys, and wires the
	// coordinator in -- the exact sequence cmd/client performs. Assembling
	// the manager by hand here would skip that wiring and test the
	// assembly instead of the product.
	cm, err := cache.NewCacheManager(&cache.CacheConfig{
		NVMePath:    t.TempDir(),
		MaxNVMeSize: 64 << 20,
		ChunkSize:   8 << 20,
		// No Coordinator on the cache config would give it a no-op peer
		// tier; passing the real one means a global read is reached only
		// after the peer tier has genuinely missed, as in production.
		Coordinator:   coord,
		LocalPeerID:   cfg.id + "-node-0",
		CloudProvider: "s3",
		S3Bucket:      sharedBucket,
		S3Region:      "us-east-1",
		S3Endpoint:    store.URL,
		// Path-style: the test server is addressed by host:port, and
		// virtual-host style would put the bucket in a DNS name that does
		// not resolve.
		S3ForcePathStyle: true,
		// This cluster's object-key namespace inside the shared bucket.
		// Two clusters sharing a bucket MUST differ here.
		CloudKeyPrefix: cfg.prefix,
		CloudTimeout:   10 * time.Second,
		// One attempt: a test that waits out five exponential retries on
		// every intended miss spends its time in time.Sleep.
		CloudRetryCount:    1,
		CloudRetryBaseWait: time.Millisecond,
		GlobalReadEnabled:  cfg.globalRead,
	})
	if err != nil {
		t.Fatalf("%s: cache manager: %v", cfg.id, err)
	}
	t.Cleanup(cm.Shutdown)

	return &cluster{
		id:     cfg.id,
		coord:  coord,
		agent:  agent,
		cm:     cm,
		prefix: cfg.prefix,
	}
}

// writeAndPublish performs what a cluster does when a file reaches object
// storage: write the bytes under this cluster's prefix, then record a
// cloud-tier location with its own coordinator.
//
// The publish is a CONSEQUENCE of the location write, not a separate step --
// UpdateFileLocation is what calls OnCloudPersist. Writing it this way is
// deliberate: a test that poked the agent directly would pass even if the
// coordinator stopped notifying it, which is the wiring most likely to be
// broken by a refactor and the least likely to be noticed.
func (c *cluster) writeAndPublish(t *testing.T, path string, data []byte) {
	t.Helper()
	if err := c.cm.WriteCloud(context.Background(), path, data); err != nil {
		t.Fatalf("%s: cloud write %s: %v", c.id, path, err)
	}
	c.publishLocation(t, path, int64(len(data)))
}

// publishLocation records a cloud-tier location without writing bytes. Used
// by the test that proves the index can go stale.
func (c *cluster) publishLocation(t *testing.T, path string, size int64) {
	t.Helper()
	err := c.coord.UpdateFileLocation(context.Background(), &coordinator.FileLocation{
		FilePath:    path,
		PeerID:      c.id + "-node-0",
		StorageTier: "cloud",
		FileSize:    size,
	})
	if err != nil {
		t.Fatalf("%s: update location %s: %v", c.id, path, err)
	}
}

// writeNVMeOnly records a local-tier location: bytes that exist in this
// cluster and are NOT in object storage.
func (c *cluster) writeNVMeOnly(t *testing.T, path string, size int64) {
	t.Helper()
	err := c.coord.UpdateFileLocation(context.Background(), &coordinator.FileLocation{
		FilePath:    path,
		PeerID:      c.id + "-node-0",
		StorageTier: "nvme",
		FileSize:    size,
	})
	if err != nil {
		t.Fatalf("%s: update location %s: %v", c.id, path, err)
	}
}

// read issues a read through the REAL public entry point -- the whole tier
// fallback, NVMe then peer then cloud, with the cross-cluster path reachable
// only where production puts it: after this cluster's own cloud read has
// missed.
//
// Calling getFromGlobal directly would be a smaller test of a bigger claim.
// The claim is that an ordinary read finds another cluster's bytes, and the
// branch that makes that happen sits behind two tiers that have to miss
// first.
func (c *cluster) read(path string) ([]byte, bool) {
	entry, err := c.cm.Get(context.Background(), path)
	if err != nil {
		return nil, false
	}
	return entry.Data, true
}

// waitFor polls a condition rather than sleeping past a deadline. The
// publish path is coalesced and batched by design, so the arrival time is
// not a fixed number -- keying off observed state is causal where a sleep
// would be a race the suite would lose about one run in three.
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

// waitReady blocks until the agent has completed registration with the
// control plane.
//
// This is load-bearing, not hygiene. The publish allowlist is the CP's
// ACCEPTED answer from RegisterCluster, so before registration lands it is
// empty and OnCloudPersist drops EVERYTHING. A test that writes before this
// point and then asserts "nothing was published" passes no matter what the
// publish gating does -- which is exactly how the NVMe-only test passed
// under a mutation that deleted the tier check entirely.
func (c *cluster) waitReady(t *testing.T) {
	t.Helper()
	waitFor(t, c.id+" to register with the control plane", func() bool {
		st := c.agent.Status()
		return st.Connected && len(st.AcceptedPrefixes) > 0
	})
}

// sees reports whether this cluster's mirror knows path.
func (c *cluster) sees(path string) bool {
	locs, _ := c.agent.Mirror().Resolve(path)
	return len(locs) > 0
}

// newEntry is the shape a local write takes on the public cache API.
func newEntry(path string, data []byte) *cache.CacheEntry {
	return &cache.CacheEntry{
		FilePath:     path,
		StoragePath:  path,
		Size:         int64(len(data)),
		LastAccessed: time.Now(),
		Tier:         cache.TierNVMe,
		Data:         data,
	}
}
