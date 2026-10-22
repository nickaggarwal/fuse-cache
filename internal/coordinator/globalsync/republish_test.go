package globalsync

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httptest"
	"sort"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"fuse-client/internal/coordinator"
	"fuse-client/internal/globalcp"
	"fuse-client/internal/pb"
)

// restartableCP is a control plane on a fixed loopback address that can be
// killed and replaced by a FRESH service -- empty store, no registrations --
// which is what a restart of the in-memory production CP looks like.
type restartableCP struct {
	t    *testing.T
	addr string
	mu   sync.Mutex
	svc  *globalcp.Service
	srv  *grpc.Server
}

func startRestartableCP(t *testing.T) *restartableCP {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	cp := &restartableCP{t: t, addr: lis.Addr().String()}
	cp.serve(lis)
	t.Cleanup(cp.stop)
	return cp
}

func (cp *restartableCP) serve(lis net.Listener) {
	svc := globalcp.NewService(globalcp.Config{
		Logger:                log.New(io.Discard, "", 0),
		AllowInsecureIdentity: true,
	})
	srv := grpc.NewServer()
	pb.RegisterGlobalControlPlaneServer(srv, svc)
	go func() { _ = srv.Serve(lis) }()
	cp.mu.Lock()
	cp.svc, cp.srv = svc, srv
	cp.mu.Unlock()
}

func (cp *restartableCP) stop() {
	cp.mu.Lock()
	srv := cp.srv
	cp.mu.Unlock()
	if srv != nil {
		srv.Stop()
	}
}

// restart stops the server and brings up a new, empty one on the same port.
func (cp *restartableCP) restart() {
	cp.t.Helper()
	cp.stop()
	lis, err := net.Listen("tcp", cp.addr)
	if err != nil {
		cp.t.Fatalf("relisten %s: %v", cp.addr, err)
	}
	cp.serve(lis)
}

func (cp *restartableCP) indexPaths(cluster string) []string {
	cp.mu.Lock()
	svc := cp.svc
	cp.mu.Unlock()
	rows, _ := svc.Store().ListLocations(context.Background(), []string{"/"})
	var out []string
	for _, r := range rows {
		if r.GetClusterId() == cluster {
			out = append(out, r.GetFilePath())
		}
	}
	sort.Strings(out)
	return out
}

// startLoopbackAgent runs an agent against a loopback CP. Republish spacing
// is off so a test never waits out the production interval.
func startLoopbackAgent(t *testing.T, cp *restartableCP, cfg Config) *Agent {
	t.Helper()
	cfg.Endpoint = cp.addr
	cfg.DialOptions = []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}
	if cfg.Logger == nil {
		cfg.Logger = log.New(io.Discard, "", 0)
	}
	if cfg.RepublishMinInterval == 0 {
		cfg.RepublishMinInterval = -1
	}
	a, err := New(cfg)
	if err != nil {
		t.Fatalf("new agent: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	a.Start(ctx)
	t.Cleanup(func() { cancel(); a.Stop() })
	return a
}

// waitLong is waitFor with room for the agent's jittered reconnect backoff
// (up to ~5s across two attempts).
func waitLong(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func cloudRow(path string, size int64) *coordinator.FileLocation {
	return &coordinator.FileLocation{
		FilePath: path, PeerID: "node-0", StorageTier: "cloud", FileSize: size,
		LastAccessed: time.Now(),
	}
}

// TestRepublishRestoresIndexAfterControlPlaneRestart is I3's headline
// acceptance test. A cluster publishes a file, the control plane restarts
// with an empty in-memory store, and NOTHING is rewritten. Before republish,
// B's mirror was replaced by the new, empty snapshot on reconnect and the
// path was undiscoverable until someone rewrote it.
func TestRepublishRestoresIndexAfterControlPlaneRestart(t *testing.T) {
	cp := startRestartableCP(t)
	coordA := coordinator.NewCoordinatorService()

	a := startLoopbackAgent(t, cp, Config{
		ClusterID:       "cluster-a",
		Object:          &pb.ObjectRef{Provider: "s3", Bucket: "shared", KeyPrefix: "a"},
		PublishPrefixes: []string{"/models"},
		Locations:       coordA,
	})
	b := startLoopbackAgent(t, cp, Config{
		ClusterID:         "cluster-b",
		Object:            &pb.ObjectRef{Provider: "s3", Bucket: "shared", KeyPrefix: "b"},
		SubscribePrefixes: []string{"/models"},
	})
	coordA.SetGlobalPublisher(a)
	waitFor(t, "both connected", func() bool { return a.Status().Connected && b.Status().Mirror.Ready })

	// The live path: UpdateFileLocation -> OnCloudPersist.
	if err := coordA.UpdateFileLocation(context.Background(), &coordinator.FileLocation{
		FilePath: "/models/llama.bin", PeerID: "node-0", StorageTier: "cloud",
		FileSize: 4096, IsChunked: true, ChunkSize: 1024, LastAccessed: time.Now(),
	}); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "b to learn the row", func() bool { return len(b.Mirror().Lookup("/models/llama.bin")) == 1 })
	waitFor(t, "a's first full republish", func() bool { return !a.Status().LastFullRepublishAt.IsZero() })
	firstFull := a.Status().LastFullRepublishAt

	restartAt := time.Now()
	cp.restart()
	if got := cp.indexPaths("cluster-a"); len(got) != 0 {
		t.Fatalf("restarted CP should start empty, has %v", got)
	}

	waitLong(t, "a to reconnect and finish a new full republish", func() bool {
		st := a.Status()
		return st.Connected && st.LastFullRepublishAt.After(firstFull)
	})
	// Connected AND synced since the restart. Reconnects alone is not
	// enough: it ticks when the old session ends, while the mirror still
	// serves last-good contents that include the row.
	waitLong(t, "b's mirror to converge back", func() bool {
		st := b.Status()
		return st.Connected && st.Mirror.LastSyncAt.After(restartAt) &&
			len(b.Mirror().Lookup("/models/llama.bin")) == 1
	})

	got := b.Mirror().Lookup("/models/llama.bin")[0]
	if got.GetClusterId() != "cluster-a" || got.GetObject().GetKeyPrefix() != "a" ||
		got.GetFileSize() != 4096 || !got.GetIsChunked() || got.GetChunkSize() != 1024 {
		t.Fatalf("republished row lost its shape: %v", got)
	}
	if st := a.Status(); st.Republish.Full < 2 || st.Republish.LastError != "" {
		t.Fatalf("republish status after restart: %+v", st.Republish)
	}
}

// TestRepublishHealsQueueOverflow: while the stream is down the publish
// queue drops the oldest intents past its depth. Those rows are still in the
// local location table, so the reconnect's republish must restore them --
// with the SAME control plane (no restart), which is the case a persistent
// CP store alone would not fix.
func TestRepublishHealsQueueOverflow(t *testing.T) {
	cp := startRestartableCP(t)
	coordA := coordinator.NewCoordinatorService()
	a := startLoopbackAgent(t, cp, Config{
		ClusterID:           "cluster-a",
		Object:              &pb.ObjectRef{Provider: "s3", Bucket: "shared", KeyPrefix: "a"},
		PublishPrefixes:     []string{"/models"},
		Locations:           coordA,
		RepublishRowsPerSec: 100000,
	})
	coordA.SetGlobalPublisher(a)
	waitFor(t, "a's first full republish", func() bool { return !a.Status().LastFullRepublishAt.IsZero() })
	firstFull := a.Status().LastFullRepublishAt

	// Kill the server but keep the port, so the agent cannot drain.
	cp.mu.Lock()
	oldSvc := cp.svc
	cp.mu.Unlock()
	cp.stop()
	waitFor(t, "a to notice the disconnect", func() bool { return !a.Status().Connected })

	const overflow = 25
	total := publishQueueDepth + overflow
	for i := 0; i < total; i++ {
		if err := coordA.UpdateFileLocation(context.Background(),
			cloudRow(fmt.Sprintf("/models/f%05d.bin", i), int64(i))); err != nil {
			t.Fatal(err)
		}
	}
	if d := a.Status().QueueDropped; d != overflow {
		t.Fatalf("queue dropped %d, want %d", d, overflow)
	}

	// Bring the SAME service back: its store still holds whatever it had,
	// so only republish can supply the dropped rows.
	lis, err := net.Listen("tcp", cp.addr)
	if err != nil {
		t.Fatalf("relisten: %v", err)
	}
	srv := grpc.NewServer()
	pb.RegisterGlobalControlPlaneServer(srv, oldSvc)
	go func() { _ = srv.Serve(lis) }()
	cp.mu.Lock()
	cp.srv = srv
	cp.mu.Unlock()

	waitLong(t, "a full republish after reconnect", func() bool {
		return a.Status().LastFullRepublishAt.After(firstFull)
	})
	waitLong(t, "every path, including the dropped ones, in the index", func() bool {
		return len(cp.indexPaths("cluster-a")) == total
	})
	got := cp.indexPaths("cluster-a")
	if got[0] != "/models/f00000.bin" {
		t.Fatalf("oldest (dropped) path not restored; first index path %s", got[0])
	}
	if st := a.Status(); st.QueueDroppedWithdrawals != 0 {
		t.Fatalf("no withdrawals were queued, but %d counted as dropped", st.QueueDroppedWithdrawals)
	}
}

// TestRepublishStaysInsideTheAcceptedAllowlist: republish reads the raw
// location table, which holds far more than this cluster publishes. Every
// row it sends must pass the same filter a live publish does. A row outside
// the allowlist would also make the CP kill the stream.
func TestRepublishStaysInsideTheAcceptedAllowlist(t *testing.T) {
	cp := startRestartableCP(t)
	coordA := coordinator.NewCoordinatorService()
	ctx := context.Background()

	// Seeded BEFORE the agent exists: no live publish ever sees these, so
	// anything in the index got there through republish.
	for _, l := range []*coordinator.FileLocation{
		cloudRow("/models/ok.bin", 1),
		cloudRow("/models/sub/deep.bin", 2),
		// Raw string prefix of "/models" but not under it at a boundary.
		cloudRow("/models-v2/leak.bin", 3),
		cloudRow("/secrets/key.pem", 4),
		// Chunk objects never cross (I2).
		cloudRow("/models/big.bin_chunk_0", 5),
		// NVMe-only: no cloud copy, so nothing another cluster can read.
		{FilePath: "/models/local-only.bin", PeerID: "node-0", StorageTier: "nvme", FileSize: 6},
	} {
		if err := coordA.UpdateFileLocation(ctx, l); err != nil {
			t.Fatal(err)
		}
	}
	// A path with an NVMe row FIRST and a cloud row second must still
	// publish: keying off the first replica row would miss it.
	_ = coordA.UpdateFileLocation(ctx, &coordinator.FileLocation{
		FilePath: "/models/mixed.bin", PeerID: "node-1", StorageTier: "nvme", FileSize: 7})
	_ = coordA.UpdateFileLocation(ctx, cloudRow("/models/mixed.bin", 7))

	b := startLoopbackAgent(t, cp, Config{
		ClusterID:         "cluster-b",
		SubscribePrefixes: []string{"/"},
	})
	a := startLoopbackAgent(t, cp, Config{
		ClusterID:       "cluster-a",
		Object:          &pb.ObjectRef{Provider: "s3", Bucket: "shared", KeyPrefix: "a"},
		PublishPrefixes: []string{"/models"},
		Locations:       coordA,
	})

	waitFor(t, "a's full republish", func() bool { return !a.Status().LastFullRepublishAt.IsZero() })
	want := []string{"/models/mixed.bin", "/models/ok.bin", "/models/sub/deep.bin"}
	waitFor(t, "b to learn the republished rows", func() bool {
		return len(b.Mirror().Snapshot()) == len(want)
	})

	if got := cp.indexPaths("cluster-a"); fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("index holds %v, want exactly %v", got, want)
	}
	for _, r := range b.Mirror().Snapshot() {
		if !globalcp.MatchesAny(r.GetFilePath(), []string{"/models"}) {
			t.Fatalf("row outside the allowlist crossed: %s", r.GetFilePath())
		}
	}
	if !a.Status().Connected {
		t.Fatal("a's stream died; republish must filter locally, not let the CP reject it")
	}
	if st := a.Status().Republish; st.LastCandidates != len(want) || st.LastQueued != len(want) {
		t.Fatalf("republish status %+v, want %d candidates and queued", st, len(want))
	}
}

// recordingSource records which prefixes republish asked for.
type recordingSource struct {
	inner  LocationSource
	mu     sync.Mutex
	ranged []string
}

func (r *recordingSource) RangeFileLocations(ctx context.Context, prefix string) (map[string][]*coordinator.FileLocation, error) {
	r.mu.Lock()
	r.ranged = append(r.ranged, prefix)
	r.mu.Unlock()
	return r.inner.RangeFileLocations(ctx, prefix)
}

func (r *recordingSource) prefixes() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.ranged...)
}

// TestRepublishUsesAcceptedNotConfiguredPrefixes: the CP's registration
// answer is the authority. A pass driven by the configured list would range,
// and try to send, prefixes the CP did not grant.
func TestRepublishUsesAcceptedNotConfiguredPrefixes(t *testing.T) {
	coord := coordinator.NewCoordinatorService()
	ctx := context.Background()
	_ = coord.UpdateFileLocation(ctx, cloudRow("/models/a.bin", 1))
	_ = coord.UpdateFileLocation(ctx, cloudRow("/datasets/b.bin", 2))
	src := &recordingSource{inner: coord}

	a, err := New(Config{
		ClusterID:       "cluster-a",
		Endpoint:        "unused",
		PublishPrefixes: []string{"/models", "/datasets"},
		Locations:       src,
		Logger:          log.New(io.Discard, "", 0),
	})
	if err != nil {
		t.Fatal(err)
	}
	// The CP accepted only /models.
	rows, truncated, err := a.collectRepublish(ctx, []string{"/models"})
	if err != nil || truncated {
		t.Fatalf("collect: err=%v truncated=%v", err, truncated)
	}
	if len(rows) != 1 || rows[0].GetFilePath() != "/models/a.bin" {
		t.Fatalf("rows %v, want only /models/a.bin", rows)
	}
	if got := src.prefixes(); len(got) != 1 || got[0] != "/models" {
		t.Fatalf("ranged %v, want only the accepted prefix", got)
	}
}

// TestPureConsumerNeverRangesTheTable: an empty accepted allowlist means
// nothing may be published, so there is nothing to read either.
func TestPureConsumerNeverRangesTheTable(t *testing.T) {
	cp := startRestartableCP(t)
	src := &recordingSource{inner: coordinator.NewCoordinatorService()}
	a := startLoopbackAgent(t, cp, Config{
		ClusterID:         "cluster-b",
		SubscribePrefixes: []string{"/models"},
		Locations:         src,
	})
	waitFor(t, "connected", func() bool { return a.Status().Connected && a.Status().Mirror.Ready })
	if got := src.prefixes(); len(got) != 0 {
		t.Fatalf("pure consumer ranged %v", got)
	}
	if st := a.Status().Republish; !st.Enabled || st.Passes != 0 {
		t.Fatalf("republish status %+v", st)
	}
}

// TestRepublishIsCapped: one pass never sends more than the cap, and a
// truncated pass does not claim to be full.
func TestRepublishIsCapped(t *testing.T) {
	cp := startRestartableCP(t)
	coordA := coordinator.NewCoordinatorService()
	for i := 0; i < 10; i++ {
		_ = coordA.UpdateFileLocation(context.Background(), cloudRow(fmt.Sprintf("/models/f%d.bin", i), 1))
	}
	a := startLoopbackAgent(t, cp, Config{
		ClusterID:        "cluster-a",
		Object:           &pb.ObjectRef{Provider: "s3", Bucket: "shared", KeyPrefix: "a"},
		PublishPrefixes:  []string{"/models"},
		Locations:        coordA,
		RepublishMaxRows: 4,
	})
	waitFor(t, "the pass to finish", func() bool {
		st := a.Status().Republish
		return st.Passes == 1 && !st.Running && !st.LastFinishedAt.IsZero()
	})
	st := a.Status()
	if !st.Republish.LastTruncated || st.Republish.LastQueued != 4 || st.Republish.Full != 0 {
		t.Fatalf("capped pass status %+v", st.Republish)
	}
	if !st.LastFullRepublishAt.IsZero() {
		t.Fatal("a truncated pass set last_full_republish_at")
	}
	waitFor(t, "the capped rows to land", func() bool { return len(cp.indexPaths("cluster-a")) == 4 })
}

func TestRepublishDelaySpacesPasses(t *testing.T) {
	now := time.Now()
	if d := republishDelay(time.Time{}, now, time.Minute); d != 0 {
		t.Fatalf("first pass delayed %v", d)
	}
	if d := republishDelay(now.Add(-10*time.Second), now, 30*time.Second); d != 20*time.Second {
		t.Fatalf("delay %v, want 20s", d)
	}
	if d := republishDelay(now.Add(-time.Hour), now, 30*time.Second); d != 0 {
		t.Fatalf("overdue pass delayed %v", d)
	}
	if d := republishDelay(now, now, 0); d != 0 {
		t.Fatalf("disabled spacing delayed %v", d)
	}
}

// TestStatusEndpointReportsRepublishAndDrops pins the observability fields
// on /api/global/status.
func TestStatusEndpointReportsRepublishAndDrops(t *testing.T) {
	cp := startRestartableCP(t)
	coordA := coordinator.NewCoordinatorService()
	_ = coordA.UpdateFileLocation(context.Background(), cloudRow("/models/a.bin", 1))
	a := startLoopbackAgent(t, cp, Config{
		ClusterID:       "cluster-a",
		Object:          &pb.ObjectRef{Provider: "s3", Bucket: "shared", KeyPrefix: "a"},
		PublishPrefixes: []string{"/models"},
		Locations:       coordA,
	})
	waitFor(t, "full republish", func() bool { return !a.Status().LastFullRepublishAt.IsZero() })

	mux := http.NewServeMux()
	RegisterRoutes(mux, a)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/global/status", nil))

	var body struct {
		Status map[string]any `json:"status"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode: %v (%s)", err, rec.Body.String())
	}
	for _, k := range []string{"last_full_republish_at", "queue_dropped", "queue_dropped_withdrawals", "republish"} {
		if _, ok := body.Status[k]; !ok {
			t.Fatalf("status missing %q: %s", k, rec.Body.String())
		}
	}
	rep := body.Status["republish"].(map[string]any)
	if rep["enabled"] != true || rep["full_passes_total"].(float64) < 1 {
		t.Fatalf("republish block %v", rep)
	}
}
