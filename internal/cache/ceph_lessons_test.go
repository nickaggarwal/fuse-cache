package cache

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"fuse-client/internal/coordinator"
)

// ---- health codes (#6) ----

func checkCodes(h CacheHealth) map[string]HealthCheck {
	out := map[string]HealthCheck{}
	for _, c := range h.Checks {
		out[c.Code] = c
	}
	return out
}

func TestHealthCodesAndMute(t *testing.T) {
	cm := evictTestManager(t, 1<<30)
	cm.warmIntents = newWarmIntentRegistry()
	cm.warmIntents.register("v", "/m", WarmupOptions{Mode: "full"})
	for i := 0; i < 4; i++ {
		cm.warmIntents.recordPass("v", &WarmupResult{Files: 2, Warmed: 1, AlreadyLocal: 1}, nil, time.Now())
	}
	h := cm.Health()
	c, ok := checkCodes(h)[HealthRewarmStreak]
	if !ok || c.Severity != "thrashing" || h.State != "thrashing" || c.Count != 3 {
		t.Fatalf("health = %+v", h)
	}
	if len(h.Reasons) != len(h.Checks) {
		t.Fatal("reasons no longer mirror checks")
	}
	cm.MuteHealth(HealthRewarmStreak, time.Hour)
	h = cm.Health()
	if h.State != "ok" || !checkCodes(h)[HealthRewarmStreak].Muted || len(h.Muted) != 1 {
		t.Fatalf("muted check still raised state: %+v", h)
	}
	cm.MuteHealth(HealthRewarmStreak, 0)
	if cm.Health().State != "thrashing" {
		t.Fatal("unmute did not restore the state")
	}
	raw, _ := json.Marshal(cm.Health())
	if !strings.Contains(string(raw), `"code":"REWARM_STREAK"`) {
		t.Fatalf("code missing from JSON: %s", raw)
	}
}

// ---- slow ops (#5) ----

func TestSlowOpsTracked(t *testing.T) {
	cm := evictTestManager(t, 1<<30)
	done := cm.ops.start("fetch", "/stuck.bin", "peer")
	cm.ops.mu.Lock()
	for _, op := range cm.ops.inflight {
		op.Started = time.Now().Add(-15 * time.Second)
	}
	cm.ops.mu.Unlock()
	rep := cm.Ops()
	if len(rep.Inflight) != 1 || rep.Inflight[0].Path != "/stuck.bin" || rep.Inflight[0].AgeSec < 14 {
		t.Fatalf("ops = %+v", rep)
	}
	c, ok := checkCodes(cm.Health())[HealthSlowOps]
	if !ok || c.Count != 1 {
		t.Fatal("SLOW_OPS not raised for a 15 s fetch")
	}
	done(nil)
	rep = cm.Ops()
	if len(rep.Inflight) != 0 || len(rep.RecentSlow) != 1 || rep.SlowTotal != 1 {
		t.Fatalf("after completion: %+v", rep)
	}
	if _, ok := checkCodes(cm.Health())[HealthSlowOps]; ok {
		t.Fatal("SLOW_OPS still raised after the fetch finished")
	}
}

// A remote fetch through getFromTier is registered while it runs.
func TestRemoteFetchIsTracked(t *testing.T) {
	cm := evictTestManager(t, 1<<30)
	block := make(chan struct{})
	cm.peerStorage = &blockingStorage{mockStorage: newMockStorage(), block: block}
	go cm.getFromTier(context.Background(), "/slow.bin", TierPeer)
	deadline := time.Now().Add(2 * time.Second)
	for len(cm.Ops().Inflight) == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	in := cm.Ops().Inflight
	close(block)
	if len(in) != 1 || in[0].Tier != "peer" || in[0].Path != "/slow.bin" {
		t.Fatalf("inflight = %+v", in)
	}
}

type blockingStorage struct {
	*mockStorage
	block chan struct{}
}

func (b *blockingStorage) Read(ctx context.Context, p string) ([]byte, error) {
	<-b.block
	return b.mockStorage.Read(ctx, p)
}

// ---- I/O scheduler (#4) ----

type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *fakeClock) now() time.Time { c.mu.Lock(); defer c.mu.Unlock(); return c.t }
func (c *fakeClock) advance(d time.Duration) {
	c.mu.Lock()
	c.t = c.t.Add(d)
	c.mu.Unlock()
}

func newTestSched(cfg IOSchedConfig) (*ioScheduler, *fakeClock, *time.Duration) {
	clk := &fakeClock{t: time.Unix(1_700_000_000, 0)}
	var slept time.Duration
	s := &ioScheduler{nowFn: clk.now}
	s.sleepFn = func(_ context.Context, d time.Duration) error { slept += d; clk.advance(d); return nil }
	s.configure(cfg)
	return s, clk, &slept
}

func TestIOSchedIdleBackgroundRunsFree(t *testing.T) {
	s, _, slept := newTestSched(IOSchedConfig{BusyBytesPerSec: 10 << 20})
	for i := 0; i < 20; i++ {
		_ = s.charge(context.Background(), ioWarm, 16<<20)
	}
	if *slept != 0 {
		t.Fatalf("background throttled while foreground idle: slept %v", *slept)
	}
}

// While foreground reads are active, background is held to the busy
// allowance; the foreground itself never waits.
func TestIOSchedReservesForForeground(t *testing.T) {
	s, clk, slept := newTestSched(IOSchedConfig{BusyBytesPerSec: 10 << 20})
	start := clk.now()
	var bg int64
	for clk.now().Sub(start) < 4*time.Second {
		_ = s.charge(context.Background(), ioForeground, 1<<20) // keeps the node busy
		_ = s.charge(context.Background(), ioWarm, 4<<20)
		bg += 4 << 20
	}
	rate := float64(bg) / clk.now().Sub(start).Seconds()
	if rate > 13<<20 || rate < 7<<20 {
		t.Fatalf("background ran at %.1f MB/s while busy, want ~10", rate/(1<<20))
	}
	if s.throttled[ioForeground].Load() != 0 {
		t.Fatal("foreground was throttled")
	}
	_ = slept
}

// Two active background classes split the busy allowance by weight.
func TestIOSchedWeightsSplitAllowance(t *testing.T) {
	s, clk, _ := newTestSched(IOSchedConfig{BusyBytesPerSec: 12 << 20})
	start := clk.now()
	var warm, repl int64
	for clk.now().Sub(start) < 10*time.Second {
		_ = s.charge(context.Background(), ioForeground, 1)
		_ = s.charge(context.Background(), ioWarm, 1<<20)
		warm += 1 << 20
		_ = s.charge(context.Background(), ioReplicate, 1<<20)
		repl += 1 << 20
	}
	// Each loop charges both once, so totals match; what differs is who
	// waited: replicate (weight 1) must have slept far more than warm (4).
	if s.throttled[ioReplicate].Load() <= s.throttled[ioWarm].Load() {
		t.Fatalf("replicate slept %v, warm %v: weights not applied",
			time.Duration(s.throttled[ioReplicate].Load()), time.Duration(s.throttled[ioWarm].Load()))
	}
	_ = warm
	_ = repl
}

func TestIOSchedClassLimit(t *testing.T) {
	s, clk, _ := newTestSched(IOSchedConfig{ClassBytesPerSec: map[string]int64{"scrub": 5 << 20}})
	start := clk.now()
	var n int64
	for clk.now().Sub(start) < 4*time.Second {
		_ = s.charge(context.Background(), ioScrub, 1<<20)
		n += 1 << 20
	}
	if rate := float64(n) / clk.now().Sub(start).Seconds(); rate > 7<<20 {
		t.Fatalf("scrub ran at %.1f MB/s with a 5 MB/s cap", rate/(1<<20))
	}
}

func TestIOClassTravelsInContext(t *testing.T) {
	if ioClassOf(context.Background()) != ioForeground {
		t.Fatal("untagged work must be foreground")
	}
	if ioClassOf(withIOClass(context.Background(), ioScrub)) != ioScrub {
		t.Fatal("class lost")
	}
}

// ---- scrubber (#1) ----

func scrubTestManager(t *testing.T) *DefaultCacheManager {
	cm := evictTestManager(t, 1<<30)
	cm.config.VerifyRemoteChunks = true
	return cm
}

func writeLocal(t *testing.T, cm *DefaultCacheManager, p string, data []byte) string {
	full := filepath.Join(cm.config.NVMePath, strings.TrimPrefix(p, "/"))
	if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(full, data, 0o644); err != nil {
		t.Fatal(err)
	}
	cm.mu.Lock()
	cm.entries.put(&CacheEntry{FilePath: p, StoragePath: p, Size: int64(len(data)), Tier: TierNVMe, LastAccessed: time.Now()})
	cm.accountNVMeLocked(p, int64(len(data)))
	cm.mu.Unlock()
	return full
}

func TestScrubSidecarFile(t *testing.T) {
	ctx := context.Background()
	cm := scrubTestManager(t)
	good := []byte("model weights v1")
	full := writeLocal(t, cm, "/m/good.bin", good)
	sum, _, _ := cm.hashFile(ctx, full, 0, true)
	_ = os.WriteFile(full+".sha256", []byte(sum[0]), 0o644)
	badFull := writeLocal(t, cm, "/m/rot.bin", good)
	_ = os.WriteFile(badFull+".sha256", []byte(sum[0]), 0o644)
	// Bit rot that keeps size and mtime.
	info, _ := os.Stat(badFull)
	rotten := append([]byte(nil), good...)
	rotten[3] ^= 0x40
	_ = os.WriteFile(badFull, rotten, 0o644)
	_ = os.Chtimes(badFull, info.ModTime(), info.ModTime())
	writeLocal(t, cm, "/m/nosum.bin", []byte("no checksum anywhere"))

	res, err := cm.Scrub(ctx, false)
	if err != nil {
		t.Fatal(err)
	}
	if res.Checked != 2 || res.Corrupt != 1 || res.Skipped != 1 {
		t.Fatalf("scrub = %+v, want checked 2 corrupt 1 skipped 1", res)
	}
	if _, err := os.Stat(badFull); !os.IsNotExist(err) {
		t.Fatal("corrupt copy left in place")
	}
	if m, _ := filepath.Glob(badFull + ".corrupt-*"); len(m) != 1 {
		t.Fatal("corrupt copy not moved aside")
	}
	cm.mu.RLock()
	_, still := cm.entries.get("/m/rot.bin")
	cm.mu.RUnlock()
	if still {
		t.Fatal("corrupt copy still indexed")
	}
	if _, ok := checkCodes(cm.Health())[HealthScrubErrors]; !ok {
		t.Fatal("SCRUB_ERRORS not raised")
	}
	// A clean file is not re-hashed while fresh; force re-checks it.
	if res, _ := cm.Scrub(ctx, false); res.Checked != 0 {
		t.Fatalf("fresh file re-checked: %+v", res)
	}
	if res, _ := cm.Scrub(ctx, true); res.Checked != 1 {
		t.Fatalf("force did not re-check: %+v", res)
	}
}

// Chunk files and whole files assembled from chunks are checked against the
// cloud manifest.
func TestScrubAgainstChunkManifest(t *testing.T) {
	ctx := context.Background()
	cm, parent, chunks := sumsTestManager(t, 3)
	// sumsTestManager left chunk files on NVMe (not indexed) and wrote the
	// manifest; corrupt chunk 1 in place.
	cp1 := filepath.Join(cm.config.NVMePath, strings.TrimPrefix(chunkPathFor(parent, 1), "/"))
	bad := append([]byte(nil), chunks[1]...)
	bad[0] ^= 1
	_ = os.WriteFile(cp1, bad, 0o644)
	// And an assembled whole copy of another file built from the same chunks.
	whole := append(append(append([]byte(nil), chunks[0]...), chunks[1]...), chunks[2]...)
	cm.cloudStorage.Write(ctx, chunkSumsKey("/models/m/assembled.bin"), mustRead(t, cm, chunkSumsKey(parent)))
	writeLocal(t, cm, "/models/m/assembled.bin", whole)

	res, err := cm.Scrub(ctx, false)
	if err != nil {
		t.Fatal(err)
	}
	if res.Corrupt != 1 || res.Checked != 4 {
		t.Fatalf("scrub = %+v, want 4 checked (3 chunks + assembled), 1 corrupt", res)
	}
	if _, err := os.Stat(cp1); !os.IsNotExist(err) {
		t.Fatal("corrupt chunk left in place")
	}
}

func mustRead(t *testing.T, cm *DefaultCacheManager, key string) []byte {
	b, err := cm.cloudStorage.Read(context.Background(), key)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestScrubSingleFlight(t *testing.T) {
	cm := scrubTestManager(t)
	cm.scrub.running.Store(true)
	if _, err := cm.Scrub(context.Background(), false); err != errScrubBusy {
		t.Fatalf("err = %v, want errScrubBusy", err)
	}
}

// ---- down vs out, norebalance (#2) ----

func TestDownHolderCountsUntilOut(t *testing.T) {
	var tr peerDownTracker
	start := time.Unix(1_700_000_000, 0)
	live := map[string]struct{}{"a": {}}
	tr.observeLive([]string{"a", "b"}, start)
	if !tr.counts("b", live, start, start.Add(5*time.Minute), 10*time.Minute) {
		t.Fatal("down holder (5 min) stopped counting")
	}
	if tr.counts("b", live, start, start.Add(11*time.Minute), 10*time.Minute) {
		t.Fatal("out holder (11 min) still counts")
	}
	// Never seen live: grace measured from process start.
	if !tr.counts("ghost", live, start, start.Add(time.Minute), 10*time.Minute) {
		t.Fatal("unseen holder lost its startup grace")
	}
	if tr.counts("ghost", live, start, start.Add(time.Hour), 10*time.Minute) {
		t.Fatal("holder never seen live counts forever")
	}
}

func TestNoRebalanceFlagPausesReconcile(t *testing.T) {
	cm := evictTestManager(t, 1<<30)
	coord := newDeleteCoordinator()
	cm.config.Coordinator = coord
	ps, err := NewPeerStorage(coord, time.Second, "local", false, false, false, "")
	if err != nil {
		t.Fatal(err)
	}
	defer ps.Close()
	cm.peerStorage = ps
	cm.SetClusterFlags([]string{"norebalance"})
	cm.reconcileReplicasOnce(context.Background())
	paused, _, on := cm.RebalanceStats()
	if !on || paused != 1 {
		t.Fatalf("paused=%d on=%t", paused, on)
	}
	if _, ok := checkCodes(cm.Health())[HealthNoRebalance]; !ok {
		t.Fatal("NOREBALANCE not reported")
	}
	cm.SetClusterFlags(nil)
	if _, _, on := cm.RebalanceStats(); on {
		t.Fatal("flag not cleared")
	}
}

// ---- failure reports (#3) ----

func TestFailureReporterThreshold(t *testing.T) {
	var r peerFailureReporter
	now := time.Unix(1_700_000_000, 0)
	if r.noteFailure("p", now) {
		t.Fatal("reported while disabled")
	}
	r.enabled.Store(true)
	for i := 0; i < failureReportThreshold-1; i++ {
		if r.noteFailure("p", now.Add(time.Duration(i)*time.Second)) {
			t.Fatal("reported before the threshold")
		}
	}
	if !r.noteFailure("p", now.Add(5*time.Second)) {
		t.Fatal("not reported at the threshold")
	}
	if r.noteFailure("p", now.Add(6*time.Second)) {
		t.Fatal("repeat report inside the spacing interval")
	}
	if !r.noteFailure("p", now.Add(6*time.Second+failureReportEvery)) {
		t.Fatal("still-failing peer not re-reported")
	}
	r.noteSuccess("p")
	if r.noteFailure("p", now.Add(time.Hour)) {
		t.Fatal("success did not reset the count")
	}
}

func TestSuspectPeersTriedLast(t *testing.T) {
	a := &coordinator.PeerInfo{ID: "a", Suspect: true}
	b := &coordinator.PeerInfo{ID: "b"}
	c := &coordinator.PeerInfo{ID: "c"}
	got := suspectLast([]*coordinator.PeerInfo{a, b, c})
	if got[0].ID != "b" || got[1].ID != "c" || got[2].ID != "a" {
		t.Fatalf("order = %s %s %s", got[0].ID, got[1].ID, got[2].ID)
	}
}
