package cache

import (
	"context"
	"fmt"
	"io"
	"testing"
	"time"
)

// tierMissManager builds a manager whose peer tier is empty and whose cloud
// tier holds everything — the ordinary cold-read shape, and exactly what
// stargz-test looked like when fuse_tier_peer_success_ratio read 0.0000.
func tierMissManager(t *testing.T) (*DefaultCacheManager, *mockStorage) {
	t.Helper()
	cm := evictTestManager(t, 1<<30)
	cm.config.AdaptiveRemoteRead = true
	cloud := newMockStorage()
	cm.cloudStorage = cloud
	// Peer tier is reachable but holds nothing: every Read is a miss, never a
	// transport failure. mockStorage returns context.Canceled for absent keys,
	// so wrap it to answer with a typed miss like the real PeerStorage does.
	cm.peerStorage = &missingPeerStorage{}
	return cm, cloud
}

// missingPeerStorage is a peer tier that is healthy and answers promptly, and
// simply does not hold anything.
type missingPeerStorage struct{ mockStorage }

func (m *missingPeerStorage) Read(ctx context.Context, path string) ([]byte, error) {
	return nil, &peerMissError{addr: "any", path: path}
}

func (m *missingPeerStorage) Exists(ctx context.Context, path string) bool { return false }

// TestTierPerf_ColdReadMissesDoNotInvertTierOrder is the regression test for
// the live defect. A run of cold chunk reads that the peer tier legitimately
// does not hold must not teach the tracker that the peer tier is broken.
//
// Before the fix this test fails three ways at once: peer success collapses to
// ~0, order() puts the ~100x slower cloud tier first, and shouldHedge() pins
// on because ewmaSuccess never climbs back over tierPerfReliableSuccess.
func TestTierPerf_ColdReadMissesDoNotInvertTierOrder(t *testing.T) {
	cm, cloud := tierMissManager(t)
	ctx := context.Background()

	const numChunks = 12
	for i := 0; i < numChunks; i++ {
		path := fmt.Sprintf("/cold-%d.bin", i)
		if err := cloud.Write(ctx, path, []byte("chunk-payload")); err != nil {
			t.Fatalf("seed cloud: %v", err)
		}
		data, tier, err := cm.readChunkDataFromTiers(
			ctx, path, path, int64(i), remoteReadOrderPeerFirst, false, time.Now())
		if err != nil {
			t.Fatalf("read %s: %v", path, err)
		}
		if tier != TierCloud {
			t.Fatalf("read %s served from %v, want cloud", path, tier)
		}
		if string(data) != "chunk-payload" {
			t.Fatalf("read %s returned %q", path, data)
		}
	}

	peerLat, peerSucc, peerSamples, _, cloudSucc, cloudSamples := cm.tierPerf.snapshot()

	// The peer tier answered "not held" every time. That is correct behavior,
	// carries no health signal, and must leave the tracker with nothing to
	// learn from.
	if peerSamples != 0 {
		t.Fatalf("peer samples = %d after %d misses, want 0 (latency %.2fms, success %.4f)",
			peerSamples, numChunks, peerLat, peerSucc)
	}
	if cloudSamples != numChunks {
		t.Fatalf("cloud samples = %d, want %d", cloudSamples, numChunks)
	}
	// ...but the misses are still counted, so a zero sample count can be told
	// apart from a tier that simply saw no traffic.
	if peerMisses, cloudMisses := cm.tierPerf.missCounts(); peerMisses != numChunks || cloudMisses != 0 {
		t.Fatalf("misses = (peer %d, cloud %d), want (%d, 0)", peerMisses, cloudMisses, numChunks)
	}
	if cloudSucc < 0.99 {
		t.Fatalf("cloud success = %.4f, want ~1.0", cloudSucc)
	}

	// With no peer samples the tracker stays on its cold-start prior rather
	// than demoting a tier it has never actually measured.
	order := cm.tierPerf.order()
	if order[0] != TierPeer {
		t.Fatalf("order = %v, want peer-first; misses inverted the tier order", order)
	}
	if _, ok := cm.tierPerf.shouldHedge(); ok {
		t.Fatal("shouldHedge should report ok=false with no peer samples")
	}
	if snap := cm.TierPerfSnapshot(); snap.PrimaryTier != "none" {
		t.Fatalf("PrimaryTier = %q, want \"none\" with an unmeasured tier", snap.PrimaryTier)
	}
}

// TestTierPerf_RealPeerFailuresStillDemote guards the other half: the fix must
// not make the tracker blind. A peer tier that is genuinely broken has to be
// demoted, or the adaptive ordering stops earning its keep.
func TestTierPerf_RealPeerFailuresStillDemote(t *testing.T) {
	cm, cloud := tierMissManager(t)
	cm.peerStorage = &brokenPeerStorage{}
	ctx := context.Background()

	const numChunks = 8
	for i := 0; i < numChunks; i++ {
		path := fmt.Sprintf("/broken-%d.bin", i)
		if err := cloud.Write(ctx, path, []byte("payload")); err != nil {
			t.Fatalf("seed cloud: %v", err)
		}
		if _, _, err := cm.readChunkDataFromTiers(
			ctx, path, path, int64(i), remoteReadOrderPeerFirst, false, time.Now()); err != nil {
			t.Fatalf("read %s: %v", path, err)
		}
	}

	_, peerSucc, peerSamples, _, _, _ := cm.tierPerf.snapshot()
	if peerSamples != numChunks {
		t.Fatalf("peer samples = %d, want %d: real failures must be recorded", peerSamples, numChunks)
	}
	if peerSucc > 0.2 {
		t.Fatalf("peer success = %.4f, want near 0 for a broken tier", peerSucc)
	}
	if order := cm.tierPerf.order(); order[0] != TierCloud {
		t.Fatalf("order = %v, want cloud-first: a broken peer tier must be demoted", order)
	}
}

// brokenPeerStorage is a peer tier whose transport genuinely fails.
type brokenPeerStorage struct{ mockStorage }

func (b *brokenPeerStorage) Read(ctx context.Context, path string) ([]byte, error) {
	return nil, io.ErrUnexpectedEOF
}

func (b *brokenPeerStorage) Exists(ctx context.Context, path string) bool { return false }
