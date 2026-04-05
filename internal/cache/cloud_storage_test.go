package cache

import (
	"testing"
	"time"
)

func TestShouldUseDirectS3Read(t *testing.T) {
	t.Parallel()

	if !shouldUseDirectS3Read("/foo/bar.bin_chunk_7") {
		t.Fatalf("expected chunk object to use direct s3 read")
	}
	if shouldUseDirectS3Read("/foo/bar.bin") {
		t.Fatalf("did not expect non-chunk object to use direct s3 read")
	}
}

func TestChunkReadTimeoutUsesCloudTimeoutForHybridOrCloudReads(t *testing.T) {
	t.Parallel()

	cm := &DefaultCacheManager{
		config: &CacheConfig{
			PeerTimeout:  30 * time.Second,
			CloudTimeout: 60 * time.Second,
		},
	}

	if got := cm.chunkReadTimeout([]CacheTier{TierPeer, TierCloud}, true); got != 60*time.Second {
		t.Fatalf("hybrid timeout = %v, want 60s", got)
	}
	if got := cm.chunkReadTimeout([]CacheTier{TierCloud, TierPeer}, false); got != 60*time.Second {
		t.Fatalf("cloud remote timeout = %v, want 60s", got)
	}
	if got := cm.chunkReadTimeout([]CacheTier{TierPeer}, false); got != 30*time.Second {
		t.Fatalf("peer timeout = %v, want 30s", got)
	}
}
