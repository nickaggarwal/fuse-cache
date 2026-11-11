package cache

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"testing"
)

// sumsTestManager has a chunked file of n chunks on NVMe, persisted to the
// mock cloud tier (which writes its checksum manifest).
func sumsTestManager(t *testing.T, n int) (*DefaultCacheManager, string, [][]byte) {
	t.Helper()
	cm := evictTestManager(t, 1<<30)
	cm.config.VerifyRemoteChunks = true
	const parent = "/models/m/weights.bin"
	chunks := make([][]byte, n)
	for i := range chunks {
		size := int(cm.config.ChunkSize)
		if i == n-1 {
			size = 300
		}
		chunks[i] = bytes.Repeat([]byte{byte('a' + i)}, size)
		if err := cm.nvmeStorage.Write(context.Background(), chunkPathFor(parent, int64(i)), chunks[i]); err != nil {
			t.Fatal(err)
		}
	}
	if err := cm.persistChunkedFileToCloud(context.Background(), parent, int64(n)); err != nil {
		t.Fatalf("persist: %v", err)
	}
	return cm, parent, chunks
}

func TestChunkSumsWrittenOnPersist(t *testing.T) {
	cm, parent, chunks := sumsTestManager(t, 3)
	if !cm.cloudStorage.Exists(context.Background(), chunkSumsKey(parent)) {
		t.Fatal("no checksum manifest after a chunked persist")
	}
	m := cm.fetchChunkSums(parent)(context.Background())
	if m == nil || len(m.CRC32C) != 3 || m.ChunkSize != cm.config.ChunkSize || m.Size != 2*cm.config.ChunkSize+300 {
		t.Fatalf("manifest = %+v", m)
	}
	for i, c := range chunks {
		if !m.matches(int64(i), c) {
			t.Fatalf("manifest rejects the chunk it was built from (%d)", i)
		}
	}
}

// IR-3: a corrupted or truncated chunk object in cloud storage is a read
// error, never silently wrong bytes.
func TestCorruptCloudChunkRejected(t *testing.T) {
	cm, parent, chunks := sumsTestManager(t, 3)
	ctx := context.Background()
	flipped := append([]byte(nil), chunks[1]...)
	flipped[17] ^= 0xff
	cases := map[string][]byte{
		"bitflip":   flipped,
		"truncated": chunks[1][:len(chunks[1])/2],
	}
	for name, bad := range cases {
		t.Run(name, func(t *testing.T) {
			_ = cm.cloudStorage.Write(ctx, chunkPathFor(parent, 1), bad)
			_, err := cm.getFromTier(ctx, chunkPathFor(parent, 1), TierCloud)
			var corrupt *errRemoteChunkCorrupt
			if !errors.As(err, &corrupt) {
				t.Fatalf("corrupt cloud chunk read returned err=%v, want errRemoteChunkCorrupt", err)
			}
		})
	}
	if got := cm.metrics.RemoteChunkCorrupt.Load(); got != 2 {
		t.Fatalf("RemoteChunkCorrupt = %d, want 2", got)
	}
	// Good chunks still read.
	if e, err := cm.getFromTier(ctx, chunkPathFor(parent, 0), TierCloud); err != nil || !bytes.Equal(e.Data, chunks[0]) {
		t.Fatalf("good chunk: err=%v", err)
	}
	if cm.metrics.RemoteChunkVerified.Load() == 0 {
		t.Fatal("RemoteChunkVerified not counted")
	}
}

// A peer serving bad bytes is skipped: the chunk lands from the next tier,
// and the bad bytes never reach NVMe.
func TestCorruptPeerChunkFallsThroughToCloud(t *testing.T) {
	cm, parent, chunks := sumsTestManager(t, 3)
	ctx := context.Background()
	cp := chunkPathFor(parent, 2)
	_ = cm.peerStorage.Write(ctx, cp, []byte("garbage of the wrong length"))
	_ = cm.nvmeStorage.Delete(ctx, cp)

	n, err := cm.fetchAndLandChunkOrdered(ctx, cp, []CacheTier{TierPeer, TierCloud})
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}
	got, err := cm.nvmeStorage.Read(ctx, cp)
	if err != nil || !bytes.Equal(got, chunks[2]) || n != int64(len(chunks[2])) {
		t.Fatalf("landed %d bytes (err %v), want the cloud copy", len(got), err)
	}
	if cm.metrics.RemoteChunkCorrupt.Load() != 1 {
		t.Fatalf("RemoteChunkCorrupt = %d, want 1", cm.metrics.RemoteChunkCorrupt.Load())
	}
}

// No manifest (persisted before checksums, or not persisted yet): the read
// passes and is counted as unverified, a visible state, not a silent pass.
func TestChunkWithoutManifestIsUnverified(t *testing.T) {
	cm := evictTestManager(t, 1<<30)
	cm.config.VerifyRemoteChunks = true
	ctx := context.Background()
	_ = cm.cloudStorage.Write(ctx, "/old.bin_chunk_0", []byte("legacy"))
	for i := 0; i < 3; i++ {
		if _, err := cm.getFromTier(ctx, "/old.bin_chunk_0", TierCloud); err != nil {
			t.Fatal(err)
		}
	}
	if got := cm.metrics.RemoteChunkUnverified.Load(); got != 3 {
		t.Fatalf("RemoteChunkUnverified = %d, want 3", got)
	}
}

// A re-persist (overwrite) replaces the manifest; a reader holding the old
// one refetches on mismatch instead of rejecting the new bytes.
func TestChunkSumsFollowOverwrite(t *testing.T) {
	cm, parent, _ := sumsTestManager(t, 2)
	ctx := context.Background()
	// Prime the reader's manifest cache with version 1.
	if _, err := cm.getFromTier(ctx, chunkPathFor(parent, 0), TierCloud); err != nil {
		t.Fatal(err)
	}
	v2 := bytes.Repeat([]byte("Z"), int(cm.config.ChunkSize))
	_ = cm.nvmeStorage.Write(ctx, chunkPathFor(parent, 0), v2)
	if err := cm.persistChunkedFileToCloud(ctx, parent, 2); err != nil {
		t.Fatal(err)
	}
	e, err := cm.getFromTier(ctx, chunkPathFor(parent, 0), TierCloud)
	if err != nil || !bytes.Equal(e.Data, v2) {
		t.Fatalf("new version rejected after overwrite: %v", err)
	}
	if cm.metrics.RemoteChunkCorrupt.Load() != 0 {
		t.Fatal("overwrite counted as corruption")
	}
}

// Disabled: no manifest written, nothing checked.
func TestChunkSumsDisabled(t *testing.T) {
	cm := evictTestManager(t, 1<<30)
	ctx := context.Background()
	_ = cm.nvmeStorage.Write(ctx, "/f.bin_chunk_0", []byte("x"))
	if err := cm.persistChunkedFileToCloud(ctx, "/f.bin", 1); err != nil {
		t.Fatal(err)
	}
	if cm.cloudStorage.Exists(ctx, chunkSumsKey("/f.bin")) {
		t.Fatal("manifest written with verification disabled")
	}
	_ = cm.cloudStorage.Write(ctx, "/f.bin_chunk_0", []byte("y"))
	if _, err := cm.getFromTier(ctx, "/f.bin_chunk_0", TierCloud); err != nil {
		t.Fatal(err)
	}
	if n := cm.metrics.RemoteChunkUnverified.Load() + cm.metrics.RemoteChunkVerified.Load(); n != 0 {
		t.Fatal(fmt.Sprint("verification ran while disabled: ", n))
	}
}
