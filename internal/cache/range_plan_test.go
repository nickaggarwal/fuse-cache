package cache

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func planBytes(t *testing.T, pieces []RangePiece) []byte {
	t.Helper()
	var buf bytes.Buffer
	for _, p := range pieces {
		if _, err := p.WriteTo(&buf); err != nil {
			t.Fatal(err)
		}
	}
	return buf.Bytes()
}

// Ranges over local chunk files come back as file pieces with exact bytes,
// across chunk boundaries and at the tail, and touch no range cache.
func TestPlanRange_LocalChunks(t *testing.T) {
	const chunkSize = 1 << 20
	cm, parent, want := chunkWindowManager(t, chunkSize, 4)
	entry, _ := cm.entries.get(parent)
	for _, c := range []struct{ off, n int64 }{
		{0, 4096},
		{chunkSize - 100, 200},           // straddles chunk 0/1
		{chunkSize + 7, 2*chunkSize + 9}, // three chunks
		{4*chunkSize - 10, 1 << 20},      // past EOF: clamp
	} {
		pieces, release, err := cm.PlanRange(context.Background(), entry, c.off, c.n)
		if err != nil {
			t.Fatalf("PlanRange(%d,%d): %v", c.off, c.n, err)
		}
		for _, p := range pieces {
			if p.file == nil {
				t.Errorf("PlanRange(%d,%d): remote piece for a local chunk", c.off, c.n)
			}
		}
		end := min(c.off+c.n, int64(len(want)))
		if got := planBytes(t, pieces); !bytes.Equal(got, want[c.off:end]) {
			t.Errorf("PlanRange(%d,%d): %d bytes, mismatch", c.off, c.n, len(got))
		}
		release()
	}
	if n, _ := cm.rangeCacheStats(parent); n != 0 {
		t.Errorf("range cache holds %d chunks; want 0", n)
	}
	if cm.metrics.PrefetchIssued.Load() != 0 {
		t.Errorf("issued %d read-ahead chunks; want 0", cm.metrics.PrefetchIssued.Load())
	}
}

// A chunk with no local copy is fetched from the remote tiers once, and no
// neighbouring chunk is read ahead.
func TestPlanRange_RemoteChunkNoReadAhead(t *testing.T) {
	const chunkSize = 1 << 20
	cm, parent, want := chunkWindowManager(t, chunkSize, 4)
	entry, _ := cm.entries.get(parent)
	// Move chunk 2 from NVMe to the peer tier.
	local := filepath.Join(cm.config.NVMePath, "chunked.bin_chunk_2")
	if err := os.Remove(local); err != nil {
		t.Fatal(err)
	}
	peer := cm.peerStorage.(*mockStorage)
	if err := peer.Write(context.Background(), fmt.Sprintf("%s_chunk_2", parent), want[2*chunkSize:3*chunkSize]); err != nil {
		t.Fatal(err)
	}
	off := int64(2*chunkSize + 1000)
	pieces, release, err := cm.PlanRange(context.Background(), entry, off, 5000)
	defer release()
	if err != nil {
		t.Fatal(err)
	}
	if got := planBytes(t, pieces); !bytes.Equal(got, want[off:off+5000]) {
		t.Fatalf("remote range mismatch: %d bytes", len(got))
	}
	if pieces[0].Tier != TierPeer {
		t.Errorf("served from tier %v, want peer", pieces[0].Tier)
	}
	if n := cm.metrics.PrefetchIssued.Load(); n != 0 {
		t.Errorf("issued %d read-ahead chunks; want 0", n)
	}
	if cm.metrics.BlobRangeRemoteBytes.Load() != 5000 {
		t.Errorf("remote bytes = %d, want 5000", cm.metrics.BlobRangeRemoteBytes.Load())
	}
}

// A whole local copy (the store node's layout) serves any range as one piece.
func TestPlanRange_WholeLocalFile(t *testing.T) {
	const chunkSize = 1 << 20
	cm, parent, _ := benchCacheManagerWithWholeFile(t, chunkSize, 3)
	entry, _ := cm.entries.get(parent)
	want, _ := os.ReadFile(filepath.Join(cm.config.NVMePath, "bench-whole.bin"))
	off := int64(chunkSize - 50)
	pieces, release, err := cm.PlanRange(context.Background(), entry, off, chunkSize)
	defer release()
	if err != nil {
		t.Fatal(err)
	}
	if len(pieces) != 1 || pieces[0].file == nil {
		t.Fatalf("got %d pieces (file=%v), want one file piece", len(pieces), len(pieces) > 0 && pieces[0].file != nil)
	}
	if got := planBytes(t, pieces); !bytes.Equal(got, want[off:off+chunkSize]) {
		t.Fatal("whole-file range mismatch")
	}
}

// A chunk no tier holds fails the plan, so the endpoint can answer 5xx
// before writing any bytes.
func TestPlanRange_MissingChunkFails(t *testing.T) {
	const chunkSize = 1 << 20
	cm, parent, _ := chunkWindowManager(t, chunkSize, 2)
	entry, _ := cm.entries.get(parent)
	if err := os.Remove(filepath.Join(cm.config.NVMePath, "chunked.bin_chunk_1")); err != nil {
		t.Fatal(err)
	}
	if _, release, err := cm.PlanRange(context.Background(), entry, chunkSize+10, 100); err == nil {
		release()
		t.Fatal("planned a range over a chunk no tier holds")
	}
}
