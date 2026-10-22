package globalcpe2e

import (
	"bytes"
	"context"
	"crypto/rand"
	"strings"
	"testing"
)

// chunkedStride keeps a three-chunk file small. Both clusters use it, but the
// reader must not rely on that: the stride comes from the published row.
const chunkedStride = 64 << 10

func newChunkedFederation(t *testing.T, bGlobalRead bool, bStride int64) (*s3Server, *cluster, *cluster) {
	t.Helper()
	store := startS3Server(t)
	cp := startControlPlane(t)
	a := newCluster(t, cp, store, clusterConfig{
		id:        "cluster-a",
		prefix:    "a",
		publish:   []string{"/models/"},
		chunkSize: chunkedStride,
	})
	b := newCluster(t, cp, store, clusterConfig{
		id:         "cluster-b",
		prefix:     "b",
		publish:    []string{"/models/"},
		subscribe:  []string{"/models/"},
		globalRead: bGlobalRead,
		chunkSize:  bStride,
	})
	a.waitReady(t)
	b.waitReady(t)
	return store, a, b
}

func randomBytes(t *testing.T, n int) []byte {
	t.Helper()
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		t.Fatal(err)
	}
	return b
}

// TestChunkedFileWrittenThroughTheCacheCrossesAsOneRow is I2's acceptance
// test. A three-chunk file is written the way FUSE and the HTTP streaming PUT
// write it (staged file + PutFromFile), and:
//
//   - A's own persist uploads every chunk and the durability receipt says so;
//   - exactly ONE global row appears for it, flagged chunked with the right
//     size and stride, and no "_chunk_" row ever crosses;
//   - B reads it byte-identically through the ordinary Get, from A's chunk
//     keys, having never written it;
//   - B's forced LookupEntry (what FUSE lookup/create/getattr call) resolves
//     the path with the correct shape.
//
// B deliberately runs a different -chunk-size. Reading at B's own stride
// would mis-slice every chunk boundary; the bytes only match if the stride
// came from A's published row.
func TestChunkedFileWrittenThroughTheCacheCrossesAsOneRow(t *testing.T) {
	store, a, b := newChunkedFederation(t, true, 4*chunkedStride)

	const path = "/models/llama/shard-00001.safetensors"
	want := randomBytes(t, 2*chunkedStride+chunkedStride/3) // 3 chunks
	a.writeThroughCache(t, path, want)

	receipt := a.waitDurable(t, path)
	if p := receipt.Paths[0]; !p.IsChunked || p.Objects != 3 || p.Size != int64(len(want)) {
		t.Fatalf("receipt shape wrong: %+v", p)
	}
	for i, key := range []string{"a" + path + "_chunk_0", "a" + path + "_chunk_1", "a" + path + "_chunk_2"} {
		if _, ok := store.get(key); !ok {
			t.Fatalf("chunk %d (%s) not in the bucket although the receipt says durable; keys=%v", i, key, store.keys())
		}
	}

	waitFor(t, "cluster-b's mirror to learn the chunked parent", func() bool { return b.sees(path) })

	rows := b.agent.Mirror().Snapshot()
	var parentRows int
	for _, r := range rows {
		if strings.Contains(r.GetFilePath(), "_chunk_") {
			t.Fatalf("a chunk row crossed the cluster boundary: %s", r.GetFilePath())
		}
		if r.GetFilePath() == path {
			parentRows++
			if !r.GetIsChunked() || r.GetFileSize() != int64(len(want)) || r.GetChunkSize() != chunkedStride {
				t.Fatalf("parent row shape wrong: chunked=%v size=%d stride=%d",
					r.GetIsChunked(), r.GetFileSize(), r.GetChunkSize())
			}
		}
	}
	if parentRows != 1 {
		t.Fatalf("want exactly one global row for %s, got %d (rows=%d)", path, parentRows, len(rows))
	}

	// FUSE-shaped metadata first: lookup must see the remote file and its
	// shape before any byte is read.
	entry, ok := b.cm.LookupEntry(context.Background(), path, true)
	if !ok {
		t.Fatal("forced LookupEntry in cluster-b returned ENOENT for a globally published path")
	}
	if entry.Size != int64(len(want)) || !entry.IsChunked || entry.ChunkSize != chunkedStride {
		t.Fatalf("LookupEntry shape wrong: %+v", entry)
	}

	store.resetRequests()
	got, ok := b.read(path)
	if !ok {
		t.Fatalf("cluster-b could not read the chunked file; keys tried: %v", store.requestedKeys())
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("cluster-b read %d bytes that differ from the %d written", len(got), len(want))
	}
	tried := store.requestedKeys()
	for _, k := range []string{"a" + path + "_chunk_0", "a" + path + "_chunk_2"} {
		if !containsKey(tried, k) {
			t.Fatalf("cluster-b never read A's chunk key %s; tried %v", k, tried)
		}
	}
	if containsKey(tried, "a"+path) {
		t.Fatalf("cluster-b read the parent as a single object, which does not exist: %v", tried)
	}

	// ReadRange (the FUSE read path) across a chunk boundary.
	off := int64(chunkedStride - 100)
	window, err := b.cm.ReadRange(context.Background(), path, off, 300)
	if err != nil {
		t.Fatalf("ReadRange across a chunk boundary: %v", err)
	}
	if !bytes.Equal(window, want[off:off+300]) {
		t.Fatal("ReadRange across a chunk boundary returned the wrong bytes")
	}
}

// TestGlobalLookupIsOffWithoutGlobalRead: the mount's view of global paths is
// opt-in. With -global-read off, the same published path is ENOENT through
// LookupEntry and unreadable through Get.
func TestGlobalLookupIsOffWithoutGlobalRead(t *testing.T) {
	_, a, b := newChunkedFederation(t, false, chunkedStride)

	const path = "/models/off/weights.bin"
	a.writeThroughCache(t, path, randomBytes(t, 3*chunkedStride))
	a.waitDurable(t, path)
	waitFor(t, "cluster-b's mirror to learn the path", func() bool { return b.sees(path) })

	if _, ok := b.cm.LookupEntry(context.Background(), path, true); ok {
		t.Fatal("LookupEntry resolved a global path with global read disabled")
	}
	if _, ok := b.read(path); ok {
		t.Fatal("Get served a global chunked path with global read disabled")
	}
}

// TestChunkedGlobalReadNeedsNoPriorLookup: an API client (or a FUSE open of a
// path cached from an earlier lookup that has since aged out of the view)
// calls Get with no metadata primed. The reader must learn the remote shape
// itself rather than reading the parent as one object.
func TestChunkedGlobalReadNeedsNoPriorLookup(t *testing.T) {
	store, a, b := newChunkedFederation(t, true, chunkedStride)

	const path = "/models/direct/weights.bin"
	want := randomBytes(t, 3*chunkedStride-7)
	a.writeThroughCache(t, path, want)
	a.waitDurable(t, path)
	waitFor(t, "cluster-b's mirror to learn the path", func() bool { return b.sees(path) })

	store.resetRequests()
	got, ok := b.read(path)
	if !ok || !bytes.Equal(got, want) {
		t.Fatalf("Get without a prior lookup failed: ok=%v len=%d want %d; tried %v",
			ok, len(got), len(want), store.requestedKeys())
	}
}
