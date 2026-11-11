package cache

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"sync"
	"time"
)

// Content checksums for remote chunk reads.
//
// NVMe reads are checked against a .sha256 sidecar, but that protection used
// to stop at the tier boundary: a chunk read from a peer or from object
// storage was checked for nothing but arriving. A truncated or corrupted
// remote chunk landed, got a sidecar computed from its own (bad) bytes, then
// verified cleanly forever, and the node advertised itself as a holder of it.
// Corruption spread through the swarm with a valid checksum attached.
//
// The origin checksum now travels with the durable copy: once every chunk of
// a file is in object storage, the persist path writes one small manifest
// object, "<file>.chunksums", listing each chunk's CRC-32C. A chunk read from
// the peer or cloud tier is checked against it before anyone uses or lands
// the bytes. A mismatch fails that tier's read, so the caller falls through
// to the next tier, and a cloud copy that fails is a read error, never
// silently wrong data.
//
// CRC-32C, not SHA-256: the threat is corruption and truncation inside a
// trusted cluster, not a forging peer, and CRC-32C is hardware-accelerated
// (tens of GB/s) where SHA-256 would cost a core per ~2 GB/s on the
// cold-read path this cache exists to make fast.
//
// Where no manifest exists (files persisted before this, files not yet
// persisted, no cloud tier), the read passes, counted as unverified: a
// distinct, visible state rather than a silent pass.
//
// Overwrites: the persist path deletes the old manifest before uploading new
// chunks, and a reader that sees a mismatch refetches the manifest once
// before rejecting, so a cached manifest from the previous version never
// rejects the new bytes.

const (
	chunkSumsSuffix = ".chunksums"
	// chunkSumsPositiveTTL bounds how long a fetched manifest is trusted
	// without a refetch; a mismatch refetches regardless.
	chunkSumsPositiveTTL = 10 * time.Minute
	// chunkSumsNegativeTTL bounds how often an absent manifest is asked for
	// again (one small GET per file per window, not one per chunk).
	chunkSumsNegativeTTL = 30 * time.Second
	chunkSumsMaxEntries  = 4096
	chunkSumsFetchCap    = 5 * time.Second
)

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

// chunkSumsManifest is the on-cloud form.
type chunkSumsManifest struct {
	Version   int      `json:"v"`
	Size      int64    `json:"size"`
	ChunkSize int64    `json:"chunk_size"`
	CRC32C    []string `json:"crc32c"`
}

func chunkCRC(data []byte) string {
	var b [4]byte
	c := crc32.Checksum(data, castagnoli)
	b[0], b[1], b[2], b[3] = byte(c>>24), byte(c>>16), byte(c>>8), byte(c)
	return hex.EncodeToString(b[:])
}

func chunkSumsKey(filePath string) string { return filePath + chunkSumsSuffix }

// matches reports whether data is chunk idx as the manifest describes it:
// the right length (every chunk is ChunkSize but the last) and the right CRC.
func (m *chunkSumsManifest) matches(idx int64, data []byte) bool {
	if idx < 0 || idx >= int64(len(m.CRC32C)) || m.ChunkSize <= 0 {
		return false
	}
	want := m.ChunkSize
	if idx == int64(len(m.CRC32C))-1 {
		want = m.Size - idx*m.ChunkSize
	}
	return int64(len(data)) == want && chunkCRC(data) == m.CRC32C[idx]
}

// chunkSumsCache holds fetched manifests per parent file, with single-flight
// fetches so the first wave of a cold read (hundreds of chunks at once) costs
// one manifest GET, not one per chunk.
type chunkSumsCache struct {
	mu      sync.Mutex
	entries map[string]*chunkSumsEntry
}

type chunkSumsEntry struct {
	ready     chan struct{}
	manifest  *chunkSumsManifest // nil: absent or unreadable
	fetchedAt time.Time
}

func (c *chunkSumsCache) get(ctx context.Context, parent string, fetch func(context.Context) *chunkSumsManifest) *chunkSumsManifest {
	c.mu.Lock()
	if c.entries == nil {
		c.entries = make(map[string]*chunkSumsEntry)
	}
	if e, ok := c.entries[parent]; ok {
		select {
		case <-e.ready:
			ttl := chunkSumsPositiveTTL
			if e.manifest == nil {
				ttl = chunkSumsNegativeTTL
			}
			if time.Since(e.fetchedAt) < ttl {
				c.mu.Unlock()
				return e.manifest
			}
		default:
			c.mu.Unlock()
			select {
			case <-e.ready:
				return e.manifest
			case <-ctx.Done():
				return nil
			}
		}
	}
	if len(c.entries) >= chunkSumsMaxEntries {
		for k, e := range c.entries {
			select {
			case <-e.ready:
				delete(c.entries, k)
			default:
			}
			if len(c.entries) < chunkSumsMaxEntries {
				break
			}
		}
	}
	e := &chunkSumsEntry{ready: make(chan struct{})}
	c.entries[parent] = e
	c.mu.Unlock()

	e.manifest = fetch(ctx)
	e.fetchedAt = time.Now()
	close(e.ready)
	return e.manifest
}

func (c *chunkSumsCache) invalidate(parent string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if e, ok := c.entries[parent]; ok {
		select {
		case <-e.ready:
			delete(c.entries, parent)
		default:
		}
	}
}

func (cm *DefaultCacheManager) fetchChunkSums(parent string) func(context.Context) *chunkSumsManifest {
	return func(ctx context.Context) *chunkSumsManifest {
		fctx, cancel := context.WithTimeout(ctx, chunkSumsFetchCap)
		defer cancel()
		raw, err := cm.cloudStorage.Read(fctx, chunkSumsKey(parent))
		if err != nil {
			return nil
		}
		var m chunkSumsManifest
		if json.Unmarshal(raw, &m) != nil || m.ChunkSize <= 0 || len(m.CRC32C) == 0 {
			return nil
		}
		return &m
	}
}

// errRemoteChunkCorrupt marks a remote chunk whose bytes contradict the
// file's checksum manifest. Not a miss: the tier answered, wrongly.
type errRemoteChunkCorrupt struct {
	path string
	tier CacheTier
}

func (e *errRemoteChunkCorrupt) Error() string {
	return fmt.Sprintf("chunk %s from %s does not match its checksum manifest", e.path, cacheTierToStorageTier(e.tier))
}

// verifyRemoteChunk checks a chunk read from the peer or cloud tier against
// its file's manifest. nil means use the bytes (verified, or unverifiable and
// counted as such).
func (cm *DefaultCacheManager) verifyRemoteChunk(ctx context.Context, chunkPath string, tier CacheTier, data []byte) error {
	if !cm.config.VerifyRemoteChunks || cm.cloudStorage == nil {
		return nil
	}
	parent, ok := parentFilePathFromChunkPath(chunkPath)
	if !ok {
		return nil
	}
	idx, _ := chunkIndexFromPath(chunkPath)
	m := cm.chunkSums.get(ctx, parent, cm.fetchChunkSums(parent))
	if m == nil {
		cm.metrics.RemoteChunkUnverified.Add(1)
		return nil
	}
	if m.matches(idx, data) {
		cm.metrics.RemoteChunkVerified.Add(1)
		return nil
	}
	// The cached manifest may describe a previous version of the file.
	cm.chunkSums.invalidate(parent)
	m = cm.chunkSums.get(ctx, parent, cm.fetchChunkSums(parent))
	if m == nil {
		cm.metrics.RemoteChunkUnverified.Add(1)
		return nil
	}
	if m.matches(idx, data) {
		cm.metrics.RemoteChunkVerified.Add(1)
		return nil
	}
	cm.metrics.RemoteChunkCorrupt.Add(1)
	cm.logger.Printf("[VERIFY] %s from %s: %d bytes do not match the checksum manifest; rejected", chunkPath, cacheTierToStorageTier(tier), len(data))
	return &errRemoteChunkCorrupt{path: chunkPath, tier: tier}
}

// chunkSumsBuilder collects per-chunk CRCs during a chunked persist. Workers
// write distinct indexes, so the slices need no lock.
type chunkSumsBuilder struct {
	crcs []string
	lens []int64
}

func newChunkSumsBuilder(numChunks int64) *chunkSumsBuilder {
	return &chunkSumsBuilder{crcs: make([]string, numChunks), lens: make([]int64, numChunks)}
}

func (b *chunkSumsBuilder) add(idx int64, data []byte) {
	if b == nil || idx < 0 || idx >= int64(len(b.crcs)) {
		return
	}
	b.crcs[idx] = chunkCRC(data)
	b.lens[idx] = int64(len(data))
}

// dropChunkSums removes a file's manifest ahead of uploading a new version,
// so readers holding the old one refetch and find none rather than reject
// the new bytes. An error other than "not there" fails the persist: leaving
// an old manifest beside new chunks would make the cloud copy unreadable.
func (cm *DefaultCacheManager) dropChunkSums(ctx context.Context, filePath string) error {
	if cm.cloudStorage == nil || !cm.config.VerifyRemoteChunks {
		return nil
	}
	dctx, cancel := context.WithTimeout(ctx, chunkSumsFetchCap)
	defer cancel()
	err := cm.cloudStorage.Delete(dctx, chunkSumsKey(filePath))
	cm.chunkSums.invalidate(filePath)
	if err != nil && !isCloudMiss(err) && !isDeleteNotFoundError(err) {
		return fmt.Errorf("drop checksum manifest of %s: %w", filePath, err)
	}
	return nil
}

// writeChunkSums uploads the manifest once every chunk is durable. Advisory:
// a failure leaves the file readable, just unverified.
func (cm *DefaultCacheManager) writeChunkSums(ctx context.Context, filePath string, b *chunkSumsBuilder) {
	if cm.cloudStorage == nil || !cm.config.VerifyRemoteChunks || b == nil || len(b.crcs) == 0 {
		return
	}
	var size int64
	for i, c := range b.crcs {
		if c == "" {
			return // a chunk was never hashed: no honest manifest
		}
		size += b.lens[i]
	}
	raw, err := json.Marshal(chunkSumsManifest{Version: 1, Size: size, ChunkSize: b.lens[0], CRC32C: b.crcs})
	if err != nil {
		return
	}
	timeout := cm.config.CloudTimeout
	if timeout <= 0 {
		timeout = chunkSumsFetchCap
	}
	wctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	if err := cm.cloudStorage.Write(wctx, chunkSumsKey(filePath), raw); err != nil {
		cm.metrics.ChunkSumsWriteFailed.Add(1)
		cm.logger.Printf("Checksum manifest for %s not written: %v (chunks stay readable, unverified)", filePath, err)
		return
	}
	cm.metrics.ChunkSumsWritten.Add(1)
	cm.chunkSums.invalidate(filePath)
}
