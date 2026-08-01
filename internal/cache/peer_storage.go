package cache

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"math/big"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"fuse-client/internal/coordinator"
	pb "fuse-client/internal/pb"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

const (
	peerGRPCInitialWindowBytes     = 64 * 1024 * 1024
	peerGRPCInitialConnWindowBytes = 256 * 1024 * 1024
	peerGRPCMaxMessageBytes        = 128 * 1024 * 1024
	peerDefaultReadConnPerPeer     = 8
)

// PeerStorage implements TierStorage for peer-to-peer storage via gRPC.
type PeerStorage struct {
	coordinator         coordinator.Coordinator
	timeout             time.Duration
	minReplicationCount int
	localPeerID         string
	sortByNetwork       bool
	parallelFanout      bool
	connMu              sync.RWMutex
	connPool            map[string]*grpc.ClientConn
	readConnMu          sync.RWMutex
	readConnPool        map[string][]*grpc.ClientConn
	readConnPerPeer     int
	metaMu              sync.RWMutex
	peersCache          []*coordinator.PeerInfo
	peersCacheAt        time.Time
	peersCacheTTL       time.Duration
	fileHints           map[string]fileHintCacheEntry
	fileHintsTTL        time.Duration
	rawTransport        bool
	apiKey              string
	httpClient          *http.Client

	// promoteSink, when set, lets raw HTTP reads stream arriving bytes to
	// local NVMe while the response buffer fills (tee), so promotion costs no
	// second full-file write after the transfer. Nil disables streaming
	// promotion; the cache manager's background promote path still applies.
	promoteSink PromotionSink

	// replicationStagger is the base jittered delay between successive replica
	// writes of one object; 0 uses the default.
	replicationStagger time.Duration

	// pairLatency tracks observed this-node→peer latency/success EWMAs from
	// real transfers; traversal prefers measured-close peers over the
	// coordinator's single-target probe estimates.
	pairLatency *peerLatencyTracker

	// Thundering-herd control counters (exposed via PeerLoadSnapshot).
	busySkipsTotal     atomic.Int64
	jitterRetriesTotal atomic.Int64
	replBusySkipsTotal atomic.Int64
	replStaggersTotal  atomic.Int64
}

// peerBusyError marks a "source is at capacity" rejection (gRPC
// RESOURCE_EXHAUSTED or raw HTTP 503) as distinct from a miss or a failure:
// the holder has the data but is shedding load, so the right reaction is to
// fail over to another holder and retry this one only after jitter.
type peerBusyError struct {
	addr string
}

func (e *peerBusyError) Error() string {
	return fmt.Sprintf("peer %s busy (serve capacity exhausted)", e.addr)
}

// isPeerBusy reports whether err is a serve-side admission rejection.
func isPeerBusy(err error) bool {
	if err == nil {
		return false
	}
	var busy *peerBusyError
	if errors.As(err, &busy) {
		return true
	}
	return status.Code(err) == codes.ResourceExhausted
}

type fileHintCacheEntry struct {
	peerIDs   map[string]struct{}
	expiresAt time.Time
}

// NewPeerStorage creates a new peer storage instance. When rawTransport is set,
// bulk chunk reads prefer a plain-HTTP transport (with sendfile on the serving
// side) over gRPC, falling back to gRPC on any error. apiKey, when non-empty,
// is sent as X-API-Key on the HTTP read requests.
func NewPeerStorage(coord coordinator.Coordinator, timeout time.Duration, localPeerID string, sortByNetwork bool, parallelFanout bool, rawTransport bool, apiKey string) (*PeerStorage, error) {
	return &PeerStorage{
		coordinator:         coord,
		timeout:             timeout,
		minReplicationCount: 3,
		localPeerID:         localPeerID,
		sortByNetwork:       sortByNetwork,
		parallelFanout:      parallelFanout,
		connPool:            make(map[string]*grpc.ClientConn),
		readConnPool:        make(map[string][]*grpc.ClientConn),
		readConnPerPeer:     peerDefaultReadConnPerPeer,
		peersCacheTTL:       2 * time.Second,
		fileHints:           make(map[string]fileHintCacheEntry),
		fileHintsTTL:        5 * time.Second,
		rawTransport:        rawTransport,
		apiKey:              apiKey,
		pairLatency:         newPeerLatencyTracker(),
		httpClient: &http.Client{
			Transport: &http.Transport{
				MaxIdleConns:          256,
				MaxIdleConnsPerHost:   32,
				IdleConnTimeout:       90 * time.Second,
				ResponseHeaderTimeout: timeout,
				WriteBufferSize:       256 * 1024,
				ReadBufferSize:        256 * 1024,
			},
		},
	}, nil
}

func dialPeerConn(addr string) (*grpc.ClientConn, error) {
	conn, err := grpc.Dial(addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithInitialWindowSize(peerGRPCInitialWindowBytes),
		grpc.WithInitialConnWindowSize(peerGRPCInitialConnWindowBytes),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(peerGRPCMaxMessageBytes),
			grpc.MaxCallSendMsgSize(peerGRPCMaxMessageBytes),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to dial peer %s: %v", addr, err)
	}
	return conn, nil
}

// getOrDial returns a cached gRPC connection or dials a new one.
func (ps *PeerStorage) getOrDial(addr string) (*grpc.ClientConn, error) {
	ps.connMu.RLock()
	conn, ok := ps.connPool[addr]
	ps.connMu.RUnlock()
	if ok {
		return conn, nil
	}

	ps.connMu.Lock()
	defer ps.connMu.Unlock()

	// Double-check after acquiring write lock
	if conn, ok := ps.connPool[addr]; ok {
		return conn, nil
	}

	conn, err := dialPeerConn(addr)
	if err != nil {
		return nil, err
	}
	ps.connPool[addr] = conn
	return conn, nil
}

func (ps *PeerStorage) readConnSlot(path string) int {
	if ps.readConnPerPeer <= 1 {
		return 0
	}
	return peerStartIndexForKey(path+"#read-conn", ps.readConnPerPeer)
}

func (ps *PeerStorage) getOrDialReadConn(addr, path string) (*grpc.ClientConn, int, error) {
	slot := ps.readConnSlot(path)

	ps.readConnMu.RLock()
	if conns, ok := ps.readConnPool[addr]; ok && slot < len(conns) && conns[slot] != nil {
		conn := conns[slot]
		ps.readConnMu.RUnlock()
		return conn, slot, nil
	}
	ps.readConnMu.RUnlock()

	ps.readConnMu.Lock()
	defer ps.readConnMu.Unlock()

	conns, ok := ps.readConnPool[addr]
	if !ok || len(conns) < ps.readConnPerPeer {
		newConns := make([]*grpc.ClientConn, ps.readConnPerPeer)
		copy(newConns, conns)
		conns = newConns
		ps.readConnPool[addr] = conns
	}
	if conns[slot] != nil {
		return conns[slot], slot, nil
	}

	conn, err := dialPeerConn(addr)
	if err != nil {
		return nil, slot, err
	}
	conns[slot] = conn
	ps.readConnPool[addr] = conns
	return conn, slot, nil
}

func (ps *PeerStorage) evictReadConn(addr string, slot int) {
	ps.readConnMu.Lock()
	var conn *grpc.ClientConn
	if conns, ok := ps.readConnPool[addr]; ok && slot >= 0 && slot < len(conns) {
		conn = conns[slot]
		conns[slot] = nil
	}
	ps.readConnMu.Unlock()
	if conn != nil {
		_ = conn.Close()
	}
}

func (ps *PeerStorage) evictConn(addr string) {
	ps.connMu.Lock()
	conn, ok := ps.connPool[addr]
	if ok {
		delete(ps.connPool, addr)
	}
	ps.connMu.Unlock()
	if ok && conn != nil {
		_ = conn.Close()
	}
}

// Read reads a file from peer storage using parallel fan-out.
func (ps *PeerStorage) Read(ctx context.Context, path string) ([]byte, error) {
	peers, err := ps.getPeers(ctx)
	if err != nil {
		return nil, err
	}
	preferred, fallback := ps.partitionPeersForPath(ctx, path, peers)
	preferred = orderedPeersForPath(preferred, path)
	fallback = orderedPeersForPath(fallback, path+"#fallback")

	ordered := make([]*coordinator.PeerInfo, 0, len(preferred)+len(fallback))
	ordered = append(ordered, preferred...)
	ordered = append(ordered, fallback...)
	candidates := make([]*coordinator.PeerInfo, 0, len(ordered))
	for _, peer := range ordered {
		if peer == nil || peer.Status != "active" || peer.GRPCAddress == "" {
			continue
		}
		candidates = append(candidates, peer)
	}
	if len(candidates) == 0 {
		return nil, fmt.Errorf("no active peers available")
	}
	if ps.sortByNetwork {
		sortPeersByNetwork(candidates, path)
	}
	// Observed pairwise latency beats the coordinator's single-target probe:
	// once real transfer samples exist, traverse measured-closest peers first.
	ps.sortByObservedLatency(candidates, path)

	var lastErr error
	var busyPeers []*coordinator.PeerInfo
	// sequentialStart is the first candidate the sequential loop below should
	// try. Only candidates already attempted by the parallel fan-out are
	// skipped; with fanout <= 1 no one has been tried yet, so start at 0.
	// (Starting at parallelFanout unconditionally silently skipped the best
	// candidate for every non-fanout read, and all peers when only one
	// candidate existed.)
	sequentialStart := 0
	parallelFanout := peerReadParallelFanout(path, len(candidates), ps.parallelFanout)
	if parallelFanout > 1 {
		if data, err := ps.readFromPeersParallel(ctx, path, candidates[:parallelFanout]); err == nil {
			return data, nil
		} else {
			if isPeerBusy(err) {
				ps.busySkipsTotal.Add(1)
				busyPeers = append(busyPeers, candidates[:parallelFanout]...)
			}
			lastErr = err
		}
		sequentialStart = parallelFanout
	}

	for i := sequentialStart; i < len(candidates); i++ {
		peer := candidates[i]
		data, err := ps.readPeerData(ctx, peer, path)
		if err == nil {
			return data, nil
		}
		// A busy holder has the data but is shedding load: move on to the next
		// candidate immediately and remember this one for a jittered retry.
		if isPeerBusy(err) {
			ps.busySkipsTotal.Add(1)
			busyPeers = append(busyPeers, peer)
		}
		lastErr = err
	}

	// All non-busy candidates failed. If some holders were merely busy, retry
	// them once after randomized jitter — the spread de-synchronizes the herd
	// so the source drains instead of absorbing synchronized retries.
	if len(busyPeers) > 0 && ctx.Err() == nil {
		ps.jitterRetriesTotal.Add(1)
		if err := sleepWithJitter(ctx, peerBusyRetryMinWait, peerBusyRetryMaxWait); err != nil {
			return nil, lastErr
		}
		for _, peer := range busyPeers {
			data, err := ps.readPeerData(ctx, peer, path)
			if err == nil {
				return data, nil
			}
			lastErr = err
		}
	}

	if lastErr != nil {
		return nil, lastErr
	}
	return nil, fmt.Errorf("file not found on any peer")
}

func sortPeersByNetwork(peers []*coordinator.PeerInfo, key string) {
	if len(peers) <= 1 {
		return
	}
	sort.SliceStable(peers, func(i, j int) bool {
		pi := peerSortScore(peers[i])
		pj := peerSortScore(peers[j])
		if pi == pj {
			// Deterministic tie break keeps chunk distribution stable.
			hi := peerStartIndexForKey(key+"#"+peers[i].ID, 1<<30)
			hj := peerStartIndexForKey(key+"#"+peers[j].ID, 1<<30)
			return hi < hj
		}
		return pi > pj
	})
}

func peerSortScore(peer *coordinator.PeerInfo) float64 {
	if peer == nil {
		return -1
	}
	speed := peer.NetworkSpeedMBps
	if speed <= 0 {
		speed = 100 // fallback neutral score when probe data is absent
	}
	lat := peer.NetworkLatencyMs
	if lat <= 0 {
		lat = 1
	}
	return speed / lat
}

func peerReadParallelFanout(path string, candidateCount int, enabled bool) int {
	if !enabled {
		return 1
	}
	if candidateCount <= 1 {
		return candidateCount
	}
	// Chunk reads benefit most from immediate multi-peer fan-out.
	if _, ok := chunkIndexFromPath(path); ok {
		if candidateCount >= 2 {
			return 2
		}
	}
	return 1
}

func (ps *PeerStorage) readFromPeersParallel(ctx context.Context, path string, peers []*coordinator.PeerInfo) ([]byte, error) {
	if len(peers) == 0 {
		return nil, fmt.Errorf("no peers provided")
	}
	if len(peers) == 1 {
		return ps.readPeerData(ctx, peers[0], path)
	}

	type readResult struct {
		data []byte
		err  error
	}

	readCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	resultCh := make(chan readResult, len(peers))
	var wg sync.WaitGroup
	for _, peer := range peers {
		p := peer
		wg.Add(1)
		go func() {
			defer wg.Done()
			data, err := ps.readPeerData(readCtx, p, path)
			resultCh <- readResult{data: data, err: err}
		}()
	}

	var lastErr error
	for i := 0; i < len(peers); i++ {
		res := <-resultCh
		if res.err == nil {
			cancel()
			go func() {
				wg.Wait()
				close(resultCh)
			}()
			return res.data, nil
		}
		lastErr = res.err
	}
	wg.Wait()
	close(resultCh)

	if lastErr == nil {
		lastErr = fmt.Errorf("parallel peer read failed")
	}
	return nil, lastErr
}

func orderedPeersForPath(peers []*coordinator.PeerInfo, key string) []*coordinator.PeerInfo {
	if len(peers) <= 1 {
		return peers
	}
	start := peerStartIndexForKey(key, len(peers))
	ordered := make([]*coordinator.PeerInfo, 0, len(peers))
	ordered = append(ordered, peers[start:]...)
	ordered = append(ordered, peers[:start]...)
	return ordered
}

func peerStartIndexForKey(key string, count int) int {
	if count <= 1 {
		return 0
	}
	if idx, ok := chunkIndexFromPath(key); ok {
		if idx < 0 {
			idx = -idx
		}
		return int(idx % int64(count))
	}
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	return int(h.Sum32() % uint32(count))
}

func chunkIndexFromPath(path string) (int64, bool) {
	idx := strings.LastIndex(path, "_chunk_")
	if idx <= 0 {
		return 0, false
	}
	n, err := strconv.ParseInt(path[idx+len("_chunk_"):], 10, 64)
	if err != nil {
		return 0, false
	}
	return n, true
}

func (ps *PeerStorage) partitionPeersForPath(ctx context.Context, path string, peers []*coordinator.PeerInfo) ([]*coordinator.PeerInfo, []*coordinator.PeerInfo) {
	if len(peers) <= 1 {
		return peers, nil
	}
	peerIDs := ps.peerIDsForPath(ctx, path)
	if len(peerIDs) == 0 {
		return nil, peers
	}

	prioritized := make([]*coordinator.PeerInfo, 0, len(peers))
	rest := make([]*coordinator.PeerInfo, 0, len(peers))
	for _, peer := range peers {
		if peer == nil || peer.ID == "" {
			continue
		}
		if _, ok := peerIDs[peer.ID]; ok {
			prioritized = append(prioritized, peer)
		} else {
			rest = append(rest, peer)
		}
	}
	if len(prioritized) == 0 {
		return nil, peers
	}
	return prioritized, rest
}

func (ps *PeerStorage) peerIDsForPath(ctx context.Context, path string) map[string]struct{} {
	now := time.Now()
	hintKey := fileHintKey(path)

	ps.metaMu.RLock()
	if hint, ok := ps.fileHints[hintKey]; ok && now.Before(hint.expiresAt) {
		peerIDs := clonePeerIDSet(hint.peerIDs)
		ps.metaMu.RUnlock()
		return peerIDs
	}
	ps.metaMu.RUnlock()

	out := make(map[string]struct{})
	collect := func(target string) bool {
		if target == "" {
			return false
		}
		callCtx, cancel := context.WithTimeout(ctx, ps.timeout)
		defer cancel()
		locations, err := ps.coordinator.GetFileLocation(callCtx, target)
		if err != nil {
			return false
		}
		found := false
		for _, loc := range locations {
			if loc == nil || loc.PeerID == "" {
				continue
			}
			if loc.PeerID == ps.localPeerID {
				continue
			}
			out[loc.PeerID] = struct{}{}
			found = true
		}
		return found
	}

	if parent, ok := parentFilePathFromChunkPath(path); ok {
		// Chunk metadata is usually published at parent-file granularity.
		// Avoid per-chunk coordinator lookups that add latency and pressure.
		_ = collect(parent)
		ps.putFileHint(hintKey, out)
		return out
	}
	if collect(path) {
		ps.putFileHint(hintKey, out)
		return out
	}
	if len(out) == 0 {
		ps.putFileHint(hintKey, out)
	}
	return out
}

func (ps *PeerStorage) putFileHint(path string, peerIDs map[string]struct{}) {
	ps.metaMu.Lock()
	ps.fileHints[path] = fileHintCacheEntry{
		peerIDs:   clonePeerIDSet(peerIDs),
		expiresAt: time.Now().Add(ps.fileHintsTTL),
	}
	ps.metaMu.Unlock()
}

func clonePeerIDSet(src map[string]struct{}) map[string]struct{} {
	dst := make(map[string]struct{}, len(src))
	for k := range src {
		dst[k] = struct{}{}
	}
	return dst
}

func fileHintKey(path string) string {
	if parent, ok := parentFilePathFromChunkPath(path); ok {
		return parent
	}
	return path
}

// startGC launches a background goroutine that removes expired fileHints entries.
// Call after creating PeerStorage. The goroutine exits when ctx is cancelled.
func (ps *PeerStorage) startGC(ctx context.Context) {
	gcInterval := ps.fileHintsTTL * 10
	if gcInterval < 30*time.Second {
		gcInterval = 30 * time.Second
	}
	go func() {
		ticker := time.NewTicker(gcInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				now := time.Now()
				ps.metaMu.Lock()
				for k, h := range ps.fileHints {
					if now.After(h.expiresAt) {
						delete(ps.fileHints, k)
					}
				}
				ps.metaMu.Unlock()
			}
		}
	}()
}

// Write writes a file to peer storage with replication.
//
// Thundering-herd control (Phase 1/2 of the plan): replica targets are chosen
// by headroom (available space + network score) instead of a pure random
// shuffle, successive replica RPCs are staggered with jitter so write-time
// replication does not itself stampede the network, and targets that signal
// busy (serve-side admission control) are skipped rather than retried.
func (ps *PeerStorage) Write(ctx context.Context, path string, data []byte) error {
	peers, err := ps.getPeers(ctx)
	if err != nil {
		return err
	}

	// Shuffle first so equally-scored peers get load spread across objects,
	// then prefer targets with more headroom.
	cryptoShuffle(peers)
	sortPeersByReplicationScore(peers)

	successCount := 0
	var lastErr error

	for _, peer := range peers {
		if successCount >= ps.minReplicationCount {
			break
		}
		if peer.Status != "active" || peer.GRPCAddress == "" {
			continue
		}
		// Stagger replicas after the first so one write does not burst
		// minReplicationCount transfers onto the network at the same instant.
		if successCount > 0 {
			stagger := ps.replicationStagger
			if stagger <= 0 {
				stagger = defaultPeerReplicationStagger
			}
			ps.replStaggersTotal.Add(1)
			if err := sleepWithJitter(ctx, stagger/2, stagger*3/2); err != nil {
				break // context cancelled; keep what we have
			}
		}
		if err := ps.writeToPeer(ctx, peer.GRPCAddress, path, data); err == nil {
			successCount++
		} else if isPeerBusy(err) {
			// Busy target: skip it (replication is best-effort; the reconciler
			// or read-promotion will top up replicas later).
			ps.replBusySkipsTotal.Add(1)
			lastErr = err
		} else {
			lastErr = err
		}
	}

	if successCount > 0 {
		if successCount < ps.minReplicationCount {
			log.Printf("[CACHE] WARNING: peer replication for %s: %d/%d replicas written",
				path, successCount, ps.minReplicationCount)
		}
		return nil
	}

	if lastErr != nil {
		return fmt.Errorf("failed to write to any peer: %v", lastErr)
	}
	return fmt.Errorf("no active peers available")
}

// ReplicateTo writes path to up to count additional peers, excluding the IDs
// in exclude (peers that already hold the object). Targets are chosen by
// headroom score, RPCs are staggered with jitter, and busy targets are
// skipped — the replica top-up used by the Phase 3 reconciler must never
// itself stampede. Returns how many replicas were written.
func (ps *PeerStorage) ReplicateTo(ctx context.Context, path string, data []byte, exclude map[string]struct{}, count int) (int, error) {
	if count <= 0 {
		return 0, nil
	}
	peers, err := ps.getPeers(ctx)
	if err != nil {
		return 0, err
	}

	candidates := make([]*coordinator.PeerInfo, 0, len(peers))
	for _, peer := range peers {
		if peer == nil || peer.Status != "active" || peer.GRPCAddress == "" {
			continue
		}
		if _, held := exclude[peer.ID]; held {
			continue
		}
		candidates = append(candidates, peer)
	}
	cryptoShuffle(candidates)
	sortPeersByReplicationScore(candidates)

	written := 0
	var lastErr error
	for _, peer := range candidates {
		if written >= count {
			break
		}
		if written > 0 {
			stagger := ps.replicationStagger
			if stagger <= 0 {
				stagger = defaultPeerReplicationStagger
			}
			ps.replStaggersTotal.Add(1)
			if err := sleepWithJitter(ctx, stagger/2, stagger*3/2); err != nil {
				break
			}
		}
		if err := ps.writeToPeer(ctx, peer.GRPCAddress, path, data); err == nil {
			written++
		} else if isPeerBusy(err) {
			ps.replBusySkipsTotal.Add(1)
			lastErr = err
		} else {
			lastErr = err
		}
	}
	if written == 0 && lastErr != nil {
		return 0, lastErr
	}
	return written, nil
}

// Delete removes a file from peer storage.
func (ps *PeerStorage) Delete(ctx context.Context, path string) error {
	peers, err := ps.getPeers(ctx)
	if err != nil {
		return err
	}

	var lastErr error
	for _, peer := range peers {
		if peer.Status != "active" || peer.GRPCAddress == "" {
			continue
		}
		if err := ps.deleteFromPeer(ctx, peer.GRPCAddress, path); err != nil {
			lastErr = err
		}
	}
	return lastErr
}

// Exists checks if a file exists in peer storage.
func (ps *PeerStorage) Exists(ctx context.Context, path string) bool {
	peers, err := ps.getPeers(ctx)
	if err != nil {
		return false
	}

	for _, peer := range peers {
		if peer.Status != "active" || peer.GRPCAddress == "" {
			continue
		}
		if ps.existsOnPeer(ctx, peer.GRPCAddress, path) {
			return true
		}
	}
	return false
}

// Size returns the size of a file in peer storage.
func (ps *PeerStorage) Size(ctx context.Context, path string) (int64, error) {
	peers, err := ps.getPeers(ctx)
	if err != nil {
		return 0, err
	}

	for _, peer := range peers {
		if peer.Status != "active" || peer.GRPCAddress == "" {
			continue
		}
		if size, err := ps.sizeOnPeer(ctx, peer.GRPCAddress, path); err == nil {
			return size, nil
		}
	}
	return 0, fmt.Errorf("file not found on any peer")
}

// Close closes all pooled gRPC connections.
func (ps *PeerStorage) Close() {
	ps.connMu.Lock()
	for addr, conn := range ps.connPool {
		conn.Close()
		delete(ps.connPool, addr)
	}
	ps.connMu.Unlock()

	ps.readConnMu.Lock()
	for addr, conns := range ps.readConnPool {
		for i, conn := range conns {
			if conn == nil {
				continue
			}
			_ = conn.Close()
			conns[i] = nil
		}
		delete(ps.readConnPool, addr)
	}
	ps.readConnMu.Unlock()
}

// sortPeersByReplicationScore orders replica targets by headroom, best first:
// available space weighted by network score. Peers without probe/space data
// keep a neutral score, so with no signals the pre-existing random shuffle
// order is preserved (sort is stable).
func sortPeersByReplicationScore(peers []*coordinator.PeerInfo) {
	if len(peers) <= 1 {
		return
	}
	sort.SliceStable(peers, func(i, j int) bool {
		return peerReplicationScore(peers[i]) > peerReplicationScore(peers[j])
	})
}

// peerReplicationScore is a higher-is-better headroom score for choosing
// write-replication targets.
func peerReplicationScore(peer *coordinator.PeerInfo) float64 {
	if peer == nil {
		return -1
	}
	// Normalize available space to GiB so it composes with the network score
	// on comparable magnitudes; unknown space gets a neutral 1.0.
	spaceGiB := 1.0
	if peer.AvailableSpace > 0 {
		spaceGiB = float64(peer.AvailableSpace) / (1 << 30)
	}
	return spaceGiB * peerSortScore(peer)
}

// cryptoShuffle performs a Fisher-Yates shuffle using crypto/rand.
func cryptoShuffle(peers []*coordinator.PeerInfo) {
	for i := len(peers) - 1; i > 0; i-- {
		n, err := rand.Int(rand.Reader, big.NewInt(int64(i+1)))
		if err != nil {
			continue
		}
		j := int(n.Int64())
		peers[i], peers[j] = peers[j], peers[i]
	}
}

func (ps *PeerStorage) getPeers(ctx context.Context) ([]*coordinator.PeerInfo, error) {
	ps.metaMu.RLock()
	if time.Since(ps.peersCacheAt) < ps.peersCacheTTL && len(ps.peersCache) > 0 {
		cached := make([]*coordinator.PeerInfo, 0, len(ps.peersCache))
		for _, peer := range ps.peersCache {
			if peer != nil {
				cached = append(cached, peer)
			}
		}
		ps.metaMu.RUnlock()
		return cached, nil
	}
	ps.metaMu.RUnlock()

	callCtx, cancel := context.WithTimeout(ctx, ps.timeout)
	defer cancel()
	peers, err := ps.coordinator.GetPeers(callCtx, "")
	if err != nil {
		return nil, err
	}
	if ps.localPeerID == "" {
		ps.metaMu.Lock()
		ps.peersCache = peers
		ps.peersCacheAt = time.Now()
		ps.metaMu.Unlock()
		return peers, nil
	}

	filtered := make([]*coordinator.PeerInfo, 0, len(peers))
	for _, peer := range peers {
		if peer == nil || peer.ID == "" {
			continue
		}
		if peer.ID == ps.localPeerID {
			continue
		}
		filtered = append(filtered, peer)
	}
	ps.metaMu.Lock()
	ps.peersCache = filtered
	ps.peersCacheAt = time.Now()
	ps.metaMu.Unlock()
	return filtered, nil
}

// readPeerData reads a path from one peer, preferring the raw HTTP transport
// (with sendfile on the serving side) when enabled, and falling back to the
// gRPC path on any raw error. Every completed attempt (success or failure)
// feeds the pairwise latency tracker; busy rejections do not, since they are
// admission control rather than network signal.
func (ps *PeerStorage) readPeerData(ctx context.Context, peer *coordinator.PeerInfo, path string) ([]byte, error) {
	start := time.Now()
	data, err := ps.readPeerDataInner(ctx, peer, path)
	if peer != nil && !isPeerBusy(err) && ctx.Err() == nil {
		ps.pairLatency.record(peer.ID, time.Since(start), err == nil)
	}
	return data, err
}

func (ps *PeerStorage) readPeerDataInner(ctx context.Context, peer *coordinator.PeerInfo, path string) ([]byte, error) {
	if ps.rawTransport && peer != nil && peer.Address != "" {
		data, err := ps.readFromPeerRaw(ctx, peer.Address, path)
		if err == nil {
			return data, nil
		}
		// Busy means the node is shedding load via admission control; retrying
		// the same node over gRPC would just hit the same full gate. Surface it
		// so the caller fails over to another holder.
		if isPeerBusy(err) {
			return nil, err
		}
		// Fall through to gRPC on any other raw-transport error.
	}
	return ps.readFromPeer(ctx, peer.GRPCAddress, path)
}

// bestEffortWriter forwards writes to w until one fails, then silently drops
// the rest: the promotion is abandoned (Abort) but the read continues.
type bestEffortWriter struct {
	w      io.Writer
	failed bool
}

func (b *bestEffortWriter) Write(p []byte) (int, error) {
	if !b.failed {
		if _, err := b.w.Write(p); err != nil {
			b.failed = true
		}
	}
	return len(p), nil
}

// readFromPeerRaw fetches a path from a peer's plain-HTTP bulk-read endpoint.
func (ps *PeerStorage) readFromPeerRaw(ctx context.Context, httpAddr, path string) ([]byte, error) {
	if ps.httpClient == nil {
		return nil, fmt.Errorf("raw transport not initialized")
	}
	reqCtx, cancel := context.WithTimeout(ctx, ps.timeout)
	defer cancel()

	endpoint := "http://" + httpAddr + "/api/peer/read?path=" + url.QueryEscape(path)
	req, err := http.NewRequestWithContext(reqCtx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, err
	}
	if ps.apiKey != "" {
		req.Header.Set("X-API-Key", ps.apiKey)
	}

	resp, err := ps.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() {
		// Drain so the connection can be reused.
		_, _ = io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
	}()

	if resp.StatusCode == http.StatusServiceUnavailable {
		return nil, &peerBusyError{addr: httpAddr}
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("peer raw read %s: status %d", httpAddr, resp.StatusCode)
	}

	// Size the buffer from Content-Length when known to avoid reallocation.
	if resp.ContentLength >= 0 {
		// Streaming promotion: tee arriving bytes to local NVMe while the
		// buffer fills, so promotion costs no second full-object write (and
		// no background copy) after the transfer. Failure to promote never
		// fails the read.
		if ps.promoteSink != nil {
			if promo, ok := ps.promoteSink.BeginPromotion(path, resp.ContentLength); ok {
				// bestEffortWriter: a disk-write failure abandons the
				// promotion but must never fail the network read, so the tee
				// swallows write errors (io.TeeReader would surface them).
				bw := &bestEffortWriter{w: promo}
				buf := make([]byte, resp.ContentLength)
				if _, err := io.ReadFull(io.TeeReader(resp.Body, bw), buf); err != nil {
					promo.Abort()
					return nil, err
				}
				if bw.failed {
					promo.Abort()
				} else {
					promo.Commit()
				}
				return buf, nil
			}
		}
		buf := make([]byte, resp.ContentLength)
		if _, err := io.ReadFull(resp.Body, buf); err != nil {
			return nil, err
		}
		return buf, nil
	}
	return io.ReadAll(resp.Body)
}

func (ps *PeerStorage) readFromPeer(ctx context.Context, grpcAddr, path string) ([]byte, error) {
	readWithConn := func(conn *grpc.ClientConn) ([]byte, error) {
		client := pb.NewPeerServiceClient(conn)
		callCtx, cancel := context.WithTimeout(ctx, ps.timeout)
		defer cancel()
		stream, err := client.ReadFile(callCtx, &pb.ReadFileRequest{Path: path})
		if err != nil {
			return nil, err
		}

		first, err := stream.Recv()
		if err == io.EOF {
			return []byte{}, nil
		}
		if err != nil {
			return nil, err
		}

		second, err := stream.Recv()
		if err == io.EOF {
			// Single-message payload (common case for 8MiB cache chunks with
			// larger gRPC frame size): return without an extra copy.
			return first.Data, nil
		}
		if err != nil {
			return nil, err
		}

		// Multi-message payload: allocate once and append all fragments.
		capHint := len(first.Data) + len(second.Data)*2
		if capHint < len(first.Data)+len(second.Data) {
			capHint = len(first.Data) + len(second.Data)
		}
		data := make([]byte, 0, capHint)
		data = append(data, first.Data...)
		data = append(data, second.Data...)

		for {
			chunk, err := stream.Recv()
			if err == io.EOF {
				break
			}
			if err != nil {
				return nil, err
			}
			data = append(data, chunk.Data...)
		}
		return data, nil
	}

	conn, slot, err := ps.getOrDialReadConn(grpcAddr, path)
	if err != nil {
		return nil, err
	}
	data, err := readWithConn(conn)
	if err == nil {
		return data, nil
	}
	if ctx.Err() != nil {
		return nil, err
	}
	// Admission-control rejection: the connection is healthy, the peer is just
	// shedding load. Do not evict/reconnect-retry — surface busy to the caller
	// so it fails over to another holder.
	if isPeerBusy(err) {
		return nil, err
	}

	// On stream failures, drop this read-slot connection and retry once.
	ps.evictReadConn(grpcAddr, slot)
	retryConn, _, dialErr := ps.getOrDialReadConn(grpcAddr, path)
	if dialErr != nil {
		return nil, fmt.Errorf("peer read failed and reconnect failed for %s: read=%v reconnect=%v", grpcAddr, err, dialErr)
	}
	retryData, retryErr := readWithConn(retryConn)
	if retryErr == nil {
		return retryData, nil
	}
	return nil, fmt.Errorf("peer read failed after reconnect for %s: first=%v retry=%v", grpcAddr, err, retryErr)
}

func (ps *PeerStorage) writeToPeer(ctx context.Context, grpcAddr, path string, data []byte) error {
	conn, err := ps.getOrDial(grpcAddr)
	if err != nil {
		return err
	}

	client := pb.NewPeerServiceClient(conn)
	callCtx, cancel := context.WithTimeout(ctx, ps.timeout)
	defer cancel()
	stream, err := client.WriteFile(callCtx)
	if err != nil {
		return err
	}

	// Send first message with metadata + first chunk
	chunkSize := grpcChunkSize
	firstEnd := chunkSize
	if firstEnd > len(data) {
		firstEnd = len(data)
	}

	if err := stream.Send(&pb.WriteFileRequest{
		Path:      path,
		TotalSize: int64(len(data)),
		Data:      data[:firstEnd],
	}); err != nil {
		return err
	}

	// Send remaining chunks
	for offset := firstEnd; offset < len(data); offset += chunkSize {
		end := offset + chunkSize
		if end > len(data) {
			end = len(data)
		}
		if err := stream.Send(&pb.WriteFileRequest{
			Data: data[offset:end],
		}); err != nil {
			return err
		}
	}

	_, err = stream.CloseAndRecv()
	return err
}

func (ps *PeerStorage) deleteFromPeer(ctx context.Context, grpcAddr, path string) error {
	conn, err := ps.getOrDial(grpcAddr)
	if err != nil {
		return err
	}
	client := pb.NewPeerServiceClient(conn)
	callCtx, cancel := context.WithTimeout(ctx, ps.timeout)
	defer cancel()
	_, err = client.DeleteFile(callCtx, &pb.DeleteFileRequest{Path: path})
	return err
}

func (ps *PeerStorage) existsOnPeer(ctx context.Context, grpcAddr, path string) bool {
	conn, err := ps.getOrDial(grpcAddr)
	if err != nil {
		return false
	}
	client := pb.NewPeerServiceClient(conn)
	callCtx, cancel := context.WithTimeout(ctx, ps.timeout)
	defer cancel()
	resp, err := client.FileExists(callCtx, &pb.FileExistsRequest{Path: path})
	if err != nil {
		return false
	}
	return resp.Exists
}

func (ps *PeerStorage) sizeOnPeer(ctx context.Context, grpcAddr, path string) (int64, error) {
	conn, err := ps.getOrDial(grpcAddr)
	if err != nil {
		return 0, err
	}
	client := pb.NewPeerServiceClient(conn)
	callCtx, cancel := context.WithTimeout(ctx, ps.timeout)
	defer cancel()
	resp, err := client.FileSize(callCtx, &pb.FileSizeRequest{Path: path})
	if err != nil {
		return 0, err
	}
	return resp.Size, nil
}
