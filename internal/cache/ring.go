package cache

import (
	"context"
	"hash/fnv"
	"math"
	"sort"
	"time"

	"fuse-client/internal/coordinator"
)

// Ring placement: rendezvous (highest-random-weight) hashing per chunk.
//
// Without it, a blob lives whole on whichever node fetched it first, and
// every other node reads every chunk from that one holder: on the Azure
// 6-node fan-out, five concurrent readers raised each read from 8 to 11 ms
// and spread readiness over 46 s. With it, each chunk path has R owners
// that any node computes from the live peer list alone (no coordinator
// lookup), so one blob's chunks spread over the cluster and readers pull
// from many nodes at once. tools/cachesim (10 images x 20 models, 16 nodes)
// put R=2 at p95 224-292 s vs 321-475 s without; R=3 added nothing.
//
// Owners are a placement hint, never a correctness requirement:
//
//   - reads try owners first, then the holders the coordinator lists, then
//     the rest, exactly as before; an owner that lacks the chunk is a typed
//     miss and the next candidate is tried;
//   - a node that fetches a chunk it owns keeps it (fills/promotions land
//     the same way they always did), so owners become holders as the
//     cluster reads;
//   - the replica reconciler places copies on owners first, so readers find
//     them where they look.
//
// Weighted by each peer's cache size (used + available), so a node with 4x
// the NVMe owns about 4x the chunks. Membership is the active peer list; a
// restarting node drops out and its chunks move to the next-ranked owners
// (about 1/N of chunks), and back when it returns.

// ringOwners ranks peers for key and returns them all, best first. The
// first R are the owners; callers use the full order as their read order.
func ringOwners(peers []*coordinator.PeerInfo, key string) []*coordinator.PeerInfo {
	type scored struct {
		p *coordinator.PeerInfo
		s float64
	}
	out := make([]scored, 0, len(peers))
	for _, p := range peers {
		if p == nil || p.ID == "" {
			continue
		}
		h := fnv.New64a()
		_, _ = h.Write([]byte(p.ID))
		_, _ = h.Write([]byte{0})
		_, _ = h.Write([]byte(key))
		// FNV alone keeps too much structure for IDs and keys that differ in
		// a digit (3 of 16 nodes owned nothing in the spread test); a
		// splitmix64 finaliser makes the bits uniform. Uniform in (0,1)
		// from the top 53 bits.
		u := (float64(mix64(h.Sum64())>>11) + 0.5) / float64(uint64(1)<<53)
		w := ringWeight(p)
		out = append(out, scored{p, -w / math.Log(u)})
	}
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].s != out[j].s {
			return out[i].s > out[j].s
		}
		return out[i].p.ID < out[j].p.ID
	})
	ranked := make([]*coordinator.PeerInfo, len(out))
	for i, s := range out {
		ranked[i] = s.p
	}
	return ranked
}

// mix64 is the splitmix64 finaliser.
func mix64(x uint64) uint64 {
	x ^= x >> 30
	x *= 0xbf58476d1ce4e5b9
	x ^= x >> 27
	x *= 0x94d049bb133111eb
	x ^= x >> 31
	return x
}

// ringWeight is a peer's share: its cache size in GiB, at least 1.
func ringWeight(p *coordinator.PeerInfo) float64 {
	gb := float64(p.UsedSpace+p.AvailableSpace) / (1 << 30)
	if gb < 1 {
		return 1
	}
	return gb
}

// ringOrder orders read candidates for key: owners the coordinator lists
// as holders first, then the other holders, then owners not yet holding the
// chunk, then everyone else. It also returns those not-yet-holding owners,
// for nudgeOwners.
//
// The first version put every owner first. In a cold cluster owners have
// not filled yet, so each read paid failed probes before reaching a real
// holder: on the 14-node Azure A/B the cold fan-out went from 2.2 s median
// without the ring to 3.1 s with it, half the bytes falling through to blob
// storage. Owners now jump the queue only once they hold the chunk.
func ringOrder(preferred, rest []*coordinator.PeerInfo, key string, r int) ([]*coordinator.PeerInfo, []*coordinator.PeerInfo, []*coordinator.PeerInfo) {
	if r <= 0 {
		return preferred, rest, nil
	}
	all := make([]*coordinator.PeerInfo, 0, len(preferred)+len(rest))
	all = append(all, preferred...)
	all = append(all, rest...)
	owners := ringOwners(all, key)
	if len(owners) > r {
		owners = owners[:r]
	}
	isOwner := make(map[string]bool, len(owners))
	for _, o := range owners {
		isOwner[o.ID] = true
	}
	isHolder := make(map[string]bool, len(preferred))
	for _, p := range preferred {
		isHolder[p.ID] = true
	}
	var heldOwners, unheldOwners, holders, others []*coordinator.PeerInfo
	for _, o := range owners {
		if isHolder[o.ID] {
			heldOwners = append(heldOwners, o)
		} else {
			unheldOwners = append(unheldOwners, o)
		}
	}
	for _, p := range preferred {
		if !isOwner[p.ID] {
			holders = append(holders, p)
		}
	}
	for _, p := range rest {
		if !isOwner[p.ID] {
			others = append(others, p)
		}
	}
	first := append(heldOwners, holders...)
	second := append(append([]*coordinator.PeerInfo{}, unheldOwners...), others...)
	return first, second, unheldOwners
}

// nudgeOwners asks each owner that does not hold key yet for one byte of it,
// in the background, once per key per node: the owner's miss queues the
// chunk for its deferred fill (NoteOwnedMiss), so later readers find it on
// its owner. Never on the read's critical path.
func (ps *PeerStorage) nudgeOwners(key string, owners []*coordinator.PeerInfo) {
	if len(owners) == 0 || !ps.rawTransport || ps.httpClient == nil {
		return
	}
	if _, isChunk := parentFilePathFromChunkPath(key); !isChunk {
		return
	}
	ps.nudgeMu.Lock()
	if ps.nudged == nil || len(ps.nudged) > 100000 {
		ps.nudged = make(map[string]struct{})
	}
	if _, done := ps.nudged[key]; done {
		ps.nudgeMu.Unlock()
		return
	}
	ps.nudged[key] = struct{}{}
	ps.nudgeMu.Unlock()
	for _, o := range owners {
		if o == nil || o.Address == "" || o.ID == ps.localPeerID {
			continue
		}
		addr := o.Address
		ps.ringNudges.Add(1)
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_, _ = ps.readRangeFromPeerRaw(ctx, addr, key, 0, 1)
		}()
	}
}

// SetRingReplicas turns ring placement on (r owners per chunk) or off (0).
func (cm *DefaultCacheManager) SetRingReplicas(r int) {
	if ps, ok := cm.peerStorage.(*PeerStorage); ok {
		ps.SetRingReplicas(r)
	}
}

// RingStats is exported to /metrics.
type RingStats struct {
	Replicas    int
	Members     int
	FillsQueued int64
	Nudges      int64
}

// RingStats reports ring settings and owner fills.
func (cm *DefaultCacheManager) RingStats() RingStats {
	ps, ok := cm.peerStorage.(*PeerStorage)
	if !ok {
		return RingStats{}
	}
	return RingStats{Replicas: ps.ringReplicas(), Members: len(ps.ringMembers()), FillsQueued: cm.ringFillsQueued.Load(), Nudges: ps.ringNudges.Load()}
}

// NoteOwnedMiss is called when a peer asked this node for a chunk it does
// not hold. If this node is one of the chunk's ring owners it queues the
// chunk for the deferred background fill (range_plan.go), so the next reader
// finds it here. A non-owner does nothing: it is not where readers look.
func (cm *DefaultCacheManager) NoteOwnedMiss(chunkPath string) {
	ps, ok := cm.peerStorage.(*PeerStorage)
	if !ok || ps.ringReplicas() <= 0 || cm.config.LocalPeerID == "" {
		return
	}
	parent, isChunk := parentFilePathFromChunkPath(chunkPath)
	if !isChunk {
		return
	}
	idx, ok := chunkIndexFromPath(chunkPath)
	if !ok {
		return
	}
	peers := ps.ringMembers()
	owners := ringOwners(peers, chunkPath)
	if len(owners) > ps.ringReplicas() {
		owners = owners[:ps.ringReplicas()]
	}
	for _, o := range owners {
		if o.ID == cm.config.LocalPeerID {
			cm.ringFillsQueued.Add(1)
			cm.deferBlobFill(parent, chunkPath, idx)
			return
		}
	}
}

// ringMembers is the cached active peer list plus this node (getPeers
// excludes self), so every node ranks the same membership.
func (ps *PeerStorage) ringMembers() []*coordinator.PeerInfo {
	ps.metaMu.RLock()
	peers := make([]*coordinator.PeerInfo, 0, len(ps.peersCache)+1)
	for _, p := range ps.peersCache {
		if p != nil && p.Status == "active" {
			peers = append(peers, p)
		}
	}
	self := ps.self
	ps.metaMu.RUnlock()
	if self != nil {
		peers = append(peers, self)
	}
	return peers
}

// ownersFirst moves owners to the front of peers, keeping the rest's order.
func ownersFirst(peers, owners []*coordinator.PeerInfo) []*coordinator.PeerInfo {
	isOwner := make(map[string]bool, len(owners))
	for _, o := range owners {
		isOwner[o.ID] = true
	}
	out := append([]*coordinator.PeerInfo{}, owners...)
	for _, p := range peers {
		if !isOwner[p.ID] {
			out = append(out, p)
		}
	}
	return out
}
