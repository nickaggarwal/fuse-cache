package globalcpe2e

import (
	"bytes"
	"context"
	"strings"
	"testing"
)

// newFederation builds the standard two-cluster world: one control plane, one
// shared bucket, cluster A publishing /models/ under prefix "a", cluster B
// subscribing to it under prefix "b".
//
// The prefixes differ because that is the only configuration the design
// supports for two clusters on one bucket, and a suite that used the same
// prefix for both would pass every assertion below for the wrong reason: B
// would find A's objects at B's own key.
func newFederation(t *testing.T, bGlobalRead bool) (*s3Server, *controlPlane, *cluster, *cluster) {
	t.Helper()
	store := startS3Server(t)
	cp := startControlPlane(t)
	a := newCluster(t, cp, store, clusterConfig{
		id:      "cluster-a",
		prefix:  "a",
		publish: []string{"/models/"},
	})
	b := newCluster(t, cp, store, clusterConfig{
		id:         "cluster-b",
		prefix:     "b",
		publish:    []string{"/models/"},
		subscribe:  []string{"/models/"},
		globalRead: bGlobalRead,
	})
	// Both agents must be registered before any test writes. Registration
	// is what delivers the accepted publish allowlist, and an empty
	// allowlist suppresses every publish -- so a negative assertion made
	// before this point would hold for a reason that has nothing to do
	// with the gating it claims to test.
	a.waitReady(t)
	b.waitReady(t)
	return store, cp, a, b
}

// TestCrossClusterReadIsServedFromThePublishersPrefix is the claim the whole
// design rests on, and the reason this package exists.
//
// It asserts three things that are easy to confuse for one:
//
//  1. The row crosses. B's mirror learns about a path it never wrote.
//  2. An ORDINARY read finds it. Not getFromGlobal -- cm.Get, the same call
//     the FUSE layer makes, which reaches the global branch only after NVMe,
//     peer, and B's own cloud scope have all missed.
//  3. The bytes come from A's prefix. The recorded request log is what makes
//     this checkable: a read that returned the right bytes from B's own key
//     would mean the prefixes are not isolating anything, and every
//     assertion about cluster scoping would be vacuous.
func TestCrossClusterReadIsServedFromThePublishersPrefix(t *testing.T) {
	store, _, a, b := newFederation(t, true)

	want := bytes.Repeat([]byte("checkpoint"), 512)
	a.writeAndPublish(t, "/models/llama/weights.bin", want)

	waitFor(t, "cluster-b's mirror to learn /models/llama/weights.bin", func() bool {
		return b.sees("/models/llama/weights.bin")
	})

	// Only A's key exists. If this were two keys, the read below would prove
	// nothing about where the bytes came from.
	if keys := store.keys(); len(keys) != 1 || keys[0] != "a/models/llama/weights.bin" {
		t.Fatalf("store should hold exactly A's object, got %v", keys)
	}

	store.resetRequests()
	got, ok := b.read("/models/llama/weights.bin")
	if !ok {
		t.Fatal("cluster-b could not read a path cluster-a published")
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("cluster-b read %d bytes, want %d", len(got), len(want))
	}

	tried := store.requestedKeys()
	// B must have looked under its OWN prefix first and missed. Without
	// this, a passing read could mean the key mapper is ignoring the prefix
	// entirely.
	if !containsKey(tried, "b/models/llama/weights.bin") {
		t.Fatalf("cluster-b never tried its own prefix; keys tried: %v", tried)
	}
	if !containsKey(tried, "a/models/llama/weights.bin") {
		t.Fatalf("cluster-b never read A's key; keys tried: %v", tried)
	}

	stats := b.cm.GlobalReadSnapshot()
	if stats.Hits != 1 {
		t.Fatalf("expected exactly one global hit, got %+v", stats)
	}
}

// TestLocalAutonomyWhenTheControlPlaneDies is requirement 3, and the one most
// worth an explicit test: a partitioned cluster must behave exactly like
// today's single-cluster deployment.
//
// "Behaves the same" is asserted as three separate properties, because they
// fail independently:
//
//   - local write then local read still works (the data plane never consults
//     the global CP on a path it holds);
//   - a cross-cluster path already in the mirror STILL reads, because the
//     mirror serves its last-good contents and the bytes live in object
//     storage, which the partition did not take away;
//   - only NEW discovery is lost.
//
// The third is what separates degradation from failure, and it is the only
// one a reader of the code cannot assume.
func TestLocalAutonomyWhenTheControlPlaneDies(t *testing.T) {
	_, cp, a, b := newFederation(t, true)

	before := []byte("published while the control plane was up")
	a.writeAndPublish(t, "/models/before.bin", before)
	waitFor(t, "cluster-b to learn /models/before.bin", func() bool {
		return b.sees("/models/before.bin")
	})

	cp.stop()

	// 1. Local write, local read. The global CP is not on this path at all,
	// and if it ever becomes so, this is where it shows.
	local := []byte("written in B with no control plane")
	if err := b.cm.Put(context.Background(), newEntry("/local/scratch.bin", local)); err != nil {
		t.Fatalf("local write with the control plane down: %v", err)
	}
	got, err := b.cm.Get(context.Background(), "/local/scratch.bin")
	if err != nil || !bytes.Equal(got.Data, local) {
		t.Fatalf("local read with the control plane down: err=%v", err)
	}

	// 2. Last-good mirror contents still resolve, and the object read still
	// succeeds: the partition is between the cluster and the CP, not between
	// the cluster and the bucket.
	crossed, ok := b.read("/models/before.bin")
	if !ok || !bytes.Equal(crossed, before) {
		t.Fatal("a path already in the mirror stopped resolving when the control plane died")
	}

	// 3. New discovery is what degrades. A writes a second path; B must not
	// learn it, and must say so as a MISS rather than an error.
	a.writeAndPublish(t, "/models/after.bin", []byte("published into the void"))
	if _, ok := b.read("/models/after.bin"); ok {
		t.Fatal("cluster-b discovered a path published after the control plane died")
	}
	if b.sees("/models/after.bin") {
		t.Fatal("mirror grew while disconnected")
	}
}

// TestNVMeOnlyPathIsNeverPublished: the global data path is object storage
// only, so advertising a path whose only copy is on a node's local disk would
// promise bytes no other cluster can fetch. The allowlist is not enough --
// the tier has to be cloud too.
func TestNVMeOnlyPathIsNeverPublished(t *testing.T) {
	_, _, a, b := newFederation(t, true)

	a.writeNVMeOnly(t, "/models/nvme-only.bin", 4096)

	// A later, legitimately publishable path acts as the barrier. Publishes
	// go through one queue drained in arrival order onto one stream, so the
	// sentinel's arrival proves the earlier path would have arrived too if
	// it had ever been queued. Sleeping for "long enough" instead would make
	// this test a clock measurement.
	a.writeAndPublish(t, "/models/sentinel.bin", []byte("sentinel"))
	waitFor(t, "the sentinel to cross", func() bool { return b.sees("/models/sentinel.bin") })

	if b.sees("/models/nvme-only.bin") {
		t.Fatal("a path held only on local NVMe was published to the global index")
	}
}

// TestPathOutsideTheAllowlistIsNeverPublished is requirement 2: everything is
// local-only unless an operator named its prefix.
//
// The near-miss matters more than the obvious case. "/models-staging/" shares
// a textual prefix with the allowlisted "/models/" and must not match, or the
// allowlist silently leaks a sibling namespace.
func TestPathOutsideTheAllowlistIsNeverPublished(t *testing.T) {
	_, _, a, b := newFederation(t, true)

	a.writeAndPublish(t, "/datasets/private.bin", []byte("not allowlisted"))
	a.writeAndPublish(t, "/models-staging/near-miss.bin", []byte("textual prefix, different namespace"))
	a.writeAndPublish(t, "/models/allowed.bin", []byte("allowlisted"))

	waitFor(t, "the allowlisted path to cross", func() bool { return b.sees("/models/allowed.bin") })

	for _, p := range []string{"/datasets/private.bin", "/models-staging/near-miss.bin"} {
		if b.sees(p) {
			t.Fatalf("%s was published despite being outside the allowlist", p)
		}
		if _, ok := b.read(p); ok {
			t.Fatalf("%s was readable cross-cluster despite being outside the allowlist", p)
		}
	}
}

// TestStaleIndexRowReadsAsAMissNotAFailure. The global index is eventually
// consistent by design, so a row that outlives its object is a normal state,
// not a fault.
//
// The distinction is not cosmetic: a miss feeds the tier tracker's miss
// counter and a failure feeds its health EWMA, and the codebase has already
// been bitten once by that confusion (tier_miss.go). A stale global row that
// scored as a cloud failure would make the cloud tier look sick for reasons
// that have nothing to do with the cloud.
func TestStaleIndexRowReadsAsAMissNotAFailure(t *testing.T) {
	store, _, a, b := newFederation(t, true)

	payload := []byte("about to be deleted")
	a.writeAndPublish(t, "/models/ghost.bin", payload)
	waitFor(t, "cluster-b to learn /models/ghost.bin", func() bool { return b.sees("/models/ghost.bin") })

	// Delete the object WITHOUT withdrawing the row: exactly what a bucket
	// lifecycle rule or an out-of-band delete produces.
	store.remove("a/models/ghost.bin")

	if _, ok := b.read("/models/ghost.bin"); ok {
		t.Fatal("read succeeded against an object that no longer exists")
	}

	stats := b.cm.GlobalReadSnapshot()
	if stats.RowsFound == 0 {
		t.Fatalf("expected the stale row to be found and attempted, got %+v", stats)
	}
	if stats.Misses == 0 {
		t.Fatalf("a stale row must be counted as a miss, got %+v", stats)
	}
	if stats.Hits != 0 {
		t.Fatalf("no bytes exist, so there can be no hit: %+v", stats)
	}

	// The counters are the point: RowsFound above Hits is the operator's
	// only signal that the global index is advertising objects this cluster
	// cannot read. Hit rate alone cannot say it.
	if stats.RowsFound <= stats.Hits {
		t.Fatalf("stale rows must be distinguishable from hits: %+v", stats)
	}
}

// TestGlobalReadDisabledIsInert. Stage 3 shipped behind a default-off flag,
// and "default off" has to mean the branch is not reached, not merely that it
// usually returns nothing.
//
// Asserting on the request log rather than on the read's return value is
// deliberate: a cluster with global read off and a cluster with global read on
// but no matching row both fail the read. Only the keys actually requested
// tell them apart.
func TestGlobalReadDisabledIsInert(t *testing.T) {
	store, _, a, b := newFederation(t, false)

	a.writeAndPublish(t, "/models/visible.bin", []byte("published and mirrored"))
	waitFor(t, "cluster-b to learn /models/visible.bin", func() bool {
		return b.sees("/models/visible.bin")
	})

	store.resetRequests()
	if _, ok := b.read("/models/visible.bin"); ok {
		t.Fatal("global read is disabled but the cross-cluster read succeeded")
	}

	for _, k := range store.requestedKeys() {
		if strings.HasPrefix(k, "a/") {
			t.Fatalf("global read is disabled but cluster-b requested %s", k)
		}
	}
	if stats := b.cm.GlobalReadSnapshot(); stats.Lookups != 0 {
		t.Fatalf("global read is disabled but the resolver was consulted: %+v", stats)
	}
}

// TestSharedBucketKeyLayoutIsOnePrefixPerCluster pins the layout the design
// documents as the supported case: one bucket, one prefix per cluster.
//
// This is the assertion the per-cluster fakes in internal/cache cannot make.
// There, the reader's expected key comes from a fixture that encodes the
// reader's own assumption, so a writer and a reader that disagreed would
// still agree with the fixture. Here both clusters write into one real store
// and the keys are whatever the AWS SDK actually sent.
func TestSharedBucketKeyLayoutIsOnePrefixPerCluster(t *testing.T) {
	store, _, a, b := newFederation(t, true)

	a.writeAndPublish(t, "/models/shared-name.bin", []byte("written by A"))
	b.writeAndPublish(t, "/models/shared-name.bin", []byte("written by B"))

	keys := store.keys()
	if len(keys) != 2 {
		t.Fatalf("one logical path written in two clusters must produce two objects, got %v", keys)
	}
	for _, want := range []string{"a/models/shared-name.bin", "b/models/shared-name.bin"} {
		if !containsKey(keys, want) {
			t.Fatalf("missing %s in %v", want, keys)
		}
	}

	// The same path in two clusters is two distinct objects, and the local
	// one wins. Cross-cluster discovery is a fallback for paths this cluster
	// does not have, never a source of a different version of one it does --
	// SEMANTICS.md's last-publish-wins applies to the index, not to bytes.
	got, ok := b.read("/models/shared-name.bin")
	if !ok || !bytes.Equal(got, []byte("written by B")) {
		t.Fatalf("cluster-b must read its own copy, got ok=%v %q", ok, got)
	}
}

func containsKey(keys []string, want string) bool {
	for _, k := range keys {
		if k == want {
			return true
		}
	}
	return false
}
