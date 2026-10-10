package globalsync

import (
	"fmt"
	"testing"
	"time"

	"fuse-client/internal/pb"
)

func TestPublishableRequiresCloudTier(t *testing.T) {
	allow := []string{"/models"}
	// The bytes must actually be in object storage: the global data path is
	// object-storage-only, so an NVMe-only path would advertise something no
	// other cluster can structurally reach.
	for _, tier := range []string{"nvme", "peer", ""} {
		if publishable("/models/a.bin", tier, allow) {
			t.Fatalf("tier %q was published", tier)
		}
	}
	if !publishable("/models/a.bin", "cloud", allow) {
		t.Fatal("cloud-tier allowlisted path was not publishable")
	}
}

func TestPublishableHonorsAllowlist(t *testing.T) {
	allow := []string{"/models"}
	if publishable("/secrets/key.pem", "cloud", allow) {
		t.Fatal("published a path outside the allowlist")
	}
	// Prefix matching is at path boundaries; /models must not swallow
	// /models-v2.
	if publishable("/models-v2/a.bin", "cloud", allow) {
		t.Fatal("/models matched /models-v2")
	}
	// An empty allowlist publishes nothing -- a cluster that configured no
	// prefixes is a pure consumer, not an everything-publisher.
	if publishable("/models/a.bin", "cloud", nil) {
		t.Fatal("empty allowlist published")
	}
}

func TestPublishCoalescesWithinWindow(t *testing.T) {
	q := newPublishQueue()
	t0 := time.Now()

	if !q.Publish(loc("a", "/models/x.bin", 1, 0), t0) {
		t.Fatal("first publish was suppressed")
	}
	if q.Publish(loc("a", "/models/x.bin", 2, 0), t0.Add(time.Second)) != true {
		t.Fatal("in-window publish of a still-queued path should refresh it")
	}
	up, _ := q.Drain()
	if len(up) != 1 {
		t.Fatalf("want 1 coalesced row, got %d", len(up))
	}
	// The queued payload must be the FRESHER one: a refresh that kept the
	// stale size would advertise the wrong length for the whole window.
	if up[0].GetFileSize() != 2 {
		t.Fatalf("want refreshed size 2, got %d", up[0].GetFileSize())
	}

	// Still inside the window and nothing queued now: suppressed outright.
	if q.Publish(loc("a", "/models/x.bin", 3, 0), t0.Add(2*time.Second)) {
		t.Fatal("in-window publish was not suppressed after the drain")
	}
	// Past the window: publishes again.
	if !q.Publish(loc("a", "/models/x.bin", 4, 0), t0.Add(publishCoalesceWindow+time.Second)) {
		t.Fatal("post-window publish was suppressed")
	}
}

// TestWithdrawIsNeverCoalescedAway: a suppressed upsert costs staleness, a
// suppressed withdraw leaves a row pointing at an object that is gone.
func TestWithdrawIsNeverCoalescedAway(t *testing.T) {
	q := newPublishQueue()
	t0 := time.Now()

	q.Publish(loc("a", "/models/x.bin", 1, 0), t0)
	q.Drain()
	q.Withdraw("/models/x.bin", t0.Add(time.Second))

	up, wd := q.Drain()
	if len(up) != 0 || len(wd) != 1 || wd[0] != "/models/x.bin" {
		t.Fatalf("withdraw lost to the coalescing window: up=%v wd=%v", up, wd)
	}
}

// TestWithdrawResetsTheCoalescingClock: after a withdraw, the next write of
// that path must publish immediately rather than be suppressed as a
// duplicate of the row that was just deleted.
func TestWithdrawResetsTheCoalescingClock(t *testing.T) {
	q := newPublishQueue()
	t0 := time.Now()

	q.Publish(loc("a", "/models/x.bin", 1, 0), t0)
	q.Drain()
	q.Withdraw("/models/x.bin", t0.Add(time.Second))
	q.Drain()

	if !q.Publish(loc("a", "/models/x.bin", 5, 0), t0.Add(2*time.Second)) {
		t.Fatal("re-publish after withdraw was suppressed; the path would stay missing")
	}
}

func TestWithdrawSupersedesAQueuedPublish(t *testing.T) {
	q := newPublishQueue()
	t0 := time.Now()

	q.Publish(loc("a", "/models/x.bin", 1, 0), t0)
	q.Withdraw("/models/x.bin", t0)

	up, wd := q.Drain()
	if len(up) != 0 {
		t.Fatalf("stale publish survived a later withdraw: %v", up)
	}
	if len(wd) != 1 {
		t.Fatalf("want the withdraw, got %v", wd)
	}
}

func TestQueueDropsOldestWhenFull(t *testing.T) {
	q := newPublishQueue()
	t0 := time.Now()

	for i := 0; i < publishQueueDepth+10; i++ {
		q.Publish(loc("a", fmt.Sprintf("/models/f%d.bin", i), 1, 0), t0)
	}
	if got := q.Len(); got != publishQueueDepth {
		t.Fatalf("queue depth %d, want %d", got, publishQueueDepth)
	}
	if q.Dropped() != 10 {
		t.Fatalf("dropped %d, want 10", q.Dropped())
	}

	// The survivors must be the NEWEST intents: the global index converges
	// on the last write, so dropping the newest would leave it permanently
	// disagreeing with this cluster.
	up, _ := q.Drain()
	if up[0].GetFilePath() != "/models/f10.bin" {
		t.Fatalf("dropped the wrong end: head is %s", up[0].GetFilePath())
	}
}

func TestDrainIsBatchCapped(t *testing.T) {
	q := newPublishQueue()
	t0 := time.Now()
	for i := 0; i < publishBatchMax+5; i++ {
		q.Publish(loc("a", fmt.Sprintf("/models/f%d.bin", i), 1, 0), t0)
	}
	up, wd := q.Drain()
	if len(up)+len(wd) != publishBatchMax {
		t.Fatalf("drained %d, want %d", len(up)+len(wd), publishBatchMax)
	}
	if q.Len() != 5 {
		t.Fatalf("want 5 left over, got %d", q.Len())
	}
}

func TestRequeueGoesToTheFrontAndYieldsToNewerIntents(t *testing.T) {
	q := newPublishQueue()
	t0 := time.Now()

	failed := []*pb.GlobalLocation{
		loc("a", "/models/old.bin", 1, 0),
		loc("a", "/models/superseded.bin", 1, 0),
	}
	// A newer intent for one of the failed paths landed while the send was
	// in flight; resurrecting the older row would undo it.
	q.Publish(loc("a", "/models/superseded.bin", 99, 0), t0)
	q.Publish(loc("a", "/models/new.bin", 1, 0), t0)
	q.Requeue(failed, nil)

	up, _ := q.Drain()
	if up[0].GetFilePath() != "/models/old.bin" {
		t.Fatalf("requeued row did not go to the front: head is %s", up[0].GetFilePath())
	}
	for _, l := range up {
		if l.GetFilePath() == "/models/superseded.bin" && l.GetFileSize() != 99 {
			t.Fatalf("requeue clobbered a newer intent: size %d", l.GetFileSize())
		}
	}
}

func TestRequeueOfAWithdrawSurvives(t *testing.T) {
	q := newPublishQueue()
	q.Requeue(nil, []string{"/models/gone.bin"})
	_, wd := q.Drain()
	if len(wd) != 1 || wd[0] != "/models/gone.bin" {
		t.Fatalf("requeued withdraw lost: %v", wd)
	}
}
