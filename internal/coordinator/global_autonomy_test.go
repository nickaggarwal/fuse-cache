package coordinator

import (
	"context"
	"errors"
	"sync"
	"testing"
)

// recordingPublisher stands in for a sync agent and can be made to behave
// like one whose control plane is gone.
type recordingPublisher struct {
	mu       sync.Mutex
	persists []string
	deletes  []string
}

func (p *recordingPublisher) OnCloudPersist(filePath, tier string, size int64, chunked bool, chunkSize int64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.persists = append(p.persists, filePath+"|"+tier)
}

func (p *recordingPublisher) OnDelete(filePath string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.deletes = append(p.deletes, filePath)
}

func (p *recordingPublisher) snapshot() ([]string, []string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]string(nil), p.persists...), append([]string(nil), p.deletes...)
}

// TestLocationTableWorksWithNoPublisher pins the default: a coordinator with
// no global publisher behaves exactly as it always has.
func TestLocationTableWorksWithNoPublisher(t *testing.T) {
	cs := NewCoordinatorService()
	ctx := context.Background()

	loc := &FileLocation{FilePath: "/models/a.bin", PeerID: "p1", StorageTier: "cloud", FileSize: 10}
	if err := cs.UpdateFileLocation(ctx, loc); err != nil {
		t.Fatalf("update: %v", err)
	}
	got, err := cs.store.GetFileLocations(ctx, "/models/a.bin")
	if err != nil || len(got) != 1 {
		t.Fatalf("locations: %v %v", got, err)
	}
	if err := cs.DeleteFileLocation(ctx, "/models/a.bin", "p1", true); err != nil {
		t.Fatalf("delete: %v", err)
	}
}

func TestPublisherSeesCloudPersistAndWholePathDelete(t *testing.T) {
	cs := NewCoordinatorService()
	pub := &recordingPublisher{}
	cs.SetGlobalPublisher(pub)
	ctx := context.Background()

	for _, tier := range []string{"nvme", "peer", "cloud"} {
		if err := cs.UpdateFileLocation(ctx, &FileLocation{
			FilePath: "/models/a.bin", PeerID: "p-" + tier, StorageTier: tier, FileSize: 10,
		}); err != nil {
			t.Fatalf("update %s: %v", tier, err)
		}
	}
	persists, _ := pub.snapshot()
	// The coordinator hands every tier to the publisher and lets the agent
	// filter: the tier rule belongs in one place, not duplicated here.
	if len(persists) != 3 {
		t.Fatalf("want all 3 tiers offered to the publisher, got %v", persists)
	}

	// A single peer dropping its replica must NOT withdraw the global row:
	// the object in cloud storage is still there, and that is the only
	// thing another cluster can read.
	if err := cs.DeleteFileLocation(ctx, "/models/a.bin", "p-nvme", false); err != nil {
		t.Fatalf("per-peer delete: %v", err)
	}
	if _, deletes := pub.snapshot(); len(deletes) != 0 {
		t.Fatalf("per-peer delete withdrew a still-fetchable global row: %v", deletes)
	}

	if err := cs.DeleteFileLocation(ctx, "/models/a.bin", "", true); err != nil {
		t.Fatalf("whole-path delete: %v", err)
	}
	if _, deletes := pub.snapshot(); len(deletes) != 1 || deletes[0] != "/models/a.bin" {
		t.Fatalf("whole-path delete did not withdraw: %v", deletes)
	}
}

// TestStoreFailureSuppressesPublish: the global index must never advertise a
// path this cluster's own coordinator failed to record.
func TestStoreFailureSuppressesPublish(t *testing.T) {
	cs := NewCoordinatorServiceWithStore(&failingStore{Store: NewInMemoryStore()})
	pub := &recordingPublisher{}
	cs.SetGlobalPublisher(pub)

	err := cs.UpdateFileLocation(context.Background(), &FileLocation{
		FilePath: "/models/a.bin", PeerID: "p1", StorageTier: "cloud",
	})
	if err == nil {
		t.Fatal("expected the store error to surface")
	}
	if persists, _ := pub.snapshot(); len(persists) != 0 {
		t.Fatalf("published a row the store rejected: %v", persists)
	}
}

type failingStore struct {
	Store
}

func (f *failingStore) PutFileLocation(ctx context.Context, loc *FileLocation) error {
	return errors.New("store unavailable")
}
