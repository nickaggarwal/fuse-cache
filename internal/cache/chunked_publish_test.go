package cache

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"fuse-client/internal/coordinator"
)

// failWriteCloud fails writes for one key and behaves normally otherwise, so
// a test can leave a chunked upload genuinely incomplete rather than faking
// the bookkeeping that would follow from one.
type failWriteCloud struct {
	*mockStorage
	failKey string
}

func (f *failWriteCloud) Write(ctx context.Context, path string, data []byte) error {
	if path == f.failKey {
		return errors.New("upload failed")
	}
	return f.mockStorage.Write(ctx, path, data)
}

// newChunkedPublishManager stages a chunked file: chunk bytes on NVMe and
// entry-table rows for the parent and every chunk, which is the state
// persistChunkedFileToCloud runs against after a real chunked write.
func newChunkedPublishManager(t *testing.T, cloud TierStorage, path string, numChunks int64) (*DefaultCacheManager, *coordinator.CoordinatorService) {
	t.Helper()
	cm := newTestCacheManager()
	if cloud != nil {
		cm.cloudStorage = cloud
	}
	coord := coordinator.NewCoordinatorService()
	cm.config.Coordinator = coord
	cm.config.LocalPeerID = "peer-1"

	const chunkSize = int64(4)
	ctx := context.Background()
	for i := int64(0); i < numChunks; i++ {
		cp := fmt.Sprintf("%s_chunk_%d", path, i)
		if err := cm.nvmeStorage.Write(ctx, cp, []byte("aaaa")); err != nil {
			t.Fatalf("nvme write: %v", err)
		}
		cm.entries.put(&CacheEntry{FilePath: cp, StoragePath: cp, Size: chunkSize, Tier: TierNVMe})
	}
	cm.entries.put(&CacheEntry{
		FilePath: path, StoragePath: path, Size: chunkSize * numChunks,
		Tier: TierNVMe, IsChunked: true, NumChunks: numChunks, ChunkSize: chunkSize,
	})
	return cm, coord
}

func cloudRowForParent(t *testing.T, coord *coordinator.CoordinatorService, path string) *coordinator.FileLocation {
	t.Helper()
	locs, err := coord.GetFileLocation(context.Background(), path)
	if err != nil {
		t.Fatalf("GetFileLocation: %v", err)
	}
	for _, l := range locs {
		if l.StorageTier == "cloud" {
			return l
		}
	}
	return nil
}

// TestChunkedFilePublishesAParentCloudRow closes a gap that is invisible
// locally and fatal across clusters.
//
// persistToCloud publishes one cloud row per "<file>_chunk_<n>" object and
// nothing ever names the file itself as being in cloud storage. Every local
// reader of a chunked file resolves chunks individually, so nothing notices.
// The global publisher filters on the cloud tier of the whole path, so
// without a parent row no chunked file ever crosses a cluster boundary --
// silently, and for exactly the large files the feature exists for.
func TestChunkedFilePublishesAParentCloudRow(t *testing.T) {
	const path = "/models/big.bin"
	cm, coord := newChunkedPublishManager(t, nil, path, 3)

	cm.persistChunkedFileToCloud(context.Background(), path, 3)

	row := cloudRowForParent(t, coord, path)
	if row == nil {
		t.Fatal("no cloud row for the parent path; no chunked file would be globally discoverable")
	}
	if !row.IsChunked {
		t.Error("parent row does not say the file is chunked; a reader would fetch the whole path as one object")
	}
	if row.ChunkSize != 4 {
		t.Errorf("ChunkSize = %d, want 4: a reader addressing chunks at the wrong stride reads the wrong bytes", row.ChunkSize)
	}
	if row.FileSize != 12 {
		t.Errorf("FileSize = %d, want 12", row.FileSize)
	}
}

// TestPartiallyUploadedChunkedFileIsNotPublished is the other half, and the
// one worth more: a row advertising a file whose bytes are not all in the
// bucket sends another cluster to read an object that does not exist, and
// the resulting miss looks like a stale index rather than an upload still
// in progress.
func TestPartiallyUploadedChunkedFileIsNotPublished(t *testing.T) {
	const path = "/models/big.bin"
	cloud := &failWriteCloud{mockStorage: newMockStorage(), failKey: path + "_chunk_1"}
	cm, coord := newChunkedPublishManager(t, cloud, path, 3)

	cm.persistChunkedFileToCloud(context.Background(), path, 3)

	if row := cloudRowForParent(t, coord, path); row != nil {
		t.Fatalf("published %s as cloud-resident while chunk 1 failed to upload: %+v", path, row)
	}
	// The chunks that DID land still publish their own rows -- the partial
	// upload is not rolled back, only the whole-file claim is withheld.
	if row := cloudRowForParent(t, coord, path+"_chunk_0"); row == nil {
		t.Error("a chunk that uploaded successfully lost its own cloud row")
	}
}
