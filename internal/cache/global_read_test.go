package cache

import (
	"context"
	"errors"
	"log"
	"os"
	"sync"
	"testing"
	"time"

	"fuse-client/internal/coordinator"

	"github.com/aws/aws-sdk-go/aws/awserr"
)

// TestRemoteObjectKeyMatchesWhatTheOtherClusterWrote is the test that keeps
// the feature honest. Everything else can be right and a one-character
// difference here makes every cross-cluster read miss, with the index
// looking perfectly healthy.
func TestRemoteObjectKeyMatchesWhatTheOtherClusterWrote(t *testing.T) {
	cases := []struct {
		name    string
		loc     *coordinator.GlobalLocation
		path    string
		wantKey string
		wantOK  bool
	}{
		{
			name:    "s3 trims the leading slash before scoping, as the writer did",
			loc:     &coordinator.GlobalLocation{FilePath: "/models/a.bin", Bucket: "shared", KeyPrefix: "cluster-a", Provider: "s3"},
			path:    "/models/a.bin",
			wantKey: "cluster-a/models/a.bin",
			wantOK:  true,
		},
		{
			name:    "azure keeps the leading slash, as the writer did",
			loc:     &coordinator.GlobalLocation{FilePath: "/models/a.bin", Bucket: "shared", KeyPrefix: "cluster-a", Provider: "azure"},
			path:    "/models/a.bin",
			wantKey: "cluster-a/models/a.bin",
			wantOK:  true,
		},
		{
			name:    "a chunk resolves through its parent's row",
			loc:     &coordinator.GlobalLocation{FilePath: "/models/big.bin", Bucket: "shared", KeyPrefix: "cluster-a", Provider: "s3", IsChunked: true},
			path:    "/models/big.bin_chunk_7",
			wantKey: "cluster-a/models/big.bin_chunk_7",
			wantOK:  true,
		},
		{
			name:   "a chunk of a DIFFERENT file does not resolve",
			loc:    &coordinator.GlobalLocation{FilePath: "/models/other.bin", Bucket: "shared", KeyPrefix: "cluster-a", Provider: "s3"},
			path:   "/models/big.bin_chunk_7",
			wantOK: false,
		},
		{
			name:    "an unprefixed publisher yields the bare key",
			loc:     &coordinator.GlobalLocation{FilePath: "/models/a.bin", Bucket: "shared", KeyPrefix: "", Provider: "s3"},
			path:    "/models/a.bin",
			wantKey: "models/a.bin",
			wantOK:  true,
		},
		{
			name:   "a row with no bucket names no readable bytes",
			loc:    &coordinator.GlobalLocation{FilePath: "/models/a.bin", KeyPrefix: "cluster-a", Provider: "s3"},
			path:   "/models/a.bin",
			wantOK: false,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			key, ok := remoteObjectKey(tc.loc, tc.path)
			if ok != tc.wantOK {
				t.Fatalf("ok = %v, want %v (key %q)", ok, tc.wantOK, key)
			}
			if ok && key != tc.wantKey {
				t.Fatalf("key = %q, want %q", key, tc.wantKey)
			}
		})
	}
}

// TestRemoteKeyRoundTripsThroughTheWritersOwnMapper proves the derivation
// above is not merely self-consistent: it reproduces the key the publishing
// cluster's backend would actually have written, by running the same
// KeyMapper the writer uses.
func TestRemoteKeyRoundTripsThroughTheWritersOwnMapper(t *testing.T) {
	const prefix = "cluster-a"
	writer := NewKeyMapper(prefix)

	for _, path := range []string{"/models/a.bin", "/models/sub/dir/b.bin", "/models/big.bin_chunk_12"} {
		// What an S3 backend in the publishing cluster writes.
		wroteS3 := writer.Key(normalizeS3Key(path))
		// What an Azure backend in the publishing cluster writes.
		wroteAzure := writer.Key(path)

		gotS3, ok := remoteObjectKey(&coordinator.GlobalLocation{
			FilePath: parentOrSelf(path), Bucket: "shared", KeyPrefix: prefix, Provider: "s3",
		}, path)
		if !ok || gotS3 != wroteS3 {
			t.Errorf("s3 %s: reader derives %q, writer wrote %q", path, gotS3, wroteS3)
		}
		gotAz, ok := remoteObjectKey(&coordinator.GlobalLocation{
			FilePath: parentOrSelf(path), Bucket: "shared", KeyPrefix: prefix, Provider: "azure",
		}, path)
		if !ok || gotAz != wroteAzure {
			t.Errorf("azure %s: reader derives %q, writer wrote %q", path, gotAz, wroteAzure)
		}
	}
}

func parentOrSelf(path string) string {
	if parent, ok := parentFilePathFromChunkPath(path); ok {
		return parent
	}
	return path
}

// fakeGlobalCoordinator is a Coordinator that also answers global resolves.
type fakeGlobalCoordinator struct {
	coordinator.Coordinator
	mu       sync.Mutex
	res      *coordinator.GlobalResolution
	err      error
	calls    int
	blockFor time.Duration
}

func (f *fakeGlobalCoordinator) ResolveGlobal(ctx context.Context, path string) (*coordinator.GlobalResolution, error) {
	f.mu.Lock()
	f.calls++
	block, res, err := f.blockFor, f.res, f.err
	f.mu.Unlock()
	if block > 0 {
		select {
		case <-time.After(block):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return res, err
}

func (f *fakeGlobalCoordinator) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

// keyRecordingCloud is a cloud tier that records every raw key read.
type keyRecordingCloud struct {
	TierStorage
	bucket string
	mu     sync.Mutex
	keys   []string
	data   map[string][]byte
}

func (k *keyRecordingCloud) ReadKey(ctx context.Context, key string) ([]byte, error) {
	k.mu.Lock()
	k.keys = append(k.keys, key)
	d, ok := k.data[key]
	k.mu.Unlock()
	if !ok {
		return nil, awserr.NewRequestFailure(awserr.New("NoSuchKey", "not found", nil), 404, "req")
	}
	return d, nil
}

func (k *keyRecordingCloud) BucketName() string { return k.bucket }

func (k *keyRecordingCloud) readKeys() []string {
	k.mu.Lock()
	defer k.mu.Unlock()
	return append([]string(nil), k.keys...)
}

func newGlobalTestManager(t *testing.T, coord coordinator.Coordinator, cloud TierStorage, enabled bool) *DefaultCacheManager {
	t.Helper()
	return &DefaultCacheManager{
		config: &CacheConfig{
			Coordinator:       coord,
			GlobalReadEnabled: enabled,
		},
		cloudStorage: cloud,
		logger:       log.New(os.Stderr, "[TEST] ", 0),
	}
}

// TestGlobalReadIsOffByDefault: the whole stage must be inert until an
// operator opts in. Not "cheap when off" -- inert. A disabled feature that
// still makes one coordinator call per cloud miss would change the latency
// of every existing deployment's miss path.
func TestGlobalReadIsOffByDefault(t *testing.T) {
	coord := &fakeGlobalCoordinator{res: &coordinator.GlobalResolution{
		Ready:     true,
		Locations: []*coordinator.GlobalLocation{{FilePath: "/models/a.bin", Bucket: "shared", KeyPrefix: "b", Provider: "s3"}},
	}}
	cloud := &keyRecordingCloud{bucket: "shared", data: map[string][]byte{"b/models/a.bin": []byte("hi")}}
	cm := newGlobalTestManager(t, coord, cloud, false)

	if _, ok := cm.getFromGlobal(context.Background(), "/models/a.bin"); ok {
		t.Fatal("global read served bytes while disabled")
	}
	if coord.callCount() != 0 {
		t.Fatalf("disabled global read still called the coordinator %d times", coord.callCount())
	}
	if len(cloud.readKeys()) != 0 {
		t.Fatalf("disabled global read still touched the cloud tier: %v", cloud.readKeys())
	}
}

// TestGlobalReadServesFromTheOtherClustersPrefix is the headline path.
func TestGlobalReadServesFromTheOtherClustersPrefix(t *testing.T) {
	coord := &fakeGlobalCoordinator{res: &coordinator.GlobalResolution{
		Ready: true,
		Locations: []*coordinator.GlobalLocation{
			{ClusterID: "cluster-a", FilePath: "/models/a.bin", Bucket: "shared", KeyPrefix: "cluster-a", Provider: "s3", FileSize: 5},
		},
	}}
	cloud := &keyRecordingCloud{bucket: "shared", data: map[string][]byte{
		"cluster-a/models/a.bin": []byte("hello"),
	}}
	cm := newGlobalTestManager(t, coord, cloud, true)

	data, ok := cm.getFromGlobal(context.Background(), "/models/a.bin")
	if !ok {
		t.Fatalf("global read missed; keys tried: %v", cloud.readKeys())
	}
	if string(data) != "hello" {
		t.Fatalf("data = %q", data)
	}
	// The key is the assertion, not just the bytes: reading the right bytes
	// under a key we derived differently from the writer would pass today
	// and break the moment a prefix is configured.
	if keys := cloud.readKeys(); len(keys) != 1 || keys[0] != "cluster-a/models/a.bin" {
		t.Fatalf("read keys = %v, want exactly [cluster-a/models/a.bin]", keys)
	}
	if got := cm.GlobalReadSnapshot(); got.Hits != 1 || got.Lookups != 1 {
		t.Fatalf("stats = %+v, want 1 lookup and 1 hit", got)
	}
}

// TestGlobalReadSkipsAnUnreachableBucket: the honest limitation. A separate
// bucket needs credentials this stage does not distribute, so the read must
// be declined rather than attempted and failed slowly.
func TestGlobalReadSkipsAnUnreachableBucket(t *testing.T) {
	coord := &fakeGlobalCoordinator{res: &coordinator.GlobalResolution{
		Ready: true,
		Locations: []*coordinator.GlobalLocation{
			{ClusterID: "cluster-a", FilePath: "/models/a.bin", Bucket: "someone-elses-bucket", KeyPrefix: "cluster-a", Provider: "s3"},
		},
	}}
	cloud := &keyRecordingCloud{bucket: "shared", data: map[string][]byte{}}
	cm := newGlobalTestManager(t, coord, cloud, true)

	if _, ok := cm.getFromGlobal(context.Background(), "/models/a.bin"); ok {
		t.Fatal("served from a bucket this cluster has no credentials for")
	}
	if keys := cloud.readKeys(); len(keys) != 0 {
		t.Fatalf("attempted a read against an unreachable bucket: %v", keys)
	}
	if got := cm.GlobalReadSnapshot(); got.Unreachable != 1 {
		t.Fatalf("stats = %+v, want unreachable=1", got)
	}
}

// TestGlobalReadDegradesWhenTheCoordinatorCannotAnswer: advisory to the end.
// A resolve failure leaves the read exactly where it already was.
func TestGlobalReadDegradesWhenTheCoordinatorCannotAnswer(t *testing.T) {
	coord := &fakeGlobalCoordinator{err: errors.New("coordinator unreachable")}
	cloud := &keyRecordingCloud{bucket: "shared", data: map[string][]byte{}}
	cm := newGlobalTestManager(t, coord, cloud, true)

	if _, ok := cm.getFromGlobal(context.Background(), "/models/a.bin"); ok {
		t.Fatal("reported a hit after a failed resolve")
	}
	if got := cm.GlobalReadSnapshot(); got.ResolveErrors != 1 || got.Hits != 0 {
		t.Fatalf("stats = %+v, want 1 resolve error and 0 hits", got)
	}
}

// TestGlobalResolveIsBounded: the resolve runs after a cloud miss, on a path
// a reader is waiting on. A coordinator that hangs must cost the bounded
// timeout, not the caller's whole deadline.
func TestGlobalResolveIsBounded(t *testing.T) {
	coord := &fakeGlobalCoordinator{blockFor: time.Hour}
	cloud := &keyRecordingCloud{bucket: "shared", data: map[string][]byte{}}
	cm := newGlobalTestManager(t, coord, cloud, true)

	start := time.Now()
	if _, ok := cm.getFromGlobal(context.Background(), "/models/a.bin"); ok {
		t.Fatal("hit from a coordinator that never answered")
	}
	if elapsed := time.Since(start); elapsed > 2*globalReadTimeout {
		t.Fatalf("resolve took %v against a hung coordinator; must be bounded by %v", elapsed, globalReadTimeout)
	}
}

// TestStaleGlobalRowIsAMissNotAFailure: the index says the object is there
// and it is not. That is stale routing, the same class as a peer miss, and
// it must not be reported as a hit or logged as a failure.
func TestStaleGlobalRowIsAMissNotAFailure(t *testing.T) {
	coord := &fakeGlobalCoordinator{res: &coordinator.GlobalResolution{
		Ready: true,
		Locations: []*coordinator.GlobalLocation{
			{ClusterID: "cluster-a", FilePath: "/models/gone.bin", Bucket: "shared", KeyPrefix: "cluster-a", Provider: "s3"},
		},
	}}
	cloud := &keyRecordingCloud{bucket: "shared", data: map[string][]byte{}}
	cm := newGlobalTestManager(t, coord, cloud, true)

	if _, ok := cm.getFromGlobal(context.Background(), "/models/gone.bin"); ok {
		t.Fatal("hit on an object the remote bucket does not have")
	}
	got := cm.GlobalReadSnapshot()
	if got.RowsFound != 1 {
		t.Fatalf("stats = %+v, want the row to have been found", got)
	}
	if got.Misses != 1 {
		t.Fatalf("stats = %+v, want the stale row counted as a miss", got)
	}
}

// TestGlobalReadTriesEveryClusterThatPublished: with two publishers, a stale
// row from one must not shadow a live copy in the other.
func TestGlobalReadTriesEveryClusterThatPublished(t *testing.T) {
	coord := &fakeGlobalCoordinator{res: &coordinator.GlobalResolution{
		Ready: true,
		Locations: []*coordinator.GlobalLocation{
			{ClusterID: "cluster-a", FilePath: "/models/a.bin", Bucket: "shared", KeyPrefix: "cluster-a", Provider: "s3"},
			{ClusterID: "cluster-b", FilePath: "/models/a.bin", Bucket: "shared", KeyPrefix: "cluster-b", Provider: "s3"},
		},
	}}
	cloud := &keyRecordingCloud{bucket: "shared", data: map[string][]byte{
		"cluster-b/models/a.bin": []byte("from b"),
	}}
	cm := newGlobalTestManager(t, coord, cloud, true)

	data, ok := cm.getFromGlobal(context.Background(), "/models/a.bin")
	if !ok || string(data) != "from b" {
		t.Fatalf("data=%q ok=%v; keys tried %v", data, ok, cloud.readKeys())
	}
}

// TestGlobalReadNeedsARawKeyCapableCloudTier: a backend that cannot read by
// absolute key must decline rather than fall back to its own scoping, which
// would look under THIS cluster's prefix and quietly re-do the read that
// already missed.
func TestGlobalReadNeedsARawKeyCapableCloudTier(t *testing.T) {
	coord := &fakeGlobalCoordinator{res: &coordinator.GlobalResolution{
		Ready:     true,
		Locations: []*coordinator.GlobalLocation{{FilePath: "/models/a.bin", Bucket: "shared", KeyPrefix: "b", Provider: "s3"}},
	}}
	cm := newGlobalTestManager(t, coord, &plainTierStorage{}, true)

	if _, ok := cm.getFromGlobal(context.Background(), "/models/a.bin"); ok {
		t.Fatal("served a global read through a backend with no raw-key support")
	}
	if coord.callCount() != 0 {
		t.Fatal("resolved against the control plane before checking the backend could use the answer")
	}
}

// plainTierStorage implements TierStorage and nothing else.
type plainTierStorage struct{}

func (plainTierStorage) Read(context.Context, string) ([]byte, error) { return nil, errors.New("no") }
func (plainTierStorage) Write(context.Context, string, []byte) error  { return errors.New("no") }
func (plainTierStorage) Delete(context.Context, string) error         { return errors.New("no") }
func (plainTierStorage) Exists(context.Context, string) bool          { return false }
func (plainTierStorage) Size(context.Context, string) (int64, error)  { return 0, errors.New("no") }

// TestRealBackendsSatisfyRawKeyReader pins the capability on the three types
// that must have it. A backend silently losing the method would disable
// cross-cluster reads for that provider with no build error.
func TestRealBackendsSatisfyRawKeyReader(t *testing.T) {
	var (
		_ rawKeyReader = (*CloudStorage)(nil)
		_ rawKeyReader = (*AzureStorage)(nil)
		_ rawKeyReader = (*GCPStorage)(nil)
	)
}
