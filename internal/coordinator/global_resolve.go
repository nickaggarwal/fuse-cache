package coordinator

import "context"

// GlobalLocation is where a path lives in another cluster's object storage.
//
// Deliberately not a FileLocation. That type is peer-scoped -- PeerID is an
// unqualified pod name and StoragePath is a local filesystem path -- and
// neither means anything across a cluster boundary. A reader of this type
// gets the only two things it can act on: which bucket, and under which key
// prefix.
type GlobalLocation struct {
	ClusterID string `json:"cluster_id"`
	FilePath  string `json:"file_path"`
	FileSize  int64  `json:"file_size"`
	IsChunked bool   `json:"is_chunked"`
	ChunkSize int64  `json:"chunk_size,omitempty"`
	Provider  string `json:"provider"`
	Bucket    string `json:"bucket"`
	KeyPrefix string `json:"key_prefix"`
	// Region (S3) or storage account (Azure). Advisory: a reader that
	// already has a client for this bucket ignores it.
	Region      string `json:"region,omitempty"`
	Endpoint    string `json:"endpoint,omitempty"`
	PublishedAt int64  `json:"published_unix,omitempty"`
}

// GlobalResolution is the answer to "does another cluster hold this".
//
// Ready is the load-bearing field. An empty Locations with Ready false means
// "I have never synced", which is a different claim from "no other cluster
// has it" -- and conflating them is how a cluster that restarted during a
// control-plane outage would cache a negative answer and stop looking. Same
// unknown-is-not-zero rule as the residency endpoint and the llm-d scorer.
type GlobalResolution struct {
	Locations []*GlobalLocation `json:"locations"`
	Ready     bool              `json:"ready"`
}

// GlobalResolver is the optional coordinator capability for cross-cluster
// discovery, in the same shape as FetchLeaser and NetworkStatusUpdater: a
// client type-asserts for it and falls back to local-only behavior when the
// assertion fails, so nothing has to know whether the feature is compiled
// in, configured, or merely unreachable.
//
// It is advisory in the strongest sense. Implementations serve from a local
// mirror and must never dial the global control plane to answer, because
// this is called on a cache miss, which a reader is waiting on.
type GlobalResolver interface {
	ResolveGlobal(ctx context.Context, filePath string) (*GlobalResolution, error)
}

// GlobalMirror is the subset of *globalsync.Mirror the coordinator needs.
// Held as an interface for the same reason GlobalPublisher is: nil is the
// normal configuration and the coordinator keeps no dependency on the global
// path.
type GlobalMirror interface {
	Resolve(filePath string) (locations []*GlobalLocation, ready bool)
}

// SetGlobalMirror installs the read side of the global index. Call before
// serving.
func (cs *CoordinatorService) SetGlobalMirror(m GlobalMirror) { cs.globalMirror = m }

// ResolveGlobal implements GlobalResolver on the coordinator service.
//
// A coordinator with no mirror returns empty-and-not-ready rather than an
// error. "Not configured" and "configured but never synced" are the same
// thing to a caller -- in both cases the honest answer is "I don't know" --
// and an error here would push every caller into distinguishing a disabled
// feature from a broken one on a path where neither changes what it does.
func (cs *CoordinatorService) ResolveGlobal(_ context.Context, filePath string) (*GlobalResolution, error) {
	if cs.globalMirror == nil {
		return &GlobalResolution{}, nil
	}
	locs, ready := cs.globalMirror.Resolve(filePath)
	return &GlobalResolution{Locations: locs, Ready: ready}, nil
}

// Compile-time proof that every transport can answer a global resolve. The
// HTTP client is deliberately absent: it has no global endpoint, so a
// deployment on the HTTP fallback transport type-asserts to nothing and
// stays local-only rather than silently resolving against a stale answer.
var (
	_ GlobalResolver = (*CoordinatorService)(nil)
	_ GlobalResolver = (*GRPCCoordinatorClient)(nil)
)
