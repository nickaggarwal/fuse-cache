package globalcp

import (
	"encoding/json"
	"net/http"
	"time"

	"fuse-client/internal/pb"
)

// RegisterRoutes mounts the read-only operator surface.
//
// Read-only on purpose: every mutation arrives over the authenticated gRPC
// stream, where identity is a client certificate. An HTTP write endpoint
// would be a second, weaker way to change global state -- and the repo's
// existing HTTP surfaces have no auth middleware at all.
func RegisterRoutes(mux *http.ServeMux, svc *Service) {
	mux.HandleFunc("/api/global/clusters", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		clusters, err := svc.Store().ListClusters(r.Context())
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		out := make([]clusterView, 0, len(clusters))
		for _, c := range clusters {
			out = append(out, clusterView{
				ClusterID:         c.ID,
				Provider:          c.Object.GetProvider(),
				Bucket:            c.Object.GetBucket(),
				KeyPrefix:         c.Object.GetKeyPrefix(),
				PublishPrefixes:   c.PublishPrefixes,
				SubscribePrefixes: c.SubscribePrefixes,
				RegisteredAt:      c.RegisteredAt,
				LastSeen:          c.LastSeen,
			})
		}
		writeJSON(w, map[string]any{
			"clusters":            out,
			"connected_streams":   svc.SubscriberCount(),
			"registered_clusters": len(out),
		})
	})

	mux.HandleFunc("/api/global/index", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		prefix := r.URL.Query().Get("prefix")
		if prefix == "" {
			// Explicit, because "" means "nothing" everywhere else in this
			// package and an operator listing the index wants everything.
			prefix = "/"
		}
		locs, err := svc.Store().ListLocations(r.Context(), []string{prefix})
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		writeJSON(w, map[string]any{"prefix": prefix, "count": len(locs), "locations": locationViews(locs)})
	})

	mux.HandleFunc("/api/health", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, map[string]any{"status": "healthy", "streams": svc.SubscriberCount()})
	})
}

type clusterView struct {
	ClusterID         string    `json:"cluster_id"`
	Provider          string    `json:"provider,omitempty"`
	Bucket            string    `json:"bucket,omitempty"`
	KeyPrefix         string    `json:"key_prefix,omitempty"`
	PublishPrefixes   []string  `json:"publish_prefixes,omitempty"`
	SubscribePrefixes []string  `json:"subscribe_prefixes,omitempty"`
	RegisteredAt      time.Time `json:"registered_at"`
	LastSeen          time.Time `json:"last_seen"`
}

type locationView struct {
	ClusterID string `json:"cluster_id"`
	FilePath  string `json:"file_path"`
	FileSize  int64  `json:"file_size"`
	IsChunked bool   `json:"is_chunked,omitempty"`
	ChunkSize int64  `json:"chunk_size,omitempty"`
	Provider  string `json:"provider,omitempty"`
	Bucket    string `json:"bucket,omitempty"`
	KeyPrefix string `json:"key_prefix,omitempty"`
	Published int64  `json:"published_unix"`
}

func locationViews(locs []*pb.GlobalLocation) []locationView {
	out := make([]locationView, 0, len(locs))
	for _, l := range locs {
		out = append(out, locationView{
			ClusterID: l.GetClusterId(),
			FilePath:  l.GetFilePath(),
			FileSize:  l.GetFileSize(),
			IsChunked: l.GetIsChunked(),
			ChunkSize: l.GetChunkSize(),
			Provider:  l.GetObject().GetProvider(),
			Bucket:    l.GetObject().GetBucket(),
			KeyPrefix: l.GetObject().GetKeyPrefix(),
			Published: l.GetPublishedUnix(),
		})
	}
	return out
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}
