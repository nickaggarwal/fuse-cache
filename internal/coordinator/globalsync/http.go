package globalsync

import (
	"encoding/json"
	"net/http"
	"time"

	"fuse-client/internal/globalcp"
)

// RegisterRoutes mounts the coordinator's read-only global-sync surface.
//
// Read-only, like the control plane's own operator surface: publication is
// driven by the location table, not by an HTTP poke, so a write endpoint
// here would be a second way to change global state that bypasses the
// allowlist the CP granted.
//
// A nil agent still mounts the routes and reports disabled, rather than
// 404ing. An operator checking whether global sync is on gets an answer
// either way; a 404 is indistinguishable from an old binary.
func RegisterRoutes(mux *http.ServeMux, a *Agent) {
	mux.HandleFunc("/api/global/status", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		if a == nil {
			writeJSON(w, map[string]any{"enabled": false})
			return
		}
		st := a.Status()
		writeJSON(w, map[string]any{
			"enabled": true,
			"status":  st,
			// Surfaced explicitly because it is the question an operator
			// actually has during an incident: a stale mirror still serves,
			// so "how old is what I'm serving" is the number that matters,
			// not whether a socket is up.
			"mirror_age_seconds": ageSeconds(st.Mirror.LastSyncAt),
		})
	})

	mux.HandleFunc("/api/global/mirror", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		if a == nil {
			writeJSON(w, map[string]any{"enabled": false, "locations": []any{}, "count": 0})
			return
		}
		prefix := r.URL.Query().Get("prefix")
		rows := a.Mirror().Snapshot()
		out := make([]mirrorRow, 0, len(rows))
		for _, l := range rows {
			if prefix != "" && !globalcp.MatchesPrefix(l.GetFilePath(), prefix) {
				continue
			}
			out = append(out, mirrorRow{
				ClusterID:     l.GetClusterId(),
				FilePath:      l.GetFilePath(),
				FileSize:      l.GetFileSize(),
				IsChunked:     l.GetIsChunked(),
				ChunkSize:     l.GetChunkSize(),
				Provider:      l.GetObject().GetProvider(),
				Bucket:        l.GetObject().GetBucket(),
				KeyPrefix:     l.GetObject().GetKeyPrefix(),
				PublishedUnix: l.GetPublishedUnix(),
			})
		}
		st := a.Mirror().Status()
		writeJSON(w, map[string]any{
			"enabled": true,
			// Ready distinguishes "synced and empty" from "never synced".
			// Without it an empty list reads as authoritative when it is
			// not, which is exactly the wrong thing to act on.
			"ready":     st.Ready,
			"count":     len(out),
			"locations": out,
		})
	})
}

type mirrorRow struct {
	ClusterID     string `json:"cluster_id"`
	FilePath      string `json:"file_path"`
	FileSize      int64  `json:"file_size"`
	IsChunked     bool   `json:"is_chunked"`
	ChunkSize     int64  `json:"chunk_size,omitempty"`
	Provider      string `json:"provider"`
	Bucket        string `json:"bucket"`
	KeyPrefix     string `json:"key_prefix,omitempty"`
	PublishedUnix int64  `json:"published_unix"`
}

func ageSeconds(t time.Time) float64 {
	if t.IsZero() {
		return -1
	}
	return time.Since(t).Seconds()
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}
