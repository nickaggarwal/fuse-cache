package session

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// CachePolicy controls per-session cache behavior.
type CachePolicy struct {
	CacheMode    string // "readonly", "writeback", "writethrough"
	Warmup       string // "none", "metadata", "full"
	Pinned       bool   // prevent eviction while session active
	SourcePolicy string // "peer-first", "cloud-first", "hybrid"
}

// Session represents an active CSI volume mount session.
type Session struct {
	VolumeID  string
	RootPath  string // subtree root within FUSE mount
	HostPath  string // resolved host path for bind mount
	ReadOnly  bool
	Policy    CachePolicy
	RefCount  int32
	CreatedAt time.Time
}

// Manager tracks active CSI mount sessions on this node.
type Manager struct {
	mu       sync.Mutex
	sessions map[string]*Session // keyed by VolumeID
	fuseRoot string              // e.g. "/host/mnt/fuse"
	logger   *log.Logger
}

// NewManager creates a session manager. fuseRoot is the host path where the
// FUSE filesystem is mounted (e.g. "/host/mnt/fuse").
func NewManager(fuseRoot string) *Manager {
	return &Manager{
		sessions: make(map[string]*Session),
		fuseRoot: fuseRoot,
		logger:   log.New(log.Writer(), "[SESSION] ", log.LstdFlags),
	}
}

// Create creates a new session or increments the refcount of an existing one
// with the same volumeID. Returns the session with the resolved host path.
func (m *Manager) Create(_ context.Context, volumeID, rootPath string, readOnly bool, policy CachePolicy) (*Session, error) {
	if volumeID == "" {
		return nil, fmt.Errorf("volume_id is required")
	}
	rootPath = filepath.Clean("/" + rootPath)

	m.mu.Lock()
	defer m.mu.Unlock()

	// Idempotent: if session already exists for this volume, bump refcount.
	if existing, ok := m.sessions[volumeID]; ok {
		existing.RefCount++
		m.logger.Printf("Session %s refcount incremented to %d", volumeID, existing.RefCount)
		cp := *existing
		return &cp, nil
	}

	hostPath := filepath.Join(m.fuseRoot, rootPath)

	// Verify the host path base directory exists (the FUSE mount must be up).
	if _, err := os.Stat(m.fuseRoot); err != nil {
		return nil, fmt.Errorf("fuse root %s is not accessible: %w", m.fuseRoot, err)
	}

	sess := &Session{
		VolumeID:  volumeID,
		RootPath:  rootPath,
		HostPath:  hostPath,
		ReadOnly:  readOnly,
		Policy:    policy,
		RefCount:  1,
		CreatedAt: time.Now(),
	}
	m.sessions[volumeID] = sess
	m.logger.Printf("Session created: volume=%s root=%s host=%s readonly=%t pinned=%t",
		volumeID, rootPath, hostPath, readOnly, policy.Pinned)
	// Return a copy, like Get/List, so callers never share the map-backed
	// struct whose RefCount mutates under the manager mutex.
	cp := *sess
	return &cp, nil
}

// Delete removes a session. If refcount > 1, it decrements instead.
// Returns true if the session was fully removed (refcount reached 0).
func (m *Manager) Delete(_ context.Context, volumeID string) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	sess, ok := m.sessions[volumeID]
	if !ok {
		// Idempotent: already deleted.
		return true, nil
	}

	sess.RefCount--
	if sess.RefCount > 0 {
		m.logger.Printf("Session %s refcount decremented to %d", volumeID, sess.RefCount)
		return false, nil
	}

	delete(m.sessions, volumeID)
	m.logger.Printf("Session deleted: volume=%s", volumeID)
	return true, nil
}

// Get returns a session by volume ID, or nil if not found.
func (m *Manager) Get(_ context.Context, volumeID string) *Session {
	m.mu.Lock()
	defer m.mu.Unlock()
	sess, ok := m.sessions[volumeID]
	if !ok {
		return nil
	}
	// Return a copy to avoid data races.
	cp := *sess
	return &cp
}

// List returns all active sessions.
func (m *Manager) List(_ context.Context) []*Session {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]*Session, 0, len(m.sessions))
	for _, s := range m.sessions {
		cp := *s
		out = append(out, &cp)
	}
	return out
}

// PinnedPrefixes returns all root path prefixes that are currently pinned
// by at least one active session. Cache eviction should skip these.
func (m *Manager) PinnedPrefixes() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	var prefixes []string
	for _, s := range m.sessions {
		if s.Policy.Pinned {
			prefixes = append(prefixes, s.RootPath)
		}
	}
	return prefixes
}
