package session

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// CachePolicy controls per-session cache behavior.
type CachePolicy struct {
	CacheMode       string // "readonly", "writeback", "writethrough"
	Warmup          string // "none", "metadata", "full"
	WarmupBandwidth string // "", "background", "max"
	Pinned          bool   // prevent eviction while session active
	SourcePolicy    string // "peer-first", "cloud-first", "hybrid"
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
	mu           sync.Mutex
	sessions     map[string]*Session // keyed by VolumeID
	fuseRoot     string              // e.g. "/host/mnt/fuse"
	logger       *log.Logger
	warmupHook   func(WarmupRequest)
	teardownHook func(volumeID string)
	// statePath, when set, is where the session table is persisted (see
	// EnablePersistence). Empty: in-memory only.
	statePath string
}

// WarmupRequest describes a newly created session whose cache policy asks for
// warmup ("metadata" or "full"). Source carries the session's SourcePolicy
// and Bandwidth its WarmupBandwidth so the warm strategy follows the policy.
type WarmupRequest struct {
	VolumeID  string
	RootPath  string
	Mode      string
	Source    string
	Bandwidth string
}

// SetWarmupHook registers fn to run (in its own goroutine) when a session is
// first created with Policy.Warmup other than ""/"none". Refcount bumps on an
// existing session do not re-trigger it — the subtree is already warming or
// warm. Set before the agent server starts accepting sessions.
func (m *Manager) SetWarmupHook(fn func(WarmupRequest)) {
	m.mu.Lock()
	m.warmupHook = fn
	m.mu.Unlock()
}

// SetTeardownHook registers fn to run when a session's refcount reaches 0 and
// the session is removed — the counterpart to SetWarmupHook. A declarative
// warmup is an obligation held for as long as the volume is mounted, so
// whatever took the obligation on needs to know when it is discharged.
// Refcount decrements that leave the session alive do not fire it, and a
// repeat Delete of an already-removed volume does not re-fire it.
//
// Called synchronously with the manager lock held, so it must be cheap and
// must not call back into the Manager. Synchronous on purpose: it runs while
// the session is provably gone from the map, which is what makes "hook fired"
// equivalent to "obligation discharged" for the caller's bookkeeping.
func (m *Manager) SetTeardownHook(fn func(volumeID string)) {
	m.mu.Lock()
	m.teardownHook = fn
	m.mu.Unlock()
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
		m.saveLocked()
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
	m.saveLocked()
	m.logger.Printf("Session created: volume=%s root=%s host=%s readonly=%t pinned=%t warmup=%s",
		volumeID, rootPath, hostPath, readOnly, policy.Pinned, policy.Warmup)

	if m.warmupHook != nil && policy.Warmup != "" && policy.Warmup != "none" {
		hook := m.warmupHook
		req := WarmupRequest{
			VolumeID: volumeID, RootPath: rootPath, Mode: policy.Warmup,
			Source: policy.SourcePolicy, Bandwidth: policy.WarmupBandwidth,
		}
		go hook(req)
	}
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
		m.saveLocked()
		m.logger.Printf("Session %s refcount decremented to %d", volumeID, sess.RefCount)
		return false, nil
	}

	delete(m.sessions, volumeID)
	m.saveLocked()
	m.logger.Printf("Session deleted: volume=%s", volumeID)
	if m.teardownHook != nil {
		m.teardownHook(volumeID)
	}
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

// Session persistence.
//
// The CSI driver short-circuits NodePublishVolume when the target is already
// a mountpoint, so after a daemon restart a surviving bind mount never
// recreates its session. With an in-memory table that silently dropped
// everything a session carries: its pin (eviction could reclaim a served
// model) and its declarative warmup (nothing re-asserted residency). And the
// eventual NodeUnpublishVolume found no session, so nothing was released.
//
// With persistence the table is written atomically on every change and
// reloaded at startup, refcounts included: the unpublish that follows a
// restart decrements the restored session and fires the teardown hook at zero
// exactly as it would have without the restart. A mount torn down while the
// daemon was down is not lost either: unpublish fails while the agent is
// unreachable and kubelet retries it until the daemon is back.
//
// The table is only valid for the boot that wrote it: a node reboot takes
// every bind mount with it and kubelet publishes the volumes again, so
// restoring the old refcounts would double-count each one and the pin would
// outlive the last unpublish. The state records the kernel boot ID and is
// discarded when it no longer matches.

// bootID returns the kernel's per-boot ID (host-wide, not namespaced, so a
// container sees the node's). Empty if unavailable. A variable for tests.
var bootID = func() string {
	b, err := os.ReadFile("/proc/sys/kernel/random/boot_id")
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(b))
}

type persistedState struct {
	BootID   string             `json:"boot_id"`
	Sessions []persistedSession `json:"sessions"`
}

// persistedSession is the on-disk form of one session.
type persistedSession struct {
	VolumeID  string      `json:"volume_id"`
	RootPath  string      `json:"root_path"`
	HostPath  string      `json:"host_path"`
	ReadOnly  bool        `json:"read_only"`
	Policy    CachePolicy `json:"policy"`
	RefCount  int32       `json:"ref_count"`
	CreatedAt time.Time   `json:"created_at"`
}

// EnablePersistence loads the session table from path (if present) and keeps
// it written there from now on. Call it before the agent server accepts
// sessions and before anything reads PinnedPrefixes, so pins are in force
// from the first eviction pass. Restored sessions do not fire the warmup hook
// here (it is usually not set yet); call ReplayWarmups once it is.
// Returns how many sessions were restored. A missing file is not an error; an
// unreadable one is logged and treated as empty (the pre-persistence state).
func (m *Manager) EnablePersistence(path string) (int, error) {
	if path == "" {
		return 0, nil
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return 0, fmt.Errorf("session state dir: %w", err)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.statePath = path
	raw, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return 0, nil
	}
	if err != nil {
		m.logger.Printf("Session state %s unreadable, starting empty: %v", path, err)
		return 0, nil
	}
	var st persistedState
	if err := json.Unmarshal(raw, &st); err != nil {
		m.logger.Printf("Session state %s corrupt, starting empty: %v", path, err)
		return 0, nil
	}
	if cur := bootID(); st.BootID != cur {
		m.logger.Printf("Session state %s is from another boot (%q, now %q), discarding: kubelet republishes after a reboot",
			path, st.BootID, cur)
		m.saveLocked()
		return 0, nil
	}
	n := 0
	for _, r := range st.Sessions {
		if r.VolumeID == "" || r.RefCount <= 0 {
			continue
		}
		if _, ok := m.sessions[r.VolumeID]; ok {
			continue
		}
		m.sessions[r.VolumeID] = &Session{
			VolumeID: r.VolumeID, RootPath: r.RootPath, HostPath: r.HostPath,
			ReadOnly: r.ReadOnly, Policy: r.Policy, RefCount: r.RefCount,
			CreatedAt: r.CreatedAt,
		}
		n++
		m.logger.Printf("Session restored: volume=%s root=%s refcount=%d pinned=%t warmup=%s",
			r.VolumeID, r.RootPath, r.RefCount, r.Policy.Pinned, r.Policy.Warmup)
	}
	return n, nil
}

// ReplayWarmups fires the warmup hook for every live session whose policy
// asks for warmup: the declarations restored by EnablePersistence. Safe to
// call when nothing was restored.
func (m *Manager) ReplayWarmups() {
	m.mu.Lock()
	hook := m.warmupHook
	var reqs []WarmupRequest
	for _, s := range m.sessions {
		if s.Policy.Warmup == "" || s.Policy.Warmup == "none" {
			continue
		}
		reqs = append(reqs, WarmupRequest{
			VolumeID: s.VolumeID, RootPath: s.RootPath, Mode: s.Policy.Warmup,
			Source: s.Policy.SourcePolicy, Bandwidth: s.Policy.WarmupBandwidth,
		})
	}
	m.mu.Unlock()
	if hook == nil {
		return
	}
	for _, r := range reqs {
		go hook(r)
	}
}

// saveLocked writes the table atomically (temp file + rename). Advisory: a
// failed write is logged, and the in-memory table stays authoritative for
// this process. Caller holds m.mu.
func (m *Manager) saveLocked() {
	if m.statePath == "" {
		return
	}
	rows := make([]persistedSession, 0, len(m.sessions))
	for _, s := range m.sessions {
		rows = append(rows, persistedSession{
			VolumeID: s.VolumeID, RootPath: s.RootPath, HostPath: s.HostPath,
			ReadOnly: s.ReadOnly, Policy: s.Policy, RefCount: s.RefCount,
			CreatedAt: s.CreatedAt,
		})
	}
	raw, err := json.Marshal(persistedState{BootID: bootID(), Sessions: rows})
	if err != nil {
		m.logger.Printf("Session state encode: %v", err)
		return
	}
	tmp := m.statePath + ".tmp"
	if err := os.WriteFile(tmp, raw, 0o644); err != nil {
		m.logger.Printf("Session state write %s: %v", tmp, err)
		return
	}
	if err := os.Rename(tmp, m.statePath); err != nil {
		m.logger.Printf("Session state rename %s: %v", m.statePath, err)
	}
}
