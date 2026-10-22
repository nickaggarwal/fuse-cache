package cache

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"hash/fnv"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

// Artifact durability tracking (I1).
//
// Every local write is acked at NVMe speed and uploaded to the cloud tier in
// the background. Before this file existed, that upload was fire-and-forget:
// persistToCloud gave up after CloudRetryCount attempts and logged CRITICAL,
// nothing re-queued it, and a restart forgot it entirely (rehydration adopts
// on-disk files without re-uploading them). A file in that state stays
// NVMe-only indefinitely while looking healthy, and a consumer like
// pod-snapshotter had no way to ask "does this artifact survive node loss".
//
// The tracker gives every tracked write one of four states:
//
//	local      committed to NVMe, upload not started (or interrupted)
//	uploading  an upload attempt is running
//	persisted  every object, including a chunked file's parent, is in cloud
//	failed     the last attempt exhausted its retries; it will be retried
//
// Non-persisted records are journalled, one small JSON file per path under
// <NVMePath>/.fuse-durability/, so a process restart resumes the upload
// instead of dropping it. A persisted record's journal file is removed.
//
// The journal write is temp-file + atomic rename WITHOUT fsync. Every FUSE
// Flush already pays one fsync for the staged data; a second per commit would
// roughly double the flush cost for small-file writers (kvstash direct mode
// writes many). Rename-without-fsync survives a process crash or pod restart,
// which is the case this exists for. A kernel crash or power loss can lose a
// just-written record: the file is then adopted by rehydration as an
// unpersisted entry (still never evicted, evictionSafe probes cloud) but its
// upload does not auto-resume, and the receipt API reports it as "local" from
// its own probe -- never as persisted.
//
// fsync on the mount stays LOCAL (docs/SEMANTICS.md). The durability barrier
// is the explicit receipt API (DurabilityReceipt), so only consumers that
// need cloud durability pay cloud latency.

// durabilityDirName is the reserved NVMe subdirectory holding the journal.
// Rehydration skips it so journal files never become cache entries.
const durabilityDirName = ".fuse-durability"

// DurabilityState is the persist state of one path.
type DurabilityState string

const (
	DurabilityLocal     DurabilityState = "local"
	DurabilityUploading DurabilityState = "uploading"
	DurabilityPersisted DurabilityState = "persisted"
	DurabilityFailed    DurabilityState = "failed"
	// DurabilityUnknown means neither this node nor the coordinator knows the
	// path. It is never durable.
	DurabilityUnknown DurabilityState = "unknown"
)

const (
	durabilityRetryBase      = 30 * time.Second
	durabilityRetryMax       = 10 * time.Minute
	durabilityMaxPersisted   = 65536
	durabilityJournalStripes = 64
)

// errPersistAbandoned reports an upload that stopped because the path was
// deleted or rewritten. It is neither success nor failure: whoever deleted or
// rewrote the path owns its state now.
var errPersistAbandoned = errors.New("persist abandoned: path deleted or superseded")

// persistShape is the object layout an upload must produce.
type persistShape struct {
	Size      int64
	IsChunked bool
	ChunkSize int64
	NumChunks int64
}

// durabilityRecord is one path's persist state. Exported fields are the
// journal format.
type durabilityRecord struct {
	Path        string          `json:"path"`
	State       DurabilityState `json:"state"`
	Size        int64           `json:"size"`
	IsChunked   bool            `json:"is_chunked,omitempty"`
	ChunkSize   int64           `json:"chunk_size,omitempty"`
	NumChunks   int64           `json:"num_chunks,omitempty"`
	CommittedAt int64           `json:"committed_unix_nano"`
	Attempts    int             `json:"attempts,omitempty"`
	LastError   string          `json:"last_error,omitempty"`
	UpdatedAt   int64           `json:"updated_unix_nano"`

	gen       uint64
	inflight  bool
	nextRetry time.Time
}

func (r *durabilityRecord) shape() persistShape {
	return persistShape{Size: r.Size, IsChunked: r.IsChunked, ChunkSize: r.ChunkSize, NumChunks: r.NumChunks}
}

// durabilityTracker owns persist state. All methods tolerate a nil receiver
// (tests build DefaultCacheManager literals): a nil tracker tracks nothing,
// reports every generation current, and never journals.
type durabilityTracker struct {
	mu      sync.Mutex
	dir     string // "" = in-memory only
	nextGen uint64
	recs    map[string]*durabilityRecord
	logger  *log.Logger
	wake    chan struct{}
	// journalMu serializes journal writes per path stripe, so two commits of
	// the same path cannot land their files out of order.
	journalMu [durabilityJournalStripes]sync.Mutex
}

func newDurabilityTracker(dir string, logger *log.Logger) *durabilityTracker {
	if logger == nil {
		logger = log.New(os.Stderr, "[DURABILITY] ", log.LstdFlags)
	}
	t := &durabilityTracker{
		dir:    dir,
		recs:   make(map[string]*durabilityRecord),
		logger: logger,
		wake:   make(chan struct{}, 1),
	}
	if dir != "" {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			logger.Printf("Durability journal disabled: cannot create %s: %v", dir, err)
			t.dir = ""
		}
	}
	return t
}

// begin records a fresh local commit of path and returns its generation.
// The record is journalled before begin returns, so the write is resumable
// from the moment the caller acks it.
func (t *durabilityTracker) begin(path string, shape persistShape) uint64 {
	if t == nil {
		return 0
	}
	now := time.Now()
	t.mu.Lock()
	t.nextGen++
	rec := &durabilityRecord{
		Path:        path,
		State:       DurabilityLocal,
		Size:        shape.Size,
		IsChunked:   shape.IsChunked,
		ChunkSize:   shape.ChunkSize,
		NumChunks:   shape.NumChunks,
		CommittedAt: now.UnixNano(),
		UpdatedAt:   now.UnixNano(),
		gen:         t.nextGen,
		inflight:    true,
	}
	t.recs[path] = rec
	snap := *rec
	t.pruneLocked()
	t.mu.Unlock()
	t.journal(&snap)
	return snap.gen
}

// enqueue records a pending upload that no caller is running yet; the retry
// loop picks it up immediately.
func (t *durabilityTracker) enqueue(path string, shape persistShape) uint64 {
	if t == nil {
		return 0
	}
	gen := t.begin(path, shape)
	t.mu.Lock()
	if rec, ok := t.recs[path]; ok && rec.gen == gen {
		rec.inflight = false
		rec.nextRetry = time.Time{}
	}
	t.mu.Unlock()
	t.signal()
	return gen
}

// current reports whether gen is still path's newest generation.
func (t *durabilityTracker) current(path string, gen uint64) bool {
	if t == nil {
		return true
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	rec, ok := t.recs[path]
	return ok && rec.gen == gen
}

// markUploading flips a current generation to uploading. In memory only: on
// restart "uploading" and "local" both mean "resume".
func (t *durabilityTracker) markUploading(path string, gen uint64) {
	if t == nil {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if rec, ok := t.recs[path]; ok && rec.gen == gen {
		rec.State = DurabilityUploading
		rec.inflight = true
		rec.UpdatedAt = time.Now().UnixNano()
	}
}

// finish records the outcome of an upload of gen. A stale generation is
// ignored: a newer write owns the path's state.
func (t *durabilityTracker) finish(path string, gen uint64, err error) {
	if t == nil {
		return
	}
	now := time.Now()
	t.mu.Lock()
	rec, ok := t.recs[path]
	if !ok || rec.gen != gen {
		t.mu.Unlock()
		return
	}
	rec.inflight = false
	rec.UpdatedAt = now.UnixNano()
	var snap *durabilityRecord
	remove := false
	switch {
	case err == nil:
		rec.State = DurabilityPersisted
		rec.Attempts = 0
		rec.LastError = ""
		remove = true
	case errors.Is(err, errPersistAbandoned):
		// Deleted: forget handled it, or the write was superseded and the
		// newer generation already replaced this record.
		delete(t.recs, path)
		remove = true
	case isShutdownErr(err):
		// Interrupted, not failed: keep the journal row so a restart resumes.
		rec.State = DurabilityLocal
	default:
		rec.State = DurabilityFailed
		rec.Attempts++
		rec.LastError = err.Error()
		rec.nextRetry = now.Add(durabilityBackoff(rec.Attempts))
		c := *rec
		snap = &c
	}
	t.mu.Unlock()
	if remove {
		t.removeJournal(path)
	} else if snap != nil {
		t.journal(snap)
	}
}

// forget drops path's record and journal file (delete).
func (t *durabilityTracker) forget(path string) {
	if t == nil {
		return
	}
	t.mu.Lock()
	_, ok := t.recs[path]
	delete(t.recs, path)
	t.mu.Unlock()
	if ok || t.dir != "" {
		t.removeJournal(path)
	}
}

// get returns a copy of path's record.
func (t *durabilityTracker) get(path string) (durabilityRecord, bool) {
	if t == nil {
		return durabilityRecord{}, false
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	rec, ok := t.recs[path]
	if !ok {
		return durabilityRecord{}, false
	}
	return *rec, true
}

// list returns copies of every record under prefix ("" = all).
func (t *durabilityTracker) list(prefix string) []durabilityRecord {
	if t == nil {
		return nil
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	out := make([]durabilityRecord, 0, len(t.recs))
	for p, rec := range t.recs {
		if prefix != "" && !pathUnderPrefix(p, prefix) {
			continue
		}
		out = append(out, *rec)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Path < out[j].Path })
	return out
}

// kick makes a failed or pending record due now and wakes the retry loop.
func (t *durabilityTracker) kick(path string) {
	if t == nil {
		return
	}
	t.mu.Lock()
	if rec, ok := t.recs[path]; ok && !rec.inflight &&
		(rec.State == DurabilityFailed || rec.State == DurabilityLocal) {
		rec.nextRetry = time.Time{}
	}
	t.mu.Unlock()
	t.signal()
}

func (t *durabilityTracker) signal() {
	if t == nil {
		return
	}
	select {
	case t.wake <- struct{}{}:
	default:
	}
}

// dueItem is one upload the retry loop should run.
type dueItem struct {
	path string
	gen  uint64
}

// due claims up to limit records that need an upload now, marking them in
// flight so the caller owns them until finish.
func (t *durabilityTracker) due(now time.Time, limit int) []dueItem {
	if t == nil || limit <= 0 {
		return nil
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	var out []dueItem
	for p, rec := range t.recs {
		if rec.inflight || rec.nextRetry.After(now) {
			continue
		}
		if rec.State != DurabilityLocal && rec.State != DurabilityFailed {
			continue
		}
		rec.inflight = true
		out = append(out, dueItem{path: p, gen: rec.gen})
		if len(out) >= limit {
			break
		}
	}
	return out
}

// release returns a claimed item without an outcome (e.g. nothing to upload
// yet), leaving it due at the next retry.
func (t *durabilityTracker) release(path string, gen uint64) {
	if t == nil {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if rec, ok := t.recs[path]; ok && rec.gen == gen {
		rec.inflight = false
		rec.nextRetry = time.Now().Add(durabilityRetryBase)
	}
}

// counts reports how many records are in each state.
func (t *durabilityTracker) counts() map[DurabilityState]int {
	out := map[DurabilityState]int{}
	if t == nil {
		return out
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, rec := range t.recs {
		out[rec.State]++
	}
	return out
}

// load reads the journal and installs every record as due now. Called once at
// startup, before the retry loop starts.
func (t *durabilityTracker) load() []durabilityRecord {
	if t == nil || t.dir == "" {
		return nil
	}
	entries, err := os.ReadDir(t.dir)
	if err != nil {
		t.logger.Printf("Durability journal unreadable at %s: %v", t.dir, err)
		return nil
	}
	var loaded []durabilityRecord
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, ".json") {
			// Leftover temp files from an interrupted journal write.
			if strings.HasPrefix(name, ".tmp-") {
				_ = os.Remove(filepath.Join(t.dir, name))
			}
			continue
		}
		raw, err := os.ReadFile(filepath.Join(t.dir, name))
		if err != nil {
			continue
		}
		var rec durabilityRecord
		if err := json.Unmarshal(raw, &rec); err != nil || rec.Path == "" {
			t.logger.Printf("Durability journal: dropping unreadable record %s: %v", name, err)
			_ = os.Remove(filepath.Join(t.dir, name))
			continue
		}
		if rec.State != DurabilityFailed {
			rec.State = DurabilityLocal
		}
		t.nextGen++
		rec.gen = t.nextGen
		rec.inflight = false
		rec.nextRetry = time.Time{}
		cp := rec
		t.recs[rec.Path] = &cp
		loaded = append(loaded, rec)
	}
	return loaded
}

// pruneLocked bounds memory held by persisted records, which are kept only so
// receipts can report their commit generation.
func (t *durabilityTracker) pruneLocked() {
	if len(t.recs) <= durabilityMaxPersisted {
		return
	}
	type aged struct {
		path string
		at   int64
	}
	var done []aged
	for p, rec := range t.recs {
		if rec.State == DurabilityPersisted {
			done = append(done, aged{p, rec.UpdatedAt})
		}
	}
	sort.Slice(done, func(i, j int) bool { return done[i].at < done[j].at })
	excess := len(t.recs) - durabilityMaxPersisted/2
	for i := 0; i < len(done) && i < excess; i++ {
		delete(t.recs, done[i].path)
	}
}

func (t *durabilityTracker) journalFile(path string) string {
	sum := sha256.Sum256([]byte(path))
	return filepath.Join(t.dir, hex.EncodeToString(sum[:])+".json")
}

func (t *durabilityTracker) stripe(path string) *sync.Mutex {
	h := fnv.New32a()
	_, _ = h.Write([]byte(path))
	return &t.journalMu[h.Sum32()%durabilityJournalStripes]
}

// journal writes rec atomically (temp + rename), skipping the write
// if a newer generation has replaced it in the meantime.
func (t *durabilityTracker) journal(rec *durabilityRecord) {
	if t == nil || t.dir == "" {
		return
	}
	mu := t.stripe(rec.Path)
	mu.Lock()
	defer mu.Unlock()
	if !t.stillLatest(rec) {
		return
	}
	raw, err := json.Marshal(rec)
	if err != nil {
		return
	}
	tmp, err := os.CreateTemp(t.dir, ".tmp-*")
	if err != nil {
		t.logger.Printf("Durability journal write failed for %s: %v", rec.Path, err)
		return
	}
	_, werr := tmp.Write(raw)
	cerr := tmp.Close()
	if werr != nil || cerr != nil {
		_ = os.Remove(tmp.Name())
		t.logger.Printf("Durability journal write failed for %s: %v %v", rec.Path, werr, cerr)
		return
	}
	if err := os.Rename(tmp.Name(), t.journalFile(rec.Path)); err != nil {
		_ = os.Remove(tmp.Name())
		t.logger.Printf("Durability journal commit failed for %s: %v", rec.Path, err)
	}
}

func (t *durabilityTracker) stillLatest(rec *durabilityRecord) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	cur, ok := t.recs[rec.Path]
	return ok && cur.gen == rec.gen && cur.State != DurabilityPersisted
}

func (t *durabilityTracker) removeJournal(path string) {
	if t == nil || t.dir == "" {
		return
	}
	mu := t.stripe(path)
	mu.Lock()
	defer mu.Unlock()
	// A newer generation may have journalled since; keep its row.
	t.mu.Lock()
	cur, ok := t.recs[path]
	keep := ok && cur.State != DurabilityPersisted
	t.mu.Unlock()
	if keep {
		return
	}
	if err := os.Remove(t.journalFile(path)); err != nil && !os.IsNotExist(err) {
		t.logger.Printf("Durability journal cleanup failed for %s: %v", path, err)
	}
}

func durabilityBackoff(attempts int) time.Duration {
	if attempts < 1 {
		attempts = 1
	}
	d := durabilityRetryBase
	for i := 1; i < attempts; i++ {
		d *= 2
		if d >= durabilityRetryMax {
			return durabilityRetryMax
		}
	}
	return d
}

func isShutdownErr(err error) bool {
	return errors.Is(err, errPersistCancelled)
}

// pathUnderPrefix reports whether path equals prefix or lies beneath it on a
// path boundary ("/a" covers "/a/b", not "/ab").
func pathUnderPrefix(path, prefix string) bool {
	p := strings.TrimSuffix(prefix, "/")
	if p == "" {
		return strings.HasPrefix(path, "/")
	}
	return path == p || strings.HasPrefix(path, p+"/")
}
