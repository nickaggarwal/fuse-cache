//! Reader for a cache directory written by the Go client
//! (`internal/cache/nvme_storage.go` and friends). Byte-compatible with:
//!
//! * whole files at `<root>/<path>` (FUSE writes, `PutFromFile`, chunk
//!   assembly, small `Put`s);
//! * per-chunk files `<root>/<path>_chunk_<n>` (`PutFromReader`, `putChunked`,
//!   chunk-granular promotion), every chunk but the last exactly one stride;
//! * `<root>/<path>.sha256` sidecars: 64 lowercase hex chars, no newline,
//!   SHA-256 of the whole file; checked once per file version, as Go does;
//! * the durability journal `<root>/.fuse-durability/<sha256hex(path)>.json`,
//!   used here only as a shape hint (`size`, `chunk_size`, `is_chunked`);
//! * staging temp files (`.nvme-write-*`, `.nvme-stream-*`, `.nvme-dwrite-*`,
//!   `*.stage-*`), which are never served.
//!
//! What it deliberately does differently from Go, per the profile in
//! `rust/fusedata/README.md`:
//!
//! * A window is served by `pread` of exactly the window's bytes into the
//!   caller's buffer. Go's chunk path reads (and zero-fills) the whole 16 MiB
//!   chunk to serve a 128 KiB window, 128x amplification on random reads.
//! * File descriptors stay open across reads (cached per path/chunk), so the
//!   kernel's per-open readahead state survives. Go's whole-file ranged path
//!   opens, stats and closes a fresh descriptor per window, which resets it.
//! * Sequential streams get an explicit `POSIX_FADV_WILLNEED` hint ahead of
//!   the reader, crossing chunk-file boundaries the kernel cannot see.
//! * No per-read allocation: callers own the buffer.

use crate::chunk::{self, chunk_count_for, chunk_path, clamp_chunk_slice, plan_range};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::fs::File;
use std::io::{self, Read};
use std::os::unix::fs::{FileExt, MetadataExt};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock, RwLock};
use std::time::{Duration, Instant};

pub const DURABILITY_DIR: &str = ".fuse-durability";

/// Shape information learned from outside the cache dir (coordinator
/// location row, journal). Mirrors the fields Go's `CacheEntry` carries.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Hint {
    pub size: i64,
    pub chunked: bool,
    /// 0 = unknown (legacy metadata); the configured size is used instead.
    pub chunk_size: i64,
}

/// What `stat` of a logical path looks like.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Shape {
    pub size: i64,
    pub chunked: bool,
    pub stride: i64,
    pub nchunks: i64,
    /// True when every byte is available locally right now.
    pub complete: bool,
}

/// `(dev, ino, size, mtime_ns)`: a file version, as Go's `nvmeFileID` but
/// also keyed on inode so a rename-over with an equal mtime is noticed.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
struct FileId {
    dev: u64,
    ino: u64,
    size: i64,
    mtime_ns: i64,
}

impl FileId {
    fn of(md: &std::fs::Metadata) -> Self {
        FileId { dev: md.dev(), ino: md.ino(), size: md.size() as i64, mtime_ns: md.mtime() * 1_000_000_000 + md.mtime_nsec() }
    }
}

struct ChunkFile {
    file: File,
    len: i64,
}

enum Layout {
    Whole { file: File, size: i64 },
    /// Chunk files are opened lazily and cached once found. A slot is never
    /// cached as missing, so a chunk promoted later is picked up.
    Chunked { stride: i64, total: i64, slots: Vec<OnceLock<Arc<ChunkFile>>> },
    /// Known to exist remotely (hint), unchunked, not local.
    Remote { size: i64 },
}

struct Entry {
    layout: Layout,
    /// Identity of the file the entry was resolved from (whole file or
    /// chunk 0); `None` when nothing local backed it.
    id: Option<FileId>,
    resolved_ms: u64,
    checked_ms: AtomicU64,
    /// End offset of the previous read, for sequential detection.
    last_end: AtomicU64,
    /// Logical offset up to which WILLNEED has been issued.
    ra_until: AtomicU64,
}

#[derive(Clone, Copy, Debug)]
pub struct Options {
    /// Node `-chunk-size` in bytes, used when metadata has no stride.
    pub configured_chunk_size: i64,
    /// How often an entry re-stats its backing file to notice replacement.
    pub revalidate: Duration,
    /// Bytes of `WILLNEED` kept ahead of a sequential reader (0 disables).
    pub readahead: u64,
    /// Verify `.sha256` sidecars on whole files (once per version).
    pub verify_sidecars: bool,
    /// Upper bound on cached entries before the table is cleared.
    pub max_entries: usize,
    /// Prefetch workers that pull hinted ranges into the page cache, in
    /// `readahead_piece` units, so the device sees parallel streams (the lab
    /// disk does 5.4 GB/s with one reader and 17.5 GB/s with eight). 0 only
    /// issues async `WILLNEED` hints on the reading thread. 4 x 4 MiB won a
    /// cold-cache sweep (bench/sweep-readahead.sh).
    pub readahead_threads: usize,
    /// Prefetch work unit; a hint is split into pieces of this size so
    /// several workers share one chunk file.
    pub readahead_piece: i64,
}

impl Default for Options {
    fn default() -> Self {
        Options {
            configured_chunk_size: 16 << 20,
            revalidate: Duration::from_millis(500),
            readahead: 128 << 20,
            verify_sidecars: true,
            max_entries: 8192,
            readahead_threads: 4,
            readahead_piece: 4 << 20,
        }
    }
}

/// Counters exposed for basic metrics parity.
#[derive(Default, Debug)]
pub struct LayoutStats {
    pub reads: AtomicU64,
    pub bytes: AtomicU64,
    pub misses: AtomicU64,
    pub resolves: AtomicU64,
    pub verifies: AtomicU64,
    pub verify_failures: AtomicU64,
    pub readahead_hints: AtomicU64,
}

pub struct CacheDir {
    root: PathBuf,
    opts: Options,
    epoch: Instant,
    entries: RwLock<HashMap<String, Arc<Entry>>>,
    hints: RwLock<HashMap<String, Hint>>,
    verified: Mutex<HashMap<String, FileId>>,
    prefetch: Option<std::sync::mpsc::SyncSender<(Arc<ChunkFile>, i64, i64)>>,
    pub stats: LayoutStats,
}

/// Readahead hint pool: each job is one `WILLNEED` on one chunk file.
fn spawn_prefetch(threads: usize) -> Option<std::sync::mpsc::SyncSender<(Arc<ChunkFile>, i64, i64)>> {
    if threads == 0 {
        return None;
    }
    let (tx, rx) = std::sync::mpsc::sync_channel::<(Arc<ChunkFile>, i64, i64)>(threads * 8);
    let rx = Arc::new(Mutex::new(rx));
    for i in 0..threads {
        let rx = rx.clone();
        std::thread::Builder::new()
            .name(format!("ra-{i}"))
            .spawn(move || loop {
                let job = rx.lock().unwrap().recv();
                match job {
                    Ok((cf, off, len)) => populate(&cf.file, off, len),
                    Err(_) => return,
                }
            })
            .ok()?;
    }
    Some(tx)
}

/// Go `filepath.Clean("/" + raw)`: resolves `.`, `..` and duplicate slashes
/// against a rooted path, so the result can never escape the root.
pub fn clean_path(raw: &str) -> String {
    let mut parts: Vec<&str> = Vec::new();
    for seg in raw.split('/') {
        match seg {
            "" | "." => {}
            ".." => {
                parts.pop();
            }
            s => parts.push(s),
        }
    }
    let mut out = String::with_capacity(raw.len() + 1);
    out.push('/');
    out.push_str(&parts.join("/"));
    out
}

/// Names that are cache-internal state, never objects (Go
/// `rehydrateSkipFile` plus the write-path staging prefixes).
pub fn is_internal_name(name: &str) -> bool {
    name.ends_with(".sha256")
        || name.starts_with(".nvme-write-")
        || name.starts_with(".nvme-stream-")
        || name.starts_with(".nvme-dwrite-")
        || name.contains(".stage-")
}

fn not_found(what: &str) -> io::Error {
    io::Error::new(io::ErrorKind::NotFound, what.to_string())
}

/// `pread` until `buf` is full or EOF.
pub fn pread_full(file: &File, buf: &mut [u8], mut off: u64) -> io::Result<usize> {
    let mut done = 0;
    while done < buf.len() {
        match file.read_at(&mut buf[done..], off) {
            Ok(0) => break,
            Ok(n) => {
                done += n;
                off += n as u64;
            }
            Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
            Err(e) => return Err(e),
        }
    }
    Ok(done)
}

#[cfg(target_os = "linux")]
fn willneed(file: &File, off: i64, len: i64) {
    use std::os::fd::AsRawFd;
    unsafe {
        libc::posix_fadvise(file.as_raw_fd(), off, len, libc::POSIX_FADV_WILLNEED);
    }
}

#[cfg(not(target_os = "linux"))]
fn willneed(_file: &File, _off: i64, _len: i64) {}

/// Bring `[off, off+len)` into the page cache and wait until it is there,
/// from a prefetch worker. Several workers doing this on different pieces
/// give the device parallel streams; async hints alone (`WILLNEED`,
/// `readahead(2)`) were measured to give no gain over a single stream.
///
/// `MADV_POPULATE_READ` on a transient read-only mapping faults the pages in
/// without copying them to userspace (Linux 5.14+); older kernels fall back
/// to `pread` into a per-thread scratch buffer.
#[cfg(target_os = "linux")]
fn populate(file: &File, off: i64, len: i64) {
    use std::os::fd::AsRawFd;
    let page = 4096i64;
    let aligned = off & !(page - 1);
    let maplen = (len + (off - aligned)) as usize;
    unsafe {
        let p = libc::mmap(std::ptr::null_mut(), maplen, libc::PROT_READ, libc::MAP_SHARED, file.as_raw_fd(), aligned);
        if p != libc::MAP_FAILED {
            let r = libc::madvise(p, maplen, libc::MADV_POPULATE_READ);
            libc::munmap(p, maplen);
            if r == 0 {
                return;
            }
        }
    }
    thread_local! {
        static SCRATCH: std::cell::RefCell<Vec<u8>> = const { std::cell::RefCell::new(Vec::new()) };
    }
    SCRATCH.with(|b| {
        let mut b = b.borrow_mut();
        if b.len() < len as usize {
            b.resize(len as usize, 0);
        }
        let _ = pread_full(file, &mut b[..len as usize], off as u64);
    });
}

#[cfg(not(target_os = "linux"))]
fn populate(file: &File, off: i64, len: i64) {
    willneed(file, off, len)
}


#[cfg(target_os = "linux")]
fn fadvise_sequential(file: &File) {
    use std::os::fd::AsRawFd;
    unsafe {
        libc::posix_fadvise(file.as_raw_fd(), 0, 0, libc::POSIX_FADV_SEQUENTIAL);
    }
}

#[cfg(not(target_os = "linux"))]
fn fadvise_sequential(_file: &File) {}

/// Journal row fields used as a hint. Unknown fields are ignored, so newer
/// Go journal versions still parse.
#[derive(serde::Deserialize)]
struct JournalRow {
    path: String,
    #[serde(default)]
    size: i64,
    #[serde(default)]
    is_chunked: bool,
    #[serde(default)]
    chunk_size: i64,
}

impl CacheDir {
    pub fn new(root: impl Into<PathBuf>, opts: Options) -> Self {
        CacheDir {
            root: root.into(),
            opts,
            epoch: Instant::now(),
            entries: RwLock::new(HashMap::new()),
            hints: RwLock::new(HashMap::new()),
            verified: Mutex::new(HashMap::new()),
            prefetch: spawn_prefetch(opts.readahead_threads),
            stats: LayoutStats::default(),
        }
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    pub fn options(&self) -> &Options {
        &self.opts
    }

    /// Absolute on-disk location for a logical cache path.
    pub fn local_path(&self, path: &str) -> PathBuf {
        let clean = clean_path(path);
        self.root.join(&clean[1..])
    }

    fn now_ms(&self) -> u64 {
        self.epoch.elapsed().as_millis() as u64
    }

    /// Record shape metadata learned elsewhere (coordinator, journal).
    pub fn set_hint(&self, path: &str, hint: Hint) {
        let p = clean_path(path);
        let mut h = self.hints.write().unwrap();
        if h.get(&p) != Some(&hint) {
            h.insert(p.clone(), hint);
            drop(h);
            self.forget(&p);
        }
    }

    pub fn hint(&self, path: &str) -> Option<Hint> {
        let p = clean_path(path);
        if let Some(h) = self.hints.read().unwrap().get(&p) {
            return Some(*h);
        }
        self.journal_hint(&p)
    }

    /// Drop the cached entry for `path` (e.g. after promoting a chunk).
    pub fn forget(&self, path: &str) {
        self.entries.write().unwrap().remove(&clean_path(path));
    }

    fn journal_hint(&self, path: &str) -> Option<Hint> {
        let name = hex::encode(Sha256::digest(path.as_bytes()));
        let f = self.root.join(DURABILITY_DIR).join(format!("{name}.json"));
        let raw = std::fs::read(f).ok()?;
        let row: JournalRow = serde_json::from_slice(&raw).ok()?;
        if row.path != path {
            return None;
        }
        Some(Hint { size: row.size, chunked: row.is_chunked, chunk_size: row.chunk_size })
    }

    fn entry(&self, path: &str) -> io::Result<Arc<Entry>> {
        if let Some(e) = self.entries.read().unwrap().get(path) {
            let now = self.now_ms();
            let checked = e.checked_ms.load(Ordering::Relaxed);
            if now.saturating_sub(checked) < self.opts.revalidate.as_millis() as u64 {
                return Ok(e.clone());
            }
            if e.checked_ms.compare_exchange(checked, now, Ordering::Relaxed, Ordering::Relaxed).is_err() {
                // Another reader is revalidating; this read may use the
                // current entry.
                return Ok(e.clone());
            }
            if self.still_valid(path, e) {
                return Ok(e.clone());
            }
        }
        let fresh = Arc::new(self.resolve(path)?);
        let mut map = self.entries.write().unwrap();
        if map.len() >= self.opts.max_entries {
            map.clear();
        }
        map.insert(path.to_string(), fresh.clone());
        Ok(fresh)
    }

    /// Cheap revalidation: the backing file is still the same version, and
    /// an entry that lacked a whole local copy has not since gained one.
    fn still_valid(&self, path: &str, e: &Entry) -> bool {
        let whole = std::fs::metadata(self.local_path(path)).ok().filter(|m| m.is_file());
        match &e.layout {
            Layout::Whole { .. } => whole.map(|m| Some(FileId::of(&m)) == e.id).unwrap_or(false),
            Layout::Chunked { .. } | Layout::Remote { .. } => {
                if whole.is_some() {
                    return false;
                }
                match e.id {
                    Some(id) => std::fs::metadata(self.local_path(&chunk_path(path, 0)))
                        .map(|m| FileId::of(&m) == id)
                        .unwrap_or(false),
                    // Nothing local when resolved: re-resolve at most once a
                    // revalidation period so promoted chunk 0 is noticed.
                    None => self.now_ms().saturating_sub(e.resolved_ms) < self.opts.revalidate.as_millis() as u64,
                }
            }
        }
    }

    fn mk_entry(&self, layout: Layout, id: Option<FileId>) -> Entry {
        let now = self.now_ms();
        Entry { layout, id, resolved_ms: now, checked_ms: AtomicU64::new(now), last_end: AtomicU64::new(u64::MAX), ra_until: AtomicU64::new(0) }
    }

    fn resolve(&self, path: &str) -> io::Result<Entry> {
        self.stats.resolves.fetch_add(1, Ordering::Relaxed);
        let hint = self.hint(path);
        let whole_path = self.local_path(path);

        // Whole local copy. A chunked parent's whole copy is used only when
        // its size equals the recorded size (Go readWholeChunkedRange); a
        // short or stale copy falls through to the chunk files.
        if let Ok(f) = File::open(&whole_path) {
            let md = f.metadata()?;
            if md.is_file() {
                let size = md.size() as i64;
                let size_ok = match hint {
                    Some(h) if h.chunked && h.size > 0 => h.size == size,
                    _ => true,
                };
                if size_ok {
                    let id = FileId::of(&md);
                    if self.opts.verify_sidecars && !self.verify_sidecar(path, &f, id)? {
                        self.stats.verify_failures.fetch_add(1, Ordering::Relaxed);
                        return Err(io::Error::new(io::ErrorKind::InvalidData, format!("checksum mismatch for {path}")));
                    }
                    return Ok(self.mk_entry(Layout::Whole { file: f, size }, Some(id)));
                }
            }
        }

        // Per-chunk layout.
        let c0 = File::open(self.local_path(&chunk_path(path, 0))).ok();
        let c0md = match &c0 {
            Some(f) => Some(f.metadata()?),
            None => None,
        };
        let hinted_chunked = hint.map(|h| h.chunked).unwrap_or(false);
        if c0.is_some() || hinted_chunked {
            let stride = match (hint, &c0md) {
                // The stride the file was written with, as published.
                (Some(h), _) if h.chunked && h.chunk_size > 0 => h.chunk_size,
                // No published stride: chunk 0's length is the stride (every
                // chunk but the last is full width; chunk_complete.go relies
                // on the same fact).
                (_, Some(md)) if md.size() > 0 => md.size() as i64,
                _ => self.opts.configured_chunk_size,
            };
            let total = match hint {
                Some(h) if h.size > 0 => h.size,
                _ => self.infer_chunked_size(path, stride)?,
            };
            let n = chunk_count_for(total, stride).max(1) as usize;
            let slots: Vec<OnceLock<Arc<ChunkFile>>> = (0..n).map(|_| OnceLock::new()).collect();
            if let (Some(f), Some(md)) = (c0, &c0md) {
                let _ = slots[0].set(Arc::new(ChunkFile { len: md.size() as i64, file: f }));
            }
            let id = c0md.as_ref().map(FileId::of);
            return Ok(self.mk_entry(Layout::Chunked { stride, total, slots }, id));
        }

        if let Some(h) = hint {
            return Ok(self.mk_entry(Layout::Remote { size: h.size.max(0) }, None));
        }
        self.stats.misses.fetch_add(1, Ordering::Relaxed);
        Err(not_found(path))
    }

    /// Without metadata, a per-chunk file's size is the sum of its
    /// contiguous chunk files: (n-1) full strides plus the last one's length.
    fn infer_chunked_size(&self, path: &str, stride: i64) -> io::Result<i64> {
        let mut n = 0i64;
        let mut last_len = 0i64;
        loop {
            match std::fs::metadata(self.local_path(&chunk_path(path, n))) {
                Ok(m) => {
                    last_len = m.size() as i64;
                    n += 1;
                    if last_len < stride {
                        break;
                    }
                }
                Err(_) => break,
            }
        }
        if n == 0 {
            return Err(not_found(path));
        }
        Ok((n - 1) * stride + last_len)
    }

    /// Check `<path>.sha256` against the file once per version. A missing
    /// sidecar means unverifiable, not corrupt (Go `verifyNVMeReader`).
    fn verify_sidecar(&self, path: &str, f: &File, id: FileId) -> io::Result<bool> {
        if self.verified.lock().unwrap().get(path) == Some(&id) {
            return Ok(true);
        }
        let side = match std::fs::read(self.local_path(&format!("{path}.sha256"))) {
            Ok(s) => s,
            Err(_) => {
                self.mark_verified(path, id);
                return Ok(true);
            }
        };
        self.stats.verifies.fetch_add(1, Ordering::Relaxed);
        let mut h = Sha256::new();
        let mut buf = vec![0u8; 1 << 20];
        let mut off = 0u64;
        loop {
            let n = f.read_at(&mut buf, off)?;
            if n == 0 {
                break;
            }
            h.update(&buf[..n]);
            off += n as u64;
        }
        let actual = hex::encode(h.finalize());
        if actual.as_bytes() != side.as_slice() {
            self.verified.lock().unwrap().remove(path);
            return Ok(false);
        }
        self.mark_verified(path, id);
        Ok(true)
    }

    fn mark_verified(&self, path: &str, id: FileId) {
        let mut v = self.verified.lock().unwrap();
        if v.len() >= 4096 && !v.contains_key(path) {
            v.clear();
        }
        v.insert(path.to_string(), id);
    }

    /// Logical size of `path` without probing chunk files (FUSE getattr).
    pub fn size(&self, path: &str) -> io::Result<i64> {
        let e = self.entry(&clean_path(path))?;
        Ok(match &e.layout {
            Layout::Whole { size, .. } => *size,
            Layout::Chunked { total, .. } => *total,
            Layout::Remote { size } => *size,
        })
    }

    /// Logical shape of `path`, local or hinted. Opens every chunk file to
    /// compute `complete`; use [`size`](Self::size) on hot paths.
    pub fn shape(&self, path: &str) -> io::Result<Shape> {
        let p = clean_path(path);
        let e = self.entry(&p)?;
        Ok(match &e.layout {
            Layout::Whole { size, .. } => Shape { size: *size, chunked: false, stride: 0, nchunks: 0, complete: true },
            Layout::Chunked { stride, total, slots } => {
                let complete = (0..slots.len()).all(|i| self.chunk_file(&p, slots, i as i64).is_some());
                Shape { size: *total, chunked: true, stride: *stride, nchunks: slots.len() as i64, complete }
            }
            Layout::Remote { size } => Shape { size: *size, chunked: false, stride: 0, nchunks: 0, complete: false },
        })
    }

    fn chunk_file(&self, path: &str, slots: &[OnceLock<Arc<ChunkFile>>], idx: i64) -> Option<Arc<ChunkFile>> {
        let slot = slots.get(usize::try_from(idx).ok()?)?;
        if let Some(c) = slot.get() {
            return Some(c.clone());
        }
        let f = File::open(self.local_path(&chunk_path(path, idx))).ok()?;
        let len = f.metadata().ok()?.size() as i64;
        let _ = slot.set(Arc::new(ChunkFile { file: f, len }));
        slot.get().cloned()
    }

    /// Read `[off, off+buf.len())` of `path` from local files only.
    /// `NotFound` if any byte of the window is not local.
    pub fn read_at(&self, path: &str, off: u64, buf: &mut [u8]) -> io::Result<usize> {
        self.read_at_with(path, off, buf, |_, _, _| Err(not_found("chunk not local")))
    }

    /// Like [`read_at`], but a span whose chunk file is not local is handed
    /// to `on_miss(chunk_index, from, dst)`, which must fill `dst` from
    /// offset `from` within that chunk and return the bytes written (already
    /// clamped to the chunk's real length). `chunk_index == -1` means the
    /// whole unchunked object, and `from` is then the file offset.
    pub fn read_at_with<F>(&self, path: &str, off: u64, buf: &mut [u8], mut on_miss: F) -> io::Result<usize>
    where
        F: FnMut(i64, i64, &mut [u8]) -> io::Result<usize>,
    {
        let p = clean_path(path);
        let e = self.entry(&p)?;
        self.stats.reads.fetch_add(1, Ordering::Relaxed);
        let n = match &e.layout {
            Layout::Whole { file, size } => {
                if off as i64 >= *size {
                    return Ok(0);
                }
                let want = buf.len().min((*size - off as i64) as usize);
                self.readahead(&e, off, want as u64, |o, l| willneed(file, o, l), *size);
                pread_full(file, &mut buf[..want], off)?
            }
            Layout::Remote { size } => {
                if off as i64 >= *size {
                    return Ok(0);
                }
                let want = buf.len().min((*size - off as i64) as usize);
                on_miss(-1, off as i64, &mut buf[..want])?
            }
            Layout::Chunked { stride, total, slots } => {
                let spans = plan_range(*total, *stride, off as i64, buf.len() as i64);
                let mut pos = 0usize;
                for s in &spans {
                    if pos == buf.len() {
                        break;
                    }
                    match self.chunk_file(&p, slots, s.index) {
                        Some(cf) => {
                            let (from, to) = clamp_chunk_slice(s.from, s.to, cf.len);
                            // Bounded by the chunk (clamp) AND by the space left
                            // in the caller's buffer: with a stride smaller than
                            // the files on disk, each span clamps to a whole
                            // chunk file and their sum exceeds the window. Go
                            // appends into a growing slice there (and go-fuse
                            // truncates); here it would overrun.
                            let len = ((to - from) as usize).min(buf.len() - pos);
                            let got = pread_full(&cf.file, &mut buf[pos..pos + len], from as u64)?;
                            pos += got;
                            if got < len {
                                break; // chunk shrank under us; return short
                            }
                        }
                        None => {
                            let want = ((s.to - s.from).max(0) as usize).min(buf.len() - pos);
                            pos += on_miss(s.index, s.from, &mut buf[pos..pos + want])?;
                        }
                    }
                }
                if !spans.is_empty() {
                    let stride = *stride;
                    self.readahead(
                        &e,
                        off,
                        pos as u64,
                        |o, l| {
                            // Hint every chunk file the window [o, o+l) covers.
                            let mut cur = o;
                            while cur < o + l {
                                let idx = cur / stride;
                                let within = cur - idx * stride;
                                let take = (stride - within).min(o + l - cur);
                                if let Some(cf) = self.chunk_file(&p, slots, idx) {
                                    if within == 0 {
                                        fadvise_sequential(&cf.file);
                                    }
                                    match &self.prefetch {
                                        // Full queue: drop the piece rather
                                        // than block the reader.
                                        Some(tx) => {
                                            let mut o = within;
                                            while o < within + take {
                                                let l = self.opts.readahead_piece.max(4096).min(within + take - o);
                                                let _ = tx.try_send((cf.clone(), o, l));
                                                o += l;
                                            }
                                        }
                                        None => willneed(&cf.file, within, take),
                                    }
                                }
                                cur += take;
                            }
                        },
                        *total,
                    );
                }
                pos
            }
        };
        self.stats.bytes.fetch_add(n as u64, Ordering::Relaxed);
        Ok(n)
    }

    /// Sequential detection and `WILLNEED` ahead of the reader. Only fires
    /// when this read starts where the previous one ended, and only for the
    /// part of the window not already hinted, so steady state costs one
    /// fadvise per `readahead/2` bytes consumed.
    fn readahead<H: FnMut(i64, i64)>(&self, e: &Entry, off: u64, len: u64, mut hint: H, total: i64) {
        let ra = self.opts.readahead;
        let end = off + len;
        let prev = e.last_end.swap(end, Ordering::Relaxed);
        if ra == 0 || prev != off {
            return;
        }
        let until = e.ra_until.load(Ordering::Relaxed);
        if until > end + ra / 2 {
            return;
        }
        let from = until.max(end);
        let to = (end + ra).min(total as u64);
        if to <= from {
            return;
        }
        if e.ra_until.compare_exchange(until, to, Ordering::Relaxed, Ordering::Relaxed).is_ok() {
            self.stats.readahead_hints.fetch_add(1, Ordering::Relaxed);
            hint(from as i64, (to - from) as i64);
        }
    }

    /// Resolve a peer `/api/peer/read` request path to `(file, offset,
    /// length)` exactly as Go's handler does: a literal local file first
    /// (`LocalFilePath`, which includes literal chunk files), then a chunk
    /// of a whole chunked parent (`LocalChunkFile`).
    pub fn peer_serve_target(&self, req_path: &str) -> Option<(File, u64, u64)> {
        let p = clean_path(req_path);
        if let Ok(f) = File::open(self.local_path(&p)) {
            if let Ok(md) = f.metadata() {
                if md.is_file() {
                    return Some((f, 0, md.size()));
                }
            }
        }
        let (parent, idx) = chunk::parse_chunk_path(&p)?;
        if idx < 0 {
            return None;
        }
        // Go serves a chunk of a whole parent only when the parent's entry
        // says IsChunked (LocalChunkFile); after a restart that comes from
        // the durability journal or the coordinator. Same rule here: a
        // hint/journal row must say chunked, otherwise 404 — guessing the
        // stride could hand a peer misaligned bytes.
        let f = File::open(self.local_path(parent)).ok()?;
        let md = f.metadata().ok()?;
        if !md.is_file() {
            return None;
        }
        let (size, stride) = match self.hint(parent) {
            Some(h) if h.chunked && h.size > 0 => (h.size, chunk::effective_chunk_size(h.chunk_size, self.opts.configured_chunk_size)),
            _ => return None,
        };
        if stride <= 0 || size <= 0 {
            return None;
        }
        let offset = idx.checked_mul(stride)?;
        if offset >= size {
            return None;
        }
        let length = stride.min(size - offset);
        Some((f, offset as u64, length as u64))
    }

    /// List a directory the way the FUSE mount should present it: chunk
    /// files collapse to their parent, internal files are hidden.
    pub fn list_dir(&self, dir: &str) -> io::Result<Vec<(String, bool)>> {
        let d = clean_path(dir);
        let abs = self.local_path(&d);
        let mut seen: HashMap<String, bool> = HashMap::new();
        for ent in std::fs::read_dir(abs)? {
            let ent = ent?;
            let name = match ent.file_name().into_string() {
                Ok(n) => n,
                Err(_) => continue,
            };
            if d == "/" && name == DURABILITY_DIR {
                continue;
            }
            if is_internal_name(&name) {
                continue;
            }
            let is_dir = ent.file_type().map(|t| t.is_dir()).unwrap_or(false);
            let logical = match chunk::parse_chunk_path(&name) {
                Some((parent, i)) if !is_dir && i >= 0 => parent.to_string(),
                _ => name,
            };
            seen.entry(logical).and_modify(|d| *d |= is_dir).or_insert(is_dir);
        }
        let mut out: Vec<(String, bool)> = seen.into_iter().collect();
        out.sort();
        Ok(out)
    }

    /// Read a sidecar or other small file verbatim (tests, tooling).
    pub fn read_small(&self, path: &str) -> io::Result<Vec<u8>> {
        let mut v = Vec::new();
        File::open(self.local_path(path))?.read_to_end(&mut v)?;
        Ok(v)
    }
}
