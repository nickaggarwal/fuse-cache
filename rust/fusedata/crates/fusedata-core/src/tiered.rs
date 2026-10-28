//! Tier fall-through for one read: local NVMe, then peers, then Azure.
//!
//! Remote tiers move whole chunk objects (that is the unit peers and the
//! cloud store), but only what the window needs is copied to the caller.
//! Fetched chunks go through a small per-process chunk cache with
//! single-flight, so concurrent windows of one chunk cost one fetch, and are
//! promoted to NVMe in the Go layout (`<path>_chunk_<n>`, written to a
//! `.nvme-stream-*` temp file and renamed into place) so the next read and
//! the Go client both see them.
//!
//! Peer errors keep Go's three classes: busy and miss move on to the next
//! holder; only "other" is a failure. Busy holders get one jittered retry
//! pass when every holder was busy.

use crate::azure::AzureReader;
use crate::chunk::chunk_path;
use crate::coord::CoordClient;
use crate::layout::CacheDir;
use crate::peer::{PeerClient, PeerError};
use std::collections::HashMap;
use std::io::{self, Write};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;

#[derive(Default, Debug)]
pub struct TierStats {
    pub local_bytes: AtomicU64,
    pub peer_fetches: AtomicU64,
    pub peer_bytes: AtomicU64,
    pub cloud_fetches: AtomicU64,
    pub cloud_bytes: AtomicU64,
    pub promotions: AtomicU64,
    pub singleflight_waits: AtomicU64,
}

enum Slot {
    Loading,
    Ready(Arc<Vec<u8>>),
}

/// Bounded chunk cache with single-flight. FIFO eviction is enough: a FUSE
/// reader consumes a chunk's windows close together in time.
struct ChunkCache {
    map: Mutex<(HashMap<String, Slot>, std::collections::VecDeque<String>)>,
    cv: Condvar,
    cap: usize,
}

impl ChunkCache {
    fn new(cap: usize) -> Self {
        ChunkCache { map: Mutex::new((HashMap::new(), Default::default())), cv: Condvar::new(), cap }
    }
}

pub struct TieredReader {
    pub cache: Arc<CacheDir>,
    peers: Option<(Arc<CoordClient>, Arc<PeerClient>)>,
    cloud: Option<Arc<AzureReader>>,
    chunks: ChunkCache,
    promote: bool,
    pub stats: TierStats,
}

fn other(e: impl std::fmt::Display) -> io::Error {
    io::Error::other(e.to_string())
}

impl TieredReader {
    pub fn new(cache: Arc<CacheDir>) -> Self {
        TieredReader { cache, peers: None, cloud: None, chunks: ChunkCache::new(16), promote: true, stats: TierStats::default() }
    }

    pub fn with_peers(mut self, coord: Arc<CoordClient>, client: Arc<PeerClient>) -> Self {
        self.peers = Some((coord, client));
        self
    }

    pub fn with_cloud(mut self, az: Arc<AzureReader>) -> Self {
        self.cloud = Some(az);
        self
    }

    pub fn with_promotion(mut self, on: bool) -> Self {
        self.promote = on;
        self
    }

    /// Make shape metadata from the coordinator available to the layout
    /// reader before the first read of `path`.
    pub fn learn_shape(&self, path: &str) {
        if let Some((coord, _)) = &self.peers {
            if self.cache.hint(path).is_none() {
                if let Some(h) = coord.shape(path) {
                    self.cache.set_hint(path, h);
                }
            }
        }
    }

    /// Read `[off, off+buf.len())` of `path` through the tiers.
    pub fn read(&self, path: &str, off: u64, buf: &mut [u8]) -> io::Result<usize> {
        match self.cache.read_at(path, off, buf) {
            Ok(n) => {
                self.stats.local_bytes.fetch_add(n as u64, Ordering::Relaxed);
                return Ok(n);
            }
            Err(e) if e.kind() == io::ErrorKind::NotFound => {}
            Err(e) => return Err(e),
        }
        if self.peers.is_none() && self.cloud.is_none() {
            return Err(io::Error::new(io::ErrorKind::NotFound, format!("{path} not local")));
        }
        self.learn_shape(path);
        let p = crate::layout::clean_path(path);
        self.cache.read_at_with(&p, off, buf, |idx, from, dst| {
            let object = if idx < 0 { p.clone() } else { chunk_path(&p, idx) };
            let data = self.fetch_object(&object)?;
            if idx >= 0 && self.promote {
                self.promote_chunk(&p, &object, &data);
            }
            let (f, t) = crate::chunk::clamp_chunk_slice(from, from + dst.len() as i64, data.len() as i64);
            let n = (t - f) as usize;
            dst[..n].copy_from_slice(&data[f as usize..t as usize]);
            Ok(n)
        })
    }

    /// One remote object (chunk or whole small file), single-flighted.
    pub fn fetch_object(&self, object: &str) -> io::Result<Arc<Vec<u8>>> {
        {
            let mut g = self.chunks.map.lock().unwrap();
            loop {
                match g.0.get(object) {
                    Some(Slot::Ready(d)) => return Ok(d.clone()),
                    Some(Slot::Loading) => {
                        self.stats.singleflight_waits.fetch_add(1, Ordering::Relaxed);
                        g = self.chunks.cv.wait(g).unwrap();
                    }
                    None => {
                        g.0.insert(object.to_string(), Slot::Loading);
                        break;
                    }
                }
            }
        }
        let r = self.fetch_remote(object);
        let mut g = self.chunks.map.lock().unwrap();
        let out = match r {
            Ok(d) => {
                let d = Arc::new(d);
                g.0.insert(object.to_string(), Slot::Ready(d.clone()));
                g.1.push_back(object.to_string());
                while g.1.len() > self.chunks.cap {
                    if let Some(old) = g.1.pop_front() {
                        g.0.remove(&old);
                    }
                }
                Ok(d)
            }
            Err(e) => {
                // Failures are not cached; the next reader retries.
                g.0.remove(object);
                Err(e)
            }
        };
        self.chunks.cv.notify_all();
        out
    }

    fn fetch_remote(&self, object: &str) -> io::Result<Vec<u8>> {
        let parent = crate::chunk::parse_chunk_path(object).map(|(p, _)| p).unwrap_or(object);
        let mut last: Option<io::Error> = None;
        if let Some((coord, client)) = &self.peers {
            let cands = coord.candidates(parent, object);
            let mut busy = Vec::new();
            let mut buf = Vec::new();
            for addr in &cands {
                self.stats.peer_fetches.fetch_add(1, Ordering::Relaxed);
                match client.fetch(addr, object, &mut buf) {
                    Ok(n) => {
                        buf.truncate(n);
                        self.stats.peer_bytes.fetch_add(n as u64, Ordering::Relaxed);
                        return Ok(buf);
                    }
                    Err(PeerError::Busy) => busy.push(addr.clone()),
                    Err(PeerError::Miss) => {}
                    Err(e) => last = Some(other(e)),
                }
            }
            if !busy.is_empty() && busy.len() == cands.len() {
                std::thread::sleep(jitter(Duration::from_millis(10), Duration::from_millis(150)));
                for addr in &busy {
                    if let Ok(n) = client.fetch(addr, object, &mut buf) {
                        buf.truncate(n);
                        self.stats.peer_bytes.fetch_add(n as u64, Ordering::Relaxed);
                        return Ok(buf);
                    }
                }
            }
        }
        if let Some(az) = &self.cloud {
            self.stats.cloud_fetches.fetch_add(1, Ordering::Relaxed);
            let mut buf = Vec::new();
            let n = az.read_object(object, &mut buf).map_err(io::Error::from)?;
            buf.truncate(n);
            self.stats.cloud_bytes.fetch_add(n as u64, Ordering::Relaxed);
            return Ok(buf);
        }
        Err(last.unwrap_or_else(|| io::Error::new(io::ErrorKind::NotFound, format!("{object}: no tier holds it"))))
    }

    /// Land a fetched chunk at `<root>/<object>` with Go's staging
    /// convention, then drop the cached layout entry so it is used.
    fn promote_chunk(&self, parent: &str, object: &str, data: &[u8]) {
        let dst = self.cache.local_path(object);
        if dst.exists() {
            return;
        }
        let Some(dir) = dst.parent() else { return };
        if std::fs::create_dir_all(dir).is_err() {
            return;
        }
        let tmp = dir.join(format!(".nvme-stream-{}-{}", std::process::id(), fastrand_u64()));
        let ok = std::fs::File::create(&tmp).and_then(|mut f| f.write_all(data)).is_ok() && std::fs::rename(&tmp, &dst).is_ok();
        if ok {
            self.stats.promotions.fetch_add(1, Ordering::Relaxed);
            self.cache.forget(parent);
        } else {
            let _ = std::fs::remove_file(&tmp);
        }
    }
}

fn fastrand_u64() -> u64 {
    use std::hash::{BuildHasher, Hasher};
    let mut h = std::collections::hash_map::RandomState::new().build_hasher();
    h.write_u64(std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_nanos() as u64).unwrap_or(0));
    h.finish()
}

fn jitter(min: Duration, max: Duration) -> Duration {
    let span = (max - min).as_micros() as u64;
    min + Duration::from_micros(fastrand_u64() % span.max(1))
}
