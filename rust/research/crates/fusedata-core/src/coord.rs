//! Minimal coordinator client: which peers hold a path, and where they are.
//! Speaks the coordinator's HTTP API (`GET /api/files/location?path=`,
//! `GET /api/peers`), the documented fallback transport. Results are cached
//! briefly, mirroring PeerStorage's 5s file hints and 2s peer list.

use serde::Deserialize;
use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

#[derive(Clone, Debug, Deserialize)]
pub struct FileLocation {
    pub file_path: String,
    #[serde(default)]
    pub peer_id: String,
    #[serde(default)]
    pub storage_tier: String,
    #[serde(default)]
    pub file_size: i64,
    #[serde(default)]
    pub is_chunked: bool,
    #[serde(default)]
    pub chunk_size: i64,
}

#[derive(Clone, Debug, Deserialize)]
pub struct PeerInfo {
    pub id: String,
    /// HTTP API address (`POD_IP:8081`), where `/api/peer/read` lives.
    pub address: String,
    #[serde(default)]
    pub status: String,
}

pub struct CoordClient {
    base: String,
    agent: ureq::Agent,
    local_peer_id: String,
    locs: Mutex<HashMap<String, (Instant, Vec<FileLocation>)>>,
    peers: Mutex<Option<(Instant, Vec<PeerInfo>)>>,
    loc_ttl: Duration,
    peer_ttl: Duration,
}

impl CoordClient {
    /// `addr` is `host:port` of the coordinator HTTP API.
    pub fn new(addr: &str, local_peer_id: &str) -> Self {
        CoordClient {
            base: format!("http://{addr}"),
            agent: ureq::AgentBuilder::new().timeout(Duration::from_secs(5)).build(),
            local_peer_id: local_peer_id.to_string(),
            locs: Mutex::new(HashMap::new()),
            peers: Mutex::new(None),
            loc_ttl: Duration::from_secs(5),
            peer_ttl: Duration::from_secs(2),
        }
    }

    pub fn locations(&self, path: &str) -> Result<Vec<FileLocation>, String> {
        if let Some((at, v)) = self.locs.lock().unwrap().get(path) {
            if at.elapsed() < self.loc_ttl {
                return Ok(v.clone());
            }
        }
        // Go builds this URL with fmt.Sprintf and no escaping; escape here
        // so paths with spaces or '&' survive.
        let url = format!("{}/api/files/location?path={}", self.base, crate::peer::query_escape(path));
        let v: Vec<FileLocation> = self
            .agent
            .get(&url)
            .call()
            .map_err(|e| e.to_string())?
            .into_string()
            .map_err(|e| e.to_string())
            .and_then(|b| serde_json::from_str::<Option<Vec<FileLocation>>>(&b).map_err(|e| e.to_string()))?
            .unwrap_or_default();
        self.locs.lock().unwrap().insert(path.to_string(), (Instant::now(), v.clone()));
        Ok(v)
    }

    pub fn peers(&self) -> Result<Vec<PeerInfo>, String> {
        if let Some((at, v)) = &*self.peers.lock().unwrap() {
            if at.elapsed() < self.peer_ttl {
                return Ok(v.clone());
            }
        }
        let url = format!("{}/api/peers?requester_id={}", self.base, crate::peer::query_escape(&self.local_peer_id));
        let v: Vec<PeerInfo> = self
            .agent
            .get(&url)
            .call()
            .map_err(|e| e.to_string())?
            .into_string()
            .map_err(|e| e.to_string())
            .and_then(|b| serde_json::from_str::<Option<Vec<PeerInfo>>>(&b).map_err(|e| e.to_string()))?
            .unwrap_or_default();
        *self.peers.lock().unwrap() = Some((Instant::now(), v.clone()));
        Ok(v)
    }

    /// Ordered HTTP addresses of active peers holding `parent` (other than
    /// this node), holders first then everyone else, the way
    /// `partitionPeersForPath` orders candidates. Holder order is rotated
    /// by a hash of `key` so different chunks spread across holders.
    pub fn candidates(&self, parent: &str, key: &str) -> Vec<String> {
        let peers = match self.peers() {
            Ok(p) => p,
            Err(_) => return Vec::new(),
        };
        let holders: std::collections::HashSet<String> = self
            .locations(parent)
            .unwrap_or_default()
            .into_iter()
            .filter(|l| !l.peer_id.is_empty() && l.peer_id != self.local_peer_id && l.storage_tier != "cloud")
            .map(|l| l.peer_id)
            .collect();
        let mut pref: Vec<&PeerInfo> = Vec::new();
        let mut rest: Vec<&PeerInfo> = Vec::new();
        for p in peers.iter().filter(|p| p.status == "active" && !p.address.is_empty() && p.id != self.local_peer_id) {
            if holders.contains(&p.id) {
                pref.push(p)
            } else {
                rest.push(p)
            }
        }
        let rot = |v: &mut Vec<&PeerInfo>| {
            if v.len() > 1 {
                v.sort_by(|a, b| a.id.cmp(&b.id));
                let h = fnv1a(key.as_bytes()) as usize % v.len();
                v.rotate_left(h);
            }
        };
        rot(&mut pref);
        // Only holders are worth asking when the coordinator named any; with
        // no location data, every peer is a candidate (Go's fallback).
        if pref.is_empty() {
            rot(&mut rest);
            return rest.into_iter().map(|p| p.address.clone()).collect();
        }
        pref.into_iter().map(|p| p.address.clone()).collect()
    }

    /// Shape hint from the location rows (prefers a chunked row's stride).
    pub fn shape(&self, path: &str) -> Option<crate::layout::Hint> {
        let locs = self.locations(path).ok()?;
        let best = locs.iter().find(|l| l.is_chunked).or_else(|| locs.first())?;
        Some(crate::layout::Hint { size: best.file_size, chunked: best.is_chunked, chunk_size: if best.is_chunked { best.chunk_size } else { 0 } })
    }
}

fn fnv1a(b: &[u8]) -> u32 {
    let mut h: u32 = 0x811c_9dc5;
    for &x in b {
        h ^= x as u32;
        h = h.wrapping_mul(0x0100_0193);
    }
    h
}
