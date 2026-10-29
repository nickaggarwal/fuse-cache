//! Burst load generator: N concurrent clients reading from one target the
//! way model nodes do during a cold start.
//!
//! * `chunk` mode: `GET /api/peer/read?path=<obj>_chunk_<i>` for whole
//!   chunk objects (what a node's fuse-client asks a store for);
//! * `blob` mode: `GET /v2/x/blobs/sha256:<d>` with `Range` windows of
//!   `window` bytes (what nydusd asks its registry proxy for).
//!
//! Each client walks its own work list (sequential through a target, or
//! scattered with a seeded RNG) on one keep-alive connection. A 503 counts
//! as busy and is retried after `Retry-After` jitter (as Go peers do), not
//! as an error. Output: one JSON object with aggregate MB/s, per-request
//! p50/p90/p99/max, busy and error counts.

use crate::http::{self, Conn};
use serde::Serialize;
use std::io::{self, Write};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;
use std::time::{Duration, Instant};

#[derive(Clone, Debug)]
pub enum Mode {
    /// Whole chunk objects of these parent paths: (path, size, stride).
    Chunk(Vec<(String, u64, u64)>),
    /// Range windows of these blobs: (hex digest, size).
    Blob(Vec<(String, u64)>),
}

#[derive(Clone, Debug)]
pub struct Opts {
    pub target: String,
    pub mode: Mode,
    pub clients: usize,
    /// Bytes each client reads (0 = one full pass over every target).
    pub bytes_per_client: u64,
    pub window: u64,
    /// "seq" (each client starts at a different offset and walks forward)
    /// or "rand" (scattered window-aligned offsets).
    pub pattern: String,
    pub api_key: Option<String>,
    pub seed: u64,
    pub timeout: Duration,
    pub verify: Option<fn(&[u8], u64) -> bool>,
}

#[derive(Serialize, Debug, Default)]
pub struct Report {
    pub target: String,
    pub mode: String,
    pub pattern: String,
    pub clients: usize,
    pub window_bytes: u64,
    pub requests: u64,
    pub bytes: u64,
    pub seconds: f64,
    pub mb_per_s: f64,
    pub p50_ms: f64,
    pub p90_ms: f64,
    pub p99_ms: f64,
    pub max_ms: f64,
    pub busy_503: u64,
    pub errors: u64,
    pub first_error: Option<String>,
    pub reconnects: u64,
}

fn splitmix64(mut z: u64) -> u64 {
    z = z.wrapping_add(0x9e37_79b9_7f4a_7c15);
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    z ^ (z >> 31)
}

/// One request: (target path+query, Range header, absolute offset in the
/// object for verification, expected bytes).
struct Req {
    target: String,
    range: Option<String>,
    abs: u64,
}

fn plan(o: &Opts, client: usize) -> Vec<Req> {
    let mut out = Vec::new();
    let mut rng = o.seed ^ (client as u64).wrapping_mul(0x9e37_79b9_7f4a_7c15);
    match &o.mode {
        Mode::Chunk(objs) => {
            // Work units: every chunk of every object.
            let units: Vec<(usize, u64)> =
                objs.iter().enumerate().flat_map(|(i, (_, size, stride))| (0..size.div_ceil(*stride)).map(move |c| (i, c))).collect();
            if units.is_empty() {
                return out;
            }
            let total: u64 = objs.iter().map(|(_, s, _)| *s).sum();
            let budget = if o.bytes_per_client == 0 { total } else { o.bytes_per_client };
            let mut got = 0u64;
            let mut k = (client * units.len() / o.clients.max(1)) % units.len();
            while got < budget {
                let idx = if o.pattern == "rand" {
                    rng = splitmix64(rng);
                    (rng % units.len() as u64) as usize
                } else {
                    let i = k;
                    k = (k + 1) % units.len();
                    i
                };
                let (oi, c) = units[idx];
                let (path, size, stride) = &objs[oi];
                out.push(Req { target: format!("/api/peer/read?path={}", http::query_escape(&format!("{path}_chunk_{c}"))), range: None, abs: c * stride });
                got += (*stride).min(size - c * stride);
            }
        }
        Mode::Blob(blobs) => {
            let w = o.window.max(1);
            let units: Vec<(usize, u64)> = blobs.iter().enumerate().flat_map(|(i, (_, size))| (0..size.div_ceil(w)).map(move |c| (i, c))).collect();
            if units.is_empty() {
                return out;
            }
            let total: u64 = blobs.iter().map(|(_, s)| *s).sum();
            let budget = if o.bytes_per_client == 0 { total } else { o.bytes_per_client };
            let mut got = 0u64;
            let mut k = (client * units.len() / o.clients.max(1)) % units.len();
            while got < budget {
                let idx = if o.pattern == "rand" {
                    rng = splitmix64(rng);
                    (rng % units.len() as u64) as usize
                } else {
                    let i = k;
                    k = (k + 1) % units.len();
                    i
                };
                let (bi, c) = units[idx];
                let (hex, size) = &blobs[bi];
                let start = c * w;
                let end = (start + w).min(*size) - 1;
                out.push(Req { target: format!("/v2/x/blobs/sha256:{hex}"), range: Some(format!("bytes={start}-{end}")), abs: start });
                got += end - start + 1;
            }
        }
    }
    out
}

struct Shared {
    lat_us: Mutex<Vec<u64>>,
    bytes: AtomicU64,
    busy: AtomicU64,
    errors: AtomicU64,
    reconnects: AtomicU64,
    first_error: Mutex<Option<String>>,
}

pub fn run(o: &Opts) -> Report {
    let sh = Shared {
        lat_us: Mutex::new(Vec::new()),
        bytes: AtomicU64::new(0),
        busy: AtomicU64::new(0),
        errors: AtomicU64::new(0),
        reconnects: AtomicU64::new(0),
        first_error: Mutex::new(None),
    };
    let plans: Vec<Vec<Req>> = (0..o.clients).map(|c| plan(o, c)).collect();
    let t0 = Instant::now();
    std::thread::scope(|sc| {
        for (ci, p) in plans.iter().enumerate() {
            let sh = &sh;
            sc.spawn(move || client(o, ci, p, sh));
        }
    });
    let secs = t0.elapsed().as_secs_f64();
    let mut lat = sh.lat_us.into_inner().unwrap();
    lat.sort_unstable();
    let pct = |p: f64| -> f64 {
        if lat.is_empty() {
            return 0.0;
        }
        lat[((p * lat.len() as f64).ceil() as usize).saturating_sub(1).min(lat.len() - 1)] as f64 / 1000.0
    };
    let bytes = sh.bytes.load(Ordering::Relaxed);
    Report {
        target: o.target.clone(),
        mode: match o.mode {
            Mode::Chunk(_) => "chunk".into(),
            Mode::Blob(_) => "blob".into(),
        },
        pattern: o.pattern.clone(),
        clients: o.clients,
        window_bytes: o.window,
        requests: lat.len() as u64,
        bytes,
        seconds: secs,
        mb_per_s: bytes as f64 / (1u64 << 20) as f64 / secs,
        p50_ms: pct(0.50),
        p90_ms: pct(0.90),
        p99_ms: pct(0.99),
        max_ms: lat.last().map(|v| *v as f64 / 1000.0).unwrap_or(0.0),
        busy_503: sh.busy.load(Ordering::Relaxed),
        errors: sh.errors.load(Ordering::Relaxed),
        first_error: sh.first_error.into_inner().unwrap(),
        reconnects: sh.reconnects.load(Ordering::Relaxed),
    }
}

fn client(o: &Opts, ci: usize, plan: &[Req], sh: &Shared) {
    let mut conn: Option<Conn> = None;
    let mut buf: Vec<u8> = Vec::new();
    let mut mine = Vec::with_capacity(plan.len());
    let mut rng = o.seed ^ 0xabcdef ^ ci as u64;
    for r in plan {
        let mut tries = 0;
        loop {
            tries += 1;
            let t = Instant::now();
            match one(o, &mut conn, r, &mut buf) {
                Ok((200 | 206, n)) => {
                    mine.push(t.elapsed().as_micros() as u64);
                    sh.bytes.fetch_add(n, Ordering::Relaxed);
                    if let Some(v) = o.verify {
                        if !v(&buf[..n as usize], r.abs) {
                            fail(sh, format!("content mismatch {} {:?}", r.target, r.range));
                        }
                    }
                    break;
                }
                Ok((503, _)) => {
                    sh.busy.fetch_add(1, Ordering::Relaxed);
                    if tries > 20 {
                        fail(sh, format!("still busy after 20 tries: {}", r.target));
                        break;
                    }
                    rng = splitmix64(rng);
                    std::thread::sleep(Duration::from_millis(10 + rng % 140));
                }
                Ok((code, _)) => {
                    fail(sh, format!("{} {:?}: status {code}", r.target, r.range));
                    break;
                }
                Err(e) => {
                    conn = None;
                    sh.reconnects.fetch_add(1, Ordering::Relaxed);
                    if tries > 3 {
                        fail(sh, format!("{} {:?}: {e}", r.target, r.range));
                        break;
                    }
                }
            }
        }
    }
    sh.lat_us.lock().unwrap().extend(mine);
}

fn fail(sh: &Shared, msg: String) {
    sh.errors.fetch_add(1, Ordering::Relaxed);
    sh.first_error.lock().unwrap().get_or_insert(msg);
}

/// One request on the client's connection: (status, body bytes in `buf`).
fn one(o: &Opts, conn: &mut Option<Conn>, r: &Req, buf: &mut Vec<u8>) -> io::Result<(u16, u64)> {
    if conn.is_none() {
        let s = http::connect(&o.target, Duration::from_secs(5))?;
        s.set_read_timeout(Some(o.timeout)).ok();
        s.set_write_timeout(Some(o.timeout)).ok();
        *conn = Some(Conn::new(s));
    }
    let c = conn.as_mut().unwrap();
    let mut req = format!("GET {} HTTP/1.1\r\nHost: {}\r\nUser-Agent: fusedata-burst\r\n", r.target, o.target);
    if let Some(rg) = &r.range {
        req.push_str(&format!("Range: {rg}\r\n"));
    }
    if let Some(k) = &o.api_key {
        req.push_str(&format!("X-API-Key: {k}\r\n"));
    }
    req.push_str("\r\n");
    c.stream.write_all(req.as_bytes())?;
    let n = c.read_head()?.ok_or_else(|| io::Error::new(io::ErrorKind::UnexpectedEof, "closed"))?;
    let head = c.take(n);
    let rh = http::parse_response(&head).map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "bad response"))?;
    let mut sink = VecSink(buf);
    sink.0.clear();
    let got = if rh.chunked {
        c.copy_chunked(&mut sink)?
    } else if let Some(len) = rh.content_length {
        c.copy_exact(len, &mut sink)?;
        len
    } else {
        let n = c.copy_to_eof(&mut sink)?;
        *conn = None;
        return Ok((rh.status, n));
    };
    if rh.conn_close {
        *conn = None;
    }
    Ok((rh.status, got))
}

struct VecSink<'a>(&'a mut Vec<u8>);

impl Write for VecSink<'_> {
    fn write(&mut self, b: &[u8]) -> io::Result<usize> {
        self.0.extend_from_slice(b);
        Ok(b.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

/// readbench's deterministic content: byte at offset `o` is byte `o % 8` of
/// `splitmix64(o / 8)`. For `--verify` against datasets written by
/// `readbench gen`/`gen-blob`.
pub fn check_pattern(buf: &[u8], off: u64) -> bool {
    buf.iter().enumerate().all(|(i, b)| {
        let o = off + i as u64;
        *b == splitmix64(o >> 3).to_le_bytes()[(o & 7) as usize]
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn opts(mode: Mode, clients: usize, pattern: &str) -> Opts {
        Opts {
            target: "x:1".into(),
            mode,
            clients,
            bytes_per_client: 0,
            window: 1 << 20,
            pattern: pattern.into(),
            api_key: None,
            seed: 42,
            timeout: Duration::from_secs(1),
            verify: None,
        }
    }

    #[test]
    fn chunk_plan_covers_object() {
        let o = opts(Mode::Chunk(vec![("/m.bin".into(), 40 << 20, 16 << 20)]), 2, "seq");
        let p0 = plan(&o, 0);
        assert_eq!(p0.len(), 3);
        assert!(p0[0].target.ends_with("%2Fm.bin_chunk_0"));
        let p1 = plan(&o, 1);
        assert!(p1[0].target.ends_with("_chunk_1")); // clients start apart
    }

    #[test]
    fn blob_plan_windows_and_tail() {
        let o = opts(Mode::Blob(vec![("ab".repeat(32), (3 << 20) + 5)]), 1, "seq");
        let p = plan(&o, 0);
        assert_eq!(p.len(), 4);
        assert_eq!(p[3].range.as_deref(), Some(format!("bytes={}-{}", 3 << 20, (3 << 20) + 4).as_str()));
        let r = opts(Mode::Blob(vec![("ab".repeat(32), 64 << 20)]), 1, "rand");
        let pr = plan(&r, 0);
        assert_eq!(pr.len(), 64);
        assert!(pr.iter().any(|q| q.abs != pr[0].abs));
    }

    #[test]
    fn pattern_check() {
        let mut b = vec![0u8; 20];
        for (i, x) in b.iter_mut().enumerate() {
            let o = 100 + i as u64;
            *x = splitmix64(o >> 3).to_le_bytes()[(o & 7) as usize];
        }
        assert!(check_pattern(&b, 100));
        b[3] ^= 1;
        assert!(!check_pattern(&b, 100));
    }
}
