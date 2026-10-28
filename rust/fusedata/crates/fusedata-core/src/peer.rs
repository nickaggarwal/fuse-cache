//! The peer bulk-read protocol, `GET /api/peer/read?path=<p>` over plain
//! HTTP/1.1, as served by `internal/api/peer_read.go` and consumed by
//! `PeerStorage.readFromPeerRaw`:
//!
//! * `X-API-Key` must match when the server has a key (else 401);
//! * admission control first: a full serve gate answers `503` with
//!   `Retry-After: 1` (busy — the requester moves to the next holder);
//! * missing `path` → 400; the path is `filepath.Clean("/"+raw)`;
//! * a literal local file (whole file or a literal `_chunk_N` file) is
//!   streamed whole; else `<parent>_chunk_N` of a whole chunked parent is
//!   streamed as that byte range; else `404` (miss — stale routing);
//! * `200` carries `Content-Type: application/octet-stream` and an exact
//!   `Content-Length`, body via `sendfile(2)`.
//!
//! The server here is byte-compatible with that and adds `MSG_MORE` on the
//! header so headers and the first payload bytes share a segment. The client
//! keeps connections alive, reads the body straight into a caller-owned
//! buffer (no intermediate copies, no per-request allocation once warm) and
//! classifies errors into the same three classes Go uses: busy, miss, other.

use crate::layout::{clean_path, CacheDir};
use std::collections::HashMap;
use std::fs::File;
use std::io::{self, Read, Write};
use std::net::{Shutdown, TcpListener, TcpStream, ToSocketAddrs};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// The three peer error classes from CLAUDE.md: busy is admission control,
/// miss is stale routing, everything else is a real failure.
#[derive(Debug)]
pub enum PeerError {
    Busy,
    Miss,
    Unreachable(io::Error),
    Other(String),
}

impl std::fmt::Display for PeerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PeerError::Busy => write!(f, "peer busy"),
            PeerError::Miss => write!(f, "peer miss"),
            PeerError::Unreachable(e) => write!(f, "peer unreachable: {e}"),
            PeerError::Other(s) => write!(f, "peer error: {s}"),
        }
    }
}

impl std::error::Error for PeerError {}

impl From<io::Error> for PeerError {
    fn from(e: io::Error) -> Self {
        PeerError::Other(e.to_string())
    }
}

/// Characters Go's `url.QueryEscape` leaves alone: `[A-Za-z0-9-_.~]`.
/// Space becomes `+` in Go; `%20` decodes identically on every server.
const QUERY: &percent_encoding::AsciiSet = &percent_encoding::NON_ALPHANUMERIC.remove(b'-').remove(b'_').remove(b'.').remove(b'~');

pub fn query_escape(s: &str) -> String {
    percent_encoding::utf8_percent_encode(s, QUERY).to_string()
}

/// Go `url.ParseQuery(...).Get(key)`: first value, `+` is a space.
pub fn query_get(query: &str, key: &str) -> Option<String> {
    for pair in query.split('&') {
        let (k, v) = pair.split_once('=').unwrap_or((pair, ""));
        let k = percent_encoding::percent_decode_str(&k.replace('+', " ")).decode_utf8().ok()?.into_owned();
        if k == key {
            let v = v.replace('+', " ");
            return percent_encoding::percent_decode_str(&v).decode_utf8().ok().map(|c| c.into_owned());
        }
    }
    None
}

// ---------------------------------------------------------------------------
// Client

#[derive(Clone, Debug)]
pub struct PeerClientConfig {
    pub api_key: Option<String>,
    /// Per-request I/O timeout (Go: PeerTimeout, 30s).
    pub timeout: Duration,
    /// TCP connect timeout (Go: peerConnectTimeout, 2s).
    pub connect_timeout: Duration,
    /// Idle connections kept per peer (Go: MaxIdleConnsPerHost 32).
    pub max_idle_per_peer: usize,
}

impl Default for PeerClientConfig {
    fn default() -> Self {
        PeerClientConfig { api_key: None, timeout: Duration::from_secs(30), connect_timeout: Duration::from_secs(2), max_idle_per_peer: 32 }
    }
}

#[derive(Default, Debug)]
pub struct PeerClientStats {
    pub requests: AtomicU64,
    pub bytes: AtomicU64,
    pub busy: AtomicU64,
    pub miss: AtomicU64,
    pub errors: AtomicU64,
    pub dials: AtomicU64,
}

pub struct PeerClient {
    cfg: PeerClientConfig,
    idle: Mutex<HashMap<String, Vec<TcpStream>>>,
    pub stats: PeerClientStats,
}

/// Parsed response head.
struct Head {
    status: u16,
    content_length: Option<u64>,
    close: bool,
    chunked: bool,
    head_len: usize,
}

fn parse_head(buf: &[u8]) -> Result<Option<Head>, PeerError> {
    let mut hdrs = [httparse::EMPTY_HEADER; 32];
    let mut resp = httparse::Response::new(&mut hdrs);
    match resp.parse(buf) {
        Ok(httparse::Status::Partial) => Ok(None),
        Ok(httparse::Status::Complete(n)) => {
            let mut h = Head { status: resp.code.unwrap_or(0), content_length: None, close: resp.version == Some(0), chunked: false, head_len: n };
            for hd in resp.headers.iter() {
                if hd.name.eq_ignore_ascii_case("content-length") {
                    h.content_length = std::str::from_utf8(hd.value).ok().and_then(|v| v.trim().parse().ok());
                } else if hd.name.eq_ignore_ascii_case("connection") {
                    h.close = hd.value.eq_ignore_ascii_case(b"close");
                } else if hd.name.eq_ignore_ascii_case("transfer-encoding") {
                    h.chunked = !hd.value.eq_ignore_ascii_case(b"identity");
                }
            }
            Ok(Some(h))
        }
        Err(e) => Err(PeerError::Other(format!("bad response: {e}"))),
    }
}

impl PeerClient {
    pub fn new(cfg: PeerClientConfig) -> Self {
        PeerClient { cfg, idle: Mutex::new(HashMap::new()), stats: PeerClientStats::default() }
    }

    fn checkout(&self, addr: &str) -> Result<(TcpStream, bool), PeerError> {
        if let Some(c) = self.idle.lock().unwrap().get_mut(addr).and_then(|v| v.pop()) {
            return Ok((c, true));
        }
        self.stats.dials.fetch_add(1, Ordering::Relaxed);
        let mut last = None;
        let addrs = addr.to_socket_addrs().map_err(PeerError::Unreachable)?;
        for sa in addrs {
            match TcpStream::connect_timeout(&sa, self.cfg.connect_timeout) {
                Ok(s) => {
                    s.set_nodelay(true).ok();
                    s.set_read_timeout(Some(self.cfg.timeout)).ok();
                    s.set_write_timeout(Some(self.cfg.timeout)).ok();
                    set_rcvbuf(&s, 4 << 20);
                    return Ok((s, false));
                }
                Err(e) => last = Some(e),
            }
        }
        Err(PeerError::Unreachable(last.unwrap_or_else(|| io::Error::new(io::ErrorKind::NotFound, "no address"))))
    }

    fn checkin(&self, addr: &str, s: TcpStream) {
        let mut idle = self.idle.lock().unwrap();
        let v = idle.entry(addr.to_string()).or_default();
        if v.len() < self.cfg.max_idle_per_peer {
            v.push(s);
        }
    }

    /// Fetch `path` from the peer at `addr` (host:port of its HTTP API).
    /// The body lands in `buf[..n]`; `buf` only ever grows, so a reused
    /// buffer costs no allocation or zero-fill in steady state.
    pub fn fetch(&self, addr: &str, path: &str, buf: &mut Vec<u8>) -> Result<usize, PeerError> {
        self.fetch_query(addr, path, "", buf)
    }

    /// Fetch `[off, off+len)` of `path` (`off`/`len` query parameters, the
    /// sub-range form of `/api/peer/read` the Go blob path uses). The server
    /// clamps to the object.
    pub fn fetch_range(&self, addr: &str, path: &str, off: u64, len: u64, buf: &mut Vec<u8>) -> Result<usize, PeerError> {
        self.fetch_query(addr, path, &format!("&off={off}&len={len}"), buf)
    }

    fn fetch_query(&self, addr: &str, path: &str, extra: &str, buf: &mut Vec<u8>) -> Result<usize, PeerError> {
        self.stats.requests.fetch_add(1, Ordering::Relaxed);
        // A pooled connection may have been closed by the server while
        // idle; retry exactly once on a fresh dial in that case.
        let (s, reused) = self.checkout(addr)?;
        let r = self.fetch_on(addr, s, path, extra, buf);
        let r = match r {
            Err(PeerError::Other(ref m)) if reused && m.starts_with("stale:") => {
                let (s, _) = self.checkout_fresh(addr)?;
                self.fetch_on(addr, s, path, extra, buf)
            }
            other => other,
        };
        match &r {
            Ok(n) => {
                self.stats.bytes.fetch_add(*n as u64, Ordering::Relaxed);
            }
            Err(PeerError::Busy) => {
                self.stats.busy.fetch_add(1, Ordering::Relaxed);
            }
            Err(PeerError::Miss) => {
                self.stats.miss.fetch_add(1, Ordering::Relaxed);
            }
            Err(_) => {
                self.stats.errors.fetch_add(1, Ordering::Relaxed);
            }
        }
        r
    }

    fn checkout_fresh(&self, addr: &str) -> Result<(TcpStream, bool), PeerError> {
        self.idle.lock().unwrap().remove(addr);
        self.checkout(addr)
    }

    fn fetch_on(&self, addr: &str, mut s: TcpStream, path: &str, extra: &str, buf: &mut Vec<u8>) -> Result<usize, PeerError> {
        let mut req = String::with_capacity(160 + path.len() * 3);
        req.push_str("GET /api/peer/read?path=");
        req.push_str(&query_escape(path));
        req.push_str(extra);
        req.push_str(" HTTP/1.1\r\nHost: ");
        req.push_str(addr);
        req.push_str("\r\nUser-Agent: fusedata/0.1\r\nAccept-Encoding: identity\r\n");
        if let Some(k) = &self.cfg.api_key {
            req.push_str("X-API-Key: ");
            req.push_str(k);
            req.push_str("\r\n");
        }
        req.push_str("\r\n");
        if let Err(e) = s.write_all(req.as_bytes()) {
            return Err(PeerError::Other(format!("stale: write: {e}")));
        }

        let mut head = [0u8; 8192];
        let mut have = 0usize;
        let h = loop {
            let n = match s.read(&mut head[have..]) {
                Ok(0) if have == 0 => return Err(PeerError::Other("stale: closed before response".into())),
                Ok(0) => return Err(PeerError::Other("truncated response head".into())),
                Ok(n) => n,
                Err(e) if have == 0 && is_reset(&e) => return Err(PeerError::Other(format!("stale: {e}"))),
                Err(e) => return Err(PeerError::Other(format!("read head: {e}"))),
            };
            have += n;
            if let Some(h) = parse_head(&head[..have])? {
                break h;
            }
            if have == head.len() {
                return Err(PeerError::Other("response head too large".into()));
            }
        };
        let extra = &head[h.head_len..have];

        if h.status != 200 {
            // Drain a small fixed-length error body to keep the connection;
            // anything else (chunked, unknown length) just closes it.
            let mut reusable = !h.close && !h.chunked;
            if let (true, Some(cl)) = (reusable, h.content_length) {
                if cl <= 64 * 1024 {
                    let mut left = cl as usize - extra.len().min(cl as usize);
                    let mut sink = [0u8; 4096];
                    while left > 0 {
                        match s.read(&mut sink[..left.min(4096)]) {
                            Ok(0) | Err(_) => {
                                reusable = false;
                                break;
                            }
                            Ok(n) => left -= n,
                        }
                    }
                } else {
                    reusable = false;
                }
            } else {
                reusable = false;
            }
            if reusable {
                self.checkin(addr, s);
            }
            return Err(match h.status {
                503 => PeerError::Busy,
                404 => PeerError::Miss,
                code => PeerError::Other(format!("peer raw read {addr}: status {code}")),
            });
        }

        if h.chunked {
            return Err(PeerError::Other("chunked transfer-encoding not supported on the bulk path".into()));
        }
        match h.content_length {
            Some(cl) => {
                let cl = cl as usize;
                if buf.len() < cl {
                    buf.resize(cl, 0);
                }
                let pre = extra.len().min(cl);
                buf[..pre].copy_from_slice(&extra[..pre]);
                read_exact_at(&mut s, &mut buf[pre..cl]).map_err(|e| PeerError::Other(format!("read body: {e}")))?;
                if !h.close {
                    self.checkin(addr, s);
                }
                Ok(cl)
            }
            None => {
                // Body delimited by close.
                let mut n = extra.len();
                if buf.len() < n {
                    buf.resize(n, 0);
                }
                buf[..n].copy_from_slice(extra);
                loop {
                    if buf.len() == n {
                        buf.resize((n * 2).max(1 << 20), 0);
                    }
                    match s.read(&mut buf[n..]) {
                        Ok(0) => break,
                        Ok(k) => n += k,
                        Err(e) => return Err(PeerError::Other(format!("read body: {e}"))),
                    }
                }
                Ok(n)
            }
        }
    }
}

fn is_reset(e: &io::Error) -> bool {
    matches!(e.kind(), io::ErrorKind::ConnectionReset | io::ErrorKind::ConnectionAborted | io::ErrorKind::BrokenPipe)
}

fn read_exact_at(s: &mut TcpStream, mut dst: &mut [u8]) -> io::Result<()> {
    while !dst.is_empty() {
        match s.read(dst) {
            Ok(0) => return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "short body")),
            Ok(n) => dst = &mut dst[n..],
            Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
            Err(e) => return Err(e),
        }
    }
    Ok(())
}

fn set_rcvbuf(s: &TcpStream, bytes: i32) {
    use std::os::fd::AsRawFd;
    unsafe {
        libc::setsockopt(
            s.as_raw_fd(),
            libc::SOL_SOCKET,
            libc::SO_RCVBUF,
            &bytes as *const i32 as *const libc::c_void,
            std::mem::size_of::<i32>() as libc::socklen_t,
        );
    }
}

// ---------------------------------------------------------------------------
// Server

#[derive(Clone, Debug)]
pub struct PeerServerConfig {
    pub api_key: Option<String>,
    /// Go `-peer-serve-max-inflight` (default 64).
    pub max_inflight: usize,
}

impl Default for PeerServerConfig {
    fn default() -> Self {
        PeerServerConfig { api_key: None, max_inflight: 64 }
    }
}

#[derive(Default, Debug)]
pub struct PeerServerStats {
    pub served: AtomicU64,
    pub bytes: AtomicU64,
    pub busy: AtomicU64,
    pub miss: AtomicU64,
}

pub struct PeerServer {
    cache: Arc<CacheDir>,
    blob: Option<Arc<crate::blob::BlobServer>>,
    cfg: PeerServerConfig,
    inflight: AtomicUsize,
    pub stats: PeerServerStats,
}

struct Gate<'a>(&'a AtomicUsize);

impl Drop for Gate<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::AcqRel);
    }
}

impl PeerServer {
    pub fn new(cache: Arc<CacheDir>, cfg: PeerServerConfig) -> Arc<Self> {
        Arc::new(PeerServer { cache, blob: None, cfg, inflight: AtomicUsize::new(0), stats: PeerServerStats::default() })
    }

    /// Also serve the OCI blob routes (`/v2/<repo>/blobs/<digest>`,
    /// `/oci/blobs/<digest>`, `/v2/`).
    pub fn with_blobs(cache: Arc<CacheDir>, cfg: PeerServerConfig, blob: Arc<crate::blob::BlobServer>) -> Arc<Self> {
        Arc::new(PeerServer { cache, blob: Some(blob), cfg, inflight: AtomicUsize::new(0), stats: PeerServerStats::default() })
    }

    /// Accept loop: one thread per connection (connections are long-lived
    /// keep-alive streams from a bounded set of peers).
    pub fn serve(self: &Arc<Self>, listener: TcpListener) -> io::Result<()> {
        for conn in listener.incoming() {
            let conn = match conn {
                Ok(c) => c,
                Err(_) => continue,
            };
            let me = self.clone();
            std::thread::Builder::new().name("peer-conn".into()).spawn(move || {
                let _ = me.handle_conn(conn);
            })?;
        }
        Ok(())
    }

    fn try_admit(&self) -> Option<Gate<'_>> {
        let prev = self.inflight.fetch_add(1, Ordering::AcqRel);
        if prev >= self.cfg.max_inflight {
            self.inflight.fetch_sub(1, Ordering::AcqRel);
            return None;
        }
        Some(Gate(&self.inflight))
    }

    pub fn handle_conn(&self, mut s: TcpStream) -> io::Result<()> {
        let mut range: Option<String>;
        s.set_nodelay(true).ok();
        let mut buf = vec![0u8; 16 * 1024];
        let mut have = 0usize;
        loop {
            // Read one request head.
            range = None;
            let (method, target, api_key, close, head_len) = loop {
                if have > 0 {
                    let mut hdrs = [httparse::EMPTY_HEADER; 64];
                    let mut req = httparse::Request::new(&mut hdrs);
                    match req.parse(&buf[..have]) {
                        Ok(httparse::Status::Complete(n)) => {
                            let mut key = None;
                            let mut close = req.version == Some(0);
                            let mut body_len = 0usize;
                            for h in req.headers.iter() {
                                if h.name.eq_ignore_ascii_case("x-api-key") {
                                    key = Some(String::from_utf8_lossy(h.value).into_owned());
                                } else if h.name.eq_ignore_ascii_case("connection") {
                                    close = h.value.eq_ignore_ascii_case(b"close");
                                } else if h.name.eq_ignore_ascii_case("range") {
                                    range = Some(String::from_utf8_lossy(h.value).into_owned());
                                } else if h.name.eq_ignore_ascii_case("content-length") {
                                    body_len = std::str::from_utf8(h.value).ok().and_then(|v| v.trim().parse().ok()).unwrap_or(0);
                                }
                            }
                            if body_len > 0 {
                                // GET with a body is unusual; a body that is
                                // not fully buffered is not worth handling.
                                if n + body_len > have {
                                    return Ok(());
                                }
                            }
                            break (req.method.unwrap_or("").to_string(), req.path.unwrap_or("").to_string(), key, close, n + body_len);
                        }
                        Ok(httparse::Status::Partial) => {}
                        Err(_) => {
                            let _ = s.write_all(b"HTTP/1.1 400 Bad Request\r\nContent-Length: 0\r\nConnection: close\r\n\r\n");
                            return Ok(());
                        }
                    }
                }
                if have == buf.len() {
                    return Ok(());
                }
                let n = s.read(&mut buf[have..])?;
                if n == 0 {
                    return Ok(());
                }
                have += n;
            };
            buf.copy_within(head_len..have, 0);
            have -= head_len;

            self.respond(&mut s, &method, &target, api_key.as_deref(), range.as_deref())?;
            if close {
                let _ = s.shutdown(Shutdown::Write);
                return Ok(());
            }
        }
    }

    fn respond(&self, s: &mut TcpStream, method: &str, target: &str, api_key: Option<&str>, range: Option<&str>) -> io::Result<()> {
        let (path, query) = target.split_once('?').unwrap_or((target, ""));
        if path == "/api/health" {
            return write_simple(s, 200, "OK", "ok\n", &[]);
        }
        // OCI blob routes are unauthenticated in Go too (isOCIBlobRoute).
        if path == "/v2/" && (method == "GET" || method == "HEAD") {
            return write_simple(s, 200, "OK", "", &[("Docker-Distribution-API-Version", "registry/2.0")]);
        }
        if let Some(digest) = blob_route_digest(path) {
            if method != "GET" && method != "HEAD" {
                return write_simple(s, 405, "Method Not Allowed", "Method Not Allowed\n", &[]);
            }
            let missing = if path.starts_with("/v2/") { 503 } else { 404 };
            return match self.blob.as_ref() {
                Some(b) => b.serve(s, method == "HEAD", digest, range, missing),
                None => write_simple(s, 404, "Not Found", "404 page not found\n", &[]),
            };
        }
        if let Some(k) = &self.cfg.api_key {
            if api_key != Some(k.as_str()) {
                return write_simple(s, 401, "Unauthorized", "Unauthorized\n", &[]);
            }
        }
        if path != "/api/peer/read" {
            return write_simple(s, 404, "Not Found", "404 page not found\n", &[]);
        }
        if method != "GET" {
            return write_simple(s, 405, "Method Not Allowed", "Method Not Allowed\n", &[]);
        }
        let _gate = match self.try_admit() {
            Some(g) => g,
            None => {
                self.stats.busy.fetch_add(1, Ordering::Relaxed);
                return write_simple(s, 503, "Service Unavailable", "peer serve capacity exhausted\n", &[("Retry-After", "1")]);
            }
        };
        let raw = match query_get(query, "path") {
            Some(p) if !p.is_empty() => p,
            _ => return write_simple(s, 400, "Bad Request", "path query parameter required\n", &[]),
        };
        let clean = clean_path(&raw);
        // Optional sub-range of the object (Go peerReadSubRange).
        let sub = match (query_get(query, "off"), query_get(query, "len")) {
            (None, None) => None,
            (o, l) => match (o.and_then(|v| v.parse::<i64>().ok()), l.and_then(|v| v.parse::<i64>().ok())) {
                (Some(o), Some(l)) if o >= 0 && l > 0 => Some((o as u64, l as u64)),
                _ => return write_simple(s, 400, "Bad Request", "off and len must be a byte offset and a positive length\n", &[]),
            },
        };
        match self.cache.peer_serve_target(&clean) {
            Some((file, off, len)) => {
                let (off, len) = match sub {
                    None => (off, len),
                    Some((so, _)) if so >= len => return write_simple(s, 416, "Requested Range Not Satisfiable", "range outside object\n", &[]),
                    Some((so, sl)) => (off + so, sl.min(len - so)),
                };
                self.stats.served.fetch_add(1, Ordering::Relaxed);
                self.stats.bytes.fetch_add(len, Ordering::Relaxed);
                send_file_response(s, &file, off, len)
            }
            None => {
                self.stats.miss.fetch_add(1, Ordering::Relaxed);
                write_simple(s, 404, "Not Found", "not found\n", &[])
            }
        }
    }
}

/// The digest segment of `/oci/blobs/{digest}` or `/v2/<repo>/blobs/{digest}`.
fn blob_route_digest(path: &str) -> Option<&str> {
    if let Some(d) = path.strip_prefix("/oci/blobs/") {
        return (!d.is_empty() && !d.contains('/')).then_some(d);
    }
    let rest = path.strip_prefix("/v2/")?;
    let i = rest.rfind("/blobs/")?;
    let d = &rest[i + "/blobs/".len()..];
    (i > 0 && !d.is_empty() && !d.contains('/')).then_some(d)
}

pub(crate) fn write_simple(s: &mut TcpStream, code: u16, reason: &str, body: &str, extra: &[(&str, &str)]) -> io::Result<()> {
    let mut out = format!(
        "HTTP/1.1 {code} {reason}\r\nContent-Type: text/plain; charset=utf-8\r\nX-Content-Type-Options: nosniff\r\nDate: {}\r\nContent-Length: {}\r\n",
        httpdate::fmt_http_date(std::time::SystemTime::now()),
        body.len()
    );
    for (k, v) in extra {
        out.push_str(k);
        out.push_str(": ");
        out.push_str(v);
        out.push_str("\r\n");
    }
    out.push_str("\r\n");
    out.push_str(body);
    s.write_all(out.as_bytes())
}

fn send_file_response(s: &mut TcpStream, f: &File, off: u64, len: u64) -> io::Result<()> {
    let head = format!(
        "HTTP/1.1 200 OK\r\nContent-Type: application/octet-stream\r\nDate: {}\r\nContent-Length: {len}\r\n\r\n",
        httpdate::fmt_http_date(std::time::SystemTime::now())
    );
    send_more(s, head.as_bytes())?;
    sendfile_all(s, f, off, len)
}

#[cfg(target_os = "linux")]
pub(crate) fn send_more(s: &mut TcpStream, data: &[u8]) -> io::Result<()> {
    use std::os::fd::AsRawFd;
    let mut d = data;
    while !d.is_empty() {
        let n = unsafe { libc::send(s.as_raw_fd(), d.as_ptr() as *const libc::c_void, d.len(), libc::MSG_MORE) };
        if n < 0 {
            let e = io::Error::last_os_error();
            if e.kind() == io::ErrorKind::Interrupted {
                continue;
            }
            return Err(e);
        }
        d = &d[n as usize..];
    }
    Ok(())
}

#[cfg(not(target_os = "linux"))]
pub(crate) fn send_more(s: &mut TcpStream, data: &[u8]) -> io::Result<()> {
    s.write_all(data)
}

#[cfg(target_os = "linux")]
pub(crate) fn sendfile_all(s: &mut TcpStream, f: &File, off: u64, len: u64) -> io::Result<()> {
    use std::os::fd::AsRawFd;
    let mut o = off as libc::off_t;
    let end = (off + len) as libc::off_t;
    while o < end {
        let want = ((end - o) as usize).min(1 << 30);
        let n = unsafe { libc::sendfile(s.as_raw_fd(), f.as_raw_fd(), &mut o, want) };
        if n < 0 {
            let e = io::Error::last_os_error();
            if e.kind() == io::ErrorKind::Interrupted {
                continue;
            }
            return Err(e);
        }
        if n == 0 {
            return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "file shrank during sendfile"));
        }
    }
    Ok(())
}

#[cfg(not(target_os = "linux"))]
pub(crate) fn sendfile_all(s: &mut TcpStream, f: &File, off: u64, len: u64) -> io::Result<()> {
    use std::os::unix::fs::FileExt;
    let mut buf = vec![0u8; 1 << 20];
    let mut o = off;
    let end = off + len;
    while o < end {
        let want = ((end - o) as usize).min(buf.len());
        let n = f.read_at(&mut buf[..want], o)?;
        if n == 0 {
            return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "file shrank"));
        }
        s.write_all(&buf[..n])?;
        o += n as u64;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn blob_routes() {
        let d = "sha256:".to_string() + &"a".repeat(64);
        assert_eq!(blob_route_digest(&format!("/oci/blobs/{d}")), Some(d.as_str()));
        assert_eq!(blob_route_digest(&format!("/v2/library/x/blobs/{d}")), Some(d.as_str()));
        assert_eq!(blob_route_digest(&format!("/v2/blobs/{d}")), None);
        assert_eq!(blob_route_digest("/oci/blobs/"), None);
        assert_eq!(blob_route_digest("/api/peer/read"), None);
    }

    #[test]
    fn escape_matches_go_query_escape() {
        // Go: url.QueryEscape("/a b/c+d_chunk_1") = "%2Fa+b%2Fc%2Bd_chunk_1";
        // we emit %20 for space, which Go's ParseQuery decodes the same.
        assert_eq!(query_escape("/a b/c+d_chunk_1"), "%2Fa%20b%2Fc%2Bd_chunk_1");
        assert_eq!(query_escape("az-AZ09_.~"), "az-AZ09_.~");
        assert_eq!(query_get("path=%2Fa+b%2Fc%2Bd&x=1", "path").as_deref(), Some("/a b/c+d"));
        assert_eq!(query_get("x=1&path=", "path").as_deref(), Some(""));
        assert_eq!(query_get("x=1", "path"), None);
    }
}
