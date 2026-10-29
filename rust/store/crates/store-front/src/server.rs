//! store-front: owns the store node's public HTTP port, serves the hot read
//! routes from the cache dir, and forwards everything else to the Go client
//! on an internal port.
//!
//! Native routes (served from the local cache dir when the bytes are there):
//! * `GET /api/peer/read?path=<p>[&off=&len=]` — Go's `handlePeerRead`;
//! * `GET|HEAD /v2/<repo>/blobs/<digest>`, `GET|HEAD /oci/blobs/<digest>` —
//!   Go's `serveOCIBlob`, for a blob held whole at `/oci/sha256/<hex>`;
//! * `GET|HEAD /v2/` — Go's registry ping;
//! * `PUT /api/files/<path>` — streamed to a staged file beside the final
//!   path and hashed here; Go adopts it (`POST /api/internal/commit`), so Go
//!   still owns the index, coordinator rows and cloud copy (`write.rs`).
//!
//! Anything native that cannot be answered from an exact local file is
//! forwarded unchanged to Go (which knows its index, peers and cloud), as is
//! every other route. Error answers (400, 404, 416, 502, 503-miss) are
//! always Go's: a request that would produce one is forwarded, so status,
//! headers and body are Go's own, byte for byte. Own routes: `/rust/healthz` (liveness), `/rust/readyz`
//! (Go healthy and cache dir readable), `/rust/metrics`.
//!
//! Admission: native serves take a slot from `max_inflight` (Go's
//! `peerServeGate`); a full gate answers peer reads 503 + `Retry-After: 1`,
//! exactly as Go does. Blob routes are not gated in Go and are not here.
//!
//! Auth: the Go rule. With an API key set, `X-API-Key` must match on
//! everything except `/api/health`, `/metrics`, `/api/netprobe`, `/rust/*`
//! and GET/HEAD on the registry/blob routes. Forwarded requests carry the
//! header unchanged, so Go checks it again.

use crate::flog;
use crate::http::{self, Conn, RequestHead};
use crate::metrics::{Metrics, Outcome, Route};
use crate::write;
use std::collections::HashMap;
use std::io::{self, Write};
use std::net::{Shutdown, TcpListener, TcpStream};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use store_layout as layout;

#[derive(Clone, Debug)]
pub struct Config {
    pub cache_dir: PathBuf,
    /// Go client, e.g. `127.0.0.1:8091`.
    pub backend: String,
    /// Coordinator HTTP address for published chunk strides (`_chunk_N` of a
    /// whole file). Without it those requests are forwarded to Go.
    pub coordinator: Option<String>,
    pub api_key: Option<String>,
    pub max_inflight: usize,
    /// Idle keep-alive connections are closed after this long.
    pub idle_timeout: Duration,
    pub backend_timeout: Duration,
    /// How long published-stride and blob-size lookups are cached.
    pub stride_ttl: Duration,
    /// Handle `PUT /api/files/<path>` natively (stage + Go commit). Off:
    /// every PUT is forwarded to Go.
    pub native_put: bool,
    /// Go's `-max-upload-gb`, in bytes: larger PUTs go to Go for its 413.
    pub max_upload: u64,
}

impl Config {
    pub fn new(cache_dir: PathBuf, backend: String) -> Self {
        Config {
            cache_dir,
            backend,
            coordinator: None,
            api_key: None,
            max_inflight: 64,
            idle_timeout: Duration::from_secs(120),
            backend_timeout: Duration::from_secs(300),
            stride_ttl: Duration::from_secs(30),
            native_put: true,
            max_upload: 2 << 30,
        }
    }
}

pub struct Server {
    pub cfg: Config,
    pub metrics: Metrics,
    native: AtomicUsize,
    stopping: AtomicBool,
    strides: Mutex<HashMap<String, (Instant, Option<layout::Published>)>>,
    blob_sizes: Mutex<HashMap<String, (Instant, u64)>>,
}

/// What the router decided for a request.
#[derive(Debug, PartialEq, Eq)]
pub enum Plan {
    PeerRead,
    Blob { digest: String },
    RegistryPing,
    Healthz,
    Readyz,
    Metrics,
    Put { path: String },
    Proxy,
}

/// Route a request (method + path), before auth.
pub fn route(method: &str, path: &str) -> Plan {
    let get_head = method == "GET" || method == "HEAD";
    match path {
        "/rust/healthz" => return Plan::Healthz,
        "/rust/readyz" => return Plan::Readyz,
        "/rust/metrics" => return Plan::Metrics,
        "/api/peer/read" if method == "GET" => return Plan::PeerRead,
        "/v2/" if get_head => return Plan::RegistryPing,
        _ => {}
    }
    if method == "PUT" {
        if let Some(p) = path.strip_prefix("/api/files/") {
            // `/api/files/<p>/size` is GET-only in Go; a PUT to it is a file
            // named "size" under <p>, as Go's router resolves it.
            return Plan::Put { path: p.to_string() };
        }
    }
    if get_head {
        if let Some(d) = path.strip_prefix("/oci/blobs/") {
            if !d.is_empty() && !d.contains('/') {
                return Plan::Blob { digest: d.to_string() };
            }
        }
        if let Some(rest) = path.strip_prefix("/v2/") {
            if let Some(i) = rest.rfind("/blobs/") {
                let d = &rest[i + "/blobs/".len()..];
                if i > 0 && !d.is_empty() && !d.contains('/') {
                    return Plan::Blob { digest: d.to_string() };
                }
            }
        }
    }
    Plan::Proxy
}

/// Go's `authMiddleware` exemptions (plus this server's own `/rust/*`).
pub fn auth_exempt(method: &str, path: &str) -> bool {
    if matches!(path, "/api/health" | "/metrics" | "/api/netprobe") || path.starts_with("/rust/") {
        return true;
    }
    // isOCIBlobRoute
    (method == "GET" || method == "HEAD") && (path == "/v2/" || path.starts_with("/oci/blobs/") || (path.starts_with("/v2/") && path.contains("/blobs/")))
}

struct Slot<'a>(&'a Server);

impl Drop for Slot<'_> {
    fn drop(&mut self) {
        self.0.native.fetch_sub(1, Ordering::AcqRel);
        self.0.metrics.native_inflight.fetch_sub(1, Ordering::Relaxed);
    }
}

/// A local answer, prepared (file opened, size checked) before any byte is
/// written, so anything else can still be forwarded.
struct Local {
    status: u16,
    file: std::fs::File,
    off: u64,
    len: u64,
    extra: Vec<(&'static str, String)>,
    head_only: bool,
}

impl Server {
    pub fn new(cfg: Config) -> Arc<Self> {
        Arc::new(Server {
            cfg,
            metrics: Metrics::default(),
            native: AtomicUsize::new(0),
            stopping: AtomicBool::new(false),
            strides: Mutex::new(HashMap::new()),
            blob_sizes: Mutex::new(HashMap::new()),
        })
    }

    pub fn stop(&self) {
        self.stopping.store(true, Ordering::SeqCst);
    }

    pub fn is_stopping(&self) -> bool {
        self.stopping.load(Ordering::SeqCst)
    }

    fn try_admit(&self) -> Option<Slot<'_>> {
        let prev = self.native.fetch_add(1, Ordering::AcqRel);
        if prev >= self.cfg.max_inflight {
            self.native.fetch_sub(1, Ordering::AcqRel);
            return None;
        }
        self.metrics.native_inflight.fetch_add(1, Ordering::Relaxed);
        Some(Slot(self))
    }

    /// Accept until `stop()`; each connection on its own thread. Returns
    /// once the listener is closed and in-flight requests finished (or
    /// `drain` elapsed).
    pub fn serve(self: &Arc<Self>, listener: TcpListener, drain: Duration) -> io::Result<()> {
        listener.set_nonblocking(true)?;
        while !self.is_stopping() {
            match listener.accept() {
                Ok((s, _)) => {
                    s.set_nonblocking(false).ok();
                    let me = self.clone();
                    std::thread::Builder::new().name("front-conn".into()).spawn(move || {
                        me.metrics.connections.fetch_add(1, Ordering::Relaxed);
                        if let Err(e) = me.handle_conn(s) {
                            if !matches!(
                                e.kind(),
                                io::ErrorKind::BrokenPipe
                                    | io::ErrorKind::ConnectionReset
                                    | io::ErrorKind::UnexpectedEof
                                    | io::ErrorKind::WouldBlock
                                    | io::ErrorKind::TimedOut
                            ) {
                                flog!("connection error: {e}");
                            }
                        }
                        me.metrics.connections.fetch_sub(1, Ordering::Relaxed);
                    })?;
                }
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => std::thread::sleep(Duration::from_millis(20)),
                Err(e) => {
                    flog!("accept: {e}");
                    std::thread::sleep(Duration::from_millis(50));
                }
            }
        }
        drop(listener);
        let t = Instant::now();
        while self.metrics.inflight.load(Ordering::SeqCst) > 0 && t.elapsed() < drain {
            std::thread::sleep(Duration::from_millis(50));
        }
        let left = self.metrics.inflight.load(Ordering::SeqCst);
        if left > 0 {
            flog!("shutdown: {left} request(s) still in flight after {drain:?}");
        }
        Ok(())
    }

    pub fn handle_conn(&self, s: TcpStream) -> io::Result<()> {
        s.set_nodelay(true).ok();
        s.set_read_timeout(Some(self.cfg.idle_timeout)).ok();
        let mut c = Conn::new(s);
        let mut backend: Option<Conn> = None;
        loop {
            let Some(n) = c.read_head()? else { return Ok(()) };
            let head = c.take(n);
            let req = match http::parse_request(&head) {
                Ok(r) => r,
                Err(_) => {
                    http::write_error(&mut c.stream, 400, "Bad Request", &[], true)?;
                    return Ok(());
                }
            };
            self.metrics.inflight.fetch_add(1, Ordering::SeqCst);
            let r = self.handle_request(&mut c, &head, &req, &mut backend);
            self.metrics.inflight.fetch_sub(1, Ordering::SeqCst);
            let keep = r?;
            // After a stop, finish the current request but take no more on
            // this connection.
            if !keep || req.conn_close || self.is_stopping() {
                let _ = c.stream.flush();
                let _ = c.stream.shutdown(Shutdown::Write);
                return Ok(());
            }
        }
    }

    /// Handle one request. `Ok(true)` = the connection can carry another.
    fn handle_request(&self, c: &mut Conn, head: &[u8], req: &RequestHead, backend: &mut Option<Conn>) -> io::Result<bool> {
        let t0 = Instant::now();
        let path = req.path().to_string();
        let plan = route(&req.method, &path);

        // Without the key, nothing is served natively: the request goes to
        // Go, whose 401 (or whatever its rule says) is relayed verbatim.
        let authorized = match &self.cfg.api_key {
            Some(key) => auth_exempt(&req.method, &path) || req.api_key.as_deref() == Some(key.as_str()),
            None => true,
        };
        let plan = if authorized { plan } else { Plan::Proxy };

        match plan {
            Plan::Healthz => {
                self.write_small(c, 200, "text/plain; charset=utf-8", b"ok\n")?;
                self.metrics.record(Route::Own, Outcome::Local, 3, t0.elapsed());
                Ok(true)
            }
            Plan::Readyz => {
                let (ok, why) = self.ready();
                let body = format!("{why}\n");
                self.write_small(c, if ok { 200 } else { 503 }, "text/plain; charset=utf-8", body.as_bytes())?;
                self.metrics.record(Route::Own, Outcome::Local, body.len() as u64, t0.elapsed());
                Ok(true)
            }
            Plan::Metrics => {
                let body = self.metrics.render();
                self.write_small(c, 200, "text/plain; version=0.0.4; charset=utf-8", body.as_bytes())?;
                self.metrics.record(Route::Own, Outcome::Local, body.len() as u64, t0.elapsed());
                Ok(true)
            }
            Plan::RegistryPing => {
                // Go: 200, Docker-Distribution-API-Version, empty body.
                let h = format!("HTTP/1.1 200 OK\r\nDocker-Distribution-API-Version: registry/2.0\r\nDate: {}\r\nContent-Length: 0\r\n\r\n", http::http_date());
                c.stream.write_all(h.as_bytes())?;
                self.metrics.record(Route::RegistryPing, Outcome::Local, 0, t0.elapsed());
                Ok(true)
            }
            Plan::PeerRead => self.native(c, head, req, backend, Route::PeerRead, t0, |me| me.plan_peer_read(req)),
            Plan::Blob { ref digest } => {
                let digest = digest.clone();
                self.native(c, head, req, backend, Route::Blob, t0, move |me| me.plan_blob(req, &digest))
            }
            Plan::Put { ref path } => self.put(c, head, req, backend, path, t0),
            Plan::Proxy => {
                let (keep, n, ok) = self.forward(c, head, req, backend)?;
                self.metrics.record(Route::Proxy, if ok { Outcome::Forwarded } else { Outcome::Error }, n, t0.elapsed());
                Ok(keep)
            }
        }
    }

    /// Native route: plan locally; answer locally or forward.
    #[allow(clippy::too_many_arguments)]
    fn native<F>(&self, c: &mut Conn, head: &[u8], req: &RequestHead, backend: &mut Option<Conn>, route: Route, t0: Instant, plan: F) -> io::Result<bool>
    where
        F: FnOnce(&Self) -> Option<Local>,
    {
        // A body on a GET is not ours to interpret: let Go see it.
        if req.has_body() {
            let (keep, n, ok) = self.forward(c, head, req, backend)?;
            self.metrics.record(route, if ok { Outcome::Forwarded } else { Outcome::Error }, n, t0.elapsed());
            return Ok(keep);
        }
        let _slot = if route == Route::PeerRead {
            match self.try_admit() {
                Some(s) => Some(s),
                None => {
                    // Go: 503, Retry-After: 1, before parsing the path.
                    http::write_error(&mut c.stream, 503, "peer serve capacity exhausted", &[("Retry-After", "1")], false)?;
                    self.metrics.record(route, Outcome::Busy, 0, t0.elapsed());
                    return Ok(true);
                }
            }
        } else {
            None
        };
        match plan(self) {
            Some(Local { status, file, off, len, extra, head_only }) => {
                // Header order as Go's net/http writes them (sorted).
                let mut hs: Vec<(&str, String)> = extra;
                hs.push(("Content-Length", len.to_string()));
                hs.push(("Date", http::http_date()));
                hs.sort_by(|a, b| a.0.cmp(b.0));
                let mut h = format!("HTTP/1.1 {status} {}\r\n", http::reason(status));
                for (k, v) in &hs {
                    h.push_str(&format!("{k}: {v}\r\n"));
                }
                h.push_str("\r\n");
                if head_only {
                    c.stream.write_all(h.as_bytes())?;
                    self.metrics.record(route, Outcome::Local, 0, t0.elapsed());
                    return Ok(true);
                }
                http::send_more(&mut c.stream, h.as_bytes())?;
                let r = http::sendfile_all(&mut c.stream, &file, off, len);
                match &r {
                    Ok(()) => self.metrics.record(route, Outcome::Local, len, t0.elapsed()),
                    // A requester that raced several holders and cancelled
                    // this one: its hang-up is not a serve failure.
                    Err((e, sent)) if client_gone(e) => self.metrics.record(route, Outcome::Aborted, *sent, t0.elapsed()),
                    Err((_, sent)) => self.metrics.record(route, Outcome::Error, *sent, t0.elapsed()),
                }
                r.map_err(|(e, _)| e)?;
                Ok(true)
            }
            None => {
                drop(_slot); // Go applies its own gate
                let (keep, n, ok) = self.forward(c, head, req, backend)?;
                self.metrics.record(route, if ok { Outcome::Forwarded } else { Outcome::Error }, n, t0.elapsed());
                Ok(keep)
            }
        }
    }

    /// Go's `handlePeerRead` from the cache dir. `None` = forward (a miss,
    /// an unconfirmed stride, or any request Go would answer with an error).
    fn plan_peer_read(&self, req: &RequestHead) -> Option<Local> {
        let q = req.query();
        let raw = http::query_get(q, "path").filter(|p| !p.is_empty())?;
        let sub = layout::parse_off_len(http::query_get(q, "off").as_deref(), http::query_get(q, "len").as_deref()).ok()?;
        let target = layout::peer_target(&self.cfg.cache_dir, &raw, |parent, size| self.published(parent, size)).ok()?;
        let (off, len) = match sub {
            None => (target.offset, target.length),
            Some((so, sl)) => layout::sub_range(target.offset, target.length, so, sl)?,
        };
        Some(Local { status: 200, file: target.file, off, len, extra: vec![("Content-Type", "application/octet-stream".into())], head_only: false })
    }

    /// Go's `serveOCIBlob` for a blob held whole at `/oci/sha256/<hex>`.
    /// `None` = forward.
    ///
    /// The blob's size comes from Go's own index (a `HEAD` to the backend,
    /// cached for `stride_ttl`), and the local file must hold exactly that
    /// many bytes (Go's `openLocalExact`): a short file still landing, an
    /// older copy, or a blob Go does not know is never served from here.
    /// A Range Go would refuse (416) is forwarded so the refusal is Go's.
    fn plan_blob(&self, req: &RequestHead, digest: &str) -> Option<Local> {
        let hex = layout::digest_hex(digest)?;
        let size = self.blob_size(hex)?;
        let (file, _) = layout::blob_exact(&self.cfg.cache_dir, hex, size)?;
        let mut extra = vec![("Accept-Ranges", "bytes".to_string()), ("Content-Type", "application/octet-stream".to_string())];
        if req.method == "HEAD" {
            return Some(Local { status: 200, file, off: 0, len: size, extra, head_only: true });
        }
        let Some(range) = req.range.as_deref() else {
            // Whole-blob GET: 200 with every byte (Go writeWholeBlob).
            return Some(Local { status: 200, file, off: 0, len: size, extra, head_only: false });
        };
        let (start, mut end) = layout::parse_byte_range(range, size).ok()?;
        if end - start + 1 > layout::OCI_MAX_RANGE {
            end = start + layout::OCI_MAX_RANGE - 1;
        }
        extra.push(("Content-Range", format!("bytes {start}-{end}/{size}")));
        Some(Local { status: 206, file, off: start, len: end - start + 1, extra, head_only: false })
    }

    /// The blob's size as Go's index reports it (`HEAD /oci/blobs/<hex>` on
    /// the backend), cached. `None` when Go does not hold it.
    fn blob_size(&self, hex: &str) -> Option<u64> {
        {
            let m = self.blob_sizes.lock().unwrap();
            if let Some((t, n)) = m.get(hex) {
                if t.elapsed() < self.cfg.stride_ttl {
                    return Some(*n);
                }
            }
        }
        let n = http::head_len(&self.cfg.backend, &format!("/oci/blobs/{hex}"), Duration::from_secs(5)).ok()??;
        let mut m = self.blob_sizes.lock().unwrap();
        if m.len() > 65536 {
            m.clear();
        }
        m.insert(hex.to_string(), (Instant::now(), n));
        Some(n)
    }

    /// Published shape of `path` from the coordinator (cached), when its
    /// size equals `local_size`.
    fn published(&self, path: &str, local_size: i64) -> Option<layout::Published> {
        let coord = self.cfg.coordinator.as_ref()?;
        {
            let m = self.strides.lock().unwrap();
            if let Some((t, v)) = m.get(path) {
                if t.elapsed() < self.cfg.stride_ttl {
                    return v.filter(|p| p.size == local_size);
                }
            }
        }
        let v = fetch_published(coord, path);
        let mut m = self.strides.lock().unwrap();
        if m.len() > 65536 {
            m.clear();
        }
        m.insert(path.to_string(), (Instant::now(), v));
        v.filter(|p| p.size == local_size)
    }

    fn ready(&self) -> (bool, String) {
        if std::fs::read_dir(&self.cfg.cache_dir).is_err() {
            return (false, format!("cache dir {} not readable", self.cfg.cache_dir.display()));
        }
        match http::get(&self.cfg.backend, "/api/health", &[], Duration::from_secs(2), 64 * 1024) {
            Ok((200, _)) => (true, "ok".into()),
            Ok((code, _)) => (false, format!("backend /api/health returned {code}")),
            Err(e) => (false, format!("backend unreachable: {e}")),
        }
    }

    /// `PUT /api/files/<path>`: native when exactly framed and under the
    /// cap, else Go's.
    fn put(&self, c: &mut Conn, head: &[u8], req: &RequestHead, backend: &mut Option<Conn>, raw: &str, t0: Instant) -> io::Result<bool> {
        let len = if self.cfg.native_put { write::native_put(req, self.cfg.max_upload) } else { None };
        let staged = len.and_then(|_| write::staged_for(&self.cfg.cache_dir, raw));
        let (Some(len), Some(st)) = (len, staged) else {
            let (keep, n, ok) = self.forward(c, head, req, backend)?;
            self.metrics.record(Route::Put, if ok { Outcome::Forwarded } else { Outcome::Error }, n, t0.elapsed());
            return Ok(keep);
        };
        match write::put(c, &st, len, &self.cfg.backend, self.cfg.api_key.as_deref(), self.cfg.backend_timeout) {
            Ok(n) => {
                // Go's own success answer: 200, empty body.
                let h = format!("HTTP/1.1 200 OK\r\nDate: {}\r\nContent-Length: 0\r\n\r\n", http::http_date());
                c.stream.write_all(h.as_bytes())?;
                self.metrics.record(Route::Put, Outcome::Local, n, t0.elapsed());
                flog!("stored {} ({n} bytes)", st.file_path);
                Ok(true)
            }
            Err(write::PutError::Client(e)) => {
                // Body never fully arrived: no answer, connection done.
                self.metrics.record(Route::Put, Outcome::Aborted, 0, t0.elapsed());
                flog!("PUT {}: client body: {e}", st.file_path);
                Ok(false)
            }
            Err(write::PutError::DigestMismatch(got)) => {
                flog!("rejected OCI blob {}: content hashes to {got}", st.file_path);
                http::write_error(&mut c.stream, 400, "content does not match the blob digest", &[], false)?;
                self.metrics.record(Route::Put, Outcome::Rejected, 0, t0.elapsed());
                Ok(true)
            }
            Err(write::PutError::Stage(e)) => {
                flog!("PUT {}: staging: {e}", st.file_path);
                http::write_error(&mut c.stream, 500, "Failed to create staging file", &[], true)?;
                self.metrics.record(Route::Put, Outcome::Error, 0, t0.elapsed());
                Ok(false)
            }
            Err(write::PutError::Commit(code, msg)) => {
                flog!("PUT {}: commit {code}: {msg}", st.file_path);
                if code == 502 {
                    self.metrics.backend_errors.fetch_add(1, Ordering::Relaxed);
                }
                let msg = if msg.is_empty() { "Failed to store file".to_string() } else { msg };
                http::write_error(&mut c.stream, code, &msg, &[], false)?;
                self.metrics.record(Route::Put, if code >= 500 { Outcome::Error } else { Outcome::Rejected }, 0, t0.elapsed());
                Ok(true)
            }
        }
    }

    fn write_small(&self, c: &mut Conn, status: u16, ctype: &str, body: &[u8]) -> io::Result<()> {
        let h = format!(
            "HTTP/1.1 {status} {}\r\nContent-Type: {ctype}\r\nDate: {}\r\nContent-Length: {}\r\n\r\n",
            http::reason(status),
            http::http_date(),
            body.len()
        );
        c.stream.write_all(h.as_bytes())?;
        c.stream.write_all(body)
    }

    /// Forward one request to the Go client and relay the response, bodies
    /// streamed both ways. Returns (client connection reusable, response
    /// body bytes, backend OK).
    fn forward(&self, c: &mut Conn, head: &[u8], req: &RequestHead, backend: &mut Option<Conn>) -> io::Result<(bool, u64, bool)> {
        // Reuse the per-connection backend socket; one fresh retry if a
        // pooled one turns out dead before any byte of the body was sent.
        let mut attempts = 0;
        loop {
            attempts += 1;
            let reused = backend.is_some();
            if backend.is_none() {
                match http::connect(&self.cfg.backend, Duration::from_secs(2)) {
                    Ok(s) => {
                        s.set_read_timeout(Some(self.cfg.backend_timeout)).ok();
                        s.set_write_timeout(Some(self.cfg.backend_timeout)).ok();
                        *backend = Some(Conn::new(s));
                    }
                    Err(e) => {
                        self.metrics.backend_errors.fetch_add(1, Ordering::Relaxed);
                        flog!("backend {} unreachable: {e}", self.cfg.backend);
                        let close = req.has_body();
                        http::write_error(&mut c.stream, 502, "backend unavailable", &[], close)?;
                        return Ok((!close, 0, false));
                    }
                }
            }
            let b = backend.as_mut().unwrap();
            if b.stream.write_all(head).is_err() {
                *backend = None;
                if reused && attempts == 1 {
                    continue;
                }
                self.metrics.backend_errors.fetch_add(1, Ordering::Relaxed);
                http::write_error(&mut c.stream, 502, "backend write failed", &[], true)?;
                return Ok((false, 0, false));
            }
            // Expect: 100-continue is end-to-end: Go answers it (or refuses
            // with 401/413 before reading the body), and its response head
            // is relayed below; the client then sends the body.
            if req.expect_continue && req.has_body() {
                let n = match b.read_head() {
                    Ok(Some(n)) => n,
                    _ => {
                        *backend = None;
                        if reused && attempts == 1 {
                            continue;
                        }
                        self.metrics.backend_errors.fetch_add(1, Ordering::Relaxed);
                        http::write_error(&mut c.stream, 502, "backend closed", &[], true)?;
                        return Ok((false, 0, false));
                    }
                };
                let rh = http::parse_response(&b.buffered()[..n]).map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "bad backend response"))?;
                if rh.status == 100 {
                    let interim = b.take(n);
                    c.stream.write_all(&interim)?;
                } else {
                    // Final answer without the body: relay it; the client's
                    // unsent body makes the connection unusable.
                    let (_, sent) = self.relay_response(c, b, req)?;
                    *backend = None;
                    return Ok((false, sent, true));
                }
            }
            // Request body, verbatim as framed.
            let body_res = if req.chunked {
                c.copy_chunked(&mut b.stream).map(|_| ())
            } else if let Some(n) = req.content_length {
                c.copy_exact(n, &mut b.stream)
            } else {
                Ok(())
            };
            if let Err(e) = body_res {
                // Go may have answered early (401, 413) and closed: relay
                // that answer if one arrived; the client's connection is
                // unusable either way (its body was not fully consumed).
                if let Ok((_, sent)) = self.relay_response(c, b, req) {
                    *backend = None;
                    return Ok((false, sent, true));
                }
                *backend = None;
                self.metrics.backend_errors.fetch_add(1, Ordering::Relaxed);
                flog!("forward body {} {}: {e}", req.method, req.path());
                return Ok((false, 0, false));
            }
            match self.relay_response(c, b, req) {
                Ok((keep_backend, sent)) => {
                    if !keep_backend {
                        *backend = None;
                    }
                    return Ok((true, sent, true));
                }
                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof && reused && attempts == 1 && !req.has_body() => {
                    // Pooled connection closed by Go between requests.
                    *backend = None;
                    continue;
                }
                Err(e) => {
                    *backend = None;
                    self.metrics.backend_errors.fetch_add(1, Ordering::Relaxed);
                    return Err(e);
                }
            }
        }
    }

    /// Relay one response (head verbatim, body as framed). Returns (backend
    /// connection reusable, body bytes).
    fn relay_response(&self, c: &mut Conn, b: &mut Conn, req: &RequestHead) -> io::Result<(bool, u64)> {
        loop {
            let n = b.read_head()?.ok_or_else(|| io::Error::new(io::ErrorKind::UnexpectedEof, "backend closed before response"))?;
            let head = b.take(n);
            let rh = http::parse_response(&head).map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "bad backend response"))?;
            c.stream.write_all(&head)?;
            if (100..200).contains(&rh.status) {
                continue; // interim response; the final one follows
            }
            let no_body = req.method == "HEAD" || rh.status == 204 || rh.status == 304;
            if no_body {
                return Ok((!rh.conn_close, 0));
            }
            if rh.chunked {
                let n = b.copy_chunked(&mut c.stream)?;
                return Ok((!rh.conn_close, n));
            }
            if let Some(n) = rh.content_length {
                b.copy_exact(n, &mut c.stream)?;
                return Ok((!rh.conn_close, n));
            }
            let n = b.copy_to_eof(&mut c.stream)?;
            return Ok((false, n));
        }
    }
}

fn client_gone(e: &io::Error) -> bool {
    matches!(e.kind(), io::ErrorKind::BrokenPipe | io::ErrorKind::ConnectionReset | io::ErrorKind::ConnectionAborted)
}

/// `GET /api/files/location?path=` on the coordinator; the chunked row's
/// shape (Go `publishedChunkSize` uses the same rows).
fn fetch_published(coord: &str, path: &str) -> Option<layout::Published> {
    #[derive(serde::Deserialize)]
    struct Loc {
        #[serde(default)]
        file_size: i64,
        #[serde(default)]
        is_chunked: bool,
        #[serde(default)]
        chunk_size: i64,
    }
    let target = format!("/api/files/location?path={}", http::query_escape(path));
    let (code, body) = http::get(coord, &target, &[], Duration::from_secs(3), 4 << 20).ok()?;
    if code != 200 {
        return None;
    }
    let locs: Vec<Loc> = serde_json::from_slice::<Option<Vec<Loc>>>(&body).ok()??;
    let best = locs.iter().find(|l| l.is_chunked && l.chunk_size > 0).or_else(|| locs.first())?;
    Some(layout::Published { size: best.file_size, chunked: best.is_chunked, chunk_size: best.chunk_size })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn routing_native_vs_forwarded() {
        let d = "sha256:".to_string() + &"a".repeat(64);
        assert_eq!(route("GET", "/api/peer/read"), Plan::PeerRead);
        assert_eq!(route("HEAD", "/api/peer/read"), Plan::Proxy); // Go: GET only -> Go answers 405
        assert_eq!(route("GET", &format!("/v2/lib/x/blobs/{d}")), Plan::Blob { digest: d.clone() });
        assert_eq!(route("HEAD", &format!("/oci/blobs/{d}")), Plan::Blob { digest: d.clone() });
        assert_eq!(route("PUT", &format!("/oci/blobs/{d}")), Plan::Proxy);
        assert_eq!(route("GET", &format!("/v2/blobs/{d}")), Plan::Proxy); // no repo
        assert_eq!(route("GET", "/v2/"), Plan::RegistryPing);
        assert_eq!(route("GET", "/rust/healthz"), Plan::Healthz);
        assert_eq!(route("GET", "/rust/metrics"), Plan::Metrics);
        for p in ["/api/health", "/metrics", "/api/files/a/b", "/api/cache/stats", "/api/peers", "/"] {
            assert_eq!(route("GET", p), Plan::Proxy, "{p}");
        }
        assert_eq!(route("PUT", "/api/files/oci/sha256/x"), Plan::Put { path: "oci/sha256/x".into() });
        assert_eq!(route("PUT", "/api/files/models/m/r1/w.bin"), Plan::Put { path: "models/m/r1/w.bin".into() });
        assert_eq!(route("POST", "/api/files/a"), Plan::Proxy);
        assert_eq!(route("DELETE", "/api/files/a"), Plan::Proxy);
    }

    #[test]
    fn auth_rule_matches_go() {
        for (m, p) in [
            ("GET", "/api/health"),
            ("GET", "/metrics"),
            ("GET", "/api/netprobe"),
            ("GET", "/rust/metrics"),
            ("GET", "/v2/"),
            ("HEAD", "/v2/r/blobs/sha256:x"),
            ("GET", "/oci/blobs/x"),
        ] {
            assert!(auth_exempt(m, p), "{m} {p}");
        }
        for (m, p) in [("GET", "/api/peer/read"), ("PUT", "/api/files/x"), ("PUT", "/oci/blobs/x"), ("DELETE", "/v2/r/blobs/x"), ("GET", "/api/cache/stats")] {
            assert!(!auth_exempt(m, p), "{m} {p}");
        }
    }

    #[test]
    fn admission_limit() {
        let s = Server::new(Config { max_inflight: 2, ..Config::new(PathBuf::from("/nonexistent"), "127.0.0.1:1".into()) });
        let a = s.try_admit();
        let b = s.try_admit();
        assert!(a.is_some() && b.is_some());
        assert!(s.try_admit().is_none());
        drop(a);
        assert!(s.try_admit().is_some());
    }
}
