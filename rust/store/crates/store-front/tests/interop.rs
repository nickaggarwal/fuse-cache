//! store-front in front of the real Go API (`readbench serve-api`: the
//! client's api.Handler over a DefaultCacheManager, built from this repo
//! with the Go toolchain).
//!
//! * interop: PUT a blob through the front, read it back natively, health
//!   and metrics proxied, own routes answered;
//! * differential: the same request set sent to Go directly and through the
//!   front must give identical status, Content-Length, Content-Range,
//!   Accept-Ranges, Retry-After and body, for misses, ranges, the 64 MiB cap,
//!   `_chunk_N` of whole files (published stride), and peer sub-ranges;
//! * the blob PUT digest check stays Go's, and a short file is never served;
//! * a multi-GiB PUT streams through the front in bounded memory;
//! * the in-flight limit answers 503 + Retry-After: 1.
//!
//! Without `go` on PATH the tests print SKIP and pass.

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};
use store_front::server::{Config, Server};

fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../../..").canonicalize().unwrap()
}

fn readbench() -> Option<&'static Path> {
    static BIN: OnceLock<Option<PathBuf>> = OnceLock::new();
    BIN.get_or_init(|| {
        let out = std::env::temp_dir().join(format!("store-front-readbench-{}", std::process::id()));
        match Command::new("go").args(["build", "-o"]).arg(&out).arg("./tools/readbench").current_dir(repo_root()).status() {
            Ok(s) if s.success() => Some(out),
            Ok(s) => panic!("go build ./tools/readbench failed: {s}"),
            Err(e) => {
                eprintln!("SKIP: go toolchain not available ({e})");
                None
            }
        }
    })
    .as_deref()
}

struct Proc(Child);
impl Drop for Proc {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port()
}

fn wait_port(port: u16) {
    let t = Instant::now();
    while TcpStream::connect(("127.0.0.1", port)).is_err() {
        assert!(t.elapsed() < Duration::from_secs(30), "port {port} never opened");
        std::thread::sleep(Duration::from_millis(30));
    }
}

/// A Go API (+ coordinator) and a front over the same cache dir.
struct Rig {
    _tmp: tempfile::TempDir,
    dir: PathBuf,
    go: String,
    front: String,
    coord: String,
    _procs: Vec<Proc>,
    go_args: Vec<String>,
    rb: PathBuf,
    srv: Arc<Server>,
}

impl Rig {
    /// Restart the Go API on the same port: it rehydrates its index from
    /// the cache dir, as a restarted client does.
    fn restart_go(&mut self) {
        let old = self._procs.pop().unwrap();
        drop(old);
        let port: u16 = self.go.rsplit(':').next().unwrap().parse().unwrap();
        let t = Instant::now();
        while TcpStream::connect(("127.0.0.1", port)).is_ok() && t.elapsed() < Duration::from_secs(5) {
            std::thread::sleep(Duration::from_millis(20));
        }
        self._procs.push(Proc(Command::new(&self.rb).args(&self.go_args).stdout(Stdio::null()).stderr(Stdio::null()).spawn().unwrap()));
        wait_port(port);
    }
}

fn rig(rb: &Path, api_key: Option<&str>, max_inflight: usize) -> Rig {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().join("cache");
    std::fs::create_dir_all(&dir).unwrap();
    let cport = free_port();
    let gport = free_port();
    let coord = Proc(Command::new(rb).args(["coord", "-listen", &format!("127.0.0.1:{cport}")]).stdout(Stdio::null()).stderr(Stdio::null()).spawn().unwrap());
    wait_port(cport);
    let mut gargs = vec![
        "serve-api".to_string(),
        "-dir".into(),
        dir.to_str().unwrap().into(),
        "-listen".into(),
        format!("127.0.0.1:{gport}"),
        "-chunk-mb".into(),
        "1".into(),
        "-coord".into(),
        format!("127.0.0.1:{cport}"),
    ];
    if let Some(k) = api_key {
        gargs.push("-api-key".into());
        gargs.push(k.into());
    }
    let go = Proc(Command::new(rb).args(&gargs).stdout(Stdio::null()).stderr(Stdio::null()).spawn().unwrap());
    wait_port(gport);
    let mut cfg = Config::new(dir.clone(), format!("127.0.0.1:{gport}"));
    cfg.coordinator = Some(format!("127.0.0.1:{cport}"));
    cfg.api_key = api_key.map(String::from);
    cfg.max_inflight = max_inflight;
    cfg.stride_ttl = Duration::from_millis(1); // tests mutate state between requests
    let srv = Server::new(cfg);
    let l = TcpListener::bind("127.0.0.1:0").unwrap();
    let front = l.local_addr().unwrap().to_string();
    let s2 = srv.clone();
    std::thread::spawn(move || s2.serve(l, Duration::from_secs(5)));
    Rig {
        _tmp: tmp,
        dir,
        go: format!("127.0.0.1:{gport}"),
        front,
        coord: format!("127.0.0.1:{cport}"),
        _procs: vec![coord, go],
        go_args: gargs,
        rb: rb.to_path_buf(),
        srv,
    }
}

/// Raw HTTP/1.1 exchange on a fresh connection: (status, headers, body).
fn req(addr: &str, method: &str, target: &str, headers: &[(&str, &str)], body: &[u8]) -> (u16, Vec<(String, String)>, Vec<u8>) {
    let mut s = TcpStream::connect(addr).unwrap();
    s.set_read_timeout(Some(Duration::from_secs(60))).unwrap();
    let mut h = format!("{method} {target} HTTP/1.1\r\nHost: t\r\nConnection: close\r\n");
    for (k, v) in headers {
        h.push_str(&format!("{k}: {v}\r\n"));
    }
    if !body.is_empty() || method == "PUT" {
        h.push_str(&format!("Content-Length: {}\r\n", body.len()));
    }
    h.push_str("\r\n");
    s.write_all(h.as_bytes()).unwrap();
    s.write_all(body).unwrap();
    let mut raw = Vec::new();
    s.read_to_end(&mut raw).unwrap();
    let i = raw.windows(4).position(|w| w == b"\r\n\r\n").expect("response head") + 4;
    let head = String::from_utf8_lossy(&raw[..i]).to_string();
    let mut lines = head.split("\r\n");
    let status: u16 = lines.next().unwrap().split(' ').nth(1).unwrap().parse().unwrap();
    let hdrs: Vec<(String, String)> =
        lines.filter(|l| !l.is_empty()).filter_map(|l| l.split_once(": ").map(|(k, v)| (k.to_ascii_lowercase(), v.to_string()))).collect();
    let mut body = raw[i..].to_vec();
    if hdrs.iter().any(|(k, v)| k == "transfer-encoding" && v.contains("chunked")) {
        body = store_front::http::dechunk(&body).unwrap();
    }
    (status, hdrs, body)
}

fn hdr<'a>(h: &'a [(String, String)], k: &str) -> Option<&'a str> {
    h.iter().find(|(n, _)| n == k).map(|(_, v)| v.as_str())
}

fn sha256_hex(data: &[u8]) -> String {
    // Use the system sha256sum to avoid a crypto dependency in the crate.
    let mut c = Command::new("shasum")
        .args(["-a", "256"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .or_else(|_| Command::new("sha256sum").stdin(Stdio::piped()).stdout(Stdio::piped()).spawn())
        .unwrap();
    c.stdin.take().unwrap().write_all(data).unwrap();
    let out = c.wait_with_output().unwrap();
    String::from_utf8(out.stdout).unwrap().split_whitespace().next().unwrap().to_string()
}

/// (method, target, extra headers)
type Case = (&'static str, String, Vec<(&'static str, String)>);

fn pattern(n: usize, seed: u8) -> Vec<u8> {
    (0..n).map(|i| (((i as u64 * 2654435761u64) >> 7) as u8) ^ seed).collect()
}

fn put(addr: &str, path: &str, data: &[u8], key: Option<&str>) -> u16 {
    let k: Vec<(&str, &str)> = key.map(|k| vec![("X-API-Key", k)]).unwrap_or_default();
    req(addr, "PUT", &format!("/api/files{path}"), &k, data).0
}

/// Publish a location row on the test coordinator (what a Go client would
/// publish after a write).
fn publish(coord: &str, path: &str, size: usize, chunk_size: usize) {
    let body = format!(
        r#"{{"file_path":"{path}","peer_id":"store-0","storage_tier":"nvme","storage_path":"{path}","file_size":{size},"is_chunked":{},"chunk_size":{chunk_size}}}"#,
        chunk_size > 0
    );
    let (code, _, _) = req(coord, "PUT", "/api/files/location", &[("Content-Type", "application/json")], body.as_bytes());
    assert_eq!(code, 200);
}

#[test]
fn interop_put_read_health_metrics() {
    let Some(rb) = readbench() else { return };
    let r = rig(rb, None, 64);
    let blob = pattern(3 * 1024 * 1024 + 17, 1);
    let d = sha256_hex(&blob);
    // PUT through the front (proxied, streamed) -> Go verifies the digest.
    assert_eq!(put(&r.front, &format!("/oci/sha256/{d}"), &blob, None), 200);
    // Read back natively: HEAD, a range, the whole blob.
    let (code, h, _) = req(&r.front, "HEAD", &format!("/v2/x/blobs/sha256:{d}"), &[], b"");
    assert_eq!((code, hdr(&h, "content-length")), (200, Some(blob.len().to_string().as_str())));
    let (code, h, b) = req(&r.front, "GET", &format!("/v2/x/blobs/sha256:{d}"), &[("Range", "bytes=100-1048675")], b"");
    assert_eq!(code, 206);
    assert_eq!(b, &blob[100..1048676]);
    assert_eq!(hdr(&h, "content-range"), Some(format!("bytes 100-1048675/{}", blob.len()).as_str()));
    let (code, _, b) = req(&r.front, "GET", &format!("/oci/blobs/{d}"), &[], b"");
    assert_eq!((code, b.len()), (200, blob.len()));
    assert_eq!(b, blob);
    let local = r.srv.metrics.count(store_front::metrics::Route::Blob, store_front::metrics::Outcome::Local);
    assert_eq!(local, 3, "blob reads were served natively");
    // Proxied: health and Go's metrics; own: healthz, readyz, rust metrics.
    let (code, _, b) = req(&r.front, "GET", "/api/health", &[], b"");
    assert_eq!(code, 200);
    assert!(String::from_utf8_lossy(&b).contains("healthy"));
    let (code, _, b) = req(&r.front, "GET", "/metrics", &[], b"");
    assert_eq!(code, 200);
    assert!(String::from_utf8_lossy(&b).contains("fuse_"), "Go metrics proxied");
    assert_eq!(req(&r.front, "GET", "/rust/healthz", &[], b"").0, 200);
    assert_eq!(req(&r.front, "GET", "/rust/readyz", &[], b"").0, 200);
    let (_, _, m) = req(&r.front, "GET", "/rust/metrics", &[], b"");
    let m = String::from_utf8_lossy(&m);
    assert!(m.contains("fuse_front_requests_total{route=\"blob\",outcome=\"local\"} 3"), "{m}");
    assert!(m.contains("fuse_front_requests_total{route=\"proxy\",outcome=\"forwarded\"}"));
}

#[test]
fn differential_go_vs_front() {
    let Some(rb) = readbench() else { return };
    let mut r = rig(rb, None, 64);
    let mib = 1usize << 20;
    // A blob larger than the 64 MiB cap, a small one, a chunked file.
    let big = pattern(70 * mib + 5, 2);
    let small = pattern(10_000, 3);
    let (dbig, dsmall) = (sha256_hex(&big), sha256_hex(&small));
    assert_eq!(put(&r.go, &format!("/oci/sha256/{dbig}"), &big, None), 200);
    assert_eq!(put(&r.go, &format!("/oci/sha256/{dsmall}"), &small, None), 200);
    let w = pattern(3 * mib + 123, 4);
    assert_eq!(put(&r.go, "/m/w.bin", &w, None), 200); // whole file, chunked metadata at 1 MiB
                                                       // The re-indexed whole file: on disk, published chunked at 1 MiB, and
                                                       // known to a restarted Go only as an unchunked file (rehydration does
                                                       // not recover the chunk shape), so Go resolves _chunk_N through the
                                                       // published stride (publishedChunkSize).
    let re = pattern(2 * mib + 77, 5);
    std::fs::create_dir_all(r.dir.join("m")).unwrap();
    std::fs::write(r.dir.join("m/re.bin"), &re).unwrap();
    publish(&r.coord, "/m/re.bin", re.len(), mib);
    r.restart_go();
    let missing = "f".repeat(64);
    let cases: Vec<Case> = vec![
        ("GET", format!("/v2/x/blobs/sha256:{missing}"), vec![]),
        ("GET", format!("/oci/blobs/{missing}"), vec![]),
        ("HEAD", format!("/v2/x/blobs/sha256:{missing}"), vec![]),
        ("GET", "/v2/x/blobs/sha256:nothex".into(), vec![]),
        ("GET", "/v2/".into(), vec![]),
        ("HEAD", format!("/v2/x/blobs/sha256:{dsmall}"), vec![]),
        ("GET", format!("/v2/x/blobs/sha256:{dsmall}"), vec![("Range", "bytes=0-99".into())]),
        ("GET", format!("/v2/x/blobs/sha256:{dsmall}"), vec![("Range", "bytes=9000-".into())]),
        ("GET", format!("/v2/x/blobs/sha256:{dsmall}"), vec![("Range", "bytes=9000-99999".into())]),
        ("GET", format!("/v2/x/blobs/sha256:{dsmall}"), vec![("Range", "bytes=10000-".into())]), // past end -> 416
        ("GET", format!("/v2/x/blobs/sha256:{dsmall}"), vec![("Range", "bytes=-5".into())]),
        ("GET", format!("/v2/x/blobs/sha256:{dsmall}"), vec![("Range", "bytes=0-1,4-5".into())]),
        ("GET", format!("/v2/x/blobs/sha256:{dsmall}"), vec![]), // no Range -> 200 whole
        ("GET", format!("/oci/blobs/sha256:{dbig}"), vec![("Range", "bytes=0-".into())]), // 64 MiB cap
        ("GET", format!("/oci/blobs/{dbig}"), vec![("Range", format!("bytes={}-{}", 69 * mib, 70 * mib + 4))]),
        ("GET", "/api/peer/read?path=/m/w.bin".into(), vec![]),
        ("GET", "/api/peer/read?path=/m/w.bin_chunk_2".into(), vec![]),
        ("GET", "/api/peer/read?path=/m/w.bin_chunk_3".into(), vec![]), // short last chunk
        ("GET", "/api/peer/read?path=/m/w.bin_chunk_4".into(), vec![]), // past the end
        ("GET", "/api/peer/read?path=/m/w.bin_chunk_1&off=100&len=50".into(), vec![]),
        ("GET", "/api/peer/read?path=/m/w.bin_chunk_3&off=100&len=99999999".into(), vec![]),
        ("GET", "/api/peer/read?path=/m/w.bin_chunk_3&off=124&len=1".into(), vec![]), // off at end -> 416
        ("GET", "/api/peer/read?path=/m/w.bin&off=x&len=1".into(), vec![]),
        ("GET", "/api/peer/read?path=/m/w.bin&off=1".into(), vec![]),
        ("GET", "/api/peer/read?path=/m/re.bin_chunk_1".into(), vec![]), // re-indexed whole file
        ("GET", "/api/peer/read?path=/m/re.bin_chunk_2&off=7&len=10".into(), vec![]),
        ("GET", "/api/peer/read?path=/nope".into(), vec![]),
        ("GET", "/api/peer/read?path=/../../m/w.bin".into(), vec![]),
        ("GET", "/api/peer/read".into(), vec![]),
        ("HEAD", "/api/peer/read?path=/m/w.bin".into(), vec![]),
        ("GET", "/api/files/m/w.bin/size".into(), vec![]),
        ("HEAD", "/api/files/m/w.bin".into(), vec![]),
    ];
    let mut native = 0;
    for (m, t, hs) in &cases {
        let hv: Vec<(&str, &str)> = hs.iter().map(|(k, v)| (*k, v.as_str())).collect();
        let before = r.srv.metrics.count(store_front::metrics::Route::Blob, store_front::metrics::Outcome::Local)
            + r.srv.metrics.count(store_front::metrics::Route::PeerRead, store_front::metrics::Outcome::Local);
        let (gs, gh, gb) = req(&r.go, m, t, &hv, b"");
        let (fs, fh, fb) = req(&r.front, m, t, &hv, b"");
        let after = r.srv.metrics.count(store_front::metrics::Route::Blob, store_front::metrics::Outcome::Local)
            + r.srv.metrics.count(store_front::metrics::Route::PeerRead, store_front::metrics::Outcome::Local);
        native += after - before;
        assert_eq!(
            gs,
            fs,
            "{m} {t} {hs:?}: status (go body {:?}, front body {:?})",
            String::from_utf8_lossy(&gb[..gb.len().min(80)]),
            String::from_utf8_lossy(&fb[..fb.len().min(80)])
        );
        for k in ["content-length", "content-range", "accept-ranges", "retry-after", "docker-distribution-api-version"] {
            assert_eq!(hdr(&gh, k), hdr(&fh, k), "{m} {t} {hs:?}: header {k}");
        }
        assert!(gb == fb, "{m} {t} {hs:?}: body differs ({} vs {} bytes)", gb.len(), fb.len());
    }
    // Most read cases must have been served natively, not forwarded.
    assert!(native >= 14, "only {native} cases served natively");
}

#[test]
fn digest_check_stays_go_and_short_files_are_not_served() {
    let Some(rb) = readbench() else { return };
    let r = rig(rb, None, 64);
    let body = pattern(5000, 6);
    let good = sha256_hex(&body);
    assert_eq!(put(&r.front, &format!("/oci/sha256/{good}"), b"tampered bytes!!", None), 400);
    assert_ne!(req(&r.front, "HEAD", &format!("/v2/x/blobs/sha256:{good}"), &[], b"").0, 200);
    assert_eq!(put(&r.front, &format!("/oci/sha256/{good}"), &body, None), 200);
    assert_eq!(req(&r.front, "HEAD", &format!("/v2/x/blobs/sha256:{good}"), &[], b"").0, 200);
    // Truncate the file on disk under Go's index (a partial copy): the
    // front must not serve it natively; Go's own answer is relayed.
    let p = r.dir.join(format!("oci/sha256/{good}"));
    std::fs::write(&p, &body[..1000]).unwrap();
    let before = r.srv.metrics.count(store_front::metrics::Route::Blob, store_front::metrics::Outcome::Local);
    std::thread::sleep(Duration::from_millis(5)); // past the size cache TTL
    let (fs, _, fb) = req(
        &r.front,
        "GET",
        &format!("/v2/x/blobs/sha256:{good}"),
        &[("Range", "bytes=0-4999".into())].iter().map(|(k, v): &(&str, String)| (*k, v.as_str())).collect::<Vec<_>>(),
        b"",
    );
    let after = r.srv.metrics.count(store_front::metrics::Route::Blob, store_front::metrics::Outcome::Local);
    assert_eq!(before, after, "a short file was served natively");
    let (gs, _, gb) = req(&r.go, "GET", &format!("/v2/x/blobs/sha256:{good}"), &[("Range", "bytes=0-4999")], b"");
    assert_eq!((fs, fb), (gs, gb), "front relays Go's answer for the short file");
}

#[test]
fn auth_matches_go() {
    let Some(rb) = readbench() else { return };
    let r = rig(rb, Some("sekret"), 64);
    let data = pattern(4000, 7);
    assert_eq!(put(&r.front, "/a.bin", &data, None), 401);
    assert_eq!(put(&r.front, "/a.bin", &data, Some("wrong")), 401);
    assert_eq!(put(&r.front, "/a.bin", &data, Some("sekret")), 200);
    for (m, t, key, want) in [
        ("GET", "/api/peer/read?path=/a.bin", None, 401),
        ("GET", "/api/peer/read?path=/a.bin", Some("sekret"), 200),
        ("GET", "/api/health", None, 200),
        ("GET", "/metrics", None, 200),
        ("GET", "/rust/healthz", None, 200),
        ("GET", "/v2/", None, 200),
        ("GET", "/api/cache/stats", None, 401),
    ] {
        let k: Vec<(&str, &str)> = key.map(|k| vec![("X-API-Key", k)]).unwrap_or_default();
        let (fs, _, fb) = req(&r.front, m, t, &k, b"");
        assert_eq!(fs, want, "{m} {t} key={key:?}");
        if !t.starts_with("/rust/") {
            let (gs, _, gb) = req(&r.go, m, t, &k, b"");
            assert_eq!((fs, t.contains("metrics") || fb == gb), (gs, true), "{m} {t}: front differs from Go");
        }
    }
}

#[test]
fn inflight_limit_503() {
    let Some(rb) = readbench() else { return };
    let r = rig(rb, None, 0);
    let data = pattern(1000, 8);
    assert_eq!(put(&r.front, "/b.bin", &data, None), 200);
    let (code, h, b) = req(&r.front, "GET", "/api/peer/read?path=/b.bin", &[], b"");
    assert_eq!((code, hdr(&h, "retry-after")), (503, Some("1")));
    assert_eq!(String::from_utf8_lossy(&b), "peer serve capacity exhausted\n");
    // Blob routes are not gated (as in Go).
    let d = sha256_hex(&data);
    assert_eq!(put(&r.front, &format!("/oci/sha256/{d}"), &data, None), 200);
    assert_eq!(req(&r.front, "HEAD", &format!("/oci/blobs/{d}"), &[], b"").0, 200);
}

/// A PUT much larger than any buffer, generated and hashed on the fly:
/// it must arrive intact (Go checks the digest) while the front's RSS
/// stays small.
#[test]
fn large_put_streams_in_bounded_memory() {
    let Some(rb) = readbench() else { return };
    let r = rig(rb, None, 64);
    let size: u64 = std::env::var("STORE_FRONT_BIG_PUT_MIB").ok().and_then(|v| v.parse().ok()).unwrap_or(1024) << 20;
    // Digest of the stream (computed by piping the same generator).
    let gen = |sink: &mut dyn Write| {
        let mut block = vec![0u8; 1 << 20];
        let mut off = 0u64;
        while off < size {
            let n = (size - off).min(block.len() as u64) as usize;
            for (i, b) in block[..n].iter_mut().enumerate() {
                *b = ((off + i as u64) * 31 % 251) as u8;
            }
            sink.write_all(&block[..n]).unwrap();
            off += n as u64;
        }
    };
    let mut hasher = Command::new("shasum")
        .args(["-a", "256"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .or_else(|_| Command::new("sha256sum").stdin(Stdio::piped()).stdout(Stdio::piped()).spawn())
        .unwrap();
    {
        let mut stdin = hasher.stdin.take().unwrap();
        gen(&mut stdin);
    }
    let digest = String::from_utf8(hasher.wait_with_output().unwrap().stdout).unwrap().split_whitespace().next().unwrap().to_string();
    let rss0 = rss_kib();
    let mut s = TcpStream::connect(&r.front).unwrap();
    s.set_read_timeout(Some(Duration::from_secs(600))).unwrap();
    write!(s, "PUT /api/files/oci/sha256/{digest} HTTP/1.1\r\nHost: t\r\nContent-Length: {size}\r\nConnection: close\r\n\r\n").unwrap();
    let peak = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
    let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let (p2, s2) = (peak.clone(), stop.clone());
    let sampler = std::thread::spawn(move || {
        while !s2.load(std::sync::atomic::Ordering::Relaxed) {
            p2.fetch_max(rss_kib(), std::sync::atomic::Ordering::Relaxed);
            std::thread::sleep(Duration::from_millis(20));
        }
    });
    gen(&mut s);
    let mut resp = String::new();
    s.read_to_string(&mut resp).unwrap();
    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    sampler.join().unwrap();
    assert!(resp.starts_with("HTTP/1.1 200"), "{resp}");
    let grew_mib = peak.load(std::sync::atomic::Ordering::Relaxed).saturating_sub(rss0) / 1024;
    // The test process holds the front plus the generator's 1 MiB block.
    assert!(grew_mib < 64, "RSS grew {grew_mib} MiB while streaming a {} MiB PUT", size >> 20);
    let (code, h, _) = req(&r.front, "HEAD", &format!("/oci/blobs/{digest}"), &[], b"");
    assert_eq!((code, hdr(&h, "content-length")), (200, Some(size.to_string().as_str())));
}

fn rss_kib() -> u64 {
    let out = Command::new("ps").args(["-o", "rss=", "-p", &std::process::id().to_string()]).output().unwrap();
    String::from_utf8_lossy(&out.stdout).trim().parse().unwrap_or(0)
}
