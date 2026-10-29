//! Shared rig for the store-front integration tests: a coordinator, the real
//! Go API (`readbench serve-api`) and a front over one cache dir.
#![allow(dead_code)]

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};
use store_front::server::{Config, Server};

pub fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../../..").canonicalize().unwrap()
}

pub fn readbench() -> Option<&'static Path> {
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

pub struct Proc(pub Child);
impl Drop for Proc {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

pub fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port()
}

pub fn wait_port(port: u16) {
    let t = Instant::now();
    while TcpStream::connect(("127.0.0.1", port)).is_err() {
        assert!(t.elapsed() < Duration::from_secs(30), "port {port} never opened");
        std::thread::sleep(Duration::from_millis(30));
    }
}

/// A Go API (+ coordinator) and a front over the same cache dir.
pub struct Rig {
    pub _tmp: tempfile::TempDir,
    pub dir: PathBuf,
    pub go: String,
    pub front: String,
    pub coord: String,
    pub _procs: Vec<Proc>,
    pub go_args: Vec<String>,
    pub rb: PathBuf,
    pub srv: Arc<Server>,
}

impl Rig {
    /// Restart the Go API on the same port: it rehydrates its index from
    /// the cache dir, as a restarted client does.
    pub fn restart_go(&mut self) {
        let old = self._procs.pop().unwrap();
        drop(old);
        let port: u16 = self.go.rsplit(':').next().unwrap().parse().unwrap();
        let t = Instant::now();
        while TcpStream::connect(("127.0.0.1", port)).is_ok() && t.elapsed() < Duration::from_secs(5) {
            std::thread::sleep(Duration::from_millis(20));
        }
        self._procs.push(Proc(
            Command::new(&self.rb)
                .args(&self.go_args)
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .spawn()
                .unwrap(),
        ));
        wait_port(port);
    }
}

pub fn rig(rb: &Path, api_key: Option<&str>, max_inflight: usize) -> Rig {
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
pub fn req(addr: &str, method: &str, target: &str, headers: &[(&str, &str)], body: &[u8]) -> (u16, Vec<(String, String)>, Vec<u8>) {
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

pub fn hdr<'a>(h: &'a [(String, String)], k: &str) -> Option<&'a str> {
    h.iter().find(|(n, _)| n == k).map(|(_, v)| v.as_str())
}

pub fn sha256_hex(data: &[u8]) -> String {
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
pub type Case = (&'static str, String, Vec<(&'static str, String)>);

pub fn pattern(n: usize, seed: u8) -> Vec<u8> {
    (0..n).map(|i| (((i as u64 * 2654435761u64) >> 7) as u8) ^ seed).collect()
}

pub fn put(addr: &str, path: &str, data: &[u8], key: Option<&str>) -> u16 {
    let k: Vec<(&str, &str)> = key.map(|k| vec![("X-API-Key", k)]).unwrap_or_default();
    req(addr, "PUT", &format!("/api/files{path}"), &k, data).0
}

/// Publish a location row on the test coordinator (what a Go client would
/// publish after a write).
pub fn publish(coord: &str, path: &str, size: usize, chunk_size: usize) {
    let body = format!(
        r#"{{"file_path":"{path}","peer_id":"store-0","storage_tier":"nvme","storage_path":"{path}","file_size":{size},"is_chunked":{},"chunk_size":{chunk_size}}}"#,
        chunk_size > 0
    );
    let (code, _, _) = req(coord, "PUT", "/api/files/location", &[("Content-Type", "application/json")], body.as_bytes());
    assert_eq!(code, 200);
}
