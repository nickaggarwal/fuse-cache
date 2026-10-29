//! Operational behaviour that does not need the Go client: readiness when
//! the backend is down, liveness regardless, graceful drain on stop, and
//! the SIGTERM path of the real binary.

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::time::{Duration, Instant};
use store_front::server::{Config, Server};

fn get(addr: &str, target: &str) -> (u16, String) {
    let mut s = TcpStream::connect(addr).unwrap();
    s.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
    write!(s, "GET {target} HTTP/1.1\r\nHost: t\r\nConnection: close\r\n\r\n").unwrap();
    let mut out = String::new();
    s.read_to_string(&mut out).unwrap();
    let code = out.split(' ').nth(1).unwrap().parse().unwrap();
    (code, out.split("\r\n\r\n").nth(1).unwrap_or("").to_string())
}

fn start(cfg: Config) -> (Arc<Server>, String, std::thread::JoinHandle<()>) {
    let srv = Server::new(cfg);
    let l = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = l.local_addr().unwrap().to_string();
    let s2 = srv.clone();
    let h = std::thread::spawn(move || s2.serve(l, Duration::from_secs(5)).unwrap());
    (srv, addr, h)
}

#[test]
fn liveness_and_readiness() {
    let dir = tempfile::tempdir().unwrap();
    // Backend on a port nothing listens on.
    let dead = TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().to_string();
    let (srv, addr, h) = start(Config::new(dir.path().to_path_buf(), dead));
    assert_eq!(get(&addr, "/rust/healthz").0, 200);
    let (code, body) = get(&addr, "/rust/readyz");
    assert_eq!(code, 503);
    assert!(body.contains("backend unreachable"), "{body}");
    // Proxied routes answer 502 when Go is down.
    assert_eq!(get(&addr, "/api/health").0, 502);
    srv.stop();
    h.join().unwrap();

    // Cache dir missing: not ready either.
    let missing = dir.path().join("nope");
    let ok_backend = TcpListener::bind("127.0.0.1:0").unwrap();
    let (srv, addr, h) = start(Config::new(missing, ok_backend.local_addr().unwrap().to_string()));
    let (code, body) = get(&addr, "/rust/readyz");
    assert_eq!(code, 503);
    assert!(body.contains("not readable"), "{body}");
    srv.stop();
    h.join().unwrap();
}

/// A request in flight when stop() is called completes; new connections
/// are refused once the listener closes.
#[test]
fn drain_finishes_inflight_then_stops() {
    let dir = tempfile::tempdir().unwrap();
    // A backend that answers slowly.
    let backend = TcpListener::bind("127.0.0.1:0").unwrap();
    let baddr = backend.local_addr().unwrap().to_string();
    std::thread::spawn(move || {
        for s in backend.incoming() {
            let mut s = s.unwrap();
            std::thread::spawn(move || {
                let mut buf = [0u8; 4096];
                let _ = s.read(&mut buf);
                std::thread::sleep(Duration::from_millis(800));
                let _ = s.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 4\r\n\r\nslow");
            });
        }
    });
    let (srv, addr, h) = start(Config::new(dir.path().to_path_buf(), baddr));
    let a2 = addr.clone();
    let client = std::thread::spawn(move || get(&a2, "/api/cache/stats"));
    std::thread::sleep(Duration::from_millis(200));
    let t = Instant::now();
    srv.stop();
    let (code, body) = client.join().unwrap();
    assert_eq!((code, body.as_str()), (200, "slow"), "in-flight request completed");
    h.join().unwrap();
    assert!(t.elapsed() >= Duration::from_millis(400), "serve returned before the in-flight request finished");
    assert!(TcpStream::connect(&addr).is_err(), "listener closed after drain");
}

/// The binary exits cleanly on SIGTERM.
#[test]
fn binary_exits_on_sigterm() {
    let dir = tempfile::tempdir().unwrap();
    let port = TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port();
    let bin = env!("CARGO_BIN_EXE_store-front");
    let mut child = Command::new(bin)
        .args(["--cache-dir", dir.path().to_str().unwrap(), "--listen", &format!("127.0.0.1:{port}"), "--backend", "127.0.0.1:1", "--drain-sec", "5"])
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let t = Instant::now();
    while TcpStream::connect(("127.0.0.1", port)).is_err() {
        assert!(t.elapsed() < Duration::from_secs(10), "store-front never listened");
        std::thread::sleep(Duration::from_millis(20));
    }
    assert_eq!(get(&format!("127.0.0.1:{port}"), "/rust/healthz").0, 200);
    unsafe {
        libc::kill(child.id() as i32, libc::SIGTERM);
    }
    let t = Instant::now();
    let status = loop {
        if let Some(s) = child.try_wait().unwrap() {
            break s;
        }
        assert!(t.elapsed() < Duration::from_secs(10), "did not exit after SIGTERM");
        std::thread::sleep(Duration::from_millis(50));
    };
    assert!(status.success(), "{status}");
    let mut log = String::new();
    child.stderr.take().unwrap().read_to_string(&mut log).unwrap();
    for line in log.lines() {
        assert!(line.starts_with("[FRONT] "), "log line not in [FRONT] format: {line}");
    }
    assert!(log.contains("draining") && log.contains("stopped"), "{log}");
}
