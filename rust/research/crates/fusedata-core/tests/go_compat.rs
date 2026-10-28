//! Compatibility with the Go client, tested against the Go code itself:
//!
//! * the fixture cache dir is written by the real Go write paths
//!   (`readbench fixture` -> PutFromReader / PutFromFile / Put);
//! * the peer interop tests talk to the real `api.Handler` (`/api/peer/read`)
//!   and the real `PeerStorage` raw-HTTP client;
//! * the promotion test has Go's `ReadRange` read back chunks the Rust tier
//!   reader fetched and landed.
//!
//! Needs `go` on PATH (the repo's Go toolchain); without it the tests print a
//! notice and pass vacuously so `cargo test` still works in a Rust-only env.

use fusedata_core::coord::CoordClient;
use fusedata_core::layout::{CacheDir, Hint, Options};
use fusedata_core::pattern;
use fusedata_core::peer::{PeerClient, PeerClientConfig, PeerError, PeerServer, PeerServerConfig};
use fusedata_core::tiered::TieredReader;
use std::io::ErrorKind;
use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

const MB: i64 = 1 << 20;

fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../../..").canonicalize().unwrap()
}

/// Build tools/readbench once per test process. None when Go is missing.
fn readbench() -> Option<&'static Path> {
    static BIN: OnceLock<Option<PathBuf>> = OnceLock::new();
    BIN.get_or_init(|| {
        let out = std::env::temp_dir().join(format!("fusedata-readbench-{}", std::process::id()));
        let st = Command::new("go").args(["build", "-o"]).arg(&out).arg("./tools/readbench").current_dir(repo_root()).status();
        match st {
            Ok(s) if s.success() => Some(out),
            Ok(s) => panic!("go build ./tools/readbench failed: {s}"),
            Err(e) => {
                eprintln!("SKIP: go toolchain not available ({e}); Go compatibility tests not run");
                None
            }
        }
    })
    .as_deref()
}

#[derive(serde::Deserialize, Clone, Debug)]
struct FixtureFile {
    path: String,
    size: i64,
    layout: String,
    chunk_size: i64,
    sidecar: bool,
    #[serde(default)]
    corrupt: bool,
}

#[derive(serde::Deserialize)]
struct Manifest {
    files: Vec<FixtureFile>,
}

struct Fixture {
    _tmp: tempfile::TempDir,
    dir: PathBuf,
    files: Vec<FixtureFile>,
}

fn fixture(rb: &Path) -> Fixture {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().join("cache");
    let out = Command::new(rb).args(["fixture", "-dir"]).arg(&dir).output().unwrap();
    assert!(out.status.success(), "fixture: {}", String::from_utf8_lossy(&out.stderr));
    let m: Manifest = serde_json::from_slice(&std::fs::read(dir.join("manifest.json")).unwrap()).unwrap();
    Fixture { _tmp: tmp, dir, files: m.files }
}

fn opts() -> Options {
    Options { configured_chunk_size: MB, ..Default::default() }
}

fn read_all(c: &CacheDir, path: &str, size: i64, window: usize) -> std::io::Result<Vec<u8>> {
    let mut out = vec![0u8; size as usize];
    let mut off = 0usize;
    while off < size as usize {
        let end = (off + window).min(size as usize);
        let n = c.read_at(path, off as u64, &mut out[off..end])?;
        assert!(n > 0, "{path}: zero read at {off}");
        off += n;
    }
    Ok(out)
}

#[test]
fn layout_reads_go_written_cache_dir() {
    let Some(rb) = readbench() else { return };
    let fx = fixture(rb);
    let c = CacheDir::new(&fx.dir, opts());
    for f in &fx.files {
        if f.corrupt {
            let err = c.shape(&f.path).unwrap_err();
            assert_eq!(err.kind(), ErrorKind::InvalidData, "{}: corrupt file must fail sidecar verification", f.path);
            continue;
        }
        let shape = c.shape(&f.path).unwrap_or_else(|e| panic!("{}: {e}", f.path));
        assert_eq!(shape.size, f.size, "{} size", f.path);
        assert!(shape.complete, "{} complete", f.path);
        if f.layout == "chunked" {
            assert!(shape.chunked);
            assert_eq!(shape.stride, f.chunk_size, "{} stride", f.path);
        }
        // Several window sizes, including ones that straddle chunk
        // boundaries at odd offsets.
        for window in [4096usize, 128 * 1024 + 7, 1 << 20, 3 << 20] {
            let data = read_all(&c, &f.path, f.size, window).unwrap();
            assert_eq!(pattern::check(&data, 0), None, "{} window {window}", f.path);
        }
        // A read past EOF is empty, a read straddling EOF is short.
        let mut b = vec![0u8; 100];
        assert_eq!(c.read_at(&f.path, f.size as u64, &mut b).unwrap(), 0);
        assert_eq!(c.read_at(&f.path, (f.size - 10) as u64, &mut b).unwrap(), 10);
        if f.sidecar {
            assert!(c.stats.verifies.load(std::sync::atomic::Ordering::Relaxed) > 0);
        }
    }
    // Directory view: chunk files collapse to their parent; sidecars,
    // staging files and the journal are hidden.
    let names: Vec<String> = c.list_dir("/models/a").unwrap().into_iter().map(|(n, _)| n).collect();
    assert_eq!(names, vec!["chunked-exact.bin", "chunked-short-tail.bin"]);
    let names: Vec<String> = c.list_dir("/models").unwrap().into_iter().map(|(n, _)| n).collect();
    assert_eq!(names, vec!["a", "b", "corrupt.bin", "dir with space", "small.bin"]);
    let root: Vec<String> = c.list_dir("/").unwrap().into_iter().map(|(n, _)| n).collect();
    assert!(!root.iter().any(|n| n.starts_with(".fuse-durability")), "{root:?}");
    // Path cleaning: traversal cannot escape the root.
    let f = &fx.files[0];
    let mut b = vec![0u8; 16];
    assert_eq!(c.read_at(&format!("/../../models/a/../a/{}", f.path.rsplit('/').next().unwrap()), 0, &mut b).unwrap(), 16);
}

/// Mapped views (the FUSE read path) return the same bytes as read_at for
/// sequential and random windows of every layout, and leave nothing mapped
/// once reaped.
#[test]
fn read_view_matches_and_releases_mappings() {
    let Some(rb) = readbench() else { return };
    let fx = fixture(rb);
    for view in [fusedata_core::layout::ViewMode::Map, fusedata_core::layout::ViewMode::Segment, fusedata_core::layout::ViewMode::Copy] {
        let c = CacheDir::new(&fx.dir, Options { configured_chunk_size: MB, view, readahead_min: MB as u64, readahead: 4 * MB as u64, ..Default::default() });
        for f in fx.files.iter().filter(|f| !f.corrupt) {
            // Sequential stream, odd window crossing chunk boundaries.
            for window in [4096usize, 128 * 1024 + 7, 1 << 20] {
                let mut off = 0u64;
                let mut all = Vec::with_capacity(f.size as usize);
                while (off as i64) < f.size {
                    let n = c
                        .read_view(&f.path, off, window, |d| {
                            all.extend_from_slice(d);
                            d.len()
                        })
                        .unwrap();
                    assert!(n > 0, "{} {view:?} zero read at {off}", f.path);
                    off += n as u64;
                }
                assert_eq!(all.len() as i64, f.size);
                assert_eq!(pattern::check(&all, 0), None, "{} {view:?} window {window}", f.path);
            }
            // Random windows.
            let mut x = 12345u64;
            for _ in 0..50 {
                x = fusedata_core::pattern::splitmix64(x);
                let off = x % f.size as u64;
                c.read_view(&f.path, off, 70_000, |d| assert_eq!(pattern::check(d, off), None, "{} {view:?} rand {off}", f.path)).unwrap();
            }
        }
    }
    // Let prefetch workers finish, then everything must unmap.
    std::thread::sleep(Duration::from_millis(300));
    fusedata_core::layout::reap_mapped(Duration::ZERO);
    #[cfg(target_os = "linux")]
    {
        let maps = std::fs::read_to_string("/proc/self/smaps").unwrap();
        let mut rss_in_cache = 0u64;
        let mut in_cache = false;
        for line in maps.lines() {
            if line.contains('-') && line.split_whitespace().count() >= 5 {
                in_cache = line.contains(fx.dir.to_str().unwrap());
            } else if in_cache && line.starts_with("Rss:") {
                rss_in_cache += line.split_whitespace().nth(1).unwrap().parse::<u64>().unwrap();
            }
        }
        assert_eq!(rss_in_cache, 0, "cache files still mapped: {rss_in_cache} KiB");
    }
}

#[test]
fn layout_infers_shape_without_journal() {
    let Some(rb) = readbench() else { return };
    let fx = fixture(rb);
    std::fs::remove_dir_all(fx.dir.join(".fuse-durability")).unwrap();
    let c = CacheDir::new(&fx.dir, opts());
    for f in fx.files.iter().filter(|f| !f.corrupt) {
        let shape = c.shape(&f.path).unwrap();
        assert_eq!(shape.size, f.size, "{} inferred size", f.path);
        let data = read_all(&c, &f.path, f.size, 1 << 20).unwrap();
        assert_eq!(pattern::check(&data, 0), None, "{}", f.path);
    }
}

#[test]
fn mismatched_stride_hint_never_panics() {
    // Metadata claims a stride that does not match the chunk files on disk
    // (the -chunk-size change case clampChunkSlice guards in Go). Reads may
    // return wrong or short data, but must not panic or overrun.
    let Some(rb) = readbench() else { return };
    let fx = fixture(rb);
    let f = fx.files.iter().find(|f| f.path.ends_with("chunked-short-tail.bin")).unwrap().clone();
    for wrong in [MB / 2, 3 * MB, 16 * MB, 1] {
        let c = CacheDir::new(&fx.dir, opts());
        c.set_hint(&f.path, Hint { size: f.size, chunked: true, chunk_size: wrong });
        let mut buf = vec![0u8; 3 << 20];
        for off in (0..f.size + 2 * MB).step_by(777_777) {
            match c.read_at(&f.path, off as u64, &mut buf) {
                Ok(n) => assert!(n <= buf.len()),
                Err(e) => assert_eq!(e.kind(), ErrorKind::NotFound, "stride {wrong} off {off}: {e}"),
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Peer protocol interop against the Go implementation.

fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port()
}

struct Proc(Child);

impl Drop for Proc {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn wait_port(port: u16) {
    let t = Instant::now();
    while TcpStream::connect(("127.0.0.1", port)).is_err() {
        assert!(t.elapsed() < Duration::from_secs(30), "port {port} never opened");
        std::thread::sleep(Duration::from_millis(50));
    }
}

fn publish_args(files: &[FixtureFile]) -> Vec<String> {
    files.iter().filter(|f| f.chunk_size > 0).flat_map(|f| ["-publish".to_string(), format!("{}:{}:{}", f.path, f.size, f.chunk_size / MB)]).collect()
}

fn start_go_peer(rb: &Path, dir: &Path, files: &[FixtureFile], key: &str) -> (Proc, String) {
    let port = free_port();
    let child = Command::new(rb)
        .args(["serve-peer", "-dir"])
        .arg(dir)
        .args(["-listen", &format!("127.0.0.1:{port}"), "-chunk-mb", "1", "-api-key", key])
        .args(publish_args(files))
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    wait_port(port);
    (Proc(child), format!("127.0.0.1:{port}"))
}

fn start_rust_peer(dir: &Path, key: &str) -> String {
    let c = Arc::new(CacheDir::new(dir, opts()));
    let srv = PeerServer::new(c, PeerServerConfig { api_key: Some(key.into()), max_inflight: 64 });
    let l = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = l.local_addr().unwrap().to_string();
    std::thread::spawn(move || srv.serve(l));
    addr
}

/// Every chunk object of every chunked fixture file, fetched and verified.
fn fetch_all_chunks(client: &PeerClient, addr: &str, files: &[FixtureFile]) {
    let mut buf = Vec::new();
    for f in files.iter().filter(|f| f.chunk_size > 0 && !f.corrupt) {
        let n = (f.size + f.chunk_size - 1) / f.chunk_size;
        for i in 0..n {
            let got = client.fetch(addr, &format!("{}_chunk_{i}", f.path), &mut buf).unwrap_or_else(|e| panic!("{} chunk {i}: {e}", f.path));
            let want = f.chunk_size.min(f.size - i * f.chunk_size) as usize;
            assert_eq!(got, want, "{} chunk {i} length", f.path);
            assert_eq!(pattern::check(&buf[..got], (i * f.chunk_size) as u64), None, "{} chunk {i}", f.path);
        }
        // One past the end is a typed miss.
        assert!(matches!(client.fetch(addr, &format!("{}_chunk_{n}", f.path), &mut buf), Err(PeerError::Miss)));
    }
    // Whole small file, and a path nobody has.
    let small = files.iter().find(|f| f.path == "/models/small.bin").unwrap();
    assert_eq!(client.fetch(addr, &small.path, &mut buf).unwrap(), small.size as usize);
    assert!(matches!(client.fetch(addr, "/models/nope", &mut buf), Err(PeerError::Miss)));
}

#[test]
fn rust_client_against_go_peer_server() {
    let Some(rb) = readbench() else { return };
    let fx = fixture(rb);
    let (_p, addr) = start_go_peer(rb, &fx.dir, &fx.files, "k1");
    let client = PeerClient::new(PeerClientConfig { api_key: Some("k1".into()), ..Default::default() });
    fetch_all_chunks(&client, &addr, &fx.files);
    // Keep-alive: the whole run should have used very few connections.
    assert!(client.stats.dials.load(std::sync::atomic::Ordering::Relaxed) <= 2);
    // Wrong key is a plain failure (401), not busy or miss.
    let bad = PeerClient::new(PeerClientConfig { api_key: Some("nope".into()), ..Default::default() });
    let mut buf = Vec::new();
    assert!(matches!(bad.fetch(&addr, "/models/small.bin", &mut buf), Err(PeerError::Other(_))));
}

#[test]
fn rust_client_against_rust_peer_server() {
    let Some(rb) = readbench() else { return };
    let fx = fixture(rb);
    let addr = start_rust_peer(&fx.dir, "k2");
    let client = PeerClient::new(PeerClientConfig { api_key: Some("k2".into()), ..Default::default() });
    fetch_all_chunks(&client, &addr, &fx.files);
}

#[test]
fn go_client_against_rust_peer_server() {
    let Some(rb) = readbench() else { return };
    let fx = fixture(rb);
    let addr = start_rust_peer(&fx.dir, "k3");
    // Go's PeerStorage (raw HTTP transport) fetches and verifies every chunk
    // of the per-chunk file and of the whole-layout chunked file.
    for f in fx.files.iter().filter(|f| f.chunk_size > 0) {
        let out = Command::new(rb)
            .args(["bench-peer", "-addr", &addr, "-path", &f.path, "-size", &f.size.to_string(), "-stride-mb", "1", "-threads", "2"])
            .args(["-total-mb", &((f.size + MB - 1) / MB).to_string(), "-verify", "-api-key", "k3", "-server", "rust"])
            .output()
            .unwrap();
        assert!(out.status.success(), "{}: {}", f.path, String::from_utf8_lossy(&out.stderr));
    }
}

#[test]
fn rust_server_sheds_load_with_503() {
    let Some(rb) = readbench() else { return };
    let fx = fixture(rb);
    let c = Arc::new(CacheDir::new(&fx.dir, opts()));
    let srv = PeerServer::new(c, PeerServerConfig { api_key: None, max_inflight: 0 });
    let l = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = l.local_addr().unwrap().to_string();
    std::thread::spawn(move || srv.serve(l));
    let client = PeerClient::new(PeerClientConfig::default());
    let mut buf = Vec::new();
    assert!(matches!(client.fetch(&addr, "/models/small.bin", &mut buf), Err(PeerError::Busy)));
}

#[test]
fn tiered_fetch_from_go_peer_and_go_reads_promoted_chunks() {
    let Some(rb) = readbench() else { return };
    let fx = fixture(rb);
    let (_peer, peer_addr) = start_go_peer(rb, &fx.dir, &fx.files, "");
    let coord_port = free_port();
    let f = fx.files.iter().find(|f| f.path.ends_with("chunked-short-tail.bin")).unwrap().clone();
    let _coord = Proc(
        Command::new(rb)
            .args(["coord", "-listen", &format!("127.0.0.1:{coord_port}")])
            .args(["-peer", &format!("srv={peer_addr}")])
            .args(["-publish", &format!("{}:{}:1:srv", f.path, f.size)])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap(),
    );
    wait_port(coord_port);

    // Empty local cache: every byte comes from the Go peer.
    let local = tempfile::tempdir().unwrap();
    let cache = Arc::new(CacheDir::new(local.path(), opts()));
    let coord = Arc::new(CoordClient::new(&format!("127.0.0.1:{coord_port}"), "rust-node"));
    let t = TieredReader::new(cache).with_peers(coord, Arc::new(PeerClient::new(PeerClientConfig::default())));
    let mut buf = vec![0u8; 300 * 1024];
    let mut off = 0u64;
    let mut all = Vec::new();
    while (off as i64) < f.size {
        let n = t.read(&f.path, off, &mut buf).unwrap();
        assert!(n > 0);
        all.extend_from_slice(&buf[..n]);
        off += n as u64;
    }
    assert_eq!(all.len() as i64, f.size);
    assert_eq!(pattern::check(&all, 0), None);
    let nchunks = (f.size + MB - 1) / MB;
    assert_eq!(t.stats.promotions.load(std::sync::atomic::Ordering::Relaxed) as i64, nchunks);
    assert!(t.stats.peer_fetches.load(std::sync::atomic::Ordering::Relaxed) as i64 >= nchunks);

    // The promoted chunks are in the Go layout: Go's own ReadRange serves
    // them from the local dir with no remote tier at all.
    let out = Command::new(rb)
        .args(["verify", "-dir"])
        .arg(local.path())
        .args(["-path", &f.path, "-size", &f.size.to_string(), "-chunk-mb", "1", "-stride-mb", "1"])
        .output()
        .unwrap();
    assert!(out.status.success(), "go verify of rust-promoted chunks: {}", String::from_utf8_lossy(&out.stderr));
    // No staging files left behind.
    let leftovers: Vec<_> = std::fs::read_dir(local.path().join("models/a"))
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.file_name().into_string().unwrap())
        .filter(|n| n.starts_with(".nvme-"))
        .collect();
    assert!(leftovers.is_empty(), "{leftovers:?}");
}
