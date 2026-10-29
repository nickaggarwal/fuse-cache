//! The native PUT path (stage + hash here, Go commits) against the real Go
//! API, and the read path's error accounting:
//!
//! * differential: the same PUTs sent to Go directly and through the front
//!   store identical bytes and get identical status codes, for small,
//!   chunked, empty, nested and blob paths, wrong digests and oversize;
//! * failures leave nothing: a truncated body, a wrong digest, Go down at
//!   commit time, a client that hangs up mid-upload — no stored file, no
//!   staged file left in the cache dir;
//! * concurrency: many PUTs of the same blob and of different files, with
//!   readers on the same paths, end with every file whole and correct;
//! * Go restarted after native PUTs rehydrates them (the staged names are
//!   skipped, the committed files are indexed);
//! * a reader that hangs up mid-response counts as `aborted`, not `error`.

mod common;
use common::*;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::time::Duration;
use store_front::metrics::{Outcome, Route};

fn stage_files(dir: &std::path::Path) -> Vec<String> {
    let mut out = Vec::new();
    let mut stack = vec![dir.to_path_buf()];
    while let Some(d) = stack.pop() {
        for e in std::fs::read_dir(&d).unwrap().flatten() {
            let p = e.path();
            if p.is_dir() {
                stack.push(p);
            } else if p.file_name().unwrap().to_string_lossy().contains(".stage-") {
                out.push(p.display().to_string());
            }
        }
    }
    out
}

fn get_file(addr: &str, path: &str) -> (u16, Vec<u8>) {
    let (c, _, b) = req(addr, "GET", &format!("/api/files{path}"), &[], b"");
    (c, b)
}

#[test]
fn put_differential_go_vs_front() {
    let Some(rb) = readbench() else { return };
    // Two rigs: one written through Go directly, one through the front.
    let g = rig(rb, None, 64);
    let f = rig(rb, None, 64);
    let blob = pattern(2 * 1024 * 1024 + 5, 3);
    let d = sha256_hex(&blob);
    let bad = sha256_hex(b"something else");
    let cases: Vec<(String, Vec<u8>)> = vec![
        ("/small.txt".into(), b"hello".to_vec()),
        ("/empty".into(), Vec::new()),
        ("/models/m/r1/model-00001.safetensors".into(), pattern(3 * 1024 * 1024 + 11, 9)), // > 1 MiB chunk size: chunked
        (format!("/oci/sha256/{d}"), blob.clone()),
        (format!("/oci/sha256/{bad}"), blob.clone()), // wrong digest: 400 from both
        ("/overwrite.bin".into(), pattern(1000, 1)),
        ("/overwrite.bin".into(), pattern(2000, 2)), // replaced
    ];
    for (path, body) in &cases {
        let cg = put(&g.go, path, body, None);
        let cf = put(&f.front, path, body, None);
        assert_eq!(cg, cf, "PUT {path}: Go {cg}, front {cf}");
    }
    let native = f.srv.metrics.count(Route::Put, Outcome::Local);
    assert_eq!(native, 6, "every storable PUT went native");
    assert_eq!(f.srv.metrics.count(Route::Put, Outcome::Rejected), 1, "the wrong digest was refused natively");
    for (path, _) in &cases {
        let (sg, bg) = get_file(&g.go, path);
        let (sf, bf) = get_file(&f.go, path); // read from Go: the commit indexed it
        assert_eq!((sg, bg.len()), (sf, bf.len()), "GET {path}");
        assert!(bg == bf, "GET {path}: bytes differ");
    }
    assert!(stage_files(&f.dir).is_empty(), "staged files left: {:?}", stage_files(&f.dir));
    // The committed blob is served natively on the blob route.
    let (code, _, b) = req(&f.front, "GET", &format!("/oci/blobs/{d}"), &[], b"");
    assert_eq!((code, b == blob), (200, true));
}

#[test]
fn failed_puts_leave_nothing() {
    let Some(rb) = readbench() else { return };
    let mut r = rig(rb, None, 64);
    let body = pattern(1 << 20, 4);

    // Client hangs up after half the body.
    {
        let mut s = TcpStream::connect(&r.front).unwrap();
        write!(s, "PUT /api/files/cut.bin HTTP/1.1\r\nHost: t\r\nContent-Length: {}\r\n\r\n", body.len()).unwrap();
        s.write_all(&body[..body.len() / 2]).unwrap();
    } // dropped: connection closed mid-body
    let t = std::time::Instant::now();
    while r.srv.metrics.count(Route::Put, Outcome::Aborted) == 0 && t.elapsed() < Duration::from_secs(10) {
        std::thread::sleep(Duration::from_millis(20));
    }
    assert_eq!(r.srv.metrics.count(Route::Put, Outcome::Aborted), 1);
    assert_eq!(get_file(&r.go, "/cut.bin").0, 404, "a partial upload was stored");

    // Go down at commit time: 502 to the client, nothing stored, nothing staged.
    let port: u16 = r.go.rsplit(':').next().unwrap().parse().unwrap();
    drop(r._procs.pop());
    while TcpStream::connect(("127.0.0.1", port)).is_ok() {
        std::thread::sleep(Duration::from_millis(20));
    }
    assert_eq!(put(&r.front, "/nogo.bin", &body, None), 502);
    assert!(stage_files(&r.dir).is_empty(), "staged files left: {:?}", stage_files(&r.dir));
    r._procs.push(Proc(
        std::process::Command::new(&r.rb).args(&r.go_args).stdout(std::process::Stdio::null()).stderr(std::process::Stdio::null()).spawn().unwrap(),
    ));
    wait_port(port);
    assert_eq!(get_file(&r.go, "/nogo.bin").0, 404);
    assert!(!r.dir.join("nogo.bin").exists());

    // Wrong digest: 400, nothing stored.
    let d = sha256_hex(&body);
    assert_eq!(put(&r.front, &format!("/oci/sha256/{d}"), b"not it", None), 400);
    assert!(!r.dir.join(format!("oci/sha256/{d}")).exists());

    // Over the cap: forwarded, Go answers 413 (cap = 2 MiB here).
    let mut cfg = r.srv.cfg.clone();
    cfg.max_upload = 2 << 20;
    let small = store_front::server::Server::new(cfg);
    let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = l.local_addr().unwrap().to_string();
    let s2 = small.clone();
    std::thread::spawn(move || s2.serve(l, Duration::from_secs(1)));
    let big = pattern(3 << 20, 1);
    let code = put(&addr, "/big.bin", &big, None);
    assert_eq!(small.metrics.count(Route::Put, Outcome::Forwarded), 1, "oversize went to Go");
    assert_eq!(code, 200, "Go's own cap (2 GiB in serve-api) accepts it");

    assert!(stage_files(&r.dir).is_empty(), "staged files left: {:?}", stage_files(&r.dir));
}

#[test]
fn concurrent_puts_and_reads() {
    let Some(rb) = readbench() else { return };
    let r = rig(rb, None, 64);
    let blob = pattern(3 * 1024 * 1024 + 1, 7);
    let d = sha256_hex(&blob);
    let front = r.front.clone();
    let mut hs = Vec::new();
    // 8 writers of the same blob, 8 writers of distinct files, 8 readers.
    for i in 0..8u8 {
        let (f, b, dd) = (front.clone(), blob.clone(), d.clone());
        hs.push(std::thread::spawn(move || assert_eq!(put(&f, &format!("/oci/sha256/{dd}"), &b, None), 200)));
        let f = front.clone();
        hs.push(std::thread::spawn(move || {
            let body = pattern(1024 * 1024 + i as usize * 4099, i);
            assert_eq!(put(&f, &format!("/many/f{i}.bin"), &body, None), 200);
        }));
        let (f, d) = (front.clone(), d.clone());
        hs.push(std::thread::spawn(move || {
            // A reader either misses (Go's answer) or gets whole, correct
            // bytes: never a torn file.
            for _ in 0..20 {
                let (code, _, b) = req(&f, "GET", &format!("/oci/blobs/{d}"), &[], b"");
                if code == 200 {
                    assert_eq!(b.len(), 3 * 1024 * 1024 + 1);
                }
                std::thread::sleep(Duration::from_millis(5));
            }
        }));
    }
    for h in hs {
        h.join().unwrap();
    }
    let (code, _, b) = req(&front, "GET", &format!("/oci/blobs/{d}"), &[], b"");
    assert_eq!((code, b == blob), (200, true));
    for i in 0..8u8 {
        let (code, b) = get_file(&r.go, &format!("/many/f{i}.bin"));
        assert_eq!((code, b == pattern(1024 * 1024 + i as usize * 4099, i)), (200, true), "f{i}");
    }
    assert!(stage_files(&r.dir).is_empty(), "staged files left: {:?}", stage_files(&r.dir));
}

#[test]
fn go_restart_rehydrates_native_puts() {
    let Some(rb) = readbench() else { return };
    let mut r = rig(rb, None, 64);
    let body = pattern(2 * 1024 * 1024 + 3, 5);
    let d = sha256_hex(&body);
    assert_eq!(put(&r.front, &format!("/oci/sha256/{d}"), &body, None), 200);
    assert_eq!(put(&r.front, "/w/after-restart.bin", &body, None), 200);
    // A stray staged file (a front killed mid-upload) must not be adopted.
    std::fs::write(r.dir.join("w/stray.bin.stage-front-1-1"), b"partial").unwrap();
    r.restart_go();
    assert_eq!(get_file(&r.go, "/w/after-restart.bin"), (200, body.clone()));
    let (code, _, b) = req(&r.front, "GET", &format!("/oci/blobs/{d}"), &[], b"");
    assert_eq!((code, b == body), (200, true));
    assert_eq!(get_file(&r.go, "/w/stray.bin").0, 404);
    let (_, _, cache) = req(&r.go, "GET", "/api/cache", &[], b"");
    assert!(!String::from_utf8_lossy(&cache).contains(".stage-"), "a staged file was indexed");
}

#[test]
fn reader_hangup_is_aborted_not_error() {
    let Some(rb) = readbench() else { return };
    let r = rig(rb, None, 64);
    let body = pattern(32 * 1024 * 1024, 8);
    let d = sha256_hex(&body);
    assert_eq!(put(&r.front, &format!("/oci/sha256/{d}"), &body, None), 200);
    for _ in 0..5 {
        let mut s = TcpStream::connect(&r.front).unwrap();
        write!(s, "GET /oci/blobs/{d} HTTP/1.1\r\nHost: t\r\n\r\n").unwrap();
        let mut b = [0u8; 4096];
        let _ = s.read(&mut b).unwrap();
        // Hang up with most of the 32 MiB unsent.
        let _ = s.shutdown(std::net::Shutdown::Both);
    }
    let t = std::time::Instant::now();
    while r.srv.metrics.count(Route::Blob, Outcome::Aborted) < 5 && t.elapsed() < Duration::from_secs(10) {
        std::thread::sleep(Duration::from_millis(20));
    }
    assert_eq!(r.srv.metrics.count(Route::Blob, Outcome::Error), 0);
    assert_eq!(r.srv.metrics.count(Route::Blob, Outcome::Aborted), 5);
}

/// A same-size corrupted blob on the store is never served natively and
/// never served by Go either: Go hashes it, moves it aside, and both answer
/// "not here" so the requester tries the next holder or the registry.
#[test]
fn corrupted_blob_is_not_served() {
    let Some(rb) = readbench() else { return };
    let r = rig(rb, None, 64);
    let body = pattern(3 * 1024 * 1024 + 9, 11);
    let d = sha256_hex(&body);
    assert_eq!(put(&r.front, &format!("/oci/sha256/{d}"), &body, None), 200);
    // Served natively right after the PUT: the commit marked it verified.
    let (code, _, b) = req(&r.front, "GET", &format!("/oci/blobs/{d}"), &[], b"");
    assert_eq!((code, b == body), (200, true));
    assert_eq!(r.srv.metrics.count(Route::Blob, Outcome::Local), 1);

    // Corrupt it in place, same size, keeping the mtime so only a hash can tell.
    let p = r.dir.join(format!("oci/sha256/{d}"));
    let mtime = std::fs::metadata(&p).unwrap().modified().unwrap();
    let mut f = std::fs::OpenOptions::new().write(true).open(&p).unwrap();
    std::io::Seek::seek(&mut f, std::io::SeekFrom::Start(1 << 20)).unwrap();
    f.write_all(&[0xA5; 4096]).unwrap();
    f.set_modified(mtime).unwrap();
    drop(f);
    // Same mtime: the cached verdict still holds until Go re-checks. A
    // rewrite with a new mtime is refused at once (below).
    let future = std::time::SystemTime::now() + Duration::from_secs(2);
    std::fs::File::options().write(true).open(&p).unwrap().set_modified(future).unwrap();
    std::thread::sleep(Duration::from_millis(5)); // past the front's version cache

    let local0 = r.srv.metrics.count(Route::Blob, Outcome::Local);
    // Neither the blob route nor a peer read hands out the bad bytes.
    for _ in 0..50 {
        let (code, _, b) = req(&r.front, "GET", &format!("/oci/blobs/{d}"), &[("Range", "bytes=0-2097151")], b"");
        assert!(code != 200 && code != 206 || b == body[..2097152], "bad bytes served: {code}");
        let (code, _, b) = req(&r.front, "GET", &format!("/api/peer/read?path=/oci/sha256/{d}"), &[], b"");
        assert!(code != 200 || b == body, "bad bytes served to a peer: {code}");
        if !p.exists() {
            break;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    assert_eq!(r.srv.metrics.count(Route::Blob, Outcome::Local), local0, "served natively after the rewrite");
    assert!(!p.exists(), "Go did not move the corrupt copy aside");
    let aside: Vec<_> = std::fs::read_dir(p.parent().unwrap()).unwrap().flatten().filter(|e| e.file_name().to_string_lossy().contains(".corrupt-")).collect();
    assert_eq!(aside.len(), 1, "corrupt copy kept for inspection");
    // The registry route now answers 503 so nydusd falls back.
    let (code, _, _) = req(&r.front, "GET", &format!("/v2/x/blobs/sha256:{d}"), &[("Range", "bytes=0-99")], b"");
    assert_eq!(code, 503);
    // A fresh good PUT restores service.
    assert_eq!(put(&r.front, &format!("/oci/sha256/{d}"), &body, None), 200);
    let (code, _, b) = req(&r.front, "GET", &format!("/oci/blobs/{d}"), &[], b"");
    assert_eq!((code, b == body), (200, true));
}
