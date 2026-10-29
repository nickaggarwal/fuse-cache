//! Native PUT: the body is streamed to a staged file beside its final path
//! and hashed on the way in; Go then adopts the staged file through
//! `POST /api/internal/commit` (Go `commit.go`) and stays the only writer of
//! the index, the coordinator rows and the cloud copy.
//!
//! Every outcome either stores the bytes through Go or leaves nothing: the
//! staged file is removed on any failure here, and Go removes it on any
//! refusal. Go re-checks size, digest and path on commit, so a bug here can
//! refuse a write but cannot store a wrong one.
//!
//! What stays Go's: a PUT this side cannot frame exactly (chunked bodies,
//! `Expect: 100-continue`, no Content-Length) is forwarded unchanged, as are
//! uploads over `max_upload` (Go answers 413 itself).

use crate::http::{self, Conn, RequestHead};
use sha2::{Digest, Sha256};
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use store_layout as layout;

/// Why a native PUT did not store anything, and what the client is told.
#[derive(Debug)]
pub enum PutError {
    /// The client's body ended early or its connection failed: nothing is
    /// answered (the connection is gone or unusable).
    Client(io::Error),
    /// The body hashed to something else than the blob path's digest (Go's
    /// own answer for the same case: 400).
    DigestMismatch(String),
    /// Staging on local disk failed (500, as Go's "Failed to create staging
    /// file" / "Failed to flush staging file").
    Stage(io::Error),
    /// Go refused or could not be reached for the commit: its status and
    /// body are relayed (502 when unreachable).
    Commit(u16, String),
}

/// What a PUT path stages to: `<cache>/<clean path>.stage-front-<n>`, a
/// name Go's commit accepts and its startup rehydration skips.
pub struct Staged {
    pub file_path: String,
    pub abs: PathBuf,
    pub rel: String,
}

static SEQ: AtomicU64 = AtomicU64::new(0);

/// Plan the staged file for `PUT /api/files/<raw>`; `None` for paths Go
/// would not store as a file (the root).
pub fn staged_for(cache_dir: &Path, raw: &str) -> Option<Staged> {
    let file_path = layout::clean_path(raw);
    // Go's router never stores a path that is not already clean: mux
    // redirects `.`/`..`/`//` paths (301) before the handler runs. Only a
    // path whose cleaned form is itself is native; the rest get Go's answer.
    if file_path == "/" || file_path[1..] != *raw {
        return None;
    }
    let rel = format!("{}.stage-front-{}-{}", &file_path[1..], std::process::id(), SEQ.fetch_add(1, Ordering::Relaxed));
    Some(Staged { abs: cache_dir.join(&rel), rel, file_path })
}

/// The hex digest a blob path names (Go `ociBlobDigestFromPath`), if any.
pub fn blob_digest(file_path: &str) -> Option<&str> {
    let hex = file_path.strip_prefix(layout::OCI_BLOB_PREFIX)?;
    (hex.len() == 64 && hex.bytes().all(|c| c.is_ascii_digit() || (b'a'..=b'f').contains(&c))).then_some(hex)
}

/// Removes the staged file unless disarmed (after Go adopted it).
struct Cleanup<'a>(Option<&'a Path>);
impl Drop for Cleanup<'_> {
    fn drop(&mut self) {
        if let Some(p) = self.0 {
            let _ = fs::remove_file(p);
        }
    }
}

/// Stream exactly `len` body bytes from `c` into the staged file, hash them,
/// and ask Go to commit. Returns the stored size.
pub fn put(c: &mut Conn, st: &Staged, len: u64, backend: &str, api_key: Option<&str>, timeout: Duration) -> Result<u64, PutError> {
    if let Some(parent) = st.abs.parent() {
        fs::create_dir_all(parent).map_err(PutError::Stage)?;
    }
    let mut f = OpenOptions::new().write(true).create_new(true).open(&st.abs).map_err(PutError::Stage)?;
    let mut guard = Cleanup(Some(&st.abs));
    let mut h = Sha256::new();
    copy_hashed(c, len, &mut f, &mut h)?;
    f.sync_data().map_err(PutError::Stage)?;
    drop(f);
    let got = hex(&h.finalize());
    if let Some(want) = blob_digest(&st.file_path) {
        if got != want {
            return Err(PutError::DigestMismatch(got));
        }
    }
    commit(backend, api_key, st, len, &got, timeout)?;
    // Go renamed it into place (or removed it on refusal, handled above).
    guard.0 = None;
    Ok(len)
}

/// Body → file + hasher. A client-side failure is `Client`, a disk failure
/// `Stage`, so the caller answers (or not) correctly.
fn copy_hashed(c: &mut Conn, mut n: u64, f: &mut File, h: &mut Sha256) -> Result<(), PutError> {
    let mut sink = HashWrite { f, h };
    // Buffered bytes first, then the socket; Conn::copy_exact mixes read and
    // write errors, so split them here.
    let have = c.buffered().len().min(n as usize);
    if have > 0 {
        let b = c.take(have);
        sink.write_all(&b).map_err(PutError::Stage)?;
        n -= have as u64;
    }
    let mut buf = vec![0u8; 1 << 20];
    while n > 0 {
        let want = buf.len().min(n as usize);
        let got = match c.stream.read(&mut buf[..want]) {
            Ok(0) => return Err(PutError::Client(io::Error::new(io::ErrorKind::UnexpectedEof, "body ended early"))),
            Ok(k) => k,
            Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
            Err(e) => return Err(PutError::Client(e)),
        };
        sink.write_all(&buf[..got]).map_err(PutError::Stage)?;
        n -= got as u64;
    }
    Ok(())
}

struct HashWrite<'a> {
    f: &'a mut File,
    h: &'a mut Sha256,
}
impl Write for HashWrite<'_> {
    fn write(&mut self, b: &[u8]) -> io::Result<usize> {
        let n = self.f.write(b)?;
        self.h.update(&b[..n]);
        Ok(n)
    }
    fn flush(&mut self) -> io::Result<()> {
        self.f.flush()
    }
}

fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

/// `POST /api/internal/commit` on the Go client, with the API key when one
/// is set (Go's middleware covers this route like every other).
fn commit(backend: &str, api_key: Option<&str>, st: &Staged, size: u64, sha: &str, timeout: Duration) -> Result<(), PutError> {
    let body = serde_json::json!({ "path": st.file_path, "staged": st.rel, "size": size, "sha256": sha }).to_string();
    let s = http::connect(backend, Duration::from_secs(2)).map_err(|e| PutError::Commit(502, format!("backend unavailable: {e}")))?;
    s.set_read_timeout(Some(timeout)).ok();
    s.set_write_timeout(Some(timeout)).ok();
    let mut c = Conn::new(s);
    let key = api_key.map(|k| format!("X-API-Key: {k}\r\n")).unwrap_or_default();
    let req = format!(
        "POST /api/internal/commit HTTP/1.1\r\nHost: {backend}\r\nUser-Agent: store-front\r\n{key}Content-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    c.stream.write_all(req.as_bytes()).map_err(|e| PutError::Commit(502, format!("backend write failed: {e}")))?;
    let n = match c.read_head() {
        Ok(Some(n)) => n,
        _ => return Err(PutError::Commit(502, "backend closed during commit".into())),
    };
    let rh = http::parse_response(&c.take(n)).map_err(|_| PutError::Commit(502, "bad backend response".into()))?;
    if rh.status == 200 {
        return Ok(());
    }
    let mut msg = Vec::new();
    if let Some(len) = rh.content_length {
        let _ = c.copy_exact(len.min(4096), &mut msg);
    }
    Err(PutError::Commit(rh.status, String::from_utf8_lossy(&msg).trim().to_string()))
}

/// Can this PUT be handled natively? Only an exactly framed body under the
/// upload cap; everything else is Go's.
pub fn native_put(req: &RequestHead, max_upload: u64) -> Option<u64> {
    if req.chunked || req.expect_continue {
        return None;
    }
    let n = req.content_length?;
    (n <= max_upload).then_some(n)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn blob_digest_matches_go() {
        let d = "a".repeat(64);
        assert_eq!(blob_digest(&format!("/oci/sha256/{d}")), Some(d.as_str()));
        assert_eq!(blob_digest(&format!("/oci/sha256/{}", "A".repeat(64))), None);
        assert_eq!(blob_digest(&format!("/oci/sha256/{}_chunk_0", "a".repeat(58))), None);
        assert_eq!(blob_digest("/oci/sha256/abc"), None);
        assert_eq!(blob_digest("/models/x"), None);
    }

    #[test]
    fn staged_name_is_a_go_stage_sibling() {
        let st = staged_for(Path::new("/c"), "m/f.bin").unwrap();
        assert_eq!(st.file_path, "/m/f.bin");
        assert!(st.rel.starts_with("m/f.bin.stage-front-"), "{}", st.rel);
        assert_eq!(st.abs, Path::new("/c").join(&st.rel));
        // Not already clean: Go's router answers these, so they are not native.
        for raw in ["", "a/../m/f.bin", "./x", "a//b", "a/", "../x"] {
            assert!(staged_for(Path::new("/c"), raw).is_none(), "{raw}");
        }
    }
}
