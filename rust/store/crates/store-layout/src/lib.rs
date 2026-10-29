//! How the Go fuse-client lays out its cache directory, and how its HTTP
//! handlers resolve a request to bytes, ported so a front server can answer
//! the same requests from the same directory and get the same answer.
//!
//! Only the resolution rules live here (no I/O policy, no HTTP): paths
//! ([`clean_path`], [`local_path`]), chunk addressing ([`chunk`]), the peer
//! read target ([`peer_target`], Go's `LocalFilePath` + `LocalChunkFile`),
//! `off`/`len` sub-ranges ([`sub_range`], Go's `serveSubRange`), OCI byte
//! ranges and digests ([`parse_byte_range`], [`digest_hex`]), and node-init's
//! config file ([`NodeInit`]).

pub mod chunk;

use std::fs::File;
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};

/// A request parameter the Go client would reject (400 / 416).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Invalid;

impl std::fmt::Display for Invalid {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("invalid request parameter")
    }
}

impl std::error::Error for Invalid {}

/// The cache namespace OCI blobs live in: `/oci/sha256/<hex>`.
pub const OCI_BLOB_PREFIX: &str = "/oci/sha256/";

/// Go's `ociBlobMaxRange`: one GET returns at most this many bytes.
pub const OCI_MAX_RANGE: u64 = 64 << 20;

/// Go `filepath.Clean("/" + raw)` (`sanitizePath`): resolves `.`, `..` and
/// duplicate slashes against a rooted path, so the result can never escape
/// the root.
pub fn clean_path(raw: &str) -> String {
    let mut parts: Vec<&str> = Vec::new();
    for seg in raw.split('/') {
        match seg {
            "" | "." => {}
            ".." => {
                parts.pop();
            }
            s => parts.push(s),
        }
    }
    let mut out = String::with_capacity(raw.len() + 1);
    out.push('/');
    out.push_str(&parts.join("/"));
    out
}

/// `<root>/<clean path>`.
pub fn local_path(root: &Path, path: &str) -> PathBuf {
    let c = clean_path(path);
    root.join(&c[1..])
}

/// A region of a local file to send.
#[derive(Debug)]
pub struct Target {
    pub file: File,
    pub path: PathBuf,
    pub offset: u64,
    pub length: u64,
}

/// Shape of a path as published to the coordinator (a location row).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Published {
    pub size: i64,
    pub chunked: bool,
    pub chunk_size: i64,
}

/// Why a peer read was not resolved locally.
#[derive(Debug, PartialEq, Eq)]
pub enum Unresolved {
    /// Neither a literal local file nor a chunk of a local whole parent.
    NotLocal,
    /// A `_chunk_N` of a local whole parent whose stride is not confirmed
    /// (no published chunked row, or its size differs from the file). Go
    /// may still serve it from its in-memory index; the caller forwards.
    StrideUnknown,
}

/// Resolve `/api/peer/read?path=<p>` the way Go's `handlePeerRead` does:
///
/// 1. a literal regular file at `<root>/<p>` is served whole (`LocalFilePath`
///    serves any local file, including literal `_chunk_N` files);
/// 2. `<parent>_chunk_N` where `<parent>` is a local whole file is served as
///    bytes `[N*stride, N*stride+stride)` clamped to the file, but only at
///    the stride the file was published with and only when the published
///    size equals the local size (`LocalChunkFile` +
///    `publishedChunkSize`). `published(parent, local_size)` asks the
///    coordinator.
///
/// Go additionally consults its in-memory index (a parent written on this
/// node knows its stride without the coordinator); this function cannot,
/// so it reports [`Unresolved::StrideUnknown`] and the caller forwards to
/// Go rather than guess.
pub fn peer_target(root: &Path, req_path: &str, published: impl FnOnce(&str, i64) -> Option<Published>) -> Result<Target, Unresolved> {
    let p = clean_path(req_path);
    let lp = local_path(root, &p);
    if let Ok(f) = File::open(&lp) {
        if let Ok(md) = f.metadata() {
            if md.is_file() {
                return Ok(Target { file: f, path: lp, offset: 0, length: md.size() });
            }
        }
    }
    let Some((parent, idx)) = chunk::parse_chunk_path(&p) else { return Err(Unresolved::NotLocal) };
    if idx < 0 {
        return Err(Unresolved::NotLocal);
    }
    let pp = local_path(root, parent);
    let Ok(f) = File::open(&pp) else { return Err(Unresolved::NotLocal) };
    let Ok(md) = f.metadata() else { return Err(Unresolved::NotLocal) };
    if !md.is_file() {
        return Err(Unresolved::NotLocal);
    }
    let size = md.size() as i64;
    let stride = match published(parent, size) {
        Some(pb) if pb.chunked && pb.chunk_size > 0 && pb.size == size => pb.chunk_size,
        _ => return Err(Unresolved::StrideUnknown),
    };
    let Some(offset) = idx.checked_mul(stride) else { return Err(Unresolved::NotLocal) };
    if offset >= size {
        // Go: offset past the end -> not served (404).
        return Err(Unresolved::NotLocal);
    }
    let length = stride.min(size - offset);
    Ok(Target { file: f, path: pp, offset: offset as u64, length: length as u64 })
}

/// Go `peerReadSubRange` + `serveSubRange`: `[sub_off, sub_off+sub_len)` of
/// an object held at `[offset, offset+length)`. `None` when `sub_off` is at
/// or past the object's end (Go answers 416). `sub_len` is clamped to the
/// object without computing `sub_off + sub_len` (which can overflow).
pub fn sub_range(offset: u64, length: u64, sub_off: u64, sub_len: u64) -> Option<(u64, u64)> {
    if sub_off >= length {
        return None;
    }
    let n = sub_len.min(length - sub_off);
    Some((offset + sub_off, n))
}

/// Parse the optional `off`/`len` query parameters. `Ok(None)` when both are
/// absent; `Err` for anything Go rejects with 400 (non-numeric, negative
/// offset, non-positive length, only one of the two).
pub fn parse_off_len(off: Option<&str>, len: Option<&str>) -> Result<Option<(u64, u64)>, Invalid> {
    let (off, len) = match (off.filter(|s| !s.is_empty()), len.filter(|s| !s.is_empty())) {
        (None, None) => return Ok(None),
        (o, l) => (o.unwrap_or(""), l.unwrap_or("")),
    };
    let o: i64 = parse_go_int(off).ok_or(Invalid)?;
    let n: i64 = parse_go_int(len).ok_or(Invalid)?;
    if o < 0 || n <= 0 {
        return Err(Invalid);
    }
    Ok(Some((o as u64, n as u64)))
}

/// Go `strconv.ParseInt(s, 10, 64)`.
fn parse_go_int(s: &str) -> Option<i64> {
    let digits = s.strip_prefix(['+', '-']).unwrap_or(s);
    if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    s.parse().ok()
}

/// `sha256:<64 hex>` or `<64 hex>` -> the hex (Go's `ociDigestHex`).
pub fn digest_hex(d: &str) -> Option<&str> {
    let h = d.strip_prefix("sha256:").unwrap_or(d);
    (h.len() == 64 && h.bytes().all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))).then_some(h)
}

/// Go `parseByteRange`: a single `bytes=a-b` or `bytes=a-`, end clamped to
/// the blob. Anything else is an error (Go: 416).
pub fn parse_byte_range(header: &str, size: u64) -> Result<(u64, u64), Invalid> {
    if size == 0 {
        return Err(Invalid);
    }
    let spec = header.strip_prefix("bytes=").ok_or(Invalid)?;
    if spec.contains(',') {
        return Err(Invalid);
    }
    let (a, b) = spec.split_once('-').ok_or(Invalid)?;
    if a.is_empty() {
        return Err(Invalid);
    }
    let start = parse_go_int(a).ok_or(Invalid)?;
    if start < 0 || start as u64 >= size {
        return Err(Invalid);
    }
    let mut end = size - 1;
    if !b.is_empty() {
        let e = parse_go_int(b).ok_or(Invalid)?;
        if e < start {
            return Err(Invalid);
        }
        end = (e as u64).min(size - 1);
    }
    Ok((start as u64, end))
}

/// The local whole copy of an OCI blob when it holds exactly `size` bytes
/// (Go's `openLocalExact`: a short file still landing, or one of another
/// size, is never served).
pub fn blob_exact(root: &Path, hex: &str, size: u64) -> Option<(File, PathBuf)> {
    let p = local_path(root, &format!("{OCI_BLOB_PREFIX}{hex}"));
    let f = File::open(&p).ok()?;
    let md = f.metadata().ok()?;
    (md.is_file() && md.size() == size && size > 0).then_some((f, p))
}

/// The digest of a whole OCI blob file under `root` (`<root>/oci/sha256/<hex>`,
/// no chunk suffix), if `path` is one. Literal chunk files return `None`:
/// they are not whole blobs and carry no digest of their own.
pub fn blob_hex_of(path: &Path, root: &Path) -> Option<String> {
    let rel = path.strip_prefix(root).ok()?.to_str()?;
    let hex = rel.strip_prefix(&OCI_BLOB_PREFIX[1..])?;
    (hex.len() == 64 && hex.bytes().all(|c| c.is_ascii_digit() || (b'a'..=b'f').contains(&c))).then(|| hex.to_string())
}

/// The whole blob at exactly the version Go verified against its digest
/// (`size`, mtime in ns). Checked on the opened file, so a copy rewritten
/// after Go's check (new mtime) is never served.
pub fn blob_verified(root: &Path, hex: &str, size: u64, mtime_ns: i64) -> Option<(File, PathBuf)> {
    let (f, p) = blob_exact(root, hex, size)?;
    let md = f.metadata().ok()?;
    let m = md.mtime() * 1_000_000_000 + md.mtime_nsec();
    (m == mtime_ns).then_some((f, p))
}

/// node-init's discovered cache location (`node-init.json`), as the Go client
/// reads it: the cache dir is `cache_dir` under the host-root prefix.
#[derive(Debug, serde::Deserialize)]
pub struct NodeInit {
    pub cache_dir: String,
}

impl NodeInit {
    pub fn read(path: &Path) -> std::io::Result<NodeInit> {
        let raw = std::fs::read(path)?;
        serde_json::from_slice(&raw).map_err(std::io::Error::other)
    }

    /// `host_root` joined with `cache_dir` (Go: `filepath.Join(root, dir)`).
    pub fn resolve(&self, host_root: &Path) -> PathBuf {
        host_root.join(self.cache_dir.trim_start_matches('/'))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    #[test]
    fn clean_path_is_go_clean() {
        assert_eq!(clean_path("a/b"), "/a/b");
        assert_eq!(clean_path("/../../etc/passwd"), "/etc/passwd");
        assert_eq!(clean_path("//a//./b/../c"), "/a/c");
        assert_eq!(clean_path(""), "/");
    }

    #[test]
    fn sub_range_matches_go() {
        // Go TestPeerReadSubRange: chunk 1 of a 3 MiB file at 1 MiB stride,
        // off=100 len=50; and chunk 2 off=1048000 len=1000 clamps to 576.
        let mib = 1u64 << 20;
        assert_eq!(sub_range(mib, mib, 100, 50), Some((mib + 100, 50)));
        assert_eq!(sub_range(2 * mib, mib, 1_048_000, 1000), Some((2 * mib + 1_048_000, 576)));
        assert_eq!(sub_range(0, 10, 10, 1), None); // off at end -> 416
        assert_eq!(sub_range(0, 10, 9, u64::MAX), Some((9, 1))); // no overflow
    }

    #[test]
    fn off_len_parse_matches_go() {
        assert_eq!(parse_off_len(None, None), Ok(None));
        assert_eq!(parse_off_len(Some("5"), Some("7")), Ok(Some((5, 7))));
        assert_eq!(parse_off_len(Some("5"), None), Err(Invalid));
        assert_eq!(parse_off_len(Some("-1"), Some("7")), Err(Invalid));
        assert_eq!(parse_off_len(Some("1"), Some("0")), Err(Invalid));
        assert_eq!(parse_off_len(Some("x"), Some("1")), Err(Invalid));
    }

    #[test]
    fn byte_range_matches_go() {
        assert_eq!(parse_byte_range("bytes=0-9", 100), Ok((0, 9)));
        assert_eq!(parse_byte_range("bytes=90-", 100), Ok((90, 99)));
        assert_eq!(parse_byte_range("bytes=90-200", 100), Ok((90, 99)));
        for bad in ["bytes=100-", "bytes=-5", "bytes=5-4", "bytes=0-1,4-5", "items=0-1", "bytes=a-1"] {
            assert_eq!(parse_byte_range(bad, 100), Err(Invalid), "{bad}");
        }
        assert_eq!(parse_byte_range("bytes=0-1", 0), Err(Invalid));
    }

    #[test]
    fn digest_forms() {
        let h = "0123456789abcdef".repeat(4);
        assert_eq!(digest_hex(&h), Some(h.as_str()));
        assert_eq!(digest_hex(&format!("sha256:{h}")), Some(h.as_str()));
        assert_eq!(digest_hex(&h.to_uppercase()), None);
        assert_eq!(digest_hex("sha256:abc"), None);
    }

    fn write(root: &Path, rel: &str, n: usize) {
        let p = root.join(rel);
        std::fs::create_dir_all(p.parent().unwrap()).unwrap();
        let mut f = File::create(p).unwrap();
        f.write_all(&vec![7u8; n]).unwrap();
    }

    #[test]
    fn peer_target_rules() {
        let d = tempfile::tempdir().unwrap();
        let r = d.path();
        write(r, "m/whole.bin", 10 * 4 + 3); // 43 bytes, stride 4 -> 11 chunks
        write(r, "m/lit.bin_chunk_2", 5);
        // Literal file, whole.
        let t = peer_target(r, "/m/lit.bin_chunk_2", |_, _| None).unwrap();
        assert_eq!((t.offset, t.length), (0, 5));
        // Chunk of a whole parent at the published stride.
        let pubd = |_: &str, _: i64| Some(Published { size: 43, chunked: true, chunk_size: 4 });
        let t = peer_target(r, "/m/whole.bin_chunk_3", pubd).unwrap();
        assert_eq!((t.offset, t.length), (12, 4));
        let t = peer_target(r, "/m/whole.bin_chunk_10", pubd).unwrap();
        assert_eq!((t.offset, t.length), (40, 3)); // short last chunk
        assert_eq!(peer_target(r, "/m/whole.bin_chunk_11", pubd).unwrap_err(), Unresolved::NotLocal);
        // Unknown or mismatched stride: never guess.
        assert_eq!(peer_target(r, "/m/whole.bin_chunk_1", |_, _| None).unwrap_err(), Unresolved::StrideUnknown);
        let wrong_size = |_: &str, _: i64| Some(Published { size: 44, chunked: true, chunk_size: 4 });
        assert_eq!(peer_target(r, "/m/whole.bin_chunk_1", wrong_size).unwrap_err(), Unresolved::StrideUnknown);
        let unchunked = |_: &str, _: i64| Some(Published { size: 43, chunked: false, chunk_size: 0 });
        assert_eq!(peer_target(r, "/m/whole.bin_chunk_1", unchunked).unwrap_err(), Unresolved::StrideUnknown);
        // Not local at all; traversal stays inside the root.
        assert_eq!(peer_target(r, "/nope", |_, _| None).unwrap_err(), Unresolved::NotLocal);
        assert_eq!(peer_target(r, "/../../m/lit.bin_chunk_2", |_, _| None).unwrap().length, 5);
        assert_eq!(peer_target(r, "/m", |_, _| None).unwrap_err(), Unresolved::NotLocal);
        // a dir
    }

    #[test]
    fn blob_exact_refuses_short_files() {
        let d = tempfile::tempdir().unwrap();
        let h = "ab".repeat(32);
        write(d.path(), &format!("oci/sha256/{h}"), 100);
        assert!(blob_exact(d.path(), &h, 100).is_some());
        assert!(blob_exact(d.path(), &h, 101).is_none()); // short
        assert!(blob_exact(d.path(), &h, 99).is_none());
    }

    #[test]
    fn node_init_resolves_under_host_root() {
        let d = tempfile::tempdir().unwrap();
        let p = d.path().join("node-init.json");
        std::fs::write(&p, r#"{"version":1,"cache_dir":"/skyfleet-fuse-cache","device":"/dev/root"}"#).unwrap();
        let n = NodeInit::read(&p).unwrap();
        assert_eq!(n.resolve(Path::new("/host-root")), PathBuf::from("/host-root/skyfleet-fuse-cache"));
    }
}
