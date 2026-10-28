//! OCI blob endpoint, the registry-proxy path nydusd reads image blobs
//! through. Same contract as Go's `internal/api/oci_blob.go`:
//!
//! * routes `/oci/blobs/{digest}` (a blob this node cannot find: 404) and
//!   `/v2/<repo>/blobs/{digest}` (503, so nydusd falls back to the origin
//!   registry), `HEAD` for the size, `GET` with `Range: bytes=a-b`;
//! * the digest is `sha256:<hex>` or `<hex>`; the blob lives in the cache at
//!   `/oci/sha256/<hex>`, whole or as `_chunk_N` files;
//! * a range is capped at 64 MiB (Content-Range tells the client); a GET
//!   without Range streams the whole blob with 200;
//! * the range is planned before the status line ([`CacheDir::plan_pieces`]):
//!   local pieces go out with `sendfile`, a piece that is not local is
//!   fetched as just that window from a holder, then the cloud (no
//!   read-ahead, no promotion). A failed fetch answers 502, again so nydusd
//!   falls back.
//!
//! Shape for a blob that is not local comes from the coordinator (the
//! hinted size and stride), as `LookupEntry` does in Go; misses are
//! remembered for 5 s (`ociMissTTL`) so a cold image's thousands of chunk
//! requests do not each become a coordinator lookup.

use crate::layout::{CacheDir, Piece};
use crate::peer::{send_more, sendfile_all, write_simple};
use crate::tiered::TieredReader;
use std::collections::HashMap;
use std::io::{self, Write};
use std::net::TcpStream;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

pub const OCI_BLOB_PREFIX: &str = "/oci/sha256/";
pub const MAX_RANGE: u64 = 64 << 20;
const MISS_TTL: Duration = Duration::from_secs(5);

#[derive(Default, Debug)]
pub struct BlobStats {
    pub requests: AtomicU64,
    pub local_bytes: AtomicU64,
    pub remote_bytes: AtomicU64,
    pub misses: AtomicU64,
    pub errors: AtomicU64,
}

pub struct BlobServer {
    cache: Arc<CacheDir>,
    reader: Arc<TieredReader>,
    misses: Mutex<HashMap<String, Instant>>,
    pub stats: BlobStats,
}

/// `sha256:<64 hex>` or `<64 hex>` → the 64 hex chars.
pub fn digest_hex(d: &str) -> Option<&str> {
    let h = d.strip_prefix("sha256:").unwrap_or(d);
    (h.len() == 64 && h.bytes().all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))).then_some(h)
}

/// Go `parseByteRange`: a single `bytes=a-b` or `bytes=a-`.
pub fn parse_byte_range(header: &str, size: u64) -> Result<(u64, u64), String> {
    if size == 0 {
        return Err("empty blob".into());
    }
    let spec = header.strip_prefix("bytes=").ok_or_else(|| format!("unsupported range {header:?}"))?;
    if spec.contains(',') {
        return Err(format!("unsupported range {header:?}"));
    }
    let (a, b) = spec.split_once('-').ok_or_else(|| format!("unsupported range {header:?}"))?;
    if a.is_empty() {
        return Err(format!("unsupported range {header:?}"));
    }
    let start: u64 = a.parse().map_err(|_| format!("range {header:?} outside blob of {size} bytes"))?;
    if start >= size {
        return Err(format!("range {header:?} outside blob of {size} bytes"));
    }
    let mut end = size - 1;
    if !b.is_empty() {
        end = b.parse().map_err(|_| format!("bad range {header:?}"))?;
        if end < start {
            return Err(format!("bad range {header:?}"));
        }
        end = end.min(size - 1);
    }
    Ok((start, end))
}

impl BlobServer {
    pub fn new(reader: Arc<TieredReader>) -> Arc<Self> {
        Arc::new(BlobServer { cache: reader.cache.clone(), reader, misses: Mutex::new(HashMap::new()), stats: BlobStats::default() })
    }

    /// Size of the blob at `path`, learning its shape from the coordinator
    /// when it is not local. `None` = this cluster does not have it.
    fn size_of(&self, path: &str) -> Option<u64> {
        if let Ok(n) = self.cache.size(path) {
            return Some(n.max(0) as u64);
        }
        {
            let mut m = self.misses.lock().unwrap();
            if let Some(t) = m.get(path) {
                if t.elapsed() < MISS_TTL {
                    return None;
                }
                m.remove(path);
            }
        }
        self.reader.learn_shape(path);
        match self.cache.size(path) {
            Ok(n) => Some(n.max(0) as u64),
            Err(_) => {
                let mut m = self.misses.lock().unwrap();
                if m.len() > 65536 {
                    m.clear();
                }
                m.insert(path.to_string(), Instant::now());
                None
            }
        }
    }

    pub fn serve(&self, s: &mut TcpStream, head: bool, digest: &str, range: Option<&str>, missing: u16) -> io::Result<()> {
        self.stats.requests.fetch_add(1, Ordering::Relaxed);
        let Some(hex) = digest_hex(digest) else {
            return write_simple(s, 400, "Bad Request", "blob id must be a sha256 hex digest\n", &[]);
        };
        let path = format!("{OCI_BLOB_PREFIX}{hex}");
        let Some(size) = self.size_of(&path) else {
            self.stats.misses.fetch_add(1, Ordering::Relaxed);
            let reason = if missing == 503 { "Service Unavailable" } else { "Not Found" };
            return write_simple(s, missing, reason, "blob not found\n", &[]);
        };
        if head {
            let h = format!(
                "HTTP/1.1 200 OK\r\nAccept-Ranges: bytes\r\nContent-Type: application/octet-stream\r\nDate: {}\r\nContent-Length: {size}\r\n\r\n",
                httpdate::fmt_http_date(std::time::SystemTime::now())
            );
            return s.write_all(h.as_bytes());
        }
        let Some(range) = range else {
            return self.serve_whole(s, &path, size);
        };
        let (start, mut end) = match parse_byte_range(range, size) {
            Ok(r) => r,
            Err(e) => {
                let cr = format!("bytes */{size}");
                return write_simple(s, 416, "Requested Range Not Satisfiable", &format!("{e}\n"), &[("Content-Range", &cr)]);
            }
        };
        if end - start + 1 > MAX_RANGE {
            end = start + MAX_RANGE - 1;
        }
        let pieces = match self.resolve(&path, start, end - start + 1) {
            Ok(p) => p,
            Err(e) => {
                self.stats.errors.fetch_add(1, Ordering::Relaxed);
                eprintln!("[BLOB] {} range {start}-{end}: {e}", &hex[..12]);
                return write_simple(s, 502, "Bad Gateway", "read failed\n", &[]);
            }
        };
        let n: u64 = pieces.iter().map(|p| p.len()).sum();
        let h = format!(
            "HTTP/1.1 206 Partial Content\r\nAccept-Ranges: bytes\r\nContent-Type: application/octet-stream\r\nDate: {}\r\nContent-Length: {n}\r\nContent-Range: bytes {start}-{}/{size}\r\n\r\n",
            httpdate::fmt_http_date(std::time::SystemTime::now()),
            start + n.max(1) - 1
        );
        send_more(s, h.as_bytes())?;
        self.write_pieces(s, &pieces)
    }

    fn serve_whole(&self, s: &mut TcpStream, path: &str, size: u64) -> io::Result<()> {
        let mut off = 0u64;
        while off < size {
            let n = MAX_RANGE.min(size - off);
            let pieces = match self.resolve(path, off, n) {
                Ok(p) => p,
                Err(e) => {
                    self.stats.errors.fetch_add(1, Ordering::Relaxed);
                    if off == 0 {
                        return write_simple(s, 502, "Bad Gateway", "read failed\n", &[]);
                    }
                    return Err(e); // cut short; the client sees it against Content-Length
                }
            };
            if off == 0 {
                let h = format!(
                    "HTTP/1.1 200 OK\r\nAccept-Ranges: bytes\r\nContent-Type: application/octet-stream\r\nDate: {}\r\nContent-Length: {size}\r\n\r\n",
                    httpdate::fmt_http_date(std::time::SystemTime::now())
                );
                send_more(s, h.as_bytes())?;
            }
            self.write_pieces(s, &pieces)?;
            off += n;
        }
        Ok(())
    }

    /// Plan, then fetch every remote piece into memory, so a failure can
    /// still become a 502 before the status line (Go resolves pieces first
    /// for the same reason).
    fn resolve(&self, path: &str, off: u64, len: u64) -> io::Result<Vec<Resolved>> {
        let plan = self.cache.plan_pieces(path, off, len)?;
        let mut out = Vec::with_capacity(plan.len());
        for p in plan {
            match p {
                Piece::Local { file, off, len } => {
                    self.stats.local_bytes.fetch_add(len, Ordering::Relaxed);
                    out.push(Resolved::Local(file, off, len));
                }
                Piece::Remote { object, off, len } => {
                    if !self.reader.has_remote() {
                        return Err(io::Error::new(io::ErrorKind::NotFound, format!("{object} not local and no remote tier")));
                    }
                    let mut buf = vec![0u8; len as usize];
                    let n = self.reader.fetch_window(&object, off, &mut buf)?;
                    buf.truncate(n);
                    self.stats.remote_bytes.fetch_add(n as u64, Ordering::Relaxed);
                    out.push(Resolved::Bytes(buf));
                }
            }
        }
        Ok(out)
    }

    fn write_pieces(&self, s: &mut TcpStream, pieces: &[Resolved]) -> io::Result<()> {
        for p in pieces {
            match p {
                Resolved::Local(f, off, len) => sendfile_all(s, f.file(), *off, *len)?,
                Resolved::Bytes(b) => s.write_all(b)?,
            }
        }
        Ok(())
    }
}

enum Resolved {
    Local(crate::layout::FileRef, u64, u64),
    Bytes(Vec<u8>),
}

impl Resolved {
    fn len(&self) -> u64 {
        match self {
            Resolved::Local(_, _, n) => *n,
            Resolved::Bytes(b) => b.len() as u64,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn digest_forms() {
        let h = "0123456789abcdef".repeat(4);
        assert_eq!(digest_hex(&h), Some(h.as_str()));
        assert_eq!(digest_hex(&format!("sha256:{h}")), Some(h.as_str()));
        assert_eq!(digest_hex(&h.to_uppercase()), None);
        assert_eq!(digest_hex("sha256:abc"), None);
    }

    #[test]
    fn byte_range_matches_go() {
        assert_eq!(parse_byte_range("bytes=0-9", 100), Ok((0, 9)));
        assert_eq!(parse_byte_range("bytes=90-", 100), Ok((90, 99)));
        assert_eq!(parse_byte_range("bytes=90-200", 100), Ok((90, 99)));
        assert!(parse_byte_range("bytes=100-", 100).is_err());
        assert!(parse_byte_range("bytes=-5", 100).is_err());
        assert!(parse_byte_range("bytes=5-4", 100).is_err());
        assert!(parse_byte_range("bytes=0-1,4-5", 100).is_err());
        assert!(parse_byte_range("items=0-1", 100).is_err());
        assert!(parse_byte_range("bytes=0-1", 0).is_err());
    }
}
