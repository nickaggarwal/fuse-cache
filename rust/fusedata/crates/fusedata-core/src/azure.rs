//! Azure Blob ranged reader, compatible with `internal/cache/azure_storage.go`:
//!
//! * blob names are the raw cache path (leading slash kept), scoped as
//!   `<prefix>/<path-without-leading-slash>` when a cluster key prefix is set,
//!   with the unscoped name tried second on `BlobNotFound` (KeyMapper);
//! * names are URL-path-escaped as one segment, like the SDK's
//!   `NewBlobClient` (`url.PathEscape`, so `/` becomes `%2F`);
//! * Shared Key auth (HMAC-SHA256 over the canonical string, API version
//!   2020-10-02, the version azblob v1.0.0 sends);
//! * chunk objects (`*_chunk_<n>`) are read without a HEAD: the first block
//!   is a ranged GET whose `Content-Range` reveals the total, the rest is
//!   fetched as parallel ranged GETs (Go `readFirstRangeThenParallel`).
//!
//! Unlike Go, the bytes land directly in one caller-owned buffer: each
//! parallel range writes its own disjoint slice, so there is no per-block
//! buffer and no final assembly copy. Ranged reads of an arbitrary window
//! (`read_range`) are supported too, which is what a FUSE miss needs.

use base64::Engine;
use hmac::{Hmac, Mac};
use sha2::Sha256;
use std::io::{self, Read};
use std::time::{Duration, SystemTime};

pub const API_VERSION: &str = "2020-10-02";

#[derive(Debug)]
pub enum AzureError {
    NotFound,
    Http(u16, String),
    Transport(String),
}

impl std::fmt::Display for AzureError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AzureError::NotFound => write!(f, "blob not found"),
            AzureError::Http(c, m) => write!(f, "azure http {c}: {m}"),
            AzureError::Transport(m) => write!(f, "azure transport: {m}"),
        }
    }
}

impl std::error::Error for AzureError {}

impl From<AzureError> for io::Error {
    fn from(e: AzureError) -> Self {
        match e {
            AzureError::NotFound => io::Error::new(io::ErrorKind::NotFound, e.to_string()),
            other => io::Error::other(other.to_string()),
        }
    }
}

#[derive(Clone, Debug)]
pub struct AzureConfig {
    pub account: String,
    /// Base64 account key.
    pub key: String,
    pub container: String,
    /// e.g. `https://<account>.blob.core.windows.net` or an Azurite
    /// `http://host:10000/<account>`. No trailing slash.
    pub endpoint: String,
    /// Cluster key prefix (`-cloud-key-prefix`); empty = unscoped.
    pub key_prefix: String,
    /// Go `-azure-download-block-size-mb` (default 4 MiB).
    pub block_size: u64,
    /// Go `-azure-download-concurrency` (default 8).
    pub concurrency: usize,
    pub timeout: Duration,
}

impl AzureConfig {
    pub fn public(account: &str, key: &str, container: &str) -> Self {
        AzureConfig {
            account: account.into(),
            key: key.into(),
            container: container.into(),
            endpoint: format!("https://{account}.blob.core.windows.net"),
            key_prefix: String::new(),
            block_size: 4 << 20,
            concurrency: 8,
            timeout: Duration::from_secs(60),
        }
    }
}

pub struct AzureReader {
    cfg: AzureConfig,
    key: Vec<u8>,
    agent: ureq::Agent,
    /// Path component of the endpoint (Azurite puts the account there); it
    /// is part of the canonicalized resource.
    endpoint_path: String,
}

/// `url.PathEscape`: escape everything except unreserved and the sub-delims
/// Go keeps in a path segment. Notably `/` is escaped.
const PATH_SEGMENT: &percent_encoding::AsciiSet = &percent_encoding::NON_ALPHANUMERIC
    .remove(b'-')
    .remove(b'_')
    .remove(b'.')
    .remove(b'~')
    .remove(b'!')
    .remove(b'$')
    .remove(b'&')
    .remove(b'\'')
    .remove(b'(')
    .remove(b')')
    .remove(b'*')
    .remove(b'+')
    .remove(b',')
    .remove(b';')
    .remove(b'=')
    .remove(b':')
    .remove(b'@');

pub fn path_escape(s: &str) -> String {
    percent_encoding::utf8_percent_encode(s, PATH_SEGMENT).to_string()
}

/// Go `KeyMapper.Candidates` for Azure (no leading-slash trimming).
pub fn read_key_candidates(prefix: &str, path: &str) -> Vec<String> {
    let p = prefix.trim().trim_matches('/');
    if p.is_empty() {
        vec![path.to_string()]
    } else {
        vec![format!("{p}/{}", path.trim_start_matches('/')), path.to_string()]
    }
}

fn is_chunk_object(name: &str) -> bool {
    match name.rfind("_chunk_") {
        Some(i) => {
            let s = &name[i + 7..];
            !s.is_empty() && s.bytes().all(|b| b.is_ascii_digit())
        }
        None => false,
    }
}

fn parse_total(content_range: &str) -> Option<u64> {
    content_range.rsplit_once('/')?.1.trim().parse().ok()
}

impl AzureReader {
    pub fn new(cfg: AzureConfig) -> Result<Self, AzureError> {
        let key = base64::engine::general_purpose::STANDARD.decode(cfg.key.trim()).map_err(|e| AzureError::Transport(format!("bad account key: {e}")))?;
        let endpoint_path = match cfg.endpoint.split_once("://") {
            Some((_, rest)) => rest.find('/').map(|i| rest[i..].trim_end_matches('/').to_string()).unwrap_or_default(),
            None => String::new(),
        };
        let agent = ureq::AgentBuilder::new()
            .timeout_connect(Duration::from_secs(5))
            .timeout(cfg.timeout)
            .max_idle_connections_per_host(cfg.concurrency.max(1) * 2)
            .build();
        Ok(AzureReader { cfg, key, agent, endpoint_path })
    }

    fn url(&self, name: &str) -> (String, String) {
        let escaped = path_escape(name);
        let url = format!("{}/{}/{}", self.cfg.endpoint, self.cfg.container, escaped);
        let resource = format!("/{}{}/{}/{}", self.cfg.account, self.endpoint_path, self.cfg.container, escaped);
        (url, resource)
    }

    /// Shared Key signature for a GET. The range travels as `x-ms-range`
    /// (as the Go SDK sends it), so it is a canonicalized header and the
    /// standard Range slot stays empty.
    pub fn sign(&self, date: &str, range: &str, resource: &str) -> String {
        // VERB, Content-Encoding, Content-Language, Content-Length,
        // Content-MD5, Content-Type, Date, If-Modified-Since, If-Match,
        // If-None-Match, If-Unmodified-Since, Range, canonical headers
        // (sorted x-ms-*), canonical resource.
        let ms_range = if range.is_empty() { String::new() } else { format!("x-ms-range:{range}\n") };
        let sts = format!("GET\n\n\n\n\n\n\n\n\n\n\n\nx-ms-date:{date}\n{ms_range}x-ms-version:{API_VERSION}\n{resource}");
        let mut mac = Hmac::<Sha256>::new_from_slice(&self.key).expect("hmac accepts any key length");
        mac.update(sts.as_bytes());
        let sig = base64::engine::general_purpose::STANDARD.encode(mac.finalize().into_bytes());
        format!("SharedKey {}:{sig}", self.cfg.account)
    }

    fn get(&self, name: &str, range: Option<(u64, u64)>) -> Result<ureq::Response, AzureError> {
        let (url, resource) = self.url(name);
        let date = httpdate::fmt_http_date(SystemTime::now());
        let range_hdr = range.map(|(off, len)| format!("bytes={}-{}", off, off + len - 1)).unwrap_or_default();
        let mut req = self.agent.get(&url).set("x-ms-date", &date).set("x-ms-version", API_VERSION).set("Authorization", &self.sign(&date, &range_hdr, &resource));
        if !range_hdr.is_empty() {
            req = req.set("x-ms-range", &range_hdr);
        }
        match req.call() {
            Ok(r) => Ok(r),
            Err(ureq::Error::Status(404, r)) => {
                let _ = r.into_string();
                Err(AzureError::NotFound)
            }
            Err(ureq::Error::Status(416, r)) => {
                // Range past the end (including any range of an empty blob).
                let _ = r.into_string();
                Err(AzureError::Http(416, "range not satisfiable".into()))
            }
            Err(ureq::Error::Status(code, r)) => {
                let code_hdr = r.header("x-ms-error-code").unwrap_or("").to_string();
                let body = r.into_string().unwrap_or_default();
                Err(AzureError::Http(code, format!("{code_hdr} {}", body.chars().take(300).collect::<String>())))
            }
            Err(e) => Err(AzureError::Transport(e.to_string())),
        }
    }

    fn read_into(resp: ureq::Response, dst: &mut [u8]) -> Result<usize, AzureError> {
        let mut r = resp.into_reader();
        let mut n = 0;
        while n < dst.len() {
            match r.read(&mut dst[n..]) {
                Ok(0) => break,
                Ok(k) => n += k,
                Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
                Err(e) => return Err(AzureError::Transport(e.to_string())),
            }
        }
        Ok(n)
    }

    /// Try each key candidate in order; only `NotFound` moves on.
    fn with_candidates<T>(&self, path: &str, mut f: impl FnMut(&str) -> Result<T, AzureError>) -> Result<T, AzureError> {
        let mut first = None;
        for name in read_key_candidates(&self.cfg.key_prefix, path) {
            match f(&name) {
                Ok(v) => return Ok(v),
                Err(AzureError::NotFound) => {
                    first.get_or_insert(AzureError::NotFound);
                }
                Err(e) => return Err(e),
            }
        }
        Err(first.unwrap_or(AzureError::NotFound))
    }

    /// Whole object into `buf` (resized to fit). For chunk objects this is
    /// first-range-then-parallel; for others a single GET (the Go path does
    /// a HEAD + parallel download for those; whole non-chunk objects are
    /// small files on the read path, so one round trip wins).
    pub fn read_object(&self, path: &str, buf: &mut Vec<u8>) -> Result<usize, AzureError> {
        self.with_candidates(path, |name| {
            if is_chunk_object(name) {
                self.first_range_then_parallel(name, buf)
            } else {
                let resp = self.get(name, None)?;
                let len: usize = resp.header("Content-Length").and_then(|v| v.parse().ok()).unwrap_or(0);
                if buf.len() < len {
                    buf.resize(len, 0);
                }
                Self::read_into(resp, &mut buf[..len])
            }
        })
    }

    fn first_range_then_parallel(&self, name: &str, buf: &mut Vec<u8>) -> Result<usize, AzureError> {
        let bs = self.cfg.block_size.max(1);
        let resp = match self.get(name, Some((0, bs))) {
            Ok(r) => r,
            Err(AzureError::Http(416, _)) => return Ok(0), // empty blob
            Err(e) => return Err(e),
        };
        let total = match resp.header("Content-Range").and_then(parse_total) {
            Some(t) => t,
            None => {
                // No Content-Range: the server ignored the range and sent the
                // whole blob.
                let len: usize = resp.header("Content-Length").and_then(|v| v.parse().ok()).unwrap_or(0);
                if buf.len() < len {
                    buf.resize(len, 0);
                }
                return Self::read_into(resp, &mut buf[..len]);
            }
        };
        let total_us = total as usize;
        if buf.len() < total_us {
            buf.resize(total_us, 0);
        }
        let first = total.min(bs) as usize;
        let got = Self::read_into(resp, &mut buf[..first])?;
        if got != first {
            return Err(AzureError::Transport(format!("short first range: {got}/{first}")));
        }
        if total <= bs {
            return Ok(total_us);
        }
        self.parallel_ranges(name, first as u64, &mut buf[first..total_us])?;
        Ok(total_us)
    }

    /// Fill `dst` with bytes `[off, off+dst.len())` using up to
    /// `concurrency` parallel ranged GETs of `block_size` each. Each worker
    /// writes a disjoint sub-slice, so no assembly copy is needed.
    fn parallel_ranges(&self, name: &str, off: u64, dst: &mut [u8]) -> Result<(), AzureError> {
        let bs = self.cfg.block_size.max(1) as usize;
        let blocks: Vec<(u64, &mut [u8])> = dst.chunks_mut(bs).enumerate().map(|(i, s)| (off + (i * bs) as u64, s)).collect();
        let workers = self.cfg.concurrency.clamp(1, blocks.len().max(1));
        let queue = std::sync::Mutex::new(blocks.into_iter());
        let err: std::sync::Mutex<Option<AzureError>> = std::sync::Mutex::new(None);
        std::thread::scope(|sc| {
            for _ in 0..workers {
                sc.spawn(|| loop {
                    if err.lock().unwrap().is_some() {
                        return;
                    }
                    let next = queue.lock().unwrap().next();
                    let Some((o, slice)) = next else { return };
                    let want = slice.len();
                    let r = self.get(name, Some((o, want as u64))).and_then(|resp| Self::read_into(resp, slice));
                    match r {
                        Ok(n) if n == want => {}
                        Ok(n) => {
                            *err.lock().unwrap() = Some(AzureError::Transport(format!("short range at {o}: {n}/{want}")));
                            return;
                        }
                        Err(e) => {
                            err.lock().unwrap().get_or_insert(e);
                            return;
                        }
                    }
                });
            }
        });
        match err.into_inner().unwrap() {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }

    /// Bytes `[off, off+dst.len())` of object `path` (clamped to its end).
    /// Windows larger than one block are split across parallel GETs.
    pub fn read_range(&self, path: &str, off: u64, dst: &mut [u8]) -> Result<usize, AzureError> {
        if dst.is_empty() {
            return Ok(0);
        }
        self.with_candidates(path, |name| {
            if dst.len() as u64 <= self.cfg.block_size {
                return match self.get(name, Some((off, dst.len() as u64))) {
                    Ok(resp) => Self::read_into(resp, dst),
                    Err(AzureError::Http(416, _)) => Ok(0),
                    Err(e) => Err(e),
                };
            }
            // Learn the size with the first block so the tail is clamped.
            let bs = self.cfg.block_size as usize;
            let resp = match self.get(name, Some((off, bs as u64))) {
                Ok(r) => r,
                Err(AzureError::Http(416, _)) => return Ok(0),
                Err(e) => return Err(e),
            };
            let total = resp.header("Content-Range").and_then(parse_total).unwrap_or(u64::MAX);
            let avail = (total.saturating_sub(off) as usize).min(dst.len());
            let first = avail.min(bs);
            let got = Self::read_into(resp, &mut dst[..first])?;
            if got < first || avail <= first {
                return Ok(got);
            }
            self.parallel_ranges(name, off + first as u64, &mut dst[first..avail])?;
            Ok(avail)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn key_candidates_match_go_keymapper() {
        assert_eq!(read_key_candidates("", "/a/b"), vec!["/a/b"]);
        assert_eq!(read_key_candidates(" /c1/ ", "/a/b"), vec!["c1/a/b", "/a/b"]);
        assert_eq!(read_key_candidates("c1", "a/b"), vec!["c1/a/b", "a/b"]);
    }

    #[test]
    fn path_escape_matches_go() {
        // url.PathEscape("/models/x y+z_chunk_3") == "%2Fmodels%2Fx%20y+z_chunk_3"
        assert_eq!(path_escape("/models/x y+z_chunk_3"), "%2Fmodels%2Fx%20y+z_chunk_3");
        assert_eq!(path_escape("a:b@c=d"), "a:b@c=d");
    }

    #[test]
    fn chunk_object_detection() {
        assert!(is_chunk_object("/m.bin_chunk_0"));
        assert!(is_chunk_object("c1/m.bin_chunk_12"));
        assert!(!is_chunk_object("/m.bin_chunk_"));
        assert!(!is_chunk_object("/m.bin_chunk_1a"));
        assert!(!is_chunk_object("/m.bin"));
    }

    /// The vector is a capture of what the Go azblob SDK actually sent
    /// (`readbench azure-vector`): reproducing its Authorization header from
    /// the same date, range and resource proves the signer matches.
    #[test]
    fn shared_key_signature_matches_go_sdk_capture() {
        let v: serde_json::Value = serde_json::from_str(include_str!("../tests/data/azure_vector.json")).unwrap();
        let s = |k: &str| v[k].as_str().unwrap().to_string();
        let r = AzureReader::new(AzureConfig {
            endpoint: format!("http://127.0.0.1:10000{}", s("endpoint_path")),
            ..AzureConfig::public(&s("account"), &s("key"), &s("container"))
        })
        .unwrap();
        let (url, res) = r.url(&s("blob"));
        assert!(url.ends_with(&s("escaped_path")), "{url}");
        assert_eq!(res, format!("/{}{}", s("account"), s("escaped_path")));
        assert_eq!(s("x_ms_version"), API_VERSION);
        assert_eq!(r.sign(&s("x_ms_date"), &s("x_ms_range"), &res), s("authorization"));
    }
}
