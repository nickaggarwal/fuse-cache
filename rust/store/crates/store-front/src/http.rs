//! Minimal HTTP/1.1 plumbing over blocking sockets: a buffered connection
//! that keeps bytes read past a message head, request/response head parsing,
//! body framing (Content-Length, chunked, close-delimited) copied verbatim
//! with a fixed-size buffer, sendfile for native responses, and a small
//! client for the coordinator and health checks.
//!
//! Proxying copies the client's request head bytes unchanged and the body
//! as framed on the wire, so the Go client behind it sees exactly what the
//! caller sent (every header, including `X-API-Key`), and memory stays at
//! one copy buffer per connection whatever the body size.

use std::fs::File;
use std::io::{self, Read, Write};
use std::net::{TcpStream, ToSocketAddrs};
use std::time::Duration;

pub const MAX_HEAD: usize = 64 * 1024;
const COPY_BUF: usize = 256 * 1024;

/// A socket plus bytes already read from it but not yet consumed.
pub struct Conn {
    pub stream: TcpStream,
    buf: Vec<u8>,
    start: usize,
    end: usize,
}

impl Conn {
    pub fn new(stream: TcpStream) -> Self {
        Conn { stream, buf: vec![0u8; MAX_HEAD], start: 0, end: 0 }
    }

    pub fn buffered(&self) -> &[u8] {
        &self.buf[self.start..self.end]
    }

    pub fn has_buffered(&self) -> bool {
        self.end > self.start
    }

    fn consume(&mut self, n: usize) {
        self.start += n;
        if self.start == self.end {
            self.start = 0;
            self.end = 0;
        }
    }

    /// Read more bytes into the buffer. `Ok(0)` = EOF.
    pub fn fill(&mut self) -> io::Result<usize> {
        if self.start > 0 && self.end == self.buf.len() {
            self.buf.copy_within(self.start..self.end, 0);
            self.end -= self.start;
            self.start = 0;
        }
        if self.end == self.buf.len() {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "message head too large"));
        }
        let n = self.stream.read(&mut self.buf[self.end..])?;
        self.end += n;
        Ok(n)
    }

    /// Wait for a complete message head; returns its length in the buffer
    /// (head bytes are `buffered()[..len]`, not yet consumed). `Ok(None)` =
    /// clean EOF before any byte of a new message.
    pub fn read_head(&mut self) -> io::Result<Option<usize>> {
        loop {
            if let Some(i) = find_head_end(self.buffered()) {
                return Ok(Some(i));
            }
            if self.fill()? == 0 {
                if self.has_buffered() {
                    return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "truncated message head"));
                }
                return Ok(None);
            }
        }
    }

    /// Take `n` head bytes out of the buffer.
    pub fn take(&mut self, n: usize) -> Vec<u8> {
        let v = self.buffered()[..n].to_vec();
        self.consume(n);
        v
    }

    /// Copy exactly `n` bytes (buffered first) to `dst`.
    pub fn copy_exact(&mut self, mut n: u64, dst: &mut impl Write) -> io::Result<()> {
        let have = (self.end - self.start).min(n as usize);
        if have > 0 {
            dst.write_all(&self.buf[self.start..self.start + have])?;
            self.consume(have);
            n -= have as u64;
        }
        let mut tmp = vec![0u8; COPY_BUF.min(n.max(1) as usize)];
        while n > 0 {
            let want = tmp.len().min(n as usize);
            let got = self.stream.read(&mut tmp[..want])?;
            if got == 0 {
                return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "body ended early"));
            }
            dst.write_all(&tmp[..got])?;
            n -= got as u64;
        }
        Ok(())
    }

    /// Read and discard exactly `n` bytes.
    pub fn discard(&mut self, n: u64) -> io::Result<()> {
        self.copy_exact(n, &mut io::sink())
    }

    /// Copy until EOF.
    pub fn copy_to_eof(&mut self, dst: &mut impl Write) -> io::Result<u64> {
        let mut total = 0u64;
        if self.has_buffered() {
            dst.write_all(self.buffered())?;
            total += (self.end - self.start) as u64;
            self.start = 0;
            self.end = 0;
        }
        let mut tmp = vec![0u8; COPY_BUF];
        loop {
            let got = self.stream.read(&mut tmp)?;
            if got == 0 {
                return Ok(total);
            }
            dst.write_all(&tmp[..got])?;
            total += got as u64;
        }
    }

    /// One CRLF-terminated line, including the CRLF.
    fn read_line(&mut self, max: usize) -> io::Result<Vec<u8>> {
        loop {
            if let Some(i) = self.buffered().windows(2).position(|w| w == b"\r\n") {
                return Ok(self.take(i + 2));
            }
            if self.end - self.start > max {
                return Err(io::Error::new(io::ErrorKind::InvalidData, "chunk line too long"));
            }
            if self.fill()? == 0 {
                return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "chunked body ended early"));
            }
        }
    }

    /// Copy a chunked body verbatim (size lines, data, trailers) to `dst`,
    /// stopping after the terminating empty line. Returns payload bytes.
    pub fn copy_chunked(&mut self, dst: &mut impl Write) -> io::Result<u64> {
        let mut payload = 0u64;
        loop {
            let line = self.read_line(4096)?;
            dst.write_all(&line)?;
            let text = std::str::from_utf8(&line[..line.len() - 2]).map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "bad chunk size"))?;
            let hex = text.split(';').next().unwrap_or("").trim();
            let size = u64::from_str_radix(hex, 16).map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "bad chunk size"))?;
            if size == 0 {
                // Trailers, then the final CRLF.
                loop {
                    let t = self.read_line(16 * 1024)?;
                    dst.write_all(&t)?;
                    if t.len() == 2 {
                        return Ok(payload);
                    }
                }
            }
            self.copy_exact(size + 2, dst)?; // data + CRLF
            payload += size;
        }
    }
}

pub fn find_head_end(b: &[u8]) -> Option<usize> {
    b.windows(4).position(|w| w == b"\r\n\r\n").map(|i| i + 4)
}

/// A message head that does not parse as HTTP/1.x.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BadHead;

/// The parts of a request head the server routes and frames on.
#[derive(Debug, Default, Clone)]
pub struct RequestHead {
    pub method: String,
    pub target: String,
    pub http10: bool,
    pub api_key: Option<String>,
    pub range: Option<String>,
    pub content_length: Option<u64>,
    pub chunked: bool,
    pub expect_continue: bool,
    pub conn_close: bool,
}

impl RequestHead {
    pub fn path(&self) -> &str {
        self.target.split_once('?').map(|(p, _)| p).unwrap_or(&self.target)
    }

    pub fn query(&self) -> &str {
        self.target.split_once('?').map(|(_, q)| q).unwrap_or("")
    }

    pub fn has_body(&self) -> bool {
        self.chunked || self.content_length.unwrap_or(0) > 0
    }
}

pub fn parse_request(head: &[u8]) -> Result<RequestHead, BadHead> {
    let mut hdrs = [httparse::EMPTY_HEADER; 128];
    let mut req = httparse::Request::new(&mut hdrs);
    match req.parse(head) {
        Ok(httparse::Status::Complete(_)) => {}
        _ => return Err(BadHead),
    }
    let mut h = RequestHead {
        method: req.method.unwrap_or("").to_string(),
        target: req.path.unwrap_or("").to_string(),
        http10: req.version == Some(0),
        ..Default::default()
    };
    h.conn_close = h.http10;
    for hd in req.headers.iter() {
        let v = String::from_utf8_lossy(hd.value);
        let n = hd.name;
        if n.eq_ignore_ascii_case("x-api-key") {
            h.api_key = Some(v.into_owned());
        } else if n.eq_ignore_ascii_case("range") {
            h.range = Some(v.into_owned());
        } else if n.eq_ignore_ascii_case("content-length") {
            h.content_length = Some(v.trim().parse().map_err(|_| BadHead)?);
        } else if n.eq_ignore_ascii_case("transfer-encoding") {
            h.chunked = v.to_ascii_lowercase().contains("chunked");
        } else if n.eq_ignore_ascii_case("expect") {
            h.expect_continue = v.eq_ignore_ascii_case("100-continue");
        } else if n.eq_ignore_ascii_case("connection") {
            let l = v.to_ascii_lowercase();
            if l.contains("close") {
                h.conn_close = true;
            } else if l.contains("keep-alive") {
                h.conn_close = false;
            }
        }
    }
    Ok(h)
}

/// The parts of a response head needed to relay its body.
#[derive(Debug, Clone)]
pub struct ResponseHead {
    pub status: u16,
    pub content_length: Option<u64>,
    pub chunked: bool,
    pub conn_close: bool,
}

pub fn parse_response(head: &[u8]) -> Result<ResponseHead, BadHead> {
    let mut hdrs = [httparse::EMPTY_HEADER; 128];
    let mut resp = httparse::Response::new(&mut hdrs);
    match resp.parse(head) {
        Ok(httparse::Status::Complete(_)) => {}
        _ => return Err(BadHead),
    }
    let mut r = ResponseHead { status: resp.code.unwrap_or(0), content_length: None, chunked: false, conn_close: resp.version == Some(0) };
    for hd in resp.headers.iter() {
        if hd.name.eq_ignore_ascii_case("content-length") {
            r.content_length = std::str::from_utf8(hd.value).ok().and_then(|v| v.trim().parse().ok());
        } else if hd.name.eq_ignore_ascii_case("transfer-encoding") {
            r.chunked = String::from_utf8_lossy(hd.value).to_ascii_lowercase().contains("chunked");
        } else if hd.name.eq_ignore_ascii_case("connection") {
            r.conn_close = hd.value.eq_ignore_ascii_case(b"close");
        }
    }
    Ok(r)
}

pub fn http_date() -> String {
    httpdate::fmt_http_date(std::time::SystemTime::now())
}

/// A plain-text response the way Go's `http.Error` writes it.
pub fn write_error(s: &mut impl Write, code: u16, msg: &str, extra: &[(&str, &str)], close: bool) -> io::Result<()> {
    let body = format!("{msg}\n");
    let mut out = format!(
        "HTTP/1.1 {code} {}\r\nContent-Type: text/plain; charset=utf-8\r\nX-Content-Type-Options: nosniff\r\nDate: {}\r\nContent-Length: {}\r\n",
        reason(code),
        http_date(),
        body.len()
    );
    for (k, v) in extra {
        out.push_str(&format!("{k}: {v}\r\n"));
    }
    if close {
        out.push_str("Connection: close\r\n");
    }
    out.push_str("\r\n");
    out.push_str(&body);
    s.write_all(out.as_bytes())
}

pub fn reason(code: u16) -> &'static str {
    match code {
        100 => "Continue",
        200 => "OK",
        206 => "Partial Content",
        400 => "Bad Request",
        401 => "Unauthorized",
        404 => "Not Found",
        405 => "Method Not Allowed",
        416 => "Requested Range Not Satisfiable",
        500 => "Internal Server Error",
        502 => "Bad Gateway",
        503 => "Service Unavailable",
        _ => "Status",
    }
}

/// Write a header block with `MSG_MORE` so it shares a segment with the
/// first body bytes sendfile sends next.
#[cfg(target_os = "linux")]
pub fn send_more(s: &mut TcpStream, data: &[u8]) -> io::Result<()> {
    use std::os::fd::AsRawFd;
    let mut d = data;
    while !d.is_empty() {
        let n = unsafe { libc::send(s.as_raw_fd(), d.as_ptr() as *const libc::c_void, d.len(), libc::MSG_MORE) };
        if n < 0 {
            let e = io::Error::last_os_error();
            if e.kind() == io::ErrorKind::Interrupted {
                continue;
            }
            return Err(e);
        }
        d = &d[n as usize..];
    }
    Ok(())
}

#[cfg(not(target_os = "linux"))]
pub fn send_more(s: &mut TcpStream, data: &[u8]) -> io::Result<()> {
    s.write_all(data)
}

/// `[off, off+len)` of `f` to the socket, zero-copy on Linux.
#[cfg(target_os = "linux")]
pub fn sendfile_all(s: &mut TcpStream, f: &File, off: u64, len: u64) -> io::Result<()> {
    use std::os::fd::AsRawFd;
    let mut o = off as libc::off_t;
    let end = (off + len) as libc::off_t;
    while o < end {
        let want = ((end - o) as usize).min(1 << 30);
        let n = unsafe { libc::sendfile(s.as_raw_fd(), f.as_raw_fd(), &mut o, want) };
        if n < 0 {
            let e = io::Error::last_os_error();
            if e.kind() == io::ErrorKind::Interrupted {
                continue;
            }
            return Err(e);
        }
        if n == 0 {
            return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "file shrank during sendfile"));
        }
    }
    Ok(())
}

#[cfg(not(target_os = "linux"))]
pub fn sendfile_all(s: &mut TcpStream, f: &File, off: u64, len: u64) -> io::Result<()> {
    use std::os::unix::fs::FileExt;
    let mut buf = vec![0u8; COPY_BUF];
    let (mut o, end) = (off, off + len);
    while o < end {
        let want = ((end - o) as usize).min(buf.len());
        let n = f.read_at(&mut buf[..want], o)?;
        if n == 0 {
            return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "file shrank"));
        }
        s.write_all(&buf[..n])?;
        o += n as u64;
    }
    Ok(())
}

pub fn connect(addr: &str, timeout: Duration) -> io::Result<TcpStream> {
    let mut last = None;
    for sa in addr.to_socket_addrs()? {
        match TcpStream::connect_timeout(&sa, timeout) {
            Ok(s) => {
                s.set_nodelay(true).ok();
                return Ok(s);
            }
            Err(e) => last = Some(e),
        }
    }
    Err(last.unwrap_or_else(|| io::Error::new(io::ErrorKind::NotFound, format!("no address for {addr}"))))
}

/// A one-shot GET returning (status, body). Bodies over `max` are an error.
pub fn get(addr: &str, target: &str, headers: &[(&str, &str)], timeout: Duration, max: usize) -> io::Result<(u16, Vec<u8>)> {
    let s = connect(addr, timeout)?;
    s.set_read_timeout(Some(timeout))?;
    s.set_write_timeout(Some(timeout))?;
    let mut req = format!("GET {target} HTTP/1.1\r\nHost: {addr}\r\nUser-Agent: store-front\r\nConnection: close\r\n");
    for (k, v) in headers {
        req.push_str(&format!("{k}: {v}\r\n"));
    }
    req.push_str("\r\n");
    let mut c = Conn::new(s);
    c.stream.write_all(req.as_bytes())?;
    let n = c.read_head()?.ok_or_else(|| io::Error::new(io::ErrorKind::UnexpectedEof, "no response"))?;
    let head = c.take(n);
    let r = parse_response(&head).map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "bad response head"))?;
    let mut body = LimitedVec { v: Vec::new(), max };
    if r.chunked {
        let mut raw = LimitedVec { v: Vec::new(), max: max * 2 + 4096 };
        c.copy_chunked(&mut raw)?;
        body.v = dechunk(&raw.v)?;
    } else if let Some(n) = r.content_length {
        c.copy_exact(n, &mut body)?;
    } else {
        c.copy_to_eof(&mut body)?;
    }
    Ok((r.status, body.v))
}

/// `HEAD target` -> `Some(Content-Length)` on 200, `None` on any other
/// status.
pub fn head_len(addr: &str, target: &str, timeout: Duration) -> io::Result<Option<u64>> {
    let s = connect(addr, timeout)?;
    s.set_read_timeout(Some(timeout))?;
    s.set_write_timeout(Some(timeout))?;
    let mut c = Conn::new(s);
    let req = format!("HEAD {target} HTTP/1.1\r\nHost: {addr}\r\nUser-Agent: store-front\r\nConnection: close\r\n\r\n");
    c.stream.write_all(req.as_bytes())?;
    let n = c.read_head()?.ok_or_else(|| io::Error::new(io::ErrorKind::UnexpectedEof, "no response"))?;
    let r = parse_response(&c.take(n)).map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "bad response head"))?;
    Ok(if r.status == 200 { r.content_length } else { None })
}

struct LimitedVec {
    v: Vec<u8>,
    max: usize,
}

impl Write for LimitedVec {
    fn write(&mut self, b: &[u8]) -> io::Result<usize> {
        if self.v.len() + b.len() > self.max {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "body too large"));
        }
        self.v.extend_from_slice(b);
        Ok(b.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

/// Decode a complete chunked body (as copied by `copy_chunked`).
pub fn dechunk(raw: &[u8]) -> io::Result<Vec<u8>> {
    let bad = || io::Error::new(io::ErrorKind::InvalidData, "bad chunked body");
    let mut out = Vec::new();
    let mut i = 0;
    loop {
        let e = raw[i..].windows(2).position(|w| w == b"\r\n").ok_or_else(bad)? + i;
        let line = std::str::from_utf8(&raw[i..e]).map_err(|_| bad())?;
        let size = usize::from_str_radix(line.split(';').next().unwrap_or("").trim(), 16).map_err(|_| bad())?;
        i = e + 2;
        if size == 0 {
            return Ok(out);
        }
        out.extend_from_slice(raw.get(i..i + size).ok_or_else(bad)?);
        i += size + 2;
    }
}

/// Go `url.QueryEscape` set (space as %20, which Go decodes the same).
const QUERY: &percent_encoding::AsciiSet = &percent_encoding::NON_ALPHANUMERIC.remove(b'-').remove(b'_').remove(b'.').remove(b'~');

pub fn query_escape(s: &str) -> String {
    percent_encoding::utf8_percent_encode(s, QUERY).to_string()
}

/// Go `url.ParseQuery(q).Get(key)`: first value, `+` is a space.
pub fn query_get(query: &str, key: &str) -> Option<String> {
    for pair in query.split('&').filter(|p| !p.is_empty()) {
        let (k, v) = pair.split_once('=').unwrap_or((pair, ""));
        let k = percent_encoding::percent_decode_str(&k.replace('+', " ")).decode_utf8().ok()?.into_owned();
        if k == key {
            return percent_encoding::percent_decode_str(&v.replace('+', " ")).decode_utf8().ok().map(|c| c.into_owned());
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn head_parsing() {
        let raw = b"PUT /api/files/x?y=1 HTTP/1.1\r\nHost: a\r\nX-API-Key: k\r\nContent-Length: 12\r\nExpect: 100-continue\r\n\r\nbody";
        let n = find_head_end(raw).unwrap();
        let h = parse_request(&raw[..n]).unwrap();
        assert_eq!((h.method.as_str(), h.path(), h.query()), ("PUT", "/api/files/x", "y=1"));
        assert_eq!(h.api_key.as_deref(), Some("k"));
        assert_eq!(h.content_length, Some(12));
        assert!(h.expect_continue && !h.chunked && !h.conn_close && h.has_body());
        let h = parse_request(b"GET / HTTP/1.0\r\n\r\n").unwrap();
        assert!(h.conn_close);
        assert!(parse_request(b"GET / HTTP/1.1\r\nContent-Length: x\r\n\r\n").is_err());
    }

    #[test]
    fn dechunk_roundtrip() {
        assert_eq!(dechunk(b"3\r\nabc\r\n2;x=y\r\nde\r\n0\r\n\r\n").unwrap(), b"abcde");
    }

    #[test]
    fn query_helpers() {
        assert_eq!(query_get("path=%2Fa+b&off=1", "path").as_deref(), Some("/a b"));
        assert_eq!(query_get("off=1", "path"), None);
        assert_eq!(query_escape("/a b_chunk_1"), "%2Fa%20b_chunk_1");
    }
}
