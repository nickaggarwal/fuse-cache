//! Read-only FUSE frontend over a cache dir (Linux, `fuser`).
//!
//! Inodes map to logical cache paths (chunk files collapse to their parent;
//! internal files are hidden). Reads go through [`TieredReader`], so a miss
//! falls through to peers and cloud when those are configured.
//!
//! Throughput choices, from the Go profile:
//! * replies carry exactly the requested window, read by `pread` into a
//!   per-worker buffer that is reused (no per-read allocation);
//! * the kernel is told `max_readahead` = 16 MiB (like the Go mount) with
//!   `FOPEN_KEEP_CACHE`, so the page cache serves repeat reads;
//! * fuser's receive loop is single-threaded and rejects requests on any
//!   session that did not see INIT, so reads are handed to a pool of worker
//!   threads (the reply handle is `Send`); with `FUSE_ASYNC_READ` the kernel
//!   keeps several reads in flight and they are served concurrently.

use fuser::{FileAttr, FileType, Filesystem, KernelConfig, ReplyAttr, ReplyData, ReplyDirectory, ReplyEntry, ReplyOpen, ReplyStatfs, Request};
use fusedata_core::tiered::TieredReader;
use std::cell::RefCell;
use std::collections::HashMap;
use std::ffi::OsStr;
use std::sync::{Arc, RwLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

const TTL: Duration = Duration::from_secs(5);
const ROOT: u64 = 1;
const FOPEN_KEEP_CACHE: u32 = 1 << 1;

#[derive(Default)]
struct Inodes {
    by_ino: HashMap<u64, (String, bool)>,
    by_path: HashMap<String, u64>,
    next: u64,
}

impl Inodes {
    fn get_or_add(&mut self, path: &str, is_dir: bool) -> u64 {
        if let Some(&i) = self.by_path.get(path) {
            return i;
        }
        self.next += 1;
        let i = self.next;
        self.by_ino.insert(i, (path.to_string(), is_dir));
        self.by_path.insert(path.to_string(), i);
        i
    }
}

type ReadJob = (String, u64, u32, ReplyData);

#[derive(Clone)]
pub struct Fs {
    reader: Arc<TieredReader>,
    inodes: Arc<RwLock<Inodes>>,
    uid: u32,
    gid: u32,
    pool: Option<std::sync::mpsc::SyncSender<ReadJob>>,
}

thread_local! {
    static BUF: RefCell<Vec<u8>> = const { RefCell::new(Vec::new()) };
}

fn join(dir: &str, name: &str) -> String {
    if dir == "/" {
        format!("/{name}")
    } else {
        format!("{dir}/{name}")
    }
}

impl Fs {
    pub fn new(reader: Arc<TieredReader>) -> Self {
        let mut ino = Inodes { next: ROOT, ..Default::default() };
        ino.by_ino.insert(ROOT, ("/".into(), true));
        ino.by_path.insert("/".into(), ROOT);
        Fs { reader, inodes: Arc::new(RwLock::new(ino)), uid: unsafe { libc::getuid() }, gid: unsafe { libc::getgid() }, pool: None }
    }

    /// Serve reads on `threads` workers (0 or 1 = inline on the loop).
    pub fn with_workers(mut self, threads: usize) -> Self {
        if threads <= 1 {
            return self;
        }
        let (tx, rx) = std::sync::mpsc::sync_channel::<ReadJob>(threads * 4);
        let rx = Arc::new(std::sync::Mutex::new(rx));
        for i in 0..threads {
            let rx = rx.clone();
            let reader = self.reader.clone();
            std::thread::Builder::new()
                .name(format!("fuse-read-{i}"))
                .spawn(move || loop {
                    let job = rx.lock().unwrap().recv();
                    match job {
                        Ok((path, off, size, reply)) => serve_read(&reader, &path, off, size, reply),
                        Err(_) => return,
                    }
                })
                .expect("spawn fuse worker");
        }
        self.pool = Some(tx);
        self
    }

    fn attr(&self, ino: u64, is_dir: bool, size: u64) -> FileAttr {
        let now = SystemTime::now();
        FileAttr {
            ino,
            size,
            blocks: size.div_ceil(512),
            atime: now,
            mtime: UNIX_EPOCH,
            ctime: UNIX_EPOCH,
            crtime: UNIX_EPOCH,
            kind: if is_dir { FileType::Directory } else { FileType::RegularFile },
            perm: if is_dir { 0o755 } else { 0o444 },
            nlink: if is_dir { 2 } else { 1 },
            uid: self.uid,
            gid: self.gid,
            rdev: 0,
            blksize: 128 * 1024,
            flags: 0,
        }
    }

    fn path_of(&self, ino: u64) -> Option<(String, bool)> {
        self.inodes.read().unwrap().by_ino.get(&ino).cloned()
    }

    /// Stat a logical path: directory on disk, else a file shape (local,
    /// or hinted by the coordinator).
    fn stat_path(&self, path: &str) -> Option<(bool, u64)> {
        let abs = self.reader.cache.local_path(path);
        if abs.is_dir() {
            return Some((true, 0));
        }
        self.reader.learn_shape(path);
        self.reader.cache.size(path).ok().map(|s| (false, s.max(0) as u64))
    }
}

impl Filesystem for Fs {
    fn init(&mut self, _req: &Request<'_>, config: &mut KernelConfig) -> Result<(), libc::c_int> {
        // Unmap mappings that went idle (see layout::reap_mapped).
        std::thread::Builder::new()
            .name("reaper".into())
            .spawn(move || loop {
                std::thread::sleep(Duration::from_millis(500));
                fusedata_core::layout::reap_mapped(Duration::from_secs(1));
            })
            .map_err(|_| libc::EIO)?;
        let _ = config.set_max_readahead(16 * 1024 * 1024);
        // 1 MiB reads, as go-fuse negotiates (max_read=1048576 on its mount).
        let _ = config.set_max_write(1024 * 1024);
        let _ = config.set_max_background(512);
        let _ = config.set_congestion_threshold(384);
        Ok(())
    }

    fn lookup(&mut self, _req: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEntry) {
        let Some((dir, _)) = self.path_of(parent) else { return reply.error(libc::ENOENT) };
        let Some(name) = name.to_str() else { return reply.error(libc::ENOENT) };
        if fusedata_core::layout::is_internal_name(name) || (dir == "/" && name == fusedata_core::layout::DURABILITY_DIR) {
            return reply.error(libc::ENOENT);
        }
        let path = join(&dir, name);
        match self.stat_path(&path) {
            Some((is_dir, size)) => {
                let ino = self.inodes.write().unwrap().get_or_add(&path, is_dir);
                reply.entry(&TTL, &self.attr(ino, is_dir, size), 0)
            }
            None => reply.error(libc::ENOENT),
        }
    }

    fn getattr(&mut self, _req: &Request<'_>, ino: u64, _fh: Option<u64>, reply: ReplyAttr) {
        let Some((path, is_dir)) = self.path_of(ino) else { return reply.error(libc::ENOENT) };
        if is_dir {
            return reply.attr(&TTL, &self.attr(ino, true, 0));
        }
        match self.stat_path(&path) {
            Some((d, size)) => reply.attr(&TTL, &self.attr(ino, d, size)),
            None => reply.error(libc::ENOENT),
        }
    }

    fn open(&mut self, _req: &Request<'_>, ino: u64, flags: i32, reply: ReplyOpen) {
        if flags & libc::O_ACCMODE != libc::O_RDONLY {
            return reply.error(libc::EROFS);
        }
        match self.path_of(ino) {
            Some((_, false)) => reply.opened(0, FOPEN_KEEP_CACHE),
            Some((_, true)) => reply.error(libc::EISDIR),
            None => reply.error(libc::ENOENT),
        }
    }

    fn read(&mut self, _req: &Request<'_>, ino: u64, _fh: u64, offset: i64, size: u32, _flags: i32, _lock: Option<u64>, reply: ReplyData) {
        let Some((path, _)) = self.path_of(ino) else { return reply.error(libc::ENOENT) };
        match &self.pool {
            Some(tx) => {
                if let Err(std::sync::mpsc::SendError((path, off, size, reply))) = tx.send((path, offset as u64, size, reply)) {
                    serve_read(&self.reader, &path, off, size, reply)
                }
            }
            None => serve_read(&self.reader, &path, offset as u64, size, reply),
        }
    }

    fn opendir(&mut self, _req: &Request<'_>, ino: u64, _flags: i32, reply: ReplyOpen) {
        match self.path_of(ino) {
            Some((_, true)) => reply.opened(0, 0),
            Some(_) => reply.error(libc::ENOTDIR),
            None => reply.error(libc::ENOENT),
        }
    }

    fn readdir(&mut self, _req: &Request<'_>, ino: u64, _fh: u64, offset: i64, mut reply: ReplyDirectory) {
        let Some((dir, true)) = self.path_of(ino) else { return reply.error(libc::ENOTDIR) };
        let kids = match self.reader.cache.list_dir(&dir) {
            Ok(k) => k,
            Err(_) => return reply.error(libc::EIO),
        };
        let mut entries = vec![(ino, FileType::Directory, ".".to_string()), (ino, FileType::Directory, "..".to_string())];
        {
            let mut inos = self.inodes.write().unwrap();
            for (name, is_dir) in kids {
                let i = inos.get_or_add(&join(&dir, &name), is_dir);
                entries.push((i, if is_dir { FileType::Directory } else { FileType::RegularFile }, name));
            }
        }
        for (i, (ino, kind, name)) in entries.into_iter().enumerate().skip(offset as usize) {
            if reply.add(ino, (i + 1) as i64, kind, name) {
                break;
            }
        }
        reply.ok()
    }

    fn statfs(&mut self, _req: &Request<'_>, _ino: u64, reply: ReplyStatfs) {
        reply.statfs(0, 0, 0, 0, 0, 128 * 1024, 255, 0)
    }
}

fn serve_read(reader: &TieredReader, path: &str, off: u64, size: u32, reply: ReplyData) {
    // The reply is written to /dev/fuse straight from the mapping slice
    // (zero-copy view) or from the per-thread buffer the slow path filled.
    let mut reply = Some(reply);
    let r = reader.read_view(path, off, size as usize, |data| reply.take().unwrap().data(data));
    if let Err(e) = r {
        eprintln!("[FUSE] read {path} off={off} size={size}: {e}");
        if let Some(rp) = reply.take() {
            rp.error(libc::EIO)
        }
    }
}

/// Set the mount's backing-device readahead (`/sys/class/bdi/<maj:min>/
/// read_ahead_kb`). The kernel only sends FUSE READs as large as this
/// window for a sequential reader, and new FUSE mounts default to 128 KiB,
/// which caps a single-reader stream at ~2.5-3.5 GB/s through either
/// daemon on the lab VM; 4 MiB measured 14 GB/s (bench/probe-fuse-ra.sh).
/// Needs root (as the mount itself does in the DaemonSet).
fn set_bdi_readahead(mnt: &str, kb: u32) -> std::io::Result<()> {
    use std::os::unix::fs::MetadataExt;
    let dev = std::fs::metadata(mnt)?.dev();
    let (maj, min) = (libc::major(dev), libc::minor(dev));
    std::fs::write(format!("/sys/class/bdi/{maj}:{min}/read_ahead_kb"), kb.to_string())
}

/// Mount and serve until unmounted. `readahead_kb` > 0 sets the mount's
/// kernel readahead once the mount is up.
pub fn mount(fs: Fs, mnt: &str, readahead_kb: u32) -> anyhow::Result<()> {
    use fuser::MountOption;
    let opts = [
        MountOption::RO,
        MountOption::FSName("fusedata".into()),
        MountOption::Subtype("fusedata".into()),
        MountOption::DefaultPermissions,
        MountOption::CUSTOM("max_read=1048576".into()),
    ];
    eprintln!("[FUSE] mounting at {mnt}");
    if readahead_kb > 0 {
        // The mount's BDI exists once the kernel finishes INIT, which needs
        // the session loop below running; set it from a side thread.
        let m = mnt.to_string();
        std::thread::spawn(move || {
            let root_dev = std::fs::metadata(std::path::Path::new(&m).parent().unwrap_or(std::path::Path::new("/"))).map(|md| {
                use std::os::unix::fs::MetadataExt;
                md.dev()
            });
            for _ in 0..250 {
                std::thread::sleep(std::time::Duration::from_millis(20));
                let dev = std::fs::metadata(&m).map(|md| {
                    use std::os::unix::fs::MetadataExt;
                    md.dev()
                });
                // Wait until the path is the FUSE mount, not the directory under it.
                if dev.is_err() || dev.as_ref().ok() == root_dev.as_ref().ok() {
                    continue;
                }
                match set_bdi_readahead(&m, readahead_kb) {
                    Ok(()) => {
                        eprintln!("[FUSE] kernel readahead set to {readahead_kb} KiB");
                        return;
                    }
                    Err(e) => {
                        eprintln!("[FUSE] kernel readahead NOT set ({e}); needs root");
                        return;
                    }
                }
            }
            eprintln!("[FUSE] kernel readahead NOT set (mount not seen)");
        });
    }
    fuser::mount2(fs, mnt, &opts)?;
    Ok(())
}
