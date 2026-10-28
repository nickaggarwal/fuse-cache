//! `fusedata`: prototype Rust read path for fuse-client.
//!
//! Subcommands mirror `tools/readbench` so the same workloads run against
//! both implementations on the same cache directory.

mod bench;
#[cfg(target_os = "linux")]
mod fuse_fs;

use anyhow::{anyhow, bail, Context};
use clap::{Args, Parser, Subcommand};
use fusedata_core::azure::{AzureConfig, AzureReader};
use fusedata_core::coord::CoordClient;
use fusedata_core::layout::{CacheDir, Hint, Options};
use fusedata_core::pattern;
use fusedata_core::peer::{PeerClient, PeerClientConfig, PeerServer, PeerServerConfig};
use fusedata_core::tiered::TieredReader;
use std::sync::Arc;
use std::time::Duration;

#[derive(Parser)]
#[command(name = "fusedata", version, about = "Rust read data path for the fuse-client cache (prototype)")]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Args, Clone)]
struct CacheArgs {
    /// Cache directory written by the Go client.
    #[arg(long)]
    dir: String,
    /// Node -chunk-size in MiB (used when metadata has no stride).
    #[arg(long, default_value_t = 16)]
    chunk_mb: i64,
    /// Max prefetch window in MiB for a sequential stream (0 = off).
    #[arg(long, default_value_t = 512)]
    readahead_mb: u64,
    /// Prefetch worker threads (0 = async hints only). 16 x 8 MiB up to a
    /// 512 MiB window won the single-reader sweep (bench/ablate-seq.sh): the
    /// lab disk needs ~16 concurrent streams to reach its ceiling.
    #[arg(long, default_value_t = 16)]
    readahead_threads: usize,
    /// Prefetch piece size in KiB.
    #[arg(long, default_value_t = 8192)]
    readahead_piece_kib: i64,
    /// Starting prefetch window in MiB (doubles up to --readahead-mb).
    #[arg(long, default_value_t = 32)]
    readahead_min_mb: u64,
    /// Prefetch worker mode: populate | pread | hint.
    #[arg(long, default_value = "populate")]
    prefetch_mode: String,
    /// How sequential windows are served: segment | map | copy.
    #[arg(long, default_value = "map")]
    view: String,
    /// Segment size in KiB for --view segment.
    #[arg(long, default_value_t = 4096)]
    segment_kib: usize,
    /// path:size:strideMiB shape hints (what the coordinator would say).
    #[arg(long = "publish")]
    publish: Vec<String>,
}

impl CacheArgs {
    fn open(&self) -> anyhow::Result<Arc<CacheDir>> {
        let c = CacheDir::new(
            &self.dir,
            Options {
                configured_chunk_size: self.chunk_mb << 20,
                readahead: self.readahead_mb << 20,
                readahead_threads: self.readahead_threads,
                readahead_piece: self.readahead_piece_kib << 10,
                readahead_min: self.readahead_min_mb << 20,
                prefetch_mode: match self.prefetch_mode.as_str() {
                    "populate" => fusedata_core::layout::PrefetchMode::Populate,
                    "pread" => fusedata_core::layout::PrefetchMode::Pread,
                    "hint" => fusedata_core::layout::PrefetchMode::Hint,
                    m => bail!("unknown --prefetch-mode {m}"),
                },
                view: match self.view.as_str() {
                    "segment" => fusedata_core::layout::ViewMode::Segment,
                    "map" => fusedata_core::layout::ViewMode::Map,
                    "copy" => fusedata_core::layout::ViewMode::Copy,
                    v => bail!("unknown --view {v}"),
                },
                segment: self.segment_kib << 10,
                ..Default::default()
            },
        );
        for p in &self.publish {
            let parts: Vec<&str> = p.split(':').collect();
            if parts.len() < 3 {
                bail!("bad --publish {p} (want path:size:strideMiB)");
            }
            let size: i64 = parts[1].parse()?;
            let stride: i64 = parts[2].parse()?;
            c.set_hint(parts[0], Hint { size, chunked: stride > 0, chunk_size: stride << 20 });
        }
        Ok(Arc::new(c))
    }
}

#[derive(Args, Clone)]
struct WorkArgs {
    #[arg(long, default_value_t = 128)]
    window_kib: i64,
    #[arg(long, default_value = "seq")]
    pattern: String,
    #[arg(long, default_value_t = 1)]
    threads: usize,
    #[arg(long, default_value_t = 1024)]
    total_mb: i64,
    #[arg(long)]
    verify: bool,
    /// Library bench: copy into a caller buffer (read_at) instead of the
    /// zero-copy view API.
    #[arg(long)]
    copy: bool,
}

#[derive(Args, Clone)]
struct RemoteArgs {
    /// Coordinator HTTP address (enables the peer tier).
    #[arg(long)]
    coord: Option<String>,
    #[arg(long, default_value = "fusedata-local")]
    peer_id: String,
    #[arg(long, env = "FUSE_API_KEY")]
    api_key: Option<String>,
    /// Azure account (enables the cloud tier); key from AZURE_STORAGE_KEY.
    #[arg(long, env = "AZURE_STORAGE_ACCOUNT")]
    azure_account: Option<String>,
    #[arg(long, env = "AZURE_STORAGE_KEY", hide_env_values = true)]
    azure_key: Option<String>,
    #[arg(long, env = "AZURE_CONTAINER_NAME", default_value = "fuse-cache")]
    azure_container: String,
    /// Override the blob endpoint (e.g. Azurite http://host:10000/devstoreaccount1).
    #[arg(long)]
    azure_endpoint: Option<String>,
    #[arg(long, default_value = "")]
    cloud_key_prefix: String,
    #[arg(long, default_value_t = 4)]
    azure_block_mb: u64,
    #[arg(long, default_value_t = 8)]
    azure_concurrency: usize,
    /// Do not write fetched chunks back to the cache dir.
    #[arg(long)]
    no_promote: bool,
}

impl RemoteArgs {
    fn azure(&self) -> anyhow::Result<Option<Arc<AzureReader>>> {
        let Some(acct) = &self.azure_account else { return Ok(None) };
        let key = self.azure_key.clone().ok_or_else(|| anyhow!("AZURE_STORAGE_KEY required with --azure-account"))?;
        let mut cfg = AzureConfig::public(acct, &key, &self.azure_container);
        if let Some(ep) = &self.azure_endpoint {
            cfg.endpoint = ep.trim_end_matches('/').to_string();
        }
        cfg.key_prefix = self.cloud_key_prefix.clone();
        cfg.block_size = self.azure_block_mb << 20;
        cfg.concurrency = self.azure_concurrency;
        Ok(Some(Arc::new(AzureReader::new(cfg).map_err(|e| anyhow!("{e}"))?)))
    }

    fn tiered(&self, cache: Arc<CacheDir>) -> anyhow::Result<TieredReader> {
        let mut t = TieredReader::new(cache).with_promotion(!self.no_promote);
        if let Some(c) = &self.coord {
            let client = PeerClient::new(PeerClientConfig { api_key: self.api_key.clone(), ..Default::default() });
            t = t.with_peers(Arc::new(CoordClient::new(c, &self.peer_id)), Arc::new(client));
        }
        if let Some(az) = self.azure()? {
            t = t.with_cloud(az);
        }
        Ok(t)
    }
}

#[derive(Subcommand)]
enum Cmd {
    /// Benchmark the library read path (layout reader) in-process.
    Bench {
        #[command(flatten)]
        cache: CacheArgs,
        #[arg(long, default_value = "/bench/model.bin")]
        path: String,
        #[command(flatten)]
        work: WorkArgs,
    },
    /// Read a whole logical file through all tiers and verify content.
    Verify {
        #[command(flatten)]
        cache: CacheArgs,
        #[command(flatten)]
        remote: RemoteArgs,
        #[arg(long, default_value = "/bench/model.bin")]
        path: String,
        #[arg(long, default_value_t = 1024)]
        window_kib: usize,
    },
    /// Serve /api/peer/read over a cache dir (Go-compatible).
    /// Serve /api/peer/read and the OCI blob routes (/v2/<repo>/blobs/,
    /// /oci/blobs/) over a cache dir. With --coord, blob windows that are
    /// not local are fetched from holders (and Azure with --azure-account).
    ServePeer {
        #[command(flatten)]
        cache: CacheArgs,
        #[command(flatten)]
        remote: RemoteArgs,
        #[arg(long, default_value = "127.0.0.1:18082")]
        listen: String,
        #[arg(long, default_value_t = 64)]
        max_inflight: usize,
    },
    /// Benchmark the peer client: fetch chunk objects from one peer.
    BenchPeer {
        #[arg(long, default_value = "127.0.0.1:18081")]
        addr: String,
        #[arg(long, default_value = "/bench/model.bin")]
        path: String,
        #[arg(long)]
        size: i64,
        #[arg(long, default_value_t = 16)]
        stride_mb: i64,
        #[arg(long, default_value_t = 1)]
        threads: usize,
        #[arg(long, default_value_t = 2048)]
        total_mb: i64,
        #[arg(long)]
        verify: bool,
        #[arg(long, env = "FUSE_API_KEY")]
        api_key: Option<String>,
        #[arg(long, default_value = "?")]
        server: String,
    },
    /// Benchmark Azure ranged reads of one chunk object or window.
    BenchAzure {
        #[command(flatten)]
        remote: RemoteArgs,
        #[arg(long, default_value = "/bench/model.bin")]
        path: String,
        #[arg(long)]
        size: i64,
        #[arg(long, default_value_t = 16)]
        stride_mb: i64,
        #[arg(long, default_value_t = 1)]
        threads: usize,
        #[arg(long, default_value_t = 256)]
        total_mb: i64,
        #[arg(long)]
        verify: bool,
    },
    /// Mount a cache dir read-only over FUSE (Linux).
    Mount {
        #[command(flatten)]
        cache: CacheArgs,
        #[command(flatten)]
        remote: RemoteArgs,
        #[arg(long)]
        mnt: String,
        #[arg(long, default_value_t = 8)]
        workers: usize,
        /// FUSE passthrough for files stored whole (kernel 6.9+).
        #[arg(long)]
        passthrough: bool,
        /// Kernel readahead for the mount in KiB (0 = leave the 128 KiB
        /// default).
        #[arg(long, default_value_t = 4096)]
        kernel_readahead_kb: u32,
    },
}

fn emit(mut v: serde_json::Value, note: String) {
    if !note.is_empty() {
        v["note"] = serde_json::Value::String(note);
    }
    println!("{v}");
}

fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    match cli.cmd {
        Cmd::Bench { cache, path, work } => {
            let c = cache.open()?;
            let shape = c.shape(&path).with_context(|| format!("stat {path}"))?;
            let w = bench::Workload {
                size: shape.size,
                window: work.window_kib << 10,
                pattern: work.pattern,
                threads: work.threads,
                total_bytes: work.total_mb << 20,
                seed: 42,
            };
            let verify = work.verify;
            // Default: the API the FUSE frontend uses (read_view, zero-copy
            // for sequential local windows, the caller consumes the slice in
            // place). --copy: read_at into a caller buffer.
            let copy = work.copy;
            let r = bench::run(
                &w,
                "rust",
                "library",
                || vec![0u8; w.window as usize],
                |buf, off, n| {
                    if copy {
                        let got = c.read_at(&path, off as u64, &mut buf[..n])?;
                        if verify {
                            if let Some(bad) = pattern::check(&buf[..got], off as u64) {
                                bail!("content mismatch at {bad}");
                            }
                        }
                        return Ok(got);
                    }
                    let res = c.read_view(&path, off as u64, n, |d| {
                        if verify {
                            if let Some(bad) = pattern::check(d, off as u64) {
                                return Err(anyhow!("content mismatch at {bad}"));
                            }
                        } else {
                            // Touch every page so a mapped view is really read.
                            let mut x = 0u8;
                            for i in (0..d.len()).step_by(4096) {
                                x ^= unsafe { std::ptr::read_volatile(&d[i]) };
                            }
                            std::hint::black_box(x);
                        }
                        Ok(d.len())
                    })?;
                    res
                },
            )?;
            use std::sync::atomic::Ordering::Relaxed;
            fusedata_core::layout::reap_mapped(Duration::ZERO);
            emit(
                r,
                format!(
                    "layout={} readahead_hints={} view_reads={} copy={copy}",
                    if shape.chunked { "chunked" } else { "whole" },
                    c.stats.readahead_hints.load(Relaxed),
                    c.stats.view_reads.load(Relaxed)
                ),
            );
        }
        Cmd::Verify { cache, remote, path, window_kib } => {
            let t = remote.tiered(cache.open()?)?;
            t.learn_shape(&path);
            let shape = t.cache.shape(&path)?;
            let mut buf = vec![0u8; window_kib << 10];
            let mut off = 0u64;
            while (off as i64) < shape.size {
                let n = t.read(&path, off, &mut buf)?;
                if n == 0 {
                    bail!("short read at {off} of {}", shape.size);
                }
                if let Some(bad) = pattern::check(&buf[..n], off) {
                    bail!("content mismatch at {bad}");
                }
                off += n as u64;
            }
            let s = &t.stats;
            use std::sync::atomic::Ordering::Relaxed;
            println!(
                "verify ok path={path} size={} local_bytes={} peer_fetches={} peer_bytes={} cloud_fetches={} cloud_bytes={} promotions={}",
                shape.size,
                s.local_bytes.load(Relaxed),
                s.peer_fetches.load(Relaxed),
                s.peer_bytes.load(Relaxed),
                s.cloud_fetches.load(Relaxed),
                s.cloud_bytes.load(Relaxed),
                s.promotions.load(Relaxed)
            );
        }
        Cmd::ServePeer { cache, remote, listen, max_inflight } => {
            let c = cache.open()?;
            let api_key = remote.api_key.clone();
            let t = Arc::new(remote.tiered(c.clone())?.with_promotion(false));
            let blob = fusedata_core::blob::BlobServer::new(t);
            let srv = PeerServer::with_blobs(c, PeerServerConfig { api_key, max_inflight }, blob);
            // Views are not used here, but keep the reaper contract.
            std::thread::spawn(|| loop {
                std::thread::sleep(Duration::from_millis(500));
                fusedata_core::layout::reap_mapped(Duration::from_secs(1));
            });
            let l = std::net::TcpListener::bind(&listen)?;
            eprintln!("rust peer+blob server on {listen}");
            srv.serve(l)?;
        }
        Cmd::BenchPeer { addr, path, size, stride_mb, threads, total_mb, verify, api_key, server } => {
            let stride = stride_mb << 20;
            let n_chunks = (size + stride - 1) / stride;
            let client = PeerClient::new(PeerClientConfig { api_key, ..Default::default() });
            let w = bench::Workload { size: n_chunks * stride, window: stride, pattern: "seq".into(), threads, total_bytes: total_mb << 20, seed: 42 };
            let mut r = bench::run(&w, "rust", "peer-client", Vec::<u8>::new, |buf, off, _| {
                let idx = off / stride;
                let n = client.fetch(&addr, &format!("{path}_chunk_{idx}"), buf).map_err(|e| anyhow!("{e}"))?;
                if verify {
                    if let Some(bad) = pattern::check(&buf[..n], (idx * stride) as u64) {
                        bail!("content mismatch at {bad}");
                    }
                }
                Ok(n)
            })?;
            r["pattern"] = "chunk".into();
            emit(r, format!("server={server} dials={}", client.stats.dials.load(std::sync::atomic::Ordering::Relaxed)));
        }
        Cmd::BenchAzure { remote, path, size, stride_mb, threads, total_mb, verify } => {
            let az = remote.azure()?.ok_or_else(|| anyhow!("--azure-account required"))?;
            let stride = stride_mb << 20;
            let n_chunks = (size + stride - 1) / stride;
            let w = bench::Workload { size: n_chunks * stride, window: stride, pattern: "seq".into(), threads, total_bytes: total_mb << 20, seed: 42 };
            let mut r = bench::run(&w, "rust", "azure", Vec::<u8>::new, |buf, off, _| {
                let idx = off / stride;
                let n = az.read_object(&format!("{path}_chunk_{idx}"), buf).map_err(|e| anyhow!("{e}"))?;
                if verify {
                    if let Some(bad) = pattern::check(&buf[..n], (idx * stride) as u64) {
                        bail!("content mismatch at {bad}");
                    }
                }
                Ok(n)
            })?;
            r["pattern"] = "chunk".into();
            emit(r, String::new());
        }
        Cmd::Mount { cache, remote, mnt, workers, passthrough, kernel_readahead_kb } => {
            #[cfg(target_os = "linux")]
            {
                let t = Arc::new(remote.tiered(cache.open()?)?);
                fuse_fs::mount(fuse_fs::Fs::new(t).with_workers(workers).with_passthrough(passthrough), &mnt, kernel_readahead_kb)?;
            }
            #[cfg(not(target_os = "linux"))]
            {
                let _ = (cache, remote, mnt, workers, passthrough, kernel_readahead_kb);
                bail!("mount is Linux-only");
            }
        }
    }
    Ok(())
}
