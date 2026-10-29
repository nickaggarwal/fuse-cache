//! `store-front` binary: flags, cache-dir discovery, signals, serve.

use clap::Parser;
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};
use store_front::flog;
use store_front::server::{Config, Server};

#[derive(Parser)]
#[command(
    name = "store-front",
    version,
    about = "Serves peer reads and OCI blob ranges from the store node's cache dir; proxies everything else to the Go client"
)]
struct Args {
    /// Cache dir (the Go client's -nvme). Overrides --node-init-config.
    #[arg(long, env = "FRONT_CACHE_DIR")]
    cache_dir: Option<PathBuf>,
    /// node-init.json written by node-init; its cache_dir (under
    /// --host-root) is used when --cache-dir is not given, as the Go client
    /// does with -node-init-config / -node-init-host-root.
    #[arg(long, env = "FRONT_NODE_INIT_CONFIG")]
    node_init_config: Option<PathBuf>,
    #[arg(long, env = "FRONT_HOST_ROOT", default_value = "/")]
    host_root: PathBuf,
    /// How long to wait for node-init.json to appear.
    #[arg(long, default_value_t = 60)]
    node_init_wait_sec: u64,
    #[arg(long, env = "FRONT_LISTEN", default_value = "0.0.0.0:8081")]
    listen: String,
    /// The Go client's HTTP address (run Go with -port=8091 -advertise-port=8081).
    #[arg(long, env = "FRONT_BACKEND", default_value = "127.0.0.1:8091")]
    backend: String,
    /// Coordinator HTTP address, for the published chunk stride of whole
    /// files (`_chunk_N` reads). Without it those requests go to Go.
    #[arg(long, env = "FRONT_COORDINATOR")]
    coordinator: Option<String>,
    /// Required X-API-Key (same rule as the Go client's -api-key).
    #[arg(long, env = "FUSE_API_KEY", hide_env_values = true)]
    api_key: Option<String>,
    /// Concurrent native peer serves before answering 503 (Go
    /// -peer-serve-max-inflight).
    #[arg(long, default_value_t = 64)]
    max_inflight: usize,
    /// Go's -max-upload-gb: native PUTs up to this size; larger ones are
    /// forwarded so Go answers 413.
    #[arg(long, default_value_t = 2)]
    max_upload_gb: u64,
    /// Forward every PUT to Go instead of staging it here.
    #[arg(long)]
    no_native_put: bool,
    /// Seconds to finish in-flight requests after SIGTERM/SIGINT.
    #[arg(long, default_value_t = 30)]
    drain_sec: u64,
}

static STOP: AtomicBool = AtomicBool::new(false);

extern "C" fn on_signal(_: libc::c_int) {
    STOP.store(true, Ordering::SeqCst);
}

fn cache_dir(a: &Args) -> anyhow_lite::Result<PathBuf> {
    if let Some(d) = &a.cache_dir {
        return Ok(d.clone());
    }
    let Some(cfg) = &a.node_init_config else {
        return Err("one of --cache-dir or --node-init-config is required".into());
    };
    let t = Instant::now();
    loop {
        match store_layout::NodeInit::read(cfg) {
            Ok(n) => {
                let d = n.resolve(&a.host_root);
                flog!("node-init: using discovered cache dir {}", d.display());
                return Ok(d);
            }
            Err(e) if t.elapsed() < Duration::from_secs(a.node_init_wait_sec) => {
                if t.elapsed() < Duration::from_secs(1) {
                    flog!("node-init: waiting for {} ({e})", cfg.display());
                }
                std::thread::sleep(Duration::from_secs(1));
            }
            Err(e) => return Err(format!("node-init config {}: {e}", cfg.display()).into()),
        }
    }
}

/// Tiny error alias so the binary needs no extra crate.
mod anyhow_lite {
    pub type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;
}

fn main() -> anyhow_lite::Result<()> {
    let a = Args::parse();
    let dir = cache_dir(&a)?;
    if !Path::new(&dir).is_dir() {
        flog!("warning: cache dir {} does not exist yet; native reads will forward until it does", dir.display());
    }
    let mut cfg = Config::new(dir.clone(), a.backend.clone());
    cfg.coordinator = a.coordinator.clone();
    cfg.api_key = a.api_key.clone().filter(|k| !k.is_empty());
    cfg.max_inflight = a.max_inflight;
    cfg.max_upload = a.max_upload_gb << 30;
    cfg.native_put = !a.no_native_put;
    let srv = Server::new(cfg);

    unsafe {
        libc::signal(libc::SIGTERM, on_signal as usize);
        libc::signal(libc::SIGINT, on_signal as usize);
        libc::signal(libc::SIGPIPE, libc::SIG_IGN);
    }
    let watcher = srv.clone();
    std::thread::spawn(move || loop {
        if STOP.load(Ordering::SeqCst) {
            flog!("signal received: draining (up to {}s)", a.drain_sec);
            watcher.stop();
            return;
        }
        std::thread::sleep(Duration::from_millis(100));
    });

    let l = TcpListener::bind(&a.listen)?;
    flog!(
        "listening on {} cache_dir={} backend={} coordinator={} max_inflight={} auth={}",
        a.listen,
        dir.display(),
        a.backend,
        a.coordinator.as_deref().unwrap_or("-"),
        a.max_inflight,
        if srv.cfg.api_key.is_some() { "on" } else { "off" }
    );
    srv.serve(l, Duration::from_secs(a.drain_sec))?;
    flog!("stopped");
    Ok(())
}
