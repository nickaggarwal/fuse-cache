//! `burst`: concurrent cold-start style reads against a store (Go client or
//! store-front). Prints one JSON report.
//!
//!   burst --target 10.0.0.5:8081 --clients 32 chunk --object /models/m.safetensors:15400000000:16
//!   burst --target 10.0.0.5:8081 --clients 32 --window-kib 1024 --pattern rand blob --blob <hex>:<size>

use clap::{Parser, Subcommand};
use std::time::Duration;
use store_front::burst::{self, Mode, Opts};

#[derive(Parser)]
#[command(name = "burst", version, about = "Concurrent peer-read / blob-range load against a store node")]
struct Args {
    /// host:port of the store's HTTP API.
    #[arg(long)]
    target: String,
    #[arg(long, default_value_t = 16)]
    clients: usize,
    /// MiB each client reads (0 = one pass over every object).
    #[arg(long, default_value_t = 0)]
    mib_per_client: u64,
    /// seq | rand
    #[arg(long, default_value = "seq")]
    pattern: String,
    /// Range window for blob mode, KiB.
    #[arg(long, default_value_t = 1024)]
    window_kib: u64,
    #[arg(long, env = "FUSE_API_KEY", hide_env_values = true)]
    api_key: Option<String>,
    #[arg(long, default_value_t = 42)]
    seed: u64,
    #[arg(long, default_value_t = 120)]
    timeout_sec: u64,
    /// Check content against readbench's deterministic pattern.
    #[arg(long)]
    verify: bool,
    #[command(subcommand)]
    mode: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Whole chunk objects via /api/peer/read.
    Chunk {
        /// path:size_bytes:stride_mib (repeatable).
        #[arg(long = "object", required = true)]
        objects: Vec<String>,
    },
    /// Range windows via /v2/x/blobs/sha256:<hex>.
    Blob {
        /// hex_digest:size_bytes (repeatable).
        #[arg(long = "blob", required = true)]
        blobs: Vec<String>,
    },
}

fn main() {
    let a = Args::parse();
    let mode = match &a.mode {
        Cmd::Chunk { objects } => Mode::Chunk(
            objects
                .iter()
                .map(|s| {
                    let p: Vec<&str> = s.rsplitn(3, ':').collect();
                    if p.len() != 3 {
                        eprintln!("bad --object {s} (want path:size:stride_mib)");
                        std::process::exit(2);
                    }
                    (p[2].to_string(), p[1].parse().expect("size"), p[0].parse::<u64>().expect("stride") << 20)
                })
                .collect(),
        ),
        Cmd::Blob { blobs } => Mode::Blob(
            blobs
                .iter()
                .map(|s| {
                    let (h, n) = s.split_once(':').unwrap_or_else(|| {
                        eprintln!("bad --blob {s} (want hex:size)");
                        std::process::exit(2)
                    });
                    (h.trim_start_matches("sha256:").to_string(), n.parse().expect("size"))
                })
                .collect(),
        ),
    };
    let o = Opts {
        target: a.target,
        mode,
        clients: a.clients.max(1),
        bytes_per_client: a.mib_per_client << 20,
        window: a.window_kib << 10,
        pattern: a.pattern,
        api_key: a.api_key,
        seed: a.seed,
        timeout: Duration::from_secs(a.timeout_sec),
        verify: if a.verify { Some(burst::check_pattern) } else { None },
    };
    let r = burst::run(&o);
    println!("{}", serde_json::to_string(&r).unwrap());
    if r.errors > 0 {
        std::process::exit(1);
    }
}
