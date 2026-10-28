//! Workload engine with the same semantics as `tools/readbench` (Go):
//! same offset plans (sequential regions per thread, or splitmix64-driven
//! random window-aligned offsets with seed 42), same percentile method, same
//! JSON fields, so the two outputs can be joined row by row.

use fusedata_core::pattern::splitmix64;
use serde_json::json;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;
use std::time::Instant;

#[derive(Clone, Debug)]
pub struct Workload {
    pub size: i64,
    pub window: i64,
    pub pattern: String,
    pub threads: usize,
    pub total_bytes: i64,
    pub seed: u64,
}

impl Workload {
    pub fn offsets(&self) -> Vec<Vec<i64>> {
        let mut out = vec![Vec::new(); self.threads];
        let per_thread = self.total_bytes / self.threads as i64;
        let ops = (per_thread / self.window).max(1);
        for (t, plan) in out.iter_mut().enumerate() {
            if self.pattern == "seq" {
                let region = self.size / self.threads as i64;
                let mut start = t as i64 * region;
                start -= start % self.window;
                for i in 0..ops {
                    let off = start + i * self.window;
                    if off + self.window > start + region || off + self.window > self.size {
                        break;
                    }
                    plan.push(off);
                }
            } else {
                let slots = (self.size - self.window) / self.window;
                let mut s = self.seed.wrapping_add((t as u64).wrapping_mul(0x9e37_79b9_7f4a_7c15));
                for _ in 0..ops {
                    s = splitmix64(s);
                    plan.push((s % (slots as u64 + 1)) as i64 * self.window);
                }
            }
        }
        out
    }
}

pub fn cpu_seconds() -> f64 {
    let mut ru: libc::rusage = unsafe { std::mem::zeroed() };
    unsafe { libc::getrusage(libc::RUSAGE_SELF, &mut ru) };
    let tv = |t: libc::timeval| t.tv_sec as f64 + t.tv_usec as f64 / 1e6;
    tv(ru.ru_utime) + tv(ru.ru_stime)
}

/// Run `w`, calling `read(thread_state, off, len)` per op. Each thread gets
/// its own state from `init` (buffers, etc.).
pub fn run<S, I, R>(w: &Workload, imp: &str, layer: &str, init: I, read: R) -> anyhow::Result<serde_json::Value>
where
    I: Fn() -> S + Sync,
    R: Fn(&mut S, i64, usize) -> anyhow::Result<usize> + Sync,
{
    let plan = w.offsets();
    let total = AtomicU64::new(0);
    let lats: Mutex<Vec<u64>> = Mutex::new(Vec::new());
    let err: Mutex<Option<anyhow::Error>> = Mutex::new(None);
    let cpu0 = cpu_seconds();
    let t0 = Instant::now();
    std::thread::scope(|sc| {
        for offs in &plan {
            let (init, read, total, lats, err) = (&init, &read, &total, &lats, &err);
            sc.spawn(move || {
                let mut st = init();
                let mut mine = Vec::with_capacity(offs.len());
                for &off in offs {
                    let t = Instant::now();
                    match read(&mut st, off, w.window as usize) {
                        Ok(n) => {
                            mine.push(t.elapsed().as_nanos() as u64);
                            total.fetch_add(n as u64, Ordering::Relaxed);
                        }
                        Err(e) => {
                            err.lock().unwrap().get_or_insert(e);
                            return;
                        }
                    }
                }
                lats.lock().unwrap().extend(mine);
            });
        }
    });
    let secs = t0.elapsed().as_secs_f64();
    let cpu = cpu_seconds() - cpu0;
    if let Some(e) = err.into_inner().unwrap() {
        return Err(e);
    }
    let mut all = lats.into_inner().unwrap();
    all.sort_unstable();
    let pct = |p: f64| -> f64 {
        if all.is_empty() {
            return 0.0;
        }
        let idx = ((p * all.len() as f64).ceil() as usize).saturating_sub(1);
        all[idx] as f64 / 1e3
    };
    let b = total.load(Ordering::Relaxed);
    Ok(json!({
        "impl": imp, "layer": layer, "pattern": w.pattern, "window_kib": w.window >> 10,
        "threads": w.threads, "bytes": b, "ops": all.len(), "seconds": secs,
        "mb_per_s": b as f64 / (1u64 << 20) as f64 / secs,
        "p50_us": pct(0.50), "p99_us": pct(0.99),
        "cpu_s_per_gib": cpu / (b as f64 / (1u64 << 30) as f64),
    }))
}
