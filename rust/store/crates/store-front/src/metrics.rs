//! Prometheus metrics in the Go client's `fuse_*` style, served on
//! `/rust/metrics` (`/metrics` belongs to Go and is proxied).

use std::fmt::Write as _;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::time::Duration;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Route {
    PeerRead,
    Blob,
    RegistryPing,
    Proxy,
    Own,
    /// Native PUT (`PUT /api/files/<path>`), staged here and committed by Go.
    Put,
}

impl Route {
    const ALL: [Route; 6] = [Route::PeerRead, Route::Blob, Route::RegistryPing, Route::Proxy, Route::Own, Route::Put];
    fn name(self) -> &'static str {
        match self {
            Route::PeerRead => "peer_read",
            Route::Blob => "blob",
            Route::RegistryPing => "registry_ping",
            Route::Proxy => "proxy",
            Route::Own => "front",
            Route::Put => "put",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Outcome {
    /// Served from the cache dir by this process.
    Local,
    /// Forwarded to the Go client (a native route that missed, or a
    /// proxied route).
    Forwarded,
    /// Rejected by the in-flight limit (503).
    Busy,
    /// Answered with an error status by this process (400/401/416/...).
    Rejected,
    /// I/O or backend failure (502, broken connection).
    Error,
    /// The client went away mid-response (a requester that cancelled a
    /// hedged or raced read) or mid-upload: not a server fault.
    Aborted,
}

impl Outcome {
    const ALL: [Outcome; 6] = [Outcome::Local, Outcome::Forwarded, Outcome::Busy, Outcome::Rejected, Outcome::Error, Outcome::Aborted];
    fn name(self) -> &'static str {
        match self {
            Outcome::Local => "local",
            Outcome::Forwarded => "forwarded",
            Outcome::Busy => "busy",
            Outcome::Rejected => "rejected",
            Outcome::Error => "error",
            Outcome::Aborted => "aborted",
        }
    }
}

/// Upper bounds in seconds.
const BUCKETS: [f64; 14] = [0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0];

#[derive(Default)]
struct Histogram {
    counts: [AtomicU64; BUCKETS.len() + 1],
    sum_us: AtomicU64,
}

pub struct Metrics {
    requests: [[AtomicU64; 6]; 6],
    bytes: [[AtomicU64; 6]; 6],
    latency: [Histogram; 6],
    pub inflight: AtomicI64,
    pub native_inflight: AtomicI64,
    pub connections: AtomicI64,
    pub backend_errors: AtomicU64,
}

impl Default for Metrics {
    fn default() -> Self {
        Metrics {
            requests: Default::default(),
            bytes: Default::default(),
            latency: Default::default(),
            inflight: AtomicI64::new(0),
            native_inflight: AtomicI64::new(0),
            connections: AtomicI64::new(0),
            backend_errors: AtomicU64::new(0),
        }
    }
}

fn ri(r: Route) -> usize {
    Route::ALL.iter().position(|x| *x == r).unwrap()
}
fn oi(o: Outcome) -> usize {
    Outcome::ALL.iter().position(|x| *x == o).unwrap()
}

impl Metrics {
    pub fn record(&self, route: Route, outcome: Outcome, bytes: u64, dur: Duration) {
        self.requests[ri(route)][oi(outcome)].fetch_add(1, Ordering::Relaxed);
        self.bytes[ri(route)][oi(outcome)].fetch_add(bytes, Ordering::Relaxed);
        let h = &self.latency[ri(route)];
        let s = dur.as_secs_f64();
        let b = BUCKETS.iter().position(|ub| s <= *ub).unwrap_or(BUCKETS.len());
        h.counts[b].fetch_add(1, Ordering::Relaxed);
        h.sum_us.fetch_add(dur.as_micros() as u64, Ordering::Relaxed);
    }

    pub fn count(&self, route: Route, outcome: Outcome) -> u64 {
        self.requests[ri(route)][oi(outcome)].load(Ordering::Relaxed)
    }

    pub fn render(&self) -> String {
        let mut o = String::new();
        o.push_str("# HELP fuse_front_requests_total Requests handled by store-front, by route and outcome.\n# TYPE fuse_front_requests_total counter\n");
        for r in Route::ALL {
            for oc in Outcome::ALL {
                let _ = writeln!(o, "fuse_front_requests_total{{route=\"{}\",outcome=\"{}\"}} {}", r.name(), oc.name(), self.count(r, oc));
            }
        }
        o.push_str("# HELP fuse_front_response_bytes_total Body bytes moved (sent for reads, received for PUTs), by route and outcome.\n# TYPE fuse_front_response_bytes_total counter\n");
        for r in Route::ALL {
            for oc in Outcome::ALL {
                let _ = writeln!(
                    o,
                    "fuse_front_response_bytes_total{{route=\"{}\",outcome=\"{}\"}} {}",
                    r.name(),
                    oc.name(),
                    self.bytes[ri(r)][oi(oc)].load(Ordering::Relaxed)
                );
            }
        }
        o.push_str(
            "# HELP fuse_front_request_duration_seconds Time from request head to last response byte.\n# TYPE fuse_front_request_duration_seconds histogram\n",
        );
        for r in Route::ALL {
            let h = &self.latency[ri(r)];
            let mut cum = 0u64;
            for (i, ub) in BUCKETS.iter().enumerate() {
                cum += h.counts[i].load(Ordering::Relaxed);
                let _ = writeln!(o, "fuse_front_request_duration_seconds_bucket{{route=\"{}\",le=\"{}\"}} {}", r.name(), ub, cum);
            }
            cum += h.counts[BUCKETS.len()].load(Ordering::Relaxed);
            let _ = writeln!(o, "fuse_front_request_duration_seconds_bucket{{route=\"{}\",le=\"+Inf\"}} {}", r.name(), cum);
            let _ = writeln!(o, "fuse_front_request_duration_seconds_sum{{route=\"{}\"}} {}", r.name(), h.sum_us.load(Ordering::Relaxed) as f64 / 1e6);
            let _ = writeln!(o, "fuse_front_request_duration_seconds_count{{route=\"{}\"}} {}", r.name(), cum);
        }
        let g = |o: &mut String, name: &str, help: &str, v: i64| {
            let _ = write!(o, "# HELP {name} {help}\n# TYPE {name} gauge\n{name} {v}\n");
        };
        g(&mut o, "fuse_front_inflight_requests", "Requests being handled.", self.inflight.load(Ordering::Relaxed));
        g(&mut o, "fuse_front_native_inflight", "Native serves holding an admission slot.", self.native_inflight.load(Ordering::Relaxed));
        g(&mut o, "fuse_front_open_connections", "Client connections open.", self.connections.load(Ordering::Relaxed));
        let _ = write!(
            o,
            "# HELP fuse_front_backend_errors_total Failures connecting to or talking to the Go client.\n# TYPE fuse_front_backend_errors_total counter\nfuse_front_backend_errors_total {}\n",
            self.backend_errors.load(Ordering::Relaxed)
        );
        o
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn render_has_series() {
        let m = Metrics::default();
        m.record(Route::Blob, Outcome::Local, 100, Duration::from_millis(3));
        m.record(Route::Blob, Outcome::Forwarded, 7, Duration::from_secs(20));
        let t = m.render();
        assert!(t.contains("fuse_front_requests_total{route=\"blob\",outcome=\"local\"} 1"));
        assert!(t.contains("fuse_front_response_bytes_total{route=\"blob\",outcome=\"local\"} 100"));
        assert!(t.contains("fuse_front_request_duration_seconds_bucket{route=\"blob\",le=\"0.005\"} 1"));
        assert!(t.contains("fuse_front_request_duration_seconds_bucket{route=\"blob\",le=\"+Inf\"} 2"));
    }
}
