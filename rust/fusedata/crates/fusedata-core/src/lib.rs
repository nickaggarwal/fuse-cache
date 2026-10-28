//! Read data path for the fuse-client NVMe cache, as a library:
//!
//! * [`layout`] serves windows straight off a cache directory written by the
//!   Go client (whole files, per-chunk files, `.sha256` sidecars);
//! * [`peer`] speaks the `/api/peer/read` raw-HTTP protocol as client and
//!   server (sendfile), interoperable with Go peers;
//! * [`azure`] reads blobs with Shared Key auth and parallel ranged GETs;
//! * [`coord`] is a minimal coordinator lookup client;
//! * [`tiered`] falls through local -> peer -> cloud for one read.

pub mod azure;
pub mod chunk;
pub mod coord;
pub mod layout;
pub mod pattern;
pub mod peer;
pub mod tiered;
