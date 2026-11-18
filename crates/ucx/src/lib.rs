//! Safe RAII wrapper over UCX `ucp`. All UCX `unsafe` lives in this crate.
//!
//! Ownership: `Arc<Context>` → [`Worker`] (single thread, `!Send`) → [`Endpoint`] /
//! [`Listener`], each holding its parent alive. Dropping releases the C resource.
//!
//! Logging (target `ucx`): `info` connections/listeners, `debug` resource lifecycle,
//! `trace` every AM sent/received. UCX's internal logs: `UCX_LOG_LEVEL=debug`.

mod context;
mod endpoint;
mod listener;
mod sockaddr;
mod status;
mod worker;

pub use context::Context;
pub use endpoint::{Endpoint, ErrorMode, TransportLane};
pub use listener::Listener;
pub use status::{Error, Result, Status};
pub use worker::{AmId, AmMessage, EpId, Worker};

use std::fmt;

use ucx_sys as sys;

/// Version of the UCX library that was loaded at runtime.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct Version {
    pub major: u32,
    pub minor: u32,
    pub release: u32,
}

impl Version {
    /// Version of the UCX headers this crate was compiled against.
    pub const COMPILED: (u32, u32) = (sys::UCP_API_MAJOR, sys::UCP_API_MINOR);

    /// Queries the dynamically loaded library.
    pub fn runtime() -> Self {
        let (mut major, mut minor, mut release) = (0, 0, 0);
        // SAFETY: ucp_get_version only writes to the three provided out-pointers,
        // which are valid, aligned, and live for the duration of the call.
        unsafe { sys::ucp_get_version(&mut major, &mut minor, &mut release) };
        Self {
            major,
            minor,
            release,
        }
    }
}

impl fmt::Display for Version {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}.{}.{}", self.major, self.minor, self.release)
    }
}

#[cfg(test)]
mod tests;
