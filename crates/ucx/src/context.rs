use std::ptr;
use std::sync::Arc;

use tracing::debug;
use ucx_sys as sys;

use crate::status::{Result, check};

/// A UCP context: the process-wide UCX instance (transports, memory domains).
/// Shared via `Arc`; each thread creates its own [`crate::Worker`] from it.
#[derive(Debug)]
pub struct Context {
    pub(crate) raw: sys::ucp_context_h,
}

// SAFETY: the context is created with `mt_workers_shared = 1`, which makes it safe to
// create and use workers from multiple threads concurrently. The handle itself is an
// opaque pointer that UCX synchronizes internally.
unsafe impl Send for Context {}
// SAFETY: see `Send` above.
unsafe impl Sync for Context {}

impl Context {
    /// Creates a context with Active Messages and RMA enabled. Configuration comes from
    /// `UCX_*` environment variables (e.g. `UCX_TLS`, `UCX_LOG_LEVEL`).
    pub fn new() -> Result<Arc<Self>> {
        let mut config = ptr::null_mut();
        // SAFETY: NULL env prefix/filename means "defaults + UCX_* env"; `config` is a
        // valid out-pointer.
        check("ucp_config_read", unsafe {
            sys::ucp_config_read(ptr::null(), ptr::null(), &mut config)
        })?;
        let params = sys::ucp_params_t {
            field_mask: (sys::UCP_PARAM_FIELD_FEATURES | sys::UCP_PARAM_FIELD_MT_WORKERS_SHARED)
                .into(),
            features: (sys::UCP_FEATURE_AM | sys::UCP_FEATURE_RMA).into(),
            mt_workers_shared: 1,
            ..Default::default()
        };
        let mut raw = ptr::null_mut();
        // SAFETY: `params` and `config` are valid for the call; `ucp_init` is a static
        // inline wrapper around exactly this call with the compiled-against API version.
        let status = unsafe {
            sys::ucp_init_version(
                sys::UCP_API_MAJOR,
                sys::UCP_API_MINOR,
                &params,
                config,
                &mut raw,
            )
        };
        // SAFETY: `config` came from ucp_config_read and is released exactly once.
        unsafe { sys::ucp_config_release(config) };
        check("ucp_init", status)?;

        debug!(features = "am,rma", "ucp context created");
        Ok(Arc::new(Self { raw }))
    }
}

impl Drop for Context {
    fn drop(&mut self) {
        // SAFETY: all workers hold an `Arc<Context>`, so none outlive this call.
        unsafe { sys::ucp_cleanup(self.raw) };
        debug!("ucp context destroyed");
    }
}
