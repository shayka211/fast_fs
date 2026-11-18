//! UCP endpoints: a connection to one peer.

use std::cell::Cell;
use std::ffi::{CStr, CString, c_void};
use std::fmt;
use std::ptr;
use std::rc::Rc;
use std::time::Duration;

use tracing::{debug, info, trace, warn};
use ucx_sys as sys;

use crate::status::{Error, Result, Status, check};
use crate::worker::{AmId, Worker, WorkerInner};

const CLOSE_TIMEOUT: Duration = Duration::from_secs(2);

/// A connection to one peer, created from the peer's worker address. Usable immediately:
/// UCX queues sends until wire-up completes, so no Connecting/Connected typestate is needed.
///
/// There is deliberately no UCX listener/sockaddr connect: addresses are exchanged
/// out-of-band (a plain TCP socket at bootstrap, later cluster config/Raft), so UCX never
/// activates its TCP transport and the data path stays on shared memory / RDMA.
pub struct Endpoint {
    raw: sys::ucp_ep_h,
    /// Boxed so the error callback's `arg` pointer stays stable.
    state: Box<EpState>,
    worker: Rc<WorkerInner>,
}

struct EpState {
    peer: String,
    mode: ErrorMode,
    failed: Cell<Option<Status>>,
}

/// How UCX handles a failing peer. This is a real trade-off, not a detail:
///
/// - [`ErrorMode::Peer`]: UCX detects peer failure and reports it ([`Endpoint::failed`]),
///   but **excludes shared-memory transports** (they can't detect a dead peer), so
///   same-host traffic falls back to TCP (~4.5 µs vs ~0.07 µs one-way).
/// - [`ErrorMode::None`]: all transports, including shm. A dead peer is *not* reported;
///   the application must detect it (Raft heartbeats / RPC timeouts do).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum ErrorMode {
    #[default]
    Peer,
    None,
}

impl ErrorMode {
    fn raw(self) -> sys::ucp_err_handling_mode_t {
        match self {
            Self::Peer => sys::UCP_ERR_HANDLING_MODE_PEER,
            Self::None => sys::UCP_ERR_HANDLING_MODE_NONE,
        }
    }
}

/// One lane of an endpoint, e.g. `posix/memory` or `tcp/lo`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransportLane {
    pub transport: String,
    pub device: String,
}

impl fmt::Display for TransportLane {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}/{}", self.transport, self.device)
    }
}

impl Endpoint {
    /// Connects to a peer's worker address ([`Worker::address`]); `peer` is a label for
    /// logs. This is how `ucx_perftest` and `ucp_hello_world` connect.
    pub fn connect(
        worker: &Worker,
        address: &[u8],
        peer: impl Into<String>,
        mode: ErrorMode,
    ) -> Result<Self> {
        let state = Box::new(EpState {
            peer: peer.into(),
            mode,
            failed: Cell::new(None),
        });
        // Named endpoints show up in UCX's own logs (UCX_LOG_LEVEL=debug).
        let name = CString::new(format!("fastfs->{}", state.peer)).unwrap_or_default();
        let params = sys::ucp_ep_params_t {
            field_mask: u64::from(
                sys::UCP_EP_PARAM_FIELD_REMOTE_ADDRESS
                    | sys::UCP_EP_PARAM_FIELD_ERR_HANDLER
                    | sys::UCP_EP_PARAM_FIELD_ERR_HANDLING_MODE
                    | sys::UCP_EP_PARAM_FIELD_NAME,
            ),
            address: address.as_ptr().cast(),
            err_mode: mode.raw(),
            err_handler: sys::ucp_err_handler {
                cb: Some(ep_error_cb),
                arg: ptr::from_ref(&*state).cast_mut().cast(),
            },
            name: name.as_ptr(),
            ..Default::default()
        };

        let mut raw = ptr::null_mut();
        // SAFETY: params (and the address/name they point to) are valid for the call;
        // the error-handler arg is boxed and outlives the endpoint.
        check("ucp_ep_create", unsafe {
            sys::ucp_ep_create(worker.inner.raw, &params, &mut raw)
        })?;
        info!(peer = %state.peer, ?mode, "endpoint created");
        Ok(Self {
            raw,
            state,
            worker: Rc::clone(&worker.inner),
        })
    }

    pub fn peer(&self) -> &str {
        &self.state.peer
    }

    /// Set once UCX reports the peer failed or disconnected; the endpoint is then unusable.
    pub fn failed(&self) -> Option<Status> {
        self.state.failed.get()
    }

    /// Sends an Active Message and waits (progressing the worker) until the buffers can be
    /// reused. Blocking; M3 replaces this with async. The receiver does not learn which
    /// endpoint a message came from: protocols carry the sender's identity in `header`.
    pub fn send_am(&self, id: AmId, header: &[u8], data: &[u8]) -> Result<()> {
        if let Some(status) = self.failed() {
            return Err(Error {
                op: "ucp_am_send_nbx",
                status,
            });
        }
        trace!(peer = %self.state.peer, am_id = id, header_len = header.len(), data_len = data.len(), "am send");
        let params = sys::ucp_request_param_t::default();
        // SAFETY: header/data stay borrowed until `wait` returns, i.e. until UCX is done
        // with them.
        let status = unsafe {
            sys::ucp_am_send_nbx(
                self.raw,
                id.into(),
                header.as_ptr().cast(),
                header.len(),
                data.as_ptr().cast(),
                data.len(),
                &params,
            )
        };
        self.worker.wait("ucp_am_send_nbx", status)
    }

    /// Transports and devices this endpoint selected (meaningful once wire-up is done).
    pub fn transports(&self) -> Result<Vec<TransportLane>> {
        const MAX_LANES: usize = 16;
        let mut entries = [sys::ucp_transport_entry_t {
            transport_name: ptr::null(),
            device_name: ptr::null(),
        }; MAX_LANES];
        let mut attr = sys::ucp_ep_attr_t {
            field_mask: sys::UCP_EP_ATTR_FIELD_TRANSPORTS.into(),
            transports: sys::ucp_transports_t {
                entries: entries.as_mut_ptr(),
                num_entries: MAX_LANES as u32,
                entry_size: size_of::<sys::ucp_transport_entry_t>(),
            },
            ..Default::default()
        };
        // SAFETY: `entries` has room for `num_entries` items of `entry_size` bytes.
        check("ucp_ep_query", unsafe {
            sys::ucp_ep_query(self.raw, &mut attr)
        })?;
        let n = (attr.transports.num_entries as usize).min(MAX_LANES);
        let to_string = |p: *const std::ffi::c_char| {
            if p.is_null() {
                String::new()
            } else {
                // SAFETY: UCX returns NUL-terminated strings valid while the ep lives.
                unsafe { CStr::from_ptr(p) }.to_string_lossy().into_owned()
            }
        };
        Ok(entries[..n]
            .iter()
            .map(|e| TransportLane {
                transport: to_string(e.transport_name),
                device: to_string(e.device_name),
            })
            .collect())
    }
}

impl fmt::Debug for Endpoint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Endpoint")
            .field("peer", &self.state.peer)
            .field("failed", &self.failed())
            .finish_non_exhaustive()
    }
}

impl Drop for Endpoint {
    fn drop(&mut self) {
        // Graceful close flushes outstanding sends. A failed peer can't be flushed, so
        // force-close — which UCX only permits in `ErrorMode::Peer`.
        let flags = if self.failed().is_some() && self.state.mode == ErrorMode::Peer {
            sys::UCP_EP_CLOSE_FLAG_FORCE
        } else {
            0
        };
        let params = sys::ucp_request_param_t {
            op_attr_mask: sys::UCP_OP_ATTR_FIELD_FLAGS,
            flags,
            ..Default::default()
        };
        // SAFETY: `raw` is a live endpoint closed exactly once; the worker is alive (Rc).
        let status = unsafe { sys::ucp_ep_close_nbx(self.raw, &params) };
        match self
            .worker
            .wait_until("ucp_ep_close_nbx", status, Some(CLOSE_TIMEOUT))
        {
            Ok(()) => debug!(peer = %self.state.peer, "endpoint closed"),
            Err(e) if e.status.is_disconnect() => {
                debug!(peer = %self.state.peer, "endpoint closed (peer already gone)");
            }
            Err(e) => warn!(peer = %self.state.peer, error = %e, "endpoint close failed"),
        }
    }
}

unsafe extern "C" fn ep_error_cb(arg: *mut c_void, _ep: sys::ucp_ep_h, status: sys::ucs_status_t) {
    // SAFETY: `arg` is the endpoint's boxed EpState, alive until the endpoint is closed.
    let state = unsafe { &*arg.cast::<EpState>() };
    let status = Status(status);
    state.failed.set(Some(status));
    if status.is_disconnect() {
        info!(peer = %state.peer, %status, "peer disconnected");
    } else {
        warn!(peer = %state.peer, %status, "endpoint failed");
    }
}
