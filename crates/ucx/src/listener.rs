//! Server-side listener that accepts client-server connections on a socket address.

use std::cell::RefCell;
use std::collections::VecDeque;
use std::ffi::c_void;
use std::net::SocketAddr;
use std::ptr;
use std::rc::Rc;

use tracing::{debug, info, warn};
use ucx_sys as sys;

use crate::endpoint::{Endpoint, ErrorMode};
use crate::sockaddr::{RawSockAddr, from_storage};
use crate::status::{Result, check};
use crate::worker::{Worker, WorkerInner};

type ConnQueue = RefCell<VecDeque<(sys::ucp_conn_request_h, Option<SocketAddr>)>>;

#[derive(Debug)]
pub struct Listener {
    raw: sys::ucp_listener_h,
    local_addr: SocketAddr,
    mode: ErrorMode,
    /// Boxed so the connection callback's `arg` pointer stays stable.
    pending: Box<ConnQueue>,
    worker: Rc<WorkerInner>,
}

impl Listener {
    /// Listens on `addr` (port 0 picks a free port; see [`Self::local_addr`]). Accepted
    /// endpoints use `mode` (see [`ErrorMode`] for the shm trade-off).
    pub fn bind(worker: &Worker, addr: SocketAddr, mode: ErrorMode) -> Result<Self> {
        let pending: Box<ConnQueue> = Box::default();
        let sockaddr = RawSockAddr::new(addr);
        let params = sys::ucp_listener_params_t {
            field_mask: (sys::UCP_LISTENER_PARAM_FIELD_SOCK_ADDR
                | sys::UCP_LISTENER_PARAM_FIELD_CONN_HANDLER)
                .into(),
            sockaddr: sockaddr.as_ucs(),
            conn_handler: sys::ucp_listener_conn_handler_t {
                cb: Some(conn_cb),
                arg: ptr::from_ref(&*pending).cast_mut().cast(),
            },
            ..Default::default()
        };
        let mut raw = ptr::null_mut();
        // SAFETY: params and sockaddr valid for the call; `pending` is boxed and outlives
        // the listener.
        check("ucp_listener_create", unsafe {
            sys::ucp_listener_create(worker.inner.raw, &params, &mut raw)
        })?;

        let mut attr = sys::ucp_listener_attr_t {
            field_mask: sys::UCP_LISTENER_ATTR_FIELD_SOCKADDR.into(),
            ..Default::default()
        };
        // SAFETY: valid listener and attr out-struct.
        check("ucp_listener_query", unsafe {
            sys::ucp_listener_query(raw, &mut attr)
        })?;
        let local_addr = from_storage(&attr.sockaddr).unwrap_or(addr);
        info!(%local_addr, "listening");

        Ok(Self {
            raw,
            local_addr,
            mode,
            pending,
            worker: Rc::clone(&worker.inner),
        })
    }

    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    /// Accepts the next pending connection, if one arrived during `progress`.
    pub fn accept(&self) -> Result<Option<Endpoint>> {
        let Some((req, peer)) = self.pending.borrow_mut().pop_front() else {
            return Ok(None);
        };
        Endpoint::accept(&self.worker, req, peer, self.mode).map(Some)
    }
}

impl Drop for Listener {
    fn drop(&mut self) {
        for (req, peer) in self.pending.borrow_mut().drain(..) {
            warn!(?peer, "rejecting unaccepted connection");
            // SAFETY: an unaccepted request from this listener, rejected exactly once.
            unsafe { sys::ucp_listener_reject(self.raw, req) };
        }
        // SAFETY: live listener destroyed exactly once; the worker is alive (Rc).
        unsafe { sys::ucp_listener_destroy(self.raw) };
        debug!(local_addr = %self.local_addr, "listener destroyed");
    }
}

unsafe extern "C" fn conn_cb(conn_request: sys::ucp_conn_request_h, arg: *mut c_void) {
    let mut attr = sys::ucp_conn_request_attr_t {
        field_mask: sys::UCP_CONN_REQUEST_ATTR_FIELD_CLIENT_ADDR.into(),
        ..Default::default()
    };
    // SAFETY: valid conn request handed to us by UCX; attr is a valid out-struct.
    let peer = (unsafe { sys::ucp_conn_request_query(conn_request, &mut attr) } == sys::UCS_OK)
        .then(|| from_storage(&attr.client_address))
        .flatten();
    match peer {
        Some(peer) => info!(%peer, "incoming connection"),
        None => info!("incoming connection (unknown address)"),
    }
    // SAFETY: `arg` is the listener's boxed queue, alive until ucp_listener_destroy.
    let pending = unsafe { &*arg.cast::<ConnQueue>() };
    pending.borrow_mut().push_back((conn_request, peer));
}
