//! UCP worker: the progress engine, plus Active Message receive.

use std::cell::{Cell, RefCell};
use std::collections::VecDeque;
use std::ffi::c_void;
use std::ptr;
use std::rc::Rc;
use std::slice;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tracing::{debug, error, trace, warn};
use ucx_sys as sys;

use crate::context::Context;
use crate::status::{Error, Result, Status, StatusPtr, check};

/// Active Message handler id. Each service registers its own id.
pub type AmId = u16;

/// A fully received Active Message (eager and rendezvous are both delivered whole).
#[derive(Debug)]
pub struct AmMessage {
    pub id: AmId,
    pub header: Vec<u8>,
    pub data: Vec<u8>,
}

/// Single-threaded UCP worker. `!Send` by construction: a worker (and everything
/// created from it) must stay on the thread that created it.
#[derive(Debug)]
pub struct Worker {
    pub(crate) inner: Rc<WorkerInner>,
}

#[derive(Debug)]
pub(crate) struct WorkerInner {
    pub(crate) raw: sys::ucp_worker_h,
    inbox: RefCell<VecDeque<AmMessage>>,
    /// Per-id callback args. Boxed so their addresses stay stable when the Vec grows:
    /// UCX holds raw pointers to them.
    #[allow(clippy::vec_box, reason = "UCX stores pointers to the elements")]
    handlers: RefCell<Vec<Box<AmHandlerArg>>>,
    pending_rndv: Cell<usize>,
    // Dropped after `raw` is destroyed (field order + explicit Drop).
    _ctx: Arc<Context>,
}

#[derive(Debug)]
struct AmHandlerArg {
    worker: *const WorkerInner,
    id: AmId,
}

/// A rendezvous receive in flight; owned by UCX (via `user_data`) until completion.
struct PendingRndv {
    worker: *const WorkerInner,
    id: AmId,
    header: Vec<u8>,
    data: Vec<u8>,
}

impl Worker {
    pub fn new(ctx: &Arc<Context>) -> Result<Self> {
        let params = sys::ucp_worker_params_t {
            field_mask: sys::UCP_WORKER_PARAM_FIELD_THREAD_MODE.into(),
            thread_mode: sys::UCS_THREAD_MODE_SINGLE,
            ..Default::default()
        };
        let mut raw = ptr::null_mut();
        // SAFETY: valid context, params and out-pointer.
        check("ucp_worker_create", unsafe {
            sys::ucp_worker_create(ctx.raw, &params, &mut raw)
        })?;
        debug!(worker = ?raw, "ucp worker created");
        Ok(Self {
            inner: Rc::new(WorkerInner {
                raw,
                inbox: RefCell::default(),
                handlers: RefCell::default(),
                pending_rndv: Cell::new(0),
                _ctx: Arc::clone(ctx),
            }),
        })
    }

    /// Drives communication; callbacks (AM receive, errors, completions) run in here.
    /// Returns the number of events processed.
    #[inline]
    pub fn progress(&self) -> u32 {
        // SAFETY: the worker is valid and only used from this thread (`!Send`).
        unsafe { sys::ucp_worker_progress(self.inner.raw) }
    }

    /// Starts delivering Active Messages with this id into the inbox ([`Self::try_recv`]).
    pub fn register_am(&self, id: AmId) -> Result<()> {
        let arg = Box::new(AmHandlerArg {
            worker: Rc::as_ptr(&self.inner),
            id,
        });
        let params = sys::ucp_am_handler_param_t {
            field_mask: (sys::UCP_AM_HANDLER_PARAM_FIELD_ID
                | sys::UCP_AM_HANDLER_PARAM_FIELD_CB
                | sys::UCP_AM_HANDLER_PARAM_FIELD_ARG
                | sys::UCP_AM_HANDLER_PARAM_FIELD_FLAGS)
                .into(),
            id: id.into(),
            flags: sys::UCP_AM_FLAG_WHOLE_MSG,
            cb: Some(am_recv_cb),
            arg: ptr::from_ref(&*arg).cast_mut().cast(),
        };
        // SAFETY: `arg` is boxed and kept in `handlers` for the worker's lifetime, so the
        // pointer UCX stores stays valid until ucp_worker_destroy.
        check("ucp_worker_set_am_recv_handler", unsafe {
            sys::ucp_worker_set_am_recv_handler(self.inner.raw, &params)
        })?;
        self.inner.handlers.borrow_mut().push(arg);
        debug!(am_id = id, "registered AM handler");
        Ok(())
    }

    /// This worker's address blob, for peers to pass to [`crate::Endpoint::connect`].
    pub fn address(&self) -> Result<Vec<u8>> {
        let mut addr = ptr::null_mut();
        let mut len = 0;
        // SAFETY: valid worker and out-pointers.
        check("ucp_worker_get_address", unsafe {
            sys::ucp_worker_get_address(self.inner.raw, &mut addr, &mut len)
        })?;
        // SAFETY: UCX returned a buffer of `len` bytes, copied then released exactly once.
        let bytes = unsafe {
            let bytes = copy_bytes(addr.cast(), len);
            sys::ucp_worker_release_address(self.inner.raw, addr);
            bytes
        };
        debug!(len, "worker address packed");
        Ok(bytes)
    }

    /// Pops the next received Active Message, if any. Call [`Self::progress`] first.
    #[inline]
    pub fn try_recv(&self) -> Option<AmMessage> {
        self.inner.inbox.borrow_mut().pop_front()
    }
}

impl WorkerInner {
    /// Completes a non-blocking operation by progressing until it finishes.
    pub(crate) fn wait(&self, op: &'static str, ptr: sys::ucs_status_ptr_t) -> Result<()> {
        self.wait_until(op, ptr, None)
    }

    /// Like [`Self::wait`] but gives up (freeing the request, which UCX then releases on
    /// completion) after `timeout`.
    pub(crate) fn wait_until(
        &self,
        op: &'static str,
        ptr: sys::ucs_status_ptr_t,
        timeout: Option<Duration>,
    ) -> Result<()> {
        let req = match StatusPtr::decode(ptr) {
            StatusPtr::Done => return Ok(()),
            StatusPtr::Failed(status) => return Err(Error { op, status }),
            StatusPtr::Pending(req) => req,
        };
        let deadline = timeout.map(|t| Instant::now() + t);
        let status = loop {
            // SAFETY: valid worker on its owning thread; `req` is a live request.
            let status = unsafe {
                sys::ucp_worker_progress(self.raw);
                sys::ucp_request_check_status(req)
            };
            if status != sys::UCS_INPROGRESS {
                break status;
            }
            if deadline.is_some_and(|d| Instant::now() >= d) {
                warn!(op, "operation timed out; abandoning request");
                break sys::UCS_ERR_TIMED_OUT;
            }
        };
        // SAFETY: `req` is freed exactly once; UCX defers release if still in flight.
        unsafe { sys::ucp_request_free(req) };
        check(op, status)
    }

    fn deliver(&self, msg: AmMessage) {
        trace!(
            am_id = msg.id,
            header_len = msg.header.len(),
            data_len = msg.data.len(),
            "am recv"
        );
        self.inbox.borrow_mut().push_back(msg);
    }
}

impl Drop for WorkerInner {
    fn drop(&mut self) {
        let pending = self.pending_rndv.get();
        if pending > 0 {
            warn!(
                pending,
                "destroying worker with rendezvous receives in flight (leaked)"
            );
        }
        // SAFETY: endpoints/listeners hold an `Rc<WorkerInner>`, so all are gone by now.
        unsafe { sys::ucp_worker_destroy(self.raw) };
        debug!("ucp worker destroyed");
    }
}

/// Copies `len` bytes from a possibly-NULL C pointer.
///
/// # Safety
/// If `len > 0`, `ptr` must be valid for reads of `len` bytes.
unsafe fn copy_bytes(ptr: *const c_void, len: usize) -> Vec<u8> {
    if len == 0 {
        Vec::new()
    } else {
        // SAFETY: guaranteed by the caller.
        unsafe { slice::from_raw_parts(ptr.cast::<u8>(), len) }.to_vec()
    }
}

unsafe extern "C" fn am_recv_cb(
    arg: *mut c_void,
    header: *const c_void,
    header_length: usize,
    data: *mut c_void,
    length: usize,
    param: *const sys::ucp_am_recv_param_t,
) -> sys::ucs_status_t {
    // SAFETY: `arg` is the boxed AmHandlerArg registered in `register_am`; it and its
    // worker outlive every callback (callbacks only run inside progress on a live worker).
    let (worker, id) = unsafe {
        let arg = &*arg.cast::<AmHandlerArg>();
        (&*arg.worker, arg.id)
    };
    // SAFETY: UCX passes a valid param struct for the duration of the callback.
    let param = unsafe { &*param };
    // SAFETY: UCX guarantees `header` is valid for `header_length` bytes here.
    let header = unsafe { copy_bytes(header, header_length) };

    if param.recv_attr & u64::from(sys::UCP_AM_RECV_ATTR_FLAG_RNDV) == 0 {
        // Eager: data is valid only during this callback, so copy it out.
        // SAFETY: valid for `length` bytes during the callback.
        let data = unsafe { copy_bytes(data, length) };
        worker.deliver(AmMessage { id, header, data });
        return sys::UCS_OK;
    }

    // Rendezvous: `data` is a descriptor; fetch the payload into our own buffer.
    trace!(am_id = id, length, "am rndv recv started");
    let pending = Box::into_raw(Box::new(PendingRndv {
        worker,
        id,
        header,
        data: vec![0; length],
    }));
    let params = sys::ucp_request_param_t {
        op_attr_mask: sys::UCP_OP_ATTR_FIELD_CALLBACK | sys::UCP_OP_ATTR_FIELD_USER_DATA,
        cb: sys::ucp_request_param_t__bindgen_ty_1 {
            recv_am: Some(rndv_done_cb),
        },
        user_data: pending.cast(),
        ..Default::default()
    };
    // SAFETY: `pending` stays alive (leaked box) until `rndv_done_cb` or the immediate
    // paths below reclaim it; its buffer is exactly `length` bytes.
    let status = unsafe {
        sys::ucp_am_recv_data_nbx(
            worker.raw,
            data,
            (*pending).data.as_mut_ptr().cast(),
            length,
            &params,
        )
    };
    match StatusPtr::decode(status) {
        StatusPtr::Done => {
            // SAFETY: completed inline, so the callback will not run; reclaim the box.
            let p = unsafe { Box::from_raw(pending) };
            worker.deliver(p.into_message());
        }
        StatusPtr::Failed(s) => {
            // SAFETY: failed inline, the callback will not run; reclaim the box.
            drop(unsafe { Box::from_raw(pending) });
            error!(am_id = id, length, status = %s, "am rndv recv failed");
        }
        StatusPtr::Pending(_) => worker.pending_rndv.set(worker.pending_rndv.get() + 1),
    }
    sys::UCS_OK
}

impl PendingRndv {
    fn into_message(self) -> AmMessage {
        AmMessage {
            id: self.id,
            header: self.header,
            data: self.data,
        }
    }
}

unsafe extern "C" fn rndv_done_cb(
    request: *mut c_void,
    status: sys::ucs_status_t,
    length: usize,
    user_data: *mut c_void,
) {
    // SAFETY: `user_data` is the PendingRndv leaked in `am_recv_cb`; reclaimed once here.
    let mut p = unsafe { Box::from_raw(user_data.cast::<PendingRndv>()) };
    // SAFETY: the worker outlives its in-flight requests (callbacks run inside progress).
    let worker = unsafe { &*p.worker };
    worker.pending_rndv.set(worker.pending_rndv.get() - 1);
    if status == sys::UCS_OK {
        p.data.truncate(length);
        worker.deliver(p.into_message());
    } else {
        error!(am_id = p.id, status = %Status(status), "am rndv recv failed");
    }
    // SAFETY: completed request returned by ucp_am_recv_data_nbx, freed exactly once.
    unsafe { sys::ucp_request_free(request) };
}
