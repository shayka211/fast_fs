//! UCP worker: the progress engine, Active Message receive, and background requests.
//!
//! Two ways to receive, one code path:
//! - [`Worker::progress_with`]: a closure sees each message *during* progress, borrowing
//!   UCX's buffers (no copy, no allocation). The fast path.
//! - [`Worker::progress`] + [`Worker::try_recv`]: messages are copied into a queue.

use std::any::Any;
use std::cell::{Cell, RefCell};
use std::collections::VecDeque;
use std::ffi::c_void;
use std::panic::{self, AssertUnwindSafe};
use std::ptr;
use std::rc::Rc;
use std::slice;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tracing::{debug, error, trace, warn};
use ucx_sys as sys;

use crate::context::Context;
use crate::message::{AmMessage, AmView, Header};
use crate::request::{Orphan, reap};
use crate::status::{Error, Result, Status, StatusPtr, check};

/// Active Message handler id. Each service registers its own id.
pub type AmId = u16;

/// How long dropping a [`Worker`] waits for background requests (closes, sends).
const DROP_DRAIN: Duration = Duration::from_millis(100);

/// The closure installed by `progress_with`, lifetime-erased (see `progress_with`).
type Sink = *mut (dyn for<'v> FnMut(AmView<'v>) + 'static);

/// Single-threaded UCP worker. `!Send` by construction: a worker (and everything
/// created from it) must stay on the thread that created it.
#[derive(Debug)]
pub struct Worker {
    pub(crate) inner: Rc<WorkerInner>,
}

pub(crate) struct WorkerInner {
    pub(crate) raw: sys::ucp_worker_h,
    inbox: RefCell<VecDeque<AmMessage>>,
    /// Set only while `progress_with` runs.
    sink: Cell<Option<Sink>>,
    /// Guards against re-entering UCX progress from a receive callback (forbidden by UCX).
    in_progress: Cell<bool>,
    /// A panic caught inside a C callback, re-raised once `progress` is back in Rust.
    panic: RefCell<Option<Box<dyn Any + Send>>>,
    /// Per-id callback args. Boxed so their addresses stay stable when the Vec grows:
    /// UCX holds raw pointers to them.
    #[allow(clippy::vec_box, reason = "UCX stores pointers to the elements")]
    handlers: RefCell<Vec<Box<AmHandlerArg>>>,
    /// Requests nobody waits on (dropped sends, endpoint closes); reaped in `progress`.
    orphans: RefCell<Vec<Orphan>>,
    pending_rndv: Cell<usize>,
    // Dropped after `raw` is destroyed (field order + explicit Drop).
    _ctx: Arc<Context>,
}

impl std::fmt::Debug for WorkerInner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WorkerInner")
            .field("raw", &self.raw)
            .field("inbox", &self.inbox.borrow().len())
            .field("orphans", &self.orphans.borrow().len())
            .finish_non_exhaustive()
    }
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
    header: Header,
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
                sink: Cell::new(None),
                in_progress: Cell::new(false),
                panic: RefCell::new(None),
                handlers: RefCell::default(),
                orphans: RefCell::default(),
                pending_rndv: Cell::new(0),
                _ctx: Arc::clone(ctx),
            }),
        })
    }

    /// Drives communication; received messages are queued for [`Self::try_recv`].
    /// Returns the number of UCX events processed.
    ///
    /// # Errors
    /// [`Error::Reentrant`] if called from inside a `progress_with` closure.
    #[inline]
    pub fn progress(&self) -> Result<u32> {
        self.inner.progress(None, "Worker::progress")
    }

    /// Drives communication, handing each received message to `on_message` while UCX's
    /// buffers are still valid: no copy, no allocation. Copy out what must outlive the
    /// call (`AmView::to_message`).
    ///
    /// Inside `on_message`, non-blocking [`crate::Endpoint::send`] is allowed; anything
    /// that progresses this worker (`progress*`, `send_blocking`, `drain`) returns
    /// [`Error::Reentrant`] — UCX forbids re-entering progress from a callback.
    ///
    /// If `on_message` panics, the panic is caught before it reaches UCX's C code and
    /// re-raised from this call once progress has finished, as an ordinary Rust panic:
    /// it can be caught with `catch_unwind` and the worker remains usable. Messages that
    /// arrive after the panic in the same call are queued for [`Self::try_recv`].
    ///
    /// # Errors
    /// [`Error::Reentrant`] if called from inside another `progress_with` closure.
    #[inline]
    pub fn progress_with(&self, mut on_message: impl FnMut(AmView<'_>)) -> Result<u32> {
        let sink: &mut (dyn for<'v> FnMut(AmView<'v>) + '_) = &mut on_message;
        // SAFETY: only the trait object's lifetime bound is erased. The pointer is
        // installed for the duration of `progress` below and cleared before we return,
        // so it is never used after `on_message` goes out of scope.
        let sink: Sink = unsafe { std::mem::transmute(ptr::from_mut(sink)) };
        self.inner.progress(Some(sink), "Worker::progress_with")
    }

    /// Progresses until all background requests (dropped sends, endpoint closes) finish
    /// or `timeout` passes. Returns `true` if everything completed. Call before shutdown
    /// to make sure fire-and-forget sends were actually delivered.
    ///
    /// # Errors
    /// [`Error::Reentrant`] if called from inside a `progress_with` closure.
    pub fn drain(&self, timeout: Duration) -> Result<bool> {
        let deadline = Instant::now() + timeout;
        loop {
            self.inner.progress(None, "Worker::drain")?;
            let left = self.inner.orphans.borrow().len();
            if left == 0 {
                return Ok(true);
            }
            if Instant::now() >= deadline {
                debug!(left, "drain timed out");
                return Ok(false);
            }
        }
    }

    /// Starts delivering Active Messages with this id.
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
            // No PERSISTENT_DATA: measured slower on shm (see message.rs).
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
        // SAFETY: UCX returned `len` readable bytes; copied, then released exactly once.
        let bytes = unsafe {
            let bytes = slice::from_raw_parts(addr.cast::<u8>(), len).to_vec();
            sys::ucp_worker_release_address(self.inner.raw, addr);
            bytes
        };
        debug!(len, "worker address packed");
        Ok(bytes)
    }

    /// Pops the next queued message, if any (queued by [`Self::progress`]).
    #[inline]
    pub fn try_recv(&self) -> Option<AmMessage> {
        self.inner.inbox.borrow_mut().pop_front()
    }
}

impl Drop for Worker {
    fn drop(&mut self) {
        // Give background closes/sends a moment to finish — unless we're unwinding from a
        // panic: progressing then could panic again (double panic = abort).
        if std::thread::panicking() {
            return;
        }
        match self.drain(DROP_DRAIN) {
            Ok(true) => {}
            Ok(false) => warn!(
                left = self.inner.orphans.borrow().len(),
                "worker dropped with requests still in flight"
            ),
            Err(e) => warn!(error = %e, "worker dropped inside its own receive callback"),
        }
    }
}

impl WorkerInner {
    /// Fails with [`Error::Reentrant`] when called from inside a receive callback.
    /// Operations that lend borrowed memory to UCX must check this *before* starting.
    #[inline]
    pub(crate) fn ensure_not_in_callback(&self, op: &'static str) -> Result<()> {
        if self.in_progress.get() {
            Err(Error::Reentrant { op })
        } else {
            Ok(())
        }
    }

    #[inline]
    pub(crate) fn progress(&self, sink: Option<Sink>, op: &'static str) -> Result<u32> {
        self.ensure_not_in_callback(op)?;
        Ok(self.progress_checked(sink))
    }

    /// The one place UCX progress happens: sink install, background request reaping, and
    /// re-raising panics caught in callbacks. Caller guarantees we're not in a callback.
    #[inline]
    fn progress_checked(&self, sink: Option<Sink>) -> u32 {
        self.in_progress.set(true);
        self.sink.set(sink);
        // SAFETY: the worker is valid, used only from this thread (`!Send`), and not
        // re-entered (checked by the caller).
        let events = unsafe { sys::ucp_worker_progress(self.raw) };
        self.sink.set(None);
        self.in_progress.set(false);
        if !self.orphans.borrow().is_empty() {
            reap(&mut self.orphans.borrow_mut());
        }
        let caught = self.panic.borrow_mut().take();
        if let Some(payload) = caught {
            // Back in Rust frames: safe to unwind now, and the worker state is consistent.
            panic::resume_unwind(payload);
        }
        events
    }

    /// Completes a non-blocking operation by progressing until it finishes.
    ///
    /// Precondition: the caller ran [`Self::ensure_not_in_callback`] before starting the
    /// operation, so progressing here is always allowed (no error path that could return
    /// while UCX still reads borrowed memory).
    pub(crate) fn wait(&self, op: &'static str, ptr: sys::ucs_status_ptr_t) -> Result<()> {
        let req = match StatusPtr::decode(ptr) {
            StatusPtr::Done => return Ok(()),
            StatusPtr::Failed(status) => return Err(Error::Ucx { op, status }),
            StatusPtr::Pending(req) => req,
        };
        let status = loop {
            self.progress_checked(None);
            // SAFETY: `req` is a live request.
            let status = unsafe { sys::ucp_request_check_status(req) };
            if status != sys::UCS_INPROGRESS {
                break status;
            }
        };
        // SAFETY: `req` is completed and freed exactly once.
        unsafe { sys::ucp_request_free(req) };
        check(op, status)
    }

    /// Hands an in-flight request to the worker to finish in the background.
    pub(crate) fn adopt(&self, orphan: Orphan) {
        trace!(op = orphan.op, "request continues in background");
        self.orphans.borrow_mut().push(orphan);
    }

    /// The single delivery point: to the `progress_with` closure if one is running (and
    /// hasn't panicked during this call), otherwise into the queue. Never unwinds: it is
    /// called from C callbacks, so panics are caught and stored for `progress` to re-raise.
    fn dispatch(&self, msg: AmView<'_>) {
        let result = panic::catch_unwind(AssertUnwindSafe(|| self.deliver(msg)));
        if let Err(payload) = result {
            error!(
                am_id = msg.id,
                "panic in receive handler; re-raising after progress"
            );
            *self.panic.borrow_mut() = Some(payload);
        }
    }

    fn deliver(&self, msg: AmView<'_>) {
        trace!(
            am_id = msg.id,
            header_len = msg.header.len(),
            data_len = msg.data.len(),
            "am recv"
        );
        let panicked = self.panic.borrow().is_some();
        match self.sink.get() {
            // SAFETY: a sink is only installed while its closure is alive and borrowed
            // mutably by `progress_with`; callbacks run on this thread inside that call.
            Some(sink) if !panicked => unsafe { (*sink)(msg) },
            _ => self.inbox.borrow_mut().push_back(msg.to_message()),
        }
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
        // Requests adopted after `Worker::drop` (e.g. endpoints closed later). Free the
        // handles but leak what they protect: UCX may still touch it until destroy.
        for o in self.orphans.get_mut().drain(..) {
            // SAFETY: live request, freed exactly once; UCX defers release if in flight.
            unsafe { sys::ucp_request_free(o.req) };
            std::mem::forget(o.keep);
        }
        // SAFETY: endpoints hold an `Rc<WorkerInner>`, so all are gone by now.
        unsafe { sys::ucp_worker_destroy(self.raw) };
        debug!("ucp worker destroyed");
    }
}

/// # Safety
/// If `len > 0`, `ptr` must be valid for reads of `len` bytes for `'a`.
unsafe fn bytes<'a>(ptr: *const c_void, len: usize) -> &'a [u8] {
    if len == 0 {
        &[]
    } else {
        // SAFETY: guaranteed by the caller.
        unsafe { slice::from_raw_parts(ptr.cast::<u8>(), len) }
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
    let (worker_ptr, id) = unsafe {
        let arg = &*arg.cast::<AmHandlerArg>();
        (arg.worker, arg.id)
    };
    // SAFETY: see above.
    let worker = unsafe { &*worker_ptr };
    // SAFETY: UCX passes a valid param struct for the duration of the callback.
    let recv_attr = unsafe { (*param).recv_attr };
    // SAFETY: UCX guarantees `header` is valid for `header_length` bytes in the callback.
    let header = unsafe { bytes(header, header_length) };

    if recv_attr & u64::from(sys::UCP_AM_RECV_ATTR_FLAG_RNDV) == 0 {
        // Eager: `data` is valid for the duration of this callback.
        // SAFETY: valid for `length` bytes during the callback.
        let data = unsafe { bytes(data, length) };
        worker.dispatch(AmView { id, header, data });
        return sys::UCS_OK;
    }

    // Rendezvous: `data` is a descriptor; fetch the payload into our own buffer.
    trace!(am_id = id, length, "am rndv recv started");
    let pending = Box::into_raw(Box::new(PendingRndv {
        worker: worker_ptr,
        id,
        header: Header::from_slice(header),
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
            worker.dispatch(p.view());
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
    fn view(&self) -> AmView<'_> {
        AmView {
            id: self.id,
            header: &self.header,
            data: &self.data,
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
        worker.dispatch(p.view());
    } else {
        error!(am_id = p.id, status = %Status(status), "am rndv recv failed");
    }
    // SAFETY: completed request returned by ucp_am_recv_data_nbx, freed exactly once.
    unsafe { sys::ucp_request_free(request) };
}
