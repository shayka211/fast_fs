//! Non-blocking sends: payload ownership, completion, and orphaned requests.

use std::any::Any;
use std::ffi::c_void;
use std::fmt;
use std::rc::Rc;

use ucx_sys as sys;

use crate::status::{Error, Result, Status, check};
use crate::worker::WorkerInner;

/// A buffer UCX may read *after* `send` returns, until the request completes.
///
/// # Safety
/// The bytes returned by `as_bytes` must stay at the same address, with the same length
/// and contents, for the value's whole life — **including after the value is moved**
/// (a `SendRequest` moves it). Heap-backed buffers satisfy this; inline arrays do not.
pub unsafe trait SendBuf: 'static {
    fn as_bytes(&self) -> &[u8];
}

// SAFETY: a Vec's heap allocation doesn't move when the Vec value moves; it is never
// mutated or reallocated while owned by a SendRequest.
unsafe impl SendBuf for Vec<u8> {
    fn as_bytes(&self) -> &[u8] {
        self
    }
}

// SAFETY: as for Vec: the boxed slice's heap allocation is stable across moves.
unsafe impl SendBuf for Box<[u8]> {
    fn as_bytes(&self) -> &[u8] {
        self
    }
}

// SAFETY: 'static data never moves or dies.
unsafe impl SendBuf for &'static [u8] {
    fn as_bytes(&self) -> &[u8] {
        self
    }
}

/// An in-flight UCX request the caller no longer tracks. The worker keeps it (and the
/// resources it uses) alive and frees both once UCX completes it.
pub(crate) struct Orphan {
    pub(crate) op: &'static str,
    pub(crate) req: *mut c_void,
    /// Whatever must outlive the request (send buffer, endpoint error-callback state).
    pub(crate) keep: Box<dyn Any>,
}

/// A non-blocking send. The payload is owned by the request until UCX is done with it.
///
/// Dropping a pending request is fine (fire-and-forget): the worker finishes it in the
/// background during `progress()` and then frees the payload. Errors are then only
/// logged — keep the request and call [`Self::poll`]/[`Self::wait`] if you need them.
pub struct SendRequest<B: SendBuf> {
    state: State<B>,
    worker: Rc<WorkerInner>,
}

enum State<B> {
    Done(Result<()>),
    Pending { req: *mut c_void, buf: B },
}

const OP: &str = "ucp_am_send_nbx";

impl<B: SendBuf> SendRequest<B> {
    pub(crate) fn done(worker: &Rc<WorkerInner>) -> Self {
        Self {
            state: State::Done(Ok(())),
            worker: Rc::clone(worker),
        }
    }

    pub(crate) fn pending(worker: &Rc<WorkerInner>, req: *mut c_void, buf: B) -> Self {
        Self {
            state: State::Pending { req, buf },
            worker: Rc::clone(worker),
        }
    }

    /// Checks completion without progressing the worker. `None` = still in flight.
    pub fn poll(&mut self) -> Option<Result<()>> {
        if let State::Pending { req, .. } = self.state {
            // SAFETY: `req` is a live request from ucp_am_send_nbx, not yet freed.
            let status = unsafe { sys::ucp_request_check_status(req) };
            if status == sys::UCS_INPROGRESS {
                return None;
            }
            // SAFETY: completed request, freed exactly once (state leaves Pending below).
            unsafe { sys::ucp_request_free(req) };
            self.state = State::Done(check(OP, status)); // drops the buffer
        }
        match &self.state {
            State::Done(r) => Some(r.clone()),
            State::Pending { .. } => unreachable!(),
        }
    }

    /// True while UCX still owns the buffer (`false` right away if the send completed
    /// inside `send`, which is typical for small messages).
    pub fn is_pending(&self) -> bool {
        matches!(self.state, State::Pending { .. })
    }

    /// Progresses the worker until the send completes.
    ///
    /// # Errors
    /// The send's own error, or [`Error::Reentrant`] if called inside a `progress_with`
    /// closure (the request then continues in the background).
    pub fn wait(mut self) -> Result<()> {
        loop {
            if let Some(r) = self.poll() {
                return r;
            }
            self.worker.progress(None, "SendRequest::wait")?;
        }
    }
}

impl<B: SendBuf> Drop for SendRequest<B> {
    fn drop(&mut self) {
        if let State::Pending { req, buf } = std::mem::replace(&mut self.state, State::Done(Ok(())))
        {
            self.worker.adopt(Orphan {
                op: OP,
                req,
                keep: Box::new(buf),
            });
        }
    }
}

impl<B: SendBuf> fmt::Debug for SendRequest<B> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let state = match &self.state {
            State::Done(Ok(())) => "done".to_owned(),
            State::Done(Err(e)) => format!("failed: {e}"),
            State::Pending { .. } => "pending".to_owned(),
        };
        f.debug_struct("SendRequest")
            .field("state", &state)
            .finish()
    }
}

/// Completes orphans whose requests finished; returns how many are still pending.
pub(crate) fn reap(orphans: &mut Vec<Orphan>) -> usize {
    orphans.retain(|o| {
        // SAFETY: each orphan owns a live, not-yet-freed request.
        let status = unsafe { sys::ucp_request_check_status(o.req) };
        if status == sys::UCS_INPROGRESS {
            return true;
        }
        // SAFETY: completed request, freed exactly once (removed from the list here).
        unsafe { sys::ucp_request_free(o.req) };
        if status != sys::UCS_OK {
            let status = Status(status);
            if status.is_disconnect() {
                tracing::debug!(op = o.op, %status, "background request ended: peer gone");
            } else {
                tracing::warn!(op = o.op, error = %Error::Ucx { op: o.op, status }, "background request failed");
            }
        }
        false
    });
    orphans.len()
}
