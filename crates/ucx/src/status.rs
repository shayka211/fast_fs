//! UCX status codes, errors, and the `ucs_status_ptr_t` encoding.

use std::ffi::{CStr, c_void};
use std::fmt;

use ucx_sys as sys;

/// A raw UCX status code (`UCS_OK`, `UCS_ERR_*`).
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct Status(pub(crate) sys::ucs_status_t);

impl Status {
    pub fn code(self) -> i32 {
        self.0.into()
    }

    pub fn message(self) -> &'static str {
        // SAFETY: ucs_status_string returns a pointer to a static, NUL-terminated string
        // for every status value (unknown values map to a static "unknown" string).
        unsafe { CStr::from_ptr(sys::ucs_status_string(self.0)) }
            .to_str()
            .unwrap_or("<non-utf8 status>")
    }

    /// The peer closed or reset the connection — expected during normal shutdown.
    pub fn is_disconnect(self) -> bool {
        matches!(
            self.0,
            sys::UCS_ERR_CONNECTION_RESET | sys::UCS_ERR_ENDPOINT_TIMEOUT | sys::UCS_ERR_CANCELED
        )
    }
}

impl fmt::Debug for Status {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} ({})", self.message(), self.code())
    }
}

impl fmt::Display for Status {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, f)
    }
}

/// Errors from the `ucx` crate. Every one is recoverable: the worker stays usable.
#[derive(Debug, Clone, thiserror::Error)]
pub enum Error {
    /// A UCX call failed: which operation, and why.
    #[error("{op} failed: {status}")]
    Ucx { op: &'static str, status: Status },
    /// Something that progresses the worker (`progress*`, `drain`, `send_blocking`,
    /// `SendRequest::wait`) was called from inside a `progress_with` closure. UCX forbids
    /// re-entering progress from a receive callback; only non-blocking sends are allowed.
    #[error(
        "{op} called from inside a receive callback (only non-blocking sends are allowed there)"
    )]
    Reentrant { op: &'static str },
}

impl Error {
    /// The UCX status, if this error came from UCX.
    pub fn status(&self) -> Option<Status> {
        match self {
            Self::Ucx { status, .. } => Some(*status),
            Self::Reentrant { .. } => None,
        }
    }
}

pub type Result<T> = std::result::Result<T, Error>;

pub(crate) fn check(op: &'static str, status: sys::ucs_status_t) -> Result<()> {
    if status == sys::UCS_OK {
        Ok(())
    } else {
        Err(Error::Ucx {
            op,
            status: Status(status),
        })
    }
}

/// Decoded `ucs_status_ptr_t` (the `UCS_PTR_IS_ERR` / `UCS_PTR_IS_PTR` macros).
pub(crate) enum StatusPtr {
    /// Completed immediately (NULL).
    Done,
    /// In flight; the request must be progressed and then freed.
    Pending(*mut c_void),
    Failed(Status),
}

impl StatusPtr {
    pub(crate) fn decode(ptr: sys::ucs_status_ptr_t) -> Self {
        let raw = ptr as usize;
        // Error statuses are encoded as small negative integers cast to pointers, so
        // they occupy the top of the address space: [UCS_ERR_LAST as uintptr_t, MAX].
        let err_floor = sys::UCS_ERR_LAST as isize as usize;
        if raw == 0 {
            Self::Done
        } else if raw >= err_floor {
            Self::Failed(Status(raw as isize as sys::ucs_status_t))
        } else {
            Self::Pending(ptr)
        }
    }
}
