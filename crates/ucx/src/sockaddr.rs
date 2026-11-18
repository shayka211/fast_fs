//! Conversions between `std::net::SocketAddr` and the C `sockaddr` UCX expects.

use std::mem::size_of;
use std::net::SocketAddr;
use std::ptr;

use socket2::SockAddr;
use ucx_sys as sys;

/// Owned C sockaddr; keep it alive while UCX reads the pointer.
pub(crate) struct RawSockAddr(SockAddr);

impl RawSockAddr {
    pub(crate) fn new(addr: SocketAddr) -> Self {
        Self(SockAddr::from(addr))
    }

    pub(crate) fn as_ucs(&self) -> sys::ucs_sock_addr {
        sys::ucs_sock_addr {
            addr: self.0.as_ptr().cast::<sys::sockaddr>(),
            addrlen: self.0.len(),
        }
    }
}

/// Reads a `sockaddr_storage` filled in by a UCX query.
pub(crate) fn from_storage(storage: &sys::sockaddr_storage) -> Option<SocketAddr> {
    // SAFETY: we copy at most `len` bytes (the size of socket2's storage) from a fully
    // initialized `sockaddr_storage` of the same platform layout.
    let (_, addr) = unsafe {
        SockAddr::try_init(|dst, len| {
            let n = size_of::<sys::sockaddr_storage>().min(*len as usize);
            ptr::copy_nonoverlapping(ptr::from_ref(storage).cast::<u8>(), dst.cast::<u8>(), n);
            Ok(())
        })
    }
    .ok()?;
    addr.as_socket()
}
