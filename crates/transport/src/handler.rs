use types::WireHeader;

use crate::Transport;

/// A received message, borrowed from UCX's buffer: valid only inside [`Handler::on_message`].
#[derive(Debug, Clone, Copy)]
pub struct Incoming<'a> {
    pub header: WireHeader,
    pub payload: &'a [u8],
}

/// A response to [`Transport::request`], owning its bytes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Reply {
    pub header: WireHeader,
    pub payload: Vec<u8>,
}

/// Handles every message for one [`types::Service`]. Called **synchronously** inside the
/// transport's progress loop, with zero copy, so keep it short:
/// - reply with [`Transport::reply`] or send with [`Transport::send`] (non-blocking, allowed);
/// - for async follow-up work, `tokio::task::spawn_local` a task;
/// - copy out any bytes that must outlive the call.
///
/// Closures `|transport: &Transport, msg: Incoming<'_>| ...` implement it.
pub trait Handler {
    fn on_message(&mut self, transport: &Transport, msg: Incoming<'_>);
}

impl<F> Handler for F
where
    F: FnMut(&Transport, Incoming<'_>),
{
    fn on_message(&mut self, transport: &Transport, msg: Incoming<'_>) {
        self(transport, msg);
    }
}
