//! Cross-thread access: a `Send` proxy whose commands the owning core's driver executes.

use tokio::sync::{mpsc, oneshot};
use types::{NodeId, Service};

use crate::error::{Error, Result};
use crate::handler::Reply;

pub(crate) enum Command {
    Send {
        to: NodeId,
        service: Service,
        kind: u16,
        payload: Vec<u8>,
    },
    Request {
        to: NodeId,
        service: Service,
        kind: u16,
        payload: Vec<u8>,
        reply: oneshot::Sender<Result<Reply>>,
    },
    Stop,
}

/// A `Send + Clone` proxy for a [`crate::Transport`] owned by another thread. Each call is
/// forwarded through a channel and executed by that transport's driver, which costs a
/// cross-thread hop; code on the owning thread should use the `Transport` directly.
#[derive(Debug, Clone)]
pub struct RemoteHandle {
    node: NodeId,
    tx: mpsc::UnboundedSender<Command>,
}

impl RemoteHandle {
    pub(crate) fn new(node: NodeId, tx: mpsc::UnboundedSender<Command>) -> Self {
        Self { node, tx }
    }

    /// The node this handle sends as.
    pub fn node(&self) -> NodeId {
        self.node
    }

    /// One-way message (errors on the owning thread are logged there).
    pub fn send(&self, to: NodeId, service: Service, kind: u16, payload: Vec<u8>) -> Result<()> {
        self.tx
            .send(Command::Send {
                to,
                service,
                kind,
                payload,
            })
            .map_err(|_| Error::Closed)
    }

    /// Request/response through the owning transport.
    pub async fn request(
        &self,
        to: NodeId,
        service: Service,
        kind: u16,
        payload: Vec<u8>,
    ) -> Result<Reply> {
        let (reply, rx) = oneshot::channel();
        self.tx
            .send(Command::Request {
                to,
                service,
                kind,
                payload,
                reply,
            })
            .map_err(|_| Error::Closed)?;
        rx.await.map_err(|_| Error::Closed)?
    }

    /// Stops the owning transport's driver.
    pub fn stop(&self) {
        let _ = self.tx.send(Command::Stop);
    }
}
