use types::NodeId;

/// Transport errors. All are recoverable; the transport stays usable.
#[derive(Debug, Clone, thiserror::Error)]
pub enum Error {
    /// The underlying UCX call failed (or was refused, e.g. re-entrant progress).
    #[error(transparent)]
    Ucx(#[from] ucx::Error),
    /// No endpoint for this node: `add_peer` it first.
    #[error("no endpoint for {0}")]
    UnknownPeer(NodeId),
    /// The transport was stopped (or dropped) before the operation finished.
    #[error("transport stopped")]
    Closed,
    /// `reply` was called for a message that wasn't a request (`request_id == 0`).
    #[error("message is not a request (request_id 0); nothing to reply to")]
    NotARequest,
}

pub type Result<T> = std::result::Result<T, Error>;
