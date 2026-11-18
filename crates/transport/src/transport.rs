//! The per-core transport: one UCX worker, peers by `NodeId`, routes by `Service`,
//! request/response matching, and the progress driver.

use std::cell::{Cell, RefCell};
use std::rc::Rc;
use std::sync::Arc;

use rustc_hash::FxHashMap;
use tokio::sync::{mpsc, oneshot};
use tracing::{debug, info, trace, warn};
use types::{NodeId, Service, WireHeader};
use ucx::{AmId, AmView, Context, Endpoint, ErrorMode, SendBuf, TransportLane, Worker};

use crate::error::{Error, Result};
use crate::handler::{Handler, Incoming, Reply};
use crate::remote::{Command, RemoteHandle};

/// UCX Active Message ids used on the wire. Requests and one-way messages share one id;
/// responses get their own, so the receive path knows where to deliver without parsing.
const AM_MESSAGE: AmId = 10;
const AM_RESPONSE: AmId = 11;

/// Empty polls between yields to the tokio scheduler while idle. Yielding costs a
/// scheduler tick plus a timer-driver clock read, so the driver yields at once only when
/// a poll delivered something (woken tasks must run), and otherwise every N empty polls
/// (so timers and other tasks still get turns). See docs/PERF.md.
const IDLE_SPINS_PER_YIELD: u32 = 64;

type Route = RefCell<Option<Box<dyn Handler>>>;

/// A node's messaging endpoint on one core (thread). Cheap to clone (`Rc`); `!Send` on
/// purpose: it owns a UCX worker, which must stay on its thread. Other threads use a
/// [`RemoteHandle`] (from [`Transport::remote`]).
///
/// Create with [`Transport::builder`], connect peers with [`Transport::add_peer`], and run
/// [`Transport::run`] as a task on a tokio current-thread runtime (`LocalSet`).
#[derive(Clone)]
pub struct Transport {
    inner: Rc<Inner>,
}

struct Inner {
    node: NodeId,
    routes: [Route; Service::COUNT],
    // Fx: fast integer hashing; keys come from our own nodes, so DoS resistance
    // (SipHash, the std default) isn't needed.
    peers: RefCell<FxHashMap<NodeId, Endpoint>>,
    /// In-flight `request`s: request_id → where to deliver the response.
    pending: RefCell<FxHashMap<u64, oneshot::Sender<Reply>>>,
    next_request_id: Cell<u64>,
    /// Commands from `RemoteHandle`s (other threads), drained by the driver.
    commands: RefCell<
        Option<(
            mpsc::UnboundedSender<Command>,
            mpsc::UnboundedReceiver<Command>,
        )>,
    >,
    stopped: Cell<bool>,
    // Last: dropped after the endpoints above.
    worker: Worker,
}

impl std::fmt::Debug for Transport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Transport")
            .field("node", &self.inner.node)
            .field("peers", &self.inner.peers.borrow().len())
            .field("pending", &self.inner.pending.borrow().len())
            .finish_non_exhaustive()
    }
}

/// Builds a [`Transport`]: every route is registered before any message can arrive.
pub struct TransportBuilder {
    node: NodeId,
    routes: [Option<Box<dyn Handler>>; Service::COUNT],
}

impl std::fmt::Debug for TransportBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let routed: Vec<Service> = Service::ALL
            .into_iter()
            .filter(|s| self.routes[s.index()].is_some())
            .collect();
        f.debug_struct("TransportBuilder")
            .field("node", &self.node)
            .field("routes", &routed)
            .finish()
    }
}

impl TransportBuilder {
    /// Delivers every message for `service` to `handler` (replacing any earlier one).
    pub fn route(mut self, service: Service, handler: impl Handler + 'static) -> Self {
        self.routes[service.index()] = Some(Box::new(handler));
        self
    }

    pub fn build(self, ctx: &Arc<Context>) -> Result<Transport> {
        let worker = Worker::new(ctx)?;
        worker.register_am(AM_MESSAGE)?;
        worker.register_am(AM_RESPONSE)?;
        let routed: Vec<Service> = Service::ALL
            .into_iter()
            .filter(|s| self.routes[s.index()].is_some())
            .collect();
        info!(node = %self.node, ?routed, "transport created");
        Ok(Transport {
            inner: Rc::new(Inner {
                node: self.node,
                routes: self.routes.map(RefCell::new),
                peers: RefCell::default(),
                pending: RefCell::default(),
                next_request_id: Cell::new(1),
                commands: RefCell::default(),
                stopped: Cell::new(false),
                worker,
            }),
        })
    }
}

impl Transport {
    pub fn builder(node: NodeId) -> TransportBuilder {
        TransportBuilder {
            node,
            routes: Default::default(),
        }
    }

    pub fn node(&self) -> NodeId {
        self.inner.node
    }

    /// This transport's UCX worker address, for peers' [`Self::add_peer`].
    pub fn address(&self) -> Result<Vec<u8>> {
        Ok(self.inner.worker.address()?)
    }

    /// Connects to `node` at its worker address (replacing any existing endpoint).
    pub fn add_peer(&self, node: NodeId, address: &[u8]) -> Result<()> {
        let ep = Endpoint::connect(
            &self.inner.worker,
            address,
            node.to_string(),
            ErrorMode::None,
        )?;
        if self.inner.peers.borrow_mut().insert(node, ep).is_some() {
            debug!(peer = %node, "peer endpoint replaced");
        } else {
            debug!(peer = %node, "peer added");
        }
        Ok(())
    }

    /// The UCX lanes (transport/device) the endpoint to `node` uses, e.g. `sysv/memory`.
    /// Meaningful once wire-up is done (after the first exchange).
    pub fn peer_transports(&self, node: NodeId) -> Result<Vec<TransportLane>> {
        let peers = self.inner.peers.borrow();
        let ep = peers.get(&node).ok_or(Error::UnknownPeer(node))?;
        Ok(ep.transports()?)
    }

    /// Drops the endpoint to `node` (closed in the background).
    pub fn remove_peer(&self, node: NodeId) -> bool {
        self.inner.peers.borrow_mut().remove(&node).is_some()
    }

    /// One-way message. Non-blocking: `payload` is owned until UCX is done with it. Allowed
    /// inside handlers.
    pub fn send<B: SendBuf>(
        &self,
        to: NodeId,
        service: Service,
        kind: u16,
        payload: B,
    ) -> Result<()> {
        let header = WireHeader::new(self.inner.node, service, kind, 0);
        self.send_raw(to, AM_MESSAGE, &header, payload)
    }

    /// Replies to a request received in a handler. Non-blocking; allowed inside handlers.
    pub fn reply<B: SendBuf>(&self, request: &WireHeader, kind: u16, payload: B) -> Result<()> {
        if request.request_id == 0 {
            return Err(Error::NotARequest);
        }
        let header = WireHeader::new(self.inner.node, request.service, kind, request.request_id);
        self.send_raw(request.source, AM_RESPONSE, &header, payload)
    }

    /// Sends a request and waits for its reply. Cancellation-safe: dropping the future (or
    /// a `tokio::time::timeout` around it) forgets the request; a late reply is ignored.
    ///
    /// The transport's driver ([`Self::run`]) must be running on this thread.
    pub async fn request<B: SendBuf>(
        &self,
        to: NodeId,
        service: Service,
        kind: u16,
        payload: B,
    ) -> Result<Reply> {
        let id = self.inner.next_request_id.get();
        self.inner.next_request_id.set(id.wrapping_add(1).max(1));
        let (tx, rx) = oneshot::channel();
        self.inner.pending.borrow_mut().insert(id, tx);
        let _guard = PendingGuard {
            inner: &self.inner,
            id,
        };

        let header = WireHeader::new(self.inner.node, service, kind, id);
        self.send_raw(to, AM_MESSAGE, &header, payload)?;
        rx.await.map_err(|_| Error::Closed)
    }

    /// A `Send + Clone` handle for other threads. Its commands are executed by this
    /// transport's driver, so each costs a cross-thread hop.
    pub fn remote(&self) -> RemoteHandle {
        let mut commands = self.inner.commands.borrow_mut();
        let (tx, _) = commands.get_or_insert_with(mpsc::unbounded_channel);
        RemoteHandle::new(self.inner.node, tx.clone())
    }

    /// Makes [`Self::run`] return (pending requests fail with [`Error::Closed`]).
    pub fn stop(&self) {
        self.inner.stopped.set(true);
    }

    /// In-flight requests (diagnostics; also used by tests to check cancellation).
    pub fn pending_requests(&self) -> usize {
        self.inner.pending.borrow().len()
    }

    /// The progress driver: polls UCX (delivering messages to handlers and replies to
    /// waiting requests) and executes remote commands, yielding to other tasks between
    /// rounds. Busy-polls: lowest latency, one core at 100%. Returns after [`Self::stop`].
    ///
    /// A panic in a handler propagates out of this future (and can be caught on its
    /// `JoinHandle`); the transport stays consistent.
    pub async fn run(&self) -> Result<()> {
        info!(node = %self.inner.node, "transport driver started (busy-poll)");
        let mut idle = 0;
        while !self.inner.stopped.get() {
            let commands = self.drain_commands();
            if self.progress()? > 0 || commands > 0 {
                idle = 0;
                tokio::task::yield_now().await; // let woken requests/tasks run now
            } else {
                idle += 1;
                if idle >= IDLE_SPINS_PER_YIELD {
                    idle = 0;
                    tokio::task::yield_now().await;
                }
            }
        }
        let failed = std::mem::take(&mut *self.inner.pending.borrow_mut()).len();
        info!(node = %self.inner.node, failed_requests = failed, "transport driver stopped");
        Ok(())
    }

    /// One progress round: delivers whatever has arrived.
    pub fn progress(&self) -> Result<u32> {
        Ok(self.inner.worker.progress_with(|m| self.dispatch(m))?)
    }

    fn send_raw<B: SendBuf>(
        &self,
        to: NodeId,
        am: AmId,
        header: &WireHeader,
        payload: B,
    ) -> Result<()> {
        let peers = self.inner.peers.borrow();
        let ep = peers.get(&to).ok_or(Error::UnknownPeer(to))?;
        trace!(to = %to, service = ?header.service, kind = header.kind, request_id = header.request_id, len = payload.as_bytes().len(), "send");
        // Fire-and-forget: a pending request is finished by the worker in the background.
        drop(ep.send(am, &header.encode(), payload)?);
        Ok(())
    }

    fn dispatch(&self, msg: AmView<'_>) {
        let header = match WireHeader::decode(msg.header) {
            Ok((header, _service_header)) => header,
            Err(e) => {
                warn!(am_id = msg.id, error = %e, "dropping message with bad header");
                return;
            }
        };
        trace!(from = %header.source, service = ?header.service, kind = header.kind, request_id = header.request_id, len = msg.data.len(), "recv");
        match msg.id {
            AM_MESSAGE => self.route(header, msg.data),
            AM_RESPONSE => self.complete(header, msg.data),
            other => warn!(am_id = other, "unexpected AM id"),
        }
    }

    fn route(&self, header: WireHeader, payload: &[u8]) {
        let Ok(mut slot) = self.inner.routes[header.service.index()].try_borrow_mut() else {
            warn!(service = ?header.service, "handler re-entered; dropping message");
            return;
        };
        match slot.as_mut() {
            Some(handler) => handler.on_message(self, Incoming { header, payload }),
            None => {
                warn!(service = ?header.service, from = %header.source, "no route for service; dropping")
            }
        }
    }

    fn complete(&self, header: WireHeader, payload: &[u8]) {
        let waiter = self.inner.pending.borrow_mut().remove(&header.request_id);
        match waiter {
            Some(tx) => {
                // The receiver may be gone (cancelled request); that's fine.
                let _ = tx.send(Reply {
                    header,
                    payload: payload.to_vec(),
                });
            }
            None => {
                debug!(request_id = header.request_id, from = %header.source, "late or unknown response; ignored")
            }
        }
    }

    /// Executes queued remote commands; returns how many.
    fn drain_commands(&self) -> usize {
        let mut commands = self.inner.commands.borrow_mut();
        let Some((_, rx)) = commands.as_mut() else {
            return 0;
        };
        let mut n = 0;
        while let Ok(cmd) = rx.try_recv() {
            n += 1;
            match cmd {
                Command::Send {
                    to,
                    service,
                    kind,
                    payload,
                } => {
                    if let Err(e) = self.send(to, service, kind, payload) {
                        warn!(to = %to, error = %e, "remote send failed");
                    }
                }
                Command::Request {
                    to,
                    service,
                    kind,
                    payload,
                    reply,
                } => {
                    let this = self.clone();
                    tokio::task::spawn_local(async move {
                        let _ = reply.send(this.request(to, service, kind, payload).await);
                    });
                }
                Command::Stop => self.stop(),
            }
        }
        n
    }
}

/// Removes a request's pending entry when its future finishes or is dropped.
struct PendingGuard<'a> {
    inner: &'a Inner,
    id: u64,
}

impl Drop for PendingGuard<'_> {
    fn drop(&mut self) {
        self.inner.pending.borrow_mut().remove(&self.id);
    }
}
