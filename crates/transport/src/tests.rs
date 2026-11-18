use std::cell::RefCell;
use std::future::Future;
use std::rc::Rc;
use std::sync::mpsc as std_mpsc;
use std::thread;
use std::time::Duration;

use tokio::sync::Notify;
use types::{NodeId, Service};
use ucx::Context;

use super::*;

const SERVER: NodeId = NodeId::new(1);
const CLIENT: NodeId = NodeId::new(2);

/// Where a node sends its worker address, and where it receives the peer's.
type AddrChannels = (std_mpsc::Sender<Vec<u8>>, std_mpsc::Receiver<Vec<u8>>);

/// Runs `f` on a current-thread runtime with a `LocalSet` (what a core does).
fn block_local<F: Future>(f: F) -> F::Output {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .unwrap();
    tokio::task::LocalSet::new().block_on(&rt, f)
}

/// Starts a node on its own thread: builds its transport, swaps worker addresses with the
/// peer, starts the driver, runs `body`, then waits for the driver (bodies call `stop`, or
/// a `RemoteHandle` does).
///
/// `setup` runs *on the node's thread* and returns `(routes, body)`, so they may hold
/// non-`Send` state (`Rc`, `RefCell`) like real per-core services do.
fn spawn_node<S, R, F, Fut, T>(
    id: NodeId,
    peer: NodeId,
    (addr_out, addr_in): AddrChannels,
    setup: S,
) -> thread::JoinHandle<T>
where
    S: FnOnce() -> (R, F) + Send + 'static,
    R: FnOnce(TransportBuilder) -> TransportBuilder,
    F: FnOnce(Transport) -> Fut,
    Fut: Future<Output = T>,
    T: Send + 'static,
{
    thread::spawn(move || {
        let (routes, body) = setup();
        block_local(async move {
            let ctx = Context::new().unwrap();
            let t = routes(Transport::builder(id)).build(&ctx).unwrap();
            addr_out.send(t.address().unwrap()).unwrap();
            t.add_peer(peer, &addr_in.recv().unwrap()).unwrap();
            let driver = tokio::task::spawn_local({
                let t = t.clone();
                async move { t.run().await }
            });
            let out = body(t).await;
            driver.await.unwrap().unwrap();
            out
        })
    })
}

/// A client node with no routes.
fn spawn_client<F, Fut>(addrs: AddrChannels, body: F) -> thread::JoinHandle<()>
where
    F: FnOnce(Transport) -> Fut + Send + 'static,
    Fut: Future<Output = ()>,
{
    spawn_node(CLIENT, SERVER, addrs, || (|b| b, body))
}

/// Two address channels, crossed: what A sends, B receives, and vice versa.
fn crossed() -> [AddrChannels; 2] {
    let (a_tx, a_rx) = std_mpsc::channel();
    let (b_tx, b_rx) = std_mpsc::channel();
    [(a_tx, b_rx), (b_tx, a_rx)]
}

/// Server routes: `Meta` requests are answered with the payload reversed; `Node` one-way
/// messages are recorded, and `b"done"` stops the server.
fn echo_server(
    log: Rc<RefCell<Vec<Vec<u8>>>>,
    done: Rc<Notify>,
) -> impl FnOnce(TransportBuilder) -> TransportBuilder {
    move |b| {
        b.route(Service::Meta, |t: &Transport, m: Incoming<'_>| {
            let reversed: Vec<u8> = m.payload.iter().rev().copied().collect();
            t.reply(&m.header, 7, reversed).unwrap();
        })
        .route(Service::Node, move |t: &Transport, m: Incoming<'_>| {
            // Re-entering progress from a handler is refused, not a crash.
            assert!(matches!(
                t.progress(),
                Err(Error::Ucx(ucx::Error::Reentrant { .. }))
            ));
            assert_eq!(m.header.request_id, 0, "one-way message");
            log.borrow_mut().push(m.payload.to_vec());
            if m.payload == b"done" {
                done.notify_one();
            }
        })
    }
}

/// The echo server node; returns the one-way messages it logged.
fn run_echo_server(addrs: AddrChannels) -> thread::JoinHandle<Vec<Vec<u8>>> {
    spawn_node(SERVER, CLIENT, addrs, || {
        let log = Rc::new(RefCell::new(Vec::new()));
        let done = Rc::new(Notify::new());
        let routes = echo_server(Rc::clone(&log), Rc::clone(&done));
        let body = |t: Transport| async move {
            done.notified().await;
            t.stop();
            log.take()
        };
        (routes, body)
    })
}

#[test]
fn requests_match_replies_and_one_way_messages_route() {
    let [server_addrs, client_addrs] = crossed();
    let server = run_echo_server(server_addrs);
    let client = spawn_client(client_addrs, |t: Transport| async move {
        // Concurrent requests: each must get *its own* reply back.
        let tasks: Vec<_> = (0u8..10)
            .map(|i| {
                let t = t.clone();
                tokio::task::spawn_local(async move {
                    let reply = t
                        .request(SERVER, Service::Meta, 1, vec![i, i + 1, i + 2])
                        .await
                        .unwrap();
                    assert_eq!(reply.payload, vec![i + 2, i + 1, i]);
                    assert_eq!(
                        (reply.header.source, reply.header.service, reply.header.kind),
                        (SERVER, Service::Meta, 7)
                    );
                })
            })
            .collect();
        for task in tasks {
            task.await.unwrap();
        }
        assert_eq!(t.pending_requests(), 0);
        t.send(SERVER, Service::Node, 0, b"hello".to_vec()).unwrap();
        t.send(SERVER, Service::Node, 0, b"done".to_vec()).unwrap();
        // Let the one-way sends go out before stopping.
        tokio::time::sleep(Duration::from_millis(50)).await;
        t.stop();
    });
    client.join().unwrap();
    assert_eq!(
        server.join().unwrap(),
        vec![b"hello".to_vec(), b"done".to_vec()]
    );
}

#[test]
fn unknown_peer_is_an_error_and_leaves_nothing_pending() {
    block_local(async {
        let ctx = Context::new().unwrap();
        let t = Transport::builder(CLIENT).build(&ctx).unwrap();
        let stranger = NodeId::new(99);
        assert!(
            matches!(t.send(stranger, Service::Node, 0, vec![1]), Err(Error::UnknownPeer(n)) if n == stranger)
        );
        let err = t
            .request(stranger, Service::Meta, 0, vec![1])
            .await
            .unwrap_err();
        assert!(matches!(err, Error::UnknownPeer(_)));
        assert_eq!(t.pending_requests(), 0);
        assert!(matches!(
            t.reply(
                &types::WireHeader::new(stranger, Service::Meta, 0, 0),
                0,
                vec![]
            ),
            Err(Error::NotARequest)
        ));
    });
}

#[test]
fn timed_out_request_is_forgotten() {
    let [server_addrs, client_addrs] = crossed();
    // This server has no `Raft` route, so a Raft request is dropped and never answered.
    let server = run_echo_server(server_addrs);
    let client = spawn_client(client_addrs, |t: Transport| async move {
        let r = tokio::time::timeout(
            Duration::from_millis(100),
            t.request(SERVER, Service::Raft, 0, vec![1]),
        )
        .await;
        assert!(r.is_err(), "no route → no reply → timeout");
        assert_eq!(t.pending_requests(), 0, "cancelled request must not leak");
        t.send(SERVER, Service::Node, 0, b"done".to_vec()).unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;
        t.stop();
    });
    client.join().unwrap();
    server.join().unwrap();
}

#[test]
fn remote_handle_works_from_another_thread() {
    let [server_addrs, client_addrs] = crossed();
    let server = run_echo_server(server_addrs);
    let (handle_tx, handle_rx) = std_mpsc::channel();
    // The client transport lives on its own thread; we only get a RemoteHandle.
    let client = spawn_client(client_addrs, move |t: Transport| async move {
        handle_tx.send(t.remote()).unwrap();
    });
    let remote: RemoteHandle = handle_rx.recv().unwrap();
    block_local(async {
        for i in 0u8..5 {
            let reply = remote
                .request(SERVER, Service::Meta, 1, vec![i, 9])
                .await
                .unwrap();
            assert_eq!(reply.payload, vec![9, i]);
        }
        remote
            .send(SERVER, Service::Node, 0, b"done".to_vec())
            .unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;
        remote.stop();
    });
    client.join().unwrap();
    server.join().unwrap();
    assert!(matches!(
        remote.send(SERVER, Service::Node, 0, vec![]),
        Err(Error::Closed)
    ));
}
