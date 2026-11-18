use std::sync::{Arc, mpsc};
use std::thread;
use std::time::{Duration, Instant};

use super::*;

const PING: AmId = 1;
const PONG: AmId = 2;
const SIZES: [usize; 4] = [0, 8, 64 << 10, 4 << 20];

fn recv_blocking(worker: &Worker) -> AmMessage {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        worker.progress().unwrap();
        if let Some(m) = worker.try_recv() {
            return m;
        }
        assert!(Instant::now() < deadline, "timed out waiting for a message");
    }
}

fn pattern(size: usize) -> Vec<u8> {
    (0..size).map(|i| (i % 251) as u8).collect()
}

/// Runs `peer` on its own thread with its own worker, connected to a worker on this
/// thread. Each side needs its own thread: progress only advances one worker, and
/// wire-up needs both.
fn with_peer(
    mode: ErrorMode,
    peer: impl FnOnce(&Worker, &Endpoint) + Send + 'static,
    main: impl FnOnce(&Worker, &Endpoint),
) {
    let ctx = Context::new().unwrap();
    let (to_peer, from_main) = mpsc::channel::<Vec<u8>>();
    let (to_main, from_peer) = mpsc::channel::<Vec<u8>>();
    let peer_ctx = Arc::clone(&ctx);
    let handle = thread::spawn(move || {
        let w = Worker::new(&peer_ctx).unwrap();
        w.register_am(PING).unwrap();
        to_main.send(w.address().unwrap()).unwrap();
        let ep = Endpoint::connect(&w, &from_main.recv().unwrap(), "main", mode).unwrap();
        peer(&w, &ep);
    });
    let w = Worker::new(&ctx).unwrap();
    w.register_am(PONG).unwrap();
    to_peer.send(w.address().unwrap()).unwrap();
    let ep = Endpoint::connect(&w, &from_peer.recv().unwrap(), "peer", mode).unwrap();
    main(&w, &ep);
    handle.join().unwrap();
}

#[test]
fn runtime_matches_compiled_api() {
    let v = Version::runtime();
    assert_eq!((v.major, v.minor), Version::COMPILED);
}

#[test]
fn roundtrip_peer_mode() {
    roundtrip(ErrorMode::Peer);
}

#[test]
fn roundtrip_no_err_mode() {
    roundtrip(ErrorMode::None);
}

/// Eager and rendezvous sizes, header passthrough, inline small payloads, non-blocking
/// owned echo, and no TCP on the `ErrorMode::None` path.
fn roundtrip(mode: ErrorMode) {
    with_peer(
        mode,
        |w, ep| {
            for _ in SIZES {
                let ping = recv_blocking(w);
                assert_eq!((ping.id, &ping.header[..]), (PING, &b"from-main"[..]));
                assert_eq!(ping.data.is_inline(), ping.data.len() <= PAYLOAD_INLINE);
                // Owned, non-blocking send (fire-and-forget; drained below).
                drop(ep.send(PONG, &[], ping.data.into_vec()).unwrap());
            }
            assert!(
                w.drain(Duration::from_secs(5)).unwrap(),
                "echo sends did not finish"
            );
        },
        |w, ep| {
            for size in SIZES {
                let payload = pattern(size);
                ep.send_blocking(PING, b"from-main", &payload).unwrap();
                let pong = recv_blocking(w);
                assert_eq!(pong.id, PONG);
                assert_eq!(&pong.data[..], &payload[..], "size {size}");
            }
            let lanes = ep.transports().unwrap();
            assert!(!lanes.is_empty());
            if mode == ErrorMode::None {
                assert!(
                    lanes.iter().all(|l| l.transport != "tcp"),
                    "same-host data path must not use TCP: {lanes:?}"
                );
            }
        },
    );
}

/// A large send whose request is dropped while still in flight must still be delivered
/// intact: the worker adopts the request and keeps the buffer alive until completion.
#[test]
fn dropped_pending_send_still_delivers() {
    const SIZE: usize = 8 << 20;
    with_peer(
        ErrorMode::None,
        |w, _ep| {
            let ping = recv_blocking(w);
            assert_eq!(&ping.data[..], &pattern(SIZE)[..]);
        },
        |w, ep| {
            let req = ep.send(PING, b"from-main", pattern(SIZE)).unwrap();
            assert!(req.is_pending(), "8 MiB should use rendezvous");
            drop(req); // fire-and-forget; buffer now owned by the worker
            assert!(
                w.drain(Duration::from_secs(5)).unwrap(),
                "background send did not finish"
            );
        },
    );
}

/// The zero-copy path: the closure sees borrowed header/data during progress, and can
/// reply with a non-blocking send from inside it.
#[test]
fn progress_with_borrows_and_replies() {
    with_peer(
        ErrorMode::None,
        |w, ep| {
            let mut done = false;
            let deadline = Instant::now() + Duration::from_secs(10);
            while !done {
                w.progress_with(|m| {
                    assert_eq!((m.id, m.header, m.data), (PING, &b"hdr"[..], &b"hello"[..]));
                    // Non-blocking send is allowed inside the callback.
                    drop(ep.send(PONG, &[], m.data.to_vec()).unwrap());
                    done = true;
                })
                .unwrap();
                assert!(Instant::now() < deadline, "timed out");
            }
            assert!(w.drain(Duration::from_secs(5)).unwrap());
        },
        |w, ep| {
            ep.send_blocking(PING, b"hdr", b"hello").unwrap();
            assert_eq!(&recv_blocking(w).data[..], b"hello");
        },
    );
}

/// Progressing from inside a receive callback (forbidden by UCX) is a recoverable error,
/// and nothing is sent by a refused `send_blocking`.
#[test]
fn reentrant_calls_return_errors() {
    with_peer(
        ErrorMode::None,
        |w, ep| {
            let mut errors = Vec::new();
            let deadline = Instant::now() + Duration::from_secs(10);
            while errors.is_empty() {
                w.progress_with(|_| {
                    errors.push(w.progress().unwrap_err());
                    errors.push(w.progress_with(|_| {}).unwrap_err());
                    errors.push(w.drain(Duration::ZERO).unwrap_err());
                    errors.push(ep.send_blocking(PONG, &[], b"never").unwrap_err());
                    drop(ep.send(PONG, &[], b"ok".to_vec()).unwrap()); // allowed
                })
                .unwrap();
                assert!(Instant::now() < deadline, "timed out");
            }
            assert!(
                errors.iter().all(|e| matches!(e, Error::Reentrant { .. })),
                "{errors:?}"
            );
            assert!(w.drain(Duration::from_secs(5)).unwrap());
        },
        |w, ep| {
            ep.send_blocking(PING, &[], b"go").unwrap();
            // Only the non-blocking reply arrives; the refused blocking send sent nothing.
            assert_eq!(&recv_blocking(w).data[..], b"ok");
        },
    );
}

/// A panic inside the closure surfaces as a normal Rust panic from `progress_with` (not an
/// abort through C), can be caught, and the same worker keeps working afterwards.
#[test]
fn panic_in_handler_is_recoverable() {
    with_peer(
        ErrorMode::None,
        |w, ep| {
            let deadline = Instant::now() + Duration::from_secs(10);
            let caught = loop {
                let r = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    w.progress_with(|m| panic!("boom on {:?}", m.data)).unwrap()
                }));
                if let Err(payload) = r {
                    break payload;
                }
                assert!(Instant::now() < deadline, "timed out");
            };
            assert!(
                caught
                    .downcast_ref::<String>()
                    .is_some_and(|m| m.contains("boom"))
            );
            // Still usable: receive the next message and reply.
            let next = recv_blocking(w);
            assert_eq!(&next.data[..], b"second");
            ep.send_blocking(PONG, &[], b"alive").unwrap();
        },
        |w, ep| {
            ep.send_blocking(PING, &[], b"first").unwrap();
            ep.send_blocking(PING, &[], b"second").unwrap();
            assert_eq!(&recv_blocking(w).data[..], b"alive");
        },
    );
}
