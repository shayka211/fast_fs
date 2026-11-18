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
        worker.progress();
        if let Some(m) = worker.try_recv() {
            return m;
        }
        assert!(Instant::now() < deadline, "timed out waiting for a message");
    }
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

/// Two workers on two threads (as two nodes are), connected by exchanging worker
/// addresses. Covers eager and rendezvous sizes, header passthrough, and that the data
/// path never uses TCP in `ErrorMode::None`.
///
/// Each side needs its own thread: `send_am` blocks progressing only its own worker, and
/// wire-up needs the peer to progress too.
fn roundtrip(mode: ErrorMode) {
    let ctx = Context::new().unwrap();
    let (to_echo, from_main) = mpsc::channel::<Vec<u8>>();
    let (to_main, from_echo) = mpsc::channel::<Vec<u8>>();

    let echo_ctx = Arc::clone(&ctx);
    let echo = thread::spawn(move || {
        let w = Worker::new(&echo_ctx).unwrap();
        w.register_am(PING).unwrap();
        to_main.send(w.address().unwrap()).unwrap();
        let ep = Endpoint::connect(&w, &from_main.recv().unwrap(), "main", mode).unwrap();
        for _ in SIZES {
            let ping = recv_blocking(&w);
            assert_eq!((ping.id, &ping.header[..]), (PING, &b"from-main"[..]));
            ep.send_am(PONG, &[], &ping.data).unwrap();
        }
    });

    let w = Worker::new(&ctx).unwrap();
    w.register_am(PONG).unwrap();
    to_echo.send(w.address().unwrap()).unwrap();
    let ep = Endpoint::connect(&w, &from_echo.recv().unwrap(), "echo", mode).unwrap();
    for size in SIZES {
        let payload: Vec<u8> = (0..size).map(|i| (i % 251) as u8).collect();
        ep.send_am(PING, b"from-main", &payload).unwrap();
        let pong = recv_blocking(&w);
        assert_eq!(pong.id, PONG);
        assert_eq!(pong.data, payload, "size {size}");
    }

    let lanes = ep.transports().unwrap();
    assert!(!lanes.is_empty());
    if mode == ErrorMode::None {
        assert!(
            lanes.iter().all(|l| l.transport != "tcp"),
            "same-host data path must not use TCP: {lanes:?}"
        );
    }
    echo.join().unwrap();
}
