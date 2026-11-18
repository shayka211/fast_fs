use std::time::{Duration, Instant};

use super::*;

const PING: AmId = 1;
const PONG: AmId = 2;

fn progress_until<T>(worker: &Worker, mut f: impl FnMut() -> Option<T>) -> T {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        worker.progress();
        if let Some(v) = f() {
            return v;
        }
        assert!(Instant::now() < deadline, "timed out waiting for progress");
    }
}

#[test]
fn runtime_matches_compiled_api() {
    let v = Version::runtime();
    assert_eq!((v.major, v.minor), Version::COMPILED);
}

#[test]
fn loopback_am_roundtrip_peer_mode() {
    loopback_am_roundtrip(ErrorMode::Peer);
}

#[test]
fn loopback_am_roundtrip_no_err_mode() {
    loopback_am_roundtrip(ErrorMode::None);
}

/// Client and server on one worker over a real listener: eager and rendezvous sizes,
/// and replies routed back via `reply_ep`.
fn loopback_am_roundtrip(mode: ErrorMode) {
    let ctx = Context::new().unwrap();
    let worker = Worker::new(&ctx).unwrap();
    worker.register_am(PING).unwrap();
    worker.register_am(PONG).unwrap();

    let listener = Listener::bind(&worker, "127.0.0.1:0".parse().unwrap(), mode).unwrap();
    assert_ne!(listener.local_addr().port(), 0);
    let client = Endpoint::connect(&worker, listener.local_addr(), mode).unwrap();
    // Creating the endpoint starts the connection; accept before the (blocking) send, which
    // can't complete until the server side exists.
    let server = progress_until(&worker, || listener.accept().unwrap());
    client.send_am(PING, b"hdr", b"first").unwrap();

    let first = progress_until(&worker, || worker.try_recv());
    assert_eq!(
        (first.id, &first.header[..], &first.data[..]),
        (PING, &b"hdr"[..], &b"first"[..])
    );
    assert_eq!(first.reply_ep, Some(server.id()));

    for size in [0, 8, 64 << 10, 4 << 20] {
        let payload: Vec<u8> = (0..size).map(|i| (i % 251) as u8).collect();
        client.send_am(PING, &[], &payload).unwrap();
        let ping = progress_until(&worker, || worker.try_recv());
        assert_eq!(ping.data, payload, "ping size {size}");
        assert_eq!(ping.reply_ep, Some(server.id()));

        server.send_am(PONG, &[], &ping.data).unwrap();
        let pong = progress_until(&worker, || worker.try_recv());
        assert_eq!((pong.id, pong.reply_ep), (PONG, Some(client.id())));
        assert_eq!(pong.data.len(), size);
    }
    assert!(!client.transports().unwrap().is_empty());
}
