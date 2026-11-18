//! Direct `ucx` mode: one worker, synchronous loop, zero-copy receive. The baseline that
//! the transport modes are compared against.

use std::net::{SocketAddr, TcpListener};

use anyhow::{Context as _, Result, bail};
use tracing::{info, warn};
use ucx::{AmId, Context, Endpoint, ErrorMode, Worker};

use crate::bench::{BenchOpts, Recorder};
use crate::bootstrap;

/// Header = client token (u32 LE), data = payload.
const PING: AmId = 1;
/// Data = the same payload.
const PONG: AmId = 2;

pub fn server(listen: SocketAddr, mode: ErrorMode) -> Result<()> {
    let ctx = Context::new()?;
    let worker = Worker::new(&ctx)?;
    worker.register_am(PING)?;
    let listener = TcpListener::bind(listen).with_context(|| format!("bind {listen}"))?;
    info!(%listen, "listening for bootstrap");
    let joined = bootstrap::spawn_acceptor(listener, worker.address()?);

    // Index = client token.
    let mut peers: Vec<Option<Endpoint>> = Vec::new();
    // Received pings, copied out of the zero-copy view (replies can't be sent blocking from
    // inside the receive callback). Reused every loop: no allocation after warmup.
    let mut scratch: Vec<u8> = Vec::new();
    let mut pings: Vec<(u32, std::ops::Range<usize>)> = Vec::new();
    loop {
        scratch.clear();
        pings.clear();
        worker.progress_with(|m| match token_of(m.header) {
            Some(token) => {
                let start = scratch.len();
                scratch.extend_from_slice(m.data);
                pings.push((token, start..scratch.len()));
            }
            None => warn!(am_id = m.id, header = ?m.header, "message without token, dropping"),
        })?;
        // Lock-free channel poll: no syscall in the data loop.
        while let Ok(j) = joined.try_recv() {
            match Endpoint::connect(&worker, &j.address, j.peer.to_string(), mode) {
                Ok(ep) => {
                    let slot = j.token as usize;
                    if peers.len() <= slot {
                        peers.resize_with(slot + 1, || None);
                    }
                    peers[slot] = Some(ep);
                }
                Err(e) => warn!(peer = %j.peer, error = %e, "connect to joined peer failed"),
            }
        }
        for (token, range) in pings.drain(..) {
            pong(&peers, token, &scratch[range]);
        }
        // Dropping a failed endpoint closes it (only reported in `--err-mode peer`).
        for slot in &mut peers {
            if slot.as_ref().is_some_and(|p| p.failed().is_some()) {
                *slot = None;
            }
        }
    }
}

/// Echoes a PING back to the peer named by its token.
fn pong(peers: &[Option<Endpoint>], token: u32, data: &[u8]) {
    let Some(peer) = peers.get(token as usize).and_then(Option::as_ref) else {
        warn!(token, "ping with unknown token, dropping");
        return;
    };
    if let Err(e) = peer.send_blocking(PONG, &[], data) {
        warn!(peer = peer.peer(), error = %e, "pong failed");
    }
}

fn token_of(header: &[u8]) -> Option<u32> {
    Some(u32::from_le_bytes(header.get(..4)?.try_into().ok()?))
}

pub fn client(connect: SocketAddr, mode: ErrorMode, opts: BenchOpts) -> Result<()> {
    let ctx = Context::new()?;
    let worker = Worker::new(&ctx)?;
    worker.register_am(PONG)?;
    let (token, server_address) = bootstrap::join(connect, &worker.address()?)
        .with_context(|| format!("bootstrap with {connect}"))?;
    let ep = Endpoint::connect(&worker, &server_address, connect.to_string(), mode)?;
    info!(%connect, token, "bootstrap complete, TCP closed");

    let token = token.to_le_bytes();
    let payload = vec![0xAB_u8; opts.size];
    let mut rec = Recorder::new(opts)?;
    info!(
        peer = ep.peer(),
        iters = opts.iters,
        warmup = opts.warmup,
        size = opts.size,
        "running"
    );
    for i in 0..opts.total() {
        if i == opts.warmup {
            let lanes: Vec<String> = ep.transports()?.iter().map(ToString::to_string).collect();
            info!(?lanes, "wire-up complete, measuring");
            rec.start();
        }
        ep.send_blocking(PING, &token, &payload)
            .with_context(|| format!("ping #{i}"))?;
        // Zero-copy receive: we only need the pong's length.
        let mut pong_len = None;
        let pong_len = loop {
            worker.progress_with(|m| pong_len = Some(m.data.len()))?;
            if let Some(n) = pong_len {
                break n;
            }
            if let Some(status) = ep.failed() {
                bail!("server connection failed: {status}");
            }
        };
        rec.round_trip_done()?;
        if pong_len != opts.size {
            bail!("pong #{i}: expected {} bytes, got {pong_len}", opts.size);
        }
    }
    rec.finish();
    Ok(())
}
