//! Transport modes: the same ping-pong as async request/response addressed by `NodeId`,
//! through the `transport` crate. Two client variants:
//! - on-core: the benchmark runs on the transport's own thread (`Transport::request`);
//! - `--cross-thread`: the benchmark runs on another thread (`RemoteHandle::request`),
//!   paying a channel hop each way — what callers like FUSE threads would see.

use std::future::Future;
use std::net::{SocketAddr, TcpListener};
use std::sync::mpsc as std_mpsc;
use std::thread;
use std::time::Duration;

use anyhow::{Context as _, Result, anyhow, bail};
use tracing::{info, warn};
use transport::{Incoming, Reply, Transport};
use types::{NodeId, Service};
use ucx::Context;

use crate::bench::{BenchOpts, Recorder};
use crate::bootstrap;

const SERVER: NodeId = NodeId::new(1);
const PING: u16 = 1;
const PONG: u16 = 2;

/// Bootstrap "hello" in transport mode: the client announces its own NodeId (as real
/// nodes will, from config), followed by its worker address.
fn encode_hello(node: NodeId, address: &[u8]) -> Vec<u8> {
    let mut hello = node.get().to_le_bytes().to_vec();
    hello.extend_from_slice(address);
    hello
}

fn decode_hello(hello: &[u8]) -> Option<(NodeId, &[u8])> {
    let (id, address) = hello.split_first_chunk::<4>()?;
    Some((NodeId::new(u32::from_le_bytes(*id)), address))
}

/// A core: current-thread runtime + `LocalSet`, so `!Send` transports can be used.
fn block_local<F: Future>(f: F) -> F::Output {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .expect("tokio runtime");
    tokio::task::LocalSet::new().block_on(&rt, f)
}

pub fn server(listen: SocketAddr) -> Result<()> {
    block_local(async move {
        let ctx = Context::new()?;
        let t = Transport::builder(SERVER)
            .route(Service::Node, |t: &Transport, m: Incoming<'_>| {
                if let Err(e) = t.reply(&m.header, PONG, m.payload.to_vec()) {
                    warn!(from = %m.header.source, error = %e, "pong failed");
                }
            })
            .build(&ctx)?;
        let listener = TcpListener::bind(listen).with_context(|| format!("bind {listen}"))?;
        info!(%listen, node = %SERVER, "listening for bootstrap");
        let joined = bootstrap::spawn_acceptor(listener, t.address()?);

        // Admit clients without touching the driver's hot loop: check every millisecond.
        let admit = tokio::task::spawn_local({
            let t = t.clone();
            async move {
                loop {
                    while let Ok(j) = joined.try_recv() {
                        let Some((node, address)) = decode_hello(&j.address) else {
                            warn!(peer = %j.peer, "malformed hello, ignoring");
                            continue;
                        };
                        match t.add_peer(node, address) {
                            Ok(()) => info!(peer = %j.peer, %node, "client joined"),
                            Err(e) => warn!(peer = %j.peer, %node, error = %e, "add_peer failed"),
                        }
                    }
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
            }
        });
        let result = t.run().await;
        admit.abort();
        Ok(result?)
    })
}

/// Builds a client transport, bootstraps with the server (announcing our NodeId), and
/// connects to it.
fn connect(ctx: &std::sync::Arc<Context>, server: SocketAddr) -> Result<Transport> {
    // Unique per local process; real nodes take their id from config.
    let me = NodeId::new(1_000_000 + std::process::id());
    let t = Transport::builder(me).build(ctx)?;
    let (_token, server_address) = bootstrap::join(server, &encode_hello(me, &t.address()?))
        .with_context(|| format!("bootstrap with {server}"))?;
    t.add_peer(SERVER, &server_address)?;
    info!(%server, node = %me, "bootstrap complete, TCP closed");
    Ok(t)
}

pub fn client(server: SocketAddr, opts: BenchOpts, cross_thread: bool) -> Result<()> {
    if !cross_thread {
        return block_local(async move {
            let ctx = Context::new()?;
            let t = connect(&ctx, server)?;
            let driver = tokio::task::spawn_local({
                let t = t.clone();
                async move { t.run().await }
            });
            // A 'static payload: no allocation per request.
            let payload: &'static [u8] = Box::leak(vec![0xAB_u8; opts.size].into_boxed_slice());
            let lanes = || {
                t.peer_transports(SERVER)
                    .map(|l| l.iter().map(ToString::to_string).collect::<Vec<_>>())
            };
            measure(opts, "on-core", lanes, || {
                t.request(SERVER, Service::Node, PING, payload)
            })
            .await?;
            t.stop();
            driver.await??;
            Ok(())
        });
    }

    // The transport (and its driver) live on their own "core" thread; we only get a handle.
    let (handle_tx, handle_rx) = std_mpsc::channel();
    let span = tracing::Span::current();
    let core = thread::Builder::new()
        .name("transport".into())
        .spawn(move || -> Result<()> {
            let _span = span.entered();
            block_local(async move {
                let ctx = Context::new()?;
                let t = connect(&ctx, server)?;
                handle_tx.send(t.remote()).ok();
                Ok(t.run().await?)
            })
        })?;
    let remote = handle_rx
        .recv()
        .map_err(|_| anyhow!("transport thread failed to start"))?;
    let payload = vec![0xAB_u8; opts.size];
    let result = block_local(async {
        let no_lanes = || Ok(vec!["(owned by the transport thread)".to_owned()]);
        measure(opts, "cross-thread", no_lanes, || {
            remote.request(SERVER, Service::Node, PING, payload.clone())
        })
        .await
    });
    remote.stop();
    core.join()
        .map_err(|_| anyhow!("transport thread panicked"))??;
    result
}

/// The benchmark loop shared by both client variants.
async fn measure<F, Fut>(
    opts: BenchOpts,
    variant: &str,
    lanes: impl Fn() -> transport::Result<Vec<String>>,
    mut ping: F,
) -> Result<()>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = transport::Result<Reply>>,
{
    wait_until_ready(&mut ping).await?;
    let mut rec = Recorder::new(opts)?;
    info!(
        variant,
        iters = opts.iters,
        warmup = opts.warmup,
        size = opts.size,
        "running"
    );
    for i in 0..opts.total() {
        if i == opts.warmup {
            info!(lanes = ?lanes()?, "wire-up complete, measuring");
            rec.start();
        }
        let reply = ping().await.with_context(|| format!("request #{i}"))?;
        rec.round_trip_done()?;
        if reply.payload.len() != opts.size {
            bail!(
                "reply #{i}: expected {} bytes, got {}",
                opts.size,
                reply.payload.len()
            );
        }
    }
    rec.finish();
    Ok(())
}

/// Retries a short-timeout probe until the server answers. The server registers a new
/// client asynchronously after bootstrap, so the very first request can arrive before it
/// knows how to reply; without a deadline that request would wait forever. (M3c replaces
/// this with in-band joining: the client announces itself in its first message.)
async fn wait_until_ready<F, Fut>(ping: &mut F) -> Result<()>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = transport::Result<Reply>>,
{
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        match tokio::time::timeout(Duration::from_millis(100), ping()).await {
            Ok(reply) => {
                reply.context("readiness probe")?;
                return Ok(());
            }
            Err(_) if tokio::time::Instant::now() < deadline => {
                tracing::debug!("server not ready yet, retrying");
            }
            Err(_) => bail!("server did not answer within 10 s"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hello_roundtrips_and_rejects_short_input() {
        let hello = encode_hello(NodeId::new(42), b"addr");
        assert_eq!(decode_hello(&hello), Some((NodeId::new(42), &b"addr"[..])));
        assert_eq!(decode_hello(&[1, 2, 3]), None);
    }
}
