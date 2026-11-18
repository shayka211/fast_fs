//! Two-process UCX Active Message ping-pong. Compare with UCX's C benchmark:
//!   server: ucx_perftest -t ucp_am_lat
//!   client: ucx_perftest 127.0.0.1 -t ucp_am_lat -n 1000000 -s 8
//! (`ucx_perftest` reports one-way latency = RTT / 2, as we do.)

use std::net::SocketAddr;
use std::time::{Duration, Instant};

use anyhow::{Context as _, Result, bail};
use clap::{Parser, Subcommand, ValueEnum};
use hdrhistogram::Histogram;
use tracing::{debug, info, warn};
use ucx::{AmId, AmMessage, Context, Endpoint, ErrorMode, Listener, Worker};

/// Bootstrap: client → server, data = client worker address, header = [DataPath].
const HELLO: AmId = 1;
/// Bootstrap reply: header = peer token (u32 LE), data = server worker address.
const HELLO_ACK: AmId = 2;
/// Data phase: header = peer token, data = payload.
const PING: AmId = 3;
const PONG: AmId = 4;

#[derive(Parser)]
#[command(about)]
struct Cli {
    /// UCX peer-failure handling. `peer` excludes shared memory (see ucx::ErrorMode).
    #[arg(long, value_enum, default_value_t = ErrMode::None, global = true)]
    err_mode: ErrMode,
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Clone, Copy, ValueEnum)]
enum ErrMode {
    Peer,
    None,
}

impl From<ErrMode> for ErrorMode {
    fn from(m: ErrMode) -> Self {
        match m {
            ErrMode::Peer => Self::Peer,
            ErrMode::None => Self::None,
        }
    }
}

/// Which endpoint carries the data phase. The listener connection is always used for
/// bootstrap (TCP handshake + address exchange), never for data in `direct` mode.
#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
#[repr(u8)]
enum DataPath {
    /// Endpoints created from exchanged worker addresses (like ucx_perftest).
    Direct = 0,
    /// Keep using the listener (client-server) endpoint.
    Listener = 1,
}

#[derive(Subcommand)]
enum Cmd {
    /// Echo every PING back as a PONG, forever.
    Server {
        #[arg(long, default_value = "127.0.0.1:13337")]
        listen: SocketAddr,
    },
    /// Bootstrap, then send PINGs one at a time and measure round trips.
    Client {
        #[arg(long, default_value = "127.0.0.1:13337")]
        connect: SocketAddr,
        #[arg(long, value_enum, default_value_t = DataPath::Direct)]
        path: DataPath,
        #[arg(long, default_value_t = 1_000_000)]
        iters: u64,
        #[arg(long, default_value_t = 10_000)]
        warmup: u64,
        /// Payload size in bytes.
        #[arg(long, default_value_t = 8)]
        size: usize,
        /// Skip per-iteration timing (no percentiles): isolates messaging cost from
        /// measurement cost.
        #[arg(long)]
        overall_only: bool,
    },
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    let role = match cli.cmd {
        Cmd::Server { .. } => "server",
        Cmd::Client { .. } => "client",
    };
    let _log = logging::init(&format!("pingpong-{role}"), role);
    info!(ucx = %ucx::Version::runtime(), "starting");

    let ctx = Context::new()?;
    let workers = Workers {
        boot: Worker::new(&ctx)?,
        data: Worker::new(&ctx)?,
    };
    let mode = cli.err_mode.into();
    match cli.cmd {
        Cmd::Server { listen } => server(&workers, listen, mode),
        Cmd::Client {
            connect,
            path,
            iters,
            warmup,
            size,
            overall_only,
        } => {
            let session = bootstrap(&workers, connect, path, mode)?;
            let worker = match path {
                DataPath::Direct => &workers.data,
                DataPath::Listener => &workers.boot,
            };
            run_client(
                worker,
                &session.token,
                &session.data,
                RunOpts {
                    iters,
                    warmup,
                    size,
                    overall_only,
                },
            )
        }
    }
}

/// Keeps the direct endpoint if there is one, closing the bootstrap (TCP) endpoint: after
/// bootstrap, no TCP connection is kept open for data.
fn data_endpoint(bootstrap: Endpoint, direct: Option<Endpoint>) -> Endpoint {
    match direct {
        Some(direct) => {
            debug!(peer = bootstrap.peer(), "closing bootstrap endpoint");
            drop(bootstrap); // graceful close: flushes anything still queued (e.g. HELLO_ACK)
            direct
        }
        None => bootstrap,
    }
}

/// Two workers from one context. The bootstrap worker owns the listener and TCP; once it
/// has activated UCX's TCP interface, *every* progress call on it does an `epoll_wait`
/// syscall (~100 ns). The data worker only ever has direct (shared-memory) endpoints, so
/// its progress loop is syscall-free. See docs/PERF.md.
struct Workers {
    boot: Worker,
    data: Worker,
}

/// The server progresses the bootstrap worker only every N data-loop iterations.
const BOOT_POLL_EVERY: u32 = 256;

fn server(workers: &Workers, listen: SocketAddr, mode: ErrorMode) -> Result<()> {
    let Workers { boot, data } = workers;
    boot.register_am(HELLO)?;
    boot.register_am(PING)?; // `--path listener` clients ping over the bootstrap endpoint
    data.register_am(PING)?;
    let my_address = data.address()?;
    let listener = Listener::bind(boot, listen, mode)?;
    // Index = peer token. Slots are cleared (not removed) so tokens stay stable.
    let mut peers: Vec<Option<Endpoint>> = Vec::new();
    let mut pending: Vec<Endpoint> = Vec::new();
    let mut tick = 0u32;
    loop {
        data.progress();
        while let Some(msg) = data.try_recv() {
            pong(&peers, &msg);
        }

        tick = tick.wrapping_add(1);
        if !tick.is_multiple_of(BOOT_POLL_EVERY) {
            continue;
        }
        boot.progress();
        while let Some(ep) = listener.accept()? {
            pending.push(ep);
        }
        while let Some(msg) = boot.try_recv() {
            match msg.id {
                HELLO => {
                    let Some(pos) = pending.iter().position(|ep| Some(ep.id()) == msg.reply_ep)
                    else {
                        warn!(from = ?msg.reply_ep, "HELLO from unknown endpoint, dropping");
                        continue;
                    };
                    let bootstrap = pending.swap_remove(pos);
                    let peer = bootstrap.peer().to_owned();
                    let token = u32::try_from(peers.len())?;
                    // A failing peer is logged and dropped; it must never take the server down.
                    match admit(data, bootstrap, &msg, token, &my_address, mode) {
                        Ok(ep) => peers.push(Some(ep)),
                        Err(e) => warn!(%peer, error = %e, "bootstrap failed, dropping peer"),
                    }
                }
                PING => pong(&peers, &msg),
                id => warn!(am_id = id, "unexpected message"),
            }
        }
        // Dropping a failed endpoint force-closes it.
        for slot in &mut peers {
            if slot.as_ref().is_some_and(|p| p.failed().is_some()) {
                *slot = None;
            }
        }
        pending.retain(|ep| ep.failed().is_none());
    }
}

/// Echoes a PING back to the peer named by its token.
fn pong(peers: &[Option<Endpoint>], msg: &AmMessage) {
    let peer = token_of(&msg.header)
        .and_then(|t| peers.get(t as usize))
        .and_then(Option::as_ref);
    let Some(peer) = peer else {
        warn!(header = ?msg.header, "PING with unknown token, dropping");
        return;
    };
    if let Err(e) = peer.send_am(PONG, &[], &msg.data) {
        warn!(peer = peer.peer(), error = %e, "pong failed");
    }
}

/// Completes one peer's bootstrap: optional direct endpoint, HELLO_ACK, close TCP.
fn admit(
    data: &Worker,
    bootstrap: Endpoint,
    hello: &AmMessage,
    token: u32,
    my_address: &[u8],
    mode: ErrorMode,
) -> Result<Endpoint> {
    let direct = (hello.header.first() == Some(&(DataPath::Direct as u8)))
        .then(|| {
            Endpoint::connect_address(
                data,
                &hello.data,
                format!("{}(direct)", bootstrap.peer()),
                mode,
            )
        })
        .transpose()?;
    bootstrap.send_am(HELLO_ACK, &token.to_le_bytes(), my_address)?;
    info!(
        peer = bootstrap.peer(),
        token,
        direct = direct.is_some(),
        "peer bootstrapped"
    );
    Ok(data_endpoint(bootstrap, direct))
}

fn token_of(header: &[u8]) -> Option<u32> {
    Some(u32::from_le_bytes(header.get(..4)?.try_into().ok()?))
}

/// Client-side result of bootstrap: the connection(s) and our token.
struct Session {
    token: [u8; 4],
    data: Endpoint,
}

/// Connects via the listener, swaps worker addresses, and (for `Direct`) opens the direct
/// endpoint. Everything is set up before the data phase starts.
fn bootstrap(
    workers: &Workers,
    addr: SocketAddr,
    path: DataPath,
    mode: ErrorMode,
) -> Result<Session> {
    let Workers { boot, data } = workers;
    boot.register_am(HELLO_ACK)?;
    boot.register_am(PONG)?;
    data.register_am(PONG)?;
    let bootstrap = Endpoint::connect(boot, addr, mode)?;
    bootstrap.send_am(HELLO, &[path as u8], &data.address()?)?;

    let deadline = Instant::now() + Duration::from_secs(10);
    let ack = loop {
        boot.progress();
        if let Some(m) = boot.try_recv() {
            if m.id == HELLO_ACK {
                break m;
            }
            warn!(am_id = m.id, "unexpected message during bootstrap");
        }
        if let Some(status) = bootstrap.failed() {
            bail!("bootstrap connection failed: {status}");
        }
        if Instant::now() > deadline {
            bail!("bootstrap timed out waiting for HELLO_ACK from {addr}");
        }
    };
    let token = token_of(&ack.header).context("HELLO_ACK without token")?;
    let direct = match path {
        DataPath::Direct => Some(Endpoint::connect_address(
            data,
            &ack.data,
            format!("{addr}(direct)"),
            mode,
        )?),
        DataPath::Listener => None,
    };
    info!(%addr, token, ?path, "bootstrap complete");
    Ok(Session {
        token: token.to_le_bytes(),
        data: data_endpoint(bootstrap, direct),
    })
}

struct RunOpts {
    iters: u64,
    warmup: u64,
    size: usize,
    overall_only: bool,
}

fn run_client(worker: &Worker, token: &[u8; 4], ep: &Endpoint, opts: RunOpts) -> Result<()> {
    let RunOpts {
        iters,
        warmup,
        size,
        overall_only,
    } = opts;
    let payload = vec![0xAB_u8; size];
    let mut rtt = Histogram::<u64>::new_with_bounds(1, 60_000_000_000, 3)?;
    let report_every = (iters / 10).max(1);

    info!(peer = ep.peer(), iters, warmup, size, "running");
    let mut measured_since = Instant::now();
    // One clock read per round trip: each iteration's end is the next one's start
    // (a vDSO clock read costs ~20 ns, comparable to the latency being measured).
    let mut t = Instant::now();
    for i in 0..warmup + iters {
        if i == warmup {
            let lanes: Vec<String> = ep.transports()?.iter().map(ToString::to_string).collect();
            info!(?lanes, "wire-up complete, measuring");
            measured_since = Instant::now();
            t = measured_since;
        }
        ep.send_am(PING, token, &payload)
            .with_context(|| format!("ping #{i}"))?;
        let pong = loop {
            worker.progress();
            if let Some(m) = worker.try_recv() {
                break m;
            }
            if let Some(status) = ep.failed() {
                bail!("server connection failed: {status}");
            }
        };
        if i >= warmup && !overall_only {
            let now = Instant::now();
            rtt.record((now - t).as_nanos() as u64)?;
            t = now;
            let n = i - warmup + 1;
            if n.is_multiple_of(report_every) {
                debug!(
                    done = n,
                    p50_rtt_ns = rtt.value_at_quantile(0.5),
                    "progress"
                );
            }
        }
        if pong.data.len() != size {
            bail!("pong #{i}: expected {size} bytes, got {}", pong.data.len());
        }
    }

    report(&rtt, iters, size, measured_since.elapsed());
    Ok(())
}

fn report(rtt: &Histogram<u64>, iters: u64, size: usize, elapsed: Duration) {
    // One-way latency = RTT / 2, matching ucx_perftest.
    let us = |q: f64| rtt.value_at_quantile(q) as f64 / 2_000.0;
    info!(
        iters,
        size,
        one_way_p50_us = format!("{:.3}", us(0.50)),
        one_way_p99_us = format!("{:.3}", us(0.99)),
        one_way_p999_us = format!("{:.3}", us(0.999)),
        one_way_max_us = format!("{:.3}", rtt.max() as f64 / 2_000.0),
        one_way_mean_us = format!("{:.3}", rtt.mean() / 2_000.0),
        // Total wall time / iterations: no per-iteration clock overhead. This is the
        // apples-to-apples number vs ucx_perftest's "overall" latency column.
        one_way_overall_us = format!("{:.3}", elapsed.as_secs_f64() * 1e6 / iters as f64 / 2.0),
        msg_rate = format!("{:.0}", iters as f64 / elapsed.as_secs_f64()),
        "results"
    );
}
