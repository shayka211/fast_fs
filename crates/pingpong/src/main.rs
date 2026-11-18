//! Two-process UCX Active Message ping-pong. See README.md. Compare with UCX's C benchmark:
//!   server: ucx_perftest -t ucp_am_lat
//!   client: ucx_perftest 127.0.0.1 -t ucp_am_lat -n 1000000 -s 8
//! (`ucx_perftest` reports one-way latency = RTT / 2, as we do.)

mod bootstrap;

use std::net::{SocketAddr, TcpListener};
use std::time::{Duration, Instant};

use anyhow::{Context as _, Result, bail};
use clap::{Parser, Subcommand, ValueEnum};
use hdrhistogram::Histogram;
use tracing::{debug, info, warn};
use ucx::{AmId, Context, Endpoint, ErrorMode, Worker};

/// Header = client token (u32 LE), data = payload.
const PING: AmId = 1;
/// Data = the same payload.
const PONG: AmId = 2;

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

#[derive(Subcommand)]
enum Cmd {
    /// Echo every PING back as a PONG, forever.
    Server {
        /// TCP address for the bootstrap (address exchange) only.
        #[arg(long, default_value = "127.0.0.1:13337")]
        listen: SocketAddr,
    },
    /// Bootstrap, then send PINGs one at a time and measure round trips.
    Client {
        #[arg(long, default_value = "127.0.0.1:13337")]
        connect: SocketAddr,
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
    let worker = Worker::new(&ctx)?;
    let mode = cli.err_mode.into();
    match cli.cmd {
        Cmd::Server { listen } => server(&worker, listen, mode),
        Cmd::Client {
            connect,
            iters,
            warmup,
            size,
            overall_only,
        } => {
            worker.register_am(PONG)?;
            let (token, server_address) = bootstrap::join(connect, &worker.address()?)
                .with_context(|| format!("bootstrap with {connect}"))?;
            let ep = Endpoint::connect(&worker, &server_address, connect.to_string(), mode)?;
            info!(%connect, token, "bootstrap complete, TCP closed");
            let opts = RunOpts {
                iters,
                warmup,
                size,
                overall_only,
            };
            run_client(&worker, &token.to_le_bytes(), &ep, opts)
        }
    }
}

fn server(worker: &Worker, listen: SocketAddr, mode: ErrorMode) -> Result<()> {
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
            match Endpoint::connect(worker, &j.address, j.peer.to_string(), mode) {
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
        ep.send_blocking(PING, token, &payload)
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
        if pong_len != size {
            bail!("pong #{i}: expected {size} bytes, got {pong_len}");
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
