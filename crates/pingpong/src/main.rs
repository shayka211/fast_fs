//! Two-process UCX ping-pong benchmark. See README.md. Compare with UCX's C benchmark:
//!   server: ucx_perftest -t ucp_am_lat
//!   client: ucx_perftest 127.0.0.1 -t ucp_am_lat -n 1000000 -s 8
//! (`ucx_perftest` reports one-way latency = RTT / 2, as we do.)

mod bench;
mod bootstrap;
mod raw;
mod via_transport;

use std::net::SocketAddr;

use anyhow::Result;
use clap::{Parser, Subcommand, ValueEnum};
use tracing::info;
use ucx::ErrorMode;

use crate::bench::BenchOpts;

#[derive(Parser)]
#[command(about)]
struct Cli {
    /// UCX peer-failure handling for the raw modes. `peer` excludes shared memory (see
    /// ucx::ErrorMode). The transport modes always use `none`.
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
    /// Raw `ucx`: echo every PING back as a PONG, forever.
    Server {
        /// TCP address for the bootstrap (address exchange) only.
        #[arg(long, default_value = "127.0.0.1:13337")]
        listen: SocketAddr,
    },
    /// Raw `ucx`: bootstrap, then send PINGs one at a time and measure round trips.
    Client {
        #[arg(long, default_value = "127.0.0.1:13337")]
        connect: SocketAddr,
        #[command(flatten)]
        opts: BenchOpts,
    },
    /// Through the `transport` crate: answer requests, forever.
    TransportServer {
        #[arg(long, default_value = "127.0.0.1:13337")]
        listen: SocketAddr,
    },
    /// Through the `transport` crate: async request/response round trips.
    TransportClient {
        #[arg(long, default_value = "127.0.0.1:13337")]
        connect: SocketAddr,
        #[command(flatten)]
        opts: BenchOpts,
        /// Run the benchmark on another thread than the transport (via `RemoteHandle`),
        /// to measure the cross-thread hop.
        #[arg(long)]
        cross_thread: bool,
    },
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    let role = match cli.cmd {
        Cmd::Server { .. } | Cmd::TransportServer { .. } => "server",
        Cmd::Client { .. } | Cmd::TransportClient { .. } => "client",
    };
    let _log = logging::init(&format!("pingpong-{role}"), role);
    info!(ucx = %ucx::Version::runtime(), "starting");

    match cli.cmd {
        Cmd::Server { listen } => raw::server(listen, cli.err_mode.into()),
        Cmd::Client { connect, opts } => raw::client(connect, cli.err_mode.into(), opts),
        Cmd::TransportServer { listen } => via_transport::server(listen),
        Cmd::TransportClient {
            connect,
            opts,
            cross_thread,
        } => via_transport::client(connect, opts, cross_thread),
    }
}
