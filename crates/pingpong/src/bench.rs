//! What every client mode shares: benchmark options and the round-trip recorder.

use std::time::{Duration, Instant};

use anyhow::Result;
use hdrhistogram::Histogram;
use tracing::{debug, info};

/// Client benchmark options (flattened into each client subcommand).
#[derive(Debug, Clone, Copy, clap::Args)]
pub struct BenchOpts {
    /// Measured round trips.
    #[arg(long, default_value_t = 1_000_000)]
    pub iters: u64,
    /// Unmeasured round trips first (finishes wire-up, warms caches).
    #[arg(long, default_value_t = 10_000)]
    pub warmup: u64,
    /// Payload size in bytes.
    #[arg(long, default_value_t = 8)]
    pub size: usize,
    /// Skip per-iteration timing (no percentiles): isolates messaging cost from
    /// measurement cost.
    #[arg(long)]
    pub overall_only: bool,
}

impl BenchOpts {
    pub fn total(&self) -> u64 {
        self.warmup + self.iters
    }
}

/// Times round trips. Usage: call [`Self::start`] once warmup is over, then
/// [`Self::round_trip_done`] after every round trip, then [`Self::finish`].
pub struct Recorder {
    opts: BenchOpts,
    rtt: Histogram<u64>,
    report_every: u64,
    measuring: bool,
    measured: u64,
    since: Instant,
    /// One clock read per round trip: each iteration's end is the next one's start
    /// (a vDSO clock read costs ~20 ns, comparable to the latency being measured).
    last: Instant,
}

impl Recorder {
    pub fn new(opts: BenchOpts) -> Result<Self> {
        let now = Instant::now();
        Ok(Self {
            opts,
            rtt: Histogram::new_with_bounds(1, 60_000_000_000, 3)?,
            report_every: (opts.iters / 10).max(1),
            measuring: false,
            measured: 0,
            since: now,
            last: now,
        })
    }

    /// Warmup is over: start the clock.
    pub fn start(&mut self) {
        self.measuring = true;
        self.since = Instant::now();
        self.last = self.since;
    }

    pub fn round_trip_done(&mut self) -> Result<()> {
        if !self.measuring {
            return Ok(());
        }
        self.measured += 1;
        if self.opts.overall_only {
            return Ok(());
        }
        let now = Instant::now();
        self.rtt.record((now - self.last).as_nanos() as u64)?;
        self.last = now;
        if self.measured.is_multiple_of(self.report_every) {
            debug!(
                done = self.measured,
                p50_rtt_ns = self.rtt.value_at_quantile(0.5),
                "progress"
            );
        }
        Ok(())
    }

    pub fn finish(self) {
        report(
            &self.rtt,
            self.opts.iters,
            self.opts.size,
            self.since.elapsed(),
        );
    }
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
