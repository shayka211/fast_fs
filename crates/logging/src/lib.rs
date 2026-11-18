//! Logging setup shared by every fast_fs binary. Call [`init`] first thing in `main`
//! and keep the returned guard alive until exit.
//!
//! Environment:
//! - `RUST_LOG`: filter, e.g. `info` (default), `debug`, `ucx=trace,pingpong=debug`.
//! - `FASTFS_LOG_FORMAT`: `compact` (default), `pretty` (multi-line, file:line), `json`.
//! - `FASTFS_LOG_DIR`: also write `<process>.log` (JSON lines) into this directory, so many
//!   local node processes keep separate, greppable logs (`jq` friendly).
//!
//! Every line emitted on the main thread carries the process identity (the root span,
//! e.g. `pingpong{role=server}`), so interleaved output from many processes in one
//! terminal stays readable.
//!
//! Levels (see CLAUDE.md): error = failed, can't proceed; warn = recoverable anomaly;
//! info = lifecycle (startup, bound, connected, results); debug = per-connection/control
//! details; trace = every message sent/received.

use std::{env, io::IsTerminal, path::PathBuf};

use tracing::span::EnteredSpan;
use tracing_appender::non_blocking::WorkerGuard;
use tracing_subscriber::{EnvFilter, Layer, fmt, layer::SubscriberExt, util::SubscriberInitExt};

/// Keeps the file writer flushing and the process root span entered. Drop at exit.
#[must_use = "logs stop being written to file when the guard is dropped"]
#[derive(Debug)]
pub struct LogGuard {
    _file: Option<WorkerGuard>,
    _root: EnteredSpan,
}

/// Installs the global subscriber. `process` names this process for the log file
/// (e.g. `node-3`), and `role` is attached to every main-thread line.
pub fn init(process: &str, role: &str) -> LogGuard {
    let filter = || EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));

    let stderr = fmt::layer()
        .with_writer(std::io::stderr)
        .with_ansi(std::io::stderr().is_terminal())
        .with_thread_names(true);
    let stderr = match env::var("FASTFS_LOG_FORMAT").as_deref() {
        Ok("pretty") => stderr.pretty().boxed(),
        Ok("json") => stderr.json().boxed(),
        _ => stderr.compact().boxed(),
    };

    let (file_layer, file_guard) = match env::var_os("FASTFS_LOG_DIR") {
        Some(dir) => {
            let dir = PathBuf::from(dir);
            let appender = tracing_appender::rolling::never(&dir, format!("{process}.log"));
            let (writer, guard) = tracing_appender::non_blocking(appender);
            let layer = fmt::layer()
                .json()
                .with_writer(writer)
                .with_filter(filter());
            (Some(layer), Some(guard))
        }
        None => (None, None),
    };

    tracing_subscriber::registry()
        .with(stderr.with_filter(filter()))
        .with(file_layer)
        .init();

    let root = tracing::info_span!("proc", name = process, role).entered();
    LogGuard {
        _file: file_guard,
        _root: root,
    }
}
