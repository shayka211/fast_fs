#![doc = include_str!("../README.md")]

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
