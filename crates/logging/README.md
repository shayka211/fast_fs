# logging

One call that sets up `tracing` for every fast_fs binary, so all processes log the same way
and can be debugged the same way.

```rust,no_run
// First thing in `main`. Keep the guard alive until exit: it flushes the log file and
// holds the root span.
let _log = logging::init("node-3", "storage");
tracing::info!(port = 13337, "listening");
```

`init(process, role)`:
- installs a stderr logger (colored when it's a terminal, plain when redirected);
- optionally also writes JSON lines to a file per process;
- enters a root span `proc{name=<process> role=<role>}`, so every line from the main thread
  says which process produced it. That matters when many local node processes share one
  terminal.

## Configuration (environment)

| Variable | Default | Meaning |
|---|---|---|
| `RUST_LOG` | `info` | Filter, e.g. `debug`, `info,ucx=trace`, `pingpong=debug,ucx=warn` |
| `FASTFS_LOG_FORMAT` | `compact` | `compact` (one line), `pretty` (multi-line, with file:line), `json` |
| `FASTFS_LOG_DIR` | unset | Also write `<process>.log` (JSON lines) into this directory |

Per-process JSON files combine well with `jq`:

```sh
FASTFS_LOG_DIR=/tmp/logs ./target/release/pingpong server
jq -r 'select(.level=="WARN") | "\(.timestamp) \(.fields.message)"' /tmp/logs/pingpong-server.log
jq -c 'select(.target=="ucx::endpoint")' /tmp/logs/*.log      # every endpoint event, all nodes
```

## Levels: what goes where

All crates follow the same convention:

| Level | Use for | Examples |
|---|---|---|
| `error` | failed, can't proceed | rendezvous receive failed, panic in a receive handler |
| `warn` | recoverable anomaly | peer endpoint failed, a background send failed, bad bootstrap frame |
| `info` | lifecycle | starting, listening, endpoint created, peer bootstrapped, results |
| `debug` | resource lifecycle and control details | worker created, endpoint closed, AM handler registered |
| `trace` | every message | `am send` / `am recv` with peer, id, and sizes |

`trace` costs nothing when filtered out (a cached per-call-site check), but turning it on
slows down hot paths noticeably. Enable it per crate (`RUST_LOG=info,ucx=trace`) rather
than globally.

## Threads

The root span is entered on the main thread only. Code that spawns threads should carry it
over, as `pingpong`'s bootstrap thread does:

```rust,no_run
let span = tracing::Span::current();
std::thread::spawn(move || {
    let _span = span.entered();
    tracing::info!("this line is tagged with the process name too");
});
```
