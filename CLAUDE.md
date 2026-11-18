# fast_fs

Distributed file system in Rust over UCX. **Read `docs/DESIGN.md` first** — it is the single
source of truth for architecture and milestones. Update it when a decision changes; don't
re-derive designs in conversation.

## Layout

Cargo workspace, crates in `crates/`. Shared deps/lints in the root `Cargo.toml`
(`[workspace.dependencies]`, `[workspace.lints]`); member crates use `x.workspace = true`.
Only create a crate when a milestone needs it.

## UCX

- `ucx/` is a git submodule — never edit it.
- Build once: `scripts/build-ucx.sh release` (or `debug`) → `.ucx/<flavor>/` (gitignored).
- `ucx-sys` locates UCX via `UCX_DIR`, then `.ucx/release`, then pkg-config.
- Runtime lookup: a RUNPATH (`.cargo/config.toml`) points at `.ucx/release/lib`, so binaries
  run directly from `target/`. `LD_LIBRARY_PATH` overrides it (e.g. `.ucx/debug/lib`).
- `ucx-sys` has `doctest = false` (UCX's C doc comments aren't Rust).
- Hot paths receive with `Worker::progress_with` (borrowed `AmView`, zero-copy); only
  non-blocking `Endpoint::send` inside it. `progress()` + `try_recv()` copies (convenience).
- Benchmark A/B against the previous commit (separate worktree, same pinned cores,
  alternating runs) and check no stray process spins on those cores. See `docs/PERF.md`.

## Conventions

- **Sans-IO core:** `raft`, `meta`, `placement`, planners are pure and deterministic — no I/O,
  clocks, randomness, or async. Drivers do the effects.
- **Patterns** (use where they fit, not speculatively): typestate (Raft roles, UCX lifecycles),
  newtypes for all ids, RAII for every C resource, actor + cloneable `Handle`, Command
  (`FsMutation`), Strategy (`DataProtection`), Proxy (local/remote `ChunkReader`), Builder for
  configs, Observer via `tokio::sync::watch`/`broadcast` (no callback lists).
- **No duplication:** one generic actor runtime, one generic RPC layer (`Request` trait with
  associated `Response`), one `Transport` trait. Prefer traits/generics; macros only when traits
  can't. No single-impl traits unless they are test seams.
- **Errors:** `thiserror` per library crate with explicit retry variants
  (`NotLeader { hint }`, `StaleEpoch`, `Timeout`); `anyhow` only in binaries.
- **No panics as API behavior.** Misuse returns an error (e.g. `ucx::Error::Reentrant`), not a
  panic. Panics from user code running inside C callbacks are caught at the FFI boundary
  and re-raised in Rust, so they are catchable and leave the state usable. `Drop` never
  panics (it checks `std::thread::panicking()` before doing work that could).
- **unsafe:** confined to `ucx-sys`/`ucx`; every `unsafe` block gets a `// SAFETY:` comment.
- **Docs:** every crate has a `README.md` (purpose, types, usage, rules, logging), included as
  its rustdoc via `#![doc = include_str!("../README.md")]`, so there's one source and its examples are
  compile-checked. Update it with the code. `cargo doc` must pass with `-D warnings`.
- Async traits: native `async fn`; `-> impl Future<Output = _> + Send` when spawned.

## Logging / debugging

- `RUST_LOG` filter (default `info`), e.g. `RUST_LOG=debug`, `RUST_LOG=info,ucx=trace`.
- `FASTFS_LOG_FORMAT=pretty|compact|json`, `FASTFS_LOG_DIR=dir` → also `<process>.log` (JSON).
- UCX's own logs: `UCX_LOG_LEVEL=info|debug` (release build); `trace` needs the debug build:
  `scripts/build-ucx.sh debug`, then `LD_LIBRARY_PATH=$PWD/.ucx/debug/lib`.
  `UCX_PROTO_INFO=y` prints the protocol/transport chosen per message size.
- Every new component logs per the levels in `docs/DESIGN.md` (Logging section).
- Profiling: see `docs/PERF.md` (perf recipes; use `cpu-clock:u` on this AMD box, pin cores).
- zsh gotcha: don't name a shell loop variable `path` (it is tied to `$PATH`).

## Checks

`cargo fmt --all && cargo clippy --workspace --all-targets -- -D warnings && cargo test --workspace`
`RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps`
