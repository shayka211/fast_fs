# fast_fs — Design

A distributed file system: N identical node processes (locally: many processes on one
machine, each with its own data dir and UCX port) presenting one POSIX-ish namespace.

## Architecture

- **Metadata** (inodes, dirents, layouts, cluster map) = a Raft-replicated state machine.
  One Raft group at first (makes `rename` atomic); shard later (Multi-Raft) behind a router.
- **Data** = files split into fixed **chunks** (stripe units, 1–4 MiB). Each chunk is placed
  independently by **rendezvous hashing (HRW)** over the cluster map → big files stripe across
  all nodes. "Shard" is reserved for erasure-coding fragments.
- **Transport** = UCX. Active Messages for RPC, RMA (`get_nbx`/`put_nbx`) for bulk chunk data.

## Core principle: sans-IO (functional core, imperative shell)

`raft`, `meta`, `placement`, read/write planning are **pure and deterministic**: no I/O, no
clock, no randomness, no `.await`. Shape: `step(Input) -> Vec<Output>`. Async drivers execute
outputs. Two drivers: real (tokio + UCX + files) and **simulation** (seeded RNG, fake clock,
faulty network, fake disk that loses un-fsynced writes). Any failing seed replays exactly.

## Raft

- Typestate roles: `ClusterNode<Follower|Candidate|Leader>`; role-specific data lives in the
  state struct (`Leader { progress }`, `Candidate { votes }`). Transitions consume `self`.
- Runtime dispatch via `enum RaftNode { Follower(..), Candidate(..), Leader(..) }`; `step`
  is the only place transitions happen.
- `Committed<T>` proof token (private constructor) — `meta::apply` only accepts committed entries.
- Order: election + **pre-vote** → replication/commit (leader no-op on election) → persistence
  (fsync term/vote/entries **before** replying) → ReadIndex reads → snapshots → membership change.
- Hand-written, Figure 2 of the Raft paper is the spec.

## Metadata state machine

- `Inode { id, kind, mode, uid, gid, size, nlink, times, generation, layout }`,
  `Layout { chunk_size, protection, chunks: BTreeMap<idx, ChunkVersion> }`,
  dirents `BTreeMap<(parent, name), InodeId>`.
- `FsMutation` enum (Command pattern). **Leader fills all nondeterminism** (new ids, timestamps,
  versions) before proposing; `apply` must be bit-identical on every replica (sim checks hashes).
- Client sessions `(session_id, seq)` deduplicated in the state machine → idempotent retries.

## Data path

- **Write (copy-on-write):** get layout + new version → write `(inode, idx, v+1)` to k HRW
  replicas (tmp → fsync → rename) → all ack → propose `CommitChunk` → visible. GC removes
  orphans/old versions by inventory vs metadata.
- **Read (smart client assembles):** fetch+cache layout (pins versions) → pure
  `plan_read(layout, off, len) -> Vec<ChunkRead>` → split dest buffer into disjoint `&mut`
  slices → concurrent bounded reads (`FuturesUnordered`) fill slices in place. No server-side
  assembly. Later: RMA directly into registered client buffer.
- **Fencing:** every data RPC carries cluster-map `Epoch`; stale epochs rejected.
- Protection: `DataProtection` strategy; stored as an enum in the layout. Replication(3) first,
  Reed–Solomon later.

## WAL & recovery

- The Raft log **is** the metadata WAL. Segment files (~64 MiB); record =
  `[len][crc32c][term][index][payload]`. Recovery truncates a torn tail (bad CRC/short record —
  never acked, so safe). Snapshot at N → delete segments below N.
- Hard state (`term`, `voted_for`): tiny file, atomic write (tmp, fsync, rename, fsync dir).
- Chunks: per-4KiB checksums, verified on read + background scrubber; repair from healthy replica.
- Node gone past grace period → leader proposes cluster-map change → repair actor re-replicates.

## Async & UCX

- tokio. Actors own state, communicate over `mpsc`; replies via `oneshot`. Public API is
  cloneable `Handle`s with plain `async fn` methods. Observer = `watch` (latest value: cluster
  map, leader) / `broadcast` (events).
- Native `async fn` in traits, `-> impl Future + Send` where spawned; `dyn` only when needed.
- UCX worker is single-threaded and owned by a `TransportActor`; integrated with tokio via
  `ucp_worker_get_efd` + `AsyncFd` (arm/progress loop); busy-poll mode as config flag.
  Request completion callbacks fire `oneshot`s. Pre-registered buffer pool for RMA.
- `Transport` trait: `UcxTransport` and `SimTransport`.
- Serialization: `bincode`/`postcard` for control; raw bytes for chunk payloads.

## Connections (decided in M2, measured)

- **TCP only for startup.** Nodes swap UCX *worker addresses* over a plain TCP socket
  (the `ucp_hello_world` pattern), close it, and create endpoints with
  `Endpoint::connect(worker, address, ..)`. UCX never opens a TCP connection, so Raft and data
  traffic run over shared memory / RDMA only. The `ucx` crate deliberately has no UCX
  listener/sockaddr API. A node is "ready" only after bootstrap completes. Later, addresses
  come from cluster config / the Raft cluster map instead.
- **`ErrorMode::None` for data endpoints.** `ErrorMode::Peer` makes UCX exclude shared memory
  (it can't detect a dead peer), so same-host traffic drops to TCP: 4.5 µs vs 0.22 µs one-way.
  Peer liveness is the application's job (Raft heartbeats, RPC timeouts), not UCX's.
- Why that matters: a UCX worker progresses every interface any endpoint ever activated.
  An earlier UCX-listener bootstrap left TCP active, which cost an `epoll_wait` syscall per
  progress call (0.221 vs 0.083 µs). See `docs/PERF.md`.
- Peers identify themselves in AM headers (node id / token). UCX doesn't tell a receiver
  which endpoint a message came from.

## Logging

`logging::init(process, role)` in every binary; `tracing` everywhere. Levels: **error** failed,
can't proceed · **warn** recoverable anomaly · **info** lifecycle (start, listen, connect,
bootstrap, results) · **debug** resource lifecycle and control details · **trace** every
message sent/received (peer, AM id, sizes). Root span carries process name + role.

## UCX build

`scripts/build-ucx.sh [release|debug]` builds the submodule into `.ucx/<flavor>` once.
`ucx-sys/build.rs` only locates it: `UCX_DIR` → `.ucx/release` → pkg-config (system).
Dynamic linking (UCX loads transport plugins at runtime anyway). FFI has no call overhead;
verified by benchmarking against C `ucx_perftest`.

## Testing

1. Unit + `proptest` on pure crates.
2. Deterministic simulation with invariants: ≤1 leader/term, committed never lost, replica
   state hashes match, k replicas after quiescence, linearizable client history.
3. Real multi-process chaos (`launcher`): kill -9 / SIGSTOP random nodes, verify checksums.
   (Does not test power loss — the sim disk does.)

## Milestones

| # | Deliverable | Done when |
|---|---|---|
| M0 ✅ | Workspace: `ucx-sys`, `ucx`, `pingpong` (`transport` deferred to M3) | `cargo clippy` clean |
| M1 ✅ | `build-ucx.sh`, `ucx-sys` bindgen + lookup | UCX 1.24 with posix/sysv/cma/tcp; runtime version test passes |
| M2 ✅* | Safe sync wrapper: Context, Worker, Endpoint (by address), AM; RAII; logging; TCP bootstrap | 2-process 1M pings vs `ucx_perftest` (pinned cores) |

\* M2 results (pinned, 8 B, one-way): TCP within 3% of C (4.52 vs 4.40 µs). Shared memory
0.221 µs → **0.083 µs** overall vs C 0.067 µs after moving the bootstrap to plain TCP
(`docs/PERF.md`). Remaining ~16 ns/hop = per-message `Vec` alloc + copy + queue in the AM callback → fixed in M3
(zero-copy receive with `UCP_AM_FLAG_PERSISTENT_DATA`, buffer pool, TSC clock).

| M3 | Async `TransportActor` + `Transport` trait; zero-copy AM receive; heartbeat-based peer liveness | same bench async ≤ C+10%; event vs busy-poll recorded |
| M4 | Raft core + simulator | invariants hold over many seeds |
| M5 | WAL + snapshots | crash recovery in sim and kill -9 |
| M6 | Metadata FS over Raft + client CLI | create/mkdir/lookup/readdir/rename/unlink |
| M7 | Chunk store + HRW + 3x replication, CoW commit | striped read/write verified |
| M8 | Failure handling: liveness, repair, scrub, GC, membership | chaos runs pass |
| M9 | Perf: RMA zero-copy, FUSE, EC, thread-per-core, Multi-Raft | — |
