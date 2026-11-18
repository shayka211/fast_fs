# fast_fs — Design

A distributed file system meant to scale to **~10,000 servers**, each running one fast_fs
process that owns that server's NVMe drives. All servers together appear as **one big folder**
through a FUSE mount, so every normal program (shells, editors, `yazi`, `cp`, `fio`) works
unchanged.

Locally, "servers" are processes on one machine (each with its own data directories), network
namespaces, or VMs (see [Test environments](#test-environments)). The code is the same.

## Design rules for 10k servers

- **Nothing is O(N) per request, nothing is all-to-all, and no single leader sits in the data
  path.** Every per-request decision is computed locally from a cached map.
- **Failure is normal.** At 10k servers something is always down. Recovery is continuous and
  throttled, not an emergency.
- **No reliance on synchronized clocks.** Ordering comes from Raft terms and map epochs.
- **Every message carries a protocol version**, because rolling upgrades are mandatory.

## Architecture

```text
                         ┌──────────────────────────────┐
                         │ control plane: 1 Raft group  │  cluster map (nodes, drives,
                         │ of 5, on 5 different racks   │  failure domains, states, epochs),
                         └──────────────┬───────────────┘  shard map, worker addresses
                     map epochs (diffs) │ watched by everyone
        ┌───────────────────────────────┼───────────────────────────────┐
        ▼                               ▼                               ▼
 ┌─────────────┐                 ┌─────────────┐                 ┌─────────────┐
 │  server A   │                 │  server B   │      ...        │  server Z   │
 │ fast_fs proc│                 │ fast_fs proc│                 │ fast_fs proc│
 │ ─ metadata  │ replicas of     │ ─ metadata  │                 │ ─ metadata  │
 │   shard reps│ thousands of    │   shard reps│                 │   shard reps│
 │ ─ PG replica│ metadata shards │ ─ PG replica│                 │ ─ PG replica│
 │ ─ drives    │ (Raft groups)   │ ─ drives    │                 │ ─ drives    │
 │ ─ client    │ and placement   │ ─ client    │                 │ ─ client    │
 │   (FUSE)    │ groups          │   (FUSE)    │                 │   (FUSE)    │
 └─────────────┘                 └─────────────┘                 └─────────────┘
```

- **One process per server.** Each process owns its drives (one core or worker per drive,
  thread-per-core later), hosts replicas of many metadata shards and placement groups, and can
  run the client.
- **Control plane:** a single small Raft group (5 voters, one per rack) holds only the
  **cluster map** (nodes, drives, failure domains, up/down/out state, weights, worker
  addresses) and the **shard map**. It changes rarely and is distributed as versioned
  **epochs** (incremental diffs).
- **Metadata:** **thousands of shards**, each a Raft group of 3–5 replicas on different racks.
  Every shard has its own leader, so the load spreads across the cluster.
- **Data:** files are split into chunks, chunks are grouped into **placement groups (PGs)**, and
  each PG is replicated across servers by **primary-ordered replication**. Metadata never
  appears on the data write path.
- **Transport:** UCX. Active Messages for RPCs, RMA (`get`/`put`) for bulk data. Shared memory on
  one host; RDMA (RC/UD/DC) or TCP between hosts. UCX picks.

## Identity

| ID | Lives in | Purpose |
|---|---|---|
| `ClusterId` | config + every drive's superblock | never use a drive from another cluster |
| `NodeId` | node config | routing, membership |
| `DriveId` | **on the drive** (superblock) | a drive moved to another server keeps its identity |
| `ShardId` | encoded in every `InodeId` | inode → shard is arithmetic, not a lookup |
| `PgId` | `hash(chunk) mod pg_count` | chunk → PG is arithmetic |
| `Epoch` | cluster map version | fencing: stale requests are rejected |
| Failure domain | config labels (`rack`, `host`) | replicas never share one |

All IDs are newtypes (one small macro generates them all), defined in a pure `types` crate.

## Files

### Semantics
- **Metadata is linearizable.** create, mkdir, rename, unlink and setattr are atomic and
  immediately visible to everyone. They go through Raft.
- **Data is close-to-open consistent** (like NFS): after a writer's `close()` or `fsync()`, any
  later `open()` sees the data. `fsync` is the durability barrier. Strict POSIX byte-level
  coherence could become an opt-in mode later.

### Namespace and metadata sharding
- `InodeId` (64-bit) = **shard bits plus a local counter**. New inodes are allocated on the
  parent directory's shard, for locality.
- A directory's entries live on its own shard. **Huge directories are hash-split** across
  shards.
- Shards **split and move** when they are large or hot, driven by the control plane.
- **Cross-shard operations** (rename across shards, hard links) use **two-phase commit with
  recoverable intents**, coordinated by one shard. They're rare, so they can be slower.
- The client caches the shard map and goes straight to the right shard leader. A
  `WrongShard`/`NotLeader { hint }` reply makes it refresh its cache and retry.

### Inode
`Inode { id, generation, kind, mode, uid, gid, size, nlink, atime/mtime/ctime, layout, inline }`

- `layout = { chunk_size (default 4 MiB), protection (Replicate(3) | ErasureCode(k, m)) }`.
  There is **no per-chunk location list**: placement is computed.
- `inline`: files ≤ 4 KiB are stored **in the inode** on the metadata shard and never touch
  the data path. That matters for millions of tiny files.
- Room is reserved for later features: snapshots (a generation/snapshot id), xattrs, ACLs.

### Data layout and placement
```text
file offset ─▶ chunk (inode, index) ─hash─▶ PG ─hierarchical HRW─▶ [drive@server, drive@server, drive@server]
                                            (fixed count,          (rack → host → drive, CRUSH-like;
                                             ~100 per drive)        never two replicas in one domain)
```
- Clients compute placement locally from the cached cluster map: no metadata hop per chunk.
- A map change moves only the affected PGs. Every data request carries the map `Epoch`, so
  stale ones are fenced off.
- Missing chunk = zeros, which gives sparse files for free.

### Write path (no metadata on it)
1. `open` on the inode's shard returns the layout and a **capability** (see Permissions).
2. For each chunk range, the client sends the write to the **PG primary**, with a header and
   capability; bulk bytes are pulled by RDMA `get` from the client's registered buffer.
3. The primary orders the write (a per-PG version), writes it to its drive's **journal**,
   replicates it to the other PG replicas, and acks the client once all replicas are durable.
4. File **size and mtime are updated lazily** on the metadata shard: at `fsync`, at `close`, or
   when the write lease expires. That's one metadata update per session, not per write.

### Read path (the smart client assembles the file)
1. `open` returns the layout and capability. Attributes and directory entries come from a
   **leased client cache**.
2. A pure `plan_read(layout, offset, len) -> Vec<ChunkRead>` maps the byte range to chunks
   and PG replicas.
3. Reads go **in parallel**, one replica per chunk (least loaded or closest), each filling its
   own disjoint `&mut` slice of the destination buffer. RDMA lets servers write straight into
   it. Chunks arrive in any order, but **offsets** make the result ordered.
4. Sequential readers trigger read-ahead.

### Delete and garbage collection
`unlink` removes the directory entry. An inode that is still open goes to an orphan list.
When it is no longer referenced, a background job deletes its chunks; it knows where they
are because placement is computed. Per-PG tombstones plus a sweep catch stragglers.

## Permissions

- **POSIX model:** uid, gid and mode bits, checked by the inode's metadata shard at `open`,
  `create`, `unlink` and so on. ACLs come later.
- **Capabilities:** a successful `open` returns a signed token, `{inode, generation, rights
  (r/w), expiry}`, signed with a cluster key distributed by the control plane. Data servers
  **verify it locally** with no metadata round trip. Expiry bounds how long a revoked
  permission can still be used.
- Client identity (which uid a request acts as) is authenticated at connection setup, with a
  per-node key at first.

## Redundancy and recovery

| Part | Redundancy | One server lost |
|---|---|---|
| Control plane | Raft, 5 voters on 5 racks | survives 2 losses; new leader in ~1 s |
| Metadata shard | Raft, 3–5 replicas on different racks | new leader for that shard; nothing lost |
| Data (PG) | 3x replication on different servers/racks (erasure coding later) | reads use the other replicas; rebuild starts |
| Client | no authoritative state | leases expire; it reopens |

**When a server dies:**
1. **Detect.** Gossip probes (SWIM-style) and Raft heartbeats notice; the control plane marks
   the server *suspect*, then *down*.
2. **Degraded.** Its PGs keep serving from the surviving replicas.
3. **Out.** After a **grace period (default 5 min, so reboots don't trigger rebuilds)**, the
   control plane publishes a new epoch. Placement picks new homes for the affected PGs, spread
   across many servers.
4. **Rebuild in parallel.** Many servers copy from many others, throttled so user traffic
   stays fast.
5. **Return.** A server that comes back *before* "out" catches up per PG (by version). After
   "out", its stale copies are discarded.

**When a drive dies:** the node reports it (I/O errors, checksum failures), and only that
drive's PGs rebuild. **Integrity:** per-4 KiB checksums on chunks, verified on read and by a
background scrubber that repairs from a healthy replica.

**Replication is not backup.** It copies mistakes too (`rm -rf`, bugs). Later: **snapshots**
(read-only point-in-time trees, sharing unchanged data) and streaming snapshots off-cluster.

## Exposure

| Layer | For | Order |
|---|---|---|
| `client` library (Rust, async) | our tools, tests, benchmarks | 1st |
| **FUSE mount** (`fuser`): `fastfs mount /mnt/fastfs` | every program, `yazi` included | 2nd |
| `fastfs` CLI (`stat`, `where`, `health`) | operators, scripts | alongside FUSE |
| NFS / S3 gateways | other machines and tools | maybe, much later |

## Core principle: sans-IO (functional core, imperative shell)

`raft`, the metadata state machine, `placement` and read/write planning are **pure and
deterministic**: no I/O, no clock, no randomness, no `.await`. Their shape is
`step(Input) -> Vec<Output>`, and async drivers execute the outputs. There are two drivers:
real (tokio + UCX + files) and **simulation** (seeded RNG, fake clock, faulty network, fake
disk that loses un-fsynced writes). Any failing seed replays exactly.

## Raft

The same Raft code runs the control plane and every metadata shard. A node hosts many groups
(**Multi-Raft**), with heartbeats and messages batched per peer node, not per group.

- Typestate roles: `ClusterNode<Follower|Candidate|Leader>`. Role-specific data lives in the
  state struct (`Leader { progress }`, `Candidate { votes }`), and transitions consume `self`.
- Runtime dispatch goes through `enum RaftNode { Follower(..), Candidate(..), Leader(..) }`;
  `step` is the only place transitions happen.
- `Committed<T>` is a proof token with a private constructor: the state machine only accepts
  committed entries.
- Order of work:
  1. election + **pre-vote**;
  2. replication/commit (the leader appends a no-op on election);
  3. persistence (fsync term, vote and entries **before** replying);
  4. ReadIndex reads;
  5. snapshots;
  6. membership change.
- Hand-written; Figure 2 of the Raft paper is the spec.
- State machines are a trait (Strategy): the control-plane map and the metadata shard
  implement it. **The leader fills in all nondeterminism** (new ids, timestamps) before
  proposing, so `apply` is bit-identical on every replica (the simulator checks hashes).
  Client sessions `(session_id, seq)` are deduplicated, which makes retries idempotent.

## WAL and on-drive formats

- **The Raft log is the metadata WAL.** It is stored in segment files (~64 MiB), with records
  `[len][crc32c][term][index][payload]`. Recovery truncates a torn tail; a record with a bad
  CRC or short length was never acked, so that's safe. A snapshot at index N lets segments
  below N be deleted. Hard state (`term`, `voted_for`) goes in a tiny file written atomically
  (tmp, fsync, rename, fsync dir).
- **Per drive:** a superblock (`ClusterId`, `DriveId`, format version), a journal for PG
  writes, and chunk storage behind a `ChunkStore` trait. The first backend is files on a
  normal filesystem (easy to inspect); raw NVMe via `io_uring`/`O_DIRECT` (or SPDK) comes
  later. A fake backend serves the simulator.

## Async and UCX

- tokio. Actors own their state and communicate over `mpsc`, with replies via `oneshot`. The
  public API is cloneable `Handle`s with plain `async fn` methods. The Observer pattern uses
  `watch` (latest value: cluster map, leader) and `broadcast` (events).
- Native `async fn` in traits; `-> impl Future + Send` where spawned; `dyn` only when needed.
- **`TransportActor`:** a dedicated thread owns the UCX worker (`!Send`) and receives via
  `progress_with` (zero-copy). Two modes: busy-poll (lowest latency) or event (sleep on
  `ucp_worker_get_efd`).
- **Messages are addressed by `NodeId`**, never by endpoint. A wire header on every message
  carries `{protocol version, source NodeId, service, kind}`.
- **Connections are lazy:** opened on first use and capped with an LRU (no full mesh at 10k).
  UCX's DC transport helps on Mellanox. Worker addresses come from the cluster map. With
  thread-per-core, a node publishes one address per worker.
- `Transport` trait: `UcxTransport` and `SimTransport`.
- Serialization: `bincode`/`postcard` for control messages; raw bytes for chunk payloads.

## Connections (decided in M2, measured)

- **TCP only for startup.** Nodes swap UCX *worker addresses* over a plain TCP socket (the
  `ucp_hello_world` pattern), close it, and create endpoints with
  `Endpoint::connect(worker, address, ..)`. UCX never opens a TCP connection, so Raft and data
  traffic run over shared memory or RDMA only. The `ucx` crate deliberately has no UCX
  listener/sockaddr API. At scale, addresses come from the cluster map, and joining needs
  only seed nodes.
- **`ErrorMode::None` on same-host endpoints.** `ErrorMode::Peer` makes UCX exclude shared
  memory, so same-host traffic drops to TCP (4.5 µs vs 0.08 µs one-way). Cross-host links can
  use `Peer` cheaply. Peer liveness is the application's job either way (gossip, heartbeats,
  RPC timeouts).
- A UCX worker progresses every interface any endpoint ever activated. An earlier UCX-listener
  bootstrap left TCP active, which cost an `epoll_wait` syscall per progress call. See
  `docs/PERF.md`.

## Logging

`logging::init(process, role)` in every binary; `tracing` everywhere. Levels:
- **error:** failed, can't proceed;
- **warn:** recoverable anomaly;
- **info:** lifecycle (start, listen, connect, bootstrap, results);
- **debug:** resource lifecycle and control details;
- **trace:** every message sent or received (peer, AM id, sizes).

The root span carries the process name and role. See `crates/logging/README.md`.

## UCX build

`scripts/build-ucx.sh [release|debug]` builds the submodule into `.ucx/<flavor>` once.
`ucx-sys/build.rs` only locates it: `UCX_DIR`, then `.ucx/release`, then pkg-config (system).
UCX is linked dynamically. The build includes verbs RDMA (RC/UD/DC, mlx5) as well as shared
memory and TCP.

## Test environments

| Setup | Transport | Tests |
|---|---|---|
| N processes, one host | shared memory | logic, fast iteration |
| N processes in **network namespaces** + `tc netem` | TCP | multi-host behavior, latency, loss, partitions |
| N **VMs** + **Soft-RoCE** (`rdma_rxe`) | **RDMA verbs** | the real cross-server path; per-VM kernels and disks; VM kill = power loss |
| Servers with RDMA NICs (or SR-IOV VFs into VMs) | hardware RDMA | production numbers |

UCX can't use shared memory between VMs (different kernels), but Soft-RoCE gives each VM a
software RDMA device, so our code runs through the same verbs path as a Mellanox NIC:
`modprobe rdma_rxe; rdma link add rxe0 type rxe netdev <nic>`.

Test layers:
1. Unit tests and `proptest` on the pure crates.
2. Deterministic simulation with invariants: at most one leader per term, committed entries
   never lost, replica state hashes match, every PG fully replicated after quiescence, client
   history linearizable.
3. Multi-process chaos (`launcher`): kill -9 or SIGSTOP random nodes, corrupt or delete
   "drives", verify checksums.
4. VM runs: power loss, real per-node disks, RDMA.

## Milestones (baby steps: each is one focused session)

| # | Step | Done when |
|---|---|---|
| M0–M2 ✅ | workspace, UCX build, safe sync wrapper, TCP bootstrap, logging | pingpong vs `ucx_perftest` |
| M3a ✅ | `ucx`: non-blocking send, background close, zero-copy `progress_with`, recoverable errors | 0.075 µs vs C 0.065 µs |
| **Transport** | | |
| M3b.1 | `types` crate: ID newtypes + versioned wire header (encode/decode) | round-trip + rejects bad versions (proptest) |
| M3b.2 | `transport` crate: `TransportActor` thread (busy-poll) + `TransportHandle` (tokio), messages by `NodeId` | async pingpong works; latency recorded |
| M3b.3 | event mode (`efd`/`arm`) | both modes benchmarked, CPU use compared |
| M3c.1 | `node` binary + config (`node_id`, seeds, drives, failure domain) + join via seeds, lazy connections | 3 local nodes exchange messages |
| M3c.2 | `launcher` (spawn N nodes, kill/restart) + heartbeat liveness via `watch` | kill -9 a node → others report it down |
| **Consensus** | | |
| M4.1 | sans-IO Raft: election + pre-vote; simulator skeleton | one leader per term over many seeds |
| M4.2 | log replication + commit | committed entries never lost (sim) |
| M4.3 | persistence rules + fake disk | crash/restart invariants (sim) |
| M4.4 | ReadIndex reads | linearizable reads (sim) |
| M4.5 | snapshots + log compaction | restart from snapshot (sim) |
| M4.6 | membership change | add/remove voters (sim) |
| M5 | WAL segments on real disk | kill -9 recovery |
| **Control plane & metadata** | | |
| M6.1 | control plane: cluster map + epochs, watched by nodes | map changes reach all nodes |
| M6.2 | metadata shard state machine (one shard, shard-aware ids): create/mkdir/lookup/getattr/readdir/unlink/rmdir/rename, inline small files | ops correct + deterministic (sim) |
| M6.3 | `client` library: metadata ops, routing, retries; `fastfs` CLI | `fastfs mkdir/ls/rm` against a local cluster |
| **Data** | | |
| M7.1 | per-drive store: superblock, journal, `ChunkStore` (files backend) | crash-safe chunk writes |
| M7.2 | placement: PGs + hierarchical HRW (pure) | balance + failure-domain properties (proptest) |
| M7.3 | PG primary replication write path + parallel read path + capabilities | write/read big files across nodes |
| M7.4 | RMA bulk transfer | zero-copy reads/writes measured |
| **Exposure** | | |
| M8 | FUSE mount | `yazi`, `cp`, `fio` work on `/mnt/fastfs` |
| **Resilience** | | |
| M9.1 | degraded mode + rebuild after the grace period | kill a node → data re-replicated |
| M9.2 | scrubbing, checksums, GC | corruption detected and repaired |
| M9.3 | VM + Soft-RoCE environment, chaos runs | passes on RDMA |
| **Scale** | | |
| M10 | multi-shard metadata (split/move, cross-shard 2PC), SWIM gossip, connection LRU, map diffs, erasure coding, snapshots, thread-per-core, raw NVMe | — (each becomes its own steps) |

Measured results and lessons live in `docs/PERF.md`.
