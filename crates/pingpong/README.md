# pingpong

A two-process UCX latency benchmark. One process (the **server**) echoes every message back
and the other (the **client**) measures how long each round trip takes.

## Why it exists

- **It validates the `ucx` wrapper end to end:** context → worker → endpoints → Active
  Messages, across two real processes.
- **It shows whether Rust adds overhead** by comparing against UCX's own C benchmark,
  `ucx_perftest`, under identical conditions.
- **It's the reference for how nodes connect.** fast_fs nodes will connect the same way: swap
  UCX worker addresses once over plain TCP, then talk only over the fast transport.
- **It catches performance regressions** in the transport layer as the code grows.

## Modes

| Subcommands | Path measured |
|---|---|
| `server` / `client` | **raw `ucx`**: one worker, synchronous loop, zero-copy receive. The baseline. |
| `transport-server` / `transport-client` | **through the `transport` crate**: async `request().await` addressed by `NodeId`, on the transport's own thread (on-core) |
| `transport-server` / `transport-client --cross-thread` | the same, but called from **another thread** via `RemoteHandle`: what FUSE threads would pay |

The rest of this section describes the raw mode. The transport modes use the same TCP
bootstrap, except the client announces its own `NodeId` in the hello
(`[node id u32][worker address]`), and requests and replies go through `Transport`.

## How it works

```
 client                                              server
 ──────                                              ──────
TcpListener on --listen
(accepted on a background thread)
 ── TCP connect ────────────────────────────────▶
 ── frame(client worker address) ───────────────▶
 ◀── token (u32) + frame(server worker address) ──
 (TCP socket closed on both sides)

 both create a UCX endpoint from the peer's worker address

 ── PING [token] + payload ═══ shared memory ═══▶
 ◀══ PONG payload ═════════════ shared memory ═══
   ... repeated --warmup + --iters times, one message in flight at a time
```

- **TCP is only for startup.** It's the same pattern as UCX's own `ucp_hello_world` and
  `ucx_perftest`. The TCP socket carries only the worker-address swap, a few hundred bytes,
  and is then closed. UCX never touches TCP, so it never activates its TCP transport, and
  the data phase runs entirely over shared memory (or RDMA between machines). The server
  keeps only its listening socket open, so new clients can join.
- **The bootstrap runs on its own thread** on the server. It hands each new client to the data
  loop through a channel, and the loop's check of that channel makes no syscall.
- **The token** identifies the client. UCX doesn't tell the receiver which endpoint a message
  came from, so every PING carries the 4-byte token the client was given during bootstrap.
  The fast_fs transport will do the same thing with node ids.
- **One bad client never kills the server.** Bootstrap and send errors are logged, and that
  client is dropped.

Bootstrap wire format (TCP), with `frame` = `[u32 LE length][bytes]`:

| Direction | Content |
|---|---|
| client → server | `frame(client worker address)` |
| server → client | `u32 LE token`, `frame(server worker address)` |

UCX Active Messages (data phase):

| AM id | Name | Direction | Header | Data |
|---|---|---|---|---|
| 1 | `PING` | client → server | token (u32 LE) | payload (`--size` bytes) |
| 2 | `PONG` | server → client | — | the same payload |

## Usage

Build in release mode. Debug builds measure the debug build, not UCX.

```sh
cargo build --release -p pingpong
```

Two terminals, with each process pinned to its own core (always pin when measuring):

```sh
taskset -c 2 ./target/release/pingpong server
taskset -c 4 ./target/release/pingpong client
```

### Options

| Flag | Applies to | Default | Meaning |
|---|---|---|---|
| `--err-mode peer\|none` | both | `none` | UCX dead-peer detection. `peer` makes UCX exclude shared memory, so data falls back to TCP (~50x slower here). |
| `--listen ADDR` | server | `127.0.0.1:13337` | TCP address for the bootstrap only |
| `--connect ADDR` | client | `127.0.0.1:13337` | The server's bootstrap address |
| `--iters N` | client | `1000000` | Measured round trips |
| `--warmup N` | client | `10000` | Unmeasured round trips first (finishes connection setup, warms caches) |
| `--size BYTES` | client | `8` | Payload size. Above a few KB UCX switches protocols (copy-in, then rendezvous/zero-copy), so latency jumps. |
| `--overall-only` | client | off | Skip the per-iteration clock reads. Gives only the `overall` number, the cleanest comparison with C (see below). |
| `--cross-thread` | `transport-client` | off | Run the benchmark on a different thread than the transport (through `RemoteHandle`). Pin to two cores, e.g. `taskset -c 4,5`. |

The transport modes always use `--err-mode none`. The client options (`--iters`,
`--warmup`, `--size`, `--overall-only`) work the same for `client` and `transport-client`.

Both sides must use the same `--err-mode`.

### Examples

```sh
# Standard run
pingpong client

# Purest latency number (no stopwatch overhead in the loop)
pingpong client --overall-only --iters 4000000

# Larger messages
pingpong client --size 65536 --iters 100000

# See the cost of UCX dead-peer detection (TCP instead of shared memory)
pingpong --err-mode peer server
pingpong --err-mode peer client
```

## Reading the output

```
INFO proc: pingpong: bootstrap complete, TCP closed connect=127.0.0.1:13337 token=0
INFO proc: pingpong: wire-up complete, measuring lanes=["sysv/memory", "posix/memory", "cma/memory"]
INFO proc: pingpong: results iters=1000000 size=8 one_way_p50_us="0.095" one_way_p99_us="0.131"
     one_way_p999_us="0.401" one_way_max_us="12.871" one_way_mean_us="0.102"
     one_way_overall_us="0.104" msg_rate="4800000"
```

- **`lanes`**: the transports the data endpoint uses. This is the first thing to check.
  - `sysv/memory`, `posix/memory`: shared-memory FIFOs (small messages).
  - `cma/memory`: kernel cross-memory attach (large zero-copy transfers).
  - `tcp/<nic>` means you are **not** on shared memory: most likely `--err-mode peer`.
- **`one_way_*_us`**: round-trip time ÷ 2, in microseconds, the same convention as
  `ucx_perftest`.
  - `p50` is the typical message, `p99`/`p999` the tail, `max` the worst single message (often
    an OS interrupt or scheduler hiccup).
  - `mean` is the average of the per-iteration samples.
- **`one_way_overall_us`**: total measured wall time ÷ iterations ÷ 2. It includes all loop
  overhead but needs no per-iteration clocks. Compare this with C's `overall` column. With
  `--overall-only` the percentile fields print `0`, because no samples were taken.
- **`msg_rate`**: round trips per second (one message in flight at a time, so it's about
  1 / RTT).

Typical numbers on the dev box (Ryzen 5600X, pinned to cores 2 and 4, 8 B, fresh server):

| Setup | One-way overall |
|---|---|
| C `ucx_perftest` | 0.065 µs |
| `pingpong client --overall-only` | 0.075 µs |
| `pingpong client` (with percentiles) | 0.090 µs (p50 0.090 µs) |
| `--err-mode peer` (TCP) | ~4.5 µs |

Transport modes, measured on cores 3 and 4 (cross-thread client on 4 and 5). The raw row is
repeated as a same-core reference:

| Setup | p50 | overall |
|---|---|---|
| `client` (raw) | 0.095 µs | 0.10 µs |
| `transport-client` (on-core) | 0.185 µs | 0.20 µs |
| `transport-client --cross-thread` | 1.44 µs | 1.6 µs |

Both sides receive with `Worker::progress_with`, a closure that sees each message's bytes
*borrowed* from UCX during progress (no copy, no allocation). The server copies pings into a
reused scratch buffer and replies after `progress_with` returns, because blocking sends
aren't allowed inside the receive callback. How we got here, and why UCX's own "zero-copy"
receive was slower, is in [`docs/PERF.md`](../../docs/PERF.md).

**Restart the server between benchmark runs.** In the default `--err-mode none`, UCX doesn't
report that a client left, so a long-lived server keeps endpoints to every past client, and
later runs measure ~15% slower. fast_fs nodes will detect dead peers with heartbeats (M3).

## Comparing with C (`ucx_perftest`)

`ucx_perftest` is built along with UCX in `.ucx/release/bin/`. Its default port is also
**13337**, so give one of the two benchmarks a different port.

```sh
taskset -c 2 .ucx/release/bin/ucx_perftest -t ucp_am_lat -p 15000 &
taskset -c 4 .ucx/release/bin/ucx_perftest 127.0.0.1 -p 15000 -t ucp_am_lat -n 1000000 -s 8
#   columns: ... latency 50%ile | average | overall | ...   (one-way µs)
```

To compare with dead-peer detection on, use `-epeer` (no space; `-e peer` is parsed
differently).

## Debugging

The logging setup is shared with the rest of the project (see the root `CLAUDE.md`):

```sh
RUST_LOG=debug pingpong client         # endpoint lifecycle, bootstrap, progress
RUST_LOG=info,ucx=trace pingpong server # every AM sent/received (slows the benchmark!)
UCX_PROTO_INFO=y pingpong client       # which UCX protocol/transport per message size
UCX_LOG_LEVEL=debug pingpong client    # UCX's internal logs
ss -tanp | grep pingpong               # confirm: no ESTAB TCP sockets during the data phase
```

Profiling with `perf` is covered in [`docs/PERF.md`](../../docs/PERF.md).

## Troubleshooting

| Symptom | Cause |
|---|---|
| `bind 127.0.0.1:13337` … `Address already in use` | Another process holds the port (often `ucx_perftest`, which also defaults to 13337). Use `--listen`/`--connect` with another port. |
| `bootstrap with …` … `Connection refused` / timed out | Server not running, or the wrong address |
| `lanes` shows `tcp/...` | Running with `--err-mode peer`, which excludes shared memory |
| Latency much higher than the table | Not pinned, debug build, a long-lived server (see above), CPU power-saving, or another busy process on those cores |
| `error while loading shared libraries: libucp.so.0` | UCX not built: run `scripts/build-ucx.sh release` |



UCX_PROTO_INFO=y pingpong client
❯ UCX_PROTO_INFO=y taskset -c 4 ./target/release/pingpong client --size 65536 --iters 100000
2026-11-18T16:43:29.069334Z  INFO main proc: pingpong: starting ucx=1.24.0 name="pingpong-client" role="client"
2026-11-18T16:43:29.077850Z  INFO main proc: ucx::endpoint: endpoint created peer=127.0.0.1:13337 mode=None name="pingpong-client" role="client"
2026-11-18T16:43:29.077860Z  INFO main proc: pingpong: bootstrap complete, TCP closed connect=127.0.0.1:13337 token=2 name="pingpong-client" role="client"
2026-11-18T16:43:29.077960Z  INFO main proc: pingpong: running peer="127.0.0.1:13337" iters=100000 warmup=10000 size=65536 name="pingpong-client" role="client"
[1795020209.078131] [cachyos-x8664:21725:0]   +--------+-------------+-----------------------------------+
[1795020209.078131] [cachyos-x8664:21725:0]   | ucp_context_0 intra-node cfg#0                           |
[1795020209.078131] [cachyos-x8664:21725:0]   | remote memory write by ucp_put* from host memory to host |
[1795020209.078131] [cachyos-x8664:21725:0]   +--------+-------------+-----------------------------------+
[1795020209.078131] [cachyos-x8664:21725:0]   | Range  | Description |              Config               |
[1795020209.078131] [cachyos-x8664:21725:0]   +--------+-------------+-----------------------------------+
[1795020209.078131] [cachyos-x8664:21725:0]   | 0..inf | zero-copy   | cma/memory                        |
[1795020209.078131] [cachyos-x8664:21725:0]   +--------+-------------+-----------------------------------+
[1795020209.078291] [cachyos-x8664:21725:0]   +-----------+-------------------------------------------+-------------+
[1795020209.078291] [cachyos-x8664:21725:0]   | ucp_context_0 intra-node cfg#0                                      |
[1795020209.078291] [cachyos-x8664:21725:0]   | active message by ucp_am_send* from host memory                     |
[1795020209.078291] [cachyos-x8664:21725:0]   +-----------+-------------------------------------------+-------------+
[1795020209.078291] [cachyos-x8664:21725:0]   |   Range   |                Description                |   Config    |
[1795020209.078291] [cachyos-x8664:21725:0]   +-----------+-------------------------------------------+-------------+
[1795020209.078291] [cachyos-x8664:21725:0]   |     0..92 | short                                     | sysv/memory |
[1795020209.078291] [cachyos-x8664:21725:0]   |  93..5028 | copy-in                                   | sysv/memory |
[1795020209.078291] [cachyos-x8664:21725:0]   | 5029..inf | (?) rendezvous zero-copy read from remote | cma/memory  |
[1795020209.078291] [cachyos-x8664:21725:0]   +-----------+-------------------------------------------+-------------+
[1795020209.078361] [cachyos-x8664:21725:0]   +--------------+-------------------------------------------+-------------+
[1795020209.078361] [cachyos-x8664:21725:0]   | ucp_context_0 intra-node cfg#0                                         |
[1795020209.078361] [cachyos-x8664:21725:0]   | active message by ucp_am_send*(fast-completion) from host memory       |
[1795020209.078361] [cachyos-x8664:21725:0]   +--------------+-------------------------------------------+-------------+
[1795020209.078361] [cachyos-x8664:21725:0]   |    Range     |                Description                |   Config    |
[1795020209.078361] [cachyos-x8664:21725:0]   +--------------+-------------------------------------------+-------------+
[1795020209.078361] [cachyos-x8664:21725:0]   |        0..92 | short                                     | sysv/memory |
[1795020209.078361] [cachyos-x8664:21725:0]   |     93..8248 | copy-in                                   | sysv/memory |
[1795020209.078361] [cachyos-x8664:21725:0]   | 8249..262143 | multi-frag copy-in                        | sysv/memory |
[1795020209.078361] [cachyos-x8664:21725:0]   |    256K..inf | (?) rendezvous zero-copy read from remote | cma/memory  |
[1795020209.078361] [cachyos-x8664:21725:0]   +--------------+-------------------------------------------+-------------+
[1795020209.078500] [cachyos-x8664:21725:0]   +-----------+-------------------------------------------+-------------+
[1795020209.078500] [cachyos-x8664:21725:0]   | ucp_context_0 intra-node cfg#0                                      |
[1795020209.078500] [cachyos-x8664:21725:0]   | active message by ucp_am_send*(multi) from host memory              |
[1795020209.078500] [cachyos-x8664:21725:0]   +-----------+-------------------------------------------+-------------+
[1795020209.078500] [cachyos-x8664:21725:0]   |   Range   |                Description                |   Config    |
[1795020209.078500] [cachyos-x8664:21725:0]   +-----------+-------------------------------------------+-------------+
[1795020209.078500] [cachyos-x8664:21725:0]   |     0..92 | short                                     | sysv/memory |
[1795020209.078500] [cachyos-x8664:21725:0]   |  93..6030 | copy-in                                   | sysv/memory |
[1795020209.078500] [cachyos-x8664:21725:0]   | 6031..inf | (?) rendezvous zero-copy read from remote | cma/memory  |
[1795020209.078500] [cachyos-x8664:21725:0]   +-----------+-------------------------------------------+-------------+
[1795020209.078566] [cachyos-x8664:21725:0]   +-----------+-------------------------------------------+-------------+
[1795020209.078566] [cachyos-x8664:21725:0]   | ucp_context_0 intra-node cfg#0                                      |
[1795020209.078566] [cachyos-x8664:21725:0]   | active message by ucp_am_send* with reply flag from host memory     |
[1795020209.078566] [cachyos-x8664:21725:0]   +-----------+-------------------------------------------+-------------+
[1795020209.078566] [cachyos-x8664:21725:0]   |   Range   |                Description                |   Config    |
[1795020209.078566] [cachyos-x8664:21725:0]   +-----------+-------------------------------------------+-------------+
[1795020209.078566] [cachyos-x8664:21725:0]   |     0..84 | short                                     | sysv/memory |
[1795020209.078566] [cachyos-x8664:21725:0]   |  85..5028 | copy-in                                   | sysv/memory |
[1795020209.078566] [cachyos-x8664:21725:0]   | 5029..inf | (?) rendezvous zero-copy read from remote | cma/memory  |
[1795020209.078566] [cachyos-x8664:21725:0]   +-----------+-------------------------------------------+-------------+
[1795020209.078624] [cachyos-x8664:21725:0]   +--------------+-------------------------------------------+-----------------------+
[1795020209.078624] [cachyos-x8664:21725:0]   | ucp_context_0 intra-node cfg#0                                                   |
[1795020209.078624] [cachyos-x8664:21725:0]   | active message by ucp_am_send* with reply flag(fast-completion) from host memory |
[1795020209.078624] [cachyos-x8664:21725:0]   +--------------+-------------------------------------------+-----------------------+
[1795020209.078624] [cachyos-x8664:21725:0]   |    Range     |                Description                |        Config         |
[1795020209.078624] [cachyos-x8664:21725:0]   +--------------+-------------------------------------------+-----------------------+
[1795020209.078624] [cachyos-x8664:21725:0]   |        0..84 | short                                     | sysv/memory           |
[1795020209.078624] [cachyos-x8664:21725:0]   |     85..8240 | copy-in                                   | sysv/memory           |
[1795020209.078624] [cachyos-x8664:21725:0]   | 8241..262143 | multi-frag copy-in                        | sysv/memory           |
[1795020209.078624] [cachyos-x8664:21725:0]   |    256K..inf | (?) rendezvous zero-copy read from remote | cma/memory            |
[1795020209.078624] [cachyos-x8664:21725:0]   +--------------+-------------------------------------------+-----------------------+
[1795020209.078694] [cachyos-x8664:21725:0]   +-----------+-------------------------------------------+----------------+
[1795020209.078694] [cachyos-x8664:21725:0]   | ucp_context_0 intra-node cfg#0                                         |
[1795020209.078694] [cachyos-x8664:21725:0]   | active message by ucp_am_send* with reply flag(multi) from host memory |
[1795020209.078694] [cachyos-x8664:21725:0]   +-----------+-------------------------------------------+----------------+
[1795020209.078694] [cachyos-x8664:21725:0]   |   Range   |                Description                |     Config     |
[1795020209.078694] [cachyos-x8664:21725:0]   +-----------+-------------------------------------------+----------------+
[1795020209.078694] [cachyos-x8664:21725:0]   |     0..84 | short                                     | sysv/memory    |
[1795020209.078694] [cachyos-x8664:21725:0]   |  85..6030 | copy-in                                   | sysv/memory    |
[1795020209.078694] [cachyos-x8664:21725:0]   | 6031..inf | (?) rendezvous zero-copy read from remote | cma/memory     |
[1795020209.078694] [cachyos-x8664:21725:0]   +-----------+-------------------------------------------+----------------+
2026-11-18T16:43:29.199737Z  INFO main proc: pingpong: wire-up complete, measuring lanes=["sysv/memory", "posix/memory", "cma/memory"] name="pingpong-client" role="client"
2026-11-18T16:43:30.565897Z  INFO main proc: pingpong: results iters=100000 size=65536 one_way_p50_us="6.779" one_way_p99_us="8.752" one_way_p999_us="12.303" one_way_max_us="96.064" one_way_mean_us="6.831" one_way_overall_us="6.831" msg_rate="73200" name="pingpong-client" role="client"