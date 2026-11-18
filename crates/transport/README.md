# transport

Node-to-node messaging for fast_fs. Messages are addressed by **`NodeId`**, routed to a
**`Service`** handler (`Node`, `Raft`, `Meta`, `Data`), and can be one-way or
**request/response** with `async`/`.await`. It's built on the `ucx` crate (shared memory /
RDMA / TCP) and the versioned `WireHeader` from `types`.

## The model: thread-per-core

```text
core thread (tokio current-thread runtime + LocalSet)
├─ Transport        ── owns one UCX worker; !Send, never leaves this thread
├─ run() task       ── the progress driver: polls UCX, delivers messages
├─ service handlers ── called synchronously, zero-copy, inside the driver's poll
└─ your async tasks ── call transport.send / request directly: no thread hop

other threads ──RemoteHandle (Send)──channel──▶ executed by the driver (costs a hop)
```

A `Transport` is **`!Send` on purpose**: it owns a UCX worker, which must stay on one
thread. Code on that thread uses it directly, at the full speed of the core. Code on other
threads (FUSE, CLI) gets a `RemoteHandle`, a `Send` proxy whose calls cross a channel. The
compiler makes it obvious which one you hold, and the hop is paid only where it's needed.
This is the shared-nothing design used by ScyllaDB/Seastar and Glommio.

## Usage

```rust,no_run
use std::time::Duration;
use transport::{Incoming, Transport};
use types::{NodeId, Service};

# async fn example(peer_address: Vec<u8>) -> transport::Result<()> {
let ctx = ucx::Context::new()?;
let t = Transport::builder(NodeId::new(1))
    // Every Meta request is answered with its payload reversed.
    .route(Service::Meta, |t: &Transport, m: Incoming<'_>| {
        let reversed: Vec<u8> = m.payload.iter().rev().copied().collect();
        let _ = t.reply(&m.header, 0, reversed);
    })
    .build(&ctx)?;
t.add_peer(NodeId::new(2), &peer_address)?; // worker address, swapped out-of-band

// The driver must run on this thread (inside a LocalSet).
tokio::task::spawn_local({ let t = t.clone(); async move { t.run().await } });

t.send(NodeId::new(2), Service::Node, /*kind*/ 1, b"hello".to_vec())?;          // one-way
let reply = tokio::time::timeout(Duration::from_secs(1),                         // always use
    t.request(NodeId::new(2), Service::Meta, 1, b"abc".to_vec())).await;         // a deadline
# let _ = reply;
t.stop();
# Ok(())
# }
```

| Call | What it does |
|---|---|
| `Transport::builder(node).route(service, handler)...build(&ctx)` | Builder: every route exists before any message can arrive |
| `address()` / `add_peer(node, address)` / `remove_peer(node)` | the peer table, `NodeId → endpoint` |
| `send(to, service, kind, payload)` | one-way message, non-blocking |
| `request(to, service, kind, payload).await` | request/response, matched by `request_id` |
| `reply(&request_header, kind, payload)` | answer a request (inside a handler, or later) |
| `run().await` / `stop()` | the progress driver |
| `remote()` | a `Send` `RemoteHandle` for other threads |
| `peer_transports(node)` | which UCX lanes reach that peer (`sysv/memory`, ...) |
| `pending_requests()` | in-flight requests (diagnostics) |

Payloads are anything implementing `ucx::SendBuf` (`Vec<u8>`, `Box<[u8]>`,
`&'static [u8]`). The payload is owned until UCX is done with it, so no copy is needed.

## Handlers

A handler (`impl Handler`, or any closure `|t: &Transport, m: Incoming<'_>|`) is called
**synchronously inside the driver's UCX poll**. `m.payload` is borrowed straight from UCX's
buffer, with no copy. That fits the sans-IO design: a Raft group's `step(msg)` is just a
function call. Rules:
- Keep it short: it runs inside the hot loop.
- `send` and `reply` are allowed (they're non-blocking).
- For async follow-up work, `tokio::task::spawn_local` a task.
- Copy out whatever bytes must outlive the call.
- Calling `progress()` from a handler returns an error (UCX forbids re-entering progress),
  and a panic propagates out of `run()` as a normal, catchable panic. Neither corrupts the
  transport.

## Requests, deadlines and cancellation

`request` stores a one-shot slot under a fresh `request_id`, sends, and awaits the reply.
The slot is removed by an RAII guard when the future finishes **or is dropped**. Wrapping a
request in `tokio::time::timeout` is therefore safe: a timed-out request leaves nothing
behind, and a late reply is ignored.

**Always use a deadline.** A message can be lost: the peer may be gone, have no route for
that service, or not know the sender yet. A request without a timeout can then wait
forever. We hit exactly that in the benchmark: the server registered a new client a moment
after the client's first request arrived. `stop()` fails all pending requests with
`Error::Closed`.

## The driver and its yield policy

`run()` loops: execute remote commands, poll UCX once, and then:
- **yield to tokio immediately** if the poll delivered something or commands ran, so woken
  tasks (waiting requests) continue right away;
- otherwise **spin again without yielding**, up to 64 empty polls, then yield once so timers
  and other tasks still get turns.

Yielding on every spin was expensive. Each yield is a scheduler tick plus a timer-driver
clock read, which was 22% of on-core time in the profile. With this policy, on-core latency
went from 0.29 to 0.20 µs. It's still a busy-poll: one core at 100%. An event mode that
sleeps on the worker's file descriptor when idle is the next step (M3b.3).

## Performance

Same host, 8 B payload, one-way, pinned cores (from `pingpong`):

| Path | p50 | overall |
|---|---|---|
| raw `ucx` (sync loop, no transport) | 0.095 µs | 0.10 µs |
| **on-core** `request().await` | **0.185 µs** | **0.20 µs** |
| cross-thread `RemoteHandle::request` | 1.44 µs | 1.6 µs |

The on-core overhead (~0.1 µs per hop) is per-request bookkeeping: a `oneshot` allocation, a
task wakeup through the scheduler, and header encode/decode. The cross-thread cost is the
two channel hops, plus waking the caller's sleeping thread. That's the data behind
thread-per-core: keep hot callers on the owning core. Details: `docs/PERF.md`.

## Logging (target `transport`)

`info` transport created, driver started/stopped · `debug` peers added/replaced, late
responses · `warn` bad headers, no route for a service, failed remote sends · `trace` every
send and receive (peer, service, kind, request_id, length).

## Not yet

- **`Transport` trait.** It will be extracted in M4.1, when the deterministic simulated
  network becomes the second implementation. Designing it around one implementation would
  likely get it wrong.
- **Event mode** (sleep when idle): M3b.3.
- **In-band joining** (a node announces itself in its first message) and liveness: M3c.
- Several cores per node (one transport per core), cross-core routing, connection LRU.

## Layout

| File | Contents |
|---|---|
| `transport.rs` | `Transport`, builder, routing, requests, the driver |
| `handler.rs` | `Handler`, `Incoming`, `Reply` |
| `remote.rs` | `RemoteHandle` and its commands |
| `error.rs` | `Error` |
| `tests.rs` | two-node tests, each node on its own thread |
