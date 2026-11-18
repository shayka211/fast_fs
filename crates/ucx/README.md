# ucx

A safe Rust wrapper over UCX's `ucp` API. This crate is how fast_fs moves bytes between
processes: shared memory on the same host, RDMA or TCP between hosts. UCX picks the
transport. All of the project's UCX `unsafe` code lives here (and in the generated
`ucx-sys` bindings beneath it), so the code above it is safe Rust.

What it adds on top of the raw bindings:

| Raw UCX (C) | This crate |
|---|---|
| `goto err_*` cleanup chains, manual destroy order | RAII: dropping a value releases it, and `Rc`/`Arc` keep parents alive |
| `ucs_status_t` codes, `UCS_PTR_IS_ERR` macros | `Result<_, ucx::Error>` |
| `void *arg` callbacks | typed closures and stable internal state |
| buffer lifetime rules in the docs | enforced by ownership (`SendBuf`, `SendRequest`, borrowed `AmView`) |
| "don't call progress from a callback" | an `Error::Reentrant` you can handle |

Scope today: Active Messages (small RPC-style messages). RMA (`put`/`get` for bulk file
data) comes later.

## The types

| Type | What it is |
|---|---|
| `Context` | The process-wide UCX instance (`Arc<Context>`). Create one per process. |
| `Worker` | A progress engine plus its message handlers. **Single-threaded and `!Send`**: create it on the thread that uses it. |
| `Endpoint` | A connection to one peer, created from the peer's *worker address*. |
| `ErrorMode` | `None` (default for data: all transports, including shared memory) or `Peer` (UCX detects dead peers, but shared memory is excluded). |
| `AmView<'a>` | A received message whose header and data are **borrowed** from UCX, valid during `progress_with`. |
| `AmMessage` | A received message that owns its bytes (small parts stored inline). |
| `SendRequest<B>` | A non-blocking send in flight. It owns the payload `B` until UCX is done with it. |
| `SendBuf` | Payload types safe to hand to a non-blocking send (`Vec<u8>`, `Box<[u8]>`, `&'static [u8]`). |
| `Error` | `Ucx { op, status }` or `Reentrant { op }`. Both are recoverable. |

Ownership:

```text
Arc<Context> ──▶ Worker (one thread) ──▶ Endpoint, SendRequest, ...
                  each child holds its parent alive; dropping releases the C resource
```

## Connecting

Endpoints are created from a peer's **worker address**, an opaque blob of a few hundred
bytes from `Worker::address()`. The two processes swap addresses *out-of-band*: plain TCP at
bootstrap (see `pingpong`'s `bootstrap.rs`), later cluster config or Raft. On purpose, there
is no UCX listener/sockaddr API here. With it, UCX activates its own TCP transport, and the
data worker then pays a syscall on every progress call (see `docs/PERF.md`).

```rust,no_run
use std::time::Duration;
use ucx::{Context, Endpoint, ErrorMode, Worker};

# fn main() -> ucx::Result<()> {
# let peer_address = Vec::<u8>::new();
let ctx = Context::new()?;
let worker = Worker::new(&ctx)?;
worker.register_am(1)?;                       // receive Active Messages with id 1
let my_address = worker.address()?;           // give this to the peer, out-of-band
let ep = Endpoint::connect(&worker, &peer_address, "node-2", ErrorMode::None)?;

ep.send_blocking(1, b"hdr", b"hello")?;       // borrowed data
drop(ep.send(1, &[], vec![7u8; 1 << 20])?);   // owned data, non-blocking, fire-and-forget

worker.progress_with(|msg| {                  // zero-copy receive
    println!("AM {}: {} bytes", msg.id, msg.data.len());
})?;
worker.drain(Duration::from_secs(1))?;        // before shutdown: finish background work
# let _ = my_address;
# Ok(())
# }
```

## Sending

| | `send_blocking(id, header, &data)` | `send(id, header, data)` |
|---|---|---|
| Payload | borrowed `&[u8]` | owned (`SendBuf`) |
| Returns | when UCX no longer needs `data` | right away, with a `SendRequest` |
| Inside a receive closure | `Err(Reentrant)`, nothing sent | allowed |
| Use for | simple code, reused buffers | hot paths, large payloads, sending from callbacks |

A `SendRequest` can be polled (`poll`), waited on (`wait`), or **dropped**. A dropped request
that is still in flight is adopted by the worker, which keeps the payload alive and frees
both when UCX completes it. Fire-and-forget is safe: errors are then only logged.
Headers are copied by UCX during the call, so only the payload needs to stay alive.

## Receiving

Call `register_am(id)` once per message id, then pick one of two modes:

- **`worker.progress_with(|msg: AmView| ...)`**: the fast path. Your closure runs *during*
  progress, with `msg.header` and `msg.data` borrowed straight from UCX's buffers: no copy,
  no allocation. Copy out whatever must outlive the call (`msg.to_message()`, or into your
  own buffer).
- **`worker.progress()` then `worker.try_recv()`**: convenient. Each message is copied into an
  owned `AmMessage` and queued.

Both go through the same dispatch point. Large messages (rendezvous) are fetched into a
buffer first, then delivered the same way.

### Rules inside a receive closure

UCX forbids re-entering progress from a receive callback. Inside `progress_with`:
- allowed: non-blocking `Endpoint::send`, reading and copying the message, your own logic;
- refused with `Error::Reentrant`: `progress`, `progress_with`, `drain`, `send_blocking`,
  `SendRequest::wait`. Nothing happens, and you can handle the error.

### Panics are recoverable

A panic in your closure would normally abort the process, because Rust can't unwind
through UCX's C frames. Instead, the panic is caught at the C boundary, progress completes
normally, and the panic is re-raised from `progress_with` as an ordinary Rust panic. You can
catch it with `std::panic::catch_unwind`, and the worker stays usable. Messages that arrive
after the panic within the same call are queued for `try_recv` rather than lost.

## Shutdown and background work

Dropping an `Endpoint` starts a graceful close and returns immediately. Dropped in-flight
sends continue as well. Both finish during later `progress` calls. Before exiting, call
`worker.drain(timeout)` to wait for them. `Worker`'s own `Drop` drains for up to 100 ms.

## Threading

One `Worker` per thread, never shared (`!Send` is enforced by the compiler). `Context` is
`Send + Sync`, and many threads can create workers from it. Since `send_blocking`
progresses only its own worker, a test that drives both peers from one thread can
deadlock on the first send. Give each side its own thread, as real nodes do (see
`src/tests.rs`).

## Performance

Same-host, 8-byte Active Messages, one-way: about **0.075 µs**, vs 0.065 µs for UCX's own
C benchmark. How we measured it, and why UCX's "zero-copy" receive flag turned out slower,
is in `docs/PERF.md`. The `pingpong` crate is the benchmark.

## Logging

Target `ucx`: `info` endpoint created / peer disconnected · `debug` resource lifecycle
(context, worker, close) · `trace` every message sent and received (peer, id, sizes) · `warn`
background request failures. UCX's own internal logs: `UCX_LOG_LEVEL=debug`; which protocol
is used per message size: `UCX_PROTO_INFO=y`.

## Layout

| File | Contents |
|---|---|
| `context.rs` | `Context` |
| `worker.rs` | `Worker`: progress, dispatch, receive callbacks, background requests |
| `endpoint.rs` | `Endpoint`, `ErrorMode`, sending, background close |
| `request.rs` | `SendRequest`, `SendBuf`, orphaned requests |
| `message.rs` | `AmView`, `AmMessage`, `SmallBytes` |
| `status.rs` | `Status`, `Error`, decoding `ucs_status_ptr_t` |
| `tests.rs` | two-thread integration tests |
