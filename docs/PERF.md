# Profiling fast_fs with perf

This guide covers how `perf` works, the recipes we use, and a worked investigation: why our
shared-memory ping-pong started out 3.4x slower than UCX's C benchmark, and how we found it.

## How perf works (the short version)

Modern CPUs have **hardware performance counters (PMU)**: registers that count events such
as cycles, retired instructions, cache misses and branch mispredictions. `perf` uses them in
two ways:

1. **Counting (`perf stat`)**: start the counters, run the program, read the totals. It costs
   almost nothing and gives you ratios such as **IPC** (instructions per cycle). High IPC
   (around 2–4) means the CPU is busy doing work. Low IPC (around 1 or less) means it is
   stalled waiting on memory, mispredicted branches or the kernel.
2. **Sampling (`perf record` / `perf report`)**: program a counter to interrupt the CPU every
   N events (or N times a second). On each interrupt perf records the instruction pointer,
   and optionally the call stack. Functions that show up in many samples are where the time
   goes. It's statistical, so run long enough to collect thousands of samples.

Events can be **hardware** (`cycles`, `instructions`, `cache-misses`, `branch-misses`) or
**software** (`cpu-clock`, `task-clock`, `context-switches`, `page-faults`, which are kernel
timers and counters). The suffix `:u` means "user space only".

### Gotchas on this machine (CachyOS, AMD Ryzen 5600X)

- **`kernel.perf_event_paranoid = 2`** means unprivileged users can only measure user space.
  Always pass `-e <event>:u`. Plain `perf record` fails with "Disallow kernel profiling".
- **Skid on AMD:** sampling on `cycles:u` put about 95% of samples at `[unknown] [k] 0`. The
  counter-overflow interrupt arrives a few instructions late. If the CPU has entered the
  kernel (a syscall) by then, the address is hidden, so the hot spot is invisible. Use the
  timer-based **`cpu-clock:u`** event for sampling instead. It is precise enough and reports
  user addresses correctly.
- **Call stacks:** `--call-graph dwarf` didn't work at paranoid 2 here, so we used flat profiles
  (`--sort dso,sym`). To get stacks: `sudo sysctl kernel.perf_event_paranoid=1`, or build with
  `RUSTFLAGS="-C force-frame-pointers=yes"` and use `--call-graph fp`. The second only helps
  for our own code, because the UCX C libraries are built without frame pointers.
- **`strace` is not installed.** To count syscalls without it, compare user vs. system time
  (see the difference method below).

## Recipes

```sh
# Pin processes to fixed cores, always. Unpinned busy-pollers get migrated and the numbers
# turn to noise. On the 5600X, CPUs n and n+6 are SMT siblings of the same physical core,
# so 2 and 4 are separate cores.
taskset -c 2 ./target/release/pingpong server --listen 127.0.0.1:15000 &
taskset -c 4 perf stat -e cycles:u,instructions:u,cache-misses:u,branch-misses:u \
    ./target/release/pingpong client --connect 127.0.0.1:15000 --iters 2000000

# User vs. system time (perf stat prints both), e.g. to detect hidden syscalls
taskset -c 4 perf stat -e task-clock ./target/release/pingpong client ...

# Where does the time go? (flat profile, user space)
taskset -c 4 perf record -e cpu-clock:u -F 9999 -o cli.data ./target/release/pingpong client ...
perf report -i cli.data --stdio --sort dso,sym --percent-limit 1

# Interactive browser (arrow keys, Enter to zoom into a DSO or symbol, 'a' to annotate asm)
perf report -i cli.data

# Same flow for the C baseline (ucx_perftest's default port is 13337, so use -p to avoid
# clashing with our server)
.ucx/release/bin/ucx_perftest -t ucp_am_lat -p 15001 &
perf stat -e task-clock .ucx/release/bin/ucx_perftest 127.0.0.1 -p 15001 -t ucp_am_lat -n 2000000 -s 8
```

### The difference method (separating setup from steady state)

Totals include one-time setup (UCX opens devices, memory-maps shared segments, and so on).
Run with two iteration counts and subtract: the delta divided by the extra iterations is the
true per-iteration cost.

| | n=100k sys | n=4M sys | ⇒ kernel time per round trip |
|---|---|---|---|
| C `ucx_perftest` | 0.212 s | 0.218 s | ~0 ns |
| Rust (before fix) | 0.211 s | 0.624 s | **~105 ns** |

C's kernel time stays flat, so it is setup only. Ours grew with every iteration, meaning one
or more **syscalls per round trip** on a path that should never enter the kernel.

## Case study: "why is shared memory slower than C?" (M2)

Starting point: pinned, 8-byte Active Messages, one-way latency: **C 0.065 µs, Rust 0.221 µs**.
Both used the same UCX library and the same protocol (`UCX_PROTO_INFO=y` showed `short` over
`sysv/memory` for both), so the transport was not the cause.

**Step 1: `perf stat`, per round trip**

| | C | Rust |
|---|---|---|
| instructions | ~1,940 | ~1,270 |
| user cycles | ~710 | ~1,070 |
| IPC | 2.7 | **1.2** |
| branch misses | 1.8 | **7.1** |

We ran *fewer* instructions in *more* cycles, so the CPU was stalling rather than working.

**Step 2: user vs. system time.** Kernel time made up about 30% of the run and grew with the
iteration count (the difference method above). So we were doing syscalls in the hot loop.

**Step 3: `perf record -e cpu-clock:u`**

```
36.34%  libc.so.6        [.] 0x00000000000b5a22      <- syscall return site (unnamed)
15.26%  libc.so.6        [.] __sched_yield            <- setup barrier (C has it too)
 7.85%  libuct.so        [.] uct_mm_iface_progress    <- shared-memory polling: the real work
 3.47%  libc.so.6        [.] epoll_wait               <-+
 3.16%  libucs.so        [.] ucs_event_set_wait       <-+- TCP interface polled on
 2.71%  libuct.so        [.] uct_tcp_iface_progress   <-+  EVERY progress call
```

C's profile has no `epoll_wait` and no `uct_tcp_iface_progress` at all.

**Root cause.** A UCX worker progresses every transport interface that any endpoint has
activated. Our bootstrap connection (listener → TCP handshake) activated the **TCP**
interface on the same worker that later carried the shared-memory data. Closing the TCP
endpoint does not deactivate it, so every `worker.progress()` in the spin loop also called
`epoll_wait` (a syscall, ~100 ns) to check a socket we no longer used. That slowed the
polling loop, and with it the time to notice each incoming message.

So shared memory itself was never slow. We were paying for TCP polling on every iteration.

**First fix: two workers from one context.**
- A **bootstrap worker** owns the UCX listener and the TCP handshake, and is polled rarely.
- A **data worker** only ever gets direct worker-address endpoints, so it never activates
  TCP and its progress loop makes no syscalls.

That fixed the problem (0.221 → 0.087 µs), but it was a workaround.

**Final fix: don't let UCX touch TCP at all.** UCX's own `ucp_hello_world.c` and
`ucx_perftest` swap worker addresses over a *plain* TCP socket (out-of-band), then create
endpoints from those addresses. UCX never activates its TCP transport, so one worker is
enough. We switched to that and deleted the UCX listener code, the two-worker split and
the polling hack: **0.083 µs** with less code. The general lesson is to pick the bootstrap
mechanism that keeps the data worker clean, instead of cleaning up after it.

**Step 4: re-profile.** The top is now `uct_mm_iface_progress` (waiting for the peer, as in
C). The next item was **our own stopwatch**: `__vdso_clock_gettime` at 14%. Two `Instant::now()`
calls per round trip at ~20 ns each is significant when you're measuring 65 ns. C reads the
TSC directly (`ucs_get_time`), which is cheaper.

### Results (pinned, 8 B, one-way)

| | p50 | overall (wall ÷ iters) |
|---|---|---|
| C `ucx_perftest` | 0.065 µs | 0.070 µs |
| Rust, single worker (bootstrap TCP still polled) | 0.221 µs | — |
| Rust, two workers | 0.096 µs | 0.104 µs |
| Rust, two workers, `--overall-only` (no per-iteration clocks) | — | 0.087 µs |
| Rust, plain-TCP bootstrap, one worker, `--overall-only` (current) | — | **0.083 µs** |

**Remaining gap: about 16 ns per hop** of real messaging overhead. In the current profile the
Rust-side items are `WorkerInner::deliver`, `am_recv_cb` and `malloc`: each received message
is copied into a fresh `Vec`, pushed through a `RefCell<VecDeque>`, then freed. C receives
into a buffer it set up once. M3 will fix this:
- zero-copy receive (keep UCX's buffer with `UCP_AM_FLAG_PERSISTENT_DATA` and return
  `UCS_INPROGRESS`, then release it later);
- a pooled buffer allocator;
- a TSC-based clock for latency histograms.

## Rules of thumb we learned

1. Pin both sides before measuring anything.
2. Compare against the C baseline using the *same metric* (p50 vs. p50, overall vs. overall).
3. Low IPC plus growing system time means syscalls or stalls in the hot loop: find them first.
4. On AMD, sample with `cpu-clock:u`, not `cycles:u`.
5. Don't let the stopwatch become the workload: at sub-100 ns, clock reads matter.
6. Keep control-plane transports (TCP, connection setup) off the data worker. Best of all,
   exchange addresses out-of-band so UCX never opens a TCP connection.
