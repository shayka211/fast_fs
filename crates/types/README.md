# types

Shared vocabulary for every fast_fs crate: the ID types, and the versioned header at the
front of every node-to-node message. Pure code: no I/O, no UCX, no async. That's what lets
the Raft core, the metadata state machine and the simulator depend on it.

## IDs

Each ID is a **newtype**, a struct wrapping an integer. Mixing them up is then a compile
error (a `ShardId` can't be passed where a `NodeId` is expected). Construction and access are
explicit, with no silent conversions:

```rust
use types::{InodeId, NodeId, ShardId};

let n = NodeId::new(3);
assert_eq!(n.get(), 3);
assert_eq!(n.to_string(), "node-3");      // Display: logs and CLI
assert_eq!(format!("{n:?}"), "NodeId(3)"); // Debug

let ino = InodeId::new(ShardId::new(7), 42).expect("in range");
assert_eq!(ino.home_shard(), ShardId::new(7));
assert_eq!(ino.local(), 42);
```

| Type | Width | Meaning |
|---|---|---|
| `ClusterId` | u64 | the cluster; also stored on every drive, so a foreign drive is never used |
| `NodeId` | u32 | one server process; messages are addressed by it |
| `DriveId` | u64 | a physical drive; stored *on* the drive, so it survives moving servers |
| `ShardId` | u32 | a metadata shard (one Raft group) |
| `PgId` | u32 | a placement group: the unit of data replication and recovery |
| `Epoch` | u64 | cluster-map version; stale requests are rejected (fencing) |
| `InodeId` | u64 | `[home shard: 20 bits][local counter: 44 bits]` |

### `InodeId` and shards
The upper 20 bits name the shard the inode was **created on** (its *home*), and the lower
44 bits are a counter within that shard. So:
- creating an inode needs no global coordination, because each shard counts on its own;
- the home shard is plain arithmetic (`ino.home_shard()`), so there's no lookup table;
- inodes sort by home shard first, which is handy for range scans.

Shards will split and move as the cluster grows, so the shard that *currently* owns an
inode is found through the shard map (home shard → current location). The ID itself never
changes.

Limits: ~1M shards (2²⁰) and ~17.6 trillion inodes per shard (2⁴⁴). `InodeId::new` returns
`None` when out of range, rather than wrapping silently.

## Wire header

Every message between nodes starts with a `WireHeader`:

```text
offset  0      2        4           8        10     12            20
        magic  version  source      service  kind   request_id
        u16    u16      NodeId u32  u16      u16    u64          (little-endian)
```

| Field | Purpose |
|---|---|
| `magic` (`0xF5F5`) | rejects garbage or misrouted bytes immediately |
| `version` | protocol version, so rolling upgrades work (below) |
| `source` | the sending `NodeId`; UCX doesn't tell the receiver who sent a message |
| `service` | `Node`, `Raft`, `Meta` or `Data`: which subsystem handles it |
| `kind` | service-specific message type |
| `request_id` | pairs a response with its request; `0` = one-way |

It's 20 bytes, which fits UCX's inline header buffer (`ucx::HEADER_INLINE` = 32), so headers
never allocate. Bytes after the header belong to the service, and `decode` returns them.

```rust
use types::{NodeId, Service, WireHeader};

let h = WireHeader::new(NodeId::new(3), Service::Meta, /*kind*/ 7, /*request_id*/ 42);
let mut msg = h.encode().to_vec();
msg.extend_from_slice(b"service payload");

let (decoded, rest) = WireHeader::decode(&msg).unwrap();
assert_eq!(decoded, h);
assert_eq!(rest, b"service payload");
```

**Encode and decode are hand-written**, not derived through serde, so the byte layout is
explicit and can't change by accident. The `layout_is_stable` test pins the exact bytes: any
layout change has to be a deliberate version bump.

**Decoding never panics.** Malformed input is normal at the edge of a distributed system,
so `decode` returns a typed `HeaderError` (`TooShort`, `BadMagic`, `UnsupportedVersion`,
`UnknownService`). A property test feeds it random bytes to prove it never panics.

### Versioning and rolling upgrades
`PROTOCOL_VERSION` is what this build sends. `SUPPORTED_VERSIONS` is what it accepts. To
upgrade 10,000 servers without downtime:
1. Release N+1 *accepts* versions N and N+1 but still *sends* N.
2. Once every node runs N+1, switch it to sending N+1.
3. A later release drops support for N.

## Tests
- Unit tests: formatting, range limits, exact byte layout, each kind of bad input.
- Property tests (`proptest`): header round-trips with arbitrary trailing payload, every
  truncation is rejected, random bytes never panic, `InodeId` packing round-trips, and inode
  ordering groups by shard.
