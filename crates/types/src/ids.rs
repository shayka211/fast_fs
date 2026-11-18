//! Identifier newtypes. Each wraps an integer so IDs of different kinds can't be mixed up
//! (passing a `ShardId` where a `NodeId` is expected is a compile error).

use std::fmt;

/// Defines an ID newtype: `Copy`, ordered, hashable, `Debug` as `Name(3)`, `Display` as
/// `prefix-3`. Construction and access are explicit (`new`, `get`): no silent conversions.
macro_rules! id {
    ($(#[$doc:meta])* $name:ident($inner:ty), $prefix:literal) => {
        $(#[$doc])*
        #[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
        pub struct $name($inner);

        impl $name {
            pub const fn new(raw: $inner) -> Self {
                Self(raw)
            }

            pub const fn get(self) -> $inner {
                self.0
            }
        }

        impl fmt::Debug for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, concat!(stringify!($name), "({})"), self.0)
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, concat!($prefix, "-{}"), self.0)
            }
        }
    };
}

id!(
    /// A cluster. Stored in config and in every drive's superblock, so a drive from another
    /// cluster is never used by mistake.
    ClusterId(u64), "cluster"
);
id!(
    /// A server process (one per server). Messages are addressed by `NodeId`.
    NodeId(u32), "node"
);
id!(
    /// A physical drive. Stored on the drive itself, so it survives moving to another server.
    DriveId(u64), "drive"
);
id!(
    /// A metadata shard (one Raft group).
    ShardId(u32), "shard"
);
id!(
    /// A placement group: the unit of data replication and recovery.
    PgId(u32), "pg"
);
id!(
    /// Cluster-map version. Requests carry it; stale ones are rejected (fencing).
    Epoch(u64), "epoch"
);

/// An inode: `[ home shard : 20 bits | local counter : 44 bits ]`.
///
/// The home shard is where the inode was created. Shards may later split or move, so the
/// *current* owner is found through the shard map (home shard → current location); the ID
/// itself never changes, so files keep their identity across moves.
/// 20 bits allow ~1M shards; 44 bits allow ~17.6 trillion inodes per shard.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct InodeId(u64);

impl InodeId {
    pub const SHARD_BITS: u32 = 20;
    pub const LOCAL_BITS: u32 = 64 - Self::SHARD_BITS;
    pub const MAX_SHARD: u32 = (1 << Self::SHARD_BITS) - 1;
    pub const MAX_LOCAL: u64 = (1 << Self::LOCAL_BITS) - 1;

    /// Packs a home shard and a per-shard counter. `None` if either is out of range.
    pub const fn new(shard: ShardId, local: u64) -> Option<Self> {
        if shard.get() > Self::MAX_SHARD || local > Self::MAX_LOCAL {
            return None;
        }
        Some(Self(((shard.get() as u64) << Self::LOCAL_BITS) | local))
    }

    /// The shard this inode was created on (resolve the current owner via the shard map).
    pub const fn home_shard(self) -> ShardId {
        ShardId::new((self.0 >> Self::LOCAL_BITS) as u32)
    }

    /// The per-shard counter part.
    pub const fn local(self) -> u64 {
        self.0 & Self::MAX_LOCAL
    }

    /// The raw 64-bit value (for storage and the wire).
    pub const fn get(self) -> u64 {
        self.0
    }

    /// Rebuilds from a raw value. Every `u64` is a valid inode id.
    pub const fn from_raw(raw: u64) -> Self {
        Self(raw)
    }
}

impl fmt::Debug for InodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "InodeId({}:{})", self.home_shard().get(), self.local())
    }
}

impl fmt::Display for InodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ino-{}:{}", self.home_shard().get(), self.local())
    }
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;

    #[test]
    fn formatting() {
        assert_eq!(NodeId::new(3).to_string(), "node-3");
        assert_eq!(format!("{:?}", NodeId::new(3)), "NodeId(3)");
        let ino = InodeId::new(ShardId::new(7), 42).unwrap();
        assert_eq!(ino.to_string(), "ino-7:42");
    }

    #[test]
    fn inode_rejects_out_of_range() {
        assert!(InodeId::new(ShardId::new(InodeId::MAX_SHARD + 1), 0).is_none());
        assert!(InodeId::new(ShardId::new(0), InodeId::MAX_LOCAL + 1).is_none());
        let max = InodeId::new(ShardId::new(InodeId::MAX_SHARD), InodeId::MAX_LOCAL).unwrap();
        assert_eq!(max.get(), u64::MAX);
    }

    proptest! {
        #[test]
        fn inode_packing_roundtrips(shard in 0..=InodeId::MAX_SHARD, local in 0..=InodeId::MAX_LOCAL) {
            let ino = InodeId::new(ShardId::new(shard), local).unwrap();
            prop_assert_eq!(ino.home_shard(), ShardId::new(shard));
            prop_assert_eq!(ino.local(), local);
            prop_assert_eq!(InodeId::from_raw(ino.get()), ino);
        }

        #[test]
        fn inode_order_groups_by_shard(a in any::<u64>(), b in any::<u64>()) {
            // Ordering by raw value sorts by home shard first: handy for range scans.
            let (x, y) = (InodeId::from_raw(a), InodeId::from_raw(b));
            if x.home_shard() < y.home_shard() {
                prop_assert!(x < y);
            }
        }
    }
}
