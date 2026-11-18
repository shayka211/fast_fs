#![doc = include_str!("../README.md")]

mod ids;
mod wire;

pub use ids::{ClusterId, DriveId, Epoch, InodeId, NodeId, PgId, ShardId};
pub use wire::{HeaderError, MAGIC, PROTOCOL_VERSION, SUPPORTED_VERSIONS, Service, WireHeader};
