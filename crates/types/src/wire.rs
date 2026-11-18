//! The versioned header at the front of every node-to-node message.
//!
//! Fixed 20-byte little-endian layout (fits UCX's inline header buffer, so no allocation):
//!
//! ```text
//! offset  0      2        4           8        10     12            20
//!         magic  version  source      service  kind   request_id
//!         u16    u16      NodeId u32  u16      u16    u64
//! ```
//!
//! Hand-written encode/decode (no serde) so the format is explicit and stable across
//! releases. Bytes after the header belong to the service (`decode` returns them).

use std::ops::RangeInclusive;

use crate::ids::NodeId;

/// Marks a fast_fs message; catches garbage or misrouted bytes early.
pub const MAGIC: u16 = 0xF5F5;
/// The version this build speaks.
pub const PROTOCOL_VERSION: u16 = 1;
/// Versions this build can decode. Rolling upgrades: a new release keeps accepting the
/// previous version until every node is upgraded.
pub const SUPPORTED_VERSIONS: RangeInclusive<u16> = 1..=PROTOCOL_VERSION;

/// Which subsystem a message is for. The receiving node dispatches on this.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[repr(u16)]
pub enum Service {
    /// Liveness probes and node-level control.
    Node = 1,
    /// Raft messages (control plane and metadata shards).
    Raft = 2,
    /// Metadata operations (lookup, create, ...).
    Meta = 3,
    /// Chunk reads/writes and replication.
    Data = 4,
}

impl Service {
    pub const ALL: [Self; 4] = [Self::Node, Self::Raft, Self::Meta, Self::Data];

    fn from_u16(raw: u16) -> Option<Self> {
        Self::ALL.into_iter().find(|s| *s as u16 == raw)
    }
}

/// The message header. `kind` is service-specific (each service defines its own message
/// kinds); `request_id` pairs a response with its request (0 = one-way message).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct WireHeader {
    pub version: u16,
    pub source: NodeId,
    pub service: Service,
    pub kind: u16,
    pub request_id: u64,
}

/// Why a header could not be decoded. Never a panic: malformed input is normal at the
/// edge of a distributed system.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum HeaderError {
    #[error("header too short: {len} bytes, need {}", WireHeader::LEN)]
    TooShort { len: usize },
    #[error("bad magic {0:#06x} (not a fast_fs message)")]
    BadMagic(u16),
    #[error("unsupported protocol version {got} (this build supports {min}..={max})")]
    UnsupportedVersion { got: u16, min: u16, max: u16 },
    #[error("unknown service {0}")]
    UnknownService(u16),
}

impl WireHeader {
    /// Encoded size in bytes.
    pub const LEN: usize = 20;

    /// A header for this build's protocol version.
    pub const fn new(source: NodeId, service: Service, kind: u16, request_id: u64) -> Self {
        Self {
            version: PROTOCOL_VERSION,
            source,
            service,
            kind,
            request_id,
        }
    }

    pub fn encode(&self) -> [u8; Self::LEN] {
        let mut b = [0u8; Self::LEN];
        b[0..2].copy_from_slice(&MAGIC.to_le_bytes());
        b[2..4].copy_from_slice(&self.version.to_le_bytes());
        b[4..8].copy_from_slice(&self.source.get().to_le_bytes());
        b[8..10].copy_from_slice(&(self.service as u16).to_le_bytes());
        b[10..12].copy_from_slice(&self.kind.to_le_bytes());
        b[12..20].copy_from_slice(&self.request_id.to_le_bytes());
        b
    }

    /// Decodes the header at the front of `bytes`; returns it and the remaining bytes.
    pub fn decode(bytes: &[u8]) -> Result<(Self, &[u8]), HeaderError> {
        let Some((head, rest)) = bytes.split_first_chunk::<{ Self::LEN }>() else {
            return Err(HeaderError::TooShort { len: bytes.len() });
        };
        let u16_at = |i: usize| u16::from_le_bytes([head[i], head[i + 1]]);
        let magic = u16_at(0);
        if magic != MAGIC {
            return Err(HeaderError::BadMagic(magic));
        }
        let version = u16_at(2);
        if !SUPPORTED_VERSIONS.contains(&version) {
            return Err(HeaderError::UnsupportedVersion {
                got: version,
                min: *SUPPORTED_VERSIONS.start(),
                max: *SUPPORTED_VERSIONS.end(),
            });
        }
        let service_raw = u16_at(8);
        let service =
            Service::from_u16(service_raw).ok_or(HeaderError::UnknownService(service_raw))?;
        let mut source = [0; 4];
        source.copy_from_slice(&head[4..8]);
        let mut request_id = [0; 8];
        request_id.copy_from_slice(&head[12..20]);
        let header = Self {
            version,
            source: NodeId::new(u32::from_le_bytes(source)),
            service,
            kind: u16_at(10),
            request_id: u64::from_le_bytes(request_id),
        };
        Ok((header, rest))
    }
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;

    fn header_strategy() -> impl Strategy<Value = WireHeader> {
        (
            SUPPORTED_VERSIONS,
            any::<u32>(),
            prop::sample::select(Service::ALL.to_vec()),
            any::<u16>(),
            any::<u64>(),
        )
            .prop_map(|(version, source, service, kind, request_id)| WireHeader {
                version,
                source: NodeId::new(source),
                service,
                kind,
                request_id,
            })
    }

    #[test]
    fn layout_is_stable() {
        // Pin the exact bytes: changing the layout must be a deliberate version bump.
        let h = WireHeader::new(
            NodeId::new(0x0403_0201),
            Service::Meta,
            0x0605,
            0x0E0D_0C0B_0A09_0807,
        );
        assert_eq!(
            h.encode(),
            [
                0xF5, 0xF5, 0x01, 0x00, // magic, version
                0x01, 0x02, 0x03, 0x04, // source
                0x03, 0x00, 0x05, 0x06, // service, kind
                0x07, 0x08, 0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x0E, // request_id
            ]
        );
    }

    #[test]
    fn rejects_bad_input() {
        let good = WireHeader::new(NodeId::new(1), Service::Raft, 0, 0).encode();

        let mut b = good;
        b[0] = 0;
        assert!(matches!(
            WireHeader::decode(&b),
            Err(HeaderError::BadMagic(_))
        ));

        let mut b = good;
        b[2..4].copy_from_slice(&(PROTOCOL_VERSION + 1).to_le_bytes());
        assert_eq!(
            WireHeader::decode(&b).unwrap_err(),
            HeaderError::UnsupportedVersion {
                got: PROTOCOL_VERSION + 1,
                min: 1,
                max: PROTOCOL_VERSION
            }
        );
        b[2..4].copy_from_slice(&0u16.to_le_bytes());
        assert!(matches!(
            WireHeader::decode(&b),
            Err(HeaderError::UnsupportedVersion { got: 0, .. })
        ));

        let mut b = good;
        b[8..10].copy_from_slice(&999u16.to_le_bytes());
        assert_eq!(
            WireHeader::decode(&b).unwrap_err(),
            HeaderError::UnknownService(999)
        );
    }

    proptest! {
        #[test]
        fn roundtrips_with_trailing_payload(h in header_strategy(), payload in prop::collection::vec(any::<u8>(), 0..64)) {
            let mut bytes = h.encode().to_vec();
            bytes.extend_from_slice(&payload);
            let (decoded, rest) = WireHeader::decode(&bytes).unwrap();
            prop_assert_eq!(decoded, h);
            prop_assert_eq!(rest, &payload[..]);
        }

        #[test]
        fn every_truncation_is_rejected(h in header_strategy(), len in 0..WireHeader::LEN) {
            let bytes = h.encode();
            prop_assert_eq!(WireHeader::decode(&bytes[..len]), Err(HeaderError::TooShort { len }));
        }

        #[test]
        fn random_bytes_never_panic(bytes in prop::collection::vec(any::<u8>(), 0..64)) {
            let _ = WireHeader::decode(&bytes);
        }
    }
}
