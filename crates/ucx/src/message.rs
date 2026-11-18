//! Received Active Messages: borrowed views ([`AmView`], zero-copy, valid during
//! `Worker::progress_with`) and owned messages ([`AmMessage`], queued by `progress`).
//!
//! Owned messages store small headers/payloads inline (no allocation).
//!
//! Why not keep UCX's buffer (`UCP_AM_FLAG_PERSISTENT_DATA`)? Measured on shared memory it
//! was ~10 ns/hop *slower*: UCX copies the FIFO slot into a descriptor anyway, and
//! releasing it costs another call. The borrowed view is the real zero-copy path; bulk
//! data will use RMA (`get`/`put`). See docs/PERF.md.

use std::fmt;
use std::ops::Deref;

use crate::worker::AmId;

/// A received Active Message borrowing UCX's buffers: valid only inside the
/// `progress_with` closure it was passed to.
#[derive(Debug, Clone, Copy)]
pub struct AmView<'a> {
    pub id: AmId,
    pub header: &'a [u8],
    pub data: &'a [u8],
}

impl AmView<'_> {
    /// Copies into an owned message (small parts inline, no allocation).
    pub fn to_message(&self) -> AmMessage {
        AmMessage {
            id: self.id,
            header: Header::from_slice(self.header),
            data: Payload::from_slice(self.data),
        }
    }
}

/// A received Active Message that owns its bytes.
#[derive(Debug)]
pub struct AmMessage {
    pub id: AmId,
    pub header: Header,
    pub data: Payload,
}

/// Headers up to this size are stored inline.
pub const HEADER_INLINE: usize = 32;
/// Payloads up to this size are stored inline.
pub const PAYLOAD_INLINE: usize = 64;

/// An owned AM header.
pub type Header = SmallBytes<HEADER_INLINE>;
/// An owned AM payload.
pub type Payload = SmallBytes<PAYLOAD_INLINE>;

/// Bytes stored inline when `len <= N`, on the heap otherwise. Derefs to `&[u8]`.
pub struct SmallBytes<const N: usize>(Repr<N>);

enum Repr<const N: usize> {
    Inline { len: u8, buf: [u8; N] },
    Heap(Vec<u8>),
}

impl<const N: usize> SmallBytes<N> {
    const FITS_U8: () = assert!(N <= u8::MAX as usize, "inline length is stored in a u8");

    /// Copies `src`: inline if it fits, heap otherwise.
    pub fn from_slice(src: &[u8]) -> Self {
        let () = Self::FITS_U8;
        let len = src.len();
        if len <= N {
            let mut buf = [0; N];
            buf[..len].copy_from_slice(src);
            Self(Repr::Inline {
                len: len as u8,
                buf,
            })
        } else {
            Self(Repr::Heap(src.to_vec()))
        }
    }

    /// True if stored inline (no allocation was made).
    pub fn is_inline(&self) -> bool {
        matches!(self.0, Repr::Inline { .. })
    }

    /// Converts into an owned buffer (copies only if inline), e.g. to forward it with
    /// the non-blocking [`crate::Endpoint::send`].
    pub fn into_vec(self) -> Vec<u8> {
        match self.0 {
            Repr::Inline { len, buf } => buf[..usize::from(len)].to_vec(),
            Repr::Heap(v) => v,
        }
    }
}

impl<const N: usize> Deref for SmallBytes<N> {
    type Target = [u8];
    fn deref(&self) -> &[u8] {
        match &self.0 {
            Repr::Inline { len, buf } => &buf[..usize::from(*len)],
            Repr::Heap(v) => v,
        }
    }
}

impl<const N: usize> fmt::Debug for SmallBytes<N> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SmallBytes")
            .field("len", &self.len())
            .field("inline", &self.is_inline())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn inline_and_heap() {
        let s = SmallBytes::<16>::from_slice(&[7; 10]);
        assert!(s.is_inline());
        assert_eq!(&s[..], &[7; 10]);

        let b = SmallBytes::<16>::from_slice(&[9; 17]);
        assert!(!b.is_inline());
        assert_eq!(b.into_vec(), vec![9; 17]);

        let e = SmallBytes::<16>::from_slice(&[]);
        assert!(e.is_empty() && e.is_inline());
    }
}
