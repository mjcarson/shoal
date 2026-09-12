//! The three frames of a snapshot stream on the bulk lane
//!
//! ```text
//!  begin : [header][stream 16 B][transition 16 B][boundary u64][total u64][manifest len u32][reserved 4 B][manifest]
//!  chunk : [header][stream 16 B][offset u64][len u32][gxhash32 u32][bytes]
//!  end   : [header][stream 16 B][total u64][gxhash32 u32][status u8][reserved 3 B][resume from u64]
//! ```
//!
//! These are the fields [C2](../../../../../docs/src/distributed/transport.md)'s table asks a
//! snapshot stream to carry - identity, manifest, boundary, offset, length, checksum and resume
//! metadata - defined at M2 so that the bulk lane exists as a lane with a shape, and so that the
//! bounded-bytes test can drive it. Nothing installs a snapshot yet: M7 owns the receiver, and
//! at M2 a stream is counted, checksummed and discarded.

use super::super::ProtocolError;
use super::{bytes16_at, u32_at, u64_at};

/// The size of a begin frame's fixed fields
pub const SNAPSHOT_BEGIN_LEN: usize = 56;

/// The size of a chunk frame's fixed fields
pub const SNAPSHOT_CHUNK_LEN: usize = 32;

/// The size of an end frame
pub const SNAPSHOT_END_LEN: usize = 40;

/// How a stream ended
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum SnapshotStatus {
    /// Every chunk was sent
    Complete = 0,
    /// The sender gave up, and `resume_from` says where a later stream may start
    Aborted = 1,
}

impl SnapshotStatus {
    /// Get the byte this status is written as
    #[inline]
    pub const fn as_byte(self) -> u8 {
        self as u8
    }

    /// Parse a status, reading anything unknown as aborted
    ///
    /// # Arguments
    ///
    /// * `raw` - The byte to parse
    #[inline]
    pub const fn from_byte(raw: u8) -> Self {
        match raw {
            0 => SnapshotStatus::Complete,
            _ => SnapshotStatus::Aborted,
        }
    }
}

/// The start of a stream
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SnapshotBegin {
    /// The stream, so chunks of two streams on one connection are told apart
    pub stream: [u8; 16],
    /// The transition this stream serves, from the control plane's records
    pub transition: [u8; 16],
    /// The committed position this snapshot is a cut at
    pub boundary: u64,
    /// How many bytes the whole stream will carry
    pub total: u64,
    /// How many manifest bytes follow this head
    pub manifest_len: u32,
}

impl SnapshotBegin {
    /// Write this head
    #[must_use]
    pub fn encode(&self) -> [u8; SNAPSHOT_BEGIN_LEN] {
        let mut body = [0u8; SNAPSHOT_BEGIN_LEN];
        body[..16].copy_from_slice(&self.stream);
        body[16..32].copy_from_slice(&self.transition);
        body[32..40].copy_from_slice(&self.boundary.to_le_bytes());
        body[40..48].copy_from_slice(&self.total.to_le_bytes());
        body[48..52].copy_from_slice(&self.manifest_len.to_le_bytes());
        body
    }

    /// Read a head, checking the manifest fits the frame
    ///
    /// # Arguments
    ///
    /// * `raw` - The head bytes
    /// * `body_len` - The number of bytes the frame's header said follow it
    pub fn decode(raw: &[u8; SNAPSHOT_BEGIN_LEN], body_len: usize) -> Result<Self, ProtocolError> {
        let begin = SnapshotBegin {
            stream: bytes16_at(raw, 0),
            transition: bytes16_at(raw, 16),
            boundary: u64_at(raw, 32),
            total: u64_at(raw, 40),
            manifest_len: u32_at(raw, 48),
        };
        if body_len != SNAPSHOT_BEGIN_LEN + begin.manifest_len as usize {
            return Err(ProtocolError::MalformedForward("a snapshot begin does not fit its frame"));
        }
        Ok(begin)
    }
}

/// One chunk of a stream
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SnapshotChunk {
    /// The stream this chunk belongs to
    pub stream: [u8; 16],
    /// Where in the stream these bytes go
    pub offset: u64,
    /// How many bytes follow this head
    pub len: u32,
    /// What those bytes hash to under gxhash32 with seed zero
    pub checksum: u32,
}

impl SnapshotChunk {
    /// Write this head
    #[must_use]
    pub fn encode(&self) -> [u8; SNAPSHOT_CHUNK_LEN] {
        let mut body = [0u8; SNAPSHOT_CHUNK_LEN];
        body[..16].copy_from_slice(&self.stream);
        body[16..24].copy_from_slice(&self.offset.to_le_bytes());
        body[24..28].copy_from_slice(&self.len.to_le_bytes());
        body[28..32].copy_from_slice(&self.checksum.to_le_bytes());
        body
    }

    /// Read a head, checking the chunk fits the frame
    ///
    /// # Arguments
    ///
    /// * `raw` - The head bytes
    /// * `body_len` - The number of bytes the frame's header said follow it
    pub fn decode(raw: &[u8; SNAPSHOT_CHUNK_LEN], body_len: usize) -> Result<Self, ProtocolError> {
        let chunk = SnapshotChunk {
            stream: bytes16_at(raw, 0),
            offset: u64_at(raw, 16),
            len: u32_at(raw, 24),
            checksum: u32_at(raw, 28),
        };
        if body_len != SNAPSHOT_CHUNK_LEN + chunk.len as usize {
            return Err(ProtocolError::MalformedForward("a snapshot chunk does not fit its frame"));
        }
        Ok(chunk)
    }

    /// Check a chunk's bytes against its checksum
    ///
    /// # Arguments
    ///
    /// * `bytes` - The chunk's bytes
    pub fn verify(&self, bytes: &[u8]) -> Result<(), ProtocolError> {
        let computed = checksum(bytes);
        if computed != self.checksum {
            return Err(ProtocolError::SnapshotChecksum {
                claimed: self.checksum,
                computed,
            });
        }
        Ok(())
    }
}

/// The end of a stream
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SnapshotEnd {
    /// The stream that ended
    pub stream: [u8; 16],
    /// How many bytes were sent in all
    pub total: u64,
    /// What the whole stream hashed to
    pub checksum: u32,
    /// Whether it completed
    pub status: SnapshotStatus,
    /// Where a later stream may resume, if this one was aborted
    pub resume_from: u64,
}

impl SnapshotEnd {
    /// Write this frame body
    #[must_use]
    pub fn encode(&self) -> [u8; SNAPSHOT_END_LEN] {
        let mut body = [0u8; SNAPSHOT_END_LEN];
        body[..16].copy_from_slice(&self.stream);
        body[16..24].copy_from_slice(&self.total.to_le_bytes());
        body[24..28].copy_from_slice(&self.checksum.to_le_bytes());
        body[28] = self.status.as_byte();
        body[32..40].copy_from_slice(&self.resume_from.to_le_bytes());
        body
    }

    /// Read a frame body
    ///
    /// # Arguments
    ///
    /// * `raw` - The body bytes
    #[must_use]
    pub fn decode(raw: &[u8; SNAPSHOT_END_LEN]) -> Self {
        SnapshotEnd {
            stream: bytes16_at(raw, 0),
            total: u64_at(raw, 16),
            checksum: u32_at(raw, 24),
            status: SnapshotStatus::from_byte(raw[28]),
            resume_from: u64_at(raw, 32),
        }
    }
}

/// The checksum every chunk carries: gxhash32 with seed zero
///
/// gxhash because it is what every intent log frame and every control log frame is checksummed
/// with, and the pin on its version ([Resolved #65](../../../../../docs/src/appendix/resolved/gxhash-pin.md))
/// is what makes two builds agree about it.
///
/// # Arguments
///
/// * `bytes` - The bytes to hash
#[must_use]
pub fn checksum(bytes: &[u8]) -> u32 {
    gxhash::gxhash32(bytes, 0)
}
