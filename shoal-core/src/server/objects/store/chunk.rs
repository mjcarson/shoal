//! A stripe chunk on a slice: its identity, its header block and the checksum of every unit
//!
//! A stripe chunk is one file: a header block, then its units, each at an aligned offset
//! ([S6](../../../../../docs/src/object-storage/device-store.md#a-stripe-chunk)). The header names
//! the chunk's identity, its geometry, the label of the write it holds, how much of it holds bytes,
//! and a CRC-64/NVME for every unit, bound to the unit's place.
//!
//! # Invariants
//!
//! **A unit's checksum binds its bytes to their place** (P15): it is the CRC of the unit's bytes
//! followed by its identity - consumer, object, stripe, position and unit index - made from the
//! CRC of each by [`combine`](crate::shared::checksum::combine), so bytes that verify at one place fail at every other.
//!
//! **The header is one 4 KiB block and checks itself**: its own CRC covers its fields and every
//! unit's checksum, so a torn header is told from a header, and a pool's chunk is cut into at most
//! [`MAX_CHUNK_UNITS`] units so their checksums fit.
//!
//! **Every format here is frozen once written**: a changed byte is a chunk no slice reads.
//! `chunk_format_is_frozen` holds the encoding to known bytes.

use crate::shared::checksum::{crc64, Crc64};
use crate::shared::objects::Label;
use crate::shared::poolmap::PlacementGroup;
use crate::shared::pools::MAX_CHUNK_UNITS;

/// A chunk's identity, its unit identities and the checksum binding a unit to its place are the
/// wire's too since the object lane names chunks ([F86](../../../../../docs/src/features/replicated-pools.md)),
/// so they are defined once, in `shoal-proto`
pub use crate::shared::stripes::{bound_crc, ChunkId, IDENTITY_BYTES};

/// The block every header and every unit is aligned to
pub const BLOCK: usize = 4096;

/// The magic a chunk's header starts with
const MAGIC: &[u8; 8] = b"SHOALCHK";

/// The format of a chunk's header
pub const CHUNK_FORMAT: u16 = 1;

/// Where the unit checksums start in a header block
const CRCS_AT: usize = 128;

/// Where the header's own checksum is
const HEADER_CRC_AT: usize = 96;

/// The checksum of a unit's bytes at its place, computed in steps of 64 KiB with a yield between
///
/// No object loop runs past its queue's latency goal without yielding
/// ([S13](../../../../../docs/src/object-storage/isolation.md#shared-executors-or-dedicated-ones)).
///
/// # Arguments
///
/// * `bytes` - The unit's bytes
/// * `id` - The chunk
/// * `unit` - The unit's index
pub async fn unit_crc(bytes: &[u8], id: &ChunkId, unit: u32) -> u64 {
    let mut crc = Crc64::new();
    for step in bytes.chunks(super::STEP) {
        crc.update(step);
        glommio::yield_if_needed().await;
    }
    bound_crc(crc.finish(), id, unit)
}

/// A chunk's header: everything a reader needs to verify it, in one block
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChunkHeader {
    /// The chunk's identity
    pub id: ChunkId,
    /// The bytes of one unit
    pub unit_bytes: u32,
    /// The bytes of the whole chunk, a whole number of units
    pub chunk_bytes: u64,
    /// The label of the write this chunk holds
    pub label: Label,
    /// How many of its bytes hold data; a unit wholly past this reads as zeros
    pub length: u64,
    /// Every unit's checksum, bound to its place; zero for a unit wholly past `length`
    pub crcs: Vec<u64>,
}

/// Why a header block does not read
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HeaderError {
    /// It does not start with the magic, so it is not a header
    NotAHeader,
    /// It is a header in a format this build does not read
    Format(u16),
    /// Its own checksum does not match, so it was torn or damaged
    Checksum,
    /// It describes a chunk no pool can have
    Shape(String),
}

impl ChunkHeader {
    /// A header for a chunk of a geometry with no bytes yet, at no label
    ///
    /// # Arguments
    ///
    /// * `id` - The chunk
    /// * `chunk_bytes` - The bytes of the whole chunk
    /// * `unit_bytes` - The bytes of a unit
    #[must_use]
    pub fn empty(id: ChunkId, chunk_bytes: u64, unit_bytes: u32) -> Self {
        let units = usize::try_from(chunk_bytes / u64::from(unit_bytes.max(1))).unwrap_or(0);
        ChunkHeader {
            id,
            unit_bytes,
            chunk_bytes,
            label: Label::default(),
            length: 0,
            crcs: vec![0; units],
        }
    }

    /// How many units the chunk has
    #[must_use]
    pub fn units(&self) -> u32 {
        u32::try_from(self.crcs.len()).unwrap_or(u32::MAX)
    }

    /// The units a range of the chunk touches, as a range of indices
    ///
    /// # Arguments
    ///
    /// * `offset` - Where the range starts
    /// * `len` - How long it is
    #[must_use]
    pub fn units_of(&self, offset: u64, len: u64) -> std::ops::Range<u32> {
        let unit = u64::from(self.unit_bytes.max(1));
        let first = offset / unit;
        let end = (offset + len).div_ceil(unit);
        u32::try_from(first).unwrap_or(u32::MAX)..u32::try_from(end).unwrap_or(u32::MAX)
    }

    /// Whether a unit holds any bytes: whether any of it is under `length`
    ///
    /// # Arguments
    ///
    /// * `unit` - The unit's index
    #[must_use]
    pub fn holds(&self, unit: u32) -> bool {
        u64::from(unit) * u64::from(self.unit_bytes) < self.length
    }

    /// Where a unit is in the chunk's file: past the header block
    ///
    /// # Arguments
    ///
    /// * `unit` - The unit's index
    #[must_use]
    pub fn offset_of(&self, unit: u32) -> u64 {
        BLOCK as u64 + u64::from(unit) * u64::from(self.unit_bytes)
    }

    /// The bytes of the chunk's file: its header block and every unit
    #[must_use]
    pub fn file_bytes(&self) -> u64 {
        BLOCK as u64 + self.chunk_bytes
    }

    /// Write the header as its block
    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        let mut block = vec![0u8; BLOCK];
        block[0..8].copy_from_slice(MAGIC);
        block[8..10].copy_from_slice(&CHUNK_FORMAT.to_le_bytes());
        block[12..16].copy_from_slice(&self.unit_bytes.to_le_bytes());
        block[16..20].copy_from_slice(&self.units().to_le_bytes());
        block[24..32].copy_from_slice(&self.chunk_bytes.to_le_bytes());
        block[32..40].copy_from_slice(&self.id.consumer.to_le_bytes());
        block[40..56].copy_from_slice(&self.id.object.to_le_bytes());
        block[56..64].copy_from_slice(&self.id.stripe.to_le_bytes());
        block[64] = self.id.position;
        block[66..68].copy_from_slice(&self.id.pg.tablet.to_le_bytes());
        block[68..70].copy_from_slice(&self.id.pg.sub.to_le_bytes());
        block[72..80].copy_from_slice(&self.label.sequence.to_le_bytes());
        block[80..88].copy_from_slice(&self.label.tag.to_le_bytes());
        block[88..96].copy_from_slice(&self.length.to_le_bytes());
        for (index, crc) in self.crcs.iter().enumerate() {
            let at = CRCS_AT + index * 8;
            block[at..at + 8].copy_from_slice(&crc.to_le_bytes());
        }
        // the header's own checksum, over the whole block with its field zero
        let own = crc64(&block);
        block[HEADER_CRC_AT..HEADER_CRC_AT + 8].copy_from_slice(&own.to_le_bytes());
        block
    }

    /// Read a header from its block
    ///
    /// # Arguments
    ///
    /// * `block` - The block, at least [`BLOCK`] bytes
    ///
    /// # Errors
    ///
    /// A block that is not a header, is in another format, fails its own checksum, or describes a
    /// chunk no pool can have.
    pub fn decode(block: &[u8]) -> Result<Self, HeaderError> {
        if block.len() < BLOCK || &block[0..8] != MAGIC {
            return Err(HeaderError::NotAHeader);
        }
        let u16_at = |at: usize| u16::from_le_bytes([block[at], block[at + 1]]);
        let u32_at = |at: usize| u32::from_le_bytes(block[at..at + 4].try_into().unwrap_or([0; 4]));
        let u64_at = |at: usize| u64::from_le_bytes(block[at..at + 8].try_into().unwrap_or([0; 8]));
        let format = u16_at(8);
        if format != CHUNK_FORMAT {
            return Err(HeaderError::Format(format));
        }
        // the header's own checksum, over the block with its field zero
        let mut zeroed = block[..BLOCK].to_vec();
        zeroed[HEADER_CRC_AT..HEADER_CRC_AT + 8].fill(0);
        if crc64(&zeroed) != u64_at(HEADER_CRC_AT) {
            return Err(HeaderError::Checksum);
        }
        let unit_bytes = u32_at(12);
        let units = u32_at(16);
        let chunk_bytes = u64_at(24);
        if unit_bytes == 0
            || u64::from(units) > MAX_CHUNK_UNITS
            || u64::from(units) * u64::from(unit_bytes) != chunk_bytes
        {
            return Err(HeaderError::Shape(format!(
                "{units} units of {unit_bytes} bytes against a chunk of {chunk_bytes}"
            )));
        }
        let crcs = (0..units as usize)
            .map(|index| u64_at(CRCS_AT + index * 8))
            .collect();
        Ok(ChunkHeader {
            id: ChunkId {
                consumer: u64_at(32),
                pg: PlacementGroup {
                    tablet: u16_at(66),
                    sub: u16_at(68),
                },
                object: u128::from_le_bytes(block[40..56].try_into().unwrap_or([0; 16])),
                stripe: u64_at(56),
                position: block[64],
            },
            unit_bytes,
            chunk_bytes,
            label: Label {
                sequence: u64_at(72),
                tag: u64_at(80),
            },
            length: u64_at(88),
            crcs,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::{bound_crc, ChunkHeader, ChunkId, HeaderError, BLOCK};
    use crate::shared::checksum::{crc64, Crc64};
    use crate::shared::objects::Label;
    use crate::shared::poolmap::PlacementGroup;

    /// A chunk to test with
    fn chunk() -> ChunkId {
        ChunkId {
            consumer: 3,
            pg: PlacementGroup { tablet: 0x7ab, sub: 1 },
            object: 0x0192_6f1e_7a00_7000_8000_0000_0000_0001,
            stripe: 9,
            position: 2,
        }
    }

    /// A header round trips, a torn one fails its own checksum, and the encoding is frozen
    #[test]
    fn chunk_format_is_frozen() {
        let mut header = ChunkHeader::empty(chunk(), 4 << 20, 64 << 10);
        header.label = Label { sequence: 5, tag: 0xfeed };
        header.length = 3 << 20;
        for (index, crc) in header.crcs.iter_mut().enumerate() {
            *crc = index as u64 * 0x0101_0101;
        }
        let block = header.encode();
        assert_eq!(block.len(), BLOCK);
        assert_eq!(ChunkHeader::decode(&block), Ok(header.clone()));
        // the block's bytes, frozen: a change here is a chunk no slice reads
        let mut crc = Crc64::new();
        crc.update(&block);
        assert_eq!(crc.finish(), FROZEN_HEADER, "{:#018x}", crc.finish());
        // a torn header fails, by its own checksum
        let mut torn = block.clone();
        torn[200] ^= 1;
        assert_eq!(ChunkHeader::decode(&torn), Err(HeaderError::Checksum));
        // zeros are not a header at all
        assert_eq!(ChunkHeader::decode(&vec![0; BLOCK]), Err(HeaderError::NotAHeader));
        // the path names the slice's inventory
        assert_eq!(
            chunk().relative().to_string_lossy(),
            "0000000000000003/7ab-0001/01926f1e7a0070008000000000000001/0000000000000009.2"
        );
    }

    /// One header block, frozen
    const FROZEN_HEADER: u64 = 0x4535_e6f5_b1d6_c030;

    /// A unit's checksum is bound to its place: the same bytes at another chunk, position, stripe
    /// or index check differently
    #[test]
    fn a_units_checksum_is_bound_to_its_place() {
        let bytes = vec![7u8; 4096];
        let crc = crc64(&bytes);
        let here = bound_crc(crc, &chunk(), 0);
        let mut elsewhere = chunk();
        elsewhere.position = 3;
        assert_ne!(here, bound_crc(crc, &elsewhere, 0));
        assert_ne!(here, bound_crc(crc, &chunk(), 1));
        let mut other = chunk();
        other.object += 1;
        assert_ne!(here, bound_crc(crc, &other, 0));
        // and it is the CRC of the bytes followed by the identity, made without reading them again
        let mut whole = bytes.clone();
        whole.extend_from_slice(&chunk().identity(0));
        assert_eq!(here, crc64(&whole));
    }
}
