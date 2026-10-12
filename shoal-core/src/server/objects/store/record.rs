//! A staged write as the journal holds it, and what a record found at a start is
//!
//! Every stage is a record in its slice's journal, durable before it is answered
//! ([S6](../../../../../docs/src/object-storage/device-store.md#staging-two-cases)):
//!
//! - **part of a chunk**: a record block, then the whole chunk header the apply will write, then
//!   the units' new values. A record holds new values and never a patch, so writing it twice
//!   writes the same bytes;
//! - **a whole chunk**: a record block naming the file from the slice's pool the chunk was
//!   written into, so a start finds every staged chunk without walking `chunks/`.
//!
//! # Invariants
//!
//! **A record's block checks itself under a seed**: its CRC covers the slice's id and its
//! segment's generation before the block, so a record of an older use of a segment, or a client's
//! bytes that happen to look like a block, never read as a record, and a start can scan every block
//! of a segment for records rather than walk a chain one tear would cut
//! ([F85](../../../../../docs/src/features/devices-and-pools.md#the-journal)).
//!
//! **What a start makes of a record is a function of the record and the chunk**, [`classify`],
//! tested on its own: applied, torn, staged or inert.

use super::chunk::{ChunkHeader, ChunkId, BLOCK};
use crate::shared::checksum::Crc64;
use crate::shared::devices::SliceId;
use crate::shared::objects::Label;
use crate::shared::poolmap::PlacementGroup;

/// The magic a record's block starts with
const MAGIC: &[u8; 8] = b"SHOALJRN";

/// The magic a segment's header block starts with
const SEGMENT_MAGIC: &[u8; 8] = b"SHOALSEG";

/// The format of a record and a segment's header
pub const RECORD_FORMAT: u16 = 1;

/// Where a record block's own checksum is
const RECORD_CRC_AT: usize = 128;

/// Where a record block lists the units it carries
const UNITS_AT: usize = 256;

/// The most units one record carries: its block's room for their indices
pub const MAX_RECORD_UNITS: usize = (BLOCK - UNITS_AT) / 4;

/// What a record stages
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RecordKind {
    /// New values for some units of a chunk, and the header the chunk has once they are applied
    Units {
        /// The units' indices, in the order their bytes follow
        units: Vec<u32>,
        /// The whole header the apply writes
        header: ChunkHeader,
    },
    /// A whole chunk written into a file of the slice's pool
    Whole {
        /// The chunk's file's length, which names the pool it came from
        file_bytes: u64,
        /// The file's number in that pool
        file: u64,
    },
}

/// A staged write's record
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Record {
    /// Its sequence on its slice, which only grows
    pub seq: u64,
    /// The chunk it stages
    pub id: ChunkId,
    /// The label the chunk carries once it is applied
    pub makes: Label,
    /// The label the chunk must carry for it to apply, or none for a chunk not there yet
    pub expects: Option<Label>,
    /// What it stages
    pub kind: RecordKind,
}

impl Record {
    /// The blocks it takes in its segment, before any unit's bytes
    #[must_use]
    pub fn head_blocks(&self) -> u64 {
        match self.kind {
            RecordKind::Units { .. } => 2,
            RecordKind::Whole { .. } => 1,
        }
    }

    /// The bytes it takes in its segment, its units' bytes included
    #[must_use]
    pub fn len(&self) -> u64 {
        match &self.kind {
            RecordKind::Units { units, header } => {
                2 * BLOCK as u64 + units.len() as u64 * u64::from(header.unit_bytes)
            }
            RecordKind::Whole { .. } => BLOCK as u64,
        }
    }

    /// Whether it carries nothing, which no record does
    #[must_use]
    pub fn is_empty(&self) -> bool {
        false
    }

    /// The seeded checksum of a record block with its own field zero
    ///
    /// # Arguments
    ///
    /// * `slice` - The slice the record is on
    /// * `generation` - Its segment's generation
    /// * `block` - The block, its checksum field zero
    fn seeded(slice: SliceId, generation: u64, block: &[u8]) -> u64 {
        let mut crc = Crc64::new();
        crc.update(slice.0.as_bytes());
        crc.update(&generation.to_le_bytes());
        crc.update(block);
        crc.finish()
    }

    /// Write its record block, and for part of a chunk the header block after it
    ///
    /// # Arguments
    ///
    /// * `slice` - The slice it is on
    /// * `generation` - Its segment's generation
    #[must_use]
    pub fn encode_head(&self, slice: SliceId, generation: u64) -> Vec<u8> {
        let mut block = vec![0u8; BLOCK];
        block[0..8].copy_from_slice(MAGIC);
        block[8..10].copy_from_slice(&RECORD_FORMAT.to_le_bytes());
        block[16..24].copy_from_slice(&self.seq.to_le_bytes());
        block[24..32].copy_from_slice(&generation.to_le_bytes());
        block[32..40].copy_from_slice(&self.len().to_le_bytes());
        block[40..48].copy_from_slice(&self.id.consumer.to_le_bytes());
        block[48..64].copy_from_slice(&self.id.object.to_le_bytes());
        block[64..72].copy_from_slice(&self.id.stripe.to_le_bytes());
        block[72] = self.id.position;
        block[74..76].copy_from_slice(&self.id.pg.tablet.to_le_bytes());
        block[76..78].copy_from_slice(&self.id.pg.sub.to_le_bytes());
        block[80..88].copy_from_slice(&self.makes.sequence.to_le_bytes());
        block[88..96].copy_from_slice(&self.makes.tag.to_le_bytes());
        if let Some(expects) = self.expects {
            block[96..104].copy_from_slice(&expects.sequence.to_le_bytes());
            block[104..112].copy_from_slice(&expects.tag.to_le_bytes());
            block[112] = 1;
        }
        match &self.kind {
            RecordKind::Units { units, .. } => {
                block[10] = 1;
                block[12..16].copy_from_slice(&u32::try_from(units.len()).unwrap_or(0).to_le_bytes());
                for (index, unit) in units.iter().enumerate() {
                    let at = UNITS_AT + index * 4;
                    block[at..at + 4].copy_from_slice(&unit.to_le_bytes());
                }
            }
            RecordKind::Whole { file_bytes, file } => {
                block[10] = 2;
                block[136..144].copy_from_slice(&file_bytes.to_le_bytes());
                block[144..152].copy_from_slice(&file.to_le_bytes());
            }
        }
        let own = Self::seeded(slice, generation, &block);
        block[RECORD_CRC_AT..RECORD_CRC_AT + 8].copy_from_slice(&own.to_le_bytes());
        // part of a chunk carries the header its apply writes, in the block after
        if let RecordKind::Units { header, .. } = &self.kind {
            block.extend_from_slice(&header.encode());
        }
        block
    }

    /// Read a record from its block, if the block is one of this slice's segment's
    ///
    /// Returns the record without its header for part of a chunk: that is read from the next
    /// block with [`Record::with_header`].
    ///
    /// # Arguments
    ///
    /// * `slice` - The slice
    /// * `generation` - The segment's generation
    /// * `block` - One block of the segment
    #[must_use]
    pub fn decode_block(slice: SliceId, generation: u64, block: &[u8]) -> Option<(Record, u32)> {
        if block.len() < BLOCK || &block[0..8] != MAGIC {
            return None;
        }
        let u16_at = |at: usize| u16::from_le_bytes([block[at], block[at + 1]]);
        let u32_at = |at: usize| u32::from_le_bytes(block[at..at + 4].try_into().unwrap_or([0; 4]));
        let u64_at = |at: usize| u64::from_le_bytes(block[at..at + 8].try_into().unwrap_or([0; 8]));
        if u16_at(8) != RECORD_FORMAT || u64_at(24) != generation {
            return None;
        }
        let mut zeroed = block[..BLOCK].to_vec();
        zeroed[RECORD_CRC_AT..RECORD_CRC_AT + 8].fill(0);
        if Self::seeded(slice, generation, &zeroed) != u64_at(RECORD_CRC_AT) {
            return None;
        }
        let id = ChunkId {
            consumer: u64_at(40),
            pg: PlacementGroup {
                tablet: u16_at(74),
                sub: u16_at(76),
            },
            object: u128::from_le_bytes(block[48..64].try_into().unwrap_or([0; 16])),
            stripe: u64_at(64),
            position: block[72],
        };
        let makes = Label {
            sequence: u64_at(80),
            tag: u64_at(88),
        };
        let expects = (block[112] == 1).then(|| Label {
            sequence: u64_at(96),
            tag: u64_at(104),
        });
        let kind = match block[10] {
            1 => {
                let count = (u32_at(12) as usize).min(MAX_RECORD_UNITS);
                RecordKind::Units {
                    units: (0..count).map(|index| u32_at(UNITS_AT + index * 4)).collect(),
                    header: ChunkHeader::empty(id, 0, 1),
                }
            }
            2 => RecordKind::Whole {
                file_bytes: u64_at(136),
                file: u64_at(144),
            },
            _ => return None,
        };
        let len = u64_at(32);
        let record = Record {
            seq: u64_at(16),
            id,
            makes,
            expects,
            kind,
        };
        Some((record, u32::try_from(len.div_ceil(BLOCK as u64)).unwrap_or(u32::MAX)))
    }

    /// Give part of a chunk's record the header its apply writes, read from the block after
    ///
    /// # Arguments
    ///
    /// * `block` - The block after the record's
    #[must_use]
    pub fn with_header(mut self, block: &[u8]) -> Option<Record> {
        let header = ChunkHeader::decode(block).ok()?;
        // the header has to be the record's own chunk at the label it makes
        if header.id != self.id || header.label != self.makes {
            return None;
        }
        if let RecordKind::Units { header: held, .. } = &mut self.kind {
            *held = header;
        }
        Some(self)
    }
}

/// A segment's header block: its generation, which every record in it is seeded with
///
/// # Arguments
///
/// * `generation` - The generation
#[must_use]
pub fn encode_segment(generation: u64) -> Vec<u8> {
    let mut block = vec![0u8; BLOCK];
    block[0..8].copy_from_slice(SEGMENT_MAGIC);
    block[8..10].copy_from_slice(&RECORD_FORMAT.to_le_bytes());
    block[16..24].copy_from_slice(&generation.to_le_bytes());
    let mut crc = Crc64::new();
    crc.update(&block[..64]);
    block[64..72].copy_from_slice(&crc.finish().to_le_bytes());
    block
}

/// A segment's generation from its header block, or none for a segment never used
///
/// # Arguments
///
/// * `block` - The segment's first block
#[must_use]
pub fn decode_segment(block: &[u8]) -> Option<u64> {
    if block.len() < BLOCK || &block[0..8] != SEGMENT_MAGIC {
        return None;
    }
    let mut crc = Crc64::new();
    crc.update(&block[..64]);
    let stored = u64::from_le_bytes(block[64..72].try_into().ok()?);
    (crc.finish() == stored).then(|| u64::from_le_bytes(block[16..24].try_into().unwrap_or([0; 8])))
}

/// What a record found at a start is, against the chunk it stages
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Outcome {
    /// The chunk carries what it makes and every unit it covers verifies: it was applied, and is
    /// dropped
    Applied,
    /// Its apply began and did not finish: the chunk carries what it makes with a unit that fails,
    /// or what it expects with a unit it covers that fails. It is applied again, from itself
    Torn,
    /// The chunk, or a record beneath it, can make the label it expects: it waits to be applied
    Staged,
    /// Nothing can make the label it expects; it is kept until a committed fact settles it (M15)
    Inert,
}

/// What a record found at a start is
///
/// A pure function of the record's two labels, the chunk's label, whether every unit the record
/// covers verifies under the chunk's header, and whether a record beneath it makes the label it
/// expects. An apply writes units only once its write committed, so units changed under a chunk's
/// label are an apply that began: re-applying them writes the same new values, and an uncommitted
/// record is never applied by this (P9).
///
/// # Arguments
///
/// * `makes` - The label the record makes
/// * `expects` - The label it expects, none for a chunk not there
/// * `chunk` - The chunk's label, none when there is no chunk
/// * `covered_verify` - Whether every unit the record covers verifies under the chunk's header
/// * `beneath` - Whether another record found makes the label this one expects
#[must_use]
pub fn classify(
    makes: Label,
    expects: Option<Label>,
    chunk: Option<Label>,
    covered_verify: bool,
    beneath: bool,
) -> Outcome {
    match (chunk, covered_verify) {
        // the chunk is what it makes: applied whole, or torn after its header
        (Some(label), true) if label == makes => Outcome::Applied,
        (Some(label), false) if label == makes => Outcome::Torn,
        // the chunk is what it expects with a unit changed under it: torn before its header
        (Some(label), false) if Some(label) == expects => Outcome::Torn,
        // the chunk, or one beneath, makes what it expects: it waits for its commit
        (label, _) if label == expects => Outcome::Staged,
        _ if beneath => Outcome::Staged,
        _ => Outcome::Inert,
    }
}

#[cfg(test)]
mod tests {
    use super::{classify, decode_segment, encode_segment, Outcome, Record, RecordKind};
    use crate::server::objects::store::chunk::{ChunkHeader, ChunkId};
    use crate::shared::devices::SliceId;
    use crate::shared::objects::Label;
    use crate::shared::poolmap::PlacementGroup;

    /// A label
    fn label(sequence: u64) -> Label {
        Label { sequence, tag: sequence * 7 }
    }

    /// Every case a start meets, by the rule
    #[test]
    fn a_record_is_classified_by_the_chunk_it_stages() {
        let (base, made) = (label(1), label(2));
        // applied: the chunk is what it makes, and verifies
        assert_eq!(classify(made, Some(base), Some(made), true, false), Outcome::Applied);
        // torn after its header: what it makes, a unit failing
        assert_eq!(classify(made, Some(base), Some(made), false, false), Outcome::Torn);
        // torn before its header: what it expects, a covered unit failing
        assert_eq!(classify(made, Some(base), Some(base), false, false), Outcome::Torn);
        // staged: what it expects, untouched
        assert_eq!(classify(made, Some(base), Some(base), true, false), Outcome::Staged);
        // staged: a first write, no chunk yet
        assert_eq!(classify(made, None, None, true, false), Outcome::Staged);
        // staged over a record beneath it
        assert_eq!(classify(label(3), Some(made), Some(base), true, true), Outcome::Staged);
        // inert: nothing makes what it expects
        assert_eq!(classify(label(3), Some(made), Some(base), true, false), Outcome::Inert);
        assert_eq!(classify(made, Some(base), None, true, false), Outcome::Inert);
    }

    /// A record reads back only under its own slice and generation
    #[test]
    fn a_record_reads_only_under_its_seed() {
        let id = ChunkId {
            consumer: 1,
            pg: PlacementGroup { tablet: 9, sub: 0 },
            object: 77,
            stripe: 3,
            position: 0,
        };
        let mut header = ChunkHeader::empty(id, 1 << 20, 64 << 10);
        header.label = label(2);
        let record = Record {
            seq: 41,
            id,
            makes: label(2),
            expects: Some(label(1)),
            kind: RecordKind::Units {
                units: vec![0, 5],
                header: header.clone(),
            },
        };
        let slice = SliceId(uuid::Uuid::from_u128(5));
        let head = record.encode_head(slice, 7);
        let (read, blocks) = Record::decode_block(slice, 7, &head).expect("its own seed reads");
        // two blocks of head, then two units of sixteen blocks each
        assert_eq!(blocks, 34);
        let read = read.with_header(&head[4096..]).expect("its header reads");
        assert_eq!(read, record);
        // another generation of the segment, or another slice, does not read it
        assert!(Record::decode_block(slice, 8, &head).is_none());
        assert!(Record::decode_block(SliceId(uuid::Uuid::from_u128(6)), 7, &head).is_none());
        // and a segment's header round trips
        assert_eq!(decode_segment(&encode_segment(12)), Some(12));
        assert_eq!(decode_segment(&[0; 4096]), None);
    }
}
