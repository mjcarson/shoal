//! The frames between the driver and a holder: Shoal's eight byte header, and a body a request
//!
//! Nothing here is the product's wire. The header has Shoal's layout, as X11's spike frames did
//! (`shoal-spike/src/stream/wire.rs`), so a frame costs what a Shoal frame would: a version, a
//! kind, sixteen bits of flags and a 32-bit length counting every byte after it. A lane carries
//! one request at a time and its answer, so a frame needs no id. The kinds are what S6's holder
//! does for a write in place ([S6](../../docs/src/object-storage/device-store.md#staging-two-cases)):
//!
//! - `Stage`: a record in the journal, the units' new bytes behind a head naming the slot, the
//!   offset, the label it makes and the label it expects, answered once durable;
//! - `Apply`: the staged record written in place in its chunk and synced, once its commit applied;
//! - `Fold`: a write that rode inside its commit written in place and synced, its bytes made on
//!   the holder from the write's seed, as a replica on the same host would hand them over;
//! - `Stats` and `Probe`: the holder's counters, and its device's sync floor;
//! - `Verify`: a chunk's range read back and compared with the bytes a write's seed makes, which
//!   says the applies and folds landed what their writes carried.

/// The version byte every frame carries, which no Shoal frame does
pub const VERSION: u8 = 0x38;

/// The length of a header
pub const HEADER_LEN: usize = 8;

/// The largest body either side accepts: the largest write and its head, with room
pub const MAX_FRAME: u32 = 1 << 20;

/// The port every holder listens on, past X11's 13100 and X13's 13200
pub const PORT: u16 = 13_300;

/// The alignment of every offset and length a holder writes: the filesystems' block
pub const ALIGN: u64 = 4096;

/// A stripe chunk's header block, written in place with every apply and fold
pub const CHUNK_HEADER: u64 = 4096;

/// What a frame is
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum Kind {
    /// A write's bytes to stage in the journal
    Stage = 1,
    /// A stage is durable
    Staged = 2,
    /// A committed stage to write in place
    Apply = 3,
    /// An apply is durable
    Applied = 4,
    /// A write that rode inside its commit, to write in place
    Fold = 5,
    /// A fold is durable
    Folded = 6,
    /// The holder's counters asked for
    Stats = 7,
    /// The holder's counters
    StatsOut = 8,
    /// The device's sync floor asked for
    Probe = 9,
    /// The device's sync floor
    ProbeOut = 10,
    /// A request refused, with why
    Error = 11,
    /// A chunk's range asked to be read back and compared with the bytes a seed makes
    Verify = 12,
    /// Whether the range held them: one byte, one or zero
    Verified = 13,
}

impl Kind {
    /// The kind a byte names
    ///
    /// # Arguments
    ///
    /// * `byte` - The byte
    #[must_use]
    pub fn from_byte(byte: u8) -> Option<Kind> {
        // every kind the spike speaks, and nothing else
        Some(match byte {
            1 => Kind::Stage,
            2 => Kind::Staged,
            3 => Kind::Apply,
            4 => Kind::Applied,
            5 => Kind::Fold,
            6 => Kind::Folded,
            7 => Kind::Stats,
            8 => Kind::StatsOut,
            9 => Kind::Probe,
            10 => Kind::ProbeOut,
            11 => Kind::Error,
            12 => Kind::Verify,
            13 => Kind::Verified,
            _ => return None,
        })
    }
}

/// A frame's header
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Header {
    /// What the frame is
    pub kind: Kind,
    /// How many bytes follow the header
    pub len: u32,
}

impl Header {
    /// A header
    ///
    /// # Arguments
    ///
    /// * `kind` - What the frame is
    /// * `len` - How many bytes follow
    #[must_use]
    pub fn new(kind: Kind, len: usize) -> Self {
        Header {
            kind,
            len: u32::try_from(len).expect("a frame fits a u32"),
        }
    }

    /// The header as the eight bytes that go on the wire
    #[must_use]
    pub fn encode(&self) -> [u8; HEADER_LEN] {
        // version, kind, no flags, and the length, little endian as Shoal's are
        let len = self.len.to_le_bytes();
        [VERSION, self.kind as u8, 0, 0, len[0], len[1], len[2], len[3]]
    }

    /// Read a header, refusing a version, a kind or a length the spike does not speak
    ///
    /// # Arguments
    ///
    /// * `bytes` - The eight bytes
    ///
    /// # Errors
    ///
    /// When the version, the kind or the length is one no frame has.
    pub fn decode(bytes: &[u8; HEADER_LEN]) -> Result<Header, String> {
        // the version first, since nothing else means anything under another one
        if bytes[0] != VERSION {
            return Err(format!("version {:#x}", bytes[0]));
        }
        let kind = Kind::from_byte(bytes[1]).ok_or_else(|| format!("kind {}", bytes[1]))?;
        let len = u32::from_le_bytes([bytes[4], bytes[5], bytes[6], bytes[7]]);
        // a length is an allocation, so it is bounded before anything uses it
        if len > MAX_FRAME {
            return Err(format!("length {len}"));
        }
        Ok(Header { kind, len })
    }
}

/// A chunk's label on the wire: the sequence and the tag of the write that made it
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct WireLabel {
    /// The sequence
    pub sequence: u64,
    /// The tag
    pub tag: u64,
}

/// A little endian reader over a body, which fails rather than panics on a short one
struct Cursor<'a> {
    /// The body
    bytes: &'a [u8],
    /// Where the next field starts
    at: usize,
}

impl<'a> Cursor<'a> {
    /// A reader at the start of a body
    ///
    /// # Arguments
    ///
    /// * `bytes` - The body
    fn new(bytes: &'a [u8]) -> Self {
        Cursor { bytes, at: 0 }
    }

    /// The next `N` bytes
    fn take<const N: usize>(&mut self) -> Result<[u8; N], String> {
        let end = self.at + N;
        let field = self
            .bytes
            .get(self.at..end)
            .ok_or_else(|| format!("a body of {} bytes is short", self.bytes.len()))?;
        self.at = end;
        Ok(field.try_into().expect("the slice is N long"))
    }

    /// The next u32
    fn u32(&mut self) -> Result<u32, String> {
        Ok(u32::from_le_bytes(self.take::<4>()?))
    }

    /// The next u64
    fn u64(&mut self) -> Result<u64, String> {
        Ok(u64::from_le_bytes(self.take::<8>()?))
    }

    /// The next label
    fn label(&mut self) -> Result<WireLabel, String> {
        Ok(WireLabel {
            sequence: self.u64()?,
            tag: self.u64()?,
        })
    }
}

/// What a stage names before its bytes
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StageHead {
    /// The chunk slot the record is for
    pub slot: u32,
    /// Where in the chunk's units the bytes go
    pub offset: u32,
    /// How many bytes follow the head
    pub len: u32,
    /// The label the write makes
    pub label: WireLabel,
    /// The label it expects the chunk to carry
    pub expected: WireLabel,
}

impl StageHead {
    /// The head's length on the wire
    pub const LEN: usize = 48;

    /// The head as its bytes
    #[must_use]
    pub fn encode(&self) -> [u8; Self::LEN] {
        let mut out = [0u8; Self::LEN];
        // three u32s, then the two labels, the rest left zero
        out[0..4].copy_from_slice(&self.slot.to_le_bytes());
        out[4..8].copy_from_slice(&self.offset.to_le_bytes());
        out[8..12].copy_from_slice(&self.len.to_le_bytes());
        out[16..24].copy_from_slice(&self.label.sequence.to_le_bytes());
        out[24..32].copy_from_slice(&self.label.tag.to_le_bytes());
        out[32..40].copy_from_slice(&self.expected.sequence.to_le_bytes());
        out[40..48].copy_from_slice(&self.expected.tag.to_le_bytes());
        out
    }

    /// A head from its bytes
    ///
    /// # Arguments
    ///
    /// * `bytes` - The head's bytes
    ///
    /// # Errors
    ///
    /// When the head is short, or names bytes that are not whole blocks.
    pub fn decode(bytes: &[u8]) -> Result<Self, String> {
        let mut cursor = Cursor::new(bytes);
        let slot = cursor.u32()?;
        let offset = cursor.u32()?;
        let len = cursor.u32()?;
        let _pad = cursor.u32()?;
        let head = StageHead {
            slot,
            offset,
            len,
            label: cursor.label()?,
            expected: cursor.label()?,
        };
        // a direct write is whole blocks at an aligned offset
        if u64::from(head.len) % ALIGN != 0 || u64::from(head.offset) % ALIGN != 0 || head.len == 0 {
            return Err(format!("a stage of {} bytes at {} is not whole blocks", head.len, head.offset));
        }
        Ok(head)
    }
}

/// A request the driver sends a holder
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Request {
    /// Apply a staged record in place
    Apply {
        /// The chunk slot
        slot: u32,
        /// The label the committed write made
        label: WireLabel,
    },
    /// Write a write that rode inside its commit in place, its bytes made here
    Fold {
        /// The chunk slot
        slot: u32,
        /// Where in the chunk's units the bytes go
        offset: u32,
        /// How many bytes
        len: u32,
        /// The write's seed
        seed: u64,
        /// The stream its bytes are named by
        object: u64,
    },
    /// Read a chunk's range back and compare it with the bytes a write's seed makes
    Verify {
        /// The chunk slot
        slot: u32,
        /// Where in the chunk's units the bytes are
        offset: u32,
        /// How many bytes
        len: u32,
        /// The write's seed
        seed: u64,
        /// The stream its bytes are named by
        object: u64,
    },
    /// The holder's counters
    Stats,
    /// The device's sync floor, from this many 4 KiB overwrites each synced
    Probe {
        /// How many
        count: u32,
    },
}

impl Request {
    /// The request as one whole frame
    #[must_use]
    pub fn frame(&self) -> Vec<u8> {
        let (kind, body) = match self {
            Request::Apply { slot, label } => {
                let mut body = Vec::with_capacity(24);
                body.extend_from_slice(&slot.to_le_bytes());
                body.extend_from_slice(&[0u8; 4]);
                body.extend_from_slice(&label.sequence.to_le_bytes());
                body.extend_from_slice(&label.tag.to_le_bytes());
                (Kind::Apply, body)
            }
            Request::Fold {
                slot,
                offset,
                len,
                seed,
                object,
            } => (Kind::Fold, range_body(*slot, *offset, *len, *seed, *object)),
            Request::Verify {
                slot,
                offset,
                len,
                seed,
                object,
            } => (Kind::Verify, range_body(*slot, *offset, *len, *seed, *object)),
            Request::Stats => (Kind::Stats, Vec::new()),
            Request::Probe { count } => (Kind::Probe, count.to_le_bytes().to_vec()),
        };
        frame(kind, &body)
    }

    /// A request from its kind and body; a stage is read apart, since its bytes go straight to
    /// a buffer for direct I/O
    ///
    /// # Arguments
    ///
    /// * `kind` - The frame's kind
    /// * `body` - Its body
    ///
    /// # Errors
    ///
    /// When the kind is not a request or the body is short.
    pub fn decode(kind: Kind, body: &[u8]) -> Result<Self, String> {
        let mut cursor = Cursor::new(body);
        match kind {
            Kind::Apply => {
                let slot = cursor.u32()?;
                let _pad = cursor.u32()?;
                Ok(Request::Apply {
                    slot,
                    label: cursor.label()?,
                })
            }
            Kind::Fold | Kind::Verify => {
                let slot = cursor.u32()?;
                let offset = cursor.u32()?;
                let len = cursor.u32()?;
                let _pad = cursor.u32()?;
                let (seed, object) = (cursor.u64()?, cursor.u64()?);
                // a direct write or read is whole blocks at an aligned offset
                if u64::from(len) % ALIGN != 0 || u64::from(offset) % ALIGN != 0 || len == 0 {
                    return Err(format!("a range of {len} bytes at {offset} is not whole blocks"));
                }
                Ok(if kind == Kind::Fold {
                    Request::Fold {
                        slot,
                        offset,
                        len,
                        seed,
                        object,
                    }
                } else {
                    Request::Verify {
                        slot,
                        offset,
                        len,
                        seed,
                        object,
                    }
                })
            }
            Kind::Stats => Ok(Request::Stats),
            Kind::Probe => Ok(Request::Probe { count: cursor.u32()? }),
            other => Err(format!("{other:?} is not a request")),
        }
    }
}

/// The body of a request that names a chunk's range and the seed of its bytes
///
/// # Arguments
///
/// * `slot` - The chunk slot
/// * `offset` - Where in the chunk's units the range starts
/// * `len` - How many bytes
/// * `seed` - The write's seed
/// * `object` - The stream its bytes are named by
fn range_body(slot: u32, offset: u32, len: u32, seed: u64, object: u64) -> Vec<u8> {
    let mut body = Vec::with_capacity(32);
    body.extend_from_slice(&slot.to_le_bytes());
    body.extend_from_slice(&offset.to_le_bytes());
    body.extend_from_slice(&len.to_le_bytes());
    body.extend_from_slice(&[0u8; 4]);
    body.extend_from_slice(&seed.to_le_bytes());
    body.extend_from_slice(&object.to_le_bytes());
    body
}

/// A stage as one whole frame, its bytes made from a seed into the frame itself
///
/// The bytes are made once a write and the same frame is sent to every holder: every copy of a
/// replicated pool holds the same bytes.
///
/// # Arguments
///
/// * `head` - What the stage names
/// * `seed` - The write's seed
/// * `object` - The stream its bytes are named by
#[must_use]
pub fn stage_frame(head: &StageHead, seed: u64, object: u64) -> Vec<u8> {
    let len = head.len as usize;
    let mut out = Vec::with_capacity(HEADER_LEN + StageHead::LEN + len);
    out.extend_from_slice(&Header::new(Kind::Stage, StageHead::LEN + len).encode());
    out.extend_from_slice(&head.encode());
    // the payload made in place behind the head
    let start = out.len();
    out.resize(start + len, 0);
    crate::bytes::fill(seed, object, 0, &mut out[start..]);
    out
}

/// Write a stage's head into a frame [`stage_frame`] made, once what it names is known
///
/// # Arguments
///
/// * `frame` - The frame
/// * `head` - The head, naming the same length the frame was made for
pub fn write_stage_head(frame: &mut [u8], head: &StageHead) {
    debug_assert_eq!(frame.len(), HEADER_LEN + StageHead::LEN + head.len as usize, "the head fits its frame");
    frame[HEADER_LEN..HEADER_LEN + StageHead::LEN].copy_from_slice(&head.encode());
}

/// What a holder has done since it started, and what it is doing now
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct HolderStats {
    /// Stages made durable
    pub stages: u64,
    /// Syncs of the journal
    pub stage_syncs: u64,
    /// Records those syncs covered, together
    pub stage_records: u64,
    /// Bytes written to the journal, header blocks included
    pub stage_bytes: u64,
    /// Applies made durable
    pub applies: u64,
    /// Syncs of a chunk for an apply
    pub apply_syncs: u64,
    /// Folds made durable
    pub folds: u64,
    /// Syncs of a chunk for a fold
    pub fold_syncs: u64,
    /// Bytes written in place in chunks, header blocks included
    pub chunk_bytes: u64,
    /// The cpu time the holder's executor thread has used, in nanoseconds
    pub exec_cpu_ns: u64,
    /// Staged records not yet applied or dropped
    pub staged_now: u64,
    /// Whether kTLS holds the lane this was asked on: one or zero
    pub ktls: u64,
    /// Applies that found no staged record for their label
    pub apply_missing: u64,
    /// Flushes that covered a batch of applies and folds, when the holder batches them
    pub in_place_syncs: u64,
    /// Whether the holder batches its applies' and folds' syncs: one or zero
    pub batched: u64,
}

impl HolderStats {
    /// How many counters there are, each eight bytes on the wire
    const FIELDS: usize = 15;

    /// The counters in their wire order
    fn fields(&self) -> [u64; Self::FIELDS] {
        [
            self.stages,
            self.stage_syncs,
            self.stage_records,
            self.stage_bytes,
            self.applies,
            self.apply_syncs,
            self.folds,
            self.fold_syncs,
            self.chunk_bytes,
            self.exec_cpu_ns,
            self.staged_now,
            self.ktls,
            self.apply_missing,
            self.in_place_syncs,
            self.batched,
        ]
    }

    /// The counters as their bytes
    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        self.fields().iter().flat_map(|field| field.to_le_bytes()).collect()
    }

    /// The counters from their bytes
    ///
    /// # Arguments
    ///
    /// * `body` - The bytes
    ///
    /// # Errors
    ///
    /// When the body is short.
    pub fn decode(body: &[u8]) -> Result<Self, String> {
        let mut cursor = Cursor::new(body);
        let mut fields = [0u64; Self::FIELDS];
        for field in &mut fields {
            *field = cursor.u64()?;
        }
        Ok(HolderStats {
            stages: fields[0],
            stage_syncs: fields[1],
            stage_records: fields[2],
            stage_bytes: fields[3],
            applies: fields[4],
            apply_syncs: fields[5],
            folds: fields[6],
            fold_syncs: fields[7],
            chunk_bytes: fields[8],
            exec_cpu_ns: fields[9],
            staged_now: fields[10],
            ktls: fields[11],
            apply_missing: fields[12],
            in_place_syncs: fields[13],
            batched: fields[14],
        })
    }

    /// What a holder did between two reads of its counters; what it holds now is the later's
    ///
    /// # Arguments
    ///
    /// * `before` - The earlier read
    #[must_use]
    pub fn since(&self, before: &HolderStats) -> HolderStats {
        HolderStats {
            stages: self.stages.saturating_sub(before.stages),
            stage_syncs: self.stage_syncs.saturating_sub(before.stage_syncs),
            stage_records: self.stage_records.saturating_sub(before.stage_records),
            stage_bytes: self.stage_bytes.saturating_sub(before.stage_bytes),
            applies: self.applies.saturating_sub(before.applies),
            apply_syncs: self.apply_syncs.saturating_sub(before.apply_syncs),
            folds: self.folds.saturating_sub(before.folds),
            fold_syncs: self.fold_syncs.saturating_sub(before.fold_syncs),
            chunk_bytes: self.chunk_bytes.saturating_sub(before.chunk_bytes),
            exec_cpu_ns: self.exec_cpu_ns.saturating_sub(before.exec_cpu_ns),
            staged_now: self.staged_now,
            ktls: self.ktls,
            apply_missing: self.apply_missing.saturating_sub(before.apply_missing),
            in_place_syncs: self.in_place_syncs.saturating_sub(before.in_place_syncs),
            batched: self.batched,
        }
    }
}

/// The device's sync floor as a probe found it
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ProbeOut {
    /// The median overwrite and sync, in nanoseconds
    pub p50_ns: u64,
    /// The slowest, in nanoseconds
    pub max_ns: u64,
}

impl ProbeOut {
    /// The probe as its bytes
    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        let mut out = self.p50_ns.to_le_bytes().to_vec();
        out.extend_from_slice(&self.max_ns.to_le_bytes());
        out
    }

    /// The probe from its bytes
    ///
    /// # Arguments
    ///
    /// * `body` - The bytes
    ///
    /// # Errors
    ///
    /// When the body is short.
    pub fn decode(body: &[u8]) -> Result<Self, String> {
        let mut cursor = Cursor::new(body);
        Ok(ProbeOut {
            p50_ns: cursor.u64()?,
            max_ns: cursor.u64()?,
        })
    }
}

/// A whole frame as one buffer
///
/// # Arguments
///
/// * `kind` - What it is
/// * `body` - Its body
#[must_use]
pub fn frame(kind: Kind, body: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(HEADER_LEN + body.len());
    out.extend_from_slice(&Header::new(kind, body.len()).encode());
    out.extend_from_slice(body);
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every request and answer comes back as it was sent
    #[test]
    fn frames_round_trip() {
        let label = WireLabel { sequence: 7, tag: 0xDEAD };
        for request in [
            Request::Apply { slot: 3, label },
            Request::Fold {
                slot: 255,
                offset: 1 << 18,
                len: 4096,
                seed: 9,
                object: 11,
            },
            Request::Verify {
                slot: 1,
                offset: 8192,
                len: 8192,
                seed: 3,
                object: 4,
            },
            Request::Stats,
            Request::Probe { count: 5 },
        ] {
            let bytes = request.frame();
            let header = Header::decode(bytes[..HEADER_LEN].try_into().expect("eight")).expect("a header");
            assert_eq!(header.len as usize, bytes.len() - HEADER_LEN);
            assert_eq!(Request::decode(header.kind, &bytes[HEADER_LEN..]), Ok(request));
        }
        // a stage, its head and its bytes
        let head = StageHead {
            slot: 4,
            offset: 8192,
            len: 16 << 10,
            label,
            expected: WireLabel { sequence: 6, tag: 1 },
        };
        let bytes = stage_frame(&head, 21, 22);
        let header = Header::decode(bytes[..HEADER_LEN].try_into().expect("eight")).expect("a header");
        assert_eq!(header.kind, Kind::Stage);
        assert_eq!(header.len as usize, StageHead::LEN + (16 << 10));
        let body = &bytes[HEADER_LEN..];
        assert_eq!(StageHead::decode(&body[..StageHead::LEN]), Ok(head));
        assert_eq!(&body[StageHead::LEN..], crate::bytes::make(21, 22, 16 << 10).as_slice());
        // and a head written over another once the read says what it names
        let mut patched = bytes.clone();
        let moved = StageHead { offset: 4096, ..head };
        write_stage_head(&mut patched, &moved);
        assert_eq!(StageHead::decode(&patched[HEADER_LEN..HEADER_LEN + StageHead::LEN]), Ok(moved));
        assert_eq!(&patched[HEADER_LEN + StageHead::LEN..], &bytes[HEADER_LEN + StageHead::LEN..]);
        // and the counters
        let stats = HolderStats {
            stages: 1,
            stage_syncs: 2,
            stage_records: 3,
            stage_bytes: 4,
            applies: 5,
            apply_syncs: 6,
            folds: 7,
            fold_syncs: 8,
            chunk_bytes: 9,
            exec_cpu_ns: 10,
            staged_now: 11,
            ktls: 1,
            apply_missing: 12,
            in_place_syncs: 13,
            batched: 1,
        };
        assert_eq!(HolderStats::decode(&stats.encode()), Ok(stats));
        let probe = ProbeOut { p50_ns: 81_000, max_ns: 99_000 };
        assert_eq!(ProbeOut::decode(&probe.encode()), Ok(probe));
    }

    /// A frame past the bound, a version or kind the spike does not speak, or a stage that is
    /// not whole blocks, is refused before anything is allocated for it
    #[test]
    fn bad_frames_are_refused() {
        let mut header = Header::new(Kind::Stats, 0).encode();
        header[4..8].copy_from_slice(&(MAX_FRAME + 1).to_le_bytes());
        assert!(Header::decode(&header).is_err());
        let mut header = Header::new(Kind::Stats, 0).encode();
        header[0] = 0x58;
        assert!(Header::decode(&header).is_err());
        let mut header = Header::new(Kind::Stats, 0).encode();
        header[1] = 99;
        assert!(Header::decode(&header).is_err());
        let head = StageHead {
            slot: 0,
            offset: 100,
            len: 4096,
            label: WireLabel::default(),
            expected: WireLabel::default(),
        };
        assert!(StageHead::decode(&head.encode()).is_err());
        assert!(Request::decode(Kind::Apply, &[0u8; 3]).is_err());
        assert!(Request::decode(Kind::Staged, &[]).is_err());
    }
}
