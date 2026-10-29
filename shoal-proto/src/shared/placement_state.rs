//! A placement group's state in its tablet group, and the record of what each position missed
//!
//! A placement group is a sub-range of one tablet's keys, so the tablet group that orders its
//! stripes' writes is the one place that saw every one of them
//! ([S5](../../../docs/src/object-storage/placement.md#generations)). It holds, for each placement
//! group that has ever moved or missed anything, three things beside the rows:
//!
//! - an **epoch**, moved by every command that changes where the group's chunks are staged or
//!   read. Every stripe commit that makes a label current names the epoch it was staged under,
//!   and the group refuses it at any other: what makes a write staged under an old placement
//!   unable to land under a new one ([P8](../../../docs/src/object-storage/contract.md));
//! - the **generation** the group's chunks sit under, its **positions** at it, and the **move** in
//!   flight, while the group is in both generations ([S10](../../../docs/src/object-storage/recovery.md#moves));
//! - the **missed record**: for each position, the stripes it is stale on and what each missed,
//!   derived at apply from the committed rows alone, bounded, and the index a rebuild finds them
//!   by ([Q17](../../../docs/src/object-storage/contract.md)).
//!
//! Since [F89](../../../docs/src/features/scrub-and-repair.md) it holds the group's scrub cursors
//! too, one a kind ([`ScrubState`]): where each pass is among the group's placement groups, moved
//! by commands that change nothing a writer reads, so a new leader resumes a pass where the last
//! one stopped.
//!
//! A group with none of these is at its pool's base - the generation the pool was pinned at, in
//! the rule's order - so nothing is kept for one that never moved or missed anything. Every
//! function here is pure: the shard applies them in committed order and persists the result at
//! its checkpoint, and the stripe model's `shoal-model/src/stripe/group.rs` is what they follow.
//!
//! # Invariants
//!
//! **The epoch only moves forward**, and a command that names another epoch, another move or a
//! position list that is not a permutation is refused and changes nothing.
//!
//! **The record is derived from what a committed row says and nothing else**: its marks, what its
//! commit changed, and the label it named before. A position the row calls current has no entry.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::shared::objects::{ChangedUnits, Label};
use crate::shared::devices::DeviceId;
use crate::shared::poolmap::{PlacementGroup, SliceRef};
use crate::shared::scrub::{ScrubKind, ScrubState};
use crate::shared::stripes::StripeRow;

/// A map written as a list of pairs, so its keys survive any format, a JSON file's included
mod pairs {
    use std::collections::BTreeMap;

    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    /// Write the map as its pairs, in key order
    ///
    /// # Arguments
    ///
    /// * `map` - The map
    /// * `serializer` - Where it goes
    pub fn serialize<S, K, V>(map: &BTreeMap<K, V>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
        K: Serialize,
        V: Serialize,
    {
        serializer.collect_seq(map.iter())
    }

    /// Read the map back from its pairs
    ///
    /// # Arguments
    ///
    /// * `deserializer` - Where it comes from
    pub fn deserialize<'de, D, K, V>(deserializer: D) -> Result<BTreeMap<K, V>, D::Error>
    where
        D: Deserializer<'de>,
        K: Deserialize<'de> + Ord,
        V: Deserialize<'de>,
    {
        let pairs: Vec<(K, V)> = Vec::deserialize(deserializer)?;
        Ok(pairs.into_iter().collect())
    }
}

/// How many stripes one position's part of the record holds before it overflows, unless set
///
/// Q17's bound, decided with the user on 2026-10-14: about a second of a group's top commit rate
/// ([X10](../../../docs/src/object-storage/stripe-row-costs.md)), minutes at ordinary rates.
pub const DEFAULT_MISSED_RECORD_STRIPES: usize = 4096;

/// How many writes one stripe's entry keeps before it is the whole chunk
pub const MAX_MISSED_WRITES: usize = 16;

/// The key a placement group's state is kept under: its consumer and the group
pub type PgKey = (u64, PlacementGroup);

/// A stripe within its placement group: its object and its index
pub type StripeKey = (u128, u64);

/// A move in flight: the generation the group moves to and the positions it will have there
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Moving {
    /// The move's identity, minted by the control group, which its switch and its abort name
    pub id: u64,
    /// The generation the group moves to
    pub generation: u64,
    /// Each position's slice there, as its index in the rule's answer at that generation
    pub positions: Vec<u8>,
}

/// What one stripe's entry in the missed record says a position missed
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Missed {
    /// More than the entry keeps, or not known: the chunk is rebuilt whole
    Whole,
    /// Every write the position missed since the entry began, and the label it began at
    Ranges {
        /// The label the row named here when the entry began: a holder below it may have dropped
        /// a committed write it never applied, which no range names
        since: Label,
        /// Each write's label and the units of the chunk it changed, as `(first, count)`
        writes: Vec<(Label, u32, u32)>,
    },
}

impl Missed {
    /// The units a holder at a label has to be given to make the row's state, if the entry knows
    ///
    /// The run from the least unit to the greatest of every write after the holder's label. None
    /// is the whole chunk: a whole entry, a holder below where the entry began, or one past every
    /// write the entry recorded, which holds something it cannot explain.
    ///
    /// # Arguments
    ///
    /// * `held` - The label the holder's chunk is at
    #[must_use]
    pub fn units_after(&self, held: Label) -> Option<(u32, u32)> {
        let Missed::Ranges { since, writes } = self else {
            return None;
        };
        // a holder below where the entry began fell back past a write it never saw
        if held.sequence < since.sequence {
            return None;
        }
        // the run covering every write after the holder's label
        let mut range: Option<(u32, u32)> = None;
        for (label, first, count) in writes {
            if label.sequence <= held.sequence {
                continue;
            }
            let last = first.saturating_add(count.saturating_sub(1));
            range = Some(match range {
                Some((low, high)) => (low.min(*first), high.max(last)),
                None => (*first, last),
            });
        }
        range.map(|(low, high)| (low, high - low + 1))
    }

    /// The labels after a holder's that this entry recorded, which a rebuild's units stand on too
    ///
    /// # Arguments
    ///
    /// * `held` - The label the holder's chunk is at
    /// * `target` - The label the rebuild makes
    #[must_use]
    pub fn labels_between(&self, held: Label, target: Label) -> Vec<Label> {
        match self {
            Missed::Whole => Vec::new(),
            Missed::Ranges { writes, .. } => writes
                .iter()
                .map(|(label, _, _)| *label)
                .filter(|label| label.sequence > held.sequence && label.sequence < target.sequence)
                .collect(),
        }
    }
}

/// One position's part of the missed record
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct PositionRecord {
    /// Each stripe the position is stale on, and what it missed
    #[serde(with = "pairs")]
    pub stripes: BTreeMap<StripeKey, Missed>,
    /// Whether a stripe was ever dropped for the bound since the last backfill said otherwise:
    /// a stripe the position is stale on may then be missing here, and only a walk finds it
    pub overflowed: bool,
    /// How many stripes were dropped for the bound, ever: a backfill says the record is whole
    /// again only if none was dropped while it walked
    pub drops: u64,
}

/// A placement group's state, beside its rows in its tablet group
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct PgState {
    /// Moved by every begin, switch and abort of a move; zero for a group that never moved
    pub epoch: u64,
    /// The generation its chunks sit under, if it moved from its pool's base
    pub generation: Option<u64>,
    /// Each position's slice, as its index in the rule's answer at that generation; none for the
    /// rule's own order
    pub positions: Option<Vec<u8>>,
    /// The move in flight, while the group is in both generations
    pub moving: Option<Moving>,
    /// Each position's part of the missed record, empty until a position is stale
    pub record: Vec<PositionRecord>,
    /// The slices the group left still holding what it placed there, each until it is released:
    /// a move's old holders once it switched, its arrivals once it was given up (M20)
    ///
    /// Always written: the sidecar and a placement answer are postcard, which names no field, so
    /// a field skipped when empty would be read as the next one's bytes.
    #[serde(default)]
    pub left: Vec<SliceRef>,
}

impl PgState {
    /// Whether there is nothing here a group at its base does not have, so nothing need be kept
    #[must_use]
    pub fn is_base(&self) -> bool {
        self.epoch == 0
            && self.generation.is_none()
            && self.positions.is_none()
            && self.moving.is_none()
            && self.left.is_empty()
            && self
                .record
                .iter()
                .all(|part| part.stripes.is_empty() && !part.overflowed && part.drops == 0)
    }

    /// One position's part of the record, made as wide as the stripe first
    ///
    /// # Arguments
    ///
    /// * `pos` - The position
    /// * `width` - How many positions the stripe has
    fn part(&mut self, pos: usize, width: usize) -> &mut PositionRecord {
        if self.record.len() < width {
            self.record.resize_with(width, PositionRecord::default);
        }
        &mut self.record[pos]
    }

    /// What one stripe's entry says a position missed
    ///
    /// # Arguments
    ///
    /// * `stripe` - The stripe
    /// * `pos` - The position
    #[must_use]
    pub fn missed(&self, stripe: StripeKey, pos: usize) -> Option<&Missed> {
        self.record.get(pos).and_then(|part| part.stripes.get(&stripe))
    }
}

/// What a writer needs of a placement group's state: where its chunks are and its epoch
///
/// A group's state without its missed record, which only the rebuild driver on the group's
/// leader reads. What a placement read answers, so a coordinator stages on the right holders and
/// claims the epoch it read ([F88](../../../docs/src/features/recovery-and-moves.md)).
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct PgView {
    /// The epoch every commit staged on these holders claims
    pub epoch: u64,
    /// The generation its chunks sit under, none for its pool's pin
    pub generation: Option<u64>,
    /// Each position's slice as its index in the rule's answer, none for the rule's own order
    pub positions: Option<Vec<u8>>,
    /// The move in flight, while the group is in both generations
    pub moving: Option<Moving>,
    /// The slices the group left, which its leader's release driver empties (M20); always
    /// written, since a placement answer is postcard
    #[serde(default)]
    pub left: Vec<SliceRef>,
}

impl PgState {
    /// What a writer needs of this state
    #[must_use]
    pub fn view(&self) -> PgView {
        PgView {
            epoch: self.epoch,
            generation: self.generation,
            positions: self.positions.clone(),
            moving: self.moving.clone(),
            left: self.left.clone(),
        }
    }
}

/// What a placement group's tablet group is asked to commit about the group itself
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum PlacementCommand {
    /// A move begins: the group is in both generations until it switches or aborts
    MoveBegin {
        /// The consumer whose group it is
        consumer: u64,
        /// The placement group
        pg: PlacementGroup,
        /// The move's identity, minted by the control group
        id: u64,
        /// The epoch its driver read, which it is refused at any other
        expect: u64,
        /// The generation the group moves to
        generation: u64,
        /// Each position's slice there, as its index in the rule's answer at that generation
        positions: Vec<u8>,
    },
    /// The move's copier is done: the group is at the new generation, with its new positions
    MoveSwitch {
        /// The consumer whose group it is
        consumer: u64,
        /// The placement group
        pg: PlacementGroup,
        /// The move's identity
        id: u64,
        /// The old holders that hold no position at the new generation, which the group names
        /// until each is released (M20)
        left: Vec<SliceRef>,
    },
    /// The move is given up: the group is back in its one generation
    MoveAbort {
        /// The consumer whose group it is
        consumer: u64,
        /// The placement group
        pg: PlacementGroup,
        /// The move's identity
        id: u64,
        /// The arrivals the copier may have written to, which the group names until each is
        /// released (M20)
        left: Vec<SliceRef>,
    },
    /// A slice the group left was released of what it held, or holds a position again: the group
    /// no longer names it (M20). It moves no epoch, since nothing a writer reads changes
    ReleaseDone {
        /// The consumer whose group it is
        consumer: u64,
        /// The placement group
        pg: PlacementGroup,
        /// The slice
        slice: SliceRef,
    },
    /// A backfill walked every stripe a position is stale on and found each in the record: the
    /// record holds every one again, if nothing was dropped while it walked
    BackfillDone {
        /// The consumer whose group it is
        consumer: u64,
        /// The placement group
        pg: PlacementGroup,
        /// The position
        position: u8,
        /// The drops the record had counted when the backfill began
        drops: u64,
    },
    /// A scrub's pass begins over the tablet group's placement groups (F89)
    ///
    /// Like every scrub command it moves no epoch: nothing a writer stages by or claims changes.
    ScrubBegin {
        /// The consumer of the placement group it names
        consumer: u64,
        /// A placement group of the tablet group, which routes the command to it
        pg: PlacementGroup,
        /// The kind of scrub
        kind: ScrubKind,
        /// The pass, one past the cursor's
        pass: u64,
        /// The pool's count of requests of the kind the pass serves
        request: u64,
        /// Whether an operator's request begins it
        asked: bool,
        /// When it began, by its driver's clock, in milliseconds since the Unix epoch
        began_ms: u64,
    },
    /// A pass finished a placement group: the cursor moves past it, to the next in key order
    ScrubStep {
        /// The consumer of the placement group it finished
        consumer: u64,
        /// The placement group it finished
        pg: PlacementGroup,
        /// The kind of scrub
        kind: ScrubKind,
        /// The pass, the cursor's
        pass: u64,
        /// The placement group the cursor was past when the driver read it
        after: Option<PgKey>,
    },
    /// A pass finished every placement group of its tablet group
    ScrubEnd {
        /// The consumer of the placement group it names
        consumer: u64,
        /// A placement group of the tablet group, which routes the command to it
        pg: PlacementGroup,
        /// The kind of scrub
        kind: ScrubKind,
        /// The pass, the cursor's
        pass: u64,
        /// When it finished, by its driver's clock, in milliseconds since the Unix epoch
        finished_ms: u64,
    },
}

impl PlacementCommand {
    /// The bytes a command is replicated as, fixed little-endian fields behind a tag
    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        let (consumer, pg) = self.key();
        let mut out = Vec::with_capacity(48);
        // the tag, then the group every command names, then the command's own fields
        let tag = match self {
            PlacementCommand::MoveBegin { .. } => 1u8,
            PlacementCommand::MoveSwitch { .. } => 2,
            PlacementCommand::MoveAbort { .. } => 3,
            PlacementCommand::BackfillDone { .. } => 4,
            PlacementCommand::ScrubBegin { .. } => 5,
            PlacementCommand::ScrubStep { .. } => 6,
            PlacementCommand::ScrubEnd { .. } => 7,
            PlacementCommand::ReleaseDone { .. } => 8,
        };
        out.push(tag);
        out.extend_from_slice(&consumer.to_le_bytes());
        out.extend_from_slice(&pg.tablet.to_le_bytes());
        out.extend_from_slice(&pg.sub.to_le_bytes());
        match self {
            PlacementCommand::MoveBegin {
                id,
                expect,
                generation,
                positions,
                ..
            } => {
                out.extend_from_slice(&id.to_le_bytes());
                out.extend_from_slice(&expect.to_le_bytes());
                out.extend_from_slice(&generation.to_le_bytes());
                // a stripe is at most sixteen positions wide, so its count fits a byte
                #[allow(clippy::cast_possible_truncation)]
                out.push(positions.len() as u8);
                out.extend_from_slice(positions);
            }
            PlacementCommand::MoveSwitch { id, left, .. } | PlacementCommand::MoveAbort { id, left, .. } => {
                out.extend_from_slice(&id.to_le_bytes());
                // a group is at most sixteen positions wide, so the slices it leaves fit a byte
                #[allow(clippy::cast_possible_truncation)]
                out.push(left.len() as u8);
                for slice in left {
                    put_slice(&mut out, *slice);
                }
            }
            PlacementCommand::ReleaseDone { slice, .. } => put_slice(&mut out, *slice),
            PlacementCommand::BackfillDone {
                position, drops, ..
            } => {
                out.push(*position);
                out.extend_from_slice(&drops.to_le_bytes());
            }
            PlacementCommand::ScrubBegin {
                kind,
                pass,
                request,
                asked,
                began_ms,
                ..
            } => {
                out.push(kind.byte());
                out.push(u8::from(*asked));
                out.extend_from_slice(&pass.to_le_bytes());
                out.extend_from_slice(&request.to_le_bytes());
                out.extend_from_slice(&began_ms.to_le_bytes());
            }
            PlacementCommand::ScrubStep {
                kind, pass, after, ..
            } => {
                out.push(kind.byte());
                out.extend_from_slice(&pass.to_le_bytes());
                // the place the cursor was past, if it was past any
                let (consumer, pg) = after.unwrap_or_default();
                out.push(u8::from(after.is_some()));
                out.extend_from_slice(&consumer.to_le_bytes());
                out.extend_from_slice(&pg.tablet.to_le_bytes());
                out.extend_from_slice(&pg.sub.to_le_bytes());
            }
            PlacementCommand::ScrubEnd {
                kind,
                pass,
                finished_ms,
                ..
            } => {
                out.push(kind.byte());
                out.extend_from_slice(&pass.to_le_bytes());
                out.extend_from_slice(&finished_ms.to_le_bytes());
            }
        }
        out
    }

    /// Read a command back out of its bytes, or none if they are not one whole
    ///
    /// # Arguments
    ///
    /// * `raw` - The bytes [`PlacementCommand::encode`] wrote
    #[must_use]
    pub fn decode(raw: &[u8]) -> Option<Self> {
        // a cursor over the bytes that fails rather than reading past them
        let mut at = 0usize;
        let mut take = |len: usize| -> Option<&[u8]> {
            let bytes = raw.get(at..at + len)?;
            at += len;
            Some(bytes)
        };
        let word = |bytes: &[u8]| u64::from_le_bytes(bytes.try_into().expect("eight bytes"));
        let half = |bytes: &[u8]| u16::from_le_bytes(bytes.try_into().expect("two bytes"));
        let tag = take(1)?[0];
        let consumer = word(take(8)?);
        let pg = PlacementGroup {
            tablet: half(take(2)?),
            sub: half(take(2)?),
        };
        let cmd = match tag {
            1 => {
                let id = word(take(8)?);
                let expect = word(take(8)?);
                let generation = word(take(8)?);
                let count = usize::from(take(1)?[0]);
                let positions = take(count)?.to_vec();
                PlacementCommand::MoveBegin {
                    consumer,
                    pg,
                    id,
                    expect,
                    generation,
                    positions,
                }
            }
            2 | 3 => {
                let id = word(take(8)?);
                let count = usize::from(take(1)?[0]);
                let mut left = Vec::with_capacity(count);
                for _ in 0..count {
                    left.push(slice_of(take(SLICE_BYTES)?)?);
                }
                if tag == 2 {
                    PlacementCommand::MoveSwitch { consumer, pg, id, left }
                } else {
                    PlacementCommand::MoveAbort { consumer, pg, id, left }
                }
            }
            8 => PlacementCommand::ReleaseDone {
                consumer,
                pg,
                slice: slice_of(take(SLICE_BYTES)?)?,
            },
            4 => PlacementCommand::BackfillDone {
                consumer,
                pg,
                position: take(1)?[0],
                drops: word(take(8)?),
            },
            5 => {
                let kind = ScrubKind::from_byte(take(1)?[0])?;
                let asked = take(1)?[0] != 0;
                PlacementCommand::ScrubBegin {
                    consumer,
                    pg,
                    kind,
                    pass: word(take(8)?),
                    request: word(take(8)?),
                    asked,
                    began_ms: word(take(8)?),
                }
            }
            6 => {
                let kind = ScrubKind::from_byte(take(1)?[0])?;
                let pass = word(take(8)?);
                let some = take(1)?[0] != 0;
                let after_consumer = word(take(8)?);
                let after_pg = PlacementGroup {
                    tablet: half(take(2)?),
                    sub: half(take(2)?),
                };
                PlacementCommand::ScrubStep {
                    consumer,
                    pg,
                    kind,
                    pass,
                    after: some.then_some((after_consumer, after_pg)),
                }
            }
            7 => PlacementCommand::ScrubEnd {
                consumer,
                pg,
                kind: ScrubKind::from_byte(take(1)?[0])?,
                pass: word(take(8)?),
                finished_ms: word(take(8)?),
            },
            _ => return None,
        };
        // every byte is the command's, or it is not one
        (at == raw.len()).then_some(cmd)
    }

    /// The placement group the command is about
    #[must_use]
    pub fn key(&self) -> PgKey {
        match self {
            PlacementCommand::MoveBegin { consumer, pg, .. }
            | PlacementCommand::MoveSwitch { consumer, pg, .. }
            | PlacementCommand::MoveAbort { consumer, pg, .. }
            | PlacementCommand::BackfillDone { consumer, pg, .. }
            | PlacementCommand::ScrubBegin { consumer, pg, .. }
            | PlacementCommand::ScrubStep { consumer, pg, .. }
            | PlacementCommand::ScrubEnd { consumer, pg, .. }
            | PlacementCommand::ReleaseDone { consumer, pg, .. } => (*consumer, *pg),
        }
    }

    /// Whether this is a scrub's command, which changes nothing a writer reads
    #[must_use]
    pub const fn is_scrub(&self) -> bool {
        matches!(
            self,
            PlacementCommand::ScrubBegin { .. }
                | PlacementCommand::ScrubStep { .. }
                | PlacementCommand::ScrubEnd { .. }
        )
    }
}

/// The bytes a slice is written as in a command: its device's id and its index
const SLICE_BYTES: usize = 18;

/// Write a slice into a command's bytes
///
/// # Arguments
///
/// * `out` - Where it goes
/// * `slice` - The slice
fn put_slice(out: &mut Vec<u8>, slice: SliceRef) {
    out.extend_from_slice(slice.device.0.as_bytes());
    out.extend_from_slice(&slice.index.to_le_bytes());
}

/// Read a slice from a command's bytes
///
/// # Arguments
///
/// * `raw` - Its [`SLICE_BYTES`] bytes
fn slice_of(raw: &[u8]) -> Option<SliceRef> {
    let device = uuid::Uuid::from_slice(raw.get(..16)?).ok()?;
    let index = u16::from_le_bytes(raw.get(16..18)?.try_into().ok()?);
    Some(SliceRef {
        device: DeviceId(device),
        index,
    })
}

/// Why a placement command changed nothing
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PlacementRefusal {
    /// A begin read another epoch than the group's
    Epoch,
    /// A begin found a move in flight already
    Moving,
    /// A switch or an abort named a move that is not the one in flight
    NotMoving,
    /// A begin's positions are not a permutation of the new generation's answer
    NotPermutation,
    /// A backfill found a stripe dropped while it walked
    Dropped,
    /// A scrub's command does not follow its cursor: another driver moved it since it was read
    ScrubCursor,
    /// A release named a slice the group does not name as left (M20)
    NotLeft,
}

impl std::fmt::Display for PlacementRefusal {
    /// Write the refusal as the reason it gives
    ///
    /// # Arguments
    ///
    /// * `f` - The formatter
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // one phrase a reason
        f.write_str(match self {
            PlacementRefusal::Epoch => "the placement group's epoch moved since it was read",
            PlacementRefusal::Moving => "the placement group is moving already",
            PlacementRefusal::NotMoving => "the placement group is not in that move",
            PlacementRefusal::NotPermutation => "the positions are not a permutation",
            PlacementRefusal::Dropped => "the record dropped a stripe while the backfill walked",
            PlacementRefusal::ScrubCursor => "the scrub's cursor moved since it was read",
            PlacementRefusal::NotLeft => "the placement group names no such slice as left",
        })
    }
}

/// What the missed record reads of a committed stripe row, kept so a change can be replayed
///
/// The row's labels, whether it was reclaimed, what its commit changed and the label it named
/// before: [`PlacementState::derive`] reads nothing else, so this is all a tablet group keeps of a
/// commit between its checkpoint and its applied position.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RecordInput {
    /// The label each position's chunk must carry, marked where its holder missed it
    pub labels: Vec<Label>,
    /// Whether reclamation reclaimed the stripe
    pub tombstone: bool,
    /// What the commit changed of its chunks
    pub changed: ChangedUnits,
    /// The label the row named before the commit
    pub prior: Label,
}

impl RecordInput {
    /// What the record reads of a row
    ///
    /// # Arguments
    ///
    /// * `row` - The committed row
    #[must_use]
    pub fn of(row: &StripeRow) -> Self {
        RecordInput {
            labels: row.labels.clone(),
            tombstone: row.tombstone,
            changed: row.changed,
            prior: row.prior,
        }
    }
}

/// One change a tablet group's apply made to its placement state, at a log index
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum PlacementChange {
    /// A stripe commit applied, from which the missed record is derived
    Derive {
        /// The stripe's placement group
        key: PgKey,
        /// The stripe
        stripe: StripeKey,
        /// What the committed row says
        row: RecordInput,
        /// The bound the apply derived under
        bound: usize,
    },
    /// A placement command applied, accepted or refused
    Command(PlacementCommand),
}

/// A tablet group's placement state as of its checkpoint, and every change applied since
///
/// The state a stripe commit's claim is judged against has to be the one its apply saw, on every
/// replica and on every replay. The log above the checkpoint is replayed at open, so the state kept
/// with the checkpoint is the checkpoint's own and never the applied position's; a snapshot cut
/// between the two is the checkpoint's state with every change at or below its boundary.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct PlacementHistory {
    /// The state as of the checkpoint
    pub base: PlacementState,
    /// Every change applied above the checkpoint, oldest first, by its log index
    pub changes: Vec<(u64, PlacementChange)>,
}

impl PlacementHistory {
    /// The state as of an index at or above the checkpoint
    ///
    /// # Arguments
    ///
    /// * `index` - The boundary
    #[must_use]
    pub fn at(&self, index: u64) -> PlacementState {
        let mut state = self.base.clone();
        for (_, change) in self.changes.iter().take_while(|(at, _)| *at <= index) {
            state.replay(change);
        }
        state
    }
}

/// Every placement group's state a tablet group holds, by group
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct PlacementState {
    /// Each group's state, for a group that is not at its base
    #[serde(with = "pairs")]
    pub groups: BTreeMap<PgKey, PgState>,
    /// Where each kind of scrub is in the tablet group's passes (F89)
    #[serde(default)]
    pub scrub: ScrubState,
}

impl PlacementState {
    /// A group's state, if it is not at its base
    ///
    /// # Arguments
    ///
    /// * `key` - The group
    #[must_use]
    pub fn get(&self, key: &PgKey) -> Option<&PgState> {
        self.groups.get(key)
    }

    /// The epoch a group is at: what a stripe commit's claim is compared with
    ///
    /// # Arguments
    ///
    /// * `key` - The group
    #[must_use]
    pub fn epoch(&self, key: &PgKey) -> u64 {
        self.groups.get(key).map_or(0, |state| state.epoch)
    }

    /// A group's state to change, made at its base if it has none
    ///
    /// # Arguments
    ///
    /// * `key` - The group
    fn entry(&mut self, key: PgKey) -> &mut PgState {
        self.groups.entry(key).or_default()
    }

    /// Forget a group that is back at its base, so nothing is kept for it
    ///
    /// # Arguments
    ///
    /// * `key` - The group
    fn tidy(&mut self, key: &PgKey) {
        if self.groups.get(key).is_some_and(PgState::is_base) {
            self.groups.remove(key);
        }
    }

    /// Apply a placement command in committed order, judging its condition here
    ///
    /// # Arguments
    ///
    /// * `cmd` - The command
    ///
    /// # Errors
    ///
    /// What the command found that it was refused for; nothing is changed then.
    pub fn apply(&mut self, cmd: &PlacementCommand) -> Result<(), PlacementRefusal> {
        let key = cmd.key();
        let current = self.groups.get(&key).cloned().unwrap_or_default();
        match cmd {
            PlacementCommand::MoveBegin {
                id,
                expect,
                generation,
                positions,
                ..
            } => {
                // one move at a time, from the epoch its driver read
                if current.moving.is_some() {
                    return Err(PlacementRefusal::Moving);
                }
                if current.epoch != *expect {
                    return Err(PlacementRefusal::Epoch);
                }
                // the positions name each of the new answer's members once
                let mut seen = positions.clone();
                seen.sort_unstable();
                seen.dedup();
                if seen.len() != positions.len()
                    || seen.last().is_some_and(|last| usize::from(*last) >= positions.len())
                {
                    return Err(PlacementRefusal::NotPermutation);
                }
                let state = self.entry(key);
                state.moving = Some(Moving {
                    id: *id,
                    generation: *generation,
                    positions: positions.clone(),
                });
                state.epoch += 1;
                Ok(())
            }
            PlacementCommand::MoveSwitch { id, left, .. } => match &current.moving {
                // the generation and the positions move together, in this one commit
                Some(moving) if moving.id == *id => {
                    let state = self.entry(key);
                    let moving = state.moving.take().expect("checked");
                    state.generation = Some(moving.generation);
                    state.positions = Some(moving.positions);
                    state.epoch += 1;
                    // the old holders it left are named until each is released (M20)
                    for slice in left {
                        if !state.left.contains(slice) {
                            state.left.push(*slice);
                        }
                    }
                    Ok(())
                }
                _ => Err(PlacementRefusal::NotMoving),
            },
            PlacementCommand::MoveAbort { id, left, .. } => match &current.moving {
                Some(moving) if moving.id == *id => {
                    let state = self.entry(key);
                    state.moving = None;
                    state.epoch += 1;
                    // and the arrivals it may have written to (M20)
                    for slice in left {
                        if !state.left.contains(slice) {
                            state.left.push(*slice);
                        }
                    }
                    Ok(())
                }
                _ => Err(PlacementRefusal::NotMoving),
            },
            PlacementCommand::ReleaseDone { slice, .. } => {
                // a slice the group names once, released once
                if !current.left.contains(slice) {
                    return Err(PlacementRefusal::NotLeft);
                }
                let state = self.entry(key);
                state.left.retain(|left| left != slice);
                self.tidy(&key);
                Ok(())
            }
            PlacementCommand::ScrubBegin {
                kind,
                pass,
                request,
                asked,
                began_ms,
                ..
            } => {
                // a pass begins one past the last, whether or not the last finished
                let cursor = self.scrub.cursor_mut(*kind);
                if *pass != cursor.pass + 1 {
                    return Err(PlacementRefusal::ScrubCursor);
                }
                cursor.pass = *pass;
                cursor.after = None;
                cursor.done = 0;
                cursor.finished = false;
                cursor.request = *request;
                cursor.asked = *asked;
                cursor.began_ms = *began_ms;
                Ok(())
            }
            PlacementCommand::ScrubStep {
                kind, pass, after, ..
            } => {
                // the cursor moves past the group the step finished, forward and from where its
                // driver read it, so no group is skipped and none counted twice
                let cursor = self.scrub.cursor_mut(*kind);
                if *pass != cursor.pass || !cursor.running() || cursor.after != *after || Some(key) <= *after {
                    return Err(PlacementRefusal::ScrubCursor);
                }
                cursor.after = Some(key);
                cursor.done += 1;
                Ok(())
            }
            PlacementCommand::ScrubEnd {
                kind,
                pass,
                finished_ms,
                ..
            } => {
                let cursor = self.scrub.cursor_mut(*kind);
                if *pass != cursor.pass || !cursor.running() {
                    return Err(PlacementRefusal::ScrubCursor);
                }
                cursor.finished = true;
                cursor.served = Some(cursor.request);
                cursor.last_ms = *finished_ms;
                Ok(())
            }
            PlacementCommand::BackfillDone {
                position, drops, ..
            } => {
                let Some(part) = current.record.get(usize::from(*position)) else {
                    return Ok(());
                };
                // the record is whole again only if nothing was dropped while the backfill walked
                if part.drops != *drops {
                    return Err(PlacementRefusal::Dropped);
                }
                if let Some(part) = self
                    .groups
                    .get_mut(&key)
                    .and_then(|state| state.record.get_mut(usize::from(*position)))
                {
                    part.overflowed = false;
                }
                self.tidy(&key);
                Ok(())
            }
        }
    }

    /// Derive the missed record from a committed stripe row
    ///
    /// A position the row leaves stale is recorded with what the commit changed there, and one it
    /// leaves current, or a reclaimed row, is dropped. An entry begins at the label the row named
    /// before its first commit; a position whose part is at the bound drops a stripe with no entry,
    /// and says so.
    ///
    /// Returns whether the state may have changed: false only when the row leaves nothing stale in
    /// a group with no record, which is every commit while nothing is stale.
    ///
    /// # Arguments
    ///
    /// * `key` - The stripe's placement group
    /// * `stripe` - The stripe
    /// * `row` - The row its commit produced
    /// * `bound` - How many stripes one position's part may hold
    pub fn derive(&mut self, key: PgKey, stripe: StripeKey, row: &StripeRow, bound: usize) -> bool {
        self.derive_from(key, stripe, &RecordInput::of(row), bound)
    }

    /// Derive the missed record from what a committed row says, as [`PlacementState::derive`] does
    ///
    /// # Arguments
    ///
    /// * `key` - The stripe's placement group
    /// * `stripe` - The stripe
    /// * `row` - What the row its commit produced says
    /// * `bound` - How many stripes one position's part may hold
    pub fn derive_from(
        &mut self,
        key: PgKey,
        stripe: StripeKey,
        row: &RecordInput,
        bound: usize,
    ) -> bool {
        let width = row.labels.len();
        let stale = |pos: usize| !row.tombstone && row.labels[pos].is_missed();
        // a row that leaves nothing stale, for a group with no record, keeps nothing
        if !(0..width).any(stale) && !self.groups.contains_key(&key) {
            return false;
        }
        let changed = row.changed;
        let state = self.entry(key);
        for pos in 0..width {
            if !stale(pos) {
                if let Some(part) = state.record.get_mut(pos) {
                    part.stripes.remove(&stripe);
                }
                continue;
            }
            // a position this commit did not act on keeps its entry, or its absence
            if !changed.acted_on(pos) || changed.is_none() {
                continue;
            }
            let label = row.labels[pos].unmarked();
            let part = state.part(pos, width);
            match part.stripes.get_mut(&stripe) {
                Some(Missed::Whole) => {}
                Some(Missed::Ranges { writes, .. }) => {
                    if changed.is_whole() || writes.len() >= MAX_MISSED_WRITES {
                        part.stripes.insert(stripe, Missed::Whole);
                    } else {
                        writes.push((label, changed.first, changed.count));
                    }
                }
                None => {
                    // the bound: a stripe past it is dropped, and the record says it overflowed
                    if part.stripes.len() >= bound {
                        part.overflowed = true;
                        part.drops += 1;
                        continue;
                    }
                    let entry = if changed.is_whole() {
                        Missed::Whole
                    } else {
                        Missed::Ranges {
                            since: row.prior,
                            writes: vec![(label, changed.first, changed.count)],
                        }
                    };
                    part.stripes.insert(stripe, entry);
                }
            }
        }
        self.tidy(&key);
        true
    }

    /// Apply one change the tablet group committed, as the apply that committed it did
    ///
    /// Returns whether the state may have changed.
    ///
    /// # Arguments
    ///
    /// * `change` - The change
    pub fn replay(&mut self, change: &PlacementChange) -> bool {
        match change {
            PlacementChange::Derive {
                key,
                stripe,
                row,
                bound,
            } => self.derive_from(*key, *stripe, row, *bound),
            PlacementChange::Command(cmd) => self.apply(cmd).is_ok(),
        }
    }

    /// What a writer needs of a group's state, its base's when it has none
    ///
    /// # Arguments
    ///
    /// * `key` - The group
    #[must_use]
    pub fn view(&self, key: &PgKey) -> PgView {
        self.groups.get(key).map(PgState::view).unwrap_or_default()
    }

    /// Every group whose record holds a stripe a position is stale on, or overflowed there
    #[must_use]
    pub fn stale_groups(&self) -> Vec<PgKey> {
        self.groups
            .iter()
            .filter(|(_, state)| {
                state
                    .record
                    .iter()
                    .any(|part| !part.stripes.is_empty() || part.overflowed)
            })
            .map(|(key, _)| *key)
            .collect()
    }
}

#[cfg(test)]
mod tests;
