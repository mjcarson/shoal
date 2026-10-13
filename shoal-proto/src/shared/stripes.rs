//! A stripe's row as the object layer reasons about it, and every change a commit makes to it
//!
//! A stripe written in place has a `{Marker}StripeMeta` row, and every write to the stripe is a
//! conditional commit of that row in its tablet group ([S7](../../../docs/src/object-storage/write-path.md)).
//! The commit is the whole next row, computed here from the row its writer read, and is applied
//! only if the row's sequence is still the one read: so every change below is a pure function of
//! the row read, and the group's condition is what makes it the next state
//! ([F86](../../../docs/src/features/replicated-pools.md)).
//!
//! The functions follow the stripe model's apply rules (`shoal-model/src/stripe/group.rs`) with
//! one difference of encoding. The model keeps a `missed` flag beside each position's label; the
//! product keeps no such field in the row ([Q25](../../../docs/src/object-storage/contract.md)),
//! and marks the label itself: a position whose holder missed a write carries that write's label
//! with [`MISSED_BIT`] set, which no holder can hold. Every rule the model reads as a position's
//! label compares [`Label::unmarked`]; every rule it reads as the `missed` flag reads
//! [`Label::is_missed`]. `shoal-proto/tests/stripe_model_conformance.rs` holds the two encodings
//! to each other over the model's saved schedules.
//!
//! # Invariants
//!
//! **No write's tag carries [`MISSED_BIT`].** A tag is minted by [`write_tag`], which clears it.
//! A frame or a store call carrying a marked label is refused wherever one arrives.
//!
//! **A commit is a function of the row read and nothing else.** The group compares the sequence
//! alone (F68's equality on one field), which is enough only because every change here moves the
//! sequence; a new change that did not would let a writer commit over a row it never read.

use uuid::Uuid;

use crate::shared::checksum::{combine, crc64};
use crate::shared::objects::{Label, ObjectEntry, PendingBytes, PendingUnit, MISSED_BIT};
use crate::shared::poolmap::PlacementGroup;

/// The seed a write's tag and its commit's identity are hashed under
///
/// Frozen: a retried write on another node derives its commit's identity again, and has to derive
/// the same one for the group's retry table to recognise it.
pub const STRIPE_IDENTITY_SEED: i64 = 0x5354_5249_5045; // "STRIPE"

/// The bytes a commit's identity starts with, so one never collides with a client's v7 bundle
const COMMIT_MARKER: [u8; 6] = *b"STRCMT";

/// The tag of one try of a write to one stripe, as one coordinating task makes it
///
/// A tag a try, so two tries of one write never stage different bytes under one label
/// ([X1](../../../docs/src/object-storage/stripe-model.md#what-the-search-found-and-the-repairs)),
/// and a task's own nonce beside the try, so two coordinators of one retried write never do
/// either. Never zero, which is the put's, and never with [`MISSED_BIT`] set.
///
/// # Arguments
///
/// * `op` - The write's identity
/// * `stripe` - The stripe it writes
/// * `attempt` - Which try of it
/// * `nonce` - The coordinating task's own nonce
#[must_use]
pub fn write_tag(op: &Uuid, stripe: u64, attempt: u32, nonce: u64) -> u64 {
    // the identity, the stripe, the try and the task, hashed together under the frozen seed
    let mut bytes = [0u8; 36];
    bytes[..16].copy_from_slice(op.as_bytes());
    bytes[16..24].copy_from_slice(&stripe.to_le_bytes());
    bytes[24..28].copy_from_slice(&attempt.to_le_bytes());
    bytes[28..36].copy_from_slice(&nonce.to_le_bytes());
    let tag = gxhash::gxhash64(&bytes, STRIPE_IDENTITY_SEED) & !MISSED_BIT;
    // zero is the put's tag, so a write never takes it
    if tag == 0 {
        1
    } else {
        tag
    }
}

/// The identity of one try's commit of one stripe, the same on every node that derives it
///
/// The group's retry table remembers a commit by this, so the same try proposed again is answered
/// as it was the first time, and a retry of the write on another coordinator learns which of its
/// tries committed by proposing them in turn ([F86](../../../docs/src/features/replicated-pools.md)).
/// Unlike a tag it names no task: two coordinators of one write share their tries' identities.
///
/// # Arguments
///
/// * `op` - The write's identity
/// * `stripe` - The stripe
/// * `attempt` - Which try
#[must_use]
pub fn commit_bundle(op: &Uuid, stripe: u64, attempt: u32) -> Uuid {
    // the identity, the stripe and the try, hashed under the frozen seed
    let mut bytes = [0u8; 28];
    bytes[..16].copy_from_slice(op.as_bytes());
    bytes[16..24].copy_from_slice(&stripe.to_le_bytes());
    bytes[24..28].copy_from_slice(&attempt.to_le_bytes());
    let hash = gxhash::gxhash128(&bytes, STRIPE_IDENTITY_SEED).to_le_bytes();
    // a v8 identity behind a marker no client's v7 bundle can start with, its version and
    // variant bits set as F80's lane ids set them
    let mut id = [0u8; 16];
    id[..6].copy_from_slice(&COMMIT_MARKER);
    id[6..].copy_from_slice(&hash[..10]);
    id[6] = (id[6] & 0x0f) | 0x80;
    id[8] = (id[8] & 0x3f) | 0x80;
    Uuid::from_bytes(id)
}

/// The bytes a unit's identity is written as: consumer, object, stripe, position and unit index
pub const IDENTITY_BYTES: usize = 8 + 16 + 8 + 1 + 4;

/// A stripe chunk's identity: whose stripe, which position of it
///
/// What a slice files a chunk under and what an object lane's frame names it by
/// ([S6](../../../docs/src/object-storage/device-store.md#a-stripe-chunk)).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ChunkId {
    /// The consumer the stripe's bucket is bound as
    pub consumer: u64,
    /// The placement group the stripe falls in
    pub pg: PlacementGroup,
    /// The object the stripe is part of
    pub object: u128,
    /// The stripe's index in its object
    pub stripe: u64,
    /// The chunk's position in its stripe
    pub position: u8,
}

impl ChunkId {
    /// The chunk's path under a slice's `chunks/` directory
    ///
    /// `<consumer>/<tablet>-<sub>/<object>/<stripe>.<position>`, so a directory listing is an
    /// inventory of a placement group on a slice, and removing an object's directory is removing
    /// its chunks there.
    #[must_use]
    pub fn relative(&self) -> std::path::PathBuf {
        std::path::PathBuf::from(format!("{:016x}", self.consumer))
            .join(format!("{:03x}-{:04x}", self.pg.tablet, self.pg.sub))
            .join(format!("{:032x}", self.object))
            .join(format!("{:016x}.{}", self.stripe, self.position))
    }

    /// The identity a unit's checksum is bound to
    ///
    /// # Arguments
    ///
    /// * `unit` - The unit's index in the chunk
    #[must_use]
    pub fn identity(&self, unit: u32) -> [u8; IDENTITY_BYTES] {
        let mut bytes = [0u8; IDENTITY_BYTES];
        bytes[0..8].copy_from_slice(&self.consumer.to_le_bytes());
        bytes[8..24].copy_from_slice(&self.object.to_le_bytes());
        bytes[24..32].copy_from_slice(&self.stripe.to_le_bytes());
        bytes[32] = self.position;
        bytes[33..37].copy_from_slice(&unit.to_le_bytes());
        bytes
    }
}

/// The checksum a unit's bytes are stored under: their CRC followed by their identity's
///
/// # Arguments
///
/// * `crc` - The CRC of the unit's bytes, `crc64(unit)`
/// * `id` - The chunk
/// * `unit` - The unit's index
#[must_use]
pub fn bound_crc(crc: u64, id: &ChunkId, unit: u32) -> u64 {
    combine(crc, crc64(&id.identity(unit)), IDENTITY_BYTES as u64)
}

/// A stripe's row, apart from the schema that generated its type
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct StripeRow {
    /// The consumer whose stripe this is
    pub consumer: u64,
    /// The object the stripe is part of
    pub object: u128,
    /// Which stripe of the object this is
    pub stripe: u64,
    /// How many writes to this stripe have committed
    pub sequence: u64,
    /// The label each position's chunk must carry, marked where its holder missed it
    pub labels: Vec<Label>,
    /// How much of the stripe holds bytes
    pub length: u64,
    /// The object's truncate epoch when this row was last committed
    pub stamp: u64,
    /// The least truncate epoch a writer must have read to commit
    pub fence: u64,
    /// Whether reclamation reclaimed this stripe
    pub tombstone: bool,
    /// The bytes of small writes that rode in their commits
    pub pending: Option<Box<PendingBytes>>,
}

/// What a write did at one position, for its commit
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Touch {
    /// The write did not touch the position, which keeps its label
    Untouched,
    /// The write touched it and its holder holds the write: a synced stage, or for a small write
    /// in its commit a confirmation of the chunk its bytes are laid over
    Held,
    /// The write touched it and its holder did not answer that it holds it: marked missed
    Missed,
}

/// What a write's commit changes in its stripe's row
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WriteCommit {
    /// The write's label: the row's next sequence and the write's tag
    pub label: Label,
    /// What it did at each position
    pub positions: Vec<Touch>,
    /// The truncate epoch its writer read, which the row is stamped with
    pub stamp: u64,
    /// How much of the stripe holds bytes after it
    pub length: u64,
    /// For a small write in its commit, the bytes the row holds after it; a staged write carried
    /// any that were pending in its stages, so it leaves none
    pub pending: Option<PendingBytes>,
}

impl StripeRow {
    /// The state of a stripe its object's put wrote and nothing has changed: the row it would
    /// have if it had one
    ///
    /// # Arguments
    ///
    /// * `consumer` - The consumer
    /// * `object` - The object
    /// * `stripe` - The stripe
    /// * `width` - How many positions the stripe has
    /// * `length` - How much of the stripe the put wrote
    #[must_use]
    pub fn put(consumer: u64, object: u128, stripe: u64, width: usize, length: u64) -> Self {
        // every position at the put's label, at sequence zero, stamped at the object's first epoch
        StripeRow {
            consumer,
            object,
            stripe,
            sequence: 0,
            labels: vec![Label::PUT; width],
            length,
            stamp: 0,
            fence: 0,
            tombstone: false,
            pending: None,
        }
    }

    /// The row's key: its consumer, object and stripe
    #[must_use]
    pub fn key(&self) -> (u64, u128, u64) {
        (self.consumer, self.object, self.stripe)
    }

    /// The label the row names at a position, without any mark: the write it must hold
    ///
    /// # Arguments
    ///
    /// * `pos` - The position
    #[must_use]
    pub fn named(&self, pos: usize) -> Label {
        self.labels[pos].unmarked()
    }

    /// Whether the row calls a position's chunk current: not reclaimed, and not marked missed
    ///
    /// # Arguments
    ///
    /// * `pos` - The position
    #[must_use]
    pub fn current(&self, pos: usize) -> bool {
        !self.tombstone && !self.labels[pos].is_missed()
    }

    /// Whether a chunk at a label is the position's state the row names, pending bytes laid over
    /// it included: the label named, or a label pending bytes fold from into it
    ///
    /// # Arguments
    ///
    /// * `pos` - The position
    /// * `held` - The label the chunk is at
    #[must_use]
    pub fn makes(&self, pos: usize, held: Label) -> bool {
        // the label itself, or a source of the pending bytes that make it
        held == self.named(pos)
            || self.pending.as_ref().is_some_and(|pending| {
                pending.label == self.named(pos) && pending.folds_from(pos, held)
            })
    }

    /// The labels a holder may hold for a position to be current: the one named, then the ones
    /// pending bytes fold from into it
    ///
    /// # Arguments
    ///
    /// * `pos` - The position
    #[must_use]
    pub fn current_labels(&self, pos: usize) -> Vec<Label> {
        let mut labels = vec![self.named(pos)];
        // the pending bytes' sources make the named label once they are laid over
        if let Some(pending) = &self.pending {
            if pending.label == self.named(pos) {
                labels.extend(pending.sources(pos));
            }
        }
        labels
    }

    /// Whether no write has changed what this row names since the put: every label still the
    /// put's, nothing pending, nothing reclaimed
    ///
    /// A row a no-op or a fence made over a stripe the put never wrote is such a row, and it is
    /// still a hole ([F86](../../../docs/src/features/replicated-pools.md)).
    #[must_use]
    pub fn names_only_the_put(&self) -> bool {
        !self.tombstone
            && self.pending.is_none()
            && self.labels.iter().all(|label| label.unmarked() == Label::PUT)
    }

    /// The row after a write's commit
    ///
    /// The sequence moves, every position the write touched takes its label (marked where its
    /// holder missed it), the row is stamped with the epoch its writer read, and its pending bytes
    /// become what the write leaves.
    ///
    /// # Arguments
    ///
    /// * `write` - What the write commits
    #[must_use]
    pub fn commit(&self, write: &WriteCommit) -> Self {
        let mut next = self.clone();
        // a write un-reclaims a reclaimed stripe, and moves the sequence
        next.tombstone = false;
        next.sequence = self.sequence + 1;
        // each position it touched takes its label, marked where the holder did not take it
        for (pos, touch) in write.positions.iter().enumerate() {
            match touch {
                Touch::Untouched => {}
                Touch::Held => next.labels[pos] = write.label.unmarked(),
                Touch::Missed => next.labels[pos] = write.label.unmarked().marked(),
            }
        }
        // P13: stamped with the epoch its writer read
        next.stamp = write.stamp;
        next.length = write.length;
        next.pending = write.pending.clone().map(Box::new);
        next
    }

    /// The row after a no-op, which moves the sequence and changes no label
    ///
    /// The fact that excludes a stage whose stager died before proposing: once the sequence is
    /// past it, no commit can name it ([P16](../../../docs/src/object-storage/contract.md)).
    #[must_use]
    pub fn noop(&self) -> Self {
        let mut next = self.clone();
        // only the sequence moves
        next.sequence = self.sequence + 1;
        next
    }

    /// The row after a truncate fences it at the epoch it is about to commit
    ///
    /// # Arguments
    ///
    /// * `epoch` - The epoch the truncate will commit
    #[must_use]
    pub fn fence(&self, epoch: u64) -> Self {
        let mut next = self.clone();
        // the sequence moves, so every write staged before the fence is refused
        next.sequence = self.sequence + 1;
        next.fence = self.fence.max(epoch);
        next
    }

    /// The row after its pending bytes are cleared, once enough holders hold their label
    ///
    /// Every position whose holder said it holds the label is current, and every other is marked
    /// missed ([P17](../../../docs/src/object-storage/contract.md)).
    ///
    /// # Arguments
    ///
    /// * `holding` - Whether each position's holder said, in the clear's round, that it holds the label
    #[must_use]
    pub fn clear(&self, holding: &[bool]) -> Self {
        let mut next = self.clone();
        // a clear moves the sequence, as every change to what a holder may serve does
        next.sequence = self.sequence + 1;
        next.pending = None;
        // the holders that hold it are current, and every other is missed
        for (pos, holds) in holding.iter().enumerate() {
            let named = self.named(pos);
            next.labels[pos] = if *holds { named } else { named.marked() };
        }
        next
    }

    /// The row after a rebuild made a position current, or a move left it missed
    ///
    /// # Arguments
    ///
    /// * `pos` - The position
    /// * `current` - Whether its slice now holds the chunk
    #[must_use]
    pub fn rebuilt(&self, pos: usize, current: bool) -> Self {
        let mut next = self.clone();
        // only the position's mark changes
        let named = self.named(pos);
        next.labels[pos] = if current { named } else { named.marked() };
        next
    }

    /// The row after reclamation reclaims a stripe a floor hides: a tombstone, never deleted
    ///
    /// # Arguments
    ///
    /// * `below` - The epoch of the floor the stripe sits under
    #[must_use]
    pub fn reclaimed(&self, below: u64) -> Self {
        let mut next = self.clone();
        // a sequence past the row it replaces, every chunk missed, stamped with the floor
        next.tombstone = true;
        next.sequence = self.sequence + 1;
        next.labels = self.labels.iter().map(|label| label.unmarked().marked()).collect();
        next.stamp = below;
        next.pending = None;
        next
    }
}

impl PendingBytes {
    /// The labels a holder's chunk can be at for these bytes to make the label they make
    ///
    /// # Arguments
    ///
    /// * `pos` - The position
    #[must_use]
    pub fn sources(&self, pos: usize) -> Vec<Label> {
        // the base, then every earlier write merged since it
        let mut sources = vec![self.base[pos].unmarked()];
        sources.extend(self.chain.iter().copied());
        sources
    }

    /// Whether a chunk at a label makes these bytes' label once they are laid over it
    ///
    /// # Arguments
    ///
    /// * `pos` - The position
    /// * `held` - The label the chunk is at
    #[must_use]
    pub fn folds_from(&self, pos: usize, held: Label) -> bool {
        self.sources(pos).contains(&held)
    }

    /// How many bytes these hold
    #[must_use]
    pub fn bytes(&self) -> usize {
        self.units.iter().map(|unit| unit.bytes.len()).sum()
    }

    /// The bytes a small write leaves in a row: its units merged over what is pending there
    ///
    /// Merged and never replaced: a holder still at the base folds every write merged since,
    /// so replacing them lost the earlier writes' units ([X1](../../../docs/src/object-storage/stripe-model.md#a-small-write-in-its-commit), P9).
    ///
    /// # Arguments
    ///
    /// * `row` - The row the write commits over
    /// * `label` - The write's label
    /// * `written` - The units it writes, any zeros it adds included
    #[must_use]
    pub fn after(row: &StripeRow, label: Label, written: &[PendingUnit]) -> Self {
        let Some(pending) = &row.pending else {
            // the first small write since the chunks were whole: the row's labels are the base
            let mut units = written.to_vec();
            units.sort_unstable_by_key(|unit| unit.index);
            return PendingBytes {
                label,
                base: row.labels.iter().map(|label| label.unmarked()).collect(),
                chain: Vec::new(),
                units,
            };
        };
        // what was pending and this write left alone, then this write's own
        let mut units: Vec<PendingUnit> = pending
            .units
            .iter()
            .filter(|unit| !written.iter().any(|w| w.index == unit.index))
            .cloned()
            .chain(written.iter().cloned())
            .collect();
        units.sort_unstable_by_key(|unit| unit.index);
        // the earlier write joins the chain its holders may already have made
        let mut chain = pending.chain.clone();
        chain.push(pending.label);
        PendingBytes {
            label,
            base: pending.base.clone(),
            chain,
            units,
        }
    }
}

/// What a reader shows of one stripe of an object
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Shown {
    /// Where the stripe starts in the object
    pub start: u64,
    /// Where it ends: the stripe's end or the object's, whichever is first
    pub end: u64,
    /// Where its bytes stop being read from a chunk: past this, up to `end`, it reads as zeros
    ///
    /// `start` for a hole, the first byte a floor hides for a stripe stamped below it, `end`
    /// otherwise ([S3](../../../docs/src/object-storage/objects.md#size-holes-and-truncate)).
    pub read_to: u64,
}

impl Shown {
    /// Whether nothing of the stripe is read from a chunk
    #[must_use]
    pub fn is_zeros(&self) -> bool {
        self.read_to <= self.start
    }
}

/// Whether a stripe is a hole: reclaimed, or past what the put wrote with nothing since naming
/// anything but the put
///
/// The stripe model's rule, `tombstone || (no row && start >= created)`, with one more case: a
/// row a no-op or a fence made over a stripe the put never wrote names only the put's label, which
/// no holder holds there, and is a hole as a stripe with no row is.
///
/// # Arguments
///
/// * `entry` - The object's entry
/// * `row` - The stripe's row, if it has one
/// * `stripe` - The stripe
#[must_use]
pub fn is_hole(entry: &ObjectEntry, row: Option<&StripeRow>, stripe: u64) -> bool {
    let start = stripe * u64::from(entry.geometry.stripe_bytes);
    match row {
        Some(row) if row.tombstone => true,
        Some(row) => start >= entry.put_length && row.names_only_the_put(),
        None => start >= entry.put_length,
    }
}

/// Where a stripe's bytes are hidden from: the first byte a floor hides, or its end
///
/// A floor hides a stripe stamped below its epoch from where it cut on
/// ([S3](../../../docs/src/object-storage/objects.md#size-holes-and-truncate)). A writer zeros
/// every unit from here that it does not write, and a reader reads nothing from here.
///
/// # Arguments
///
/// * `entry` - The object's entry
/// * `row` - The stripe's row, if it has one
/// * `stripe` - The stripe
#[must_use]
pub fn hidden_from(entry: &ObjectEntry, row: Option<&StripeRow>, stripe: u64) -> u64 {
    let stripe_bytes = u64::from(entry.geometry.stripe_bytes);
    let start = stripe * stripe_bytes;
    let end = start + stripe_bytes;
    // a stripe with no row is stamped as its put left it
    let stamp = row.map_or(0, |row| row.stamp);
    // the lowest cut of every floor this stripe is stamped below
    entry
        .floors
        .iter()
        .filter(|floor| floor.epoch > stamp && floor.length < end)
        .map(|floor| floor.length.max(start))
        .min()
        .unwrap_or(end)
}

/// What a reader shows of a stripe, or nothing if it starts past the object's end
///
/// # Arguments
///
/// * `entry` - The object's entry
/// * `row` - The stripe's row, if it has one
/// * `stripe` - The stripe
#[must_use]
pub fn shown(entry: &ObjectEntry, row: Option<&StripeRow>, stripe: u64) -> Option<Shown> {
    let stripe_bytes = u64::from(entry.geometry.stripe_bytes);
    let start = stripe * stripe_bytes;
    // nothing past the object's end is shown
    if start >= entry.size {
        return None;
    }
    let end = (start + stripe_bytes).min(entry.size);
    // a hole reads nothing; a floor hides what it cut
    let read_to = if is_hole(entry, row, stripe) {
        start
    } else {
        hidden_from(entry, row, stripe).min(end)
    };
    Some(Shown {
        start,
        end,
        read_to,
    })
}

/// The stripes a range of an object falls in
///
/// # Arguments
///
/// * `stripe_bytes` - The object's stripe size
/// * `offset` - Where the range starts
/// * `len` - How long it is
#[must_use]
pub fn stripes_of(stripe_bytes: u32, offset: u64, len: u64) -> std::ops::Range<u64> {
    let stripe_bytes = u64::from(stripe_bytes);
    // an empty range falls in no stripe
    if len == 0 {
        return 0..0;
    }
    let first = offset / stripe_bytes;
    let last = (offset + len - 1) / stripe_bytes;
    first..last + 1
}

#[cfg(test)]
mod tests;
