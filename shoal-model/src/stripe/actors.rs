//! The drivers: a stager for each write, a reader for each read, and the group's own drivers
//!
//! A stager is the coordinating shard of [S7](../../../docs/src/object-storage/write-path.md#the-preferred-direction-step-by-step):
//! it reads the entry strongly and the stripe's row at its leader, reads the chunks a partial write
//! needs, stages every touched chunk, proposes the conditional commit once enough chunks are
//! current, acknowledges, and tells the holders to apply. Several may act on one stripe, which is
//! Q15's preferred answer and the harder case. A reader is S9's, a rebuilder and a mover S10's, a
//! reclaimer S3's and S10's. None of them decides anything a holder or a group does not.

use std::collections::{BTreeMap, BTreeSet};

use crate::ids::OpId;
use crate::stripe::content::{decode, encode, lay_over, Change, Content, Unit};
use crate::stripe::event::{Body, ChunkReply, Endpoint, Payload, PgView};
use crate::stripe::group::{
    Decision, EntryCommand, EntryState, Evidence, Floor, PendingBytes, PgCommand, PgState,
    RowState, StripeCommand,
};
use crate::stripe::ids::{Epoch, Label, Pos, SliceId, StripeIx, Tag};
use crate::stripe::layout::UNITS;
use crate::stripe::oracle::{OpKind, ReadResult, StripeOutcome};
use crate::stripe::policy::{
    AckRule, BothRule, ClearRule, EntryRule, EpochRule, FloorDrop, HiddenRule, PendingRead,
    PutCheck, ReaderRule, RebuildRefusal, RebuildRule, ReleaseRule, Reservation, RowLevel,
    SmallWritePath, StagerWord, SwitchRule, TagRule, TruncateRule, UntouchedRule, WholeWrite,
    PENDING_BOUND,
};
use crate::stripe::world::{StripeWorld, LATE_PUT};

/// How many times a reader reads its row again before it fails by name
pub const MAX_REREADS: u8 = 3;

/// The chunks a driver is reading, and which it may still ask
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ChunkReads {
    /// The positions the row calls current, in the order they are tried
    pub candidates: Vec<Pos>,
    /// The next candidate to try
    pub next: usize,
    /// The ones asked and not yet answered
    pub pending: BTreeSet<Pos>,
    /// The ones answered at their label
    pub got: BTreeMap<Pos, (Label, Content)>,
    /// The slice each position is read from, once the placement group's state is the group's;
    /// empty for the row's own placement
    pub at: Vec<SliceId>,
}

/// Where a read of chunks stands after an answer
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReadStep {
    /// Waiting for more
    Wait,
    /// `k` chunks are in
    Done,
    /// A holder has a label newer than the row: the row is stale
    Newer(Pos, Label, Content),
    /// Every candidate was tried and fewer than `k` answered
    Exhausted,
}

/// A write's coordinator
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Stager {
    /// The stripe
    pub stripe: StripeIx,
    /// The data units the client writes
    pub units: Vec<u8>,
    /// Where it is
    pub phase: StagerPhase,
    /// The entry it read
    pub entry: Option<(u32, EntryState)>,
    /// The row it read
    pub row: Option<(u32, RowState)>,
    /// The chunks it reads for the old bytes a partial write needs
    pub reads: ChunkReads,
    /// The label it stages under
    pub label: Option<Label>,
    /// The data units it writes, zeros it adds included
    pub written: Vec<(u8, Unit)>,
    /// The positions it touches
    pub touched: Vec<Pos>,
    /// The touched positions that answered their stage
    pub staged: BTreeSet<Pos>,
    /// The touched positions that refused
    pub refused: BTreeSet<Pos>,
    /// The untouched positions' answers to a confirmation
    pub confirms: BTreeMap<Pos, bool>,
    /// Whether its bytes ride in its commit, with nothing staged (Q27)
    pub in_commit: bool,
    /// Whether every chunk it touches is staged whole, its old bytes read first (F96)
    pub whole_write: bool,
    /// The placement group's state it read with the row, once that is the group's (M16)
    pub pg: Option<PgView>,
    /// Each slice that answered its stage, by position, once a position may have two
    pub staged_at: BTreeSet<(Pos, SliceId)>,
    /// Each slice that refused its stage
    pub refused_at: BTreeSet<(Pos, SliceId)>,
    /// Each slice's answer to a confirmation
    pub confirms_at: BTreeMap<(Pos, SliceId), bool>,
}

/// Where a stager is
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StagerPhase {
    /// Reading the entry, strongly
    Entry,
    /// Waiting for the leader's turn
    Reserve,
    /// Reading the row at the leader
    Row,
    /// Reading old chunks
    Read,
    /// Staging
    Stage,
    /// Waiting for the commit's answer
    Commit,
    /// Waiting for the size's commit
    Extend,
    /// Moving the epoch past a fence a truncate left
    Advance,
}

/// A truncate's driver
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Truncator {
    /// The new length
    pub len: u32,
    /// Where it is
    pub phase: TruncatePhase,
    /// The epoch it read, which it commits at
    pub read: Option<Epoch>,
    /// The size it read, which decided its floor and its fence, and which it commits at
    pub read_size: Option<u32>,
}

/// Where a truncate is
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TruncatePhase {
    /// Reading the entry, strongly, for the epoch and the size
    Entry,
    /// Fencing the stripe its cut falls inside
    Fence,
    /// Committing to the entry
    Commit,
}

/// A read's driver
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Reader {
    /// The stripe
    pub stripe: StripeIx,
    /// Whether it is strong
    pub strong: bool,
    /// Whether it is reading the entry, the row, or chunks
    pub phase: ReaderPhase,
    /// The entry it read
    pub entry: Option<(u32, EntryState)>,
    /// The row it read
    pub row: Option<(u32, RowState)>,
    /// The chunks
    pub reads: ChunkReads,
    /// How many times it has read its row or entry again
    pub rereads: u8,
    /// The placement group's state it read with the row (M16)
    pub pg: Option<PgState>,
}

/// Where a reader is
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReaderPhase {
    /// Reading the entry
    Entry,
    /// Reading the row
    Row,
    /// Reading chunks
    Chunks,
}

/// A rebuild or a move of one position
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Mover {
    /// The stripe
    pub stripe: StripeIx,
    /// The position
    pub pos: Pos,
    /// For a move, the slice it moves to
    pub to: Option<SliceId>,
    /// Where it is
    pub phase: MoverPhase,
    /// The row it read
    pub row: Option<(u32, RowState)>,
    /// The chunks it reads to rebuild from
    pub reads: ChunkReads,
    /// The slice the chunk is written to
    pub target: Option<SliceId>,
    /// Whether that slice already held the label when the rebuild read the row
    pub held: bool,
    /// The placement group's state and the stripe's record it read with the row (M16)
    pub pg: Option<PgView>,
    /// The label the holder's chunk was at when asked, which the record's units are laid over
    pub held_label: Option<Label>,
    /// The units it staged, when it rebuilt fewer than the whole chunk
    pub units: Option<Vec<u8>>,
}

/// Where a rebuild is
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MoverPhase {
    /// Reading the row
    Row,
    /// Asking the holder what it holds
    Ask,
    /// Reading `k` current chunks
    Read,
    /// Staging the rebuilt chunk
    Stage,
    /// Waiting for the commit
    Commit,
}

/// The reclaimer of the oldest floor's stripes
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Reclaimer {
    /// Where it is
    pub phase: ReclaimPhase,
    /// The floor it reclaims under
    pub floor: Option<Floor>,
    /// The stripes wholly past it
    pub stripes: Vec<StripeIx>,
    /// The one it is at
    pub at: usize,
    /// Whether every stripe the floor hides has gone, so the floor can
    pub all: bool,
    /// The row of the stripe it is at
    pub row: Option<RowState>,
    /// The placement group's state it read with that row (M16)
    pub pg: Option<PgState>,
    /// The stripe the floor's cut falls inside, which M20 judges before the floor goes: whether a
    /// write since stamped it past the floor (M20)
    pub cut: Option<StripeIx>,
}

/// Where a reclaimer is
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReclaimPhase {
    /// Reading the entry
    Entry,
    /// Reading a stripe's row
    Row,
    /// Committing its reclamation
    Commit,
    /// Reading the row of the stripe the cut falls inside (M20)
    Cut,
    /// Committing the floor's removal
    Drop,
}

/// The release of a slice a placement group left, of what it holds of the group (M20)
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Releaser {
    /// The slice
    pub slice: SliceId,
    /// Where it is
    pub phase: ReleasePhase,
    /// Whether it sealed the slice, so it unseals it if it does not release
    pub sealed: bool,
}

/// Where a release is
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReleasePhase {
    /// Waiting for the slice to say it is sealed
    Seal,
    /// Reading the group's state at its leader
    Read,
    /// Waiting for the slice to say it released
    Release,
    /// Waiting for the commit that the group no longer names the slice
    Done,
}

/// The leader's driver that has a stripe's holders fold its pending bytes, then clears them
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Clearer {
    /// The stripe
    pub stripe: StripeIx,
    /// Where it is
    pub phase: ClearPhase,
    /// The row it read
    pub row: Option<RowState>,
    /// The positions whose holders said, in its round, that they hold the bytes' label durably
    pub holding: BTreeSet<Pos>,
    /// The positions that answered at all
    pub answered: BTreeSet<Pos>,
    /// The placement group's state it read with the row (M16)
    pub pg: Option<PgState>,
    /// Each slice that said it holds the bytes' label, once a position may have two
    pub holding_at: BTreeSet<(Pos, SliceId)>,
    /// Each slice that answered
    pub answered_at: BTreeSet<(Pos, SliceId)>,
}

/// Where a clear is
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClearPhase {
    /// Reading the row at its leader
    Row,
    /// Asking every holder to fold the pending bytes
    Fold,
    /// Committing the clear
    Commit,
}

/// A move of a placement group's position: begun, copied onto its arrival, and switched (M16)
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Copier {
    /// The position
    pub pos: Pos,
    /// The slice it moves to
    pub to: SliceId,
    /// The move it drives: its own, or one a driver that died left in flight
    pub move_op: OpId,
    /// Where it is
    pub phase: CopyPhase,
    /// The group's state once the move began
    pub state: Option<PgState>,
    /// The stripe it is at
    pub stripe: u8,
    /// The row of that stripe
    pub row: Option<RowState>,
    /// The old holders it listed that stripe on, and what each answered
    pub listed: BTreeMap<SliceId, Option<bool>>,
    /// How many of them answered
    pub reached: usize,
    /// How much of the object the entry said a committed put had written, read once the move
    /// began: a stripe past it with no row is a put's still in flight, or one given up
    pub created: Option<u32>,
    /// The chunks it reads to copy from
    pub reads: ChunkReads,
}

/// Where a move is
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CopyPhase {
    /// Reading the group's state
    Read,
    /// Waiting for the begin's commit
    Begin,
    /// Reading the entry, for which stripes with no row are an object's
    Entry,
    /// Reading a stripe's row and listing its chunks on the old holders
    Row,
    /// Asking the arrival whether it holds the row's label
    Ask,
    /// Reading `k` current chunks
    Copy,
    /// Staging the copy on the arrival
    Stage,
    /// Marking a stripe it could not copy missed
    Mark,
    /// Waiting for the switch's commit
    Switch,
    /// Waiting for the abort's commit
    Abort,
}

/// The put of the object's last stripe, racing whatever moves its placement group (M16)
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Putter {
    /// Where it is
    pub phase: PutPhase,
    /// The entry it read
    pub entry: Option<EntryState>,
    /// The group's state it placed under
    pub placed: Option<PgState>,
    /// The group's state when its pieces were durable
    pub now: Option<PgState>,
    /// What each slice answered its piece
    pub answers: BTreeMap<(Pos, SliceId), bool>,
    /// The positions it marks missed, in order
    pub marks: Vec<Pos>,
}

/// Where a put is
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PutPhase {
    /// Reading the entry, strongly
    Entry,
    /// Reading the group's state
    Pg,
    /// Writing its pieces
    Place,
    /// Reading the group's state again once they are durable
    Check,
    /// Marking the positions they did not reach missed
    Mark,
    /// Committing the entry
    Commit,
}

/// A backfill of one position's stale stripes, which says the record holds them all again (M16)
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Backfiller {
    /// The position
    pub pos: Pos,
    /// Where it is
    pub phase: BackfillPhase,
    /// The drops the record had counted when it began
    pub drops: u32,
    /// The stripe it is at
    pub stripe: u8,
    /// Whether every stale stripe it walked was in the record
    pub whole: bool,
}

/// Where a backfill is
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackfillPhase {
    /// Reading the record
    Read,
    /// Walking the stripes' rows
    Walk,
    /// Waiting for its commit
    Done,
}

/// An operation's driver
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Driver {
    /// A write
    Write(Box<Stager>),
    /// A truncate
    Truncate(Truncator),
    /// A read
    Read(Box<Reader>),
    /// A rebuild or a move
    Rebuild(Box<Mover>),
    /// A reclamation
    Reclaim(Box<Reclaimer>),
    /// A clear of a row's pending bytes
    Clear(Box<Clearer>),
    /// A move of a placement group's position (M16)
    Copy(Box<Copier>),
    /// The put of the object's last stripe (M16)
    Put(Box<Putter>),
    /// A backfill of one position's stale stripes (M16)
    Backfill(Box<Backfiller>),
    /// A release of a slice the placement group left (M20)
    Release(Box<Releaser>),
}

/// The slices a position's write, confirmation or fold has to reach to count as held
///
/// Its own slice, and while the group is in both generations its arrival too: until the switch a
/// reader reads the old slice and after it the arrival, so a label is current only where both
/// hold it. An old slice that is lost for good - its device gone, or failed and reported - never
/// serves a read again, and the arrival alone counts. The unsafe setting counts the old slice
/// alone (M16).
///
/// # Arguments
///
/// * `state` - The placement group's state
/// * `pos` - The position
/// * `rule` - What counts a moving position as holding a label
/// * `lost` - Whether the position's old slice is lost for good
pub fn counted_slices(state: &PgState, pos: Pos, rule: BothRule, lost: bool) -> Vec<SliceId> {
    match (rule, state.arrival(pos)) {
        (BothRule::BothHold, Some(arrival)) if lost => vec![arrival],
        (BothRule::BothHold, _) => state.slices(pos),
        (BothRule::OldAlone, _) => vec![state.slice(pos)],
    }
}

/// Whether every slice a position counts on answered yes, any answered no, or some have not
///
/// # Arguments
///
/// * `answers` - What each slice answered, by position
/// * `slices` - The slices the position counts on
/// * `pos` - The position
fn judged(answers: &BTreeMap<(Pos, SliceId), bool>, slices: &[SliceId], pos: Pos) -> Option<bool> {
    let mut all = true;
    for slice in slices {
        match answers.get(&(pos, *slice)) {
            Some(false) => return Some(false),
            Some(true) => {}
            None => all = false,
        }
    }
    all.then_some(true)
}

impl StripeWorld {
    /// A client sends a write
    ///
    /// # Arguments
    ///
    /// * `op` - Its identity
    /// * `stripe` - The stripe
    /// * `units` - The data units it writes
    pub(crate) fn invoke_write(&mut self, op: OpId, stripe: StripeIx, units: &[u8]) -> bool {
        let data_units = self.layout().data_units() as u8;
        if usize::from(stripe.0) >= self.rows.len()
            || units.is_empty()
            || units.iter().any(|unit| *unit >= data_units)
        {
            return false;
        }
        // the last stripe of a world that leaves it to a put is a new object's in the product:
        // nothing writes it before its put has committed (M16)
        let last = usize::from(stripe.0) + 1 == self.rows.len();
        if last && self.late_put_pending(true) {
            return false;
        }
        let kind = OpKind::Write {
            stripe,
            units: units.to_vec(),
        };
        if !self.ledger.invoke(op, kind, self.step) {
            return false;
        }
        self.start_write(op, stripe, units.to_vec());
        true
    }

    /// Start a stager for a write, in the operation's current round
    fn start_write(&mut self, op: OpId, stripe: StripeIx, units: Vec<u8>) {
        self.drivers.insert(
            op,
            Driver::Write(Box::new(Stager {
                stripe,
                units,
                phase: StagerPhase::Entry,
                entry: None,
                row: None,
                reads: ChunkReads::default(),
                label: None,
                written: Vec::new(),
                touched: Vec::new(),
                staged: BTreeSet::new(),
                refused: BTreeSet::new(),
                confirms: BTreeMap::new(),
                in_commit: false,
                whole_write: false,
                pg: None,
                staged_at: BTreeSet::new(),
                refused_at: BTreeSet::new(),
                confirms_at: BTreeMap::new(),
            })),
        );
        let me = self.endpoint(op);
        self.send(me, Endpoint::Entry, Body::EntryRead { strong: true });
    }

    /// A client sends a truncate
    ///
    /// # Arguments
    ///
    /// * `op` - Its identity
    /// * `len` - The new length
    pub(crate) fn invoke_truncate(&mut self, op: OpId, len: u32) -> bool {
        let max = u32::from(self.params.stripes) * self.layout().data_units() as u32;
        // nor does a truncate run before that put has committed, since the product's put is a new
        // object with no floor before it, and one that failed is an object nobody reads (M16)
        if self.late_put_pending(true) {
            return false;
        }
        if len > max || !self.ledger.invoke(op, OpKind::Truncate { len }, self.step) {
            return false;
        }
        self.start_truncate(op, len);
        true
    }

    /// Start a truncate's driver
    fn start_truncate(&mut self, op: OpId, len: u32) {
        let me = self.endpoint(op);
        // POLICY Q18: the contract's truncate reads the entry and fences first; S3's commits alone
        if self.policy.truncate == TruncateRule::Fenced {
            self.drivers.insert(
                op,
                Driver::Truncate(Truncator {
                    len,
                    phase: TruncatePhase::Entry,
                    read: None,
                    read_size: None,
                }),
            );
            self.send(me, Endpoint::Entry, Body::EntryRead { strong: true });
        } else {
            self.drivers.insert(
                op,
                Driver::Truncate(Truncator {
                    len,
                    phase: TruncatePhase::Commit,
                    read: None,
                    read_size: None,
                }),
            );
            self.send(
                me,
                Endpoint::Entry,
                Body::EntryPropose(EntryCommand::Truncate {
                    op,
                    len,
                    expect: None,
                    size: None,
                }),
            );
        }
    }

    /// A truncate's driver receives a message
    ///
    /// # Arguments
    ///
    /// * `op` - The truncate
    /// * `truncator` - Its state
    /// * `body` - The message
    fn truncator_receive(&mut self, op: OpId, mut truncator: Truncator, body: &Body) {
        let me = self.endpoint(op);
        let data_units = self.layout().data_units() as u32;
        match (truncator.phase, body) {
            (TruncatePhase::Entry, Body::EntryAnswer { state, .. }) => {
                truncator.read = Some(state.epoch);
                truncator.read_size = Some(state.size);
                // the cut is where the floor will be; a cut inside a stripe fences that stripe
                let cut = truncator.len.min(state.size);
                let stripe = cut / data_units;
                if cut % data_units != 0 && (stripe as usize) < self.rows.len() {
                    truncator.phase = TruncatePhase::Fence;
                    self.send(
                        me,
                        Endpoint::Row(StripeIx(stripe as u8)),
                        Body::Propose(StripeCommand::Fence {
                            op,
                            epoch: Epoch(state.epoch.0 + 1),
                        }),
                    );
                } else {
                    self.commit_truncate(op, &mut truncator);
                }
            }
            (TruncatePhase::Fence, Body::Decided(_)) => self.commit_truncate(op, &mut truncator),
            (TruncatePhase::Commit, Body::Decided(decision)) => {
                match decision {
                    Decision::Committed { .. } | Decision::Repeated { .. } => {
                        self.drivers.remove(&op);
                        self.checker.coverage.truncates += 1;
                        self.complete(op, StripeOutcome::Ok);
                    }
                    // the epoch moved under it: read it again and fence again, in a new round
                    Decision::Refused if self.policy.truncate == TruncateRule::Fenced => {
                        *self.rounds.entry(op).or_insert(0) += 1;
                        truncator.phase = TruncatePhase::Entry;
                        truncator.read = None;
                        truncator.read_size = None;
                        let me = self.endpoint(op);
                        self.send(me, Endpoint::Entry, Body::EntryRead { strong: true });
                        self.drivers.insert(op, Driver::Truncate(truncator));
                    }
                    Decision::Refused => {
                        self.drivers.remove(&op);
                        self.complete(op, StripeOutcome::Refused);
                    }
                }
                return;
            }
            _ => return,
        }
        if self.drivers.contains_key(&op) {
            self.drivers.insert(op, Driver::Truncate(truncator));
        }
    }

    /// Commit a truncate at the epoch it read
    ///
    /// # Arguments
    ///
    /// * `op` - The truncate
    /// * `truncator` - Its state
    fn commit_truncate(&mut self, op: OpId, truncator: &mut Truncator) {
        truncator.phase = TruncatePhase::Commit;
        let me = self.endpoint(op);
        self.send(
            me,
            Endpoint::Entry,
            Body::EntryPropose(EntryCommand::Truncate {
                op,
                len: truncator.len,
                expect: truncator.read,
                size: truncator.read_size,
            }),
        );
    }

    /// A client sends a read
    ///
    /// # Arguments
    ///
    /// * `op` - Its identity
    /// * `stripe` - The stripe
    /// * `strong` - Whether it is strong
    pub(crate) fn invoke_read(&mut self, op: OpId, stripe: StripeIx, strong: bool) -> bool {
        if usize::from(stripe.0) >= self.rows.len()
            || !self
                .ledger
                .invoke(op, OpKind::Read { stripe, strong }, self.step)
        {
            return false;
        }
        self.drivers.insert(
            op,
            Driver::Read(Box::new(Reader {
                stripe,
                strong,
                phase: ReaderPhase::Entry,
                entry: None,
                row: None,
                reads: ChunkReads::default(),
                rereads: 0,
                pg: None,
            })),
        );
        let me = self.endpoint(op);
        self.send(me, Endpoint::Entry, Body::EntryRead { strong });
        true
    }

    /// The leader's driver starts a rebuild, or the pool map a move
    ///
    /// # Arguments
    ///
    /// * `op` - Its identity
    /// * `stripe` - The stripe
    /// * `pos` - The position
    /// * `to` - For a move, the slice it moves to
    pub(crate) fn invoke_rebuild(
        &mut self,
        op: OpId,
        stripe: StripeIx,
        pos: Pos,
        to: Option<SliceId>,
    ) -> bool {
        if usize::from(stripe.0) >= self.rows.len()
            || usize::from(pos.0) >= self.layout().width()
            || to.is_some_and(|slice| !self.slices.contains_key(&slice))
        {
            return false;
        }
        // with the placement group's state the group's, a move is the group's, not a stripe's
        if to.is_some() && self.policy.placement.grouped() {
            return false;
        }
        if !self
            .ledger
            .invoke(op, OpKind::Rebuild { stripe, pos, to }, self.step)
        {
            return false;
        }
        self.drivers.insert(
            op,
            Driver::Rebuild(Box::new(Mover {
                stripe,
                pos,
                to,
                phase: MoverPhase::Row,
                row: None,
                reads: ChunkReads::default(),
                target: None,
                held: false,
                pg: None,
                held_label: None,
                units: None,
            })),
        );
        let me = self.endpoint(op);
        self.send(me, Endpoint::Row(stripe), Body::RowRead { strong: true });
        true
    }

    /// The reclaimer starts on the oldest floor
    ///
    /// # Arguments
    ///
    /// * `op` - Its identity
    pub(crate) fn invoke_reclaim(&mut self, op: OpId) -> bool {
        if !self.ledger.invoke(op, OpKind::Reclaim, self.step) {
            return false;
        }
        self.drivers.insert(
            op,
            Driver::Reclaim(Box::new(Reclaimer {
                phase: ReclaimPhase::Entry,
                floor: None,
                stripes: Vec::new(),
                at: 0,
                all: true,
                row: None,
                pg: None,
                cut: None,
            })),
        );
        let me = self.endpoint(op);
        self.send(me, Endpoint::Entry, Body::EntryRead { strong: true });
        true
    }

    /// The leader's driver starts clearing a stripe's pending bytes
    ///
    /// # Arguments
    ///
    /// * `op` - Its identity
    /// * `stripe` - The stripe
    pub(crate) fn invoke_clear(&mut self, op: OpId, stripe: StripeIx) -> bool {
        if usize::from(stripe.0) >= self.rows.len()
            || !self
                .ledger
                .invoke(op, OpKind::ClearPendingBytes { stripe }, self.step)
        {
            return false;
        }
        self.drivers.insert(
            op,
            Driver::Clear(Box::new(Clearer {
                stripe,
                phase: ClearPhase::Row,
                row: None,
                holding: BTreeSet::new(),
                answered: BTreeSet::new(),
                pg: None,
                holding_at: BTreeSet::new(),
                answered_at: BTreeSet::new(),
            })),
        );
        let me = self.endpoint(op);
        self.send(me, Endpoint::Row(stripe), Body::RowRead { strong: true });
        true
    }

    /// A client tries a write or a truncate again, under the same identity, in a new round
    ///
    /// # Arguments
    ///
    /// * `op` - The operation
    pub(crate) fn retry(&mut self, op: OpId) -> bool {
        let Some(kind) = self.ledger.kind_of(op) else {
            return false;
        };
        if !matches!(kind, OpKind::Write { .. } | OpKind::Truncate { .. }) {
            return false;
        }
        // a try still running is not tried again
        if self.drivers.contains_key(&op) || !self.ledger.retry(op) {
            return false;
        }
        *self.rounds.entry(op).or_insert(0) += 1;
        self.checker.coverage.retries += 1;
        match kind {
            OpKind::Write { stripe, units } => self.start_write(op, stripe, units),
            OpKind::Truncate { len } => self.start_truncate(op, len),
            _ => unreachable!("checked above"),
        }
        true
    }

    /// A client gives up waiting
    ///
    /// # Arguments
    ///
    /// * `op` - The operation
    pub(crate) fn client_timeout(&mut self, op: OpId) -> bool {
        if !self.ledger.is_pending(op) {
            return false;
        }
        self.checker.coverage.unknown_outcomes += 1;
        self.complete(op, StripeOutcome::Unknown);
        true
    }

    /// A stager gives up while what it sent may still be in flight
    ///
    /// # Arguments
    ///
    /// * `op` - The write
    pub(crate) fn stager_timeout(&mut self, op: OpId) -> bool {
        let Some(Driver::Write(stager)) = self.drivers.get(&op) else {
            return false;
        };
        let stager = stager.clone();
        // POLICY P16: the contract's stager says nothing; the unsafe one tells holders to drop
        if self.policy.stager_word == StagerWord::Obeyed {
            if let (Some(label), Some((_, row))) = (stager.label, &stager.row) {
                let me = self.endpoint(op);
                for pos in &stager.touched {
                    for slice in self.slices_of(row, stager.pg.as_ref(), *pos) {
                        self.send(
                            me,
                            Endpoint::Slice(slice),
                            Body::Drop {
                                stripe: stager.stripe,
                                label,
                            },
                        );
                    }
                }
            }
        }
        self.end_driver(op, stager.stripe);
        if self.ledger.is_pending(op) {
            self.checker.coverage.unknown_outcomes += 1;
            self.complete(op, StripeOutcome::Unknown);
        }
        true
    }

    /// An operation's coordinating shard dies
    ///
    /// # Arguments
    ///
    /// * `op` - The operation
    pub(crate) fn driver_crash(&mut self, op: OpId) -> bool {
        let Some(driver) = self.drivers.get(&op) else {
            return false;
        };
        let stripe = match driver {
            Driver::Write(stager) => Some(stager.stripe),
            _ => None,
        };
        self.drivers.remove(&op);
        if let Some(stripe) = stripe {
            self.release(stripe, op);
        }
        if self.ledger.is_pending(op) {
            self.checker.coverage.unknown_outcomes += 1;
            self.complete(op, StripeOutcome::Unknown);
        }
        true
    }

    /// End a write's stager and give up its turn
    fn end_driver(&mut self, op: OpId, stripe: StripeIx) {
        self.drivers.remove(&op);
        self.release(stripe, op);
    }

    /// A driver receives a message
    ///
    /// # Arguments
    ///
    /// * `op` - The operation
    /// * `msg` - The message
    pub(crate) fn driver_receive(&mut self, op: OpId, msg: &crate::stripe::event::Message) {
        let Some(driver) = self.drivers.get(&op).cloned() else {
            return;
        };
        match driver {
            Driver::Write(stager) => {
                // a decision is the row's while committing, the entry's while extending or advancing
                let expected = match (stager.phase, &msg.body) {
                    (StagerPhase::Commit, Body::Decided(_)) => {
                        msg.from == Endpoint::Row(stager.stripe)
                    }
                    (StagerPhase::Extend | StagerPhase::Advance, Body::Decided(_)) => {
                        msg.from == Endpoint::Entry
                    }
                    _ => true,
                };
                if expected {
                    self.stager_receive(op, *stager, msg.from, &msg.body);
                }
            }
            Driver::Truncate(truncator) => {
                // a decision is the entry's only in the commit phase, a row's only in the fence's
                let expected = match truncator.phase {
                    TruncatePhase::Fence => matches!(msg.from, Endpoint::Row(_)),
                    _ => msg.from == Endpoint::Entry,
                };
                if expected {
                    self.truncator_receive(op, truncator, &msg.body);
                }
            }
            Driver::Read(reader) => self.reader_receive(op, *reader, &msg.body),
            Driver::Rebuild(mover) => self.mover_receive(op, *mover, &msg.body),
            Driver::Reclaim(reclaimer) => self.reclaimer_receive(op, *reclaimer, msg),
            Driver::Clear(clearer) => {
                // a decision is the row's, and only while committing
                let expected = match &msg.body {
                    Body::Decided(_) => {
                        clearer.phase == ClearPhase::Commit
                            && msg.from == Endpoint::Row(clearer.stripe)
                    }
                    _ => true,
                };
                if expected {
                    self.clearer_receive(op, *clearer, msg.from, &msg.body);
                }
            }
            Driver::Copy(copier) => self.copier_receive(op, *copier, msg),
            Driver::Put(putter) => self.putter_receive(*putter, msg),
            Driver::Backfill(backfiller) => self.backfiller_receive(op, *backfiller, msg),
            Driver::Release(releaser) => self.releaser_receive(op, *releaser, msg),
        }
    }

    /// Ask the first `k` candidates for their chunks
    ///
    /// # Arguments
    ///
    /// * `op` - The driver
    /// * `stripe` - The stripe
    /// * `row` - The row it read
    /// * `exclude` - A position not to read, the one being rebuilt
    /// * `pg` - The placement group's state read with the row, once that is the group's (M16)
    fn start_reads(
        &mut self,
        op: OpId,
        stripe: StripeIx,
        row: &RowState,
        exclude: Option<Pos>,
        pg: Option<&PgState>,
    ) -> ChunkReads {
        let layout = self.layout();
        // the positions the row calls current, data first
        let mut candidates: Vec<Pos> = layout
            .positions()
            .into_iter()
            .filter(|pos| row.current(*pos) && Some(*pos) != exclude)
            .collect();
        candidates.sort_by_key(|pos| !layout.is_data(*pos));
        // a chunk is read where the group's generation puts it, never from an arrival
        let at = pg
            .map(|pg| layout.positions().into_iter().map(|pos| pg.slice(pos)).collect())
            .unwrap_or_default();
        let mut reads = ChunkReads {
            candidates,
            at,
            ..ChunkReads::default()
        };
        let me = self.endpoint(op);
        while reads.pending.len() < layout.k() && reads.next < reads.candidates.len() {
            let pos = reads.candidates[reads.next];
            reads.next += 1;
            reads.pending.insert(pos);
            let slice = reads.at.get(usize::from(pos.0)).copied().unwrap_or(row.slice(pos));
            self.send(
                me,
                Endpoint::Slice(slice),
                Body::ChunkRead {
                    stripe,
                    pos,
                    label: row.label(pos),
                    also: row.current_labels(pos).split_off(1),
                },
            );
        }
        reads
    }

    /// Take one chunk answer into a read of chunks
    ///
    /// # Arguments
    ///
    /// * `op` - The driver
    /// * `stripe` - The stripe
    /// * `row` - The row it read
    /// * `reads` - Where the read stands
    /// * `pos` - The position that answered
    /// * `asked` - The label it was asked for
    /// * `reply` - Its answer
    /// * `rule` - What it does with a chunk at the base the row's pending bytes overlay
    #[allow(clippy::too_many_arguments)]
    fn take_chunk(
        &mut self,
        op: OpId,
        stripe: StripeIx,
        row: &RowState,
        reads: &mut ChunkReads,
        pos: Pos,
        asked: Label,
        reply: &ChunkReply,
        rule: PendingRead,
    ) -> ReadStep {
        if !reads.pending.remove(&pos) {
            return ReadStep::Wait;
        }
        // the row's pending bytes, if the label asked for is theirs
        let pending = row
            .pending_bytes
            .as_ref()
            .filter(|pending| pending.label == asked);
        match reply {
            ChunkReply::Bytes(content) => {
                reads.got.insert(pos, (asked, content.clone()));
            }
            ChunkReply::Other { label, content } if label.seq > asked.seq => {
                return ReadStep::Newer(pos, *label, content.clone());
            }
            // Q27: a chunk the pending bytes fold from is the asked label with them laid over
            ChunkReply::Other { label, content }
                if pending.is_some_and(|pending| pending.folds_from(pos, *label)) =>
            {
                let pending = pending.expect("checked");
                // POLICY P10, P12: the contract lays the pending units over the base; the unsafe
                // setting takes the base as the label the row names
                let content = match rule {
                    PendingRead::Overlaid => lay_over(content, &pending.units),
                    PendingRead::BaseTaken => content.clone(),
                };
                self.checker.coverage.overlaid_reads += 1;
                reads.got.insert(pos, (asked, content));
            }
            _ => {
                // missing to this reader: try another candidate
                if reads.next < reads.candidates.len() {
                    let next = reads.candidates[reads.next];
                    reads.next += 1;
                    reads.pending.insert(next);
                    let me = self.endpoint(op);
                    let slice = reads.at.get(usize::from(next.0)).copied().unwrap_or(row.slice(next));
                    self.send(
                        me,
                        Endpoint::Slice(slice),
                        Body::ChunkRead {
                            stripe,
                            pos: next,
                            label: row.label(next),
                            also: row.current_labels(next).split_off(1),
                        },
                    );
                }
            }
        }
        if reads.got.len() >= self.layout().k() {
            ReadStep::Done
        } else if reads.pending.is_empty() {
            ReadStep::Exhausted
        } else {
            ReadStep::Wait
        }
    }

    /// The data units a read of chunks decodes to
    ///
    /// # Arguments
    ///
    /// * `reads` - The chunks
    fn decoded(&self, reads: &ChunkReads) -> Vec<Unit> {
        let chunks: Vec<(Pos, Content)> = reads
            .got
            .iter()
            .map(|(pos, (_, content))| (*pos, content.clone()))
            .collect();
        decode(self.layout(), &chunks)
    }

    /// A stager receives a message
    ///
    /// # Arguments
    ///
    /// * `op` - The write
    /// * `stager` - Its state
    /// * `from` - Who sent the message
    /// * `body` - The message
    fn stager_receive(&mut self, op: OpId, mut stager: Stager, from: Endpoint, body: &Body) {
        let me = self.endpoint(op);
        let stripe = stager.stripe;
        match (stager.phase, body) {
            (StagerPhase::Entry, Body::EntryAnswer { index, state }) => {
                stager.entry = Some((*index, state.clone()));
                // with reservations, the leader orders the stagers before any of them reads the row
                if self.policy.reservation == Reservation::Granted {
                    stager.phase = StagerPhase::Reserve;
                    self.send(me, Endpoint::Row(stripe), Body::Reserve { stripe });
                } else {
                    stager.phase = StagerPhase::Row;
                    self.send(me, Endpoint::Row(stripe), Body::RowRead { strong: true });
                }
            }
            (StagerPhase::Reserve, Body::Granted { .. }) => {
                stager.phase = StagerPhase::Row;
                self.send(me, Endpoint::Row(stripe), Body::RowRead { strong: true });
            }
            (
                StagerPhase::Row,
                Body::RowAnswer {
                    index,
                    state,
                    written,
                    pg,
                    ..
                },
            ) => {
                stager.row = Some((*index, state.clone()));
                stager.pg = pg.clone();
                // a retry of a write that committed is answered as its first try was
                if *written {
                    self.end_driver(op, stripe);
                    self.finish_write(op, stripe, &stager);
                    return;
                }
                // a fence above the epoch it read: a truncate is committing, or died having
                // fenced. It moves the epoch past the fence if it must, and reads the entry again
                let (_, entry) = stager.entry.clone().expect("read");
                if self.policy.truncate == TruncateRule::Fenced && state.fence > entry.epoch {
                    stager.phase = StagerPhase::Advance;
                    self.send(
                        me,
                        Endpoint::Entry,
                        Body::EntryPropose(EntryCommand::Advance {
                            op,
                            from: entry.epoch,
                        }),
                    );
                    self.drivers.insert(op, Driver::Write(Box::new(stager)));
                    return;
                }
                self.plan_write(op, &mut stager);
                if stager.phase == StagerPhase::Read {
                    let pg = stager.pg.as_ref().map(|view| view.state.clone());
                    stager.reads = self.start_reads(op, stripe, state, None, pg.as_ref());
                    if stager.reads.candidates.len() < self.layout().k() {
                        // too few current chunks to read the old bytes from: fail by name
                        self.end_driver(op, stripe);
                        self.complete(op, self.unproposed(op));
                        return;
                    }
                } else {
                    self.stage(op, &mut stager, None);
                    // a small write in its commit waits for no stage; counting on the row's word
                    // it waits for nothing at all
                    if stager.in_commit && self.maybe_propose(op, &mut stager) {
                        return;
                    }
                }
            }
            (
                StagerPhase::Read,
                Body::ChunkAnswer {
                    pos, label, reply, ..
                },
            ) => {
                let row = stager.row.clone().expect("read").1;
                let mut reads = std::mem::take(&mut stager.reads);
                let rule = self.policy.small_writes.stager;
                let step =
                    self.take_chunk(op, stripe, &row, &mut reads, *pos, *label, reply, rule);
                stager.reads = reads;
                match step {
                    ReadStep::Wait => {}
                    ReadStep::Done => {
                        let old = self.decoded(&stager.reads);
                        self.stage(op, &mut stager, Some(old));
                    }
                    ReadStep::Newer(..) | ReadStep::Exhausted => {
                        // the row moved under it, or too few chunks answered: it fails by name
                        self.end_driver(op, stripe);
                        self.complete(op, self.unproposed(op));
                        return;
                    }
                }
            }
            (
                StagerPhase::Stage,
                Body::StageAnswer {
                    pos,
                    label,
                    refused,
                    ..
                },
            ) => {
                if Some(*label) != stager.label || !stager.touched.contains(pos) {
                    return;
                }
                match (stager.pg.is_some(), from) {
                    // M16: a moving position counts once every slice it counts on answered
                    (true, Endpoint::Slice(slice)) => {
                        match refused {
                            None => stager.staged_at.insert((*pos, slice)),
                            Some(_) => stager.refused_at.insert((*pos, slice)),
                        };
                        self.judge_stages(&mut stager);
                    }
                    _ => match refused {
                        None => {
                            stager.staged.insert(*pos);
                        }
                        Some(_) => {
                            stager.refused.insert(*pos);
                        }
                    },
                }
                if self.maybe_propose(op, &mut stager) {
                    return;
                }
            }
            (StagerPhase::Stage, Body::ConfirmAnswer { pos, holds, .. }) => {
                match (&stager.pg, from) {
                    (Some(view), Endpoint::Slice(slice)) => {
                        stager.confirms_at.insert((*pos, slice), *holds);
                        let counted = self.counted(&view.state, *pos);
                        if let Some(holds) = judged(&stager.confirms_at, &counted, *pos) {
                            stager.confirms.insert(*pos, holds);
                        }
                    }
                    _ => {
                        stager.confirms.insert(*pos, *holds);
                    }
                }
                if self.maybe_propose(op, &mut stager) {
                    return;
                }
            }
            (StagerPhase::Commit, Body::Decided(decision)) => {
                let (_, row) = stager.row.clone().expect("read");
                let label = stager.label.expect("staged");
                match decision {
                    Decision::Committed { .. } if stager.in_commit => {
                        // every holder is given the bytes to fold, a stale one included; one that
                        // never hears is asked by the leader's clear
                        let pending =
                            PendingBytes::after(&row, label, &stager.written, &self.policy);
                        for pos in self.layout().positions() {
                            for slice in self.slices_of(&row, stager.pg.as_ref(), pos) {
                                self.send(
                                    me,
                                    Endpoint::Slice(slice),
                                    Body::Fold {
                                        stripe,
                                        pos,
                                        pending: pending.clone(),
                                    },
                                );
                            }
                        }
                    }
                    Decision::Committed { .. } => {
                        // the holders are told; one that never hears asks the row
                        for pos in &stager.touched {
                            for slice in self.slices_of(&row, stager.pg.as_ref(), *pos) {
                                self.send(me, Endpoint::Slice(slice), Body::Apply { stripe, label });
                            }
                        }
                    }
                    Decision::Repeated { .. } => {}
                    Decision::Refused => {
                        self.end_driver(op, stripe);
                        self.complete(op, StripeOutcome::Refused);
                        return;
                    }
                }
                // a write past the size commits the size after its stripe, if no truncate came between
                self.end_driver(op, stripe);
                self.finish_write(op, stripe, &stager);
                return;
            }
            (StagerPhase::Advance, Body::Decided(_)) => {
                // whether its advance or the truncate's commit moved the epoch, read it again
                *self.rounds.entry(op).or_insert(0) += 1;
                stager.phase = StagerPhase::Entry;
                stager.entry = None;
                stager.row = None;
                let me = self.endpoint(op);
                self.send(me, Endpoint::Entry, Body::EntryRead { strong: true });
            }
            (StagerPhase::Extend, Body::Decided(decision)) => {
                self.end_driver(op, stripe);
                match decision {
                    Decision::Committed { .. } | Decision::Repeated { .. } => {
                        self.checker.coverage.extensions += 1;
                        self.complete(op, StripeOutcome::Ok);
                    }
                    // its stripe committed under a floor the truncate left: whether it happened is not known
                    Decision::Refused => self.complete(op, StripeOutcome::Unknown),
                }
                return;
            }
            _ => return,
        }
        if self.drivers.contains_key(&op) {
            self.drivers.insert(op, Driver::Write(Box::new(stager)));
        }
    }

    /// Decide what a write writes and touches, from the entry and row it read
    ///
    /// # Arguments
    ///
    /// * `op` - The write
    /// * `stager` - Its state
    fn plan_write(&mut self, op: OpId, stager: &mut Stager) {
        let layout = self.layout();
        let (_, entry) = stager.entry.clone().expect("read");
        let (_, row) = stager.row.clone().expect("read");
        let data_units = layout.data_units();
        let start = u32::from(stager.stripe.0) * data_units as u32;
        // the client's units, under this write's identity
        let mut written: Vec<(u8, Unit)> = stager
            .units
            .iter()
            .map(|unit| (*unit, Unit::Write(op)))
            .collect();
        let hole = hole(&row, &entry, start);
        // a write into a hole writes the whole stripe: zeros wherever the client wrote nothing
        if hole {
            for unit in 0..data_units as u8 {
                if !written.iter().any(|(u, _)| *u == unit) {
                    written.push((unit, Unit::Zero));
                }
            }
        }
        // Q18: a write into a stripe a floor hides writes the hidden units as zeros, once that is the rule
        if self.policy.hidden == HiddenRule::Zeroed && !hole {
            for floor in &entry.floors {
                if row.stamp >= floor.epoch {
                    continue;
                }
                for unit in 0..data_units as u8 {
                    let offset = start + u32::from(unit);
                    if offset >= floor.len && !written.iter().any(|(u, _)| *u == unit) {
                        written.push((unit, Unit::Zero));
                    }
                }
            }
        }
        written.sort_unstable();
        stager.written = written;
        // POLICY P9: the contract tags each try apart; S7 as written tags by the identity alone
        let attempt = match self.policy.tag {
            TagRule::PerAttempt => self.rounds.get(&op).copied().unwrap_or(0),
            TagRule::PerIdentity => 0,
        };
        let label = Label {
            seq: row.seq.next(),
            tag: Tag::of(op, stager.stripe, attempt),
        };
        stager.label = Some(label);
        // Q27: a small write of a replicated stripe rides in its commit, if what it leaves in the
        // row is within the bound; it reads nothing and stages nothing, and every position's
        // holder confirms the chunk its bytes are laid over
        let pending = PendingBytes::after(&row, label, &stager.written, &self.policy);
        // F96: the product stages a write whole, its old bytes read first, once it takes a chunk
        // past its file's class; chunks of two units have no class to show, so under the drawn
        // rule its op's parity picks it, and such a write never rides its commit
        stager.whole_write = self.policy.whole_write == WholeWrite::Drawn
            && !hole
            && stager.written.len() < data_units
            && op.0 % 2 == 1;
        stager.in_commit = self.policy.small_writes.path == SmallWritePath::InCommit
            && layout.is_replicated()
            && !hole
            && !row.tombstone
            && !stager.whole_write
            && pending.units.len() <= PENDING_BOUND;
        if stager.in_commit {
            stager.touched = layout.positions();
            stager.phase = StagerPhase::Stage;
            return;
        }
        // a hole, or a write of every unit, needs no old bytes and touches every chunk whole
        let whole = hole || stager.written.len() == data_units;
        stager.touched = if whole || stager.whole_write || layout.is_replicated() {
            layout.positions()
        } else {
            layout
                .positions()
                .into_iter()
                .filter(|pos| {
                    !layout.is_data(*pos)
                        || stager
                            .written
                            .iter()
                            .any(|(unit, _)| layout.locate(usize::from(*unit)).0 == *pos)
                })
                .collect()
        };
        stager.phase = if whole {
            StagerPhase::Stage
        } else {
            StagerPhase::Read
        };
    }

    /// Stage every touched chunk, and confirm untouched ones when the rule asks
    ///
    /// # Arguments
    ///
    /// * `op` - The write
    /// * `stager` - Its state
    /// * `old` - The stripe's data before the write, for a partial write
    fn stage(&mut self, op: OpId, stager: &mut Stager, old: Option<Vec<Unit>>) {
        let layout = self.layout();
        let me = self.endpoint(op);
        let stripe = stager.stripe;
        let (_, entry) = stager.entry.clone().expect("read");
        let (_, row) = stager.row.clone().expect("read");
        let label = stager.label.expect("planned");
        let start = u32::from(stripe.0) * layout.data_units() as u32;
        let hole = hole(&row, &entry, start);
        // the stripe's data after the write
        let mut after = old
            .clone()
            .unwrap_or_else(|| vec![Unit::Zero; layout.data_units()]);
        for (unit, value) in &stager.written {
            after[usize::from(*unit)] = *value;
        }
        let whole = old.is_none();
        // Q27: a staged write over pending bytes carries them: the old bytes it read had them
        // laid over, so it stages every unit, whole. The unsafe stager stages over the base alone
        let over_pending = row.pending_bytes.as_ref().filter(|_| !stager.in_commit);
        let carried =
            over_pending.is_some() && self.policy.small_writes.stager == PendingRead::Overlaid;
        // a small write in its commit stages nothing
        let staged = if stager.in_commit {
            Vec::new()
        } else {
            stager.touched.clone()
        };
        for pos in staged {
            // POLICY P9: the unsafe stager expects the base, as though nothing were pending
            let expects = match over_pending {
                _ if whole => None,
                Some(pending) if !carried => Some(pending.base[usize::from(pos.0)]),
                _ => Some(row.label(pos)),
            };
            let payload = if whole || carried || stager.whole_write {
                Payload::Whole(encode(layout, pos, &after))
            } else if layout.is_data(pos) {
                // the units of this chunk the write changes, at their place in the chunk
                let base = if layout.is_replicated() {
                    0
                } else {
                    usize::from(pos.0) * UNITS
                };
                let units: Vec<(u8, Unit)> = stager
                    .written
                    .iter()
                    .filter(|(unit, _)| {
                        let unit = usize::from(*unit);
                        unit >= base && unit < base + chunk_units(layout)
                    })
                    .map(|(unit, value)| ((usize::from(*unit) - base) as u8, *value))
                    .collect();
                if units.len() == chunk_units(layout) {
                    Payload::Whole(encode(layout, pos, &after))
                } else {
                    Payload::Units(units)
                }
            } else {
                // a parity holder is sent the change, and folds it into its own parity
                let old = old.as_ref().expect("a partial write read its old bytes");
                Payload::Delta(
                    stager
                        .written
                        .iter()
                        .filter(|(unit, value)| old[usize::from(*unit)] != *value)
                        .map(|(unit, value)| Change {
                            index: *unit,
                            old: old[usize::from(*unit)],
                            new: *value,
                        })
                        .collect(),
                )
            };
            // a whole chunk needs no base; a change to part of one names the label it expects
            let expects = match payload {
                Payload::Whole(_) => None,
                _ => expects,
            };
            // M16: a moving position is staged on its arrival too
            for slice in self.slices_of(&row, stager.pg.as_ref(), pos) {
                self.send(
                    me,
                    Endpoint::Slice(slice),
                    Body::Stage {
                        stripe,
                        pos,
                        label,
                        base_exists: row.exists,
                        base: row.seq,
                        expects,
                        payload: payload.clone(),
                        over: Vec::new(),
                    },
                );
            }
        }
        // Q16: an untouched chunk counts only if its holder says so, when that is the rule; a
        // small write in its commit touches none, and confirms the chunk each position holds
        if self.policy.untouched == UntouchedRule::Confirmed && !hole {
            for pos in layout.positions() {
                let touched = stager.touched.contains(&pos) && !stager.in_commit;
                if touched || !row.current(pos) {
                    continue;
                }
                // a chunk the row's pending bytes fold from is current too
                let also = row.current_labels(pos).split_off(1);
                for slice in self.slices_of(&row, stager.pg.as_ref(), pos) {
                    self.send(
                        me,
                        Endpoint::Slice(slice),
                        Body::Confirm {
                            stripe,
                            pos,
                            label: row.label(pos),
                            also: also.clone(),
                        },
                    );
                }
            }
        }
        stager.phase = StagerPhase::Stage;
    }

    /// Propose the commit once enough chunks are current, or fail by name once they cannot be
    ///
    /// Returns whether the stager is finished with.
    ///
    /// # Arguments
    ///
    /// * `op` - The write
    /// * `stager` - Its state
    fn maybe_propose(&mut self, op: OpId, stager: &mut Stager) -> bool {
        let layout = self.layout();
        let (_, entry) = stager.entry.clone().expect("read");
        let (_, row) = stager.row.clone().expect("read");
        // the touched chunks that staged count on their answers
        let mut counted: Vec<(Pos, Evidence)> = stager
            .staged
            .iter()
            .map(|pos| (*pos, Evidence::Staged))
            .collect();
        // an untouched chunk the row calls current counts by the rule in force (Q16)
        let mut unanswered = false;
        for pos in layout.positions() {
            // a small write in its commit counts every position as an untouched one
            let touched = stager.touched.contains(&pos) && !stager.in_commit;
            if touched || !row.current(pos) {
                continue;
            }
            match self.policy.untouched {
                UntouchedRule::UpOnly => {
                    let slice = stager.pg.as_ref().map_or(row.slice(pos), |view| view.state.slice(pos));
                    if self.believed_up(slice) {
                        counted.push((pos, Evidence::RowWord));
                    }
                }
                UntouchedRule::CountedWhenDown => counted.push((pos, Evidence::RowWord)),
                UntouchedRule::Confirmed => match stager.confirms.get(&pos) {
                    Some(true) => counted.push((pos, Evidence::Confirmed)),
                    Some(false) => {}
                    None => unanswered = true,
                },
            }
        }
        counted.sort_unstable();
        // POLICY P11: the contract waits for k + f; the unsafe setting proposes after one stage
        let ready = match self.policy.ack {
            AckRule::KPlusF => counted.len() >= layout.ack_floor(),
            AckRule::AfterOneStage => !stager.staged.is_empty(),
        };
        if ready {
            let label = stager.label.expect("planned");
            let cmd = StripeCommand::Write {
                op,
                attempt: self.rounds.get(&op).copied().unwrap_or(0),
                base_exists: row.exists,
                base: row.seq,
                generation: row.generation,
                read_epoch: entry.epoch,
                tag: label.tag,
                touched: stager.touched.clone(),
                staged: stager.staged.iter().copied().collect(),
                counted,
                units: stager.written.clone(),
                bytes_in_commit: stager.in_commit,
                epoch: stager.pg.as_ref().map(|view| view.state.epoch),
            };
            stager.phase = StagerPhase::Commit;
            let me = self.endpoint(op);
            self.send(me, Endpoint::Row(stager.stripe), Body::Propose(cmd));
            return false;
        }
        // every touched chunk has answered and too few are current: refused by name. A small
        // write in its commit staged nothing, and waits for its confirmations alone
        let answered = stager.in_commit
            || stager.staged.len() + stager.refused.len() == stager.touched.len();
        if answered && !unanswered {
            self.end_driver(op, stager.stripe);
            self.complete(op, self.unproposed(op));
            return true;
        }
        false
    }

    /// Every slice a position of a row is staged, confirmed or folded on: its own, then its
    /// arrival while the placement group is in both generations (M16)
    ///
    /// # Arguments
    ///
    /// * `row` - The row read
    /// * `pg` - The placement group's state read with it, once that is the group's
    /// * `pos` - The position
    pub(crate) fn slices_of(&self, row: &RowState, pg: Option<&PgView>, pos: Pos) -> Vec<SliceId> {
        match pg {
            Some(view) => view.state.slices(pos),
            None => vec![row.slice(pos)],
        }
    }

    /// The slices a position counts on, with whether its old slice is lost for good judged as a
    /// coordinator knows it: its device gone, or its node reporting it failed (M16)
    ///
    /// # Arguments
    ///
    /// * `state` - The placement group's state
    /// * `pos` - The position
    pub(crate) fn counted(&self, state: &PgState, pos: Pos) -> Vec<SliceId> {
        let old = state.slice(pos);
        let lost = self.slices.get(&old).is_none_or(|slice| {
            slice.gone || self.nodes.get(&slice.node).is_some_and(|node| node.reported)
        });
        counted_slices(state, pos, self.policy.placement.both, lost)
    }

    /// Recount a stager's touched positions from what each of their slices answered (M16)
    ///
    /// # Arguments
    ///
    /// * `stager` - Its state
    fn judge_stages(&self, stager: &mut Stager) {
        let Some(view) = &stager.pg else {
            return;
        };
        let mut answers: BTreeMap<(Pos, SliceId), bool> = BTreeMap::new();
        for at in &stager.staged_at {
            answers.insert(*at, true);
        }
        for at in &stager.refused_at {
            answers.insert(*at, false);
        }
        for pos in stager.touched.clone() {
            match judged(&answers, &self.counted(&view.state, pos), pos) {
                Some(true) => {
                    stager.staged.insert(pos);
                }
                Some(false) => {
                    stager.refused.insert(pos);
                }
                None => {}
            }
        }
    }

    /// Whether the last stripe is left to a put that has not finished (M16)
    ///
    /// # Arguments
    ///
    /// * `until_ok` - Whether the put has to have committed, rather than merely have an answer
    pub(crate) fn late_put_pending(&self, until_ok: bool) -> bool {
        if !self.params.late_put || !self.policy.placement.grouped() {
            return false;
        }
        match self.ledger.records.get(&LATE_PUT).and_then(|record| record.outcome.as_ref()) {
            None => true,
            Some(outcome) => until_ok && *outcome != StripeOutcome::Ok,
        }
    }

    /// What a write that gives up without proposing is told
    ///
    /// Refused, on its first try: nothing of it can commit. On a later try an earlier one's
    /// proposal may still be in flight, so it is not known.
    ///
    /// # Arguments
    ///
    /// * `op` - The write
    fn unproposed(&self, op: OpId) -> StripeOutcome {
        if self.rounds.get(&op).copied().unwrap_or(0) == 0 {
            StripeOutcome::Refused
        } else {
            StripeOutcome::Unknown
        }
    }

    /// A write's commit is done: extend the size if it wrote past it, or acknowledge it
    ///
    /// # Arguments
    ///
    /// * `op` - The write
    /// * `stripe` - The stripe
    /// * `stager` - Its state
    fn finish_write(&mut self, op: OpId, stripe: StripeIx, stager: &Stager) {
        let (_, entry) = stager.entry.clone().expect("read");
        let start = u32::from(stripe.0) * self.layout().data_units() as u32;
        let end = start + u32::from(stager.units.iter().max().copied().unwrap_or(0)) + 1;
        if end > entry.size {
            let mut stager = stager.clone();
            stager.phase = StagerPhase::Extend;
            let me = self.endpoint(op);
            self.send(
                me,
                Endpoint::Entry,
                Body::EntryPropose(EntryCommand::Extend {
                    op,
                    to: end,
                    read_epoch: entry.epoch,
                }),
            );
            self.drivers.insert(op, Driver::Write(Box::new(stager)));
        } else {
            self.complete(op, StripeOutcome::Ok);
        }
    }

    /// A reader receives a message
    ///
    /// # Arguments
    ///
    /// * `op` - The read
    /// * `reader` - Its state
    /// * `body` - The message
    fn reader_receive(&mut self, op: OpId, mut reader: Reader, body: &Body) {
        let stripe = reader.stripe;
        match (reader.phase, body) {
            (ReaderPhase::Entry, Body::EntryAnswer { index, state }) => {
                reader.entry = Some((*index, state.clone()));
                reader.phase = ReaderPhase::Row;
                let me = self.endpoint(op);
                let strong = self.row_level(&reader);
                self.send(me, Endpoint::Row(stripe), Body::RowRead { strong });
            }
            (ReaderPhase::Row, Body::RowAnswer { index, state, pg, .. }) => {
                reader.row = Some((*index, state.clone()));
                reader.pg = pg.as_ref().map(|view| view.state.clone());
                let (_, entry) = reader.entry.clone().expect("read");
                let start = u32::from(stripe.0) * self.layout().data_units() as u32;
                // Q18: a row stamped past the entry read means a truncate it has not seen
                if self.policy.reader_entry == EntryRule::ForwardToStamp
                    && state.stamp > entry.epoch
                {
                    if !self.reread(op, &mut reader, true) {
                        return;
                    }
                } else if hole(state, &entry, start) {
                    let zeros = vec![Unit::Zero; self.layout().data_units()];
                    self.finish_read(op, &reader, zeros, Vec::new());
                    return;
                } else {
                    let pg = reader.pg.clone();
                    reader.reads = self.start_reads(op, stripe, state, None, pg.as_ref());
                    reader.phase = ReaderPhase::Chunks;
                    if reader.reads.candidates.len() < self.layout().k() {
                        self.drivers.remove(&op);
                        self.complete(op, StripeOutcome::Failed);
                        return;
                    }
                }
            }
            (
                ReaderPhase::Chunks,
                Body::ChunkAnswer {
                    pos, label, reply, ..
                },
            ) => {
                let row = reader.row.clone().expect("read").1;
                let mut reads = std::mem::take(&mut reader.reads);
                let rule = self.policy.small_writes.reader;
                let step =
                    self.take_chunk(op, stripe, &row, &mut reads, *pos, *label, reply, rule);
                reader.reads = reads;
                match step {
                    ReadStep::Wait => {}
                    ReadStep::Done => {
                        let data = self.decoded(&reader.reads);
                        let used = reader
                            .reads
                            .got
                            .iter()
                            .map(|(pos, (label, _))| (*pos, *label))
                            .collect();
                        self.finish_read(op, &reader, data, used);
                        return;
                    }
                    ReadStep::Newer(pos, label, content) => {
                        // POLICY P10: the contract reads its row again; the unsafe reader takes the chunk
                        if self.policy.reader == ReaderRule::AcceptsNewer {
                            reader.reads.got.insert(pos, (label, content));
                            if reader.reads.got.len() >= self.layout().k() {
                                let data = self.decoded(&reader.reads);
                                let used = reader
                                    .reads
                                    .got
                                    .iter()
                                    .map(|(pos, (label, _))| (*pos, *label))
                                    .collect();
                                self.finish_read(op, &reader, data, used);
                                return;
                            }
                        } else if !self.reread(op, &mut reader, false) {
                            return;
                        }
                    }
                    ReadStep::Exhausted => {
                        if !self.reread(op, &mut reader, false) {
                            return;
                        }
                    }
                }
            }
            _ => return,
        }
        if self.drivers.contains_key(&op) {
            self.drivers.insert(op, Driver::Read(Box::new(reader)));
        }
    }

    /// A reader reads its row again, or its entry and then its row, in a new round
    ///
    /// Returns false if it has read again too often and failed by name.
    ///
    /// # Arguments
    ///
    /// * `op` - The read
    /// * `reader` - Its state
    /// * `entry` - Whether to read the entry again first
    fn reread(&mut self, op: OpId, reader: &mut Reader, entry: bool) -> bool {
        reader.rereads += 1;
        if reader.rereads > MAX_REREADS {
            self.drivers.remove(&op);
            self.complete(op, StripeOutcome::Failed);
            return false;
        }
        *self.rounds.entry(op).or_insert(0) += 1;
        reader.reads = ChunkReads::default();
        let me = self.endpoint(op);
        if entry {
            reader.phase = ReaderPhase::Entry;
            self.send(me, Endpoint::Entry, Body::EntryRead { strong: true });
        } else {
            reader.phase = ReaderPhase::Row;
            let strong = self.row_level(reader);
            self.send(me, Endpoint::Row(reader.stripe), Body::RowRead { strong });
        }
        true
    }

    /// Whether a reader takes its row at the leader
    ///
    /// # Arguments
    ///
    /// * `reader` - The reader
    fn row_level(&self, reader: &Reader) -> bool {
        // POLICY P12: the contract takes even a default read's row at the leader, after the entry
        reader.strong || self.policy.reader_row == RowLevel::AfterEntryAtLeader
    }

    /// A read returns: the data units, hidden under floors and clipped at the size
    ///
    /// # Arguments
    ///
    /// * `op` - The read
    /// * `reader` - Its state
    /// * `data` - The stripe's data units as decoded
    /// * `used` - The chunks it decoded from
    fn finish_read(&mut self, op: OpId, reader: &Reader, data: Vec<Unit>, used: Vec<(Pos, Label)>) {
        let (entry_index, entry) = reader.entry.clone().expect("read");
        let (row_index, row) = reader.row.clone().expect("read");
        let start = u32::from(reader.stripe.0) * self.layout().data_units() as u32;
        let units = data
            .iter()
            .enumerate()
            .map(|(unit, value)| {
                let offset = start + unit as u32;
                if offset >= entry.size {
                    return None;
                }
                // POLICY P13: the contract hides a unit past a floor its stripe is stamped below
                let hidden = self.policy.epoch == EpochRule::Stamped
                    && entry
                        .floors
                        .iter()
                        .any(|floor| offset >= floor.len && row.stamp < floor.epoch);
                Some(if hidden { Unit::Zero } else { *value })
            })
            .collect();
        let result = ReadResult {
            row: row_index,
            entry: entry_index,
            row_seen: (self.rows[usize::from(reader.stripe.0)].history.len() - 1) as u32,
            entry_seen: (self.entry.history.len() - 1) as u32,
            used,
            units,
        };
        if reader.strong {
            self.checker.coverage.strong_reads += 1;
        } else {
            self.checker.coverage.default_reads += 1;
        }
        self.drivers.remove(&op);
        self.complete(op, StripeOutcome::Read(result));
    }

    /// A rebuild or a move receives a message
    ///
    /// # Arguments
    ///
    /// * `op` - The driver
    /// * `mover` - Its state
    /// * `body` - The message
    fn mover_receive(&mut self, op: OpId, mut mover: Mover, body: &Body) {
        let me = self.endpoint(op);
        let stripe = mover.stripe;
        let pos = mover.pos;
        match (mover.phase, body) {
            (MoverPhase::Row, Body::RowAnswer { index, state, pg, .. }) => {
                mover.row = Some((*index, state.clone()));
                mover.pg = pg.clone();
                // M16: while the group is in both generations a moving position is the copier's,
                // unless the unsafe setting rebuilds it on its old slice alone
                let moving = pg
                    .as_ref()
                    .is_some_and(|view| view.state.arrival(pos).is_some());
                if moving && self.policy.placement.both == BothRule::BothHold {
                    self.drivers.remove(&op);
                    self.complete(op, StripeOutcome::Ok);
                    return;
                }
                // a rebuild of a current chunk, or a move to where it already is, has nothing to do
                let done = match mover.to {
                    None => state.current(pos) || state.tombstone,
                    Some(to) => state.slice(pos) == to || state.tombstone,
                };
                if done {
                    self.drivers.remove(&op);
                    self.complete(op, StripeOutcome::Ok);
                    return;
                }
                let home = pg.as_ref().map_or(state.slice(pos), |view| view.state.slice(pos));
                let target = mover.to.unwrap_or(home);
                mover.target = Some(target);
                // a move of a position that holds nothing current switches it, with nothing to copy
                if mover.to.is_some() && !state.current(pos) {
                    let row = state.clone();
                    self.propose_rebuild(op, &mut mover, &row, false);
                    self.drivers.insert(op, Driver::Rebuild(Box::new(mover)));
                    return;
                }
                mover.held = self
                    .slices
                    .get(&target)
                    .is_some_and(|s| s.holds(stripe, pos, state.label(pos)));
                // POLICY progress: the contract asks the holder what it holds before writing; with
                // the group's state the group's, which label its chunk is at, for the record's units
                if mover.to.is_none() && self.policy.rebuild == RebuildRule::AsksHolder {
                    mover.phase = MoverPhase::Ask;
                    let body = if pg.is_some() {
                        Body::Inquire { stripe, pos }
                    } else {
                        Body::Confirm {
                            stripe,
                            pos,
                            label: state.label(pos),
                            // a rebuild asks for the label exactly: a holder at a base the pending
                            // bytes fold from is rewritten whole
                            also: Vec::new(),
                        }
                    };
                    self.send(me, Endpoint::Slice(target), body);
                } else {
                    let view = pg.as_ref().map(|view| view.state.clone());
                    mover.reads = self.start_reads(op, stripe, state, Some(pos), view.as_ref());
                    mover.phase = MoverPhase::Read;
                }
            }
            (
                MoverPhase::Ask,
                Body::Held {
                    label, unreachable, ..
                },
            ) => {
                let (_, row) = mover.row.clone().expect("read");
                // a holder that cannot be asked cannot be rebuilt onto either: try again later
                if *unreachable {
                    self.drivers.remove(&op);
                    self.complete(op, StripeOutcome::Failed);
                    return;
                }
                if *label == Some(row.label(pos)) {
                    // a lost acknowledgement left a chunk current after all: commit it, unwritten
                    self.propose_rebuild(op, &mut mover, &row, true);
                } else {
                    mover.held = false;
                    mover.held_label = *label;
                    let view = mover.pg.as_ref().map(|view| view.state.clone());
                    mover.reads = self.start_reads(op, stripe, &row, Some(pos), view.as_ref());
                    mover.phase = MoverPhase::Read;
                }
            }
            (
                MoverPhase::Ask,
                Body::ConfirmAnswer {
                    holds, unreachable, ..
                },
            ) => {
                let (_, row) = mover.row.clone().expect("read");
                // a holder that cannot be asked cannot be rebuilt onto either: try again later
                if *unreachable {
                    self.drivers.remove(&op);
                    self.complete(op, StripeOutcome::Failed);
                    return;
                }
                if *holds {
                    // a lost acknowledgement left a chunk current after all: commit it, unwritten
                    self.propose_rebuild(op, &mut mover, &row, true);
                } else {
                    // the holder said it did not hold the label, so it did not hold it throughout,
                    // whatever it holds by the time the rebuild writes
                    mover.held = false;
                    mover.reads = self.start_reads(op, stripe, &row, Some(pos), None);
                    mover.phase = MoverPhase::Read;
                }
            }
            (
                MoverPhase::Read,
                Body::ChunkAnswer {
                    pos: from,
                    label,
                    reply,
                    ..
                },
            ) => {
                let row = mover.row.clone().expect("read").1;
                let mut reads = std::mem::take(&mut mover.reads);
                let rule = self.policy.small_writes.rebuild;
                let step =
                    self.take_chunk(op, stripe, &row, &mut reads, *from, *label, reply, rule);
                mover.reads = reads;
                match step {
                    ReadStep::Wait => {}
                    ReadStep::Done => {
                        let data = self.decoded(&mover.reads);
                        let content = encode(self.layout(), pos, &data);
                        let target = mover.target.expect("chosen");
                        // the progress check: a rebuild rewrites only a chunk its holder did not
                        // hold, from when it read the row to when it writes, and could be asked
                        if mover.to.is_none() {
                            let held = mover.held
                                && self.answers(target)
                                && self.slices[&target].holds(stripe, pos, row.label(pos));
                            self.progress
                                .on_rebuild_write(op, stripe, pos, held, self.step);
                            if self.stalled.is_none() {
                                self.stalled = self.progress.failure.clone();
                            }
                        }
                        // M16: a replicated chunk the record names the units of, and whose holder
                        // is at a label it recorded writes after, is given those units alone
                        let units = match (&mover.pg, mover.held_label) {
                            (Some(view), Some(held))
                                if self.layout().is_replicated() && row.pending_bytes.is_none() =>
                            {
                                view.missed[usize::from(pos.0)]
                                    .as_ref()
                                    .and_then(|entry| entry.units_after(held))
                                    .filter(|units| units.len() < self.layout().data_units())
                            }
                            _ => None,
                        };
                        let (expects, payload) = match &units {
                            Some(units) => (
                                mover.held_label,
                                Payload::Units(
                                    units.iter().map(|unit| (*unit, data[usize::from(*unit)])).collect(),
                                ),
                            ),
                            None => (None, Payload::Whole(content)),
                        };
                        // the units cover every write since the label the holder answered, so
                        // any later label of the position it reaches meanwhile is a base too
                        let over: Vec<Label> = match (&units, &mover.pg, mover.held_label) {
                            (Some(_), Some(view), Some(held)) => match &view.missed[usize::from(pos.0)] {
                                Some(crate::stripe::group::Missed::Ranges { writes, .. }) => writes
                                    .iter()
                                    .map(|(label, _)| *label)
                                    .filter(|label| label.seq > held.seq && label.seq < row.label(pos).seq)
                                    .collect(),
                                _ => Vec::new(),
                            },
                            _ => Vec::new(),
                        };
                        mover.units = units;
                        mover.phase = MoverPhase::Stage;
                        self.send(
                            me,
                            Endpoint::Slice(target),
                            Body::Stage {
                                stripe,
                                pos,
                                label: row.label(pos),
                                base_exists: row.exists,
                                base: row.seq,
                                expects,
                                payload,
                                over,
                            },
                        );
                    }
                    ReadStep::Newer(..) | ReadStep::Exhausted => {
                        self.drivers.remove(&op);
                        self.complete(op, StripeOutcome::Failed);
                        return;
                    }
                }
            }
            (MoverPhase::Stage, Body::StageAnswer { refused, .. }) => {
                if refused.is_some() {
                    self.drivers.remove(&op);
                    self.complete(op, StripeOutcome::Failed);
                    return;
                }

                let (_, row) = mover.row.clone().expect("read");
                self.propose_rebuild(op, &mut mover, &row, true);
            }
            (MoverPhase::Commit, Body::Decided(decision)) => {
                self.drivers.remove(&op);
                match decision {
                    Decision::Committed { .. } => {
                        let (_, row) = mover.row.clone().expect("read");
                        let target = mover.target.expect("chosen");
                        if row.current(pos) || mover.to.is_none() {
                            self.send(
                                me,
                                Endpoint::Slice(target),
                                Body::Apply {
                                    stripe,
                                    label: row.label(pos),
                                },
                            );
                        }
                        if mover.to.is_some() {
                            self.checker.coverage.moves += 1;
                        } else {
                            self.checker.coverage.rebuilds += 1;
                        }
                        if mover.units.is_some() {
                            self.checker.coverage.unit_rebuilds += 1;
                        }
                        self.complete(op, StripeOutcome::Ok);
                    }
                    _ => {
                        // POLICY P16: the contract keeps what it staged, a label a committed row
                        // named; the unsafe setting tells the holder to drop it
                        let dropped = self.policy.placement.refusal == RebuildRefusal::Dropped;
                        if mover.pg.is_some() && dropped && mover.phase == MoverPhase::Commit {
                            let (_, row) = mover.row.clone().expect("read");
                            let target = mover.target.expect("chosen");
                            self.send(
                                me,
                                Endpoint::Slice(target),
                                Body::Drop {
                                    stripe,
                                    label: row.label(pos),
                                },
                            );
                        }
                        self.complete(op, StripeOutcome::Failed);
                    }
                }
                return;
            }
            _ => return,
        }
        if self.drivers.contains_key(&op) {
            self.drivers.insert(op, Driver::Rebuild(Box::new(mover)));
        }
    }

    /// Propose a rebuilt or moved chunk current under the label it holds, or a bare switch
    ///
    /// # Arguments
    ///
    /// * `op` - The driver
    /// * `mover` - Its state
    /// * `row` - The row it read
    /// * `current` - Whether the slice holds the chunk now
    fn propose_rebuild(&mut self, op: OpId, mover: &mut Mover, row: &RowState, current: bool) {
        let cmd = StripeCommand::Rebuild {
            op,
            pos: mover.pos,
            label: row.label(mover.pos),
            base: row.seq,
            generation: row.generation,
            slice: mover.target.expect("chosen"),
            switch: mover.to.is_some(),
            current,
            epoch: mover.pg.as_ref().map(|view| view.state.epoch),
        };
        mover.phase = MoverPhase::Commit;
        let me = self.endpoint(op);
        self.send(me, Endpoint::Row(mover.stripe), Body::Propose(cmd));
    }

    /// The reclaimer receives a message
    ///
    /// # Arguments
    ///
    /// * `op` - The reclaimer
    /// * `reclaimer` - Its state
    /// * `msg` - The message
    fn reclaimer_receive(
        &mut self,
        op: OpId,
        mut reclaimer: Reclaimer,
        msg: &crate::stripe::event::Message,
    ) {
        let me = self.endpoint(op);
        let data_units = self.layout().data_units() as u32;
        // it speaks to one stripe's group at a time: an answer from another is a late one
        if let Endpoint::Row(stripe) = msg.from {
            // the stripe the cut falls inside is the one it speaks to last (M20)
            let speaking = match reclaimer.phase {
                ReclaimPhase::Cut => reclaimer.cut,
                _ => reclaimer.stripes.get(reclaimer.at).copied(),
            };
            if speaking != Some(stripe) {
                return;
            }
        }
        let body = &msg.body;
        match (reclaimer.phase, body) {
            (ReclaimPhase::Entry, Body::EntryAnswer { state, .. }) => {
                // the oldest floor, and the stripes wholly past it
                let Some(floor) = state.floors.iter().min_by_key(|floor| floor.epoch).copied()
                else {
                    self.drivers.remove(&op);
                    self.complete(op, StripeOutcome::Ok);
                    return;
                };
                reclaimer.floor = Some(floor);
                reclaimer.stripes = (0..self.rows.len() as u8)
                    .map(StripeIx)
                    .filter(|stripe| u32::from(stripe.0) * data_units >= floor.len)
                    .collect();
                // a stripe the cut falls inside keeps units under the floor, so the floor stays; M20
                // judges that stripe's row once the rest are reclaimed, and the floor goes if a
                // write since stamped it past the floor, which wrote what it hid as zeros
                reclaimer.all = floor.len % data_units == 0;
                if !reclaimer.all && self.policy.reclamation.released() {
                    match self.policy.reclamation.drop {
                        FloorDrop::CutRewritten => {
                            reclaimer.all = true;
                            reclaimer.cut = Some(StripeIx((floor.len / data_units) as u8));
                        }
                        // POLICY P13: the unsafe setting drops it whatever the cut stripe holds
                        FloorDrop::Always => reclaimer.all = true,
                    }
                }
                if !self.next_reclaim(op, &mut reclaimer) {
                    return;
                }
            }
            (ReclaimPhase::Row, Body::RowAnswer { state, pg, .. }) => {
                let floor = reclaimer.floor.expect("chosen");
                let stripe = reclaimer.stripes[reclaimer.at];
                reclaimer.row = Some(state.clone());
                reclaimer.pg = pg.as_ref().map(|view| view.state.clone());
                // reclaimed or written since this floor's truncate: nothing it hides. A tombstone
                // left under an older floor is reclaimed again, so no writer that read an epoch
                // below this floor can commit to it once the floor is gone
                if state.exists && state.stamp >= floor.epoch {
                    reclaimer.at += 1;
                    if !self.next_reclaim(op, &mut reclaimer) {
                        return;
                    }
                } else {
                    reclaimer.phase = ReclaimPhase::Commit;
                    self.send(
                        me,
                        Endpoint::Row(stripe),
                        Body::Propose(StripeCommand::Reclaim {
                            op,
                            base_exists: state.exists,
                            base: state.seq,
                            below: floor.epoch,
                        }),
                    );
                }
            }
            (ReclaimPhase::Commit, Body::Decided(decision)) => {
                let stripe = reclaimer.stripes[reclaimer.at];
                match decision {
                    Decision::Committed { .. } => {
                        // the holders may discard now: the commit is the fact
                        let row = reclaimer.row.clone().expect("read");
                        let positions = match &reclaimer.pg {
                            Some(state) => state.positions.clone(),
                            None => row.positions.clone(),
                        };
                        for slice in positions {
                            self.send(
                                me,
                                Endpoint::Slice(slice),
                                Body::Discard {
                                    stripe,
                                    through: row.seq,
                                },
                            );
                        }
                        self.checker.coverage.reclaims += 1;
                    }
                    _ => reclaimer.all = false,
                }
                reclaimer.at += 1;
                if !self.next_reclaim(op, &mut reclaimer) {
                    return;
                }
            }
            (ReclaimPhase::Cut, Body::RowAnswer { state, .. }) => {
                // the floor goes only if a write since its truncate stamped the cut stripe past it
                let floor = reclaimer.floor.expect("chosen");
                if !(state.exists && state.stamp >= floor.epoch) {
                    reclaimer.all = false;
                }
                reclaimer.cut = None;
                if !self.next_reclaim(op, &mut reclaimer) {
                    return;
                }
            }
            (ReclaimPhase::Drop, Body::Decided(_)) => {
                // counted only under M20's rules: a run saved before them folds the same line
                // into its digest, and its old drops counted would change it
                if matches!(body, Body::Decided(Decision::Committed { .. })) && !self.policy.reclamation.is_default() {
                    self.checker.coverage.dropped_floors += 1;
                }
                self.drivers.remove(&op);
                self.complete(op, StripeOutcome::Ok);
                return;
            }
            _ => return,
        }
        if self.drivers.contains_key(&op) {
            self.drivers
                .insert(op, Driver::Reclaim(Box::new(reclaimer)));
        }
    }

    /// The leader's clear receives a message
    ///
    /// It reads the row at the leader, gives every position's holder the pending bytes to fold,
    /// and once every holder has answered, and `k + f` of them say they hold their label durably,
    /// commits the clear, which marks every other position missed. A holder answers a fold as it
    /// answers a stage: at once if it holds the label or cannot fold, once it is synced otherwise,
    /// and a slice that is down or gone refuses at once.
    ///
    /// # Arguments
    ///
    /// * `op` - The driver
    /// * `clearer` - Its state
    /// * `from` - Who sent the message
    /// * `body` - The message
    fn clearer_receive(&mut self, op: OpId, mut clearer: Clearer, from: Endpoint, body: &Body) {
        let me = self.endpoint(op);
        let stripe = clearer.stripe;
        match (clearer.phase, body) {
            (ClearPhase::Row, Body::RowAnswer { state, pg, .. }) => {
                clearer.pg = pg.as_ref().map(|view| view.state.clone());
                // nothing pending, or reclaimed: nothing to clear
                let Some(pending) = state.pending_bytes.clone().filter(|_| !state.tombstone)
                else {
                    self.drivers.remove(&op);
                    self.complete(op, StripeOutcome::Ok);
                    return;
                };
                clearer.row = Some(state.clone());
                clearer.phase = ClearPhase::Fold;
                for pos in self.layout().positions() {
                    let slices = match &clearer.pg {
                        Some(pg) => pg.slices(pos),
                        None => vec![state.slice(pos)],
                    };
                    for slice in slices {
                        self.send(
                            me,
                            Endpoint::Slice(slice),
                            Body::Fold {
                                stripe,
                                pos,
                                pending: pending.clone(),
                            },
                        );
                    }
                }
            }
            (
                ClearPhase::Fold,
                Body::StageAnswer {
                    pos,
                    label,
                    refused,
                    ..
                },
            ) => {
                let row = clearer.row.clone().expect("read");
                let pending = row.pending_bytes.clone().expect("checked when read");
                if *label != pending.label {
                    return;
                }
                match (&clearer.pg, from) {
                    // M16: a moving position holds once every slice it counts on folded
                    (Some(pg), Endpoint::Slice(slice)) => {
                        clearer.answered_at.insert((*pos, slice));
                        if refused.is_none() {
                            clearer.holding_at.insert((*pos, slice));
                        }
                        let every: usize = self
                            .layout()
                            .positions()
                            .into_iter()
                            .map(|pos| pg.slices(pos).len())
                            .sum();
                        let counted = self.counted(pg, *pos);
                        if counted.iter().all(|slice| clearer.holding_at.contains(&(*pos, *slice))) {
                            clearer.holding.insert(*pos);
                        }
                        // a position has answered once all its slices have, and the clear waits
                        // for every slice of every position
                        if clearer.answered_at.len() == every {
                            clearer.answered = self.layout().positions().into_iter().collect();
                        }
                    }
                    _ => {
                        clearer.answered.insert(*pos);
                        if refused.is_none() {
                            clearer.holding.insert(*pos);
                        }
                    }
                }
                // POLICY P11: the contract hears every holder out and needs k + f of them; the
                // unsafe setting clears on the first one's word
                let ready = match self.policy.small_writes.clear {
                    ClearRule::KPlusF => {
                        clearer.answered.len() == self.layout().width()
                            && clearer.holding.len() >= self.layout().ack_floor()
                    }
                    ClearRule::FirstHolder => !clearer.holding.is_empty(),
                };
                if ready {
                    clearer.phase = ClearPhase::Commit;
                    self.send(
                        me,
                        Endpoint::Row(stripe),
                        Body::Propose(StripeCommand::ClearPendingBytes {
                            op,
                            base: row.seq,
                            generation: row.generation,
                            label: pending.label,
                            holding: clearer.holding.iter().copied().collect(),
                            epoch: clearer.pg.as_ref().map(|pg| pg.epoch),
                        }),
                    );
                } else if clearer.answered.len() == self.layout().width() {
                    // every holder answered and too few hold it: try again another time
                    self.drivers.remove(&op);
                    self.complete(op, StripeOutcome::Failed);
                    return;
                }
            }
            (ClearPhase::Commit, Body::Decided(decision)) => {
                self.drivers.remove(&op);
                let outcome = match decision {
                    Decision::Committed { .. } => StripeOutcome::Ok,
                    _ => StripeOutcome::Failed,
                };
                self.complete(op, outcome);
                return;
            }
            _ => return,
        }
        if self.drivers.contains_key(&op) {
            self.drivers.insert(op, Driver::Clear(Box::new(clearer)));
        }
    }

    /// The reclaimer moves to its next stripe, or drops the floor once every one is done
    ///
    /// Returns false if it finished.
    ///
    /// # Arguments
    ///
    /// * `op` - The reclaimer
    /// * `reclaimer` - Its state
    fn next_reclaim(&mut self, op: OpId, reclaimer: &mut Reclaimer) -> bool {
        let me = self.endpoint(op);
        if let Some(stripe) = reclaimer.stripes.get(reclaimer.at).copied() {
            reclaimer.phase = ReclaimPhase::Row;
            self.send(me, Endpoint::Row(stripe), Body::RowRead { strong: true });
            return true;
        }
        // M20: the stripe the cut falls inside is judged last, once every stripe past it is gone
        if let (true, Some(cut)) = (reclaimer.all, reclaimer.cut) {
            reclaimer.phase = ReclaimPhase::Cut;
            self.send(me, Endpoint::Row(cut), Body::RowRead { strong: true });
            return true;
        }
        let floor = reclaimer.floor.expect("chosen");
        if reclaimer.all {
            reclaimer.phase = ReclaimPhase::Drop;
            self.send(
                me,
                Endpoint::Entry,
                Body::EntryPropose(EntryCommand::DropFloor {
                    op,
                    epoch: floor.epoch,
                }),
            );
            return true;
        }
        self.drivers.remove(&op);
        self.complete(op, StripeOutcome::Ok);
        false
    }
}

impl StripeWorld {
    /// The placement group's move of a position begins, or a move another driver left in flight
    /// is taken up again (M16)
    ///
    /// # Arguments
    ///
    /// * `op` - Its identity
    /// * `pos` - The position
    /// * `to` - The slice it moves to
    pub(crate) fn invoke_pg_move(&mut self, op: OpId, pos: Pos, to: SliceId) -> bool {
        if !self.policy.placement.grouped()
            || usize::from(pos.0) >= self.layout().width()
            || !self.slices.contains_key(&to)
        {
            return false;
        }
        // a move in flight whose driver is gone is taken up by this one, as a new leader would
        let latest = self.pg.latest().clone();
        let (pos, to, move_op) = match latest.moving {
            Some(moving) => {
                let driven = self.drivers.values().any(|driver| {
                    matches!(driver, Driver::Copy(copier) if copier.move_op == moving.op)
                });
                if driven {
                    return false;
                }
                (moving.pos, moving.to, moving.op)
            }
            None => (pos, to, op),
        };
        if !self.ledger.invoke(op, OpKind::PgMove { pos, to }, self.step) {
            return false;
        }
        self.drivers.insert(
            op,
            Driver::Copy(Box::new(Copier {
                pos,
                to,
                move_op,
                phase: CopyPhase::Read,
                state: None,
                stripe: 0,
                row: None,
                listed: BTreeMap::new(),
                reached: 0,
                created: None,
                reads: ChunkReads::default(),
            })),
        );
        let me = self.endpoint(op);
        self.send(me, Endpoint::Pg, Body::PgRead);
        true
    }

    /// The put of the object's last stripe begins (M16)
    pub(crate) fn invoke_put(&mut self) -> bool {
        if !self.policy.placement.grouped() || !self.params.late_put {
            return false;
        }
        if !self.ledger.invoke(LATE_PUT, OpKind::Put, self.step) {
            return false;
        }
        self.drivers.insert(
            LATE_PUT,
            Driver::Put(Box::new(Putter {
                phase: PutPhase::Entry,
                entry: None,
                placed: None,
                now: None,
                answers: BTreeMap::new(),
                marks: Vec::new(),
            })),
        );
        let me = self.endpoint(LATE_PUT);
        self.send(me, Endpoint::Entry, Body::EntryRead { strong: true });
        true
    }

    /// A backfill of a position whose record overflowed begins (M16)
    ///
    /// # Arguments
    ///
    /// * `op` - Its identity
    /// * `pos` - The position
    pub(crate) fn invoke_backfill(&mut self, op: OpId, pos: Pos) -> bool {
        if !self.policy.placement.grouped() || usize::from(pos.0) >= self.layout().width() {
            return false;
        }
        if !self.pg.record.positions[usize::from(pos.0)].overflowed {
            return false;
        }
        if !self.ledger.invoke(op, OpKind::Backfill { pos }, self.step) {
            return false;
        }
        self.drivers.insert(
            op,
            Driver::Backfill(Box::new(Backfiller {
                pos,
                phase: BackfillPhase::Read,
                drops: 0,
                stripe: 0,
                whole: true,
            })),
        );
        let me = self.endpoint(op);
        self.send(me, Endpoint::Pg, Body::PgRead);
        true
    }

    /// The release of a slice the placement group left begins (M20)
    ///
    /// # Arguments
    ///
    /// * `op` - Its identity
    /// * `slice` - The slice
    pub(crate) fn invoke_release(&mut self, op: OpId, slice: SliceId) -> bool {
        if !self.policy.placement.grouped() || !self.policy.reclamation.released() {
            return false;
        }
        if !self.pg.latest().left.contains(&slice) {
            return false;
        }
        if !self.ledger.invoke(op, OpKind::Release { slice }, self.step) {
            return false;
        }
        let me = self.endpoint(op);
        // POLICY P16, P17: the contract seals the slice before it reads the group's state; the
        // unsafe setting reads the state and tells the slice to release with no seal
        let sealed = self.policy.reclamation.release == ReleaseRule::Sealed;
        let phase = if sealed {
            self.send(me, Endpoint::Slice(slice), Body::Seal { seal: op });
            ReleasePhase::Seal
        } else {
            self.send(me, Endpoint::Pg, Body::PgRead);
            ReleasePhase::Read
        };
        self.drivers.insert(
            op,
            Driver::Release(Box::new(Releaser {
                slice,
                phase,
                sealed,
            })),
        );
        true
    }

    /// A release receives a message
    ///
    /// It seals the slice, then reads the group's state at its leader. A slice the state shows
    /// neither holding a position nor arriving at one is told to release under its seal; one that
    /// holds or arrives again is unsealed. Either way the group stops naming it once it is done.
    ///
    /// # Arguments
    ///
    /// * `op` - The release
    /// * `releaser` - Its state
    /// * `msg` - The message
    fn releaser_receive(&mut self, op: OpId, mut releaser: Releaser, msg: &crate::stripe::event::Message) {
        let me = self.endpoint(op);
        let slice = releaser.slice;
        match (releaser.phase, &msg.body) {
            (ReleasePhase::Seal, Body::Sealed { unreachable }) if msg.from == Endpoint::Slice(slice) => {
                // a slice that cannot be reached is released another time
                if *unreachable {
                    self.drivers.remove(&op);
                    self.complete(op, StripeOutcome::Failed);
                    return;
                }
                releaser.phase = ReleasePhase::Read;
                self.send(me, Endpoint::Pg, Body::PgRead);
            }
            (ReleasePhase::Read, Body::PgAnswer { state, .. }) => {
                // another release finished it already
                if !state.left.contains(&slice) {
                    if releaser.sealed {
                        self.send(me, Endpoint::Slice(slice), Body::Unseal { seal: op });
                    }
                    self.drivers.remove(&op);
                    self.complete(op, StripeOutcome::Ok);
                    return;
                }
                // a slice that holds a position again, or arrives at one, keeps what it holds
                if state.places_on(slice) {
                    if releaser.sealed {
                        self.send(me, Endpoint::Slice(slice), Body::Unseal { seal: op });
                    }
                    releaser.phase = ReleasePhase::Done;
                    self.send(
                        me,
                        Endpoint::Pg,
                        Body::PgPropose(PgCommand::ReleaseDone { op, slice }),
                    );
                } else {
                    releaser.phase = ReleasePhase::Release;
                    let seal = releaser.sealed.then_some(op);
                    self.send(me, Endpoint::Slice(slice), Body::Release { seal });
                }
            }
            (ReleasePhase::Release, Body::ReleaseAnswer { done }) if msg.from == Endpoint::Slice(slice) => {
                // a seal that lapsed or was taken by another refuses the release, for a later one
                if !*done {
                    self.drivers.remove(&op);
                    self.complete(op, StripeOutcome::Failed);
                    return;
                }
                releaser.phase = ReleasePhase::Done;
                self.send(
                    me,
                    Endpoint::Pg,
                    Body::PgPropose(PgCommand::ReleaseDone { op, slice }),
                );
            }
            (ReleasePhase::Done, Body::Decided(decision)) if msg.from == Endpoint::Pg => {
                self.drivers.remove(&op);
                let outcome = if matches!(decision, Decision::Committed { .. }) {
                    StripeOutcome::Ok
                } else {
                    StripeOutcome::Failed
                };
                self.complete(op, outcome);
                return;
            }
            _ => return,
        }
        if self.drivers.contains_key(&op) {
            self.drivers.insert(op, Driver::Release(Box::new(releaser)));
        }
    }

    /// A move's copier goes on to a stripe: its row read at the leader, its chunks listed on the
    /// old holders; or, past the last, the switch
    ///
    /// # Arguments
    ///
    /// * `op` - The move
    /// * `copier` - Its state
    fn copy_stripe(&mut self, op: OpId, copier: &mut Copier) {
        // a new round for each stripe, so an answer about the last one is dropped
        *self.rounds.entry(op).or_insert(0) += 1;
        let me = self.endpoint(op);
        let state = copier.state.clone().expect("begun");
        copier.row = None;
        copier.listed.clear();
        copier.reached = 0;
        copier.reads = ChunkReads::default();
        if usize::from(copier.stripe) >= self.rows.len() {
            // the arrival has to answer before it takes its position; a move onto one that does not
            // is given up
            if !self.believed_up(copier.to) {
                copier.phase = CopyPhase::Abort;
                self.send(me, Endpoint::Pg, Body::PgPropose(PgCommand::MoveAbort { op: copier.move_op }));
                return;
            }
            copier.phase = CopyPhase::Switch;
            self.send(me, Endpoint::Pg, Body::PgPropose(PgCommand::MoveSwitch { op: copier.move_op }));
            return;
        }
        let stripe = StripeIx(copier.stripe);
        copier.phase = CopyPhase::Row;
        self.send(me, Endpoint::Row(stripe), Body::RowRead { strong: true });
        // the old holders it lists: every one but the moving position's, whose slice may be why
        // it moves. A put marks every position its pieces did not reach, so a stripe with no row
        // is on every holder, and one that answers decides it
        let layout = self.layout();
        for pos in layout.positions().into_iter().filter(|pos| *pos != copier.pos) {
            let slice = state.slice(pos);
            copier.listed.insert(slice, None);
            self.send(me, Endpoint::Slice(slice), Body::List { stripe, pos });
        }
    }

    /// A move's copier has a stripe's row and its listings: copy it, or pass it by
    ///
    /// # Arguments
    ///
    /// * `op` - The move
    /// * `copier` - Its state
    fn copy_decide(&mut self, op: OpId, copier: &mut Copier) {
        let Some(row) = copier.row.clone() else {
            return;
        };
        if copier.listed.values().any(Option::is_none) {
            return;
        }
        let listed = copier.listed.values().any(|has| *has == Some(true));
        let pos = copier.pos;
        // a stripe with no row is found by its holders' listings alone: if none that answered
        // has it, and none answered at all, the copier cannot tell a stripe from a hole, and the
        // move is given up
        let reached = copier.reached > 0;
        if !row.exists && !listed && !reached {
            copier.phase = CopyPhase::Abort;
            let me = self.endpoint(op);
            self.send(me, Endpoint::Pg, Body::PgPropose(PgCommand::MoveAbort { op: copier.move_op }));
            return;
        }
        // a reclaimed stripe, one no put has written, and one whose position is stale already need
        // nothing on the arrival: the switch moves a stale position as it is
        if row.tombstone || !(row.exists || listed) || !row.current(pos) {
            copier.stripe += 1;
            self.copy_stripe(op, copier);
            return;
        }
        copier.phase = CopyPhase::Ask;
        let me = self.endpoint(op);
        self.send(
            me,
            Endpoint::Slice(copier.to),
            Body::Confirm {
                stripe: StripeIx(copier.stripe),
                pos,
                label: row.label(pos),
                also: row.current_labels(pos).split_off(1),
            },
        );
    }

    /// A move's copier marks a stripe it could not copy missed at the moving position
    ///
    /// # Arguments
    ///
    /// * `op` - The move
    /// * `copier` - Its state
    fn copy_mark(&mut self, op: OpId, copier: &mut Copier) {
        let row = copier.row.clone().expect("read");
        let state = copier.state.clone().expect("begun");
        let me = self.endpoint(op);
        // a copier never marks a position of an object's stripe whose old slice still holds it
        // current: the switch would throw that copy away before the arrival had one. It gives
        // the move up instead, for a later one to try again. A stripe with no row past what the
        // entry says a committed put wrote is a put's in flight, or one given up: it is marked,
        // which makes its row, and a put still running marks it too when it reads the move
        let old = state.slice(copier.pos);
        let lost = self.slices.get(&old).is_none_or(|slice| {
            slice.gone || self.nodes.get(&slice.node).is_some_and(|node| node.reported)
        });
        let start = u32::from(copier.stripe) * self.layout().data_units() as u32;
        let live = row.exists || copier.created.is_some_and(|created| start < created);
        if live && !lost {
            copier.phase = CopyPhase::Abort;
            self.send(me, Endpoint::Pg, Body::PgPropose(PgCommand::MoveAbort { op: copier.move_op }));
            return;
        }
        copier.phase = CopyPhase::Mark;
        self.send(
            me,
            Endpoint::Row(StripeIx(copier.stripe)),
            Body::Propose(StripeCommand::Rebuild {
                op,
                pos: copier.pos,
                label: row.label(copier.pos),
                base: row.seq,
                generation: row.generation,
                slice: copier.to,
                switch: false,
                current: false,
                epoch: Some(state.epoch),
            }),
        );
    }

    /// A move's copier receives a message
    ///
    /// It reads the group's state and begins the move, then for each stripe reads its row and
    /// lists it on enough old holders, asks the arrival whether it holds the row's label, copies it
    /// there from `k` current chunks if not, and marks it missed if it cannot. Past the last
    /// stripe it switches, if the arrival still answers, or gives the move up.
    ///
    /// # Arguments
    ///
    /// * `op` - The move
    /// * `copier` - Its state
    /// * `msg` - The message
    fn copier_receive(&mut self, op: OpId, mut copier: Copier, msg: &crate::stripe::event::Message) {
        let me = self.endpoint(op);
        let stripe = StripeIx(copier.stripe);
        match (copier.phase, &msg.body) {
            (CopyPhase::Read, Body::PgAnswer { state, .. }) => {
                if copier.move_op != op {
                    // taking up a move in flight: copy on, unless it was settled meanwhile
                    if state.moving.is_some_and(|moving| moving.op == copier.move_op) {
                        copier.state = Some(state.clone());
                        copier.phase = CopyPhase::Entry;
                        self.send(me, Endpoint::Entry, Body::EntryRead { strong: true });
                    } else {
                        self.drivers.remove(&op);
                        self.complete(op, StripeOutcome::Ok);
                        return;
                    }
                } else if state.moving.is_some() || state.positions.contains(&copier.to) {
                    // one move at a time, onto a slice not in the set
                    self.drivers.remove(&op);
                    self.complete(op, StripeOutcome::Failed);
                    return;
                } else {
                    copier.state = Some(state.clone());
                    copier.phase = CopyPhase::Begin;
                    self.send(
                        me,
                        Endpoint::Pg,
                        Body::PgPropose(PgCommand::MoveBegin {
                            op,
                            expect: state.epoch,
                            pos: copier.pos,
                            to: copier.to,
                        }),
                    );
                }
            }
            (CopyPhase::Begin, Body::Decided(decision)) if msg.from == Endpoint::Pg => {
                if !matches!(decision, Decision::Committed { .. }) {
                    self.drivers.remove(&op);
                    self.complete(op, StripeOutcome::Failed);
                    return;
                }
                // the state the begin made: in both generations, at the next epoch
                let mut state = copier.state.clone().expect("read");
                state.moving = Some(crate::stripe::group::Moving {
                    op,
                    pos: copier.pos,
                    to: copier.to,
                });
                state.epoch += 1;
                copier.state = Some(state);
                // POLICY P17: the contract copies every stripe before it switches; the unsafe
                // setting switches at once
                if self.policy.placement.switch == SwitchRule::BeforeCopier {
                    copier.phase = CopyPhase::Switch;
                    self.send(me, Endpoint::Pg, Body::PgPropose(PgCommand::MoveSwitch { op }));
                } else {
                    copier.phase = CopyPhase::Entry;
                    self.send(me, Endpoint::Entry, Body::EntryRead { strong: true });
                }
            }
            (CopyPhase::Entry, Body::EntryAnswer { state, .. }) => {
                copier.created = Some(state.created);
                copier.stripe = 0;
                self.copy_stripe(op, &mut copier);
            }
            (CopyPhase::Row, Body::RowAnswer { stripe: at, state, pg, .. }) if *at == stripe => {
                // the move it copies for is no longer in flight: someone settled it
                let ours = pg.as_ref().is_some_and(|view| {
                    view.state
                        .moving
                        .is_some_and(|moving| moving.op == copier.move_op)
                });
                if !ours {
                    self.drivers.remove(&op);
                    self.complete(op, StripeOutcome::Failed);
                    return;
                }
                if let Some(view) = pg {
                    copier.state = Some(view.state.clone());
                }
                copier.row = Some(state.clone());
                self.copy_decide(op, &mut copier);
            }
            (
                CopyPhase::Row,
                Body::Listed {
                    stripe: at,
                    has,
                    unreachable,
                    ..
                },
            ) if *at == stripe => {
                // a holder's first answer is its answer: a duplicated listing answered again
                // after its node went down says nothing about the chunk it already said it has
                if let Endpoint::Slice(slice) = msg.from {
                    if copier.listed.get(&slice) == Some(&None) {
                        copier.listed.insert(slice, Some(*has));
                        copier.reached += usize::from(!*unreachable);
                    }
                }
                self.copy_decide(op, &mut copier);
            }
            (
                CopyPhase::Ask,
                Body::ConfirmAnswer {
                    stripe: at,
                    holds,
                    unreachable,
                    ..
                },
            ) if *at == stripe => {
                if *unreachable {
                    // an arrival that cannot be reached is no place to move to
                    copier.phase = CopyPhase::Abort;
                    self.send(me, Endpoint::Pg, Body::PgPropose(PgCommand::MoveAbort { op: copier.move_op }));
                } else if *holds {
                    copier.stripe += 1;
                    self.copy_stripe(op, &mut copier);
                } else {
                    let row = copier.row.clone().expect("read");
                    let state = copier.state.clone().expect("begun");
                    copier.reads = self.start_reads(op, stripe, &row, None, Some(&state));
                    copier.phase = CopyPhase::Copy;
                    if copier.reads.candidates.len() < self.layout().k() {
                        self.copy_mark(op, &mut copier);
                    }
                }
            }
            (
                CopyPhase::Copy,
                Body::ChunkAnswer {
                    stripe: at,
                    pos,
                    label,
                    reply,
                },
            ) if *at == stripe => {
                let row = copier.row.clone().expect("read");
                let mut reads = std::mem::take(&mut copier.reads);
                let rule = self.policy.small_writes.rebuild;
                let step = self.take_chunk(op, stripe, &row, &mut reads, *pos, *label, reply, rule);
                copier.reads = reads;
                match step {
                    ReadStep::Wait => {}
                    ReadStep::Done => {
                        let data = self.decoded(&copier.reads);
                        let content = encode(self.layout(), copier.pos, &data);
                        copier.phase = CopyPhase::Stage;
                        self.send(
                            me,
                            Endpoint::Slice(copier.to),
                            Body::Stage {
                                stripe,
                                pos: copier.pos,
                                label: row.label(copier.pos),
                                base_exists: row.exists,
                                base: row.seq,
                                expects: None,
                                payload: Payload::Whole(content),
                                over: Vec::new(),
                            },
                        );
                    }
                    ReadStep::Newer(..) | ReadStep::Exhausted => self.copy_mark(op, &mut copier),
                }
            }
            (CopyPhase::Stage, Body::StageAnswer { stripe: at, refused, .. }) if *at == stripe => {
                let row = copier.row.clone().expect("read");
                match refused {
                    None => {
                        // the arrival holds a label a committed row names: it applies it
                        self.send(
                            me,
                            Endpoint::Slice(copier.to),
                            Body::Apply {
                                stripe,
                                label: row.label(copier.pos),
                            },
                        );
                        self.checker.coverage.copies += 1;
                        copier.stripe += 1;
                        self.copy_stripe(op, &mut copier);
                    }
                    Some(crate::stripe::event::StageRefusal::Failed) => {
                        copier.phase = CopyPhase::Abort;
                        self.send(me, Endpoint::Pg, Body::PgPropose(PgCommand::MoveAbort { op: copier.move_op }));
                    }
                    Some(_) => self.copy_mark(op, &mut copier),
                }
            }
            (CopyPhase::Mark, Body::Decided(decision)) if msg.from == Endpoint::Row(stripe) => {
                if matches!(decision, Decision::Committed { .. }) {
                    self.checker.coverage.marks += 1;
                    copier.stripe += 1;
                }
                // a refused mark means the row or the group moved: the stripe is read again
                self.copy_stripe(op, &mut copier);
            }
            (CopyPhase::Switch, Body::Decided(decision)) if msg.from == Endpoint::Pg => {
                self.drivers.remove(&op);
                if matches!(decision, Decision::Committed { .. }) {
                    self.checker.coverage.switches += 1;
                    self.complete(op, StripeOutcome::Ok);
                } else {
                    self.complete(op, StripeOutcome::Failed);
                }
                return;
            }
            (CopyPhase::Abort, Body::Decided(decision)) if msg.from == Endpoint::Pg => {
                self.drivers.remove(&op);
                if matches!(decision, Decision::Committed { .. }) {
                    self.checker.coverage.aborts += 1;
                }
                self.complete(op, StripeOutcome::Failed);
                return;
            }
            _ => return,
        }
        if self.drivers.contains_key(&op) {
            self.drivers.insert(op, Driver::Copy(Box::new(copier)));
        }
    }

    /// The put's positions its pieces reached under a state of the group, as the rule counts
    ///
    /// # Arguments
    ///
    /// * `putter` - Its state
    /// * `state` - The group's state
    fn put_held(&self, putter: &Putter, state: &PgState) -> Vec<Pos> {
        self.layout()
            .positions()
            .into_iter()
            .filter(|pos| judged(&putter.answers, &self.counted(state, *pos), *pos) == Some(true))
            .collect()
    }

    /// The put marks the next position it did not reach, or commits its entry once none is left
    ///
    /// # Arguments
    ///
    /// * `putter` - Its state
    fn put_next(&mut self, putter: &mut Putter) {
        let op = LATE_PUT;
        let me = self.endpoint(op);
        let last = StripeIx((self.rows.len() - 1) as u8);
        if putter.marks.is_empty() {
            let entry = putter.entry.clone().expect("read");
            putter.phase = PutPhase::Commit;
            let to = self.rows.len() as u32 * self.layout().data_units() as u32;
            self.send(
                me,
                Endpoint::Entry,
                Body::EntryPropose(EntryCommand::Put {
                    op,
                    to,
                    read_epoch: entry.epoch,
                }),
            );
            return;
        }
        // its row is read first, for the sequence and label a mark is conditional on
        putter.phase = PutPhase::Mark;
        self.send(me, Endpoint::Row(last), Body::RowRead { strong: true });
    }

    /// The put of the object's last stripe receives a message
    ///
    /// It reads the entry, then the group's state, writes a piece to every slice a position
    /// counts on, and once they answer reads the state again: a position its pieces did not reach
    /// under the state now is marked missed in the stripe's row before the entry commits, so no
    /// row calls current a chunk the group's generation does not hold.
    ///
    /// # Arguments
    ///
    /// * `putter` - Its state
    /// * `msg` - The message
    fn putter_receive(&mut self, mut putter: Putter, msg: &crate::stripe::event::Message) {
        let op = LATE_PUT;
        let me = self.endpoint(op);
        let layout = self.layout();
        let last = StripeIx((self.rows.len() - 1) as u8);
        match (putter.phase, &msg.body) {
            (PutPhase::Entry, Body::EntryAnswer { state, .. }) => {
                putter.entry = Some(state.clone());
                putter.phase = PutPhase::Pg;
                self.send(me, Endpoint::Pg, Body::PgRead);
            }
            (PutPhase::Pg, Body::PgAnswer { state, .. }) => {
                putter.placed = Some(state.clone());
                putter.phase = PutPhase::Place;
                let data = vec![Unit::Write(LATE_PUT); layout.data_units()];
                for pos in layout.positions() {
                    for slice in state.slices(pos) {
                        self.send(
                            me,
                            Endpoint::Slice(slice),
                            Body::Place {
                                stripe: last,
                                pos,
                                label: Label::PUT,
                                content: encode(layout, pos, &data),
                            },
                        );
                    }
                }
            }
            (PutPhase::Place, Body::StageAnswer { stripe, pos, refused, .. }) if *stripe == last => {
                let Endpoint::Slice(slice) = msg.from else {
                    return;
                };
                putter.answers.insert((*pos, slice), refused.is_none());
                let placed = putter.placed.clone().expect("read");
                let every: usize = layout.positions().into_iter().map(|pos| placed.slices(pos).len()).sum();
                if putter.answers.len() < every {
                    self.drivers.insert(op, Driver::Put(Box::new(putter)));
                    return;
                }
                if self.put_held(&putter, &placed).len() < layout.ack_floor() {
                    // too few pieces durable: the put is not known to have happened
                    self.drivers.remove(&op);
                    self.complete(op, StripeOutcome::Unknown);
                    return;
                }
                // POLICY P17: the contract reads the group's state again once its pieces are
                // durable; the unsafe setting commits on the state it placed under
                if self.policy.placement.put_check == PutCheck::Checked {
                    putter.phase = PutPhase::Check;
                    self.send(me, Endpoint::Pg, Body::PgRead);
                } else {
                    let held = self.put_held(&putter, &placed);
                    putter.marks = layout.positions().into_iter().filter(|pos| !held.contains(pos)).collect();
                    putter.now = Some(placed);
                    self.put_next(&mut putter);
                }
            }
            (PutPhase::Check, Body::PgAnswer { state, .. }) => {
                let held = self.put_held(&putter, state);
                if held.len() < layout.ack_floor() {
                    self.drivers.remove(&op);
                    self.complete(op, StripeOutcome::Unknown);
                    return;
                }
                putter.marks = layout.positions().into_iter().filter(|pos| !held.contains(pos)).collect();
                putter.now = Some(state.clone());
                self.put_next(&mut putter);
            }
            (PutPhase::Mark, Body::RowAnswer { stripe, state, .. }) if *stripe == last => {
                let now = putter.now.clone().expect("checked");
                if state.tombstone {
                    putter.marks.clear();
                    self.put_next(&mut putter);
                } else {
                    let pos = putter.marks[0];
                    self.send(
                        me,
                        Endpoint::Row(last),
                        Body::Propose(StripeCommand::Rebuild {
                            op,
                            pos,
                            label: state.label(pos),
                            base: state.seq,
                            generation: state.generation,
                            slice: now.slice(pos),
                            switch: false,
                            current: false,
                            epoch: Some(now.epoch),
                        }),
                    );
                }
            }
            (PutPhase::Mark, Body::Decided(decision)) if msg.from == Endpoint::Row(last) => {
                if matches!(decision, Decision::Committed { .. }) {
                    self.checker.coverage.marks += 1;
                    putter.marks.remove(0);
                    self.put_next(&mut putter);
                } else {
                    // the group moved again: what its pieces reached is judged again
                    putter.phase = PutPhase::Check;
                    self.send(me, Endpoint::Pg, Body::PgRead);
                }
            }
            (PutPhase::Commit, Body::Decided(decision)) if msg.from == Endpoint::Entry => {
                self.drivers.remove(&op);
                if matches!(decision, Decision::Committed { .. } | Decision::Repeated { .. }) {
                    self.checker.coverage.puts += 1;
                    self.complete(op, StripeOutcome::Ok);
                } else {
                    self.complete(op, StripeOutcome::Unknown);
                }
                return;
            }
            _ => return,
        }
        if self.drivers.contains_key(&op) {
            self.drivers.insert(op, Driver::Put(Box::new(putter)));
        }
    }

    /// A backfill receives a message
    ///
    /// It reads the record's drops for its position, walks every stripe's row at its leader, and
    /// if every stripe the position is stale on is in the record says so, conditional on nothing
    /// having been dropped since it began.
    ///
    /// # Arguments
    ///
    /// * `op` - The backfill
    /// * `backfiller` - Its state
    /// * `msg` - The message
    fn backfiller_receive(&mut self, op: OpId, mut backfiller: Backfiller, msg: &crate::stripe::event::Message) {
        let me = self.endpoint(op);
        let pos = backfiller.pos;
        match (backfiller.phase, &msg.body) {
            (BackfillPhase::Read, Body::PgAnswer { record, .. }) => {
                let part = &record.positions[usize::from(pos.0)];
                if !part.overflowed {
                    self.drivers.remove(&op);
                    self.complete(op, StripeOutcome::Ok);
                    return;
                }
                backfiller.drops = part.drops;
                backfiller.phase = BackfillPhase::Walk;
                backfiller.stripe = 0;
                self.send(me, Endpoint::Row(StripeIx(0)), Body::RowRead { strong: true });
            }
            (BackfillPhase::Walk, Body::RowAnswer { stripe, state, pg, .. })
                if stripe.0 == backfiller.stripe =>
            {
                // a stripe the position is stale on that the record does not hold
                let stale = !state.tombstone && !state.current(pos);
                let held = pg
                    .as_ref()
                    .is_some_and(|view| view.missed[usize::from(pos.0)].is_some());
                if stale && !held {
                    backfiller.whole = false;
                }
                backfiller.stripe += 1;
                if usize::from(backfiller.stripe) < self.rows.len() {
                    let next = StripeIx(backfiller.stripe);
                    self.send(me, Endpoint::Row(next), Body::RowRead { strong: true });
                } else if backfiller.whole {
                    backfiller.phase = BackfillPhase::Done;
                    self.send(
                        me,
                        Endpoint::Pg,
                        Body::PgPropose(PgCommand::BackfillDone {
                            op,
                            pos,
                            drops: backfiller.drops,
                        }),
                    );
                } else {
                    // a stale stripe the record lost is found by a rebuild's walk first
                    self.drivers.remove(&op);
                    self.complete(op, StripeOutcome::Failed);
                    return;
                }
            }
            (BackfillPhase::Done, Body::Decided(decision)) if msg.from == Endpoint::Pg => {
                self.drivers.remove(&op);
                if matches!(decision, Decision::Committed { .. }) {
                    self.checker.coverage.backfills += 1;
                    self.complete(op, StripeOutcome::Ok);
                } else {
                    self.complete(op, StripeOutcome::Failed);
                }
                return;
            }
            _ => return,
        }
        if self.drivers.contains_key(&op) {
            self.drivers.insert(op, Driver::Backfill(Box::new(backfiller)));
        }
    }
}

/// Whether a stripe is a hole: no row and past what the put wrote, or reclaimed
///
/// # Arguments
///
/// * `row` - Its row
/// * `entry` - The object's entry
/// * `start` - The stripe's first data unit's offset in the object
pub fn hole(row: &RowState, entry: &EntryState, start: u32) -> bool {
    row.tombstone || (!row.exists && start >= entry.created)
}

/// How many units a data chunk holds: a replicated stripe's one chunk holds them all
///
/// # Arguments
///
/// * `layout` - The pool's layout
fn chunk_units(layout: crate::stripe::layout::Layout) -> usize {
    if layout.is_replicated() {
        layout.data_units()
    } else {
        UNITS
    }
}
