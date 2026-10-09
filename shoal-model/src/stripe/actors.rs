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
use crate::stripe::event::{Body, ChunkReply, Endpoint, Payload};
use crate::stripe::group::{
    Decision, EntryCommand, EntryState, Evidence, Floor, PendingBytes, RowState, StripeCommand,
};
use crate::stripe::ids::{Epoch, Label, Pos, SliceId, StripeIx, Tag};
use crate::stripe::layout::UNITS;
use crate::stripe::oracle::{OpKind, ReadResult, StripeOutcome};
use crate::stripe::policy::{
    AckRule, ClearRule, EntryRule, EpochRule, HiddenRule, PendingRead, ReaderRule, RebuildRule,
    Reservation, RowLevel, SmallWritePath, StagerWord, TagRule, TruncateRule, UntouchedRule,
    PENDING_BOUND,
};
use crate::stripe::world::StripeWorld;

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
    /// Committing the floor's removal
    Drop,
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
                    self.send(
                        me,
                        Endpoint::Slice(row.slice(*pos)),
                        Body::Drop {
                            stripe: stager.stripe,
                            label,
                        },
                    );
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
                    self.stager_receive(op, *stager, &msg.body);
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
                    self.clearer_receive(op, *clearer, &msg.body);
                }
            }
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
    fn start_reads(
        &mut self,
        op: OpId,
        stripe: StripeIx,
        row: &RowState,
        exclude: Option<Pos>,
    ) -> ChunkReads {
        let layout = self.layout();
        // the positions the row calls current, data first
        let mut candidates: Vec<Pos> = layout
            .positions()
            .into_iter()
            .filter(|pos| row.current(*pos) && Some(*pos) != exclude)
            .collect();
        candidates.sort_by_key(|pos| !layout.is_data(*pos));
        let mut reads = ChunkReads {
            candidates,
            ..ChunkReads::default()
        };
        let me = self.endpoint(op);
        while reads.pending.len() < layout.k() && reads.next < reads.candidates.len() {
            let pos = reads.candidates[reads.next];
            reads.next += 1;
            reads.pending.insert(pos);
            self.send(
                me,
                Endpoint::Slice(row.slice(pos)),
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
                    self.send(
                        me,
                        Endpoint::Slice(row.slice(next)),
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
    /// * `body` - The message
    fn stager_receive(&mut self, op: OpId, mut stager: Stager, body: &Body) {
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
                    ..
                },
            ) => {
                stager.row = Some((*index, state.clone()));
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
                    stager.reads = self.start_reads(op, stripe, state, None);
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
                match refused {
                    None => {
                        stager.staged.insert(*pos);
                    }
                    Some(_) => {
                        stager.refused.insert(*pos);
                    }
                }
                if self.maybe_propose(op, &mut stager) {
                    return;
                }
            }
            (StagerPhase::Stage, Body::ConfirmAnswer { pos, holds, .. }) => {
                stager.confirms.insert(*pos, *holds);
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
                            self.send(
                                me,
                                Endpoint::Slice(row.slice(pos)),
                                Body::Fold {
                                    stripe,
                                    pos,
                                    pending: pending.clone(),
                                },
                            );
                        }
                    }
                    Decision::Committed { .. } => {
                        // the holders are told; one that never hears asks the row
                        for pos in &stager.touched {
                            self.send(
                                me,
                                Endpoint::Slice(row.slice(*pos)),
                                Body::Apply { stripe, label },
                            );
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
        stager.in_commit = self.policy.small_writes.path == SmallWritePath::InCommit
            && layout.is_replicated()
            && !hole
            && !row.tombstone
            && pending.units.len() <= PENDING_BOUND;
        if stager.in_commit {
            stager.touched = layout.positions();
            stager.phase = StagerPhase::Stage;
            return;
        }
        // a hole, or a write of every unit, needs no old bytes and touches every chunk whole
        let whole = hole || stager.written.len() == data_units;
        stager.touched = if whole || layout.is_replicated() {
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
            let payload = if whole || carried {
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
            self.send(
                me,
                Endpoint::Slice(row.slice(pos)),
                Body::Stage {
                    stripe,
                    pos,
                    label,
                    base_exists: row.exists,
                    base: row.seq,
                    expects,
                    payload,
                },
            );
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
                self.send(
                    me,
                    Endpoint::Slice(row.slice(pos)),
                    Body::Confirm {
                        stripe,
                        pos,
                        label: row.label(pos),
                        also,
                    },
                );
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
                    if self.believed_up(row.slice(pos)) {
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
            (ReaderPhase::Row, Body::RowAnswer { index, state, .. }) => {
                reader.row = Some((*index, state.clone()));
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
                    reader.reads = self.start_reads(op, stripe, state, None);
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
            (MoverPhase::Row, Body::RowAnswer { index, state, .. }) => {
                mover.row = Some((*index, state.clone()));
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
                let target = mover.to.unwrap_or(state.slice(pos));
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
                // POLICY progress: the contract asks the holder what it holds before writing
                if mover.to.is_none() && self.policy.rebuild == RebuildRule::AsksHolder {
                    mover.phase = MoverPhase::Ask;
                    self.send(
                        me,
                        Endpoint::Slice(target),
                        Body::Confirm {
                            stripe,
                            pos,
                            label: state.label(pos),
                            // a rebuild asks for the label exactly: a holder at a base the pending
                            // bytes fold from is rewritten whole
                            also: Vec::new(),
                        },
                    );
                } else {
                    mover.reads = self.start_reads(op, stripe, state, Some(pos));
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
                    mover.reads = self.start_reads(op, stripe, &row, Some(pos));
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
                                expects: None,
                                payload: Payload::Whole(content),
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
                        self.complete(op, StripeOutcome::Ok);
                    }
                    _ => self.complete(op, StripeOutcome::Failed),
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
            if reclaimer.stripes.get(reclaimer.at) != Some(&stripe) {
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
                // a stripe the cut falls inside keeps units under the floor, so the floor stays
                reclaimer.all = floor.len % data_units == 0;
                if !self.next_reclaim(op, &mut reclaimer) {
                    return;
                }
            }
            (ReclaimPhase::Row, Body::RowAnswer { state, .. }) => {
                let floor = reclaimer.floor.expect("chosen");
                let stripe = reclaimer.stripes[reclaimer.at];
                reclaimer.row = Some(state.clone());
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
                        for slice in row.positions {
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
            (ReclaimPhase::Drop, Body::Decided(_)) => {
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
    /// * `body` - The message
    fn clearer_receive(&mut self, op: OpId, mut clearer: Clearer, body: &Body) {
        let me = self.endpoint(op);
        let stripe = clearer.stripe;
        match (clearer.phase, body) {
            (ClearPhase::Row, Body::RowAnswer { state, .. }) => {
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
                    self.send(
                        me,
                        Endpoint::Slice(state.slice(pos)),
                        Body::Fold {
                            stripe,
                            pos,
                            pending: pending.clone(),
                        },
                    );
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
                clearer.answered.insert(*pos);
                if refused.is_none() {
                    clearer.holding.insert(*pos);
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
