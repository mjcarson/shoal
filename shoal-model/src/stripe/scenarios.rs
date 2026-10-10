//! The schedules of S7, built by hand, and the ones X1's findings are recorded by
//!
//! Each of S7's sixteen schedules ([S7](../../../docs/src/object-storage/write-path.md#the-schedules-that-shaped-it))
//! is a function of the policy, as the tablet model's stale report schedule is: built under its
//! unsafe setting it is saved and replays to the clause S16 names for it, and built under the safe
//! policy it has to find nothing. A schedule is a story told with the builder: which write reads
//! what, which message arrives when, which node crashes where. The layout is the schedule's own,
//! the smallest that tells its story.
//!
//! Beside them are the schedules for Q16's two answers that count a chunk on the row's word, and
//! for the one rule as S3 wrote it that a search of the generated runs did not reach by itself: a
//! stamp that moves backwards. The other rules the model found unsafe are recorded by generated
//! schedules, minimized ([`crate::stripe::minimize`]).

use crate::ids::{NodeId, OpId};
use crate::stripe::event::{Body, Endpoint, Message, StripeEvent};
use crate::stripe::ids::{Pos, SliceId, StripeIx};
use crate::stripe::layout::Layout;
use crate::stripe::policy::StripePolicy;
use crate::stripe::schedule::{StripeBuilder, StripeParams, StripeSchedule};

/// A schedule built by hand: its S7 number, if one, the setting it is saved under, and its builder
pub type Scenario = (Option<u8>, &'static str, fn(StripePolicy) -> StripeSchedule);

/// What kind of message a body is, for the builder to hold or pass
///
/// # Arguments
///
/// * `body` - The body
pub fn kind(body: &Body) -> &'static str {
    match body {
        Body::EntryRead { .. } => "entry_read",
        Body::EntryAnswer { .. } => "entry_answer",
        Body::RowRead { .. } => "row_read",
        Body::RowAnswer { .. } => "row_answer",
        Body::Stage { .. } => "stage",
        Body::StageAnswer { .. } => "stage_answer",
        Body::Confirm { .. } => "confirm",
        Body::ConfirmAnswer { .. } => "confirm_answer",
        Body::ChunkRead { .. } => "chunk_read",
        Body::ChunkAnswer { .. } => "chunk_answer",
        Body::Propose(_) => "propose",
        Body::EntryPropose(_) => "entry_propose",
        Body::Decided(_) => "decided",
        Body::Fold { .. } => "fold",
        Body::Apply { .. } => "apply",
        Body::Drop { .. } => "drop",
        Body::Discard { .. } => "discard",
        Body::Reserve { .. } => "reserve",
        Body::Granted { .. } => "granted",
    }
}

/// Whether a message is to or from an operation's driver
///
/// # Arguments
///
/// * `msg` - The message
/// * `op` - The operation
fn involves(msg: &Message, op: OpId) -> bool {
    matches!(msg.from, Endpoint::Op { op: o, .. } if o == op)
        || matches!(msg.to, Endpoint::Op { op: o, .. } if o == op)
}

/// Whether a message is between an operation and a slice
///
/// # Arguments
///
/// * `msg` - The message
/// * `slice` - The slice
fn at_slice(msg: &Message, slice: u32) -> bool {
    msg.from == Endpoint::Slice(SliceId(slice)) || msg.to == Endpoint::Slice(SliceId(slice))
}

impl StripeBuilder {
    /// Drive an operation: deliver its messages, but not the kinds held, syncing as it needs
    ///
    /// # Arguments
    ///
    /// * `op` - The operation
    /// * `hold` - The kinds of message not to deliver
    pub fn drive(&mut self, op: OpId, hold: &[&str]) -> &mut Self {
        for _ in 0..10_000 {
            if self.world.violation.is_some() {
                return self;
            }
            let next = self
                .world
                .net
                .in_flight
                .iter()
                .find(|msg| involves(msg, op) && !hold.contains(&kind(&msg.body)))
                .cloned();
            if let Some(msg) = next {
                self.event(StripeEvent::Deliver { msg });
                continue;
            }
            // a slice with a stage of this operation to sync syncs
            let unsynced = self.world.slices.iter().find_map(|(id, slice)| {
                slice
                    .unsynced
                    .iter()
                    .any(
                        |pending| matches!(pending.reply_to, Endpoint::Op { op: o, .. } if o == op),
                    )
                    .then_some(*id)
            });
            match unsynced {
                Some(slice) if self.world.answers(slice) => {
                    self.event(StripeEvent::Sync { slice });
                }
                _ => return self,
            }
        }
        panic!("driving never settled");
    }

    /// Deliver the first message in flight that is an operation's, of a kind, at a slice
    ///
    /// # Arguments
    ///
    /// * `op` - The operation
    /// * `kind_of` - The kind
    /// * `slice` - The slice it is to or from, or none for any
    pub fn pass(&mut self, op: OpId, kind_of: &str, slice: Option<u32>) -> &mut Self {
        let frozen = self.world.violation.is_some();
        let delivered = self.deliver_where(|msg| {
            involves(msg, op)
                && kind(&msg.body) == kind_of
                && slice.is_none_or(|slice| at_slice(msg, slice))
        });
        assert!(delivered || frozen, "no {kind_of} of {op:?} in flight");
        self
    }

    /// Lose the first message in flight that is an operation's, of a kind, at a slice
    ///
    /// # Arguments
    ///
    /// * `op` - The operation
    /// * `kind_of` - The kind
    /// * `slice` - The slice it is to or from
    pub fn lose(&mut self, op: OpId, kind_of: &str, slice: u32) -> &mut Self {
        let dropped = self.drop_where(|msg| {
            involves(msg, op) && kind(&msg.body) == kind_of && at_slice(msg, slice)
        });
        assert!(dropped, "no {kind_of} of {op:?} at slice {slice} in flight");
        self
    }

    /// Run every apply a slice can take for a stripe to its end
    ///
    /// # Arguments
    ///
    /// * `slice` - The slice
    /// * `stripe` - The stripe
    pub fn apply_all(&mut self, slice: u32, stripe: u8) -> &mut Self {
        for _ in 0..100 {
            if self.world.violation.is_some() {
                return self;
            }
            let skipped = self.world.skipped;
            self.event(StripeEvent::ApplyStep {
                slice: SliceId(slice),
                stripe: StripeIx(stripe),
            });
            if self.world.skipped > skipped {
                self.events.pop();
                return self;
            }
        }
        self
    }
}

/// A builder over a layout and a number of stripes
fn builder(layout: Layout, stripes: u8, policy: StripePolicy) -> StripeBuilder {
    StripeBuilder::new(StripeParams::by_hand(layout, stripes), policy)
}

/// A write of some units of a stripe
fn write(op: u32, stripe: u8, units: &[u8]) -> StripeEvent {
    StripeEvent::Write {
        op: OpId(op),
        stripe: StripeIx(stripe),
        units: units.to_vec(),
    }
}

/// The kinds a drive up to a proposal holds
const UP_TO_PROPOSE: &[&str] = &["propose"];

/// S7 1: two stagers on one base; with sequence-only labels a holder applies the loser's bytes
///
/// # Arguments
///
/// * `policy` - The policy to build it under
pub fn s01_two_stagers_on_one_base(policy: StripePolicy) -> StripeSchedule {
    let mut b = builder(Layout::Replicated3, 1, policy);
    // B is op 1 and A op 2, so B's tag sorts first wherever both are staged
    b.event(write(1, 0, &[1])).event(write(2, 0, &[0]));
    b.drive(OpId(1), UP_TO_PROPOSE)
        .drive(OpId(2), UP_TO_PROPOSE);
    // A commits; B's proposal on the same base is refused
    b.pass(OpId(2), "propose", None).drive(OpId(2), &[]);
    b.pass(OpId(1), "propose", None).drive(OpId(1), &[]);
    for slice in 0..3 {
        b.apply_all(slice, 0);
    }
    b.finish("s01_two_stagers_on_one_base", Some(1))
}

/// S7 2: a parity change built from a row another write has moved, committed unconditionally
///
/// # Arguments
///
/// * `policy` - The policy to build it under
pub fn s02_parity_from_a_stale_row(policy: StripePolicy) -> StripeSchedule {
    let mut b = builder(Layout::TwoPlusOne, 1, policy);
    // W1 changes data chunk 0, W2 data chunk 1, both against the stripe as created
    b.event(write(1, 0, &[0])).event(write(2, 0, &[2]));
    b.drive(OpId(1), UP_TO_PROPOSE)
        .drive(OpId(2), UP_TO_PROPOSE);
    b.pass(OpId(1), "propose", None).drive(OpId(1), &[]);
    b.apply_all(0, 0).apply_all(2, 0);
    // W2's parity describes a stripe in which W1 never happened
    b.pass(OpId(2), "propose", None).drive(OpId(2), &[]);
    b.finish("s02_parity_from_a_stale_row", Some(2))
}

/// S7 3: a slice that missed a write while down returns, and nothing recorded what it missed
///
/// # Arguments
///
/// * `policy` - The policy to build it under
pub fn s03_returning_slice(policy: StripePolicy) -> StripeSchedule {
    let mut b = builder(Layout::Replicated3, 1, policy);
    b.event(StripeEvent::Crash { node: NodeId(2) });
    // the write stages on the two copies that answer, which is k + f
    b.event(write(1, 0, &[0])).drive(OpId(1), &[]);
    b.apply_all(0, 0).apply_all(1, 0);
    // an hour later the slice returns
    b.event(StripeEvent::Restart { node: NodeId(2) });
    b.event(StripeEvent::Rebuild {
        op: OpId(2),
        stripe: StripeIx(0),
        pos: Pos(2),
    });
    b.drive(OpId(2), &[]).apply_all(2, 0);
    b.finish("s03_returning_slice", Some(3))
}

/// S7 4: a writer reads the size, a truncate cuts its stripe, it commits, the object grows back
///
/// # Arguments
///
/// * `policy` - The policy to build it under
pub fn s04_truncate_under_a_writer(policy: StripePolicy) -> StripeSchedule {
    let mut b = builder(Layout::Replicated3, 2, policy);
    // the writer reads the entry before the truncate
    b.event(write(1, 1, &[0])).drive(OpId(1), &["row_read"]);
    // a truncate cuts the object to the first stripe, at the stripe's edge
    b.event(StripeEvent::Truncate {
        op: OpId(2),
        len: 2,
    })
    .drive(OpId(2), &[]);
    // the writer commits the stripe past the cut
    b.drive(OpId(1), &[]);
    b.apply_all(0, 1).apply_all(1, 1).apply_all(2, 1);
    // the object grows back over it, and a strong read looks
    b.event(StripeEvent::Truncate {
        op: OpId(3),
        len: 4,
    })
    .drive(OpId(3), &[]);
    b.event(StripeEvent::Read {
        op: OpId(4),
        stripe: StripeIx(1),
        strong: true,
    })
    .drive(OpId(4), &[]);
    b.finish("s04_truncate_under_a_writer", Some(4))
}

/// S7 5: the pool map moves a position while a write is staged under the old one
///
/// # Arguments
///
/// * `policy` - The policy to build it under
pub fn s05_map_changes_during_a_write(policy: StripePolicy) -> StripeSchedule {
    let mut b = builder(Layout::Replicated3, 1, policy);
    b.event(write(1, 0, &[0])).drive(OpId(1), UP_TO_PROPOSE);
    // position 2 moves to the spare slice 3, and the group switches generation
    b.event(StripeEvent::Move {
        op: OpId(2),
        stripe: StripeIx(0),
        pos: Pos(2),
        to: SliceId(3),
    });
    b.drive(OpId(2), &[]).apply_all(3, 0);
    // the write staged under the old generation proposes
    b.drive(OpId(1), &[]);
    b.finish("s05_map_changes_during_a_write", Some(5))
}

/// S7 6: a stager times out and tells its holders to drop while its commit is in flight
///
/// # Arguments
///
/// * `policy` - The policy to build it under
pub fn s06_stager_timeout(policy: StripePolicy) -> StripeSchedule {
    let mut b = builder(Layout::Replicated3, 1, policy);
    b.event(write(1, 0, &[0])).drive(OpId(1), UP_TO_PROPOSE);
    b.event(StripeEvent::StagerTimeout { op: OpId(1) });
    // whatever the stager said reaches the holders before its commit reaches the group
    for slice in 0..3 {
        while b.deliver_where(|msg| kind(&msg.body) == "drop" && at_slice(msg, slice)) {}
    }
    b.deliver_where(|msg| kind(&msg.body) == "propose");
    b.finish("s06_stager_timeout", Some(6))
}

/// S7 7: a 4+2 write to one data chunk is acknowledged after one stage
///
/// # Arguments
///
/// * `policy` - The policy to build it under
pub fn s07_ack_after_one_stage(policy: StripePolicy) -> StripeSchedule {
    let mut b = builder(Layout::FourPlusTwo, 1, policy);
    b.event(write(1, 0, &[0])).drive(OpId(1), &["stage_answer"]);
    // the data chunk's holder answers first
    b.pass(OpId(1), "stage_answer", Some(0));
    b.drive(OpId(1), &[]);
    b.finish("s07_ack_after_one_stage", Some(7))
}

/// S7 8: parity staged as a patch, applied, and replayed after a crash that lost the fact
///
/// # Arguments
///
/// * `policy` - The policy to build it under
pub fn s08_patch_replayed(policy: StripePolicy) -> StripeSchedule {
    let mut b = builder(Layout::TwoPlusOne, 1, policy);
    b.event(write(1, 0, &[0])).drive(OpId(1), &[]);
    // the parity holder writes in place and syncs, and crashes before dropping the record
    for _ in 0..2 {
        b.event(StripeEvent::ApplyStep {
            slice: SliceId(2),
            stripe: StripeIx(0),
        });
    }
    b.event(StripeEvent::Crash { node: NodeId(2) })
        .event(StripeEvent::Restart { node: NodeId(2) });
    b.finish("s08_patch_replayed", Some(8))
}

/// S7 9: a crash in the middle of an apply in place
///
/// # Arguments
///
/// * `policy` - The policy to build it under
pub fn s09_crash_during_an_apply(policy: StripePolicy) -> StripeSchedule {
    let mut b = builder(Layout::Replicated3, 1, policy);
    b.event(write(1, 0, &[0])).drive(OpId(1), &[]);
    // the units are being written in place when the node goes
    b.event(StripeEvent::ApplyStep {
        slice: SliceId(0),
        stripe: StripeIx(0),
    });
    b.event(StripeEvent::Crash { node: NodeId(0) })
        .event(StripeEvent::Restart { node: NodeId(0) });
    b.apply_all(0, 0);
    b.finish("s09_crash_during_an_apply", Some(9))
}

/// S7 10: a stager paused for minutes resumes and proposes
///
/// # Arguments
///
/// * `policy` - The policy to build it under
pub fn s10_paused_stager(policy: StripePolicy) -> StripeSchedule {
    let mut b = builder(Layout::Replicated3, 1, policy);
    b.event(write(1, 0, &[0])).drive(OpId(1), UP_TO_PROPOSE);
    // while it is paused another write commits and is applied
    b.event(write(2, 0, &[1])).drive(OpId(2), &[]);
    for slice in 0..3 {
        b.apply_all(slice, 0);
    }
    // it resumes
    b.drive(OpId(1), &[]);
    b.finish("s10_paused_stager", Some(10))
}

/// S7 11: a stage's acknowledgement is lost, the commit proceeds without that holder, a rebuild follows
///
/// # Arguments
///
/// * `policy` - The policy to build it under
pub fn s11_lost_stage_acknowledgement(policy: StripePolicy) -> StripeSchedule {
    let mut b = builder(Layout::Replicated3, 1, policy);
    b.event(write(1, 0, &[0])).drive(OpId(1), &["stage_answer"]);
    b.lose(OpId(1), "stage_answer", 2);
    b.drive(OpId(1), &[]);
    // the holder whose answer was lost hears the commit and applies
    for slice in 0..3 {
        b.apply_all(slice, 0);
    }
    b.event(StripeEvent::Rebuild {
        op: OpId(2),
        stripe: StripeIx(0),
        pos: Pos(2),
    });
    b.drive(OpId(2), &[]);
    b.finish("s11_lost_stage_acknowledgement", Some(11))
}

/// S7 12: a disk swapped for an empty one at the same path
///
/// # Arguments
///
/// * `policy` - The policy to build it under
pub fn s12_empty_disk_at_the_same_path(policy: StripePolicy) -> StripeSchedule {
    let mut b = builder(Layout::Replicated3, 1, policy);
    b.event(write(1, 0, &[0])).drive(OpId(1), &[]);
    for slice in 0..3 {
        b.apply_all(slice, 0);
    }
    b.event(StripeEvent::Crash { node: NodeId(1) })
        .event(StripeEvent::DiskReplace { node: NodeId(1) })
        .event(StripeEvent::Restart { node: NodeId(1) });
    b.finish("s12_empty_disk_at_the_same_path", Some(12))
}

/// S7 13: the disk fills between the stage and the apply
///
/// # Arguments
///
/// * `policy` - The policy to build it under
pub fn s13_disk_fills_before_the_apply(policy: StripePolicy) -> StripeSchedule {
    let mut b = builder(Layout::Replicated3, 1, policy);
    b.event(write(1, 0, &[0])).drive(OpId(1), UP_TO_PROPOSE);
    b.event(StripeEvent::Fill { node: NodeId(0) });
    b.drive(OpId(1), &[]).apply_all(0, 0);
    b.finish("s13_disk_fills_before_the_apply", Some(13))
}

/// S7 14: a holder asks about its staged write and a replica that lags shows no row
///
/// # Arguments
///
/// * `policy` - The policy to build it under
pub fn s14_lagging_replica_shows_no_row(policy: StripePolicy) -> StripeSchedule {
    let mut b = builder(Layout::Replicated3, 1, policy);
    // the stripe's first write in place commits, and slice 2 never hears so
    b.event(write(1, 0, &[0])).drive(OpId(1), &["apply"]);
    b.event(StripeEvent::Ask {
        slice: SliceId(2),
        stripe: StripeIx(0),
    });
    let ask = b
        .world
        .net
        .in_flight
        .iter()
        .find(|msg| msg.from == Endpoint::Slice(SliceId(2)) && kind(&msg.body) == "row_read")
        .cloned()
        .expect("the ask is in flight");
    b.event(StripeEvent::DeliverLagging { msg: ask, lag: 1 });
    b.deliver_where(|msg| msg.to == Endpoint::Slice(SliceId(2)) && kind(&msg.body) == "row_answer");
    b.finish("s14_lagging_replica_shows_no_row", Some(14))
}

/// S7 15: a holder applies a stage before its commit, and the commit is refused
///
/// # Arguments
///
/// * `policy` - The policy to build it under
pub fn s15_apply_before_the_commit(policy: StripePolicy) -> StripeSchedule {
    let mut b = builder(Layout::Replicated3, 1, policy);
    b.event(write(1, 0, &[0])).event(write(2, 0, &[1]));
    // both read the stripe as created; W2 commits first, on the copies at slices 1 and 2
    b.drive(OpId(1), &["stage"]);
    b.drive(OpId(2), &["stage"]);
    b.lose(OpId(2), "stage", 0);
    b.drive(OpId(2), &["apply"]);
    // W1's stage reaches slice 0, is synced, and the holder applies it
    b.pass(OpId(1), "stage", Some(0));
    b.event(StripeEvent::Sync { slice: SliceId(0) });
    b.apply_all(0, 0);
    b.drive(OpId(1), &[]);
    b.finish("s15_apply_before_the_commit", Some(15))
}

/// S7 16: a reader consults the row; a later write commits and is applied on one holder first
///
/// # Arguments
///
/// * `policy` - The policy to build it under
pub fn s16_reader_meets_a_newer_chunk(policy: StripePolicy) -> StripeSchedule {
    let mut b = builder(Layout::FourPlusTwo, 1, policy);
    b.event(StripeEvent::Read {
        op: OpId(1),
        stripe: StripeIx(0),
        strong: false,
    });
    b.drive(OpId(1), &["chunk_read"]);
    // data chunk 0's node goes, so the reader will need a parity chunk
    b.event(StripeEvent::Crash { node: NodeId(0) });
    // two writes to data chunk 1 commit, and are applied on the first parity holder only, so even
    // a previous state it keeps is newer than the reader's row
    b.event(write(2, 0, &[2])).drive(OpId(2), &["apply"]);
    b.pass(OpId(2), "apply", Some(4)).apply_all(4, 0);
    b.pass(OpId(2), "apply", Some(5)).apply_all(5, 0);
    b.event(write(3, 0, &[3])).drive(OpId(3), &["apply"]);
    b.pass(OpId(3), "apply", Some(4)).apply_all(4, 0);
    b.drive(OpId(1), &[]);
    b.finish("s16_reader_meets_a_newer_chunk", Some(16))
}

/// Q16: an untouched chunk counted while its holder is believed up, on a disk that died unseen
///
/// # Arguments
///
/// * `policy` - The policy to build it under
pub fn q16_counted_while_believed_up(policy: StripePolicy) -> StripeSchedule {
    let mut b = builder(Layout::FourPlusTwo, 1, policy);
    // data chunk 1's disk fails and nobody has noticed
    b.event(StripeEvent::DiskFail { node: NodeId(1) });
    // a write to data chunk 0 stages on it and the first parity; the second never hears
    b.event(write(1, 0, &[0])).drive(OpId(1), &["stage"]);
    b.drop_where(|msg| kind(&msg.body) == "stage" && at_slice(msg, 5));
    b.drive(OpId(1), &[]);
    b.finish("q16_counted_while_believed_up", None)
}

/// Q16: an untouched chunk counted on a node that is down, whose disk was swapped meanwhile
///
/// # Arguments
///
/// * `policy` - The policy to build it under
pub fn q16_counted_when_down(policy: StripePolicy) -> StripeSchedule {
    let mut b = builder(Layout::FourPlusTwo, 1, policy);
    b.event(StripeEvent::Crash { node: NodeId(1) })
        .event(StripeEvent::DiskReplace { node: NodeId(1) });
    b.event(write(1, 0, &[0])).drive(OpId(1), &["stage"]);
    b.drop_where(|msg| kind(&msg.body) == "stage" && at_slice(msg, 5));
    b.drive(OpId(1), &[]);
    b.finish("q16_counted_when_down", None)
}

/// Q18: a writer that read the epoch before a truncate commits after a fresh write and moves the
/// stripe's stamp back below the floor, hiding the fresh write
///
/// # Arguments
///
/// * `policy` - The policy to build it under
pub fn q18_stamp_moves_backwards(policy: StripePolicy) -> StripeSchedule {
    let mut b = builder(Layout::Replicated3, 2, policy);
    // the stale writer reads the entry before the truncate
    b.event(write(1, 1, &[0])).drive(OpId(1), &["row_read"]);
    b.event(StripeEvent::Truncate {
        op: OpId(2),
        len: 2,
    })
    .drive(OpId(2), &[]);
    // a fresh write after the truncate grows the object back and is acknowledged
    b.event(write(3, 1, &[1])).drive(OpId(3), &[]);
    for slice in 0..3 {
        b.apply_all(slice, 1);
    }
    // the stale writer reads the row the fresh write left, and commits under the epoch it read
    b.drive(OpId(1), &[]);
    for slice in 0..3 {
        b.apply_all(slice, 1);
    }
    b.event(StripeEvent::Read {
        op: OpId(4),
        stripe: StripeIx(1),
        strong: true,
    })
    .drive(OpId(4), &[]);
    b.finish("q18_stamp_moves_backwards", None)
}

/// Q27: a small write rides in its commit, a reader is served it before any holder has folded it,
/// every holder folds it, and the leader's clear takes it out of the row
///
/// Not a saved schedule: it breaks nothing under the safe policy, and a saved file records what
/// it breaks. A test builds it and holds the world it leaves to what the path promises.
///
/// # Arguments
///
/// * `policy` - The policy to build it under
pub fn small_write_folded_and_cleared(policy: StripePolicy) -> StripeSchedule {
    let mut b = builder(Layout::Replicated3, 1, policy);
    // a write of one unit rides in its commit; the holders' folds are held back for now
    b.event(write(1, 0, &[1])).drive(OpId(1), &["fold"]);
    // a strong read is served every holder's base with the pending bytes laid over
    b.event(StripeEvent::Read {
        op: OpId(2),
        stripe: StripeIx(0),
        strong: true,
    })
    .drive(OpId(2), &[]);
    // the holders fold them, journalled and then applied in place
    b.drive(OpId(1), &[]);
    for slice in 0..3 {
        b.apply_all(slice, 0);
    }
    // the leader's clear finds every holder holding the write's label, and takes the bytes out
    b.event(StripeEvent::ClearPendingBytes {
        op: OpId(3),
        stripe: StripeIx(0),
    })
    .drive(OpId(3), &[]);
    // and a read after it is served the chunks as they are
    b.event(StripeEvent::Read {
        op: OpId(4),
        stripe: StripeIx(0),
        strong: true,
    })
    .drive(OpId(4), &[]);
    b.finish("small_write_folded_and_cleared", None)
}

/// Every schedule of S7, with the unsafe setting it is saved under
pub fn s7() -> Vec<Scenario> {
    vec![
        (Some(1), "sequence_as_label", s01_two_stagers_on_one_base),
        (
            Some(2),
            "commit_with_no_condition",
            s02_parity_from_a_stale_row,
        ),
        (
            Some(3),
            "returning_slice_taken_as_current",
            s03_returning_slice,
        ),
        (
            Some(4),
            "commit_ignores_the_truncate_epoch",
            s04_truncate_under_a_writer,
        ),
        (
            Some(5),
            "commit_ignores_the_generation",
            s05_map_changes_during_a_write,
        ),
        (
            Some(6),
            "holder_discards_on_a_stagers_word",
            s06_stager_timeout,
        ),
        (Some(7), "ack_after_one_stage", s07_ack_after_one_stage),
        (Some(8), "parity_staged_as_a_patch", s08_patch_replayed),
        (
            Some(9),
            "staged_copy_dropped_when_its_apply_starts",
            s09_crash_during_an_apply,
        ),
        (Some(10), "commit_with_no_condition", s10_paused_stager),
        (
            Some(11),
            "rebuild_without_asking_the_holder",
            s11_lost_stage_acknowledgement,
        ),
        (
            Some(12),
            "device_known_by_its_path",
            s12_empty_disk_at_the_same_path,
        ),
        (
            Some(13),
            "space_taken_at_the_apply",
            s13_disk_fills_before_the_apply,
        ),
        (
            Some(14),
            "discard_on_a_lagging_replicas_view",
            s14_lagging_replica_shows_no_row,
        ),
        (
            Some(15),
            "apply_before_the_commit",
            s15_apply_before_the_commit,
        ),
        (
            Some(16),
            "reader_accepts_a_newer_chunk",
            s16_reader_meets_a_newer_chunk,
        ),
    ]
}

/// The schedules built by hand for X1's findings, with the rule as written they are saved under
pub fn findings() -> Vec<Scenario> {
    vec![
        (
            None,
            "untouched_chunk_counted_while_believed_up",
            q16_counted_while_believed_up,
        ),
        (
            None,
            "untouched_chunk_counted_when_down",
            q16_counted_when_down,
        ),
        (None, "stamp_moves_backwards", q18_stamp_moves_backwards),
    ]
}

/// The policy a setting names: one of S16's, the progress one, a rule the small write in its
/// commit depends on, or a rule as the pages wrote it
///
/// # Arguments
///
/// * `name` - The setting
pub fn policy_named(name: &str) -> StripePolicy {
    if let Some((_, policy, _, _)) = StripePolicy::unsafe_settings()
        .into_iter()
        .find(|(setting, _, _, _)| *setting == name)
    {
        return policy;
    }
    let (progress, policy, _) = StripePolicy::progress_setting();
    if progress == name {
        return policy;
    }
    if let Some((_, policy, _)) = StripePolicy::small_write_settings()
        .into_iter()
        .find(|(setting, _, _)| *setting == name)
    {
        return policy;
    }
    StripePolicy::documented_rules()
        .into_iter()
        .find(|(setting, _)| *setting == name)
        .map(|(_, policy)| policy)
        .unwrap_or_else(|| panic!("no setting called {name}"))
}
