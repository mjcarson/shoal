//! What can happen to the stripe model, and the messages it sends
//!
//! An event is everything a schedule is made of
//! ([S16](../../../docs/src/object-storage/testing.md#the-model)): a client's operation, a message
//! delivered, lost or delivered twice, a holder's sync and the steps of an apply, a crash and a
//! restart, a disk that fails, fills, is reported or is swapped for an empty one, the leader's
//! timer, and the drivers that rebuild, move and reclaim. Messages travel whole inside
//! [`StripeEvent::Deliver`], so a saved schedule replays from itself alone.

use serde::{Deserialize, Serialize};

use crate::ids::{NodeId, OpId};
use crate::stripe::content::{Change, Content, Unit};
use crate::stripe::group::{
    Decision, EntryCommand, EntryState, PendingBytes, RowState, StripeCommand,
};
use crate::stripe::ids::{Label, Pos, Seq, SliceId, StripeIx};

/// Who a message is from or to
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Endpoint {
    /// An operation's driver - a stager, a reader, a rebuilder, a reclaimer - in one round
    Op {
        /// The operation
        op: OpId,
        /// Its round; an answer to an earlier round is ignored
        round: u8,
    },
    /// A slice, the holder of one chunk of every stripe placed on it
    Slice(SliceId),
    /// A stripe's tablet group
    Row(StripeIx),
    /// The object entry's tablet group
    Entry,
}

/// What a stage carries to a holder
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Payload {
    /// A whole chunk, staged beside the old one and renamed over it
    Whole(Content),
    /// New values for some units of a data chunk, journalled
    Units(Vec<(u8, Unit)>),
    /// The change a parity holder folds into its own parity, journalled
    Delta(Vec<Change>),
}

/// Why a holder refused a stage
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StageRefusal {
    /// Its chunk is not at the label the write expects
    Label,
    /// Its device cannot hold the stage
    Full,
    /// Its device failed
    Failed,
}

/// What a holder answered a read of a chunk at a label
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ChunkReply {
    /// The chunk at that label, verified
    Bytes(Content),
    /// It holds another label; the bytes are given so a reader that takes them can be shown to
    Other {
        /// The label it holds
        label: Label,
        /// What it holds under it
        content: Content,
    },
    /// It holds nothing for the stripe, or nothing that verifies
    Nothing,
}

/// A message body
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Body {
    /// Read the entry
    EntryRead {
        /// Whether it takes a barrier at the leader; a read that does not may be answered by
        /// a replica that lags
        strong: bool,
    },
    /// The entry, as one replica of its group had it
    EntryAnswer {
        /// Its index in the group's history
        index: u32,
        /// The state
        state: EntryState,
    },
    /// Read a stripe's row
    RowRead {
        /// Whether it takes a barrier at the leader
        strong: bool,
    },
    /// The row, as one replica of its group had it
    RowAnswer {
        /// Which stripe
        stripe: StripeIx,
        /// Its index in the group's history
        index: u32,
        /// The state
        state: RowState,
        /// Whether the operation that asked already committed a write here, by its identity:
        /// the group's retry table, which a retry is answered from
        #[serde(default, skip_serializing_if = "std::ops::Not::not")]
        written: bool,
    },
    /// Stage a stripe chunk under a write's label
    Stage {
        /// The stripe
        stripe: StripeIx,
        /// The position
        pos: Pos,
        /// The write's label
        label: Label,
        /// Whether the row existed when the write read it
        base_exists: bool,
        /// The sequence the write was staged against
        base: Seq,
        /// The label the holder's chunk has to carry, for a change to part of it
        expects: Option<Label>,
        /// The bytes
        payload: Payload,
    },
    /// A holder answers a stage once it is synced, or refuses it
    StageAnswer {
        /// The stripe
        stripe: StripeIx,
        /// The position
        pos: Pos,
        /// The write's label
        label: Label,
        /// Why it refused, if it did
        refused: Option<StageRefusal>,
    },
    /// Ask a holder whether it holds a label
    Confirm {
        /// The stripe
        stripe: StripeIx,
        /// The position
        pos: Pos,
        /// The label
        label: Label,
        /// Labels the asker's row makes that one from with the pending bytes it holds, any of
        /// which the holder may hold instead; the holder never reads the row itself
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        also: Vec<Label>,
    },
    /// Whether it does
    ConfirmAnswer {
        /// The stripe
        stripe: StripeIx,
        /// The position
        pos: Pos,
        /// The label
        label: Label,
        /// Whether it holds it, durably, on a device that answers
        holds: bool,
        /// Whether the holder could not be reached at all, which says nothing of what it holds
        #[serde(default, skip_serializing_if = "std::ops::Not::not")]
        unreachable: bool,
    },
    /// Read a chunk at a label
    ChunkRead {
        /// The stripe
        stripe: StripeIx,
        /// The position
        pos: Pos,
        /// The label the reader's row names
        label: Label,
        /// The labels the row's pending bytes fold from into it, each named by a committed row
        /// state, any of which the holder may answer with for the reader to lay them over
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        also: Vec<Label>,
    },
    /// What the holder had
    ChunkAnswer {
        /// The stripe
        stripe: StripeIx,
        /// The position
        pos: Pos,
        /// The label asked for
        label: Label,
        /// The answer
        reply: ChunkReply,
    },
    /// Propose a command to a stripe's group
    Propose(StripeCommand),
    /// Propose a command to the entry's group
    EntryPropose(EntryCommand),
    /// What a group decided
    Decided(Decision),
    /// Fold a row's pending bytes into the holder's chunk: journal them as the committed record
    /// they are, over the chunk they fold from, and answer as a stage is answered once synced
    Fold {
        /// The stripe
        stripe: StripeIx,
        /// The position
        pos: Pos,
        /// The pending bytes, as the committed row the sender read holds them
        pending: PendingBytes,
    },
    /// The row names this label for the holder's chunk: apply it
    Apply {
        /// The stripe
        stripe: StripeIx,
        /// The label
        label: Label,
    },
    /// A stager that gave up tells a holder to drop what it staged (an unsafe setting only)
    Drop {
        /// The stripe
        stripe: StripeIx,
        /// The label
        label: Label,
    },
    /// The reclaimer tells a holder a stripe is reclaimed
    Discard {
        /// The stripe
        stripe: StripeIx,
        /// The sequence the reclamation committed past: chunks labelled at or below it go
        through: Seq,
    },
    /// Ask the leader for the next turn at a stripe
    Reserve {
        /// The stripe
        stripe: StripeIx,
    },
    /// The leader grants it
    Granted {
        /// The stripe
        stripe: StripeIx,
    },
}

/// A message
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Message {
    /// The sender
    pub from: Endpoint,
    /// The receiver
    pub to: Endpoint,
    /// What it says
    pub body: Body,
}

/// One thing that happens
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum StripeEvent {
    /// A client writes some data units of one stripe; a write past the size extends the object
    Write {
        /// The write's identity
        op: OpId,
        /// The stripe
        stripe: StripeIx,
        /// The data units it writes, by index within the stripe
        units: Vec<u8>,
    },
    /// A client tries an operation whose outcome it does not know again, under the same identity
    Retry {
        /// The operation
        op: OpId,
    },
    /// A client cuts or grows the object
    Truncate {
        /// The truncate's identity
        op: OpId,
        /// The new length, in data units
        len: u32,
    },
    /// A client reads one stripe
    Read {
        /// The read's identity
        op: OpId,
        /// The stripe
        stripe: StripeIx,
        /// Whether it is a strong read, which takes a barrier on the entry and the row
        strong: bool,
    },
    /// A client gives up waiting: its outcome is unknown
    ClientTimeout {
        /// The operation
        op: OpId,
    },
    /// A stager gives up, while what it sent may still be in flight
    StagerTimeout {
        /// The write
        op: OpId,
    },
    /// The coordinating shard of an operation dies, forgetting it
    DriverCrash {
        /// The operation
        op: OpId,
    },
    /// A message arrives, or one already delivered arrives again
    Deliver {
        /// The message
        msg: Message,
    },
    /// A read of a group arrives at a replica that lags, and is answered from there
    DeliverLagging {
        /// The read
        msg: Message,
        /// How many commits behind the replica is
        lag: u8,
    },
    /// A slice's journal and staged files are synced, and the stages answered
    Sync {
        /// The slice
        slice: SliceId,
    },
    /// A slice takes one step of applying what it has learned is committed
    ApplyStep {
        /// The slice
        slice: SliceId,
        /// The stripe
        stripe: StripeIx,
    },
    /// A slice holding a staged write asks the stripe's group about it
    Ask {
        /// The slice
        slice: SliceId,
        /// The stripe
        stripe: StripeIx,
    },
    /// A node stops, losing everything not synced
    Crash {
        /// The node
        node: NodeId,
    },
    /// A node starts again and claims what it finds at its device's path
    Restart {
        /// The node
        node: NodeId,
    },
    /// A node's disk fails silently: every call to it errors, and nobody knows yet
    DiskFail {
        /// The node
        node: NodeId,
    },
    /// A node reports its disk failed, and the pool map marks it so
    DiskReport {
        /// The node
        node: NodeId,
    },
    /// A node's disk is swapped for an empty one at the same path, while the node is down
    DiskReplace {
        /// The node
        node: NodeId,
    },
    /// A node's disk fills, with something else's bytes
    Fill {
        /// The node
        node: NodeId,
    },
    /// The space comes back
    Free {
        /// The node
        node: NodeId,
    },
    /// The stripe group's leader, prompted by its timer, commits a no-op
    Noop {
        /// The stripe
        stripe: StripeIx,
    },
    /// The leader's rebuild driver brings a stale position current
    Rebuild {
        /// The driver's identity
        op: OpId,
        /// The stripe
        stripe: StripeIx,
        /// The position
        pos: Pos,
    },
    /// The pool map moves a position to another slice, and the driver copies it there and switches
    Move {
        /// The driver's identity
        op: OpId,
        /// The stripe
        stripe: StripeIx,
        /// The position
        pos: Pos,
        /// The slice it moves to
        to: SliceId,
    },
    /// The reclaimer deletes the stripes a floor hides, then the floor
    Reclaim {
        /// The reclaimer's identity
        op: OpId,
    },
    /// The leader's driver asks a stripe's holders to fold its pending bytes, and clears them
    /// once enough hold their label
    ClearPendingBytes {
        /// The driver's identity
        op: OpId,
        /// The stripe
        stripe: StripeIx,
    },
    /// The leader's reservation of a stripe lapses, and the next stager gets its turn
    ReservationLapse {
        /// The stripe
        stripe: StripeIx,
    },
}
