//! The events a schedule is made of, the messages nodes exchange, and what a step reports
//!
//! Everything that can happen to the model is one [`Event`]. A schedule is a list of them, and
//! replaying a list is applying each in turn - which is what makes a schedule saveable,
//! minimizable and reproducible. Messages are carried inside [`Event::Deliver`] whole, so a
//! saved file needs nothing but itself to replay.

use serde::{Deserialize, Serialize};

use crate::ids::{Attempt, Key, LogIndex, NodeId, TabletId, Term, Value};
use crate::oracle::Outcome;
use crate::storage::Entry;

/// Who sends and receives messages
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Actor {
    /// A replica
    Node(NodeId),
    /// The control-plane observer that collects progress reports and marks nodes down
    Observer,
}

/// One thing that happens to the model
///
/// Timers are events rather than clocks, and storage completions are events rather than
/// immediate, which is the whole reason the model can be driven through elections, crashes and
/// slow disks in any order a schedule likes (C11's "explicit events").
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Event {
    /// A client sends an operation to a node
    ClientInvoke {
        /// Which attempt at which operation
        attempt: Attempt,
        /// The tablet it addresses
        tablet: TabletId,
        /// What it asks for
        op: ClientOp,
        /// The node it is sent to
        target: NodeId,
    },
    /// A client gives up waiting for an attempt; its outcome is now unknown
    ClientTimeout {
        /// The attempt given up on
        attempt: Attempt,
    },
    /// A message in flight arrives, or one already delivered arrives again
    Deliver {
        /// The message, whole
        msg: Message,
    },
    /// A node's election timer fires for a tablet
    ElectionTimeout {
        /// Which node
        node: NodeId,
        /// Which tablet's group
        tablet: TabletId,
    },
    /// A leader's heartbeat timer fires for a tablet
    HeartbeatTick {
        /// Which node
        node: NodeId,
        /// Which tablet's group
        tablet: TabletId,
    },
    /// A node reports its progress on a tablet to the observer
    Report {
        /// Which node
        node: NodeId,
        /// Which tablet's group
        tablet: TabletId,
    },
    /// The oldest pending fsync on a node's tablet log completes
    StorageComplete {
        /// Which node
        node: NodeId,
        /// Which tablet's log
        tablet: TabletId,
    },
    /// A node takes a checkpoint of a tablet's applied state
    Checkpoint {
        /// Which node
        node: NodeId,
        /// Which tablet
        tablet: TabletId,
    },
    /// A node loses everything but its stable storage
    Crash {
        /// Which node
        node: NodeId,
    },
    /// A crashed node comes back from its stable storage
    Restart {
        /// Which node
        node: NodeId,
    },
    /// A node stops running without losing anything; its inputs queue until it resumes
    Pause {
        /// Which node
        node: NodeId,
    },
    /// A paused node runs again and handles everything that queued
    Resume {
        /// Which node
        node: NodeId,
    },
    /// The observer's grace period for a node expires and it is marked down
    MarkDown {
        /// Which node
        node: NodeId,
    },
}

/// A message between actors
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Message {
    /// Who sent it
    pub from: Actor,
    /// Who it is for
    pub to: Actor,
    /// The tablet it concerns
    pub tablet: TabletId,
    /// The sender's term when it was sent
    pub term: Term,
    /// What it says
    pub body: Body,
}

/// What a message says
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Body {
    /// A candidate asks for a vote
    RequestVote {
        /// The candidate's last log index
        last_index: LogIndex,
        /// The term of that entry
        last_term: Term,
    },
    /// A voter answers
    VoteResponse {
        /// Whether the vote was granted
        granted: bool,
    },
    /// A leader replicates entries, or heartbeats with none
    AppendEntries {
        /// The index the entries follow
        prev_index: LogIndex,
        /// The term of the entry at that index
        prev_term: Term,
        /// The entries, in order
        entries: Vec<Entry>,
        /// The leader's commit index
        leader_commit: LogIndex,
    },
    /// A follower answers a replication
    AppendResponse {
        /// Whether the entries matched and were accepted
        success: bool,
        /// The highest index the follower vouches for: durable and matching, or merely received
        durable_to: LogIndex,
        /// Whether `durable_to` is backed by a completed fsync; `false` is an Async-disk receipt
        durable: bool,
        /// Where the leader should retry from after a mismatch
        conflict_hint: LogIndex,
    },
    /// A node tells the observer how far its log reaches
    ProgressReport {
        /// The node's last log index, fsynced or not
        last_index: LogIndex,
        /// The term of that entry
        last_term: Term,
        /// Whether the node believes it leads this tablet
        leader: bool,
    },
    /// The observer tells every node which nodes it considers up
    UpList {
        /// The nodes considered up
        up: Vec<NodeId>,
    },
    /// The observer appoints a leader from its reports
    ///
    /// Only ever sent under the unsafe election policy. This is the first draft's heartbeat-max
    /// promotion, and the model exists partly to show it losing a write.
    Promote {
        /// The term the promoted node should lead in
        term: Term,
    },
}

/// What a client asks for
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ClientOp {
    /// A mutation, ordered by the tablet's leader
    Mutate(MutationOp),
    /// A `One` read of a key from whichever node it was sent to
    Read {
        /// The key
        key: Key,
    },
}

impl ClientOp {
    /// The key this operation touches
    pub fn key(&self) -> Key {
        match self {
            ClientOp::Mutate(op) => op.key(),
            ClientOp::Read { key } => *key,
        }
    }
}

/// A mutation of one key
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MutationOp {
    /// Insert if absent
    Insert {
        /// The key
        key: Key,
        /// The value
        value: Value,
    },
    /// Update if present
    Update {
        /// The key
        key: Key,
        /// The value
        value: Value,
    },
    /// Delete if present
    Delete {
        /// The key
        key: Key,
    },
    /// Set if the current value is the expected one
    Cas {
        /// The key
        key: Key,
        /// What the current value must be, `None` meaning absent
        expected: Option<Value>,
        /// The value to set
        value: Value,
    },
}

impl MutationOp {
    /// The key this mutation touches
    pub fn key(&self) -> Key {
        match self {
            MutationOp::Insert { key, .. }
            | MutationOp::Update { key, .. }
            | MutationOp::Delete { key }
            | MutationOp::Cas { key, .. } => *key,
        }
    }
}

/// What an operation returned
///
/// A mutation's result depends on the state it was applied to, which is why it is derived in
/// committed order and stored with the effect (P4, C5): a retry gets the original.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OpResult {
    /// Whether a mutation changed anything
    Applied(bool),
    /// What a read saw
    Value(Option<Value>),
}

/// What a node is handed in one step
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Input {
    /// A message from another actor
    Message(Message),
    /// A local event: a timer, a client, a storage completion
    Local(Event),
}

/// What a node produces in one step
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Output {
    /// A message to send
    Send(Message),
    /// A fact for the checker
    Effect(Effect),
    /// An answer to a client
    Client {
        /// The attempt answered
        attempt: Attempt,
        /// What it is told
        outcome: Outcome,
    },
}

/// A fact about a transition, for the invariant checker
///
/// The checker never reads a node's opinion of what is committed; it reads these, which are
/// what a node *did*, and works out the rest from durable state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Effect {
    /// A node started leading a tablet
    BecameLeader {
        /// Which node
        node: NodeId,
        /// Which tablet
        tablet: TabletId,
        /// In which term
        term: Term,
        /// Whether it was appointed by the observer rather than elected
        by_promotion: bool,
        /// Where its log ended as it took over, before the noop of its term
        last_index: LogIndex,
    },
    /// A leader stopped leading
    SteppedDown {
        /// Which node
        node: NodeId,
        /// Which tablet
        tablet: TabletId,
        /// The term it stepped down in
        term: Term,
    },
    /// Entries were appended to a node's volatile log
    Appended {
        /// Which node
        node: NodeId,
        /// Which tablet
        tablet: TabletId,
        /// The first new index
        from: LogIndex,
        /// The last new index
        to: LogIndex,
    },
    /// A node truncated its log from an index
    Truncated {
        /// Which node
        node: NodeId,
        /// Which tablet
        tablet: TabletId,
        /// The first removed index
        from: LogIndex,
        /// Every removed entry's index and term
        removed: Vec<(LogIndex, Term)>,
    },
    /// A node's log became durable through an index
    Fsynced {
        /// Which node
        node: NodeId,
        /// Which tablet
        tablet: TabletId,
        /// The durable prefix now ends here
        through: LogIndex,
        /// The node's term when the fsync completed
        at_term: Term,
    },
    /// A node vouched to a leader, or to itself, that its log is durable through an index
    DurableClaim {
        /// Which node
        node: NodeId,
        /// Which tablet
        tablet: TabletId,
        /// The index claimed durable
        through: LogIndex,
    },
    /// A leader advanced its commit index
    CommitAdvanced {
        /// Which node
        node: NodeId,
        /// Which tablet
        tablet: TabletId,
        /// The leader's term
        term: Term,
        /// The new commit index
        to: LogIndex,
        /// The replicas it counted, in the order it counted them
        evidence: Vec<NodeId>,
        /// The population it counted them against
        over: Vec<NodeId>,
    },
    /// A client was told its mutation succeeded
    ClientOk {
        /// The attempt
        attempt: Attempt,
        /// Which tablet
        tablet: TabletId,
        /// The log index the mutation holds
        index: LogIndex,
    },
    /// A node answered a read
    Read {
        /// Which node
        node: NodeId,
        /// Which tablet
        tablet: TabletId,
        /// The key
        key: Key,
        /// The last log index reflected in what it answered
        observed: LogIndex,
    },
    /// A node took a checkpoint
    Checkpointed {
        /// Which node
        node: NodeId,
        /// Which tablet
        tablet: TabletId,
        /// The last log index the checkpoint includes
        last_included: LogIndex,
    },
    /// A node crashed
    Crashed {
        /// Which node
        node: NodeId,
    },
    /// A node restarted
    Restarted {
        /// Which node
        node: NodeId,
    },
    /// A node paused
    Paused {
        /// Which node
        node: NodeId,
    },
    /// A node resumed
    Resumed {
        /// Which node
        node: NodeId,
    },
}
