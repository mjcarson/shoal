# C12. Lessons from other clusters

## Context

Every protocol and implementation decision in this chapter cites a primary source, and similar
vocabulary is never taken as equivalence. The first draft's universal claims - about automatic
removal, interchangeable leases and high watermarks, and an extra per-write vote in Raft - were
withdrawn before anything was built. **Shoal embeds coordination and failover; these references
introduced no external service.** What each system lent, and what was refused, is below.

## The systems

### ScyllaDB

[How ScyllaDB implemented tablets](https://www.scylladb.com/2024/06/17/how-tablets/) is useful for
persisted transitions and independent tablet storage. Its load balancer reconciles transitions
after failure and serializes conflicting operations, which is what [C8](rebalancing.md)'s move
record, its phase-by-phase commit and its queue behind a repair do, under `openraft`'s own
membership transition.

A global tablet-id range shared across every table is not per-table tablet replication: Shoal
keeps logical identity per table, a placement rule shared across tables, and one physical WAL
per shard as three separate things ([C4](tablet-map.md), [C5](replication.md)). Scylla's
timestamp-based data path is not Shoal's primary model.

### Kafka

[Kafka replication design](https://kafka.apache.org/41/design/design/#replication) and
[KIP-101: leader epochs and truncation](https://cwiki.apache.org/confluence/display/KAFKA/KIP-101+-+Alter+Replication+Protocol+to+use+Leader+Epoch+rather+than+High+Watermark+for+Truncation)
are the references for the distinction between leader history, replication progress and
commitment. Kafka's ISR policy is not an arbitrary majority of currently `Up` replicas, its
names are not [C5](replication.md)'s quorum arithmetic, and a high watermark is not a read lease.
A controller's choice supplies neither Shoal's storage reconciliation nor its durability proof.

### Kudu, TiKV and CockroachDB

The pattern taken is independent replicated ranges with safe data leadership.
[raft-rs](https://github.com/tikv/raft-rs) was pinned as a candidate at the Before-M0 gate - a
consensus core whose embedding application supplies log, state machine and transport, driven
through [RawNode](https://docs.rs/raft/latest/raft/raw_node/struct.RawNode.html) - and was
considered, not measured: it has no runtime seam to adapt, which is the seam Q1 was blocking on,
and `openraft` took both planes ([C13](protocol.md#q1-and-q13-at-m1)). TiKV's placement service
was never a requirement.

[CockroachDB's replication layer](https://docs.cockroachlabs.com/docs/stable/architecture/replication-layer)
is the reference for separating replicas, leadership and serving authority. Its leases are not
evidence that a timer reset on metadata contact is a correct lease, and Shoal takes none: a
strong read is a barrier ([C6](reads.md)), and the one lease judged is a write's
([C7](failover.md#the-lease)).

[The Raft paper](https://raft.github.io/raft.pdf) specifies log matching, election safety,
configuration changes, snapshots and read considerations, and is what every adapter assumption
was reviewed against; the library's tested behaviour was preferred to consensus written from a
summary. A per-tablet replicated log provides no cross-tablet transaction, and none is promised.

### Cassandra

Cassandra's leaderless conflict resolution and tombstone handling address a different data model.
The first draft's analogy between a retired replica's cleanup grace and row tombstone GC was
withdrawn: keeping source files briefly proves nothing about resurrected deletes, and a
snapshot's total coverage ([C7](failover.md#snapshots-and-atomic-installation)) and a retired
copy's refusal by name ([C8](rebalancing.md#a-move)) are the boundary instead. Phi-accrual
detection came from Cassandra's lineage and stays separate from membership authority: phi is
a suspicion score, not a probability that a node is dead and not a fixed deadline ([C3](membership.md#failure-detection)).

### MongoDB

The comparison shows why write acknowledgement policy and rollback behaviour are stated
separately. An accepted-only `One` write would lose an uncommitted suffix, which is why it is
refused until an API says so; the default durable quorum loses nothing. No other product's
timeout default justifies the five second election base; what the base makes the failover
window is measured ([C7](failover.md#the-window-and-what-a-client-sees)).

### Aurora

Different quorum systems have different fault models and storage layouts. Shoal uses distinct
node replicas under `openraft`'s configuration rules and infers no durability promise from
another system's replica counts; [C13](protocol.md#failure-model-and-availability) is the
failure model it does promise.

### DragonflyDB

Thread ownership and journal batching are the architectural comparison - the shared WAL with one
fsync per batch across groups is the same instinct - and establish nothing about the safety of
elections or configuration changes. An external placement or failover controller was never in
the design; both protocols run in the nodes.

### FoundationDB

[The FoundationDB paper](https://www.foundationdb.org/files/fdb-paper.pdf), especially its testing
section, is the reference for simulation and controlled fault injection. [C11](testing.md)
applies that discipline to a bounded protocol model plus real process tests; the absence of a
simulated glommio runtime was not taken as a reason to reject deterministic schedules.

## Implementation reading list

These links were read for the decisions they name. The source for the pinned release was read at
each gate - `openraft 0.10.0-alpha.34` from the registry, never `latest` or a default branch -
and the [decision record](protocol.md#decision-record) carries the paths and lines it read.

| Reference | What it decided | Decided at |
| --- | --- | --- |
| [Raft extended paper](https://raft.github.io/raft.pdf) | Election restrictions, matching histories, configuration transitions and snapshot metadata: the contract's P2–P5 | Before M0; [C13](protocol.md#the-contract) |
| [OpenRaft integration guide](https://docs.rs/openraft/latest/openraft/docs/getting_started/index.html) | The network and storage seams the control plane and the shards implement; the conformance suites both pass | M1, M3 |
| [OpenRaft source](https://github.com/databendlabs/openraft) | The runtime, network and membership APIs at the pinned alpha; `enable_leader_restore` off, `allow_log_reversion` on for the control group | M1, M10c |
| [OpenRaft RaftLogStorage](https://docs.rs/openraft/latest/openraft/storage/trait.RaftLogStorage.html) | Vote and log persistence, truncation and purge, implemented by the control store and the shared WAL | M1, M4 |
| [OpenRaft RaftStateMachine](https://docs.rs/openraft/latest/openraft/storage/trait.RaftStateMachine.html) | Apply, and the snapshot build and install lifecycle | M4, M7 |
| [raft-rs source](https://github.com/tikv/raft-rs) and [RawNode](https://docs.rs/raft/latest/raft/raw_node/struct.RawNode.html) | Pinned as the alternative with application-supplied I/O; considered, not measured, and not taken | M1 |
| [etcd Raft library](https://github.com/etcd-io/raft) | An independent embedded example of a deterministic core and its read paths; never an etcd deployment | Before M0, C11 |
| [etcd learner design](https://etcd.io/docs/v3.5/learning/design-learner/) | Why catching up precedes promotion: the move's `Learner` and `CatchingUp` phases | M9a |
| [Scylla tablet implementation](https://www.scylladb.com/2024/06/17/how-tablets/) | Resumable transition records, conflicting operations serialized, movable tablet storage | M9a, M9b |
| [Linux fsync/fdatasync](https://man7.org/linux/man-pages/man2/fsync.2.html) | Durability completion, I/O errors and directory synchronization for the WAL, the checkpoint and the install marker | M4, M7 |
| [Linux rename](https://man7.org/linux/man-pages/man2/rename.2.html) | Atomic replacement, which is not a durability barrier: the install syncs the data, the marker and the directory | M7 |
| [FoundationDB paper](https://www.foundationdb.org/files/fdb-paper.pdf) | Reproducible fault scheduling: the protocol model's saved schedules and the fixture's named crash points | M0 onward |

## The comparison

Shoal's guarantees follow its embedded protocol and storage adapter, not an analogy to any one
product. Automatic removal is a deployment policy with a finite grace - thirty minutes, `null`
to disable - not an industry-wide rule claimed to be universally on or off. The distinction
every page keeps is between suspected failure, data authority, safe replacement and final
cleanup.

## Related

[C13](protocol.md), [C3](membership.md), [C5](replication.md),
[C7](failover.md), [C8](rebalancing.md), [C11](testing.md),
[D9](../direction/prior-art.md) for the separate client-side review.
