# C12. Lessons from other clusters

## Context

Use primary sources for individual protocol and implementation decisions. Similar vocabulary does
not make protocols equivalent. This revision withdraws the first draft's universal claims about
automatic removal, interchangeable leases/high watermarks, and an extra per-write vote in Raft.
**Shoal embeds coordination and failover; these references introduce no external service.**

## The systems

### ScyllaDB

[How ScyllaDB implemented tablets](https://www.scylladb.com/2024/06/17/how-tablets/) is useful for
persisted transitions and independent tablet storage. Its load balancer reconciles transitions
after failure and serializes conflicting operations. Use those ideas in C8 while implementing
Shoal's own selected ordered-replication configuration protocol.

Do not equate a global tablet-id range shared across every table with per-table tablet replication.
Separate logical identity, shared placement templates and physical WAL batching (C4/C5).
Scylla's timestamp-based data path is not the chosen Shoal primary model.

### Kafka

[Kafka replication design](https://kafka.apache.org/41/design/design/#replication) and
[KIP-101: leader epochs and truncation](https://cwiki.apache.org/confluence/display/KAFKA/KIP-101+-+Alter+Replication+Protocol+to+use+Leader+Epoch+rather+than+High+Watermark+for+Truncation)
are references for the distinction between leader history, replication progress and commitment.
Kafka's ISR policy is not an arbitrary majority of currently Up replicas. Do not copy its names
onto C5's quorum arithmetic or call a high watermark a read lease. A controller choice also does
not by itself supply Shoal's storage reconciliation or durability proof.

### Kudu, TiKV and CockroachDB

The useful pattern is independent replicated ranges/tablets with safe data leadership. For Shoal's
Rust integration spike, [raft-rs](https://github.com/tikv/raft-rs) provides a consensus core while
the embedding application supplies log, state machine and transport. Inspect
[RawNode](https://docs.rs/raft/latest/raft/raw_node/struct.RawNode.html) and its persistence/apply
advance points before selecting the adapter. It is a library candidate, not a requirement to
run TiKV or its placement service.

[CockroachDB's replication layer](https://docs.cockroachlabs.com/docs/stable/architecture/replication-layer)
is a reference for separating replicas, leadership and serving authority. Its implementation is
not evidence that Shoal can implement a correct lease by resetting a timer on metadata contact.

[The Raft paper](https://raft.github.io/raft.pdf) specifies log matching, election safety,
configuration changes, snapshots and read considerations. Use it to review adapter assumptions;
prefer tested library behavior rather than implement consensus from a summary. A per-tablet
replicated log does not provide cross-tablet transactions.

### Cassandra

Cassandra's leaderless conflict resolution and tombstone handling address a different data model.
The earlier analogy between a dropped Shoal replica's cleanup grace and row tombstone GC is
withdrawn: keeping source files briefly is not a proof that deletes cannot be resurrected.
C7's complete checkpoint manifest and C8's eligibility/configuration rules provide that boundary.
Failure suspicion remains separate from membership authority; phi is not a literal dead-node
probability or a fixed-duration bound (C3).

### MongoDB

The comparison illustrates why write acknowledgement policy and rollback behavior must be stated
separately. Shoal's optional accepted-only `One` policy may lose an uncommitted suffix; the default
durable quorum may not. No foreign product's timeout default is used as justification for Shoal's
five-second election base. Q1 measures/tunes that base under the chosen embedded protocol.

### Aurora

Different quorum systems have different fault models and storage layouts. Shoal uses distinct
node replicas and the selected consensus configuration rules; it does not infer a durability
promise by copying another system's replica counts. C13 defines the supported failure model.

### DragonflyDB

Thread ownership and journal batching are useful architectural comparisons. They do not establish
the safety of Shoal's elections or configuration changes. In particular, an external placement
or failover controller is outside this plan. Shoal's control and data protocols run in its nodes.

### FoundationDB

[The FoundationDB paper](https://www.foundationdb.org/files/fdb-paper.pdf), especially its testing
section, is a reference for simulation and controlled fault injection. C11 applies that discipline
to a bounded new protocol/adapter state machine plus real process tests. The absence of a simulated
Glommio runtime is not a reason to reject every deterministic schedule test.

## Implementation reading list

These links were consulted or identify APIs/contracts vital to implementation. Read the source
for the chosen release at each gate; `latest` documentation and default branches are discovery
links, not dependency pins. Replace/add exact release and commit permalinks in the decision record
when Q1/Q2/Q10 settle the implementation. That [record](protocol.md#decision-record) exists since
2026-09-11 and pins the Q1 candidates at the versions it read; it selects none of them yet.

| Reference | Why to read it | Gate |
| --- | --- | --- |
| [Raft extended paper](https://raft.github.io/raft.pdf) | Election restrictions, matching histories, configuration transitions and snapshot metadata | C13 Q1, M1/M4 |
| [OpenRaft integration guide](https://docs.rs/openraft/latest/openraft/docs/getting_started/index.html) | Embedded application network/storage seams and storage conformance tests | C3, M1/M3 |
| [OpenRaft source](https://github.com/databendlabs/openraft) | Pin runtime, network and membership APIs; inspect actual completion behavior | C13 Q1 |
| [OpenRaft RaftLogStorage](https://docs.rs/openraft/latest/openraft/storage/trait.RaftLogStorage.html) | Vote and log persistence, truncation and purge obligations | C3/C5, M1/M4 |
| [OpenRaft RaftStateMachine](https://docs.rs/openraft/latest/openraft/storage/trait.RaftStateMachine.html) | Applied state, configuration and checkpoint installation lifecycle | C7, M7 |
| [raft-rs source](https://github.com/tikv/raft-rs) | Candidate Rust consensus core with application-supplied I/O | C13 Q1 |
| [raft-rs RawNode](https://docs.rs/raft/latest/raft/raw_node/struct.RawNode.html) | Ready batches, persistence/application advancement, read-index and membership entry points | C5/C6, M4/M5 |
| [etcd Raft library](https://github.com/etcd-io/raft) | Independent embedded-library example of deterministic core, read paths and I/O integration; not an external etcd deployment | C13 Q1, C11 |
| [etcd learner design](https://etcd.io/docs/v3.5/learning/design-learner/) | Why catching up a new member must be separated from increasing voting requirements | C8, M9a |
| [Scylla tablet implementation](https://www.scylladb.com/2024/06/17/how-tablets/) | Resumable transition metadata, conflicting operations and tablet storage organization | C4/C8, M9a/b |
| [Linux fsync/fdatasync](https://man7.org/linux/man-pages/man2/fsync.2.html) | Durability completion, I/O errors and required directory synchronization | C5/C7, M4/M7 |
| [Linux rename](https://man7.org/linux/man-pages/man2/rename.2.html) | Atomic replacement semantics and filesystem constraints; rename alone is not a durability barrier | C7, M7 |
| [FoundationDB paper](https://www.foundationdb.org/files/fdb-paper.pdf) | Reproducible fault scheduling and simulation/testing discipline | C11, M0 onward |

## The comparison

Shoal's selected guarantees must follow its actual embedded protocol and storage adapter, not an
analogy to any one product. Automatic removal is a deployment policy with a proposed finite grace,
not an industry-wide rule claimed to be universally on or off. The critical distinction is
between suspected failure, data authority, safe replacement and final cleanup.

## Related

[C13](protocol.md) decisions and open gates, [C3](membership.md), [C5](replication.md),
[C7](failover.md), [C8](rebalancing.md), [C11](testing.md),
[D9](../direction/prior-art.md) for the separate client-side review.
