# Distributed Shoal

~~**Nothing in this part is built.**~~ ~~**Nothing distributed in this part is built.**~~
~~**Nodes speak to nodes; nothing is replicated yet.**~~ **Every tablet is replicated, and a
default write waits for a durable quorum.** These pages
plan a highly available cluster of Shoal nodes. The `C` pages are design records;
[milestones](milestones.md) name implementation gates, acceptance tests and benchmark evidence.
~~Four~~ ~~Five~~ Six gates are met: the protocol contract that precedes M0 was agreed on 2026-09-11 and is
numbered P1–P6 in [C13](protocol.md#the-contract);
[M0](milestones.md#m0-step-0-the-harness-and-the-facts) — the executable model of that contract,
the process fixture, and the benchmark's cluster record — was delivered the same day as
[F36](../features/cluster-harness.md); and
[M1](milestones.md#m1-node-identity-and-the-control-plane-thread) — node and cluster identity in
a format 2 marker, the `cluster:` block, a control thread on its own core running an embedded
`openraft` group of one on a glommio runtime, and the Q1/Q13 spike that chose the library and
found that heartbeats do not coalesce across groups — as
[F37](../features/node-identity-control-plane.md); and
[M2](milestones.md#m2-the-inter-node-transport) — the peer transport: three bounded lanes on
three sockets, a pre-schema hello, bundles forwarded as validated bytes against a static
placement, the control group's RPCs over the control lane, a trace that crosses the hop, mutual
kTLS, and the hop arms that price it — as [F38](../features/inter-node-transport.md); and
[M3](milestones.md#m3-membership) — membership: a node joining through seeds as a learner, the
voter policy enforced, a duplicate identity fenced by a persisted incarnation, the map committed
and pushed to every shard and every subscribed client, admin operations over the client
connection, writes admitted against their quorum, and the leader's phi-accrual detector — as
[F39](../features/membership.md); and
[M4](milestones.md#m4-replication-and-quorum-writes) — replication: a Raft group per table and
replica set on every shard, one shared WAL per shard with one fsync per batch across groups,
one command applied once in committed order on every replica with its result derived there,
a write answered by a durable majority's evidence or by a definite refusal or an unknown
outcome, `One` reads from the local replica's committed state, checkpoints the compactor
moves, and three arms that price a durable and a volatile quorum against the same placement
replicating to nobody — as [F40](../features/replication.md).
~~No node speaks to a node yet.~~ ~~A node speaks to a node it was placed beside; nothing joins,
elects across nodes or replicates yet.~~ And a read that can be made to see the write: a
`Quorum` read through a barrier from the tablet's leader, a session token a write hands back,
every gather with a slot per share and a deadline, and seven arms that price a barrier, a
token and a fan-out — as [F41](../features/read-consistency.md). And
[M6](milestones.md#m6-primary-failover) — failover: a retry table that survives the purge
point, a lapsed lease answered `NotLeader` before anything is appended, a barrier that follows
the leader, routing by health with a never-written share sent to another holder once, a client
identity and an opt-in retry, and one arm that records the outage as a time series — as
[F42](../features/primary-failover.md). And
[M7](milestones.md#m7-recover-a-node-brought-back-online) — recovery: a member behind the
purge point fed a snapshot per group, one file cut where the archives stand still, streamed in
resumable chunks over the bulk lane and installed atomically under a marker with the install
redone at open, the sealed WAL bounded in bytes with a forced purge behind the groups pinning
it, an installing group's tablets refusing reads while the rest of the node serves, and two
arms that price the catch-up by log and by snapshot — as
[F43](../features/node-recovery.md). What is not there: ~~strong
reads (M5),~~ ~~failover that
moves leadership and the retry table's durable mark (M6),~~ ~~a member behind the purge point
catching up (M7),~~ leadership moved toward a reader or
back to a returning node, and everything from rebalancing on. They extend the unbuilt
[Distribution](../appendix/todos.md#distribution) and
[Rebalancing](../appendix/todos.md#rebalancing) entries.

## What is being asked for

| # | Requirement | Where it is met |
| --- | --- | --- |
| R1 | Nodes are easy to add and remove | C3 membership, C8 migration, C9 runbooks |
| R2 | Removed nodes cause rebalancing | C8, including insufficient-capacity handling |
| R3 | Down nodes do not immediately cause rebalancing | C3 grace period, C7 elections without moving data |
| R4 | Eventually consistent by default | C6 `One` reads from committed replica prefixes |
| R5 | Default writes wait for a quorum | C5 stable-storage quorum acknowledgements |
| R6 | Reads may coordinate across nodes | C2 forwarding, C6 fan-out and strong reads |
| R7 | No external service for membership or failover | Embedded control and data protocols in C13 |

Performance matters throughout. C10 separates the overhead of distribution from gains due to
additional hardware and measures latency tails and sustainable throughput, including recovery.
No performance target permits weakening R5 without an explicit different consistency policy.

## Decisions and review changes

Keep a primary per tablet, embedded `openraft` for the control plane, and independent data
shards. The control-plane core defaults to CPU 0 and is configurable for restricted cpusets and
multi-process tests. Default writes are durable `Quorum`, default reads are `One`, and automatic
removal follows a configurable grace (proposed default 30m; `null` disables it).

The first draft's heartbeat-max promotion and map-only fencing are superseded. **The data
protocol is embedded Raft**, agreed at the Before-M0 gate; which library and runtime drive it
is subject to the spike in [C13](protocol.md#decision-record).
A control-plane placement decision cannot itself authorize a tablet primary. Table-qualified
replication streams have explicit durable, committed, applied and checkpointed progress;
snapshots have a stable cut and atomic installation; migrations use persisted consensus
configuration transitions. No separate service is deployed for any of this.

Reads remain eventually consistent by default even with consensus-backed writes. Single-tablet
strong reads use a read barrier; cross-tablet transactions and a shared query snapshot are not
part of this feature. Retry identity and unambiguous versus unknown write outcomes are included
before failover becomes application-ready.

## Vocabulary

| Term | Meaning |
| --- | --- |
| Node | One Shoal process, persistent NodeId, storage directory and advertised endpoints |
| Shard | One data thread owning its tables and sockets, normally pinned to a core |
| Range id | Initially the partition hash's top twelve bits; 4096 ranges |
| Tablet | `(TableId, range_id)`, the logical unit of replication, migration and progress |
| Replica set | Committed voters on distinct nodes, separate from learners and desired RF |
| Primary | The current data-protocol leader for a tablet, not merely a map preference |
| Term / epoch | Persisted leadership generation, established by the data protocol |
| Log index | Logical position across terms; does not reset on WAL rotation |
| Topology version | Control-plane committed version; separate from data term and configuration id |
| Control plane | Embedded membership/placement consensus, detector, admin and rebalancer |
| Data plane | Shards, embedded tablet replication groups, read execution and peer transport |

## The constraint every page inherits

Keep thread ownership and the local fast path. Peer frames contain validated bytes, never a
`ServerMsg::Partition` carrying a Glommio read buffer across threads. Share physical group commit
where appropriate, and serialize replication payloads once. A remote gather may require decoding;
zero-copy is a property of specific paths, not a blanket claim about distributed execution.

The control plane never serializes ordinary queries or tablet writes. Topology is cached; stale
routing uses bounded forwarding/refresh, and can return a retryable routing error when necessary.
Data groups establish their own authority. During loss of control-plane quorum existing tablet
groups can operate if their data quorum survives, while topology changes stop.

## The chapter

| # | Page | Purpose |
| --- | --- | --- |
| C1 | [Nodes and configuration](node-identity.md) | Identity, core allocation, cluster defaults |
| C2 | [Transport](transport.md) | Framing, ownership, flow control, compatibility |
| C3 | [Membership](membership.md) | Embedded Raft, liveness and removal grace |
| C4 | [Tablet map](tablet-map.md) | Placement versus data authority and transitions |
| C5 | [Replication](replication.md) | Durable writes, progress and retry identities |
| C6 | [Reads](reads.md) | Eventual reads, barriers, session tokens and gather semantics |
| C7 | [Failover and recovery](failover.md) | Safe elections, history reconciliation and snapshots |
| C8 | [Rebalancing](rebalancing.md) | Migration state machine and capacity-aware placement |
| C9 | [Operations](operations.md) | Admin, repair, upgrades and disaster recovery |
| C10 | [Performance](performance.md) | Comparable experiments and sustainable capacity |
| C11 | [Testing](testing.md) | Protocol model, process faults and operation histories |
| C12 | [Prior art](prior-art.md) | Relevant mechanisms with bounded claims and sources |
| C13 | [Protocol decisions and open questions](protocol.md) | Failure model, invariants and decision gates |
| — | [Milestones](milestones.md) | Ordered implementation, split into reviewable stages |

## The dependency graph

| Edge | Why |
| --- | --- |
| C13 + C11 → implementation | A protocol and executable safety properties precede storage changes |
| C1 → C2 → C3 | Identity, peer transport, embedded membership |
| C3 → C4 | Durable placement and transition records |
| C4 + C13 → C5 | Table-qualified groups and a selected data protocol |
| C5 → C6 + C7 | Read barriers and promotion depend on committed history |
| C7 → C8 | Atomic snapshots and recovery precede migration |
| C2 onward → C10 | Measure transport first, then replication, recovery and scale-out |
| C2 onward → C9 | Compatibility and admin authorization are designed early, exercised throughout |

M5 and M6 together ~~establish~~ established strong-read behavior during failover. C7's snapshot/checkpoint
contract is designed before M4, although transfer ~~lands~~ landed in M7. M9 separates migration mechanics,
placement policy and changing shard counts. See the detailed exit gates on the milestones page.

## How a C page is written

Design pages retain the sections Context, What exists today, The design, Alternatives rejected,
What it costs, What it breaks, Invariants to uphold, Prerequisites, How it would be measured,
Acceptance tests, and Related where applicable. Questions name the milestone they block in C13.
The acceptance tables are indexed by C11 without a second manually copied list of test names.

C and M identifiers are never reused. M9a/b/c refine M9 rather than renumbering M10. When work
ships, a new F page describes the implementation and evidence; its C page remains the design
record. Earlier unsafe claims are explained where replaced, not retained as live requirements.

## What this part is not

It is not a dated roadmap or a dependency on an external coordinator. It does not promise
multi-region availability without placement policy, cross-tablet transactions, or durability
beyond the documented failure model. [D7](../direction/shard-aware-routing.md) remains the client
routing design, but compatibility, retry identity and reconnect behavior required for cluster
correctness are dependencies of this feature rather than optional client optimizations.

## Related

[Partitioning](../architecture/partitioning.md), [Storage](../storage/overview.md),
[Direction](../direction/overview.md), and [C13](protocol.md).
