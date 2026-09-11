# Distributed Shoal

~~**Nothing in this part is built.**~~ **Nothing distributed in this part is built.** These pages
plan a highly available cluster of Shoal nodes. The `C` pages are design records;
[milestones](milestones.md) name implementation gates, acceptance tests and benchmark evidence.
Two gates are met: the protocol contract that precedes M0 was agreed on 2026-09-11 and is
numbered P1–P6 in [C13](protocol.md#the-contract), and
[M0](milestones.md#m0-step-0-the-harness-and-the-facts) — the executable model of that contract,
the process fixture, and the benchmark's cluster record — was delivered the same day as
[F36](../features/cluster-harness.md). No node speaks to a node yet. They extend the unbuilt
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

M5 and M6 together establish strong-read behavior during failover. C7's snapshot/checkpoint
contract is designed before M4, although transfer lands in M7. M9 separates migration mechanics,
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
