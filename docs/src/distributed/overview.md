# Distributed Shoal

**Nothing in this part is built.** Every page describes how Shoal would run as a cluster of nodes —
replicated, highly available, elastic — and none of it exists in the tree. A `C` page is the design
written down *before* a feature page, in the same relationship to [TODOs](../appendix/todos.md) that
the [Direction](../direction/overview.md) chapter has: `todos.md` records *that* distribution is
unbuilt, in two entries ([Distribution](../appendix/todos.md#distribution) and
[Rebalancing](../appendix/todos.md#rebalancing)); this part says how it would be built, what it
would cost, what it would break, and how each piece would be proved to work.

It is a chapter rather than two longer TODO entries for the reason Direction gave for itself
(`direction/overview.md:11-16`): every piece lands on the same two missing things. There is one
transport seam — `ShardContact` has one variant, `Local(usize)`
(`shoal-core/src/server/shard.rs:824`), and the `match` in `Comms::send` has one arm
(`shoal-core/src/server/comms.rs:40-53`) — and there is one authority missing, over the tablet map
that `Ring::new` recomputes from the shard count on every start
(`shoal-core/src/server/ring.rs:72`). Replication, membership, failover and rebalancing each ask
for both, and answering them once is most of the work.

## What is being asked for

Six requirements, numbered so the pages can name which one a decision serves:

| # | Requirement | Where it is met |
| --- | --- | --- |
| **R1** | Adding and removing a node is easy | [C3](membership.md) joins; [C8](rebalancing.md) assigns and drains |
| **R2** | A node that is *removed* causes the cluster to rebalance | [C8](rebalancing.md) |
| **R3** | A node that is merely *down* does not immediately cause a rebalance | [C3](membership.md), [C7](failover.md) — only its primaries move, and no data does |
| **R4** | Eventually consistent by default | [C5](replication.md), [C6](reads.md) — reads default to `One` |
| **R5** | Writes wait for a quorum before the client is told they were accepted | [C5](replication.md) — writes default to `Quorum` |
| **R6** | Reads may have to be coordinated across nodes | [C2](transport.md), [C6](reads.md) — the coordinator's fan-out crosses nodes |

And one constraint that every page has to answer for rather than a requirement that one page
meets: **distribution must not drastically slow down reads or writes.** Every `C` page has a *What
it costs* section, [C10](performance.md) says what would measure each cost, and the
[milestones](milestones.md) make the first of those measurements an exit criterion rather than a
hope.

Four decisions were taken before any page was written, and every page assumes them:

- **The data path has a primary per tablet, and replication is ordered.** Each tablet's primary
  shard serializes that tablet's writes, stamps each one `(epoch, seq)`, forwards the intent bytes
  it has already serialized to the followers, and acknowledges the client at a quorum. This is not
  Scylla's leaderless model, and [C5](replication.md#alternatives-rejected) says why: an update in
  Shoal carries only the fields it changes (`T::UpdateData`,
  [The Intent Log](../storage/intent-log.md#intent-types)), and last-writer-wins over partial
  updates needs a timestamp per field, which is a redesign of row storage.
- **The control plane is Raft, via `openraft`, on the cpu nothing uses.** `Resources::cpus()`
  drops cpu 0 unconditionally as "the coordinator cpu" (`shoal-core/src/server/conf.rs:75`) and
  no coordinator has ever run there. Membership, the tablet map and primary epochs live in one
  Raft group. [C3](membership.md#openraft-and-the-runtime) records what was checked against
  `openraft`'s source, and when.
- **A down node keeps its tablets; a removed node loses them.** Removal is an operator's action, or
  — when `cluster.auto_remove_after` is set, which it is not by default — a timeout's. Until then
  the only thing that moves off a down node is the role of primary, which is a map edit and not a
  data transfer.
- **Eventually consistent by default, with the strong options one field away.** Writes default to
  `Quorum` and reads to `One`; the defaults live in `shoal.yml`, a table may override them, and a
  bundle may override both on the wire.

## Vocabulary

The book's [glossary](../appendix/glossary.md) says "distributed" has meant "multiple shards in one
process" in Shoal so far. These pages use the following words with these meanings and no others:

| Term | Meaning here |
| --- | --- |
| **Node** | One Shoal process on one machine, with one `NodeId` and one storage directory ([C1](node-identity.md)) |
| **Shard** | What it is today: one thread pinned to one core owning a slice of every table. Addressed cluster-wide as `(node, shard)` |
| **Tablet** | What it is today: one of 4096 slices of the partition key space, named by the key's top twelve bits (`ring.rs:25-31`) |
| **Replica set** | The `RF` shards, on `RF` distinct nodes, that hold a tablet's data. Stored per tablet in the map ([C4](tablet-map.md)) |
| **Primary** | The one replica that serializes a tablet's writes. **A role in the map, not a kind of shard** — a shard is primary for some tablets and follower for others |
| **Follower** | Every other replica. Holds the same intent records in the same order, and may answer reads at `One` |
| **Epoch** | A counter per tablet, incremented every time its primary changes. What fences a primary that does not know it has been replaced |
| **Seq** | A counter per tablet per epoch, assigned by the primary to every write. What lets a follower apply in order and say how far behind it is |
| **Topology version** | The Raft log index at which the map last changed. What a stale map is stale relative to |
| **Consistency level** | How many replicas a write waits for, or which replica a read asks: `One`, `Quorum`, `All` for writes; `One`, `Primary`, `Quorum` for reads ([C6](reads.md)) |
| **Control plane** | The Raft group, the failure detector and the rebalancer, on cpu 0 of every node. Never on a query path |
| **Data plane** | The shards, exactly as today, plus the peer connections between them ([C2](transport.md)) |

## The constraint every page inherits

Direction's constraint (`direction/overview.md:34-50`) still applies — the link between peers is
fast, reliable and inside a trust boundary, and **a microsecond is a large number** — and three
properties of the single-node engine sharpen it. Each page has to say whether its recommendation
keeps them:

- **No locks on the data path.** Every table is owned by one thread and reached by no other
  ([Thread per Core](../architecture/thread-per-core.md)). A peer connection therefore belongs to
  one shard, and a frame that arrives on the wrong shard crosses to the right one over the same
  `kanal` mesh a client's query crosses today, never through shared state.
- **The `Send` invariant on `ServerMsg::Partition`.** It may only ever travel on a shard's own
  channel ([Thread per Core](../architecture/thread-per-core.md#the-send-escape-hatch)). A remote
  contact can carry bytes and never a partition; [C2](transport.md#invariants-to-uphold) restates
  this because it is the one rule a transport is best placed to break.
- **The response path is zero-copy** and the request path is routed without deserializing
  ([Wire Protocol](../architecture/wire-protocol.md#alignment),
  [F26](../features/archive-routed-requests.md)). A query forwarded to another node is forwarded as
  the bytes it arrived in, and its answer comes back as the bytes it was sealed into.

## The chapter

| # | Page | In one line |
| --- | --- | --- |
| C1 | [Nodes, identity, and the cluster configuration](node-identity.md) | A `NodeId`, a `ClusterId`, a `cluster:` block that is optional, and a control-plane thread on the cpu that was always reserved for one |
| C2 | [The inter-node transport](transport.md) | `ShardContact::Remote`, one peer connection per shard per node, the same framing module with new message types, and forwarded bytes rather than forwarded rows |
| C3 | [Membership and failure detection](membership.md) | One Raft group of nodes, a phi-accrual detector, and the rule that being down moves nothing |
| C4 | [The replicated tablet map](tablet-map.md) | `Vec<u16>` becomes `Vec<ReplicaSet>`, becomes Raft state, and is pushed to shards and clients with a version |
| C5 | [Replication and the write path](replication.md) | The primary stamps and forwards the intent bytes it already has; `PendingResponse` waits for acks instead of a watermark |
| C6 | [Reads and consistency levels](reads.md) | `One` from any replica, `Primary` under a lease, `Quorum` with read repair; a per-bundle override on the wire |
| C7 | [Primary failover and recovering a node](failover.md) | A new primary is the replica that has the most, and a returning node catches up by seq or by snapshot |
| C8 | [Adding, removing and rebalancing nodes](rebalancing.md) | One tablet per pair at a time, follower first, and storage that stays keyed by shard |
| C9 | [Operating a cluster](operations.md) | The admin frames, a `shoalctl` cluster tab, metrics, and the runbooks |
| C10 | [Performance, and the benchmarks that judge it](performance.md) | What each path costs, how a cluster is emulated on one machine, and the workloads that would say |
| C11 | [Acceptance tests and the cluster harness](testing.md) | N processes, real signals, a write ledger, and the table every page's tests roll up into |
| C12 | [Lessons from other clusters](prior-art.md) | Scylla, Kafka, Kudu, Cassandra, MongoDB, Aurora, Dragonfly and FoundationDB — what to copy and what not to |
| — | [Milestones](milestones.md) | Eleven steps in four groups, each with its tests, its benchmark and its exit criterion |

## The dependency graph

The book has no mermaid preprocessor, so this is a table, hard edges only, the same way
[Direction](../direction/overview.md#the-dependency-graph) and
[Optimizations](../appendix/optimizations.md) draw theirs.

| Edge | Why |
| --- | --- |
| C1 → C2 | A peer handshake names a cluster and a node; neither exists until C1 mints them |
| C2 → C3 | Raft messages travel on the peer connections. The control plane does not open sockets of its own |
| C3 → C4 | The map is Raft state. Without a group there is nobody to hold it |
| C4 → C5 | A write is routed to a *primary*, which is a field of the map |
| C5 → C6 | `Quorum` and `Primary` reads compare `(epoch, seq)`, which only exist once writes are stamped |
| C3, C5 → C7 | Failover is a map edit the leader proposes when the detector says `Down`, and the new primary is chosen by what C5 stamped |
| C4, C5, C7 → C8 | A move adds a follower first, which is C7's catch-up stream, then flips a map entry |
| C2 → C10 | The first thing worth measuring is the hop, and it needs a transport to cross |
| C11 → everything | The harness is step 0. Nothing on any other page can be shown to work without a way to start three processes and kill one |

The one edge deliberately **not** there: **C6 does not gate C7.** A cluster with only `One` reads
still has to fail over, and the ledger test on [C7](failover.md#acceptance-tests) reads at `Quorum`
only because that is the read that proves nothing was lost — the failover itself does not need it.

## How a `C` page is written

Direction's template (`direction/overview.md:63-70`), plus two sections this part needs that
Direction did not: *Invariants to uphold*, taken from the feature-page template because the
reader of these pages will one day be changing the code they describe, and *Acceptance tests*,
because the ask was that each piece say how it would be proved. Every page uses the same sections,
in this order:

**Context** — what makes this worth having. **What exists today**, cited — a proposal is only
legible against what it replaces. **The design**, or **The options** where several are still live.
**Alternatives rejected**. **What it costs** and **What it breaks**. **Invariants to uphold** —
what the design depends on, not what it does. **Prerequisites**, naming other `C` items and the
`D`, `F` and issue numbers they rest on. **How it would be measured**, naming a workload that
exists or one [C10](performance.md) proposes. **Acceptance tests**, one row per test, each naming
the milestone that builds it. **Related**.

Two rules from the rest of the book apply here unchanged.

**`C` numbers and `M` numbers are never reused.** The same rule `F`, `D`, `O` and issue numbers
follow. When a `C` item is built its page stays here as the design record and a new `F` page
describes what shipped — "a design that survived contact with the code unchanged is rare enough to
be worth showing" (`direction/overview.md:52-59`). [Milestones](milestones.md) says which `C` page
each step turns into an `F` page.

**A design page that argues from a dependency's prose rather than its source can be confidently
wrong.** Direction learned this from D4 (`direction/overview.md:178-185`). This part argues from
one dependency it does not yet have, `openraft`, and [C3](membership.md#openraft-and-the-runtime)
records what was read in its source and on what date, and names the two things that still have to
be read before the milestone that needs them starts.

## What this part is not

It is not a client design. [D7](../direction/shard-aware-routing.md) is the *client* half of
routing — a client holding the tablet map and reaching the owning shard directly — and
[TODOs](../appendix/todos.md#distribution) already says it is a prerequisite for multi-node routing
and not a substitute for it. That is still true. What changes is the order: D7 recommended building
the `Topology` frame first, as pure observability, and [C4](tablet-map.md#pushed-to-clients) builds
exactly that step, because the cluster needs a versioned map pushed to every shard before a client
has any use for one. D7's remaining steps — per-shard ports, sub-pools, client-side routing — stay
where they are, ranked behind a measurement.

It is not a roadmap with dates. The [milestones](milestones.md) are an order with reasons, and the
reasons are the part worth reading.

## Related

- [TODOs — Distribution](../appendix/todos.md#distribution) and
  [Rebalancing](../appendix/todos.md#rebalancing) — the two entries this part grew out of, kept in
  place
- [Direction](../direction/overview.md) — the chapter this one is modelled on, and D7 in particular
- [Partitioning and the Tablet Map](../architecture/partitioning.md) — the map every page here
  makes authoritative, movable and replicated
- [items 11, 12, 37](../appendix/resolved/tablet-ring.md) — why a tablet map and not a hash ring,
  and the paragraph that deferred the replica set until "write fan-out, quorum acknowledgement,
  read repair and consistency levels" were the actual work. They are, now
- [Introduction — What Shoal is not](../introduction.md#what-shoal-is-not) — the five bullets this
  part exists to strike through
