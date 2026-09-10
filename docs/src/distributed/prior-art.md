# C12. Lessons from other clusters

## Context

[D9](../direction/prior-art.md) did this for the client: per system, what to copy and what not to,
so the recommendations on the other pages could be checked against something other than argument.
This page does it for the cluster. The systems overlap — Scylla, Cassandra, FoundationDB and
Dragonfly appear on both — but the lessons are different ones, because the questions are: who
orders writes, who decides membership, what happens when a node dies, and how a tablet moves.

Each entry says **what to copy** and **what not to**, and the second matters more. Every system
here is older and more general than Shoal, and carries compromises Shoal has not made and should
not make by imitation.

## The systems

### ScyllaDB

The closest analogue, again, and the source of more of this part than any other system. Tablets
rather than vnodes, with ownership stored per tablet in a system table; topology and schema under
Raft since 5.x, with membership moved *into* the Raft group rather than gossiped beside it; a
rebalancer that moves one tablet per `(source, destination)` pair at a time; and a node that is
down keeps its tablets until an operator removes it.

**Copy:** all of that. [C3](membership.md) is Scylla's topology-over-Raft; [C4](tablet-map.md) is
its tablet table; [C8](rebalancing.md#the-rebalancer) is its one-move-per-pair rule; and its
refusal to auto-remove is R3's default.

**Do not copy: the leaderless data path.** Scylla writes to every replica from any coordinator and
resolves conflicts on read by timestamp — per cell, because Cassandra's data model stores a
timestamp per column. That works because a Scylla update *is* a set of cells with timestamps.
Shoal's update is a struct of changed fields with no timestamp, and last-writer-wins over it at
row granularity diverges ([C5](replication.md#why-a-primary)). Scylla's model is the right one for
Scylla's data model, and Shoal has a different one. What Shoal gives up is writes that never
pause; what it keeps is a replica that is always a prefix of one log.

Also do not copy the `system.peers`-style discovery for clients; Scylla itself pushes, and
[C4](tablet-map.md#pushed-to-clients) pushes.

### Kafka

The closest analogue to [C5](replication.md), and the one most people forget is a replicated log.
A partition has a leader; the leader appends and fans out to followers; a write is acknowledged
at `acks=all` only when every in-sync replica has it; the in-sync set shrinks when a follower
falls behind by `replica.lag.time.max.ms` and grows when it catches up; and a new leader is
chosen from the in-sync set, which is why it holds every acknowledged write.

**Copy:** the shape. A primary that orders and fans out, a quorum that counts only replicas that
are keeping up, and a new primary chosen from those — that is C5 and [C7](failover.md#when-a-primary-is-down)
with Kafka's names changed. Kafka's `min.insync.replicas` is `Quorum`'s `needed`, and its
refusal to acknowledge below it is `Unavailable`.

**Do not copy: `unclean.leader.election.enable`.** Kafka lets an operator choose availability
over durability by electing a leader that is *not* in sync, losing acknowledged writes to get the
partition back faster. Shoal's `primary_failover_after` is the same trade-off pushed to a safer
place: the wait is configurable, the choice of primary is not. Kafka's knob is the cautionary
tale on [C7](failover.md#the-window-and-what-a-client-sees), and its default of *off* is the
evidence.

Also do not copy the ZooKeeper era. Kafka spent a decade with membership outside the system it
governed and rebuilt it inside (KRaft); [C3](membership.md) starts inside.

### Kudu, TiKV and CockroachDB

Three systems with a leader per range and Raft per range — multi-Raft. Each range (region, tablet)
is its own consensus group; a write is a Raft proposal; the leader holds a lease that lets it
serve reads without a round trip; and a node runs thousands of groups, whose heartbeats are batched
per node pair to make that affordable.

**Copy: the lease and the epoch.** [C6](reads.md#three-read-levels)'s `Primary` read is
CockroachDB's leaseholder read at the coarsest granularity that works; [C5](replication.md)'s
`epoch` is Kudu's term and TiKV's `conf_ver`/`version` pair, used the same way — a stale leader's
messages are refused by a number comparison at the top of every handler.

**Do not copy: Raft per tablet.** 4096 groups on three nodes is 4096 elections, 4096 heartbeats
per interval per pair, and a batching layer — TiKV's "Raft batch system" — that is most of the
transport. Every one of these systems built that layer because a per-range vote on every write
was the price of linearizability across ranges, which Shoal does not offer. One Raft group for
the control plane, appointing primaries that replicate without a vote per write, is what
[C5](replication.md#alternatives-rejected) chose, and this is where its cost was measured by
others.

### Cassandra

The lineage Scylla comes from, and the source of two mechanisms Shoal takes and one it does not
need.

**Copy: the phi-accrual failure detector.** [C3](membership.md#failure-detection) is Cassandra's
detector — Hayashibara's, adopted in 2009 and never replaced — with the same default threshold
of 8. And **tombstone GC grace**: a deleted row in Cassandra is a tombstone for `gc_grace_seconds`
so that a replica that missed the delete does not resurrect the row at the next repair.
[C8](rebalancing.md#a-move)'s `drop_grace` on a moved tablet is the same idea for a coarser object.

**Do not copy: gossip for membership.** Cassandra's gossiper is eventually consistent about who is
in the ring, which is the one fact that must not be, and its long history of "schema disagreement"
and "ghost node" incidents is the reason Scylla moved it under Raft. Nor **hinted handoff**: a
coordinator stores a hint for every write a down replica missed, because a coordinator is not
necessarily a replica. In Shoal the primary is a replica and its log is the hint
([C7](failover.md#no-hinted-handoff)).

### MongoDB

A replica set with one primary, elected by the set, with an `electionTimeoutMillis` (default ten
seconds), a heartbeat interval (two seconds), and `w: majority` as the write concern that
survives a failover. Reads default to the primary; `readPreference: nearest` is `One`.

**Copy: the semantics of `w: majority`** — a write acknowledged at majority survives the loss of
any minority, and a new primary is chosen from those that have it — and the honesty about the
alternative: MongoDB documents that a `w: 1` write may be rolled back on failover, in a page
called *Rollbacks During Replica Set Failover*, and keeps the rolled-back documents in a file for
the operator. [C7](failover.md)'s `a_one_write_may_be_lost_and_the_page_says_so` is that page as
a test.

**Do not copy: the election.** MongoDB's replicas vote among themselves; Shoal's control plane
appoints, with the membership group's majority behind it, and the tablet does not need to hold an
election of its own. Nor the ten-second default: MongoDB's timeout is set for WAN replica sets,
and [C1](node-identity.md#the-cluster-block)'s five seconds is set for a datacenter.

### Aurora

Six copies across three availability zones, a write quorum of four and a read quorum of three,
and a storage layer that repairs itself by gossip among the six. The canonical "quorum is not a
majority of a replica set, it is a design" example.

**Copy:** the framing that a quorum is chosen for what it survives, not derived from RF. Shoal's
`Quorum` is `RF/2 + 1` because that is what makes any two quorums intersect, and
[C5](replication.md#what-the-client-is-promised) says what it survives in those terms.

**Do not copy:** the six-way, three-zone layout, or the separation of storage from compute that
makes it possible. Aurora's replicas hold *pages* and its compute holds no state; Shoal's replicas
are shards with logs, and a shard is compute. The problem Aurora solves — a fleet of stateless
databases in front of one replicated volume — is not the one this part is solving.

### DragonflyDB

The counterweight, again. Dragonfly's cluster mode is Redis Cluster's — hash slots, one primary
per slot, replicas that replay the primary's journal, no consensus of any kind, and a control
plane that is *external*: an operator, or a tool, edits the slot map and pushes it.

**Copy: replicas that replay the primary's journal.** That is exactly [C5](replication.md), and
Dragonfly is the evidence that a thread-per-core engine can replicate that way without a lock on
the data path — its journal is per shard, its replicas apply per shard, and the fan-out is a
socket write from the shard that owns the data.

**Do not copy: the external control plane.** Dragonfly's answer to "who edits the map" is "not
us", which is a legitimate answer for a cache and not for a store that promises a quorum.
[C3](membership.md) puts the map under Raft precisely so that the cluster can edit it while an
operator is asleep.

### FoundationDB

Not a model for the data path — its transaction layer solves a problem Shoal does not have — but
the model for two things.

**Copy: the testing posture.** Deterministic simulation is the reason FoundationDB can claim
correctness under partition and process death, and [C11](testing.md) says plainly that Shoal
cannot have it, because glommio and io_uring are not simulable without a runtime abstraction that
does not exist. What it copies instead is the *discipline*: every fault injected, every
acknowledged write ledgered and checked, every race given repetitions. And **the coordinator
epoch** — FoundationDB's recovery increments an epoch that every old process's messages fail to
match, which is `epoch` on every page here.

**Do not copy:** the architecture. Proxies, resolvers, log servers and storage servers solve
distributed transactions, and this part builds none.

## The comparison

| System | Data path | Membership | Failover | A node that is down | A tablet moves by |
| --- | --- | --- | --- | --- | --- |
| **Shoal, proposed** | Primary per tablet, ordered replication, quorum ack | One Raft group | Control plane appoints the highest replica after a timeout | Keeps its tablets; primaries move | Follower first, snapshot + tail, one per pair |
| **ScyllaDB** | Leaderless, timestamp LWW per cell | Raft (since 5.x) | None needed — no leader | Keeps its tablets | Streaming, one per pair, under Raft |
| **Kafka** | Leader per partition, ISR quorum | KRaft | Controller picks from ISR | Leaves the ISR | Reassignment by the controller |
| **Kudu / TiKV / CockroachDB** | Raft per range, leaseholder reads | Their own Raft (master / PD / meta ranges) | Range Raft election | Replicas re-elect | Raft membership change per range |
| **Cassandra** | Leaderless, LWW per cell | Gossip | None needed | Keeps its tokens; hints accumulate | Streaming, token ranges |
| **MongoDB** | Primary per replica set | The replica set votes | Election, ~10 s | Falls behind; may roll back on return | Chunk migration by the balancer |
| **Aurora** | 4-of-6 write quorum over pages | Internal | Storage self-heals | A copy is repaired by gossip | Not applicable |
| **DragonflyDB** | Primary per slot, journal replay | External tool | External tool | Nothing happens | Slot migration, external |
| **FoundationDB** | Transaction layer over log + storage | Coordinators | Epoch bump, full recovery | Repaired by the data distributor | Data distributor |

Two things stand out.

**Every system with a leader per unit and no per-unit consensus — Kafka, MongoDB, Dragonfly —
puts the choice of leader somewhere with a majority behind it**: Kafka's controller, MongoDB's set
vote, and Dragonfly's operator. Shoal's control plane is the first of those with the second's
guarantee, and it is the row's design in one sentence.

**Nobody rebalances on a node being down.** Not one row moves data because a node stopped
answering. Every one of them either keeps the node's assignment until told otherwise or has no
assignment to keep. R3 is not a preference; it is the industry's unanimous answer, and
`auto_remove_after` is the knob every one of them has under some name and every one of them ships
off.

## Related

- [D9. Lessons from other databases](../direction/prior-art.md) — the client-side lessons, and
  the same systems from the other side
- [C3](membership.md), [C5](replication.md), [C7](failover.md), [C8](rebalancing.md) — the four
  pages this one is checked against
- [items 11, 12, 37](../appendix/resolved/tablet-ring.md) — Shoal's own vnodes-to-tablets
  migration, which is Scylla's
