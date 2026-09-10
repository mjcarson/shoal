# C8. Adding, removing and rebalancing nodes

## Context

This is the page that meets **R1** and **R2** — a node is easy to add and remove, and removing one
rebalances the cluster — and the one where **R3**'s other half lives: what happens when a node
that was merely down is finally declared gone. It is also the page [TODOs](../appendix/todos.md#rebalancing)
has been waiting for, because its second half, "key storage by tablet rather than by shard", was
deliberately not built until the streaming protocol existed. It exists now, on
[C7](failover.md#a-returning-node), and this page decides the storage question with it in hand.

## What exists today

**Shard count is part of the on-disk format.** Intent logs are `Shard-N-active`, archive maps are
`maps/Shard-N`, and a directory written by one count is refused by another
(`ShardCountMismatch`, [Partitioning](../architecture/partitioning.md#limitations)). [TODOs —
Rebalancing](../appendix/todos.md#rebalancing) names the two pieces that were needed and the order:

> **Persist the tablet assignment.** … The map has to become durable state before it can become
> editable state.
>
> **Key storage by tablet rather than by shard.** … what the layout should be depends on how
> migration streams data, and that protocol does not exist yet. Building the layout first risks
> building the wrong one and migrating twice.

The first is [C4](tablet-map.md). The second is decided below, and the answer is not the one the
entry expected.

**`Ring::add` warns and does nothing** (`ring.rs:121-133`); there is no rebalancer, no notion of
a node leaving, and no way to find files belonging to a shard that no longer exists.

**The archive map is keyed by partition** (`ArchiveEntry { key, archive, offset, size }`,
[Archives and the Archive Map](../storage/archives-and-map.md)), and a partition's key names its
tablet in its top twelve bits. A tablet's partitions are therefore enumerable from the map with
one comparison per entry, today, without a layout change.

## The design

### The rebalancer

A task of the **leader's** control plane ([C1](node-identity.md#the-control-plane-thread)),
woken by a membership change and by a timer, that holds a plan and executes it one step at a
time. It never runs on a follower's control plane, so there is one plan.

**The plan** targets three things, in priority order:

1. **Every tablet has `RF` replicas on `RF` distinct `Up` or `Leaving` nodes.** Under-replicated
   tablets — after a removal, after a bootstrap with fewer nodes than `RF` — are widened first.
2. **Tablets per shard are even, weighted by nothing** — a shard is a core, and a core on a small
   node is a core. A node with twelve shards holds twice the tablets of one with six because it
   has twice the shards, which is the same rule bootstrap used ([C4](tablet-map.md#the-map-is-raft-state)).
   Within ±1 per shard is done.
3. **Primaries per node are even**, within ±1. Off by default (`cluster.rebalance_primaries:
   false`), because a primary change pauses a tablet's writers for a Raft round trip and an
   operator who has just recovered a node may want to choose the moment.

**One move per `(source, destination)` pair at a time.** Scylla's rule, for Scylla's reason: a
move is a stream, a stream costs the source's disk and the destination's, and `N` simultaneous
streams into one node is how a rebalance takes the node it was helping offline. A three-node
cluster rebalancing toward a fourth runs at most three streams, one from each existing node.

### A move

```
 leader                          source (A3, current replica)      destination (D1, new replica)
 ──────                          ─────────────────────────────     ─────────────────────────────
 1. AddReplica {tablet, D1} ────▶ (map: followers += D1, marked catching-up)
                                  StreamBegin {tablet, at_seq} ───▶ install snapshot
                                  StreamPartition × n ───────────▶
                                  StreamEnd ─────────────────────▶
                                  Replicate (tail, from at_seq) ─▶ apply; report lag
 2. (leader sees lag 0 in D1's heartbeat)
    if A3 was primary: SetPrimary {tablet, D1 or another follower, epoch+1}
 3. DropReplica {tablet, A3} ───▶ (map: followers -= A3)
                                  A3 keeps the data for `drop_grace` (default 60s), then tombstones it
```

Three log entries, three map versions, and at every version the tablet has at least `RF` replicas
that hold its data. The destination is a **follower first** — it receives the stream that
[C7](failover.md#a-returning-node) built, which is why that stream is built once — and it becomes
a candidate for primary only once its reported lag is zero. A move of a tablet whose source was
its primary is two moves in one: the replica moves, and the role moves separately, because
moving the role is a pause and moving the data is not.

**The grace on the source** is [D7 §5](../direction/shard-aware-routing.md#5-staleness-which-is-what-makes-it-safe)
applied on the server: a coordinator on an older map may still forward to `A3`, and `A3` forwards
on ([C4](tablet-map.md#staleness-on-servers-too)) — but a *read* at `One` that lands there during
the grace can be answered there, because the data is still current, and a forward that would
have been a hop is a hit. After the grace, `A3` tombstones the tablet's partitions in its archive
map, and compaction reclaims them as it reclaims any pruned partition
([Compaction](../storage/compaction.md#3-apply)).

### Storage stays keyed by shard

The TODO expected intent logs and archive maps to be renamed by tablet — `intents/<tablet>-active`
— so that moving a tablet is moving a file. With the stream in hand, the layout question has a
different answer, and this is where the entry was wrong in a way worth recording:

**Moving a tablet is not moving a file, on either end.** The destination does not want the
source's log — it wants the tablet's *state* at a seq, which is the partitions, and then the tail.
A per-tablet log on the source would still have to be replayed into partitions before streaming,
because a log holds updates that need their base partition. And the source does not want to move
a file away — it wants to keep serving during the grace, and then forget. Per-tablet files buy
nothing the stream needs.

What they cost is 4096 logs per table per node, 4096 `StreamWriter`s each holding a DMA buffer
and a write-behind queue, and an fsync per log per group commit. The single intent log per shard
per table is what makes group commit work ([The Intent Log](../storage/intent-log.md#group-commit));
cutting it 4096 ways would undo that on every write to pay for a rebalance that happens once a
month.

So: **intent logs and archive maps stay per shard.** What changes is that every record already
carries its tablet ([C5](replication.md#the-intent-record-format-2)), and the archive map groups
its entries by tablet on load — a `BTreeMap<u16, Vec<u64>>` beside the `HashMap<u64, ArchiveEntry>`,
built once at startup from the top bits of every key and maintained on every insert and prune.
That index is what the snapshot stream walks, what a tombstone-the-tablet drop walks, and what a
restarting node uses to find data for tablets the map no longer assigns to it.

### Orphaned tablets

A node restarting into a cluster ([C4](tablet-map.md#persistence-and-what-a-restart-does)) may
hold data for a tablet whose replica set no longer names it — moved away while it was down, after
`auto_remove_after`. That data is **orphaned**: current as of the node's death, stale since. It is
not deleted on sight. The node reports orphaned tablets in its heartbeat; the rebalancer, if the
tablet is under-replicated (which after a removal it may be, since the removal was what moved it),
may choose this node as the destination and catch it up by log or snapshot from where it is —
cheaper than a fresh replica. If the tablet is fully replicated elsewhere, the orphan is
tombstoned after `drop_grace`. Either way the decision is the rebalancer's and the data waits for
it.

This is the TODO's "a way to discover files belonging to shards that no longer exist", answered
at tablet granularity: a shard that no longer exists left records tagged with tablets, and the
per-tablet index finds them.

### `ShardCountMismatch` retires

`Resources.cores` may now change between restarts. On startup, a node whose marker says `12` and
whose config says `8` starts eight shards, builds the per-tablet index across *all* files in the
directory — `Shard-8-*` through `Shard-11-*` included — reports every tablet those files hold as
orphaned-on-this-node, and the rebalancer re-homes them onto its eight live shards as moves
where the source and destination are the same node. The files of the vanished shards are
tombstoned once every tablet they held has been moved. The marker's `shards` becomes informational.

~~**Shard count is part of the on-disk format.** Changing `resources.cores` between restarts is
refused.~~ Superseded by this page when M9 lands; the refusal stays until then
([C4](tablet-map.md#persistence-and-what-a-restart-does)).

### Adding a node

[C3](membership.md#joining) puts it in the cluster with no tablets. The rebalancer sees a node
with zero tablets and a plan that wants them even, and starts moves — at most one per source at a
time — until it is within ±1. On a three-node cluster at RF=3 adding a fourth node means every
tablet goes from three replicas on three nodes to three replicas on four, so a quarter of every
node's tablets move: `AddReplica` to the new node, `DropReplica` from one of the old ones, per
tablet. The cluster stays at `RF` throughout.

### Removing a node

Two verbs, one path:

- **`Decommission`** ([C9](operations.md)): the node is `Up` and is asked to leave. It goes
  `Leaving`; the rebalancer moves every tablet off it — it is the source of every stream, at one
  per destination — and when it holds nothing, `Leave` commits and it is `Removed`. It serves
  throughout. This is the graceful path and it is R2.
- **`Remove`**, by an operator on a `Down` node, or by `auto_remove_after`: the node cannot be a
  source. It goes `Removing`; every tablet it held is under-replicated, and the rebalancer widens
  each from its *other* replicas — the stream's source is whichever `Up` replica has the highest
  stamp. When every tablet is back at `RF`, it is `Removed`.

A `Removed` node that comes back — the partition healed, the operator was wrong — is refused by
the handshake: its `NodeId` is in the log as `Removed`, and a removed node does not rejoin. Its
directory holds orphans and nothing else, and starting it from empty is a new node.

### `auto_remove_after`

When set, a node `Down` for that long is `Remove`d automatically. The page for the setting
([C1](node-identity.md#the-cluster-block)) says it is off by default, and this is why: the
rebalance it starts moves every byte the node held, from the surviving replicas, which are also
the replicas serving reads — and if the node was not dead but partitioned, it comes back to find
itself `Removed` and refused. That is the correct outcome for a node that was gone for thirty
minutes and a bad one for a network that was. The setting exists for the operator who runs an
unattended cluster and would rather be over-replicated for an hour than under-replicated
overnight; the default is for the one who would rather be paged.

## Alternatives rejected

**Per-tablet intent logs.** Above, at length: 4096 writers per table per node, group commit
destroyed, for a file move the stream does not want.

**Moving the log instead of streaming partitions.** The destination would replay the source's
whole log history for the tablet — which was compacted away — or the source would stream the
partitions anyway. Streaming partitions plus a tail is the only shape that works after the first
compaction.

**Moving several tablets per pair at once.** Faster rebalances and a source whose disk queue is
shared between a stream and its own writes. One per pair is the conservative rule and it is
Scylla's; a `max_concurrent_moves_per_pair` knob is filed, not built.

**Deleting a moved tablet's data on the source immediately.** Turns the staleness window into a
window of forwards instead of hits, and makes an operator's mistake unrecoverable a second sooner.
`drop_grace` is cheap.

**Deleting orphans on sight.** They may be the cheapest source for a re-replication that is
about to happen.

**Letting a `Removed` node rejoin.** Its data is stale by an unknown amount and its identity is
in the log as gone; a rejoin would have to reconcile every tablet it held and would be indistinguishable
from a partition healing at the worst moment. A removed node is a new node.

**Rebalancing on `Down`.** R3. Said everywhere, and said once more here because this is the page
that would have been tempted.

## What it costs

- **A stream per move**: the tablet's partitions, then its tail, over one peer connection. Bounded
  by one per pair.
- **A per-tablet index in every archive map**: 4096 entries of a `Vec<u64>` of partition keys,
  which is the same keys the map already holds, indexed a second way. Memory proportional to the
  partition count, maintained on every insert and prune.
- **`drop_grace` worth of disk** per moved tablet on the source.
- **A pause per primary move**, when `rebalance_primaries` is on.

## What it breaks

- **`ShardCountMismatch`** and its test, and the sentence about it in `configuration.md`,
  `partitioning.md` and `introduction.md`. All struck through when M9 lands.
- **The archive map's load path** builds a second index, which is startup time proportional to the
  entry count.
- **"Nothing is shared between shards except channels"** ([Architecture Overview](../architecture/overview.md))
  gains an exception during a same-node re-home: a shard reads the files of a shard that no
  longer exists. Read-only, at startup, before any shard serves.

## Invariants to uphold

- **A tablet never has fewer than `RF` replicas holding its data during a move.** `AddReplica`
  before `DropReplica`, always, and `DropReplica` only after the destination reports lag zero.
- **One move per `(source, destination)` pair at a time.**
- **The primary role moves by `SetPrimary`, never as a side effect of `DropReplica`.** Dropping
  the primary's replica without first appointing another is a tablet with no primary.
- **A `Removed` node never rejoins under its `NodeId`.**
- **A `Down` node is never a stream source, and never a stream destination.**
- **Orphaned data is tombstoned only by the rebalancer's decision, after `drop_grace`**, never on
  startup.
- **Intent logs and archive maps stay per shard.** The per-tablet index is a view over them, not a
  layout.

## Prerequisites

[C4](tablet-map.md) for the map entries; [C7](failover.md) for the stream and the catch-up;
[C3](membership.md) for `Leaving`, `Removing`, `Removed` and `auto_remove_after`'s trigger;
[C5](replication.md) for the tablet id in every record. The retirement of `ShardCountMismatch`
waits for all of it.

## How it would be measured

`macro/cluster/rebalance/{add,decommission,remove}`: the reference mixture at `Quorum` on three
nodes while a fourth is added, decommissioned, or removed-while-down, reporting client-visible
error count (expected: zero for add and decommission), p99 latency during the move against
before it, and seconds to even. Like [C7](failover.md#how-it-would-be-measured)'s arm it reports
a duration and a delta, never a single throughput that averages the move away. On the emulated
cluster the streams share one device with the workload, and the page reading the number has to
say so ([C10](performance.md#emulating-a-cluster-on-one-machine)).

## Acceptance tests

| Test | Asserts | Milestone |
| --- | --- | --- |
| `adding_a_node_under_load_causes_no_client_error` | Add a fourth node during the reference mixture; zero errors; tablets even within ±1 per shard within a bound; every tablet at RF | M9 |
| `a_move_adds_before_it_drops` | Watch the `Topology` sequence for one tablet: `AddReplica`, then (maybe) `SetPrimary`, then `DropReplica`; at every version the set is ≥ RF | M9 |
| `one_move_per_pair_at_a_time` | With a fourth node added, at most one in-flight stream per source (span attributes) | M9 |
| `decommission_drains_and_serves` | Decommission under load: zero errors, the node answers reads until it holds nothing, then `Removed` | M9 |
| `remove_of_a_down_node_re_replicates_from_survivors` | Kill a node, `Remove` it: every tablet it held is widened from the highest-stamp survivor; back at RF | M9 |
| `a_removed_node_cannot_rejoin` | Restart the removed node from its directory: refused by name; from empty: joins as a new node | M9 |
| `a_stale_forward_during_a_move_is_answered` | Pin a coordinator's map (test hook) before a move: its forwards land on the source during the grace and are answered; after it, forwarded on; never refused | M9 |
| `a_dropped_tablet_is_kept_for_the_grace_then_tombstoned` | The source's archive map holds the tablet's keys until `drop_grace`, then prunes them | M9 |
| `auto_remove_after_removes_and_rebalances` | Set it to 5s in test config; kill a node; it is `Removed` and every tablet is back at RF | M9 |
| `down_shorter_than_auto_remove_after_moves_nothing` | The R3 test, restated here: replica sets unchanged | M9 |
| `orphans_are_reused_as_a_source` | Set `auto_remove_after` short; kill a node; bring it back **after** it is `Removing` and **before** the re-replication of its tablets finishes: the rebalancer chooses its orphans as a destination and catches them up rather than streaming fresh copies (span attributes name the path) | M9 |
| `changing_cores_re_homes_tablets_on_the_same_node` | Restart a node at fewer cores: it starts, orphans are re-homed onto its live shards, the old files are tombstoned after | M9 |
| `shard_count_mismatch_is_gone` | The error variant no longer exists; the storage-meta test that asserted it now asserts the re-home | M9 |

## Related

- [TODOs — Rebalancing](../appendix/todos.md#rebalancing) — the entry this page answers, and the half it answers differently
- [C7. Failover](failover.md) — the stream, built there and reused here
- [C4. The tablet map](tablet-map.md) — the entries a move edits, and the staleness rule
- [C3. Membership](membership.md) — `Leaving`, `Removing`, `Removed`
- [C1](node-identity.md#the-cluster-block) — `auto_remove_after`, and why it is off
- [Archives and the Archive Map](../storage/archives-and-map.md) — where the per-tablet index lives
- [Compaction](../storage/compaction.md#3-apply) — how a dropped tablet's space comes back
- [Partitioning — Limitations](../architecture/partitioning.md#limitations) — the four bullets this page strikes through
- [C12 — ScyllaDB](prior-art.md#scylladb) — one move per pair
