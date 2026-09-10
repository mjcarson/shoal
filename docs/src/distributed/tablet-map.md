# C4. The replicated tablet map

## Context

The tablet map is the one data structure in Shoal that was built for this page before this page
existed. [Items 11, 12 and 37](../appendix/resolved/tablet-ring.md) replaced a consistent-hash
ring with a stored assignment precisely so that a tablet could one day be moved, replicated, and
owned by something other than arithmetic — and then deferred all three, because "write fan-out,
quorum acknowledgement, read repair and consistency levels are the actual work". This page widens
the stored value from a shard index to a replica set, makes the map the cluster's agreed state
rather than each shard's derivation, and pushes it to everything that routes.

## What exists today

```rust
pub struct Ring {
    /// The shard that owns each tablet, indexed by tablet id
    tablets: Vec<u16>,
    pub shards: Vec<ShardInfo>,
}
```

`shoal-core/src/server/ring.rs:35`. 4096 entries — `TABLET_BITS = 12` (`ring.rs:25`) — of two
bytes each, 8 KiB, built on every start as `tablet % shard_count` (`ring.rs:72`) and looked up as
a shift and two indexed loads (`ring.rs:140`).

Three facts about it carry into this page unchanged, and [Partitioning](../architecture/partitioning.md)
argues each:

- **Ownership is stored, not derived.** "You cannot express an ownership you did not derive"
  (`partitioning.md`, *Why the owner is stored rather than computed*). The cost, named there, is
  that "something has to be authoritative about the table." This page is that something.
- **The tablet id is the key's high bits**, so a tablet can split by consuming one more bit
  without disturbing any other (`partitioning.md`, *Why the id comes from the high bits*). Nothing
  here splits a tablet; nothing here makes it harder.
- **Every shard agrees because every shard derives the same map from the same number**
  (`partitioning.md`, *Why every shard agrees*). That argument ends here, and the page has to
  replace it with a better one.

Two limitations it names are what this page removes: "Tablet assignment is derived, not
persisted" and "No replication. One shard, one copy."

**The frame to push it in is reserved.** `MessageType::Topology = 9` exists, documented as "the
shards in this cluster and what each one owns — reserved for shard aware routing"
(`shoal-proto/src/shared/protocol.rs:131`), and `Flags::STALE_TOPOLOGY` is bit 1
(`protocol.rs:223`). Both are unwired. [D7](../direction/shard-aware-routing.md#2-the-topology-pushed)
designed the push and recommended, as step 1, building the frame with the client "doing nothing
but recording it" — pure observability.

**Routing reads the map in one place**, `route_archived` (`shoal-core/src/server/routing.rs:193`
and the per-query impls), through `group_by_shard`, which deduplicates on `mesh_id()`.

## The design

### The value widens

```rust
pub struct ShardAddr { pub node: NodeId, pub shard: u16 }

pub struct ReplicaSet {
    /// The replica that serializes this tablet's writes
    pub primary: ShardAddr,
    /// The other replicas, in no order that means anything
    pub followers: SmallVec<[ShardAddr; 2]>,
    /// Incremented every time `primary` changes; what fences a primary that was replaced
    pub epoch: u32,
}

pub struct TabletMap {
    pub version: u64,                       // the Raft log index that produced it
    pub tablets: Vec<ReplicaSet>,           // TABLET_COUNT of them
    pub nodes: BTreeMap<NodeId, SocketAddr>, // so a ShardAddr can be dialled without a second lookup
}
```

4096 × (18 + 2 × 18 + 4) bytes is about **240 KiB** at RF=3 — larger than the 8 KiB D7 counted on,
and still one frame, still small enough that pushing the whole map on every change is simpler than
any delta scheme. It stays true after tablet splitting doubles it.

**The replica set is the value, not a walk.** [TODOs](../appendix/todos.md#distribution) said it
best: on a token ring the replica set is a walk that skips vnodes belonging to a node already
chosen and filters by rack; with tablets it is simply the value. The rebalancer decides the
value ([C8](rebalancing.md)); routing reads it.

### The map is Raft state

`TabletMap` is a field of `ClusterState` ([C3](membership.md#one-raft-group-of-nodes)). Every
change to it is a committed log entry — `MoveTablet`, `SetPrimary`, or the bootstrap assignment —
and `version` is the index of the last one. Two shards on two nodes hold the same map if they
hold the same version, and the argument that they agree is Raft's, not a derivation's.

**Bootstrap assignment**, proposed by the bootstrap node's control plane when the cluster first
has enough members for its replication factor (or immediately, at RF=1, for a single node):
tablets are dealt round-robin over `(node, shard)` pairs weighted by each node's shard count, so a
node with twelve shards takes twice the tablets of one with six; each tablet's `RF` replicas are
on `RF` distinct nodes; and the primary role is dealt round-robin over nodes so every node is
primary for about `TABLET_COUNT / N` tablets. A cluster started with fewer nodes than its
replication factor assigns as many replicas as it has nodes and the rebalancer widens the sets as
nodes join — so a one-node cluster at `replication_factor: 3` is legal, serves, and is one copy
until it is not.

**One map for every table, in v1.** A table whose `replication_factor` overrides the cluster's
([C1](node-identity.md#the-cluster-block)) needs its own replica sets, which is a map per table —
what Scylla does, and what this page defers. Until then, `cluster.tables.<name>.replication_factor`
is accepted and refused at startup with a message naming this page, so the config shape is
settled before the mechanism is. This is a limitation and the first line of *What it breaks*.

### Every shard holds an `Arc` of the current map

The control plane broadcasts `ServerMsg::Topology(Arc<TabletMap>)` over the mesh
([C1](node-identity.md#the-control-plane-thread)) whenever the committed version changes; each
shard swaps its `Arc` between messages. Routing reads the map it holds and never waits for a
newer one. `Ring` is deleted and `Shard.ring` becomes `Shard.map: Arc<TabletMap>`; `find_shard`
becomes `find_replicas(partition) -> &ReplicaSet`, and every call site chooses a replica out of
the set by the consistency level in hand ([C6](reads.md#choosing-a-replica)) — for a write, always
the primary.

### Pushed to clients

D7's step 1, built here because the cluster needs it first. On every client connection, the
server sends a `Topology` frame after the handshake and again whenever the version changes, and
the client **records it and does nothing else** — a `Shoal::topology()` accessor for `shoalctl`
([C9](operations.md)) and no routing. The frame's payload is the rkyv archive of `TabletMap`; it is
a server-to-client frame with no query id, like `Auth`, read by the proxy and stored.

D7's later steps — per-shard ports, sub-pools, client-side routing — stay on D7's page at D7's
rank. What this page fixes is the frame's contents, which D7 left as "the shard count, each
shard's endpoint, the tablet→shard map, and a version": it is the replica sets, the node
addresses, and the version, because a client that one day routes will route to a *replica* and
needs to know which one is primary.

### Staleness, on servers too

[D7 §5](../direction/shard-aware-routing.md#5-staleness-which-is-what-makes-it-safe) is written
about clients and is exactly as true of coordinators: a shard routing on version 41 while a move
committed at 42 will forward a query to a shard that no longer owns the tablet. **A stale route
degrades to a forward, never to an error.** The receiving shard, holding version 42, forwards the
query on to the current owner and sets `STALE_TOPOLOGY` on its reply, which tells the coordinator
to expect a `Topology` it may already have received. A tablet that has moved twice forwards twice.
Nothing refuses.

For this to be safe, the *old* owner must still be able to forward — which means it must still
hold the map, and it does, because every shard holds the whole map. What it need not hold is the
data, and [C8](rebalancing.md#a-move) keeps a moved tablet's data on the source for a grace period
for a different reason: a forward is a hop, but a read that lands at the source during the grace
can still be answered there.

### Persistence, and what a restart does

The map persists as part of the Raft snapshot on every node. A node restarting alone — a single
node with no `cluster:` block — reads it back and serves. A node restarting into a cluster reads
its own `StorageMeta.topology_version` ([C1](node-identity.md#the-storage-marker-format-2)),
rejoins the group, receives the current map, and compares: every tablet it holds whose replica
set still names it is caught up by [C7](failover.md#a-returning-node); every tablet it holds that
no longer names it — moved away while it was down, after `auto_remove_after` — is orphaned data,
and [C8](rebalancing.md#orphaned-tablets) says what happens to it.

**`ShardCountMismatch` stays until C8.** The map says which `(node, shard)` holds a tablet, and a
node whose shard count changed has shards that hold tablets the map assigns to shards it no longer
runs. Until the rebalancer can re-home them, a changed count is still a refusal, and the reason
moves from "the files would be in the wrong place" to "the map names shards that do not exist".

## Alternatives rejected

**A map per table from the start.** Right eventually, and it is the only way per-table
replication factor is real. It multiplies the map's size by the table count and every push with
it, for a feature nobody has asked for yet. The config accepts the override and refuses it, so
the day it is built no config changes.

**Deltas instead of whole-map pushes.** A move changes one entry of 4096; pushing 240 KiB for it
looks wasteful. It is one frame a few times a minute during a rebalance and never otherwise, and
a delta scheme needs a version chain, a resync path when a delta is missed, and a test for each.
Aerospike and Scylla push whole maps ([D9](../direction/prior-art.md#aerospike)).

**Deriving the map from the member list, consistent-hash style.** Every node would agree with no
coordination, and no node could move one tablet. That is the ring this map replaced, and
[Partitioning](../architecture/partitioning.md#why-the-owner-is-stored-rather-than-computed) is
the argument against it; a cluster adds "and no node could choose a primary" to the list.

**Storing the map only on the leader and having shards ask.** A lookup per query on a Raft
round trip. The map is small, changes rarely, and is read on every query; it is pushed.

**Refusing a query routed on a stale map.** Turns every rebalance into a window of errors. This
is the Redis Cluster `MOVED` lesson D7 already took ([D9](../direction/prior-art.md#redis-cluster-and-resp3)).

## What it costs

- **240 KiB per node per map change**, over the mesh and over every client connection. Rare.
- **One `Arc` load per routed query** where there was a field access. Not measurable.
- **A `ReplicaSet` per tablet instead of a `u16`**: 240 KiB resident per shard instead of 8 KiB,
  because every shard holds its own `Arc` to the same allocation — so 240 KiB per node, not per
  shard.
- **A forward per stale route during a move**, bounded by the number of moves since the shard's
  version.

## What it breaks

- **Per-table `replication_factor` is accepted and refused** until a map per table exists.
- **`Ring`, `Ring::new`, `Ring::add`, `find_shard`** and every test that names them. `group_by_shard`
  survives with a `ReplicaSet` in place of a `ShardInfo`.
- **The argument that every shard agrees** (`partitioning.md`, *Why every shard agrees*) is
  replaced. A partial map is still never constructed — a shard has the whole of some version or
  nothing — but two shards may hold two versions, and the page has to say what happens then. It
  does, above.
- **`ShardCountMismatch`'s reason changes** and, after C8, the check is deleted.
- **The `Topology` frame's contents differ from D7's sketch**, and D7's page should be annotated
  when M3 lands.

## Invariants to uphold

- **The map changes only by a committed log entry.** No node edits its copy. A shard that
  believes a tablet moved and has not received a `Topology` saying so is wrong, and forwards.
- **A stale route is forwarded, never refused.** Every shard holds the whole map, so every shard
  can forward. A shard that refuses because "I do not own this" has broken every rebalance.
- **`epoch` increments on every `SetPrimary` and on nothing else.** [C5](replication.md) and
  [C7](failover.md) use it to fence, and a fence that moves for other reasons fences the wrong
  thing.
- **A tablet's replicas are on distinct nodes.** A replica set naming one node twice has RF−1
  copies and claims RF. The rebalancer never proposes one and the state machine refuses one.
- **The tablet id is still the key's high bits.** Nothing here touches `tablet_of`.
- **A shard holds the whole map or none.** Never a partial one — the property the derivation had,
  kept.

## Prerequisites

[C3](membership.md) for the group that holds it; [C1](node-identity.md) for
`topology_version` in the marker; [F10](../features/framing-and-protocol-evolution.md) for the
reserved frame and flag. D7's step 1 is *built* here, not depended on.

## How it would be measured

`routing/split_by_shard/*` and `routing/find_shard` ([F24](../features/routing-benchmarks.md))
already measure the lookup; the change from `Vec<u16>` to `Vec<ReplicaSet>` plus a
consistency-level choice is what they would show, and
[O39](../appendix/optimizations.md#o39-routing-a-multi-partition-get-is-quadratic-before-the-query-reaches-a-table)'s
quadratic term is unchanged by it. The push itself is measured by nothing and should not be — it
is rare and off the query path.

## Acceptance tests

| Test | Asserts | Milestone |
| --- | --- | --- |
| `every_node_holds_the_same_map_at_the_same_version` | The `Topology` admin reply is byte-identical across nodes | M3 |
| `bootstrap_spreads_tablets_by_shard_count` | Two nodes of 8 and 4 shards hold tablets 2:1 ±1 per shard | M3 |
| `a_replica_set_names_distinct_nodes` | For every tablet at RF=3 on three nodes, three different node ids | M4 |
| `primaries_are_dealt_evenly` | Primary count per node within ±1 | M4 |
| `a_client_receives_a_topology_frame_on_connect` | `Shoal::topology()` is `Some` after `new`, at the server's version | M1 |
| `a_client_receives_a_topology_frame_on_change` | After a `MoveTablet`, the client's version advances without reconnecting | M9 |
| `a_stale_route_is_forwarded_not_refused` | A shard pinned at an old version (test hook) forwards to the new owner; the reply carries `STALE_TOPOLOGY`; the rows are right | M9 |
| `a_one_node_cluster_at_rf_three_serves_one_copy` | Legal, serves, every replica set has one entry, widened when nodes join | M4 |
| `a_per_table_replication_factor_is_refused_by_name` | The startup error names this page | M4 |
| `the_map_survives_a_restart` | A single node restarts with the map it had, not a re-dealt one | M3 |
| `routing_benchmarks_still_run` | `routing/*` in the micro layer builds and runs against a `ReplicaSet` map | M3 |

## Related

- [Partitioning and the Tablet Map](../architecture/partitioning.md) — the page whose three arguments this one inherits and whose two limitations it removes
- [items 11, 12, 37](../appendix/resolved/tablet-ring.md) — why the map is a table and not a ring, and the replica set it deferred
- [TODOs — Rebalancing](../appendix/todos.md#rebalancing) — "persist the tablet assignment", which this is
- [D7 §2](../direction/shard-aware-routing.md#2-the-topology-pushed), [§5](../direction/shard-aware-routing.md#5-staleness-which-is-what-makes-it-safe) — the push and the staleness rule, built here for servers and clients at once
- [C5. Replication](replication.md), [C7. Failover](failover.md) — what `primary` and `epoch` are for
- [C8. Rebalancing](rebalancing.md) — what edits the map
- [F24. Routing benchmarks](../features/routing-benchmarks.md) — what measures the lookup
