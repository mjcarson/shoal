# C4. The replicated tablet map

## Context

The map tells routers where a tablet might be served and tells the rebalancer its intended
placement. It does not replace the tablet's own consensus configuration or elect a primary.
Separating these facts prevents a stale map or a metadata-majority decision from losing writes.

## What exists today

`Ring` (`shoal-core/src/server/ring.rs`) stores 4096 `u16` shard assignments, derived from shard
count at startup. `tablet_of` uses the partition hash's high twelve bits. `Topology` and
`STALE_TOPOLOGY` are reserved protocol surfaces; [D7](../direction/shard-aware-routing.md)
describes the client half. Assignment is shared across all tables today.

## The design

### The value widens

The following is a logical schema, not a fixed wire encoding:

```rust
struct TabletId { table: TableId, range: u16 }
struct ShardAddr { node: NodeId, shard: u16 }
struct TabletPlacement {
    desired_rf: u16,
    committed_config: ConfigRef,
    voters: Vec<ShardAddr>,
    learners: Vec<ReplicaProgress>,
    leader_hint: Option<LeaderHint>,
    transition: Option<TransitionId>,
}
```

A `ReplicaProgress` distinguishes planned, installing, catching up, ready and quarantined copies.
A `LeaderHint` carries a term and address, but current authority is checked by the data group.
Configuration ids, leader terms, snapshot generations and control-plane topology versions are
separate types. “Has a copy”, “can vote”, “can count toward durability” and “can serve this read”
are not interchangeable booleans.

### The map is Raft state

Embedded control-plane Raft commits member records, placement policy and desired transitions.
The rebalancer drives a tablet's data-protocol configuration changes and records their completion
back in control-plane state. Those two groups are not one atomic transaction. C8's persisted
transition id, expected configuration and idempotent reconciliation resolve either-side crashes.
A cached map can lag a completed data transition; the tablet configuration remains authoritative.

Stable table identity is part of the schema contract. Initially each table has 4096 logical
tablets, but may share a compact placement template with others. Per-table progress and leadership
remain independent. Q13 measures the resulting group/map count at realistic table counts; a
coarser grouping is an explicit design alternative with migration and hotspot tradeoffs.

Bootstrap has an explicit initialization state. For desired RF=3, first establish ready copies
on three distinct nodes and initialize that data configuration before accepting default writes.
A single-node control plane may bootstrap alone and expose admin/readiness without pretending
its data has a three-copy quorum. An explicit RF=1 deployment can serve immediately. A later
RF change is a configuration transition, never inferred from how many nodes currently answer.

Placement respects distinct nodes and capacity constraints. At N=RF every node necessarily holds
every tablet: shard count cannot yield a 2:1 distribution of replica bytes across those nodes.
Within a node distribute work among its shards; with N>RF use C8's capacity-aware assignment.
Per-table RF overrides are admitted only once their configuration transitions are supported;
unimplemented overrides are rejected with an actionable message.

### Every shard holds an Arc of the current map

Push immutable map snapshots or versioned deltas to shards. An initial full snapshot is simplest;
coalesce updates and provide full resynchronization when a delta is missed. Routing never asks
the control plane synchronously. Installation swaps a complete validated version between
messages, not partially updated entries. Publish leader hints separately when useful so frequent
elections do not rewrite an entire table placement map to every client.

### Pushed to clients

Expose topology for observability and reconnect/routing. Clients can initially record it without
shard-aware routing. Advertise both peer and client endpoints, including address changes, rather
than asking clients to dial peer ports. Define reachability/private-address behavior in Q11.
Full-map fanout cost scales with tables × tablets × subscribers × transition rate; it is measured,
not described as unconditionally negligible. Bound send queues and coalesce obsolete versions.

### Staleness, on servers too

A stale request can be forwarded only with a bounded hop count and original deadline, ideally
to a demonstrably newer routing version. Forwarding through two stale nodes must not loop.
Otherwise return a structured not-leader/stale-topology response with a hint and allow bounded
refresh. A removed or dead source cannot be required to forward forever. Write retries retain
the same operation identity, and a redirect cannot imply a previously accepted command was absent.

After a replica is dropped, retained source files are for cleanup/recovery grace. They are not
current data and are not read-eligible merely because they still exist. C6 determines eligibility.

### Persistence, and what a restart does

Control-plane snapshots persist placement intent. Each tablet also persists its own configuration,
term/vote, checkpoint and log matching evidence. Node restart reconciles the two before serving.
A stale storage marker/topology version does not prove data freshness. Removed nodes cannot
bootstrap old local files as a new authoritative group.

Keep `ShardCountMismatch` until M9c implements durable local rehoming of logs, archives and
metadata, including files of shards that no longer run. A per-tablet index helps discovery; it is
not by itself a recovery executor for those files.

## Alternatives rejected

The initial draft's `SetPrimary` and map-only `AddReplica`/`DropReplica` authority are superseded
by embedded data elections and configuration transitions. Whole-map pushes are an initial
implementation choice, not a reason to omit scale budgets or a resync path. Per-table placement
templates can save memory without sharing unrelated log sequences.

## What it costs

More metadata than the original 8 KiB ring, multiplied by table and group count. Measure actual
Rust and serialized sizes, not the sum of field widths alone. During migration both configurations
and transfer progress exist. A stale route adds a hop or refresh, bounded by deadline and attempts.

## What it breaks

`Ring`, `find_shard`, shard-local routing indices and existing topology sketches change. Table
identity becomes part of the storage/wire contract. A topology version alone is no longer used
as a durability or leadership generation. Initial cluster readiness becomes explicit.

## Invariants to uphold

- Placement intent changes by control-plane commit; voting authority changes by data consensus.
- Distinct-node replica placement and desired RF are enforced separately from current availability.
- Installing learners do not vote or satisfy durability/read readiness.
- Routing refresh is bounded; accepted operations retain their identity across redirects.
- Map/delta installation is atomic per version and supports resynchronization.

## Prerequisites

[C3](membership.md), [C13](protocol.md), [C2](transport.md). Bootstrap and map land in M3;
replicated data configurations in M4; migration reconciliation in M9a.

## How it would be measured

Routing microbenchmarks, topology updates to realistic client counts, memory and idle CPU versus
table/tablet counts. Include a full rebalance with many transitions in [C10](performance.md).

## Acceptance tests

| Test | Asserts | Milestone |
| --- | --- | --- |
| `map_versions_install_atomically_and_resync` | Missed delta causes full refresh; no partial placement is visible | M3 |
| `table_ids_and_streams_are_stable_across_restart` | Distinct tables share placement only, not identity or progress | M3 |
| `placement_respects_distinct_nodes_and_feasible_capacity` | RF constraints hold; at N=RF each node holds every tablet; N>RF uses feasible weights | M4 |
| `client_receives_topology_with_client_endpoints` | Initial topology and subsequent coalesced updates identify reachable client endpoints | M3 |
| `stale_routes_terminate_without_duplicate_writes` | Stale A↔B route, absent old owner and repeated redirects complete or error within budget | M9a |
| `data_configuration_outlives_stale_placement_hint` | Crash between data config commit and metadata completion cannot reactivate an old configuration | M9a |

## Related

[C8](rebalancing.md), [C5](replication.md), [C13](protocol.md),
[Partitioning](../architecture/partitioning.md), [D7](../direction/shard-aware-routing.md).
