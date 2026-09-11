# C8. Adding, removing and rebalancing nodes

## Context

A removed node causes rebalancing; a down node retains its assignments during grace. Migration
must preserve acknowledged operations even when writes continue and a leader changes mid-move.
The transfer mechanism and the placement policy are separate milestones, M9a and M9b.

## What exists today

Logs and archive maps are per shard and table. `ShardCountMismatch` protects this layout. Archive
entries name partition keys, so a per-tablet index can enumerate data, but existing compaction
and log retention know nothing about replicated configurations or resumable migration.

## The design

### The rebalancer

The embedded control-plane leader computes desired transitions and reconciles persisted progress.
A new leader resumes those records; it does not reconstruct a plan solely from “lag zero” reports.
Only one configuration/migration transition per logical tablet is active at a time. Repair,
removal, RF changes, same-node moves and leadership transfers must either cooperate with that
transition or wait. A plan is not an in-memory task whose cancellation undoes committed steps.

Placement priorities:

1. Restore the intended distinct-node/failure-domain replication level where a surviving data
   quorum and capacity permit safe reconfiguration.
2. Protect disk reserve, installation space and sustainable replication/apply capacity.
3. Balance measured bytes, write/read load and primary CPU pressure using explicit node/shard
   capacity weights; prefer minimal movement and hysteresis over exact tablet-count equality.
4. Transfer leadership separately where supported and worthwhile, using the data protocol.

At N=RF every node holds every tablet regardless of capacity weights. Unequal nodes can distribute
leadership and local shard work differently, but the slowest replica still needs to keep up with
its full write stream. Do not run a planner forever trying to reach an impossible 2:1 replica-byte
ratio. With N>RF, weights influence feasible replica placement. A single hot partition remains
indivisible by range splitting; expose that limit and measure its primary bottleneck.

### A move

The initial transition schema records tablet, operation id, expected old configuration, target
configuration, source/destination, snapshot id/boundary, phase and last completed data-config id.
Exact fields and Raft membership APIs are selected in Q1/Q2; these are the required semantics:

| Phase | Action and durable completion condition |
| --- | --- |
| Plan | Commit transition intent after verifying capacity, source eligibility and expected configuration |
| Add learner | Add destination as a non-voting learner through the data protocol; persist/report operation identity |
| Install and catch up | C7 atomic snapshot plus retained tail; learner never counts toward quorum merely because bytes arrived |
| Reconfigure | Execute the library's safe membership transition, including joint old/new quorum rules where required; drain/resolve old-configuration writes according to that protocol |
| Activate | Establish the new committed configuration and required applied barrier; use a proper leadership transfer if moving the primary |
| Publish | Record completed data configuration in control-plane placement; stale routers refresh/forward within bounds |
| Retire | Remove source eligibility, retain files for grace/references, then durably tombstone and reclaim |

The essential barrier is not a cached zero-lag report. New writes can arrive between that report
and DropReplica, and old-config requests can remain in flight. Membership transition rules must
ensure all acknowledged operations are represented in the new authoritative history before the
old copy is no longer needed.

Data and control commits can finish in either order around a crash. On restart reconcile the
recorded transition with the data group's actual committed configuration; retry idempotently or
finish publication. Never roll a completed data configuration backward to match a stale map.
A source/destination/control-leader failure at every phase is an acceptance test.

[etcd's learner design](https://etcd.io/docs/v3.5/learning/design-learner/) is useful background on
why catching up a replica must precede making it a voter. This is a protocol reference, not an
etcd service dependency. The selected embedded library's exact membership API and completion
rules must be linked in Q1 before implementing this state machine.

### Transfer budgets

Keep at most one stream per source/destination pair initially, and additionally enforce aggregate
limits per source, per destination and per node/device. Configure bytes/sec, concurrent transfers,
buffer bytes and minimum free disk space. Reserve space for old and new checkpoint generations,
retained WAL and concurrent foreground growth. Expose a blocked reason instead of repeatedly
restarting transfers when reserves are insufficient.

Snapshot streams use separate bounded lanes from queries, replication and control traffic.
Adapt/reduce background work when foreground tails or replica lag exceed thresholds; it must
still make progress under a documented supported load envelope. One stream per pair alone does
not prevent N peers overloading one destination.

### Storage stays keyed by shard

Keep shared physical WAL/group commit as the initial storage choice. Add per-tablet indices and
manifest/checkpoint ownership needed for atomic replacement and reclamation. Logical tablet
checkpoint/archive organization is independent of whether one physical WAL serves many tablets.
Evaluate tablet-organized immutable files if they reduce transfer/cleanup costs; they do not
require thousands of independent fsync loops.

A shared segment can be reclaimed only when every tablet that depends on it has durable coverage
or the specified retained replacement history. Dropping a tablet must not delete another tablet's
log records or archives. Snapshot/read references pin immutable generations until released.

### Orphaned tablets

An orphan is data no longer assigned to this node. Report it before deleting; never serve or
count it solely because it exists. A verified old checkpoint may seed a new learner only after
identity/configuration checks and full reconciliation with the current group. A removed NodeId
cannot regain voting authority by reporting useful files. Reusing an old directory needs an
explicit replacement/import workflow; it is not a normal rejoin.

### ShardCountMismatch retires

M9c, after inter-node migration works, adds a local startup recovery executor for files belonging
to vanished shards. It reads all per-table WALs, checkpoints, term/vote and dedup metadata under
a manifest describing the in-progress rehome. Transfer to live shards is atomic and resumable;
a crash halfway through cannot double-own or abandon data. If a node is also a tablet voter,
address/incarnation and group configuration changes must use the same safe transition contract.

Keep the mismatch refusal until this mechanism and its crash tests exist. An index alone cannot
read/replay a dead shard's files, and the marker does not become informational prematurely.

### Adding a node

Join as a member first, with no tablet voting authority. Then approve/automatically execute the
capacity-checked plan. At RF=3 adding a fourth node permits storage redistribution; give it
learners and safely replace old replicas. Serving remains available where healthy quorums and
capacity permit it. A stalled move pauses visibly without taking unrelated tablets offline.

### Removing a node

`Decommission` marks a live node Leaving, excludes it from new placement and drains it through
safe transitions while it serves its remaining eligible replicas. `Remove` marks a down node
Removing and rebuilds from surviving authoritative groups. No old copy is dropped before the
new configuration is safe. Mark Removed only after required data transitions and control-voter
replacement are complete, with an identity tombstone persisted.

For a three-node RF=3 cluster with one dead node, add a replacement (or initiate an integrated
Replace operation) before expecting removal to finish. There are only two surviving distinct
nodes otherwise. Decommissioning from three nodes to two at RF=3 similarly blocks unless RF is
explicitly changed through a separate supported policy transition. Never silently reduce RF.

### auto_remove_after

Proposed default 30m, `null` disables, maintenance can suspend. Expiry requests Removing; it does
not guarantee capacity to finish. Preserve remaining copies and show blocked under-replication
if there is no safe target. Persist episode/progress across control-leader restart (C3).
A partitioned node can be fenced and replaced after grace; its return cannot undo completed
transitions. Operators see remaining grace, planned bytes and replacement capacity before expiry.

## Alternatives rejected

Three map edits and a lag-zero heartbeat are superseded by the persisted data-configuration
transition. Replica-count equality is superseded by feasible capacity targets. Immediate file
cleanup and source reads during post-drop grace are excluded: retained files can already be stale.
Automatic removal is not permission to force a new configuration after losing the data majority.

## What it costs

Temporary disk amplification, retained history, foreground interference and metadata transitions.
Leadership transfers can cause short retries; measure them rather than promise unconditional
zero client errors under arbitrary simultaneous failures. Supported healthy add/drain operations
should complete with zero final operation errors within their configured deadline/load envelope.

## What it breaks

Map-only MoveTablet/AddReplica/DropReplica semantics, ownership cleanup, disk budgeting and local
rehoming. The earlier claim that per-tablet files buy nothing is withdrawn; WAL sharing and
checkpoint organization will be evaluated separately.

## Invariants to uphold

- Migration preserves acknowledged operations across both configurations and every crash phase.
- Learners become voters only through a committed data-protocol transition.
- Transitions resume by durable identity, never solely from cached progress reports.
- Down keeps assignments during grace; removal cannot manufacture capacity or a majority.
- Per-node and per-device resource budgets bound streams and retained state.
- Retired copies do not serve; cleanup waits for safe configuration and reference release.

## Prerequisites

[C7](failover.md) complete, C13 Q7–Q9, [C4](tablet-map.md), [C5](replication.md).
M9a migration, M9b planner/removal, M9c local shard-count changes.

## How it would be measured

[C10](performance.md): add/drain/remove, capacity weights, hot tablets, foreground tails, error
counts and time/bytes to reach a feasible target. Measure lag and disk reserve during transfer.

## Acceptance tests

| Test | Asserts | Milestone |
| --- | --- | --- |
| `move_preserves_write_after_zero_lag_report` | Acknowledge more writes after the catch-up report and delay old-config requests; all survive source retirement | M9a |
| `migration_resumes_after_each_phase_failure` | Kill source, destination or control leader at every phase; reconcile without lost/duplicate operations | M9a |
| `learner_never_counts_before_configuration_commit` | Delayed snapshot installation cannot prematurely satisfy quorum | M9a |
| `retired_copy_never_serves_from_grace_files` | Stale routing forwards/refreshes rather than returning post-drop stale source data | M9a |
| `shared_wal_cleanup_preserves_other_tablets` | Moving one stream cannot erase history required by another | M9a |
| `node_transfer_budgets_bound_concurrent_sources` | Many sources respect destination memory/disk/bandwidth limits while foreground work progresses | M9b |
| `heterogeneous_placement_obeys_feasible_weights` | N>RF balances bytes/load; N=RF reports the full-copy constraint without oscillation | M9b |
| `remove_without_replacement_capacity_stays_blocked` | Three-node RF=3 loss preserves two copies and desired RF until a fourth identity joins | M9b |
| `automatic_removal_and_rejoin_preserve_fencing` | Grace expiry, leader restart and old-node return cannot revive old authority | M9b |
| `decommission_drains_within_supported_load_envelope` | Healthy migration preserves all operations and finishes without final client errors | M9b |
| `local_rehome_recovers_after_each_crash_point` | Fewer configured shards preserve full data and consensus metadata across interrupted rehome | M9c |

## Related and implementation references

[C3](membership.md), [C4](tablet-map.md), [C7](failover.md), [C13](protocol.md).
[Scylla's tablet implementation](https://www.scylladb.com/2024/06/17/how-tablets/) describes durable
transition metadata, conflict serialization and movable tablet storage; these motivate the
separation between migration mechanics, file organization and placement policy here. Shoal's
ordered replication still needs its own selected library's configuration-transition rules.
