# C7. Primary failover and recovering a node

## Context

A tablet primary can die, pause, or lose only some connections. Success means both preserving
acknowledged operations and recovering service once a viable majority can communicate. Membership
and elections stay inside Shoal; no external coordinator participates. [C13](protocol.md) defines
the failure assumptions.

## What exists today

Local [recovery](../storage/recovery.md) replays per-shard/table logs and compacts them. Archives
hold current state, not historical versions. Once compaction merged an intent and deleted its
log, rereading the archive cannot roll that mutation back. Recovery must change with replication,
not simply acquire a peer request after today's replay finishes.

## The design

### When a primary is Down

A tablet's embedded consensus group elects a primary using its own persisted votes, durable
history and voter configuration. Node-level `Down` is useful to routing and the removal grace;
it is not a prerequisite for a tablet election or a proof of tablet safety. This also permits
failover when the control-plane thread is alive but a data shard is stalled.

The original rule, “choose the highest heartbeat stamp,” is unsafe. B can report index 100,
C report 101, then A and B durably acknowledge 102 before A dies. Those cached reports would
choose C and incorrectly discard B's acknowledged write. Reports remain observability hints.
Do not select an authoritative history from them.

Use the selected protocol's election restriction and log matching. Before strong reads or
successful new commands, the elected leader establishes current-term authority and the required
committed prefix, applies it, and synchronizes followers through the protocol. For the Raft
baseline, the current-term commitment/read-barrier requirements are part of that library's
integration contract. A metadata majority never substitutes for a data quorum.

### Fencing

Persist terms/votes before responding as required by consensus. Replication messages name the
tablet, term and matching history; receivers reject obsolete authority and notify the sender.
The safety guarantee is that incompatible histories cannot both commit, not that an isolated
old primary cannot receive a client request. Such requests can time out with an unknown outcome.

Do not assume a committed topology update has instantly reached all shards. A leader hint in
the map is routing information; it cannot override the data group's term or configuration.
Removed nodes retain identities in control-plane tombstones, and old copies cannot recreate a
voting group on restart. Only reconciliation with the established cluster can make them eligible.

Strong reads initially use a data-quorum read barrier, not a timer reset by any control-plane
message. A future lease needs the protocol, expiry and clock evidence in C13 Q6. `One` reads
may remain stale during a partition, but only from an installed committed prefix.

### The window, and what a client sees

The proposed `primary_failover_after` is a base for the randomized tablet election timeout,
not an extra sleep after `Down` commits and not a read lease. The exact mapping to the selected
library is settled in Q1. Timeout tuning affects detection and contention, never safety.

Record outage from the client's first failed/uncompleted operation until a sustained run of
successful operations. Include reconnect, election, log recovery and application time. A target
such as base timeout plus two seconds is tested only with healthy survivors and bounded injected
delay; a large backlog or unavailable majority can take longer. Do not promise identical
throughput before and after losing one third of the hardware.

Requests definitely refused before admission and requests with unknown outcomes are distinct.
The client retries the latter using C5's stable identity. Bounded read retries may select another
eligible replica within the original deadline. `One` availability through every instant of a
kill is not guaranteed: an in-flight socket request can fail before detection.

### A returning node

Recover local storage without advertising readiness for its tablets. Restore term/vote,
configuration, checkpoint term/index, log history and deduplication state. Reconcile with the
current group before enabling replication acknowledgements or reads.

| Local condition | Recovery |
| --- | --- |
| Matching retained history, behind | Fetch missing entries and commit/application progress |
| Conflicting uncommitted suffix | Locate common history with the protocol and durably truncate WAL only; never roll authoritative archives backward |
| Required history no longer retained | Install a complete checkpoint, then its subsequent log tail |
| Same index, mismatched checksums/state | Quarantine and use verified repair, not a claim that equal stamps imply equal data |
| Obsolete configuration or removed identity | No autonomous voting/serving; follow C8/C9 replacement and orphan rules |

Do not automatically move leadership back to a returning node. A later load-aware leadership
transfer is separately scheduled. A node can serve its healthy tablets while another tablet
installs a snapshot; per-tablet eligibility, not a node-wide `Up`, decides that.

### Snapshots and atomic installation

A snapshot is a stable committed applied state at `(tablet, last_term, last_index)` plus its
configuration, schema/storage version and deduplication state. Recording an index before walking
mutable partitions does not freeze those partitions. Select immutable checkpoint generations,
copy-on-write views, or a bounded tablet pause to establish the cut (Q3).

Transfer protocol:

1. Establish checkpoint S and pin its manifest/files. Retain the log strictly after S for the
   transfer or explicitly abort/restart if the retention budget is exceeded.
2. Send a manifest with snapshot/transition id, tablet identity, history boundary, file/chunk
   lengths and checksums, schema/format identity, and all required metadata.
3. Transfer bounded chunks with offset acknowledgements and resumable identity. Validate chunks;
   duplicates are harmless, and a receiver never combines chunks from different snapshots.
4. Write into a temporary checkpoint generation. The receiver remains ineligible for serving or
   voting acknowledgements from this copy. Fsync required data and metadata, then atomically
   switch the installed manifest and durably record the switch, including directory metadata.
5. Replay entries with index strictly greater than S, using matching history. Announce readiness
   only after the protocol's required configuration and committed prefix are installed/applied.
6. Reclaim replaced files only after durable installation and references permit it. Recovery at
   every intermediate crash sees the old complete checkpoint or the new complete checkpoint.

The snapshot enumerates deletions/absence through a complete manifest: old partitions absent
from the new generation must not survive installation. Reads see a stable pinned generation;
compaction must not delete files while a snapshot or query still owns them. Bounds apply to
memory, disk space, transfer duration and concurrent installs. This is also C8's bootstrap path.

The filesystem adapter must distinguish atomic name replacement from persistence after a crash.
Consult [Linux rename](https://man7.org/linux/man-pages/man2/rename.2.html) for replacement semantics
and [fsync/fdatasync](https://man7.org/linux/man-pages/man2/fsync.2.html) for completion and directory
durability requirements. Test on the supported filesystem; a successful rename alone is not the
durable installation barrier.

### Retention and convergence

Separate checkpoint compaction from replication log retention. Configure byte/time budgets and
expose the oldest retained index per stream. A single lagging follower cannot pin shared WALs
without bound. When incremental catch-up is no longer possible, select a snapshot and account
for the space required to retain both generations plus a tail. If incoming mutation rate exceeds
catch-up throughput, throttle or reserve recovery capacity; never report a permanently growing
backlog as healthy convergence.

### No hinted handoff

The retained log and checkpoint paths supply catch-up. A second coordinator hint store is not
required. This is only true because the retention and snapshot contracts above replace history
that ordinary compaction would otherwise discard.

## Alternatives rejected

Heartbeat-max promotion, topology-only fencing, rollback from already advanced archives, and a
snapshot assembled from a mutable walk are superseded. They fail under ordinary message delay
or compaction, independent of timer choice. External failover services are outside the design.

## What it costs

Elections and recovery pause affected tablets. Snapshot creation can consume memory or briefly
pause writes depending on Q3; transfer consumes disk and network resources. Admission and recovery
budgets must preserve capacity for unrelated tablets. Avoid attributing recovery time solely to
one configured timer.

## What it breaks

Recovery is no longer self-contained, but its local phase must not erase consensus evidence.
The compactor gains committed checkpoint boundaries and retention ownership. Startup readiness
becomes per tablet as well as per process. Retry outcomes and snapshot installation introduce
persistent metadata beyond the existing storage marker.

## Invariants to uphold

- Every acknowledged quorum operation and its original result survive every permitted election.
- Obsolete metadata cannot make an old primary authoritative or a learner a voter.
- Recovery truncates only history the consensus protocol permits; committed checkpoints never roll back.
- Snapshot and tail meet exactly at one recorded boundary with no gap or duplicate application.
- Incomplete or corrupt copies never serve, vote from fabricated state, or count toward durability.
- Recovery retains enough evidence to resume after another failure.

## Prerequisites

[C13](protocol.md), [C5](replication.md), [C2](transport.md), [C4](tablet-map.md).
Design checkpoint/retention boundaries before M4; implement transfer at M7. C6 strong reads and
M6 failover share an authority proof and must be validated together.

## How it would be measured

[C10](performance.md) measures client-visible outage, recovery debt, before/during/after tails,
and seconds to catch up by log and checkpoint at specified write rates. Report the failed node's
role, pending data and surviving hardware with each result.

## Acceptance tests

| Test | Asserts | Milestone |
| --- | --- | --- |
| `stale_heartbeat_reports_cannot_lose_acked_write` | Force the B=100/C=101/A+B=102 schedule before election; 102 survives | M6 |
| `delayed_topology_cannot_authorize_old_primary` | Delay map and term messages independently; conflicting leaders cannot both commit | M6 |
| `shard_stall_with_live_control_plane_can_fail_over` | Stop only the owning data shard; surviving tablet majority recovers | M6 |
| `quorum_history_survives_repeated_elections` | Updates/deletes/no-ops and original results survive multiple leaders and response loss | M6 |
| `strong_read_refuses_isolated_old_primary` | Old primary cannot pass a fresh read barrier after replacement | M6 |
| `quorum_loss_is_unavailable_without_data_loss` | RF=3 minority never commits; healing restores progress with acknowledged history intact | M6 |
| `returning_node_catches_up_by_log_or_snapshot` | Exercise both retention cases and verify state and request-result history | M7 |
| `snapshot_has_one_stable_boundary_under_writes` | Concurrent insert/delete/reinsert/update and compaction produce the exact checkpoint plus tail state | M7 |
| `snapshot_install_is_atomic_at_every_crash_point` | Kill after each file/manifest/fsync transition; restart has one complete generation | M7 |
| `snapshot_duplicates_and_resume_are_safe` | Reordered/repeated chunks and source failover never mix generations or apply twice | M7 |
| `installing_tablet_never_serves_partial_state` | Stale-read tolerance does not expose an incomplete copy | M7 |
| `retention_and_recovery_memory_are_bounded` | Slow follower and high write rate trigger bounded fallback/backpressure | M7 |
| `down_within_grace_moves_no_replicas` | Election may change leadership; replica placement remains unchanged | M7 |
| `whole_cluster_restart_preserves_durable_history` | Restart all nodes after pending writes and compaction; acknowledged operations remain | M7 |

## Related

[C6](reads.md), [C8](rebalancing.md), [C9](operations.md), [C11](testing.md).
For adapter lifecycle requirements consult
[OpenRaft state-machine and snapshot APIs](https://docs.rs/openraft/latest/openraft/storage/trait.RaftStateMachine.html).
The full protocol reference and implementation decision gates are in [C13](protocol.md).
