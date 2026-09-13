# C7. Primary failover and recovering a node

## Context

A tablet primary can die, pause, or lose only some connections. Success means both preserving
acknowledged operations and recovering service once a viable majority can communicate. Membership
and elections stay inside Shoal; no external coordinator participates. [C13](protocol.md) defines
the failure assumptions.

## What exists today

**The checkpoint and retention boundaries are designed and in place at M4**
([F40](../features/replication.md)): a cluster node's recovery is openraft's, not the intent
replay - a group starts at the checkpoint its table's archives are complete to
(`wal/Shard-N/checkpoint.json`), re-applies the shared WAL from there to what was committed,
and takes the rest from its leader; compaction is what moves the checkpoint, and a segment is
handed to it only once every group applied past its frames and deleted only once every group
purged past them, so an uncommitted suffix never reaches an archive and a lagging member's
history stays until openraft says it is not needed. **Failover is delivered at M6 by
[F42](../features/primary-failover.md).** A tablet's election is its group's own: node-level
`Down` is not a prerequisite (`shard_stall_with_live_control_plane_can_fail_over`), a cached
report cannot choose a history (`stale_heartbeat_reports_cannot_lose_acked_write`), a
topology commit authorizes nothing (`delayed_topology_cannot_authorize_old_primary`,
`metadata_quorum_cannot_replace_a_missing_data_quorum`), and a minority never commits
(`quorum_loss_is_unavailable_without_data_loss`). An isolated old primary learns it is not one
at its lease and answers `NotLeader` before it appends; a strong read through it is refused
(`strong_read_refuses_isolated_old_primary`). A retry under the same identity returns the
original result across an election and across the purge point, from a retry table persisted
beside the checkpoint. A node holding no copy of a tablet routes by health and a forward the
link never wrote is sent to another holder once. The window is measured by the failover arm.
**Recovery of a returning node is delivered at M7 by [F43](../features/node-recovery.md).** A
member inside its group's retained log is fed from the log; one behind the purge point is fed
a snapshot per group: one file cut by the table's compactor between two of its jobs, exactly the
archives' state at the group's checkpoint with the dedup table's remembered results in its
trailer, streamed in bounded chunks over the bulk lane with the control on the replication lane,
resumable from the prefix the receiver holds, and installed atomically - a marker written and
synced before the archives change, the install redone from it at open if the node dies before
the checkpoint that carries the installed state is durable
(`snapshot_install_is_atomic_at_every_crash_point`). Absence is total: every partition of a
covered tablet not in the file is removed. A volatile group's snapshot is the same file
installed into memory. While a group installs, a read of its tablets is refused and the rest
of the node serves (`installing_tablet_never_serves_partial_state`); readiness and `GROUPS`
count the install. The sealed WAL is bounded in bytes (`replication.retained_bytes`): a sweep
past the budget forces the groups pinning the oldest segments to snapshot and purge, so a slow
or cut member is fed a snapshot rather than pinning history
(`retention_and_recovery_memory_are_bounded`). A `Down` member keeps its placement through the
grace and returns into it (`down_within_grace_moves_no_replicas`); a whole cluster restarts
with every acknowledged key once everywhere
(`whole_cluster_restart_preserves_durable_history`). The catch-up is priced by the two
catch-up arms. ~~What is not there: a member behind the purge point cannot be fed a snapshot
(M7);~~ What is not there: leadership
after a failover stays where the election put it and is not moved back to a returning node,
whose groups are led again only once the lease it held lapses and an election runs
([item 103](../appendix/known-issues.md#103-a-returning-leader-is-refused-its-own-re-election-until-its-old-lease-lapses-and-hops-to-it-wait));
a snapshot is per group, so a returning node installs every tablet its replica set shares; and
a partial transfer survives a lane cut but not a receiver restart.
Before that: local
[recovery](../storage/recovery.md) replays per-shard/table logs and compacts them, which a
standalone node still does. Archives
hold current state, not historical versions. Once compaction merged an intent and deleted its
log, rereading the archive cannot roll that mutation back. ~~Recovery must change with replication,
not simply acquire a peer request after today's replay finishes.~~

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
not an extra sleep after `Down` commits and not a read lease. ~~The exact mapping to the selected
library is settled in Q1.~~ The mapping, since M4 and read from the map since M6: openraft's
`election_timeout_min` is the base, `election_timeout_max` twice it, the heartbeat a tenth. What
that makes the window: a follower refuses every vote for `election_timeout_max` after it last
heard from its leader - openraft's follower lease - and a randomized timeout between the two
follows, so a failover completes between two and three times the base: two to three seconds at
the fixture's one, ten to fifteen at the default five. A killed leader that returns inside that
lease is refused its old term by the same rule ([item 103](../appendix/known-issues.md#103-a-returning-leader-is-refused-its-own-re-election-until-its-old-lease-lapses-and-hops-to-it-wait)).
Timeout tuning affects detection and contention, never safety.

Record outage from the client's first failed/uncompleted operation until a sustained run of
successful operations. Include reconnect, election, log recovery and application time. A target
such as base timeout plus two seconds is tested only with healthy survivors and bounded injected
delay; a large backlog or unavailable majority can take longer. Do not promise identical
throughput before and after losing one third of the hardware.

Requests definitely refused before admission and requests with unknown outcomes are distinct.
The client retries the latter using C5's stable identity. Bounded read retries may select another
eligible replica within the original deadline. `One` availability through every instant of a
kill is not guaranteed: an in-flight socket request can fail before detection. *As built at
M6:* a write refused at a lapsed lease, or whose hop the link never wrote, is `NotLeader`; one
the link wrote and never answered, or that a leader could not commit within its deadline, is
`OutcomeUnknown`; a forward the link never wrote is sent to another holder once by the server;
and the client retries under `SendOptions::identity` and `retry` what says to try again, with
`ShoalResponse::attempts()` saying how many times. The failover arm records what a client
without the retry sees: the dead node's third of the writes refused within a hundred
milliseconds, the rest served, until the election.

### A returning node

Recover local storage without advertising readiness for its tablets. Restore term/vote,
configuration, checkpoint term/index, log history and deduplication state. Reconcile with the
current group before enabling replication acknowledgements or reads.

| Local condition | Recovery | Delivered |
| --- | --- | --- |
| Matching retained history, behind | Fetch missing entries and commit/application progress | M4: openraft's replication from the retained log (`returning_node_catches_up_by_log_or_snapshot`, first half) |
| Conflicting uncommitted suffix | Locate common history with the protocol and durably truncate WAL only; never roll authoritative archives backward | M4 for a volatile group; a durable group's reversion is [item 99](../appendix/known-issues.md), M8's |
| Required history no longer retained | Install a complete checkpoint, then its subsequent log tail | M7: a snapshot per group at its checkpoint, the log strictly after it from the leader (`returning_node_catches_up_by_log_or_snapshot`, second half) |
| Same index, mismatched checksums/state | Quarantine and use verified repair, not a claim that equal stamps imply equal data | M8 |
| Obsolete configuration or removed identity | No autonomous voting/serving; follow C8/C9 replacement and orphan rules | M9 |

Do not automatically move leadership back to a returning node. A later load-aware leadership
transfer is separately scheduled. A node can serve its healthy tablets while another tablet
installs a snapshot; per-tablet eligibility, not a node-wide `Up`, decides that. *At M7* the
eligibility is per group: `MachineState::installing` is set before the first archive write and
cleared after the cleanup, a read of an installing group's tablets is answered `Unavailable`,
a write still proposes, and readiness reports the installing count
(`installing_tablet_never_serves_partial_state`).

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

*At M7* the six steps are [F43](../features/node-recovery.md#what-it-does): the cut is one
file the compactor writes between two of its jobs at the group's merged boundary (Q3), pinned by
the file itself rather than by references to the archives; the manifest is the `Begin` RPC on
the replication lane; the chunks ride the bulk lane with a resumable stream id and the `End`
RPC answers `Resume { from }` with the prefix held; the receiver writes into `install/`,
fdatasyncs, writes and syncs a marker, and only then hands the file to openraft, whose
`install_full_snapshot` runs the compactor's install job - every covered partition replaced or
removed, the archive map repointed - and the marker is cleaned up once the checkpoint that
carries the installed state is durable; the tail is openraft's replication past the boundary;
and readiness is per group. Recovery at any of the seven crash points sees the old generation,
the marker and a redo, or the new generation (`snapshot_install_is_atomic_at_every_crash_point`).

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

*At M7* the budget is `replication.retained_bytes` over the sealed WAL (Q9): a sweep past it
forces the groups pinning the oldest segments to snapshot and purge, so the member behind is
fed a snapshot rather than the WAL growing; the receiver's side is `replication.install_bytes`
over the partials it holds, past which a `Begin` is refused. Neither throttles the foreground.
The catch-up arms' record says `none` when a run ends before the lag is held at zero, and
keeps the series (`retention_and_recovery_memory_are_bounded`, `catchup_capture_records_convergence`).

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
~~Design checkpoint/retention boundaries before M4~~ The boundaries are designed at M4
([Q3](protocol.md#q2-q3-and-q4-at-m4)); ~~implement transfer at M7~~ the transfer is
delivered at M7 ([Q3 and Q9](protocol.md#q3-and-q9-at-m7)). C6 strong reads and
M6 failover share an authority proof and ~~must be validated~~ were validated together
(`strong_read_refuses_isolated_old_primary`, `read_barrier_survives_leader_change_and_delayed_messages`).

## How it would be measured

[C10](performance.md) measures client-visible outage, recovery debt, before/during/after tails,
and seconds to catch up by log and checkpoint at specified write rates. Report the failed node's
role, pending data and surviving hardware with each result. *At M7* the seconds to catch up are
`macro/cluster/catchup/{log,snapshot}`: the kill arm's shape with the survivors' retention at the
defaults and shortened past what the node missed, the returning node sampled each second
against node zero's committed positions, the record split by path
([F43](../features/node-recovery.md#the-catch-up-arms)).

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
