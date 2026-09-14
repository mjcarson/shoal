# F45. Safe replica migration

## Context

[M9a](../distributed/milestones.md#m9a-safe-replica-migration) is the first milestone at which
data moves. Everything before it placed tablets by one rule over an ordered node list
([F39](membership.md)), replicated them under an identity minted from that rule
([F40](replication.md)), fed a member that fell behind ([F43](node-recovery.md)) and repaired a
copy that went wrong ([F44](repair.md)) - but a replica set, once initialized, was the same
three shards for as long as the cluster lived. A node admitted after `Initialize` held nothing
and could be given nothing; a second `Initialize` was refused, naming this milestone.

[C8](../distributed/rebalancing.md) sets out what a move has to be: a durable transition record
resumed from wherever it stopped, a non-voting learner caught up before it counts, the library's
joint membership transition so both quorums hold through it, an activation barrier that is the
new copy's own apply rather than a lag report, a published configuration stale routers are
bounded against, a retirement that keeps the old copy's files for a grace and reclaims them
without touching the other tablets that share its WAL, and a retry identity that survives the
move. [C4](../distributed/tablet-map.md) reserved the per-tablet records for it,
[C5](../distributed/replication.md)'s Q4 left the expiry of a retry identity to it, and
[F44](repair.md)'s per-group serialization is what its transition lock inherits.

This page is the model. The mechanism that most needed one is the identity: a group is named by
the hash of its table and the replica set the rule derived at initialization, and everything
durable - its log's frames in the shared WAL, its checkpoint, its retry sidecar, its snapshot
files, the tokens a client holds - names that hash. A move keeps it.

## What it does

### A configuration beside the rule

The placement rule stays the default. A replica set that moved is a `DataConfiguration` (`shoal-core/src/server/control/migrate.rs`) the
control state commits and the map carries beside the rule: the set's tablets, its members with
the primary first, and the log index of the uniform membership each of its groups committed.
`TabletMap::replicas_of` answers the configuration's members for exactly its tablets and the
rule's for every other; `rule_replicas_of` is the rule alone, and it is what a group's identity
is still minted from, so a set that moved is one set with a different membership rather than a
new set. `replica_groups` and `groups_of` compute the rule's sets and overlay each with its
configuration; `holds`, `preferred_holder`, `alternate_holder` and the read ring all go through
`replicas_of`, so a node in no placement slot but in a configuration is a holder like any other.
`places` and the routing list are the placement plus every node a configuration or a move
brings in, so a member admitted after `Initialize` routes and is routed to from the moment a
move names it. `active_rf` and write admission read the placement's size as before.

A configuration covers a whole set: a `Move` names a tablet, and the set is every table's group
over the tablets the rule placed together with it. Routing is per tablet and not per table
([F44](repair.md) already accepted that for a quarantine), so moving one table's group and not
another's over the same tablets would send the second table's queries to a node holding no copy
of them.

### `Move` and its record

`AdminKind::Move { tablet, from, to }` is a mutation: authorized against `cluster.admins`,
versioned, idempotent by operation id and audited, exactly as `Repair` is. Its `ControlCommand`
is applied whole by the state machine against the map it derives: `from` has to be a member of
the set serving the tablet - rule or configuration - and `to` an `Up` member that is not, placed
or not. The record - `MoveRecord` in `control/migrate.rs` - keeps the set's tablets, the source
and the destination as shard addresses (the destination's shard is the one the rule would give
the set's first tablet on it, `(first / N) % shards`), `expected` as the set was, `target` as the
set with `from` replaced by `to` in place, one `GroupMove` per table, who asked and when. The
newest sixty-four are kept; the ones not done ride the map. `AdminKind::MoveStatus { op }` reads
the record; the fixture's `MOVE <key-hex> <from> <to>` and `MOVE_STATUS <op>` drive it.

One transition per set. A move asked for while another move of the set, or a repair holding
any of its groups, is not done is recorded `Queued { behind }` rather than `Planned`; a repair
asked for while a move of the set is not done records that group `Queued { behind }` rather
than `Pending`. Nobody drives a queued record and a driver's word on one is refused. The last
group done of the transition ahead releases the queue - the first queued move to `Planned` with
the rest requeued behind it, every queued repair group to `Pending` - in apply, so every member
derives the same queue; a scheduled scrub leaves a moving set alone.

### The driver, one group at a time

`shard/migrate.rs` mirrors the repair driver. `drive_moves` runs on every map install, every
group up and every driver finished: for every record the map carries that is planned or
published, for every group this shard hosts whose handle it leads, up to
`cluster.migration.concurrent` at a time, it starts a driver at the phase the record has - or
the phase this shard committed last if the map is behind. Every phase is committed through
`ControlCommand::MoveProgress` before the step it names, a driver that loses the lead leaves
the record at the phase it reached for the next leader (`NOT_LEADER`, half a second's pause so
a hand-off is not committed once per tick), and any other error commits `Done` with a `Failed`
outcome and the reason.

1. **Reconcile.** The group's committed membership wins over the record: a group whose committed
   voters are already the target is at least `Configured`, at the committed membership's index,
   whatever a crashed driver failed to say. Nothing here ever proposes a membership that is not
   the target.
2. **`Learner`.** Committed, then `raft.add_learner(to, to, false)`, which openraft answers as
   a membership entry adding the destination to the group's nodes and starts replicating to it.
   The destination's shard has already built the group as a learner from the map - a
   `GroupSpec` with `learner: true` and the shard the record names - so the leader's replication
   reaches a `Raft`; a learner spec never initializes the group and never takes the head start,
   whatever slot the target puts it in. The snapshot stream that feeds it carries the move's
   operation in `SnapshotBegin.transition`, which was reserved and always zero until now.
3. **`CatchingUp`.** Committed when the leader's replication first reports the destination's
   matched index; the driver polls until `last_log_index - matched <= cluster.migration.catchup_lag`,
   charging the record with the snapshot bytes sent to the destination and its log position.
   If this leader is the source, it hands the lead to a member of the target that is up - the
   library's transfer, which now rides the replication lane as `ReplicateKind::TransferLeader`
   so the member named elects at once - commits the record with no driver and steps aside. The
   source never drives its own removal.
4. **`Reconfiguring`.** `change_membership(ReplaceAllVoters(target), retain: false)`: the
   library writes the joint configuration `[expected, target]`, waits for it to commit under
   both majorities, writes the uniform `target`, and drops the source from the group's nodes. A
   leader lost between the two leaves the joint configuration behind; the next leader's call
   from the joint state is the uniform step, since the library's next coherent step from
   `[c1, c2]` toward `c2` is `c2`. `InProgress` is waited for, never failed.
5. **`Configured`**, with the uniform entry's index read from `committed_membership_config`.
6. **`Activated`.** The destination's matched index has passed the uniform entry, and the
   destination has answered a `ReplicateKind::Applied` probe with an applied index past it. The
   barrier is the destination's own apply, never a lag report. The driver leaves the group here:
   the set's other groups on the same shard need its slot, and the record cannot go further
   until every one is activated.
7. **`Published`.** The last group's `Activated` publishes the configuration in apply - the
   target members with every group's uniform index, replacing an earlier configuration for the
   same tablets unless that would roll a group's index backward - and moves the topology
   version. The push makes every node's `rebuild_groups` see the new members: the destination's
   spec loses `learner`, refreshed in place with its handle untouched, and the source's shard
   sees the group gone.
8. **`Retiring` → `Done`.** Started again once the map carries the record published, the driver
   polls the source's `ReplicateKind::Retired` answer until its copy is gone, or the source is
   `Down`, or `retire_after + timeout` has passed, then commits `Done` with `Moved` and the time
   each phase took. The last group's `Done` finishes the record and releases the queue.

### A retired copy

When a published move drops this shard from a group's members and names it as the source, the
shard retires the copy rather than only stopping it. The handle is shut down - openraft keeps a
removed member as a candidate that never wins - the resident partitions are evicted, the log is
forgotten, and the copy is held under `wal/Shard-N/retired/<group>` with its files for
`cluster.migration.retire_after`. A forgotten durable log is a `FrameKind::Forget` marker in the
shared WAL: the group's state and every segment's memory of its frames are dropped, a replay past
the marker rebuilds nothing from the frames before it, and a copy of the same group added to
this shard later starts with no log, no vote and no committed position - which is what a
re-added learner has to start with, since a stale memory log with a stale committed position is
what openraft refuses as a committed entry that conflicts. A volatile log is dropped whole.

After the grace the table's compactor runs `CompactionJob::Drop` - the absence half of an
install with no file: every archived partition of the tablets removed through the map intent
log and the map repointed - the group's snapshot, install and quarantine files and its marker
go, the checkpoint no longer names the group, and the source answers `Retired`. A `Down`
source retires at its next start from the marker, with the grace counted from when it began.
The sealed segments that held the retired group's frames beside other groups' are handed for
those groups alone - `sweep_segments` already treats a group no longer hosted as resolved and
never hands a group not in `replication.groups` - and reclaimed once those groups purge past
them.

### The tablet gate

A query of a tablet no group on the shard serves - a read judged by its partitions, a write by
its key - is refused **`ErrorCode::StaleTopology` (55)** naming the map version held, never
answered from the resident or archived rows the cluster no longer counts. On a cluster node
this is the first thing `execute_query` decides, before a write is proposed or a read waits for
its barrier. A query a peer forwarded is refused on a frame of its own - `ReplyKind::Stale`,
which the peer relay writes as a `ForwardedKind::Error` - so the origin can act on it: the
coordinator sends the forward once to another holder of its partitions under the same attempt
and slot, the shape a lost link already had ([F42](primary-failover.md)), within the bundle's
budget, and answers the client `StaleTopology` otherwise. A stale coordinator therefore
terminates in at most two sends and never duplicates a write, and a client that meets the
refusal retries under the same identity. `ReadStats` counts `stale_served` on the refusing shard
and `stale_refusals` on the coordinator.

### A retry identity with a time and a window

Every bundle identity the client mints is a version 7 uuid - `Queries::default`, the retry
loop's default, the stream ids - so a group that has forgotten it can tell its age;
`SendOptions::identity` still takes any uuid. Before a write is proposed, its identity is judged
on the coordinator's own replica against `cluster.replication.retry_window` (five minutes) and
against `MachineState::expired_before`, the newest time-ordered identity the retry table has
evicted: older than either is refused **`ErrorCode::IdentityExpired` (22)** by name, and the log
never carries a retry that might be a second effect, so every replica answers alike. An
identity that is not time-ordered expires never and is applied as new once forgotten, as
before. The watermark rides the checkpoint (`GroupCheckpoint::expired_before`) and a snapshot's
manifest, so a copy built from either - the destination of a move among them - refuses what
its source would; `retry_floor` stays recorded.

### Found on the way

A snapshot's manifest carried the state's *current* membership while its boundary could be
older than the last membership entry, so a learner fed a snapshot installed a membership whose
entry then arrived and openraft refused it as going backward. `MachineState` now keeps every
membership applied since the checkpoint and a cut carries the one as of its boundary; a
checkpoint moved by a compaction records the same. And the cluster harness's `apply` rebuilt a
node's block from the defaults, dropping every override `resolve` had put on it
([Resolved #108](../appendix/resolved/cluster-arm-overrides-dropped.md)).

## Design choices

**The identity is pinned.** A new identity per configuration would restart the log at index
zero over the old archives and orphan every frame, checkpoint, sidecar, snapshot and token
that names the old one. Keeping the hash the rule minted costs a `rule_replicas_of` beside
`replicas_of` and nothing else; the identity is a name, not a description.

**A move is a set, not a group.** Because routing is per tablet. A record still keeps a
progress per group, since each is its own raft with its own leader and driver.

**The driver is the group's leader, and phases are committed before their steps.** The shape
[F44](repair.md) proved: a driver that dies leaves a phase the next leader resumes from, and the
control plane never reconstructs a plan from lag reports. The reconciliation at the start of a
drive is what makes a crash between the uniform commit and `Published` safe: the group's
committed membership is the truth and the record follows it.

**The activation barrier is the destination's apply.** A matched index says the entry is
durable on the destination; the probe says the destination has applied it. Both are asked for
because a read served by the destination at `One` after publication is served from its applied
state.

**Publication is in apply.** The last group activated publishes, so no driver has to hold a
slot waiting for the others; the same reason the driver leaves the group at `Activated`.

**The source keeps its files for a grace and refuses by name.** A stale router's window is the
grace, and inside it the source is the one node that can say why a query is misrouted. The files
are evidence rather than authority until they are reclaimed.

**The coordinator reroutes; nobody relays a second hop.** A forwarded answer travels back down
the peer connection it came in on; a third node's answer would need relaying through the
second, a new frame path for what one more send from the origin already reaches in the same
number of round trips.

**Expiry is judged at the proposer.** An apply-time refusal from a wall clock would differ
between replicas; a refusal before proposal never enters the log. The eviction watermark is
replica-local, and that is accepted: the coordinator's own replica is the one asked, and a
replica that has forgotten more than another refuses more, never less.

**Version 7 identities.** Decided with the user on 2026-09-13: a time-ordered identity with a
retry window over a counter or an index a client could not compare its retry to.

**Repair and move queue behind each other.** Decided the same day, over refusing: an operator's
request is recorded and runs when it can, and the record says what it waits behind.

## Alternatives rejected

- **A membership change as a new group.** See the identity above; it was the first sketch and
  it orphans everything.
- **`add_learner(blocking = true)`.** The library's wait has no lag bound and no timeout of its
  own; the driver's own poll has both and charges the record on the way.
- **Making the source drive its own removal.** A leader removed by its own proposal steps down
  after `removed_leader_step_down` with the uniform entry possibly uncommitted; a transfer
  first keeps the driver on a member that stays.
- **Judging expiry in apply against `expired_before` alone.** Deterministic given equal
  histories, but replicas built from different checkpoints have different eviction histories, so
  it is not deterministic in fact; and without a clock a retry a week old with nothing evicted
  since would be applied as new.
- **A second forward hop with a relayed answer.** Above.
- **Refusing a repair asked under a move.** Above.
- **A per-group `Published`.** The set's tablets are routed as one; publishing a group ahead of
  its set would route a tablet to a node holding one table's copy and not the other's.

## Limitations

- **A failed move leaves the group where its committed membership says.** A group activated
  before another of the set failed keeps its target membership while the configuration is not
  published; the record's `Failed` outcome names the reason, and asking the move again
  reconciles from the committed membership. Nothing rolls a group back.
- **A move of a set is every table's group over it**, and `concurrent` serializes the groups on
  a shard: a set of four tables moves its groups one at a time, and the first waits at
  `Activated` for the last. The record's `activated` time is that wait.
- ~~**The catch-up lag is the only pacing.** There is no transfer budget - bytes per second, per
  pair, per device - and no disk reserve; a move feeds the destination as fast as the bulk lane
  runs. M9b's.~~ Since [F46](capacity-rebalancing.md) every stream draws on the node's byte
  bucket, a shard installs a bounded number at once, and a receiver short of its reserve
  refuses the begin.
- ~~**One move per set, and one set per move.** Moving a node's every set is a sequence of
  operator requests, not a plan. M9b's rebalancer.~~ Since [F46](capacity-rebalancing.md) a
  plan is a sequence of moves the leader issues; a `Move` is still one set.
- **A same-node move, a replication factor change and a move to a shard chosen by the operator**
  are not operations. The destination's shard is the rule's.
- **The snapshot fed to a learner is the whole group's**, as it is for a returning member,
  even where the retained log would do ([O55](../appendix/optimizations.md#o55-a-learner-inside-the-retained-log-is-fed-a-snapshot-when-the-leaders-cached-cut-is-newer-than-its-purge-point)).
- **The retirement grace is a per-node setting**, not the cluster's; a stale router's window is
  the source's `retire_after`.
- **A write between the uniform commit and publication through the source is proposed through
  a handle the group has dropped**: it follows the leader hint one hop, and a hint that went
  stale is `NotLeader`, which the client's retry covers. A `One` read through the source in that
  window is served from a copy that stops receiving entries; the window is a crash's width.
- **An expiry refusal depends on the coordinator's replica.** Two coordinators can answer the
  same late retry differently when one has evicted more; neither applies it twice.
- **The manifest gained a field**, which is a wire change for a snapshot's begin RPC and its
  pending marker; M10's compatibility rules are where that is judged.
- **The volatile-group assertion the crash matrix found is not this milestone's.** Two voters
  of an ephemeral table's group losing their memory log at once elect a fresh leader whose log
  conflicts with the survivor's committed entries, which is that table's data gone by
  definition and an openraft debug assertion on the survivor
  ([item 109](../appendix/known-issues.md#109-a-volatile-groups-survivor-trips-an-openraft-debug-assertion-when-a-majority-loses-its-memory-log-at-once)).

## Invariants to uphold

- **A group's identity is `GroupId::of(table, rule_replicas_of)` and nothing else names it.**
  `replica_groups` and `groups_of` key their sets by the rule's members and overlay the
  configuration afterwards; anything that computes an id from `replicas_of` names a group that
  does not exist.
- **A configuration covers a whole rule set.** `apply_move` takes the set's tablets from
  `rule_set_of`; a configuration over a subset would split what routing treats as one.
- **A learner spec never initializes and never elects.** `start_group` returns before the
  primary's initialize and the head start for `learner: true`, and `spawn_group_start` clears
  `primary` for one; a destination in the target's first slot is still a learner until the
  uniform entry commits.
- **A same-id spec refreshes in place.** A map that changes a group's members or its learner
  flag never rebuilds the handle; the raft already knows from its log.
- **Every phase is committed before the step it names, and the committed membership wins.**
  `drive_group_inner` reconciles first and never proposes a membership other than the target.
- **The source never drives past `CatchingUp`.** The transfer to a target member comes before
  `Reconfiguring`.
- **A configuration never rolls a group's uniform index backward.** `apply_move_progress` keeps
  the newer.
- **A retired copy serves nothing and is reclaimed only after the grace.** The gate refuses by
  `replication.tablets`, which the retired group is not in; `sweep_retired` reclaims by
  `retire_after`; `rebuild_groups` skips a spec whose group is still retired here.
- **A forgotten log is forgotten on disk.** `ShardWal::forget` writes the marker before it drops
  the state; a replay honours it; a retired group's frames are handed to no compactor.
- **The gate comes first on a cluster node.** Before the write is proposed and before a read
  waits: a query nothing here serves is answered by the map version, not by the table.
- **A stale refusal is rerouted at most once, under the same attempt and slot.** `Pending.rerouted`.
- **Expiry is judged before proposal, never in apply.** `is_expired` is called in
  `propose_write`; `remember` is the one way into the retry table and the one thing that moves
  `expired_before`.
- **A cut carries the membership as of its boundary.** `membership_as_of` over the checkpoint's
  membership and every one applied since; an install clears the history.
- **A queued record is driven by nobody.** `drive_moves` and `drive_repairs` skip it and apply
  refuses progress on it; only `release_queued` leaves the state.
- **The fixture's crash point names a group.** `MOVE_CRASH_AT <phase> <group>` kills one driver;
  armed for any group it kills every driver that commits the phase, which on a set of two tables
  is two voters at once.

## Performance

The cost is measured by one arm, `macro/cluster/migration/move`
([C10](../distributed/performance.md)): the kill arm's placement, mixture and client with a
fourth member staged beside the placement and placed on by nothing, a `Move` of one set from
node one to the spare asked for a third of the way through and its record polled each second.
`cluster.migration` records when the move was asked for and done, how long each phase took
summed over the set's groups, what the destination was fed - snapshot bytes and its log position
at catch-up - and the client's distribution before, during and after it with a per second
series, mirrored whole into the explorer. The harness stages a spare through
`ClusterOverride::spares` and shortens the source's grace through `retire_after`, since a move
is done only once the source has retired and the default grace is minutes.

**Smoke-run on the development host only** (europa, `--scale smoke --runs 1 --allow-dirty` into a
scratch directory under `target/` that was deleted, every core available since four nodes of
three shards need fifteen): the move of one set - four groups, one per table - was asked for at
7.1 s and done at 14.1 s; `learner` 0.7 s, `catching_up` 0.25 s, `reconfiguring` 0.77 s,
`configured` 0.25 s, `activated` 15.1 s summed over the four groups - the first waiting for the
last, serialized by `concurrent: 1` - and `retiring` 3.8 s of a three second grace; the
destination was fed by log, so `bytes` is zero; the client's median went from 0.84 ms before to
1.56 ms during and back, at the same depth throughout. The shape of the record and nothing about
magnitude; the capture is the benchmark host's.

## Tests

| Test | Where | What breaks if this is reverted |
| --- | --- | --- |
| `move_preserves_write_after_zero_lag_report` | `shoal/tests/cluster_fixture.rs` | The move never completes, or a write acknowledged after the destination reported no lag - a batch through the source with its shares held - is not on the destination once the source retired; the source still hosts the group; the map carries no configuration |
| `learner_never_counts_before_configuration_commit` | `shoal/tests/cluster_fixture.rs` | With the old quorum short by one and the caught-up destination a learner, a write commits, or a group passes `Reconfiguring`; healed, the write does not commit or the move does not finish |
| `data_configuration_outlives_stale_placement_hint` | `shoal/tests/cluster_fixture.rs` | A driver killed right after `Configured`: the destination's committed voters do not name it, the map carries a configuration before publication, the move does not finish from the record, or the source is a voter again |
| `migration_resumes_after_each_phase_failure` | `shoal/tests/cluster_fixture.rs` | Eighteen moves - the driver armed to die after each phase, the destination and the control leader killed at each - and any one fails to complete `Moved`, the oracle rejects the writers' history, or a holder's digest disagrees |
| `retired_copy_never_serves_from_grace_files` | `shoal/tests/cluster_fixture.rs` | A read through a router held at the placement is answered from the source's retained rows rather than refused and sent on; the marker or the archived partition survives the grace; the router does not read its own way once healed |
| `stale_routes_terminate_without_duplicate_writes` | `shoal/tests/cluster_fixture.rs` | A write through the stale router during the move, after the retirement or after the source died is answered outside the deadline or with an unnamed error, is not sent on, or reads back on a holder with a value other than the one acknowledged |
| `shared_wal_cleanup_preserves_other_tablets` | `shoal/tests/cluster_fixture.rs` | The source's other sets lose a key through the retirement, a restart and a compaction; the retired partition survives; the segments that held the retired group's frames are never reclaimed |
| `retry_identity_survives_snapshot_and_migration` | `shoal/tests/cluster_fixture.rs` | A retry across a checkpoint and a move is applied again or refused; a changed payload is applied; an identity older than the window is applied; a random identity is refused |
| `repair_serializes_with_migration_and_new_commits` | `shoal/tests/cluster_fixture.rs` | A repair asked under a move runs beside it or never runs after it; a move asked under a repair runs beside it; writes stop committing; the corrupted source partition reaches a holder |
| `a_configuration_overrides_the_rule_and_keeps_the_id` | `shoal-core/src/server/map.rs` | The overlay covers the wrong tablets, the identity changes, the spare is not placed by a move, the learner spec is missing or a primary, the rings route the moved set to the wrong holder |
| `a_move_is_recorded_queued_published_and_released` | `shoal-core/src/server/control/types.rs` | A refusal is unnamed, the set or the target is wrong, a second move does not queue, progress on a queued or done group is applied, the last `Activated` does not publish, the last `Done` does not release |
| `a_repair_and_a_move_serialize_on_a_set` | `shoal-core/src/server/control/types.rs` | A repair under a move is `Pending`, a queued group takes a driver's word, a move under a repair is `Planned`, or a done transition releases nothing |
| `move_phases_rank_in_order` | `shoal-core/src/server/control/migrate.rs` | The phases rank out of order or two share a name; a record's configuration drops a group's index |
| `the_migration_block_parses_with_its_defaults`, `validation_refuses_what_is_not_built` | `shoal-core/src/server/conf/cluster.rs` | The `migration:` block's defaults move, a timeout under the snapshot timeout or a retry window under the write timeout is accepted |
| `an_identity_past_the_window_is_expired` | `shoal-core/src/server/replication/machine.rs` | An identity inside the window is expired or one outside it is not, a random identity expires, an eviction does not move the watermark, or the checkpoint does not carry it |
| `a_forgotten_group_leaves_no_log_behind` | `shoal-core/src/server/wal/tests.rs` | A forgotten group's frames are handed or its state comes back after a reopen; the other group's frames go with it |
| `migration_capture_records_transfer_and_pauses` | `shoal-bench/src/workloads/harness/background.rs` | The record's marks, phases, transfer, windows or series are wrong, or an F44 record fails to load |
| `the_migration_arm_places_a_fourth_node` | `shoal-bench/src/workloads/cluster_migration.rs` | The arm's placement differs from the kill arm's, the spare is missing, the grace is the default, the move names the wrong nodes, or the id is out of registry order |

## Related

[C4](../distributed/tablet-map.md), [C5](../distributed/replication.md),
[C6](../distributed/reads.md), [C8](../distributed/rebalancing.md),
[C9](../distributed/operations.md), [C13](../distributed/protocol.md#q4-at-m9a),
[F39](membership.md), [F40](replication.md), [F42](primary-failover.md),
[F43](node-recovery.md), [F44](repair.md),
[Resolved #108](../appendix/resolved/cluster-arm-overrides-dropped.md),
[item 109](../appendix/known-issues.md#109-a-volatile-groups-survivor-trips-an-openraft-debug-assertion-when-a-majority-loses-its-memory-log-at-once),
[O55](../appendix/optimizations.md#o55-a-learner-inside-the-retained-log-is-fed-a-snapshot-when-the-leaders-cached-cut-is-newer-than-its-purge-point).

