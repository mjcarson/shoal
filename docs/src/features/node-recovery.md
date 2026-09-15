# F43. Recovering a node brought back online

## Context

[M4](../distributed/milestones.md#m4-replication-and-quorum-writes) gave every tablet a Raft
group and made the shard's WAL its log; [M6](../distributed/milestones.md#m6-primary-failover)
made the loss of a leader survivable. What neither did was bring a member back once it had been
away long enough. A follower behind its group's retained log was fed from the log; one behind
the *purge point* was stuck for good: `GroupPeer::full_snapshot` answered `Unreachable` by name,
the replication lane refused `ReplicateKind::Snapshot`, and `GroupMachine::install_snapshot`
returned `Unsupported`. A snapshot was metadata at the checkpoint - the archives are the rows -
and nothing could move the rows. At the default retention of ten thousand entries that took a
long absence; with every tablet replicated it was one restart away on any busy table.

[M7](../distributed/milestones.md#m7-recover-a-node-brought-back-online) is the transfer: a
stable cut of a group's rows at its checkpoint, streamed in bounded resumable chunks over the
bulk lane, installed atomically and durably on the receiver in steps a marker makes redoable,
followed by the log tail; a retention budget in bytes so a slow member cannot pin the shared
WAL; per-tablet eligibility while a copy installs; and the crash matrix over all of it. It
closes [Q3 and Q9](../distributed/protocol.md#q3-and-q9-at-m7) and the eight C7 rows filed
against M7, and on the way it fixes two defects found while mapping the code, each reproduced
first: [item 104](../appendix/resolved/segments-recompacted-after-restart.md), a restart
merging every sealed segment below the checkpoint into the archives again, and
[item 105](../appendix/resolved/volatile-groups-never-purged.md), a volatile group that never
checkpointed and so never purged.

The plan for this milestone was drawn up with the user on 2026-09-13 and four decisions were
taken there: **both catch-up arms** (`macro/cluster/catchup/{log,snapshot}`), smoke-run on the
development host only; **items 104 and 105 fixed in M7** rather than filed; **item 99 stays for
M8**; and **one commit per slice**.

## What it does

### A snapshot is one file, cut where the archives stand still

A persistent group's rows are its table's archives, and the only writer of archives is the
table's compactor, which runs one job at a time. Between two of its segment jobs the archive
map is a consistent state - exactly the effect of every frame merged so far - so that is where
the cut is taken: `CompactionJob::Snapshot` runs on the compactor's own task, walks the map for
every partition of the group's tablets, reads each at its archive offset and writes it as a
record of `wal/Shard-N/snapshots/<group>-<boundary>.snap`, then a trailer of the group's
remembered requests at or below the boundary. The boundary is the highest position the
compactor has merged for the group, which it tracks from every segment job's `positions`, or
the loop's checkpoint when that is higher - which it is only for a compactor that has merged
nothing since the process started. No write pauses: a sealed segment is immutable.

A volatile group has no archives and its rows are the ephemeral table's resident partitions,
which the shard loop is the one place to see whole; the loop serializes those of the group's
tablets and writes the same file on a task of its own. That is the bounded pause
[Q3](../distributed/protocol.md) allows as the fallback, and an ephemeral table has no other
stable view.

The file is `[magic][version][table][group][boundary][records]` then `[key][len][bytes]` per
record - the archive's own record shape, so the receiver's compactor writes the bytes as they
are - then the trailer. Its manifest travels beside it, never inside: the group, table, schema
fingerprint, boundary log id, membership, tablets, record count, length, checksum and trailer
count. The checksum folds the file in fixed 64 KiB blocks (`FileHasher`), because `GxHasher`'s
streaming write is not chunk-invariant and a receiver assembling under the lane's chunking would
never agree with a writer buffering a megabyte at a time.

**Absence is total.** The manifest names the tablets the file covers, and every partition of
those tablets that is not in the file is removed on install. That is how a delete travels.

The loop holds the newest cut per group (`Group::snapshot`) behind an `Rc`; a transfer clones
it, and a file is deleted only once a newer cut has replaced it and no transfer holds it.

### The transfer: control on the replication lane, bytes on the bulk lane

openraft owns the decision: when a follower's matched index falls below the leader's purge
point, its snapshot transmitter reads the current snapshot and calls `full_snapshot`. The
group's snapshot is a lazy handle (`SnapshotData::Own { checkpoint }`), and `full_snapshot`
asks the loop for a file at or past that checkpoint through `ServerMsg::BuildSnapshot` - a
held cut if one is at the checkpoint or past it, else a fresh one. The manifest's boundary may
be newer than the handle's; the leader resumes appends from the handle's boundary and the
follower's committed rule accepts the overlap.

Then, under one deadline (`replication.snapshot_timeout`, which is also openraft's
`install_snapshot_timeout` now - its default of two hundred milliseconds completes no snapshot
of any size):

1. **Begin**, a `ReplicateKind::Snapshot` RPC carrying the sender's vote, a stream id and the
   manifest. The receiver answers `Resume { from }` - zero for a new stream, the prefix it holds
   for the same stream asked again - `Installed { vote }` when the boundary is at or below what
   it has applied, or `Refused` by name: another schema, another table, other tablets, a group
   it does not host, a group already installing, or a begin that would pass `install_bytes`.
2. **Chunks** of `snapshot_chunk_bytes` on a bulk link the shard's network opens per peer node,
   each `[stream][offset][len][gxhash32]` as C2's frame says, preceded by a `SnapshotBegin`
   whose manifest is a `BulkRoute` naming the shard that hosts the group. A full bulk queue is
   the flow-control window: a shed chunk is offered again after ten milliseconds, never
   dropped, until the deadline; a cancellation from openraft is selected against every wait.
3. **End**, an RPC saying every byte was sent. The receiver waits until its prefix is whole,
   verifies the checksum, writes the pending marker, hands the file to
   `Raft::install_full_snapshot` under the sender's vote, and answers `Installed` once the
   install is durable - or `Resume { from }` the moment the lane feeding it is lost, the prefix
   stands still for a second, or the sender's deadline is reached, after which the sender
   streams again from `from`. An end asked again after its answer was lost with the link is
   answered `Installed` from the receiver's record of the last stream it installed.

The begin and the end RPCs are sent again after a lost link rather than failing the transfer;
a proposal wants a definite non-answer at once, a transfer is measured in its own deadline.

The receiving side is `shard/snapshots.rs`. The bulk lane lands on whichever shard the kernel
chose and relays each chunk as `ServerMsg::SnapshotBytes` to the shard the route names; a lane
that ends broadcasts `BulkLaneEnded`, so a partial fed by it stops waiting. That shard keeps at
most one `Partial` per group, in `wal/Shard-N/install/<group>.part`, and a writer task per
partial drains its queue into the file. The judgement is the `Assembler`'s, pure and tested
alone: a chunk at exactly the next offset is written and folded into the checksum, one wholly
below it is a duplicate and counted, anything else is dropped and counted - one TCP stream
delivers in order, and the resume offset is what recovers a drop. A begin for a different stream
on a group with a partial - a source that failed over - replaces it.

### Atomic durable installation

`GroupMachine::install_snapshot(Received)` posts `ServerMsg::InstallSnapshot` to the loop and
waits, never on the loop's own task. The loop:

1. Has the **pending marker** already: `install/<group>.pending`, the manifest in postcard,
   written with `write_atomic` by the end task *before* openraft was told. From here the install
   is redone at open until step 5. Crash points `BeforePending` and `PendingWritten` sit either
   side of it.
2. Marks the group **installing** (`MachineState::installing`), which the read path and the
   report read.
3. Persistent: `CompactionJob::Install` - the compactor validates every record as a partition of
   its row type and of a covered tablet, writes it into the active archive with the same size
   prefix and map intent a compaction writes, logs `MapIntent::Remove` for every covered
   partition the file does not name, syncs the data and then the map intent log, repoints the
   map, and answers `SnapshotInstalled` with the trailer. `MidInstall` after the first record
   and `MapSaved` after the repoint. Volatile: the file is read on a task and the loop puts its
   records in the ephemeral table in place of the covered tablets' partitions.
4. On `SnapshotInstalled`: evicts every resident partition of the covered tablets
   (`evict_tablets`, whatever generation they were stamped with, and marks any read in flight
   for one of them stale so it is asked for again from the new map entry when it lands), moves
   `applied` and the checkpoint to the boundary, both memberships to the manifest's, re-seeds
   the retry table from the trailer, sets `snapshot_at`, and writes the checkpoint file - the
   retry sidecar first, as ever. `BeforeCheckpoint` before the write, `AfterCheckpoint` once it
   landed.
5. On the `CheckpointWritten` that carried it: removes the marker, syncs the directory, removes
   the file, clears `installing`, answers the state machine, counts `installed`.
   `AfterCleanup` at the end.

At open, `scan_pending` finds every marker: one whose file verifies and whose boundary is past
the group's checkpoint is handed to the group's state as `pending_install`, and
`get_current_snapshot` answers it; openraft's own startup restore then sees a snapshot past
`applied` and calls `install_snapshot`, which is the same path as a live install, on the task
that builds the group. A marker at or below the checkpoint is cleaned up; one whose file does
not verify is dropped with a warning, since the marker precedes every archive write and the old
generation is whole; a partial with no marker is a stream that never finished. Redo is
idempotent: a record written twice is a new archive copy and a repoint, a removal of an absent
key is nothing.

The crash points are a process-global `AtomicU8` (`replication/install.rs`), armed by
`ShoalPool::crash_at` and the fixture's `CRASH_AT <name>` or a staged `crash_at`, checked with
one relaxed load at each step, exiting with 137 the way a kill does. Off unless armed, behind
no feature. `hold_install` pauses every install after its first record, which is how a group
is held `installing` long enough to read through.

### Per-tablet eligibility

While a group is installing, a read of any of its tablets through the shard is answered
`Unavailable` naming the group - `installing_group` is checked in `execute_query` on a cluster
node, after a strong read's barrier - and every other tablet on the shard is served. A write
proposes as ever: the log is not what installs. `GroupReport::installing`, the `Replication`
admin read's `installing` count and readiness's `data.replication.installing` all say so.

### Retention in bytes

`replication.retained_bytes` (a gibibyte, validated at two segments or more) bounds the sealed
segments the WAL keeps. Every sweep past it walks the oldest sealed segments until what is left
fits and, for each group with frames in them that is compacted past the segment and not yet
purged past it, asks openraft to snapshot at its checkpoint and purge through it
(`enforce_retention`, `SnapshotStats::forced`, a `WARN` with the lag each time). The entries
budget is a preference and the bytes budget is a bound. A member behind the forced purge point
falls to the snapshot path; nothing pins the leader's log for a follower. A hot stream that
cannot catch up inside its budget installs a snapshot at S, needs S+1 onward, may find those
purged by then and be sent the next snapshot; every such round is counted and logged, and the
report's lag is what an operator reads - a backlog that grows is visible as one rather than
reported as convergence.

Two things the sweep also does now, found on the way: a checkpoint that becomes durable asks
its group to snapshot, because openraft's own policy is judged as entries commit and a
checkpoint that landed after the writes stopped was never snapshotted or purged behind; and
every rotation carries every group's vote, committed and purged markers into the new segment,
because a segment holding only a group's newest purge marker was deleted as empty and the
group opened with its purge point forgotten and a log starting at an index it no longer had.

### What the fixture can do now

The builder gains `checkpoint_entries`, `retained_entries`, `segment_bytes`, `retained_bytes`,
`snapshot_chunk_bytes` and `bulk_queue_bytes`; a staged node gains `crash_at` and
`install_hold_ms`; the child answers `SNAPSHOT <group>` with the manifest of a cut taken now
and `CRASH_AT <name>`; the proxy link gains `throttle(bytes_per_second)`, which is what makes a
stream take seconds on a loopback that would carry it in milliseconds. The `DIGEST` verb reads
archived partitions beside resident ones, since after a restart or an install nothing is
resident; `GROUPS` carries `installing`, `compacting` and the `snapshots` counters.

### The catch-up arms

Two workloads, `macro/cluster/catchup/log` and `macro/cluster/catchup/snapshot`, the kill arm's
shape on the durable replication placement with its fault unchanged - node one killed a third
of the way through and started again two thirds through - differing in the server's retention:
`log` at the defaults, `snapshot` with `checkpoint_entries` 16 and `retained_entries` 32, so the
returning node is past the purge point once the survivors have served writes without it.
`Workload::catchup` tells the harness to sample the returning node's groups from the
`Replication` admin read each second after the restart mark, its lag judged against node zero's
committed index per group; the record is `cluster.catchup: CatchupFacts` - how it caught up,
the restart and convergence marks, the seconds between them, the bytes and entries by log and
by snapshot, and the per-second lag series - beside `cluster.fault`, mirrored into the explorer.
A run that ends before the lag is held at zero says `none` and keeps the series.

## Design choices

**The compactor cuts a persistent group.** The archive map is a consistent state only between
two of the compactor's jobs, and the compactor is the one task that knows where it stands.
Cutting on the loop would mean reading archives the compactor may be rewriting; pausing writes
would mean a pause. The cut costs one read and one write of every covered partition, on the
compactor's task, and the loop's `at_least` covers the one gap - a compactor that has merged
nothing since a restart, whose boundary is the checkpoint file's.

**The manifest travels beside the file.** A file that had to be read to be judged would be
read three times: to send, to verify, to install. The begin RPC carries the manifest to the
receiver, the marker carries it across a crash, and the file's header carries enough to catch a
file that is not the one the manifest names.

**The marker precedes openraft.** openraft purges the group's log through the boundary once it
holds the snapshot, and its own purge frame can land before anything the install writes. A
crash between that purge and the marker would leave a restart with neither the log it needs
nor a file it knows about. The end task writes the marker, then calls `install_full_snapshot`.

**Redo at open goes through openraft's own restore.** `get_current_snapshot` answers the
pending file when it is past the checkpoint, and openraft's `restore_from_snapshot` installs a
snapshot that is past `applied` before it re-applies anything. One install path, on the task
that builds the group, which is the invariant every `Raft` method call in the shard already
keeps.

**One partial per group, on disk, bounded.** `install_bytes` bounds what a shard holds in
partials, and a begin past it is refused naming the bound - the backpressure the receiver
applies. The writer's queue has a bound of its own (`INSTALL_QUEUE_BYTES`, sixteen mebibytes),
not the bulk lane's queue bound, which is the sender's flow-control window and may be set far
smaller; a chunk dropped at either bound is recovered by the resume offset.

**Resume is judged by the receiver.** The sender says every byte was sent; the receiver knows
what arrived. A lane that ended, a prefix that stands still for a second, or the sender's
deadline each answer the end with the prefix's length, and the sender streams again from
there. A stream id told apart from another's is what makes a resumed chunk a duplicate rather
than a corruption.

**The sender's vote, never the receiver's.** `install_full_snapshot` judges the vote it is
given as it judges an append. A receiver that has been electing itself while cut off holds an
uncommitted vote of its own; handing that to openraft trips its "leader vote is committed"
assertion. The partial keeps the sender's vote from the begin, and a stale one is refused with
the receiver's own, which is what a dead leader's stream deserves.

**Installing is per group, and a read is gated where it runs.** The group's tablets are the
unit a snapshot covers, and `installing` on the machine state is one cell the read path reads
before it touches the tables. What is resident during an install is the old generation and
what is on disk is half the new one; nothing between the two is a state a read may see.

## Alternatives rejected

**Chunked snapshots through openraft's own API.** This openraft has no chunked API and no
`begin_receiving_snapshot`; `full_snapshot` owns the whole transfer, and the wire already had
the bulk lane and its three frames from M2. The transfer is ours and openraft is told once.

**Sending the archives themselves, pinned.** Pinning every archive a group's partitions live in
would hold files the compactor wants to rewrite for the length of a transfer, and would send
every partition of every other group in the same archive. A cut file copies exactly the group's
partitions once; the copy is the cost of not pinning
([O52](../appendix/optimizations.md#o52-a-snapshot-copies-every-record-of-the-archives-into-one-file)).

**Installing into memory on a persistent table.** Simpler, and wrong: rows sitting in memory as
`Loaded` at the current generation are un-evictable until compacted and lost on a crash, since
the log entries below the boundary are not in the local log. The archive is the durable place,
and the compactor is the one writer of it.

**A bitmap assembler.** Chunks accepted at any offset into a bitmap of holes would tolerate
reordering the lane cannot produce; one TCP stream delivers in order, and a contiguous prefix
with a resume offset is the whole of what a lost lane needs
([O53](../appendix/optimizations.md#o53-the-assembler-keeps-a-map-of-received-chunks-and-forgets-them-on-a-restart)).

**Retention by time.** A time budget bounds nothing about bytes, and bytes are what a shared
WAL runs out of. The entries budget is what openraft keeps behind a snapshot and the bytes
budget is what forces it past that.

**Marking `installing` for the length of the stream.** The old state is consistent while the
bytes arrive; refusing reads for the transfer's whole length would refuse them for minutes on a
large tablet. Only the install itself - archive writes to eviction - is a window with no
consistent state to serve.

## Limitations

- **A snapshot is per group, and a group is every tablet a replica set shares.** On three nodes
  at a factor of three that is every tablet of the table on the node; a returning node
  installs the whole table, not the tablets it is behind on.
- **The unit half of the boundary test is a fixture test.** The compactor has no scaffolding to
  run alone, so `snapshot_has_one_stable_boundary_under_writes` cuts through a running cluster
  and reads the file back with the fixture's own parser; the oracle is the session tokens of
  every write.
- **A dead leader's stream may still install.** Bytes already in the lane's buffers arrive
  after the sender died; if they arrive whole before the receiver's vote moves, the install is
  run under the dead leader's vote and succeeds, and the new leader may send another. Two whole
  generations, never a mix, and `snapshot_duplicates_and_resume_are_safe` accepts either count.
- **Resume is within one process.** A partial survives a lane cut and heal; a receiver that
  restarts starts a stream over, since the assembler's state is not on disk.
- **Installs run concurrently across groups**, one per group, all through the table's one
  compactor queue for a persistent table.
- **Retention forces a group per sweep**, and a sweep runs every tenth deadline tick; the WAL
  can pass the budget by the segments written between two sweeps, which the retention test
  bounds at twice the budget plus the active segment.
- **A read parked on a partition load across an install is asked for again**, not answered
  from the old generation; a read that arrives during the install is refused. Nothing is
  served from a mix, and nothing is served from the old generation after the install.
- **The identity retry test goes through the leader's table.** The returning node's re-seeded
  retry table is exercised only when it leads; the fixture proves the identity survives the
  install through the node, not that the node's own table answered it.
- **Smoke numbers only.** The catch-up arms ran at smoke scale on the development host; the
  capture is the benchmark host's.
- ~~**Item 99 stays open**: a durable follower's log reversion still stops the leader's process,
  and is M8's.~~ Fixed at M8 ([Resolved #99](../appendix/resolved/durable-log-reversion.md)):
  the member is fed from the log or from this feature's snapshot.
- ~~**Item 106**, found on the way and filed: a member isolated on every lane long enough to
  inflate its term trips an openraft debug assertion in the control plane when healed.~~
  Resolved ([Resolved #106](../appendix/resolved/isolated-member-term-inflation.md)): an
  isolated member no longer stands. The retention test still cuts the data lanes alone, which
  is the shape the retention budget needs.

## Invariants to uphold

- **A cut is taken between two compactor jobs and nowhere else.** Its boundary is the highest
  position merged for the group or the loop's checkpoint, whichever is higher, and the file is
  exactly the archives' state at it. Nothing else writes archives.
- **A frame at or below a group's checkpoint is never handed to a compactor** (item 104). A
  snapshot cut inside a re-merge would carry the older generation under a newer boundary.
- **A volatile group's checkpoint is its applied position** (item 105), and never in the
  checkpoint file.
- **The marker is durable before openraft is told, and gone only after the checkpoint that
  carries the installed state is.** Between the two the install is redone at open. The
  checkpoint write that carries it is the one started after the state moved, or the next if one
  was in flight.
- **Absence is total.** Every partition of a covered tablet not in the file is removed on
  install, in the archive map and in memory.
- **The install is handed to openraft under the sender's vote.** Never the receiver's own.
- **A read of an installing group's tablets is refused; a write proposes.** The gate is
  `MachineState::installing`, set before the first archive write and cleared after the
  cleanup, including a redo at open.
- **Every rotation carries every group's markers into the new segment.** A sealed segment is
  never the only place a vote, a commit or a purge point lives.
- **A durable checkpoint past the last snapshot asks the group to snapshot.** Without it a
  quiet group never purges.
- **`install_snapshot_timeout` is `replication.snapshot_timeout`.** openraft's default is a
  fifth of a second.
- **The receiver's writer queue and the bulk lane's queue are different bounds.** One is the
  disk's, the other the sender's window; tying them together makes a small lane bound drop
  chunks on the receiver.
- **A bulk link forgets only itself on the way down.** A transfer holding an older link may see
  it drop after a newer one was opened for the same node.
- **The file checksum is chunk-invariant.** `FileHasher` folds fixed blocks; a streaming
  `GxHasher` would not agree between a writer and an assembler.
- **The fixture's DIGEST reads the archives.** A resident copy shadows the archived one.

## Performance

Both arms smoke-run on the development host (europa: `powersave` governor, three nodes of three
shards, one run each at a hundredth of the data, twenty-four second runs, the cluster's default
failover base of five seconds, `--allow-dirty` to a scratch directory that was deleted). **Not a
capture**: the shape of the record and nothing about magnitude on the benchmark host, and the
re-render is left for jove.

What the smoke run showed is that at that scale the arms cannot show catch-up, and the record
says so rather than pretending. Node one is killed at eight seconds and placed again at
sixteen; the survivors' elections at a five second base take ten to fifteen, so the client's
outage outlasts the absence (first failure at 8.0 s, recovery at 23.5 s on the snapshot arm and
unresolved on the log arm - the shape [F42](primary-failover.md#performance) recorded as item
103). Nothing was written while the node was away, both arms found it behind by the handful of
entries in flight at the kill and inside the log, and no snapshot was cut on either. The lag
then sat at one to three entries on the groups it had led until its old lease ran out, applied
moved at twenty-one seconds, and the run ended before the lag was held at zero: both arms
recorded `by: none`, `snapshots: 0`, an eight sample series and a `log_entries` of a few
hundred - the writes applied after the client recovered, not a backlog. The lag is judged
against node zero's committed index per group, since a returning node cannot know how far behind
it is until a leader tells it, and node zero's own report is what the arm reads for it.

*Found at M9a ([Resolved #108](../appendix/resolved/cluster-arm-overrides-dropped.md)): the
retention override never reached any node of either arm, node zero included - the harness
rebuilt every node's block from the defaults after resolving it - so the smoke runs above ran
at a thousand entries between snapshots and ten thousand kept, which is a second reason no
snapshot was cut. Fixed there; the arms have not been run again since.*

The snapshot arm's retention was moved from the plan's sixty-four and two hundred and fifty-six
to sixteen and thirty-two on the way: the mixture writes a few hundred entries to each of a
node's groups in the third of a run it is away at full scale, and the plan's numbers would have
kept the returning node inside the log at smoke scale even had the survivors been serving. The
path is priced at full scale, where the survivors serve writes for the last several seconds of
the absence, and proved by `returning_node_catches_up_by_log_or_snapshot`, which drives a node
past the purge point on purpose.

## Tests

| Test | Where | What breaks if this is reverted |
| --- | --- | --- |
| `returning_node_catches_up_by_log_or_snapshot` | `shoal/tests/cluster_fixture.rs` | A node inside the retained window installs a snapshot, one past the purge point never converges or installs nothing, the ephemeral table is not installed, a read through it is stale, or an identity from before the kill is applied as new |
| `snapshot_has_one_stable_boundary_under_writes` | `shoal/tests/cluster_fixture.rs` | A cut taken between two compactions holds a key at a value other than the last write at or below its boundary, a deleted key, or a write after the boundary |
| `snapshot_install_is_atomic_at_every_crash_point` | `shoal/tests/cluster_fixture.rs` | A node killed at any of the seven points comes back with a mix, fails to redo an interrupted install, leaves a marker behind, or never converges |
| `snapshot_duplicates_and_resume_are_safe` | `shoal-core/src/server/replication/install.rs`, `shoal/tests/cluster_fixture.rs` | Unit: a repeated or reordered chunk is written or the resume offset is not the prefix; fixture: a stream cut mid-way never resumes or installs twice per cut, or a sender killed mid-stream leaves a mix |
| `installing_tablet_never_serves_partial_state` | `shoal/tests/cluster_fixture.rs` | A read of an installing tablet is served, a read of another table is refused, `GROUPS` or readiness does not count the install, or the value after the install is old |
| `retention_and_recovery_memory_are_bounded` | `shoal/tests/cluster_fixture.rs` | A cut follower pins the leader's WAL past the budget, no purge is forced, the leader's memory grows past the bound, or the follower does not install once healed |
| `down_within_grace_moves_no_replicas` | `shoal/tests/cluster_fixture.rs` | A `Down` verdict moves a placement, a returning member is not `Up` in the same placement, catches up without a snapshot, or leads before an election |
| `whole_cluster_restart_preserves_durable_history` | `shoal/tests/cluster_fixture.rs` | An acknowledged key is missing after every node restarts, an unknown key holds two values, the digest changed, or a segment below a checkpoint is compacted again |
| `restart_does_not_recompact_segments_below_the_checkpoint` | `shoal/tests/cluster_fixture.rs` | Item 104 returns |
| `a_volatile_group_purges_its_log` | `shoal/tests/cluster_fixture.rs` | Item 105 returns |
| `a_snapshot_file_round_trips_and_a_torn_or_foreign_one_is_refused` | `shoal-core/src/server/replication/snapshot.rs` | The file format or the manifest verification changes shape, or the checksum stops being chunk-invariant |
| `frames_at_or_below_the_checkpoint_are_not_handed_again` | `shoal-core/src/server/wal/tests.rs` | `frames_in` hands a frame at or below the checkpoint |
| `markers_survive_the_deletion_of_their_segment` | `shoal-core/src/server/wal/tests.rs` | A group's purge point, vote or committed position is forgotten once the segment they were written in is deleted |
| `the_replication_block_parses_with_its_defaults`, `validation_refuses_what_is_not_built` | `shoal-core/src/server/conf/cluster.rs` | The four settings lose their defaults or their bounds |
| `catchup_capture_records_convergence` | `shoal-bench/src/workloads/harness/catchup.rs` | A catch-up record loses its marks, its series or its split by path, or an F42 record stops loading |
| `the_catchup_arms_share_the_replication_placement` | `shoal-bench/src/workloads/cluster_catchup.rs` | An arm drifts off the durable arm's placement, the snapshot arm's retention is not below the log arm's, or the ids leave registry order |
| `acceptance_tables_have_unique_tests_and_valid_milestones` | `shoal-bench/tests/acceptance_tables.rs` | An M7 row's test stops existing |

## Related

[C7. Primary failover and recovering a node](../distributed/failover.md),
[C5. Replication](../distributed/replication.md), [C2. The transport](../distributed/transport.md),
[C6. Reads](../distributed/reads.md), [C9. Operations](../distributed/operations.md),
[C10. Performance](../distributed/performance.md), [C13. The protocol](../distributed/protocol.md#q3-and-q9-at-m7),
[F40. Replication and quorum writes](replication.md), [F42. Primary failover](primary-failover.md),
[F38. The inter-node transport](inter-node-transport.md),
[Resolved #104](../appendix/resolved/segments-recompacted-after-restart.md),
[Resolved #105](../appendix/resolved/volatile-groups-never-purged.md),
[Storage: recovery](../storage/recovery.md), [Storage: compaction](../storage/compaction.md),
[Configuration](../getting-started/configuration.md).
