# F40 — Replication and quorum writes

## Context

[M4](../distributed/milestones.md#m4-replication-and-quorum-writes) is the milestone where a
write stops being one node's word. After [F39](membership.md) a cluster agreed on who was in
it and where every tablet lived, but every tablet lived in one copy: the placement primary
took the write, its own intent log made it durable, and a replication factor above one was a
number the topology reported beside the one it served. The gate asks for the embedded data
protocol chosen at [C13](../distributed/protocol.md), table-qualified logical histories over a
WAL adapter, persisted term and vote, distinct durable, commit, apply and checkpoint positions,
one command serialization for both storage engines, committed-order mutation and results,
bounded pending state, duplicate-safe acknowledgements and rotation, a default fsynced quorum
with `All` optional and the unimplemented policies refused by name, and `One` reads that see
committed state - fourteen acceptance rows across [C4](../distributed/tablet-map.md),
[C5](../distributed/replication.md), [C6](../distributed/reads.md) and
[C10](../distributed/performance.md), plus the library's storage conformance suite. The
evidence line wants healthy convergence across tables and restarts, exact durable evidence
before success, bounded lag under a slow follower, and standalone against RF=1 against a
feasible RF=3 with matching semantics, with no universal replication multiplier claimed.

Three questions the protocol page had left open were settled on the way, and this page records
them as decisions: **Q2**, the envelope, is a format 2 frame whose header carries the whole
log id so an index can be rebuilt from headers alone; **Q3**, the log adapter, is one shared
WAL per shard with every group's log multiplexed into it and one `fdatasync` per batch across
groups; and **Q4**, speculation, is *none* - a command is applied once, in committed order, on
every replica, and its result is derived there. One decision the plan made was changed by the
first test that ran against it: a write is proposed by the replica the node holds rather than
forwarded to the placement primary, because a replica knows the group's leader whatever the
placement says, and an isolated primary must not take every write to its tablets down with it.

## What it does

**Every tablet has a Raft group, and groups are shared.** A tablet `t` under a placement of `N`
nodes and a factor `rf` has replicas on `placement[(t + k) % N]` for `k` below
`min(rf, N)`, each on shard `(t / N) % shards` of its node - the shard that would own the
tablet were that node primary ([`TabletMap::replicas_of`](../../../shoal-core/src/server/map.rs)).
Tablets whose replica lists are the same ordered vector of `ShardAddr`s share one group per
table, identified by `GroupId::of(table, members)`, the gxhash of the two. At three nodes of
three shards a table has nine groups of 455 tablets each, and a node hosts all nine at a
factor of three or three at a factor of one - the shape the Q13 spike pointed at, since
per-group heartbeats do not coalesce and a group per tablet would be four thousand of them.
Every node computes the same groups from the same map; a shard builds the ones that name its
own address and stops the ones a newer map does not.

**The Raft log is the WAL.** A cluster node keeps no per-table intent log. Every group a shard
hosts appends its entries as format 2 frames into one physical log under
`<latency_sensitive.path>/wal/Shard-N/`, in segments named by generation; the writer task takes
whatever batch is open, writes it with one `write_at`, syncs it with one `fdatasync`, and only
then completes every `IOFlushed` in it. That completion is what openraft counts toward a
quorum, which is what [P3](../distributed/protocol.md#the-contract) requires of a durable vote:
a majority's fsyncs, distinct replicas each counted once, local durability counted once. The
frame is `[len u32][gxhash32][kind][version=2][flag][reserved][group u64][index u64][term u64][leader ShardAddr]`
and a body - a `Normal` entry's command, a `Membership` entry's configuration, or nothing for
a `Blank`, `Vote`, `Committed`, `Purged` or `Truncate` record. A torn tail is cut at open; a
later frame supersedes an earlier one at the same index; a segment is replayed whole in
generation order and never edited. An ephemeral table's groups keep the same log in memory
(`MemoryWal`), bounded by `volatile_log_bytes` and gone on restart, which is the volatile
policy C5 names and the topology reports.

**A write is one command, applied once, in order.** A shard that accepts a write builds the
table's intent once - the same `UnsortedIntents`/`SortedIntents` bytes the standalone intent log
stores - and wraps it as a `Command { table, tablet, request: {bundle, index}, payload }`. The
command is proposed through the group; every replica applies it to the table when the group
commits it, in log order, without any storage commit of its own, and derives the result -
inserted or not, deleted or not, updated or not - from the state it finds. A conditional
result is therefore the same on every replica, because every replica applied the same commands
in the same order. A partition a delete or an update needs from disk parks the batch until the
read lands and applies again; the parked batch blocks the group behind it and nothing else.
`One` reads are served from the local replica's applied state, so a follower cut off from its
leader answers with what was committed before the cut and nothing that was only appended.

**The client is answered by evidence.** A proposal is answered `Applied` once the group
committed it *and this shard applied it*, so a read through the same connection sees the
write; `Duplicate`, answered as the first time, when the request identity was seen with the
same payload digest; `Refused` when the digest differs. Past `replication.write_timeout` it is
`OutcomeUnknown`, which is what it is: the command may commit later. A group with no leader
this shard can reach is `NotLeader`; a shard whose pending bytes for the group would pass
`pending_bytes`, or whose volatile logs would pass `volatile_log_bytes`, sheds the write
`Shedding` before recording anything. Under `write_consistency: All` the answer also waits
until every voter's matched index covers the entry, read from the group's metrics, and no
`Down` verdict shrinks that set. `One` writes are refused at validation naming the
accepted-or-pending API they would need; a persistent table configured `Async` on a cluster
node is refused at `ShoalPool::start` naming C5.

**Where a write goes.** A node routes a query to its own replica of the tablet when it holds
one, and to the tablet's primary when it does not - the same ring for reads and writes
([`TabletMap::read_ring_for`](../../../shoal-core/src/server/map.rs)). A replica that is not
the leader proposes through the leader with one hop, a `Propose` request on the replication
lane to the leader's shard, whose answer carries the outcome back; a proposal that already
hopped may not hop again. The leader is preferred on the placement primary: the primary
initializes a fresh group, which openraft answers with an election, and every other member
holds its own candidacy for two election timeouts before standing. After that any member may
lead, which is what a failover needs and what `duplicates_gaps_and_old_terms_do_not_reapply`
exercises. The timers derive from `primary_failover_after`: a heartbeat every tenth of it, an
election between one and two of it.

**Checkpoints, compaction and purge.** A group's *checkpoint* is the last log id whose effect
its table's archives hold, written to `wal/Shard-N/checkpoint.json` with the membership as of
it, and it moves only when the compactor says so. A sealed segment is *resolved* once every
group with frames in it has applied past them; the loop hands its command frames, per table
in log order, to that table's compactor as `CompactionJob::Segment`, and when the compactor is
through the checkpoints of every group in it advance and the checkpoint file is rewritten.
openraft's snapshot is the checkpoint - metadata, since the archives are the rows - taken every
`checkpoint_entries` and only once the checkpoint is on disk, and the log behind it is purged
past `retained_entries`; a segment is deleted once every group purged past it and nobody is
compacting it. A speculative suffix - entries a leader appended while isolated - is never
resolved, because the group never applies it: the majority's leader truncates it and the
frames become dead bytes the next sweep ignores.

**The replication lane.** A fourth lane on the data port, `Lane::Replication`, carrying
`Replicate` and `ReplicateResponse` frames: a 24-byte head naming the correlation id, the
group, the target shard, the kind (`AppendEntries`, `Vote`, `Propose`, `Snapshot`) and the
deadline, and a postcard body. A shard holds one link per peer node with its own correlation
table, bounded by `transport.replication_queue_bytes`; a refused append is one openraft
retries. A request for a group the receiving shard does not host, or for a kind this build
does not serve - `Snapshot` is M7's - is answered by name.

**Configuration.** A `replication:` block under `cluster:`, node-local, with every default
written on the [configuration page](../getting-started/configuration.md#cluster):
`write_timeout` 5s, `pending_bytes` 64 MiB, `segment_bytes` 10 MiB, `checkpoint_entries`
1024, `retained_entries` 10000, `log_cache_bytes` 16 MiB, `volatile_log_bytes` 256 MiB; and
`transport.replication_queue_bytes` 64 MiB beside the other lanes' bounds. Validation refuses
`write_consistency: One`, a `write_timeout` past `forward_timeout`, and a
`primary_failover_after` under 100 ms. A cluster node's storage directory is claimed at
layout 2 - the WAL directory in place of the intent logs - and a standalone one at layout 1,
so a directory started the other way is refused by name.

**What the operator sees.** `AdminKind::Replication` on the client connection, and
`ShoalPool::replication()` in process, report every group a node hosts: its table, members,
leader, applied, committed and last-log indexes, checkpoint, purged index, pending bytes and
whether it is volatile and up; folded per node into groups hosted, groups led, the widest
committed-to-applied gap, pending and volatile bytes, and how many writes were answered
unknown or rejected. Readiness carries the same fold. The cluster fixture drives it through
`GROUPS`, `DIGEST <table>` - rows and a hash over every replica's applied state, which is what
every convergence test compares - `ROTATE`, `COMPACT`, and `STALL_WAL <group>` /
`RELEASE_WAL <group>`, which hold a group's flush completions on a follower so a test can
build a quorum that is short by exactly one durable voter.

**Benchmarks.** Three arms and a record. `macro/cluster/overhead/nodes/3` is the reference
mixture on three nodes of three shards replicating to nobody; `macro/cluster/replication/durable`
is the same placement at a factor of three on the persistent table, and
`macro/cluster/replication/volatile` on the ephemeral one. Every cluster record now carries
the load the arm scheduled (`offered_load`), every replica's state when the run ended - groups,
groups led, lag, pending and volatile bytes, unknown and rejected writes - and the outcomes
summed, mirrored into the explorer's index. An arm asking for more copies than it places nodes
is refused before a server starts, which is the C10 row.

## Design choices

**One Raft group per replica set, not per tablet.** The map already places tablets by a rule
that makes a tablet's replica list a function of `t mod N` and `(t / N) mod shards`, so the
number of distinct lists is `N × shards` per table whatever the tablet count. A group per
tablet would be 4096 groups per table per node with a heartbeat each; a group per list is nine
on the benchmark placement. The cost is that a tablet cannot move alone - moving one means
moving its group, or splitting it - which is M9a's problem and is filed there. The identity is
the hash of the table and the members so that every node derives the same id from the same
map without agreeing about anything first.

**The WAL is the log, and the log is shared.** C5 asked for logical histories independent and
physical writes multiplexed, and Q3 asked whether the adapter could honor openraft's
`RaftLogStorage` contract on top of that. It can: `append` stages frames into the open batch
and returns, the cache holds the unflushed tail so openraft can read what it just appended,
and `IOFlushed` fires when the batch's `fdatasync` returns. A per-group file would have cost a
sync per group per batch and a directory that grows with the placement; a per-table log would
have put two groups' histories into one stream with nothing to tell them apart on replay. The
frame names its group, and that is the whole of the multiplexing.

**Apply once, in committed order, derive the result there.** The standalone tables mutate
memory before their intent is durable and park the result; a replicated table cannot, because
a follower applying an uncommitted entry would expose it to a `One` read and a conditional
result computed before commitment would be a guess. The tables gained a second entry point,
`apply(command, generation, skip_disk)`, that mutates the same partition the same way and
computes the same `bool`, with no storage commit and the result returned rather than parked.
Speculation stays an optimization Q4 would have to specify; nothing here does it.

**A write goes to the local replica, not the primary's node.** The plan routed writes by the
placement's primary ring. The first isolation test showed why that is wrong: a node holding a
follower of the group would forward its write over the data lane to a primary that was cut
off, and answer `Unavailable` while the majority it belonged to had already elected a leader
it could have reached. A replica proposes through its group and follows the leader one hop;
only a node holding no replica needs the placement's word. The cost is one hop when the local
replica is not the leader, over the replication lane instead of the data lane, and it is the
same hop the primary would have taken.

**The primary initializes; the others wait.** openraft's `initialize` writes a membership
entry whose log id names the node that wrote it, so two members initializing the same group
each hold a different entry at index zero and refuse each other's votes until the greater
address wins - which the first attempt did, and every group's first leader was the node with
the largest id. Now only the placement primary initializes a fresh group, and a member whose
primary never did initializes it itself after two election timeouts, so a group whose primary
is absent at first start still comes up.

**A volatile follower may come back empty; a durable one may not.** openraft treats a follower
whose log shrank as a bug and stops the leader on it. That is the right verdict for a durable
group, where a shorter log means an acknowledgement was lost, and the wrong one for an
ephemeral table, whose members lose their log on every restart by design; a volatile group's
configuration allows the reversion and a durable one's does not. What the leader does with a
durable reversion is [item 99](../appendix/known-issues.md#99-a-durable-followers-log-reversion-stops-the-leaders-whole-process).

**Bounds are per group and definite.** `pending_bytes` is counted per group on the proposing
shard and a write past it is shed before it is recorded, so a stalled group holds a bounded
amount of memory and nothing else waits on it - `slow_tablet_does_not_block_other_tablets`
holds both followers' flush completions of one group and writes to another at the same time.
`write_timeout` turns a proposal that did not commit into an unknown outcome rather than a
queue; `volatile_log_bytes` bounds every in-memory log together; `replication_queue_bytes`
bounds what one link will hold for a follower that is not reading.

**Dedup is an LRU with a digest, for now.** A group remembers the last 4096 request
identities and the result each produced, keyed by bundle id and index, with the payload's
digest beside it; a retry with the same identity is answered as the first time, a different
payload under the same identity is refused. The table is rebuilt from the log on restart and
is not part of the checkpoint, which is why M6 owns the durable low-water mark that would make
the bound a promise.

**Timers from one setting.** `primary_failover_after` was recorded and unused since F37; it
is now the base every group derives its heartbeat and election range from, so the policy every
node agreed about decides how fast a leader is missed. The fixture sets it to a second.

## Alternatives rejected

- **The placement primary as the only proposer**, with every write forwarded to it over the
  data lane. Rejected by the first isolation test, above: a cut primary made every tablet it
  led unwritable through every node, while the group had a leader.
- **A group per tablet.** Four thousand heartbeats per table per node; the Q13 spike priced
  per-group heartbeat traffic and it does not coalesce. Groups are per replica set, and the
  tablet map's rule is what makes those few.
- **A log file per group, or per table.** One sync per group per batch, or two histories in
  one stream; see the WAL choice above.
- **Speculative apply with rollback.** Q4 asked for dependencies, rollback and separate
  committed visibility to be specified first; nothing was, and committed-order apply is what
  every test compares digests against.
- **Deriving results before commitment**, as the standalone tables do. A conditional insert
  answered from uncommitted state is answered differently on the replica that applies it later;
  the result is derived where the effect is applied.
- **A second, weaker commit rule beside the library** for `All` or for a `Down` member.
  `All` reads the group's own per-member matched index; a `Down` verdict changes admission and
  nothing about a quorum.
- **JSON for every persisted record.** A membership carries a map keyed by a shard address,
  which JSON cannot key; membership frames are postcard, and the checkpoint file spells the
  membership out as lists so it stays readable.
- **Direct I/O for the WAL.** The intent log writes O_DIRECT through a `DmaStreamWriter`; the
  WAL uses a `BufferedFile` with `fdatasync`, since a batch of frames from many groups is not
  block aligned and the sync is what the quorum counts. What that costs is
  [O46](../appendix/optimizations.md).

## Limitations

- **A member behind the purge point cannot catch up.** `install_snapshot` and the `Snapshot`
  kind on the lane are refused naming M7. Within `retained_entries` a lagging member is fed
  from the log; past it, it is stuck until M7 transfers the archives.
- **A node holding no replica of a tablet routes its writes to the placement primary's node.**
  When that node is down, those writes fail `Unavailable` until the map moves - which nothing
  does before M6's failover; a node holding a replica is unaffected.
- **Dedup is bounded by count, not by a durable session mark.** 4096 identities per group, in
  memory, rebuilt from the log; an identity older than that is applied as new. M6.
- **A checkpoint is per table, and the whole file is rewritten.** A shard with many tables
  rewrites every group's line when one moves; the file is small and the write is atomic.
- **The proposer's answer waits on its own apply**, so a write through a follower costs the
  leader's commit plus the follower's apply. That is what read-your-writes over `One` reads
  needs, and it is a latency every write through a non-leader pays.
- **`All` is judged from metrics.** The proposer polls the group's replication progress until
  every voter's matched index covers the entry; it is correct and it is a poll.
- **The report a peer's control plane folds is at most a tick behind its shards**, and the
  benchmark harness reads it two ticks after the run, so a replica's `lag_end` is where it was
  a moment after the client stopped rather than at the instant.
- **`overhead/nodes/3` is not `overhead/nodes/1` plus two nodes**: the machine leaves eleven
  free cores, so three nodes run three shards each where the one-node arm runs twelve. The
  three-node arms are read against each other.
- **Closed-loop arms only.** The open-loop schedule C10 asks for, which is what would expose
  a lag that grows under sustained load, is filed in [todos](../appendix/todos.md).
- **Isolation of a leader is a timeout, not a step-down.** An isolated leader learns it is
  not one when its lease expires; until then a write through it is `OutcomeUnknown` at the
  write deadline. M6 makes that a `NotLeader` at the lease.
- **Group leadership after a failover is wherever the election put it.** The primary
  preference is a head start at first start and nothing after; a leader on a non-primary stays
  there. M5.
- **The capture is the benchmark host's.** The arms ran at smoke scale on the development host;
  the numbers below prove they run and record, and nothing else.

## Invariants to uphold

- **An `IOFlushed` fires after the `fdatasync` of the batch that holds its last frame, never
  before.** Everything openraft counts as a durable vote rests on it; the stall gate the fixture
  uses holds exactly these completions and nothing else.
- **`append` returns before the flush, and the cache holds the unflushed tail.** openraft reads
  an entry the moment `append` returns, and reads below the durable watermark come from the
  file. Evicting an entry before its bytes are durable is a read of nothing.
- **A frame is never edited in place, and a later frame at an index wins.** Replay is a walk of
  every segment in generation order; a truncate record drops what is above it at the point it
  appears, and an append after it supersedes.
- **Only the shard loop touches a table, and `apply` is only ever awaited off the loop.**
  `Raft::new` re-applies the checkpoint to the committed index on the caller's task, and the
  machine's `apply` posts to the loop and waits; a group started on the loop deadlocks on
  every restart. Groups start on spawned tasks and the loop only receives.
- **A segment is handed to a compactor only when every group in it applied past its frames,
  and deleted only when every group purged past them.** The first keeps an uncommitted suffix
  out of every archive; the second keeps a lagging member's history until openraft says it is
  not needed.
- **The checkpoint moves when the compactor says so, and the snapshot is taken only once the
  checkpoint is on disk.** A snapshot at a checkpoint that a crash could roll back would let
  openraft purge log the archives do not hold.
- **The response is derived on apply, from committed state, and a duplicate identity is
  answered as the first time.** A path that answers a client from anything applied before
  commitment, or reapplies a remembered identity, breaks C5's contract and
  `conditional_results_follow_committed_order`.
- **Only the placement primary initializes a fresh group.** Two initializers give two index-zero
  entries and a term war the greater address wins; a member that initializes because its
  primary never did waits two election timeouts first.
- **Nothing holds a `RefCell` borrow across an `.await` on the shard's executor.** The writer
  task, openraft's core, its state machine worker and the loop share one executor and one cell.
- **A write's admission is judged before anything is recorded, and every refusal is definite.**
  `Shedding` and `NotLeader` mean nothing was proposed; `OutcomeUnknown` means something may
  have been. A path that sheds after proposing turns a definite refusal into a lie.
- **`replication.write_timeout <= transport.forward_timeout`.** The node that forwarded a
  write answers its client unknown at the forward deadline and drops a later answer.

## Performance

**Nothing here is a capture.** The three arms ran at smoke scale on 2026-09-12 on the
development host (`europa`, 32 threads, `powersave`, `--allow-dirty`) against a scratch copy
of `shoal.yml` with local storage, two runs each, written to a scratch directory and deleted.
Every record carried three replicas at map version 9, `active_rf` 1 or 3 as asked, 36 groups
per node at a factor of three (four tables over nine replica sets) and 12 at a factor of one,
`lag_end` 0 and `pending_bytes_end` 0 on every replica, and `outcomes` of 0 unknown, 0
rejected. 190 timed queries each after warmup, 32 outstanding, medians of two runs:

| Arm | Write p50 / p90 / p99 | Read p50 / p99 |
| --- | --- | --- |
| `overhead/nodes/3` (rf 1, persistent) | 24.8 / 44.5 / 51.5 ms | 0.28 / 25.1 ms |
| `replication/durable` (rf 3, persistent) | 52.3 / 72.8 / 80.8 ms | 0.19 / 0.74 ms |
| `replication/volatile` (rf 3, ephemeral) | 1.69 / 19.1 / 20.5 ms | 0.60 / 18.5 ms |

What the three numbers say against each other, and nothing else: a durable quorum on this
host's device cost about twice a single fsync at the median, which is what a leader's sync
followed by a follower's sync into the same device queue would cost if the two were not
overlapped; a volatile quorum cost under two milliseconds at the median with a tail at twenty,
which is the lane, the round trip and whatever the `powersave` governor did to a core that
woke to answer; and the read medians did not move, since a `One` read is the local copy on
every arm. Whether the leader's own flush and its followers' are pipelined is a question the
benchmark host answers and [O47](../appendix/optimizations.md) records. The rf 1 arm's write
median is within the range the standalone reference cell shows on this device on this host.

**What replication costs a standalone node is nothing.** A node without a `cluster:` block
builds no groups, opens no WAL, and takes the write path it always took; the routing check
is one `Option` on the shard.

**What it costs a cluster node at a factor of one** is a proposal through a group of one: a
frame into the shared WAL, a batch, a sync, an apply on the loop. The intent log it replaced
did the same work with a sync per table.

## Tests

| Test | What breaks if the feature is reverted |
| --- | --- |
| `cluster_fixture::quorum_success_requires_distinct_durable_voters` | The replication lane into both followers cut: a write through the leader is `OutcomeUnknown` at the deadline, three rotations and flushes of the leader's WAL release nothing; one follower healed, the write commits and reads back on both; the cut follower's digest differs |
| `cluster_fixture::bootstrap_does_not_reduce_configured_quorum` | Factor three on one node: readiness reports `active_rf` 1 and short default writes, an insert refused `QuorumUnavailable`; two joiners and `Initialize`: groups of three, the write admitted, committed and read on every node, `active_rf` 3 |
| `cluster_fixture::async_replica_cannot_weaken_durable_quorum` | A cluster node whose persistent table is `Async` refuses to start naming C5; a standalone `Async` node starts; `write_consistency: One` is refused at validation |
| `cluster_fixture::duplicates_gaps_and_old_terms_do_not_reapply` | One follower's lane delayed, cut and healed under writes led by node zero; a leader of other groups killed, writes through a survivor, the old one restarted at a stale term: every acknowledged key present exactly once on every node, digests equal |
| `cluster_fixture::uncommitted_suffix_never_enters_checkpoint` | The leader isolated and written through (`OutcomeUnknown`), its WAL rotated and compacted with nothing handed; the majority elects and writes; healed and restarted, the old leader holds the majority's value everywhere, digests equal, and only then does its segment resolve |
| `cluster_fixture::conditional_results_follow_committed_order` | Concurrent inserts, updates, deletes and no-ops on a small key set through all three nodes, every answer in a ledger the `shoal-model` oracle accepts, the converged reads accepted too, three equal digests |
| `cluster_fixture::slow_tablet_does_not_block_other_tablets` | Both followers hold one group's flush completions: writes to it pend to `OutcomeUnknown` or are shed `Shedding` at a 2 KiB bound, eight writes to another group led by the same node land in under a second, the leader's RSS stays bounded; released, the pended writes commit and apply |
| `cluster_fixture::volatile_replication_uses_common_encoding` | Rows of the ephemeral table written through one node read from the other two, the groups reported `volatile`; every node restarted, the rows gone and the persistent notes not |
| `cluster_fixture::one_reads_converge_without_exposing_uncommitted_state` | A cut follower serves the old committed value while the leader moves on, and converges when healed; the isolated leader takes a write it cannot commit (`OutcomeUnknown`) and a read through it does not show it; the majority writes; healed, every node converges on the majority's value |
| `wal::tests::data_store_passes_the_openraft_storage_suite` | The shared WAL and the memory log both pass openraft's `RaftLogStorage`/`RaftStateMachine` conformance suite, with the group machine stood in by a memory one |
| `wal::tests::rotation_preserves_pending_replication_requirements` | Appends from three groups across four forced rotations: every `IOFlushed` completes exactly once, every location names its generation, entries read back from a sealed segment |
| `wal::tests::table_streams_recover_independently_without_holes` | Two groups interleaved with one's completions held, the tail dropped unflushed, reopened: each log a contiguous prefix ending at its last durable index, no frame of one in the other's index |
| `wal::tests::checkpoint_file_round_trips_membership` | A checkpoint with a membership keyed by shard addresses writes and reads back equal; a checkpoint that could not be written killed the shard that tried |
| `wal::frame::tests::frames_round_trip_and_a_torn_tail_stops_the_decode` | Every frame kind round trips and a truncated tail stops the decode at the last whole frame |
| `map::tests::placement_respects_distinct_nodes_and_feasible_capacity` | Three nodes at three: every tablet on three distinct nodes and every node holding every tablet, the primary first; four at three: distinct nodes, three quarters each within one; a node with twice the shards spreading them evenly on the shard the primary rule picks; a factor past the placement served at the placement |
| `map::tests::write_admission_follows_the_policy_and_the_up_count` | The bootstrapper placed on itself at a factor of three serves one copy: `active_rf` is the smaller of the factor and the placement |
| `identity::tests::*` (`ShardAddr`, `GroupId`) | An address round-tripping its eighteen bytes; a group id the same from the same members and different from a reordered list |
| `protocol::peer::replicate::tests::*` | The replicate heads and the command envelope round-tripping, an oversized payload and an unknown kind refused, the digest stable |
| `meta::tests::a_mode_change_is_refused_both_ways` | A cluster directory at layout 2 refused by a standalone start and a standalone one at layout 1 by a cluster start, the mode named before the layout |
| `conf::cluster::tests::the_replication_block_parses_with_its_defaults` | Every field of the `replication:` block parsed in the sizes an operator writes, an empty block the defaults, an unknown field refused |
| `conf::cluster::tests::validation_refuses_what_is_not_built` | `write_consistency: One` refused naming C5; a `write_timeout` past the forward timeout refused; a failover base under 100 ms refused and one of 100 ms accepted |
| `cluster_replication::tests::the_arms_are_the_reference_cell_on_one_placement` | The three arms sharing the reference cell's every axis and one placement, differing in the factor and the table alone |
| `cluster_replication::tests::infeasible_rf_policy_is_not_a_throughput_arm` | Three copies on one or two nodes refused as an availability test; every declared arm feasible |
| `harness::cluster::tests::capacity_capture_records_lag_and_offered_load` | A record round-tripping the schedule, every replica's debt and the summed outcomes; a record from before them loading with none |
| `explore_index::the_facts_mirrors_are_total` | The schedule, the replicas and the outcomes reaching the explorer's `ClusterFactsLite` with the same keys |
| `acceptance_tables::acceptance_tables_have_unique_tests_and_valid_milestones` | M4 marked delivered forcing all fourteen rows to exist as functions |

The unit half of `volatile_replication_uses_common_encoding` the plan named - that a command's
bytes are the same for an ephemeral table and a persistent one - holds by construction:
`EphemeralUnsortedTable` is `PersistentUnsortedTable` with `NoStorage`, and `build_intent` is
one function on the one type. The fixture row is what tests it on the wire.

## Related

- [M4](../distributed/milestones.md#m4-replication-and-quorum-writes), the gate this delivers,
  and [C4](../distributed/tablet-map.md), [C5](../distributed/replication.md),
  [C6](../distributed/reads.md), [C7](../distributed/failover.md),
  [C10](../distributed/performance.md), [C11](../distributed/testing.md) and
  [C13](../distributed/protocol.md#q2-q3-and-q4-at-m4), the pages it changed
- [Configuration](../getting-started/configuration.md#cluster), for the `replication:` block
- [Storage](../storage/overview.md) and [Compaction](../storage/compaction.md), for what a
  cluster node's directory holds instead of an intent log
- [F39](membership.md), whose placement the groups follow, [F38](inter-node-transport.md),
  whose data port carries the fourth lane, and [F37](node-identity-control-plane.md), whose
  glommio runtime the groups run on
- [F36](cluster-harness.md), the fixture and the protocol model that judge the ledger
- [Item 99](../appendix/known-issues.md#99-a-durable-followers-log-reversion-stops-the-leaders-whole-process)
  and [item 100](../appendix/known-issues.md#100-duplicate_node_identity_is_fenced-fails-under-the-fixture-suite-at-full-parallelism),
  what was found on the way
- [O46](../appendix/optimizations.md), [O47](../appendix/optimizations.md) and
  [O48](../appendix/optimizations.md), what was deliberately not taken
