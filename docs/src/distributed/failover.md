# C7. Primary failover and recovering a node

## Context

A primary per tablet buys replicas that cannot diverge ([C5](replication.md#why-a-primary)) and
costs a window during which a tablet whose primary is gone accepts no writes. This page is about
that window — how it is detected, how it is closed, what it cannot lose — and about the other
side of the same event: a node that comes back and has to be brought forward to where the cluster
went without it.

It is also the page whose tests matter most. [D9](../direction/prior-art.md#foundationdb) said
that of everything in the prior art "a harness that can kill a connection underneath an in-flight
query is the single most valuable thing to copy". Every acceptance test here kills a *node*
underneath in-flight writes and asks what survived.

## What exists today

**Recovery is replay** ([Recovery](../storage/recovery.md)): sealed logs oldest first, then the
active log, every partition an update needs loaded before anything is replayed, then a forced
compaction. A shard recovers itself from its own files and asks nobody. "For a database with no
replication to fall back on, [fail-forward on corruption] is defensible" (`recovery.md`,
*Design notes*) — and once there is replication to fall back on, a truncated log is a shortfall to
be filled from a peer, not a loss to be logged.

**Compaction discards the log.** An intent log is sealed at `intent_log_size`, compacted into
archives, and deleted ([Compaction](../storage/compaction.md#5-delete-the-log)). The primary's log
is therefore a window onto recent writes, not a history, and a replica that fell behind by more
than the window cannot be caught up from the log alone.

**Nothing detects a dead shard** ([C3](membership.md#what-exists-today)) and nothing can retry a
write, because a write is not idempotent and the server has no key to recognise a repeat by
([TODOs](../appendix/todos.md), "An idempotency key, so a write can be retried";
[D6](../direction/connection-pool.md#retries)).

## The design

### When a primary is `Down`

`Down` is the cluster's verdict, committed by the leader on a majority's evidence
([C3](membership.md#the-state-machine-of-a-member)). From the moment it commits, a timer runs on
the leader's control plane; when `primary_failover_after` elapses and the node is still `Down`,
the leader proposes, for every tablet the node is primary for:

```rust
SetPrimary { tablet, new: ShardAddr, epoch: old_epoch + 1 }
```

**The new primary is the `Up` replica with the highest `(epoch, seq)` applied for that tablet.**
The leader knows each replica's stamp because every node reports, in its Raft heartbeat reply, the
highest applied stamp per tablet it holds — the same channel the detector's verdicts ride
([C3](membership.md#failure-detection)), and the second thing the `openraft` source has to be read
for. Between two replicas at the same stamp, the one on the node with fewer primaries wins.

**Why the highest replica holds every acknowledged write.** A write acknowledged at `Quorum` was
durable on `RF/2 + 1` replicas. Any `RF/2 + 1` of the `RF` replicas include at least one of them.
So among any quorum of `Up` replicas, the one with the highest stamp has every write any client
was told succeeded — the same argument Raft makes for its leader election, applied to a log the
primary already ordered. A write acknowledged at `One` was durable on the old primary alone and
may be on no `Up` replica; it is lost, and `One` was the caller's choice.

**What about the writes nobody acknowledged?** The old primary may have staged writes that
reached some followers and not a quorum. A follower that holds a suffix beyond the new primary's
stamp **truncates it** on receiving the `Topology` that names the new primary at the new epoch:
it rolls its partitions back by re-reading them from its archives and replaying its log only to
the new primary's stamp. Those writes were never acknowledged, so no client was told they
happened, and losing them is the protocol working. The truncation is expensive and rare — it only
happens to a follower that was *ahead* of the quorum, which under normal replication is a few
buffers' worth.

### Fencing

The old primary may be alive and partitioned rather than dead, and may still believe it leads.
Three things stop it from doing harm:

- **Followers refuse an old epoch.** A `Replicate` carrying `epoch < current` is answered with a
  `ReplicateAck` naming the current epoch and is not applied ([C5](replication.md#followers-apply-in-order)).
  The old primary cannot reach a quorum, so it cannot acknowledge, so its clients see
  `Unavailable` and retry elsewhere.
- **The lease lapses.** A primary answers `Primary` reads only while it has heard from the control
  plane within `primary_failover_after` ([C6](reads.md#three-read-levels)); a partitioned one has
  not, and refuses.
- **The old primary's own log is reconciled when it returns.** It rejoins as a follower of the
  new primary and truncates whatever it staged past the new primary's stamp, exactly as any
  follower would.

`epoch` is what makes all three cheap: a `u32` comparison at the top of every replicate and every
`Primary` read.

### The window, and what a client sees

Between the primary's death and `SetPrimary` committing — `primary_failover_after` plus the
detector's latency plus a Raft round trip — a write to one of its tablets is refused with
`Unavailable`, from the coordinator, because the map names a primary that is `Down`. The client
sees an error it may retry. **It may not retry a write on its own**, because a write is not
idempotent and the refused write may have committed on the old primary before it died
([D6](../direction/connection-pool.md#retries)). What would make the retry safe is the idempotency
key `todos.md` already describes — a per-write key the primary remembers long enough to answer a
repeat with the first answer — and this page records that as the reason the window is *visible*
to a caller and not merely a latency: until the key exists, the caller has to decide whether to
resend, and the honest error tells them so.

Reads at `One` continue throughout, from any `Up` replica. Reads at `Quorum` continue if a quorum
is `Up`. Reads at `Primary` fail with the writes. Writes to every other tablet — the ones this node
was a follower for — continue at `Quorum` on the remaining replicas, one short.

With the defaults, `primary_failover_after: "5s"` and a half-second detector interval, the window
is a little over five seconds. The knob is the whole trade-off, and [C12](prior-art.md#kafka) has
the cautionary tale: set it short and a slow disk elects a new leader, set it long and every
primary death is a long outage. Five seconds is Kafka's default session timeout and MongoDB's
default election timeout, chosen by people who have watched a lot of failovers.

### A returning node

A node that restarts — after a crash, after a partition, after `SIGSTOP` — recovers its shards from
its own files as today, rejoins the group, receives the current map, and then, **per tablet it
holds**, compares its own applied stamp with the tablet's current primary:

| Its stamp is | Then |
| --- | --- |
| Equal to the primary's | Nothing. It is a current follower |
| Behind, and the primary's log still holds `from_seq` | **Catch up by log**: `CatchUp { tablet, from_seq }`; the primary streams the missing records as ordinary `Replicate` frames, and the follower applies them in order |
| Behind, and the primary has compacted past `from_seq` | **Catch up by snapshot**: the primary streams the tablet's partitions — `StreamBegin { tablet, at_seq }`, one `StreamPartition` per partition it holds, `StreamEnd` — and then the log tail from `at_seq`. The follower drops its own copy of the tablet, installs the snapshot, and applies the tail |
| Ahead (it was the old primary, or a follower that was ahead of the quorum) | Truncate, as above, then it is equal |
| Its epoch is behind | It was primary and was replaced. Truncate to the new primary's stamp at the new epoch, then follow |

Where the row boundary between "by log" and "by snapshot" falls is decided by the primary, which
knows what it has compacted: a `CatchUp` for a seq that is in a sealed-and-compacted log is
answered with a `StreamBegin` instead of records. The follower asked one question and gets
whichever answer is possible.

**The snapshot is the tablet's partitions, from memory and from the archive map.** The primary
walks its partition map for the tablet — every key whose top twelve bits name it — sending
resident partitions as they are and faulting in the rest from its archives through the ordinary
loader, one `StreamPartition` per partition, each a size-prefixed rkyv partition exactly as an
archive stores one ([Storage Overview](../storage/overview.md#archives)). The receiver writes each
straight into its active archive and its archive map, which is what the compactor does with a
merged partition today ([Compaction](../storage/compaction.md#4-write-out)). A snapshot at a `seq`
is consistent because the primary serializes writes: it records `at_seq` when it starts, and any
write after that is in the tail the follower applies afterwards.

This stream is the same protocol [C8](rebalancing.md#a-move) uses to add a replica that has never
held the tablet — a returning node whose data is too old is, for the primary's purposes, a new
replica — and it is built once.

**It serves while it catches up.** A returning node's shards bind their client listener as soon
as they have a map, and answer `One` reads for tablets they are current on; a tablet still
catching up is routed around, because the coordinator's preference order skips a replica whose
reported stamp is behind by more than a configurable `read_lag_tolerance` (default: never skip —
a `One` read is allowed to be stale, that is what `One` means; the knob exists for an operator
who wants freshness at `One` without paying for `Quorum`).

**Primaries do not move back.** When the old primary returns it is a follower for every tablet it
led. Its node now leads nothing and the others lead more than before. The rebalancer can even
that out ([C8](rebalancing.md#the-rebalancer)) by proposing `SetPrimary` toward balance, and whether it
does so automatically is a rebalancer setting that defaults to off — a primary change is a
short pause for that tablet's writers, and an operator who has just recovered a node may prefer
to choose when.

### No hinted handoff

Cassandra stores, on the coordinator, a hint for every write a down replica missed, and replays
it when the replica returns. Shoal does not need one: **the primary's log is the hint.** Every
write the down replica missed is in the primary's intent log in order, and `CatchUp` replays it.
What Cassandra's hints also cover — a coordinator that is not a replica holding writes for one
that is — does not arise, because writes go to the primary and the primary is a replica. The
only case a hint would help is one the log has compacted away, and that is the snapshot path.

## Alternatives rejected

**Electing the new primary by a vote among replicas.** That is multi-Raft, declined on
[C5](replication.md#alternatives-rejected). The control plane already has a leader with a
majority's evidence; it appoints.

**Choosing the new primary by node load rather than by stamp.** A replica behind the highest
stamp is missing acknowledged writes. The highest stamp wins; load breaks ties.

**Failing over on `Unreachable` rather than `Down`.** One node's opinion would move the primary
role for every tablet that node cannot reach, including during a partition of that node alone.
`Down` needs a majority, and failover needs `Down` ([C3](membership.md#the-state-machine-of-a-member)).

**No timer — fail over as soon as `Down` commits.** `primary_failover_after` is the operator's
say in how much flapping they will tolerate. A node that dies and returns in two seconds would
otherwise have every one of its primaries moved and, if the rebalancer moves them back, moved
again. Zero is a legal value.

**Retrying writes automatically inside the window.** Unsafe without the idempotency key, and the
page says why rather than pretending the window is invisible.

**Hinted handoff.** Above.

**Keeping the primary's whole history so catch-up is always by log.** Unbounded disk, and the
snapshot path is needed anyway for a brand-new replica.

**A returning node that does not serve until fully caught up.** Simpler, and it takes a node's
`One`-read capacity offline for the whole catch-up when most of its tablets are current within
seconds. Route around the stale tablets instead.

## What it costs

- **A write outage per tablet per primary death**, of `primary_failover_after` plus detection plus
  one Raft round trip. Bounded, configured, and visible as `Unavailable`.
- **Truncation on a follower that was ahead**: a re-read of the affected partitions from archives.
  Rare and small.
- **Per-tablet stamps in every heartbeat reply**: 4096 × 12 bytes at most, once per interval, off
  the query path.
- **Catch-up traffic on return**: the missed log, or the tablet's partitions plus the tail. This
  is the cost of R3 — a node that was down for an hour has an hour's writes to receive, and
  receiving them is cheaper than having moved its tablets.

## What it breaks

- **"A write that is acknowledged is durable" gains a qualifier**: at `Quorum`, it is durable
  across the loss of a minority; at `One`, it is durable on one node and a failover may lose it.
  [Storage Overview](../storage/overview.md#durability-model) has to say so when M6 lands.
- **A client can now see `Unavailable` on a write**, and has to decide about it. The idempotency
  key is what would make that decision automatic, and it is filed, not built.
- **Recovery is no longer self-contained.** A shard still recovers itself from its own files
  first, and then asks. [Recovery](../storage/recovery.md)'s "recovery is just replay" stays true
  of the first step and gains a second.
- **`RecoveryStats::truncated_logs` changes meaning on a replica.** A torn tail on a follower is
  a shortfall the primary fills, not data loss; [item 47](../appendix/known-issues.md#47-a-torn-tail-on-the-active-log-is-counted-as-data-loss)
  gets a second reason to be fixed.

## Invariants to uphold

- **The new primary is the `Up` replica with the highest stamp.** Any other choice can lose an
  acknowledged write. Load is a tiebreak, never a criterion.
- **Failover requires `Down`, and `Down` requires a majority.** No node fails over on its own
  verdict.
- **An old epoch is refused by every follower, always.** Fencing is a comparison at the top of
  every replicate; a fast path that skips it is the bug.
- **A snapshot is taken at a `seq` and the tail starts at that `seq`.** A gap between them is a
  lost write on the new replica; an overlap is applied twice, which for an insert is idempotent
  and for a delete-then-insert is not.
- **Truncation replays from archives, never from memory.** A follower that was ahead has the
  extra writes applied in memory; the only clean state is what the archives plus the log-to-stamp
  reconstruct.
- **A write is never retried by the client or the coordinator without an idempotency key.** Until
  the key exists, `Unavailable` is the caller's problem, and saying so is the design.

## Prerequisites

[C3](membership.md) for `Down` and the heartbeat channel that carries stamps;
[C5](replication.md) for the stamps, the fencing and the `CatchUp` gap path;
[C4](tablet-map.md) for `SetPrimary` and `epoch`; [C2](transport.md) for `CatchUp` and
`Stream*`. The snapshot stream is shared with [C8](rebalancing.md) and built in M7 for both.

## How it would be measured

Failover is measured by an outage, not a throughput: `macro/cluster/failover` runs the reference
mixture at `Quorum`, kills the node holding the most primaries at a known instant, and reports
**the interval during which writes to its tablets were refused** and the throughput before and
after. It is reported as a duration and never folded into an ops-per-second figure, because a
number that averaged the outage away would be the number someone quoted
([C10](performance.md#the-workloads)). Catch-up is measured by `macro/cluster/catchup/{log,snapshot}`:
seconds to lag zero for a node that missed a fixed number of writes, on each path.

## Acceptance tests

| Test | Asserts | Milestone |
| --- | --- | --- |
| `no_acknowledged_quorum_write_is_lost_across_a_primary_kill` | **The ledger test.** A client records every `Quorum` write it was acked for while a node holding primaries is `SIGKILL`ed mid-stream; after failover, a `Quorum` read returns every ledgered row. Run 20 times per suite | M6 |
| `writes_resume_within_the_failover_window` | Writes to the dead node's tablets are `Unavailable` and then succeed within `primary_failover_after + 2 × interval + 1s` | M6 |
| `the_new_primary_has_the_highest_stamp` | Pause one follower before the kill so the two survivors differ; the one that was ahead is chosen (asserted via `Topology` and span attributes) | M6 |
| `a_follower_ahead_of_the_quorum_truncates` | Partition the primary so a write reaches one follower and no quorum; kill the primary; that follower's digest matches the new primary's after the `Topology` | M6 |
| `a_stale_primary_cannot_acknowledge` | Partition the primary from everything but one client; its writes are `Unavailable` and its `Primary` reads refused after the lease | M6 |
| `a_one_write_may_be_lost_and_the_page_says_so` | Write at `One` to a primary, kill it before replication, fail over: the row is absent. The test exists to pin the promise | M6 |
| `reads_at_one_continue_through_a_failover` | 100% read arm at `One` sees no errors across the kill | M6 |
| `a_killed_node_catches_up_by_log` | Kill, write 10k rows (under `intent_log_size`), restart: lag reaches 0 and the digest matches; span attributes name the log path | M7 |
| `a_killed_node_catches_up_by_snapshot` | Kill, write past several rotations, restart: the snapshot path is taken; digest matches | M7 |
| `a_returning_primary_becomes_a_follower` | After the kill and restart, the map names it primary for nothing and follower for what it held | M7 |
| `a_returning_node_serves_current_tablets_while_others_catch_up` | With a large backlog on one tablet, `One` reads for the others are answered locally during catch-up | M7 |
| `down_for_less_than_auto_remove_after_moves_no_tablet` | Every replica set names the same nodes before and after; only `primary` and `epoch` changed | M7 |
| `a_torn_tail_on_a_follower_is_filled_not_lost` | Corrupt a follower's active log tail, restart: `CatchUp` refills it, digest matches | M7 |

## Related

- [C5. Replication](replication.md) — the stamps and the fencing this page relies on
- [C3. Membership](membership.md) — `Down`, and the heartbeat reply that carries stamps
- [C6. Reads](reads.md) — the lease, and what each level sees during the window
- [C8. Rebalancing](rebalancing.md) — the same stream, used to add a replica
- [Recovery](../storage/recovery.md) — the first step of recovering, unchanged
- [Compaction](../storage/compaction.md) — why the log is a window and not a history
- [TODOs — An idempotency key](../appendix/todos.md) — what would make the window invisible
- [D6 — Retries](../direction/connection-pool.md#retries) — why a write is not retried
- [C12 — Kafka](prior-art.md#kafka), [MongoDB](prior-art.md#mongodb), [Cassandra](prior-art.md#cassandra) — session timeouts, election timeouts, and the hints Shoal does not need
- [item 47](../appendix/known-issues.md#47-a-torn-tail-on-the-active-log-is-counted-as-data-loss) — gains a second reason
