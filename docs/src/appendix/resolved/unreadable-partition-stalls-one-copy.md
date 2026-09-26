# 160. A copy that could not read one partition stopped its node

## Symptom

On the lab, after [#159](map-ahead-of-archive.md) left a torn entry in hyperion's map, every
start of hyperion failed the same way, 31 times until it was stopped by hand:

```text
ERROR ShoalPool::ready: error=ShardFailed { shard: 1, error: "partition 10483307249282197527 of Movie could not be read for a replicated apply" }
```

The one unreadable record belonged to one of hyperion's 36 groups. The other 35 had healthy
copies that the cluster could not use, because the node they lived on would not stay up. And the
node could never come back on its own: the entry that needed the record was in the log above the
checkpoint, so every start replayed it again.

## Cause

A replicated apply that needs a partition's archived copy parks the batch on the read
(`run_apply`, `ApplyStep::NeedsLoad`), and the read's answer resumes it (`resume_parked`,
`shard/groups.rs`). A read that failed outright returned an error from `resume_parked`, which
ends the shard loop, and a failed shard stops the node (`node::serve` breaks on
`pool.failure()`).

Refusing to apply is right. A replica that applied the entry without the partition would
compute something its leader did not, and diverge. But the refusal was made at the wrong scope.
The shard hosts every group on its core, and the node hosts every shard, so the decision about
one copy of one group was carried out on all of them. Nothing about the other groups depended
on the partition.

A copy that has stopped applying is also a copy the cluster already has a mechanism for. F44
quarantines a copy whose record fails its checksum and repairs it from a snapshot. But that path
needed the copy to answer a scrub, and a copy that has stopped applying never applies the scrub's
entry.

## Evidence

**Reproduced.** `an_unreadable_partition_stalls_one_copy_and_repairs_it`
(`shoal/tests/cluster_fixture.rs`) seeds a three node cluster at a factor of three and waits for
every copy to checkpoint. It corrupts one follower's archived record of one partition with the
fixture's `CORRUPT`, which also evicts the resident copy, and then updates that partition through
the leader. The follower's apply has to read the record. Against the unfixed tree the follower
stopped with the lab's error:

```text
assertion `left == right` failed: the follower stopped
  left: Some("shard 0 died: GlommioGeneric(\"partition 15414365086730314339 of Note could not be read for a replicated apply\")")
 right: None
```

## The fix

A copy that cannot read a partition for a replicated apply **stalls**. The copy stops, and the
shard and node do not.

- **The one copy stops applying** (`Shard::stall_copy`). `MachineState::stalled` records the
  entry's index and the partition, and the copy is quarantined with a new reason,
  `QuarantineReason::Unreadable`, persisted under `wal/Shard-N/quarantine/` and reported like
  any quarantine. Then the parked batch is dropped. Its responders fail and so does the state
  machine's `apply`, which ends that group's `RaftCore` if the group is running, or its
  `Raft::new` if the entry was being replayed at start. Nothing else on the shard notices. The
  log and the vote are untouched, since neither depends on the archives.
- **A copy that leads hands its lead on first**, ~~with `transfer_leader` to another voter,
  so the group's writes are not held for an election timeout. This is best effort: an election
  follows the core's end anyway.~~ Dropping the batch straight after asking ended the core before
  the transfer took, and on the lab a group's writes reached the dead core for 16 s. Since
  [#167](stalled-leader-handoff.md) a task holds the batch until another member leads, or for 3 s,
  and the lead goes to the voter furthest along.
- **A start that stalls is not a failed shard.** `handle_group_up` takes a failed build of a
  stalled group as a stall: the slot keeps no handle and is marked `start_failed`, and the writes
  that waited for the handle are refused retriably. A slot with no handle and `start_failed` set
  still answers what a repair asks of it (the snapshot stream, a quarantine, its applied index
  and its digest), from what the loop holds.
- **Writes through a stalled copy are refused `NotLeader`**, so a client takes them to another
  member rather than waiting on a machine that applies nothing.
- **The leader repairs it without an operator.** A stalled copy's digest answer is
  `DigestAnswer::Stalled`, and `scrub_group` records it at once rather than polling to the
  deadline. The judge makes every stalled member a target under `Unreadable`, and it needs no
  judgement to do so, since the copy said so of itself. The group's leader proposes that `Repair`
  itself (`repair_stalled_copies`, as the process, at most every 30 s per group) for any group
  with a member committed `Unreadable`. `cluster.repair.unreadable: false` turns that off, and
  then the copy waits for an operator's `Repair`.
- **The install restarts the copy.** `restart_group_for_install` starts a stalled copy with no
  handle to stop, clears the stall with the new `MachineState`, and clears `core_dead`. A new
  handle clears `core_dead` in `handle_group_up` too: it used to outlive the core it described.
- **An `Unreadable` quarantine takes precedence** over one already set with another reason.
  Otherwise a checksum failure on the same read, quarantined a moment before the stall, would
  keep the copy from being repaired automatically.
- **The reports say so.** `GroupReport::stalled` and `NodeReplication::stalled` are new. A stalled
  copy is not `up`, and it is not counted in `starting`, which `cluster upgrade` waits on.

## Alternatives rejected

- **Skip the entry and apply the rest.** openraft hands the state machine entries in order and
  has no way to skip one. Skipping it in our apply would leave the copy's state differing from
  its leader's by an unknown amount, and would serve that state after a repair decided it was
  healthy.
- **Keep the batch parked and the core alive, applying nothing.** The copy would keep voting and
  appending. But the repair's restart shuts the old core down, and a shutdown with its state
  machine worker parked inside `apply` for good would have to be shown not to wait on it, which
  nothing here does. A copy at start has no core to keep anyway. Ending the
  apply is the one shape that covers both cases.
- **Retry the read.** The failures that reach here are refused reads (a checksum, a missing
  archive, a short read), not busy ones. [#159](map-ahead-of-archive.md)'s torn entry was
  permanent. A retry would turn a stopped node into a node that spins.
- **Rebuild the copy empty under the same identity, as a volatile copy is rebuilt.** A durable
  copy's log cannot be dropped under the same vote without the risk the
  [rebuild todo](../todos.md#rebuild-a-node-from-its-peers) describes: an empty voter can elect a
  leader missing a committed write. The repair snapshot keeps the log and replaces only what the
  archives hold.
- **Leave the repair to an operator, as a checksum quarantine is.** A checksum or divergence
  quarantine is decided by comparing copies, so F44 wants a majority or an operator. A stalled
  copy decided about itself, and all it needs is a source, which the leader is.

## Invariants to uphold

- **A stalled copy applies nothing more until it is restarted from a snapshot.** The parked
  batch's `done` is never answered with the entry unapplied, and a batch for a stalled group is
  never resumed.
- **Only the group's own copy stops.** A read failure is attributed to the group of the parked
  batches, which are keyed by the partition. Nothing in `stall_copy` returns an error to the loop.
- **A slot with no handle is restarted only once its start has ended.** `start_failed` is set by
  the failed build's own `GroupUp`, so a repair's restart never runs two `Raft::new` over one
  store.
- **`Unreadable` means the copy is repaired without an operator.** The judge makes a stalled
  member a target only when the other members' verified digests supply a trusted majority. Two
  stalled copies of three leave one verified copy, no majority, and an `Unresolved` record for an
  operator.
- **The stall is in-memory and the quarantine is on disk.** A restart finds the quarantine
  persisted and the copy not yet stalled. It stalls again only if its replay meets the record
  again, which is what a copy still needing repair does.

## Still open

- **A stalled copy pins the WAL.** Its checkpoint cannot move, so no segment holding its frames
  is deleted until it is repaired. At 30 s between tries that is bounded by the repair's
  duration, unless the repair cannot run (no trusted majority, or automatic repair turned off).
- **The copy's other partitions are not served in the meantime.** The whole group's copy on the
  node stops, not one partition's. Reads are routed to the other members by the quarantine.
- **A backup of a group with a stalled member** scrubs every member and finds this one
  unreported. That is F49's rule for any member that does not answer, and nothing about it is
  specific to a stall.

## Tests

| Test | What breaks if this is reverted |
| --- | --- |
| `an_unreadable_partition_stalls_one_copy_and_repairs_it` (`shoal/tests/cluster_fixture.rs`) | The failed read fails the follower's shard and stops the node. Without the automatic repair, the copy stays quarantined. Without the `NotLeader` refusal, a write through the stalled copy waits on it |
| `a_stalled_copy_survives_a_restart_and_an_operator_repairs_it` (`shoal/tests/cluster_fixture.rs`) | The start's replay fails the shard as it did on the lab. Without the handle-less repair path, the operator's `Repair` finds the copy "still starting" and ends `Clean` with it unreported |
| `the_judge_needs_a_majority_or_an_operator` (`shoal-core`, `shard/repair.rs`) | A stalled member is not made a target, and a repair of it ends `Clean` |

## Related

- [F44](../../features/repair.md), the quarantine and the repair install this builds on.
- [Resolved #159](map-ahead-of-archive.md), the torn map entry that found this on the lab.
- [Resolved #161](failed-start-empty-archive.md), the other defect hyperion's crash loop showed.
- [Distributed cluster testing](../../cluster-testing/correctness.md), where it was proved on the
  lab.
