# 99. A durable follower's log reversion stopped the leader's whole process

## Symptom

A member of a persistent table's tablet group that came back with a shorter log than it had
acknowledged - its WAL segments gone, or its whole WAL directory - stopped the **leader's**
process, not its own. openraft met the reversion in the leader's progress update and
`panic!`ed on the thread that ran it, which on a shard is the shard's executor, and took every
group on that shard, every table and every client with it. The next leader elected among the
rest met the same follower and stopped too. One node that lost its disk was a cluster-wide
outage, and the survivors were the ones that died.

## Cause

`group_config` in `shoal-core/src/server/shard/groups.rs` set `allow_log_reversion` to whether
the group was volatile. That was the library's default for a durable group and, read as a
verdict, the right one: a durable follower whose log shrank lost an entry the leader counted
toward a quorum. What it was *not* was a state to recover from. openraft's answer to a
disallowed reversion is `panic!` in `progress/entry/update.rs`, on the leader, and nothing in
the shard stood between that and the process.

## Evidence

**Reproduced.** `durable_log_reversion_is_fed_not_fatal` in `shoal/tests/cluster_fixture.rs`
writes sixty notes through node zero of a three node cluster at a replication factor of
three, waits until node two has checkpointed every group of the table, kills it, removes every
`*.wal` under its `wal/Shard-0/`, and restarts it. Against the tree before the fix the two
leaders it reported to died at once, and the next write through node zero could not complete
a handshake:

```text
thread 'unnamed-2' (676930) panicked at .../openraft-0.10.0-alpha.34/src/progress/entry/update.rs:100:13:
follower log reversion is not allowed without `allow_log_reversion` enabled; matching: T1-N82a882e6-…/0.8; conflict: 8
thread 'unnamed-2' (676922) panicked at .../openraft-0.10.0-alpha.34/src/progress/entry/update.rs:100:13:
follower log reversion is not allowed without `allow_log_reversion` enabled; matching: T1-N71f4c9f2-…/0.10; conflict: 10

thread 'durable_log_reversion_is_fed_not_fatal' panicked at shoal/tests/cluster_fixture.rs:2745:9:
a write through 127.0.0.1:38357 was refused for another reason: Err(Handshake(HandshakeTimeout))
```

With the fix both variants - the segments alone, and the whole directory - leave the
survivors' pids unchanged, every write through node zero committing, node two fed until every
digest agrees, and its integrity report counting the log it lost.

## The fix

**The follower is the one that is wrong, so the leader feeds it.** `allow_log_reversion` is
`Some(true)` for every group. openraft then logs the reversion, resets that follower's
progress to what it now holds, and replicates from there - from the retained log when the
follower is inside it, and past the purge point through the snapshot
[F43](../../features/node-recovery.md) delivered, which is what a follower reset behind the
purge point receives. The leader keeps serving throughout; nothing on its side changed but the
flag.

**The follower says what it lost.** When `rebuild_groups` builds a durable group for which the
shard's WAL holds no frame at all - not an entry, not a purge marker
(`ShardWal::last_log_id_of`) - and either the checkpoint file names an applied index for it or
its table's archives hold a partition of its tablets (`FullArchiveMap::holds_any`), the shard
counts `IntegrityStats::log_lost` and logs an `ERROR` naming the group, the table and what it
still held. The count rides `ShardReplication::integrity` into the node's report, the
`Replication` admin read and the fixture's `GROUPS` view. The checkpoint case is the segments
gone; the archives case is the whole directory gone, checkpoint and sidecar with it.

**What the two variants converge through differs, and both are tested.** With the segments
gone the checkpoint survives, so the group starts at it and is fed the entries above it. With
the directory gone the group starts from nothing over archives that still hold the old rows;
the leader feeds it from index one or from a snapshot, and either way the archives end up
holding the leader's state - a re-applied insert is the same row, and a snapshot's absence is
total.

## Alternatives rejected

**Keep the reversion fatal and make the follower refuse to start.** The follower cannot tell
at open that its log is short - it can see that it has *no* log behind a checkpoint, which is
the case counted here, but a log truncated to a shorter one that is still a log looks like a
log. And a member that refuses to start is a member an operator has to wipe by hand to get
back, which is what the leader now does for it.

**Catch the panic.** It is on the leader's executor thread inside openraft's core; there is
nothing to catch it with that leaves the group usable.

**Reset the follower's progress from the shard rather than the library.** openraft does
exactly this when the flag allows it, and it is the one that knows the progress.

**Mark the member `Down` or fence it.** A member that lost its disk is not a member with an
old identity; it is this identity with less history, which is the case replication exists
for. Fencing it would turn one lost disk into a replacement procedure.

## Invariants to uphold

- **`allow_log_reversion` stays on for every group.** A durable follower behind what it
  acknowledged is fed, never fatal. The flag is the whole of the fix on the leader's side;
  a future config change that derives it from anything must not turn it off for a persistent
  table.
- **A lost log is counted where it is found, once per group build.** `log_lost` is the
  follower's own report; the leader's side is a library log line and nothing counts it there.
  A test that wants to know a member lost its log asks that member.
- **The checkpoint and the archives are the two witnesses.** Either says the shard once held
  the group; the count needs one of them, and a group with neither is a group this shard
  never held, which is a first start and not a loss.

## Still open

- The count does not distinguish a log that was lost from one an operator removed on purpose
  to force a resync; both are the same event, and an operator who did the second reads the
  first.
- A log truncated to a shorter one that still holds frames is not detected at open; the leader
  meets it as a reversion and feeds the member, and nothing on the member counts it.

## Tests

| Test | Where | What breaks if this is reverted |
| --- | --- | --- |
| `durable_log_reversion_is_fed_not_fatal` | `shoal/tests/cluster_fixture.rs` | The leaders panic with the message above on the first variant, and the next write through node zero times out at the handshake; with the flag kept but the count removed, `log_lost` reads zero on both variants |

## Related

[F40. Replication](../../features/replication.md), whose "a durable one may not" rule this
replaces; [F43. Node recovery](../../features/node-recovery.md), whose snapshot feeds a member
reset behind the purge point; [F44. Repair](../../features/repair.md), the milestone this was
fixed in, and whose integrity report carries the count; [C7](../../distributed/failover.md)'s
returning-node table.
