# 185. A new copy was sent snapshot after snapshot while its leader purged past each one

Filed and fixed in one change, from round 13 of the lab testing
([a step that outlasts the log](../../cluster-testing/correctness.md#a-step-that-outlasts-the-log)).
It is the first half of what [the terabyte todo](../todos.md#rebuild-and-move-at-terabyte-scale)
predicted: a step that outlasts the log.

## Symptom

hyperion was rebuilt from its peers under the mixed bench with the log retention shrunk to 2,000
entries a group, which stands in for a set too large to cut, send and install within the default
retention's span. The rebuild finished, but it streamed 3.2 GiB to move 879 MiB. hyperion installed
70 snapshots for 36 groups: 28 groups once, 6 twice, and two groups 14 and 16 times. One of them
went through this cycle for more than a minute and a half:

```text
02:29:05.57 a snapshot was received whole; installing  boundary=176623
02:29:06.08 a snapshot is installed                     boundary=176623
02:29:12.24 a snapshot was received whole; installing  boundary=183018
02:29:12.84 a snapshot is installed                     boundary=183018
02:29:18.95 a snapshot was received whole; installing  boundary=189530
...
```

Every 6 to 7 s: a 50 to 60 MB cut, sent, installed in under a second, and then another, each one
6,000 to 13,000 entries past the last.

## Cause

A leader purges its log through its latest snapshot less `retained_entries`
(`max_in_snapshot_log_to_keep`), and each snapshot it builds moves that point on. Shoal's snapshot
is metadata at the checkpoint, so one is built whenever a durable checkpoint moves, which is every
`checkpoint_entries` (1,024) applied writes.

openraft holds a purge back for a follower whose log range is in flight, and not for one being
sent a snapshot (`ProgressEntry::is_log_range_inflight`, which answers `false` for
`Inflight::Snapshot`). While a member was being sent a cut at boundary *B*, the leader went on
building snapshots and purging. By the time the member had installed *B* and asked for *B + 1*,
the leader had purged past it whenever the cut, the transfer and the install together took longer
than `retained_entries` of the group's writes. So the leader sent another snapshot, cut later and
no smaller, which the same thing happened to. It stops only when a cycle happens to be shorter than
the retention's span: a quieter moment in the group's writes, or a smaller cut.

The lab's shrunk retention made this happen at a gigabyte a node. At the default retention it
happens whenever a set takes longer to cut and send than 100,000 of its group's writes: the
terabyte case, where a step's snapshot is tens of gigabytes.

## Evidence

**Established by running it**, on the lab at `12c7eb1`, twice. The first run had `retained_bytes`
shrunk to 24 MiB as well (`target/lab/r13/tb/shrunk1`), and its numbers are the ones above. The
second held `retained_bytes` at its default, to isolate the entry retention the fix addresses
(`target/lab/r13/tb/ab.sh`). On the tree without the fix, one group installed 46 snapshots, its
step failed at the migration's deadline and was retried, and the rebuild took 991 s and streamed
8.3 GiB to move 878 MiB.

## The fix

**A member taking a snapshot holds its group's leader's snapshot builds** (`SnapshotHolds`,
`shoal-core/src/server/replication/network.rs`). The leader's `full_snapshot` takes a hold for the
member before it sends the cut, lasting the transfer's budget. When the member answers that the
install is done, the hold is renewed for `CATCH_UP_HOLD` (30 s), long enough for the member to be
fed the entries after the boundary from the log. When the transfer fails, the hold is lifted. The
group's `try_create_snapshot_builder` defers any unforced build while a hold is live. No build means
no new snapshot, so openraft's purge point stays where it was, below the boundary the member is
installing.

A forced build is never held. That is how `retained_bytes` bounds a shard's sealed WAL: past it,
every group in the oldest segment is made to snapshot at its checkpoint and purge through it.

Two more pieces came from the fixture suite, which failed two snapshot tests on the first version:

- **A deferred build is built when its hold ends.** openraft asks for a build only as entries
  apply, so a group that went quiet during a hold built nothing, and purged nothing, until its next
  write. The tests stop writing and wait for the purge. `SnapshotHolds::defer` records every build
  a hold refused, and the shard's tick triggers a build (`build_deferred_snapshots`) for each
  group whose holds have all ended.
- **A member that has gone silent holds nothing.** A send to a node killed mid-stream could wait
  out the whole transfer budget with its hold in place. On every tick the shard lifts the hold of
  any member whose link has been silent past the hop silence (`release_silent_holds`), which is
  the judgement a send already makes before it cuts anything.

On the fix, the same run: every one of the 36 groups installed exactly one snapshot, and the
rebuild took 187 s and streamed 1.0 GiB to move 876 MiB. Both arms read back every acknowledged
insert (4,715,571 and 4,912,274) and the csv through each member alone, with nothing lost.

| Arm | Rebuild | Streamed for moved | Installs per group | Steps failed |
| --- | --- | --- | --- | --- |
| Before the fix | 991 s | 8.3 GiB for 878 MiB | 31 once, 4 twice, 1 × 46 | 1, retried |
| The fix | 187 s | 1.0 GiB for 876 MiB | 36 once | 0 |

## Alternatives rejected

- **Patching openraft to count a snapshot in flight as a use of the log after its boundary.** It
  is the precise fix, and it is openraft's. Shoal keeps no fork of openraft, and deferring the
  build reaches the same purge point from Shoal's side of the storage trait.
- **Raising `retained_entries` for everyone.** A retention long enough for a terabyte step's
  transfer is minutes of every group's writes kept on every leader all the time. A hold keeps
  that log only while a member is actually taking a snapshot.
- **Holding the purge until the member's matched index passes the boundary.** That is the right
  end for the hold, but the machine does not see replication progress and the network does not
  see purges. A bounded grace after the install approximates it, and it cannot hold a log forever
  for a member that went away.
- **Holding forced builds too.** A shard's disk would then have no bound while any member of any
  of its groups was taking a snapshot.

## Invariants to uphold

- **A hold defers only unforced builds, and always lapses.** `retained_bytes` must stay a bound
  on a shard's sealed WAL whatever members are doing, and a hold's end is an expiry, a lift, or
  its member's silence. Never an event that may not come.
- **A build a hold deferred is built when the hold ends**, by the shard's tick and not by the
  next write. A quiet group is otherwise left with a log nothing purges.
- **A hold is taken before the cut is sent.** A build between the cut and the hold would move the
  purge point past the cut's boundary before anything held it.
- **The snapshot a leader offers may be older while a hold lasts.** That is safe: every transfer
  cuts its file at the current checkpoint (`network.build`), whatever openraft's snapshot says.

## Still open

- ~~**A step whose cut and transfer outlast `retained_bytes`** still loses its log to the forced
  purge. At a terabyte a node that is the next limit, and it is the operator's to raise, with the
  inventory's `replication:` block ([F51](../../features/cluster-deployment.md)).~~ Reproduced
  and fixed as [#188](forced-purge-outruns-snapshot.md): the sweep passes over a held group for
  up to `hold_bytes` past the budget. A step that outlasts both still loses its log.
- The rest of the terabyte todo: a cut streamed from the archives rather than written first, and
  several steps in flight onto a node.

## Tests

| Test | What breaks if the fix is reverted |
| --- | --- |
| `shoal-core` `server::replication::network::tests::snapshot_holds_lapse_and_lift` | A hold does not lapse, a lifted member's hold still holds, another group is held by it, or a deferred build is not named once its hold is over |
| `shoal` `cluster_fixture::snapshot_install_is_atomic_at_every_crash_point`, `snapshot_duplicates_and_resume_are_safe` | Without the deferred build or the silent release, a leader holds its log for a node the test killed, and never purges past what that node saw |
| The lab A/B (`target/lab/r13/tb/ab.sh`) | A group installs snapshot after snapshot during a rebuild at a shrunk retention |

## Related

- [F43](../../features/node-recovery.md), how a copy is fed a snapshot.
- [O67](../optimizations.md#o67-ten-thousand-retained-entries-is-seconds-of-a-busy-group), which
  set the default retention.
- [Rebuild and move at terabyte scale](../todos.md#rebuild-and-move-at-terabyte-scale).
