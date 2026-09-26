# 168. A member added after a restore was fed a log that held none of the restored rows

## Symptom

The first lab run of [F56](../../features/cluster-rebuild.md) rebuilt hyperion into a cluster
that had just been restored from a backup. The rebuild finished, every plan step `Moved`, and
then:

```text
verify --member 1 --read one   (hyperion's own copy)
read 1187691 movies back: 331350 missing
verify                         (the default, any member)
read 1187691 movies back: 82761 missing
read 58418 keyword partitions back: 14605 different
```

During the rebuild, a tenth of the bench's gets were answered as if the movie did not exist:
472,946 of them. The other two copies were whole.

## Cause

A restore installs each group's backup file into every copy at a boundary of its own, a scrub
entry a few entries into a fresh cluster's log (3 on the lab). The rows reach the archives outside
the log: no entry holds them. Every copy's log still began at entry one.

openraft feeds a new member a snapshot only if the entries it needs are purged from the leader's
log. Otherwise it sends the entries. After a restore on a quiet cluster, nothing had purged the
logs, so a member moved in afterwards got entries one onwards and applied them: the membership, the
scrub and whatever was written since, and none of the restored rows. Of hyperion's 36 groups, about
16 were fed a snapshot, because the bench had written enough to purge their logs, and the rest were
fed the log.

A repair install puts rows into one copy outside the log too. They are the source's, and the
source's rows follow from the log unless they came from a restore, so after a plain repair the
copy's log still accounts for them. The purge below is applied after both kinds of install anyway:
after a repair it costs nothing, and one rule is easier to keep than two.

## Evidence

**Found on the lab, then reproduced.** `a_copy_moved_in_after_a_restore_holds_the_restored_rows`
(`shoal/tests/cluster_fixture.rs`) backs up eighty notes from one cluster and restores them into
three placed nodes of another with a fourth spare. It moves one set onto the spare and reads that
set's restored keys from the spare's own copy at `One`. Against the unfixed tree, every run:

```text
the moved copy is missing 32 of 32 restored rows: [(37000, "Ok(None)"), (37003, "Ok(None)"), …]
```

## The fix

Every copy that installs a repair or a restore file purges its log through the install's boundary
(`purge_installed`, openraft's `purge_log`). It does so at once if the group is up, or when it comes
up (`Group::purge_through`), since the install usually lands during the group's restart. openraft
has a snapshot at the boundary after the install, so it can purge that far. A member needing the
entries below it is then sent a snapshot, which is a cut of the archives and holds the restored
rows.

On the lab the same scenario, a restore and then a rebuild of hyperion under the bench, was run
again on the fixed build ([Rebuilding a node under load](../../cluster-testing/correctness.md#rebuilding-a-node-under-load)).

## Alternatives rejected

- **Write the restored rows into the log.** A restore of 3 GB would be 3 GB of entries per group,
  replicated and retained, for rows every copy already has.
- **Refuse to move or add a member until the log has passed the restore.** On a quiet cluster that
  could be never.
- **Purge on the leader only.** Any copy can lead later.

## Invariants to uphold

- **A copy's log never reaches below the boundary of the last repair or restore it installed.**
  The purge is what makes a log a complete account of a copy's rows above its start.
- **A snapshot is what carries rows that no entry holds.** A new way of putting rows into a copy
  outside the log has to purge the same way.

## Still open

- **A cluster restored before this fix** still has logs that begin at entry one wherever nothing
  has purged them since. Such a cluster should have a `Repair` of every table run in `repair` mode
  after the upgrade, or a `rebuild` of any member added since, before a member is added or moved.
  The lab's cluster was repaired that way.

## Tests

| Test | What breaks if this is reverted |
| --- | --- |
| `a_copy_moved_in_after_a_restore_holds_the_restored_rows` (`shoal/tests/cluster_fixture.rs`) | A member moved in after a restore is fed the log from entry one and holds none of the restored rows |

## Related

- [F49](../../features/backup-and-recovery.md), the restore.
- [F45](../../features/replica-migration.md), the moves that feed a new member.
- [F56](../../features/cluster-rebuild.md), whose first lab run found it.
