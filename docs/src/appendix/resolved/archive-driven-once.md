# 261. A re-driven archive put its objects again, over a client's newer write

A restore puts a bucket's archived objects again through shard zero of the node each archive is
dealt to ([F93](../../features/operations-and-the-real-cluster.md)). When a driver ended, its shard
drove the archive again whenever its own map still showed it unfinished, and a node's map trails
the control leader's answer to the driver's last commit, so most archives driven off the control
leader were driven twice. The second drive put every object again past the stale count. A put
again under the same identity is answered "done" while the path still holds what the restore put,
and goes over the path when a client has written it since. And a driver whose last commit failed
at once started another at once, each a read of the whole archive. Filed by a code review of F87 to
F93 and fixed in the change after it.

## Symptom

A restore could bring back the archived version of an object a client had written after the
restore put it. Nothing in the restore's record said so: the second drive's progress commits are
no-ops on an archive already `Done`. A control thread refusing proposals at once (a full channel)
had each archive's driver verify its archive whole, one after another, with no wait between.

## Cause

`handle_archive_done` cleared `driving_archives` and called `drive_archives` against the shard's
map, which `drive_archives` trusts alone. A group's restore has the guard this lacked:
`handle_restore_done` records `driven_restores` and `drive_restores` trusts it over a lagging map.
`drive_archive` dropped its last commit's answer (`let _ = commit_archive(..)`) and sent
`ArchiveDone` either way. And `put_again`'s answer to a path that moved on was to put again: an
inline put whose path held another object wrote its entry over it, and a striped put whose identity
a client's replace had retired was answered `Gone` and put again under a new one.

## Evidence

**The overwrite is reproduced** by `a_restore_never_puts_over_a_clients_write`, written first and
run on the unfixed tree: two of four backed up paths written by a client in the destination before
the restore reached them, which is the state a drive again meets after a client writes a path the
first drive put.

```text
thread 'a_restore_never_puts_over_a_clients_write' panicked at shoal/tests/cluster_fixture.rs:31152:9:
kept-inline.bin reads as the backup's object, not as the client wrote it
test result: FAILED. 0 passed; 1 failed
```

**The drive again is established by reading the source**, twice: by the code review and by a second
reading. It was not reproduced: its window is the few milliseconds between the leader's answer and
the node's map, and nothing outside a node counts how often an archive is driven.

## The fix

- **An archive's end is remembered on the shard that drove it** (`driven_archives`,
  `DrivenArchive`). One whose end committed is not driven again for that try, however far behind the
  map is; a `RetryRestore` since is a new try, which is.
- **A driver's last commit is tried again** after 1, 4 and 16 seconds, and `ArchiveDone` says
  whether it landed. One that did not is driven again only after `ARCHIVE_REDRIVE_AFTER`, 30 s, from
  what the map says then.
- **A put again never goes over a live object it did not make.** Before each try `put_again` reads
  what is live at the path: one of this record's own objects, of any attempt, is the record done, and
  anyone else's is left as it is, and logged.

## Alternatives rejected

- **Waiting for the node's map to show an archive done before driving the next.** It holds every
  archive on the node behind the slowest map push, and a map that never shows it, because the end
  did not commit, holds them for good.
- **Committing every put in the control state.** Progress is committed every 32 records for its
  cost; a commit a record would make a restore of small objects a control log entry each.
- **Refusing a restore into a bucket that is not empty.** It covers a client that wrote before the
  restore began, and none that writes during it, which is the case a drive again meets.
- **Putting again under a new identity when a client's replace retired the old one**, as `put_again`
  did. A retired identity is how the put learns a client was there.

## Invariants to uphold

- **An archive's end committed by a driver here is trusted over the map for that try.** A new way of
  ending a driver goes through `handle_archive_done` with whether its end committed.
- **A driver whose end did not commit is never followed by another at once.**
- **A put again goes over no live object it did not make.** Its own objects are every attempt's, up
  to `PUT_TRIES`, so a resume that tries again under a later attempt still recognises an earlier one.

## Still open

- **A delete a client made after a put whose progress was not committed is undone by a resume.** A
  deleted path keeps no sign of which put made it: a striped entry names the deleted object retired,
  as it names a try given up, and an inline one goes with its row. A resume after a crash cannot tell
  the two apart, and puts the path again. The window is the records since the last committed
  progress, 32 at most, and a drive after a lagging map no longer opens it.
- **A path left as a client wrote it is counted as put** in the archive's progress; nothing reports
  how many were left.

## Tests

| Test | What breaks if the fix is reverted |
| --- | --- |
| `cluster_fixture::a_restore_never_puts_over_a_clients_write` | A restore's put goes over a client's write to the path, striped or inline |
| `shard::restore::tests::an_archive_is_driven_once_its_map_is_behind` | An archive whose end committed is driven again while the map is behind, or one whose end did not commit is driven again at once |

## Related

[F93](../../features/operations-and-the-real-cluster.md), which built the archive restore;
[F49](../../features/backup-and-recovery.md) for `driven_restores`, the guard this follows;
[item 263](../resolved/archives-dealt-again.md)
and [O117](../optimizations.md#o117-an-archive-restore-reads-each-archive-two-or-three-times-and-copies-each-object-on-every-try)
on the same driver.
