# 323. A backup's group queued behind a move or a repair was never released

A backup records every group of its tables when it is asked for
([F49](../../features/backup-and-recovery.md)). A group whose set is under a move or a repair not
yet done is recorded `Queued` behind it, since a cut under either would be a cut of a set half
moved or half judged. The group's leader drives only a group that is pending, cutting or writing.

Filed and fixed in one change, found by reading the backup's lifecycle while planning item 322's
record (2026-10-27).

## Symptom

A backup asked for while a move or a repair of one of its sets was under way never ended. Its
queued group stayed `Queued { behind }` after that operation was done:
- no leader drives a queued group (`drive_backups`, `shard/backup.rs`);
- the control state refuses a queued group's progress (`apply_backup_progress`);
- so the backup's record never reached `is_done()`, and `BackupStatus` showed the group queued for
  good.

The lab never met it: its backups were never asked for during a move or a repair.

## Cause

`release_queued` (`control/types.rs`) is what a finished move or repair calls to let go of what
waited behind it. It released repairs and moves and nothing else:

```rust
fn release_queued(&mut self, behind: Uuid) {
    // a repair's groups queued behind a move are pending again ...
    for record in self.repairs.values_mut() { ... }
    // and the first move queued behind it is planned
    ...
}
```

A backup's groups were queued by the same rule as a repair's, behind the newest unfinished move or
repair holding their set, and nothing ever released them.

## Evidence

**Established by reading the source and reproduced by a test.**
`a_backup_queued_behind_a_move_or_a_repair_is_released_when_it_ends` (`shoal-core`,
`control::types`) was written before the fix. It runs a move of tablet 0's set, then a backup of
the table, then the move's end. On the tree before the fix:

```text
thread 'server::control::types::tests::a_backup_queued_behind_a_move_or_a_repair_is_released_when_it_ends' (899695) panicked at shoal-core/src/server/control/types.rs:7399:9:
assertion `left == right` failed: a backup's group queued behind a move stayed queued once the move was done
  left: Queued { behind: 6a32d837-9a7a-4dc3-a47d-41919c75ea2b }
 right: Pending
```

## The fix

**A backup's group queued behind a finished operation is judged again** in `release_queued`:
- it waits behind the newest move or repair not yet done that still holds its set
  (`ControlState::holding_set`), a repair or a move released in the same step among them;
- otherwise it is pending, and its set's leader drives it.

`apply_backup` queues a group by the same helper, so a request and a release judge alike.

A group's identity is the hash of its table and its rule's replica set, which a move keeps: the set
is served by its new members under the same identity. So the released group is the same group, and
the leader of the set's new configuration drives it.

The test's group is pending once the move is done, and its progress is taken. A second backup,
queued behind a repair of the same set, is pending once the repair is done.

## Alternatives rejected

- **Releasing every queued group to pending**, as a repair's groups are released. A move's end
  releases a repair queued behind it in the same step, and a backup's cut would then run under that
  repair, of a set half judged, which is what the queue is for.
- **Never queueing a backup**, so it cuts under a move. That is the cut F49 queues to avoid.
- **Failing a queued group after a while.** A backup behind a long move would fail only for
  waiting, and have to be asked for again.

## Invariants to uphold

- **Everything that queues behind an operation is released when it is done, in `release_queued`.**
  A new kind of record that queues behind a move or a repair has to be released there too.
- **A released backup group waits behind any other unfinished move or repair holding its set**, by
  the rule its request was queued by (`holding_set`).
- **A group's identity is its rule's set**, so a move never strands a group a backup recorded.

## Still open

- ~~**A record forgotten while unfinished releases nothing.** The control state keeps the newest 64
  repairs and the newest 64 backups whatever their state. A repair still queued, or still running,
  can be forgotten while a move or a backup waits behind it.~~ Fixed as
  [Resolved #324](unfinished-records-kept.md): a repair or a backup is forgotten past its bound only
  once it is done.

## Tests

| Test | What fails if the fix is reverted |
| --- | --- |
| `a_backup_queued_behind_a_move_or_a_repair_is_released_when_it_ends` (`shoal-core`, `control::types`) | A backup's group queued behind a move stays queued once the move is done, and one behind a repair once the repair is |
| `a_repair_and_a_move_serialize_on_a_set` (`shoal-core`, `control::types`) | Unchanged: a repair and a move still queue behind each other and are released as before |

## Related

- [F49](../../features/backup-and-recovery.md): a backup's groups and their queue.
- [F45](../../features/replica-migration.md): the moves a backup waits behind.
- [Resolved #322, the rest](backup-outcome-record.md), whose record is written once a backup ends.
