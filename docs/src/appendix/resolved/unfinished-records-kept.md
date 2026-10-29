# 324. A repair or a backup was forgotten unfinished once 64 newer ones were kept

The control state keeps a record of every repair ([F44](../../features/repair.md)), move
([F45](../../features/replica-migration.md)), plan ([F46](../../features/capacity-rebalancing.md)) and
backup ([F49](../../features/backup-and-recovery.md)) it applies, and forgets the oldest once too many
are kept, so the state does not grow without bound. A move, a repair and a backup can each wait
behind another operation on the same replica set: a repair and a move serialize on a set, and a
backup waits behind both. Whatever waits is released when the operation it waits behind ends
(`release_queued`).

Filed while fixing [item 323](queued-backups-released.md), by reading the control state, and fixed
in the same window at the user's word (2026-10-27).

## Symptom

None was seen. The lab's clusters run no scheduled scrubs, and nothing else asks for 64 repairs or
backups while one is still running. With `cluster.repair.scrub_interval` set, though, every group's
leader asks for a verify scrub each interval, a repair record a group: a table of ninety groups
makes ninety an interval. A repair still queued behind a long move, or still scrubbing, could then be
forgotten. Anything that waited behind it was never released:
- a move queued behind the repair stayed `Queued` for good, and its set never moved;
- a backup's group queued behind it stayed queued, and the backup never ended.

A backup was forgotten the same way past 64 newer ones. Nothing queues behind a backup, but an
unfinished one left the map, its drivers forgot it, and nothing ended it.

## Cause

A repair's apply and `apply_backup` (`control/types.rs`) forgot the oldest record past their bounds
whatever its state:

```rust
while self.repairs.len() > KEPT_REPAIRS {
    let oldest = self.repairs.values().min_by_key(|record| record.requested_at)...;
    ...
}
```

A move's and a plan's prunings already forget only records that are done
(`.filter(|record| record.is_done())`), which is why a move never stranded what waited behind it.

## Evidence

**Established by reading the source and reproduced by a test.**
`a_repair_or_a_backup_not_done_is_never_forgotten_past_its_bound` (`shoal-core`, `control::types`) was
written before the fix. It applies a repair of tablet 0's set, a move of the set queued behind it,
and as many newer repairs as the bound keeps, none of them done. On the tree before the fix:

```text
thread 'server::control::types::tests::a_repair_or_a_backup_not_done_is_never_forgotten_past_its_bound' (982529) panicked at shoal-core/src/server/control/types.rs:7425:9:
a repair not done was forgotten for 64 newer ones, and the move queued behind it with it
```

## The fix

**A repair or a backup is forgotten past its bound only once it is done**, the oldest of those first,
as moves and plans are. One not done is kept however many newer ones come, so its end is recorded and
releases whatever waits behind it. The test's repair is kept through the 64 newer ones, its end
releases the move to `Planned`, and once done it is the one the next record past the bound forgets. A
backup not done is kept through 64 newer backups.

## Alternatives rejected

- **Releasing what waited behind a record as the record is forgotten.** A move or a backup released
  from behind a repair still scrubbing would cut or move a set half judged, which the queue exists to
  prevent. The record's end is the only safe release.
- **A larger bound.** It delays the failure, and scheduled scrubs on a large table still reach it.
- **Refusing a new record past the bound.** A scheduled scrub refused for a full state would starve
  the groups it covers, as [Resolved #195](scheduled-scrub-starved.md)'s did for a stale version.

## Invariants to uphold

- **No repair, move, plan or backup that is not done is ever forgotten for a bound.** Only a record
  that is done may be pruned, the oldest first.
- **A queue is released only by the end of what it waits behind** (`release_queued`), so the record
  of that end has to exist when it comes.

## Still open

- **The state keeps every unfinished repair and backup, however many.** It grows past the bounds
  while operations wait, which a stuck queue would let grow for as long as it is stuck, as moves
  already could.
- **Restores are still pruned whatever their state**, past 16. A cluster applies one restore in its
  life, since a second is refused, so the bound is never reached.

## Tests

| Test | What fails if the fix is reverted |
| --- | --- |
| `a_repair_or_a_backup_not_done_is_never_forgotten_past_its_bound` (`shoal-core`, `control::types`) | A repair not done is forgotten for 64 newer ones, with the move queued behind it, or a backup not done for 64 newer backups; or a done repair is kept past the bound |
| `a_repair_and_a_move_serialize_on_a_set` and `a_backup_queued_behind_a_move_or_a_repair_is_released_when_it_ends` (`shoal-core`, `control::types`) | Unchanged: the queues and their releases |
| The fixture's repair and scheduled scrub rows (`cluster_fixture`) | Unchanged: nine rows, the scheduled scrub's among them |

## Related

- [Resolved #323](queued-backups-released.md): a backup queued behind a move or a repair, which this
  keeps from being stranded by a forgotten repair.
- [Resolved #195](scheduled-scrub-starved.md): scheduled scrubs, which make most repair records.
- [F44](../../features/repair.md), [F45](../../features/replica-migration.md) and
  [F49](../../features/backup-and-recovery.md).
