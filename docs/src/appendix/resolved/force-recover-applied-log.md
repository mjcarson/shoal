# 259. `force_recover` panicked on a control log applied to its end

`force_recover` applies every entry the survivor's control log holds past what its machine applied,
before it writes the recovery. It took them as the range from the applied index plus one to the
log's last index, and when the two were equal, which an idle control group leaves, that range was
reversed and `BTreeMap::range` panicked. Found by
[F93](../../features/operations-and-the-real-cluster.md)'s `Behind` row and filed and fixed in one
change.

## Symptom

`shoal::server::recover::force_recover` on a stopped node whose control group had nothing in flight
when its majority was lost:

```text
range start is greater than range end in BTreeMap
   6: shoal_core::server::control::store::force_recover::{closure#0}
             at shoal-core/src/server/control/store.rs:967:14
   7: shoal_core::server::recover::recover_steps::{closure#0}
```

The recovery an operator runs after a permanent loss of a majority died before writing anything.

## Cause

`F49`'s recovery test held one entry appended and never applied, the leader losing its quorum with
it in flight, so the range was never empty there. A survivor whose machine had applied its whole log
asked for `applied + 1 ..= applied`, which a map refuses.

## Evidence

**Reproduced** by `a_recovery_of_a_log_applied_to_its_end_applies_nothing_more`, written first and
run on the unfixed code: two voters bootstrapped and admitted, every entry applied.

```text
thread 'server::control::store::tests::a_recovery_of_a_log_applied_to_its_end_applies_nothing_more'
  panicked at …/alloc/src/collections/btree/search.rs:121:21:
range start is greater than range end in BTreeMap
test result: FAILED. 0 passed; 1 failed
```

and by the fixture's `rows_older_than_chunks_are_refused_by_name`, the backtrace above.

## The fix

**No range is taken when the machine applied everything**: the unapplied entries are none when the
applied index is at or past the log's last.

## Alternatives rejected

None worth recording: the range was the defect.

## Invariants to uphold

- **A recovery runs whatever its survivor's log holds**, applied to its end or not.

## Still open

Nothing.

## Tests

| Test | What breaks if the fix is reverted |
| --- | --- |
| `shoal-core control::store::tests::a_recovery_of_a_log_applied_to_its_end_applies_nothing_more` | The recovery of an idle control log panics |
| `cluster_fixture::rows_older_than_chunks_are_refused_by_name` | The survivor's recovery panics before the stripe is ever read |

## Related

[F93](../../features/operations-and-the-real-cluster.md), [F49](../../features/backup-and-recovery.md)'s
`force_recover`.
