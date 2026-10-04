# F68. Conditional writes, with a typed refusal

An insert, a delete or an update can now be made conditional on the row stored under its key:
applied only if no row is there, or only if the row there passes a filter. The condition is
judged where the write is applied. On a cluster that is at apply, in committed order, on every
replica. On a standalone node it is as the table handles the write. A write whose condition
does not hold is refused with one of three reasons and changes nothing. Unsorted and sorted
tables both have it, persistent and ephemeral alike.

## Context

This is the first required row of [S1](../object-storage/prerequisites.md#required). It lands
before [M12](../object-storage/milestones.md#m12-tables-what-the-metadata-needs) because
everything the object store decides is decided by a conditional commit of a metadata row:

- a stripe's commit is conditional on the row's sequence, the object's truncate epoch and the
  pool map generation ([S7](../object-storage/write-path.md), P8);
- an object's path entry is created only if absent and changed only from what its writer read
  ([S3](../object-storage/objects.md)).

Without the condition, a parity computed from an old stripe overwrites a newer one and nothing
notices until a degraded read
([S7](../object-storage/write-path.md#what-breaks-without-the-condition)).

Before this feature a write could only be unconditional:

- an insert replaced whatever was stored;
- an update or delete succeeded whenever the row existed;
- the replicated result was `CommandResult { kind, ok }`;
- the only refusal was `ApplyOutcome::Refused(String)`, which reached a client as
  `ErrorCode::Internal` and which the dedup table never remembered.

The prerequisite named unsorted tables. The user asked for sorted tables in the same change, and
they share every piece except the partition lookup.

## What it does

### The condition and the refusal

`shoal-proto/src/shared/queries/condition.rs` holds what both table kinds share:

| Type | What it is |
| --- | --- |
| `WriteCondition<T>` | `Absent`: no row, or a tombstone. `Matches(T::Filters)`: a row that passes the table's own filter. An empty filter matches any row, so `Matches(Default::default())` means "a row exists" |
| `ConditionRefusal` | `RowExists` (expected none, found one), `RowMissing` (expected one, found none), `RowMismatch` (found one that fails the filter). `Copy`, rkyv and serde |
| `WriteCondition::judge` / `judge_archived` | The only two places a condition is evaluated, on a row or its archived form. Both reuse the derive's `is_filtered` / `is_filtered_archived`, so a condition is evaluated exactly as a get's filter is |

The filter is the table's `Filter` type. A condition can therefore name only fields marked
`#[shoal(filter)]`. Values given for one field are alternatives, and every field named must
match. A field can be both `filter` and `update`, which is how a version column is written:

```rust
#[derive(ShoalUnsortedTable, ...)]
#[shoal_table(db = "MyDb")]
pub struct Account {
    #[shoal(partition)]
    pub id: String,
    #[shoal(filter, update)]
    pub version: u64,
    #[shoal(update)]
    pub owner: String,
}
```

### Building one

Two traits from `shoal::shared::queries` add the condition to a write. The table derives
implement them.

- `ConditionalWrite::if_matches(filters)` is implemented for the row, which is an insert that
  replaces only an expected row, and for `{T}Update` and `{T}Delete`.
- `ConditionalInsert::if_absent()` is implemented for the row only. An update or a delete of a
  row that must not exist could never do anything, so neither is offered `Absent`.

```rust
// create only if nothing is there
client.send_one(account("a", 1, "me").if_absent()).await?;
// move version 1 to 2, or be told the row moved
let bump = AccountUpdate { partition_key: "a".into(), version: Some(2), owner: None };
match client.send_one(bump.if_matches(AccountFilter { version: Some(vec![1]) })).await {
    Ok(_) => {}
    Err(Errors::Refused { reason: ConditionRefusal::RowMismatch, .. }) => { /* read again */ }
    Err(other) => return Err(other),
}
```

Each returns a `Conditional<Q>`, and `#[shoal::db]` converts it into the table's
`UnsortedQuery::Conditional(UnsortedConditional)` or `SortedQuery::Conditional(SortedConditional)`.
A conditional insert carries its hashed partition key beside its row, the same way a plain insert
does. That lets a query still in its buffer be routed without hashing an archived row, which
[item 93](../appendix/known-issues.md#93-the-archived-partition-hash-disagrees-with-the-live-one-for-every-string-key)
says would disagree with the live hash for every string key.

### The answer

A refused write is answered `ResponseAction::Refused(ConditionRefusal)`. The client sees it in two
ways:

- `ShoalResponse::suceeded` turns it into `Errors::Refused { id, index, reason, end }` whatever its
  options say, since the write was not applied and a caller who sent a condition wants to know.
- `ShoalResponse::refusal()` reads the reason out of a bundle without going through
  `suceeded`.

A refusal is not an `ErrorCode`. It is a definite, committed answer, as `Update(false)` was, and
not a failure.

### Where the condition is judged

| Path | Where | What it does |
| --- | --- | --- |
| Standalone | `PersistentUnsortedTable::conditional`, `PersistentSortedTable::conditional` | Looks the row up. If the partition is not resident, it parks on a disk read the way a delete does. A sorted partition that lacks the row and has `check_disk` set reads first too. It then judges the condition. A refusal answers at once and commits nothing. A condition that holds hands the plain write to the existing `insert`, `delete` or `update`, so the intent log records the decision, never the question |
| Replicated | `build_intent` → `UnsortedIntents::Conditional` / `SortedIntents::Conditional` → `apply` | The condition rides the command. Every replica's `apply` judges it against the state every earlier committed command left. `NeedsLoad` is returned when the row might be on disk. A refusal is `ApplyStep::Done(CommandResult { kind: ResultKind::Refused(reason), ok: false })`. A condition that holds runs the same `apply_insert`, `apply_delete` or `apply_update` the plain intents do |
| Compaction | Both tables' `apply_intents`, and `fragment` for sorted tables | A refused command still has a WAL frame, so the compactor judges every `Conditional` intent again against the partition it is folding and folds only those that held. A sorted batch holding a `Conditional` is handed back by `fragment` the way one holding an update is, so the base is read and folded whole |
| Intent-log replay | Both tables' `replay`, through `replay_intent`, and `scan_keys` | Judges the same way. `scan_keys` loads the partition a condition names. A standalone log never holds a `Conditional`, so this is reached only by a log written some other way |

Because the refusal sits inside `CommandResult`, a group's dedup table remembers it. It is
persisted in `retries.bin` and in the snapshot trailer, and **a retry of a refused write under its
identity is answered with the same refusal**, even after the row has come to match.

### The wire gate

- `PROTOCOL_VERSION` is 7 and `CONDITIONAL_WIRE_VERSION` is 7.
- A replicated conditional write is refused at the coordinator, before anything is proposed, with
  `ErrorCode::WireVersion` naming the version, while the installed map's `activated_wire` is
  below 7. A replica built before F68 would refuse the command its peers applied, and the copies
  would diverge.
- A fresh cluster runs at the floor until an operator activates a newer version, as it does for
  a backup ([F49](backup-and-recovery.md)). So **a new cluster needs `activate 7`** (an
  `admin "activate 7"` or `upgrade --activate`) before it accepts a conditional write.
- `CLIENT_WIRE_VERSION` stays at 4. The query is an appended variant, which a server built before
  F68 refuses as a query it cannot validate and never misreads.

### Figures

- `QUERY_OPS` gains `refused`, so a node counts its refusals as their own kind of answer
  ([F65](query-figures-home-tab.md)).
- A conditional write that is applied is counted as the insert, update or delete it was.
- In a group's write counters, a refusal is a miss.

## Design choices

- **The table's own filter is the condition.** It is already archived, already evaluated on
  resident and archived rows alike, and already on the wire in every get. A condition on a
  version column is a filter on that column, and the object store's generated rows mark their
  sequence, epoch and generation fields `filter`.
- **One appended variant per query enum and per intent enum.** `UnsortedWrite` and `SortedWrite`
  name the three guarded writes once, beside the condition. The existing variants keep their
  encoding, so an old WAL and an old peer decode every plain write unchanged.
- **The refusal is a result, not an error.** It is derived in committed order and identical on
  every replica, so it belongs beside `ok` in `CommandResult`, where dedup, the retry sidecar and
  the snapshot trailer already carry results. `ResultKind::Refused` is appended, so old postcard
  bytes decode.
- **A standalone table logs the decision.** The plain write it applied is what its intent log
  holds, so replay and compaction there never judge anything.
- **A replicated table logs the question.** The decision is made at apply, after the command is
  committed, so the frame holds the condition and every reader of the log judges it again in
  the same order. This is the same reason an orphaned update is a no-op in both places today.
- **`if_absent` exists only on inserts.** It is the one write it means anything for. The wire
  type allows `Absent` on any write, and one sent that way is judged as written: an update or
  delete of no row answers `false`, as it always has.

## Alternatives rejected

- **An `ErrorCode` for each reason.** A caller already branches on `Errors::Server { code }`, and
  this would have needed no new response variant. But a code says the query *failed*, the
  stats count it as an error, and a refusal is the write working. It would also have kept the
  replicated result untyped: `ApplyOutcome::Refused(String)` is not remembered by dedup, so a
  retry of a refused write could have been applied.
- **A condition field on every existing variant** (`Insert { key, row, condition }` and so on).
  It would change the encoding of every write ever logged and every frame between peers, for a
  field that is `None` on almost all of them.
- **Stripping the condition from the WAL.** A replicated write cannot be decided before it is
  committed, so there is no decision to log in its place. Marking refused frames as compacting
  nothing would need a second write after apply to a log that is append only.
- **A version column the engine maintains.** Every table would pay for it, its meaning would be
  fixed by the engine rather than the schema, and the object store's condition is on three
  fields, not one.
- **Letting a conditional sorted write into an F61 fragment.** A fragment is written without
  reading the base, which is exactly what a condition cannot be judged without.
- **Answering a refusal with the row it found.** It would save the object store a read, but the
  result is persisted with the identity in every group's retry table. A row there would make that
  table as large as the rows refused through it.

## Limitations

- **Only `#[shoal(filter)]` fields, and only equality.** A condition cannot compare with `<` or
  `>`, or name a field that is not a filter. [Q25](../object-storage/contract.md#questions-to-answer)
  settles whether the generated rows need more. For the commits X10 drove, equality on
  one field was all both rows needed: a stripe's sequence and an object row's version
  ([Q25, in part](../object-storage/contract.md#q25-in-part-the-metadata-rows-2026-10-04)).
- **No SHQL.** SHQL is SELECT only, so a conditional write is a typed query only
  ([todos](../appendix/todos.md#conditional-writes-in-shql)).
- **A fresh cluster refuses conditional writes until wire 7 is activated.** This is by design,
  and an operator step all the same.
- **A forwarded refusal is counted as its write's kind.** A node relaying a forwarded query's
  answer counts it by the query it forwarded and does not read the peer's answer, so on a
  cluster `refused` counts only refusals a node answered itself
  ([todos](../appendix/todos.md#refusals-in-the-figures)).
- **A refusal is a miss in the group's write counters**, not a counter of its own (same todo).
- **A standalone refusal answers before an earlier write is durable.** It is judged against the
  in-memory state, which already holds a pending write ahead of it, and answers at once, as
  `Delete(false)` always has. A crash before that earlier write is durable could leave a client
  holding a refusal whose cause was lost. A cluster has no such window, since its refusal is
  derived from committed state.
- **A sorted conditional write to a partition resident as its archive deserializes the whole
  partition to judge one row.** An archived seek would avoid this. It was not built, because the
  apply path does the same for a plain update.

## Invariants to uphold

- **`WriteCondition::judge` and `judge_archived` are the only places a condition is evaluated.**
  A second evaluator could disagree with them, and two replicas, or an apply and a fold, would then
  decide differently.
- **The folded base equals the applied state at the frame's index.** The compactor and replay
  judge every `Conditional` again, so they must see exactly the row the apply saw. Anything that
  folds frames out of order, skips one, or folds onto a base the apply did not have breaks it.
  The fold also has to apply the frames it skips to nothing. A frame past a snapshot boundary
  is skipped for that reason.
- **A sorted partition with `check_disk` set never judges a row it lacks as absent.**
  `SortedPartition::judge` returns `None` and the caller reads first. `judge_whole` is only for a
  partition known to hold every row.
- **A refusal goes in `CommandResult`, never in `ApplyOutcome::Refused`.** Only a result is
  remembered with its identity, and a retry has to be refused again.
- **A standalone table never logs a `Conditional` intent.** Its replay does not need the
  partitions loaded for one, and a logged question would be judged against a replayed state
  instead of the one it was decided on.
- **A replicated conditional write is refused below `CONDITIONAL_WIRE_VERSION`.** Lower the
  constant and an unupgraded replica diverges.
- **Variants are appended.** `UnsortedQuery`, `SortedQuery`, both intent enums, `ResultKind`,
  `ResponseAction`, `ResponseActionNames` and `ConditionRefusal` all derive their encoding from
  their order.

## Performance

A plain write takes the same path it always did. The only change is a match on one more variant
in routing and in `handle`, and `apply`'s arms moving into functions of their own. A conditional
write costs what a delete costs to find its row, plus one filter evaluation. Nothing is in a
benchmark yet, and nothing here claims a performance effect.

The lab run is recorded below under *Proved on the lab*.

### Proved on the lab

`tmdb-dataset-loader contend` races compare and swaps through every member and checks each answer
against the committed order. It ran against the user's three node TMDB cluster (europa, titan,
hyperion, `tmdb_cluster.yaml`), destroyed and bootstrapped fresh on this build with wire 7
activated:

| Run | Shape | Compare and swaps | Applied | Refused | Failed | Sorted races |
| --- | --- | --- | --- | --- | --- | --- |
| `--run 1` | 16 counters, 24 workers × 200, 20 sorted rounds | 4,800 in 2.4 s | 2,564 | 2,236, all `RowMismatch` | 0 | 20 rounds, one insert and one delete applied each; 460 `RowExists` and 460 `RowMissing` |
| `--run 2` | 4 counters, 48 workers × 500, 50 sorted rounds | 24,000 in 7.3 s | 2,405 | 21,595, all `RowMismatch` | 0 | 50 rounds, one of each applied; 2,350 `RowExists` and 2,350 `RowMissing` |

Every counter read back at quorum through every member held exactly the increments applied to
it, and every raced keyword partition was empty afterwards. Before `activate 7`, the same command
was refused by name on all three members: `WireVersion`, "a conditional write needs wire version
7 activated, and this cluster has activated 4". `load --limit 10000` and `verify` after both
runs: 10,000 movies and 16,298 keyword partitions, none missing or different. These are counts
of answers, not a measurement: the rates are what one loaded client host drove, and nothing
here is a benchmark.

## Tests

| Test | What breaks if this is reverted |
| --- | --- |
| `insert_if_absent_applies_once` (`shoal/tests/conditional_writes.rs`) | A second insert expecting no row overwrites the first, or is refused for the wrong reason |
| `update_if_matches_follows_the_row` (same) | A compare and swap built on a value the row no longer holds is applied, or a missing row is not told apart from a moved one |
| `insert_if_matches_replaces_only_the_expected_row` (same) | A replacing insert lands over a row its writer never read, or an empty filter does not mean "a row exists" |
| `delete_if_matches_and_the_tombstone_is_absent` (same) | A delete at the wrong version deletes, or a tombstone is judged as a row |
| `unsorted_conditions_judge_rows_on_disk` (same) | A conditional write to a row on disk only is judged without reading it, or a refusal leaves anything behind across a restart |
| `a_refusal_is_readable_from_a_bundle` (same) | `ShoalResponse::refusal` does not name the reason in a bundle |
| `sorted_conditions_name_one_row` (same) | A sorted condition is judged against a neighbour, or the sorted delete and update arms ignore it |
| `sorted_conditions_judge_rows_on_disk` (same) | A partly read sorted partition judges a row it lacks as absent and inserts over a row on disk. Checked by mutating `judge` to ignore `check_disk`: this test fails |
| `ephemeral_tables_judge_conditions` (same) | Either ephemeral table answers a condition differently from its persistent twin |
| `unsorted_compaction_folds_only_the_conditions_that_held` (`shoal-core/src/server/tables/partitions.rs`) | An unsorted compaction folds a refused write into an archive |
| `sorted_compaction_folds_only_the_conditions_that_held` (same) | A sorted compaction folds a refused write, or judges against a neighbour |
| `a_partly_read_sorted_partition_defers_a_row_it_lacks` (same) | `SortedPartition::judge` answers for a row that may be on disk |
| `a_batch_with_a_condition_is_not_a_fragment` (same) | A conditional sorted write is chained as a fragment without its base being read |
| `conditional_writes_race_in_committed_order` (`shoal/tests/cluster_fixture.rs`) | Three writers' compare and swaps through three nodes are not each applied or refused as the committed order gives, a counter's final value differs from its applied increments on any node, or a sorted insert or delete race has other than one winner |
| `refused_write_retry_is_answered_the_same` (same) | A retry of a refused write under its identity is judged afresh and applied, because the refusal was not remembered |
| `conditional_write_survives_compaction_and_restart` (same) | A compactor folds refused writes into archives, and a node restarted on them holds a deleted note. Checked by disabling the compactor's judge: the restarted node's digest disagrees |
| `conditional_write_refused_below_wire_7` (same) | A conditional write is proposed to a cluster that has not activated wire 7 |
| `tmdb-dataset-loader contend` (`examples/tmdb_dataset/src/contend.rs`, on the lab) | Any of the above on real hosts, at real latencies, against the persistent sorted table the fixture does not race |

## Related

- [S1](../object-storage/prerequisites.md#required), the prerequisite this delivers.
- [S7](../object-storage/write-path.md) and [S3](../object-storage/objects.md), the conditional
  commits it exists for.
- [F40](replication.md), the committed-order results it extends, and
  [F42](primary-failover.md), the dedup table that now remembers refusals.
- [F48](rolling-compatibility.md), the activation it is gated on.
- [F61](fragmented-partitions.md), the fragments a conditional sorted write never joins.
