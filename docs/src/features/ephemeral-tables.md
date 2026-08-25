# F9. Ephemeral tables, and the benchmarks that need them

## Context

`EphemeralTable` was the original Shoal table. When persistent tables landed in August 2024 it was
renamed out of the way — `git` records the move as `server/table.rs -> server/tables/ephemeral.rs`
at 66% similarity — and in the same commit the one schema that used it switched to a persistent
table. It has compiled ever since and been reachable from nothing. Two years of maintenance
commits kept it building against a database it could not be a field of.

Two things gated it, and only the second was written down:

1. `TableKinds::new` (`shoal-derive/src/tables.rs`) decides whether a field is a sorted or an
   unsorted table by looking for those substrings in the type name. `"EphemeralTable"` contains
   neither, so the macro **panicked during expansion** before reaching anything else.
2. The generated `ShoalDatabase` impl calls twelve methods per field. `EphemeralTable` had two of
   them, both with different signatures.

[Table Types](../tables/table-types.md) said "treat it as dead code pending either a `#[db]`
integration or removal", and [`todos.md`](../appendix/todos.md) filed projections on ephemeral
tables as blocked on the same work.

The push to finish it came from the benchmarks rather than from the feature. Every macro workload
[F8](purpose-built-workloads.md) added measures a path *through* the storage layer, so none of
them can say how much of a number the storage layer is. `macro/get_resident` and
`macro/get_archived` bracket a disk read, but both sit on a table that writes an intent log,
waits on a durability barrier and compacts in the background. Nothing measured the engine without
the disk, so nothing could attribute a change to it.

## What it does

Two new table types, usable in any `#[shoal::db]` schema:

```rust
#[shoal::db]
pub struct MyDb {
    pub session: EphemeralUnsortedTable<Session>,
    pub event: EphemeralSortedTable<Event>,
    pub sample: PersistentUnsortedTable<Sample, FileSystem>,
}
```

They are **type aliases**, not implementations:

```rust
pub type EphemeralSortedTable<R, D, N> = PersistentSortedTable<R, NoStorage<D>, N>;
pub type EphemeralUnsortedTable<R, D, N> = PersistentUnsortedTable<R, NoStorage<D>, N>;
```

`NoStorage` (`shoal-core/src/server/tables/storage/none.rs`) is a second implementation of
`StorageSupport` beside `FileSystem`. It opens no file, spawns no compactor, registers no archive
map and reports `Loaders::None` as the loader it needs. Everything above it — routing, partitions,
filters, sort key selections, ranges, projections, SHQL, the query and response enums the derive
macros mint — is the same code the persistent tables run.

Eight new workloads, each a control for a workload that already existed:

| Workload | Control for | Isolates |
| --- | --- | --- |
| `macro/insert_ephemeral` | `macro/insert_unsorted` | The write path without durability |
| `macro/get_ephemeral` | `macro/get_resident` | The keyed read path without an engine beneath it |
| `macro/fanout/ephemeral/{1,2,4,16,64,256}` | `macro/fanout/resident/n` | The per-partition term of a get, without an engine beneath it |

Each copies every constant from the workload it mirrors — the same row count, row width,
concurrency, warmup and driver — so the pair differs in the storage engine and in nothing else. A
test in each file asserts that, comparing the two plans field by field, because any other
difference between them would land in the gap and be read as storage.

## Design choices

**A storage engine that stores nothing, not a table that has no storage.** The alternative shape
is in *Alternatives rejected*. This one means there is exactly one sorted table and one unsorted
table in the tree, and the ephemeral pair cannot fall behind them: a fix to the sorted read path
is a fix to both, and the benchmark pair genuinely differs in one variable.

**`compaction_due` is the wakeup.** A shard sweeps its tables — the sweep that releases a parked
response — only when `data_flushed || tables.compaction_due()` ([F5](flushed-sweep-gate.md)), and
`data_flushed` is set by a message that only the filesystem compactor sends. So `NoStorage` has to
answer `compaction_due` truthfully or an insert parks in `PendingResponse` and is never answered.
It tracks whether anything is actually parked rather than returning a constant `true`, which keeps
F5's rule intact: a database mixing ephemeral and persistent tables sweeps when a sweep could do
something, not on every message.

**Not sending `MarkEvictable` is the whole safety argument.** A partition is only evicted after
being marked evictable, and exactly two places mark one: the filesystem compactor
(`storage/fs/compactor.rs`) and the partition load path in the generated `load_partition`.
`NoStorage` reaches neither, so an ephemeral partition never enters the shared LRU and
`Shard::evict_data` can never choose it. This is load bearing rather than incidental: an evicted
ephemeral partition would be re-read on the next get, `load_partition` would answer `false`, and
the rows would be silently gone.

**A `Loaders::None` variant rather than an `Option`.** The shard spawns one loader per storage
kind and keeps a list of the kinds it has already spawned, keyed on `Loaders`. A table reporting a
kind it does not actually need would land in that list without anything being spawned for it, and
a table declared after it that *did* need that kind would never get one. The variant is checked in
the generated `init_storage_loaders` before the spawned set is consulted.

**Renaming, and deleting the old struct.** `EphemeralSortedTable` and `EphemeralUnsortedTable`
satisfy the macro's substring test for free — `"EphemeralUnsortedTable"` contains `"Unsorted"`,
and capital-`S` `"Sorted"` is not a substring of `"Unsorted"`, so the existing classification
works unchanged. The alternative was teaching `TableKinds::new`, `is_sorted_table` and
`is_unsorted_table` about a third name each. The old struct was deleted rather than kept: with the
aliases in place it is a second implementation of the same idea that nothing uses.

**Two more tables on the shared `Bench` schema, not a schema of their own.** A `#[shoal::db]`
struct mints a client type, a query enum and a response enum, so a second schema is a parallel
type universe the shared harness cannot be written against — the reason `schema.rs` gives for
having one schema at all. `MemItem` and `MemEvent` are field-for-field copies of `Item` and
`Event`, so the ephemeral workloads read over the same row shape as the workloads they control
for.

**The ephemeral workloads are appended to `IDS`, not interleaved.** `macro/insert_ephemeral`
reads best next to `macro/insert_unsorted`, but a workload's position in that list decides the
port a capture gives it, so inserting one in the middle would move every workload after it.

## Alternatives rejected

**A lean in-memory table.** Give the existing 202-line `EphemeralTable` the twelve methods the
`#[db]` macro calls, most of them no-ops, and keep its bare `BTreeMap<u64, SortedPartition<T>>`
with no `MaybeLoaded`, no `PendingGets` and no pending-response queue. This is the faster table,
and it is what an in-memory table "should" look like. Rejected on two grounds. It is a second
implementation of the whole table interface with no trait to keep it honest — the tree already
carries one such divergence, where unsorted `delete` and `update` silently ignored disk for
months ([Resolved Issues #4](../appendix/resolved/unsorted-disk-consultation.md)), and that was
between two tables that were at least both being used. And it makes the benchmark worse at the
job it exists for: a comparison between two different implementations is not a measurement of
storage, it is a measurement of two implementations. The fast path it would have bought is filed
in [`todos.md`](../appendix/todos.md) as a thing to do inside the existing table, guarded on the
engine, rather than as a table of its own.

**Keeping the name `EphemeralTable` and teaching the macro about it.** Three substring tests to
extend instead of one rename, on a type nothing could have depended on because nothing could
construct it in a database.

**Sending a `DataFlushed` message from `commit`.** This is the other way to get the shard to
sweep, and it is what the filesystem engine does. It costs a channel message per insert on a path
whose entire purpose is to be the cheap one. `compaction_due` is a synchronous bool read the shard
already performs.

**Contributing ephemeral memory to the shard's eviction counter.** The shard evicts when
`memory_usage > resources.memory` and targets 40% of that total. Ephemeral memory cannot be
reclaimed, so including it would make a large ephemeral table evict persistent partitions
continuously and size each pass off a number none of it could come from. It is excluded, which
means the memory limit does not bound an ephemeral table — see *Limitations*.

## Limitations

- **An insert is not answered inline.** It is committed to `NoStorage`, parked, and released on
  the next shard sweep, exactly as a persistent insert is. The sweep is immediate — the same loop
  iteration asks `compaction_due` — but the machinery is still there. This is the largest single
  thing separating an ephemeral table from what a bare in-memory map would cost.
- **`MaybeLoaded`, `PendingGets` and the `check_disk` probe are still paid.** A get on a partition
  this table has never seen still asks the engine whether it might be on disk; `NoStorage` answers
  `false` without doing anything, but the call happens. It happened on **every** get of every
  partition until [Resolved #80](../appendix/resolved/never-flushed-partitions.md); a resident
  partition is now asked once, which also means an ephemeral sorted table can be answered in
  place ([F27](grouped-responses.md)) — it never could before. A partition this table has never
  held is still asked on every get, and removing the last call is the fast path still filed in
  [TODOs](../appendix/todos.md).
- **Memory is not bounded.** `resources.memory` drives eviction, and ephemeral partitions are
  never evictable, so an ephemeral table grows until the process does. Bounding it is the caller's
  problem.
- **A storage directory is still required.** `ShoalPool::start` claims one with
  `StorageMeta::claim` before any shard is spawned (`shoal-core/src/server.rs:87`), whatever the
  tables are made of. A database of nothing but ephemeral tables still needs a valid, writable
  `storage.default.filesystem` path, and will still refuse to start if that directory was written
  by a different shard count.
- **No `stage-profile` durability phases.** `NoStorage` reports `StageDurability::None` and fills
  in no durability stages, which is correct — the query never touched a log — but it means a stage
  report over an ephemeral workload has fewer phases than one over a persistent workload, rather
  than the same phases at zero.
- **The ephemeral workloads do not opt into the instrumented layers.** `profiles()` is `false` for
  all eight. An instrumented run of `macro/insert_ephemeral` would attribute the storage layer
  directly, by subtracting its profile from `macro/insert_unsorted`'s; it is off because opting in
  doubles the two most expensive phases of a capture. Filed in [`todos.md`](../appendix/todos.md).
- **`MemItem` has no projection.** `Item` declares `ItemKeys`, so there is no ephemeral control
  for the projection read path. Filed in [`todos.md`](../appendix/todos.md).

## Invariants to uphold

- **`NoStorage` must never send a `MarkEvictable`.** Everything about ephemeral data being safe
  rests on this. If an ephemeral partition can enter the LRU, memory pressure deletes data.
- **`NoStorage::compaction_due` must be true whenever a response is parked.** It is the only thing
  that wakes the shard to release one. Returning a constant `false` hangs every insert; the bug
  would look like a client timeout, not like a storage bug.
- **`compact_if_needed` must report a `durable_pos` at or above every position `commit` handed
  out, and must never report `rotated: true`.** A rotation restarts positions at zero and makes
  the table drain everything unconditionally; these positions only climb.
- **Positions from `commit` must be distinct and rising.** The table parks each pending response
  under the position its commit returned. Two commits sharing one lose a response.
- **`Loaders::None` must be checked before the spawned-loader set, not after.** Checking after
  would mark it spawned and is harmless today only because no engine reports it twice.
- **The ephemeral and persistent halves of a workload pair must have identical plans.** The gap
  between them is reported as the storage layer. Each pair has a test asserting this; if a
  constant moves in one file it has to move in the other.
- **Every row type the schema declares must be listed in `driver::rows_in`.** A row type missing
  from it does not fail — it counts zero, and the workload reports having retrieved nothing while
  its latencies look healthy. The harness now bails on a get workload that retrieved no rows,
  which is the backstop for exactly this.

## Performance

Measured on the development machine with the CPU governor in `powersave` and at `smoke` scale, so
these are indicative rather than a baseline — the frozen numbers live in
[Performance Baseline](../performance/baseline.md) and a full capture under
`performance` is what belongs there.

| Workload | Wall clock | p50 |
| --- | --- | --- |
| `macro/insert_unsorted` | 12.52 ms | 6.11 ms |
| `macro/insert_ephemeral` | 2.88 ms | 0.97 ms |

**The write path is roughly 4× the work with durability attached**, for the same 2,000 rows at
the same width through the same driver. That figure is what the pair exists to produce, and it is
the first time this repository can state it rather than infer it from a profile.

| Workload | p50 |
| --- | --- |
| `macro/get_resident` | 57.5 µs |
| `macro/get_ephemeral` | 61.0 µs |

**The read path costs about the same either way**, which is the expected result and the useful
one: a resident get is not paying meaningfully for the engine underneath it. Had this gap been
large, it would have said the read path carries durability bookkeeping it does not use.

Adding two tables to the shared `Bench` schema changes the source fingerprint of every workload,
so **every capture taken before F9 is stale** and `shoal-bench status` says so. A fresh capture is
needed before `shoal-bench render`.

## Tests

| Test | What breaks without the feature |
| --- | --- |
| `shoal/tests/ephemeral_unsorted_table.rs::insert`, `delete`, `update`, `get_*`, `projection_returns_only_its_own_fields` | An ephemeral unsorted table cannot be declared, or does not answer |
| `shoal/tests/ephemeral_sorted_table.rs::insert`, `delete`, `update`, `exists`, `get_selects_named_sort_keys`, `get_by_range_selects_its_rows`, `exists_by_range_answers_for_its_rows`, `shql_bounds_rows_by_a_sort_key_range` | A sorted read path stops working when the engine beneath it stores nothing |
| `ephemeral_{sorted,unsorted}_table.rs::memory_pressure_does_not_evict` | The `MarkEvictable` invariant. Ephemeral rows disappear under memory pressure |
| `ephemeral_{sorted,unsorted}_table.rs::data_does_not_survive_a_restart` | The table is not actually ephemeral |
| `ephemeral_{sorted,unsorted}_table.rs::nothing_is_written_to_the_storage_directory` | A storage engine got wired up behind an ephemeral table |
| `ephemeral_unsorted_table.rs::a_persistent_table_declared_after_an_ephemeral_one_still_loads` | The `Loaders::None` guard. A persistent table declared after an ephemeral one gets no loader and cannot read from disk |
| `storage::none::tests::*` | The watermark state machine: rising positions, and a sweep asked for exactly when one is owed |
| `insert_ephemeral::tests::the_pair_differs_only_in_the_table_it_drives`, `get_ephemeral::tests::the_pair_differs_only_in_the_table_it_drives`, `fanout_ephemeral::tests::the_curves_differ_only_in_the_table_they_drive` | The comparison the workloads exist for. A constant moving in one half of a pair and not the other |
| `workload_ids::tests::the_declared_ids_are_the_registered_ones` | The eight new ids drifting from the eight new workloads |
| `get_ephemeral::tests::the_server_is_never_restarted`, `fanout_ephemeral::tests::the_server_is_never_restarted` | A restart-after-seed arm being added, which would measure an empty table |

## Related

- [Table Types](../tables/table-types.md) — what the aliases resolve to and what the pair shares
- [F5. The flushed sweep runs on a wakeup](flushed-sweep-gate.md) — the rule `compaction_due` has
  to satisfy
- [F8. Purpose-built workloads](purpose-built-workloads.md) — what the eight new workloads are
  eight more of
- [Memory and Eviction](../tables/memory-and-eviction.md) — the path an ephemeral partition never
  enters
- [Optimizations](../appendix/optimizations.md) — the sweep cost a mixed database now carries
