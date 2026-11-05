# F104. Memory in a capture: evictions, the cache, and a preload per file

Since this feature a node's figures count the rows its shards evicted, by what began each pass,
and carry its cache of read units. A capture keeps both for each member every two seconds, beside
the member's eviction budget. And a bench may preload each file of its dataset to its own amount:
a whole table beside 1,024 of a bucket's objects.

## Context

The user chose a share of node memory as [F102](read-unit-cache.md)'s default, once a lab run
shows what that memory costs the table rows (2026-10-22). Three things stood in the run's way:
- **What evicts rows.** A shard evicts when its rows pass its own budget, or when the process's
  resident memory passes `node_memory`
  ([Resolved #149](../appendix/resolved/node-memory-budget.md)). The second binds on the lab, so
  an empty cache costs rows almost nothing and a full one has them evicted sooner. Nothing counted
  either kind of pass: a static every shard adds to was read only by a test (`EVICTED_PARTITIONS`,
  F85).
- **The cache's figures** reached only the fixture's `OBJECTS` verb (F102).
- **The bench's preload** was one value for every file of a dataset, rows or a percent. The run
  needs the TMDB `Movie` table whole, about 1.1 GiB of rows, beside the comparison's 1,024 posters
  of each size, which `--preload 1188551` would have put as objects too.

## What it does

- **Evictions by their cause.**
  - A shard's `over_memory` says why it evicts (`MemoryPressure::Budget` or `Resident`), and
    `evict_data` counts the pass under it: the pass, its partitions, and the bytes of rows they
    held as the shard counts them (`EvictionFigures`).
  - Each shard reports its own in its `ShardReplication`, and a node's `NodeStats` sums them
    (`evictions`).
- **The cache's figures.** Each shard reads the node's caches of read units, as it reads the
  erasure code's figures, and `NodeStats` carries the latest (`object_cache`, a `CacheReport`,
  which moved from shoal-core to shoal-proto).
- **In a capture**, each member's memory sample (`MemberMemory`) keeps:
  - `memory_budget`;
  - the partitions evicted by each cause, and the bytes evicted;
  - the cache's held bytes, hits and misses;
  - since [F107](cache-default-share.md), the checks a cache gave units back on instead of a
    shard evicting rows (`cache_yields`);
  - since [item 338](../appendix/resolved/resident-floor-reserve.md), the checks that found the
    node past its floor (`floor_checks`) and the partitions evicted there above a shard's reserve
    (`evicted_at_floor`), a third cause (`MemoryPressure::Floor`).
- **In `cluster stats`**: the partitions evicted by each cause, and the cache's held bytes and hits.
  The budget's help text says what the budget is since F85: the node's memory less what its object
  work, its cache and its rings take.
- **A preload per file.** `--preload-file <table or bucket>=<rows or percent>`, once a file, sets
  that file's preload over `--preload` for the rest.
  - A spec keeps it as `preload_files`, a map left out when empty.
  - A table's file is scanned at its own (`BenchSpec::preload_of`), and a bucket's objects are put
    at its own (`ObjectDriver::preloading`).
  - A name the dataset holds no file of is refused by name.
- **The report.** `stores-ab-report.py --memory` prints each arm's sides, each figure the median of
  the side's runs:
  - the highest resident memory any member reached;
  - the partitions evicted during the run, by cause;
  - the most bytes the caches held, and the share of reads they answered;
  - the paced stream's rate, p50 and p99, and since lab 2 its p99.9, which found
    [item 304](../appendix/resolved/lone-frame-loss-probe.md).

## Design choices

- **Counted where the pass starts.** The shard knows why it evicts at the moment it decides to.
  Counting there needs no second reading of resident memory, and splits the passes exactly.
- **Counters, cumulative, in the shard.** A shard owns its counts and reports them like every
  other counter. A capture reads what each member gained over a run from its first sample to its
  last, as it reads the rest.
- **The cache's figures as the codec's are.** Every shard reads one store, so a node takes one
  shard's copy. No new message reaches the control thread.
- **A file named by its table or bucket.** A dataset file is named after what it holds, and a name
  survives the folder moving.
- **`EVICTED_PARTITIONS` stays.** `object_buffers_never_evict_rows` reads it, and it costs one add a
  pass.

## Alternatives rejected

- **The resident check alone, in the control thread.** A node's process is one, so it could read
  its own resident memory once a second. But the shards decide and evict, and a count anywhere else
  would be a second judgement of the same thing.
- **Eviction's log lines.** Each table logs its evictions at `INFO`, and the lab's nodes run at
  `Warn`. Raising the level for a run writes a line per table per pass into every node's log, and
  leaves a capture nothing to read.
- **A preload per kind of file** (one for tables, one for buckets). It answers the rows' run, but
  not a dataset of two tables wanting different shares.
- **A table loaded with `tmdb-dataset-loader load` before the bench.** The bench owns its cluster,
  made fresh for each capture, so nothing loaded before it survives.

## Limitations

- **The ceiling of a cause is not visible.** A pass begun by resident memory says nothing of what
  held it. That could be the rows, the indexes, object buffers or a full cache. Since
  [item 338](../appendix/resolved/resident-floor-reserve.md) one case is told apart: a check that
  finds the node past its memory by more than eviction could free is counted as past its floor
  (`floor_checks`), so a node whose rows were never the ceiling says so.
- **Bytes evicted are the shards' estimate of the rows they held.** On the lab a shard's count of
  its rows was about a tenth of its node's resident memory
  ([Resolved #149](../appendix/resolved/node-memory-budget.md)). With the TMDB `Movie` table alone
  it was 1.01 GiB of 2.11 (2026-10-23).
- **A capture from before F104** reads zero for every new figure. The report shows zeros there, not
  dashes.
- ~~**Not yet used on the lab.** The rows' run under the cache's memory follows.~~ Used on
  2026-10-23 for the rows under F102's cache
  ([the table rows beside it](read-unit-cache.md#the-table-rows-beside-it)).

## Invariants to uphold

- **Every eviction pass is counted once, under the cause that began it**, and a pass begun by a
  shard's rows past its own budget is never counted as the process's.
- **A node's evictions are its shards' summed**; its cache's figures are the store's, never summed
  over shards.
- **A spec that names no file's preload serializes as it did before F104**, so no committed spec's
  digest moves.
- **Every new field of `NodeStats` and of a member's memory has a default**, so a reader and a
  writer of different builds agree.

## Performance

Counters added to a pass that evicts, and a read of the cache's atomics with each shard's report,
every few seconds. Nothing on a query's path.

## Tests

| Test | What fails if the feature is reverted |
| --- | --- |
| `shoal-proto` `shared::protocol::stats::tests::evictions_are_counted_by_what_began_them` | A pass is counted under its cause, shards' figures add up, and a node's figures without the new fields read as nothing evicted and no cache, and are written without them |
| `shoal-core` `control::stats::tests::a_nodes_evictions_and_cache_are_its_shards` | A node's evictions are its shards' summed by cause, and its cache's figures are the store's, counted once |
| `shoal-loadgen` `spec::tests::a_file_names_its_own_preload` | A file's own preload is taken over the spec's, the others take the spec's, and a spec that names none writes nothing of it |
| `shoaladm` `bench::args::tests::a_file_is_preloaded_by_its_own_flag` | `--preload-file` sets a table's and a bucket's own preloads over `--preload`, and a value with no name, no value or a bad value is refused at the line |
| `shoaladm` `cluster::stats::view::tests::every_metric_reaches_the_view` (extended) | The evictions and the cache reach the stats view |

## Related

- [F102. A cache of the units reads verified](read-unit-cache.md), whose default the rows' run
  decides.
- [F71](bench-device-memory.md): each member's memory in a capture.
- [F66](dataset-benchmarks.md): the dataset and its preload.
- [Resolved #149](../appendix/resolved/node-memory-budget.md): the node's memory budget.
- [The rest of wave two](../object-storage/store-optimizations.md#the-rest-of-wave-two).
