# F76. Paging the archive map

A shard's archive map no longer holds an entry in memory for every partition it has ever
archived. The index of where each partition's records are is now paged to disk: a delta of recent
changes in memory, backed by the map's intent log; immutable runs of 4 KiB pages in key order,
each with its directory and a Bloom filter in memory and its pages read as lookups need them; a
cache of those pages; and a manifest that names the runs and is the map's commit point. What a
map holds in memory is bounded by its settings, apart from its filters' bits a partition: about
a byte and a quarter where it was about fifty.

## Context

This is the optional S1 prerequisite "Paging the archive map"
([S1](../object-storage/prerequisites.md#optional)). It is a ceiling and not a correctness
matter: an object is a row and a stripe written in place is another, so the map was what capped a
bucket, near 27 million objects a GiB of memory a replica
([S3](../object-storage/objects.md#what-it-costs), [X10](../object-storage/stripe-row-costs.md#2-bytes-a-row)).
It was filed long before the object store, as [a node's archive map is bounded by
nothing](../appendix/todos.md#a-nodes-archive-map-is-bounded-by-nothing), and round 16 of the
cluster testing deferred it with the user as a multi-day feature
([cluster testing](../cluster-testing/todo.md)).

What was there:

- **A hash map of every partition.** `PartitionIndex`, a `HashMap<u64, Slot>` of sixteen byte
  slots since [O83](../appendix/optimizations.md#o83-the-partition-index-held-forty-eight-bytes-a-partition),
  and a map of fragments beside it for chained partitions
  ([F61](fragmented-partitions.md)). Nothing evicted an entry: a partition evicted from memory
  kept its map entry for the life of the shard.
- **Saved whole.** `SerializedMap`, the whole index serialized and hashed, written to a temp file
  and renamed over the last, and an intent log beside it folded in at a quarter of the saved map
  ([O62](../appendix/optimizations.md#o62-every-compaction-rewrites-the-shards-whole-archive-map)).
  An open read and deserialized the whole file.
- **What it cost.** Round 15 measured 1.2 GiB a node at 11.8 million partitions, 615 MiB after
  O83, about fifty bytes a partition with the table's growth
  ([memory at ten times the dataset](../cluster-testing/performance.md#memory-at-ten-times-the-dataset)).
  At a terabyte a node of the lab's dataset that is about 45 GiB. [Resolved
  #149](../appendix/resolved/node-memory-budget.md) tried counting the map against a shard's
  budget and reverted it: a map larger than the budget evicted every row four times a second and
  stayed over.
- **Read synchronously.** A get of a partition not in memory asked the map on the shard's loop
  before it parked (`FileSystem::load_partition`), and that answer decided whether a read was
  sent at all; the compactor, a snapshot cut, a canonical cut, a backup, a rehome and a digest
  each walked the whole index in memory.

## What it does

### The index

`ArchiveMap` keeps the archives, their handles and formats, and the counters it reports, and holds
a `PagedIndex` (`shoal-core/src/server/tables/storage/fs/index/`) in place of the hash map:

- **The delta**, `BTreeMap<u64, Change>`, every change since the last flush: a partition's chain,
  or `Removed`. It is what the intent log holds since the last commit, in memory.
- **Runs**, newest first. A run is a file `maps/Shard-N.run-<id>` of pages in key order, then a
  footer with each page's first key, the run's archive table, its filter and its counts. A page
  is a header and fixed 28 byte slots in key order, then the fragments of the slots that have
  any, under a checksum; about 145 partitions a page. A run is written once and never changed.
- **A page cache**, the least recently used pages point lookups read.
- **The manifest**, `maps/Shard-N`: the runs it is made of, the next run's id, and what the map
  counts (`all_archives`, bytes, partitions and chains per tablet, live bytes per archive).

### Lookups

`probe` answers from memory alone: `Found` from the delta or a cached page, `Absent` when the
delta removed the key or every run's filter or range rules it out, `Unknown` when a run may hold
it and its page is not cached. `chain_of` and `chains_of` read the pages they need, each page once
however many keys it serves, several in flight. Every lookup and scan answers as of its start: the
delta's answer and the run set are taken before anything is awaited, so a flush or a merge while it
reads changes nothing it sees.

### The hot path

`FileSystem::load_partition` asks `probe`. `Absent` is answered on the loop as a key the map did
not name always was. `Found` or `Unknown` is sent to the loader, which looks the partition up -
reading its index page if it has to - and reads its records; a partition the loader finds in no
run is the `Absent` a pruned partition already was, and its parked queries replay without disk.
The shard's loop never waits on an index page. A sorted partition the loader finds absent is
marked as having nothing on disk, as one the loop ruled out always was
([Resolved #80](../appendix/resolved/never-flushed-partitions.md)); a replicated apply that asked
for the read applies again without one, as it did after a prune.

### Writes

The compactor is still the map's only writer, and its repoints stay synchronous.
`set_partition`, `set_chain` and `remove_partition` write the delta and move the counters by the
chain they replace, which each caller now passes: a merge looks every partition with intents up
once at its start (`chains_of`), an archive pass gathers what it moves, a snapshot install and a
tablet drop scan the tablets they replace, and a fault or a rehome looks its key up first.

### Flush and merge

After a job the compactor settles the map (`settle_map`). Once the delta holds `delta_entries`
partitions (`flush_due`) it commits: the delta is written as a new run and taken out of the delta
with nothing awaited between; runs are merged while the newest one's entries times `merge_ratio`
are at least the next one's, a merge into the oldest dropping removals; the manifest is saved by
temp file, rename and directory sync; and the runs merged away are deleted. The intent log is
kept. Once it passes 1 MiB or `delta_entries` times 128 bytes, whichever is more (`rotate_due`),
`compact_map` commits and then deletes the log and begins it again.

### Walks

A tablet is a key's top twelve bits, so it is a key range of every run. `scan_tablets` and
`scan_all` merge the delta and every run in key order, a few pages a read and never through the
cache. The snapshot cut, a snapshot install, a tablet drop, the canonical cut, a digest's keys, an
export and a rehome step are scans; whether a shard holds any partition of some tablets is read
from the counters. An archive pass orders archives by the live bytes the map keeps per archive,
chooses its archives until their live bytes cover its budget, and gathers all of their records in
one scan (`ArchiveMap::gather`).

### Settings

Under `storage.default.filesystem.map` (or a table's own):

| Setting | Default | What it bounds |
| --- | --- | --- |
| `delta_entries` | 16384 | partitions in the delta before a flush, about 90 bytes each |
| `page_cache_bytes` | 2 MiB | the pages a map caches; below one page caches none |
| `filter_bits` | 10 | bits a key of each run's filter; zero keeps none |
| `merge_ratio` | 4 | how much larger a run is than the one above it before they merge |

Per map, that is a table on a shard: a tmdb node of six shards and three persistent tables holds
eighteen. `Stats.archive_map_bytes` reports what they hold in memory - the delta, every run's
directory, archive table and filter, the cached pages, and the counters.

## Design choices

**A log-structured merge, not a B-tree.** Partition keys are hashes, so changes land uniformly
across the key space. A paged tree updated in place dirties every page once a flush holds more
changes than there are pages, and needs a double write or a copy on write to survive a torn page.
Immutable runs merged geometrically write each entry about log4(N / delta) times and never rewrite
a page in place.

**The delta is the intent log.** The map already logged every change before repointing it
([Resolved #159](../appendix/resolved/map-ahead-of-archive.md)); the delta is that log in memory,
so nothing new has to be made durable before a repoint.

**A flush keeps the log.** Replaying a log over the runs it was flushed into changes nothing, so a
flush writes only the map's directory and the log is begun again only at its own bound. The first
build deleted and began the log on every flush, and `a_compaction_that_meets_an_unreadable_archive_is_tried_again`
hung under a delta of sixteen: the log lives under the archive directory, which that test makes
unreadable, and a job that succeeded through the archive writer it already held flushed, failed
to delete the log, and ended the compactor. The old fold met the same only at a mebibyte of
intents, which no test reached.

**The manifest carries the counters.** An open reads a manifest and the runs' footers, not every
entry, so the per tablet and per archive figures that used to be counted at open are saved with
the runs they describe, and a replay moves them by each intent.

**A change names what it replaces.** A paged index cannot say what it held for a key without a
read, and the repoints that move the counters must not yield. Every writer already held the old
chain, so it passes it, and a debug build checks it against what the delta or a cached page says.

**Resident filters, decided with the user.** Without them every lookup of a key a run does not
hold reads a page: every new partition a merge builds from its intents alone, every conditional
insert expecting no row, every get of a missing key. Ten bits a key rules out about ninety nine in
a hundred.

**Lookups that miss go to the loader.** The loader already read off the loop and already answered
a partition it could not find as absent ([Resolved #16, 51](../appendix/resolved/partition-load-failure.md)),
so an index page read joins the record read there rather than stalling the shard's loop.

**A rehome commits once.** A rehome step is done when its destination's map names the step's
archive; a commit partway would name it early. The destination's delta is written as runs no
manifest names as it fills, and one commit at the step's end names them all.

**Per map caches.** One budget per map is what the map's conf already carries; a shard's cache
shared across its tables would be fairer to a hot table and is not needed to bound memory.

## Alternatives rejected

- **One sorted file rewritten by each fold.** The simplest paging: the old snapshot, sorted and
  paged. Bounding the delta in memory makes each fold rewrite the whole index for a fixed number of
  changes, so its write cost grows with the map: at a terabyte a node, gigabytes a shard for every
  sixteen thousand changes. O62 had just taken that cost out of the old map.
- **Pages updated in place.** See the first design choice: uniform keys dirty every page, and a
  torn page needs a second copy to recover.
- **No filters.** Bounded at any size, at a page read for every absent key: an insert-heavy load
  would read most index pages on every merge. Kept as `filter_bits: 0`.
- **Filters paged with the pages.** A filter read from disk costs what the page it guards costs,
  so it saves nothing a cold lookup pays.
- **An index rebuilt from the archives' own footers.** The cluster testing's other sketch. An
  archive does not know which of its records are live, so every lookup would need every archive's
  answer, newest first.
- **Counting the map against the row budget.** #149 tried it and reverted it; bounded, the map
  needs no budget of its own, and the node budget still sees it in the process's resident memory.
- **Awaiting the page read on the shard's loop.** Simpler, but every cold lookup would stall
  every query behind it on the shard.

## Limitations

- **The filters still grow with the map.** About a byte and a quarter a partition at ten bits a
  key: 15 MB a node at round 15's 11.8 million partitions, about a GiB at a terabyte a node of the
  lab's dataset. `filter_bits: 0` removes them and every lookup of a key a run does not hold then
  reads a page.
- **The delta's cap is checked between jobs.** A job's repoints are synchronous, so a job can
  carry the delta past `delta_entries` by every partition it repointed: a segment merge, or an
  install of a whole group.
- **A cold read pays an index page.** A partition whose page is not cached costs one more device
  read before its record's, through the loader; at the default cache a map caches about 74,000
  partitions' entries.
- **A merge holds the compactor.** As the old fold did; a merge into the oldest run of a large map
  is the longest, and logs at info with how long it took once its run holds a million entries.
- **A torn page is not rebuilt.** It fails every lookup it serves; there is no rebuild by scan
  ([todos](../appendix/todos.md#archive-map-reconstruction)).
- **A rehome holds what it moves.** A step collects its moving chains in memory to read them in
  archive order, as it held the moving entries before.
- **No conversion.** A map written before F76 is refused by name at open; the lab's clusters were
  destroyed and loaded again.

## Invariants to uphold

- **The delta and the runs together are the map.** A change is in the delta until the run that
  holds it is installed, and the two happen with nothing awaited between; `stage` takes out of the
  delta only what it wrote.
- **A flush writes nothing outside the map's directory.** The intent log is deleted only by a
  rotation at its bound, and a replay over runs that hold what it logged changes nothing.
- **A run is part of the map only once a durable manifest names it.** Runs are synced before the
  manifest is saved, and a run is deleted only after a manifest that does not name it is durable.
  An open removes every run its manifest does not name.
- **A lookup or a scan takes its view before it awaits.** The delta's answer and the run set, or
  the delta's entries in the ranges and the run set; never a borrow held across a read.
- **Every change passes what the map held.** The counters are moved by difference and never
  recounted; a caller that passes the wrong old chain drifts them for good.
- **A run is immutable.** A cached page is keyed by run and page and never invalidated, which is
  only sound because nothing rewrites a run.
- **Absent is absent.** A filter has no false negatives, a page is authoritative for its range,
  and a removal shadows every older run until a merge into the oldest drops it. A conditional
  insert expecting no row commits on that answer.
- **The rehome rule.** A destination's manifest names the step's archive only at the step's
  commit.

## Performance

Measured by the unit test that holds the bound (`resident_bytes_stay_bounded_as_partitions_grow`,
a debug build, delta of 1024, 64 KiB cache, ten bits a key): 200,000 partitions are four runs,
5.9 MB on disk, and 262,224 bytes in memory, the filters nearly all of it, where the hash map held
about 25 to 50 bytes a partition, 5 to 10 MB. Ten thousand lookups of keys never written read 245
pages.

### Ten copies on the lab

`tmdb_cluster.yaml` on europa, titan and hyperion, factor three, an 8 GiB node budget, the map's
defaults. A fresh cluster was grown to one, five and ten copies of the TMDB dataset as round 15
grew it (`load --copies --first-copy`), each step read once the compactors had drained; every
node was restarted; `verify --copies 10` read the rows back; and twenty minutes of the mixed drive
ran (`drive --duration 1200`, gets 70%, keyword reads 15%, updates 10%, inserts 5%). The commit
before F76 (`ad03c43`), built for the same hosts, then ran the same sequence on a fresh cluster the
same evening. One run a side, so the drive's figures are a comparison and not an A/B
(`target/lab/f76/`):

| | Before F76 | F76 |
| --- | ---: | ---: |
| Partitions a node at one / five / ten copies | 2.4 / 11.9 / 23.7 million | the same |
| Archive maps a node in memory | 79 MiB / 604 MiB / 1.2 GiB | 18–20 MiB / 46–50 MiB / 62–63 MiB |
| Maps on disk a node at ten copies | 650 MB, one file a map | 705 MB, 35–38 runs |
| Rows a node keeps at ten copies, of its 8 GiB | 1.0–1.8 GiB | 2.3–2.9 GiB |
| A node started again on ten copies, resident | 2.6 GiB | 1.3 GiB |
| From its start to its shards opening their tables (titan) | 6.4 s | 3.5 s |
| Loading copies 1–4 / 5–9 | 57,579 / 56,425 rows/s | 71,280 / 70,537 rows/s |
| `verify --copies 10` | 0 missing, 0 different | 0 missing, 0 different |
| Drive: gets a second, p99 | 71,727, 41 ms | 88,646, 30 ms |
| Drive: keyword reads a second | 15,369 | 18,995 |
| Drive: updates a second, p99 | 10,246, 230 ms | 12,666, 87 ms |
| Drive: inserts acknowledged | 6,153,133 | 7,603,457 |
| After the drive: partitions, archive maps a node | 29.0–29.1 million, 1.2 GiB | 30.0–30.3 million, 79–84 MiB |
| Rows kept through the drive | 2.0–3.2 GiB | 3.2–3.8 GiB |

**The memory the map gave back went to rows.** Both builds sat at their budget; before F76 the map
took 1.2 GiB of it at ten copies - twice round 15's 615 MiB, since the dataset has carried a release
row a movie since then - and that much fewer rows were kept. With about fifteen times less map, the
drive served 1.24 times the gets and its updates' p99 fell from 230 ms to 87 ms; one run a side does
not say how much of that is the rows kept and how much the run, but the cold reads a paged map adds -
a page read before the record's when a page is not cached - did not show in it. A restart reads a manifest and the runs' footers instead of deserializing every map whole.
The before figure is the hash map's capacity estimate; the after figure is what a paged map holds.

### Before and after on the bench

`shoaladm bench` on a copy of `tmdb_cluster.yaml`'s hosts (`f76-bench`), 200,000 TMDB rows preloaded
and the cluster destroyed after every run, ten seconds of warm-up and twenty measured, through the
endpoints, `performance` governor, each side built and deployed from its own tree: `ad03c43` against
`8439cb6`, four rounds, the first side alternating (`target/lab/f76/ab.sh`):

| Arm | Before, ops/s median [range] | After | |
| --- | ---: | ---: | --- |
| `read100`, 1 | 149,315 [145,375–153,746] | 151,820 [138,106–165,096] | within noise, 1.02× |
| `read100`, 16 | 266,670 [238,323–271,069] | 268,169 [240,612–307,070] | within noise, 1.01× |
| `rw50`, 1 | 6,600 [6,573–6,771] | 6,502 [6,340–6,574] | within noise, 0.99× |
| `rw50`, 16 | 49,134 [48,247–52,243] | 48,164 [41,324–50,348] | within noise, 0.98× |
| `insert100`, 1 | 3,299 [3,267–3,356] | 3,272 [3,234–3,321] | within noise, 0.99× |
| `insert100`, 16 | 27,530 [26,495–27,821] | 27,146 [26,644–28,085] | within noise, 0.99× |

**No cost was measured.** No arm's ranges are disjoint. The flushes and merges cost no device
writes either: 10.8 bytes written a byte sent before and 10.5 after, in `insert100` at sixteen. At
the end of that arm the archive maps held 38 MiB a node before and 10 to 14 MiB after. At this
scale the maps are small and their pages cached, so the bench measures the probe, the delta and the
flushes; what paging costs a cold read is in the drive above.

## Tests

| Test | What breaks if F76 is reverted |
| --- | --- |
| `shoal-core` `index::tests::resident_bytes_stay_bounded_as_partitions_grow` | 200,000 partitions hold under a filter's bytes and the cache's in memory, every key is found, and 10,000 absent keys read under 500 pages |
| `shoal-core` `index::tests::a_lookup_finds_the_newest_value_across_the_delta_and_runs` | the delta over a run, a newer run over an older, a removal over what an older run holds, one key at a time and in a batch |
| `shoal-core` `index::tests::a_tablet_scan_equals_the_whole_index_filtered` | 3,000 changes and removals across commits and merges: a scan of three tablet ranges is the model filtered, a whole scan is the model |
| `shoal-core` `index::tests::a_lookup_across_a_flush_and_a_merge_answers_as_of_its_start` | a lookup waiting on its page read while the key moves and its run is merged away and unlinked answers the old chain, a new one the new |
| `shoal-core` `index::tests::a_run_no_manifest_names_is_removed_at_open` | a staged run never committed is gone at the next open, and what it held with it |
| `shoal-core` `index::tests::a_torn_page_is_refused` | a flipped byte in a page is `MapCorruption` |
| `shoal-core` `bloom::tests::the_filter_has_no_false_negatives` | every key held passes, and under 2.5% of absent keys do |
| `shoal-core` `page::tests::a_page_round_trips_chains_and_removals` | whole records, chains and removals read back by key; a flipped byte is refused |
| `shoal-core` `map::tests::an_intent_log_replayed_over_its_own_commit_changes_nothing` | a crash between a commit and the log's deletion replays onto the same map and the same counters |
| `shoal-core` `map::tests::the_map_is_flushed_by_its_delta_and_its_log` | a delta at its cap is due to flush and a log past its bound to rotate; a commit empties the delta into one run and leaves the log as it was, a rotation empties the log |
| `shoal` `persistent_sorted_table::a_compaction_that_meets_an_unreadable_archive_is_tried_again` (and its unsorted twin), under `build_pressured_config` | a flush that rotated the map's log under an unreadable archive directory ended the compactor and hung the writes behind it |
| `shoal-core` `map::tests::archives_are_ordered_by_load_and_gathered_in_one_pass` | the per archive counters order archives as a pass over the index would, and one gather finds every chosen archive's records |
| `shoal-core` `map::tests::a_chain_is_counted_gathered_and_saved` | a chain's fragments counted and gathered under both archives, kept by a commit and an open |
| `shoal-core` `tests::tablet_bytes_follow_the_map` | the counters equal a pass over the map through 2,000 changes and commits, per tablet and per archive |
| `shoal-core` `tests::a_map_from_before_paging_is_refused_by_name` | a pre-F76 map is refused saying so, not misread |
| `shoal` `paged_archive_map::every_partition_reads_back_through_a_paged_map` | 6,000 items under a delta of 16, no cache and constant eviction, restarted: every one reads back, keys never written are absent to a get and an exists, a conditional insert is applied for a new key and refused for one only on disk, a removal survives a restart |
| `shoal` `paged_archive_map::every_bucket_reads_back_through_a_paged_map` | the same for a sorted table's buckets |

`utils::build_config` pages every integration test's maps at a delta of 256 and
`build_pressured_config` at a delta of 16 with no cache, so the persistent table, resident read,
backlog, rehome and conditional write suites all run through runs on disk.

## Related

[Archives and the archive map](../storage/archives-and-map.md) for the formats;
[F61](fragmented-partitions.md) for chains; [O83](../appendix/optimizations.md#o83-the-partition-index-held-forty-eight-bytes-a-partition)
and [O62](../appendix/optimizations.md#o62-every-compaction-rewrites-the-shards-whole-archive-map)
for what the map was; [Resolved #149](../appendix/resolved/node-memory-budget.md) for the budget it
was left out of; [F47](local-rehome.md) for the rehome rule; [S1](../object-storage/prerequisites.md#optional)
and [S3](../object-storage/objects.md#what-it-costs) for the ceiling this lifts.
