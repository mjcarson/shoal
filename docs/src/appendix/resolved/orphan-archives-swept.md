# 340. An archive file no map named was never deleted

## Symptom

An archive file that no shard's map named stayed in its table's `archives/` directory for good. Its
bytes held the device, nothing read them, and no archive pass would ever remove them. Four things left
such a file:

- **A crash between an archive pass's settle and its unlink**, the case the item was filed for. A pass
  logs an archive's `DeleteArchive`, makes the log durable, takes the archive out of the map, settles
  the map, and only then unlinks the file.
- **A job that crashed after writing the first records of a fresh active archive and before logging
  any of them.** The job runs again at the next start, into another archive, and nothing names the
  first. An active archive a job before it had logged a record of stays named, and its dead bytes go
  the way a pass gives any back.
- **A build before [Resolved #339](archives-known-after-restart.md)**, which left an archive written
  since its map's last commit out of `all_archives`. Such an archive was weighed only while the map's
  counters held a live record in it, so once it died it stayed.
- **An unlink that failed.** The pass returned on the first failure, and every archive after it in its
  list stayed too.

## Cause

An archive pass weighs only the archives its map knows, `all_archives` and the archives
`archive_bytes` counts (`ArchiveMap::sort_by_load`), and deletes only those it chose. Nothing ever
looked at the directory. A shard could not have looked: the directory is shared by every executor of
the node, and each map knows only its own archives. [Resolved #161](failed-start-empty-archive.md)
rejected a sweep at load for that reason, since another shard's fresh active archive is exactly an
empty file this shard's map does not name.

The crash case was wider than the item said. It named a manifest committed by the pass's own settle.
But every compactor commits its map and begins its log again as it starts
(`FileSystemCompactor::with_capacity`, which calls `ArchiveMap::compact_map`). So after a crash
anywhere between the log's sync and the unlink, the very next start replays the `DeleteArchive`, takes
the archive out of `all_archives`, and commits a manifest without it. From then on no manifest names
the file and no log does either.

## Evidence

**Reproduced by a test written first.** `an_archive_a_pass_left_on_disk_is_removed_at_the_next_start`
(`shoal/tests/orphan_archives.rs`) writes 2,000 rows of a kibibyte, compacts them into an archive with
a restart, writes every row again, and lets the real archive pass empty the first archive. A fault
(`faults::leave_archive_unlinks`) stops the pass after its settle and before its unlinks, which leaves
the disk exactly as a crash at that point would. Then the node is started again. Against the tree with
everything but the sweep in place:

```text
thread 'an_archive_a_pass_left_on_disk_is_removed_at_the_next_start' (1651109) panicked at
shoal/tests/orphan_archives.rs:175:5:
the archives a pass took out of its map are still on disk after a start:
[".../target/tmp/.tmpjRvRgW/OrphanRow/archives/57e2a146-5b93-42d9-abe9-b5987bf32275"]
```

#339's leftover kind was **reproduced by a unit test written beside it**.
`an_archive_a_partition_names_is_kept_and_known_again` commits a map whose partition is in an archive
its set left out. It then opens the map again and asks whether the archive is known. With the open's
new step (`ArchiveMap::know_counted`) taken out:

```text
thread 'unnamed-8' (1651785) panicked at shoal-core/src/server/tables/storage/fs/sweep.rs:790:13:
the counted archive is not known again
```

**Established by reading the source**: that a compactor's start commits the map, and the job and
unlink cases. **Not looked for on the lab**, where nothing counted archive files against what the maps
name.

## The fix

**A start sweeps every persistent table's archive directory before any shard opens a table**
(`shoal-core/src/server/tables/storage/fs/sweep.rs`, `sweep_node`). `ShoalPool::start` calls it once the
rehome has laid the files out for this start's executors, and before the hosting is settled and the
shards start, while the pool holds every root's lock. Nothing else can create, name or remove an
archive while it runs. It runs in two phases, so a start with nothing to remove reads no page of any
index:

1. **Candidates.** The directory is listed first. Then every map of the table on disk is read as far as
   its manifest's counters and its intent log, with nothing replayed (`read_known`). The maps are found
   by their files: every manifest and run in `maps/` and every log in `archives/intents/` (`map_names`).
   An archive is known if the manifest knows or counts it, or if any entry or chain of the log names it.
   It is suspect if the log deletes it. A file that is unknown, or suspect, is a candidate.
2. **Confirmation**, only when there are candidates (`confirm`). Every map is opened as a shard opens it,
   its log replayed over its runs. A candidate is kept if the replayed `all_archives` knows it, if
   `archive_bytes` counts a live record in it, or if any partition of the index names it
   (`keep_named`). The index is read by one pass, and only while a candidate is left. What remains is
   removed with its bytes counted.

The sweep **never fails a start**, and removes nothing it cannot prove dead. A table is left alone,
named in the report and at `WARN`, if its directory cannot be listed, if a map or its log cannot be
read, or if it holds archives with no map on disk at all. Only regular files named in an id's own
hyphenated form are listed. A removal that fails is logged, and the next start finds the file the same
way. The directory is not synced after a removal, since a removal a crash undoes is made again.

The tables are dealt round at most four executors (`SWEEP_EXECUTORS`), one per shard cpu, each on a
thread of its own so the start's own affinity is left alone. Each registers 256 KiB
(`SWEEP_IO_MEMORY`) rather than a shard's 10 MiB, since a test's login holds locked memory to 8 MiB.
A node whose directories hold no archive builds no executor. What the sweep found is on
`ShoalPool::swept()` (`SweepReport`). A start that removed or kept anything says so at `WARN` once,
with every count, and each file removed is logged at `INFO` with its path and its bytes.

**A map's open knows every archive its counters hold live records in** (`ArchiveMap::know_counted`,
after replay), if its file is on disk. That repairs the maps #339's build left. Such an archive is
known again, so a pass deletes it once it dies rather than leaving it for a start's sweep, and reads of
it are cached again.

**A pass tries every unlink.** One already gone is no failure. One that cannot be removed is logged at
`WARN` and left to the next start's sweep, and the pass does not fail. Its map is already settled, so
trying the pass again would find nothing of the file.

**What it costs a start**, measured on europa in a release build (2026-11-05). The map had its log at
its 2 MiB bound (30,000 intents) and 1,000 archives:

| | Time |
| --- | --- |
| A shard's own open of the map | 17 to 20 ms |
| A sweep of the table with nothing to remove | 5.4 to 5.9 ms |
| ... of which decoding the log | 4.4 ms |
| ... of which listing the directory | 1.3 ms |
| A sweep confirming one orphan over an index of 200,000 partitions | 55 to 59 ms |

A twelve-shard node of ten tables whose every log is at its bound would read about 0.66 s of logs,
which four executors deal to about 0.17 s. A log is usually far short of its bound, since every
compactor's start begins it again.

## Alternatives rejected

- **Sweep inside each shard as it opens its table.** This is #161's rejected alternative. Another
  shard's fresh active archive is a file this shard's map does not name, and a shard's compactor starts
  passes and creates archives as soon as the table opens. `get_active_writer` names an archive in
  `all_archives` only after the open that creates it returns, so a sweep beside it would race a file
  being written.
- **Unlink in replay an archive the log deletes.** That covers only the crash case, and only until the
  next compactor's start folds the delete into a manifest. It covers neither the job's archive nor
  #339's. And an open for reading - the rehome's, an export's, the sweep's own - would delete files.
- **Unlink before the settle.** The window between the log's sync and the unlink shrinks, and it is
  still a window. It covers none of the other cases.
- **Name an archive's shard in its file, or log an archive as it is made.** That is a format change an
  older build's replay refuses, as #339 and #161 found. It still needs every map, since a rehome moves
  partitions between executors' archives.
- **Move orphans aside rather than remove them.** Their bytes stay on the device, which is the defect.
- **Scan every map's index at every start.** That is correct, but it is the confirming figure for every
  map of every table at every start. The first phase chooses, and only the index is trusted to keep.
- **Trust the first phase to remove.** Its view is the manifest and the raw log, not the replayed map.
  It only selects, and every removal is confirmed against every map replayed and its index.

## Invariants to uphold

- **The sweep runs only while nothing can create, name or remove an archive**: after the rehome, before
  any shard opens a table, with every root's lock held. Moved later, a fresh active archive is a
  candidate between its create and the moment `all_archives` names it.
- **A file is removed only if no map on disk names it**: no replayed `all_archives`, no `archive_bytes`,
  and no partition of any index. The first phase never removes on its own say.
- **Every map on disk is read, found by its files**, never by the executors this start runs. Any file in
  `maps/` or `archives/intents/` is taken for a map. A map that cannot be read leaves its table alone,
  and so does a directory of archives with no map at all.
- **Only a regular file named in an id's own hyphenated form is an archive.**
- **The sweep never fails a start**, and the start never waits on a removal that failed.
- **An archive's `DeleteArchive` is durable before its unlink**, and after every intent that repoints its
  records ([Resolved #339](archives-known-after-restart.md)). This is what lets the first phase treat a
  logged delete as a suspect rather than a fact.
- **After a map's open, every archive its counters count that is on disk is in `all_archives`**, so #339's
  invariant holds of maps an older build wrote.
- **The claim refuses a fresh or emptied root beside an established one**
  ([Resolved #43](marker-every-root.md), [#46](unmarked-directory-refused.md)). That keeps a maps
  directory moved by configuration from reaching the sweep, which would otherwise find every archive
  unknown. Never weaken the claim without revisiting the sweep.

## Still open

- **The lab's clusters ran builds before this one.** Their first start on it says in its log what it
  removed (`swept the archive directories of archives no map names`). It has not been run there. The
  `m23` side cluster ran builds with the sweep in every side-round of the scans plan's *Found on the
  way* (2026-11-05), but each was bootstrapped fresh, with nothing to sweep, and a sweep that finds
  nothing logs at DEBUG; the tmdb cluster's first start on a build with it is still the run that would
  say what a long-lived node's archives left.
- ~~**[Item 361](lost-archive-skipped.md)**,
  found while the pass was read for this fix: an archive the map's counters name whose file is gone fails
  every pass of its shard at its open.~~ Resolved: a pass skips it, counted and warned once
  ([Resolved #361](lost-archive-skipped.md)).
- **An unlink a pass failed waits for the next start**, since the sweep runs only at a start.
- **What a sweep removed is in the start's log and on `ShoalPool::swept()`**, not in a node's figures.

## Tests

| Test | Where | Breaks if |
| --- | --- | --- |
| `an_archive_a_pass_left_on_disk_is_removed_at_the_next_start` | `shoal/tests/orphan_archives.rs` | The start stops sweeping, sweeps after the shards open, removes a live archive, or stops counting what it removed |
| `only_archives_no_map_names_are_removed` | `shoal-core/src/server/tables/storage/fs/sweep.rs` | A sweep removes another shard's archive, misses a file no map names, or touches a file not named as an archive |
| `an_archive_whose_delete_was_committed_is_removed` | `.../fs/sweep.rs` | The item's own case is missed: a delete committed and the log begun again |
| `an_archive_its_log_deletes_is_removed` | `.../fs/sweep.rs` | A delete still in the log stops making its archive a candidate |
| `an_archive_only_the_log_names_is_kept` | `.../fs/sweep.rs` | An archive named only since the last commit, by an entry or a chain's fragment, is removed |
| `an_archive_a_partition_names_is_kept_and_known_again` | `.../fs/sweep.rs` | An archive a partition names is removed because its set left it out, a map's open stops knowing it again, or it is kept once it is dead |
| `an_archive_the_index_names_is_kept_whatever_the_counters_say` | `.../fs/sweep.rs` | The confirmation trusts the counters alone and stops reading the index |
| `archives_with_no_map_are_left_alone` | `.../fs/sweep.rs` | A directory of archives with no map on disk is swept |
| `an_unreadable_map_leaves_its_table_alone` | `.../fs/sweep.rs` | A map that cannot be read stops leaving its table alone |
| `a_maps_names_are_read_from_its_files` | `.../fs/sweep.rs` | A manifest, a run or a log stops naming its map, or a staged manifest is taken for one |
| `a_nodes_tables_are_swept_on_several_executors` | `.../fs/sweep.rs` | The tables dealt round several executors lose one's report, or an executor is started for a table with no archive |

## Related

[Resolved #339](archives-known-after-restart.md), found beside it, whose replay keeps `all_archives`
true after a restart; [Resolved #161](failed-start-empty-archive.md), which rejected a sweep inside a
shard; [Resolved #342](removed-archive-cached-again.md), the descriptor half of a removed archive;
[F47](../../features/local-rehome.md), whose rehome runs before the sweep;
[Compaction](../../storage/compaction.md#archive-compaction) for the archive pass;
[Recovery](../../storage/recovery.md) for a start.
