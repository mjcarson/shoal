# 161. A start that failed left an empty archive behind

## Symptom

On the lab, hyperion's 31 failed starts (the crash loop that
[#159](map-ahead-of-archive.md) caused) left 30 zero-length files under `Movie/archives/`. The
table's map named none of them, and nothing ever removed them. They did no harm beyond the
clutter and an inode each, but they made the directory misleading to read when looking for the
torn archive.

## Cause

`ArchiveMap::new` gives every open of a table's storage a fresh active archive id, and
`FileSystemCompactor::with_capacity` created that archive's file at once
(`map.get_active_writer()`), before the compactor had anything to write. The header it writes
sits in the writer's buffer until the first sync, so the file stayed zero bytes long until
something was compacted into it.

Only a clean shutdown cleaned up after that. `FileSystemCompactor::shutdown` removes its active
archive when it is empty. A start that failed never shut down, and neither did a node that was
`SIGKILL`ed before its first compaction. Every such start left one file. The map could not name
these files: `all_archives` is saved with the map, and an empty active archive is in it only once
something has saved the map since the start.

## Evidence

**Reproduced.** `a_start_that_ends_without_a_shutdown_leaves_no_empty_archive`
(`shoal/tests/cluster_fixture.rs`) starts a standalone node, kills it and starts it again three
times with no write in between, kills it once more, and counts the zero-length files under every
`archives/` directory. Against the unfixed tree:

```text
four starts ended without a shutdown and left 4 empty archives: [".../Note/archives/38ee7191-…",
  ".../Note/archives/763a0f11-…", ".../Note/archives/0743fdb4-…", ".../Note/archives/5956979f-…"]
```

## The fix

- **The active archive is created by the first record written to it.** The compactor holds its
  writer as an `Option`, and `active_writer` opens the archive the first time a job needs to
  write. A rotation in archive compaction closes the old writer and leaves the next one unopened.
  `sync_job`, `write_staged` and `shutdown` treat "no writer" as "nothing written", so there is
  nothing to sync or remove. A start that ends however it ends leaves no file it did not write to.
- **A job opens the archive before it writes anything.** A compaction of an intent log or a WAL
  segment opens it right after its loads, in the phase whose failures are retried
  ([Resolved #91](compaction-retry.md)). An archive directory that cannot be written is then a job
  that is tried again, not a failure after writing that ends the compactor.

**Found on the way, by the suite rather than by reading.** The first version opened the archive
lazily inside `write_partition`. There a failed create is fatal. `a_compaction_that_meets_an_unreadable_archive_is_tried_again`
(the sorted and unsorted tables), which takes every permission off the archive directory, then
hung: the compactor ended on the first job and the rotated logs were never compacted. Before the
fix, the archive had been created at start, before the test took the permissions away. There was
a second defect behind it, in `ArchiveMap::get_active_writer`: it added the id to `all_archives`
*before* the open. A failed create left the map naming a file that did not exist, and every later
archive compaction would fail opening it. The id is now added once the file exists.

## Alternatives rejected

- **Sweep the archive directory at load for empty files the map does not name.** A table's
  `archives/` directory is shared by every shard, and each shard's map knows only its own ids.
  At start, another shard's fresh active archive is exactly an empty file this shard's map does
  not name. The sweep could only be made safe with a per-shard naming of archives, which is a
  format change for clutter.
- **Remove the active archive on a failed start.** The failure paths are many and some are not
  reached at all: a `SIGKILL` runs no clean-up. Not creating the file is the only version that
  covers every path.
- **Write and sync the header at create, so a leftover is at least a valid empty archive.** It
  would still be a leftover, and one more fsync per start.

## Invariants to uphold

- **A file under `archives/` exists only if a record was written to it**, apart from the moment
  between its creation and its first write inside one job.
- **`all_archives` names only archives that exist.** An id enters it after its file is created,
  and leaves it before its file is deleted.
- **The active archive is opened in a job's retriable phase.** Opening it later makes an
  unwritable directory fatal to the compactor. A new job kind that writes records has to open it
  before its first write, not inside it.
- **A compactor with no writer has written nothing since it started.** `shutdown` relies on
  this to skip removing an empty archive, since there is none.

## Still open

Nothing. The 30 files hyperion's crash loop left went with the lab's re-bootstrap.

## Tests

| Test | What breaks if this is reverted |
| --- | --- |
| `a_start_that_ends_without_a_shutdown_leaves_no_empty_archive` (`shoal/tests/cluster_fixture.rs`) | Every start that ends without a shutdown leaves a zero-length archive the map never names |
| `a_compaction_that_meets_an_unreadable_archive_is_tried_again` (`shoal/tests/persistent_sorted_table.rs`, `persistent_unsorted_table.rs`) | With the archive created at the first write, an unwritable directory ends the compactor. With the id named before the open, the map names an archive that does not exist |

## Related

- [Resolved #159](map-ahead-of-archive.md), whose crash loop left the files.
- [Resolved #91](compaction-retry.md), the retry of a job that failed before writing.
- [Compaction](../../storage/compaction.md), the write path.
