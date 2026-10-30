# 339. An archive written since the map's last commit was unknown after a restart

## Symptom

After a restart, an archive a shard had written since its archive map last committed was missing
from the map's `all_archives`. No archive pass weighed it, so it was never compacted, and once every
record in it died it was never deleted: its bytes stayed on the device for good. Its reads were
unaffected, since the map's entries still named it and a read opens what an entry names.

## Cause

`all_archives` reaches disk only in a committed manifest (`MapState::all_archives`, written by
`ArchiveMap::commit`), and is added to only when a writer opens a new active archive
(`ArchiveMap::get_active_writer`, `shoal-core/src/server/tables/storage/fs/map.rs`), which logs
nothing. A restart opens the manifest and replays the map's intent log over it
(`ArchiveMap::replay`). Replay took an archive out on a `DeleteArchive` intent and put none in on the
`Entry` and `Chain` intents that name archives. A manifest is committed only once the delta holds
16,384 partitions or the log 2 MiB (`settle_map`), so every archive made since the last commit, the
active one at least, was unknown after a restart. `sort_by_load`, which an archive pass picks its
archives from, iterates `all_archives` alone.

## Evidence

Established by a test written first, found while [F120](../../future-work/scans/scan-speed.md)'s
design was reviewed against the code (2026-10-30). An empty manifest is committed; three archives are
written after it, one holding records, one holding a chain's fragment, and one emptied and deleted;
the log names them as the compactor writes it; and the map is opened again. Against the unfixed
tree:

```text
thread '...::an_archive_written_since_the_last_commit_is_known_after_a_restart' panicked at
shoal-core/src/server/tables/storage/fs/map.rs:2092:13:
the archive holding records is unknown: {}
```

The map opened after the restart knew none of the three. Not observed on the lab, where nothing
watched for archives that never went: an archive pass skips one more than half live, so a table
loaded once, as the lab's are, leaves its archives alone either way.

## The fix

Replay puts the archive of every `Entry` it applies, and every archive a `Chain` it applies has a
record in, into `all_archives`, in log order beside the `DeleteArchive` intents that take them out.
An archive deleted after its entries were logged is taken out again by the delete that follows them,
since an archive pass makes its `DeleteArchive` durable before the archive's file is unlinked
(`compact_archives`). An entry replay skips, its record past the end of its archive, puts nothing in.

## Alternatives rejected

- **Log an archive's id when it is opened.** A new kind of map intent, which an older build's replay
  refuses, so a rollback after an upgrade could not open its own map. The entries already name every
  archive a live record is in.
- **Scan a table's archive directory at a start.** It would find item 340's files as well, but it
  would weigh files no entry names, such as a half-written archive of a job that never finished, and
  the directory is shared by a node's shards, each of whose maps knows only its own files.
- **Commit the manifest whenever an archive is made.** An index commit an archive, which the delta's
  bound exists to avoid.

## Invariants to uphold

- **Every archive a standing map entry names is in `all_archives`**, while a shard runs and after
  replay.
- **An archive's `DeleteArchive` follows every intent that names it in the log**, and is durable
  before its file is unlinked, so replay in log order ends with the archive taken out.
- **`all_archives` names only archives that exist**
  ([Resolved #161](failed-start-empty-archive.md)): replay applies an entry only if its record lies
  within its archive's length (`within_archive`), and a missing file has none.

## Still open

- **Archives this defect already left unknown stay unknown.** On a cluster that ran the build before
  this fix, an archive unknown after a restart was weighed by no pass, and a commit since folded the
  log entries that named it, so no replay names it again. Its records still read; once they die it
  is never deleted. Filed with the crash case below as [item 340](../known-issues.md).
- **An archive whose delete was committed but whose unlink a crash stopped** is unknown and on disk:
  item 340.

## Tests

| Test | Where | Breaks if |
| --- | --- | --- |
| `an_archive_written_since_the_last_commit_is_known_after_a_restart` | `shoal-core/src/server/tables/storage/fs/map.rs` | Replay stops putting back the archives the log's entries and chains name, or a delete logged after them stops taking one out |

## Related

[F76](../../features/paged-archive-map.md), whose manifest carries `all_archives`;
[Compaction](../../storage/compaction.md) for the archive pass; [Archives and the Archive
Map](../../storage/archives-and-map.md); [Resolved #161](failed-start-empty-archive.md), whose
invariant this keeps.
