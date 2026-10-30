# 342. A removed archive was kept open again by a read that opened it while it was removed

## Symptom

An archive an archive pass removed could be left in the map's cache of open archives. Its descriptor
was held until the shard stopped, and once the pass unlinked the file, so were its blocks on the
device: an archive's worth of space a time, never given back while the shard ran. No map entry named
the archive any more, so no pass would remove it again.

## Cause

`remove_archive` (`shoal-core/src/server/tables/storage/fs/map.rs`) took the archive's handle out of
the cache, awaited its close, and only then took the archive out of `all_archives`. `get_archive`
opened an archive it did not hold and kept the handle in the cache whatever the map knew of it. The
file is there until the pass unlinks it, after it settles the map
(`FileSystemCompactor::compact_archives`), so a read with a view from before the pass's repoint
that missed the cache in that window opened the file and kept it. Two orders reach it: a read that
misses while the removal waits on its close, and a read that began its open before the removal, of
an archive not yet cached since a start, and kept it after the removal had run.

## Evidence

Established by a test written first, found while
[F120](../../features/one-operation-a-record-read.md) was planned against the code (2026-10-30). A
removal is started and waits on its close; a read meanwhile asks for the archive, reads its record,
and lets its handle go; the removal ends. Against the unfixed tree:

```text
thread 'server::tables::storage::fs::tests::a_removed_archive_is_never_cached_again'
panicked at shoal-core/src/server/tables/storage/fs/tests.rs:775:9:
a removed archive was cached again
```

Not observed on the lab: it needs a read with a stale view of a partition to miss the cache in the
moments between a pass's removal and its unlink, and nothing counted descriptors or unlinked bytes
held open.

## The fix

`remove_archive` takes the archive out of `all_archives` and then out of the cache before it awaits
anything, and only then releases the cache's share of the file. `get_archive` keeps a handle it opened
only while the map is not closed and `all_archives` still names the archive, both read with nothing
awaited before the insert, so a removal is either wholly before the check, which finds the archive
gone, or after the insert, which it takes out again. An archive opened past its removal serves the
read that opened it alone, and that read's release closes it. `close_all` marks the map closed, so a
read in flight at the close keeps nothing open after it.

## Alternatives rejected

- **Check `all_archives` again only after the removal's close.** The second order, an open begun
  before the removal and finished after it, still keeps the archive.
- **A set of removed archives a read checks.** `all_archives` already says which archives the map
  knows, and a set of removals would only grow.
- **Make the removal wait for reads of the archive in flight.** A pass would wait on reads it cannot
  see, for a read that serves itself alone either way.

## Invariants to uphold

- **The cache keeps an archive only while `all_archives` names it and the map is not closed**, both
  read with nothing awaited before the insert.
- **`remove_archive` takes the archive out of `all_archives` and the cache before its first await**,
  and holds no borrow of either across it ([Resolved #111](archive-removal-borrow.md)).
- **Every archive is created through `get_active_writer`**, which names it in `all_archives` before it
  is cached, and every archive a map entry names is in `all_archives` after a restart
  ([Resolved #339](archives-known-after-restart.md)): an archive outside the set is read uncached,
  an open and a close a read.

## Still open

- **[Item 340](../known-issues.md#340-an-archive-file-the-map-no-longer-knows-is-never-deleted)**:
  an archive file no map knows is never deleted. That is a file on disk; this was a descriptor held.

## Tests

| Test | Where | Breaks if |
| --- | --- | --- |
| `a_removed_archive_is_never_cached_again` | `shoal-core/src/server/tables/storage/fs/tests.rs` | A read that opens an archive during its removal keeps it in the cache |
| `removing_an_archive_does_not_hold_the_handle_map_across_the_close` | `shoal-core/src/server/tables/storage/fs/map.rs` | The removal holds the cache's borrow across its release again, or takes another archive out with it |

## Related

[F120](../../features/one-operation-a-record-read.md), which built the shared handle;
[Resolved #341](held-handle-reads-verified.md), found beside it; [Resolved #111](archive-removal-borrow.md);
[Resolved #339](archives-known-after-restart.md), whose replay keeps `all_archives` true after a
restart; [Compaction](../../storage/compaction.md) for the archive pass.
