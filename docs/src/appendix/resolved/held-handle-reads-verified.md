# 341. A scrub's canonical cut read an archive a pass removed meanwhile with no checksum

## Symptom

A scrub's canonical cut read the records of an archive that an archive pass removed after the cut
collected its handle as format 1 records: no checksum checked, and each read counted as unverified.
A record of that archive that had rotted on the device was hashed as whatever bytes it held, if
rkyv's check passed them, so the copy's report said `Verified`, its `unverified` counted the reads,
and its digest disagreed with its peers' with nothing naming the record. If rkyv's check refused the
bytes, the whole cut failed by that error instead of counting a checksum failure.

## Cause

An archive's format was kept in a table of the map's by archive id (`ArchiveMap::formats`,
`shoal-core/src/server/tables/storage/fs/map.rs`), decided when `get_archive` opened the archive and
read by `read_record_from` and `read_run_from` before every read (`format_of`, which answered
`Unverified` for an archive it had no entry for). `remove_archive` took the archive's entry out with
its cached handle. A get's or a walk's read takes its handle and reads with nothing awaited between,
so a removal cannot fall there. The canonical cut does await between them: it collects a handle an
archive on the shard's loop (`FileSystem::archived_cut_once`) and reads through them later, off it
(`PendingDigest::finish`, `shoal-core/src/server/replication/digest.rs`), and an archive pass on the
same executor could remove one of its archives between. The duplicated handle kept the file
readable after the unlink, as it was meant to; the format went with the map's entry.

## Evidence

Established by a test written first, found while
[F120](../../features/one-operation-a-record-read.md) was planned against the code (2026-10-30). A
record is written to an archive, a holder takes the archive's handle as a cut does, a pass's removal
takes the archive out of the map, a byte of the record is flipped on disk, and the holder reads the
record through its handle. Against the unfixed tree:

```text
thread 'server::tables::storage::fs::tests::a_held_handle_reads_a_removed_archive_verified'
panicked at shoal-core/src/server/tables/storage/fs/tests.rs:733:14:
a flipped byte in a removed archive was read as valid: ReadResult(..)
```

The read came back with the flipped byte in it, unverified. Not observed on the lab, where it needs
an archive pass to remove an archive while a scrub of a group of the same table reads it, and a
record in that archive to have rotted.

## The fix

The format travels with the handle. F120's `OpenArchive` is an archive's open file, shared, and the
format decided from its header when it was opened, and `read_record_from` and `read_run_from` verify
by the format of the handle they are given. The map's table of formats is gone, so nothing the map
does to an archive after a handle is taken changes how a read through it is verified.

## Alternatives rejected

- **Keep a format's entry while any handle of its archive is held.** A count by archive id beside
  the count the shared handle already keeps.
- **Have the cut collect each archive's format beside its handle.** It mends the cut alone; the next
  holder of a handle meets the same.
- **Read the header again through a held handle.** An extra read a record for whoever holds a handle
  of a removed archive, to learn what was known when the handle was taken.

## Invariants to uphold

- **A record is verified by the format its handle carries**, never by anything the map looks up at
  the read.
- **A handle outlives its archive's removal and unlink**: a holder's read of an archive a pass removed
  reads the bytes the map named when the holder collected it.

## Still open

- ~~**A read that opens an archive after its unlink still fails `ArchiveMissing`**, where a get or a
  walk with a stale view of a partition the pass only moved should look again: item 343, fixed in a
  change of its own.~~ Fixed: every read by key follows a partition a pass moved under it
  ([Resolved #343](moved-partition-read-again.md)).

## Tests

| Test | Where | Breaks if |
| --- | --- | --- |
| `a_held_handle_reads_a_removed_archive_verified` | `shoal-core/src/server/tables/storage/fs/tests.rs` | A read through a handle is verified by anything but the handle's own format |
| `archive_records_are_checksummed_and_a_flipped_byte_is_refused` | same | A new archive's handle stops carrying the checksummed format |
| `a_format_1_archive_reads_unverified_and_is_counted` | same | An archive from before F44 stops reading unverified, or stops being counted |

## Related

[F120](../../features/one-operation-a-record-read.md), which built the handle;
[F44](../../features/repair.md) for the canonical cut and the formats;
[Resolved #342](removed-archive-cached-again.md), found beside it;
[Resolved #111](archive-removal-borrow.md) for the removal's borrow.
