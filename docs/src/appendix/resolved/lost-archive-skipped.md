# 361. An archive the counters named and the disk lost failed every archive pass of its shard

## Symptom

Once an archive's file was gone while its map still named live records in it - a device that lost the
file, an operator who removed it - no archive pass on that shard copied or deleted anything of the
table again. Every other archive's dead bytes stayed on the device for good, and the shard logged *A
compaction failed before writing and will be tried again* at every try: about once a pass interval,
a minute by default, since a retried pass still waits out the interval.

The archive's own records were already unreadable, and a read said so by name (`ArchiveMissing`,
[Resolved #57](missing-archive.md)). The pass's failure spread that loss to every other archive of
the shard's table.

## Cause

An archive pass weighs every archive its map knows or counts live bytes in, least used first
(`ArchiveMap::sort_by_load`), and opened each one it chose with `DmaFile::open(&path).await?`
(`compact_archives`, `shoal-core/src/server/tables/storage/fs/compactor.rs`). A missing file failed
that open, and with it the pass, before anything was written: a `JobFailure::Retry`, whose reset
throws away what the pass had staged. Dead archives sort first, so the pass had already staged their
deletes before it reached the lost one, and threw those away too. The next try chose the same
archives in the same order and failed at the same open, so the same deletes were staged and thrown
away for good. The open came before the pass weighed how live an archive was, so even a lost archive
too live to copy failed it.

Two more paths failed the same way. A chain whose base was in a chosen archive and a fragment in the
lost one failed `read_chain` with `ArchiveMissing`, which the pass did not expect, and a chosen
archive whose file went during the pass failed `read_record` the same way: both arms tolerated only
`CorruptArchive`.

Beside it, replay put the archive of every entry it applied into `all_archives`
([Resolved #339](archives-known-after-restart.md)), and `within_archive` keeps an entry whose archive
is not there, so `all_archives` named a file that did not exist, which
[Resolved #161](failed-start-empty-archive.md)'s invariant says it never does.

## Evidence

**Established by reproducing it.** `a_pass_skips_an_archive_whose_file_is_gone_and_deletes_the_dead_ones`
(`shoal/tests/missing_archive_pass.rs`) writes 2,000 rows into one archive, one more row into an
archive of its own, hides that archive's file, writes the 2,000 again and starts the node: the start's
passes must delete the first archive, which holds nothing live. On the tree before the fix:

```text
the dead archives are still on disk 30 s after the start, an archive whose file is gone failing every pass:
["/home/mcarson/projects/claude/shoal-object-store/target/tmp/.tmpNMY6Rq/LostRow/archives/3f9922d8-247d-4e22-b753-2fa7b055b8cd"]
```

On the fix the dead archive goes within the first passes, the lost row's read is answered
`ArchiveMissing`, every other row reads back as rewritten, the shard serves on, and once the file is
put back and the node started again the lost row reads again: the pass kept what the map named. The
replay's half, `an_archive_missing_from_disk_is_not_known_after_replay`, failed on the map before the
fix with the lost archive among those it knew.

## The fix

- **A pass skips a chosen archive whose file is not there** (`compact_archives`, `is_not_found` on its
  open): never deleted, its entries kept, so a read of its records still says it is missing by name;
  the pass goes on and copies and deletes every other archive it chose. An active archive found gone
  under its writer is rotated, as one past 10 MiB is, so it takes no more records.
- **A chain or a record the pass cannot read because its archive is gone stays where it is**, with the
  archive that holds it, as a record that failed its checksum does (`kept`).
- **Each lost archive is warned of once and counted** (`note_missing`): a WARN naming the table, the
  archive and its path, and `IntegrityStats::archives_missing` in the shard's replication report.
- **Replay names an archive only if its file is there** (`on_disk`): its entries are applied and
  counted as before, so reads and passes see the records, but `all_archives` names no file that does
  not exist. Only `NotFound` leaves an archive out; one whose length could not be read for another
  reason is known, as it was.

## Alternatives rejected

- **Drop the lost archive's entries from the map.** Its records would then read as never written: a
  silent loss where the map now says by name what it lost, and a file put back would no longer be
  read.
- **Quarantine the copy from the pass**, through `report_corrupt` as a record that failed its checksum
  is. Its message names a checksum and its reason, `Checksum`, waits for an operator, while a read that
  meets `ArchiveMissing` quarantines nothing: the pass would be the only thing that did, a background
  job deciding a copy's fate the foreground never noticed. A lost archive's repair is a question of its
  own, filed as [item 363](../known-issues.md#363-a-copy-that-lost-an-archive-is-not-repaired).
- **Fix replay alone.** `sort_by_load` weighs the archives the live-bytes counters name as well as those
  the map knows, so a lost archive with live records is chosen whatever `all_archives` says; the pass's
  skip is what stops the failure.

## Invariants to uphold

- **A pass never fails for a file that is not there.** It skips it and goes on, and never deletes or
  stages the delete of an archive whose records it did not move.
- **An archive's entries are never dropped because its file is missing**: a read names the loss, and
  the file put back is read.
- **`all_archives` names only archives whose file was there when it was named**; a file lost later is
  named until a pass meets it, and every consumer of `all_archives` tolerates `NotFound` (the pass, the
  rehome's removals, the unlinks).
- **A lost archive is warned of once a compactor run**, not at every pass.

## Still open

- **A cluster copy that lost an archive is not repaired**: [item 363](../known-issues.md#363-a-copy-that-lost-an-archive-is-not-repaired).
- **A standalone node has no copy to repair from**, and keeps naming the loss.

## Tests

| Test | Where | Breaks if |
| --- | --- | --- |
| `a_pass_skips_an_archive_whose_file_is_gone_and_deletes_the_dead_ones` | `shoal/tests/missing_archive_pass.rs` | A lost archive fails the pass again, its entries are dropped or its file made again, or a read of its rows stops naming the loss |
| `an_archive_missing_from_disk_is_not_known_after_replay` | `shoal-core/src/server/tables/storage/fs/map.rs` | Replay names an archive whose file is gone, or stops applying and counting its entries |
| `an_archive_a_pass_left_on_disk_is_removed_at_the_next_start` | `shoal/tests/orphan_archives.rs` | The pass's deletes and unlinks, which the skip runs beside, change |

## Related

[Resolved #57](missing-archive.md), a read of a lost archive answered by name;
[Resolved #161](failed-start-empty-archive.md) and [#339](archives-known-after-restart.md), what
`all_archives` names; [Resolved #340](orphan-archives-swept.md), whose reading of the pass found it;
[Resolved #165](corrupt-record-compaction-loop.md), the pass that kept a corrupt record rather than fail.
