# F127. A scan page's folds in steps, and its check at the end

## Context

W5's fifth step folds, filters, projects and encodes a scan page's rows in the scan queue, every step
of 64 KiB and yielding; its sixth checks the page's copy again before anything is answered
([W5](../future-work/scans/tablet-reader.md#5-fold-filter-project-and-encode-in-the-scan-queue)).
Since [F126](encodes-in-steps.md) no encode step holds a shard's loop for more than 64 KiB of a
partition, but the page then put a large partition's pieces back together in one unit of the scan
queue: every piece deserialized, its archive too, and the whole partition encoded again. The scan
queue shares its executor with the shard's loop, so a unit that long holds the loop as long, however
small its steps were: the stall F126 took off the loop's count reached the foreground all the same.
It is the common case for a sorted table after a restart, whose replayed partitions are overlays over
their archives (`check_disk`), which is how `MovieByKeyword` stands at M23's exit. This is M23's fifth
step ([the plan](../future-work/scans/milestones.md#m23-the-walk-rebuilt-for-load)).

## What it does

**A planned partition that needs a fold is folded a step at a time.** An overlay, a partition its
encode steps took in pieces, and a whole copy holding tombstones are handed to their table's fold
(`WalkPieces`, the sorted table's `walk_pieces`), which validates the partition's archive once and
reads its rows in order once, in archived form, beside each part's own rows within the part's bounds:
a resident row is the newer wherever both hold a key and a tombstone hides its key, an overlay's or an
evicted rest's archived rows lie under its own, and a whole copy's shadow the archive within its
bounds. Only the rows handed out are deserialized, and they leave as **pieces of at most 64 KiB and a
row** (`FoldedPieces`), the page's steps yielding to the scan queue after each.

**A walked partition may come out in pieces.** `WalkedPartition::more` holds the pieces after the
first (`bytes`), each a partition of a stretch of its live rows in sort order; `pieces()` reads them
in order and `walked_rows` decodes every one. Every unsorted partition, and every partition that needed
no fold, is one piece.

**The copy is checked first.** A page whose copy began an install, is installing or was quarantined
while it read is refused `Unavailable` whatever its reads found (`Shard::copy_changed`): a read that
failed under an install is not taken as a storage fault, and a corrupt record it met as the new copy's,
so it quarantines nothing.

**Nothing a scan reads becomes resident.** A scan reads archives through the map, never through a
table's loader, and resident partitions through steps that copy or share them, never through a get:
nothing it reads enters a table, its eviction order or its count of memory. It has held since F87;
a row now names it.

Two fixture verbs come with it: `WALK_START`, which asks a page and leaves its answer for `WALK_WAIT`
so the commands after it run while its reads are held (through `ShoalPool::ask_walk`), and
`HOLD_WALK_READS`, F116's hold of a class's reads before they start.

What W5's fifth step also names, filters, projections and rows encoded into chunks, is a client's:
in M23 a page answers an admin walk's keys and counts, and what a client's answer carries ~~is
[Q35](../future-work/scans/contract.md#questions-to-answer)'s to settle before M24~~ was settled by
[Q35](../future-work/scans/contract.md#q35-what-a-scans-answer-carries-2026-11-06) on 2026-11-06: rows
encoded on the shard by default, and these pieces as they are for an unfiltered, unprojected scan that
asks, under a layout version of their own.

## Design choices

- **The fold in archived form.** Archived keys compare as the sort keys do, so a part's rows and its
  archive's are merged without deserializing either; a row is deserialized only once it is handed out.
- **One ordered pass over the archive.** rkyv's archived `BTreeMap` iterates in order and seeks
  nothing, so the parts, which lie in sort order, are folded against one iterator that moves forward
  across them. The fold is an async function, so the iterator lives across its yields.
- **Pieces out, not one partition encoded again.** A partition's rows encode to one buffer in one
  call, so a partition encoded whole is one unit of work its size; pieces bound it.
- **The copy before the outcome.** An install replaces a copy's files, so what a page's reads met under
  one says nothing of the copy as it is now; refusing for the copy first keeps a fault of the old files
  from being counted, or quarantined, against the new.
- **A whole copy shadows its archive within its bounds.** A part taken from a whole partition is all of
  its rows there, so an archived row in its range is one the copy deleted.

## Alternatives rejected

- **The folded partition encoded whole**, as F126 left it: one unit of the scan queue of about three
  times the partition's bytes.
- **The archive deserialized and its rows taken by range**: one unit the archive's size.
- **The archive read from its start for every piece**: quadratic in a large partition's pieces.
- **Folding on the loop**: the archive is read off it, after the steps.
- **Folds on an executor of their own**: [Q37](../future-work/scans/contract.md#questions-to-answer)
  keeps a scan's work on the shards at M23, under a ceiling.

## Limitations

- **An archived record is validated whole, once**, a unit its size, for every archived partition a
  page reads as before.
- **A partition that needs no fold is handed out whole**: an archived record, validated and copied, a
  whole resident copy, and a shared one copied off the loop.
- **A walked sorted partition's readers read every piece.** A reader of `bytes` alone misses the rows
  past its first piece; `walked_rows` reads them all.
- ~~**Drivers fold as before** until M23's seventh step.~~ Since [F129](drivers-on-the-rebuilt-walk.md) a driver's page folds in
  steps too.
- **No filter, projection or row encoding** until a client can scan, at M24.

## Invariants to uphold

- **No fold of a scan's page holds the scan queue longer than a step's bytes of rows**, beside one
  validation of its archive.
- **A piece holds live rows alone, in sort order, and a partition's pieces are disjoint and in order.**
- **A resident row is newer than its archive's, a tombstone hides its key, and a whole part shadows its
  archive within its bounds.**
- **A page's copy is checked before its reads' outcome is taken**, and a page refused for its copy
  quarantines nothing.
- **Nothing a scan reads becomes resident.**

## Performance

In the rows below a small overlay over an archived partition of 400 rows of a kibibyte, which one
encode step takes, is folded into seven pieces or more, none over 66 KiB, where F124 and F126 folded it
whole in one unit. Measured on the lab at M23's exit, beside `MovieByKeyword` after a restart.

## Tests

| Test | Proves |
| --- | --- |
| `a_folded_partition_is_handed_out_in_pieces` (`shoal/tests/scan_encodes.rs`) | An overlay over a large archive is folded into pieces of at most 64 KiB and a row, the rows a driver's page hands out; with pieces never filled it is one piece |
| `nothing_a_scan_reads_becomes_resident` (same) | W5's row: a scan of an archived table, with evictions held, reads every partition from its archive and leaves no tablet holding a resident partition |
| `an_install_under_a_page_refuses_it` (`shoal/tests/cluster_fixture.rs`) | W5's row: a page whose copy installs a snapshot while its reads are held is refused by name; with the check left out its rows are answered |
| `a_swept_overlay_never_brings_a_deleted_row_back`, `an_evicted_partition_is_read_from_its_archive_at_or_after_the_boundary` (`shoal/tests/scan_encodes.rs`) | F126's rows, through the fold in steps |
| `every_kind_of_table_walks_in_key_order_as_a_scan` and the other `_as_a_scan` twins (`shoal/tests/tablet_walk.rs`) | Every table kind's planned partitions, folded in steps, hand out what a captured page does |

## Related

- [W5's fifth and sixth steps](../future-work/scans/tablet-reader.md#5-fold-filter-project-and-encode-in-the-scan-queue)
  and [never resident](../future-work/scans/tablet-reader.md#never-resident)
- [F126, a planned partition encoded a step at a time](encodes-in-steps.md), whose pieces this folds
- [F117, a scan class on every table shard](scan-class.md), whose queue and steps the fold yields in
- [F87, the tablet walk](tablet-walk.md), whose contract a partition in pieces keeps
