# 179. A fully live archive of short records was judged under half live

## Symptom

An archive pass copies the live records out of every archive whose live bytes are under half its
file. A fully live archive of short records could be judged under half live and copied by every
pass, whatever changed in it. Twelve-byte records were weighed at 12,000 bytes of a 28,016-byte
archive:

```text
a fully live archive of 28016 bytes was weighed at 12000
```

No lab table has records that short. The tmdb `Movie` rows run to hundreds of bytes, where the
error is a few percent. So this was not what made the lab's passes copy gigabytes
([O74](../optimizations.md#o74-a-zen1-nodes-compactor-falls-hundreds-of-jobs-behind-under-the-bench));
it was found while reading why they did.

## Cause

`ArchiveMap::sort_by_load` (`shoal-core/src/server/tables/storage/fs/map.rs`) summed each live
entry's `size`, and `compact_archives` compared the sum with the archive's file size. An entry's
`size` is its record's payload. The file also holds each record's 16-byte prefix (its length and
its checksum, since [F44](../../features/repair.md)) and the archive's 16-byte header. So the ratio
read low by `16 / (payload + 16)` per record. A payload of 16 bytes or less read as at most half
live with nothing dead in it. The page describing the map said the opposite: "so the ratio is
genuinely live/total, not an estimate". That was true of format 1 archives, whose records had no
checksum.

## Evidence

**Reproduced.** `a_fully_live_archive_of_short_records_is_not_under_half_live`
(`shoal-core/src/server/tables/storage/fs/tests.rs`) writes a thousand 12-byte records to the
active archive, all live, and asks `sort_by_load` what the archive holds. On the unfixed tree the
test failed with the message above: 12,000 of 28,016 bytes, 43%, so the pass would copy the archive.

## The fix

Each live entry counts its payload and its prefix (`RECORD_PREFIX_LEN`). A fully live archive is
then weighed at its file size less the header.

## Alternatives rejected

- **Compare with the file size less the prefixes.** The pass would have to count records per
  archive to know what to subtract, which is the same sum reached the long way round.
- **Store each record's whole length in its `ArchiveEntry`.** The entry's `size` is what a read
  asks for, and every reader would have to subtract the prefix again.

## Invariants to uphold

- **An archive's live bytes are counted in the units its file is measured in.** Anything added to a
  record's framing on disk is added to what `sort_by_load` counts per entry.
- **`tablet_bytes` stays payload bytes.** It is what a placement plan weighs a set by, and it is
  compared with nothing on disk.

## Still open

Nothing of this item. The 50% threshold itself is still hardcoded, which
[Compaction](../../storage/compaction.md#design-notes) discusses.

## Tests

| Test | What breaks if this is reverted |
| --- | --- |
| `a_fully_live_archive_of_short_records_is_not_under_half_live` (`shoal-core/src/server/tables/storage/fs/tests.rs`) | A fully live archive of 12-byte records is weighed at 43% of its file |
| ~~`archives_are_ordered_by_load_and_gathered_one_at_a_time`~~ `archives_are_ordered_by_load_and_gathered_in_one_pass` since [F76](../../features/paged-archive-map.md) (`shoal-core/src/server/tables/storage/fs/map.rs`) | Archives are weighed at their payload bytes alone |

## Related

- [O74](../optimizations.md#o74-a-zen1-nodes-compactor-falls-hundreds-of-jobs-behind-under-the-bench),
  where it was found.
- [Archives and the map](../../storage/archives-and-map.md), which described the ratio as exact.
