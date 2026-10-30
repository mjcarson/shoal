# F121. Merges written in key order

A merge now writes the partitions it holds **in key order**, whole records and fragments in one
order, where it wrote them in a hash map's iteration order, which std seeds at random. A key's top
twelve bits are its tablet, so each tablet's records of a merge now lie together in the merge's
block of its archive, one stretch a tablet, and a page of a walk that reads a tablet's archived
partitions in key order reads its block forward. This is the second of the two rows
[W42](../future-work/scans/scan-speed.md) added to
[W1](../future-work/scans/prerequisites.md#merges-written-in-key-order) for M23's rate, and
[O142](../appendix/optimizations.md#o142-a-merge-writes-its-partitions-in-a-hash-maps-order).

## Context

Two things write a table's archives. An archive pass copies an archive's live records in key order,
but only an archive under half live, so a table loaded once is never passed over. A merge writes
everything else: on a cluster node one job a sealed WAL segment and table, 10 MiB a segment by
default, and on a standalone node one job an intent log. Its `write_partition`
(`shoal-core/src/server/tables/storage/fs/compactor.rs`) took the partitions it held from
`loaded: HashMap<u64, T>` in that map's order, then its fragments from a second `HashMap`, so every
merge block held every tablet's partitions of its segment in no order.

W42 found what that costs a scan (2026-10-29): on a Zen1 shard a tablet's records are about 12 to a
block, about 870 KB apart, so a page reads them one read each, and O78's runs, which made a snapshot
cut ten times faster, would read about six times a page's bytes to spare a seventh of its reads. With
the records of a tablet together, a run is a tablet's records of a block, a few KiB apart. Nothing
reads a merge's order: every reader finds a record through the map, the map's intents are one a key
a job, and a cut sorts its records by archive and offset. That is what made it free to change.

## What it does

- **One order for the job.** `write_partition` collects every partition the job holds, each a
  whole record of `loaded` or a fragment of `fragments`, with its key, sorts them by key
  (`sort_unstable_by_key`; a key is in one of the two maps, never both), and writes them in that
  order, a whole record as before and a fragment appended to its chain as before.
- **Everything else as it was.** Each record's intent is staged as it is written and the job's
  staged intents are bounded the same way; the records are synced before the intents, and the map
  is repointed after both; the crash point a whole buffer of the job's map in is where it was.
- **Both kinds of merge.** A cluster node's segment merge and a standalone node's intent log
  compaction share `write_partition`, so both write in key order.

## Design choices

- **A sort of the job's keys, not a `BTreeMap`.** `loaded` and `fragments` stay hash maps, filled
  in the order the job's intents name partitions; a `BTreeMap` would change the signature of
  `IntentReadSupport::apply_intents`, which fills them, and pay a logarithm on every insert of a job
  where the sort pays once, after the last.
- **Whole records and fragments in one order.** A tablet's fragments belong to its stretch too: a
  page reads a chained partition's fragments beside its other records.
- **The keys sorted, never the records' bytes.** The sort moves a key, a reference and a flag a
  partition, never a serialized record.

## Alternatives rejected

- **Sort `to_mark` back into a random order** to keep a batch's eviction order as it was. It buys
  nothing a user can see: the order within one batch decides only which of its partitions an
  eviction takes first, and a key is a hash.
- **Write each tablet's records to an archive of its own.** A file a tablet a shard is 4,096 files
  open a table and a sync each a merge; key order within one block gets the same runs from one file.
- **Leave merges alone and consolidate a tablet's records later**, in a pass of their own. It moves
  every byte again; W42 kept it as the fallback if X37 finds a tablet's stretches too short.

## Limitations

- **Archives written before this keep their order** until an archive pass copies them, which it does
  only once they are under half live. A table loaded once before F121 is read as it was.
- **A tablet's stretch is one merge's.** A tablet whose records came from many merges has one stretch
  a merge block, and its page still reads a block apart for each; how long the stretches are on the
  lab's tables is X37's question, and Q62's.
- **A batch's eviction order is now key order.** The partitions of one merge are marked evictable in
  the order they were written, and the shard's LRU takes the first put first, so under pressure a
  merge's lower tablets are evicted before its higher ones, where the order was random. A key is a
  hash, so no user key is favoured, and any read of a partition moves it up as before.
- **Not measured on the lab yet**: X37 times a merge each way and counts a tablet's stretches.

## Invariants to uphold

- **A merge writes its records in ascending key order**, its whole records and fragments together,
  so a tablet's records of one merge are one stretch of its block.
- **Nothing reads a merge's order but a walk's reads of it.** Every other reader finds a record
  through the map, so the order can move again without a format change, and no reader may come to
  depend on it without saying so here.
- **A partition is in `loaded` or in `fragments`, never both**, so one key is written once a job.

## Performance

Measured in process (2026-10-30), `a_merge_writes_its_partitions_in_key_order`: a standalone node's
merge of 2,000 items into one block wrote 1,017 of them after a larger key before this change, as a
random order would, and none after; a block of 1,000 new buckets and a fragment of a large one was in
key order, the fragment among them. The sort is of some 9,800 keys a 10 MiB segment, estimated well
under a millisecond of the compactor's time. X37 measures a merge's time and memory each way on the
lab, a tablet's stretches on a shard, and the reads a page and the device bytes a partition of a walk
with runs, and its figures go here.

## Tests

| Test | Proves |
| --- | --- |
| `a_merge_writes_its_partitions_in_key_order` (`shoal/tests/paged_archive_map.rs`) | W42's M23 row, built now: every record of a merge block follows the one with the next smaller key, a fragment among the whole records |
| `a_crash_mid_compaction_leaves_every_note_readable` (`shoal/tests/cluster_fixture.rs`) | The crash point inside a merge's write, unchanged: a node killed mid-merge reads every row back |
| The persistent table, paged map, aligned record and resident read binaries | A merge's records read back whatever order they were written in |

## Related

[O142](../appendix/optimizations.md#o142-a-merge-writes-its-partitions-in-a-hash-maps-order);
[W1](../future-work/scans/prerequisites.md#merges-written-in-key-order) and
[W42](../future-work/scans/scan-speed.md), which made it required for M23's rate;
[X37](../future-work/scans/spikes.md#x37-a-tablets-records-together-priced), which prices it;
[F61](fragmented-partitions.md) for fragments; [F120](one-operation-a-record-read.md), the other W1
row; [Compaction](../storage/compaction.md); [Archives and the Archive
Map](../storage/archives-and-map.md).
