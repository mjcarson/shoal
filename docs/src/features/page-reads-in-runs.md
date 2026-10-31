# F125. A scan page's reads in runs, several in flight

## Context

A walk's page merges its resident partitions with its archived ones in key order, and until now read
each archived partition as its merge reached it: one record, one device read, and the merge waiting
out the device's latency before it could hand the partition out ([F87](tablet-walk.md)). W42 found
where a scan's time went: a Zen1 host spent about 46 µs of cpu a partition and read 1.01 partitions
a device read ([W42](../future-work/scans/scan-speed.md#where-a-partitions-time-goes)). W5's third
step reads a page's archives the way they lie instead: several reads in flight, and where a tablet's
records lie together, which merges written in key order make them do
([F121](merges-in-key-order.md)), runs of records read with one read
([W5](../future-work/scans/tablet-reader.md#3-read-archives-in-the-order-they-lie-on-disk)). X17
priced it on a spike build of the lab's side cluster: runs across 4 KiB with sixteen in flight read
11.1 partitions a device read at 5.6 µs a partition on a Zen1 host, 1,720,000 partitions a second
for the cluster unpaced, eleven and a half times one record at a time; reads in flight alone 3.2
times ([X17](../future-work/scans/page-rebuilt.md#what-it-says-for-the-design)). This is M23's third
step ([the plan](../future-work/scans/milestones.md#m23-the-walk-rebuilt-for-load)).

## What it does

A scan's page plans its archived reads ahead of its merge, **a wave at a time**. When the merge
reaches an archived partition no wave has planned and no resident copy shadows, the page plans a wave
from it (`PendingWalk::plan_reads`, `shoal-core/src/server/tables/walk/reads.rs`): the archived
partitions of its view the page could still hand out, counted with the resident partitions between
them as the merge will count them, up to the page's bounds of partitions and bytes. Then:

1. **Their whole records are sorted by archive and offset and cut into runs** (`run_ranges`, the
   map's rule a snapshot cut also cuts by): one archive, a span of at most a mebibyte
   (`RUN_SPAN`), gaps of other bytes of at most 4 KiB (`RUN_GAP`). A partition written as fragments
   is read alone, its chain folded as a get's is.
2. **The wave is drawn from the page's scan budget whole**, every run's span and every chain's
   records, before any of it is read ([F118](scan-budget.md)). Since [F128](device-time-pace.md) a
   wave holds at most what its device gives its scans in a second, and a page that has waited a
   second for its device ends short before its next. A wave the budget has no room for
   ends the page short at the last partition it handed out, or sheds it if it handed out none.
3. **A reader of the page's own**, a task in the scan queue beside the merge, **issues the reads in
   the order the merge needs them**, each paced by its device before it is issued
   ([F119](scan-pacing.md)), at most sixteen in flight (`READS_IN_FLIGHT`).
4. **Each run is one read** (`ArchiveMap::read_run_each`), **every record in it verified apart**
   exactly as a read of it alone verifies it, and the reader sends the run's records to the merge in
   one message once read.
5. **The merge takes each partition in key order** once its read is answered, draws the bytes it
   hands out, and folds it as it always did.

A record a run read and refused, its checksum failed or torn, fails the page when the merge reaches
it, with the error a read of it alone answers, counted once. A run that cannot be read whole, its
archive removed by a pass under the page or its read failed, is read again a record at a time by the
merge as it reaches each, so a moved record is followed to where it moved as a read of it alone
follows it ([Resolved #343](../appendix/resolved/moved-partition-read-again.md)).

A page says how many archive reads it issued (`WalkPage::reads`, which a node's figures sum,
[F123](walk-and-scan-counters.md)): a run one, a chain's records a read each. A driver's page reads
each archived partition as its merge reaches it, as before, until M23's seventh step moves the
drivers.

The scan budget's default is twice what it was: a page holds the buffers its reads were read into
beside the partitions it builds from them, so ~~four mebibytes a shard~~ eight mebibytes a shard
hold two pages of the default mebibyte ([below](#design-choices)).

## Design choices

- **Runs cut in disk order, issued in the merge's order.** A tablet's records of one merge block lie
  together, but a page reads the tablet's records from every block that holds some, and in key order
  the merge takes one partition from one block's run and the next from another's. Issued by offset, the
  run holding the merge's next partition could queue behind fifteen it reaches later; issued by the
  first partition each holds in key order, the merge waits only for its own, and the device sees the
  same sixteen reads.
- **A wave mirrors the page's bounds as its merge counts them.** The resident partitions between its
  archived ones are counted as certain or not exactly as the merge counts them, so a wave reads what
  the page can hand out and not past it; a partition that turns out to hold no live row leaves the
  page short of its bound, and the merge plans the next wave from where it stands.
- **The wave drawn whole, before any read.** Every run of a page lies across the tablet's key range in
  a fresh layout, so every run's buffer lives until the merge passes its last record, near the page's
  end: the wave's buffers are held together whatever the draws say. Drawn once, the reader never waits
  for room its own page holds.
- **The partitions handed out drawn again.** The merge's fold copies each partition out of its run's
  buffer, and the buffer stays while any record of its run waits, so a page holds its reads and its
  partitions at once, about twice its byte bound; drawing both keeps F118's rule that no buffer of a
  scan's page is allocated past the budget. The default doubled with it, by F118's own reasoning: two
  pages at once a shard, each of the default mebibyte, now beside its reads, and what one partition
  past a page holds.
- **One message a run, no allocation a record.** The reader answers a run once, a vector of its
  records' payloads, each a slice of the run's one buffer; X17's profile counted the spike's oneshot
  channel and its allocations a record among a partition's costs.
- **A record's outcome is its own.** `read_run_each` verifies every record of a run and answers each,
  so a corrupt record never fails the records beside it, and the merge meets its refusal where a read
  of it alone would have met it. The snapshot cut keeps its rule, the first refusal in offset order
  failing the cut, on top of the same verification (`read_run_from`).
- **Pacing in the reader, one pace a wave.** ~~A pace hands out credit 64 KiB at a time
  ([F119](scan-pacing.md)), so the reads of a wave take it one after another and carry what is left
  from one to the next;~~ The reads of a wave take their pace one after another, since
  [F128](device-time-pace.md) each exactly its bytes and its device time; the merge keeps its own
  pace beside it for the reads it makes alone (`PagePace::beside`), and a page's `paced` is both.

## Alternatives rejected

- **A channel and a task a record**, as X17's spike started each planned read: a oneshot, a send and
  an allocation every partition, which X17's profile found among the costs left once runs took the
  ring's.
- **Driving the reads from the merge**, a set of futures polled whenever the merge waits for one:
  nothing would be issued while the merge folds, steps, or waits for an encode step on its loop, so
  the queue would drain under the merge's own work.
- **A permit a run, given back once its records are folded.** In a tablet's fresh layout a page's
  runs all span its key range, so none is given back much before the page ends, and a reader waiting
  for room its own page's runs hold waits out the draw's 250 ms before it sheds.
- **Issuing in disk order**, above.
- **O78's gaps of 128 KiB**: over a tablet's sparse records they read several times a page's bytes
  to spare a seventh of its reads ([W42](../future-work/scans/scan-speed.md)).
- **A run's failure failing every record in it**, the cut's `read_run_from` as it was: a partition's
  outcome would depend on which records shared its read.
- **Folding each record as its run arrives**, which would give a run's buffer back at once and hold a
  page's bytes once rather than twice: a fold out of key order needs its own draw and its own steps,
  and M23's fifth step kept a page's folds in key order ([F127](folds-in-steps.md))
  ([O144](../appendix/optimizations.md#o144-a-scan-page-holds-its-reads-beside-the-partitions-folded-from-them)).

## Limitations

- **A scan's pages only** until M23's seventh step moves the drivers onto the rebuilt engine; a
  driver's page reads a record at a time.
- ~~**No bound on a device's reads in flight across pages** until M23's sixth step: each page keeps at
  most sixteen, so a node's shards scanning one device at two pages each can hold many more.~~ Since
  [F128](device-time-pace.md) at most four of a node's scan reads are in flight on a device, each
  page's sixteen waiting their turn.
- **A page holds about twice its byte bound** while it reads, its runs' buffers beside its
  partitions, which the doubled default pays for (O144).
- **A partition the merge reads alone is read alone**: a resident partition's archive after its encode
  step, one evicted since the plan, and every record of a run that could not be read whole.
- **A wave's reads are issued whole**, so a page that ends sooner than its wave planned, because a
  partition held more than its record or a draw ended it short, has read the rest for nothing; its
  reader is cancelled with it.
- ~~**The pace a wave's reader leaves unspent**, under one credit, is dropped with it.~~ Since F128 a
  read takes exactly what it reads.

## Invariants to uphold

- **A record read in a run is verified as a read of it alone is**, by `verify_record`, and refused with
  the error a read of it alone answers; **a run that cannot be read whole is read again a record at a
  time**, so no record's outcome depends on the run it was read in.
- **A wave is drawn from the page's budget before any of its reads is issued, and each read is paced
  before it is.** A new read path for a scan's page draws and paces as this one does.
- **Only a partition no resident copy shadows is planned, from the page's view.** A resident
  partition's archive is read as its encode step leaves the map, never from the plan's view (W5's
  third fact).
- **A wave is planned only once every partition of the one before it was taken**, so one reader at a
  time answers a page and every answer names a partition still ahead of its merge.
- **The merge takes partitions in key order**, whatever order their reads are answered in.
- **A page's reader ends with its page**: dropped with it, cancelled, its answers never read by
  anything else.

## Performance

X17's spike, the same design with its switches set at a node's start, on the lab's side cluster:
over merges in key order, runs across 4 KiB with sixteen in flight read 11.1 partitions a device read
at 5.6 µs of a Zen1 host's cpu a partition, 1,720,000 partitions a second for the cluster unpaced;
reads in flight alone, 3.2 times one at a time
([X17's tables](../future-work/scans/page-rebuilt.md#the-tables)). The spike paced every byte of a
page before any was read and kept a channel a record; this build paces each read before it is issued
and answers a run once. It is measured on the lab at M23's exit, with the pace M23's sixth step sets.

## Tests

| Test | Proves |
| --- | --- |
| `a_page_reads_its_archives_in_runs` (`shoal-core/src/server/tables/walk/reads.rs`) | W5's row: records a few KiB apart are one read and records 64 KiB apart another, a gap of exactly 4 KiB read through and one byte more parted, no run past a mebibyte or across archives, and every partition handed out in key order, a driver's page reading the same a read each |
| `a_scan_page_keeps_its_reads_in_flight` (same) | W42's row: a page's six reads are all in flight before any is answered, its wave drawn whole and each read paced before it is issued, and every draw given back with the page's bytes |
| `a_record_in_a_run_is_verified_as_a_lone_read_is` (same) | W42's row: a corrupt record in a run is refused with the error a read of it alone answers, counted once, the record after it on disk handed on; a page over it fails as a driver's page does, and one stopping short of it reads across its bytes |
| `a_compaction_under_a_page_is_read_where_it_moved` (same) | W5's row: records an archive pass moves under a planned page are read where they moved, and a partition a merge rewrites under one is read as its view names it, at the page's boundary |
| `every_kind_of_table_walks_in_key_order_as_a_scan` and the other `_as_a_scan` twins (`shoal/tests/tablet_walk.rs`) | Every table kind's scan pages, archived, resident and shadowed, hand out what a driver's page does |
| `a_scan_reads_only_in_idle_time_or_its_trickle` (`shoal/tests/scan_admission.rs`) | F119's row, through the reader: a scan page's reads take their device's idle time or its trickle |
| `a_scan_budget_comes_out_of_node_memory` (`shoal-core/src/server/conf.rs`) | The default of eight mebibytes a shard, carved out of `node_memory` |
| `cut_runs_coalesce_neighbours_and_split_at_the_bounds` (`shoal-core/src/server/tables/storage/fs/compactor.rs`) | A snapshot cut's runs, through the rule it now shares, unchanged |

## Related

- [W5's third step](../future-work/scans/tablet-reader.md#3-read-archives-in-the-order-they-lie-on-disk)
  and [W42](../future-work/scans/scan-speed.md), whose levers these are
- [X17](../future-work/scans/page-rebuilt.md), which priced them on a spike, and
  [X37](../future-work/scans/tablet-records-together.md), which priced the layout they read
- [F120, one operation a record read](one-operation-a-record-read.md), and
  [F121, merges written in key order](merges-in-key-order.md), which lay a tablet's records together
- [F118, the scan budget](scan-budget.md), and [F119, the pace](scan-pacing.md), which every read
  draws and is paced by
- [O78](../appendix/optimizations.md#o78-a-snapshot-cut-read-one-record-at-a-time-in-key-order),
  a snapshot cut's runs, whose rule the page shares with its own bounds
