# F129. The drivers on the rebuilt walk

## Context

[F87](tablet-walk.md)'s walk was built for the object store's drivers, the backfill, the copier,
the light scrub and the reclaimer, which pace themselves and ask for a page at a time. M23's second
to sixth steps rebuilt how a scan's page runs ([F124](scan-page-planned.md) to
[F128](device-time-pace.md)):

- the page is planned on the loop, copying nothing;
- each resident partition is encoded in steps of at most 64 KiB;
- its archives are read in runs, several in flight ahead of its merge;
- its folds run in steps, and its copy is checked first.

A driver's page still captured every resident partition whole on the loop and read its archives a
record at a time. W5 asks for one engine serving both, the drivers keeping their own admission
([W7](../future-work/scans/isolation.md#admission)), so that recovery gains what scans gain and no
second reader has to keep the walk's invariants a second time
([W5](../future-work/scans/tablet-reader.md)). This is M23's seventh step
([the plan](../future-work/scans/milestones.md#m23-the-walk-rebuilt-for-load)).

## What it does

**A driver's page is planned and read on the rebuilt walk.** Every page is planned on its loop
unless it asks to be captured (`WalkAsk::plan`, `Shard::walk_bounds`), and a scan's page always is.
A driver's page reads exactly as a scan's does:

- its resident partitions are planned with nothing copied, each encoded later in steps of at most
  64 KiB over a fresh lookup of its key;
- its archives are read in waves of runs, several in flight ahead of its merge;
- its folds run in steps, and its copy is checked before its reads' outcome.

An operator's walk, an admin walk naming no class, is a driver's page and reads so too, and so does
the fixture's `WALK`.

**Its admission, queue and pace are its own.**

- **Admission.** A shard reads two pages of its drivers' and operators' at once
  ([F116](scan-admission.md)), and no scan takes one of them.
- **Queue.** Its reads run in the medium queue, as a driver's always did, and its steps now yield
  there as a scan's do (`Steps::yielding`).
- **Pace.** It is never paced by its device's scan share, and draws nothing from the scan budget.

**Its encode steps are taken in order with the loop's messages.** A scan's step waits in the loop's
second inbox for the loop's idle time. The loop takes a driver's step as soon as it takes its
message (`EncodeAsk::class`), so a busy foreground never holds a driver's page back. Each step
still holds the loop for at most 64 KiB of a partition and one row.

**A backup's archive keeps the capture.** `WalkAsk::captured` asks for a page that captures every
resident partition whole on the loop at its boundary, as every page did before M23. The backup's
archive of a bucket's objects ([F93](operations-and-the-real-cluster.md)) walks so, since W5 names
the four drivers and the backup's walk is not M23's to move.

**Every driver asks a shed page again through its walk.** The backfill used to give up its pass on a
shed page. `TabletWalk` now asks a shed page again for every driver
([Resolved #345](../appendix/resolved/backfill-shed-page.md)).

## Design choices

- **One engine, every page planned unless it asks otherwise.** The walk's contract is kept once, and
  F87's tests read the rebuilt walk as they stand. The `_captured_whole` twins keep the capture the
  backup reads through under the same tests.
- **A driver's steps taken from the loop's main inbox.** The second inbox runs a step only while no
  message waits, or one every 10 ms. Under load a driver's page of a thousand resident partitions
  would take about ten seconds there, and a backfill restores a position's durability. Taken in
  order, each step bounded, a driver's page holds its loop exactly as long as a scan's page does.
- **Unpaced and unbudgeted, as before.** A driver's reads restore durability or reclaim space, and
  its pace is the object store's: a background request waits for its slice's foreground and its
  device's recovery ceiling ([S13](../object-storage/isolation.md#io-on-a-slice)). A scan's share
  binding a backfill would hold recovery behind a client's export.
- **Steps that yield in the drivers' own queue.** A planned page's folds and encodes are a scan's
  code, written to offer the core up every 64 KiB. In the medium queue they give it up whenever a
  queue with a goal is due, which bounds the foreground's wait behind them.
- **The backup kept on the capture.** Its archive is judged at a restore by the manifest written
  from its walk ([F93](operations-and-the-real-cluster.md)). Moving it is a change to the backup,
  whose rows and lab runs are F93's.

## Alternatives rejected

- **Drivers' pages in the scan queue**: recovery would share 100 shares with every client's export.
- **Drivers paced by the device's scan share**: described above.
- **A driver's encode steps in the second inbox**: described above.
- **The backup's walk moved now**: described above.
- **The drivers left on the capture**: two engines kept apart, and every driver's page holding its
  loop for as long as its whole capture.

## Limitations

- **A driver's reads are unpaced and now several in flight.** A reclaimer's walk of a whole tablet
  reads in runs as fast as its device answers, where it read a record at a time. What that costs
  the foreground beside a busy recovery, and what it buys the drivers, are measured at M23's exit.
- **A driver's page holds about twice its byte bound while its runs are read**, its runs beside its
  partitions, drawn from nothing, as its two pages a shard always were.
- **Every resident partition a driver's page plans is a round trip through its loop**: a message, a
  fresh lookup and an answer each, where the capture took the page in one step. Under load each waits
  behind what its loop has queued. Batching small partitions into one step is
  [O145](../appendix/optimizations.md#o145-a-planned-partitions-encode-step-is-a-message-through-its-loop-each),
  weighed at M23's exit beside the drivers' own figures.
- **The backup's walk still captures whole on the loop**, as every page did before M23.
- **A driver's page has no paced bound** ([F128](device-time-pace.md)). Its walk's deadline is the
  node's own, 30 s, as before.

## Invariants to uphold

- **Every walk page is planned unless it asks to be captured, and a scan's always is.**
- **A driver's encode step is taken in order with its loop's messages**, and is still at most 64 KiB
  of a partition and one row.
- **A driver's page is never paced by the scans' share nor drawn from the scan budget, and its
  admission is its own.**
- **A captured page captures every resident partition whole at its boundary**, and only a backup's
  archive asks for one.
- **Every driver walks through `TabletWalk`**, which asks a shed page again for it (Resolved #345).

## Performance

Measured in process: four hundred items of one tablet, archived after a restart, with a tenth of
them rewritten and resident. A driver's walk planned the forty resident partitions and read the
rest several records a read, where the walk captured whole read each archived partition alone. What
it saves the drivers, and what it costs the foreground beside them, are measured at M23's exit
beside the drivers' own figures from before.

## Tests

| Test | Proves |
| --- | --- |
| `every_walk_test_passes_on_the_rebuilt_engine` (`shoal/tests/tablet_walk.rs`) | W5's row: a driver's walk plans every resident partition, encodes each in a step at least, reads its archived ones several records a read, and hands out what the walk captured whole does, which planned nothing and read each alone. With drivers captured again it plans none |
| `a_planned_page_serializes_nothing_on_the_loop` (same) | A scan's page and a driver's are both planned, copy nothing on the loop, and hand out what a page captured whole does |
| `every_kind_of_table_walks_in_key_order` and F87's other four (same) | F87's tests, unchanged, on the rebuilt walk a driver's page reads on |
| `every_kind_of_table_walks_in_key_order_captured_whole` and the other four twins (same) | The same tests on the capture a backup's archive still reads through |
| `client_scans_never_shed_a_drivers_page` (`shoal/tests/scan_admission.rs`) | W7's row, on the rebuilt walk: with the scan class full, a driver's page is still read |
| `a_shed_page_is_asked_again_within_its_tries` (`shoal-core/src/server/shard/walk.rs`) | Resolved #345: a driver's walk asks a shed page again, up to its tries |
| The drivers' fixture rows: `returning_device_rebuilds_exactly_what_it_missed`, `move_serves_reads_and_writes_throughout`, `flipped_bit_is_found_by_a_read_and_by_a_deep_scrub`, `scrub_resumes_from_its_cursor`, `retired_object_is_reclaimed`, `objects_survive_a_backup_and_restore_by_archive` among them (`shoal/tests/cluster_fixture.rs`) | The backfill, the copier, the light scrub, the reclaimer and the backup's archive, unchanged in what they find, on the walk each now reads |

## Related

- [W5](../future-work/scans/tablet-reader.md), which asks for one engine, and
  [W7's admission](../future-work/scans/isolation.md#admission)
- [F124](scan-page-planned.md), [F125](page-reads-in-runs.md), [F126](encodes-in-steps.md) and
  [F127](folds-in-steps.md), the steps a driver's page now reads through
- [F116](scan-admission.md), whose count a driver's page is admitted by
- [Resolved #345](../appendix/resolved/backfill-shed-page.md), found on the way
