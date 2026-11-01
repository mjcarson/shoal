# F126. A planned partition encoded a step at a time

## Context

Since [F124](scan-page-planned.md) a scan's page plans its resident partitions on its shard's loop,
copying nothing, and has each encoded once its merge reaches it, in a step of the loop's second
inbox ([F117](scan-class.md)) over a fresh lookup of its key. Each step was a whole partition. X17
measured what that left: the plan held a page's loop a tenth as long as the capture had, but the
longest hold, a large sorted partition's encode, moved into its step unchanged, 5.8 ms on `Keyword`
([X17](../future-work/scans/page-rebuilt.md#a-page-captured-against-planned)). W7 holds a scan to no
step on a shard's loop longer than 100 µs ([W7](../future-work/scans/isolation.md#the-budget)), and
W5's fourth step bounds a step at 64 KiB, a sorted partition encoded in pieces
([W5](../future-work/scans/tablet-reader.md#4-encode-resident-partitions-in-steps-on-the-loop)). This
is M23's fourth step ([the plan](../future-work/scans/milestones.md#m23-the-walk-rebuilt-for-load)).

## What it does

An encode step asks its table for one planned partition's rows past a cursor, at most a step's bytes
of them, 64 KiB (`WalkBounds::piece`, `PieceAsk`, `Shard::encode_step`), and since
[item 347](../appendix/resolved/encode-steps-by-rows.md) at most the rows 50 µs of its loop holds at
what a row of its table has shown it costs that shard's steps (`PieceAsk::rows`, `EncodeCosts`), and since
[item 350](../appendix/resolved/encode-copy-deadline.md) no more than its copy takes in 25 µs of its
loop by the wall (`PieceAsk::until`, `ENCODE_COPY_TIME`). The table answers by what it holds of the key
now:

| The partition | The step answers |
| --- | --- |
| Held as its archive's validated bytes | The bytes shared, nothing copied on the loop (`Captured::Shared`, `ValidatedArchive::shared`), folded off it as an archived record is |
| Loaded, and fitting one step in bytes and rows, on its first | The partition whole, as a page's capture takes it: an overlay, a whole copy or a shadow. Since [item 350](../appendix/resolved/encode-copy-deadline.md) only within 64 rows (`ENCODE_ROWS_WHOLE`), since that step cannot be cut; a first piece that took every row is handed out the same way |
| A loaded sorted partition larger than a step | Its rows past the last piece's, about a step's bytes of them by the partition's own count, at most the step's rows and at least one, as a partition of them alone, tombstones included, and the archived sort key of its last row (`SortedPartition::piece`, `Captured::Piece`) |
| An unsorted partition | Its one row, whole or shared |
| Resident no longer, or with no live row | Nothing, or a shadow |

The page (`PendingWalk::encode_in_steps`) asks its steps one after another, each from the last
piece's last row, until one reaches the partition's last row or finds it whole, shared, shadowed or
resident no longer, and then makes the partition from its parts (`WalkPart`, the table's
`WalkPieces`): each part's rows within its bounds, an overlay's laid over its archive's rows within
the same bounds, a whole or shared part shadowing the archive within its own, and the rest of a
partition evicted between its steps read from its archive. The archive is read once, after the last
step, as the map holds it then. ~~The partition is made whole~~ Since [F127](folds-in-steps.md) the
parts are folded a step of the scan queue at a time and the partition handed out in pieces.

Each step's bytes are drawn from the page's scan budget before the step is asked and trued up once it
answers, the archive is drawn and paced as a read alone is, and the partition made is drawn beside
its parts, whose room is released once it is made (`PageDraws::release`). A page says how many steps
it asked (`WalkPage::encode_steps`) and the most one copied off the loop (`WalkPage::largest_step`);
a node's figures count each step as a step its scan class held its loop for
([F123](walk-and-scan-counters.md)).

Four test hooks come with it, each process wide: `faults::hold_encode_steps` holds every loop's
encode steps once a count of them has run, `encode_steps_declined` counts the holds, a test can
hold a shard's evictions (`hold_evictions`), and merges count the tombstones they sweep from
resident partitions (`tombstones_swept`). `ShoalPool::ask_walk` asks a page and leaves its answer
to be waited for, so a test can change a copy under a page whose steps it holds.

## Design choices

- **Pieces by sort key, the cursor the last row's.** A row inserted before the cursor after its
  piece was taken is not seen, and one past it is: a row made or removed while a scan runs may or
  may not be handed out ([P23](../future-work/scans/contract.md#the-contract)). Every row handed out
  is its partition's state at its piece's step, at or after the page's boundary, which is what P21
  promises of a row.
- **One archive read, after the last step.** W5's third fact asks that an overlay be folded over an
  archive state from its own step or later. A state after every step is later than each piece's, so
  a tombstone a merge swept between two steps leaves no deleted row behind it, and the rest of a
  partition evicted between them is its archive as the map holds it now, at least as new as its
  resident copy was (W5's first fact). One read serves every piece.
- **A step bounded by the partition's own count.** The step cannot know a row's encoded size before it
  encodes it; the count the table charges memory by (a row's heap, its key's and its slot of a node)
  is known, and over-counts what the row encodes to, so a piece is at most a step and one row.
- **And by rows, at what a row has shown it costs** ([item 347](../appendix/resolved/encode-steps-by-rows.md)).
  ~~A step's bytes bound its hold.~~ A step's time is mostly per row, so 64 KiB of `Keyword`'s small
  rows held a Zen1 host's loop up to 477 µs. Each shard learns what a row of each table costs its
  steps, from steps of eight rows or more, and a step takes as many rows as 50 µs holds, 32 until its
  table's steps have shown it.
- **And by its own copy's time** ([item 350](../appendix/resolved/encode-copy-deadline.md)). A count
  learned from other steps let pieces of `Keyword`'s large partitions take three to five times the rows
  50 µs holds, at twice the cpu a row, since whole partitions taken uncopied taught it. A piece's copy
  reads the clock every fourth row and ends 25 µs after its step began, half the step, for the encode
  and drop after it; a row's cost is learned from a step's own cpu.
- **A shared archive, never a copy on the loop.** An accessible partition's bytes never change
  ([F4](validated-archives.md)), so another handle to them is the partition as the step found it, and
  the copy is made in the scan queue.
- **A first step that fits takes the partition whole.** Most partitions are under 64 KiB, and they
  cost what F124's step cost: one step, no parts to make one of.
- **The cursor rides in the page's bounds.** As F124's plan did, so every table kind's generated
  `walk_tablet` reaches it unchanged; `WalkBounds` is no longer `Copy`, since a cursor is bytes.

## Alternatives rejected

- **Partitions shared copy-on-write with the page.** A write to a large partition under a page would
  clone it whole on the loop, moving the hold to the writer and making it a copy of everything.
- **A cursor by position.** A `BTreeMap` has no seek by position, and a row inserted or removed between
  steps moves every position after it.
- **A partition changed between steps started again.** One written steadily would never finish, and
  P21 is a row's promise, not a partition's.
- **An archive read a piece.** One read after the last step is as sound and reads the archive once.
- **An accessible partition's bytes copied a step at a time.** Sharing them copies nothing on the loop.

## Limitations

- **A planned partition is a step of its own**, however small, each a message through its loop
  ([O145](../appendix/optimizations.md#o145-a-planned-partitions-encode-step-is-a-message-through-its-loop-each)).
- **A row larger than a step is one step of its size.** An unsorted partition is one row, and a
  sorted piece takes a row at least.
- ~~**The partition made from its pieces is encoded whole, once, in the scan queue**: off the loop, but
  one unit of work its size, which the fifth step's steps of 64 KiB bound.~~ Since
  [F127](folds-in-steps.md) it is folded a step at a time and handed out in pieces.
- **A partition in pieces is held about twice while it is folded**: its parts and its archive for an
  overlay beside the pieces folded from them, all drawn from the scan budget.
- **A step's bound is the table's own count**, which over-counts a row's encoded bytes by its slot, so
  a piece encodes somewhat under 64 KiB.
- **An accessible partition is copied whole off the loop**, at its fold.
- **The encode hold waits at the head of the second inbox**, so a capture queued behind a held step
  waits too; a test that holds steps asks one page at a time.
- ~~**Drivers capture whole** until M23's seventh step.~~ Since [F129](drivers-on-the-rebuilt-walk.md) a driver's page is
  encoded in steps too, taken in order with its loop's messages; only a backup archive's captures.

## Invariants to uphold

- **No step on the loop takes more of a partition than a step by its own count, and one row**, nor
  more rows than its table's steps have shown 50 µs holds
  ([item 347](../appendix/resolved/encode-steps-by-rows.md)), nor more than its copy takes in 25 µs and
  three rows, nor, uncopied, more than 64 rows ([item 350](../appendix/resolved/encode-copy-deadline.md)).
  A row is never split.
- **Every piece is taken over a fresh lookup of its key**, and a partition resident no longer between
  steps has its rest read from its archive as the map holds it after the last step.
- **An overlay's pieces are folded over an archive read after the last step, never before any piece's
  step** (W5's third fact).
- **A part contributes only its rows within its bounds**, and a whole or shared part shadows the
  archive within them.
- **Every step's bytes are drawn before the step is asked.**
- **A shared buffer is only ever read.**

## Performance

In the rows below, a resident partition of 400 rows of a kibibyte ~~takes~~ took seven steps, none copying
more than 63 KiB off the loop, where F124 took one of the whole partition, about 400 KiB; an accessible one takes one step
that copies nothing. Since [item 347](../appendix/resolved/encode-steps-by-rows.md) it takes as many as
its rows' cost on the test's build needs, its first step 32 rows. Measured on the lab at M23's exit,
beside X17's whole-partition steps.

## Tests

| Test | Proves |
| --- | --- |
| `a_planned_partition_is_encoded_a_step_at_a_time` (`shoal/tests/scan_encodes.rs`) | A large resident partition is encoded in steps of at most 64 KiB and a row and handed out as a driver's capture hands it out; an accessible one is shared by one step that copies nothing |
| `an_encode_step_takes_the_rows_its_time_holds` (same) | [Item 347](../appendix/resolved/encode-steps-by-rows.md)'s row: with a row said to cost 5 µs, a 400-row partition takes at least forty steps of at most ten rows, every row handed out; by bytes alone it took seven |
| `an_encode_step_ends_once_its_rows_have_held_the_loop` (`shoal/tests/scan_encodes.rs`) | [Item 350](../appendix/resolved/encode-copy-deadline.md)'s row: with a learned cost of a nanosecond and rows that cost 5 µs each to copy, every piece ends at its copy's deadline and no step passes 100 µs on its own cpu; by its bytes it took 7 steps of 60 rows |
| `a_piece_ends_at_its_deadline_on_every_fourth_row` and `a_pieces_size_is_its_rows_recount` (`shoal-core/src/server/tables/partitions.rs`) | A piece past its deadline ends at its fourth row, and its size is its copies' recount |
| `an_encode_steps_rows_are_learned_from_its_tables_steps` (`shoal-core/src/server/shard/walk.rs`) | A table's steps take 32 rows until a step of eight or more shows a row's cost, then as many as 50 µs holds; one slow step moves the cost at most 1.75 times |
| `an_evicted_partition_is_read_from_its_archive_at_or_after_the_boundary` (same) | W5's row: a partition evicted between two of a page's pieces is handed out from its archive as the map holds it after them, every row at or after the boundary; with the archive read before the steps it hands out rows older than the boundary |
| `a_swept_overlay_never_brings_a_deleted_row_back` (same) | W5's row: an overlay whose tombstones a merge swept between two pieces is folded over the archive after them, and hides every row deleted before the page; with the archive read before the steps a deleted row comes back |
| `a_planned_page_serializes_nothing_on_the_loop` and the `_as_a_scan` twins (`shoal/tests/tablet_walk.rs`) | Every table kind's planned partitions, encoded in one step each, hand out what a captured page does |
| `a_scan_beside_writes_is_held_to_the_oracle` (`shoal/tests/cluster_fixture.rs`) | A planned scan beside writes keeps P20 to P22 |

## Related

- [W5's fourth step](../future-work/scans/tablet-reader.md#4-encode-resident-partitions-in-steps-on-the-loop)
  and [why deferring is sound](../future-work/scans/tablet-reader.md#why-deferring-is-sound), the three facts the rows are named for
- [F124, a scan's page planned on the loop](scan-page-planned.md), whose steps these bound
- [F117, a scan class on every table shard](scan-class.md), whose second inbox the steps run in
- [F118, the scan budget](scan-budget.md), which every step draws on
- [X17's second half](../future-work/scans/page-rebuilt.md#the-second-half-the-plan-step-encodes-in-steps-and-rows),
  which measured the hold these steps remove
