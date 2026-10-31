# F124. A scan's page planned on the loop

## Context

A walk's page took its resident partitions in one step on its shard's loop, serializing every one
it would hand out before the loop could take another message ([F87](tablet-walk.md),
[F117](scan-class.md)). X16 measured what that cost beside the foreground: a resident sorted table's
captures held a loop up to 6.5 ms ([X16](../future-work/scans/walk-beside-the-foreground.md)). W5's
second step replaces the capture with a plan that copies nothing, and its fourth encodes each planned
partition later in a step of the loop's second inbox
([W5](../future-work/scans/tablet-reader.md#2-plan-on-the-loop-serializing-nothing)). X17 measured the
pair on a spike build: the plan held a page's loop a tenth as long at p99, 0.0125 ms against 0.127 ms
on `Keyword`, resident and written into ([X17](../future-work/scans/page-rebuilt.md#a-page-captured-against-planned)).
This is M23's second step, with the encode steps a partition each; the fourth step bounds them by
bytes ([the plan](../future-work/scans/milestones.md#m23-the-walk-rebuilt-for-load)).

## What it does

A scan's page is **planned** (`WalkBounds::plan`): its capture names each resident partition of its
range by its kind and the size its table counts for it (`Captured::Planned`), copying nothing, and
takes its archive's view in the same step, as before. A page bounded by a count of partitions selects
the smallest keys it could hold rather than sorting every resident key of its tablet, and reads no
further than the last of them, its next page going on from there.

The page's task, off the loop, reads its archives as it always did. When its merge reaches a planned
partition it asks its shard's loop for it (`ServerMsg::EncodeStep`), and the loop takes the ask as a
step of its second inbox, run only while no message waits, or once a forced step is due
([F117](scan-class.md)). The step looks the key up afresh, through the table's own capture over a range
of that key alone, and answers what it found:

- **still resident, whole**: its bytes, as a capture would have taken them, and its archive is not read;
- **still resident, an overlay**: its new rows, and the page reads the partition's archive as the map
  holds it after the step, never as the plan's view had it, and folds the rows over that;
- **resident no longer**, evicted or deleted since the plan: nothing, and the page reads the partition
  as the map holds it now, which is at least as new as what the plan saw, or finds it gone.

Every step is counted as a step its scan class held its loop for ([F123](walk-and-scan-counters.md)),
and a page says how many partitions it planned (`WalkPage::planned`) and how many bytes its capture
copied off the loop (`WalkPage::captured_bytes`, none for a planned page). A driver's page is captured
as before until M23 moves the drivers onto this walk.

## Design choices

- **The plan travels in the page's bounds.** Each schema's `walk_tablet` is generated and hands the
  bounds to its table's `walk`, so a field of `WalkBounds` reaches every table kind without a generated
  signature changing; a flag held on the loop's thread for the capture's length would have done the
  same where nobody could see it.
- **An encode step is the table's own capture of one key.** It reuses every table kind's capture rule
  exactly, the overlay's and a tombstoned partition's included, so a step and a page captured whole
  can never disagree about what a partition is.
- **An overlay's archive is the map's after its step.** W5's third fact: an overlay folded over an older
  archive state than its own could lose a tombstone that a merge swept from it in between, and hand a
  deleted row back. The page asks the map after the step answers, so the archive it folds over is the
  overlay's own state or newer.
- **The page's budget and pace see a planned partition's archive like any other.** Its chain is looked
  up before its read and its bytes drawn and paced first ([F118](scan-budget.md), [F119](scan-pacing.md)).

## Alternatives rejected

- **Encoding the planned partitions in the plan's own step.** That is the capture again, with its
  hold; the plan only helps if the bytes are taken later, a step each.
- **The page's task reading a resident partition itself.** A resident partition is the loop's; a task
  that touched one between the loop's steps could see it half applied.
- **Steps of 64 KiB already.** A sorted partition cut into pieces each folded where it is needs its own
  care, so it is the next step of its own; X17 found the longest hold, a large partition's encode, is
  unchanged by whole-partition steps, so this step alone does not meet W12's 100 µs budget for a table
  of large sorted partitions.

## Limitations

- **A step is a whole partition**, so the longest hold a scan's page takes is its largest resident
  partition's encode, 5.8 ms on `Keyword` in X17's spike, until the fourth step bounds a step by bytes.
- **An encode step captures its key's archive view too**, which it discards: the table's capture of a
  range takes both together.
- **A page whose loop is never idle waits for forced steps**, at most one every 10 ms or fifty of its
  holds ([F117](scan-class.md)), so a page of many resident partitions beside a saturated foreground takes
  that long a partition. That is the class's contract, not a defect.
- **Drivers still capture** until M23's seventh step.

## Invariants to uphold

- **A planned page copies nothing in its plan**, and every planned partition is encoded in a step over
  a fresh lookup of its key, never from the plan's view.
- **An overlay is folded only over an archive state from its step's time or later.**
- **A step never awaits**: an encode step is a capture, synchronous on the loop.
- **A page that selected its keys ends at the last of them**, so no resident key past them is skipped
  by its next page.

## Performance

X17's spike, the same design with the plan behind a switch, on the lab's side cluster: on `Keyword`
the plan's hold 0.0125 ms at p99 against the capture's 0.127, a step's hold 13 µs and a page's wait for
its steps 38 µs; on `Big`, with almost nothing resident, no difference
([X17's record](../future-work/scans/page-rebuilt.md#a-page-captured-against-planned)). This build is
measured on the lab at M23's exit.

## Tests

| Test | Proves |
| --- | --- |
| `a_planned_page_serializes_nothing_on_the_loop` (`shoal/tests/tablet_walk.rs`) | W5's M23 row: a scan's page copies nothing in its plan, plans every resident partition, and hands out what a driver's captured page does; pages of two partitions select the smallest keys and go on from the last |
| `every_kind_of_table_walks_in_key_order_as_a_scan`, `resident_changes_shadow_their_archives_as_a_scan` and the other `_as_a_scan` twins (same) | Every table kind's planned pages, resident whole, overlaid and shadowed, hand out what a captured page does |
| `a_scan_beside_writes_is_held_to_the_oracle` (`shoal/tests/cluster_fixture.rs`) | A planned scan beside overwrites, removals and new rows keeps P20 to P22 |

## Related

- [W5's plan](../future-work/scans/tablet-reader.md#2-plan-on-the-loop-serializing-nothing) and its
  [encodes in steps](../future-work/scans/tablet-reader.md#4-encode-resident-partitions-in-steps-on-the-loop)
- [X17's second half](../future-work/scans/page-rebuilt.md#the-second-half-the-plan-step-encodes-in-steps-and-rows),
  which priced this design on a spike
- [F117, a scan class on every table shard](scan-class.md), whose second inbox the steps run in
- [F123, a node's walk and scan counters](walk-and-scan-counters.md), which counts the steps
