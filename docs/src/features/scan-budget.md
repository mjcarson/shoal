# F118. A budget for transient read buffers

A node now keeps a **scan budget**, `resources.scan_memory`, carved out of `node_memory` beside the
object store's memory and before the rows' budget, and **every buffer a scan's page holds is drawn
from it before it is allocated**: the page's byte bound before the page reaches a loop, its
capture's excess before its reads start, and every archived record past what it drew before the
record is read. A draw that finds no room waits a quarter second and is shed by name; a page that
needs more than the whole budget is refused by a code no scan asks again. The permits travel with
the page and are given back when its bytes are dropped, so a slow client holds budget, never
unbounded memory. Unless named, a node keeps ~~four~~ eight mebibytes a shard since
[F125](page-reads-in-runs.md), settled at its start, and none where it answers no scans. The object store's budget is now the same kind of budget, its face
unchanged.

## Context

This is [W1](../future-work/scans/prerequisites.md#a-budget-for-transient-read-buffers)'s budget row,
one of the four the scans plan's M23 begins with, built before M23 for scan pages alone by the
user's decision of 2026-10-29.

A walk page, a backup's copy and an answer in a shard's outbox are charged to nothing: they show in
the process's resident set alone. A node past `node_memory` has every shard evict forty percent of
its rows (`shoal-core/src/server/shard.rs`, `over_memory`). So a scan's pages, driven from every
shard at once, would push the node past it and evict the hot set of every table on it: item 191's
failure again, with a scan as the guest ([W7](../future-work/scans/isolation.md#memory)). The object
store met the same failure for its own buffers with a budget of its own, every buffer drawn before
it is allocated and a draw shed after 250 ms ([S13](../object-storage/isolation.md#memory)); this is
that budget's shape for a node's scans.

## What it does

### One kind of budget

`Budget` (`shoal-core/src/server/budget.rs`) is the accounting the object store's budget had: atomics
behind an `Arc`, `try_draw`, `draw` waiting up to `DRAW_WAIT` (250 ms) and then shedding by name, a
`Permit` that gives its bytes back when dropped on any thread. It gains a name its warnings carry,
its own warning throttle, so one budget's sheds never silence another's, and the most it ever held
(`peak`). The object store's `objects::budget::Budget` is now a thin face over it, answering a shed
as the `StoreError::Budget` it always did and counting a stream's socket buffers beside its window;
its warning reads word for word as before.

### The scan budget

- **`resources.scan_memory`**, a size, like `object_memory`. Unless named, a node's start settles it
  to ~~four~~ eight mebibytes a shard (`DEFAULT_SCAN_MEMORY_A_SHARD`): room for two pages of the
  default mebibyte, each since [F125](page-reads-in-runs.md) beside the buffers its archive reads
  were read into, and what a partition past a page holds. A node whose `scans.enabled` is false keeps
  none, whatever it names, since nothing would draw on it. ~~Provisional until X16's figures.~~
- **Carved out of `node_memory`**: `shard_budget` divides what is left once object work's
  reservation and the scan budget are taken. A start whose reservations leave the rows nothing is
  refused by name, naming both, whether or not the node runs object work (the check used to run
  only with object cpus).
- **One a node**, shared by every shard through `ScanResources` (`shoal-core/src/server/scans.rs`),
  which `ShoalPool::start` makes and every shard holds; `ShoalPool::scan_budget` reads it.

### Where a scan's page draws

1. **Before it reaches a loop.** An admin walk asked as a scan's has its byte bound drawn on a task
   of its own, waiting up to 250 ms, and is handed to the loop only once it holds it; a page asked in
   process (`ShoalPool::walk`, the fixture's `WALK`) try-draws where it is admitted. A scan's page on
   a node answering no scans is refused by name before anything is drawn.
2. **At its capture.** The resident partitions it copied off the loop (`PendingWalk::held_bytes`)
   past what it drew are drawn then, or the page is refused before its reads start. Since
   [F126](encodes-in-steps.md) a planned page copies nothing at its plan and **draws each encode step's
   bytes before the step is asked**, a partition made from its pieces drawn beside them before it is
   made and their room released after.
3. **Before each archived record is read.** `PageDraws` (`shoal-core/src/server/tables/walk.rs`)
   draws whatever the page's next record, its base and fragments, takes past what the page drew, so
   no record is read past the budget. A page that finds no room ends short at the last partition it
   handed out, its next page going on from there; one that has handed out nothing is shed. Since
   [F125](page-reads-in-runs.md) a scan's page reads its archives in waves ahead of its merge, and
   **draws a wave whole before any of its reads is issued**, every run's span and every chain's
   records, then **each partition the wave read again as its merge folds it**, since the run's buffer
   is held beside the partition built from it; the shed is the same at either draw.

The permits ride the page's call across a routed hop, wait with it in the loop's second inbox, are
held while it reads, and move into the `WalkPage`, so they drop with the page's bytes once its
answer is built.

### Refusals

| Refused | When |
| --- | --- |
| `Shedding`, "the scan budget is spent" | A draw found no room within 250 ms, at any of the three points; a scan asks again after a while |
| `ResponseTooLarge` | The page needs more than the whole budget holds; no wait answers it, and the message names `resources.scan_memory` |

## Design choices

**Draw before allocating, at every point bytes are held.** The bound is drawn first, so a page with
nothing yet held cannot race past the budget; the capture's excess and each record's are drawn
before they are copied or read, so the budget is a bound, never a count of what already happened.

**The asker draws, off the loop.** A draw may wait a quarter second, which the loop cannot; the admin
walk's handler draws on a task of its own and sends the page to the loop holding its permit, as W7
has a client's scan's page draw before it is admitted.

**A short page rather than a failed one.** The walk's cursor is the last key a page handed out, so a
page that runs out of room after handing some out ends there, and nothing is lost or asked twice; a
page that could hand out nothing is shed, as a full count sheds one.

**Settled at the start, as the object cache is.** The default depends on the shard count, which only
a start knows; settling there leaves every config a test or a tool builds as it reads, so the row
budgets the earlier features pinned hold.

**One kind of budget.** The object store's and the scans' are one accounting, so a fix to one is a
fix to both; the object store keeps its own face of it, so not one object path changed.

**~~Four~~ Eight mebibytes a shard.** Two pages at once a shard (F116's count), each of the default
mebibyte plus what one partition past a page holds, ~~a twentieth~~ a tenth of a percent of a node of
eight gibibytes a shard; doubled by [F125](page-reads-in-runs.md), whose pages hold their reads'
buffers beside the partitions folded from them.

## Alternatives rejected

**A share of `node_memory`**, as the object cache's default is. A scan's need is pages in flight,
which scale with shards, not with memory.

**A budget a shard.** The memory bounded is the process's; a shard's slice of it would let one
shard's scans spill past what the node carved while another's sat idle.

**Counting what a page holds after reading it.** A count does not stop a read past the budget, which
is what evicts the rows.

**Drawing the drivers' pages from it now.** A scan could then shed a backfill by spending the budget
first; the drivers move onto the rebuilt walk at M23, with their own budget or none decided there.

## Limitations

- **Only a scan's pages draw.** A driver's page, an operator's, a backup's copy and a shard's outbox
  are still charged to nothing ([W1](../future-work/scans/prerequisites.md#a-budget-for-transient-read-buffers)
  lists them as what else gains).
- **A page's answer is not drawn.** An admin walk answers keys and counts, so its JSON is small; a
  client scan's encoded rows, M24's, draw when they exist.
- **The fold's output is counted by its inputs.** A partition folded from an overlay and its archive
  holds both inputs until the fold makes one output, which the inputs' draw covers.
- ~~**The default is provisional**, four mebibytes a shard until X16 measures what a scan's pages hold.
  It holds two pages and their reads in flight, and runs of tens of KiB, but not W5's runs as first
  drawn, two of a mebibyte a page beside the page's own: those would want six a shard for its two
  pages. The runs [W42](../future-work/scans/scan-speed.md) plans are a merge block's stretch of a
  tablet, about 12 KB, so the default stands until X17 measures them.~~ **The default is eight
  mebibytes a shard** since [F125](page-reads-in-runs.md): a page's runs are a merge block's stretch,
  but a page holds every one it read until its merge passes their last records, beside the partitions
  it folded, about twice its bound. No capture yet says what a node's scans hold at their peak
  ([O144](../appendix/optimizations.md#o144-a-scan-page-holds-its-reads-beside-the-partitions-folded-from-them)).
- **No figure in a node's stats yet.** The budget's use, peak and sheds are read in process; a
  node's counters of scans are M23's.

## Invariants to uphold

- **No buffer of a scan's page is allocated past the node's scan budget**: the bound before the page
  reaches a loop, the capture's excess before its reads, each record before it is read.
- **A page's permits drop with its bytes**, never before: they move into the `WalkPage`.
- **A scan's page that needs more than the whole budget is refused by a code no scan asks again.**
- **A node answering no scans carves nothing for them and refuses their pages before drawing.**
- **The reservations a node starts with leave the rows something of `node_memory`**, or the start
  is refused by name.
- **Each budget throttles its own warnings.**

## Performance

On a query's path: nothing. A node with `node_memory` set gives its rows ~~four~~ eight mebibytes a
shard less than before (since [F125](page-reads-in-runs.md)), which a scan's pages hold instead of the process's slack; with it unset nothing moves.
A scan's page makes a few atomic operations a draw. The object store's draws gained one relaxed
`fetch_max` each, for the peak. Measured with the other rows in X16's window: [X16](../future-work/scans/walk-beside-the-foreground.md) found no
eviction pass begun by memory beside any scan at 4 GiB a node, on either side, and the cell alone
the same on both. A capture holds no figure of the budget's own use, so the default of four
mebibytes a shard was not revisited (X16's limitations).

## Tests

| Test | Proves |
| --- | --- |
| `a_scans_buffers_are_drawn_from_its_budget_and_shed_past_it` (`shoal/tests/scan_admission.rs`) | With the budget held by one page in flight, the next is shed by name after its wait, a page past the whole budget is refused `ResponseTooLarge` at once, nothing is drawn past the budget, and every draw comes back once the pages are answered. W7's M23 row, proved now |
| `a_pages_reads_draw_past_what_it_was_admitted_with` (`shoal-core/src/server/tables/walk.rs`) | A page's reads draw only what passes what it holds and drew, keep the permits, and are shed past the budget |
| `a_scan_budget_comes_out_of_node_memory` (`shoal-core/src/server/conf.rs`) | Four mebibytes a shard once settled, a named one kept, none with scans off, carved out of the rows' budget, and a budget that takes the node refused by name in one sentence |
| `a_budget_holds_what_is_drawn_and_no_more`, `a_draw_is_shed_by_its_own_budget` (`shoal-core/src/server/budget.rs`) | The shared budget's accounting, its peak, and sheds counted by the budget that shed them |
| `a_budget_holds_what_is_drawn_and_no_more` (`shoal-core/src/server/objects/budget.rs`) and every object suite | The object store's budget unchanged on its new footing |

The admission binary's tests take one turn each (`serial()`), since the holds they use are
process-wide and every one walks the one table.

## Related

[W7](../future-work/scans/isolation.md#memory) for the design; [S13](../object-storage/isolation.md#memory)
and [F85](devices-and-pools.md) for the object store's budget it shares; [F116](scan-admission.md) and
[F117](scan-class.md) for the class and the queue a scan's page is admitted and read by.
