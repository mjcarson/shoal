# F123. A node's walk and scan counters

## Context

M23 rebuilds the walk for load and is judged on the lab by what a scan costs and what its pages hold
a shard's loop for ([the scans plan's M23](../future-work/scans/milestones.md#m23-the-walk-rebuilt-for-load)).
Until now a node said nothing of its walks: the spikes before M23 counted pages, reads and steps in a
spike build's own fields ([X17](../future-work/scans/page-rebuilt.md)), and a scan's budget and pace
kept their counts behind accessors no report read ([F118](scan-budget.md), [F119](scan-pacing.md)).
W10 asks that a node count its scans in its figures, and W12 judges M23 on them: pages shed by cause,
short pages by why they ended, the longest step a scan held a loop for, the scan budget's peak
([W10](../future-work/scans/api.md#node-counters), [W12](../future-work/scans/performance.md#what-a-run-records)).
This is M23's first step, so every later step is measured through the product rather than a spike's
fields.

## What it does

Every shard counts its walk pages into a `WalkClassFigures` a class, a driver's and a scan's apart:

| Figure | Counted |
| --- | --- |
| `pages` | Pages answered |
| `shed_full` | Pages refused because the shard was reading as many of the class as it admits |
| `refused` | Pages refused for any other cause than a shed: a copy that could not answer, a tablet that moved, an install begun under the page |
| `ended_limit`, `ended_bytes`, `ended_short`, `ended_range` | Why each answered page ended: its bound of partitions, its bound of bytes, short on its budget, or the end of its range |
| `resident`, `archived` | Partitions read from memory and from archives |
| `partitions`, `rows`, `bytes` | What the pages handed out |
| `resident_bytes` | Bytes copied off the loop from resident partitions |
| `reads` | Archive reads issued, a chain's records a read each |
| `steps`, `steps_past`, `longest_step_us` | Steps the pages held their loop for, a capture each until M23's encodes add theirs; those past 100 µs, 1 ms and 10 ms; and the longest since the node started |

A node's `ScanFigures` holds both classes, summed over its shards by the control thread as a shard's
evictions are ([F104](memory-in-a-capture.md)), beside the node's own: its scan budget's peak and the
draws it shed, and the bytes its scans read in their devices' idle time and on their trickle, every
device's summed. `NodeStats.scans` carries them, left out of a node's frame while it walked nothing.
`shoaladm stats` draws four of them in its storage group (scan pages, scan pages shed, scan steps past
100 µs, driver walk pages), and a bench capture keeps every member's figures in its server samples
beside its object work.

## Design choices

- **By class, never mixed.** A scan's pages and a driver's read alike but are admitted, paced and
  judged apart ([F116](scan-admission.md)): W12's budget is a scan's, and the drivers' own figures
  are M23's exit's other half.
- **A step counted by the bounds it passed, not a histogram.** A capture's hold is counted past 100 µs,
  1 ms and 10 ms. A count since the start subtracts across a run, which a longest-step figure does
  not, so a capture's samples say whether any step of a run passed W12's 100 µs; the longest since
  the start is kept as well, for a node's own view.
- **A shed is counted by what shed it, carried on the refusal.** A page refused for its class's
  count says so (`ShedCause::Full`); one the scan budget shed is the node's `budget_shed`, which the
  budget counts for every draw it sheds, a page's own and those drawn before a page reaches its loop.
  Reading the cause from a refusal's message would have tied a count to its wording.
- **The node's budget and pace from its shards' reports.** Every shard reads the node's one budget
  and its paces, so the node's figures take the larger read, which is the later, as F104 takes its
  cache's.

## Alternatives rejected

- **A counter of its own a shard sends.** The shard's report already reaches the control thread a
  second, and evictions ride it; a second message would race it for no gain.
- **Counting in the walk's read task.** The task runs off the loop and ends before the page is
  checked; a page refused at its check would be counted as answered. Every count is taken on the loop,
  where a page is admitted, captured and answered.
- **Timings as histograms.** A page's read time is the driver's to see in its windows, and a
  histogram a shard would cost every page a bucket's lookup for a figure nothing judged.

## Limitations

- **A scan page shed before it reaches its loop is counted by the budget alone**: an operator's scan
  page draws its bytes off the loop, and one the budget had no room for never reaches a shard to be
  counted by its class.
- **No listing beside scans yet**: W10 names listings in a node's figures, which M26's listing adds.
- **Steps are captures only** until M23's fourth step encodes resident partitions in steps of their
  own; the figure counts both once it does.
- **The stats view draws four figures**; the rest are in `NodeStats` and a capture, not drawn.

## Invariants to uphold

- **Every page is counted once, on the loop, where it ended**: refused at its start, refused at its
  capture, or answered or refused once its reads land. A new place a page ends counts it there.
- **A budget's shed is never counted as a class's refusal**, since the budget counts it already.
- **Figures only grow**; a node's figures are its shards' sums, and the budget's and pace's the larger
  of what its shards read.

## Performance

On a page's path: a handful of additions on the loop when a page is admitted, captured and answered,
and a sum of its partitions' rows; nothing on a query's path. The report carries some forty more
integers a shard a second.

## Tests

| Test | Proves |
| --- | --- |
| `a_nodes_walk_pages_are_counted_by_class` (`shoal/tests/cluster_fixture.rs`) | A node's figures count a tablet walked as a driver and as a scan apart, each page, partition and row handed out, why each page ended, and a step a capture |
| `a_nodes_scan_figures_are_its_shards_summed` (`shoal-proto/src/shared/protocol/stats.rs`) | A step is counted past every bound it passed, shards' classes add up with the longest kept, the budget and pace are the larger read, a node's figures from before read as none and a node that walked nothing writes none |
| `a_nodes_walk_figures_are_its_shards` (`shoal-core/src/server/control/stats.rs`) | The control thread sums its shards' figures into the node's |
| `a_samples_walk_figures_round_trip` (`shoal-loadgen/src/results.rs`) | A capture's sample keeps each member's figures, reads back as written, and one from before reads as none |
| `every_metric_reaches_the_view` (`shoaladm/src/cluster/stats/view.rs`) | The stats view draws the four new metrics |

## Related

- [The scans plan's M23](../future-work/scans/milestones.md#m23-the-walk-rebuilt-for-load), whose
  first step this is, and [W10's node counters](../future-work/scans/api.md#node-counters)
- [F104, memory in a capture](memory-in-a-capture.md), whose path the figures take
- [F116](scan-admission.md), [F118](scan-budget.md) and [F119](scan-pacing.md), whose admission,
  budget and pace the figures count
- [X17's record](../future-work/scans/page-rebuilt.md), whose spike fields these replace
