# Exploratory spikes

~~**No spike here has run.**~~ **X16 and X20 have run** (2026-10-29). This page is the list of what has to be measured before the
[milestones](milestones.md) of scans can be more than a guess, and how each would be measured.

A milestone plan is a set of claims: that the walk rebuilt is fast enough and gentle enough, that a
pace exists that holds the foreground, that an order index is worth its cost. The pages before this
one make those claims as preferences and hang each on a question ([W13](contract.md#questions-to-answer)).
A spike is what settles a question, in the sense the object storage part gave the word
([its spikes](../../object-storage/spikes.md#what-a-spike-is)): it answers a question and says in
advance what result would change the design, its code is thrown away, it is never a prototype of the
feature, and it reports what it did not measure.

## How each is written

| Field | Holds |
| --- | --- |
| **Question** | The question, and the `Q` it serves |
| **What would change the design** | The result, stated before the run, that would move a preference |
| **Method** | What is built and what is run |
| **Where** | Host, device, filesystem, governor |
| **Records** | The table it prints |
| **Depends on** | Other spikes, a prerequisite, a gate |
| **Cost** | An afternoon, days, or a week. A guess, and labelled as one |

**Where a spike runs** is the object storage part's lab and its rules
([Where a spike runs](../../object-storage/spikes.md#where-a-spike-runs)): titan and hyperion (Zen1,
the 970 EVO, a disk), europa (the Optane, a disk); built for `znver1` in a target directory of its own;
a scratch directory on the device measured, never the tmdb cluster's; the tmdb node stopped and the
governor at `performance`, both put back; sides alternated by round, four rounds or more, a difference
counted only when the intervals do not overlap; every table labelled by host, cpu, governor, device and
filesystem.

## The spikes at a glance

| # | Spike | Settles | Needs | Cost |
| --- | --- | --- | --- | --- |
| ✅ X16 | The walk beside the foreground, [reported](walk-beside-the-foreground.md) | Q33, Q34, Q37 in part | The lab | ~~Days~~ Done 2026-10-29 |
| X17 | A page rebuilt | Q33, Q35 | X16 | A week |
| X18 | Table reads paced by idle time | Q34, Q37 | X16 | Days |
| X19 | The edges | Q36, and what M24 and M26 must bound | X17 | Days |
| ✅ X20 | ScyllaDB's scan path, at the source *(optional)*, [read and run](scylladb-scan-sources.md) | W3's readings | Nothing | ~~Days~~ Done 2026-10-29 |
| X21 | Declared order, priced | Q40 | M25 | A week |
| X22 | The ordered listing, two ways | Q41 | M27 | A week |
| X37 | A tablet's records together, priced | Q62 | X16 | Days |

## The spikes

### X16. The walk beside the foreground

**Reported 2026-10-29** on [its own page](walk-beside-the-foreground.md), with W1's four rows' before
and after beside it, and recorded on W13 as [evidence for Q33, Q34 and Q37](contract.md#x16-measured-evidence-for-q33-q34-and-q37-none-settled-2026-10-29).
The walk as it stands does not hold W7's budget beside a scan of a table read from its archives
(the cell's read p99 1.13 times, its insert p99 1.60 times), and its pages hold their loop for
microseconds there: what hurts is the reads beside the WAL's flushes on one device, so X18 comes
first, as the second case below says. A resident sorted table's captures held the loop up to 6.5 ms
and moved the read tail, so the capture's rebuild matters for it. W1's rows at their provisional
defaults changed neither, cost nothing on the query path, and scanned a resident table 16% slower.

| Field | |
| --- | --- |
| **Question** | What does the walk as it stands cost the reference cell when driven as a client's scan would drive it, every shard's two pages at once, back to back; and where does a page's time go? Q33 and Q34, and Q37 in part. No one has measured a walk's effect on a foreground's tail ([F87](../../features/tablet-walk.md#limitations)) |
| **What would change the design** | If the walk as it stands already holds [W7](isolation.md#the-budget)'s budget at full speed, W5's rebuild shrinks to its pacing. If no pace of it holds the budget even on a table larger than memory, where the loop holds are microseconds, then what hurts is the reads' queue and device, not the capture, and X18 comes first. If the cell's tail moves with the loop holds alone, the capture's rebuild is the first change |
| **Method** | A side cluster of the bench's own with the reference cell's table and a scanned table. The scan driven in process through `ShoalPool::walk`, or by the loader's `walk` at every shard's concurrency, beside `rw50` on the cell through `shoaladm bench`. Two scanned tables: one larger than a node's memory, after a restart, so most partitions are archived; and `MovieByKeyword`, resident and overlaid. Each page's `held`, `read` and counts kept; the cell's p99s before, during and after; eviction passes by cause; device bytes |
| **Where** | titan, hyperion and europa; `performance` governor |
| **Records** | By host and table: partitions a second, a page's held and read time at p50, p99 and max, the cell's read and write p99 ratio, `Resident` passes, device bytes a partition |
| **Depends on** | The lab. The bench's `scan` event helps and is not needed: a paced cell beside a hand-driven walk serves |
| **Cost** | Days |

### X17. A page rebuilt

| Field | |
| --- | --- |
| **Question** | What does each change of [W5](tablet-reader.md) buy, on its own: keys planned rather than bytes captured; resident partitions encoded in steps from a second inbox; archive reads in runs in disk order rather than one at a time in key order; the reads in a queue of their own rather than the medium one; rows encoded on the shard rather than walked partitions sent; and what a saved position between pages would save? Since [W42](scan-speed.md) (2026-10-29) also: where a walked partition's up to 73 µs of a Zen1 host's cpu goes, and what one operation a record and several reads in flight buy before runs do. Q33 and Q35 |
| **What would change the design** | If runs buy little because the lab's archives lie mostly in key order after archive passes, reads stay in key order with several in flight. W42 found the lab's archives in no such order: a table loaded once is never copied by a pass and keeps its merges' hash order, so runs wait on merges written in key order ([Q62](contract.md#questions-to-answer)), and reads in flight come first either way. If the profile finds most of a partition's cpu outside its read's operations, W12's cost budget is met there, not at the ring. If stepped encoding costs more than the holds it removes, encoding returns to the plan with a byte bound a step. If rows cost the shard much more than partitions cost the client, Q35's opt-in for unfiltered scans moves forward. If a saved position saves a third of a page, it becomes a gate's work rather than optional |
| **Method** | A spike build of the walk with each change behind a switch, thrown away after. Pages timed step by step on `MovieByKeyword` resident and on a table larger than memory after a restart; encode and decode cpu a GiB, rows against partitions, on server and client; a page asked twice with and without its sorted keys and index position kept. Since W42: a `perf` profile of a walked partition on titan and europa (user, kernel and interrupts, ring operations a partition); an arm with only the background running; reads in flight at 1, 4 and 16; runs at gaps of 4 KiB and 128 KiB over archives merged in hash order and in key order; one operation a record against three |
| **Where** | titan and europa |
| **Records** | Each change's page time, loop hold, device operations and bytes a partition, beside the walk as X16 measured it, and since W42 its cpu and device reads a partition beside W12's cost budget |
| **Depends on** | X16; [O139](../../appendix/optimizations.md#o139-a-scan-driver-asks-a-shed-tablet-again-where-another-tablet-would-be-answered) helps, since a shed stream's asks again are part of a Zen1 node's cpu |
| **Cost** | A week |

### X18. Table reads paced by idle time

| Field | |
| --- | --- |
| **Question** | Which ceiling and trickle let a scan read a table's archives without moving the cell's read or write p99 past the budget, on each kind of device the lab has? Since [W42](scan-speed.md) (2026-10-29), against a walk fast enough to reach its ceiling, and with the ceiling stated in device time or reads as well as bytes: at one record a read 100 MiB a second is about 100,000 reads, half a 970 EVO's. Q34 and Q37 |
| **What would change the design** | If no ceiling holds the write p99 on the 970 EVO, where a table's archives share the device with its WAL, the idle check has to see flushes too, or the default ceiling drops. If no pace holds the Optane's budget, as X12 found for recovery there, the Optane's bound is set the way M16 set one ([S13](../../object-storage/isolation.md#io-on-a-slice)). If idle gating alone, without a ceiling, holds the budget, the ceiling is a safety rather than the pace |
| **Method** | X12's method on a table's archives: ceilings of 32, 64, 128 and 256 MiB/s a device, each with and without idle gating, trickles of 1, 2 and 4 MiB/s, beside the cell; on the 970 EVO with the WAL beside, the Optane, and a disk holding the archives' root |
| **Where** | titan and hyperion (the 970 EVO, a disk), europa (the Optane, a disk) |
| **Records** | By device and pace: the cell's read and write p99 ratio, the scan's MiB/s, the trickle's share |
| **Depends on** | X16, and ~~a spike build with the pacing~~ [F119](../../features/scan-pacing.md)'s pacing, built into the product with `scans.ceiling` and `scans.trickle` as its settings; a trickle equal to the ceiling is the pace without idle gating |
| **Cost** | Days |

### X19. The edges

| Field | |
| --- | --- |
| **Question** | What does a scan cost at its edges: a table of 4096 empty tablets; a table of one sorted partition larger than a page; a listing of a bucket of inline objects, bytes read a listed entry against an inline threshold from 1 KiB to 16 MiB; a filter that keeps nothing, its pages ended by work? Q36, and what M24 and M26 must bound |
| **What would change the design** | If a listing of inline objects reads too much against the budget, keeping a listing's fields apart from an object's bytes moves from optional to required before M26. If one huge sorted partition's page breaks the budget, splitting a sorted partition across pages moves before M24 and fills the cursor's reserved suffix. If an empty table costs more than a page a shard, the per-tablet counters are not what W4 assumes |
| **Method** | A standalone node in process for the empty table, the huge partition and the filter; a side cluster for the listing |
| **Where** | europa in process; titan for the listing |
| **Records** | Pages and time for each edge; bytes read a listed entry by threshold |
| **Depends on** | X17 |
| **Cost** | Days |

### X20. ScyllaDB's scan path, at the source

*(Optional: no gate waits on it.)*

**Read and run 2026-10-29** on [its own page](scylladb-scan-sources.md), at `scylla-2026.3.4`
(`daef9dad`): of W3's twenty claims fourteen held as written, four held with something W3 left out,
and two were wrong in part, both in the semaphore (a read is admitted while up to two others need
only the cpu; a saved reader also goes when its time to live runs out), and neither changes the
design. The 2025 post's scanner, `fee-mendes/tablet-fullscans`, turned out to simulate its
scheduling rather than scan, so the run drove a scanner of the same shape written for it, beside
X16's cell on X16's hosts: ScyllaDB's scan read the table 40 to 52 times as fast as Shoal's walk
beside the same cell and doubled the cell's p99 while it ran (2.34 times on reads, 2.15 on
inserts, for 2.15 s), so it holds W7's budget no better; carrying every row to the client it filled
the client's link, and the cell's tail on that client went to the kernel's minimum retransmission
timeout. Recorded on W13 as [evidence for Q33, Q34 and Q37](contract.md#x20-run-evidence-for-q33-q34-and-q37-none-settled-2026-10-29).

| Field | |
| --- | --- |
| **Question** | Are [W3](prior-art.md)'s readings right at a pinned release, and what does ScyllaDB's own tablet-era full scan cost its foreground on the lab? |
| **What would change the design** | A borrowed mechanism read differently at the source than in the documents, saved readers' memory bound or the semaphore's admission most likely, changes W5's or W7's version of it. A ScyllaDB foreground far better beside a scan than W7's budget would argue for tightening the budget |
| **Method** | [X14](../../object-storage/ceph-and-s3-sources.md)'s method: a release pinned by its commit, the files W3 names read and cited by line, predictions written before. Optionally a three-node ScyllaDB on the lab, the 2025 post's tablet scanner beside a stress load, every host change recorded and put back |
| **Where** | Reading anywhere; the lab for the run |
| **Records** | Each W3 claim confirmed, corrected or still recalled; the foreground's p99 ratio beside a scan |
| **Depends on** | Nothing |
| **Cost** | Days |

### X21. Declared order, priced

| Field | |
| --- | --- |
| **Question** | What does an order index cost a write at apply and at compaction, an ordered page covering and not, and how evenly do skewed placement values fall over 4096 tablets with and without spread? Q40 |
| **What would change the design** | If index maintenance moves an insert's p99 past what [W12](performance.md#the-budgets) allows a declared-order table, the index's delta or its runs are redesigned before M27. If pages that are not covering are slow on disks, covering is the default for an order's fields. If realistic skew puts a group past its commit rate at spread one, a default spread, or a refusal of a declaration without one, follows |
| **Method** | A pure, seeded simulation, as [X2](../../object-storage/placement-simulation.md) was, of placement values drawn uniformly and by Zipf at 0.9, 1.1 and 1.3 over 4096 tablets and the lab's and larger node counts, spread 1 to 64. And a spike build of an index in one table's apply and compaction, measured with inserts, gets and ordered pages |
| **Where** | Anywhere for the simulation; titan and europa for the build |
| **Records** | The simulation's imbalance by spread; an insert's and a get's p99 with and without the index; ordered pages a second, covering and not, on an SSD and a disk |
| **Depends on** | [M25](milestones.md#m25-scans-across-a-cluster) |
| **Cost** | A week |

### X22. The ordered listing, two ways

| Field | |
| --- | --- |
| **Question** | Which ordered listing does M28 build: an ordered `ObjectMeta` on W8's index for new buckets, or Q32's pending-then-complete index beside the existing layout? Q41 |
| **What would change the design** | If the pending commit before every put and delete costs a put little and orphans are rare under failures, Q32's index serves every bucket and the layout stays. If it doubles a small put's latency, the ordered layout for new buckets is the way, and existing buckets list unordered |
| **Method** | Both built as spikes on a side cluster: put and delete rates and latencies, listing pages a second by prefix, list-after-write and list-after-delete checked under a kill, and orphans counted after crashes between the pending entry and the commit |
| **Where** | The lab |
| **Records** | Each way's put, delete and listing figures, and its correctness under faults |
| **Depends on** | [M27](milestones.md#m27-ordered-scans-by-declared-order) |
| **Cost** | A week |

### X37. A tablet's records together, priced

| Field | |
| --- | --- |
| **Question** | What does writing a merge's partitions in key order cost a merge, and how many stretches does it leave a tablet on its shard, under each load the lab runs: a table loaded once, a table overwritten, a table inserted into for an hour? Is that few enough for [W12](performance.md#the-budgets)'s cost budget, or does a table need its blocks consolidated? Q62 |
| **What would change the design** | If the sort costs a merge more than a tenth of its time or memory, it moves off the compactor's critical path, or merges grow. If a tablet keeps so many stretches that a page reads fewer than ten partitions a device read, consolidated blocks become a gate's work rather than optional. If archive passes reorder an overwritten table well enough on their own, consolidation stays optional for good |
| **Method** | A spike build of the compactor with the sort behind a switch, thrown away after. `Big` loaded on X16's side cluster each way; the merge's time and memory by segment; from the archive map, each tablet's stretches on a shard and their lengths; a walk of every tablet with runs at gaps of 4 KiB, its reads a page and device bytes a partition; a get's p99 and a snapshot cut timed beside, each way. Then an hour of overwrites and inserts, and the stretches counted again |
| **Where** | titan and europa |
| **Records** | By layout: a merge's time and memory, stretches a tablet and their records, reads a page, device bytes a partition, a get's p99, a cut's time |
| **Depends on** | X16 |
| **Cost** | Days |

## What has to be on the record before each gate

| Gate | Decided first |
| --- | --- |
| M23 | Q33, Q34, ~~and~~ Q37's default and Q62, by X16, X17, X18 and X37 |
| M24 | Q35 and Q36, by X17, X19 and design |
| M25 | Q38 by design, and Q39 with the user |
| M26 | What a listing reads, by X19 |
| M27 | Q40, by X21 |
| M28 | Q41, by X22 |

## Related

[W13](contract.md#questions-to-answer) for the questions; [the milestones](milestones.md) for the
gates; [the object storage part's spikes](../../object-storage/spikes.md), whose shape and rules this
keeps; [What's left to do](whats-left-todo.md) for the order drawn.
