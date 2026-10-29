# Exploratory spikes

**No spike here has run.** This page is the list of what has to be measured before the
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
| X16 | The walk beside the foreground | Q33, Q34, Q37 in part | The lab | Days |
| X17 | A page rebuilt | Q33, Q35 | X16 | A week |
| X18 | Table reads paced by idle time | Q34, Q37 | X16 | Days |
| X19 | The edges | Q36, and what M24 and M26 must bound | X17 | Days |
| X20 | ScyllaDB's scan path, at the source *(optional)* | W3's readings | Nothing | Days |
| X21 | Declared order, priced | Q40 | M25 | A week |
| X22 | The ordered listing, two ways | Q41 | M27 | A week |

## The spikes

### X16. The walk beside the foreground

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
| **Question** | What does each change of [W5](tablet-reader.md) buy, on its own: keys planned rather than bytes captured; resident partitions encoded in steps from a second inbox; archive reads in runs in disk order rather than one at a time in key order; the reads in a queue of their own rather than the medium one; rows encoded on the shard rather than walked partitions sent; and what a saved position between pages would save? Q33 and Q35 |
| **What would change the design** | If runs buy little because the lab's archives lie mostly in key order after archive passes, reads stay in key order with several in flight. If stepped encoding costs more than the holds it removes, encoding returns to the plan with a byte bound a step. If rows cost the shard much more than partitions cost the client, Q35's opt-in for unfiltered scans moves forward. If a saved position saves a third of a page, it becomes a gate's work rather than optional |
| **Method** | A spike build of the walk with each change behind a switch, thrown away after. Pages timed step by step on `MovieByKeyword` resident and on a table larger than memory after a restart; encode and decode cpu a GiB, rows against partitions, on server and client; a page asked twice with and without its sorted keys and index position kept |
| **Where** | titan and europa |
| **Records** | Each change's page time, loop hold, device operations and bytes a partition, beside the walk as X16 measured it |
| **Depends on** | X16 |
| **Cost** | A week |

### X18. Table reads paced by idle time

| Field | |
| --- | --- |
| **Question** | Which ceiling and trickle let a scan read a table's archives without moving the cell's read or write p99 past the budget, on each kind of device the lab has? Q34 and Q37 |
| **What would change the design** | If no ceiling holds the write p99 on the 970 EVO, where a table's archives share the device with its WAL, the idle check has to see flushes too, or the default ceiling drops. If no pace holds the Optane's budget, as X12 found for recovery there, the Optane's bound is set the way M16 set one ([S13](../../object-storage/isolation.md#io-on-a-slice)). If idle gating alone, without a ceiling, holds the budget, the ceiling is a safety rather than the pace |
| **Method** | X12's method on a table's archives: ceilings of 32, 64, 128 and 256 MiB/s a device, each with and without idle gating, trickles of 1, 2 and 4 MiB/s, beside the cell; on the 970 EVO with the WAL beside, the Optane, and a disk holding the archives' root |
| **Where** | titan and hyperion (the 970 EVO, a disk), europa (the Optane, a disk) |
| **Records** | By device and pace: the cell's read and write p99 ratio, the scan's MiB/s, the trickle's share |
| **Depends on** | X16, and a spike build with the pacing |
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

## What has to be on the record before each gate

| Gate | Decided first |
| --- | --- |
| M23 | Q33, Q34, and Q37's default, by X16, X17 and X18 |
| M24 | Q35 and Q36, by X17, X19 and design |
| M25 | Q38 by design, and Q39 with the user |
| M26 | What a listing reads, by X19 |
| M27 | Q40, by X21 |
| M28 | Q41, by X22 |

## Related

[W13](contract.md#questions-to-answer) for the questions; [the milestones](milestones.md) for the
gates; [the object storage part's spikes](../../object-storage/spikes.md), whose shape and rules this
keeps; [What's left to do](whats-left-todo.md) for the order drawn.
