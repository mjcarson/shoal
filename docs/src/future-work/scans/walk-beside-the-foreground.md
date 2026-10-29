# X16. The walk beside the foreground, measured

**Reported 2026-10-29.** This is the record of spike [X16](spikes.md#x16-the-walk-beside-the-foreground),
and of the before and after of [W1](prerequisites.md)'s four engine rows beside it, taken in one lab
window as the plan asked: the walk as it stands, F115's, which is X16 itself, and the walk with the
scan class, the budget, the pacing and the admission class, F119's, driven the same way on the same
hosts.

Four rounds a side, every host identical to its snapshot after. The answers:

- **The walk as it stands does not hold [W7's budget](isolation.md#the-budget).** Beside a whole
  scan of a table read from its archives, the cell's read p99 rose 13% and its insert p99 60%,
  against a budget of 10%; beside a three-second scan of a resident sorted table, its read p99 rose
  18% and its inserts held.
- **What hurts is the reads' queue and device, not the loop.** The archived table's pages held
  their shard's loop 0.11 ms at p99 and 0.34 ms at most, and the cell's insert tail still rose 60%:
  the scan reads the 970 EVOs the WAL flushes to. So [X18](spikes.md#x18-table-reads-paced-by-idle-time)
  comes first, as the spike page said it would in this case. The resident table's captures held the
  loop up to 6.5 ms, and its scan moved the read tail too, so the capture's rebuild
  ([X17](spikes.md#x17-a-page-rebuilt)) matters for resident sorted tables.
- **W1's four rows, at their provisional defaults, change neither.** The after side's ratios are the
  before side's within noise, since its pace never bound: a scan read about 28 MiB a second a node,
  far under the 100 MiB ceiling, and a resident cell keeps its device's reads idle, so the trickle
  rarely applied. Nothing on the query path moved: the cell alone is the same on both sides.
- **A resident table scans 16% slower with the rows**, 44,200 partitions a second against 52,600,
  the one difference whose ranges are disjoint: the scan queue's share of a busy core and its steps.
- **A scan's own rate is its pages' latency.** A page reads its archived partitions one at a time, a
  whole tablet's 732 in 154 ms on the Zen1 hosts, so 32 pages in flight read 84,000 partitions a
  second, a few percent of what the devices give. Runs in disk order (X17) are the lever, not more
  cores.
- **No eviction pass was begun by memory** on either side, at 4 GiB a node.

## The question

The spike page's question: what does the walk as it stands cost the reference cell when driven as
a client's scan would drive it, every shard's two pages at once, back to back; and where does a
page's time go? It feeds [Q33](contract.md#questions-to-answer) (what reads a scan's page),
[Q34](contract.md#questions-to-answer) (what a scan may cost the foreground, and what pace holds it)
and [Q37](contract.md#questions-to-answer) in part (whether scan work stays on the table shards).
Nobody had measured a walk's effect on a foreground's tail ([F87](../../features/tablet-walk.md#limitations)).

**What would have changed the design**, named on the spike page before the run: if the walk as it
stands already held W7's budget at full speed, W5's rebuild would shrink to its pacing; if no pace
of it held the budget even on a table read from its archives, where a page's hold on its loop is
microseconds, what hurts is the reads' queue and device and X18 comes first; if the cell's tail
moved with the loop holds alone, the capture's rebuild is the first change.

## How it was judged

**[W7's budget](isolation.md#the-budget)**, stated before the work: beside one full scan, the
reference cell keeps its read p99 and its write p99 within `max(1.10 × alone, alone + 0.1 ms)` at
the median over rounds; no eviction pass caused by memory during the scan; the scan's own rate
reported beside it. The cell here is the reference cell's shape (`grid/unsorted/r50/1024`: half
reads, half inserts, a kibibyte row, a bundle of one) driven by `shoaladm bench`, not shoal-bench's
own arm, so its "alone" is the scan event's *before* window in the same arm.

**The lab's rule for a before and after**: a difference is a result only where the two sides'
rounds give ranges that do not overlap. Four rounds a side, the sides alternating which goes first.

## What was run

### The cluster

`x16`, a side cluster of three nodes on the tmdb cluster's hosts and cores, bootstrapped fresh for
every side round and destroyed after it, the tmdb node stopped, every host's maintenance timers held
and its governor at `performance` for the whole window, all put back after
([x16-host-changes.txt](../../../../shoal-spike-scan/results/x16-host-changes.txt); every host
identical to its snapshot afterwards).

| Host | Cpu | Shards | Node memory | Every root on |
| --- | --- | --- | --- | --- |
| europa | Ryzen 9 7945HX (Zen 4, 16 cores, 32 threads) | 6, cpus 1-6 | 4 GiB | the Optane, `/optane/shoal-x16` (XFS) |
| titan | Ryzen Embedded V1756B (Zen 1, 4 cores, 8 threads) | 5, cpus 2-6 | 4 GiB | the 970 EVO, `/xfs/shoal-x16` (XFS on LVM) |
| hyperion | Ryzen Embedded V1756B (Zen 1, 4 cores, 8 threads) | 5, cpus 2-6 | 4 GiB | the 970 EVO, `/xfs/shoal-x16` (XFS on LVM) |

Three copies of every tablet, replication over 1 GbE, every node built for `znver1`. The driver ran
on europa, pinned to cpus 12-15 and 28-31, clear of europa's node. The window ran from 12:59 to
14:40 UTC; afterwards the tmdb cluster's units were active and listening on all three hosts and its
groups moving their leads with quorum votes. Its `status` could not be read: the loader at hand
speaks a newer protocol than the deployed nodes (`UnsupportedVersion { got: 4, ours: 5 }`), as it
did before the window.

### The two sides

| Side | Commit | What it is | Node's sha256 |
| --- | --- | --- | --- |
| before | `53286313` (branch `x16-before`): F115, with the spike's crate cherry-picked | The walk as it stands: a scan's page is a walk page like any other, captured on the loop as it is admitted, read in the medium queue beside every get's partition read, counted against the two pages a shard every walk shares, charged to no budget and paced by nothing | `e142a750253abb05...` |
| after | `1aded160`: F119, with the spike's crate | W1's four rows: the scan's own count, its capture from the loop's second inbox, its reads in the scan queue in 64 KiB steps, its buffers drawn from the scan budget, its archived reads paced by their device at the provisional defaults (100 MiB a second, 2 MiB while a foreground read is in flight) | `6f41801fe54ff945...` |

Each side's node, its admin program and the bench in it are built at its commit in a worktree of
its own, one after the other (`target/lab/x16/build.sh`), and each side drives its own build with
its own bench, so a side's capture names its own commit. The script is
[x16-lab.sh](../../../../shoal-spike-scan/results/x16-lab.sh) and the tables come from
[x16-report.py](../../../../shoal-spike-scan/results/x16-report.py).

### The tables

`shoal-spike-scan`'s schema, every table loaded from a dataset folder `x16-data` writes:

| Table | Kind | Rows | Bytes on file | What it is for |
| --- | --- | --- | --- | --- |
| `Cell` | unsorted | 300,000, 100,000 preloaded | 307 MB | The foreground: a kibibyte row a partition, resident once the cell's first arm has read it |
| `Big` | unsorted | 3,000,000, all preloaded | 3.07 GB | Scanned from its archives: every node restarted before each capture, and a walk never makes a row resident |
| `Keyword` | sorted | 1,005,240, nine tenths preloaded | 126 MB | Scanned resident and overlaid: the TMDB csv's keyword rows, 58,413 partitions up to 27,614 rows each, written into by a paced stream of 200 inserts a second through every arm |

### The runs

A side round is one cluster: bootstrapped; preloaded by a short run of reads; restarted and settled
(every member up, then 30 s); the first capture; restarted and settled again; the second capture;
destroyed. Every capture is driven attached, its preload already in.

| Capture | Arms | What it measures |
| --- | --- | --- |
| `big` | `rw50/b1/none`, `rw50/b1/scan` (a whole scan of `Big` 30% into the arm), `scan/Big/none`, `scan/Keyword/none` | The cell alone; the cell beside a scan of a table read from its archives, cut into before, during and after; and each table scanned alone, whole scans back to back |
| `keyword` | `rw50/b1/scan` (a whole scan of `Keyword`) | The cell beside a scan of a table resident and overlaid |

Every arm: 10 s of warmup and 60 s measured; the cell at four in flight, a bundle of one; the scan
at its default of two pages in flight a shard of every member, 32 streams; the scan event's clock
held for the scan up to twenty minutes.

## What was found on the way

**A node of 1 GiB stayed past its memory for good.** The first quick pass ran the nodes at 1 GiB,
`Big` three times it. Each node's resident set sat at 1.56 to 1.77 GiB while its rows held 23 to
115 MiB and its WAL's index and archive maps about 150 MiB (F104's figures), so every shard evicted
forty percent of its rows at every check: 146,000 partitions beside a scan of `Big` and 172,000
beside one of `Keyword`, the cell's among them, which kept the cell's reads on disk and the paced
scan on its trickle. At 4 GiB the nodes held 1.58 to 2.02 GiB and no eviction pass was begun by
memory. Filed as [item 338](../../appendix/known-issues.md#338-a-node-below-its-resident-floor-evicts-its-rows-at-every-check),
since its cause, what holds the other 1.5 GB, is not yet known.

**A cluster of the bench's own is reset and preloaded again after every arm that writes**, and every
arm here writes (the cell's inserts, the paced stream's), so each arm would have cost a preload of
four million rows. Each side round is one cluster the script bootstraps and the bench drives
attached instead. **An attached run refuses cold reads**, since it never restarts a cluster it did
not make, so the script restarts every node itself and waits for the members and a settle; without
the settle, the first seconds after a restart answered `NotLeader` while leaders moved to their
preferred members.

**The scan driver is shed about thirty-five times for every page of `Big` it is answered.** Its
streams are two a shard of every member, 32, and each walks a tablet the seeded queue gives it. A
client cannot know which shard of a member hosts a tablet, so streams meet on a shard past its two
pages and are shed, and ask again 2 ms later; a page of a whole tablet takes 150 ms. The quick
passes, a tenth of the rows, shed three to four times a page. Both sides' drivers shed alike, so
the before and after is fair; what a driver that moves to another tablet when shed would save is [O139](../../appendix/optimizations.md#o139-a-scan-driver-asks-a-shed-tablet-again-where-another-tablet-would-be-answered).

## The tables

Medians over the four rounds, the range beside each in brackets; **Disjoint** says whether the two
sides' ranges do not overlap, the lab's rule for a difference. The whole set, by member too, is
[x16-report.md](../../../../shoal-spike-scan/results/x16-report.md), from the slimmed captures in
[shoal-spike-scan/results/x16](../../../../shoal-spike-scan/results/x16).

### The cell alone

The first arm of every `big` capture, after the restart and its settle. What W1's rows cost a query
that is not a scan's.

| Figure | Before (F115) | After (F119) | Disjoint |
| --- | --- | --- | --- |
| Reads a second | 2,390 [2,330, 2,440] | 2,380 [2,340, 2,410] | no |
| Read p99 | 7.68 ms [7.43, 7.92] | 7.64 ms [7.43, 7.92] | no |
| Insert p99 | 24.7 ms [24.0, 25.2] | 24.5 ms [23.9, 24.6] | no |
| Partitions evicted by memory | 0 | 0 | |

### The cell beside a whole scan

The scan event's arm, cut at its marks: *before* is the 18 s before the scan was asked for, *during*
the whole scan.

| Figure | Before (F115) | After (F119) | Disjoint |
| --- | --- | --- | --- |
| **Beside `Big`**, read p99 before → during | 6.58 → 7.24 ms | 6.54 → 7.33 ms | |
| Read p99, during over before | 1.13 [1.08, 1.16] | 1.12 [1.11, 1.14] | no |
| Insert p99 before → during | 24.3 → 39.8 ms | 24.6 → 40.1 ms | |
| Insert p99, during over before | 1.60 [1.58, 1.74] | 1.63 [1.56, 1.70] | no |
| The whole scan | 112 s [79.5, 113] | 86.3 s [79.8, 99.5] | no |
| Partitions a second, during | 26,700 [26,300, 37,500] | 34,500 [30,000, 37,500] | no |
| **Beside `Keyword`**, read p99 before → during | 9.96 → 11.7 ms | 10.3 → 11.3 ms | |
| Read p99, during over before | 1.18 [1.02, 1.30] | 1.11 [1.04, 1.40] | no |
| Insert p99, during over before | 1.07 [0.97, 1.15] | 0.94 [0.88, 1.14] | no |
| The whole scan | 2.99 s [2.73, 3.39] | 3.14 s [2.50, 3.35] | no |
| Partitions evicted by memory, either scan | 0 | 0 | |

W7's budget, `max(1.10 × before, before + 0.1 ms)` on the medians: beside `Big`, the read p99 and the
insert p99 are outside it on both sides; beside `Keyword`, the read p99 is outside it on both sides
and the insert p99 inside it.

### Each table scanned alone

Whole scans back to back for 60 s, the paced inserts into `Keyword` still running.

| Figure | Before (F115) | After (F119) | Disjoint |
| --- | --- | --- | --- |
| **`Big`**, partitions a second | 83,900 [81,800, 89,800] | 85,700 [76,500, 98,700] | no |
| MiB a second, every member | 83.2 [81.1, 89.1] | 85.0 [75.9, 97.8] | no |
| A page's hold on its loop, p50 / p99 / max | 0.015 / 0.108 / 0.338 ms | 0.015 / 0.115 / 0.479 ms | no |
| A page's reads, p50 / p99 | 154 / 185 ms | 154 / 185 ms | no |
| Sheds a page answered | 35.1 | 34.0 | no |
| Device bytes a partition read | 1,870 | 1,860 | no |
| **`Keyword`**, partitions a second | 52,600 [49,900, 58,600] | 44,200 [41,800, 49,700] | **yes** |
| MiB a second, every member | 141 [133, 157] | 118 [112, 133] | **yes** |
| A page's hold on its loop, p99 / max | 0.194 / 6.46 ms | 0.144 / 4.66 ms | no |
| A page's reads, p50 / p99 | 3.13 / 9.15 ms | 3.28 / 12.7 ms | p99 **yes** |
| Sheds a page answered | 1.16 | 1.36 | **yes** |
| Device bytes a partition read | 3,320 | 3,500 | no |

### A page beside the cell, and by member

| Figure | Before (F115) | After (F119) |
| --- | --- | --- |
| `Big`: a page's hold, p99 / max | 0.126 / 0.409 ms | 0.127 / 0.405 ms |
| `Big`: a page's reads, p99 | 711 ms | 678 ms |
| `Big`: waited for its capture, p99 | | 0.023 ms |
| `Big`: waited for its device, p99 | | 5.9 ms |
| `Keyword`: a page's hold, p99 / max | 0.102 / 1.84 ms | 0.108 / 2.75 ms |
| `Keyword`: waited for its device, p99 | | 6.5 ms |
| `Big` alone, a page's reads p99: europa / titan / hyperion | 25 / 186 / 185 ms | 25 / 190 / 182 ms |
| `Big` beside the cell, a page's reads p99: europa / titan / hyperion | 32 / 780 / 772 ms | 32 / 709 / 725 ms |
| `Big` alone, partitions a second: europa / titan / hyperion | 19,700 / 34,400 / 32,700 | 22,800 / 33,700 / 31,800 |

europa's Optane reads a page seven times as fast as the Zen1 hosts' 970 EVOs and answers fewer of
them: it leads most groups (`lead_weight: 2`), and the scan asks followers first.

## What it says for the design

**Q34, what a scan may cost and what pace holds it.** Neither side holds the budget beside a table
read from its archives, and the after side's pace did nothing: it never bound. The insert tail is
the device's, a scan's reads beside the WAL's flushes on one 970 EVO, which W7 said the idle check
would not see ([W7](isolation.md#device): "the idle check sees foreground reads, not flushes"). X18's
question is now sharper: which ceiling holds the insert p99 on the 970 EVO, and whether the idle
check has to count a commit's flush as a foreground read. At 28 MiB a second a node a scan is
already well under any ceiling W7 named, so the answer is likely a ceiling far lower, or the flush
counted, not the default ceiling moved.

**Q33, what reads a scan's page.** The walk's capture is not what hurts beside an archived table;
beside a resident sorted table it moves the read tail, with loop holds up to 6.5 ms. Both W5's plan
step (keys planned, never bytes captured) and its runs in disk order (the archived reads one at a
time today) are confirmed as the rebuild's two levers, and X17 prices each. Nothing here argues for
a reader of its own over the walk rebuilt.

**Q37, whether scan work stays on the shards.** A scan's rate is bound by a page's latency, its
reads one at a time, not by cores or devices: a few percent of what the devices give. Moving scans to
executors of their own would not change that; runs would. The default, on the shards, stands.

**The rows' own cost.** None on the query path. A resident table's scan alone is 16% slower, which
is the scan queue doing what F117 built it to do, a twenty-sixth of a busy core, and its device's
trickle in three rounds of four; whether a scan queue with a longer goal, or none, reads a resident
table faster without moving the cell is X17's switch ([F117](../../features/scan-class.md#limitations)).

**The bench's driver.** Its shed storm, about thirty sheds a page beside `Big`'s long pages, is the
same on both sides, so the comparison is fair, and is [O139](../../appendix/optimizations.md#o139-a-scan-driver-asks-a-shed-tablet-again-where-another-tablet-would-be-answered)'s
to remove.

## Limitations

- **Three hosts, one of each kind of SSD and two Zen1 hosts.** The 970 EVO hosts and the Optane host
  differ by far more than the sides do; every figure is by member where it matters.
- **The cell is `shoaladm bench`'s rw50, not shoal-bench's reference cell**, and its "alone" is the
  scan event's before window in the same arm, 18 s of it.
- **A capture holds no figure of the scan budget's use.** F118's budget is read in process; a page
  shed for its budget and one shed for its count are one count in a capture. So X16 cannot revisit
  `scan_memory`'s default, four mebibytes a shard, beyond saying no node was past its memory.
- **The pacing ran at its provisional defaults** (100 MiB a second a device, 2 MiB while busy); what
  pace holds the budget is X18's.
- **Index pages a scan's page reads are not paced**, and its capture is still whole (F117, F119).

## Related

[X16](spikes.md#x16-the-walk-beside-the-foreground) for the question; [W7](isolation.md) for the
budget; [F115](../../features/bench-scans.md) for the bench's scan; [F116](../../features/scan-admission.md),
[F117](../../features/scan-class.md), [F118](../../features/scan-budget.md) and
[F119](../../features/scan-pacing.md) for the after side; [X17](spikes.md#x17-a-page-rebuilt) and
[X18](spikes.md#x18-table-reads-paced-by-idle-time) for what follows.
