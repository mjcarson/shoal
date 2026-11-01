# M23's exit on the lab

**Run 2026-10-31.** This is the record of [M23](milestones.md#m23-the-walk-rebuilt-for-load)'s
exit: the product's rebuilt walk, at the exit's commit and every scan setting its default, judged on
the lab by [W7's foreground budget](isolation.md#the-budget) beside a full scan, by
[W12's rate and cost budgets](performance.md#the-budgets) for a scan alone, and by the object store's
drivers' own figures before and after. Eight rounds of the cell beside a scan, the churn at two
segment sizes, the drivers' rounds and a control, every host identical to its snapshot after. Four
of its clauses were left to the user and [decided the same day](#decided-with-the-user-2026-10-31):
the rounds are run again once item 350 is fixed, with a cell whose feed does not wrap; item 350 is
fixed since 2026-11-01 ([below](#item-350s-steps-named-and-fixed)). The answers:

- **The read clause holds.** Beside a whole scan of `Big` after a restart the cell's read p99 went
  from 6.22 to 6.55 ms, 1.07 times [1.03, 1.08], and beside a scan of `Keyword` it fell, 0.87 times.
  Decision 1(c)'s fallback is not needed.
- **The insert clause, as W7 defines it, misses**, 1.62 times [1.56, 1.72] beside `Big` and 1.48
  [1.30, 1.55] beside `Keyword`, because the cell changes its own workload inside an arm this long.
  ~~Its 300,000 rows run out about 85 s in and its inserts wrap into overwrites of rows that are not
  resident, after which their p99 is about 40 ms with a scan beside them or not~~ About 75 s into
  every arm its inserts fall to three fifths of their rate and their p99 to about 40 ms, with a scan
  beside them or not, 8 to 29 s before its 300,000 rows run out and its feed wraps (corrected
  2026-11-01: the fall is [item 351](../../appendix/resolved/the-cells-fall-is-the-drives.md)'s, not the
  wrap's, and item 351 is the lab's 970 EVOs, whose flush triples about 75 s into the cell's synced
  writes, which the rounds again warm every arm past): the *after* windows,
  the scans done, are the worst of the three. Before the wrap, beside `Big`, the clause holds, 1.06
  times [1.03, 1.12]; beside `Keyword` it rises 1.23 times [1.05, 1.36], and the cell alone rises
  as far over the same seconds of an arm with no scan beside it, 1.16 times [0.975, 1.34] in two
  control rounds. The ranges overlap, so no part of that rise is shown to be the scan's. Each of
  those views runs to the wrap, so each holds 8 to 29 s of the cell past its fall.
- **No `Resident` eviction pass** in any of the sixteen captures.
- **Loop steps past 100 µs remain, about one in 460**: 201 [158, 268] of a `Big` capture's 92,300
  steps, the longest 326 to 539 µs. Nine in ten were in `Keyword` alone's arm, where the resident
  partitions handed out were largest. Europa's, about 20 KiB a partition, passed 90 to 208 times a
  capture though its shards were preempted least, so what holds them is in the steps
  ([item 350](../../appendix/resolved/encode-copy-deadline.md)).
  Beside the cell and a scan of `Big`, 3 to 16 a capture passed, light steps on shards that their
  own io_uring workers preempt
  ([O147](../../appendix/optimizations.md#o147-each-shards-io_uring-workers-run-on-the-shards-own-cpu)).
- **A scan alone of `Big` loaded once read 50,600 partitions a second**, 19.8 MiB a second of each
  Zen1 host's 970 EVO, where its share of 75 ms binds at the 3.81 ms a MiB its reads showed with
  sixteen in flight. The plan expected 32 MiB, from X18's 2.3 ms a MiB at 200 MiB a second.
- **Partitions a device read hold both lines**: 10.5 on the Zen1 hosts loaded once and 10.6 after an
  hour's churn, 12.6 and 11.3 on europa, so a churned table's runs need no consolidation (Q62). At
  40 MiB segments, 40 and 23.6.
- **Cpu a partition is 22.6 µs on a Zen1 host at today's segments**, past the budget's 15, and 10.1
  above its node's idle cpu, which alone is 0.23 to 0.26 cores. At 40 MiB segments 12.2 µs gross
  ([O149](../../appendix/optimizations.md#o149-larger-wal-segments-keep-a-tablets-records-together-four-times-longer)).
- **The drivers' figures are no worse**: a device drained 11 s sooner, 385 s against 396 with the
  ranges disjoint, the foreground's put p99 beside it lower; a device failure settled in 305 to 306 s
  after M23 and in 26 or 306 s before, the ranges overlapping, and the gets beside it were faster.
  Its 300 s is the readers' grace a failed device's left slice waits out on both sides
  ([item 348](../../appendix/known-issues.md#348-a-failed-devices-left-slice-waits-out-the-readers-grace-and-a-move-that-needs-it-waits-with-it)).
  One row is disjoint and worse: the puts' p99 before a device failed, 94.9 ms against 90.0, with
  no event yet running. Its twin before a scrub is disjoint and better, as two rounds a side are by
  chance one time in three.

## The question

M23's exit, as its plan states it: the cell's read and insert p99 beside a full scan of `Big` after a
restart and of `Keyword`, the stand-in for TMDB's `MovieByKeyword`, within
`max(1.10 × before, before + 0.1 ms)`; no `Resident` eviction pass; no scan loop step over 100 µs;
`Big` scanned alone at its ceiling with at least ten partitions a device read and at most 15 µs of a
Zen1 host's cpu a partition; after an hour's churn at least nine partitions a device read, at today's
10 MiB segments and at 40 MiB; the drivers' figures no worse than before
([decision 5](milestones.md#m23-the-walk-rebuilt-for-load)). If the read clause is missed, the
fallback is decision 1(c), recorded with the user.

## How it was judged

**W7's budget**, as X16 and X18 judged it: the cell's p99 in the scan event's *during* window over
its *before* window in the same arm, at the median over rounds. Two more views are given beside it,
since a whole scan here outlasts what the cell holds steady for (below): the cell's p99 a second,
each window the median of its seconds' p99s, during the scan before the cell's feed wrapped against
before it; and *during* against the mean of *before* and *after*.

**W12's cost**: partitions a device read, every partition each host's member read over the device
reads its device counted, warmup and measurement both; and a Zen1 host's node cpu over the same
partitions, both gross and above the node's idle cpu, sampled for 30 s before each scan alone with
the cluster up and nothing driving it, as X17 sampled an idle minute.

**The lab's rule for a difference**: a difference is a result only where two arms' rounds give
ranges that do not overlap.

## What was run

### The cluster and the build

`m23`, X16's side cluster under another name: three nodes on the tmdb cluster's hosts and cores at
4 GiB a node, europa six shards and titan and hyperion five, bootstrapped fresh for every round and
destroyed after it; the tmdb node stopped, every host's maintenance timers held and its governor at
`performance`, all put back after ([m23-host-changes.txt](../../../../shoal-spike-scan/results/m23-host-changes.txt)).
The build is the product at `3b713082`, every scan setting its default: the share 75 ms a second,
sixteen scan reads a device, a ceiling of 100 MiB and a trickle of 2 MiB a second a device, two scan
pages a shard. The script is [m23-lab.sh](../../../../shoal-spike-scan/results/m23-lab.sh) and the
tables come from [m23-report.py](../../../../shoal-spike-scan/results/m23-report.py).

### The rounds

Eight, each a cluster bootstrapped fresh: a preload of `Cell`'s first 100,000 rows, every row of `Big`
and nine tenths of `Keyword`'s; a restart; a capture of the cell alone (`rw50/b1/none`), `Big` and
`Keyword` scanned alone beside the paced stream, and the cell beside a whole scan of `Big` asked 30
seconds into its arm (`rw50/b1/scan`); a restart; and the cell beside a whole scan of `Keyword`. The
cell is X16's: half reads and half inserts on `Cell`, a kibibyte row, a bundle of one, `Keyword`
written into by a paced stream of 200 inserts a second throughout.

### The churn

At each of 10 and 40 MiB segments, a cluster of its own: `Big` loaded once, a restart, the idle
cluster sampled, `Big` scanned alone with nothing beside it, an hour of X17's churn into `Big`, a
restart, the idle cluster sampled again, and `Big` scanned alone again.

### The drivers

The bench's copy of `tmdb_cluster.yaml`, two pool devices a host, from a worktree at each side's
commit, `e2f80164` before M23's first step and `3b713082` after: `rw50` of 1 MiB objects at two in
flight beside no event, a device failed, a deep scrub and a device drained, each on a cluster of
its own, the sides alternating by round.

### The control

Two rounds more, run after the others. Each round's cluster went through a round's first capture
as the rounds' did and was restarted. The cell was then driven alone for 120 s beside the paced
stream, where a round runs the cell beside a scan of `Keyword`. That shows what the cell's p99 a
second does with no scan beside it, over the seconds a `Keyword` scan covers. Those seconds run
from where the `Keyword` captures' scans were asked, 28 s into their series, to the cell's own
feed's wrap.

## What was found on the way

Two quick passes before the rounds, a tenth of `Big` each, found four things the exit's figures
depended on, each fixed before the rounds and committed with its page.
[Finishing M23](whats-left-todo.md#finishing-m23) draws them with the work left after the rounds:

- **A paced scan's reads reached their device one at a time** ([item 346](../../appendix/resolved/paced-reads-alone.md)).
  Each read waited for its own credit once the share bound, and went out alone, so the scans'
  busy time counted every read's whole latency: a 970 EVO's mebibyte was learned at 18.4 ms where
  X18 measured 2.3, and a scan of `Big` alone read 4.1 MiB a second of it. A tenth of `Big` beside
  the cell took 229 s. A wave's reads are paced a group at a time since.
- **An encode step of small rows held its loop past 100 µs** ([item 347](../../appendix/resolved/encode-steps-by-rows.md)).
  64 KiB of `Keyword`'s rows held a Zen1 host's loop up to 477 µs a step: 160 of titan's steps
  passed the bound in one capture. A step is bounded by the rows 50 µs holds at what its table's
  rows have shown they cost since.
- **Four scan reads a device held a scan alone to two thirds of its rate**
  ([O146](../../appendix/optimizations.md#o146-four-scan-reads-a-device-held-a-scan-alone-to-two-thirds-of-its-rate)).
  With reads grouped, a group overlaps only as far as the device's bound. At sixteen, a page's
  sixteen runs, a Zen1 host's 970 EVO read 12 to 13 MiB a second alone at 5.2 to 5.8 ms a MiB,
  where four read 8.5 at 8.2 to 8.7; past sixteen a page's wave bounds a group first.
- **The bench's scan driver asked a shed page again every 2 ms**
  ([O148](../../appendix/optimizations.md#o148-a-scan-driver-asked-a-shed-page-again-every-2-ms-beside-pages-paced-for-tens)),
  O139's wait from when a page ended in a millisecond. Beside pages paced for tens of milliseconds
  its surplus streams asked a Zen1 member about 2,000 times a second, 16 to 33 sheds a page
  answered, and the members' cpu went to them. A stream's wait doubles to 64 ms since: sheds fell
  to about 2 a page and a Zen1 node's cpu over a scan alone from 0.68 cores to 0.52.

And three things that are not fixed, named where the tables need them:

- **Each shard's io_uring workers run on the shard's own cpu**
  ([O147](../../appendix/optimizations.md#o147-each-shards-io_uring-workers-run-on-the-shards-own-cpu)).
  `taskset -cp` on titan shows every `iou-wrk` thread pinned to its executor's one cpu, so the
  fdatasync of every WAL batch runs in a kernel thread that preempts its shard's loop: beside the
  cell, titan's shards were runnable but off their cpus 18 to 24 ms of every second. A quick pass
  found light steps past 100 µs rising with the syncs beside them, 8 beside the paced stream and 33
  beside the cell. The rounds did not bear that out: 8 to 15 a capture of `Big` alone beside the
  paced stream, and 3 to 16 beside the cell.
- **A scan's steps of large resident partitions pass 100 µs**
  ([item 350](../../appendix/resolved/encode-copy-deadline.md)),
  found in the rounds' counters and set out under the loop's steps below. The counters count a
  page's capture and an encode step alike, so which of them holds the loop is not known.
- **The cell's feed wraps inside a long scan.** `Cell`'s dataset is 300,000 rows, 100,000 of them
  preloaded, so the cell's inserts run out of new rows about 200,000 inserts into an arm, some 85
  seconds, and wrap into overwrites of ~~the rows loaded before the restart, which are not resident~~
  the rows the arm itself wrote a minute or two before. The bench's feed starts again at the pool's
  first row, as it does at every arm's start, and those rows are resident: no capture counts an
  eviction pass of either kind. This record first said the wrap led into rows loaded before the
  restart. That holds for the `Keyword` arm's first pass, before its wrap, not after it (corrected
  2026-11-01).
  ~~Its inserts then fall from about 2,300 a second to 1,400 and their p99 steps from about 24 ms to
  40, and stay there whether a scan runs or not.~~ Its inserts fall from about 2,300 a second to
  1,400 and their p99 steps from about 24 ms to 40, and stay there whether a scan runs or not. That
  fall comes about 75 s into the arm, 8 to 29 s **before** the wrap, in all eighteen arms, and a cell
  that never wraps falls at the same point. So the fall is not the wrap's, ~~and its cause is not yet
  known~~ and it is the lab's 970 EVOs, whose flush goes from 0.82 ms to 2.83 about 75 s into the
  cell's synced writes ([item 351](../../appendix/resolved/the-cells-fall-is-the-drives.md), corrected
  2026-11-01).
  X18's whole scans ended inside 40 s, before the
  ~~wrap~~ fall; a whole scan of `Big` beside the cell at this pace runs past it, so its *during* window
  holds both of the cell's workloads, and its *after* window only the second. The tables give
  W7's ratio as it is defined, and beside it the cell's p99 a second during the scan before its
  feed wrapped (`m23-report.py`, `before_the_wrap`).

## The tables

Medians over the rounds, the range beside each in brackets. The whole set is
[m23-report.md](../../../../shoal-spike-scan/results/m23-report.md), from the slimmed captures in
[shoal-spike-scan/results/m23](../../../../shoal-spike-scan/results/m23).

### The cell beside a whole scan

`rw50/b1/scan`, eight rounds:

| Figure | Beside `Big` | Beside `Keyword` |
| --- | --- | --- |
| A whole scan, s | 180 [146, 223] | 94.1 [88.4, 125] |
| The scan's partitions a second | 16,700 [13,500, 20,400] | 599 [450, 636] |
| Read p99 ms, before to during | 6.22 to 6.55 | 9.36 to 8.11 |
| Read, during over before (W7) | 1.07 [1.03, 1.08] | 0.87 [0.82, 0.97] |
| Read a second, during before the wrap over before | 1.08 [1.02, 1.11] | 0.87 [0.80, 1.03] |
| Insert p99 ms, before to during | 24.8 to 40.6 | 27.3 to 40.6 |
| Insert, during over before (W7) | 1.62 [1.56, 1.72] | 1.48 [1.30, 1.55] |
| Insert a second, during before the wrap over before | 1.06 [1.03, 1.12] | 1.23 [1.05, 1.36] |
| Insert, during over the mean of before and after | 1.19 [1.12, 1.27] | 1.03 [0.98, 1.18] |
| Seconds of the scan before the cell's feed wrapped | 61.5 [60, 64] | 68 [67, 75] |
| `Resident` eviction passes | none in any round | none in any round |

The cell alone, the first arm after each restart (`rw50/b1/none`): read p99 7.49 ms [7.21, 7.66],
insert p99 24.8 ms [24.3, 26.1], 2,370 reads and as many inserts a second.

### A scan alone

`scan/Big/none`, beside the paced stream in the rounds, and with nothing beside it once `Big` was
loaded in the churn's cluster, the run W12's cost budget is judged on:

| Figure | `Big` beside the paced stream, eight rounds | `Big` loaded once, nothing beside it, 10 MiB segments |
| --- | --- | --- |
| The cluster's partitions a second | 30,500 [28,100, 34,200] | 50,600 |
| MiB a second a Zen1 host's 970 EVO | 14.3 [13.7, 14.6] | 19.8 |
| Its busy ms a MiB, as the scans learned it | 5.16 [5.13, 5.24] | 3.81 |
| MiB a second of europa's Optane | 9.21 [7.69, 14.7] | 19.0 |
| Partitions a device read, the Zen1 hosts | 9.81 [9.76, 9.86] | 10.5 |
| Partitions a device read, europa | 11.7 [11.7, 11.8] | 12.6 |
| A Zen1 host's node cpu a partition, µs | 49.0 [47.8, 52.2] | 22.6, 10.1 above idle |
| Europa's node cpu a partition, µs | 23.8 [16.3, 28.3] | 9.62, 1.94 above idle |
| A node's cpu idle, cores | | 0.226 to 0.256 on the Zen1 hosts, 0.141 on europa |

`Keyword` alone beside the paced stream read 2,150 partitions a second [1,930, 2,580] at 1.56
partitions a device read on the Zen1 hosts: its partitions are written into throughout, so each is a
chain of fragments read alone, not a run.

### The loop's steps

Every scan step that held a shard's loop, captures and encode steps, each capture's counts since the
restart before it:

| Figure | The `Big` captures | The `Keyword` captures |
| --- | --- | --- |
| Scan steps a capture | 92,300 [78,500, 109,000] | 10,400 [9,700, 14,700] |
| Past 100 µs | 201 [158, 268] | 18.5 [6, 24] |
| The longest, µs | 424 [326, 539] | 368 [224, 489] |

Nine in ten of a `Big` capture's steps past 100 µs were in `Keyword` alone's arm, 133 to 248 a
round. They were most where the resident partitions a member's steps handed out were largest, each
round's figures in the report:

| Members and arm | KiB a resident partition handed out | Steps past 100 µs, a capture |
| --- | --- | --- |
| europa, `Keyword` alone, eight rounds | 18.7 to 23.0 | 90 to 208 |
| titan and hyperion, `Keyword` alone, the rounds at 5 KiB or more | 5.4 to 9.8 | 19 to 48 |
| titan and hyperion, `Keyword` alone, the rest | 0.6 to 3.2 | 0 to 6 |
| europa, `Keyword` beside the cell, eight rounds | 8.8 to 15.2 | 0 to 11 |
| titan and hyperion, `Keyword` beside the cell | 0.6 to 5.8 | 1 to 17 |

Size is not the whole of it, since europa beside the cell handed out partitions of 9 to 15 KiB and
few of its steps passed.

Each shard's `/proc/<tid>/schedstat` was sampled over 30 s of round four. Beside the cell and a scan
of `Big`, every one of titan's shards was preempted about 430 times a second, 42 to 54 µs each on
average, and was runnable but off its cpu 18 to 24 ms of every second
([O147](../../appendix/optimizations.md#o147-each-shards-io_uring-workers-run-on-the-shards-own-cpu)).
In `Keyword` alone's arm, titan's were preempted 142 to 257 times a second at 29 to 38 µs and
europa's 108 to 181 at 10 to 13 µs. Europa, preempted least, passed most, so what held its steps is
in them ([item 350](../../appendix/resolved/encode-copy-deadline.md)).
Beside the cell and a scan of `Big`, whose steps copy under a KiB, 3 to 16 a capture passed, and
8 to 15 in `Big` alone's arm.

### The churn

`Big` loaded once and scanned with nothing beside it, then again after an hour of X17's churn, one
run each:

| Figure | 10 MiB, loaded once | 10 MiB, churned | 40 MiB, loaded once | 40 MiB, churned |
| --- | --- | --- | --- | --- |
| The cluster's partitions a second | 50,600 | 53,900 | 99,300 | 60,600 |
| MiB a second a Zen1 host's 970 EVO | 19.8 | 21.6 | 35.0 | 26.8 |
| Its busy ms a MiB, as the scans learned it | 3.81 | 3.47 | 1.95 | 2.38 |
| Partitions a device read, the Zen1 hosts | 10.5 | 10.6 | 40.0 | 23.6 |
| Partitions a device read, europa | 12.6 | 11.3 | 45.7 | 34.0 |
| KiB a device read, the Zen1 hosts | 11.7 | 13.0 | 40.9 | 28.4 |
| A Zen1 host's node cpu a partition, µs | 22.6 | 22.0 | 12.2 | 16.8 |
| The same above its idle cpu, µs | 10.1 | 8.94 | 5.16 | 5.94 |
| Europa's node cpu a partition, µs | 9.62 | 8.90 | 5.76 | 8.51 |
| A Zen1 node's idle cpu, cores | 0.241 | 0.248 | 0.254 | 0.260 |

### The drivers

Two rounds a side, the median and range of each, a difference only where the ranges are disjoint:

| Event | Figure | Before | After | Disjoint |
| --- | --- | --- | --- | --- |
| None | Get p99 ms, put p99 ms | 29.9, 109 | 29.0, 107 | no, no |
| A device failed | Settled after, s | 166 [26, 306] | 306 [305, 306] | no |
| | Put p99 ms before it | 90.0 [89.9, 90.0] | 94.9 [93.8, 96.1] | yes |
| | Get p99 ms during | 42.3 [35.2, 49.3] | 32.5 [31.1, 34.0] | yes |
| | Put p99 ms during | 144 [127, 161] | 129 [126, 131] | no |
| | Gets and puts a second during | 22.2, 21.3 | 22.5, 22.1 | yes, yes |
| A deep scrub | Its window, s | 601, unfinished | 601, unfinished | no |
| | Put p99 ms before it | 90.7 [90.6, 90.8] | 90.2 [90.0, 90.3] | yes |
| | Put p99 ms during | 161 [153, 169] | 163 [162, 165] | no |
| A device drained | Done after, s | 396 [393, 400] | 385 [383, 387] | yes |
| | Put p99 ms during | 166 [161, 171] | 152 [151, 153] | yes |
| | Puts a second during | 21.6 | 22.1 | yes |

Every row whose ranges are disjoint is here; the rest are in the report. At two rounds a side, two
sides drawn from one distribution give disjoint ranges one time in three, so a row alone is weak
evidence. The puts' p99 before an event moved both ways: it was 5 ms worse before a device failed
and half a millisecond better before a scrub, in windows where only the build differed.

The deep scrub of the pool, four thousand placement groups of 1 MiB objects, outlasted its event's
600 s on both sides. A device failure settled in 26 s when no group had to wait for a slice it had
left, and at the readers' grace, 300 s, when one did, as three of the four did
([item 348](../../appendix/known-issues.md#348-a-failed-devices-left-slice-waits-out-the-readers-grace-and-a-move-that-needs-it-waits-with-it)).

### The cell alone, the control

`rw50/b1/none` in a `Keyword` capture's place, two rounds, beside the eight rounds' cell beside a
scan of `Keyword`. Its p99 a second is cut at 28 s, where those scans were asked, and at its own
feed's wrap, 67 s [66, 68] later:

| Figure | The cell alone, two rounds | Beside `Keyword`, eight rounds |
| --- | --- | --- |
| Read p99 a second, ms, before to after | 8.14 to 8.00 | 8.47 to 7.39 |
| Read, after and before the wrap over before | 0.99 [0.90, 1.08] | 0.87 [0.80, 1.03] |
| Insert p99 a second, ms, before to after | 26.5 to 30.3 | 25.9 to 32.7 |
| Insert, after and before the wrap over before | 1.16 [0.98, 1.34] | 1.23 [1.05, 1.36] |

The ranges overlap on both rows. Before the wrap, the cell's inserts did with no scan what they did
beside a scan of `Keyword`.

## What it says for the design

- **The rebuilt walk holds the foreground's reads.** A whole scan of a table larger than memory after
  a restart, at the pace in device time with sixteen reads a device, moved the cell's read p99 by
  7%, inside W7's 10%; X18's spike walk at 32 MiB a second moved it 14%. The read clause holds at the
  product's defaults, so decision 1(c)'s looser clause is not needed.
- **A scan's own cost is per partition, not per page or read.** Above a Zen1 node's idle cpu a
  partition costs 9 to 10 µs at 10 MiB segments and 5 to 6 at 40, within W12's 15 either way; the
  gross figure is the idle node's background spread over the partitions the pace lets through, so it
  falls as the rate rises.
- **The layout is the lever left.** Partitions a device read, and with them a scan's rate under its
  share and its cost, follow how much of a tablet a merge block holds: four times as much at 40 MiB
  segments as at 10, and still more than twice after an hour's churn. Consolidation is not needed to
  keep nine a read (Q62); a larger segment would do more, at costs not measured here (O149).
- **A share of device time is not a byte rate.** What a MiB costs a device depends on how deep and
  how large its reads are and on what else the device is doing: 3.81 ms on a 970 EVO for a scan alone
  at today's segments, 1.95 at 40 MiB, 11.1 beside the cell, 2.3 in X18's run at 200 MiB a second.
  75 ms a second is 19.8 MiB of a 970 EVO alone, not the 32 the plan read off X18.
- **A count of rows does not hold every step to its time.** An encode step's rows are bounded by a
  cost learned from other steps, and a sorted table's large resident partitions outran it on every
  member, most on the fastest host (item 350). Few light steps passed beside the cell. A shard's own
  io_uring workers take its cpu from under every step, the scan's and the foreground's alike (O147).

## For the user to decide

The exit ran with every setting the plan decided. What it leaves open:

1. **The insert clause.** W7 judges *during* over *before* in one arm, and here the cell's own
   workload changes inside the arm. Read before the wrap, the clause holds beside `Big` (1.06) and
   misses beside `Keyword` (1.23), where the cell alone rises 1.16 over the same seconds. Three
   ways to settle it:
   - judge the clause before the wrap and against a control arm, as this record does;
   - give the cell rows enough not to wrap within an arm, and run the rounds again with a control
     beside them;
   - accept the miss as W7 defines it.
2. **The step clause.** About one step in 460 passed 100 µs. Nine in ten were steps of `Keyword`'s
   large resident partitions, which item 347's bound does not hold (item 350). That is a defect of
   M23's own, to reproduce, fix and measure again before the clause is read. The light steps beside
   the cell, 3 to 16 a capture, are what moving the io_uring workers (O147) would touch. Whether that
   move is M23's is the question.
3. **The cost clause.** 22.6 µs a partition gross at today's segments, 10.1 above the idle node: which
   W12 means; or a default segment of 40 MiB, 12.2 µs gross, once its costs are measured (O149).
4. **The share.** 75 ms a second buys a 970 EVO about 20 MiB a second alone, where the plan expected
   32: about 120 ms would buy 32, at a cost beside the cell this exit did not measure, since the
   rounds ran the default alone; or 75 ms is kept and W12's rate restated as the share's.

## Decided with the user, 2026-10-31

The same day, on this record's evidence:

1. **The insert clause is read as W7 defines it**, over the whole arm, on the exit's rounds run again
   with a cell whose feed does not wrap inside an arm and a control of the cell alone beside them. The
   figures before the wrap above are evidence until then, not the verdict.
2. **Item 350 is M23's**: its steps counted by kind, its cause found and fixed before those rounds, and
   the step clause read on them. **O147 follows M23**, so the step clause is read on a step's own
   work: the rounds count each step's own cpu beside its wall time, and a step past 100 µs by the
   wall alone is named as O147's.
3. **W12's cost is a scan's own cpu, above its idle node**: 10.1 µs a partition on a Zen1 host at
   today's segments, within the 15. The budget was set from X17's figures, gross at an unpaced rate,
   where the idle node came to under half a microsecond a partition, so the two readings agreed
   when it was set. At a paced rate the gross figure is mostly the idle node's quarter core spread
   over the partitions the pace lets through. **O149 follows M23**, its costs measured on their own.
4. **The share stays at 75 ms, and W12's rate for M23 is what it buys**: about 20 MiB a second of a
   970 EVO alone, 50,600 of `Big`'s partitions a second for the cluster. Once O149 is done the rate is
   read again at its segments, where the same share bought 35 MiB a second, rather than taken from
   more of the device's time.

So the cost and rate clauses hold as read, and M23 closes once item 350 is fixed and the rounds run
again hold the insert and step clauses. Item 350 is fixed since 2026-11-01
([Resolved #350](../../appendix/resolved/encode-copy-deadline.md)), its run on the lab found
[item 353](../../appendix/known-issues.md#353-light-scan-steps-pass-100-µs-on-their-own-cpu-which-a-kernel-without-irq-time-accounting-does-not-tell-from-interrupts)
beside it, and the step clause is read on the rounds again with both in view.
[Finishing M23](whats-left-todo.md#finishing-m23) draws what is left.

## The cell for the rounds again

**Built 2026-11-01** (`917e4939`), for the first decision above. The rounds run again on a cell
whose feed never wraps inside an arm, so W7's insert clause can be read over the whole arm. A round
on the lab shows that it never wraps. The same round shows that the wrap was not what changed the
cell's inserts inside an arm: they fall about 75 s in, wrap or not
([item 351](../../appendix/resolved/the-cells-fall-is-the-drives.md)). So the rounds again also ~~wait
for~~ waited for item 351, decided with the user the same day ([below](#checked-on-the-lab)). It is the
lab's 970 EVOs, and the rounds again warm every cell arm 100 s before it measures
([below](#the-cells-arms-warmed-past-the-drives-change)), decided with the user the same day.

### How long the arms ran

The bench's feed takes an arm's inserts from the rows after the preload, in file order, starting
again at the first of them for every arm. A wrapping feed also starts again there when it runs out
(`shoal-loadgen/src/feed.rs`, `read_range`). The first exit's arms, from its captures:

| Arm | Seconds | Inserts a second, its first 60 s | The wrap, seconds in |
| --- | --- | --- | --- |
| The cell beside `Big`, eight rounds | 185 to 261 | 2,304 to 2,375 | 88 to 92 |
| The cell beside `Keyword`, eight rounds | 127 to 164 | 2,168 to 2,316 | 95 to 103 |
| The control, two rounds | 131 | 2,268 and 2,313 | 94 and 96 |

Without a wrap the longest arm inserts about 620,000 rows, at 2,375 a second for 261 s.

### Why the nodes run at 6 GiB

A cell that never wraps keeps every row it inserts. Each is a kibibyte row the cell's reads must
find resident. On titan, round 8's `none` arm beside `Big` inserted 170,000 new rows, and the
process grew by 0.66 GiB: about 4.2 KB a row, against the 1.3 KB the shards count for it. An
overwrite grew it by about 1 KB. At the end of that round's arm beside `Big`, the longest, titan's
process held 2.89 GiB and europa's 3.23. Each node's budget is its memory less its scan buffers,
3.96 GiB, and the cell had wrapped at 300,000 rows. Without the wrap, the same arm would insert some
450,000 new rows past the `none` arm's, and titan would end it near 4.4 GiB. That is past the node's
memory: a `Resident` eviction pass would evict the cell's own rows, and change its reads within the
arm as the wrap changed its inserts.

**Decided with the user, 2026-10-31: the rounds again run at 6 GiB a node.** The hosts have 14 GiB,
and X20 ran ScyllaDB at 6 GiB on titan. `Big` is still read from its archives after every restart,
since a walk never makes a row resident. Every clause of the rounds again is read at 6 GiB, not at
the first exit's 4.

### The cell

`Cell.jsonl` of 1,100,000 rows: the first 100,000 preloaded, as before, and a million to insert. An
arm wraps only after about 420 s at 2,375 a second, 1.6 times the longest arm. At 6 GiB a node, a
budget of 5.96 GiB, titan's process would pass it at about the same point. The longest arm of the
first exit would end near three quarters of it. A row's bytes are its seed's and id's alone, so
the first 300,000 rows are X16's, byte for byte. `m23-lab.sh data` writes the cell into
`target/lab/m23/dataset` with `x16-data --tables cell`, and links X16's own `Big.jsonl` and
`Keyword.jsonl` beside it rather than copying them. So the scanned tables are the first exit's
files. `x16-data` writes each table to a file of its own and renames it into place once synced, so a
write never truncates a file linked from another folder. `check` holds the cell's rows and the two
links. **A capture whose cell's feed wrapped stops the run by name**: every later round would wrap
as that one did. `m23-report.py` counts, for each round:

- its cell arms whose feed wrapped;
- its longest arm's inserts and the pool they were drawn from;
- its members' peak resident memory over the node's;
- since its check round, where the cell's inserts fell and stayed down, and after how many
  inserts (`insert_fall`, item 351's).

Beside W7's lines it says whether any round's cell wrapped, and whether any round's cell's inserts
fell. For the first exit both lines read 8 of 8 rounds, and the peak was 0.81 of the node's memory
beside `Big`.

Every arm's feed still starts again at the first row after the preload, so an arm's first inserts
overwrite the rows an earlier arm wrote. The first exit's *before* windows show that this costs what
a new row costs: the `Big` capture's scan arm overwrote its `none` arm's rows at an insert p99 of
24.8 ms, as the `none` arm wrote new ones.

### Checked on the lab

A quick pass and then one full round of the exit's, both on 2026-11-01 at `917e4939`, each in a lab
directory of its own, every scan setting its default. The round is
[shoal-spike-scan/results/m23-cell](../../../../shoal-spike-scan/results/m23-cell): its slimmed
captures, its host changes and its report. Every host was identical to its snapshot after each run.

| Figure | Beside `Big` | Beside `Keyword` |
| --- | --- | --- |
| The arm, s | 210 | 148 |
| The whole scan, s | 172 | 109 |
| The cell's inserts in the arm | 359,272 | 259,512 |
| The feed's wrap | never | never |
| Members' peak resident memory over the node's | 0.56 | 0.51 |
| Eviction passes, either kind | none | none |
| Read p99 ms, before to during (W7) | 6.31 to 6.81, 1.08 | 8.71 to 8.97, 1.03 |
| Insert p99 ms, before, during and after | 25.4, 41.3, 41.7 | 26.1, 43.9, 42.7 |
| Inserts a second, before to during | 2,380 to 1,630 | 2,360 to 1,640 |
| The inserts' fall, seconds into the arm | 78, 182,518 inserts in | 69, 158,082 in |

**The cell does what it was built for.** No arm wraps, and the nodes keep the cell's rows with
nearly half their memory to spare.

**The cell's inserts still fall inside the arm.** Beside the two scans the insert p99 rose 1.63 and
1.68 times during the scan, and stayed as high after it. The cell's inserts fell, at 78 s and at
69 s, to about 1,330 a second at 40 to 45 ms. Read back against the first exit's arms, the same fall
comes in all eighteen of them: 71 to 80 s in, 8 to 29 s **before** the wrap, the two controls with no
scan included. So the wrap never caused the insert miss. The fall did, at least in large part: it
comes with no scan beside the cell, and the *after* windows are as slow as the *during* ones. What
the scan adds beside it is not yet measured. The fall comes about 75 s into the cell's steady
inserts, whatever the rows are, and a pause between arms undoes it
([item 351](../../appendix/resolved/the-cells-fall-is-the-drives.md)).

**Decided with the user, 2026-11-01: item 351 is M23's**, found and, if it is a defect, fixed before
the exit's rounds are run again. Until the cell's own inserts hold their rate over an arm, W7's
insert clause read over the whole arm measures the cell, not the scan.

### The cell's arms warmed past the drives' change

**Found 2026-11-01** ([Resolved #351](../../appendix/resolved/the-cells-fall-is-the-drives.md)). The
cell ran truly alone, with no paced stream and no scan, every host sampled each second
(`FALL_ONLY=1`, [its record](../../../../shoal-spike-scan/results/m23-fall)). About 75 s into each of
its two arms, the lab's 970 EVOs went from a flush of 0.82 ms to one of 2.83. Each Zen1 node's WAL
syncs fell from about 592 a second to 324, and the cell's inserts from about 2,490 a second to 1,454.
Nothing of Shoal's moved at the fall: no compacting segment, no apply lag, no forced purge, and
appends a sync rose only from 4.0 to 4.35. fio replaying the same flushes did the same with no Shoal
unit up, and a rest returned the drives to their first state. So the insert clause, read from an
arm's tenth second, compared a *before* window taken with the drives rested with a *during* window
taken mostly with them loaded.

**Decided with the user, 2026-11-01: the rounds again warm every cell arm 100 s before it
measures.** `TIMES` and `CONTROL` in `m23-lab.sh` warm for 100 s, not 10, so every window of an arm
is taken in the drives' loaded state, the state a busy node is in. The scan is still asked 18 s into
the measured window. `m23-report.py`'s `insert_fall` reads the measured window, and a fall inside it
stops the round being read for W7.

The first exit's insert figures, and X16's and X17's beside a whole scan, are kept as they were
taken, each with its reading in one state beside it. Split at the fall, a scan of `Big` beside the
cell raised its insert p99 a second about a tenth (1.10 to 1.16 times over the rounds of X16 and X17).
The rest of their 1.30 to 1.63 was the drives'.

The warmup's check is one round of the exit's own at `917e4939`, its cell's arms warmed 100 s, on
2026-11-01, every host identical to its snapshot after
([shoal-spike-scan/results/m23-warm](../../../../shoal-spike-scan/results/m23-warm), its report beside
its slimmed captures):

| Figure | Beside `Big` | Beside `Keyword` |
| --- | --- | --- |
| The arm, warmup and measured, s | 100 and 240 | 100 and 219 |
| The whole scan, s | 212 | 191 |
| The cell's inserts in the arm | 536,622 | 472,985 |
| The feed's wrap | never | never |
| The inserts' fall | 78 s in, in the warmup | 67 s in, in the warmup |
| A fall inside the measured window | none | none |
| Members' peak resident memory over the node's | 0.65 | 0.59 |
| Eviction passes by the process's memory | none | none |
| Inserts a second, before to during | 1,350 to 1,340 | 1,260 to 1,280 |
| Insert p99 ms, before to during (W7) | 43.9 to 43.8, 1.00 | 47.3 to 46.4, 0.98 |
| Read p99 ms, before to during (W7) | 5.69 to 6.06, 1.07 | 9.31 to 7.84, 0.84 |

**Warmed, every window of an arm is in one state of the drives.** The inserts fall inside the warmup,
67 and 78 s in, and the measured windows hold 1,260 to 1,350 inserts a second throughout. On this one
round W7's insert clause, read as it is defined over the whole arm, held beside both scans, where the
first exit's rounds read 1.62 and 1.48. The step clause still failed (item 350), and the cost clauses
read Big alone as before. The rounds again, with their control, are what M23's clauses are read on.

## Item 350's steps named and fixed

**Named 2026-11-01** on F130's build (`263a17fc`), for the second decision above: four of this exit's
rounds as they stand, the cell warmed 100 s, on a lab directory of their own, every host identical to
its snapshot after (`shoal-spike-scan/results/m23-i350/`, `report.md` beside the captures). F130
counts a page's captures apart from its encode steps, each with its own cpu and what a long step held.

| Arm, four rounds | Encode steps past 100 µs on their own cpu, a member | Rows of every encode step | Rows of one past 100 µs |
| --- | --- | --- | --- |
| `Keyword` alone | europa 111–204, hyperion 19–32, titan 17–22 | 3–73, 15–21, 10–15 | 148–232, 62–68, 57–65 |
| The cell beside a scan of `Keyword` | europa 0–5, hyperion 7–19, titan 3–10 | 33–36, 12–17, 5–13 | 208–231, 55–63, 26–60 |

So item 350's steps were encode steps, on their own cpu, with three to five times the rows of their
arm's steps, at about twice the cpu a row. The rows bound let them, a cost learned from steps that cost
under half as much a row, and since 2026-11-01 a piece's copy ends once it has held its loop 25 µs
([Resolved #350](../../appendix/resolved/encode-copy-deadline.md)). The same four rounds on the fixed
build confirm it before its box turns green.

**The run found a second population** the deadline does not touch: 0 to 14 light steps an arm on every
member, encode steps of one 1 KiB row of `Big` and captures of about ten keys, at 160 to 280 µs that
F130 counts as their own cpu. This exit put them down to the shards' io_uring workers (O147), which
would show by the wall alone. But none of the lab's kernels sets `CONFIG_IRQ_TIME_ACCOUNTING`, so the
interrupts a shard's cpu takes are charged to its thread, and on titan one shard's cpu takes every NIC
receive softirq, about 21,600 a second. Filed as
[item 353](../../appendix/known-issues.md#353-light-scan-steps-pass-100-µs-on-their-own-cpu-which-a-kernel-without-irq-time-accounting-does-not-tell-from-interrupts),
not yet attributed.

## Limitations

- **The cell's feed wraps inside a long arm**, as above: ~~a cell of more rows, or arms that end
  before it wraps, would let W7's ratio be read as it is defined~~ since 2026-11-01 the rounds again
  run on a cell of 1,100,000 rows whose feed never wraps inside an arm, at 6 GiB a node
  ([the cell for the rounds again](#the-cell-for-the-rounds-again)). Its inserts still fall about
  75 s into an arm, as the first exit's did before their wrap
  ([item 351](../../appendix/resolved/the-cells-fall-is-the-drives.md)): the lab's 970 EVOs, whose
  flush triples there, which the rounds again warm every arm past.
- **The first exit's and X16's insert windows straddle the drives' change**: every *before* window
  was taken with the drives rested and every *during* window past about 75 s mostly with them
  loaded. Each such figure is kept, with its reading in one state beside it
  ([item 351](../../appendix/resolved/the-cells-fall-is-the-drives.md#evidence)).
- **One run of each churn arm**, not rounds: their figures are single values.
- **The cell's figures are the cluster's**, from its client, so the Optane's budget and the 970
  EVOs' cannot be told apart, as X18 found.
- **Steps past 100 µs are counted, not attributed**: the preemption figures are 30 s samples of
  `/proc/<tid>/schedstat` in two rounds, beside the step counts, not a step's own cpu time. The
  counters count a page's capture and an encode step alike and keep no long step's bytes (item 350).
  Since [F130](../../features/loop-steps-by-kind.md) (2026-11-01) they count each kind apart, with a
  step's own cpu beside its wall and what a long step held, for the rounds again.
- **The deep scrub never finished inside its event** on either side, so its own time is not compared.
- **The disks were not measured**: the exit ran the SSD layout alone.

## Related

- [M23, the walk rebuilt for load](milestones.md#m23-the-walk-rebuilt-for-load), whose exit this is
- [W7, not slowing other queries](isolation.md), and [W12's budgets](performance.md#the-budgets)
- [X16](walk-beside-the-foreground.md), whose cell and budget it judged by, [X17](page-rebuilt.md) and
  [X18](table-reads-paced.md), whose walk and pace it rebuilt, and [X37](tablet-records-together.md),
  whose churn it ran again for an hour
- [F123](../../features/walk-and-scan-counters.md) to [F129](../../features/drivers-on-the-rebuilt-walk.md),
  M23's steps, and [items 346](../../appendix/resolved/paced-reads-alone.md) and
  [347](../../appendix/resolved/encode-steps-by-rows.md), found on the way
- [Items 348 and 349](../../appendix/known-issues.md#348-a-failed-devices-left-slice-waits-out-the-readers-grace-and-a-move-that-needs-it-waits-with-it),
  [350](../../appendix/resolved/encode-copy-deadline.md), named and fixed after the rounds, and
  [351](../../appendix/resolved/the-cells-fall-is-the-drives.md), the last found by the cell for the rounds again
  and resolved the same day, [352](../../appendix/known-issues.md#352-a-shard-asks-every-groups-core-whether-it-runs-after-every-batch-it-applies),
  found while 351 was planned, [353](../../appendix/known-issues.md#353-light-scan-steps-pass-100-µs-on-their-own-cpu-which-a-kernel-without-irq-time-accounting-does-not-tell-from-interrupts),
  found by 350's run,
  [O147](../../appendix/optimizations.md#o147-each-shards-io_uring-workers-run-on-the-shards-own-cpu) and
  [O149](../../appendix/optimizations.md#o149-larger-wal-segments-keep-a-tablets-records-together-four-times-longer),
  filed from its rounds
