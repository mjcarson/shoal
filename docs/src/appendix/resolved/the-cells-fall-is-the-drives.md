# 351. A cell's steady inserts fell to three fifths of their rate about 75 s into an arm

## Symptom

[M23's exit](../../future-work/scans/m23-exit.md) drives a cell beside every scan: `rw50` on `Cell`,
rows of about a KiB, eight workers with four in flight each, through the three lab hosts at a
factor of three. The cell inserted about 2,380 rows a second at an insert p99 of about 25 ms. About
75 s into every arm, its inserts fell to about 1,330 a second at 40 to 45 ms, and they stayed there
to the arm's end. The fall came:

- with a scan beside the cell or without one;
- among new rows and among overwrites;
- at 4 GiB a node and at 6;
- before the cell's feed wrapped, and in a cell that never wraps.

A pause between arms undid it. W7's insert clause compares the cell's p99 during a scan with its p99
before it, over the whole arm. So it compared a *before* window taken before the fall with a
*during* window taken mostly after it.

Every long insert arm on the lab since X16 shows the same fall (`m23-fall-report.py falls`):

| Captures | Arms that fell | The fall, s in |
| --- | --- | --- |
| [X16](../../future-work/scans/walk-beside-the-foreground.md), the cell beside a scan of `Big` | 8 of 8 | 78 to 84 |
| X16, the cell beside a scan of `Keyword`, 71 s arms | 0 of 8 | none |
| [X17](../../future-work/scans/page-rebuilt.md), the cell beside a scan one record at a time | 6 of 8; the other two ended too soon to tell | 82 to 84 |
| X17's churn, an `insert100` arm of 1,206 s | 4 of 4 | 71 to 82 |
| M23's exit, the cell beside `Big` and beside `Keyword`, eight rounds | 16 of 16 | 71 to 80 |
| M23's exit, its controls with no scan, and its two churns of an hour | 4 of 4 | 72 to 78 |
| M23's cell for the rounds again, its check round | 2 of 2 | 69 and 78 |

[X18](../../future-work/scans/table-reads-paced.md)'s SSD leg is not among them, since every arm there
ended by 80 s. In the churns the rate steps down within five seconds and holds for the rest of the
hour. At 10 MiB segments, the five-minute means from the second on are 1,402 to 1,498. At 40 MiB the
step comes at the same point.

## Cause

**The lab's 970 EVOs flush about three times slower once they have taken about 75 s of the cell's
synced writes, and the cell's inserts wait on those flushes.**
[X6](../../object-storage/device-store-ssd.md#two-things-about-the-labs-970-evos) found the two
states in 2026-10: a cache flush of about 0.9 ms after a rest, and about 3 ms after a minute or so of
synced writes, held until the drive rests again.

Under the cell, each Zen1 node's five shards sync their WALs about 592 times a second, one device
flush each. Rested, a flush takes 0.82 ms, so the drive spends 0.49 s of every second flushing. When
the flush goes to 2.83 ms, the drive completes only about 324 a second, 0.92 s of every second. Each
shard's WAL batch then takes 13.2 ms to write and sync instead of 6.1, while its appends grow only
from 3.96 to 4.35.

Every insert's commit waits on a 970 EVO. A group's quorum is two of three. europa's Optane takes
no flush at all, since its cache writes through, but every quorum of two holds titan or hyperion.

The cell keeps 32 inserts in flight, so its rate follows its latency. The insert p50 went from
11.5 ms to 20.6, and the rate from 2,471 to 1,454 a second.

**Nothing of Shoal's changes at the fall.** The compactors' backlog is no segment before it and none
after. Apply lag is zero on every member. No node logged a forced purge or any warning, other than
openraft's notes on messages a deposed leader ignores. europa's Optane writes in 0.02 ms throughout.
Each node's shards spend less cpu after the fall, since they do less work.

**And fio alone does the same, with no Shoal unit up.** fio replayed each host's flushes a second
and bytes a flush in five jobs. 60 to 110 s into its synced writes, each 970 EVO stepped from 0.35 ms
a flush, or 1.2 ms after a partial rest, to between 1.4 and 3.8 ms. After the step neither drive
could keep the rate it was given.

So the defect was the measurement's. M23's rounds measured a cell arm from 10 s in, and so did X16's,
X17's and X18's. Each before window was taken with the drives rested, and each during window that
ran past about 75 s was taken mostly with them loaded. The loaded state is the one a busy node is
in, and X6's record already said so.

## Evidence

**Established by reproducing it on the lab** (`FALL_ONLY=1 sh shoal-spike-scan/results/m23-lab.sh
start`, 2026-11-01, at `917e4939`, the exit's own build). The cell ran truly alone on a fresh
cluster, with no paced stream and no scan, in two arms of 200 s with 180 s of rest between them.
Every host was sampled each second (`m23-fall-sample.sh`: each whole NVMe's counters, the host's cpu,
every node thread's ticks), beside every member's own figures read each second (`x16 stats --json
--watch 1`). Then fio replayed each host's flushes alone. The predictions were written first
([predictions.md](../../../../shoal-spike-scan/results/m23-fall/predictions.md)), and every host was
identical to its snapshot afterwards. The record is
[shoal-spike-scan/results/m23-fall](../../../../shoal-spike-scan/results/m23-fall): its report, a
per-second CSV, the slimmed captures and the host changes.

Medians of seconds 10 to 60 of a leg (*early*) and of seconds 120 to its end (*late*); the step is
where a figure's five-second mean passed twice its seconds 10 to 40 and stayed there 30 s:

| | Arm a, early → late | Arm b, after the rest |
| --- | --- | --- |
| The cell's inserts a second | 2,471 → 1,454 | 2,506 → 1,454 |
| Insert p50 | 11.5 → 20.6 ms | 11.6 → 20.9 ms |
| The fall | 76 s in, 186,812 inserts | 81 s in, 201,771 inserts |
| titan's 970 EVO, ms a flush | 0.82 → 2.83, stepped 75 s in | 0.81 → 2.79, stepped 78 s in |
| hyperion's 970 EVO, ms a flush | 0.82 → 2.82, stepped 76 s in | 0.81 → 2.80, stepped 80 s in |
| titan's flushes a second | 593 → 324 | 582 → 321 |
| titan's ms a write | 0.80 → 1.93 | 0.81 → 1.93 |
| titan's WAL syncs a second, and their ms | 592 → 326; 6.11 → 13.15 | 592 → 322; 6.13 → 13.21 |
| titan's WAL appends a sync | 3.96 → 4.35 | 4.15 → 4.35 |
| Compacting segments, apply lag, every member | 0 and 0 → 0 and 0 | 0 and 0 → 0 and 0 |
| titan's shards' cpu, cores | 2.55 → 1.52 | 2.64 → 1.57 |
| europa's Optane, writes a second and ms a write | 9,203 → 5,597, 0.02 → 0.02 | 9,203 → 5,592, 0.02 → 0.02 |

Arm a's seconds around its fall: titan's flush moves first, and the inserts follow it within two
seconds.

| Second | Inserts | p50 ms | titan ms a flush | titan flushes | hyperion ms a flush |
| --- | --- | --- | --- | --- | --- |
| 74 | 2,469 | 11.4 | 0.85 | 574 | 0.84 |
| 75 | 2,494 | 11.4 | 0.78 | 591 | 0.80 |
| 76 | 2,419 | 11.6 | 1.18 | 503 | 0.91 |
| 77 | 2,022 | 13.8 | 2.59 | 324 | 1.15 |
| 78 | 1,537 | 18.4 | 2.71 | 339 | 2.22 |
| 80 | 1,466 | 20.6 | 2.93 | 318 | 2.39 |

fio, with every Shoal unit down: five jobs a host, each writing and syncing its share of the first
arm's flushes (titan 592 a second of 28 KiB, hyperion 583 of 24 KiB):

| | fio a, 15 s after the cluster | fio b, after 180 s of rest |
| --- | --- | --- |
| titan, ms a flush | 0.35 → 1.40, stepped 60 s in | 1.16 → 3.80, stepped 96 s in |
| hyperion, ms a flush | 0.33 → 3.70, stepped 64 s in | 1.20 → 3.75, stepped 110 s in |
| titan's flushes a second, of 595 asked | 595 → 370 | 595 → 516 |
| hyperion's, of 585 asked | 585 → 308 | 585 → 308 |

fio's rested flush is faster than the node's, 0.35 ms against 0.82; why was not measured. Its loaded
flush is the node's or slower. How long a drive takes to rest is not simple either. 15 s after the
second arm, fio a found both drives rested. 180 s after fio a, fio b found them only partly rested, at
1.2 ms, and they stepped later, 96 and 110 s in. The node's second arm began at 0.81 ms after the
same 180 s.

**And by reading the older captures**: the table under *Symptom*, and each scan window split at its
fall (`m23-fall-report.py falls`, the median of the cell's per-second insert p99). Beside a scan of
`Big` in X16, the cell's per-second insert p99 was 22 to 25 ms before the scan, 25 to 27 ms during it
before the fall, and 41 to 46 ms during it after the fall. The scan's own cost in one state of the
drive was about a tenth. The fall was the rest of X16's 1.60.

## The fix

**The rounds again warm every cell arm 100 s before it measures**, decided with the user on
2026-11-01. `m23-lab.sh`'s `TIMES` and `CONTROL` warm for 100 s, not 10, so a capture's `none` arm, its
scan arm and the control all start measuring in the drives' loaded state. Their before, during and
after windows are then all taken in one state. `--event-at` is a share of the measured time, so the
scan is still asked 18 s into the measured window. The `QUICK` values stay short.

**The report reads the measured window.** `m23-report.py`'s `insert_fall` takes its median from the
measured window's first 50 s and looks for a fall past them. It reads no fall in an arm's last few
seconds, which every arm's wind-down dips in. A fall inside a measured window now means its windows
straddle a change of state, and its round is not read for W7 until it is explained. On the first
exit's and the check round's captures the report is unchanged, byte for byte.

**The records whose insert figures straddled the step say so where they stand**: X16's, X17's, X20's
beside X16, Q34's rows, the isolation page and M23's exit. Each old figure is kept, struck through,
with the reading in one state beside it.

**The run's tools stay**: `m23-fall-sample.sh`, the `FALL_ONLY=1` mode and `m23-fall-report.py`,
whose `falls` reads any captures for an arm whose windows straddle the fall, and says when a fall
came in an arm's warmup.

**Checked on the lab**, with one round of the exit's own under the warmup, at `917e4939`
([shoal-spike-scan/results/m23-warm](../../../../shoal-spike-scan/results/m23-warm)). The cell's
inserts fell inside the warmup, 78 s into the arm beside `Big` and 67 s into the arm beside
`Keyword`. Neither measured window held a fall, and both held 1,260 to 1,350 inserts a second. W7's
insert p99, read over the whole arm, went 43.9 to 43.8 ms beside `Big` and 47.3 to 46.4 beside
`Keyword`. The feed never wrapped, at 536,622 inserts in the longest arm, and the members' peak
resident memory was 0.65 of a node's ([the record](../../future-work/scans/m23-exit.md#the-cells-arms-warmed-past-the-drives-change)).

## Alternatives rejected

- **Keep the 10 s warmup and move the scan past the fall**, with the event at about 100 s and longer
  arms. Each arm then carries the step inside its own measured span, and every reader has to know
  where to cut it.
- **Rest the drives before every window, to measure in the rested state.** The rested state is a
  best case a busy node never sees, and any arm longer than about 75 s leaves it anyway.
- **Load the drives with fio before each capture.** That is another program's writes on the hosts
  before every arm. The cell's own warmup loads them the same way, and its rows are what the arm
  measures.
- **Treat it as Shoal's and change the write path**, with one flush a device or a longer commit
  delay. The fall is the drive's, fio shows it without Shoal, and the measurement was what was
  wrong. Whether a node should flush once a device in the loaded state is
  [O151](../optimizations.md#o151-a-zen1-nodes-wal-syncs-are-bound-by-its-970-evos-flushes-once-the-drive-is-loaded),
  after M23. [F60](../../features/shared-wal-flush.md) built one and withdrew it when the devices
  were not bound by the syncs.
- **Discard the older records.** Their windows before the fall are still sound, so each claim is
  corrected where it stands instead.

## Invariants to uphold

- **On the lab's 970 EVOs, a comparison that waits on a sync is taken within one state of the
  drive.** A cell arm measures only past 100 s of its own synced writes. A drive can rest within
  seconds: after a restart and its settle, after scans alone beside the paced stream's 200 inserts a
  second, and 15 s after the second arm. So every arm needs its own warmup, not just the first.
- **A fall inside a measured window stops the reading.** `insert_fall` reads the measured window;
  a round with a fall there is explained before its clause is read.
- **A flush figure is read on titan's and hyperion's whole NVMe.** europa's Optane takes no flush,
  and an LV or a partition counts none ([Resolved #320](flush-script-device-names.md)).
- **The step's second depends on the drive's history**: 67 to 90 s for the node's arms in every
  capture so far, and 60 to 110 s for fio. A warmup shorter than the latest step seen is a guess, so
  every round's report says whether a fall came inside a measured window.

## Still open

- **The rounds again themselves**, eight rounds and a control under the warmup, are M23's. The
  check above is one round. The earliest fall the warmup has met is 67 s into an arm, and fio's
  latest was 110 s, after a drive's partial rest. A round whose fall shows inside a measured window
  says so in its report.
- **Whether a node should flush once a device in the loaded state**:
  [O151](../optimizations.md#o151-a-zen1-nodes-wal-syncs-are-bound-by-its-970-evos-flushes-once-the-drive-is-loaded),
  optional, after M23.
- **What the scans' decisions rested on.** Q34's reading was that "the insert tail is the scan's
  reads beside the WAL's flushes on one device", and it came from X16's 1.60. That reading is now
  about a tenth of the scan's own cost, and the rest is the drive's change of state. X18's SSD
  figures, from which F128's share was chosen, are not affected: every arm there ended by 80 s.
  Nothing decided is re-decided here. The user reads which decisions change.
- **Found by its planning and its run**, and filed:
  - [item 352](../known-issues.md#352-a-shard-asks-every-groups-core-whether-it-runs-after-every-batch-it-applies),
    a shard probing every group's core after every batch it applies;
  - [O150](../optimizations.md#o150-a-merge-reads-the-archived-record-of-every-partition-an-insert-replaces-whole),
    a merge reading the archived row an insert replaces whole;
  - O151.

## Tests

| Test | Where | What breaks if this is reverted |
| --- | --- | --- |
| None in the workspace | | No engine code changed. The fault was a procedure's, as [item 218](lab-core-layout.md)'s was |
| The check round under the warmup | `ROUNDS=1 sh shoal-spike-scan/results/m23-lab.sh start` | Back at `--warmup 10`, `m23-report.py` reads a fall in every cell arm longer than about 90 s |
| `m23-fall-report.py falls <captures>` | `shoal-spike-scan/results/` | Prints where each arm's inserts fell beside its event's marks, so any later capture is checked for a window across the step |

## Related

- [X6, the device store on SSDs](../../object-storage/device-store-ssd.md#two-things-about-the-labs-970-evos),
  which found the two states
- [F98, a commit delay bounded by the sync](../../features/commit-delay-bounded-by-the-sync.md), which
  reads the last batch's time and so follows the drive's state
- [F60, one flush a device (withdrawn)](../../features/shared-wal-flush.md)
- [M23's exit](../../future-work/scans/m23-exit.md), whose cell found it, and
  [Finishing M23](../../future-work/scans/whats-left-todo.md#finishing-m23)
- [Item 218](lab-core-layout.md), the last fault in a lab procedure
