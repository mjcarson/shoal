# 359. A whole scan of `Big` seemed to lift the cell's read p99 past W7's tenth: the cell's own climb, not the scan's

## Symptom

[M23's exit](../../future-work/scans/m23-exit.md) judges the cell beside a whole scan of `Big` by W7's
budget: the cell's read p99 *during* the scan within `max(1.10 × before, before + 0.1 ms)` of its p99
*before* it, in the same arm. On the exit's rounds again (2026-11-03, eight rounds at `b3a4d7ef`) it went
from 5.11 ms to 5.81, 1.14 times [1.10, 1.17], past the budget in seven rounds of eight. The first exit
had read 1.07. Beside a scan of `Keyword` the clause held, 0.96.

Four control rounds ran beside them: the cell alone in a `Keyword` capture's place, cut at `Big`'s scan
marks. They read 0.94 a second at a time, disjoint from the scan arms' 1.14, so the rise looked like the
scan's.

Every run of the exit's rounds since the first said when the ratio moved. On drives in their loaded
state, about 1,400 of the cell's inserts a second, it rose between `9a0dc4c8` and `345e17dd`:

| Run | Build | Rounds | Read p99 ms before | During | During over before |
| --- | --- | --- | --- | --- | --- |
| The warm check | `917e4939` | 1 | 5.69 | 6.06 | 1.07 |
| Item 350's attribution | `263a17fc` | 4 | 5.72 | 6.12 | 1.04 [0.97, 1.14] |
| Item 350's confirmation | `72c14316` | 4 | 5.78 | 5.93 | 1.03 [0.95, 1.12] |
| Item 353's attribution | `9a0dc4c8` | 6 | 5.63 | 5.82 | 1.04 [0.95, 1.08] |
| Item 353's allocations | `345e17dd` | 2 | 5.09 | 5.78 | 1.14 [1.12, 1.15] |
| Item 353's confirmation | `345e17dd` | 6 | 5.23 | 5.78 | 1.10 [1.05, 1.17] |
| The rounds again | `b3a4d7ef` | 8 | 5.11 | 5.81 | 1.14 [1.10, 1.17] |

The cell's p99 beside the scan stayed at 5.8 to 6.1 ms. Before the scan it fell about half a
millisecond.

## Cause

**The cell's read tail is a disk read's tail, and at the scan arm's place in a capture it climbs with or
without a scan.** A read at One is served by the member its bundle reaches, and every member times its
own gets. Europa's get p99 is 0.3 ms throughout. Titan's and hyperion's is 5 to 7 ms, with a median of
0.2: the tail is their gets that reach their disk. A read on a loaded 970 EVO, behind the WAL's writes
and its 2.8 ms flushes, takes 4.8 to 6.5 ms at p99. So the cell's read p99 is decided by how often its
gets reach their disk, not by how long each waits there.

How often they do moves over a capture. The scan arm is a `Big` capture's fourth arm, three arms and
about ten minutes past its cluster's restart. Over the seconds a whole scan covers there, the cell's p99
rises about as far with a scan reading almost nothing as with a whole scan
([the twin](#evidence)). Since `345e17dd` the cell has read faster before that point, and no faster
after it, so the rise crossed W7's tenth. The scan's reads load the drives, as their counters show, and
the cell's reads do not notice them.

What the earlier controls showed came from their state, not from the scan. The rounds again's control
followed a restart, where the cell's reads fall as its rows come back into memory. The replay's
followed a capture of its own, where the cell's gets reached their disk only after about 160 s, and
it climbed further than the scan arm did.

## Evidence

**Established by reproducing it on the lab**, three runs at `b3a4d7ef`, the cell warmed 100 s, the nodes
at 6 GiB, every host identical to its snapshot after each run. Each run read every member's own figures
each second, and sampled every host's disk and node threads each second. Each run's predictions were
written first ([predictions.md](../../../../shoal-spike-scan/results/m23-i359/predictions.md)). Two of
the three sets were wrong: they expected the drive's queue, then a twin partly the scan's.

**Item 359's run** ([results/m23-i359](../../../../shoal-spike-scan/results/m23-i359)), the Big capture
alone, its sides the defaults and two scan reads a device:
- It showed that the slow reads are the Zen1 members' gets that reach their disk, and that europa's
  gets did not move beside the scan, though its shards handed out half a Zen1 member's partitions.
- It stopped after a round of each, by the wrap guard. Two reads a device cut the scan's rate
  ninefold, so its loop work fell as far as its reads, and the two sides could not tell the drive
  from the loop.

**The replay** ([results/m23-i359r](../../../../shoal-spike-scan/results/m23-i359r)), four rounds ABBA of
the cell alone after a `Big` capture's first three arms. In two of them, fio on titan and hyperion
replayed a scan's reads of their 970 EVOs from about where a scan is asked. It held 583 reads a second
of 9.3 KiB, Poisson spaced, up to sixteen in flight:

| Figure | fio, rounds 1 and 4 | Nothing, rounds 2 and 3 |
| --- | --- | --- |
| A Zen1 disk's reads in flight during | 0.63 to 0.81 | 0.11 to 0.14 |
| The cell's read p99 a second during, ms | 5.92, 6.24 | 5.93, 6.14 |
| The same, during over before | 1.38, 1.65 | 1.51, 1.58 |

fio put as many reads in flight as a scan (0.79 to 0.83) and cost the cell nothing, so it is not the
drive's queue. And with nothing beside it, the cell climbed from about 4 ms to about 6. It sat at 3.9
to 4.1 while its gets did not reach their disk, and jumped once they did, about 160 s in, with no
segment compacting.

**The twin** ([results/m23-i359t](../../../../shoal-spike-scan/results/m23-i359t)), six rounds ABBAAB of
the Big capture alone. In rounds 2, 3 and 6 the scan arm's scan was paced to `device_time: 1ms`, a
seventy-fifth of the default share, and its event cut at 225 s. So its windows sit where the default
scan's do, in the same arm of the same capture:

| Figure | The defaults, rounds 1, 4, 5 | The twin, rounds 2, 3, 6 | Ranges |
| --- | --- | --- | --- |
| The scan's partitions a second | 14,100 [12,100, 14,600] | 381 [350, 382] | disjoint |
| A Zen1 disk's reads in flight during | 0.80 [0.77, 0.83] | 0.10 to 0.12 | disjoint |
| A Zen1 disk's MiB read a second during | 6.4 to 6.5 | 0.24 to 0.28 | disjoint |
| The cell's read p99, during over before (W7) | 1.14 [1.05, 1.15] | 1.11 [1.06, 1.21] | overlap |
| The same a second at a time | 1.09 [1.03, 1.14] | 1.07 [1.04, 1.28] | overlap |
| Titan's own get p99 during, ms | 6.27 [6.14, 7.30] | 6.89 [5.96, 7.07] | overlap |
| Hyperion's own get p99 during, ms | 6.02 [5.78, 6.10] | 5.86 [5.62, 5.97] | overlap |
| The Zen1 shards' cores busy, during over before | 1.11 [1.09, 1.13] | 1.08 to 1.09 | overlap |

A scan reading a thirty-eighth as much lifts the cell's reads as far. The one disjoint difference in
the cell's figures is europa's gets, 0.25 ms against 0.29 at p99, far below anything the client's p99
shows.

## The fix

**None in the engine: the scan was not costing the cell's reads.** M23's read clause beside `Big` is
judged against the twin of its own arm, decided with the user on 2026-11-04
([the decisions](../../future-work/scans/m23-exit.md#decided-with-the-user-2026-11-04)): a scan of `Big`
adds nothing the twin can show, so the clause holds.

**The run's tools stay** in `m23-lab.sh`:
- `BIG_ONLY=1`, a round's Big capture alone beside per-second samples and each member's own figures,
  with `SCANS_ALT`, `ALT_ROUNDS` and `ALT_EVENT_TIMEOUT` for alternate rounds;
- `REPLAY_ONLY=1`, fio replaying a scan's reads beside the cell alone;
- `m23-i359-report.py`, which reads both kinds of round by member, disk and shard.

## Alternatives rejected

- **Decision 1(c) of M23's plan, W7's read clause restated for a scan** at 1.25 times or as an absolute
  bound. It was the fallback for a scan that costs the cell's reads, and this one does not.
- **A smaller share by default.** It would cut the scan's rate for nothing: a scan reading a
  seventy-fifth as much leaves the cell's reads where a whole scan does.
- **Fewer scan reads a device.** Two collapsed a scan of `Big` ninefold
  ([O155](../optimizations.md#o155-two-scan-reads-a-device-collapse-a-scans-rate-ninefold)), again for a
  cost the scan was not paying.
- **The rounds again's control as the comparison.** It followed a restart, where the cell's reads fall
  as its rows come back. Nor the replay's control, which followed a capture of its own and climbed
  further than the scan arm. A control that is not the same arm of the same capture reads the cell in
  another state.
- **Telling the two builds apart first.** That would say which of the purge delay and a capture's
  single allocations made the cell faster before the climb. It would not show a scan's cost.

## Invariants to uphold

- **A scan's cost to the foreground is judged against a twin of its own arm**: the same capture, the
  same arm, a scan paced to almost nothing, its event cut where a whole scan's ends. A control after
  a restart or in a capture of its own reads the cell in another state.
- **On the lab's loaded 970 EVOs, the cell's read p99 measures how often its gets reach the disk.** A
  read there takes about 5 to 6.5 ms at p99. A change that moves how many gets reach the disk moves
  the tail, whatever beside the cell is measured.
- **Each member's own get figures say where a slow read was served.** A capture keeps only the slowest
  member's p99. `stats --json --watch 1` beside a capture keeps each member's.

## Still open

- **Why the cell's gets begin reaching their disk partway into a capture.** No segment was compacting
  then, and the nodes had memory to spare. It is the cell's, or the bench's choice of keys, not a
  scan's.
- **Why the cell reads faster before that point since `345e17dd`.** `0de7f3b5`'s purge delay of 100 ms
  and `345e17dd`'s single allocations of a capture both came between the runs, and no run on loaded
  drives tells them apart.
- **Two scan reads a device collapse a scan's rate ninefold**: [O155](../optimizations.md#o155-two-scan-reads-a-device-collapse-a-scans-rate-ninefold),
  optional, after M23.

## Tests

| Test | Where | What breaks if this is reverted |
| --- | --- | --- |
| None in the workspace | | No engine code changed: the scan was not costing the cell's reads |
| The twin | `BIG_ONLY=1 SCANS_ALT="device_time: 1ms" ALT_EVENT_TIMEOUT=225 ALT_ROUNDS="2 3 6" sh shoal-spike-scan/results/m23-lab.sh start` | A read clause judged against a control in another state reads the cell's own climb as the scan's |
| The replay | `REPLAY_ONLY=1 sh shoal-spike-scan/results/m23-lab.sh start` | Without it, the drive's queue cannot be ruled out |
| `m23-i359-report.py` | `shoal-spike-scan/results/` | Reads each member's own gets beside its disk and shards, which a capture alone does not keep |

## Related

- [M23's exit](../../future-work/scans/m23-exit.md), whose rounds again found it, and
  [W7's budget](../../future-work/scans/isolation.md#the-budget)
- [Resolved #351](the-cells-fall-is-the-drives.md), the 970 EVOs' two states, which the rounds again warm
  every arm past
- [Resolved #353](capture-allocations.md), whose builds made the cell faster before the climb
- [F128](../../features/device-time-pace.md), the share the twin paced to a seventy-fifth
