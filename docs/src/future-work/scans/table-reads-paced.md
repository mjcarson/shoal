# X18. Table reads paced by idle time

**Reported 2026-10-30.** This is the record of spike [X18](spikes.md#x18-table-reads-paced-by-idle-time):
which ceiling and trickle of [F119](../../features/scan-pacing.md)'s pace let a scan read a table's
archives beside the reference cell's shape without moving its read or insert p99 past
[W7's budget](isolation.md#the-budget), on the SSDs the lab's tables live on and on a disk holding a
table's archives. It ran on X17's walk, sixteen runs in flight across gaps of 4 KiB over merges in
key order, which unpaced reads 3.6 to 6.6 times a device's default ceiling
([X17's record](page-rebuilt.md#what-it-says-for-the-design)), so every ceiling tried here binds.

Four rounds a layout, then a supplement of four rounds on the SSDs, every host identical to its
snapshot after. The answers:

- **No pace tried holds the cell's read p99 on the SSDs.** Beside a whole scan of `Big` at 32 MiB a
  second a device, the lowest ceiling, the read p99 rose 1.14 times [1.07, 1.19], past W7's 1.10; at
  64 MiB 1.19 times, at 128 MiB 1.42 to 1.65 times, at 256 MiB 1.41 to 1.60 times. The read tail
  grows with the scan's rate.
- **The insert p99 holds at 32 MiB a second a device**, 1.07 times [1.06, 1.09], and at 64 MiB with
  idle gating off at the line, 1.11 times [1.07, 1.12]; at 128 MiB and past it, 1.14 to 1.38 times.
  So the default ceiling, 100 MiB, is too high for the 970 EVO the WAL flushes to: a lower ceiling
  holds the write p99 where the default does not, so either the default drops or the idle check
  sees flushes, the two remedies the spike page named before the run.
- **Idle gating changed nothing measurable**, because it never engaged: with the cell's rows warm, a
  device has a foreground read in flight too rarely for the trickle to bind, and a scan read the same
  bytes a second with its trickle at 1, 2 or 4 MiB or at its ceiling, its cell's ratios overlapping
  each way. So the ceiling is the pace, not a safety.
- **In device time** a MiB a scan reads costs a 970 EVO 2.3 ms, europa's Optane 0.65 ms and a disk
  about 125 ms: the default ceiling is nearly a quarter of a 970 EVO's time, a fifteenth of the
  Optane's, and twelve times a disk's.
- **On a disk no ceiling in bytes binds.** A scan alone keeps a disk busy most or all of the time at
  5 to 10 MiB a second, and beside the cell it read 1 to 2 MiB a second a disk, under every trickle; the cell's
  tail on the disk layout was the disk's before any scan, 45 ms to a second at p99, and a scan moved
  it up to six times, its ratio as noisy as the tail before it. A disk's pace has to be in device
  time: a trickle of 2 MiB a second is a quarter of a disk's.
- **A shallower walk moved the read tail less, as a hint**: in the supplement, at the same scan
  rate, two runs in flight a page moved the cell's read p99 1.05 times against sixteen's 1.12 at
  32 MiB a second, and 1.11 against 1.19 at 64, but each pair's ranges overlap, so by the lab's rule
  it is not a result. At 32 MiB and two runs in flight the medians of both p99s held W7's budget,
  the read p99's range not quite: the pace bounds a scan's bytes a second, not how many of its reads
  wait ahead of a foreground read on a device, and that is the next thing to measure.

## The question

The spike page's question: which ceiling and trickle let a scan read a table's archives without
moving the cell's read or write p99 past the budget, on each kind of device the lab has, against a
walk fast enough to reach its ceiling, with the ceiling stated in device time as well as bytes? It
feeds [Q34](contract.md#questions-to-answer) (what a scan may cost the foreground, and what pace holds
it) and [Q37](contract.md#questions-to-answer) (whether scan work stays on the table shards).

**What would have changed the design**, named on the spike page before the run: if no ceiling holds
the write p99 on the 970 EVO, where a table's archives share the device with its WAL, the idle check
has to see flushes too, or the default ceiling drops; if no pace holds the Optane's budget, the
Optane's bound is set the way M16 set one; if idle gating alone, without a ceiling, holds the budget,
the ceiling is a safety rather than the pace.

## How it was judged

**[W7's budget](isolation.md#the-budget)**, as X16 judged it: beside one full scan, the cell's read
p99 and its insert p99 within `max(1.10 × alone, alone + 0.1 ms)` at the median over rounds, where
*alone* is the scan event's *before* window in the same arm. The cell is the reference cell's shape
(`rw50/b1`: half reads, half inserts, a kibibyte row, a bundle of one) on `Cell`, its first 100,000
rows preloaded, warmed by the arm before it (`rw50/b1/none`), and a whole scan of `Big` asked 30% of
the way into the arm.

**A device's share at its ceiling** is derived, since a capture keeps a scan's bytes by member and
not the pace's own counts: the walk reads past every ceiling tried, so a device's scan reads at its
ceiling while no foreground read is in flight on it and at its trickle while one is, and its rate in
the scan's window is `share × ceiling + (1 − share) × trickle`. **A ceiling in device time** is the
device's busy milliseconds a MiB a scan alone read at the widest pace, times the ceiling.

**The lab's rule for a difference**: a difference is a result only where two arms' rounds give
ranges that do not overlap. Four rounds a layout, the layouts alternating which goes first.

## What was run

### The cluster and the build

`x18`, X17's side cluster under another name: three nodes on the tmdb cluster's hosts and cores at
4 GiB a node, bootstrapped fresh for every layout of every round and destroyed after it; the tmdb
node stopped, every host's maintenance timers held and its governor at `performance` for the
whole window, all put back after ([x18-host-changes.txt](../../../../shoal-spike-scan/results/x18-host-changes.txt);
every host identical to its snapshot afterwards). The build is X17's, branch `x17-spike` at
`fe041176`, node sha256 `36129ddcd8605f8c...`, its switches set on every node to sixteen reads in
flight, runs across 4 KiB, one operation a record and merges in key order. The rounds ran from
13:57 to 18:17 UTC and the supplement from 18:21 to 19:30; afterwards the tmdb cluster's
units were active on all three hosts and every governor was back.

### The two layouts

| Layout | The WAL's root | The archives' root | `Big` | A whole scan |
| --- | --- | --- | --- | --- |
| `ssd` | the host's SSD | the same SSD: on titan and hyperion the 970 EVO the WAL flushes to, on europa the Optane | 3,000,000 partitions | 35 to 42 s at 32 MiB a second a device, 6 to 8 s at 256 |
| `disk` | the host's SSD | the host's disk at `/hdd` (XFS): a WD140EDFZ on titan and hyperion, a WD6001FZWX on europa | 150,000 partitions, a twentieth | 17 to 80 s at every pace |

`Cell`'s archives are on the archives' root too, so on the disk layout a cell's read that misses
its rows' memory reads the disk the scan reads.

### The paces

| Pace | Ceiling, a second a device | Trickle, a second a device | What it asks |
| --- | --- | --- | --- |
| `c32t2` | 32 MiB | 2 MiB | A low ceiling |
| `c64t2` | 64 MiB | 2 MiB | |
| `c128t2` | 128 MiB | 2 MiB | About the default, 100 MiB |
| `c256t2` | 256 MiB | 2 MiB | A high ceiling |
| `c64t64` | 64 MiB | 64 MiB | Idle gating off: the trickle at the ceiling |
| `c256t256` | 256 MiB | 256 MiB | Idle gating off, high |
| `c128t1` | 128 MiB | 1 MiB | A lower trickle |
| `c128t4` | 128 MiB | 4 MiB | A higher trickle |

Each pace is set through the inventory's `scans:` and a reconfigure, which restarts every node, so
each arm's rows start from their archives. Every pace's capture is `rw50/b1/none` then
`rw50/b1/scan`, each 10 s of warmup and 60 s measured, the scan event's clock held for the scan up to
five minutes. Each layout also scans `Big` alone once a round at `c256t256`, which states a ceiling
in device time. The script is [x18-lab.sh](../../../../shoal-spike-scan/results/x18-lab.sh) and the
tables come from [x18-report.py](../../../../shoal-spike-scan/results/x18-report.py).

## The tables

Medians over the rounds, the range beside each in brackets. The whole set is
[x18-report.md](../../../../shoal-spike-scan/results/x18-report.md), from the slimmed captures in
[shoal-spike-scan/results/x18](../../../../shoal-spike-scan/results/x18).

### The cell beside a scan, on the SSDs

`rw50/b1/scan`, the cell's p99 in the scan's *during* window over its *before* window, and each
device's scan rate during it:

| Pace | Read p99 ms, before to during | Read, during over before | Insert p99 ms, before to during | Insert, during over before | Scan MiB a second: titan, hyperion, europa |
| --- | --- | --- | --- | --- | --- |
| `c32t2` | 5.79 to 6.60 | 1.14 [1.07, 1.19] | 24.7 to 26.5 | 1.07 [1.06, 1.09] | 27.4, 30.3, 19.7 |
| `c64t2` | 5.92 to 7.00 | 1.19 [1.11, 1.27] | 25.2 to 28.1 | 1.12 [1.10, 1.12] | 53.9, 58.8, 38.7 |
| `c64t64` | 5.73 to 6.92 | 1.23 [1.19, 1.27] | 24.4 to 26.7 | 1.11 [1.07, 1.12] | 52.5, 58.0, 37.5 |
| `c128t1` | 5.81 to 7.81 | 1.42 [1.29, 1.58] | 24.3 to 28.2 | 1.19 [1.11, 1.38] | 97.3, 107, 71.3 |
| `c128t2` | 6.03 to 10.0 | 1.65 [1.26, 1.75] | 25.0 to 33.9 | 1.35 [1.13, 1.63] | 99.0, 108, 69.9 |
| `c128t4` | 5.89 to 8.09 | 1.36 [1.24, 1.74] | 24.0 to 28.5 | 1.14 [1.12, 1.36] | 96.9, 103, 71.3 |
| `c256t2` | 5.79 to 8.58 | 1.60 [1.38, 2.12] | 24.1 to 33.0 | 1.38 [1.31, 1.62] | 152, 167, 114 |
| `c256t256` | 5.96 to 8.11 | 1.41 [1.31, 1.88] | 24.2 to 30.3 | 1.26 [1.23, 1.43] | 152, 168, 118 |

W7's budget is `max(1.10 × before, before + 0.1 ms)`: about 1.10 times for both p99s here. The read
clause holds at no pace; the insert clause at `c32t2` and, at the line, `c64t64`. Every pace's whole
scan finished, from 35 to 42 s at 32 MiB a second to 6 to 8 s at 256, so the faster paces' *during*
windows are short. Past 128 MiB a device the walk beside the cell, two pages a shard, read about 160
MiB a second, so 256's arms ran at the walk's rate, not their ceiling's.

### The trickle

The pairs of paces that differ in their trickle alone, each device's scan MiB a second during the
cell:

| Paces | titan | hyperion | europa |
| --- | --- | --- | --- |
| `c64t2`, `c64t64` | 53.9 [48.0, 58.7], 52.5 [48.0, 58.3] | 58.8 [52.0, 60.7], 58.0 [52.0, 58.9] | 38.7 [28.6, 56.9], 37.5 [28.6, 56.9] |
| `c128t1`, `c128t2`, `c128t4` | 97.3, 99.0, 96.9 | 107, 108, 103 | 71.3, 69.9, 71.3 |
| `c256t2`, `c256t256` | 152, 152 | 167, 168 | 114, 118 |

A trickle binds only while a foreground read is in flight on its device, and these rates say it
almost never did: the cell's rows were warm, so its reads seldom reached a device. Europa read least
since it leads most groups and the driver asks followers first.

### A scan alone at the widest pace, by device

`scan/Big/none` at `c256t256`, each device's busy time over the MiB its member scanned, warmup and
measurement:

| Device | Scan MiB a second | Busy, a share of the run | Device ms a MiB | The default ceiling, 100 MiB, in device ms a second |
| --- | --- | --- | --- | --- |
| titan's 970 EVO | 204 to 246 | 0.51 to 0.54 | 2.35 [2.21, 2.57] | 235 |
| hyperion's 970 EVO | 219 to 246 | 0.48 to 0.59 | 2.25 [2.18, 2.38] | 225 |
| europa's Optane | 126 to 238 | 0.08 to 0.16 | 0.65 [0.64, 0.66] | 65 |
| titan's WD140EDFZ | 6.5 to 10.4 | about all of it | 118 [105, 148] | none: the disk reads 8 MiB a second at most |
| hyperion's WD140EDFZ | 6.8 to 9.6 | about all of it | 128 [102, 153] | none |
| europa's WD6001FZWX | 4.7 to 6.1 | 0.68 to 0.79 | 132 [125, 154] | none |

A MiB is about 93 device reads on the SSDs, runs of eleven partitions, and 210 on the disks, about
5 KiB a read, which this run did not explain.

### On the disks

The archives' root on each host's disk, `Big` at a twentieth, the WAL on the SSD:

| Pace | Read p99 ms, before to during | Insert p99 ms, before to during | Scan MiB a second a disk, titan |
| --- | --- | --- | --- |
| `c32t2` | 44.8 to 196 | 65.4 to 216 | 1.85 |
| `c64t2` | 1,080 to 231 | 2,460 to 279 | 1.22 |
| `c128t2` | 258 to 293 | 324 to 286 | 1.33 |
| `c256t2` | 101 to 296 | 82.8 to 303 | 1.51 |
| `c256t256` | 68.8 to 597 | 50.0 to 226 | 2.26 |

The cell's tail here is the disk's whatever the scan does: its rows' archives are on the disk, its
inserts' merges write it, and its *before* window ranged from 45 ms to two seconds at p99. Its
ratios are not a measure of the pace. A scan beside it read 1 to 2 MiB a second a disk at every
pace, under even the trickle.

### The depth supplement

Rounds five to eight, the SSDs alone, the cell beside a scan at two paces with two runs in flight a
page and with sixteen, the depths' order alternating by round:

| Figure | `c32t2`, two | `c32t2`, sixteen | `c64t2`, two | `c64t2`, sixteen |
| --- | --- | --- | --- | --- |
| Read p99 ms, before to during | 5.75 to 6.05 | 5.75 to 6.50 | 5.54 to 6.16 | 6.03 to 7.20 |
| Read, during over before | 1.05 [1.03, 1.17] | 1.12 [1.06, 1.20] | 1.11 [1.10, 1.22] | 1.19 [1.17, 1.20] |
| Insert p99 ms, before to during | 24.4 to 25.4 | 23.8 to 25.9 | 23.3 to 25.8 | 24.4 to 27.8 |
| Insert, during over before | 1.05 [1.02, 1.08] | 1.07 [1.03, 1.15] | 1.12 [1.09, 1.17] | 1.13 [1.09, 1.18] |
| Scan MiB a second, the cluster | 76.5 [69.2, 80.4] | 77.4 [72.6, 80.4] | 145 [135, 149] | 145 [142, 157] |

Sixteen in flight here repeats the four rounds' `c32t2` and `c64t2`, 1.12 and 1.19 against 1.14 and
1.19. Two in flight reads the same bytes a second, the pace binding either way.

## What it says for the design

- **The ceiling is the pace, and the default is too high for the lab's SSDs.** The cell's tails grow
  with a scan's bytes a second, and the trickle never engaged beside a warm foreground, so the spike
  page's rule applies: no ceiling near the default holds the write p99 on the 970 EVO, so the
  default drops, or the idle check sees flushes. On the evidence here the drop is to about 32 MiB a
  second a device, 75 ms of a 970 EVO's time a second, with a scan's runs in flight a page kept to
  two; a scan of `Big` then takes about forty seconds on the lab, against twelve at 128. ~~Whether
  Shoal ships that default or a looser budget is M23's plan's to decide with the user.~~ M23's plan
  decided with the user on 2026-10-31: a share of each device's busy time, 75 ms a second, under the
  byte ceiling, with sixteen runs in flight a page and a device-wide bound on a scan's reads
  ([F128](../../features/device-time-pace.md)), the looser budget the fallback if the exit misses the read clause.
- **A ceiling in bytes is a different pace on every device.** A MiB is 0.65 ms of the Optane's time,
  2.3 ms of a 970 EVO's and about 125 ms of a disk's, so one byte ceiling gives a 970 EVO three and
  a half times the Optane's load and a disk two hundred times. W7's device pace wants a ceiling in
  device time, a share of a device's busy time a second, with bytes as a ceiling's unit only where
  the device's cost a byte is known.
- **Idle gating as built sees too little.** It engages on a foreground read in flight on the device,
  which a warm foreground seldom has, and it sees neither the WAL's flushes nor the scan's own reads
  already queued. Its trickle mattered nowhere here; it may for a cold foreground, which this did not
  run.
- **Depth is the next lever to measure.** At one pace, fewer of a scan's reads in flight moved the
  read tail less in every pair's medians, the ranges overlapping: a device-wide bound on a scan's
  reads in flight, as the object store's background requests wait for a slice's foreground
  ([S13](../../object-storage/isolation.md#io-on-a-slice)), is the candidate.
- **A disk holding a table's archives is the foreground's problem first**: the cell's tail there was
  tens of milliseconds to seconds before any scan, from its own cold reads and its merges' writes,
  and no byte pace bound a scan's reads on it.

## Limitations

- **A device's share at its ceiling is derived from its rate**, not counted: the pace counts its idle
  and trickled bytes (`TablePace`) but no report carries them in this build. Since
  [F123](../../features/walk-and-scan-counters.md) a node's figures carry them, and since F128 the
  scans' busy time on each device.
- **The cell's reads are mostly resident**, warmed by the arm before; a foreground of cold reads
  holds the trickle longer, and was not run.
- **The cell's figures are the cluster's**, from its client: a read at `One` is answered by any
  holder, so the Optane's budget cannot be told apart from the 970 EVOs'.
- **A whole scan at the faster paces is short**, six to twelve seconds on the SSDs, so those arms'
  *during* windows hold fewer of the cell's operations than the slow paces'.
- **The disks' write cache was as each host has it**, never set for this run; X7 and X12 measured a
  disk with it off.
- **The depth supplement ran two paces and two depths**; a bound on a device's scan reads in flight,
  as opposed to a page's, was not built. Since [F128](../../features/device-time-pace.md) it is, four a device, judged at M23's
  exit.

## Related

- [X18's spike page entry](spikes.md#x18-table-reads-paced-by-idle-time)
- [F119, a scan's reads paced by their device's idle time](../../features/scan-pacing.md), the pace
  measured
- [X17's reads half](page-rebuilt.md), whose walk this ran on, and [X16](walk-beside-the-foreground.md),
  whose cell and budget it judged by
- [W7, not slowing other queries](isolation.md), and [W12's budgets](performance.md#the-budgets)
