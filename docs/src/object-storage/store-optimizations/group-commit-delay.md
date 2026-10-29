# The lab's group commit delay

| | |
| --- | --- |
| **Entry** | [O61](../../appendix/optimizations.md#o61-a-fast-device-syncs-the-wal-in-batches-too-small-to-fill-a-page), applied before the comparison as a setting, off by default |
| **Priority** | [8 of 9](../store-optimizations.md#in-order) |
| **Status** | ~~not measured: a setting the comparison ran with~~ measured on the SSD leg in both groups (2026-10-20). With no delay, small puts ran no faster, and each NVMe flushed 8 to 18% more an operation ([On the SSDs](#on-the-ssds)). ~~The loop leg, where the comparison found the gap, runs after the disk leg, and F98 is designed on its figures~~ On europa's loopback, where the comparison found the gap (2026-10-21), no delay ran small puts 3.4 times as fast at one in flight and 2.6 times at sixteen, their p50 0.60 ms where it was 2.41, for 17 to 44% more device bytes ([On europa's loopback](#on-europas-loopback)). ~~F98's design is the user's choice with both legs' figures~~ The user chose a delay bounded by the sync (2026-10-21): [F98](../../features/commit-delay-bounded-by-the-sync.md) waits no longer than the last batch took to write and sync, ~~and its before and after on both legs follows~~ measured on both legs (2026-10-22). On europa's loopback 4 KiB puts and the mixture ran 2.2 to 3.2 times as fast as with the lab's delay, within 7% of none, and kept half the device bytes the delay saved at sixteen in flight. On the SSDs no rate moved ([F98, before and after](#f98-before-and-after)) |

## What the comparison found

Every Shoal capture ran with `tmdb_cluster.yaml`'s `wal_commit_delay`: 3 ms on europa and 2 ms on
titan and hyperion, and 3 ms on every node of the loop leg
([each store as deployed](../store-comparison.md#each-store-as-deployed)). Shoal's small puts, which
are rows committed through their tablet groups:

| Arm, three copies | Shoal | Ceph |
| --- | --- | --- |
| Puts, 4 KiB, one in flight, SSDs over 1 GbE | 176 a second | 178, within |
| Puts, 4 KiB, sixteen in flight, SSDs over 1 GbE | 1,215 | 1,310, Shoal behind ≥2.9% |
| Puts, 4 KiB, one in flight, europa's loopback | 469 to 474, p50 2.4 ms, p99 3.9 ms | 962 to 1,007, p50 1.0 ms, p99 1.3 to 1.5 ms |
| Puts, 4 KiB, sixteen in flight, europa's loopback | 3,310 to 3,345, p50 4.9 ms | 6,270 to 6,452, p50 2.4 ms |
| Puts, 4 KiB, 2+1, one in flight, europa's loopback | 470 to 475 | 898 to 961, Shoal behind ≥47% |
| Puts, 4 KiB, 2+1, sixteen in flight, europa's loopback | 3,305 to 3,338 | 5,819 to 5,984, Shoal behind ≥42% |

On one host, where nothing hides a write's own costs, Shoal put small objects at half Ceph's
rate, the same in both groups, since a 4 KiB object is a row whatever its pool. Its put cost the
least cpu of any store there, 0.95 to 1.00 cpu-seconds a thousand against Ceph's 3.1 to 3.6, so a
small put spends its time waiting rather than working.

**Measured again** in the comparison (2026-10-25 and 26, [Shoal measured again](../store-comparison.md#shoal-measured-again)), under
[F98](../../features/commit-delay-bounded-by-the-sync.md) with the inventory's delays as its ceiling:
on europa's loopback 4 KiB puts ran 3.4 times as fast at one in flight as the first run's, at a p50
of 0.58 to 0.68 ms, and lead every store, Ceph by at least 38% to 56%, but SeaweedFS at sixteen in
flight, which is within. On the SSDs and disks they ran 1.02 to 1.06 times as fast, within Ceph on
the SSDs at both depths.

## Why it would help

After each sync a shard's WAL writer waits the delay before it takes its next batch, so appends
that arrive meanwhile join it. A writer that has not synced recently takes the first append at
once. The delay was set for wear: on europa a 3 ms delay halved the device's writes under the
lab's insert bench, 15.6 GB to 7.7 in 30 s, with its cluster's inserts at 18,357 and 18,649 a second
and its p99 at 506 and 520 ms, one run each. O61 states its cost: every write on a node
that delays waits up to the delay longer for that node's sync.

A put's commit waits for two of its three copies' syncs. At one in flight on loopback a put took
2.4 ms at the median and 3.9 ms at the p99, beside a 3 ms delay. Whether a put waits out the delay
depends on whether its shard synced in the last 3 ms, and nothing measured how often it did. So the
delay may be part of the gap to Ceph, or none of it.

## What would show it worked

- **The comparison's small writes with no delay.** `put100` and `rw50` at 4 KiB, one in flight and
  sixteen, on the loop and SSD legs, with `wal_commit_delay` at zero on every node beside captures
  at the lab's values.
  - The delay costs a small put if the zero-delay captures' rates are above the others', disjoint.
  - A delay that waits only while appends are queued, not on every writer that just synced, would
    then be the change, measured the same way.
- **What it saves**, read beside it: each capture's device bytes a byte put, from the counters every
  run keeps. That is what the delay was set to lower.

## On the SSDs

Measured on the lab's SSD leg on 2026-10-20, with three copies (`f98-ssd-rep3`) and at 2+1
(`f98-ssd-ec21`), under `shoal-spike/results/stores-ab/`.
- **The sides.** Both ran one build, F97's commit. One kept the lab's delays, 3 ms on europa and
  2 ms on titan and hyperion. The other set `wal_commit_delay: 0ms` on every node.
- **The captures** ran in the order delays, none, none, delays, each on devices made fresh.
- **Every figure** is the median of eight runs, with the lowest and highest.
- **Nothing was lost.** The check at each capture's end read 1,024 objects back.
- **A 4 KiB object is a row in both groups**, so the two groups measure one path twice. Their
  figures agree.

| Arm | In flight | Group | Delays, ops/s | No delay | Ratio | Puts' p50 ms, delays → none | Puts' p99 ms |
| --- | --- | --- | --- | --- | --- | --- | --- |
| Puts, 4 KiB | 1 | three copies | 180 [155–183] | 170 [152–172] | 0.94, within | 5.30 → 5.73 | 10.3 → 9.14 |
| | | 2+1 | 178 [158–182] | 171 [150–172] | 0.96, within | 5.35 → 5.69 | 10.4 → 9.11 |
| | 16 | three copies | 1,244 [1,219–1,277] | 1,232 [1,190–1,243] | 0.99, within | 12.2 → 12.2 | 26.2 → 27.1 |
| | | 2+1 | 1,233 [1,202–1,269] | 1,235 [1,183–1,254] | 1.00, within | 12.3 → 12.2 | 26.3 → 27.5 |
| Mixture, 4 KiB | 1 | three copies | 323 [315–336] | 314 [308–320] | 0.97, within | **5.28 → 5.62** | **10.0 → 8.58** |
| | | 2+1 | 322 [318–328] | 317 [313–324] | 0.99, within | **5.33 → 5.58** | **10.1 → 8.57** |
| | 16 | three copies | 2,325 [2,251–2,351] | 2,274 [2,249–2,307] | 0.98, within | 11.8 → 11.9 | 25.0 → 26.0 |
| | | 2+1 | 2,280 [2,230–2,334] | 2,299 [2,264–2,319] | 1.01, within | 11.9 → 11.8 | 25.0 → 26.0 |

A figure in bold is apart in every run. What the delay saved:

| Figure | In flight | Three copies, delays → none | 2+1 |
| --- | --- | --- | --- |
| Device bytes written a byte put, puts | 1 | 12.7 → 14.2 | 13.4 → 14.4 |
| | 16 | 10.1 → 11.3 | 10.1 → 11.3 |
| The same, the mixture | 1 | 15.3 → 14.0 | 14.1 → 14.7 |
| | 16 | 10.3 → 11.6 | 10.3 → 11.4 |
| Flushes a put, titan's and hyperion's NVMe, puts | 1 | 1.35 to 1.37 → 1.49 to 1.50 | 1.36 to 1.37 → 1.49 to 1.51 |
| | 16 | 0.42 to 0.43 → 0.50 | 0.43 → 0.48 to 0.49 |
| Flushes an operation, the same, the mixture | 1 | 0.72 → 0.78 to 0.79 | 0.72 → 0.78 |
| | 16 | 0.23 → 0.27 | 0.24 → 0.27 |
| Busy share of the same, every arm | 1 and 16 | 50 to 56% → 62 to 67% | 49 to 56% → 62 to 67% |

How these figures are taken:
- **The device bytes** are every host's device over every byte the arm put. Each host's WAL root
  and pool device share one LV, on europa a loop over a file on its Optane.
- **The flushes and the busy share** are titan's and hyperion's whole NVMe, sampled every second
  beside the runs, since an LV counts no flushes. Europa's Optane was not sampled.

What this says by the page's test:
- **The delay costs no small put its rate on the SSDs.** No arm in either group ran faster with no
  delay. Six of the eight medians are lower, every one within.
- **It moves a put's latency both ways at one in flight.** In the mixture, where a put's figures
  are apart in every run, no delay raised the puts' p50 by 0.25 to 0.34 ms and lowered their p99 by
  1.4 to 1.5 ms. The puts arm alone moved the same way, within. At sixteen in flight neither moved.
  The gets in the three copy mixture at one in flight had a higher p99 with no delay, 1.84 ms
  against 1.69, every run apart.
- **It saves what it was set to save.** With no delay each NVMe flushed 8 to 18% more an
  operation and was 10 to 16 points busier. The device bytes a byte put rose 4 to 13% in every arm
  but the three copy mixture at one in flight, which fell 8%.
- **So appends do arrive within the delay of a sync at one in flight.** At about 180 puts a second
  a put is 5.5 ms from the next, past either delay, yet the delay joined about one sync in ten.
  What joined them, and why fewer flushes gave a lower median, was not traced. X6 found the 970
  EVO's flush slower after synced writes than rested
  ([device store on an SSD](../device-store-ssd.md)), which fits a lower median with fewer flushes.

For F98, the change planned for this page: a delay taken only while appends are queued behaves as no
delay wherever they do not queue. On these figures that is one in flight. There it would trade the
saved flushes and the median for the tail, and at sixteen in flight it would change nothing. So F98
has nothing to win on the SSDs. The loop leg decides whether it is built. There a put took 2.4 ms
at the median beside a 3 ms delay, on a device whose sync is far faster than the 970 EVO's.

## On europa's loopback

Measured on the lab's loop leg on 2026-10-21, with three copies (`f98-loop-rep3`) and at 2+1
(`f98-loop-ec21`), under `shoal-spike/results/stores-ab/`.
- **The sides.** Both ran one build, F97's commit. One kept the lab's delay, 3 ms on every node of
  the leg; the other set `wal_commit_delay: 0ms`.
- **The leg.** Three nodes on europa over 127.0.0.11 to 13, every root on a loop device of its own
  over a file on the Optane.
- **The captures** ran in the order delay, none, none, delay, each on devices made fresh.
- **Every figure** is the median of eight runs, with the lowest and highest.
- **Nothing was lost.**

| Arm | In flight | Group | Delay, ops/s | No delay | Ratio | Puts' p50 ms, delay → none | Puts' p99 ms |
| --- | --- | --- | --- | --- | --- | --- | --- |
| Puts, 4 KiB | 1 | three copies | 472 [469–475] | 1,598 [1,511–1,612] | **3.39, ahead** | 2.41 → 0.60 | 3.91 → 0.89 |
| | | 2+1 | 472 [463–474] | 1,595 [1,506–1,613] | **3.38, ahead** | 2.41 → 0.60 | 3.91 → 0.90 |
| | 16 | three copies | 3,333 [3,302–3,345] | 8,764 [8,632–9,094] | **2.63, ahead** | 4.92 → 1.68 | 8.05 → 6.58 |
| | | 2+1 | 3,328 [3,307–3,348] | 8,727 [8,608–9,101] | **2.62, ahead** | 4.92 → 1.69 | 8.08 → 6.61 |
| Mixture, 4 KiB | 1 | three copies | 918 [899–927] | 2,480 [2,432–2,504] | **2.70, ahead** | 2.04 → 0.59 | 3.81 → 0.84 |
| | | 2+1 | 916 [896–927] | 2,470 [2,430–2,508] | **2.70, ahead** | 2.05 → 0.59 | 3.83 → 0.85 |
| | 16 | three copies | 5,902 [5,889–5,908] | 13,075 [12,938–13,229] | **2.22, ahead** | 4.54 → 1.66 | 7.09 → 5.32 |
| | | 2+1 | 5,906 [5,888–5,914] | 13,049 [12,917–13,235] | **2.21, ahead** | 4.55 → 1.67 | 7.14 → 5.30 |

What the delay saved:

| Figure | In flight | Three copies, delay → none | 2+1 |
| --- | --- | --- | --- |
| Device bytes written a byte put, puts | 1 | 13.8 → 16.2 | 13.5 → 16.2 |
| | 16 | 8.53 → 11.5 | 8.49 → 11.5 |
| The same, the mixture | 1 | 13.8 → 16.5 | 13.8 → 16.5 |
| | 16 | 8.77 → 12.5 | 8.74 → 12.6 |

The device bytes are the capture's own counters, every loop device's over every byte the arm put.
Europa's Optane under them was not sampled, so its flushes are not counted.

What this says by the page's test:
- **On europa's loopback the delay is the gap.** With no delay, 4 KiB puts ran 3.4 times as fast at
  one in flight and 2.6 times at sixteen, and the mixture 2.7 and 2.2 times. Every range is
  disjoint, in both groups. A put's p50 at one in flight fell from 2.41 ms to 0.60, and its p99
  from 3.91 to 0.89.
- **Beside the comparison's own captures**, which ran on another day, 4 KiB puts at one in flight
  move from behind Ceph by at least 2.0 times to ahead of it by at least 50% with three copies, and
  by at least 56% at 2+1.
- **The delay saved 17 to 44% of the device bytes.** It saved more at sixteen in flight, where it
  grouped more appends into each sync.
- **The SSD leg found none of this.** There, titan's and hyperion's 970 EVOs take milliseconds to
  flush, and no delay ran no faster ([On the SSDs](#on-the-ssds)). On the loopback every node is
  on the Optane, and the 3 ms each node waits after a sync is most of a put.
- **A delay taken only while appends are queued**, F98 as filed, would match no delay at one in
  flight, where nothing is queued. At sixteen in flight appends are always queued, so it would keep
  the delay's loss there: 2.6 times on the loopback.

## F98, before and after

Measured on 2026-10-22, with three copies on both legs: F98's parent with the lab's delays, F98
with them, and F98 with none. The tables are on
[F98's page](../../features/commit-delay-bounded-by-the-sync.md#performance). By this page's test:
- **On europa's loopback, where the gap was, F98 takes back what the delay cost.**
  - 4 KiB puts ran 3.16 times as fast at one in flight and 2.54 times at sixteen. The mixture ran
    2.62 and 2.16 times as fast. Every range is disjoint.
  - That is within 7% of no delay. The ranges overlap at sixteen in flight, and at one in flight
    F98 is 7% behind for puts and 3% for the mixture.
  - A put's p50 at one in flight fell from 2.42 ms to 0.64, beside no delay's 0.60.
- **What it saves.** At sixteen in flight F98 keeps half of the device bytes the delay saved:
  9.87 a byte put against the delay's 8.50 and no delay's 11.5. At one in flight it keeps almost
  none, 16.0 against 13.6 and 16.2.
- **On the SSDs no rate moved**, and the 970 EVOs wrote what they wrote before. Europa's Optane
  waited less and wrote about a fifth more a byte put at sixteen in flight, for nothing, since
  every commit there waits on a 970 EVO.
- **Beside the comparison's captures**, which ran on another day, 4 KiB puts on the loopback lead
  Ceph by at least 37% at one in flight and 29% at sixteen, where they trailed by at least 2.0
  times and 46%.

## What it could cost

O61's measured trade: removing the delay doubled europa's device writes under the insert bench. ~~A
delay chosen by queue depth keeps the wear saving under load and costs nothing at one in flight,
if appends at one in flight never find a queue.~~ F98 bounds the delay by the sync instead. On
europa's loopback it wrote 16% more device bytes a byte put than the lab's delay at sixteen in
flight, where no delay wrote 35% more, and 18% more at one in flight, where no delay wrote 19% more
([F98, before and after](#f98-before-and-after)).

## Related

[O61](../../appendix/optimizations.md#o61-a-fast-device-syncs-the-wal-in-batches-too-small-to-fill-a-page);
[cluster testing: a group commit delay](../../cluster-testing/performance.md#o61-a-group-commit-delay);
[F57](../../features/cluster-reconfigure.md), which sets it per inventory group.
