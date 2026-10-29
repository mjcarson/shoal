# A 64 KiB object could be a row

| | |
| --- | --- |
| **Entry** | [O120](../../appendix/optimizations.md#o120-a-pools-inline-threshold-was-set-by-the-row-path-alone-and-a-64-kib-object-pays-a-whole-chunk), filed from the comparison |
| **Priority** | [2 of 9](../store-optimizations.md#in-order) |
| **Status** | ~~not started~~ measured on the SSD leg with three copies (2026-10-20). At `inline: 64KiB` the 64 KiB puts ran 2.7 to 3.1 times as fast. That cost each node about 1.5 GiB more memory over a run, and 45% more device bytes a byte put ([On the SSDs, three copies](#on-the-ssds-three-copies)). ~~The disk leg comes next, and~~ On the disk leg (2026-10-21) they ran 10 to 11 times as fast, as rows on each node's SSD where they had been stripes on its disk ([On the disks, three copies](#on-the-disks-three-copies)). ~~The default is the user's choice with both legs' figures~~ The user kept the default at 16 KiB on 2026-10-21: `inline: 64KiB` is a pool's own choice, made with these figures ([configuration](../../getting-started/configuration.md#pools-and-buckets)) |

## What the comparison found

Shoal's two paths for an object are the two ends of its results:

| Arm, sixteen in flight, ops/s | Shoal | Ceph | Verdict beside Ceph |
| --- | --- | --- | --- |
| Puts, 4 KiB, three copies, SSDs | 1,215 | 1,310 | behind ≥2.9% |
| Puts, 4 KiB, three copies, disks | 1,222 | 389 | ahead ≥3.0× |
| Puts, 64 KiB, three copies, SSDs | 56.8 | 529 | behind ≥6.3× |
| Puts, 64 KiB, three copies, disks | 19.0 | 276 | behind ≥12.6× |

A 4 KiB object is a row. Its put ran at the same rate on the SSD and disk legs, since Shoal's rows
are on the SSD in both, and it was ahead of every store on disks at sixteen in flight. A 64 KiB
object is a stripe, and was behind every store on every leg
([A short stripe writes a whole chunk](short-stripes.md)).

**Measured again** in the comparison (2026-10-25 and 26, [Shoal measured again](../store-comparison.md#shoal-measured-again)), at the
default 16 KiB and after [F96](../../features/chunk-file-classes.md): 64 KiB puts are behind every
store but RustFS on SSDs, by at least 22% to 3.0 times, where the first run's were behind by 2.6 to
7.6 times, and behind Ceph and SeaweedFS on disks by at least 33% to 9.9 times. The comparison ran
no capture at `inline: 64KiB`.

## Why it would help

An object at or under its pool's inline threshold, 16 KiB by default, is stored in its path's
`ObjectMeta` row and committed through the row's tablet group. Past the threshold it is a stripe,
and every stripe takes a whole 4 MiB chunk on each holder
([O107](../../appendix/optimizations.md#o107-a-stripe-shorter-than-its-chunk-costs-its-device-the-whole-chunk)).
At 64 KiB the row is much the cheaper path. On titan's 970 EVO, one node and eight in flight, M13's
rows put 726 objects of 64 KiB a second where F86's stripes put 48 to 58. On disks the gap is
wider, because a row is written to the SSD and a stripe to the disk.

The 16 KiB default was chosen where X10's row path bent ([S18](../contract.md), T3). An even mixture
of inline puts and gets kept 0.58 to 0.71 of its 1 KiB rate at 16 KiB, and 0.32 to 0.55 at 32 KiB.
That compared rows with smaller rows. It was set before a stripe's cost on a device was measured
([F86](../../features/replicated-pools.md#on-the-lab)), and never set against it. A pool's `inline`
setting already allows up to 16 MiB (`MAX_INLINE_THRESHOLD`), so the change is a default, or a
default chosen by a pool's media.

## What would show it worked

- **The comparison's 64 KiB arms.** `put100`, `get100` and `rw50` at 64 KiB, one in flight and
  sixteen, both groups, on the SSD and HDD legs, with Shoal's pool at `inline: 64KiB` beside the
  captures at the default. It worked if Shoal's 64 KiB put intervals are above the default's,
  disjoint, and its 64 KiB gets are no worse.
- **The row side, on its own.** X10's size leg (`x10`, results/x10-lab.sh) at 16, 32, 64 and
  128 KiB on the lab. It says what a larger row costs the row path.
- **What must not get worse:**
  - the 4 KiB arms, rows sharing the shards with the larger rows: `put100`, `get100`, `rw50` and
    `stat100` at 4 KiB;
  - each node's peak memory and its WAL device's bytes a byte put, which every capture records.

## On the SSDs, three copies

Measured on the lab's SSD leg with three copies on 2026-10-20 (`o120-ssd-rep3`, under
`shoal-spike/results/stores-ab/`).
- **The sides.** Both ran one build, F97's commit. One pool was at `inline: 16KiB`, the default,
  and the other at `inline: 64KiB`.
- **The captures** ran in the order 16, 64, 64, 16, each on devices made fresh.
- **Every figure** is the median of eight runs, with the lowest and highest.
- **Nothing was lost.** The check at each capture's end read 2,048 objects back.

| Arm | In flight | 16 KiB, ops/s | 64 KiB | Ratio | p99 ms, 16 → 64 |
| --- | --- | --- | --- | --- | --- |
| Puts, 64 KiB | 1 | 41.9 [41.3–42.3] | 131 [131–137] | **3.14, ahead** | 33.6 → 12.7 |
| | 16 | 257 [252–259] | 683 [673–731] | **2.66, ahead** | 85.8 → 63.1 |
| Gets, 64 KiB | 1 | 564 [561–566] | 1,216 [1,184–1,235] | **2.16, ahead** | 2.94 → 1.51 |
| | 16 | 2,958 [2,919–2,999] | 3,680 [3,577–3,773] | **1.24, ahead** | 11.9 → 12.2 |
| Mixture, 64 KiB | 1 | 75.6 [74.0–76.9] | 238 [237–242] | **3.15, ahead** | gets 4.42 → 2.58, puts 39.8 → 12.0 |
| | 16 | 491 [484–498] | 1,267 [1,243–1,304] | **2.58, ahead** | gets **10.4 → 22.9**, puts 82.1 → 61.6 |
| Puts, 4 KiB | 1 | 178 [156–180] | 175 [158–179] | 0.98, within | 10.5 → 11.3 |
| | 16 | 1,229 [1,205–1,252] | 1,201 [1,188–1,228] | 0.98, within | 26.1 → 28.3 |
| Gets, 4 KiB | 1 | 1,612 [1,559–1,656] | 2,099 [2,072–2,116] | 1.30, ahead, [not the threshold's](#deleting-them) | 2.83 → 0.81 |
| | 16 | 13,229 [12,351–13,529] | 13,728 [13,287–15,038] | 1.04, within | 7.26 → 7.08 |
| Mixture, 4 KiB | 1 | 321 [315–329] | 316 [312–319] | 0.98, within | 1.69 → 1.79 |
| | 16 | 2,273 [2,244–2,299] | 2,213 [2,173–2,280] | 0.97, within | 9.32 → 10.1 |
| Stats, 4 KiB | 1 | 1,822 [1,785–1,848] | 2,319 [2,122–2,362] | 1.27, ahead, [not the threshold's](#deleting-them) | 2.51 → 0.72 |
| | 16 | 13,879 [13,014–14,111] | 14,479 [13,913–15,831] | 1.04, within | 7.14 → 7.08 |

What it cost at 64 KiB:

| Figure | In flight | 16 KiB | 64 KiB |
| --- | --- | --- | --- |
| Peak resident memory of a node, puts | 1 | 2.08 to 2.14 GiB | 2.49 to 2.51 GiB |
| | 16 | 2.10 to 2.15 GiB | **3.57 to 3.68 GiB** |
| Peak resident memory of a node, the mixture | 16 | 2.14 to 2.21 GiB | **3.48 to 3.58 GiB** |
| Device bytes written a byte put, puts | 1 | 5.12 | **7.25** |
| | 16 | 4.80 | **6.97** |
| Flushes a put, titan's and hyperion's NVMe | 1 | 3.67 to 3.72 | 1.69 to 1.71 |
| | 16 | 2.74 to 2.75 | 1.02 |
| Device write requests a put, the same | 1 | 14.2 | 5.7 |
| | 16 | 7.7 to 7.8 | 2.7 |

How these figures are taken:
- **A node's memory** is the median of its runs' peaks, one figure for each of the three nodes.
- **The device bytes** are every host's device over every byte the arm sent. The WAL's root and
  the pool's device share one LV on each host.
- **The flushes and write requests** are each host's whole NVMe, sampled every second beside the
  runs, since an LV counts no flushes.

What this says by the page's test:
- **64 KiB puts: it worked.** At sixteen in flight they ran 2.66 times as fast and at one 3.14
  times, every run apart, and their p99 fell. Beside the comparison's captures, which ran on another
  day, 683 puts a second is within Ceph's and SeaweedFS's figures. The default is behind both, by at
  least 34% and 46%.
- **64 KiB gets are no worse alone.** They ran 2.16 times as fast at one in flight, with half the
  p99, and 1.24 times at sixteen, the p99 within. An inline get is answered from its row, which was
  resident: 0.03 to 0.08 device bytes read a byte served, against 1.10 to 1.24.
- **In the mixture at sixteen in flight the gets' p99 doubled**, from 10.4 to 22.9 ms, every run
  apart, while the mixture ran 2.58 times as fast. There the gets share their shards and their
  groups' logs with 635 puts of 64 KiB a second. The default's 246 a second went to the devices.
- **The 4 KiB arms are no worse.** Every one is within at sixteen in flight, and puts and the
  mixture are within at one. Gets and stats at one in flight ran faster at 64 KiB, but not because
  of the threshold. Each followed a 64 KiB arm whose objects the bench deletes after it
  ([Deleting them](#deleting-them)). At 64 KiB the gets' p99 at one in flight, 0.81 ms, matches
  [F95](../../features/bench-object-read-levels.md)'s figure for the same arm, 0.79.
- **Memory got worse.** At sixteen in flight each node peaked about 1.5 GiB higher. That is the
  24,000 to 26,000 objects of 64 KiB the run put, each node's copy resident in its tablet's
  partitions. The 4 KiB puts and the mixture peaked 0.1 to 0.4 GiB higher too, though they put the
  same rows on both sides.
  - Each run lasted 35 seconds, and the bench deleted its objects after it, so no node came near
    its 8 GiB.
  - At this rate a node would reach 8 GiB in about two and a half minutes. What eviction then
    costs the row path was not measured.
- **Device bytes a byte put got worse**, by 42 to 46%: 4.8 to 5.1 rose to 7.0 to 7.3.
  - A row is written to its group's WAL on every member, and again into an archive.
  - A stripe's chunk is written once on each holder, into a file of its class
    ([F96](../../features/chunk-file-classes.md)).
- **Flushes a put fell by half or more**, from 2.75 to 1.02 at sixteen in flight, and the device's
  write requests to a third. A row's appends share their WAL's syncs, where a holder places each
  chunk with four syncs of its own
  ([O124](../../appendix/optimizations.md#o124-a-64-kib-put-keeps-its-970-evo-busy-at-a-tenth-of-its-bytes)).
- **The arms after the 64 KiB ones read more off the devices at 64 KiB.** At one in flight the
  4 KiB mixture's devices read 119 bytes for each byte its gets received, against 19, with no cost
  to its rate or p99. What read was not traced.

One more thing touched the figures. During the second round of the first 64 KiB capture, from
15:46 to 15:58, a `cargo check` ran for about ten minutes on one core of europa, the driver's host.
- Every run it overlapped is within the same arm's runs in the other 64 KiB capture, or below them.
- The lowest of the 64 KiB side's stats at one in flight, 2,122 a second against 2,311 to 2,362
  for the rest, is one of those runs.
- So it could only have lowered the 64 KiB side.

### Deleting them

After each run of an arm the bench deletes what the arm put, then waits ten seconds before the
next arm. A 4 KiB arm at one in flight follows a 64 KiB arm at sixteen.
- **At 16 KiB that arm's 9,000 objects are stripes.** Their reclamation begins 5 to 7 seconds after
  a delete, under the lab's `reclaim_grace` and `reclaim_poll`. It ran through the 4 KiB run that
  followed:
  - the devices wrote 25 MB/s across the hosts during a run of reads;
  - the gets' p99 was 2.83 ms, against 0.81;
  - titan's and hyperion's NVMe each made two flushes and wrote about 48 KB for each object
    deleted, for 37 to 47 seconds after the deletes.

  That cost is filed as
  [O126](../../appendix/optimizations.md#o126-a-holder-syncs-two-directories-for-every-chunk-it-reclaims).
- **At 64 KiB the 24,000 to 26,000 objects were rows.** Their deletes made 0.6 flushes an object
  while they ran, and the run that followed wrote nothing.
- **In the comparison the same 4 KiB arm followed 16 MiB puts**, 170 to 185 objects, and its
  devices wrote nothing during it. With three copies it ran 2,007 to 2,159 gets a second at a p99
  of 0.75 to 0.81 ms, as this page's 64 KiB side did.
- **The bench's settle for Shoal is a sleep of ten seconds** (`stores-lab.sh`). Ceph's runs its
  collection of deleted objects through, and SeaweedFS's vacuums its volumes. A settle that waited
  for Shoal's reclamation, as `tmdb-dataset-loader reclaim wait` does, would keep it out of the
  next arm. Filed as item 301 (2026-10-23), when the loop leg showed the same, and fixed so
  ([Resolved #301](../../appendix/resolved/a-settle-waits-for-reclamation.md)).

## On the disks, three copies

Measured on the lab's disk leg with three copies on 2026-10-20 and 21 (`o120-hdd-rep3`, under
`shoal-spike/results/stores-ab/`), as the SSD leg was.
- **The sides.** Both ran one build, F97's commit, with the pool at `inline: 16KiB` and at
  `inline: 64KiB`.
- **The captures** ran in the order 16, 64, 64, 16, each on devices made fresh.
- **Every figure** is the median of eight runs, with the lowest and highest.
- **Nothing was lost.** The check at each capture's end read 2,048 objects back.

**The leg's layout decides what this measures.** Each node's rows and logs are on its SSD's LV, as
on the SSD leg. Only the pool's device is a disk, with its journal on the SSD
([each store as deployed](../store-comparison.md#each-store-as-deployed)). So at 16 KiB a 64 KiB
object is a stripe on three disks, and at 64 KiB a row on three SSDs. In the 64 KiB puts at 64 KiB
the disks wrote nothing.

| Arm | In flight | 16 KiB, ops/s | 64 KiB | Ratio | p99 ms, 16 → 64 |
| --- | --- | --- | --- | --- | --- |
| Puts, 64 KiB | 1 | 12.7 [11.0–13.3] | 132 [130–138] | **10.4, ahead** | 233 → 12.2 |
| | 16 | 62.0 [61.1–65.3] | 681 [675–734] | **11.0, ahead** | 478 → 63.0 |
| Gets, 64 KiB | 1 | 441 [422–449] | 1,187 [1,152–1,228] | **2.69, ahead** | 3.98 → 1.52 |
| | 16 | 2,221 [2,208–2,232] | 3,541 [3,359–3,733] | **1.59, ahead** | 14.1 → 12.7 |
| Mixture, 64 KiB | 1 | 23.4 [20.7–26.2] | 238 [234–241] | **10.1, ahead** | gets 37.3 → 2.60, puts 255 → 11.8 |
| | 16 | 120 [116–124] | 1,263 [1,251–1,309] | **10.6, ahead** | gets 116 → 23.3, puts 454 → 60.6 |
| Puts, 4 KiB | 1 | 178 [155–180] | 177 [156–179] | 0.99, within | 10.5 → 11.0 |
| | 16 | 1,221 [1,204–1,258] | 1,203 [1,181–1,225] | 0.99, within | 26.4 → 27.6 |
| Gets, 4 KiB | 1 | 1,898 [1,829–1,982] | 2,104 [2,040–2,176] | 1.11, ahead, [not the threshold's](#deleting-them) | 1.54 → 0.80 |
| | 16 | 13,665 [12,989–15,548] | 15,659 [15,096–17,044] | 1.15, within | 6.26 → 6.73 |
| Mixture, 4 KiB | 1 | 320 [312–329] | 317 [313–320] | 0.99, within | 1.70 → 1.77 |
| | 16 | 2,266 [2,233–2,322] | 2,222 [2,197–2,291] | 0.98, within | 9.34 → 10.1 |
| Stats, 4 KiB | 1 | 2,136 [2,061–2,216] | 2,368 [2,312–2,420] | 1.11, ahead, [not the threshold's](#deleting-them) | 1.39 → 0.70 |
| | 16 | 14,463 [13,669–16,348] | 16,495 [16,011–17,551] | 1.14, within | 6.06 → 6.67 |

What it cost at 64 KiB:

| Figure | In flight | 16 KiB | 64 KiB |
| --- | --- | --- | --- |
| Peak resident memory of a node, puts | 16 | 2.06 to 2.12 GiB | **3.57 to 3.66 GiB** |
| Peak resident memory of a node, the mixture | 16 | 2.10 to 2.13 GiB | **3.48 to 3.56 GiB** |
| Device bytes written a byte put, every device, puts | 1 | 5.42 | **7.33** |
| | 16 | 4.82 | **6.97** |
| The same, each disk | 16 | 1.31 to 1.33 | none |
| The same, each SSD's LV | 16 | 0.26 to 0.34 | **2.34 to 2.38** |
| Busy share, each disk, puts | 16 | 82 to 94% | none |
| Busy share, titan's and hyperion's SSD LV, puts | 16 | 37% | **93%** |
| Flushes a put, titan's and hyperion's NVMe | 1 | 3.09 to 3.14 | 1.69 to 1.70 |
| | 16 | 2.31 to 2.32 | 1.03 to 1.04 |

These figures are taken as the SSD leg's are. Each device's bytes and busy share are the capture's
own counters, by device. The flushes are each host's NVMe from the samplers, which did not read the
disks.

What this says by the page's test:
- **64 KiB puts: ten times as fast, by leaving the disks.** At 16 KiB each disk was 82 to 94% busy
  at 62 puts a second. At 64 KiB the puts ran at 681 a second, the SSD leg's own figure (683), as
  rows on the SSDs. Beside the comparison's disk captures, which ran on another day, the 64 KiB
  puts at sixteen in flight move from behind Ceph and SeaweedFS by at least 4.0 times and 40% to
  ahead of them by at least 2.3 and 5.0 times.
- **64 KiB gets ran 2.69 times as fast at one in flight and 1.59 at sixteen**, every run apart,
  their p99 lower. In the mixture both kinds' p99 fell, the gets' from 116 to 23.3 ms at sixteen in
  flight. That is unlike the SSD leg, where the gets' p99 doubled. Here the 16 KiB side's gets
  read disks that the mixture's puts kept 88 to 96% busy.
- **The 4 KiB arms are no worse.** Puts and the mixture are within at both depths, and gets and
  stats at sixteen. At one in flight gets and stats ran 11% faster at 64 KiB, but each followed a
  64 KiB arm whose 2,000 or so stripes the bench deletes after it. At 16 KiB those were reclaimed
  from the disks during the 4 KiB run ([Deleting them](#deleting-them)).
- **Memory got worse by the same 1.5 GiB a node** at sixteen in flight as on the SSDs.
- **Device bytes a byte put rose 35 to 45%**, and all of them moved to the SSDs. Titan's and
  hyperion's 970 EVOs were 93% busy at sixteen in flight, where the disks had been.
- **So on this layout the threshold moves the pool's small objects off its disks** onto the device
  that holds the nodes' logs. A pool whose class is `hdd` no longer decides where they live. That
  is a different trade from the SSD leg's, where both ends were on one device.

## What it could cost

- **The row path slows as its rows grow.** That is X10's knee, and it slows every object and table
  on the shard, not only the large rows.
- **Memory.** Every inline byte is held in its tablet's partitions, replicates through its group's
  WAL, and is carried by its group's snapshots.
- **Copies.** An inline object is kept at its tablet group's replication factor, whatever its pool's
  redundancy. At 2+1 a 64 KiB inline object costs three copies, not one and a half. The 4 KiB arms
  at 2+1 already pay this.
- **Where an object lives.** An inline object lives with its rows, on each node's latency root,
  whatever its pool's class. On the disk leg that moved the pool's 64 KiB objects off the disks and
  onto the SSDs that hold the logs ([On the disks, three copies](#on-the-disks-three-copies)): a
  pool of disks keeps none of its small objects, and they take the log device's space.
- It does not reach 1 MiB, or any size where a row is the wrong place: past its size, only
  [A short stripe writes a whole chunk](short-stripes.md) helps.

## Related

[O120](../../appendix/optimizations.md#o120-a-pools-inline-threshold-was-set-by-the-row-path-alone-and-a-64-kib-object-pays-a-whole-chunk);
[S3. Small objects stay inline](../objects.md#small-objects-stay-inline);
[X10. What a stripe row costs](../stripe-row-costs.md); [S18. The contract](../contract.md).
