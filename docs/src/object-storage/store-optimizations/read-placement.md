# Reads from a pool of copies follow leadership

| | |
| --- | --- |
| **Entry** | [O119](../../appendix/optimizations.md#o119-a-get-from-a-pool-of-copies-reads-its-coordinators-own-copy-so-each-disk-serves-the-gets-its-node-leads), filed from the comparison |
| **Priority** | [3 of 9](../store-optimizations.md#in-order) |
| **Status** | ~~not started~~ built by [F97](../../features/disk-reads-by-queue.md), by each slice's queue on disks alone, the user's choice; ~~the gets on disks before and after come next. They run on top of F96, which took europa's disk from 95% busy to 35% for 64 KiB gets at sixteen in flight ([F96's figures](../../features/chunk-file-classes.md#on-the-lab))~~ measured on the disk leg on top of F96 (2026-10-21, [before and after](#before-and-after)): **slower** at sixteen in flight, by 28% at 64 KiB and 11% at 1 MiB, since the reads it moved filled europa's link; within at one. ~~Whether it stays is the user's choice~~ The user chose a margin, measured (2026-10-21): [F101](../../features/disk-read-margin.md) reads the local copy first unless a node sets one; ~~its figures come next~~ measured on the disk leg (2026-10-21, [F101's figures](../../features/disk-read-margin.md#performance)): a margin of four moved one or two reads in a hundred and lost nothing, and its sides differed by each deployment's draw of which node leads which groups. ~~Whether a margin is the default is the user's choice~~ Measured again with eight captures a side, it changed no rate and shortened the tail of 1 MiB reads at sixteen in flight, and the user made a margin of four the default (2026-10-21) |

## What the comparison found

On disks, gets of 64 KiB at sixteen in flight:

| | Shoal, three copies | Shoal, 2+1 | Ceph, three copies | SeaweedFS, three copies |
| --- | --- | --- | --- | --- |
| Gets a second | 540 [487–595] | 1,437 [1,395–1,479] | 2,079 | 2,238 |
| europa's disk busy | 95% | 72% | – | – |
| titan's and hyperion's disks busy | 53 to 54% | 90 to 92% | – | – |

With three copies Shoal was behind Ceph by at least 3.4 times and SeaweedFS by at least 3.7 times
([on disks](../store-comparison.md#on-disks)). At 1 MiB, europa's disk was 95 to 96% busy reading
65 to 74 MiB/s while the others read 38 to 49 MiB/s at 46 to 60%. The same shape is the
[drift](../store-comparison.md#drift) between Shoal's two three-copy captures: gets at sixteen in
flight 11 to 23% slower in the second, following europa's share of the leaders.

**Measured again** in the comparison (2026-10-25 and 26, [Shoal measured again](../store-comparison.md#shoal-measured-again)), with
[F107](../../features/cache-default-share.md)'s cache answering the working set, Shoal's disks were
all but idle in the 64 KiB and 1 MiB gets at sixteen in flight. With three copies the 64 KiB gets lead
Ceph and SeaweedFS by at least 37% and 28%, where the first run's were behind by at least 3.4 and
3.7 times, and at 16 MiB, which the cache does not hold, by at least 52% and 36%. A 64 KiB working set
larger than the cache was not run, so where one is read off disks O119 stands.

## Why it would help

A get goes to the leader of its path row's group, unless it reads at `One`
(`shoal-client/src/client/objects.rs`, 666-671). That coordinator tries the positions the row calls
current with its own first (`shard/objects/read.rs`, 371-373). With three copies over three nodes
every node holds every stripe. So every get is read from its coordinator's own disk, and each disk
serves the share of groups its node leads, whatever the disk and however long its queue. Europa
led the most here, and its disk was the bound while the other two were half idle.

At 2+1 a 64 KiB object lies in one data chunk, on whichever host placement put it, and the
coordinator reads that chunk wherever it is. All three disks were busy, and Shoal read more than
twice as many. A choice of holder by turn, or by each slice's queue, should do the same for a pool
of copies.

## What would show it worked

- **The comparison's gets on disks.** `get100` at 64 KiB and 1 MiB, one in flight and sixteen,
  three copies, on the HDD leg, before and after. It worked if:
  - every disk's busy share is within a few points of the others';
  - the get rate at sixteen in flight is above the before captures', disjoint;
  - the drift between Shoal's first and last capture shrinks.
- **What must not get worse:** `get100` at 1 MiB and 16 MiB, sixteen in flight, on the SSD leg.
  There a get served from europa's own copy over loopback, beside the link, put Shoal ahead of
  Ceph and SeaweedFS by at least 26% to 60%. Any choice that leaves the local copy loses that.

## Before and after

[F97](../../features/disk-reads-by-queue.md#performance) was measured with the comparison's gets on
the disk leg with three copies (2026-10-21): Shoal alone at F96's commit and at F97's, in four
captures. Its page gives the method. Gets a second, the median of eight runs [lowest–highest], and
each disk's busy share:

| Gets | Before | After | After ÷ before | europa's disk | titan's and hyperion's |
| --- | --- | --- | --- | --- | --- |
| 64 KiB, one in flight | 475 [443–512] | 462 [427–485] | 0.97, within | 12% → 11% | 11 to 12% → 12 to 13% |
| 64 KiB, sixteen | 3,254 [3,037–3,474] | 2,335 [2,239–2,427] | **0.72, behind** | 47% → 33% | 40 to 48% → 44% |
| 1 MiB, one | 54.0 [50.7–55.4] | 50.5 [48.8–51.5] | 0.93, within | 40% → 37% | 11 to 14% → 15% |
| 1 MiB, sixteen | 181 [171–193] | 160 [158–162] | **0.89, behind** | 96% → 87% | 28 to 34% → 50% |

Judged by [what would show it worked](#what-would-show-it-worked):
- **Every disk's busy share within a few points of the others': closer, not within.**
- **The get rate at sixteen in flight above the before captures', disjoint: no.** It fell 28% at
  64 KiB and 11% at 1 MiB, the ranges disjoint.
- **The drift between captures shrank**, from 13% and 8% to 7% and 0.4%, because europa's link
  bounds the after side.
- **What must not get worse.** The SSD leg is unchanged by construction. Nothing was lost.

**Why it lost: the tradeoff this page names, a remote read crosses a link a local one does not.**
- The bench's driver runs on europa, so every byte served crosses europa's link, except those of a
  get europa both coordinates and reads off its own disk. Reads by queue moved some of those.
- europa's link received 111 to 112 MiB/s at 1 MiB, a 1 GbE link's ceiling, where it had received
  79.
- And F96 had already taken away most of the queue F97 was for. Before F97, europa's disk was 47%
  busy for 64 KiB gets at sixteen in flight, where the comparison found it 95% busy.

Beside the comparison's own captures, as a guide and not a verdict, 64 KiB gets at sixteen in
flight are ahead of Ceph by at least 7.5% after, where they were ahead by at least 45% before.

~~Whether reads by queue stay a disk pool's way is the user's choice, made with these figures.~~
The user chose a margin, measured (2026-10-21): [F101](../../features/disk-read-margin.md), four by
default since the same day. Measured again once every read was counted from its choice
(2026-10-27, [Resolved #314](../../appendix/resolved/local-reads-counted-at-choice.md)), it moved no rate and took
europa's disk from 49% to 43% of the bytes read at 1 MiB and sixteen in flight.

## What it could cost

A remote read crosses a link a local one does not. The SSD leg is the counter-case: over 1 GbE the
local copy is worth more than an idle remote disk. So the choice wants both: the local copy while
its slice keeps up, and another holder once its queue costs more than the link. A coordinator
knows its own slices' queues, but no report carries a remote one's.

As built by [F97](../../features/disk-reads-by-queue.md), a coordinator counts its own reads
outstanding to each remote slice and reads its own slices' queues, and only on a pool on disks:
the local copy wins while its queue is no longer than an idle remote one's. Since
[F101](../../features/disk-read-margin.md) ~~only on a node that sets a margin~~ by a margin, four unless a node
names `null`, and the local copy wins until its queue is longer than a remote one's by more than it.

## Related

[O119](../../appendix/optimizations.md#o119-a-get-from-a-pool-of-copies-reads-its-coordinators-own-copy-so-each-disk-serves-the-gets-its-node-leads);
[O116](../../appendix/optimizations.md#o116-an-erasure-coded-stripes-first-data-chunk-favours-the-heaviest-device),
why the 2+1 reads leaned to titan's and hyperion's larger disks;
[S9. The read path](../read-path.md).
