# A get reads the device every time

| | |
| --- | --- |
| **Entry** | [O121](../../appendix/optimizations.md#o121-every-get-reads-its-bytes-off-the-device-however-often-the-object-is-read), filed from the comparison; its narrower form is [a cache of chunks a disk read](../../appendix/todos.md#a-cache-of-chunks-a-disk-read), left open by F91 |
| **Priority** | [5 of 9](../store-optimizations.md#in-order) |
| **Status** | ~~not started~~ built as [F102](../../features/read-unit-cache.md), a cache of the units reads verified in a budget of its own, ~~off unless a node names a size~~ an eighth of node memory unless a node names a size since [F107](../../features/cache-default-share.md); ~~its before and after follows~~ measured on 2026-10-22: with 2 GiB a node, gets at one in flight ran 1.25 to 3.1 times as fast on every leg and group, the devices reading 0.00 to 0.05 bytes for every byte served where they read 1.00 to 1.07 ([F102, before and after](#f102-before-and-after)). Since [F96](../../features/chunk-file-classes.md#on-the-lab), 64 KiB gets on the disks with three copies ran 3.3 times as fast at one in flight and 5.5 times at sixteen, their files 68 KiB where they were 4 MiB. Beside the comparison's captures, as a guide, they are still behind Ceph and SeaweedFS at one in flight, by at least 2.5 and 2.3 times with no cache, and by at least 36% and 30% with F102's 2 GiB |

## What the comparison found

Shoal's verdict beside each store, gets at one in flight:

| Arm | SSDs over 1 GbE | Disks |
| --- | --- | --- |
| Gets, 64 KiB, three copies | Ceph ≥49%, SeaweedFS ≥44% | Ceph ≥9.0×, SeaweedFS ≥8.2× |
| Gets, 64 KiB, 2+1 | Ceph ≥35%, MinIO ≥47%, RustFS ≥32% | Ceph ≥4.9×, MinIO ≥5.9×, RustFS ≥4.7× |
| Gets, 1 MiB and 16 MiB | behind every store, by at least 0.9% to 49% | behind every store by at least 29% to 2.6×, but at 1 MiB, where Shoal is within MinIO and ahead of RustFS by at least 33% |

On europa's loopback, where the arms ran at 1 MiB, Shoal was behind every store at one in flight, by
at least 22% to 2.1 times, and at sixteen behind every store but RustFS, by at least 9.1% to 41%.

Where the bytes came from:
- **SSDs, 64 KiB.** The other stores read 2 to 16% of the bytes they served off their devices.
  Shoal read 107 to 184%.
- **Disks, 64 KiB.** The others read at most 3 MiB/s off any disk, at a p99 of 1.3 to 2.1 ms. Shoal
  got 130 objects a second with three copies, at a p99 of 27 to 45 ms.
- **Loopback, 1 MiB at sixteen in flight, three copies.** Shoal read 2.3 GB/s off the one Optane
  through its three devices, 83 to 85% busy, for the 2.36 GB/s it served. SeaweedFS, MinIO and
  RustFS read 31 to 87 MiB/s off theirs, and Ceph half of what it served.

([On disks](../store-comparison.md#on-disks),
[where Shoal is better, and where worse](../store-comparison.md#where-shoal-is-better-and-where-worse).)

**Measured again** in the comparison (2026-10-25 and 26, [Shoal measured again](../store-comparison.md#shoal-measured-again)), with the
cache at its default eighth of node memory, 1 GiB a node across hosts and 512 MiB on loopback: on
SSDs the devices read 4 to 31% of what Shoal served of 64 KiB, where its first run's read 107 to
184%, and on loopback 0.1 to 0.3% of 1 MiB. 64 KiB gets at one in flight ran 1.33 to 1.47 times as
fast as the first run's on SSDs and 4.4 to 5.5 times on disks, and are behind every store by at least
8.5% to 35%. At 1 MiB with three copies they lead Ceph and SeaweedFS on both legs.

## Why it would help

A slice reads a chunk's range with direct I/O for every request, so each get costs a device read
however recently the same object was read. Every other store answered a repeated get from memory:
Ceph from BlueStore's own cache in each OSD, the others from the page cache. The comparison
preloads 1,024 objects of each size, 64 MiB at 64 KiB and 1 GiB at 1 MiB, so its 64 KiB and 1 MiB
gets read each object many times over a run.

S6 rejected the page cache, since object bytes held there are held twice and counted by nobody
([S6](../device-store.md#alternatives-rejected)). A cache of chunk units held by the executor that
owns the slice avoids both: ~~drawn from the object budget and counted there~~ in a budget of its
own since F102, the user's choice, keyed by chunk and label. A read takes from it only bytes at
the label its row names, as it does from a holder. F91's
open todo is the narrow form of the same thing: a small cache of whole chunks on a disk's executor,
for readers of small ranges.

## What would show it worked

- **The comparison's gets.** `get100` at 64 KiB and 1 MiB, one in flight and sixteen, on the SSD,
  HDD and loop legs, before and after. It worked if:
  - Shoal's device bytes read a byte served fall well under 1, toward the others' 0.02 to 0.16 at
    64 KiB;
  - its get intervals at one in flight are above the before captures', disjoint.
- **A working set larger than the cache,** so a miss's cost is seen too. The same `get100` arms over
  a preload many times the object budget, which the comparison does not run: no store should gain
  there.
- **What must not get worse:**
  - `put100` at the same sizes, whose budget the cache shares;
  - each node's peak memory;
  - `patch100`, since a write in place must drop what it changes;
  - the fixture's tests that a stale chunk is never read (`stale_chunk_is_never`,
    `read_overlays`).

## F102, before and after

Measured on the lab on 2026-10-22. One build ran with no cache beside 2 GiB a node, and beside
256 MiB a node on the SSD and disk legs ([F102's figures](../../features/read-unit-cache.md#performance)).
By what would show it worked:
- **Device bytes read a byte served** fell from 1.00 to 1.07 to 0.00 to 0.05 with 2 GiB, on every
  leg. The others read 0.02 to 0.16 at 64 KiB.
- **Gets at one in flight** were ahead on every leg, their ranges disjoint. Each figure is the
  gets a second with 2 GiB over those with no cache:

  | Leg | 64 KiB | 1 MiB |
  | --- | --- | --- |
  | SSDs, three copies | 1.25 | 1.35 |
  | Disks, three copies | 1.63 | 3.11 |
  | Loopback, three copies | 1.34 | 2.01 |
  | Loopback, 2+1 | 1.29 | 1.64 |

  On the SSDs each fresh deployment's leadership draw moved these. Read at one draw, the gain was
  26% at 64 KiB and 38% at 1 MiB.
- **At sixteen in flight across hosts** the bench driver's 1 GbE link was full with or without the
  cache. On the SSDs the sides were within each other. On the disks the cache ran 1.18 and 1.20
  times as fast, half of it the draw. On the loopback, which crosses no link, 1 MiB gets ran 1.98
  times as fast with three copies and 1.33 at 2+1.
- **A working set larger than the cache.** A cache of 4 MiB a node on europa's loopback held one
  1 MiB read in a hundred. Those gets ran within the range of none, 2% slower at the median, and
  their p99 at sixteen in flight rose from 10.3 ms to 11.1. It held one 64 KiB read in seven, and
  those ran 3% faster at one in flight. A cache gains there only what it holds, and this one lost
  at most 2% of a get's rate.
- **What must not get worse** did not:
  - puts at 64 KiB and 1 MiB were within on the SSDs, and wrote the same device bytes a byte;
  - 1 MiB patches ran at 0.96 and 0.94 times the rate with no cache, within;
  - a node's peak memory rose by what it held, 0.2 to 0.8 GiB with 2 GiB named;
  - the fixture's rows that a stale chunk is never read passed with every node keeping a cache
    ([F102's tests](../../features/read-unit-cache.md#tests)).

Beside the comparison's captures, which ran on another day and are a guide only:
- **64 KiB gets at one in flight** with 2 GiB are still behind Ceph and SeaweedFS. On the SSDs they
  are behind by at least 28% and 20%, where they were 46% and 41% behind. On the disks they are
  behind by at least 36% and 30%, where they were 2.5 and 2.3 times behind.
- **1 MiB gets on europa's loopback** with three copies moved ahead of Ceph and SeaweedFS at one
  in flight and sixteen. At 2+1 they are still behind Ceph and MinIO.

## What it could cost

- **Memory**, ~~taken from the object budget~~ a budget of its own since F102. On the lab a node's
  peak resident memory rose by 0.2 to 0.8 GiB with 2 GiB named, the share of the working sets it
  read.
- **Gains only where the set fits.** A cache helps only ~~a working set that fits in it~~ the reads
  it holds, in proportion. The comparison's working sets fit, and it says so among what it does
  not settle ([a working set larger than memory](../store-comparison.md#what-the-comparison-does-not-settle)).
  On the lab's disks 256 MiB a node, about half of what europa read, ran 1 MiB gets at one in
  flight 1.79 times as fast, where 2 GiB ran them 3.11. A cache of 4 MiB a node, holding one
  1 MiB read in a hundred on europa's loopback, ran those gets within the range of none, 2%
  slower at the median.
- **Invalidation.** A write in place, a rebuild, a move and a reclamation must each drop what they
  change, or a read takes a label the row no longer names. Keying by label makes a stale entry
  unreachable, but not freed.

## Related

[O121](../../appendix/optimizations.md#o121-every-get-reads-its-bytes-off-the-device-however-often-the-object-is-read);
[a cache of chunks a disk read](../../appendix/todos.md#a-cache-of-chunks-a-disk-read);
[S6. The device store](../device-store.md); [S9. The read path](../read-path.md).
