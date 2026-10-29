# A short stripe writes a whole chunk

| | |
| --- | --- |
| **Entry** | [O107](../../appendix/optimizations.md#o107-a-stripe-shorter-than-its-chunk-costs-its-device-the-whole-chunk), filed from F86 before the comparison |
| **Priority** | [1 of 9](../store-optimizations.md#in-order) |
| **Status** | ~~not started~~ built by [F96](../../features/chunk-file-classes.md), its file's source chosen by [X15](../short-chunk-files.md); ~~the comparison's puts before and after come next~~ measured on the SSD leg with three copies ([before and after](#before-and-after)): a 64 KiB put writes 4.8 device bytes a byte where it wrote 197, and sixteen in flight run 3.9 times as fast; at 2+1 it writes 3.3 where it wrote 68, and sixteen in flight run 2.5 times as fast. ~~2+1 and the other legs come next~~ ~~The disk leg and europa's loopback come next~~ On the disk leg with three copies (2026-10-21) a 64 KiB put writes 4.6 device bytes a byte where it wrote 203, and sixteen in flight run 3.1 times as fast; ~~the disk leg at 2+1 and europa's loopback come next~~ at 2+1 3.4 where it wrote 131, and sixteen in flight run 2.5 times as fast. ~~Europa's loopback comes next~~ On europa's loopback it writes 4.6 where it wrote 195 with three copies and 3.1 where it wrote 64 at 2+1, and sixteen in flight run 8.0 and 3.0 times as fast (2026-10-21). Every leg is in |

## What the comparison found

The largest gap on the page, on every leg, in both groups. Each figure is a lower bound, Shoal's
verdict beside each store at one in flight, then at sixteen
([how to read them](../store-comparison.md#how-to-read-the-charts)).

| Arm | SSDs over 1 GbE | Disks | europa's loopback |
| --- | --- | --- | --- |
| Puts, 64 KiB, three copies | Ceph ≥2.9×, ≥6.3×; SeaweedFS ≥3.2×, ≥7.6× | Ceph ≥2.2×, ≥12.6×; SeaweedFS ≥2.9×, ≥5.2× | not run |
| Puts, 64 KiB, 2+1 | Ceph ≥2.6×, ≥4.2×; MinIO ≥3.5×, ≥3.2×; RustFS ≥2.9%, ≥49% | Ceph ≥13.8×, ≥15.7×; MinIO ≥44%, ≥28%; RustFS ≥18%, ≥46% | not run |
| Puts, 1 MiB, three copies | Ceph ≥11%, ≥22%; SeaweedFS ≥25%, ≥33% | Ceph ≥44%, ≥2.7×; SeaweedFS ≥43%, ≥3.3× | Ceph ≥2.4×, ≥4.0×; SeaweedFS ≥27%, ≥4.1× |
| Puts, 1 MiB, 2+1 | Ceph ≥28%, ≥8.8%; MinIO ≥20%, ≥17%; RustFS ≥27%, ≥13% | Ceph ≥47%, ≥2.5×; MinIO within, within; RustFS within, ≥16% | Ceph ≥2.2×, ≥2.6×; MinIO ≥2.6×, ≥2.8×; RustFS within, ≥20% |

Shoal is behind in every cell. Its devices are what bound it:

| Device bytes written a byte put, sixteen in flight | Shoal, three copies | Ceph, three copies | Shoal, 2+1 | Ceph, 2+1 |
| --- | --- | --- | --- | --- |
| 64 KiB, SSDs | 196 | 3.5 | 67 | 2.1 |
| 1 MiB, SSDs | 15 | 3.0 | 6.1 | 1.6 |
| 64 KiB, disks | 197 to 217 | 3.4 to 3.5 | 127 to 132 | 3.6 to 3.7 |
| 1 MiB, disks | 15 to 16 | 3.0 to 3.1 | 10 | 1.5 |
| 1 MiB, loopback | 15.0 to 15.1 | 3.0 | 6.0 to 6.1 | 1.5 |

On the SSD leg each host's device wrote 208 MiB/s at 64 KiB and 238 MiB/s at 1 MiB, 96% busy. On
disks each wrote 85 to 95 MiB/s, 96 to 99% busy. On europa's loopback the gap is the largest on
any leg, with no link to hide it: with three copies Shoal put 144 to 150 objects of 1 MiB a second
against Ceph's 607 to 629, each of its three devices on the one Optane writing 724 MiB/s at 90%
busy.

**Measured again** in the comparison (2026-10-25 and 26, [Shoal measured again](../store-comparison.md#shoal-measured-again)): every
64 KiB put wrote 3.3 to 5.6 device bytes a byte, where the first run's wrote 67 to 217, and the
64 KiB puts ran 2.6 to 4.7 times as fast at sixteen in flight. They are still behind every store but
RustFS on SSDs, by at least 22% to 3.0 times, and behind Ceph and SeaweedFS on disks. The 1 MiB puts
are within a few per cent of the other stores on SSDs, and ahead of MinIO and RustFS on disks at
sixteen in flight.

## Why it would help

Every stripe takes a free file of its pool's whole chunk length, 4 MiB and a header block at the
default geometry, whatever it holds. It writes the units holding bytes into the file, and the pool's
refill zero-writes another file of the same length to replace it. A stripe of `n` bytes therefore
costs each holder about `4 MiB + n`. At 64 KiB that is 66 bytes a byte on each copy, about 200 for
three copies, which is the 196 measured. Every other store writes about what it was sent, times its
copies or its code's expansion.

The fix is O107's: a stripe's chunk is written into a file of its own length, its units and its
header written whole so no extent is left unwritten. The shared flush's rule still holds. As built
by [F96](../../features/chunk-file-classes.md), the file is the class of units the chunk holds, the fewest a power of two, so a chunk
grown by appends is written whole once a doubling, and it is made for the chunk and synced by itself
rather than taken from a pool written ahead. Its
device bytes a byte would then fall toward what a replicating store writes: three and some
metadata with three copies, one and a half and some at 2+1. While the device is the bound, as it
was at 96 to 99% busy, the put rate should rise with the bytes it no longer writes.

## What would show it worked

- **The comparison's puts.** `put100` at 64 KiB and 1 MiB, one in flight and sixteen, both groups,
  on the SSD, HDD and loop legs: Shoal before and after, each a fresh capture under the lab's rule. It
  worked if:
  - the after captures' run intervals are above the before captures', disjoint;
  - Shoal's device bytes a byte put at 64 KiB with three copies fall from about 196 to under 10;
  - the verdicts against the other stores move, toward within or ahead.
- **O107's own.** `shoaladm bench` `put100` at 64 KiB, 256 KiB and 1 MiB on one node and on three,
  ops/s and device bytes a byte.
- **What must not get worse:**
  - `put100` at 16 MiB, whose stripes fill their chunks;
  - a write in place and an append (`patch100` at 1 MiB), since a short chunk grown past its length
    is staged whole again;
  - the read back of every capture, which must lose nothing.

## Before and after

[F96](../../features/chunk-file-classes.md#on-the-lab) was measured with the comparison's put and get
arms: Shoal alone, before and after, in four captures for each leg and group. Its page gives the
method and every arm.

**SSDs over 1 GbE, three copies** (2026-10-20), operations a second as the median of eight runs
[lowest–highest]:

| Puts | Before | After | After ÷ before | Device bytes a byte put |
| --- | --- | --- | --- | --- |
| 64 KiB, one in flight | 30.6 [26.9–39.0] | 44.8 [28.2–45.1] | 1.46, within | 198 → 5.1 |
| 64 KiB, sixteen | 67.2 [59.8–108] | 263 [251–270] | **3.92, ahead** | 197 → 4.8 |
| 1 MiB, one | 21.2 [15.7–24.2] | 17.9 [15.8–23.9] | 0.84, within | 14.4 → 4.0 |
| 1 MiB, sixteen | 55.6 [53.1–71.2] | 73.3 [72.4–75.0] | **1.32, ahead** | 14.3 → 3.2 |

Judged by [what would show it worked](#what-would-show-it-worked):
- **Device bytes at 64 KiB with three copies, under 10: yes.** They were 4.8 at sixteen in flight
  and 5.1 at one, down from 197 and 198. Ceph's are 3.5.
- **The after ranges above the before ranges, disjoint: yes at sixteen in flight, no at one.** Both
  sizes are within at one in flight. The 1 MiB runs carried the reclamation of the 64 KiB run before
  them; [F96's page](../../features/chunk-file-classes.md#on-the-lab) says how.
- **The verdicts against the other stores: they moved toward within, and are not there yet.** The
  table sets the after side beside the comparison's own captures of Ceph and SeaweedFS, taken on
  2026-10-18. That is another day than the A/B, so the table is a guide to where the gap went, not a
  verdict under the comparison's rule. The "comparison" columns are Shoal's verdicts on the
  comparison's page.

  | Puts, three copies | Ceph: the comparison | Ceph: after F96 | SeaweedFS: the comparison | SeaweedFS: after F96 |
  | --- | --- | --- | --- | --- |
  | 64 KiB, one in flight | behind ≥2.9× | behind ≥49% | behind ≥3.2× | behind ≥2.1× |
  | 64 KiB, sixteen | behind ≥6.3× | behind ≥31% | behind ≥7.6× | behind ≥43% |
  | 1 MiB, one | behind ≥11% | behind ≥3.5% | behind ≥25% | behind ≥19% |
  | 1 MiB, sixteen | behind ≥22% | **ahead ≥7.3%** | behind ≥33% | behind ≥4.7% |
- **What must not get worse.**
  - 16 MiB puts are within, at half the device bytes.
  - Every get is within.
  - The read-back after every run lost nothing.
  - `patch100` at 1 MiB, which the comparison's spec does not run, has not been run yet.

**What is left of the 64 KiB gap is no longer the device's bytes.** The 970 EVOs were 94% busy
writing 24 MiB/s, where before they were 95% busy writing 287. Each took 7.4 device writes a put,
where Ceph's took 4.2 at twice the rate:
[O124](../../appendix/optimizations.md#o124-a-64-kib-put-keeps-its-970-evo-busy-at-a-tenth-of-its-bytes).

**SSDs over 1 GbE, 2+1** (2026-10-20):

| Puts | Before | After | After ÷ before | Device bytes a byte put |
| --- | --- | --- | --- | --- |
| 64 KiB, one in flight | 40.3 [25.4–41.2] | 41.0 [25.5–42.5] | 1.02, within | 68 → 3.7 |
| 64 KiB, sixteen | 119 [110–170] | 301 [280–304] | **2.52, ahead** | 68 → 3.3 |
| 1 MiB, one | 22.7 [16.2–27.9] | 20.6 [18.0–26.7] | 0.91, within | 5.9 → 2.7 |
| 1 MiB, sixteen | 84.2 [80.4–86.7] | 95.1 [93.1–96.1] | **1.13, ahead** | 5.8 → 2.1 |

- **Device bytes at 64 KiB fell from 68 to 3.3 and 3.7.** A 64 KiB stripe at 2+1 stores two chunks,
  its first data chunk and a parity chunk as long, which account for 2.13. Ceph's are 2.1.
- **The after ranges above the before ranges, disjoint: yes at sixteen in flight, no at one,** the
  same as with three copies. At 64 KiB and one in flight both sides placed 40 to 42 a second, apart
  from each capture's first run.
- **Every verdict against the other stores moved toward within or ahead, most at sixteen in
  flight.** At 64 KiB and one in flight the move against Ceph and MinIO is small, which matches the
  A/B finding the two sides within there. As above, this is a guide and not a verdict. Each cell gives
  Shoal's verdict on the comparison's page, then the after side's beside the same capture of that
  store:

  | Puts, 2+1 | Ceph | MinIO | RustFS |
  | --- | --- | --- | --- |
  | 64 KiB, one in flight | behind ≥2.6× → behind ≥2.5× | behind ≥3.5× → behind ≥3.3× | behind ≥2.9% → **within** |
  | 64 KiB, sixteen | behind ≥4.2× → behind ≥41% | behind ≥3.2× → behind ≥22% | behind ≥49% → **within** |
  | 1 MiB, one | behind ≥28% → behind ≥20% | behind ≥20% → behind ≥12% | behind ≥27% → behind ≥20% |
  | 1 MiB, sixteen | behind ≥8.8% → **ahead ≥2.1%** | behind ≥17% → behind ≥3.3% | behind ≥13% → **within** |
- **What must not get worse.** The 16 MiB puts are within at 1.5 device bytes a byte, down from 3.0.
  The gets are within, except 64 KiB at sixteen in flight, which was 5% faster for a reason not
  known. Nothing was lost.

The rate is not ordered by the writes a put either. At 2+1 each 970 EVO took 6.0 to 6.1 writes a
put. RustFS's took 3.2 and placed 321 puts a second, against Shoal's 301 and Ceph's 645 at 4.1 to
4.3 ([O124](../../appendix/optimizations.md#o124-a-64-kib-put-keeps-its-970-evo-busy-at-a-tenth-of-its-bytes)).

**Disks, three copies** (2026-10-21), each node's chunks on its disk, its rows and journal on its
SSD:

| Puts | Before | After | After ÷ before | Device bytes a byte put |
| --- | --- | --- | --- | --- |
| 64 KiB, one in flight | 7.05 [6.53–7.90] | 10.2 [9.47–11.1] | **1.44, ahead** | 199 → 5.6 |
| 64 KiB, sixteen | 17.8 [15.1–20.6] | 55.5 [48.7–69.2] | **3.12, ahead** | 203 → 4.6 |
| 1 MiB, one | 6.52 [6.13–7.30] | 6.32 [3.63–9.10] | 0.97, within | 14.3 → 3.6 |
| 1 MiB, sixteen | 15.6 [13.3–17.7] | 35.2 [26.2–39.0] | **2.27, ahead** | 14.3 → 3.2 |

Judged by [what would show it worked](#what-would-show-it-worked):
- **Device bytes at 64 KiB with three copies, under 10: yes.** They were 4.6 at sixteen in flight
  and 5.6 at one, down from 203 and 199. Each disk wrote 1.28 to 1.53 bytes a byte put, where it
  wrote 65 to 71.
- **The after ranges above the before ranges, disjoint: yes at sixteen in flight at both sizes,
  and at one in flight at 64 KiB.** 1 MiB at one in flight is within. Two of each after capture's
  four runs there carried other work; [F96's page](../../features/chunk-file-classes.md#on-the-lab)
  says what.
- **The verdicts against the other stores moved toward within, most at sixteen in flight, and are
  not there yet.** As on the SSDs, this is a guide and not a verdict:

  | Puts, three copies, disks | Ceph: the comparison | Ceph: after F96 | SeaweedFS: the comparison | SeaweedFS: after F96 |
  | --- | --- | --- | --- | --- |
  | 64 KiB, one in flight | behind ≥2.2× | behind ≥39% | behind ≥2.9× | behind ≥2.1× |
  | 64 KiB, sixteen | behind ≥12.6× | behind ≥3.8× | behind ≥5.2× | behind ≥36% |
  | 1 MiB, one | behind ≥44% | behind ≥31% | behind ≥43% | behind ≥30% |
  | 1 MiB, sixteen | behind ≥2.7× | behind ≥14% | behind ≥3.3× | behind ≥29% |
- **What must not get worse.**
  - 16 MiB puts ran 1.58 times as fast at sixteen in flight, and are within at one.
  - Every get is ahead or within. 64 KiB gets ran 3.3 and 5.5 times as fast, for the reason F96's
    page gives.
  - Nothing was lost.

**What is left of the 64 KiB gap on the disks is their writes.** Each disk made 143 to 148 writes a
second on both sides, of 490 to 530 KiB before and 28 to 29 KiB after. A put took 2.6 to 2.7 of
them, down from 8.1
([O124](../../appendix/optimizations.md#o124-a-64-kib-put-keeps-its-970-evo-busy-at-a-tenth-of-its-bytes)).

**Disks, 2+1** (2026-10-21):

| Puts | Before | After | After ÷ before | Device bytes a byte put |
| --- | --- | --- | --- | --- |
| 64 KiB, one in flight | 9.88 [9.40–10.2] | 12.4 [12.1–12.9] | **1.25, ahead** | 133 → 4.0 |
| 64 KiB, sixteen | 28.4 [27.4–29.7] | 72.0 [70.9–77.8] | **2.54, ahead** | 131 → 3.4 |
| 1 MiB, one | 8.95 [8.13–9.33] | 8.93 [5.07–11.1] | 1.00, within | 9.5 → 2.5 |
| 1 MiB, sixteen | 24.6 [23.8–27.5] | 53.6 [41.9–55.7] | **2.17, ahead** | 9.0 → 2.1 |

- **Device bytes at 64 KiB fell from 131 and 133 to 3.4 and 4.0.** Each disk wrote 0.82 to 1.10
  bytes a byte put, where it wrote 41 to 50.
- **The after ranges above the before ranges, disjoint: yes at sixteen in flight at both sizes,
  and at one in flight at 64 KiB**, as with three copies. 1 MiB at one in flight is within, its
  slow runs carrying other work.
- **Every verdict against the other stores moved toward within or ahead.** As a guide and not a
  verdict, each cell gives Shoal's verdict on the comparison's page, then the after side's beside
  the same capture of that store:

  | Puts, 2+1, disks | Ceph | MinIO | RustFS |
  | --- | --- | --- | --- |
  | 64 KiB, one in flight | behind ≥13.8× → behind ≥10.0× | behind ≥44% → behind ≥23% | behind ≥18% → **within** |
  | 64 KiB, sixteen | behind ≥15.7× → behind ≥5.8× | behind ≥28% → **ahead ≥56%** | behind ≥46% → **ahead ≥9.5%** |
  | 1 MiB, one | behind ≥47% → behind ≥29% | within → within | within → within |
  | 1 MiB, sixteen | behind ≥2.5× → behind ≥13% | within → **ahead ≥88%** | behind ≥16% → **ahead ≥12%** |
- **What must not get worse.** 16 MiB puts ran 1.14 times as fast at sixteen in flight, and are
  within at one. Every get is ahead or within, and nothing was lost.

**What is left on the disks is their writes here too.** Each disk made 135 to 174 writes a second
on both sides, and a put took 1.8 to 2.4 of them, down from 5.4 to 6.1
([O124](../../appendix/optimizations.md#o124-a-64-kib-put-keeps-its-970-evo-busy-at-a-tenth-of-its-bytes)).

`patch100` on the disks stopped at its first capture, which found
[item 283](../../appendix/resolved/chained-records-decided-together.md). Taken again with the fix on both sides, it
is within at one and sixteen in flight, so F96 does not make writes in place worse
([F96's page](../../features/chunk-file-classes.md#on-the-lab)).

**Europa's loopback** (2026-10-21), three nodes on europa, every device a loop device on the Optane:

| Puts | Before | After | After ÷ before | Device bytes a byte put |
| --- | --- | --- | --- | --- |
| 64 KiB, three copies, one in flight | 122 [115–123] | 198 [196–199] | **1.63, ahead** | 198 → 4.8 |
| 64 KiB, three copies, sixteen | 185 [183–186] | 1,481 [1,477–1,491] | **8.02, ahead** | 195 → 4.6 |
| 1 MiB, three copies, sixteen | 146 [144–157] | 398 [361–403] | **2.73, ahead** | 14.6 → 3.1 |
| 64 KiB, 2+1, sixteen | 501 [498–502] | 1,523 [1,516–1,531] | **3.04, ahead** | 64.1 → 3.1 |
| 1 MiB, 2+1, sixteen | 329 [327–428] | 539 [472–550] | **1.64, ahead** | 5.4 → 2.1 |

- **Device bytes at 64 KiB fell from 195 to 4.6 with three copies and from 64 to 3.1 at 2+1.**
- **The after ranges are above the before ranges at sixteen in flight, at both sizes.** At one in
  flight 1 MiB puts are within, three of their four runs in each capture carrying the reclamation
  of the 64 KiB run before them ([F96's page](../../features/chunk-file-classes.md#on-the-lab)).
- **The verdicts at 1 MiB, the size the comparison ran on this leg**, as a guide and not a verdict:
  - with three copies at sixteen in flight, behind Ceph and SeaweedFS by at least 33% and 35%,
    where the comparison had at least 4.0 and 4.1 times;
  - at 2+1, ahead of RustFS by at least 3.3%, where it was behind by at least 20%.

F96's legs are all in, and this row of the comparison's table is struck.

## What it could cost

- A short chunk's file is made for it, with its sync, instead of taken ready from the pool.
- An append that grows a short chunk rewrites it whole, once a stripe.
- O102's recycling does not reach a store that only grows.

## Related

[O107](../../appendix/optimizations.md#o107-a-stripe-shorter-than-its-chunk-costs-its-device-the-whole-chunk);
[A 64 KiB object could be a row](inline-threshold.md), which moves the same 64 KiB puts by another
road; [A whole chunk is written twice](whole-chunk-twice.md);
[S6. The device store](../device-store.md).
