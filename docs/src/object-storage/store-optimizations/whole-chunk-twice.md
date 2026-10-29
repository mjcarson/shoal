# A whole chunk is written twice

| | |
| --- | --- |
| **Entry** | [O102](../../appendix/optimizations.md#o102-a-whole-chunk-costs-its-device-twice-its-bytes), filed from F85 before the comparison |
| **Priority** | [6 of 9](../store-optimizations.md#in-order) |
| **Status** | ~~not started~~ built by [F96](../../features/chunk-file-classes.md), another way than O102 was filed: the refill is retired, so no reclaimed file needs recycling for these arms to move; ~~the 16 MiB puts before and after come next~~ on the SSD leg a 16 MiB put writes 3.0 device bytes a byte where it wrote 6.0 with three copies, and 1.5 where it wrote 3.0 at 2+1, at a rate europa's link bounds there ([before and after](#before-and-after)). ~~The disk leg, which judges it, comes next~~ On the disk leg, which judges it, a 16 MiB put with three copies writes 3.1 where it wrote 6.1, and sixteen in flight run 1.58 times as fast (2026-10-21); ~~the disk leg at 2+1 comes next~~ at 2+1 it writes 1.5 where it wrote 3.0, and sixteen in flight run 1.14 times as fast. Its row of [the comparison's table](../store-optimizations.md#in-order) is struck (2026-10-21) |

## What the comparison found

On SSDs over 1 GbE, puts of 16 MiB were bounded by europa's link for every store, and Shoal's
device keeping up did not show. On disks the device is the bound, and Shoal fell behind. Shoal's
verdict beside each store, at one in flight, then at sixteen:

| Arm, disks | Verdicts | Device bytes a byte put, sixteen in flight |
| --- | --- | --- |
| Puts, 16 MiB, three copies | Ceph ≥23%, ≥24%; SeaweedFS ≥10%, ≥28% | Shoal 5.8 to 6.1; Ceph 3.0; SeaweedFS 3.0 |
| Puts, 16 MiB, 2+1 | Ceph ≥9.9%, then within; MinIO ≥4.7%, then within; RustFS ≥7.9%, ≥4.5% | Shoal 3.0 to 3.1; Ceph, MinIO and RustFS 1.5 |

With three copies Shoal's disks were 96 to 100% busy writing 74 to 81 MiB/s, where Ceph's were 80
to 84% busy writing 64 MiB/s ([on disks](../store-comparison.md#on-disks)).

**Measured again** in the comparison (2026-10-25 and 26, [Shoal measured again](../store-comparison.md#shoal-measured-again)): a
16 MiB put wrote 3.0 device bytes a byte with three copies and 1.5 at 2+1 on every leg, as Ceph's do.
On disks they ran 1.21 to 1.54 times as fast as the first run's at sixteen in flight, within every
store or ahead but Ceph with three copies at one in flight, by at least 6.2%.

## Why it would help

A whole chunk is written into a file the slice's pool zero-wrote ahead, and renamed over the
chunk. The pool's refill then zero-writes a file of the same length to replace the one the chunk
took. So a device takes a whole chunk's length twice, once as its bytes and once as zeros. That is
the 6.0 with three copies and the 3.0 at 2+1, twice what each store's redundancy needs.

O102's change: a chunk's old file, once its rename is durable, goes back to the pool rather than
being freed. The refill then writes only what the device lacks, and a chunk written again costs its
length once. While the disk is the bound, the put rate should rise with the bytes it no longer
writes.

As filed, O102 reaches only a chunk that replaces one. The comparison's puts make new objects, and
the bench deletes them between arms, so their chunks are reclaimed, not replaced
([F92](../../features/reclamation-and-device-lifecycle.md)). To move these arms, a reclaimed chunk's
file would go back to the pool too, under the seal it is removed under. That needs the argument
O102 makes, that its old bytes never verify under another chunk's identity (P15), made again for a
file the reclaimer frees.

Built otherwise by [F96](../../features/chunk-file-classes.md): nothing is written ahead, and a whole chunk's file is made for it and
synced by itself. A device then takes a whole chunk's length once, whether the chunk is new, replaces
one or follows a reclaimed one, and [X15](../short-chunk-files.md) measured it at 1.00 device byte a
byte at 4 MiB against 2.0.

## What would show it worked

- **The comparison's large puts on disks.** `put100` and `rw50` at 16 MiB, one in flight and
  sixteen, both groups, on the HDD leg, before and after. It worked if:
  - Shoal's device bytes a byte put fall toward 3.0 with three copies and 1.5 at 2+1;
  - its put intervals are above the before captures', disjoint.

  ~~This arm moves only if reclaimed files are recycled as well.~~ Since F96 it moves without
  that: nothing is written ahead to replace a file taken.
- **A chunk written again,** which O102 as filed reaches. No arm of the comparison writes one: its
  puts make new objects, and `patch100` writes 4 KiB in place. O102's own benchmark does:
  `shoal-spike store --only whole`, and the device counters of `shoal-bench`'s `object-load` beside
  its reference cell.
- **What must not get worse:** the fixture's whole-chunk tests (`whole_object_replace`,
  `stripe_write_is_atomic_at_every_crash_point`), since a recycled file holds old bytes until
  overwritten.

## Before and after

[F96](../../features/chunk-file-classes.md#on-the-lab) was measured with the comparison's put and get
arms: Shoal alone, before and after, in four captures for each leg and group. Its page gives the
method and every arm.

**SSDs over 1 GbE, three copies** (2026-10-20), puts of 16 MiB a second as the median of eight runs
[lowest–highest]:

| In flight | Before | After | After ÷ before | Device bytes a byte put |
| --- | --- | --- | --- | --- |
| One | 2.90 [2.77–2.93] | 2.93 [2.83–2.97] | 1.01, within | 5.8 → 3.1 |
| Sixteen | 4.60 [4.33–4.97] | 4.53 [4.37–4.73] | 0.99, within | 6.0 → 3.0 |

- **The device's bytes halved, to the 3.0 that three copies need.** The doubling is gone. At
  sixteen in flight the 970 EVOs went from 45 to 51% busy to 26%.
- **The rate is within.** europa's 1 GbE link bounds 16 MiB puts on this leg for every store, as the
  comparison found. The disk leg judges the rate, because there the device is the bound.

**SSDs over 1 GbE, 2+1** (2026-10-20):

| In flight | Before | After | After ÷ before | Device bytes a byte put |
| --- | --- | --- | --- | --- |
| One | 5.03 [4.67–5.13] | 4.88 [4.07–5.10] | 0.97, within | 2.9 → 1.6 |
| Sixteen | 6.98 [6.90–7.07] | 6.95 [6.90–7.03] | 1.00, within | 3.0 → 1.5 |

- **The device's bytes halved here too, to the 1.5 that 2+1 needs.** At sixteen in flight the 970
  EVOs went from 49% busy writing 107 MiB/s to 33% busy writing 54 MiB/s.
- **The rate is within**, for the same reason as with three copies.

**Disks, three copies** (2026-10-21), where the device is the bound:

| In flight | Before | After | After ÷ before | Device bytes a byte put |
| --- | --- | --- | --- | --- |
| One | 1.43 [1.03–1.57] | 1.28 [1.13–1.83] | 0.90, within | 5.5 → 3.1 |
| Sixteen | 2.65 [2.27–2.93] | 4.20 [3.83–4.57] | **1.58, ahead** | 6.1 → 3.1 |

Judged by [what would show it worked](#what-would-show-it-worked):
- **Device bytes toward 3.0 with three copies: yes.** They were 3.1 at both depths, down from 5.5
  and 6.1. At sixteen in flight each disk wrote 1.01 to 1.05 bytes a byte put, down from 2.0.
- **The after ranges above the before ranges, disjoint: yes at sixteen in flight, no at one.**
  - At sixteen in flight the disks were 97 to 99% busy on both sides, writing 66 to 68 MiB/s after
    and 79 before. So the rate rose with the bytes they no longer wrote.
  - At one in flight the first run of each after capture placed 1.77 and 1.83 objects a second,
    and the other three 1.13 to 1.33. In those three titan's disk also read 33 to 40 times a
    second, where in the first it read 9 to 13
    ([F96's page](../../features/chunk-file-classes.md#on-the-lab)).
- **The verdicts against the other stores.** As a guide and not a verdict, beside the comparison's
  own captures:
  - at sixteen in flight Shoal is within Ceph and SeaweedFS, where the comparison had it behind by
    at least 24% and 28%;
  - at one in flight it is behind Ceph by at least 14% and within SeaweedFS, where the comparison
    had it behind by at least 23% and 10%.
- **What must not get worse.** Nothing was lost, and every other arm of F96's on this leg was
  ahead or within.

**Disks, 2+1** (2026-10-21):

| In flight | Before | After | After ÷ before | Device bytes a byte put |
| --- | --- | --- | --- | --- |
| One | 2.82 [2.73–2.93] | 2.47 [2.33–3.00] | 0.88, within | 2.7 → 1.6 |
| Sixteen | 6.00 [5.80–6.27] | 6.87 [6.53–7.13] | **1.14, ahead** | 3.0 → 1.5 |

Judged by [what would show it worked](#what-would-show-it-worked):
- **Device bytes toward 1.5 at 2+1: yes.** They were 1.5 at sixteen in flight and 1.6 at one, down
  from 3.0 and 2.7. At sixteen in flight each disk wrote 0.50 bytes a byte put, down from 1.0.
- **The after ranges above the before ranges, disjoint: yes at sixteen in flight, no at one.**
  - At sixteen in flight the disks were 86 to 94% busy after, where they were 96 to 97%.
  - At one in flight the first run of each after capture placed 2.93 and 3.00 objects a second,
    and the other three 2.33 to 2.57. In those three titan's disk also read 28 to 35 times a second,
    where in the first it read 7 to 10
    ([F96's page](../../features/chunk-file-classes.md#on-the-lab)).
- **The verdicts against the other stores.** As a guide and not a verdict, beside the comparison's
  own captures, Shoal is within or ahead of Ceph, MinIO and RustFS at both depths. The comparison had
  it behind all three at one in flight, by at least 4.7% to 9.9%, and behind RustFS at sixteen by at
  least 4.5%.
- **What must not get worse.** Nothing was lost, and every other arm of F96's on this leg was
  ahead or within.

## What it could cost

- A recycled file holds a chunk's old bytes until it is overwritten. That matters only to someone
  reading the device raw. The bytes never verify under another chunk's identity (P15).
- A rename that keeps the old inode needs a link before the rename and a directory sync after.

## Related

[O102](../../appendix/optimizations.md#o102-a-whole-chunk-costs-its-device-twice-its-bytes);
[A short stripe writes a whole chunk](short-stripes.md), whose refill is the same write;
[S6. The device store](../device-store.md).
