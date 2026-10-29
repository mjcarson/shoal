# F98. A commit delay bounded by the sync

Since this feature the WAL writer's commit delay, `cluster.replication.wal_commit_delay`, is the
most it waits after a sync before it takes the next batch. It waits no longer than that batch took
to write and sync. A device that syncs in a fraction of a millisecond is never held for the whole
delay, and a slow one still groups the appends that arrive while it waits.

## Context

[O61](../appendix/optimizations.md#o61-a-fast-device-syncs-the-wal-in-batches-too-small-to-fill-a-page) gave the WAL writer a wait after each
sync. Appends that arrive during it join the next batch, so a device syncs fewer, larger batches.
The lab's inventories set it at 3 ms on europa and 2 ms on titan and hyperion. The store comparison
then found Shoal's 4 KiB puts on europa's loopback behind Ceph by at least 42% to 2.0 times, at a
p50 of 2.4 ms against 1.0
([The lab's group commit delay](../object-storage/store-optimizations/group-commit-delay.md)).

The delay was measured beside none on two legs:
- **On the SSD leg** no delay ran small puts no faster, and each NVMe flushed 8 to 18% more an
  operation ([On the SSDs](../object-storage/store-optimizations/group-commit-delay.md#on-the-ssds)).
  Titan's and hyperion's 970 EVOs take milliseconds to flush.
- **On europa's loopback** no delay ran 4 KiB puts 3.4 times as fast at one in flight and 2.6 times
  at sixteen, for 17 to 44% more device bytes
  ([On europa's loopback](../object-storage/store-optimizations/group-commit-delay.md#on-europas-loopback)).
  Every node there is on the Optane, and the 3 ms each waited after a sync was most of a put.

F98 was filed as a delay taken only while appends are queued. That would match no delay at one in
flight, but keep the whole delay at sixteen, where appends are always queued. On 2026-10-21 the
user chose a delay bounded by the sync instead.

## What it does

In `writer` (`server/wal/mod.rs`) a batch's write and its sync are timed together, as they were for
the WAL's figures. After the batch the writer sleeps for the shorter of `wal_commit_delay` and that
time, then takes the next batch:
- **a device whose batches take less than the delay** waits as long as a batch took, so the next
  batch gathers what arrives in about one more batch's time;
- **a device whose batches take longer than the delay** waits the whole delay, as before;
- **no delay**, the engine's default, still waits nothing.

The setting keeps its name, its levels and its bound of 10 ms. What it means changes, from the wait
to the most the writer waits.

## Design choices

- **Bounded by the batch's own time.** A batch's write and sync are what the device costs it.
  Waiting about that long lets the next batch gather about as many appends again, which is the
  grouping O61 was after. Waiting longer than a sync only adds that time to every write behind it.
- **The last batch's time, not an average.** It follows the device as its load changes. The 970
  EVO's flush, for one, has two regimes, 0.9 ms rested and 3 ms after synced writes
  ([X6](../object-storage/device-store-ssd.md)). It costs nothing to keep.
- **A bound on the setting, not a new one.** Every deployment that sets a delay gets the bound, and
  a bound can only shorten the wait, never lengthen it.
- **Only a writer that has just synced waits**, as O61 has it: an append after an idle spell is
  still written at once.

## Alternatives rejected

- **No delay.** That is the engine's default already. The lab set its delays to save device bytes,
  and on the SSDs no delay cost 8 to 18% more flushes an operation and ran no faster.
- **A delay taken only while appends are queued**, F98 as filed. It matches no delay at one in
  flight and keeps the whole delay at sixteen, where the loopback lost 2.6 times to it.
- **An average of past syncs.** It would follow a device's change of regime late, and a fixed weight
  is one more number to choose.
- **A delay set by the device's class.** One figure for every SSD guesses what a device does rather
  than measuring it, and the Optane and the 970 EVO are both `ssd`.

## Limitations

- **A batch's time includes its write.** A large batch on a fast device waits longer than its sync
  alone would.
- **Each shard's writer judges its own batches.** Shards that share a device each wait their own
  time.
- ~~**Not yet measured on the lab.** Its before and after on the SSD and loop legs follows.~~
  Measured on both legs on 2026-10-22 ([Performance](#performance)).
- **A fast device among slow ones writes more for nothing.** On the SSD leg europa's Optane waited
  less than the 970 EVOs beside it. At sixteen in flight it wrote about a fifth more a byte put,
  while every commit still waited on a 970 EVO.
- **It is a little behind no delay at one in flight**: 7% for 4 KiB puts on europa's loopback. An
  append that arrives while its writer waits out the last batch's time waits for the rest of it.
- **It gives back part of the wear the delay saved**: half of it at sixteen in flight on the
  loopback, and nearly all of it at one in flight.

## Invariants to uphold

- **The writer never waits longer than `wal_commit_delay`, and never longer than its last batch
  took to write and sync.**
- **Only a writer that has just synced waits.** An append after an idle spell is written at once.
- **The wait only gathers appends.** It moves nothing that is durable, and an append's answer still
  waits for the sync of its batch.

## Performance

Measured on the lab's SSD and loop legs on 2026-10-22, with three copies (`f98-bound-ssd-rep3` and
`f98-bound-loop-rep3`, under `shoal-spike/results/stores-ab/`).
- **The sides.**
  - *Before* is F98's parent with the lab's delays: 3 ms on europa and 2 ms on titan and hyperion,
    and 3 ms on every node of the loop leg.
  - *After* is F98 with the same delays.
  - *None* is F98 with `wal_commit_delay: 0ms`, the reference O61 was measured against.
- **The arms** are the store comparison's 4 KiB puts and mixture, at one in flight and sixteen. A
  4 KiB object is a row at three copies and at 2+1 alike
  ([On the SSDs](../object-storage/store-optimizations/group-commit-delay.md#on-the-ssds)), so one
  group was measured.
- **The captures** ran in the order before, after, none, none, after, before, each on devices made
  fresh.
- **Every figure** is the median of eight runs, with the lowest and highest.
- **Nothing was lost.** The check at each capture's end read 1,024 objects back.

On europa's loopback:

| Arm | In flight | Before, ops/s | After | After ÷ before | None | After ÷ none | Puts' p50 ms, before → after → none | Puts' p99 ms |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Puts, 4 KiB | 1 | 472 [467–474] | 1,491 [1,387–1,507] | **3.16, ahead** | 1,597 [1,513–1,610] | 0.93, behind | 2.42 → 0.64 → 0.60 | 3.91 → 1.03 → 0.90 |
| | 16 | 3,334 [3,298–3,339] | 8,456 [8,356–8,779] | **2.54, ahead** | 8,753 [8,631–9,075] | 0.97, within | 4.92 → 1.73 → 1.68 | 8.01 → 7.11 → 6.58 |
| Mixture, 4 KiB | 1 | 916 [896–924] | 2,398 [2,369–2,432] | **2.62, ahead** | 2,476 [2,444–2,495] | 0.97, behind | 2.05 → 0.60 → 0.59 | 3.82 → 0.96 → 0.84 |
| | 16 | 5,893 [5,882–5,916] | 12,745 [12,611–12,856] | **2.16, ahead** | 13,053 [12,853–13,227] | 0.98, within | 4.55 → 1.74 → 1.66 | 7.20 → 5.70 → 5.38 |

What each side wrote: every loop device's bytes, over every byte the arm put.

| Arm | In flight | Before | After | None | Of what the delay saved, F98 keeps |
| --- | --- | --- | --- | --- | --- |
| Puts, 4 KiB | 1 | 13.6 | 16.0 | 16.2 | 9% |
| | 16 | 8.50 | 9.87 | 11.5 | 54% |
| Mixture, 4 KiB | 1 | 13.8 | 16.5 | 16.5 | none |
| | 16 | 8.78 | 10.6 | 12.5 | 50% |

On the SSD leg, every arm's three sides fell within each other's ranges. The device bytes are a
byte put: every host's devices together, then europa's alone as the median of its runs.

| Arm | In flight | Before, ops/s | After | None | Device bytes a byte put, before → after → none | Europa's Optane alone |
| --- | --- | --- | --- | --- | --- | --- |
| Puts, 4 KiB | 1 | 180 [154–181] | 179 [156–181] | 171 [155–172] | 13.1 → 13.5 → 14.4 | 3.80 → 3.81 → 4.28 |
| | 16 | 1,236 [1,210–1,268] | 1,222 [1,211–1,258] | 1,240 [1,171–1,252] | 10.1 → 11.0 → 11.3 | **3.97 → 4.83** → 4.93 |
| Mixture, 4 KiB | 1 | 323 [320–327] | 323 [318–326] | 318 [313–323] | 14.8 → 14.3 → 14.6 | 5.07 → 4.81 → 4.76 |
| | 16 | 2,300 [2,246–2,339] | 2,282 [2,233–2,306] | 2,292 [2,270–2,307] | 10.2 → 11.0 → 11.4 | **4.13 → 4.87** → 4.89 |

What this says:
- **On europa's loopback F98 takes back what the delay cost.** 4 KiB puts ran 3.2 times as fast at
  one in flight and 2.5 times at sixteen, and the mixture 2.6 and 2.2 times. Every range is
  disjoint. A put's p50 at one in flight fell from 2.42 ms to 0.64.
- **It is within 7% of no delay.** At sixteen in flight the ranges overlap. At one in flight F98
  is 7% behind for puts and 3% for the mixture, the ranges apart by a few operations a second. The
  two sides ran one build, so that is the wait F98 keeps: an append that arrives while its writer
  waits out the last batch's time waits for the rest of it.
- **It keeps half of what the delay saved at sixteen in flight**, and almost none at one in flight.
- **On the SSDs nothing ran faster or slower.** A batch on titan's or hyperion's 970 EVO takes
  longer to write and sync than their 2 ms delay, so they wait as long as before. Europa's Optane
  takes less, so it waits less. At sixteen in flight it wrote 22% more a byte put, and 18% more in
  the mixture, the ranges disjoint. Every commit there waits on a 970 EVO, its leader's own sync or
  a follower's, so europa's shorter wait bought nothing.
- **Beside the comparison's own captures**, which ran on another day, F98's 4 KiB puts on the
  loopback move against Ceph:
  - from behind it by at least 2.0 times at one in flight to ahead by at least 37%;
  - from behind by at least 46% at sixteen to ahead by at least 29%.

  Against SeaweedFS they move from behind by at least 2.2 and 2.7 times to ahead by at least 19% at
  one in flight and behind by at least 4.5% at sixteen. On the SSDs no verdict changed.
- **In the comparison's own second run** (2026-10-25 and 26,
  [Shoal measured again](../object-storage/store-comparison.md#shoal-measured-again)) they lead Ceph
  by at least 38% and 45% with three copies, and SeaweedFS by 20% at one in flight, within it at
  sixteen.

## Tests

| Test | What fails if the feature is reverted |
| --- | --- |
| `shoal-core` `wal::tests::a_commit_delay_waits_no_longer_than_the_batch_took` | With a 200 ms delay, ten appends, each made once the one before is durable, each become durable in under 100 ms. Before F98 each waited the delay out after the sync before it: with the bound taken out, *an append behind a 200 ms commit delay took 200.203461ms to become durable* |
| `shoal-core` `wal::tests::a_commit_delay_groups_appends_into_fewer_syncs` | O61's test, now on a device a test holds 4 ms a sync: a 5 ms delay still takes at most two thirds the syncs no delay does. On a device as fast as the test's own, the bound leaves little to group |

## Related

- [O61](../appendix/optimizations.md#o61-a-fast-device-syncs-the-wal-in-batches-too-small-to-fill-a-page), the commit delay, and its page,
  [The lab's group commit delay](../object-storage/store-optimizations/group-commit-delay.md).
- [F57](cluster-reconfigure.md), the inventory's key for it.
- [The store comparison](../object-storage/store-comparison.md), whose loopback leg found the gap.
