# F105. A read answered where the device put it

Since this feature a slice reads a range's units as one run of its chunk on every device, and
checks each unit where the device put it. When every unit came from the chunk's file, the read's
answer is those buffers, with nothing copied on the executor. A range that a record, a kept
previous state or a unit holding no bytes gives any part of is laid out once, in a buffer of its
own. A local read's answer crosses to its shard, so it copies the range once, where it copied it
twice; since [F110](store-checks-in-place.md) it hands its shard the buffer it filled, and copies
nothing.

## Context

O104's diagnosis (2026-10-22) found a node's object executor the busiest thread of a 2+1 get on
one host, 77 to 85% of a cpu at sixteen in flight. Copies were a fifth of its time
([where the executor's time goes](../object-storage/store-optimizations/object-lane.md#where-the-executors-time-goes)).
The store's own two copies were about 12%, and every read paid them, with the lane or without it
([O127](../appendix/optimizations.md#o127-a-holder-copies-every-byte-it-reads-twice-before-it-leaves-the-store)):
- `read_exact` copied each unit out of the buffer glommio read it into, and its checksum was taken
  over the copy.
- `read_chain` copied the part of each unit the range covers into the range's own `Vec`.

On SSDs each unit was a read of its own, awaited before the next was asked for. On disks
[F91](rotational-devices.md) read the range in one span, but laid the span out in a `Vec` and moved
its tail down to cut the range. Nothing recorded why an SSD read a unit at a time: it was F85's
first form, kept as it was by F91.

The user chose both copy fixes, the store's first (2026-10-22).

## What it does

- **One run on every device.** The range's units that need the chunk's file are read as one run
  of pieces of at most 1 MiB, each a whole number of units, all in flight at once (`read_pieces`).
  A disk did this since F91, and an SSD does since F105. The pieces are cut from the run's start.
- **Each unit checked where it lies.** Its CRC-64/NVME is taken over the device's buffer, and a
  unit that fails is refused and its chunk set aside as before ([F89](scrub-and-repair.md)).
- **The answer, a `Span`:**
  - *in place*, the device's buffers cut to the range, when every unit came from the chunk's file;
  - *laid out*, one buffer the range is assembled into a unit at a time, when a record, a kept
    previous state or a unit holding no bytes gives any of it.

  A span holds its read's draw on the executor's budget until it is dropped.
- **Copied once to cross a thread.** A read that answers a shard (`Slice::read`, `read_any_as`)
  copies the span into a `Vec` a step at a time, yielding between steps; a span in place cannot
  leave its executor. A parity stage's read of its own units does the same.
- **The cache** keeps a copy of each unit a read verified, as before: never a device's buffer.

## Design choices

- **Every device alike.** A run of pieces in flight together is what made a disk's read fast, and
  an SSD answers the pieces at once where it answered units one after another. A range inside one
  unit is one read as before.
- **Pieces of whole units.** No unit straddles two pieces, so a unit is checked over one buffer.
- **The run spans what needs the file.** Units a record or memory gives, between ones the file
  gives, are read with them, since a device spends little more on them than on skipping them. A
  range no unit of which needs the file reads nothing from it.
- **In place only when nothing was laid over.** A device's buffer cannot be written, so a range
  with any unit from elsewhere is laid out once instead.
- **The draw travels with the span.** What a read holds is counted until the bytes it read are
  gone, not only until the slice answered.

## Alternatives rejected

- **A read into the range's own buffer**, so a local read would copy nothing. glommio reads into
  buffers of its own, and a buffer it does not own needs a read that takes it and gives it back,
  which the fork does not have. It is filed as part of
  [O129](../appendix/optimizations.md#o129-a-local-read-still-copies-its-range-once-to-cross-to-its-shard-and-scrubs-and-records-copy-theirs),
  and [F110](store-checks-in-place.md) built it: the fork reads into a buffer of the heap's that it
  gives back, and a local read hands that buffer over.
- **Answering a local read with the device's buffer.** A shard runs on another thread, and a
  glommio buffer is `!Send`; the one copy left is the crossing.
- **Keeping one read a unit on SSDs and only dropping the copies.** Each unit's read would still
  wait for the one before.

## Limitations

- ~~**A local read still copies its range once**, to cross to its shard
  ([O129](../appendix/optimizations.md#o129-a-local-read-still-copies-its-range-once-to-cross-to-its-shard-and-scrubs-and-records-copy-theirs)).~~
  Since [F110](store-checks-in-place.md) a local read hands its shard the buffer it filled.
- ~~**A served read still copies its range into the lane's body**, until the lane writes a frame from
  the span ([O104](../appendix/optimizations.md#o104-an-object-lane-copies-a-reads-bytes-twice-and-a-stages-once-more)).~~
  Since [F106](lane-copies.md) a served read's frame is written from the span.
- **A run reads units a record overlays**, which it lays over afterwards.
- **A run takes pieces of up to 1 MiB from the executor's registered buffers**, where a unit took
  64 KiB; past them, a read's buffers come from the heap.
- **`read_ios` counts a run as one**, on every device.
- ~~**Not yet measured on the lab.** Its before and after follows with the lane's copies.~~ Measured
  with F106 on 2026-10-23 ([Performance](#performance)).

## Invariants to uphold

- **Every unit a range covers is checked where it lies before any byte is answered.**
- **A span never changes after it is checked**: a device's buffer cannot be written, and a laid-out
  one is built only after every unit is checked.
- **A span holds its read's draw and never leaves its executor**: it is `!Send`.
- **The cache holds copies, never a device's buffer**, which would pin its registered memory.
- **The reads and the checks happen under the chunk's read lock**, as before.

## Performance

Measured on the lab on 2026-10-23 with F106 (`w2-loop-ec21`, `w2-loop-rep3` and `w2-ssd-ec21`,
under `shoal-spike/results/stores-ab/`), on two legs:
- europa's loopback, its three nodes on one host, at 2+1 and with three copies;
- the SSD leg, a node on each of europa, titan and hyperion, over 1 GbE, at 2+1.

How it was run:
- **Three builds**, each side a clean worktree at its commit with its node built in it:
  - *before*, F104's commit (`628c3681`), the build [F103's run](plaintext-peer-lanes.md#performance)
    measured;
  - *F105*, this feature's commit (`958742ba`);
  - *F106*, the commit of [F106](lane-copies.md)'s stage half (`f80e0da4`), which carries both.
- **The arms** are the comparison's gets and puts at 64 KiB and 1 MiB, one in flight and sixteen.
- **The order of the captures** was before, F105, F106, F106, F105, before, each on devices made
  fresh.
- **A side's figure** is the median of its eight runs, with the lowest and highest.
- **Every settle waited out its arm's reclamation**, 18 to 54 seconds after a put arm
  ([Resolved #301](../appendix/resolved/a-settle-waits-for-reclamation.md)).
- **On the loopback each node's threads were sampled** every second and read over each run's
  measured window (`w2-threads.txt`, and `w2-cpu.txt` by `stores-ab-cpu.py`, beside the captures).
- **Nothing was lost.** The check at each capture's end read 2,048 objects back.

This page has F105's step, before to F105. F106's page has its own, and
[O104's page](../object-storage/store-optimizations/object-lane.md#before-and-after) the two
together.

### On europa's loopback

Its devices are SSDs to the store. F105 changes only reads, and every put was within, at 2+1 and
with three copies. The gets, operations a second:

| Copies | Size | In flight | Before | F105 | F105 ÷ before | p50 ms | p99 ms |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 2+1 | 64 KiB | 1 | 1,696 [1,689–1,717] | 1,666 [1,644–1,712] | 0.98, within | 0.59 → 0.60 | 0.76 → 0.77 |
| | | 16 | 13,230 [13,163–13,358] | 13,392 [13,287–13,537] | 1.01, within | 1.19 → 1.17 | 1.86 → 1.81 |
| | 1 MiB | 1 | 434 [429–441] | 519 [509–526] | **1.19, ahead** | 2.35 → 1.99 | 2.80 → 2.39 |
| | | 16 | 1,800 [1,779–1,824] | 2,117 [2,089–2,148] | **1.18, ahead** | 8.79 → 7.46 | 14.3 → 12.6 |
| Three | 64 KiB | 1 | 1,933 [1,929–1,959] | 1,963 [1,929–1,989] | 1.02, within | 0.51 → 0.50 | 0.65 → 0.65 |
| | | 16 | 14,725 [14,648–14,808] | 14,817 [14,713–14,947] | 1.01, within | 1.06 → 1.05 | 1.75 → 1.73 |
| | 1 MiB | 1 | 559 [555–564] | 732 [719–739] | **1.31, ahead** | 1.70 → 1.29 | 2.06 → 1.56 |
| | | 16 | 2,412 [2,388–2,440] | 2,581 [2,579–2,588] | **1.07, ahead** | 6.56 → 6.15 | 10.5 → 8.79 |

The cpu a GiB the gets moved cost, in cpu-seconds, before → F105, with the ratio:

| Copies | Size | In flight | Nodes | Object executors | Shards |
| --- | --- | --- | --- | --- | --- |
| 2+1 | 64 KiB | 1 | 16.1 → 16.2 (1.01) | 3.53 → 3.58 (1.01) | 12.3 → 12.4 (1.01) |
| | | 16 | 11.6 → 11.6 (1.00) | 2.66 → 2.68 (1.01) | 8.82 → 8.82 (1.00) |
| | 1 MiB | 1 | 2.92 → 2.51 (0.86) | 1.36 → 1.05 (0.78) | 1.48 → 1.42 (0.96) |
| | | 16 | 2.99 → 2.77 (0.93) | 1.37 → 1.16 (0.85) | 1.57 → 1.59 (1.01) |
| Three | 64 KiB | 1 | 13.8 → 13.7 (0.99) | 1.74 → 1.74 (1.00) | 11.8 → 11.7 (0.99) |
| | | 16 | 9.61 → 9.63 (1.00) | 1.26 → 1.25 (0.99) | 8.29 → 8.32 (1.00) |
| | 1 MiB | 1 | 1.88 → 1.42 (0.76) | 0.55 → 0.22 (0.40) | 1.28 → 1.16 (0.91) |
| | | 16 | 1.84 → 1.44 (0.78) | 0.53 → 0.25 (0.47) | 1.29 → 1.17 (0.91) |

What this says:
- **The 1 MiB gets ran 7 to 31% faster**, every range apart. A get of 1 MiB reads sixteen units.
  Before F105 an SSD's slice read them one after another and copied every byte twice. Since F105 it
  reads them as one piece, checks each unit in place, and copies the range once.
- **Their object executors spent 15 to 60% less cpu a GiB.** The cut was largest with three copies,
  0.40 and 0.47 of before: there no get's bytes cross a lane, so the store's read is more of what
  an executor does for a get.
- **With three copies at sixteen in flight the devices bound the gets.** Each device was 93 to 99%
  busy, where it was 84 to 91%, and the nodes used 3.64 cpus where they used 4.34, for 7% more
  gets. At 2+1 each device was 56 to 66% busy at most.
- **The 64 KiB gets were within**, in cpu as in rate. A 64 KiB object is one unit, a read of its
  own before F105 too, and the copy F105 saves is a small part of what its get costs.

### On the SSD leg

A node on each host, over 1 GbE, at 2+1. The bench's client is on europa, so a get's bytes cross
europa's link unless europa's own node answers it. Every put was within. The gets, operations a
second:

| Size | In flight | Before | F105 | F105 ÷ before | p50 ms | p99 ms |
| --- | --- | --- | --- | --- | --- | --- |
| 64 KiB | 1 | 413 [392–432] | 428 [403–450] | 1.03, within | 2.38 → 2.33 | 3.80 → 3.68 |
| | 16 | 1,985 [1,950–2,026] | 2,043 [1,985–2,103] | 1.03, within | 8.09 → 7.89 | 14.3 → 13.8 |
| 1 MiB | 1 | 62.3 [60.1–64.1] | 71.8 [69.0–73.6] | **1.15, ahead** | 14.8 → 13.0 | 26.7 → 24.8 |
| | 16 | 132 [130–134] | 136 [131–139] | 1.03, within | 95.6 → 86.8 | 413 → 428 |

No thread was sampled on this leg. From each capture's own counters, the three nodes' processes
spent less cpu a GiB on the 1 MiB gets, before → F105:
- at one in flight, 26.0 → 22.0 cpu-seconds (0.85);
- at sixteen, 17.1 → 15.8 (0.92);
- each with its ranges apart.

On the 64 KiB gets and the puts it was within, 0.98 to 1.00.

Each capture is a deployment of its own, and each draws again which node coordinates which gets.
The share europa's own node answered moved the gets as much as either build did. That share is
the part of the client's bytes that came over europa's loopback, not its link:

| Capture | Build | Answered by europa's node | 1 MiB gets a second, one in flight | Sixteen in flight | Europa's link received, MiB/s, sixteen in flight |
| --- | --- | --- | --- | --- | --- |
| 1 | Before | 39% | 60.5 [60.1–61.3] | 131.3 [130.4–131.4] | 111.9 |
| 2 | F105 | 47% | 69.5 [69.0–71.1] | 132.6 [131.4–133.6] | 111.8 |
| 3 | [F106](lane-copies.md) | 36% | 66.2 [65.3–66.2] | 128.4 [127.2–128.6] | 112.2 |
| 4 | F106 | 44% | 69.3 [68.9–69.7] | 128.6 [127.5–128.9] | 111.5 |
| 5 | F105 | 57% | 73.2 [72.6–73.6] | 138.2 [137.7–139.4] | 111.5 |
| 6 | Before | 49% | 64.1 [63.3–64.1] | 133.2 [132.9–133.7] | 111.4 |

What this says:
- **The 1 MiB gets at one in flight ran 15% faster across the captures, and 8% at a like share.**
  - F105's captures drew 47 and 57% of the gets to europa's own node, and the before's 39 and 49%.
  - On every build, the capture with more ran more gets.
  - Captures 2 and 6 drew alike. There F105 ran 1.08 times as many as the build before it, with
    the ranges apart, and its nodes spent 0.92 of the cpu a GiB.
- **At sixteen in flight the gets were europa's link's.**
  - The link received 111 to 112 MiB/s in every capture, so it was full on every side.
  - A capture ran as many gets as the bytes each one took across the link allowed.
  - The comparison found every store bound there
    ([where Shoal is better, and where worse](../object-storage/store-comparison.md#where-shoal-is-better-and-where-worse)).
- **The 64 KiB gets and the puts were within**, as on the loopback.

## Tests

| Test | What fails if the feature is reverted |
| --- | --- |
| `shoal-core` `objects::store::slice::tests::a_range_is_read_as_one_run` | A read of three units of a chunk, across a staged unit, is one read on an SSD as on a disk; F91's version asserted three on an SSD |
| `shoal-core` `objects::store::slice::tests::a_span_is_answered_where_the_device_put_it` | A range the file holds is answered in place, one reaching a unit that holds no bytes or a staged unit is laid out, every answer the label's bytes, and a unit flipped in the middle of a range is refused `Corrupt` and its chunk set aside, on both media |
| `shoal-core` `objects::store::slice::tests::a_run_of_several_pieces_is_answered_whole` | A range longer than a piece, seventeen units of 64 KiB, is one run of two pieces cut from the run's start, each in place, and reads back whole, on both media |
| `shoal-core` `objects::store::slice::tests::a_held_span_keeps_its_draw` | A span holds its read's draw on the budget until it is dropped |

## Related

- [O127](../appendix/optimizations.md#o127-a-holder-copies-every-byte-it-reads-twice-before-it-leaves-the-store);
  [A 2+1 get crosses the object lane](../object-storage/store-optimizations/object-lane.md).
- [F91](rotational-devices.md), whose span every device reads now; [F102](read-unit-cache.md), whose
  cache keeps copies of what a run verified.
- [F106](lane-copies.md), which writes a served read's frame from the span (O104's read half).
