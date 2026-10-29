# F97. Reads from copies on disks, by their queues

~~Since this feature a read of a stripe from a pool of copies on rotational devices goes to the
current holder with the fewest reads queued on its slice.~~ Since this feature a read of a stripe from
a pool of copies on rotational devices can go to the current holder with the fewest reads queued on
its slice: since [F101](disk-read-margin.md) ~~only on a node that sets `disk_read_margin`, and~~ only for a
queue shorter by more than the node's `disk_read_margin`, four unless the node sets it, and never
where it sets it `null`. Before it went to the coordinator's own copy first. A copy on the coordinator's node still wins a tie, so a read that finds every disk idle
stays local. A pool on SSDs reads its local copy first, as before. It answers
[O119](../appendix/optimizations.md#o119-a-get-from-a-pool-of-copies-reads-its-coordinators-own-copy-so-each-disk-serves-the-gets-its-node-leads),
the third of the optimizations [the store comparison](../object-storage/store-optimizations.md)
ranked.

## Context

A get is coordinated by the leader of its path row's group. That coordinator tried the positions
the stripe's row calls current with its own first (`read_holders`, `shard/objects/read.rs`). With
three copies over three nodes every node holds every stripe, so every get was read off its
coordinator's own disk. Each disk then served its node's share of the bucket's leaders, whatever
its queue. On the comparison's HDD leg europa led the most:
- **at 64 KiB and sixteen in flight**, europa's disk was 95% busy and titan's and hyperion's 53 to
  54%;
- **Shoal read 540 to 595 gets a second**, behind Ceph and SeaweedFS by at least 3.4 and 3.7 times
  ([Reads from a pool of copies follow leadership](../object-storage/store-optimizations/read-placement.md)).

At 2+1, where a short object's data chunk lies wherever placement put it, all three disks were busy
and Shoal read more than twice as many.

The user chose a choice by load, on disks only, before it was built. On the SSD leg the local copy
read beside a 1 GbE link is what put Shoal ahead of every store at large gets, and a choice that
left it would lose that.

## What it does

`read_holders` orders the positions the row calls current, then asks each in turn until one answers
with the bytes, as before. ~~On a pool whose media is rotational (`PoolSpec::is_rotational`) the
order is by three keys:~~ On a pool whose media is rotational (`PoolSpec::is_rotational`), ~~on a node
that sets `disk_read_margin`~~ unless the node's `disk_read_margin` is `null` ([F101](disk-read-margin.md)),
the order is by three keys, each remote holder's queue carrying the margin, four by default:

1. **The reads queued on the holder's slice** (`ObjectStore::queue_of`):
   - for a slice of this node's, its foreground requests in flight
     (`SliceState::foreground`), which counts every node's reads and stages on it, and since
     [Resolved #314](../appendix/resolved/local-reads-counted-at-choice.md) never fewer than the reads this node chose it for and has not had
     answered (`SliceState::chosen`, `SliceState::queue`): a request joins the foreground only once
     the slice's executor takes it from its mailbox, which a loaded host does late, and a burst of
     reads that chose before then each saw an empty queue and stayed on one disk;
   - for another node's slice, the reads this node has outstanding to it. `ObjectStore::read_from`
     counts a read against the slice while it is outstanding, and its guard (`Outstanding`) takes
     it back however the read ends, a cancelled one included.
2. **This node's copy first**, among holders that tie.
3. **Remote holders that tie are taken in turn** (`ObjectStore::next_turn`), each choice starting
   one further on.

A pool on SSDs keeps the order it had: this node's copy first, then the others.

## Design choices

- **Disks only**, the user's choice. A disk's read waits for its queue, often longer than a hop to
  an idle disk. An SSD's read is cheaper than the hop, and the comparison found no gap there for
  this cause.
- **A slice's own count for a local copy, this node's count for a remote one.** A coordinator knows
  every request in flight on its own slices and no report carries another node's. Its own reads to
  a remote slice are what it adds to that slice's queue, which it can count without asking.
- **A tie goes to the local copy.** An idle read never crosses a link it does not need to, and on
  disks it still meets its node's own copy first. The rows that rely on a read meeting that copy,
  `flipped_bit_is_found_by_a_read_and_by_a_deep_scrub` and
  `a_device_past_its_checksum_drain_threshold_is_drained`, read pools on SSDs, which F97 leaves as
  they were.
- **Turns among remote ties**, so that two idle remote disks share the reads a busy local one
  sheds.

## Alternatives rejected

- **Every read from every holder, the first answer taken** (a hedged read). It doubles every
  disk's reads at the point where the disks are the bound.
- **A holder chosen by turn alone**, as O119's simpler form had it. It spreads the reads but ignores
  a slow disk, which keeps its share whatever its queue, and it leaves an idle local copy for a
  remote one.
- **Asking each holder its queue.** A round trip for every read, to save one.
- **The same choice on SSDs.** Over 1 GbE a local copy is worth more than an idle remote one, and
  nothing measured says otherwise.

## Limitations

- **A remote queue is this node's view of it.** Two coordinators that each see an idle remote disk
  may both send to it. Only a slice's own node sees every request on it.
- **The queue counts the foreground alone.** An apply runs below the foreground and is counted as
  `applying` ([F91](rotational-devices.md)), and a rebuild, a move and a scrub are background, so a
  disk busy applying or rebuilding looks idle to a read's choice.
- **A queue sees the disks, never the links.** A read moved to a remote disk crosses a link that
  a local read does not. Where a link bounds the reads, moving them loses. On the lab's disk leg,
  with the bench's driver on europa and F96 in, reads by queue were 28% and 11% slower at sixteen
  in flight ([Performance](#performance)).
- **Pools of copies only.** An erasure coded read takes the data positions it needs, and has no
  choice among copies to make.
- **A read refused or failed moves to the next holder in the order**, as before: a holder that
  fails a read keeps its place for the next choice, since nothing marks it slow.

## Invariants to uphold

- **A read whose disks are all idle is served by its coordinator's copy**, when that copy is
  current.
- **Every remote read counted is uncounted however it ends**: a count is a guard's, never a pair of
  calls.
- **The order only reorders**: a read asks only the positions the row calls current, as before.

## Performance

Measured on the lab's disk leg with three copies on 2026-10-21 (`f97-hdd-rep3`, under
`shoal-spike/results/stores-ab/`).
- **The sides** were F96's commit (`af3a190b`) and this one (`943bd22b`), each node built at its
  commit.
- **`stores-lab.sh ab`** took four captures of the comparison's gets, in the order before, after,
  after, before, each on the leg's devices made fresh.
- **A side's figure** is the median of its eight runs, with the lowest and highest beside it.

Gets a second:

| Arm | Before | After | After ÷ before | p99 ms |
| --- | --- | --- | --- | --- |
| 64 KiB, one in flight | 475 [443–512] | 462 [427–485] | 0.97, within | 3.86 → 3.92 |
| 64 KiB, sixteen | 3,254 [3,037–3,474] | 2,335 [2,239–2,427] | **0.72, behind** | 11.5 → 14.0 |
| 1 MiB, one | 54.0 [50.7–55.4] | 50.5 [48.8–51.5] | 0.93, within | 43.7 → 58.0 |
| 1 MiB, sixteen | 181 [171–193] | 160 [158–162] | **0.89, behind** | 439 → 270 |

- **At sixteen in flight, reads by queue were slower, and the ranges are disjoint**: by 28% at
  64 KiB and 11% at 1 MiB. At one in flight both sizes are within. At 1 MiB and sixteen in flight
  the p99 fell from 439 to 270 ms, and the p50 rose from 47 to 97 ms.
- **The reads spread, as designed.** At 1 MiB and sixteen in flight europa's disk served 40% of the
  bytes read, where it had served 56%. It was 87% busy, where it had been 96%, and titan's and
  hyperion's went from 28 and 34% busy to 50%. At 64 KiB europa's share fell from 58% to 48%.
- **europa's link bounds the arm.** The bench's driver runs on europa, as it did for every store
  in the comparison. Every byte a get serves crosses into europa over its 1 GbE link, except the
  bytes of a get europa both coordinates and reads off its own disk. Reads by queue moved some of
  those to the other disks.
  - europa's link received 0.74 to 0.76 bytes a byte served, where it had received 0.47 to 0.50.
  - At 1 MiB it received 111 to 112 MiB/s, a 1 GbE link's ceiling. Before, it received 79 MiB/s,
    and europa's disk was the bound.
  - At 64 KiB it received 101 to 105 MiB/s, where it had received 97. europa's disk had been only
    47% busy, so moving reads off it took the link's room for nothing.
- **F96 had already done most of what F97 was for.** The comparison found europa's disk 95% busy
  for 64 KiB gets at sixteen in flight. In this before side, at F96's commit, the same disk was 47%
  busy at 3,254 gets a second, since the objects' files are 68 KiB rather than 4 MiB
  ([F96's figures](chunk-file-classes.md#on-the-lab)).
- **Nothing was lost.** The check at each capture's end read 2,048 objects back. None were lost.
- **The SSD leg is unchanged by construction**: a pool on SSDs reads its local copy first, as
  before. It was not measured again.

Judged by [what would show it worked](../object-storage/store-optimizations/read-placement.md#what-would-show-it-worked):
- **Every disk's busy share within a few points of the others': closer, not within.** At 1 MiB
  and sixteen in flight europa's disk was 87% busy beside the others' 50%.
- **The get rate at sixteen in flight above the before side's, disjoint: no.** It was below it.
- **The drift between captures shrank.** The before side's two captures were 13% and 8% apart at
  sixteen in flight, the after side's 7% and 0.4%. That is because europa's link bounds the after
  side.

~~Whether reads by queue stay a disk pool's way is the user's choice, made with these figures.~~
The user chose a margin, measured (2026-10-21): [F101](disk-read-margin.md) reads the local copy first
unless a node sets one.

## Tests

| Test | What fails if the feature is reverted |
| --- | --- |
| `shoal` `cluster_fixture` `copies_on_disks_are_read_by_queue` | On a pool of copies on disks, a lone read through node zero is read off its own disk, and with node zero's arm slowed sixteen reads through it at once are read off every node's disk too. Before F97 node zero's disk served them all |

## Related

- [Reads from a pool of copies follow leadership](../object-storage/store-optimizations/read-placement.md),
  the comparison's page, and
  [O119](../appendix/optimizations.md#o119-a-get-from-a-pool-of-copies-reads-its-coordinators-own-copy-so-each-disk-serves-the-gets-its-node-leads).
- [F86. Replicated pools](replicated-pools.md), whose read path this orders.
- [F91. Rotational devices](rotational-devices.md), whose `SLOW_ARM` the test slows a disk with.
- [S9. The read path](../object-storage/read-path.md).
