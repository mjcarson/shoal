# F101. A margin before a read leaves its local disk

Since this feature a read of a pool of copies on disks leaves its coordinator's own copy only for a
holder whose queue is shorter by more than a margin, `storage.object_timings.disk_read_margin`,
four unless the node sets another. ~~Unset~~ Set `null`, the coordinator's own copy is read first
whatever its queue, as before F97. [F97](disk-reads-by-queue.md)'s choice by queue alone is a margin of zero.

## Context

[F97](disk-reads-by-queue.md) sent a read of a pool of copies on disks to the holder with the fewest
reads queued, so that a disk whose node leads the most groups no longer served every read they lead
([O119](../appendix/optimizations.md#o119-a-get-from-a-pool-of-copies-reads-its-coordinators-own-copy-so-each-disk-serves-the-gets-its-node-leads)).
On the lab's disk leg, once [F96](chunk-file-classes.md#on-the-lab) was in, it was slower at sixteen
in flight, by 28% at 64 KiB and 11% at 1 MiB ([F97's figures](disk-reads-by-queue.md#performance)):
- **the reads it moved crossed into europa over its 1 GbE link**, where the bench's driver runs, and
  the link then bounded the arm;
- **F96 had already taken away most of the queue it was for.** europa's disk was 47% busy for 64 KiB
  gets at sixteen in flight before F97, where the comparison found it 95% busy.

O119's entry named the tradeoff when it was filed: a remote read crosses a link a local one does
not, so the choice wants the local copy while its slice keeps up, and another holder once its queue
costs more than the link. F97 compared queues alone. On 2026-10-21 the user chose to try a margin
and measure it: a read leaves its local disk only for a queue shorter by more than the margin.

## What it does

`storage.object_timings.disk_read_margin` is a node's own setting, read when it starts. It changes
how the node's coordinators order the holders of a stripe on a pool of copies on disks
(`PoolSpec::is_rotational`), in `read_holders` (`shard/objects/read.rs`):
- ~~**Unset, the default**~~ **`null`**: a copy on this node first, then the others, as on a pool on
  SSDs and as before F97.
- **A margin, four unless set** (the default since 2026-10-21): by the reads queued on each holder's disk (`ObjectStore::queue_of`, F97's
  count), once each remote holder's queue carries the margin (`order_by_queue`). A copy on this
  node goes first on a tie, and remote holders that tie are taken in turn. So a read leaves its local
  disk only for a remote queue shorter by more than the margin. A margin of zero is F97's choice by
  queue alone.

An inventory names it in its `object_timings` block, which every node's file carries.
`stores-lab.sh ab` takes it for one side as `AB_<side>_MARGIN`.

## Design choices

- ~~**Unset reads the local copy first.** That is what measured faster on the lab once F96 was in. The
  margin is measured beside it, and the default changes only with figures, the user's choice.~~
  **A margin of four by default**, the user's choice of 2026-10-21 with the figures under
  [Performance](#performance). It moved one read in seventy, changed no rate, and shortened the
  tail of 1 MiB reads at sixteen in flight at a higher median. `null` keeps the local copy first.
- **A count of reads, not a time.** The queue F97 counts is reads in flight. The margin is in the
  same unit, so the choice stays one comparison: a remote queue plus the margin against the local
  queue. What a link costs in reads depends on the read's size and the link's load, which nothing on
  the node measures.
- **A node's setting, not a pool's.** The choice is the coordinating node's, made from what it
  counts. A pool's keys are committed policy that every node reads, and a node built before this
  feature could not read a pool that named a new one. `object_timings` is already a node's own,
  and an inventory already renders it to every node.
- **The margin on the remote holders' queues.** Added to each remote queue, it leaves the remote
  holders' order among themselves F97's, and the local copy's tie rule as it was.
- **On disks only, as F97.** A pool on SSDs reads its local copy first, set or unset.

## Alternatives rejected

- **Reverting F97.** It loses the choice for a deployment whose disks, not its links, bound its
  reads, and leaves nothing to measure a margin with.
- **F97 as a setting, off by default, with no margin.** A margin of zero is that, and it lost on the
  lab. A margin is the candidate that might not.
- **A margin in the pool's policy.** Every node would read it alike. But a pool's keys are committed
  and judged by every node's build, so a node built before this feature would refuse the pool.
- **Weighing the link itself.** A coordinator would need each link's load and each read's bytes,
  which nothing reports. The margin stands in for them as one number.
- **Every read from every holder, the first answer taken.** As F97 found, it doubles every disk's
  reads where the disks are the bound.

## Limitations

- **One margin for every read, whatever its size.** A 1 MiB read's place in a queue costs more than
  a 64 KiB read's, and the margin counts both alike.
- **A queue still sees the disks, never the links.** The margin only says how much queue a link is
  worth, and the right margin depends on the links and the load.
- **A node's own**, so nodes may set different margins. Each coordinator judges by its own.
- **Read when a node starts**, as every `object_timings` key is.
- **A node built before this feature refuses the key**: `object_timings` denies unknown fields.
- **Pools of copies on disks only**, as F97.
- ~~**Its figures were taken while a local read was counted late.** Until
  [Resolved #314](../appendix/resolved/local-reads-counted-at-choice.md) this node's disk counted a read only once its executor took it, so in a
  burst the margin moved fewer reads than it says; the lab's 1 to 2 percent were measured so. A
  disk-leg A/B would show whether the figures move now that every read is counted from its choice.~~
  Measured again with every read counted from its choice (2026-10-27, [below](#performance)): no rate
  moved, and the margin now moves europa's reads at 1 MiB and sixteen in flight.

## Invariants to uphold

- **~~Unset~~ `null` is local first, exactly.** A node ~~that names no margin~~ whose margin is
  `null` orders the holders as a pool on SSDs does, and F97's counts take no part in the order.
- **The margin only reorders.** A read asks only the positions the row calls current, as before.
- **A read whose disks are all idle is served by its coordinator's copy**, margin or none.
- **Every remote read counted is uncounted however it ends**, by F97's guard.

## Performance

Measured on the lab's disk leg with three copies on 2026-10-21 (`f101-hdd-rep3`, under
`shoal-spike/results/stores-ab/`).
- **The sides** ran one build, this commit (`21466829`), with no margin (local) and with
  `disk_read_margin: 4` (m4).
- **The captures** ran in the order local, m4, m4, local, each on the leg's devices made fresh,
  with F97's arms.
- **A side's figure** is the median of its eight runs, with the lowest and highest beside it.

Gets a second:

| Arm | Local | Margin of four | M4 ÷ local | p99 ms |
| --- | --- | --- | --- | --- |
| 64 KiB, one in flight | 450 [417–456] | 498 [457–512] | 1.11, ahead | 4.14 → 3.85 |
| 64 KiB, sixteen | 2,797 [2,725–2,829] | 3,160 [3,093–3,203] | 1.13, ahead | 12.7 → 12.1 |
| 1 MiB, one | 50.4 [46.4–52.7] | 53.3 [50.7–55.5] | 1.06, within | 61.5 → 51.9 |
| 1 MiB, sixteen | 200 [196–203] | 198 [194–201] | 0.99, within | 300 → 229, lower |

**The margin of four behaved like the local copy first, and the sides differed by their draw.**
- **The margin seldom moved a read.** A read europa coordinates and another disk serves crosses
  into europa, and a read another node coordinates off europa's disk leaves it. At sixteen in flight
  europa's link received 99 and 97 MiB/s on the two sides and sent 4 and 6. So the margin moved one
  or two reads in a hundred.
- **One in flight shows the same gap, where no margin can act.** A read alone never finds a queue
  deeper than one, yet the m4 side ran 64 KiB gets 11% faster there too.
- **Every fresh deployment draws again which node leads which groups**
  ([O119](../appendix/optimizations.md#o119-a-get-from-a-pool-of-copies-reads-its-coordinators-own-copy-so-each-disk-serves-the-gets-its-node-leads)), and both m4 captures drew europa leading
  more of them. europa's disk served 54 to 57% of the bytes read in both, against 44 to 45% in both
  local captures.
- **The rate follows that share.** It did so in F97's captures too, where europa's 59% went with
  3,464 gets a second and its 50% with 3,062.

So this before and after cannot tell the margin from the draw, and on this lab the margin changed
next to nothing. Two captures a side are too few for read placement, whose figure moves with each
deployment's draw.

What it does show:
- **A margin of four loses nothing.** F97's margin of zero lost 28% at 64 KiB and sixteen in flight,
  its moved reads filling europa's link ([F97's figures](disk-reads-by-queue.md#performance)).
- **The 1 MiB gets' p99 at sixteen in flight fell from 300 to 229 ms**, the ranges disjoint.
- **Nothing was lost.** The check at each capture's end read 2,048 objects back.

**Again, with eight captures a side** (2026-10-21, `f101-hdd-rep3-draws`). The user chose to measure
again with the draw averaged out: the same build and sides, captured in the order local, m4, m4,
local four times over, sixteen fresh deployments, two runs an arm in each. Gets a second:

| Arm | Local | Margin of four | M4 ÷ local | p50 ms | p99 ms |
| --- | --- | --- | --- | --- | --- |
| 64 KiB, one in flight | 459 [398–499] | 460 [428–509] | 1.00, within | 1.79 → 2.13 | 5.61 → 5.18 |
| 64 KiB, sixteen | 3,076 [2,858–3,248] | 2,919 [2,520–3,184] | 0.95, within | 4.98 → 5.25 | 12.0 → 12.5 |
| 1 MiB, one | 50.2 [46.2–55.9] | 51.9 [47.9–55.9] | 1.03, within | 18.2 → 17.7 | 58.6 → 62.6 |
| 1 MiB, sixteen | 196 [169–212] | 200 [183–209] | 1.02, within | 53.9 → 74.5 | 383 → 238, lower |

- **Every rate is within.** The 11 to 13% the first run showed is gone. europa's disk served 49% and
  51% of the bytes on the two sides, where the first run's draws gave it 44% and 55%.
- **The margin moved about one read in seventy.** Titan's and hyperion's links received 7.4 and 10.1
  MiB/s at 64 KiB and sixteen in flight, 2.7 MiB/s of the 190 or so served.
- **Its one effect is on the tail of 1 MiB gets at sixteen in flight.** Their p99 fell from 383 to
  238 ms, the ranges disjoint, as it fell from 300 to 229 ms in the first run. Their p50 rose from
  54 to 75 ms, and their mean stayed 80 or so. The margin takes a few reads off the deepest queue,
  which shortens the longest waits.
- **Nothing was lost.** Each of the sixteen captures read 2,048 objects back at its end.

~~Whether a margin is the default is the user's choice, made with these figures.~~ The user made a
margin of four the default on 2026-10-21, with these figures.

**Again, once every read was counted from its choice** (2026-10-27, `f101-hdd-rep3-i314`). Both runs
above were taken while a local read entered its disk's queue only when its executor took it, so a
burst saw shallower local queues than it had made, and the margin moved fewer reads than it says
([Resolved #314](../appendix/resolved/local-reads-counted-at-choice.md)). This run repeats the
second's design on `bde2927b`, which counts every read from its choice:
- **The sides** are the same build, `disk_read_margin: null` (local) and the default of four (m4).
- **The cache of read units is off on both** (`object_cache: 0`), as no cache was kept when the
  runs above were taken; since [F107](cache-default-share.md) a node keeps one unless told not to,
  and it would answer most of these gets without a disk.
- **The order** is local, m4, m4, local four times over: sixteen fresh deployments, two runs an arm in
  each.
- **The hosts** are europa on kernel 7.0.0-38, and titan and hyperion on 7.0.0-34.

Gets a second:

| Arm | Local | Margin of four | M4 ÷ local | p50 ms | p99 ms, median [range] |
| --- | --- | --- | --- | --- | --- |
| 64 KiB, one in flight | 486 [436–532] | 497 [454–545] | 1.02, within | 2.39 → 2.50 | 5.5 [3.1–9.4] → 4.2 [3.1–8.0] |
| 64 KiB, sixteen | 3,227 [2,497–3,477] | 2,990 [2,885–3,241] | 0.93, within | 4.67 → 5.12 | 11.4 [10.6–13.0] → 12.1 [11.1–12.7] |
| 1 MiB, one | 53.2 [48.2–56.8] | 53.8 [46.7–58.2] | 1.01, within | 17.8 → 17.4 | 62.2 [39.2–71.7] → 57.8 [45.6–68.3] |
| 1 MiB, sixteen | 201 [164–212] | 197 [184–205] | 0.98, within | 58.5 → 74.4 | 350 [253–687] → 242 [224–264] |

- **No rate moved outside its range, as before.**
- **The margin now moves reads where a burst builds a queue.** At 1 MiB and sixteen in flight europa's
  disk served 49% [48–58] of the bytes read without a margin and 43% [39–45] with one, the ranges
  disjoint. At 64 KiB its share was 50% and 47%, the ranges overlapping, and at one in flight 49 to
  50% on both sides. With the count arriving late, the second run measured 49% and 51% at every
  arm.
- **The tail of 1 MiB gets at sixteen in flight stays lower with the margin.** Its p99 median fell from
  350 to 242 ms, and its worst run from 687 to 264 ms, as it fell from 383 to 238 ms before. The
  ranges touch, since the local side's best run was 253 ms.
- **One capture stalled.** In local's eighth, 64 KiB gets at sixteen in flight stopped 28 seconds
  into their second run, and the 1 MiB run after them answered nothing, so it is left out
  (fifteen local runs at that arm). Every get that stopped was coordinated by titan, and nothing was
  logged. It is [item 316](../appendix/known-issues.md#316-a-gets-request-on-an-object-lane-went-unanswered-and-its-coordinator-waited-with-no-bound),
  which has its figures.
- **Nothing was lost.** Each of the sixteen captures read its 2,048 objects back at its end.

So the default of four stands. Counted from its choice, the margin does what it says and costs no
rate.

## Tests

| Test | What fails if the feature is reverted |
| --- | --- |
| `shoal` `cluster_fixture` `copies_on_disks_are_read_locally_without_a_margin` | With the margin `null` (the fixture's `.local_disk_reads()`), sixteen reads queued behind node zero's slowed arm are all read off node zero's disk; reads by queue would spill them to the other holders |
| `shoal` `cluster_fixture` `copies_on_disks_are_read_by_queue` | F97's row, now under the default margin of four: a lone read stays on its coordinator's disk, and sixteen reads queued behind a slowed arm spill to every holder. Each read goes through a client of its own: through one client, whose bucket operations share the connections it sets apart for long streams, they never queued past four, and none spilled |
| `shoal-core` `shard::objects::read::tests::a_read_leaves_its_local_disk_only_past_the_margin` | The order: a local queue as deep as the margin stays local, one past it goes to the shorter remote queue, ties go local, remote ties take turns, and a margin of zero is F97's choice |
| `shoal-core` `conf::tests::a_disk_read_margin_is_four_unless_named` | The setting is four by default, a margin named is the margin, and `null` is none |

## Related

- [F97. Reads from copies on disks, by their queues](disk-reads-by-queue.md), the choice this
  bounds, and its figures.
- [F96. A chunk's file sized to what it holds](chunk-file-classes.md#on-the-lab), whose files took
  away most of the queue.
- [Reads from a pool of copies follow leadership](../object-storage/store-optimizations/read-placement.md)
  and [O119](../appendix/optimizations.md#o119-a-get-from-a-pool-of-copies-reads-its-coordinators-own-copy-so-each-disk-serves-the-gets-its-node-leads).
- [`object_timings`](../getting-started/configuration.md#object_timings), where a node sets it.
