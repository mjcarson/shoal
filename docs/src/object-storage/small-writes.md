# X8. One small write, three ways, measured

**Reported 2026-10-08.** This is the record of spike [X8](spikes.md#x8-one-small-write-three-ways).
It ran one small write in place, 4 KiB to 256 KiB, one at a time and thirty-two at once, three
ways, on two legs:

- **row**: the write's bytes as a row of their own, the cheapest a write through the log can be;
- **staged**: S7's B, a `Quorum` read of the stripe's row, the bytes staged to a holder on every
  host, which journals them as X6 found best, a conditional commit of the small row once two of
  three have them, and an apply in place after;
- **inline**: the same read, the commit carrying the bytes, and a fold of them in place after;

on the lab's three hosts, where a commit's majority and a stage's two of three always include a
970 EVO behind 1 GbE, and on three nodes over loopback on europa's Optane. Four rounds, then a
supplement of four more on the lab with every holder sharing one flush among its applies. It ends
in a recommendation, which [S18](contract.md#q27-and-q14-in-part-one-small-write-three-ways-2026-10-08)
records as Q27, and Q14 in part: **a small write rides inside its commit only on a device whose
sync flushes its cache, below 64 KiB, once a slice shares one flush among its applies; on a
device whose cache writes through, every write is staged.** T2 fired on the Optane and T3 on the
970 EVO, at 4 KiB with a flush an apply and at 64 KiB with one shared.

Five facts decide it:

- **B's second durable round is real, and it is its latency.** One write at a time on the lab a
  staged write took 7.0 ms at 4 KiB and the inline one 5.1, the difference its stage to two
  holders of three, 1.7 ms. On the Optane a stage cost about 0.1 ms, inside a WAL commit delay of
  3 ms, and neither path won below 128 KiB.
- **Under load on the Optane, B wins at every size**: 1.02× the inline path's writes a second at
  4 KiB, 1.35× at 32 KiB and 2.01× at 256 KiB, and from 32 KiB more than the row path too. The
  nodes spent about 40 µs of cpu a KiB on bytes in the log, and the holders a fourteenth of that.
- **On the 970 EVO each holder's sync bounds both paths.** With a flush an apply, as S6 has it, a
  holder took about 720 a second and both paths ran at a quarter of the row path's rate; they were
  level to 16 KiB and B led from 32 KiB.
- **Shared, the round decides it.** With one flush covering every apply and fold that had
  completed, B rose 1.70× at 4 KiB and the inline path 2.76×, and the inline path won to 32 KiB:
  1.56× at 4 KiB, 1.11× at 32 KiB. A staged write cost 1.65 disk flushes, an inline one 1.05.
- **The log writes a byte three times a copy and B twice**: 9.2 against 6.2 device bytes a byte at
  256 KiB, the WAL, the archive and the fold against the journal and the apply.

It found one defect: a client's pool retires a connection at its 30 minute lifetime while answers
are still owed on it, which fails them as `ConnectionLost` with nothing logged on either side
([item 217](../appendix/known-issues.md#217-a-pooled-connection-retired-at-its-lifetime-fails-the-answers-it-still-owes)).
It ended the first attempt at the rounds, which were started again from the first.

## The question

[Q27](contract.md#questions-to-answer) asks what one small write in place costs end to end, and
whether small writes should ride the metadata log: below a threshold, a write's bytes travel
**inside** the commit of its stripe's row, durable when the group's log is, and are folded into the
stripe chunks afterwards ([S7](write-path.md#small-writes)). It is also the cost half of
[Q14](contract.md#questions-to-answer) that [X3](bytes-through-groups.md) left: B, holders stage
and one conditional commit decides, had never been run beside the log.

B's floor is two durable rounds in sequence: the holders' sync of what they staged, then the
group's commit, where a table write pays one. [X6](device-store-ssd.md#3-a-partial-write) measured
the device's half, a journalled write in place: 2.0 ms rested and 6.1 ms loaded at 4 KiB on the
970 EVO, 81 µs on the Optane. X8 adds the network, the read before the commit and the commit
itself.

The spike's section named the results in advance, by the size at which staged chunks overtake
bytes in the log, on each kind of device:

- **there is no such size below a stripe**: then A is the design for replicated pools;
- **it is very small**: then the log path is not worth having;
- **it sits in the tens of kibibytes**: then S7's threshold is real, and the spike has found it.

## How it was judged

The lines below were set before the harness existed and agreed with the user on 2026-10-08.
`x8 report` judges them as written. Every figure is an interval over four rounds: the lowest and
the highest round, with the median between. A difference counts only where two intervals do not
overlap, which is [the lab's rule](../performance/benchmarking.md#before-and-after-on-the-lab).

The paths judged are the staged one, B, and the inline one, the bytes in the commit. Each leg is
judged apart: **the lab** stands for the 970 EVO behind 1 GbE, since a commit's majority and a
stage's two of three always include one Zen1 host; **loopback** stands for the Optane, with no
link between the copies.

- At each size, the two are compared on **writes a second at depth 32** and on **the median
  latency at depth 1**. A side wins at a size only where its interval is wholly better than the
  other's.
- **The crossover** at a depth is the smallest size from which the inline path never wins again.

| Trigger | Fires when | Then |
| --- | --- | --- |
| **T1. No threshold below a stripe** | The inline path still wins at 256 KiB, at both depths | Bytes in the log win at every small size, and A is the design for replicated pools |
| **T2. The log path is not worth having** | The crossover is at 8 KiB or below, at both depths | Small writes are staged like every other |
| **T3. The threshold is real** | Neither T1 nor T2 | S7's threshold is set at the smaller of the two depths' crossovers, by device kind |

The row path is reported beside them and never judged: it is the cheapest any write through the
log can be, and says what the read and the condition add to the inline one.

Beside the triggers, reported and not judged:

- every path's p99 at both depths, and the parts of a write: the read, the stage to two holders of
  three, the third stage, and the commit;
- syncs a write, the WAL's over every member and the holders' apart: what the spike's section
  asked for by name;
- device bytes written a byte of payload, once the merges finished;
- the work a write leaves behind, B's apply and the inline path's fold, and how long writes waited
  for it;
- the members' and the holders' cpu a write, the driver's, and the busiest link;
- each holder's device sync floor before and after every cell, which says what flush regime a
  970 EVO was in.

## What was run

### The harness

`shoal-spike-small` is a workspace crate of its own, built the way X3's `shoal-spike-bytes` and
X10's `shoal-spike-rows` are: a schema in its library, a node program, `x8-node`, a holder,
`x8-holder`, and a driver, `x8`, which also starts the holders and europa's local nodes and
carries every `shoaladm` command for the schema. It adds no crate to the workspace's lockfile,
only its own package, and like every spike's code it is thrown away.

**The rows.**

- `StripeRow { key, bytes }`, a persistent unsorted table: the first path's row, holding exactly
  a write's bytes.
- `StripeMeta`, X10's stripe row, a persistent unsorted table keyed by consumer, object and stripe:
  a `sequence` every commit is conditional on, a label of a sequence and a tag for each of three
  copies, a length, an epoch, a mask of the holders that missed the last write, and `pending`, the
  bytes of a write that rode inside its commit. Archived with nothing pending it is 144 bytes;
  a staged commit is 192, and an inline commit 192 and the write's bytes.
- `StripeHead`, a projection of `StripeMeta` without `pending`, which is what a writer reads first:
  the bytes a previous inline write left are a reader's to overlay, not a writer's to fetch.
- `Filler`, 64 KiB rows written only to seal the WAL's segments between cells.

A write's bytes are SplitMix64 in counter mode from the write's seed and its stripe, as
[X13](benchmark-shape.md) found a benchmark's object bytes should be, made before the clock starts
on every path, as a client's bytes are in hand before it sends.

**The three paths**, one write each:

| Path | One write |
| --- | --- |
| **row** | An insert of the write's bytes as a `StripeRow`, through its group's leader. No read and no condition |
| **staged**, B | A `Quorum` read of the stripe's `StripeHead` through its group's leader. Then the bytes staged to the holder on every host at once, on with the write once two of three have them durably. Then a commit through the leader: an update of `StripeMeta` conditional on the sequence read ([F68](../features/conditional-writes.md)), moving the sequence, naming the write's tag in every copy's label and the holder that had not answered as missed. Once the commit is acknowledged, every holder applies |
| **inline** | The same read, then the same commit with the write's bytes in `pending`. Once it is acknowledged, every holder folds the bytes into its chunk |

**The holder** is S6's holder of a stripe chunk, built only as far as one write in place needs
([S6](device-store.md#staging-two-cases)). One glommio executor, pinned, with its own ring:

- **a stage** reads the bytes off its lane straight into a buffer for direct I/O, writes a 4 KiB
  header block and the units into a journal of 1 GiB written ahead with zeros and overwritten as
  a ring, and answers once the committer's next `fdatasync` covers it. One sync covers every
  record whose write completed before it began: X6's journal, the way
  [X6](device-store-ssd.md#2-the-journal) found best, its code copied;
- **an apply** writes the staged units and a header block in place, together, in the slot's chunk
  file, which was written ahead to 1 MiB and 4 KiB at start, then `fdatasync`s the chunk and drops
  the record;
- **a fold** does the same for an inline write's bytes, which it makes from the write's seed, as
  the replica of the row's group on its host would hand them to its slice: on the lab, with three
  copies on three hosts, every holder has one beside it, so a fold sends nothing over the network.

Every lane is a TCP connection under the product's mutual TLS, handed to the kernel
(`shoal::client::tls::connect`, `shoal::server::tls::accept`), with leaves from an authority
minted for the holders, carrying one request at a time. The driver keeps a pool of sixty-four
lanes a holder, so no write waits for a lane.

**The coordinator** is the driver, on europa. S7's coordinating shard is the one the client's bytes
arrive at; on the lab, every key is chosen in a group europa leads, so the coordinator is on the
same host as the driver and the leader, and a staged write's bytes cross 1 GbE twice, to titan's
and hyperion's holders, as an inline write's replication does. On loopback every key is in any
group, and every query goes to its group's leader. No path pays a forward between members that
another does not.

**The work a write leaves behind is bounded.** Every holder has a gate of twice the depth in
permits. A write takes one permit of each holder before it starts, and gives it back when that
holder's share is durable: its stage and its apply, or its fold. A worker that finds a gate empty
waits, and the wait is timed apart from the write's latency. A worker also waits for a key's last
apply or fold before it writes the key again, so a holder applies a stripe's writes in the order
they committed. A cell ends when every permit is back.

**A cell** is one size, one depth and one path. Each worker owns eight keys and writes them in
turn, so no two workers have a write to one row in flight and a conditional commit is never
refused by a sibling; each key names the same chunk slot on every holder. A cell:

1. preloads its stripe rows in a key space of its own, at sequence zero;
2. **seals**: writes filler rows until every shard's WAL has grown by a segment, 10 MiB, then lets
   every merge finish, so the tail the last cell left in an open segment is not merged inside this
   one;
3. probes every holder's sync floor, five 4 KiB overwrites each synced, and reads every counter;
4. runs its workers for 10 s of warm-up and a 30 s window;
5. waits for every apply and fold, then reads the counters again: the **end** figures, which every
   sync is divided by;
6. lets the merges of the segments it sealed finish and reads the devices once more: the
   **settled** figures. The tail it left unsealed, at most a segment a shard, is counted by nobody;
7. reads every key's last write back, its row at `Quorum` or its range of every holder's chunk,
   and counts any that does not hold the bytes the write carried.

**Every figure a write is divided by is every write the cell completed**, warm-up and tail included,
counted exactly, since the counters around a cell see all of them. Syncs are counted where they
are made: the WAL's from every shard of every member (`ShardReplication.wal_syncs`, one
`fdatasync` a batch), and each holder's own, a sync of its journal or of a chunk. The kernel's
flush count a device is read beside them.

**Rounds.** Four. `results/x8-lab.sh` runs the legs lab and loopback in that order in odd rounds
and the other way in even ones; `x8 spike` reverses its sizes, depths and paths the same way, so
the three paths of a size and a depth always run back to back. Each leg is a cluster brought up for
it with its holders, a 60 s heat cell recorded and never judged (B at 4 KiB and depth 32, so the
first cell is not the first load the devices meet), then every cell; then
both are taken down, every device is trimmed, and the lab rests two minutes. `x8 report` merges the
rounds and judges the triggers, and its output is `shoal-spike-small/results/x8-report.md`.

### Where, and on what

| Leg | Hosts and nodes | Devices and roots | Factor |
| --- | --- | --- | --- |
| **lab** | europa (Zen4), titan and hyperion (Zen1): `tmdb_cluster.yaml`'s nodes, six shards and 8 GiB each, a dedicated control core, europa's `lead_weight` 2, WAL commit delays of 3 ms on europa and 2 ms on the others. Every key in a group europa leads | europa: the node's root and its holder's directory on the Optane 900P, XFS, `/optane/shoal-x8` and `/optane/shoal-x8-holder`. titan and hyperion: both on the 970 EVO's XFS volume, `/xfs/shoal-x8` and `/xfs/shoal-x8-holder`. **The WAL and the pool's device are one disk on every host** | 3 |
| **loopback** | Three nodes on europa, X3's: each six shards on three physical cores and their siblings, a control core of its own (cpus 1, 5 and 9), 6 GiB, a WAL commit delay of 3 ms. Every key in any group | Every root and holder directory on the Optane, `/optane/shoal-x8-local/<node>/data` and `…/data-holder` | 3 |

- **Each holder ran on the sibling of its node's control cpu**, which a cluster node keeps from its
  shards: cpu 1 on titan and hyperion, 16 on europa's lab node, 17, 21 and 25 on loopback. On the
  Zen1 hosts every other cpu is a shard's, so the holder shares a physical core with the control
  thread and with no shard.
- **The driver ran on europa**, pinned to cores 8 to 15 and their siblings on the lab leg, clear of
  europa's node and holder, and to cores 13 to 15 and their siblings on the loopback leg. On the
  lab a write crosses 1 GbE to titan and hyperion, 0.12 ms round trip.
- **Governor `performance`** on all three hosts for every run, and titan's and hyperion's
  `e2scrub_all` timer held; both were put back afterwards, `powersave` on europa and `schedutil`
  on the Zen1 hosts. No other shoal unit ran on any host; `shoal-tmdb` was stopped on every host
  before the spike began, and its data directories were already gone.
- **One `znver1` build** of all three programs, rustc 1.100.0-nightly (2026-09-04), kernel
  7.0.0-34 on all three hosts, the glommio fork at `f4643f7`. The tree was `8f64972` with the spike
  uncommitted, as `x8-facts.txt` says.
- **No host was changed.** Every directory the spike made was removed with its cluster or its
  holders; the holder's program and leaves lived under `/var/tmp/x8-holder` and went with it.

**The commit delay at depth one.** A shard's WAL writer sleeps its commit delay after every sync
before it writes the next batch, so appends arriving meanwhile share a sync
(`shoal-core/src/server/wal/mod.rs`, [O61](../appendix/optimizations.md#o61-a-fast-device-syncs-the-wal-in-batches-too-small-to-fill-a-page)).
At depth one a write waits out what is left of that sleep when its group is on the shard that
synced last. Which shard a key's group is on changes with the cell's key space, so on loopback,
where the Optane's own sync is tens of microseconds and the delay 3 ms, a cell's median at depth
one says which shards its eight keys landed on nearly as much as which path it took. The lab's
syncs on the 970 EVO are of the delay's own order, and its depth-one figures are steadier.

**Where the run departed from the plan on [the spikes page](spikes.md#x8-one-small-write-three-ways).**

- The plan ran the first path through "the existing bench". All three ran through the spike's own
  driver, so every path's latency is counted alike: shoal-loadgen's driver sends one query an
  operation, and the staged and inline paths are a read, a stage and a commit in turn.
- The plan said what the holder does before the commit. What it does after, B's apply and the
  inline path's fold, was decided with the user before the harness existed: both run inside the
  arm, bounded by the gates, and every sync and device byte they cost is counted.
- The plan named the records: median and p99, writes a second, syncs a write. Added: each part of
  a write, the work left behind and the waits for it, device bytes a byte, cpu, the busiest link,
  each holder's sync floor, and a read back of every key's last write.
- The plan's sizes, 4 KiB to 256 KiB, run at every power of two between, and the result named in
  advance became the three triggers above, both decided with the user.
- **A supplement was added** after the quick run, described under
  [7](#7-the-supplement-one-flush-for-many-applies).

**Three things were changed after trying the harness**, from the quick run of both legs and from a
first attempt at the rounds that round 2 ended:

- **Device flushes are counted on the disk under each root.** The kernel counts a flush on a whole
  disk alone. titan's and hyperion's roots are on a device mapper volume, which counted none while
  the 970 EVO under it counted thousands; europa's Optane, whose cache writes through, is sent
  none at all, and reads zero truthfully.
- **The driver's connections are never retired by its pool.** The first attempt lost 2 to 38 writes
  in a cell about 30 minutes into each long leg, every one `ConnectionLost`, and a filler row lost
  the same way ended round 2's loopback leg. Today's client gives a connection back to its pool
  while answers are still owed on it, and the pool retires a connection at 30 minutes however busy
  it is ([item 217](../appendix/known-issues.md#217-a-pooled-connection-retired-at-its-lifetime-fails-the-answers-it-still-owes)).
  With no lifetime and no idle timeout on the driver's pools, nothing failed. That attempt's
  records are kept apart, in `results/aborted/`, and **the four rounds started again from the first**.
- **The seal sends a refused filler row again**, up to three times, as the preload does.

**The bound was checked** as the plan asked, on the lab at 4 KiB and depth 32, where the work a
write leaves behind lags most: the gates at the rule and at twice it, alternating, twice each. The
inline path did 707 and 712 writes a second at the rule and 705 and 704 at twice it, 1.00×. The
staged path did 733 and 743 at the rule and 776 and 796 at twice it, 1.06×, two intervals apart: a
few more applies in flight let the 970 EVO take a little more. The rule was kept, and the staged
path's figures at depth 32 on the lab are about 6% lower than an unbounded holder would give.

## How to read the tables

Every figure is the median of four rounds, with the lowest and the highest round in brackets; a
figure without them was the same in every round. Latencies are a write's, from its first send to
its acknowledgement, in microseconds unless a heading says otherwise. A rate is writes
acknowledged a second over the 30 s window. **Syncs a write** are every sync the cell caused, over
every write it completed: the WAL's are every shard's of every member, three copies' worth, and a
holder's are its own, three holders' worth. **Device bytes a byte** are the bytes the kernel counted
written to every device a root or a holder is on, over the payload the cell's writes carried,
once the merges finished. The tables in full, every figure of every cell, are
`shoal-spike-small/results/x8-report.md`; the ones here are cut from it.

## 1. Depth one: a write's latency

### The lab: the 970 EVO behind 1 GbE

| Size | row p50 | staged p50 | inline p50 | staged p99 | inline p99 | Wins |
| --- | --- | --- | --- | --- | --- | --- |
| 4 KiB | 4,946 | 7,049 [7,016–7,340] | 5,089 [5,059–5,308] | 11,125 | 8,372 | inline |
| 8 KiB | 4,669 | 7,350 [7,168–7,868] | 5,194 [5,112–5,562] | 11,297 | 9,523 | inline |
| 16 KiB | 5,063 | 7,551 [7,459–7,889] | 5,550 [5,157–6,074] | 11,596 | 9,761 | inline |
| 32 KiB | 4,975 | 7,911 [7,574–8,045] | 5,470 [5,181–5,726] | 11,563 | 9,896 | inline |
| 64 KiB | 5,274 | 7,758 [7,746–7,811] | 6,906 [6,578–7,213] | 11,067 | 10,633 | inline |
| 128 KiB | 6,523 | 9,372 [9,232–9,798] | 8,057 [7,713–8,188] | 10,506 | 12,440 | inline |
| 256 KiB | 9,007 | 9,785 [9,699–9,847] | 9,454 [9,052–9,921] | 22,938 | 23,380 | neither |

**One write at a time, the bytes in the commit are a durable round faster up to 128 KiB.** The
inline write is its read and its commit: about 0.5 ms and 4.6 ms at 4 KiB, the commit waiting out a
WAL commit delay and a 970 EVO's sync on the follower that makes the majority. The staged write is
the same read and a commit of the same cost, 4.7 ms, and between them the stage: 1.7 ms to two
holders of three at 4 KiB, 2.3 ms at 64 KiB, 5.2 ms at 128 KiB, where the bytes' time on the link
to titan and hyperion is most of it. That is the second durable round
[S7](write-path.md#small-writes) said B pays, measured: 1.39× the inline write at 4 KiB and 1.16× at
128 KiB. At 256 KiB the inline commit's own bytes, sent to two followers over the same link, cost
what the stage does, and the two meet.

### Loopback: the Optane

| Size | row p50 | staged p50 | inline p50 | staged p99 | inline p99 | Wins |
| --- | --- | --- | --- | --- | --- | --- |
| 4 KiB | 890 [613–1,372] | 1,453 [953–1,579] | 1,407 [842–2,279] | 3,543 | 3,342 | neither |
| 8 KiB | 790 [692–1,563] | 1,394 [832–2,122] | 1,874 [1,066–2,228] | 3,279 | 3,256 | neither |
| 16 KiB | 1,305 [642–2,130] | 977 [786–1,588] | 1,113 [911–1,484] | 3,205 | 3,508 | neither |
| 32 KiB | 762 [609–908] | 1,153 [828–1,537] | 1,061 [927–1,383] | 3,409 | 3,422 | neither |
| 64 KiB | 1,165 [851–1,872] | 1,215 [1,039–1,737] | 1,157 [1,099–1,816] | 3,269 | 4,389 | neither |
| 128 KiB | 1,203 [1,168–1,798] | 1,070 [932–1,337] | 1,414 [1,375–1,449] | 3,431 | 7,236 | staged |
| 256 KiB | 1,844 [1,690–2,591] | 1,149 [1,133–1,215] | 1,954 [1,861–2,173] | 3,246 | 8,927 | staged |

**On the Optane a stage costs about 0.1 ms, and the commit delay hides it.** A stage to two
holders of three took 94 µs at 4 KiB and 458 µs at 256 KiB, against a WAL commit delay of 3 ms on
every shard. Up to 64 KiB every path's median wandered between about 0.6 and 2.3 ms with the
cell's keys, as [the commit delay](#where-and-on-what) does at depth one, and no side won. From
128 KiB the inline commit's bytes cost more than a stage does: a commit of 256 KiB took 1.6 ms at
the median against 0.4 ms for B's small one, and B won by 1.7×.

## 2. Depth thirty-two: writes a second

| Size | Lab: row | Lab: staged | Lab: inline | Wins | Loopback: row | Loopback: staged | Loopback: inline | Wins |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 4 KiB | 3,105 | 731 [721–736] | 702 [686–731] | neither | 8,105 | 6,884 [6,858–6,940] | 6,770 [6,678–6,813] | staged |
| 8 KiB | 2,887 | 715 [709–733] | 688 [672–717] | neither | 7,776 | 6,859 [6,818–6,887] | 6,461 [6,254–6,513] | staged |
| 16 KiB | 2,169 | 692 [681–705] | 675 [658–694] | neither | 7,090 | 6,787 [6,767–6,823] | 5,928 [5,822–6,090] | staged |
| 32 KiB | 1,411 | 628 [619–641] | 552 [539–564] | staged | 5,671 | 6,484 [6,424–6,574] | 4,810 [4,664–4,985] | staged |
| 64 KiB | 818 | 473 [466–487] | 390 [386–397] | staged | 4,196 | 4,909 [4,878–5,008] | 3,413 [3,188–3,525] | staged |
| 128 KiB | 441 | 321 [320–324] | 258 [257–260] | staged | 2,189 | 2,975 [2,946–3,039] | 1,772 [1,740–1,786] | staged |
| 256 KiB | 223 | 204 [202–204] | 172 [166–177] | staged | 972 | 1,518 [1,515–1,525] | 754 [740–758] | staged |

**Under load the inline path never wins.** On the lab the two are level to 16 KiB and B leads from
32 KiB, by 1.14× to 1.24×. On loopback B leads at every size, by 1.02× at 4 KiB, 1.35× at 32 KiB
and 2.01× at 256 KiB, and from 32 KiB it beats the row path too: a write in place staged to three
holders took more a second than the same bytes written as a row of their own.

**Their tails part as well.** On loopback B's p99 at depth 32 was 7.2 ms at 4 KiB and 28 ms at
256 KiB; the inline path's 9.4 ms and 107 ms, the row path's 6.9 ms and 79 ms. A write whose bytes
are in the log waits behind other writes' bytes in its shard's WAL batch and on the replication
lane, as [X3](bytes-through-groups.md#6-a-small-table-beside-the-stripes) found for whole stripes.

**On the lab both are held by the holders' syncs.** The row path did four times either at 4 KiB,
3,105 writes a second against 731 and 702. Both B's apply and the inline path's fold end in one
`fdatasync` of the write's chunk, as S6 has an apply, and a 970 EVO took about 720 of those a
second a holder on top of everything else: 3.5 to 3.7 disk flushes a write over titan and hyperion,
where the row path made 0.4. B's stage waited behind its holder's applies, 28 ms at the median for
two of three, and the inline path's writes waited at the gates for folds, 31 ms at the median
([6](#6-the-work-a-write-leaves-behind)). Which path wins there is decided by what each adds to
that, until 32 KiB, where the inline commit's bytes start to cost more than the stage: 12.9 ms at
the median against 12.3 at 32 KiB, 22.7 against 14.9 at 64 KiB. [7](#7-the-supplement-one-flush-for-many-applies)
takes the holders' syncs out of the way.

**Every comparison began in one flush regime.** Before every cell a 970 EVO's sync took 0.93 to
1.10 ms at the median of its probe, but for one of 1.5 ms, its rested regime
([X6](device-store-ssd.md#two-things-about-the-labs-970-evos)): the seal and the settle before a
cell let the device rest, so the three paths of a size and depth started alike. After a cell the
probe read up to 4 ms, the loaded regime, which is what each cell ran in once it began. The
Optane's took 35 to 71 µs throughout.

## 3. A write's parts

| Size | Depth | Leg | Read | Stage, two of three | Third stage | Staged commit | Inline commit |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 4 KiB | 1 | lab | 0.49 ms | 1.65 ms | 2.71 ms | 4.71 ms | 4.63 ms |
| 128 KiB | 1 | lab | 0.52 ms | 5.20 ms | 5.53 ms | 3.91 ms | 7.49 ms |
| 4 KiB | 32 | lab | 0.60 ms | 27.7 ms | 41.3 ms | 11.2 ms | 10.4 ms |
| 256 KiB | 32 | lab | 2.75 ms | 127 ms | 148 ms | 20.6 ms | 86.2 ms |
| 4 KiB | 1 | loopback | 0.23 ms | 0.09 ms | 0.10 ms | 1.11 ms | 1.10 ms |
| 256 KiB | 1 | loopback | 0.23 ms | 0.46 ms | 0.57 ms | 0.44 ms | 1.63 ms |
| 4 KiB | 32 | loopback | 0.68 ms | 0.13 ms | 0.15 ms | 3.52 ms | 3.62 ms |
| 256 KiB | 32 | loopback | 0.31 ms | 13.4 ms | 15.1 ms | 6.36 ms | 27.6 ms |

- **The read is S7's `Quorum` read through the leader**, of a 144 byte projection: 0.2 ms on
  loopback and 0.5 ms on the lab, the same on both paths, and never what decided a cell.
- **Two of three is what an acknowledgement waits for.** The third holder answered 10 to 50%
  later. On the lab two of three is europa's Optane and the faster of two 970 EVOs, so a stage is
  bounded by one 970 EVO's sync and the bytes' time on one link.
- **B's commit carries no bytes and stays small**: 3.7 to 5.3 ms at depth one on the lab whatever
  the write's size, where the inline commit grew from 4.6 to 8.9 ms with the bytes inside it.

## 4. Syncs a write

| Size | Depth | Leg | row: WAL | staged: WAL | staged: stage | staged: apply | inline: WAL | inline: fold | Records a journal sync |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 4 KiB | 1 | lab | 5.5 | 5.9 | 3.00 | 3.00 | 5.4 | 3.00 | 1.00 |
| 4 KiB | 32 | lab | 0.81 | 2.59 | 1.11 | 3.00 | 2.88 | 3.00 | 2.70 |
| 256 KiB | 32 | lab | 2.55 | 4.06 | 2.17 | 3.00 | 2.91 | 3.00 | 1.38 |
| 4 KiB | 1 | loopback | 4.8 | 5.1 | 3.00 | 3.00 | 4.8 | 3.00 | 1.00 |
| 4 KiB | 32 | loopback | 0.69 | 0.81 | 2.51 | 3.00 | 0.82 | 3.00 | 1.20 |
| 256 KiB | 32 | loopback | 1.43 | 2.08 | 3.00 | 3.00 | 1.52 | 3.00 | 1.00 |

**What the spike's section asked for by name.** One write at a time, a write through the log made
about five WAL syncs over the three members: an append on every copy, and the commit's progress
made durable after it. B added a stage and an apply on every holder, six syncs, and the inline
path a fold on every holder, three. Under load the WAL's batches took about three writes a sync
of each member on the Optane and fewer on the lab, where fewer writes a second arrived to share
one. The holders' journals batched too, 2.7 records a sync on the lab at 4 KiB; an apply and a
fold never did, since each syncs its own chunk.

## 5. Device bytes, and what the cpu went to

| Size | Depth | Leg | row | staged | inline |
| --- | --- | --- | --- | --- | --- |
| 4 KiB | 32 | lab | 5.1 | 16.2 | 15.7 |
| 64 KiB | 32 | lab | 4.8 | 6.8 | 7.9 |
| 256 KiB | 32 | lab | 6.2 | 6.2 | 9.2 |
| 4 KiB | 32 | loopback | 5.1 | 13.8 | 11.8 |
| 64 KiB | 32 | loopback | 4.4 | 6.5 | 7.6 |
| 256 KiB | 32 | loopback | 6.4 | 6.2 | 9.4 |

Device bytes written a byte of payload, every host, merges finished.

**B writes a byte twice a copy, and the inline path three times.** At 256 KiB the staged write cost
6.2 device bytes a byte on both legs: three copies, each written once to a journal and once in
place. The inline write cost 9.2 to 9.4: each copy once to its WAL, once to its archive when the
segment merged, and once in place when its holder folded it. The row path's 6.2 to 6.4 is X3's two
writes a copy. At 4 KiB the header blocks dominate: a stage and an apply each write a 4 KiB header
beside 4 KiB of units, so B writes four bytes a byte a copy before the WAL's pages, and the inline
path's fold two.

| Size | Depth | Leg | row: nodes | staged: nodes | staged: holders | inline: nodes | inline: holders |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 4 KiB | 32 | loopback | 628 | 1,054 | 240 | 1,115 | 141 |
| 64 KiB | 32 | loopback | 2,168 | 1,158 | 346 | 2,778 | 244 |
| 256 KiB | 32 | loopback | 10,537 | 1,559 | 754 | 10,602 | 563 |

Cpu a write, µs, every member's or every holder's together.

**On the Optane the cpu decides it.** The nodes spent about 10.5 ms of cpu on a write of 256 KiB
through the log, row or inline alike, against 1.6 ms for B's read and small commit and 0.75 ms on
its holders: about 40 µs a KiB of every byte a member handles, which is X3's finding that A's cpu
goes to copies ([O95](../appendix/optimizations.md#o95-a-wide-rows-bytes-are-copied-and-faulted-in-on-every-write)).
Three loopback nodes share europa's cores, so whichever path spends less of them a write takes
more writes a second, and from 32 KiB that is B. A holder's stage of the same bytes cost a
fourteenth of the nodes' cpu, since it copies them once, from the socket to the journal.

## 6. The work a write leaves behind

| Size | Depth | Leg | Path | Apply or fold p50 | p99 | Wait p50 | Wait p99 |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 4 KiB | 1 | lab | staged | 1.1 ms | 4.0 ms | 0 | 0 |
| 4 KiB | 1 | lab | inline | 1.1 ms | 4.9 ms | 0 | 1.2 ms |
| 4 KiB | 32 | lab | staged | 21.8 ms | 53 ms | 0 | 21.7 ms |
| 4 KiB | 32 | lab | inline | 30.3 ms | 92 ms | 31.3 ms | 46.6 ms |
| 256 KiB | 32 | lab | staged | 6.6 ms | 149 ms | 0 | 43.3 ms |
| 256 KiB | 32 | lab | inline | 7.8 ms | 371 ms | 63.9 ms | 153 ms |
| 4 KiB | 32 | loopback | staged | 0.13 ms | 0.27 ms | 0 | 0 |
| 256 KiB | 32 | loopback | inline | 3.9 ms | 23.6 ms | 0 | 0.02 ms |

**On the Optane nothing waited; on the 970 EVO everything did.** An apply or a fold on the Optane
took 0.1 to 13 ms and no write waited for a gate. On the lab an apply took 22 ms at the median at
4 KiB and depth 32, a fold 30 ms, and the inline path's writes waited 31 ms for one: an apply's or a
fold's sync is its own, and a holder's device takes them one flush at a time. B's writes waited
less at the gates because its stage, which shares the holder's executor and device with the
applies, had already waited behind them. Every cell drained within 0.2 s of its window.

## 7. The supplement: one flush for many applies

[2](#2-depth-thirty-two-writes-a-second) left the lab's comparison under load decided by the holders'
syncs: each apply and each fold syncs its own chunk, as S6's apply does, and a 970 EVO flushes its
whole cache for each. Added after the quick run, the supplement asks what the two paths cost when
that is not so. Every holder ran with **one flush covering every apply and fold whose writes had
completed before it began**, by the committer the journal uses, syncing a small file of its own:
on XFS an `fdatasync` flushes the device even with nothing of its own to write
([X6](device-store-ssd.md#what-the-probes-found)), so one flush makes every direct write into a
chunk written ahead durable. Four rounds of the lab leg at depth 32, run as the main rounds were
(`IN_PLACE_SYNC=batch`, `shoal-spike-small/results/supplement/`). Reported, never judged.

| Size | row | staged | inline | Wins | Staged over the main rounds | Inline over the main rounds |
| --- | --- | --- | --- | --- | --- | --- |
| 4 KiB | 2,970 | 1,244 [1,224–1,267] | 1,940 [1,852–1,983] | inline | 1.70× | 2.76× |
| 8 KiB | 2,708 | 1,198 [1,179–1,216] | 1,642 [1,621–1,686] | inline | 1.68× | 2.39× |
| 16 KiB | 2,155 | 1,003 [997–1,018] | 1,193 [1,172–1,197] | inline | 1.45× | 1.77× |
| 32 KiB | 1,405 | 738 [727–748] | 817 [812–822] | inline | 1.18× | 1.48× |
| 64 KiB | 821 | 526 [522–531] | 544 [529–556] | neither | 1.11× | 1.39× |
| 128 KiB | 441 | 357 [354–358] | 345 [331–354] | staged | 1.11× | 1.33× |
| 256 KiB | 223 | 214 [213–214] | 204 [201–209] | staged | 1.05× | 1.19× |

Writes a second at depth 32 on the lab.

**With the holders' syncs shared, the bytes in the commit win under load up to 32 KiB**, by 1.56× at
4 KiB down to 1.11× at 32 KiB, level at 64 KiB, and B leads from 128 KiB. The crossover at depth 32
moves from 4 KiB to 64 KiB. At depth one a holder never has two applies to share a flush, so the
main rounds' depth-one figures stand; read with them by the same rule, the lab's threshold would
be 64 KiB, which is T3 in the tens of kibibytes.

**What decides it is B's second durable round.** At 4 KiB a staged write cost 2.43 holder syncs and
1.65 disk flushes over titan and hyperion, its stage's and its apply's, each batched; an inline
write cost 1.21 and 1.05, its fold's alone, the log's own commit already durable. On a device that
flushes its cache for a sync, a round of syncs is a round of the device's time, and B pays one
more. It costs B's writes their latency too: a stage to two of three took 7.4 ms at the median at
4 KiB, behind the applies it shares a holder with. From 64 KiB the inline commit's bytes in the
WAL, the replication lane and the archives cost more than that round, as on the Optane: its commit
took 40.7 ms at the median at 64 KiB against B's 23.0.

**Sharing the flush is worth having for B on its own.** B's own rate rose 1.70× at 4 KiB and 1.45×
at 16 KiB, every key reading back what it last wrote. On the Optane, whose cache writes through,
a sync flushes nothing and sharing one changes nothing a write waits for.

## What would have changed the design

| Result named in advance | Found | So |
| --- | --- | --- |
| **T1.** The inline path still wins at 256 KiB at both depths: no threshold below a stripe, and A the design for replicated pools | **Does not fire** on either leg. The inline path won at 256 KiB nowhere, at either depth, in the rounds or the supplement | Bytes in the log are not the design for a replicated pool, which [X3](bytes-through-groups.md) said for whole stripes and X8 says for a single small write |
| **T2.** The crossover at 8 KiB or below at both depths: the log path is not worth having | **Fires on loopback**, the Optane: 4 KiB at both depths. The inline path won no size and depth; B won from 128 KiB one write at a time and at every size under load, by 1.02× to 2.01× | On a device whose cache writes through, a small write is staged like any other |
| **T3.** Neither: the threshold is real, at the smaller of the two depths' crossovers | **Fires on the lab**, the 970 EVO behind 1 GbE: **at 4 KiB** with a flush an apply, as S6 has it, where the inline path won every size to 128 KiB one write at a time and none under load; **at 64 KiB** in the supplement, with one flush for many applies, where it won to 32 KiB under load as well | On a device whose sync flushes its cache, the threshold is as large as the slice makes its applies cheap: none with a flush each, writes below 64 KiB with a flush shared |

## The comparison

**What one small write in place costs, by how it travels**, at 4 KiB, against the same bytes as a
row of their own:

| Built as | One write at a time, lab | Writes a second at depth 32, lab | the same with applies' flushes shared | One write at a time, loopback | Writes a second at depth 32, loopback | Device bytes a byte a copy, 256 KiB |
| --- | --- | --- | --- | --- | --- | --- |
| **A row of its own**, the log's floor | 4.9 ms | 3,105 | 2,970 | 0.9 ms | 8,105 | 2.1 |
| **B: staged, then a small commit** | 7.0 ms | 731 | 1,244 | 1.5 ms | 6,884 | 2.1 |
| **In the commit, then folded** | 5.1 ms | 702 | 1,940 | 1.4 ms | 6,770 | 3.1 |

The row is the floor and no stripe can have it: a row of a write's own bytes is not a stripe of a
pool, and the comparison that decides Q27 is the two below it. Each pays the same deferred write in
place; what differs is the round before it. B's is a stage to the holders, a journal write and a
sync each, durable before the commit is sent. The inline path's is the commit itself, carrying
the bytes through the WAL and the replication lane and later through the merges.

## Recommendation

**Keep S7's small-write path, but only where it pays: on a device whose sync flushes a volatile
cache, a write below 64 KiB rides inside its commit once the slice shares one flush among its
applies; on a device whose cache writes through, every write is staged.**
[S18](contract.md#q27-and-q14-in-part-one-small-write-three-ways-2026-10-08) records it as Q27, and
Q14 in part:

- **The bytes in the commit are one durable round, and the round is what a flushing device
  charges for.** On the 970 EVO a write was 1.39× faster in the commit at 4 KiB one at a time, and
  with the holders' syncs shared 1.56× more of them were acknowledged under load. On the Optane
  the round cost about 0.1 ms and the log's cpu cost more: the inline path never won.
- **A slice's applies share a flush.** It is what made the threshold real on the lab, and B gained
  1.70× from it at 4 KiB on its own. One flush standing for many files' direct writes is what
  the supplement measured and not what it proved durable: M14's crash tests are what make it a
  rule.
- **Above the threshold the log is dearer than a stage, on every device.** Bytes in the commit
  are written three times a copy against B's two, cost the nodes about 40 µs of cpu a KiB, and
  wait behind other writes' bytes in a shard's WAL batch: from 128 KiB B led on both legs at both
  depths, and on the Optane by 2.0× at 256 KiB.
- **The row the threshold needs is S3's stripe row with a field of pending bytes**, empty unless
  a small write rode its commit, and then less than 64 KiB, which a read overlays and a fold clears. An
  empty field costs eight archived bytes a row; adding one later would be a new row format, which
  is a new cluster ([Q10](../distributed/protocol.md#q10-at-m10a)).

## What X8 does not settle

- **Whether one flush may stand for many files' writes after a crash.** The supplement made it
  cheap, and nothing here crashed a host. M14's fixture faults ([F70](../features/storage-faults.md))
  are what hold it to [P7](contract.md#the-contract).
- **How a device says which kind it is.** The holders' probe told them apart at once, 40 µs a
  sync on the Optane against 950 µs on the 970 EVO, but a pool's default threshold needs a rule a
  slice can apply when it is claimed, and a drive with power-loss protection is the case the lab
  has none of.
- **The fold's own commit.** A fold makes the bytes durable in place, and the row still holds
  them until a later commit clears the field. X8's next write to a stripe replaced them; a stripe
  written once keeps its bytes in its row until something clears them, which M15 decides.
- **Erasure coded stripes.** Every write here was to a replicated pool's three copies. A partial
  write of a k+m stripe reads before it stages and touches `d + m` chunks
  ([S7](write-path.md#the-acknowledgement-rule)), which moves both paths' costs.
- **A rotational pool.** Its journal is on an SSD of its node ([Q23](contract.md#q23-what-a-rotational-device-needs-2026-10-06)),
  so its stage is an SSD's, and its apply a disk's.

## What it did not measure

- **The read.** Every read of a stripe here was S7's read of its row before a commit. A reader of
  a stripe with pending bytes overlays them, and a reader of a stripe a holder has not applied yet
  asks the row; neither was run.
- **A coordinator on another node.** The driver played S7's coordinating shard on europa, beside
  the leader of every group its lab keys were in. A write that arrived at titan for a group
  europa leads would add a link crossing to both paths.
- **A checksum of each unit.** A stage carried no CRC; X5 measured CRC-64/NVME at gigabytes a
  second a core, far below anything here.
- **Contention.** Every worker owned its keys, so no commit was refused and no stage was wasted.
- **Crashes and failures.** Nothing was killed; every stage and apply landed.
- **A long run.** Each cell ran 40 s; the WAL's retention budget was passed in long legs and forced
  groups past sealed segments, as it is built to.

## What it found in the engine

| Finding | Where |
| --- | --- |
| A client gives a connection back to its pool once a bundle is written, while its answers are still owed, and the pool retires a connection at its 30 minute lifetime however busy it is: every query in flight on it then fails as `ConnectionLost`, and neither side logs why. It cost X8's first attempt 2 to 38 writes a long leg and ended one | [Item 217](../appendix/known-issues.md#217-a-pooled-connection-retired-at-its-lifetime-fails-the-answers-it-still-owes) |
| The kernel counts a flush on a whole disk alone: a device mapper volume over the 970 EVO counted none, and bench captures that read a root's own device (F71's script) read no flushes on titan and hyperion | Not a defect: `shoaladm`'s device counters read the device a root is on, which is right for bytes; X8 reads the disk under it for flushes |

## Related

- [X8](spikes.md#x8-one-small-write-three-ways) for what was planned.
- [S7](write-path.md#small-writes) for the small-write path and B's two rounds.
- [S6](device-store.md#staging-two-cases) for the journal and the apply the holder followed.
- [S18](contract.md#q27-and-q14-in-part-one-small-write-three-ways-2026-10-08) for the decision.
- [X6's record](device-store-ssd.md) for the device's half of a write in place, and the journal.
- [X3's record](bytes-through-groups.md) for whole stripes through the log, and the form this page
  copies.
- [X10's record](stripe-row-costs.md) for the stripe row, its read before a commit, and the
  inline threshold of whole objects, which is another threshold from this one.
