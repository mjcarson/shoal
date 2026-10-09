# X9. Table latency beside object work, measured

**Reported 2026-10-09.** This is the record of spike [X9](spikes.md#x9-table-latency-beside-object-work).
It ran the workload grid's reference cell, `macro/grid/unsorted/r50/1024`, on titan's two shards,
alone and beside object work at 100 and 500 MiB/s, in stripes of 4+2 units of 64 KiB and of 1 MiB
that it copied in, checksummed, encoded and wrote with direct I/O:

- **on the table shards**, in a third task queue below their two, with no latency goal
  (`shards`) and with a 250 µs one (`shards-lat`), each step a unit's worth of input;
- **on a core of its own**, an executor pinned to the coordinating core's other thread (`core`);
- and, added after the quick run, **on the table shards in steps of 64 KiB under a 100 µs goal**
  (`shards-fine`).

Two legs: the pool on a null device of its own, which was judged, and on the 970 EVO the tables
are on. Eight rounds, the order rotating by round. It ends in a recommendation, which
[S18](contract.md#q24-and-q15-in-part-table-latency-beside-object-work-2026-10-09) records as Q24,
and Q15 in part: **object work runs on executors of its own; a node of four cores gives one core
up, and M14 builds no shared mode.** T1 fired at 1 MiB units on both planned shared arms at both
rates, and nowhere at 64 KiB.

Five facts decide it:

- **A core of its own costs the cell nothing measurable**: its read p99 at 0.97 to 1.06 times the
  cell alone's at every rate and unit, every interval overlapping. On titan the coordinating core's
  other thread carried 500 MiB/s.
- **On the table shards a step of a 1 MiB unit moves the cell's read p99 2.0 to 3.8 times**, with or
  without a goal on the object queue; a step of a 64 KiB unit, 1.05 to 1.20 times, inside the line.
- **A table waits for the hold, not the step.** With no goal the work kept a shard for a whole stripe,
  2 ms at the p99 at 1 MiB; glommio let a 250 µs goal cut in only between steps, which left 430 to
  470 µs.
- **Steps of 64 KiB inside a unit, under a 100 µs goal, hold 1 MiB units under the line at 100 MiB/s
  and not at 500**: 1.18 and 1.51 times, with holds of 150 µs at both. What is left at 500 is not
  the hold, and was not traced.
- **A stager's work costs a Zen1 core 0.37 to 0.50 ms a MiB**, a fifth to a quarter of a core at
  500 MiB/s: Q15's cost, before its sends.

Beside the line, the 970 EVO leg shows what a shared device costs: the cell's write p99 rose 1.2 to
1.9 times wherever the work ran, its own core included, which is why S15's budget is stated for
pools on their own devices.

It found one defect, in the lab's own procedure: on titan and hyperion a core's two threads are
adjacent cpus, so the before-and-after layout put a benchmark's client on a shard's other thread,
and a login's locked memory left every shard without registered buffers
([Resolved #218](../appendix/resolved/lab-core-layout.md)).

## The question

[Q24](contract.md#questions-to-answer) asks where object work runs: which executor owns a slice,
whether object work shares executors with tables, the lane and the memory budget. X9 is its
middle part, and [S13](isolation.md#shared-executors-or-dedicated-ones) calls it the experiment
that whole page waits for: object work on the table shards costs no cores but isolates a table only
by shares and by yielding, while executors of its own isolate by construction and cost a core, a
quarter of a four-core node. It is also the cost half of [Q15](contract.md#questions-to-answer),
which [X1](stripe-model.md) settled as the node that received a client's bytes: what that stager's
work costs the shard it runs on.

Object work is the opposite of a table's. A table's read is answered in tens of microseconds; a
stripe is megabytes copied in, checksummed, encoded and written. [X3](bytes-through-groups.md#6-a-small-table-beside-the-stripes)
measured the worst of it already, object bytes as rows on the table shards with nothing yielding:
a small table's p99 rose a hundredfold. X9 measures the design S13 lays out instead, work cut into
steps that yield.

The spike's section named the result that would change the design in advance: **the reference
cell's p99 more than a quarter above what it is alone when object-shaped work runs on the table
shards**. Then dedicated executors are required, and a node of four cores gives one up or does not
serve a pool.

## How it was judged

The line is the spike's own, set when it was planned. How it is read was agreed with the user on
2026-10-09, before the harness ran: the plan named one shared arm, a third task queue with no
latency goal, and glommio, read while the harness was designed, will not let such a queue give its
core to a table's query until the work suspends of its own accord, so a second shared arm gives the
queue a latency goal. Every figure is an interval over eight rounds: the lowest and the highest
round, with the median between. A difference counts only where two intervals do not overlap, which
is [the lab's rule](../performance/benchmarking.md#before-and-after-on-the-lab).

| Trigger | Fires at a rate and a unit when | Then |
| --- | --- | --- |
| **T1. A shared arm moves the table's tail** | On the null leg, the cell's read p99 or write p99 beside the arm has a median above **1.25×** the cell alone's, and its lowest round is above the cell alone's highest | That arm cannot share a table's executor at that rate and unit |

The decision follows both planned shared arms. **Dedicated executors are required** at a rate and
unit only if `shards` and `shards-lat` both fire there; if only `shards` fires, sharing is allowed
with a latency goal on the object queue; if neither, sharing is allowed.

`shards-fine`, steps of 64 KiB inside a unit under a 100 µs goal, was **added after the quick run**
showed why the two planned arms fired where they did. It is judged by the same line and reported
beside them, and it does not move T1's verdict; it says what sharing would need where they fired.

Reported and not judged:

- the arm on a core of its own against the cell alone, which is what dedicating a core buys and
  costs;
- the cell's medians and its operations a second;
- the object work's own figures in the measured window: the rate it reached, each step by kind,
  each hold (the time it kept the executor between two real suspensions), its group syncs, and the
  yields it offered against those taken;
- the evo leg, where the pool shares the tables' device, and what a stager's work costs a core.

## What was run

### The harness

**The cell** is the workload grid's reference cell, `macro/grid/unsorted/r50/1024`
([F17](../features/workload-grid.md)), as the spike asked: an unsorted persistent table of rows of
1 KiB, 20,000 rows seeded and then 20,000 queries timed, half gets of seeded keys and half inserts
of new ones, thirty-two outstanding, uniform keys. `shoal-workload` starts its server in process
and drives it from a tokio client, and the timed phase lasts about 2.4 seconds on titan. Its read
p99 alone was 425 µs on one leg and 444 on the other, at the median of the rounds; its writes,
each made durable through the WAL on the 970 EVO, take about 7.1 ms at the median and 14 to 15 at
the p99, so the read is the figure a stall shows in.

**The object work** is `shoal-core/src/server/x9.rs`, behind a feature, `x9`, that shoal-core,
the `shoal` facade and shoal-bench forward and that nothing else builds. A run asks for it in the
environment, `SHOAL_X9_PLACE` and five more, as the stage profile asks for its sampling: with the
feature off none of it is compiled, and with the place unset nothing runs and no shard gains a
queue. One binary, built for `znver1` with the feature, ran every side, the cell alone included.
It is a spike's code and goes when M14 builds object work.

A runner takes stripes of four data units at a set rate, from a schedule fixed at its start, and
does to each what a stager does to a client's bytes ([S7](write-path.md)):

1. **receive**: copies each data unit into a buffer of its own from 32 MiB of seeded bytes, rotated
   so no stripe's input is warm in the cache, standing for a socket's copy;
2. **checksum** each data unit, CRC-64/NVME through `crc-fast` =1.10.0, the checksum
   [X5](checksums.md) chose;
3. **encode** two parity units, `rusty_erasure` 0.4.1 on ISA-L's Cauchy matrix, the code and crate
   [X4](erasure-coding-crates.md) chose, its kernels picked at run time (AVX2 on Zen1);
4. **checksum** each parity unit;
5. **write** all six units with direct I/O to the runner's ring on the pool, one stripe in flight,
   with one `fdatasync` every 10 ms covering what was written since, as a slice's journal commits a
   group ([X6](device-store-ssd.md)).

Each step reads a step's worth of input. **By default a step is a chunk unit**, the plan's
"yielding between chunk units": one unit received, one checksummed, or one slice of the encode's
columns a quarter of a unit wide, which reads a unit's worth across the four, since a whole 4+2
encode of 1 MiB units holds a Zen1 core for half a millisecond in one call (X4). `shards-fine` cuts
steps of 64 KiB inside a unit: a CRC keeps eight bytes between pieces, which S13 says lets a yield
fall inside a unit. Between steps the runner offers its executor back with glommio's
`yield_if_needed`, the call glommio means for it, and counts whether the offer was taken.

Glommio decided the arms, read while the harness was designed (`glommio/src/executor/mod.rs`,
`reactor.rs`, `sys/uring.rs`). `yield_if_needed` yields only when the latency ring has an event:
the preempt timer, armed at the shortest latency goal among the queues active at the time, 100 ms
when none has one, or an I/O a queue with a goal is waiting on. A client socket becoming readable
is polled on the main ring, and a message from the other shard wakes the shard's loop, which runs
in glommio's default queue with no goal; neither raises the flag. An unconditional yield does not
help either: the executor picks the next runnable queue without polling I/O, and the runner's queue
is runnable. So a step is not the wait a query sees; **a hold is**, the time the runner kept its
executor between two suspensions it could not avoid: a pacing sleep, the stripe's writes, or a
yield the flag let it take. The report times both.

**The arms:**

| Arm | Where the work runs | Its queue |
| --- | --- | --- |
| `alone` | Nowhere: the cell alone | |
| `shards` | On both table shards, half the rate each | A third task queue, `LowPriority:<shard>`, 100 shares (the high queue has 1,000 and the medium 500), `Latency::NotImportant`: S13's "a third task queue on each shard, below the other two" |
| `shards-lat` | The same | The same at `Latency::Matters(250 µs)`, half the high queue's 500 µs goal |
| `shards-fine` (supplement) | The same | `Latency::Matters(100 µs)`, steps of 64 KiB |
| `core` | On an executor of its own, `Placement::Fixed` on cpu 1, the whole rate | Its default queue, started once the shards answer |

Each arm ran at 100 and 500 MiB/s of data, the rates the spike named, and at units of 64 KiB and
1 MiB. 500 MiB/s of 4+2 writes 750 MiB/s of units.

A runner whose schedule fell more than 50 ms behind dropped the time past that bound instead of
owing it. Without the bound, a runner starved while the cell seeded carried up to 0.9 s of stripes
into the timed phase and ran them there above its rate, 64 MiB/s where it was asked for 50, which
the quick run showed. A stager whose clients' bytes go unread holds them back through their
connections; it does not owe the time it lost. In the rounds the dropped time, up to about a second
a runner, fell while the cell seeded; inside the timed phase one runner of the 208 runs dropped
half a millisecond. Runners on the table shards at 500 MiB/s of 64 KiB units did start stripes up
to the bound late inside it, and caught up, so their rate in the window is the one asked for and a
burst is part of what the cell met.

The harness marks the timed phase's two edges (`harness.rs`, around `driver::timed`) and the pool
writes the report as it exits, so every figure of the work is the timed phase's alone. A unit test
holds the steps, cut either way, to one encode and one checksum a unit, byte for byte, and
`shoal-bench/tests/x9_report.rs` runs the cell at a hundredth of its data beside work on the shards
and on a core of its own and reads both reports.

**Rounds.** `shoal-spike/results/x9-lab.sh` runs from europa. Its `setup` makes the pool devices
and writes the evo leg's rings through once, each round runs both legs, in order in odd rounds and
the other way in even ones, and `x9-host.sh` runs every side of a leg on titan, the order rotated
by the round and reversed in even ones, the tables' storage wiped before each run and the pool's
rings kept, as a slice keeps its file written ahead. `x9-report.py` merges the rounds and judges
T1; its output is `shoal-spike/results/x9-report.md`, and every capture and every report is in
`shoal-spike/results/x9/`.

### Where, and on what

Everything ran on **titan**, a Ryzen Embedded V1756B (Zen1, four cores and eight threads, 14 GiB),
kernel 7.0.0-34, under the `performance` governor for the rounds and `schedutil` before and after.
Its tmdb node was already stopped and was left so. One `znver1` build with the feature ran every
side, on the tree at `6b78b9f` with the spike uncommitted, as `x9-facts.txt` says. Every cell ran
with its locked memory unlimited, as a deployed node's unit has it (`LimitMEMLOCK=infinity`).

**The cores.** Titan's two threads of a core are adjacent cpus: 0 and 1 are core 0, 2 and 3 core
1, 4 and 5 core 2, 6 and 7 core 3. The scratch configuration (`x9-conf.yml`) runs two shards with
`exclude_cores: [0, 3]`, which put them on cpus 2 and 4, one a physical core; the client ran under
`taskset -c 6,7`, the whole of core 3. That left core 0, whose cpu 0 is the coordinator's and runs
nothing in a standalone node, and **the arm on a core of its own gave the coordinating core up**:
its executor ran on cpu 1, beside cpu 0, as the spike's section said one of the four would have to.
In the first round every thread's cpu was read halfway through each side with `ps -L -o psr`, and
every runner reported the cpu it ran on: the shards on 2 and 4, the clients on 6 and 7, the own
executor on 1, in every run.

**The legs:**

| Leg | The tables | The pool | What it stands for |
| --- | --- | --- | --- |
| `null` (judged) | The root ext4 on the 970 EVO, at `/var/tmp/x9/shoal` | A null_blk device of 4 GiB (`queue_mode=2`, `bs=4096`, `memory_backed=0`, completions in a softirq), written raw through a node `mknod`ed on `/xfs` | A pool on a device of its own, as S15's budget is stated, that takes any rate and copies nothing: every difference is the executor's |
| `evo` | The same | The XFS volume on the same 970 EVO, `/xfs/x9/pool`, a ring file a runner, written through once in `setup` | The lab as it is fitted: the pool shares the tables' device, and the tables' syncs queue behind its writes |

The evo leg ran at 100 MiB/s alone: 500 MiB/s of 4+2 writes about 786 MB/s, past what the 970 EVO
takes on its one PCIe lane, which [X6](device-store-ssd.md#two-things-about-the-labs-970-evos)
put at about 720 MiB/s.

Where the run departed from the plan on the spikes page:

- **Two legs, and a null device for the judged one.** The plan named titan and no device. Decided
  with the user: a device of the pool's own, as S15's budget is stated, and the 970 EVO beside it.
  A RAM disk was chosen first and replaced before any run by null_blk, which the user also chose:
  `io_uring` issues a direct write to a RAM disk inline, so its copy of every unit would have run on
  the submitting shard's cpu, by estimate about as much as the checksum and the encode together.
  null_blk with `memory_backed=0` copies nothing.
- **Two shared arms, and a supplement.** The plan named one, a queue "at low priority"; glommio's
  scheduler made a second with a latency goal necessary to test the design S13 describes, which the
  user agreed; and `shards-fine` was added after the quick run.
- **A receive step.** The plan named checksum, encode and direct writes. A stager also copies a
  client's bytes in from its socket, so each data unit is copied in from a source too large for the
  cache.
- **Eight rounds, the side's order rotated by the round.** A side is a run of about six seconds, so
  eight rounds of the twenty-six sides took under half an hour.
- **The procedure's layout and locked memory were corrected**, which the spike found wrong for
  these hosts ([below](#what-it-found)).

Two things were changed after trying the harness, and a first attempt at the rounds was stopped:

- the backlog bound above, which the first quick run showed was needed;
- `shards-fine`, after the second quick run;
- the rounds were stopped in their first round and started again when `rusty_erasure`'s two
  sub-crates were found resolved at 0.4.2, where X4 measured 0.4.1; the lockfile holds them at
  0.4.1, and the eight rounds ran on that build.

## How to read the tables

Every figure is the median of eight rounds with the lowest and the highest round in brackets, in
microseconds unless it says otherwise. A ratio is the side's median over the cell alone's on the
same leg. *Above* and *below* say the two sides' intervals do not overlap, *within* that they do; a
ratio in bold fired T1's line. The tables in full, every side's medians and the work's steps by
kind, are `shoal-spike/results/x9-report.md`; the ones here are cut from it.

## 1. The cell alone, and beside work on a core of its own

On the null leg, where the pool is a device of its own:

| Side | Rate MiB/s | Unit | Read p99 | ÷ alone | Write p99 | ÷ alone | Ops/s |
| --- | ---: | --- | ---: | --- | ---: | --- | ---: |
| alone | | | 425 [398–474] | | 14,841 [14,367–15,896] | | 8,323 [8,189–8,518] |
| core of its own | 100 | 64 KiB | 426 [398–453] | 1.00×, within | 14,940 [13,713–16,647] | 1.01×, within | 8,250 [8,073–8,513] |
| core of its own | 100 | 1 MiB | 450 [419–473] | 1.06×, within | 14,804 [13,429–15,786] | 1.00×, within | 8,269 [8,101–8,449] |
| core of its own | 500 | 64 KiB | 411 [399–438] | 0.97×, within | 15,309 [13,908–15,749] | 1.03×, within | 8,399 [8,252–8,556] |
| core of its own | 500 | 1 MiB | 451 [402–705] | 1.06×, within | 14,681 [13,975–15,865] | 0.99×, within | 8,258 [8,126–8,314] |

**A core of its own costs the cell nothing measurable**, at either rate and either unit: every
interval overlaps the cell alone's. The executor on cpu 1 shares core 0 with a coordinator that
runs nothing, and the 4 MiB L3 that all four cores share with every shard; at 500 MiB/s of 1 MiB
units it streamed 750 MiB/s of units through that cache, and one round's p99 of 705 µs is the
highest any round of this arm reached.

## 2. On the table shards, a step a unit

| Side | Rate MiB/s | Unit | Read p99 | ÷ alone | Write p99 ÷ alone | Hold p50 | Hold p99 |
| --- | ---: | --- | ---: | --- | --- | ---: | ---: |
| `shards`, no goal | 100 | 64 KiB | 472 [430–606] | 1.11×, within | 1.05×, within | 95 | 242 |
| `shards`, no goal | 100 | 1 MiB | 977 [809–1,352] | **2.30×**, above | 1.02×, within | 527 | 2,114 |
| `shards`, no goal | 500 | 64 KiB | 511 [471–539] | 1.20×, within | 1.00×, within | 83 | 206 |
| `shards`, no goal | 500 | 1 MiB | 1,616 [1,542–1,967] | **3.81×**, above | 1.05×, within | 530 | 1,938 |
| `shards-lat`, 250 µs goal | 100 | 64 KiB | 446 [427–493] | 1.05×, within | 1.02×, within | 89 | 217 |
| `shards-lat`, 250 µs goal | 100 | 1 MiB | 861 [774–883] | **2.03×**, above | 1.02×, within | 287 | 468 |
| `shards-lat`, 250 µs goal | 500 | 64 KiB | 501 [467–633] | 1.18×, within | 1.03×, within | 80 | 199 |
| `shards-lat`, 250 µs goal | 500 | 1 MiB | 1,237 [1,143–1,445] | **2.91×**, above | 1.08×, within | 285 | 426 |

The hold columns are the larger of the two shards' figures, the median over the rounds.

**Where a step is a 1 MiB unit, both shared arms fire, at both rates**: the cell's read p99 rose
2.0 to 3.8 times. **Where a step is a 64 KiB unit, neither does**: 1.05 to 1.20 times, every
interval overlapping the cell alone's. No write p99 moved on this leg; a write waits about 7 ms on
the WAL's sync, and a millisecond's stall is inside its noise.

The step is what decides it. At 1 MiB a step held a Zen1 core for 150 to 290 µs at its p99: the
receive, the checksum and the slice of the encode each cost about the same, as they were cut to.
At 64 KiB each held it for 7 to 40 µs.

## 3. What a table waits behind: the hold

A table's query waits for the hold the shard is in, not for a step. With no goal on its queue the
runner kept the executor for a whole stripe: a hold of about 530 µs at the median and 2 ms at the
p99 at 1 MiB, from the first receive to the writes. Its offers to yield were taken 915 times of
6,790 at 100 MiB/s, when the high queue's own 500 µs goal happened to arm the timer. The cell's read
p99 beside it, 977 µs, is about its p99 alone plus a median hold.

A 250 µs goal on the third queue arms glommio's timer whenever the runner is active, and the
runner took 44% of its offers: the hold fell to 430 to 470 µs at the p99, the goal plus one step.
That took a fifth off the rise at 100 MiB/s, 2.30× to 2.03×, and a third at 500, 3.81× to 2.91×,
and did not reach the line. **A latency goal can only cut between steps**, so the hold is bounded by the goal
plus the longest step, and at 1 MiB the step alone is half the cell's p99.

## 4. Steps inside a unit: the supplement

`shards-fine` cut every unit into steps of 64 KiB and put the third queue at a 100 µs goal:

| Rate MiB/s | Unit | Read p99 | ÷ alone | Write p99 ÷ alone | Hold p99 | Yields taken |
| ---: | --- | ---: | --- | --- | ---: | ---: |
| 100 | 64 KiB | 443 [427–555] | 1.04×, within | 1.02×, within | 127 | 3,353 of 107,450 |
| 100 | 1 MiB | 503 [496–512] | 1.18×, above | 1.03×, within | 155 | 9,547 of 109,312 |
| 500 | 64 KiB | 514 [489–591] | 1.21×, above | 1.00×, within | 127 | 9,656 of 541,940 |
| 500 | 1 MiB | 642 [611–760] | **1.51×**, above | 1.09×, within | 148 | 49,326 of 572,096 |

**Steps of 64 KiB under a 100 µs goal bring 1 MiB units under the line at 100 MiB/s, 1.18×, and not
at 500, 1.51×.** The holds were alike at every cell, 125 to 155 µs at the p99, so what is left at
500 MiB/s of 1 MiB units is not the hold. Two things differ from 64 KiB units at the same rate,
1.21×: a stripe of 1 MiB units is 6 MiB passing through the shard's core and its 512 KiB of L2,
where one of 64 KiB units is 384 KiB; and the runner took five times the yields, each a switch of
queue. Which of them it is was not traced. At 64 KiB units this arm moved nothing the planned arms
had not.

## 5. The 970 EVO leg: the device shared

With the pool on the device the tables' WAL syncs to, at 100 MiB/s:

| Side | Unit | Read p99 ÷ alone | Write p99 | ÷ alone | Ops/s |
| --- | --- | --- | ---: | --- | ---: |
| alone | | | 14,314 [13,933–16,519] | | 8,296 [8,132–8,471] |
| `shards` | 64 KiB | 1.16×, within | 18,572 [17,430–19,816] | **1.30×**, above | 6,800 [6,448–6,969] |
| `shards` | 1 MiB | **2.09×**, above | 27,101 [21,304–28,738] | **1.89×**, above | 6,591 [6,432–6,677] |
| `shards-lat` | 64 KiB | 1.16×, within | 18,138 [17,374–18,880] | **1.27×**, above | 6,709 [6,508–6,934] |
| `shards-lat` | 1 MiB | **1.73×**, above | 25,933 [23,217–27,923] | **1.81×**, above | 6,541 [6,437–6,586] |
| `shards-fine` | 64 KiB | 1.14×, above | 18,121 [17,217–19,544] | **1.27×**, above | 6,735 [6,557–6,811] |
| `shards-fine` | 1 MiB | 1.22×, above | 26,696 [23,181–27,661] | **1.86×**, above | 6,575 [6,444–6,734] |
| core of its own | 64 KiB | 1.07×, within | 17,257 [15,798–19,148] | 1.21×, within | 6,973 [6,814–7,106] |
| core of its own | 1 MiB | 1.09×, within | 19,874 [19,238–21,298] | **1.39×**, above | 6,599 [6,502–6,767] |

**Here the writes move, wherever the work runs.** 150 MiB/s of units on the one-lane 970 EVO put
each WAL sync behind the pool's writes and the pool's group syncs: the cell lost a sixth to a fifth
of its operations a second on every arm, and its write p99 rose 1.2 to 1.9 times, 1.39 times on a
core of its own at 1 MiB. The reads follow the null leg's pattern: the 1 MiB arms on the shards
fire, and the 64 KiB ones and the core of its own do not. A pool's group sync took 6 ms at its p99
on a core of its own and 8 to 15 ms on the shards. This is why S15's budget is stated
for pools on their own devices, and why [S13](isolation.md#io-on-a-slice) labels a node where they
are not: a core of its own does not protect a table from a device it shares.

## 6. What a stager's work costs a core

The arm on a core of its own took no yield, so each of its holds is one stripe's compute whole, and
it is Q15's figure: what the node that received a client's bytes spends on them before it sends
them on.

| Unit | A core's time a MiB of data | Receive | Checksums, data and parity | Encode | 500 MiB/s takes |
| --- | ---: | ---: | ---: | ---: | ---: |
| 64 KiB | 0.37 to 0.41 ms | 0.10 ms | 0.16 ms | 0.14 ms | 18 to 21% of a core |
| 1 MiB | 0.49 to 0.50 ms | 0.14 ms | 0.17 ms | 0.19 ms | 25% of a core |

Measured on titan from each step kind's median, on the null leg; the evo leg's were within 5%. At
100 MiB/s that is 4 to 5% of a core. The checksums are as dear as the encode, as X5 said they would
be on Zen1, and the copy in costs three quarters of the encode. 1 MiB units cost a quarter more a
byte than 64 KiB ones; a 1 MiB step does not fit the core's 512 KiB of L2, which is the likely
reason and was not traced. None of this counts the writes'
submission, the sends a stager makes to the stripe's other holders, or kTLS, which X11 found holds
an executor about a millisecond a 1 MiB frame on Zen1.

## What would have changed the design

**It came out, at 1 MiB.** With a step of a 1 MiB unit, object work on the table shards moved the
cell's read p99 2.0 to 3.8 times, past the line on both planned arms at both rates, with or without
a latency goal on its queue. By the rule agreed, dedicated executors are required for such work,
and a node of four cores gives one up or does not serve a pool. At 64 KiB units neither arm fired,
and sharing would have been allowed there. And a core given up was enough: on titan the
coordinating core's other thread carried 500 MiB/s of either unit at 0.97 to 1.06 times the cell
alone.

## Recommendation

**Object work runs on executors of its own, and M14 builds no other way.** It is the one placement
that held the cell inside its line at every rate and unit, and the one S13 already preferred. A node
of four cores gives one core up; where the node is standalone, the coordinating core's other thread
is enough for 500 MiB/s.

Sharing a table's executor is not built at M14, though X9 found where it would hold: at 64 KiB
units to 500 MiB/s with a third queue at any goal, and at 1 MiB units cut into 64 KiB steps under a
100 µs goal to 100 MiB/s. The chunk unit is not chosen ([Q20](contract.md#questions-to-answer)'s
geometry), and a mode whose safety depends on it, and on a rate the node does not control, would be
a second way to build every object loop. What it would need is filed in
[TODOs](../appendix/todos.md#object-work-on-the-table-shards).

What M14's dedicated executors inherit from it:

- **Every object loop yields in steps a goal can cut, whatever executor it runs on.** A slice's
  executor also serves the object lane and its other slices; a 1 MiB step there holds a stage
  behind it as it held a table's read here.
- **A goal is what makes a yield real.** glommio takes `yield_if_needed` only on a latency-ring
  event, and with no queue active at a goal its timer is 100 ms; an unconditional yield picks the
  next runnable queue without polling. An object queue has a latency goal of its own.
- **The core given up is named in the configuration**, as the control core is, and refused if a
  shard holds it.

## What X9 does not settle

- **The chunk unit**: Q20's geometry, at M18. X9 says only that a unit above 64 KiB is cut into
  steps of 64 KiB or less on any executor that serves anything else.
- **Which executor owns a slice, the lane and the memory budget**: the rest of Q24, at M14 and M15.
  X9 ran one executor's worth of work and no lane.
- **What was left at 500 MiB/s of 1 MiB units with short holds**: not traced.
- **A node of more cores, or a cluster node**, whose control thread holds core 0: the core an object
  executor takes there is M14's to name.

## What it did not measure

- **The network.** No stripe was sent to another holder and no frame was encrypted, so a stager's
  sends and kTLS, which X11 measured, are not in the figures; they fall on the same executor.
- **Reads, degraded reads, rebuilds and scrubs.** A degraded read decodes, a rebuild reads `k`
  chunks for one; X12 measures recovery and scrub beside a foreground.
- **Hyperion**, which did not repeat titan; and **europa**, whose Zen4 encodes and checksums several
  times faster.
- **A real pool device apart from the tables'.** The null device has no latency; the 970 EVO leg
  shares the tables' device. The rotational disk could not take the rates.
- **More than two shards, or a table workload other than the reference cell.**

## What it found

- **The lab's before-and-after procedure ran a node unlike the one it described**: on titan and
  hyperion a core's two threads are adjacent cpus, so its layout put the client on a shard's other
  thread and left a core idle, and under a login's locked memory limit no shard registered its
  buffers, which a deployed node always does. F73's 48 runs logged it on both sides. Filed and
  fixed in one change as [Resolved #218](../appendix/resolved/lab-core-layout.md); the procedure
  now gives `exclude_cores: [0, 3]`, the client on cpus 6 and 7, and an unlimited `prlimit`.
- **`rusty_erasure` 0.4.1 resolves its sub-crates to 0.4.2**, whose kernel selection moved, so the
  version X4 measured is not what a fresh resolve builds. The workspace's lockfile holds them at
  0.4.1; M18 pins all three.
- **glommio's cooperation is by latency goal, not by yield.** Read in the fork while the harness was
  designed and borne out by the yields taken at 1 MiB: 13% of offers with no goal on the queue,
  44% with a 250 µs one.

## Related

[S13](isolation.md), the page this was for; [S15](performance.md) for the budget;
[S18](contract.md#q24-and-q15-in-part-table-latency-beside-object-work-2026-10-09) for the record;
[X4](erasure-coding-crates.md) and [X5](checksums.md) for the encode and the checksum it ran;
[X11](streamed-bodies.md) for what a frame costs the executor that sends it;
[X3](bytes-through-groups.md) for object bytes as rows on the table shards;
[Benchmarking](../performance/benchmarking.md#before-and-after-on-the-lab) for the lab's rules.
