# X17. A page rebuilt

**Its reads half reported 2026-10-30.** This is the record of spike
[X17](spikes.md#x17-a-page-rebuilt), in two halves. The first, here, is what a page's archive reads
buy: several in flight, runs across small gaps in the order records lie on disk, one operation a
record against three, and the bench driver's move to another tablet when shed
([O139](../../appendix/optimizations.md#o139-a-scan-driver-asks-a-shed-tablet-again-where-another-tablet-would-be-answered)),
with a profile of a walked partition and the cell beside a scan. It was taken in one lab window
with [X37](tablet-records-together.md), whose two layouts every arm here ran over: `Big` merged in a
hash map's order, as before [F121](../../features/merges-in-key-order.md), and in key order. The
second half, the plan step, encodes in steps, rows against partitions and saved positions, is a
window of its own, reported below once it has run.

Four rounds a layout, every host identical to its snapshot after. The answers:

- **Runs in disk order over merges in key order are the lever: eleven and a half times today's
  walk.** With the pace lifted, a scan of `Big` read 150,000 partitions a second for the cluster one
  record at a time, 484,000 with sixteen reads in flight, and 1,720,000 in runs across 4 KiB gaps
  with sixteen in flight, 1.71 GB a second; at four pages a shard, 1,920,000. Over merges in a hash
  map's order a run is one record, and runs are reads in flight and no more: 500,000.
- **W12's cost budget is met, in runs over key order and by nothing else.** A walked partition cost
  a Zen1 host 46.4 µs of its node's cpu one at a time, 22.3 µs with sixteen in flight and 5.6 µs in
  runs (europa 17.3, 7.5 and 1.7 µs), at 11.1 partitions a device read; the budget is at most about
  15 µs and at least ten partitions a read ([W12](performance.md#the-budgets)). Over hash order the
  best was 21.3 µs at 1.01 partitions a read.
- **One operation a record saves a fifth one at a time**, 46.4 µs against 58.5 µs on the Zen1 hosts
  and 17.3 against 22.3 µs on europa, the ranges disjoint, and 8% at sixteen in flight; neither rate
  moved beyond its range ([F120](../../features/one-operation-a-record-read.md)).
- **At the product's pace the walk reaches its ceiling with four reads in flight.** In a first run
  at the default 100 MiB a second a device, every setting with reads in flight read about 249,000
  partitions a second for the cluster, titan at its ceiling and a page spending 33 to 41 ms of its 41
  to 55 ms waiting for its pace; runs paced there cost the Zen1 hosts 11.7 µs a partition.
- **At the product's pace neither walk holds [W7's budget](isolation.md#the-budget) beside the
  cell.** One record at a time, a scan moved the cell's read p99 11% and its insert p99 30%; in runs,
  reading 240,000 partitions a second, five times as many, 28% and 20%. Over hash order the runs
  moved them 1.57 and 1.64 times: the same bytes in eleven times the device reads. The pace is
  X18's.
- **O139 moved europa's cost a partition, 20.9 to 17.3 µs, and little else**: sheds a page
  answered 23.7 before and 19.2 after, the ranges overlapping, the Zen1 hosts' cost and the rate
  unchanged. Sheds fall with a page's time, to 2.1 a page in runs.
- **Where the cpu goes**: one record at a time, the ring and the kernel's syscall path took about a
  quarter of titan's node's samples and Shoal's own code 3%; in runs, Shoal's code 18% (the spike's
  plan of a page, the fold, the checksum, the index's scan), the allocator 8%, validation 5%, a
  channel a record 4%, the ring 5.5%.

## The question

The spike page's question, the part this half answers: what does each change to a page's reads buy
on its own, reads in flight against one at a time, runs in disk order against reads in key order,
one operation a record against three, and where does a walked partition's cpu go? Since
[W42](scan-speed.md) these are the levers on [W12](performance.md#the-budgets)'s cost budget, at
least ten partitions a device read and at most about 15 µs of a Zen1 host's cpu a partition, where
X16 measured 0.99 reads and up to 73 µs of host cpu. It feeds [Q33](contract.md#questions-to-answer)
(what reads a scan's page).

**What would have changed the design**, named on the spike page before the run: if runs buy little
because the archives lie mostly in key order after archive passes, reads stay in key order with
several in flight; W42 found the lab's archives in no such order, so runs wait on merges written in
key order and reads in flight come first either way. If the profile finds most of a partition's cpu
outside its read's operations, W12's cost budget is met there, not at the ring.

## How it was judged

Each arm is a scan of `Big` alone, whole scans back to back from every member's holders at the
driver's default of two pages in flight a shard of every member, 32 streams, for 5 s of warmup and
45 s measured, under a pace lifted to 4 GiB a second a device so that the walk, not its pace, sets
the rate. **Its rate** is partitions a second over the measured window. **Its cost** is each host's
counters over the whole run, warmup and measured, divided by every partition both read: device reads
and bytes a partition (`/proc/diskstats` of the devices the roots are on) and the node process's cpu
a partition, given for the Zen1 hosts together, as W12's budget is stated, and for europa apart, each
by the partitions its own member read. A minute of the cluster idle each round gives the background
no capture subtracts: 0.26 busy threads on each Zen1 host and 0.18 on europa, about 3 µs of a
partition one record at a time and under half a microsecond in runs.

**The lab's rule for a difference**: a difference is a result only where two arms' rounds give
ranges that do not overlap. Four rounds a layout, the layouts alternating which goes first.

## What was run

The cluster, the build and its switches, and the table and its layouts are
[X37's](tablet-records-together.md#what-was-run): `x17`, three nodes on the lab's hosts at 4 GiB a
node; one spike build whose switches set a merge's order, a page's reads in flight, the gap a run
reads across and a record read's operations; `Big` loaded fresh for each layout of each round, and
every node restarted before each arm.

### The read settings

| Arm | Reads in flight | Run gap | Operations a record | What it is |
| --- | --- | --- | --- | --- |
| `r1` | 1 | none | one | Today's walk since F120: a page reads its records one at a time, in key order |
| `r4` | 4 | none | one | Reads in flight, each one record, issued in key order |
| `r16` | 16 | none | one | The same, sixteen |
| `g4k2` | 2 | 4 KiB | one | A page's records planned in disk order at its start and cut into runs across gaps of at most 4 KiB and a span of 1 MiB, two runs in flight: [W5](tablet-reader.md#3-read-archives-in-the-order-they-lie-on-disk)'s design |
| `g4k16` | 16 | 4 KiB | one | The same, sixteen runs |
| `g128k2` | 2 | 128 KiB | one | O78's gap, two runs |
| `t1` | 1 | none | three | Before F120: every record read a `dup`, a read and a close |
| `t16` | 16 | none | three | The same, sixteen in flight |
| `g4k16p4` | 16 | 4 KiB | one | `g4k16` at four pages a shard (`scans.pages_in_flight`) and 64 streams |
| `shedstay` | 1 | none | one | Today's walk, the driver staying on a shed tablet as before O139 (`SHOAL_SPIKE_SHED_STAY=1` on the driver); key order alone |
| `afterchurn` | 1 | none | one | Today's walk after twenty minutes of inserts and overwrites into `Big`, in round one and in X37's supplement, whose runs after churn are [X37's](tablet-records-together.md#after-churn) |

A planned page draws and paces every byte before it starts a run, reads the runs on a task of its
own with up to the setting in flight, and hands each record to its place in key order, so a page's
rows still go out in key order and a budget's shed stops it where it is.

### The other runs

| Capture | What it measures |
| --- | --- |
| `cell-r1`, `cell-g4k16` | `rw50/b1/none` then `rw50/b1/scan` on `Cell`, a whole scan of `Big` 30 s into the second, as X16 drove it, at today's walk and in runs, at the product's pace (100 MiB a second a device, a trickle of 2 MiB, two pages a shard) |
| A profile | Round one, key order: `perf record -F 999 -g` of titan's node for 20 s, 15 s into `r1` and `g4k16`, its self time sorted into kinds of work |
| An idle minute | Every round: every host's `/proc/stat` over a minute of the cluster up and nothing driving it |
| The first run | Round one's key layout at the product's pace, before the run was started again with it lifted (below) |

## What was found on the way

### The default pace binds

The first full run took every scan at the product's pace and stopped at its churn (X37's record
says why). Its round one says the pace, not the walk, set every rate with reads in flight: a page
spent most of its time waiting for its device's credit, and titan read about 96,000 partitions a
second, its ceiling's 100 MiB of records. The run was started again with every scan alone under a
pace of 4 GiB a second, and the cell's arms at the product's pace, so the cell is judged as M23
would ship it. Its round one, at the default pace, one round:

| Arm | Partitions a second | Page read p50 ms | Of it, paced, p50 ms | Zen1 hosts' µs a partition |
| --- | --- | --- | --- | --- |
| `r1` | 166,000 | 94.2 | 0 | 45.8 |
| `r4` | 242,000 | 55.0 | 33.3 | 30.0 |
| `r16` | 249,000 | 52.0 | 41.0 | 22.6 |
| `g4k2` | 249,000 | 43.5 | 37.6 | 13.3 |
| `g4k16` | 250,000 | 41.0 | 38.4 | 11.7 |
| `t1` | 143,000 | 106 | 0 | 57.7 |
| `t16` | 249,000 | 53.8 | 41.2 | 25.2 |

A paced partition in runs cost twice what it did unpaced, 11.7 µs against 5.6. The node's
background is about 2 µs a partition at this rate; the rest is likely a page's waits for credit,
asked again every millisecond (`PACE_POLL`), which this run did not separate, filed as
[O143](../../appendix/optimizations.md#o143-a-paced-scan-pages-waits-for-credit-double-its-cost-a-partition). Its captures are in `target/lab/x17/attempt1`, not in the results, since every figure
there is the pace's.

**A scan after churn read one record at a time**, so it counted stretches but not runs across the
holes; X37's supplement measured that.

## The tables

Medians over the four rounds, the range beside each in brackets; **Disjoint** says whether the two
layouts' ranges do not overlap, and **× r1** is a key-order figure over key order's `r1`. The whole
set is [x17-report.md](../../../../shoal-spike-scan/results/x17-report.md), from the slimmed
captures and profiles in [shoal-spike-scan/results/x17](../../../../shoal-spike-scan/results/x17).

### A scan's rate

Partitions a second for the cluster, `scan/Big/none` with the pace lifted:

| Arm | Hash order | Key order | Disjoint | × r1 |
| --- | --- | --- | --- | --- |
| `r1` | 149,000 [111,000, 159,000] | 150,000 [149,000, 165,000] | no | |
| `r4` | 363,000 [332,000, 398,000] | 383,000 [346,000, 402,000] | no | 2.55 |
| `r16` | 480,000 [450,000, 520,000] | 484,000 [432,000, 530,000] | no | 3.22 |
| `g4k2` | 235,000 [187,000, 260,000] | 1,250,000 [1,170,000, 1,310,000] | yes | 8.35 |
| `g4k16` | 500,000 [452,000, 547,000] | 1,720,000 [1,490,000, 1,860,000] | yes | 11.5 |
| `g128k2` | 151,000 [137,000, 160,000] | 1,250,000 [1,130,000, 1,330,000] | yes | 8.36 |
| `t1` | 119,000 [102,000, 147,000] | 137,000 [112,000, 153,000] | no | 0.92 |
| `t16` | 464,000 [382,000, 503,000] | 463,000 [402,000, 500,000] | no | 3.09 |
| `g4k16p4` | 487,000 [447,000, 563,000] | 1,920,000 [1,660,000, 2,080,000] | yes | 12.8 |
| `shedstay` | | 153,000 [144,000, 164,000] | | 1.02 |

In runs the hosts read unevenly: in round one's `g4k16`, hyperion 649,000 partitions a second at
699 MiB a second of device reads, its device busy 99% of the time, titan 479,000, and europa
358,000, since it leads most groups (`lead_weight 2`) and the driver asks followers first.

### A partition's cost

The node's cpu a partition, by the partitions each host's member read:

| Arm | Zen1, hash | Zen1, key | europa, hash | europa, key |
| --- | --- | --- | --- | --- |
| `r1` | 46.0 [45.3, 48.5] | 46.4 [45.4, 46.8] | 18.2 [17.6, 18.6] | 17.3 [16.5, 18.1] |
| `r4` | 26.7 [26.4, 27.0] | 27.1 [26.7, 27.2] | 7.97 [7.71, 8.07] | 7.77 [7.55, 8.14] |
| `r16` | 21.4 [21.1, 21.6] | 22.3 [21.8, 22.5] | 7.23 [7.09, 7.63] | 7.47 [7.18, 7.83] |
| `g4k2` | 36.3 [36.0, 38.0] | 7.01 [6.97, 7.08] | 12.2 [11.6, 12.7] | 2.09 [2.00, 2.17] |
| `g4k16` | 21.3 [21.1, 21.5] | 5.56 [5.49, 5.70] | 7.45 [7.20, 7.77] | 1.71 [1.66, 1.75] |
| `g128k2` | 42.9 [42.0, 43.5] | 7.04 [6.90, 7.11] | 13.9 [13.6, 14.2] | 2.09 [2.00, 2.21] |
| `t1` | 58.4 [56.8, 60.1] | 58.5 [56.8, 59.0] | 24.0 [22.4, 24.4] | 22.3 [21.3, 24.1] |
| `t16` | 23.2 [22.8, 23.6] | 24.2 [23.7, 24.4] | 8.00 [7.81, 8.23] | 8.17 [7.84, 8.33] |
| `g4k16p4` | 21.5 [21.3, 21.9] | 5.64 [5.58, 5.69] | 7.82 [7.67, 8.12] | 1.78 [1.78, 1.96] |
| `shedstay` | | 47.6 [46.5, 48.8] | | 20.9 [20.0, 21.7] |

Partitions a device read were 1.01 for every setting but runs over key order, which read 11.1 (10.6
on the Zen1 hosts, 12.6 on europa); the table of what a device read carries is
[X37's](tablet-records-together.md#a-tablets-records-and-what-a-walk-reads-of-them).

### A page

| Arm | Read p50 ms, key | Read p99 ms, key | Sheds a page answered, key | Sheds, hash |
| --- | --- | --- | --- | --- |
| `r1` | 94.5 [92.2, 95.2] | 114 [111, 115] | 19.2 [15.7, 21.1] | 18.6 [16.8, 31.9] |
| `r16` | 31.1 [27.5, 32.1] | 40.7 [39.4, 42.0] | 6.15 [5.38, 7.05] | 6.20 [5.48, 7.20] |
| `g4k2` | 10.7 [9.92, 10.9] | 14.5 [14.3, 15.4] | 2.96 [2.65, 3.08] | 12.6 [10.8, 19.1] |
| `g4k16` | 6.78 [5.98, 6.94] | 11.4 [10.8, 11.7] | 2.13 [1.96, 2.52] | 5.83 [4.98, 7.17] |
| `g4k16p4` | 13.5 [11.8, 14.5] | 22.7 [22.4, 23.7] | 2.48 [1.96, 2.54] | 7.11 [5.04, 9.72] |
| `t1` | 106 [103, 108] | 130 [126, 133] | 21.7 [16.1, 28.0] | 25.6 [17.4, 32.2] |
| `shedstay` | 91.6 [90.1, 92.2] | 111 [109, 112] | 23.7 [20.4, 25.8] | |

A page is a whole tablet, 732 partitions, and holds its loop about 0.11 ms at p99 on every
setting: nothing of `Big` is resident after a restart, so a capture is the archive's view alone.

### The cell beside a scan, at the product's pace

`rw50/b1/scan` on `Cell`, its read and insert p99 in the scan's *during* window over its *before*
window, as X16 judged it against W7's budget of `max(1.10 × alone, alone + 0.1 ms)`:

| Figure | `r1`, hash | `r1`, key | `g4k16`, hash | `g4k16`, key |
| --- | --- | --- | --- | --- |
| Read p99 ms before | 5.48 [5.15, 5.85] | 5.81 [5.70, 5.98] | 5.50 [5.38, 6.16] | 6.09 [5.75, 6.28] |
| Read p99 ms during | 5.87 [5.56, 6.47] | 6.44 [6.34, 6.66] | 8.54 [8.26, 10.1] | 7.73 [7.02, 8.52] |
| Read p99, during over before | 1.08 [1.06, 1.11] | 1.11 [1.09, 1.12] | 1.57 [1.51, 1.64] | 1.28 [1.18, 1.38] |
| Insert p99 ms before | 23.2 [22.3, 24.6] | 24.6 [24.2, 25.2] | 24.5 [23.2, 24.8] | 24.3 [23.6, 25.4] |
| Insert p99 ms during | 34.0 [29.6, 38.6] | 32.1 [27.7, 36.6] | 39.8 [37.8, 40.9] | 28.6 [26.2, 32.0] |
| Insert p99, during over before | 1.46 [1.32, 1.57] | 1.30 [1.13, 1.48] | 1.64 [1.56, 1.68] | 1.20 [1.03, 1.30] |
| Scan partitions a second during | 36,900 [28,600, 46,900] | 46,500 [33,300, 54,500] | 200,000 [176,000, 214,000] | 240,000 [214,000, 250,000] |

In runs the two layouts' ratios are disjoint: hash order's scan reads its bytes in eleven times
the device operations.

### Where the cpu goes

titan's node under a scan, round one, key order: its self time by kind of work, from the report's
top symbols (two thirds and three quarters of every sample):

| Kind | `r1` | `g4k16` |
| --- | --- | --- |
| Shoal's own code | 3.0% | 17.8% |
| The kernel's io_uring | 12.5% | 5.5% |
| The kernel's syscall entry and exit, with the SRSO mitigation | 7.4% | 3.2% |
| glommio and liburing | 6.5% | 3.9% |
| The allocator (mimalloc) | 2.9% | 7.7% |
| Clocks (`clock_gettime`, `Instant`) | 3.1% | 5.5% |
| Block, NVMe, iomap and XFS | 4.5% | 4.7% |
| IPIs and membarrier | 4.5% | 1.2% |
| Scheduling and futexes | 3.7% | 1.1% |
| rkyv's validation | 0.2% | 4.6% |
| kanal and futures | 0.1% | 3.7% |

In runs the top symbols are the spike's own: the closure that starts a planned read (3.4%), the
page's fold (2.9%), a oneshot channel's send for every record (2.9%, and 0.6% making them), rkyv's
validation (4.4%), the record's checksum (2.1%) and the index's scan (2.2%). One record at a time,
it is the ring's submission and completion and the kernel's path to them.

## What it says for the design

- **W5's reads, in this order of worth**: runs in disk order where a tablet's records lie together,
  which needs merges in key order; reads in flight; one operation a record. Runs across 4 KiB read
  what runs across 128 KiB do over key order and a seventh of the bytes over hash order, so a few
  KiB is the gap. Sixteen runs in flight read 38% faster than two and cost a fifth less a partition, so
  W5's "at most two runs in flight for a page" is a bound to raise; at the ceiling either suffices.
- **The rate is the pace's, by a wide margin.** Unpaced, runs read 3.6 to 6.6 times a device's
  default ceiling of records in round one, europa least; at the ceiling the walk needs four reads
  in flight to reach it. So M23's rate
  budget, the ceiling, is met by the reads alone, and what the foreground can bear is X18's to set.
- **The cost budget is met where the archive keeps a tablet's records together**, 5.6 µs and 11.1
  partitions a read on a table loaded once; what churn does to that is
  [X37's](tablet-records-together.md#after-churn). Paced at the ceiling the cost doubled, so a
  page's wait for credit is worth a look before M23 counts its cpu
  ([O143](../../appendix/optimizations.md#o143-a-paced-scan-pages-waits-for-credit-double-its-cost-a-partition)).
- **The profile says the rest is per partition**, not per read: the spike's channel a record and its
  allocations, validation and the checksum. M23's runs should hand a run's records on in place,
  never a channel and an allocation each.
- **O139 is worth keeping but is not a lever**: sheds fall with a page's time, which runs cut
  fourteenfold.
- **Two pages a shard are enough**: four add 11% unpaced, past every ceiling X18 tries.

## Limitations

- **The admin walk answers keys and row counts, never rows**, so no figure here carries a row's
  bytes over the wire or its decode on a client; that is the second half's encode benchmark, in
  process.
- **Big after a restart has nothing resident**, so a page's capture holds its loop microseconds and
  nothing here measures the plan step or encodes; that is the second half's, on `Keyword`.
- **The driver asks followers first**, so europa, which leads most groups, read the fewest
  partitions; each host's cost is by its own member's partitions, which this does not bias, but the
  cluster's rate is the Zen1 hosts'.
- **The spike's runs are a spike's**: a channel and an allocation a record, which a product's runs
  would not carry; its figures are an upper bound on what runs cost.
- **The profile is of the whole node**, every thread, over 20 s of one round, and its table holds
  the report's top symbols only.

## Related

- [X17's spike page entry](spikes.md#x17-a-page-rebuilt), and [X37](tablet-records-together.md),
  taken in the same window
- [F120, one operation a record read](../../features/one-operation-a-record-read.md), and
  [F121, merges written in key order](../../features/merges-in-key-order.md)
- [W5, reading a tablet on its shard](tablet-reader.md#3-read-archives-in-the-order-they-lie-on-disk),
  the design whose reads these are
- [W42, a scan's speed beside ScyllaDB's](scan-speed.md), and [W12's budgets](performance.md#the-budgets)
- [X16, the walk beside the foreground](walk-beside-the-foreground.md), the walk as it stood
- [O139](../../appendix/optimizations.md#o139-a-scan-driver-asks-a-shed-tablet-again-where-another-tablet-would-be-answered),
  the driver's shed streams
