# X37. A tablet's records together, priced

**Reported 2026-10-30.** This is the record of spike [X37](spikes.md#x37-a-tablets-records-together-priced),
taken in one lab window with [X17](spikes.md#x17-a-page-rebuilt)'s reads half
([its record](page-rebuilt.md)), since both read the same scans of the same table over the same two
layouts: `Big` merged in a hash map's order, as every merge was written before
[F121](../../features/merges-in-key-order.md), and in key order, as F121 writes them.

Four rounds a layout, every host identical to its snapshot after. The answers:

- **The sort costs a merge 0.45% of its time.** A merge of `Big`'s load holds 9,150 entries at the
  median; sorting them by key took 302 µs at the median, 35 ns an entry, 1.2 ms at most, beside a
  write of 75 ms. Under churn the share was 0.73%. The order's own memory is 215 KiB at most a merge.
  Both are far under a tenth of a merge's time or memory, so the sort stays where it is.
- **A freshly loaded table's tablet lies in stretches of 11.3 partitions** in key order, against
  one in a hash map's order, the medians of four rounds with every range a tenth wide or less. A walk
  in runs across gaps of 4 KiB reads 11.1 partitions a device read, 10.6 on the Zen1 hosts and 12.6 on
  europa, past [W12](performance.md#the-budgets)'s ten, and 1.10 KiB of device bytes a partition
  against 1.55 one record at a time. In hash order a run is one record (1.01 a read), and runs across
  128 KiB gaps read 10.6 KiB of the device for every 1 KiB partition, for 1.18 partitions a read.
- **Twenty minutes of churn halve the stretches**, to 5.9 partitions: a tenth of the rows
  overwritten leaves a hole of a kibibyte in most stretches, and the churn's own merges hold the
  new rows and the new copies. **Runs read across the holes**: a walk in runs across 4 KiB read 9.9
  partitions a device read after churn, 9.5 on the Zen1 hosts and 11.2 on europa, at 6.0 µs of a
  Zen1 host's cpu a partition and 1.20 KiB of device bytes, and 1,440,000 partitions a second for
  the cluster.
- **A get does not notice**: 107,000 gets a second of `Big` at a p99 of 0.92 ms in key order,
  106,000 at 0.94 ms in hash order, ranges overlapping. Nor does a load: 116 s against 109 s, the
  ranges overlapping.
- **Q62 is settled in part.** Merges in key order are the layout: cheap, and a table loaded once
  meets [W12](performance.md#the-budgets)'s cost budget with room. Consolidation is not settled: a
  table churned twenty minutes reads just under the budget's ten partitions a device read, which by
  the rule named before the run makes consolidated blocks a gate's work. Which gate, and whether ten
  is the line for a churned table, is left to M23's plan, with an hour's churn measured first.

## The question

The spike page's question: what does writing a merge's partitions in key order cost a merge, and
how many stretches does it leave a tablet on its shard, under each load the lab runs: a table
loaded once, a table overwritten, a table inserted into? Is that few enough for
[W12](performance.md#the-budgets)'s cost budget, at least ten partitions a device read, or does a
table need its blocks consolidated? It settles [Q62](contract.md#questions-to-answer), or says why
not.

**What would have changed the design**, named on the spike page before the run: if the sort cost a
merge more than a tenth of its time or memory, it moves off the compactor's critical path, or
merges grow; if a tablet keeps so many stretches that a page reads fewer than ten partitions a
device read, consolidated blocks become a gate's work rather than optional; if archive passes
reorder an overwritten table well enough on their own, consolidation stays optional for good.

## How it was judged

**A stretch** is a run of a tablet's records that lie one after another in one archive, each
beginning where the one before it ended or within 32 bytes of it. A walk counts the stretches of
every page it reads in the order the records lie on disk, so a stretch is what a run across gaps of
a few KiB can read in one operation where nothing between was overwritten, and **partitions a
stretch** is the most partitions a device read could carry for that layout. **Partitions a device
read** is every archived partition a scan read over every read its hosts' devices counted
(`/proc/diskstats`, warmup and measurement), the figure W12's budget names.

**The lab's rule for a difference**: a difference is a result only where the two layouts' rounds
give ranges that do not overlap. Four rounds a layout, the layouts alternating which goes first.

## What was run

### The cluster

`x17`, a side cluster of three nodes on the tmdb cluster's hosts and cores, X16's shape and sizes,
bootstrapped fresh for every layout of every round and destroyed after it; the tmdb node stopped,
every host's maintenance timers held and its governor at `performance` for the whole window, all
put back after ([x17-host-changes.txt](../../../../shoal-spike-scan/results/x17-host-changes.txt);
every host identical to its snapshot afterwards).

| Host | Cpu | Shards | Node memory | Every root on |
| --- | --- | --- | --- | --- |
| europa | Ryzen 9 7945HX (Zen 4, 16 cores, 32 threads) | 6, cpus 1-6 | 4 GiB | the Optane, `/optane/shoal-x17` (XFS) |
| titan | Ryzen Embedded V1756B (Zen 1, 4 cores, 8 threads) | 5, cpus 2-6 | 4 GiB | the 970 EVO, `/xfs/shoal-x17` (XFS on LVM) |
| hyperion | Ryzen Embedded V1756B (Zen 1, 4 cores, 8 threads) | 5, cpus 2-6 | 4 GiB | the 970 EVO, `/xfs/shoal-x17` (XFS on LVM) |

Three copies of every tablet, replication over 1 GbE, every node built for `znver1`. The driver ran
on europa, pinned to cpus 12-15 and 28-31, clear of europa's node. The rounds ran from 07:19 to
12:12 UTC and the churn's supplement from 12:17 to 13:15; afterwards the tmdb cluster's units were
active on all three hosts and every governor was back.

### The build and its switches

One spike build, branch `x17-spike` at `fe041176` from `b74c96bf` (spike code, never merged), node
sha256 `36129ddcd8605f8c...`. Its switches are read once at a node's start from an `Environment=`
drop-in on its unit, so every arm ran the same binary:

| Switch | Values | What it moves |
| --- | --- | --- |
| `SHOAL_SPIKE_MERGE_ORDER` | `hash`, `key` | A merge's order: a hash map's, whole records then fragments, as before F121, or F121's key order |
| `SHOAL_SPIKE_RECORD_OPS` | `three`, `one` | A record read's operations: the `dup`, read and close of before [F120](../../features/one-operation-a-record-read.md), or its one read |
| `SHOAL_SPIKE_READS` | 1, 2, 4, 16 | A page's archive reads in flight |
| `SHOAL_SPIKE_RUN_GAP` | `none`, 4096, 131072 | The gap a run of a page's records may read across, planned in disk order with a span of at most 1 MiB, W5's design |

Every node logs the switches it read at their first use, and in every scan alone each of the three
said what its arm set, 234 of 234 ([switches.tsv](../../../../shoal-spike-scan/results/x17/switches.tsv)).
Every merge on every node logged its entries, its sort's time, the bytes its order held and its
write's time, which the script read back from each node's journal. The script is
[x17-lab.sh](../../../../shoal-spike-scan/results/x17-lab.sh) and the tables come from
[x17-report.py](../../../../shoal-spike-scan/results/x17-report.py).

### The table and the layouts

`Big`, X16's: 3,000,000 unsorted partitions of a kibibyte, 3.07 GB on file, 732 to a tablet, loaded
fresh for each layout of each round with the cell's first 100,000 rows of `Cell`, the switch for its
order in place before the first row. Every arm after the load ran after its switches were set and
every node restarted and settled (every member up, then 30 s), so every partition was read from
its archives, and every scan alone under a pace lifted to 4 GiB a second, so it measured the walk
([X17's record](page-rebuilt.md#the-default-pace-binds) says why).

### The runs

| Capture | What it measures |
| --- | --- |
| `load` | The preload, and every merge it caused |
| `scan-<setting>` | Whole scans of `Big` back to back for 45 s after 5 s of warmup, at every read setting [X17's record](page-rebuilt.md#the-read-settings) lists, each layout |
| `gets` | `read100` over `Big`, a bundle of one, 45 s: a get's p50 and p99 each way |
| `churn` | Round one alone: twenty minutes of `insert100` over `Big` from a churn dataset of its own, 300,000 new rows past `Big`'s last id and an overwrite of every tenth row in turn, then the same rows again once spent (1,780,000 writes at about 1,480 a second) |
| `scan-afterchurn` | Round one alone, after the churn and a restart: a scan of `Big`, now 3,300,000 partitions, at today's walk, its stretches counted again |
| The churn's supplement | Rounds five and six, key order alone: a fresh load, the same churn (1,550 writes a second), then scans after a restart at today's walk and in runs across 4 KiB and 128 KiB, sixteen and two in flight |

## What was found on the way

**The first full run stopped at its churn**, which drove `rw50` over `Big`: with every row of the
dataset preloaded, the bench has nothing left to insert and refuses the arm by name. The churn now
inserts from a dataset of its own. That run's round one is kept apart, since its scans ran at the
product's default pace, which bound them ([X17's record](page-rebuilt.md#the-default-pace-binds)).

**A scan after churn read one record at a time**, so it counted the stretches but not what runs
read across the holes an overwrite leaves; the churn's supplement measured that.

## The tables

Medians over the rounds, the range beside each in brackets, `n` the rounds; **Disjoint** says
whether the two layouts' ranges do not overlap. The whole set is
[x17-report.md](../../../../shoal-spike-scan/results/x17-report.md), from the slimmed captures,
merge logs and profiles in [shoal-spike-scan/results/x17](../../../../shoal-spike-scan/results/x17).

### A merge's sort

Every merge on every node while `Big` was loaded, 1,060 merges a round:

| Figure | Hash order | Key order | Disjoint |
| --- | --- | --- | --- |
| Entries a merge, median | 9,150 [9,150, 9,150] | 9,150 [9,150, 9,150] | no |
| Sort µs a merge, median | 0 | 302 [300, 303] | yes |
| Sort µs a merge, p99 | 0 | 850 [782, 873] | yes |
| Sort µs a merge, most | 2.5 [0, 42] | 1,190 [1,040, 1,300] | yes |
| Sort ns an entry | 0 | 35.1 [34.9, 35.3] | yes |
| Write ms a merge, median | 76.9 [73.6, 81.4] | 74.9 [74.5, 79.8] | no |
| The sort's share of a merge's time | 0 | 0.45% [0.43, 0.45] | yes |
| The order's KiB a merge, most | 215 [215, 215] | 215 [215, 215] | no |

The order's vector is built either way; the hash side does not sort it. Under round one's churn,
626 and 631 merges of about 8,800 entries, the key side's sort took 286 µs at the median and 0.73%
of a merge's time.

### A tablet's records, and what a walk reads of them

`scan/Big/none`, four rounds a layout:

| Figure | Setting | Hash order | Key order | Disjoint |
| --- | --- | --- | --- | --- |
| Partitions a stretch | any | 1 [1, 1] | 11.3 [11.2, 11.4] | yes |
| Partitions a walk read | runs, 4 KiB gaps | 1.01 [1.01, 1.01] | 11.3 [11.3, 11.5] | yes |
| Partitions a device read | one record a read | 1.01 [1.01, 1.01] | 1.01 [1.01, 1.01] | no |
| Partitions a device read | runs, 4 KiB gaps | 1.01 [1.01, 1.01] | 11.1 [11.0, 11.2] | yes |
| Partitions a device read | runs, 128 KiB gaps | 1.18 [1.17, 1.18] | 11.1 [11.0, 11.2] | yes |
| On the Zen1 hosts | runs, 4 KiB gaps | 1.01 | 10.6 [10.6, 10.6] | yes |
| On europa | runs, 4 KiB gaps | 1.01 | 12.6 [12.6, 12.6] | yes |
| Device KiB a partition | one record a read | 1.55 [1.55, 1.55] | 1.55 [1.55, 1.55] | no |
| Device KiB a partition | runs, 4 KiB gaps | 1.56 [1.56, 1.56] | 1.10 [1.10, 1.10] | yes |
| Device KiB a partition | runs, 128 KiB gaps | 10.6 [10.6, 10.7] | 1.10 [1.10, 1.10] | yes |

A record read alone costs its device 1.55 KiB for a 1.04 KiB partition, the rest its blocks'
alignment. In key order a run of 4 KiB gaps and one of 128 KiB read the same: a tablet's stretch in
one merge's block is followed by other tablets' records to the block's end, so a wider gap finds
nothing more of it within a run's mebibyte. In hash order a wider gap reads its neighbours' records
for nothing.

### After churn

Key order, `Big` after twenty minutes of churn and a restart, 3,300,000 partitions, 806 to a
tablet; today's walk over rounds one, five and six, runs over the supplement's five and six, beside
the same settings on a table loaded once:

| Figure | Setting | Loaded once | After churn |
| --- | --- | --- | --- |
| Partitions a stretch | any | 11.3 [11.2, 11.4] | 5.93 [5.54, 5.96] |
| Partitions a device read | runs, 4 KiB gaps | 11.1 [11.0, 11.2] | 9.88 [9.87, 9.89] |
| Partitions a device read | runs, 128 KiB gaps | 11.1 [11.0, 11.2] | 9.92 [9.91, 9.93] |
| On the Zen1 hosts | runs, 4 KiB gaps | 10.6 [10.6, 10.6] | 9.51 [9.51, 9.51] |
| On europa | runs, 4 KiB gaps | 12.6 [12.6, 12.6] | 11.2 [11.2, 11.3] |
| Device KiB a partition | runs, 4 KiB gaps | 1.10 [1.10, 1.10] | 1.20 [1.20, 1.20] |
| A Zen1 host's node cpu µs a partition | runs, 4 KiB gaps | 5.56 [5.49, 5.70] | 5.96 [5.96, 5.96] |
| A Zen1 host's node cpu µs a partition | one record a read | 46.4 [45.4, 46.8] | 46.9 [46.2, 47.0] |
| Partitions a second, the cluster | runs, 4 KiB gaps | 1,720,000 [1,490,000, 1,860,000] | 1,440,000 [1,410,000, 1,470,000] |

A stretch breaks at every hole an overwrite leaves, and a run reads across it, so a run's
partitions fell a tenth where the stretches halved. Every churn merge held about 8,800 entries and
took its sort 287 µs at the median, 0.77% of its time.

### A get

`read100/b1/none` over `Big`, four rounds a layout:

| Figure | Hash order | Key order | Disjoint |
| --- | --- | --- | --- |
| Gets a second | 106,000 [106,000, 107,000] | 107,000 [107,000, 108,000] | no |
| Get p50 ms | 0.183 [0.181, 0.188] | 0.182 [0.179, 0.184] | no |
| Get p99 ms | 0.938 [0.867, 1.02] | 0.921 [0.862, 0.973] | no |

## What it says for the design

- **Keep the sort where it is.** At under 1% of a merge's time and a few hundred KiB, it needs no
  path of its own and no larger merges.
- **Merges in key order are what lets a scan read a tablet in runs at all**: over a hash map's
  order runs are single reads, and wide gaps read other tablets' records for nothing. A walk in
  runs, X17's lever, assumes this layout.
- **Runs, not stretches, are what a churned table's scan sees.** Holes of a few kibibytes cost a run
  a tenth of its records, not half, so the budget's line is met or missed by how long a tablet's
  stretch is in a merge's block, which is a merge's partitions a tablet. Longer merges or
  consolidated blocks lengthen it; a table on a shard with fewer tablets has it longer already.
- **Consolidation stays undecided, at the line.** After twenty minutes of churn a walk in runs is at
  9.9 partitions a read, a hair under ten, its cpu well inside the budget. An hour's churn, which the
  spike page asked for, and archive passes over it, would say whether the figure keeps falling or
  holds; M23's plan decides with that figure whether consolidated blocks are its work.
- **A get does not care**, and neither does a load, so F121 stays in the product either way.

## Limitations

- **A snapshot cut's runs were not measured.** The cut reads a group's records in offset order
  across gaps of 128 KiB ([O78](../../appendix/optimizations.md#o78-a-snapshot-cut-read-one-record-at-a-time-in-key-order)),
  and in key order a group's tablets lie in stretches with other groups' tablets between, so whether
  it reads fewer bytes depends on how far apart a group's tablets lie in a merge's block. It is
  measured in process with X17's second half.
- **One table, one width**: every partition is a kibibyte, so a stretch's length is a merge's
  partitions a tablet; a table of wider rows has fewer partitions a merge and shorter stretches.
- **The churn is twenty minutes, not the spike page's hour**: three rounds of it, the runs after it
  two; what archive passes do to a churned table over hours is not measured, nor whether any ran.
- **One layout of churn**: a tenth of the rows overwritten in place and a tenth added, written in
  turn; a table whose writes cluster by key, or delete, leaves other holes.
- **Every scan here is unpaced**; a churned table at the product's ceiling was not run.

## Related

- [X37's spike page entry](spikes.md#x37-a-tablets-records-together-priced), and
  [X17's reads half](page-rebuilt.md), taken in the same window
- [F121, merges written in key order](../../features/merges-in-key-order.md), the layout priced
- [W42, a scan's speed beside ScyllaDB's](scan-speed.md), which asked for it
- [W12's budgets](performance.md#the-budgets) and [Q62](contract.md#questions-to-answer)
- [X16, the walk beside the foreground](walk-beside-the-foreground.md), whose cluster and table this
  window reused
