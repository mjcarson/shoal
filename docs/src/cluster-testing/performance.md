# Performance

Every number here is one run on the lab, labelled by what ran beside it. The
[overview](overview.md#how-to-read-the-numbers) says why none of them is a capture.

## Loading and the mixed workload

| Workload | Throughput | Latency | Notes |
| --- | --- | --- | --- |
| `load`, the whole csv, eight workers, 1,024 in flight | 38,600–42,900 rows/s | not recorded by the loader | Two runs, [correctness test 1](correctness.md#1-loading-the-whole-dataset) |
| `bench --mix insert:100`, eight workers, 128 in flight | 33,800–35,100 inserts/s | p50 20–21 ms, p99 111–124 ms | Latency is queueing: 1,024 in flight at 35k/s is 29 ms by Little's law |
| `bench` mixed (`get:55,keyword:15,update:15,insert:15`) | 60,000–88,000 ops/s, a fifth of them writes | get p50 0.5–2 ms, p99 20–56 ms; writes p50 26–49 ms, p99 120–220 ms | The no-fault baseline of the [fault tests](correctness.md#4-faults-under-load) |

### Where the time goes

During the first load titan and hyperion were busy (31–35% user, 29% system) and waiting on their
disks for a third of the time (32–36% iowait). Europa was three quarters idle. The Zen1 hosts'
NVMe is the bottleneck, and every write waits for two of the three copies to sync, so a quorum
always includes one of them.

A `perf` profile of titan's node under the mixed bench is flat. No Shoal function takes more than
1.2% of samples. The largest are the allocator (`_mi_page_malloc` 2.3%, `mi_free` 1.2%),
`ArchiveMap::tablet_usage` at 1.1% (the per-report rescan
[O57](../appendix/optimizations.md#o57-tablet-bytes-are-rescanned-from-the-whole-archive-map-on-every-report)
describes, now measured), `sort_by_load` at 0.4%, and about 2% formatting strings for tracing
fields. Titan's cpu is not what limits the cluster; its disk is.

Both were taken off in [section 11](correctness.md#11-overload-silence-and-a-nearly-full-disk)'s
round: `tablet_usage` is a counter now, and absent from titan's profile
([O57](../appendix/optimizations.md#o57-tablet-bytes-are-rescanned-from-the-whole-archive-map-on-every-report)).
The formatting was per-query spans recording their arguments with `Debug`. Skipping them took
tracing and formatting from 3.4–3.6% of titan's samples to 2.3%
([O75](../appendix/optimizations.md#o75-every-query-formatted-its-metadata-into-a-tracing-span)).

An io_uring trace of europa's node over the same 12 seconds counted about 124,000 cancelled
timeouts a second: glommio arms a timer per timed operation and cancels it on completion. That is
glommio's cost and shows as nothing in the profile, so it is recorded and not pursued.

## Write amplification by device and filesystem

For the same replicated rows, europa's device took far more writes than the other two:

| Run | Acknowledged inserts | europa (Optane, btrfs) | titan (970 EVO, ext4) | hyperion (970 EVO, ext4) |
| --- | --- | --- | --- | --- |
| 30 s insert-only, first cluster | 1,048,800 | 16,577 MB | 2,454 MB | 2,474 MB |
| 30 s insert-only, europa's directory `chattr +C` (nodatacow) on a fresh cluster | 1,087,480 | 16,610 MB | 1,992 MB | 1,982 MB |

`nodatacow` changed nothing on europa, so btrfs copying data is not the cause. Splitting a 15 s
run into what the node process was charged for and what the device wrote:

| Host | Process `write_bytes` | Device writes | Ratio | `fdatasync`s in 12 s |
| --- | --- | --- | --- | --- |
| europa | 4,776 MB | 8,469 MB | 1.77× | 67,903 |
| titan | 873 MB | 1,119 MB | 1.28× | 8,176 |
| hyperion | 876 MB | 1,101 MB | 1.26× | — |

The node on europa was charged five and a half times the writeback of the others for the same
rows. It syncs its WAL eight times as often, because a sync on the Optane returns sooner, so each
batch is smaller. Every batch re-dirties the page holding the WAL's tail, so that page is written
back once per sync. btrfs then adds its own per-sync metadata (1.77× against ext4's 1.27×). Filed
as [O61](../appendix/optimizations.md#o61-a-fast-device-syncs-the-wal-in-batches-too-small-to-fill-a-page).
Throughput was the same either way, because the Zen1 hosts set it. The cost is device wear and cpu
on the fastest node.

### O61, a group-commit delay

A bounded wait in the WAL writer after each sync, so appends that arrive in it share the next
batch, set on europa alone (`cluster.replication.wal_commit_delay`), with the insert bench run at
each setting and the settings interleaved so that the table's growth does not read as an effect:

| europa's delay | europa's syncs in 10 s | europa's device writes in 30 s | Cluster inserts | p99 |
| --- | --- | --- | --- | --- |
| 0 | 45,123 | 15.6 GB | 18,357/s | 506 ms |
| 3 ms | 14,215 | 7.7 GB | 18,649/s | 520 ms |
| 0 | 44,162 | 14.9 GB | 17,401/s | 601 ms |

Half the device writes and a third of the syncs, at no measured cost to the cluster, whose write
latency the slower hosts set. Kept as a setting, off by default
([O61](../appendix/optimizations.md#o61-a-fast-device-syncs-the-wal-in-batches-too-small-to-fill-a-page)).
The first attempt at this sequence lost hyperion halfway through to the kernel's OOM killer, which
was [#149](../appendix/resolved/node-memory-budget.md).

## O62, the archive map rewrite

Stalls after the leader kill sent titan and hyperion to 78–85% iowait with their cpus idle, after a
burst of writes. A bpftrace probe on `ext4_file_write_iter` and `ext4_sync_file`, summing each
node's writes by file name for a second at a time (`target/lab/files.bt`), found what the bursts
were: the archive map's temp file, `maps/temp/Shard-N`, 85–125 MB per save. Over 28 seconds of the
mixed bench on hyperion:

| Kind of file | Written |
| --- | --- |
| Archive map saves (`Shard-N`) | 1,573 MB, in bursts of up to 400 MB in one second |
| Archives | 433 MB |
| WAL segments | 227 MB |

The compactor folded the map's intent log into a whole new map every time the log passed a
mebibyte. The fold now waits for a quarter of the map
([O62](../appendix/optimizations.md#o62-every-compaction-rewrites-the-shards-whole-archive-map),
which has the A/B). After it, the same run wrote 26 MB of map saves. Throughput rose and the worst
write fell by about a third. The first rollout of O62 also failed a node's start, which is
[Resolved #140](../appendix/resolved/intent-log-read-ahead.md).

## Leadership after a restart

A node that restarts leads none of its groups when it comes back, and nothing moves leadership back
to it. After two kills titan led 0 of its 36 groups, hyperion 12 and europa 24. Every write is
proposed through its group's leader, so europa did two thirds of the leaders' work. Tracked as
an [open question](../distributed/open-issues.md#filed-as-unbuilt) before this chapter.

Measured, and fixed as [O63](../appendix/optimizations.md#o63-leadership-never-returns-to-a-groups-placement-primary):
the rolling upgrades that tested [#139](../appendix/resolved/leadership-handoff-on-stop.md) left
titan, a Zen1 host, leading all 36 groups. With every write proposed through it, the mixed bench
did 81,000 operations a second at a write p99 of 300 ms. Once shards hand a group back to its
placement primary, the leads settle at 12, 12 and 12 within 30 seconds of an upgrade, and the same
bench did 90,000–97,000 at 217–235 ms. With europa, the fastest host, leading more than its share
it did better again, which is filed as a todo.

## Failover time against `primary_failover_after`

Killing the node that led 24 groups made writes to those groups fail for about 17 seconds, then
stalled the two survivors on their disks for about seven
([correctness](correctness.md#kill-the-node-leading-the-most-groups)). The data groups elected new
leaders 14.5 s after the kill. That is what the configuration asks for: a group's election timeout
is `primary_failover_after` to twice it (5–10 s by default), and a follower does not vote while
its leader's lease, `election_timeout_max`, has not expired since the last acknowledgement. So the
earliest election is the lease plus an election timeout, 15 to 20 s at the default. The documented
objective of base + 2 s is not what that arithmetic gives.

Each base was measured on a fresh bootstrap (`target/lab/failover-test.sh`, with the inventory's
new `failover` key): a full load, a minute of the mixed bench with elections counted from every
node's journal, and then the kill of whichever node led the most groups, in the middle of the
mixed bench.

| Base | Writes refused after the kill | Elections in a minute under load | Full load | Mixed bench |
| --- | --- | --- | --- | --- |
| 1 s | about 4 s (t=16–19), then a few hundred as leads went back | 0 | 23,200 rows/s | 108,000 ops/s |
| 2 s | about 8 s, then a two second stall | 0 | 30,300 rows/s | 118,000 ops/s |
| 5 s (default) | about 16 s (t=15–31) | 0 | 41,100 rows/s | 118,000 ops/s |

Failover is three to four times the base, as the lease-plus-election arithmetic says: the objective
of base + 2 s in [C7](../distributed/failover.md) does not hold at any base. No base caused an
unwanted election under this load. But a shorter base cost write throughput: repeated loads at 1 s
ran at 19,800–24,200 rows a second against 40,200–46,500 at 5 s, with titan syncing and writing
half as much. The cause is not isolated. It is filed as
[O64](../appendix/optimizations.md#o64-a-shorter-failover-base-halves-write-throughput-on-the-lab),
the default stays at 5 s, and a planned restart no longer pays the window at all
([Resolved #139](../appendix/resolved/leadership-handoff-on-stop.md)).

### Revisited, and the throughput is bimodal

With every fix through #157 deployed, the halving did not reproduce. Measured the same way, on
fresh bootstraps with the leads 12/12/12 before each load, the 1 s base loaded at 46,400–49,700
rows a second and the 5 s base at 33,500–34,500. That reverses the table above. Separating the
1 s base's timers one at a time on the 5 s cluster gave high arms and low arms that did not
follow the setting: a 200 ms apply wait bound gave 45,500 and 46,200 on one build and 35,200 on
the next, and an instrumented run showed that bound was never reached. The load on this cluster
runs near 35,000 or near 46,000–50,000 rows a second from one restart to the next, and the timer
settings do not pick which. The details, and the hypothesis still to test (which node leads the
hottest keyword partitions' groups), are in
[O64](../appendix/optimizations.md#o64-a-shorter-failover-base-halves-write-throughput-on-the-lab).
Reading the runtime along the way found [#158](../appendix/resolved/runtime-waker-lists.md).

## Spinning before parking

Titan's node, under the load, issued 7,700 `membarrier` calls a second: one each time an executor
went to sleep, each interrupting every core the process runs on. A 200 µs spin before parking cut
them to 1,100 a second, and the load ran at 33,700 rows a second against 33,900 without it.
Filed and reverted as [O69](../appendix/optimizations.md#o69-every-idle-moment-parks-an-executor).

## Memory

Every node holds its table data within `resources.memory` per shard, which the deployment wrote
as the node's whole budget. The first long insert runs grew the nodes past their hosts' 14 GB, and
the kernel killed hyperion and then titan. Four findings came out of taking that apart:
[#149](../appendix/resolved/node-memory-budget.md) (the budget was every shard's, and its counter
saw a hundredth of what a node held), [#150](../appendix/resolved/inline-partition-buckets.md)
(the tables' partition maps held every row inline), [O68](../appendix/optimizations.md#o68-every-archive-compaction-copies-the-shards-whole-partition-index)
(every compaction copied the whole partition index), and
[#151](../appendix/resolved/purge-ahead-of-its-marker.md) (a node killed at the wrong moment never
started again).

Resident memory under five minutes of 70% inserts, `node_memory: 8Gi` on 14 GB hosts, sampled
every 30 s:

| Build | europa | titan | hyperion |
| --- | --- | --- | --- |
| Before (`memory: 8Gi`, every shard's) | 8.9 → 12.8 GB | 7.9 → 12.1 GB, then OOM-killed | 4.2 → 10.8 GB, OOM-killed in an earlier run |
| All four fixes, a fresh cluster with the whole dataset loaded (t13g) | 3.5 → 7.8 GiB, then 7.0–7.8 GiB | 3.6 → 7.6 GiB, then 7.0–7.6 GiB | 3.6 → 7.9 GiB, then 6.7–7.9 GiB |

The nodes rise until the process passes the budget, then evict and hover under it. The fresh
cluster's load after the rebuild ran at 33,283 rows/s. That is within the range of first loads after a
bootstrap (35,800 last time), not a measured cost of the fixes.

## O74, the compactor under the bench

Section 8 of [Correctness](correctness.md#8-an-unplaced-member-coordinates) left titan's compactor
hundreds of jobs behind under the mixed bench, with every snapshot cut, and every rebuild, waiting on
it. A compaction job that holds the compactor over 5 s now logs where its time went:

```text
a compaction job ran long table="Movie" kind="segment" secs=5.517 backlog=3 frames=16786 read_ms=2208
  loaded=5256 load_ms=3195 apply_ms=16 written=15693 write_ms=74 sync_ms=18 fold_ms=0 archives=0
```

The first run with it (a fresh cluster, the bench, hyperion rebuilt 25 s in) answered the question
O74 had left open. Of a Zen1 node's average long merge, 6.8 s, **5.8 s was reading the ~9,500
partitions it merges onto, one direct read at a time**. Reading the segment's frames took 0.9 s, and
the apply, writes, syncs and map fold 0.2 s between them. Europa's Optane ran the same merges under
the 5 s bar. A merge now reads 32 partitions at a time, as a snapshot cut has since O70.

That changed what the compactor spent its time on, and three rebuilds and three A/B runs followed
it. The details are on [O74](../appendix/optimizations.md#o74-a-zen1-nodes-compactor-falls-hundreds-of-jobs-behind-under-the-bench).
The rebuilds, each on a fresh cluster loaded from the csv (build c3 is `c3b8874`):

| | base | c3 |
| --- | --- | --- |
| Long merges on titan | 762, mean 6.8 s | **0** |
| Longest archive pass | 294 s (hyperion) | **5.9 s** |
| Peak compactor backlog on titan | 139 | **4** |
| Longest wait for a snapshot cut | 6.1 s | **0.7 s** |
| Forced purges | 3 | **0** |
| Whole rebuild | 430 s | **272 s**, the fastest on record |
| Throughput after the rebuild | 30.1k ops/s | 27.9k ops/s |

**Throughput is lower, and that is the work that was not being done.** With merges backlogged,
nearly every archive pass was skipped behind the next one. The dead records the bench's rewrites
leave stayed in the archives, and the log the backlog held grew: 0.5 to 1.7 GB of WAL every five minutes on
titan. With the backlog gone, a pass ran after every merge and copied each archive as soon as it
fell under half live: 2,139 MiB in five minutes on titan, and 10% of the cluster's throughput. So a
pass now waits a minute since the last one began, copies at most 16 MiB before it yields, stopping
inside an archive if it must, and keeps 16 reads in flight. The steady state, 300 s arms on one
cluster, each build following the other:

| A/B, 300 s arms | Build | ops/s, each arm | Update p99 | Titan's archives, each arm | Titan's WAL, each arm |
| --- | --- | --- | --- | --- | --- |
| first | phases logged (`fc91fb7`) | 34,368 / 35,045 | 111 / 112 ms | passes copied 42 MiB in an arm | – |
| | c1: 32 in flight, passes unpaced (`e7302c5`) | 31,518 / 30,878 | 149 / 160 ms | passes copied 2,139 MiB in an arm | – |
| last | phases logged (`fc91fb7`) | 35,824 / 32,852 | 109 / 125 ms | **+2,523 / +2,576 MB** | **+805 / +514 MB** |
| | c3, kept (`c3b8874`) | 32,762 / 31,354 | 133 / 143 ms | −948 / −736 MB | +62 / +53 MB |

The lab's bench is a worst case for this: 45% of its operations rewrite a row, so about 6,000 rows
a second leave a dead copy behind on every node.

## The admission gate and the bench

[Section 11](correctness.md#11-overload-silence-and-a-nearly-full-disk) put a gate in front of every
group's openraft queue ([#129](../appendix/resolved/overload-sheds.md)), a quorum judgement before
every write a leader appends ([#143](../appendix/resolved/silent-partition-hops.md#the-first-seconds-closed))
and a free-space check once a second a shard ([#156](../appendix/resolved/wal-failure-stops-the-node.md#the-second-part-an-append-reserve)).
All three sit on the write path, so each build was benched against the base, `35d47a6`: the
default 120 s mixed bench (get 70, keyword 15, update 10, insert 5; eight workers × 128 in flight),
on one loaded cluster, rolling every node onto each build in turn (`target/lab/r11/benchab.sh`).

**The arm order moves the result more than the builds do.** The first run put the base first in
both pairs, and the fixed build came out 3.5% behind twice. The second reversed the order, and the
fixed build came out ahead twice:

| Pair | First arm | Second arm |
| --- | --- | --- |
| 1 | base 96,746 | fixed 93,384 |
| 2 | base 105,888 | fixed 101,997 |
| 3 | fixed 99,082 | base 97,505 |
| 4 | fixed 94,748 | base 88,634 |

Over the four pairs the fixed build averaged 97,300 operations a second and the base 97,200. The
first arm after an upgrade was ahead in every pair, by 2–6%. The one that follows inherits the
compaction backlog and the dead bytes of the one before, as [findings](findings.md#deployment-and-lab-findings)
records for O74's runs. No bench error of any code in any arm.

**Profiles agree.** A 20 s `perf` profile of titan under the bench on each build
(`target/lab/r11/profab.sh`) showed no new symbol above 0.25%. The one large difference was
`ArchiveMap::tablet_usage`, 1.36% of titan's samples on the base and nothing on the fixed build,
which is [O57](../appendix/optimizations.md#o57-tablet-bytes-are-rescanned-from-the-whole-archive-map-on-every-report)
applied in the same change.

**How to compare two builds on this lab.** Interleave the arms and reverse the order, A B B A or
two pairs each way. Compare means, never a single pair.

## O74's remainder

[O74](../appendix/optimizations.md#o74-a-zen1-nodes-compactor-falls-hundreds-of-jobs-behind-under-the-bench)
left two things: the archive pass's 50% live threshold was hardcoded, and neither a merge nor a
pass sorted its reads by offset. The threshold is now `archive_pass_live_percent`. The sort was
built and measured: the same tree with and without it, A B B A, 180 s of O74's rewrite-heavy mix
(get 40, update 45, insert 15) each (`target/lab/r11/benchab2.sh`):

| Arm | Sorted | Not sorted |
| --- | --- | --- |
| 1 and 4 | 35,606 and 34,863 ops/s | |
| 2 and 3 | | 36,136 and 36,450 ops/s |

The difference is inside the lab's spread and, if anything, against the sort. With 16 to 32 reads
in flight on NVMe the order they are asked in does not matter, so the sort was taken out again.
Eight archive passes a host ran over 5 s in the four arms: 5.6 s on titan and 8.9 s on hyperion on
average, nearly all of it reading.

## Who leads the busiest groups

`Stats` could not say which member does a cluster's write work: every member applies every row.
It now names each member's eight busiest led groups by writes a second
(`NodeStats::hot_groups`), and the memory the eviction budget leaves out. Under the default bench
on the lab:

```text
memory               rows       budget     resident
22c7a330         339.6MiB       8.0GiB       2.9GiB

busiest groups     table                led by           writes/s      bytes/s
8c070e01d3d921af   Movie                6e70a2bd             1.1k   469.8KiB/s
a9d80c97999fdf1c   Movie                6e70a2bd             1.1k   466.5KiB/s
```

Rows are a tenth of what the process holds. The busiest groups write within a few percent of each
other.

That was the tool [O64](../appendix/optimizations.md#o64-a-shorter-failover-base-halves-write-throughput-on-the-lab)
was waiting for: whether the load's bimodal throughput depends on which host leads the hottest
groups. Five fresh bootstraps loaded at 39,389 to 49,856 rows a second, and the spread of the
busiest groups' leaders did not follow the rate. The fastest and the two slowest runs each had
europa leading five of the ten. The mode is still unexplained.

## O64 in round 12: what the mode is not

[O64](../appendix/optimizations.md#o64-a-shorter-failover-base-halves-write-throughput-on-the-lab)'s
whole-load rate still varies from one fresh bootstrap to the next. Round 12 gave the loader a rate
every five seconds (`load --series`) and `cluster stats` each member's storage pipeline, then
loaded about forty fresh clusters on the build of `00149e7` and later (`target/lab/r12/o64*.sh`). The
spread was 36,000 to 48,000 rows a second, and it is set within the first ten seconds: a slow load
is slow throughout, and no load changed pace partway through.

| Candidate | Arms | Rows a second | Verdict |
| --- | --- | --- | --- |
| The page cache | every host's dropped before the bootstrap, or not | cold 41.1–46.8k (4 loads), warm 38.7–47.0k (4) | Not it |
| The Zen1 disks' state after `destroy` deleted gigabytes | `fstrim` first, `fstrim` and two minutes, neither | 36.0–44.6k (5), 38.0–47.7k (2), 40.5–47.3k (5) | Not it. The first three of each looked like it (trim 38.3k against 45.0k), and the next nine did not |
| The disks' flush latency before a load | fio, 64 KiB direct writes each followed by `fdatasync`, before and after every load | 299–317 writes a second on titan and hyperion in every probe | Constant; not it |
| One shard carrying more than its share | each shard's applied writes a second, sampled every 10 s | within 1.4× on every node in fast and slow loads alike | Not it |
| Who leads | 2:1:1 lead weights ([F58](../features/weighted-leadership.md)) against even, 4 loads | 38.3–46.9k | No change to the load |

What the figures do show is where the time goes. Titan's and hyperion's 970 EVOs flush their
cache on every `fdatasync`: 3 ms at the median for one writer, 5.9 ms each with six at once (about
900 synced writes a second in total), where europa's Optane takes 0.2 ms. Their WALs settle at 250
to 700 syncs a second a node while the bytes a second hold, so each sync carries more as the load
goes on. A write-only load is paced by the Zen1 nodes' flushes, whoever leads, since every member
applies every write.

One setting moved the distribution. `wal_commit_delay: 2ms` on the Zen1 group (O61's knob, which
makes a writer wait after a sync for more appends to join the next batch) gave 8 loads averaging
44,500 rows a second with one below 42,000, against 43,100 and 9 of 15 below 42,000 without it.
That is what a group commit with two equilibria would show, where a batch that starts small stays
small. O61 had measured a delay costing a slow device latency, so the mixed bench was measured
under it too, interleaved on one cluster:

| Arm | Operations a second | p99 get | p99 update |
| --- | --- | --- | --- |
| no delay | 110,664 | 32.3 ms | 189.2 ms |
| 2 ms on the Zen1 group | 109,300 | 29.9 ms | 212.7 ms |
| 2 ms on the Zen1 group | 116,980 | 22.3 ms | 205.5 ms |
| no delay | 99,045 | 30.6 ms | 242.8 ms |

No latency cost the bench can see, the write p99s overlapping, and about 8% more throughput (the
third arm needed no restart, which flatters it). **Applied to the lab's inventory** from here on,
beside the lead weights below.

## Weighted leadership

[F58](../features/weighted-leadership.md) sends each group's lead to the voter a weighted rendezvous
names. The lab's inventory with europa's group at `lead_weight: 2` and the Zen1 group at the default
one, rolled onto one running cluster with `cluster reconfigure` and interleaved with even weights
(`target/lab/r12/leadab.sh`), a 120 s mixed bench after the leads settled 90 s:

| Arm | Leads europa / titan / hyperion | Operations a second | p99 get | p99 keyword | p99 update | p99 insert |
| --- | --- | --- | --- | --- | --- | --- |
| even | 12 / 12 / 12 | 112,961 | 27.9 ms | 28.2 ms | 189.5 ms | 187.8 ms |
| 2:1:1 | 21 / 10 / 5 | 114,535 | 20.7 ms | 21.0 ms | 164.7 ms | 163.5 ms |
| 2:1:1 | 21 / 10 / 5 | 122,649 | 19.0 ms | 19.2 ms | 164.7 ms | 163.6 ms |
| even | 12 / 12 / 12 | 103,903 | 32.0 ms | 31.7 ms | 190.5 ms | 185.7 ms |

Weighted, the mixed bench ran about 9% faster (118,600 against 108,400 operations a second on
average), reads' p99 fell by about a third and writes' by 13%. Europa led 21 of the 36 groups where
the weights give it half on average; with 36 groups the rendezvous lands within a few of that.
Whole loads did not move (four at 2:1:1: 38,300–46,900 rows a second), for the reason above.

**Applied to the lab's inventory** at the end of round 12: `tmdb_cluster.yaml` sets
`lead_weight: 2` on europa's group and `wal_commit_delay: 2ms` on the Zen1 group. Every figure in
this chapter before that point ran with even leads and no Zen1 delay, and a comparison with them
has to say so. A deployment of unequal hosts should set weights.
