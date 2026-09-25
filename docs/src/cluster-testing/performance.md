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
