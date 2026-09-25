# Findings

Every defect and optimization this chapter's testing found or measured, and what was done about
it. A defect has an item number on [Known Issues](../appendix/known-issues.md) or
[Resolved Issues](../appendix/resolved-issues.md). An optimization has an `O` number on
[Optimizations](../appendix/optimizations.md). A finding about the lab or the deployment rather
than Shoal says so.

## Defects

| Item | Found by | What | Outcome |
| --- | --- | --- | --- |
| [133](../appendix/resolved/read-plan-rc-across-shards.md) | `verify-acks`, the first read back in gets of 256 ids | A get naming two partitions crashed the coordinating node with `SIGSEGV`: a split query's plan held an `Rc`, raced on by two shard threads | **Fixed.** `Arc`, and a test that what a query carries between shards is `Send`. Rolled out with `cluster upgrade`, then 1,048,800 acknowledged inserts read back through every member |
| [130, 131, 60](../appendix/resolved/stream-connection-accounting.md) | The first TMDB load with retries (F54), filed then and fixed here | A frame for a dropped stream ended its connection's read loop, and a stream was failed only by the last connection it wrote to, which together hung a client | **Fixed.** Per-connection owed counts, a frame that cannot be delivered is dropped, and both result streams give their slot back on `Drop` |
| [132](../appendix/known-issues.md#132-ephemeral_sorted_table-aborted-once-in-glibcs-thread-cache-teardown) | Re-examined after 133 | The one-off heap corruption abort in a standalone test binary fits 133's cause | **Open.** The binary under ASan on the unfixed tree showed no use-after-free, so it is not shown to be 133 |
| [134](../appendix/resolved/fixture-default-peer-ports.md) | The workspace suite run before the first commit | Six fixture tests failed on `AddrInUse` while the lab's europa node held ports 12001 and 12002 | **Fixed.** A fixture server not staged into a cluster takes its ports from the fixture's block |
| [135](../appendix/resolved/leftover-temp-map.md) | [Kill a follower](correctness.md#kill-a-follower) | A node killed while saving an archive map failed every restart on `AlreadyExists` for the leftover temp file: titan crash-looped nine times | **Fixed.** A save removes a leftover temp map first |
| [136](../appendix/resolved/upgrade-a-down-node.md) | Delivering 135's fix | `cluster upgrade titan` refused because titan was down, so no tool could repair a crash-looping node | **Fixed.** A named down node is a repair |
| [137](../appendix/resolved/upgrade-waits-for-groups.md) | The repair's timing against titan's journal | An upgrade judged a node caught up while its shards were still starting: zero groups lag nothing | **Fixed.** The upgrade waits for the shards and groups the node had, with none starting |
| [138](../appendix/resolved/stream-bundle-identity.md) | [Kill a follower](correctness.md#kill-a-follower) | Every write on a stream opened before a newer stream's identity was evicted, or older than the retry window, was refused `IdentityExpired`: a quarter of all operations after the kill | **Fixed.** Each stream bundle gets its own identity |
| [139](../appendix/resolved/leadership-handoff-on-stop.md) | [Rolling upgrade under load](correctness.md#rolling-upgrade-under-load) | Stopping a node took its groups' leaders with it: 84,418 writes refused `NotLeader` around one rolling upgrade | **Fixed.** A stopping shard hands off what it leads first: 14 refusals |
| [140](../appendix/resolved/intent-log-read-ahead.md) | The first rollout of O62 | A 29.7 MB map intent log took minutes to replay, three direct reads a record, and failed hyperion's start on both programs | **Fixed.** A 4 MiB read-ahead window: 3.2 s to serve |
| [141](../appendix/resolved/recycled-stream-channels.md) | Rolling upgrade under load | A failed stream's channel went to the next stream with its late answers: thousands of answers for indexes already answered, and streams that never ended | **Fixed.** Closed streams drop late answers, failed streams do not recycle, duplicates are dropped |
| [142](../appendix/known-issues.md#142-two-fixture-tests-fail-intermittently-on-an-idle-host) | The suite run after 139–141 and O62 | Two fixture tests fail intermittently on an idle host; a restarted member's checkpoint stays at zero | **Open**, with rates |
| [143](../appendix/resolved/silent-partition-hops.md) | [Partition one node](correctness.md#partition-one-node) | With one node cut off by dropped packets, every write hopped to it waited the whole write timeout, and pipelined clients stopped: zero throughput cluster-wide | **Fixed.** A hop over a link silent for two seconds is refused at once |
| [144](../appendix/resolved/post-heal-elections.md) | [Partition one node](correctness.md#partition-one-node) | The cut-off node kept standing for election at a term per timeout, and on the heal unseated healthy leaders on both other nodes: stalls for about 20 s after the partition ended | **Fixed.** Pre-Vote on every data group and the control group |
| [145](../appendix/resolved/apply-wait-on-a-stalled-copy.md) | [Partition one node](correctness.md#partition-one-node) | After the heal, writes coordinated on the node catching up were committed at once and then held the whole 5 s write timeout for its copy to apply, which it could not while installing | **Fixed.** The apply wait ends on an installing or stalled copy |
| [146](../appendix/resolved/apply-wait-on-a-lagging-copy.md) | [Pause one node](correctness.md#pause-one-node) | Writes through a node resumed after a 20 s `SIGSTOP` waited for its copy to apply its whole backlog: up to 5.3 s for twelve seconds | **Fixed.** The apply wait is bounded at two heartbeat intervals |
| [147](../appendix/resolved/paused-detector-verdicts.md) | [Pause one node](correctness.md#pause-one-node) | A control leader paused and resumed called the live members down by its own silence; the new leader it called down stayed `down` for good, and `cluster upgrade` refused to run | **Fixed.** A stalled loop re-seeds its detector; a leader held down commits itself up |
| [148](../appendix/resolved/stale-intent-log-tail.md) | [Kill every node at once](correctness.md#kill-every-node-at-once) | After `SIGKILL`, hyperion never started again: its archive map's intent log held recycled DMA buffer memory past its end, archive records whose framing an intent shares. titan's log was damaged the same way while running | **Fixed** in the glommio fork (a partial flush zeroes its buffer's tail) and in the reader (a frame that is no intent ends the log) |
| [149](../appendix/resolved/node-memory-budget.md) | The O61 experiment, then five-minute memory runs | hyperion and titan were killed by the kernel's OOM killer at 14 GB: the inventory's 8 GiB was every shard's budget, and the budget's counter saw about 100 MB of a 9 GB node | **Fixed.** `resources.node_memory`, judged against the process's resident memory; nodes level at their budget |
| [150](../appendix/resolved/inline-partition-buckets.md) | A heap profile under load | 3 GB of a 7 GB node was the tables' partition maps, holding every loaded row inline in buckets sized for their peak | **Fixed.** A loaded partition is boxed |
| [151](../appendix/resolved/purge-ahead-of-its-marker.md) | titan after an OOM kill | titan never started again: a WAL segment had been deleted behind a purge whose marker the kill lost, leaving a hole after the purge point | **Fixed.** Deletion waits for a durable purge point; titan was rebuilt |

## Deployment and lab findings

| Finding | Evidence | What was done |
| --- | --- | --- |
| Europa's node was on a root device 98% full | `df` before the first test | Its group moved to the Optane |
| Europa's Optane took 6.8× the device writes of the Zen1 hosts' NVMe for the same replicated rows | [Performance](performance.md#write-amplification-by-device-and-filesystem) | Not copy-on-write: `nodatacow` changed nothing. Most of it is the WAL syncing smaller batches on the faster device, filed as [O61](../appendix/optimizations.md#o61-a-fast-device-syncs-the-wal-in-batches-too-small-to-fill-a-page) |
| gxhash reads a whole 16-byte block past the end of a short key | Every AddressSanitizer run | Recorded: it stays within a page by design, so it is not a fault. It means an ASan build needs `-Zsanitizer-recover=address`, and every run reports it once per location |

## Optimizations

| O | What | Outcome |
| --- | --- | --- |
| [O61](../appendix/optimizations.md#o61-a-fast-device-syncs-the-wal-in-batches-too-small-to-fill-a-page) | A fast device syncs the WAL in batches too small to fill a page | **Applied as a per-node setting, off by default.** 3 ms on europa: syncs −68%, device writes −50%, cluster throughput and p99 unchanged |
| [O62](../appendix/optimizations.md#o62-every-compaction-rewrites-the-shards-whole-archive-map) | Every compaction rewrote the shard's whole archive map: 70% of a node's writes | **Applied and kept.** Map saves 1,573 MB → 26 MB over the same run, worst write down by a third |
| [O65](../appendix/optimizations.md#o65-heartbeats-to-followers-that-just-acknowledged-replication) | A heartbeat to every follower every tenth of the base, even under sustained replication | **Applied, no measurable effect, kept.** Not the cause of O64 |
| [O67](../appendix/optimizations.md#o67-ten-thousand-retained-entries-is-seconds-of-a-busy-group) | A node back from a 20 s partition was behind every group's purge point and fed snapshots repeatedly | **Applied and kept.** 13 installs and 70 s of refused reads → 0, with no measured cost but WAL bytes |
| [O66](../appendix/optimizations.md#o66-a-partitioned-peer-floods-the-log) | openraft warned for every failed heartbeat and replication attempt to a cut-off peer, per group | **Applied and kept.** journald's suppressions in a partition 50–60k lines → 0 |
| [O64](../appendix/optimizations.md#o64-a-shorter-failover-base-halves-write-throughput-on-the-lab) | A shorter failover base halves write throughput on this hardware | **Measured, not applied.** Default kept at 5 s; cause not isolated |
| [O63](../appendix/optimizations.md#o63-leadership-never-returns-to-a-groups-placement-primary) | Leadership never returned to a restarted node: one node led all 36 groups | **Applied and kept.** Leads spread 12/12/12 within 30 s; about a sixth more throughput, a quarter off the write p99 |
