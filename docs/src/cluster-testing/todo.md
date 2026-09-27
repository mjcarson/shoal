# What is left

The lab runs on the other pages fixed most of what they found. This page collects what they did
not fix: open defects, optimizations still to apply or measure, limitations the lab confirmed, and
scenarios nobody has run yet. Each row links to where the item is filed, and its number stays on
that page. A row marked **not filed** is recorded here and nowhere else.

Every figure below is a lab run. The [overview](overview.md#how-to-read-the-numbers) says why none
of them is a capture.

## Bugs

| Item | Found in | What is left | Next step |
| --- | --- | --- | --- |
| [129](../appendix/known-issues.md#129-an-overloaded-group-answers-outcomeunknown-rather-than-shedding) | [Loading the whole dataset](correctness.md#1-loading-the-whole-dataset) | A client with more writes outstanding than a group commits within `write_timeout` gets `OutcomeUnknown` instead of `Shedding`. The loader's smaller in-flight gate works around it. It does not fix it | Shed at admission once a group's queue would outlast `write_timeout`, and check writes that arrive by hop on the leader too. Measure it on the grid's high-depth rungs |
| [143](../appendix/resolved/silent-partition-hops.md#still-open), the open part | [Partition one node](correctness.md#partition-one-node), [pause one node](correctness.md#pause-one-node), and every regression pass since | The first two to three seconds of a silent partition or a pause run at zero throughput, reads included, while hops wait for the two second silence to be judged. A coordinator on the cut-off node still proposes to the groups it leads and waits `write_timeout` for each | Find a signal earlier than two seconds of silence, or stop pipelined hops from filling a client's window while one link is judged |
| [156](../appendix/known-issues.md#156-a-full-disk-stops-every-group-on-a-node-until-it-is-restarted), the open part | [Fill a node's disk](correctness.md#fill-a-nodes-disk) | A node with a full disk now stops and comes back once there is space. Nothing sheds appends before the disk fills, because `disk_reserve` guards only snapshot installs | Refuse appends retriably below a reserve, so a nearly full node keeps serving reads |
| [132](../appendix/known-issues.md#132-ephemeral_sorted_table-aborted-once-in-glibcs-thread-cache-teardown) | Re-examined after [#133](../appendix/resolved/read-plan-rc-across-shards.md) | A one-off heap corruption abort. It fits #133's cause, but ASan on the unfixed tree did not show that it was #133 | Wait for it to recur on a tree with #133 fixed, or loop the binary under ASan |
| [142](../appendix/known-issues.md#142-two-fixture-tests-fail-intermittently-on-an-idle-host) | The workspace suite run after each section's fixes | Fixture tests fail intermittently under the suite's load and pass alone. One failure is openraft's own `Some(log_id) <= committed` assertion in `migration_resumes_after_each_phase_failure`, which predates #170 | Establish whether durable groups can reach that assertion the way [#109](../appendix/resolved/volatile-majority-loss.md)'s volatile log did |
| [152](../appendix/known-issues.md#152-a_compaction_that_meets_an_unreadable_archive_is_tried_again-fails-intermittently) | Suite runs during this chapter | The rotated intent logs are sometimes not compacted within the test's 30 s | Filed with its rate. The next change to the compactor's retry path should report the rate again |

## Optimizations

| Item | Found in | What is left | Next step |
| --- | --- | --- | --- |
| [O64](../appendix/optimizations.md#o64-a-shorter-failover-base-halves-write-throughput-on-the-lab) | [Failover base, revisited](performance.md#revisited-and-the-throughput-is-bimodal) | From one restart to the next, the load runs near 35,000 rows a second or near 46,000–50,000, and no timer setting decides which | Test the hypothesis that it depends on which node leads the hottest keyword groups. That needs [per-group write rates in `Stats`](../appendix/todos.md#per-group-write-rates-in-stats) |
| [Weighted leadership](../appendix/todos.md#leadership-is-spread-evenly-whatever-each-member-can-do) | [Leadership after a restart](performance.md#leadership-after-a-restart) | Since O63, leads are spread evenly over the members. With europa, the fastest host, leading more than its share, the bench did better | Choose primaries by `weight` or by measured commit latency. This changes the placement's primaries, so it needs its own design |
| [O74](../appendix/optimizations.md#o74-a-zen1-nodes-compactor-falls-hundreds-of-jobs-behind-under-the-bench), still open | [Section 10](correctness.md#10-the-compactors-backlog-and-four-rebuilds) | The 50% live threshold for an archive pass is hardcoded. Neither a merge nor a pass sorts its reads by offset. Reclaiming space as it is made costs about 6% of steady-state throughput on the Zen1 hosts | Sort the reads by offset and measure the difference on titan. Make the threshold a setting, and sweep it against the bench's rewrite rate |
| [O61](../appendix/optimizations.md#o61-a-fast-device-syncs-the-wal-in-batches-too-small-to-fill-a-page) | [Write amplification](performance.md#write-amplification-by-device-and-filesystem) | The group-commit delay halves europa's device writes, but it is off by default and a deployment cannot set it | [A key per inventory group](../appendix/todos.md#the-wal-group-commit-delay-per-inventory-group), set on the fast nodes alone |
| [Feed a snapshot when the log is larger](../appendix/todos.md#feed-a-new-copy-a-snapshot-when-its-log-is-larger) | [Section 8](correctness.md#8-an-unplaced-member-coordinates), #170 | After a load, a new copy is fed up to 1 GiB of log from index one, ten times the size of its set's snapshot | Have the leader purge through its checkpoint when the log would be the larger, but never ahead of the other voters |
| [Rebuild and move at terabyte scale](../appendix/todos.md#rebuild-and-move-at-terabyte-scale) | [Rebuilding a node under load](correctness.md#rebuilding-a-node-under-load) | A step has about 35 s of fixed cost. Under the bench, moves ran at 3.4 MiB/s with one step at a time and with six. At about 55 GB a set, `snapshot_timeout` and the log retention would likely keep a step from converging under writes | Retention that outlasts a step, a cut streamed from the archives, several steps in flight. Prove it on hosts with the disk, or with retention shrunk on the lab |
| [O57](../appendix/optimizations.md#o57-tablet-bytes-are-rescanned-from-the-whole-archive-map-on-every-report) | [Where the time goes](performance.md#where-the-time-goes) | `ArchiveMap::tablet_usage` rescans the map on every report: 1.1% of titan's samples | A counter per tablet, as O57 sketches |
| Formatting tracing fields, **not filed** | [Where the time goes](performance.md#where-the-time-goes) | About 2% of titan's samples went to formatting strings for tracing fields | Find the spans on the hot path that format fields no subscriber reads |
| glommio's cancelled timeouts | [Where the time goes](performance.md#where-the-time-goes) | About 124,000 timeouts a second are armed and cancelled on europa. None of it shows in the profile | Recorded and not pursued. Revisit only if a profile ever shows it |
| [O69](../appendix/optimizations.md#o69-every-idle-moment-parks-an-executor) | [Spinning before parking](performance.md#spinning-before-parking) | A spin before parking cut the `membarrier` calls tenfold and changed nothing, so it was reverted | Nothing, unless a later profile shows the barriers |

## Limitations

These are confirmed on the lab and documented where they live. None is planned as a fix yet.

| Limitation | Found in | Where it is written down |
| --- | --- | --- |
| Failover takes the lease plus an election, three to four times the base: 15–20 s at the default 5 s. The objective of base + 2 s is not met at any base. A kill of every node costs 12–13 s at zero | [Failover time](performance.md#failover-time-against-primary_failover_after), every regression pass | [C7](../distributed/failover.md#the-window-and-what-a-client-sees), [C15](../distributed/open-issues.md#measured-at-smoke-scale-only) |
| Under load a group remembers about seven seconds of write identities, not the five minutes `retry_window` promises. A retry after that is refused `IdentityExpired`, which is safe | [Nine SIGKILLs under load](correctness.md#nine-sigkills-under-load) | [F45's limitations](../features/replica-migration.md#limitations) |
| A rebuild leaves the control group one failure short of its usual margin until the old identity's removal commits | [Section 8](correctness.md#8-an-unplaced-member-coordinates), runs 3 and 4 | [F56's limitations](../features/cluster-rebuild.md#limitations) |
| A get through a member with no placement slot is about four times a placed member's latency (2 ms at the median), because every share is forwarded | [A member added with no rebalance](correctness.md#a-member-added-with-no-rebalance) | **Not filed** |
| A rebalance off a live source waits `retire_after` (five minutes by default) for each set. The TMDB inventories do not set it, and `cluster rebalance` stops following after 30 minutes while the plan runs on | [A member added with no rebalance](correctness.md#a-member-added-with-no-rebalance) | [Runbooks](../operations/runbooks.md) |
| Nothing ships backup files between hosts. A restore reads each group's file on that group's new leader, so the operator copies every file to every host | [Back up, destroy and restore](correctness.md#back-up-destroy-and-restore) | [Runbook 10](../operations/runbooks.md#10-backup-and-restore) |
| Memory outside the eviction budget goes unreported, and the archive map is bounded by nothing | [Memory](performance.md#memory) | Todos: [a node's memory on `Stats`](../appendix/todos.md#a-nodes-memory-on-stats), [the archive map](../appendix/todos.md#a-nodes-archive-map-is-bounded-by-nothing) |
| `cluster upgrade` never rewrites a node's `shoal.yml`, so the lab's `node_memory` was added by hand. The inventory wizard has no field for `failover` | [Memory](performance.md#memory), [failover time](performance.md#failover-time-against-primary_failover_after) | Todos: [re-render node files](../appendix/todos.md#re-render-a-deployments-node-files), [the wizard](../appendix/todos.md#the-inventory-wizard-has-no-field-for-failover) |
| An ASan build needs `-Zsanitizer-recover=address`, because gxhash reads past the end of short keys within a page | [Findings](findings.md#deployment-and-lab-findings) | Findings |
| Openraft's debug tracing cannot be used on a loaded node. It writes about 150,000 lines a second, which filled titan's root device. The move driver's stall report replaces it | [Section 8](correctness.md#8-an-unplaced-member-coordinates), runs 3 and 4 | [Overview](overview.md#when-a-move-is-slow) |

## Not yet explored

These are scenarios the chapter has not run. Each is a test to add to `target/lab/` and to a new
section of [Correctness](correctness.md).

| Scenario | Why it matters |
| --- | --- |
| `tc netem` delay and packet loss between peers | The overview lists it as an injection tool, but no section reports a run. Every partition so far drops all packets. None of them slows or loses a fraction |
| One-way partitions, and a partition of the control ports alone | Every partition so far cut the data and control ports together, in both directions |
| Two of three nodes down: losing quorum, then recovering | Every fault so far left a majority up. What clients see, and whether the cluster recovers on its own when a node returns, has not been checked |
| A real power cut, or a device's write cache lost | Killing every node keeps the page cache, so it tests only what the processes wrote and synced |
| A slow disk that is not full | Faults have covered a full disk and corrupt bytes. None has covered a device that answers late, which is the Zen1 hosts' usual limit |
| Clock skew between hosts | Leases and the failure detector are timed locally. No run has moved one host's clock |
| Past the lab's scale: more than three nodes, a factor above three, a terabyte a node | Moves at terabyte scale are extrapolated, not measured ([above](#optimizations)). Every placement here was three nodes at a factor of three, except the factor-two run in section 8 |
| The headline numbers on the benchmark host | Europa runs `powersave` and hosts the clients as well as a node. The figures on these pages are lab runs, so any that are to be quoted need a rerun as captures |

## Related

- [Findings](findings.md), for everything this testing found and what was done about it.
- [C15](../distributed/open-issues.md), the design's own list of open questions.
- [Known Issues](../appendix/known-issues.md), [Optimizations](../appendix/optimizations.md) and
  [Todos](../appendix/todos.md), where every numbered row above is kept.
