# What is left

The lab runs on the other pages fixed most of what they found. This page collects what they did
not fix: open defects, optimizations still to apply or measure, limitations the lab confirmed, and
scenarios nobody has run yet. Each row links to where the item is filed, and its number stays on
that page. A row marked **not filed** is recorded here and nowhere else.

Every figure below is a lab run. The [overview](overview.md#how-to-read-the-numbers) says why none
of them is a capture.

Round 11 ([sections 11 and 12](correctness.md#11-overload-silence-and-a-nearly-full-disk)) closed
this page's three open defects (#129, #143's open part and #156's), ran every scenario it listed
that the lab can run, and fixed what those found: #181, #182, O76. Chasing #142's one openraft
assertion found a lost write in the ephemeral tables' groups, also fixed. What it did not close is below.

## Bugs

| Item | Found in | What is left | Next step |
| --- | --- | --- | --- |
| [180](../appendix/known-issues.md#180-a-first-write-queued-past-a-groups-identity-memory-is-refused-identityexpired) | [Section 11](correctness.md#the-loader-past-what-the-cluster-commits) | A first write that waits in the server longer than its group remembers identities (4,096, about four seconds under load) is refused `IdentityExpired`. The admission gate keeps the queue after admission short, and no gated run met it; the queue in front of admission is not bounded | Write the retry sidecar incrementally, so remembering identities for a time rather than a count costs what it adds |
| [143](../appendix/resolved/silent-partition-hops.md#still-open), what is left | [Section 11](correctness.md#a-silent-partitions-first-seconds) | The first second and a half of a silent partition runs at about a third of the rate, while hops already sent wait for the silence to be judged | A shorter judgement would refuse healthy leaders under load. Revisit only with a signal that is not silence |
| [132](../appendix/known-issues.md#132-ephemeral_sorted_table-aborted-once-in-glibcs-thread-cache-teardown) | Re-examined after [#133](../appendix/resolved/read-plan-rc-across-shards.md) | A one-off heap corruption abort. It has not recurred in any suite run since | Leave filed until it recurs, or loop the binary under ASan |
| [142](../appendix/known-issues.md#142-two-fixture-tests-fail-intermittently-on-an-idle-host) | The workspace suite run after each section's fixes | Fixture tests fail intermittently under the suite's load and pass alone. The one failure with a message from inside openraft, `Some(log_id) <= committed` in `migration_resumes_after_each_phase_failure`, was a lost write and is [resolved](../appendix/resolved/volatile-amnesiac-vote.md): a restarted volatile leader voted for a follower missing its commits. What is left are deadlines | See the item |
| [152](../appendix/known-issues.md#152-a_compaction_that_meets_an_unreadable_archive_is_tried_again-fails-intermittently) | Suite runs during this chapter | The rotated intent logs are sometimes not compacted within the test's 30 s | Filed with its rate. It did not fail in round 11's suite runs |

## Optimizations

| Item | Found in | What is left | Next step |
| --- | --- | --- | --- |
| [O64](../appendix/optimizations.md#o64-a-shorter-failover-base-halves-write-throughput-on-the-lab) | [Failover base, revisited](performance.md#revisited-and-the-throughput-is-bimodal) | Whole loads run at 39,000 to 50,000 rows a second from one bootstrap to the next. The hypothesis that it depends on who leads the busiest groups was tested in round 11 and not supported ([who leads the busiest groups](performance.md#who-leads-the-busiest-groups)) | Nothing measured yet explains the mode. The next candidates are the compactors' state after the load's first minute and the page cache |
| [Weighted leadership](../appendix/todos.md#leadership-is-spread-evenly-whatever-each-member-can-do) | [Leadership after a restart](performance.md#leadership-after-a-restart), [#182](../appendix/resolved/slow-link-leadership.md) | Leads are spread evenly by count. #182 moves them off a node whose links are far slower than its peers', which is the extreme case. Whether a Zen1 host leading its third costs the cluster is not established. The 100 ms delay run served 56,000–70,000 operations a second with hyperion leading nothing, and 60,000–80,000 after the heal once its leads came back. Its ten seconds before the delay ran at 30,000–45,000 with a write p99 over a second, which the run that found #182 did not (69,000), so that window says nothing either way | Weigh leads by each member's commit latency, which `hot_groups` and the ping round trips now make visible |
| [Rebuild and move at terabyte scale](../appendix/todos.md#rebuild-and-move-at-terabyte-scale) | [Rebuilding a node under load](correctness.md#rebuilding-a-node-under-load) | A step has about 35 s of fixed cost. A new copy is fed a snapshot when its log would be far larger than its set (round 11), which the lab's logs no longer reach | Prove it on hosts with the disk, or with retention shrunk on the lab |
| The per-query spans, **not filed** | [O75](../appendix/optimizations.md#o75-every-query-formatted-its-metadata-into-a-tracing-span) | 0.8% of titan's samples are the span slab: a slot per open span, one per query and share | Dropping the per-query spans below `Info` takes them from a collector too. Only worth it with a sampled layer in their place |
| glommio's cancelled timeouts | [Where the time goes](performance.md#where-the-time-goes) | About 124,000 timeouts a second are armed and cancelled on europa. None of it shows in the profile | Recorded and not pursued |
| [O69](../appendix/optimizations.md#o69-every-idle-moment-parks-an-executor) | [Spinning before parking](performance.md#spinning-before-parking) | A spin before parking cut the `membarrier` calls tenfold and changed nothing, so it was reverted | Nothing, unless a later profile shows the barriers |

## Limitations

These are confirmed on the lab and documented where they live. None is planned as a fix yet.

| Limitation | Found in | Where it is written down |
| --- | --- | --- |
| Failover takes the lease plus an election, three to four times the base: 15–20 s at the default 5 s. The same shape after a real power cut (15 s) | [Failover time](performance.md#failover-time-against-primary_failover_after), [a real power cut](correctness.md#a-real-power-cut) | [C7](../distributed/failover.md#the-window-and-what-a-client-sees), [C15](../distributed/open-issues.md#measured-at-smoke-scale-only) |
| Under load a group remembers about four to seven seconds of write identities, not the five minutes `retry_window` promises. A retry after that is refused `IdentityExpired`, which is safe; a first write queued that long is refused the same way (#180) | [Nine SIGKILLs under load](correctness.md#nine-sigkills-under-load), [section 11](correctness.md#the-loader-past-what-the-cluster-commits) | [F45's limitations](../features/replica-migration.md#limitations) |
| A rebuild leaves the control group one failure short of its usual margin until the old identity's removal commits | [Section 8](correctness.md#8-an-unplaced-member-coordinates), runs 3 and 4 | [F56's limitations](../features/cluster-rebuild.md#limitations) |
| A get through a member with no placement slot is about four times a placed member's latency (2 ms at the median), because every share is forwarded | [A member added with no rebalance](correctness.md#a-member-added-with-no-rebalance) | **Not filed** |
| A rebalance off a live source waits `retire_after` (five minutes by default) for each set | [A member added with no rebalance](correctness.md#a-member-added-with-no-rebalance) | [Runbooks](../operations/runbooks.md) |
| Nothing ships backup files between hosts. A restore reads each group's file on that group's new leader, so the operator copies every file to every host | [Back up, destroy and restore](correctness.md#back-up-destroy-and-restore) | [Runbook 10](../operations/runbooks.md#10-backup-and-restore) |
| The archive map is bounded by nothing: rows are a tenth of a node's resident memory under the bench (340 MiB of 2.9 GiB), now reported on `Stats` | [Memory](performance.md#memory), [who leads the busiest groups](performance.md#who-leads-the-busiest-groups) | Todos: [the archive map](../appendix/todos.md#a-nodes-archive-map-is-bounded-by-nothing) |
| A node under its append reserve serves reads from a copy that is behind; a read that needs more is refused `Unavailable` | [A nearly full disk](correctness.md#a-nearly-full-disk) | [#156](../appendix/resolved/wal-failure-stops-the-node.md#still-open) |
| A slow disk on one node costs the cluster about a quarter of its rate at 10 ms a request | [Section 12](correctness.md#12-scenarios-nobody-had-run) | **Not filed**: leads stay on the slow disk's node, which weighted leadership would weigh |
| An ASan build needs `-Zsanitizer-recover=address`, because gxhash reads past the end of short keys within a page | [Findings](findings.md#deployment-and-lab-findings) | Findings |
| Openraft's debug tracing cannot be used on a loaded node. It writes about 150,000 lines a second | [Section 8](correctness.md#8-an-unplaced-member-coordinates), runs 3 and 4 | [Overview](overview.md#when-a-move-is-slow) |

## Not yet explored

| Scenario | Why it matters |
| --- | --- |
| The power itself cut | `sysrq b` loses the page cache but not a device's write cache. Cutting a host's power by hand is the only way to test that on this lab |
| Past the lab's scale: more than three nodes, a factor above three, a terabyte a node | Moves at terabyte scale are extrapolated, not measured |
| The headline numbers on the benchmark host | Europa runs `powersave` and hosts the clients as well as a node. The figures on these pages are lab runs, so any that are to be quoted need a rerun as captures |
| A partition longer than a minute, and several in a row | #181 was found at 60 s. Recovery after longer cuts, and after flapping links, has not been measured |

## Related

- [Findings](findings.md), for everything this testing found and what was done about it.
- [C15](../distributed/open-issues.md), the design's own list of open questions.
- [Known Issues](../appendix/known-issues.md), [Optimizations](../appendix/optimizations.md) and
  [Todos](../appendix/todos.md), where every numbered row above is kept.
