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
assertion found a lost write in the ephemeral tables' groups, also fixed.

Round 12 ([section 13](correctness.md#13-round-12)) closed #180, #143's first second and a half, and
#152, whose cause was a kanal receive raced against a timer, the same race in fourteen other
places. It built weighted leadership ([F58](../features/weighted-leadership.md)) and measured it,
ruled out five more candidates for O64's mode, and applied a Zen1 commit delay and europa's lead
weight to the lab's inventory.

Round 13 ([section 14](correctness.md#14-round-13)) found and fixed #183, one way #142's restore
stall happens, and #184, reads refused through a node catching up by snapshot. It also found #185,
a new copy sent snapshot after snapshot when its install outlasted the leader's retention;
#186, a rehome's executor count lost to a concurrent rewrite of the storage marker; and #187, a
restarted node's WAL that never compacted again when a rotation came before its first write. It
built backup shipping ([F59](../features/backup-shipping.md)) and an inventory's retention
settings, closed two limitations by measuring them (a slow disk, an unplaced member), dropped
O64's batching hypothesis, and ran the partition scenarios.

Round 14 ([section 15](correctness.md#15-round-14)) fixed #188, the byte half of a step that
outlasts the log, with a new `hold_bytes`; #189, a move reported stalled while its snapshot
streamed; and #190, a leader that threw away every append answer later than a heartbeat interval,
which was most of #142's deadlines. It cut a snapshot in disk order (O78, a Zen1 cut from 5.5–8.4 s
to 0.55 s) and priced O52 against it. For O64 it built and withdrew a direct WAL with a shared
flush ([F60](../features/shared-wal-flush.md)), which found what paces a write-only load on the
Zen1 hosts: the compactor rewriting keyword partitions whole (O79), not the WAL.

Round 15 ([section 16](correctness.md#16-round-15)) built O79's fix, a large sorted partition
written as fragments ([F61](../features/fragmented-partitions.md)): the keyword table's archive
writes fell by four fifths and a node's by half, and the loads did not move, which ruled the
archives out as what paces them too. It took the lab's nodes to ten copies of the dataset, 13 GB a
node, and found a node's memory filling with openraft's channels, allocated to their bounds
([#191](../appendix/resolved/raft-channels-preallocated.md)), beside two smaller holdings (O81,
O82), all fixed, and halved the archive map's entry
([O83](../appendix/optimizations.md#o83-the-partition-index-held-forty-eight-bytes-a-partition)).
The allocator was compared and ruled out. Rebuilds at that size ran at 18 to 24 MiB/s one step at
a time and 49 MiB/s six at a time, nothing lost. A step that outlasts both retentions livelocked
the retention sweep ([#192](../appendix/resolved/forced-build-deferred.md)) and stranded a partial
snapshot against the install bound ([#194](../appendix/resolved/abandoned-partial-snapshots.md)),
both fixed. The fixture loop found a node's scheduled scrubs starved by version churn
([#195](../appendix/resolved/scheduled-scrub-starved.md)), fixed, and every rebuild's redial noise
is filed as #193.

Round 16 ([section 17](correctness.md#17-round-16)) closed both of round 15's open defects:
[#193](../appendix/resolved/rebuild-redial-thrash.md), whose cause was the control lane's links
keyed by address and thrown away at every heartbeat to the other of a rebuilt node's two
identities (a rebuild now leaves about forty verdict dials in a journal where it left 9,400), and
[#196](../appendix/resolved/row-charge-undercount.md), whose sorted rows are charged with their
B-tree nodes and keys (85% of what the heap holds for rows, from 77%, the allocator's rounding
filed). It narrowed the failover window from three to four bases to one and a half to two
([F62](../features/failover-window.md): about 10 s at the default and 2 s at 1 s, no election
in a loaded minute at either), ruled the Zen1 hosts' frequency governor out of O64 (nine
candidates), and ran the #142 loop again. What no round closed is below.

## Bugs

| Item | Found in | What is left | Next step |
| --- | --- | --- | --- |
| [143](../appendix/resolved/silent-partition-hops.md#still-open), what is left | [Round 12](correctness.md#a-silent-partitions-first-second) | The second of a silent partition's cut runs at about 45% of the rate: the kernel's verdict needs two retransmission timeouts, about 600 ms. Round 14 measured it again on the final build, 47% ([round 14's final build](correctness.md#round-14s-final-build)) | One timeout was measured and took 5% loss for a cut. Nothing further planned |
| [132](../appendix/known-issues.md#132-ephemeral_sorted_table-aborted-once-in-glibcs-thread-cache-teardown) | Re-examined after [#133](../appendix/resolved/read-plan-rc-across-shards.md) | A one-off heap corruption abort. It has not recurred in any suite run since, nor in 169 ASan runs of the binary in round 12 | Leave filed until it recurs |
| [142](../appendix/known-issues.md#142-two-fixture-tests-fail-intermittently-on-an-idle-host) | The workspace suite run after each section's fixes | ~~Deadlines under the suite's load (a write not committed or a leader not elected in time)~~ Round 14 found most of them were [#190](../appendix/resolved/append-answer-thrown-away.md). ~~What is left is the restore stall~~ Round 15's full run passed 1,740 of 1,740; its loop found [#195](../appendix/resolved/scheduled-scrub-starved.md), fixed, and once a move left at `Configured` after its driver was killed, not explained ([round 15](correctness.md#142-in-round-15)) | Read the next stuck move's logs: `target/lab/r15/142/loop-keep.sh` keeps them |
| ~~[193](../appendix/resolved/rebuild-redial-thrash.md)~~ | [Round 15's rebuilds](correctness.md#a-step-that-outlasts-both-retentions) | ~~Peers dial a rebuilt node's old identity at `reconnect_min` until its removal commits, about twenty warnings a second each side, 55,000 in a 45 minute rebuild~~ **Fixed in round 16** ([a rebuild without the dial noise](correctness.md#a-rebuild-without-the-dial-noise)): the control links were keyed by address and thrown away at every heartbeat to the other identity; keyed by identity, and a verdict's backoff grown to a minute, a rebuild leaves about forty verdict dials in a journal where it left 9,400 | None |

## Optimizations

| Item | Found in | What is left | Next step |
| --- | --- | --- | --- |
| [O64](../appendix/optimizations.md#o64-a-shorter-failover-base-halves-write-throughput-on-the-lab) | [Failover base, revisited](performance.md#revisited-and-the-throughput-is-bimodal), [rounds 12](performance.md#o64-in-round-12-what-the-mode-is-not), [13](performance.md#o64-in-round-13-the-batches-seen), [14](performance.md#o64-in-round-14-the-journal-not-the-flush) and [15](performance.md#o79-in-round-15-fragments) | Whole loads still run at 41,000 to 56,000 rows a second from one bootstrap to the next, and a slow load is slow from its first five seconds. ~~Round 14 found what paces them on the Zen1 hosts: the compactor's archive writes~~ Round 15 halved the archive writes (F61) and the loads did not move, so neither the WAL's syncs nor the archives' bytes pace them. Round 16 ruled out the Zen1 hosts' frequency governor ([not the governor either](performance.md#o64-in-round-16-not-the-governor-either)): the cores run at 3.1–3.3 GHz under a load whichever is set. Nine candidates are ruled out | Nothing planned: what differs between bootstraps is still not named |
| ~~[O79](../appendix/optimizations.md#o79-a-merge-rewrites-every-partition-it-touches-whole)~~ | [O64 in round 14](performance.md#o64-in-round-14-the-journal-not-the-flush) | **Built in round 15** as [F61](../features/fragmented-partitions.md): the keyword table's archive writes during a load fell from 55–80 MB/s to 15–19, with the bench, its p99 and cold keyword reads unchanged | None |
| ~~[Lead weights from measured commit latency](../appendix/todos.md#lead-weights-from-measured-commit-latency)~~ | [F58](../features/weighted-leadership.md) | **Not needed** (round 13): a disk that degrades costs about 2% and moving its leads away gains nothing | None |
| [Rebuild and move at terabyte scale](../appendix/todos.md#rebuild-and-move-at-terabyte-scale) | [Rebuilding a node under load](correctness.md#rebuilding-a-node-under-load), [at ten times the dataset](correctness.md#rebuilding-a-node-at-ten-times-the-dataset) | Round 15 took a node to 13.5 GiB: rebuilds ran at 18 MiB/s a step at a time and 49 MiB/s six at a time (`moves_per_node`, now an inventory key), nothing lost. A step that outlasts both retentions livelocked the sweep until [#192](../appendix/resolved/forced-build-deferred.md); on the fix it [re-snapshots](correctness.md#a-step-that-outlasts-both-retentions) | A terabyte a node needs hosts with the disk; 60 GB free is what the lab has |
| ~~The per-query spans, **not filed**~~ | [O75](../appendix/optimizations.md#o75-every-query-formatted-its-metadata-into-a-tracing-span) | **Filed as [O80](../appendix/optimizations.md#o80-every-query-opens-spans-a-collector-may-never-read) and not taken** (round 14): 0.8% is inside the lab's spread, and removing it without losing the traces needs a sampled layer | None |
| glommio's cancelled timeouts | [Where the time goes](performance.md#where-the-time-goes) | About 124,000 timeouts a second are armed and cancelled on europa. None of it shows in the profile | Recorded and not pursued |
| [O69](../appendix/optimizations.md#o69-every-idle-moment-parks-an-executor) | [Spinning before parking](performance.md#spinning-before-parking) | A spin before parking cut the `membarrier` calls tenfold and changed nothing, so it was reverted | Nothing, unless a later profile shows the barriers |

## Limitations

These are confirmed on the lab and documented where they live. None is planned as a fix yet.

| Limitation | Found in | Where it is written down |
| --- | --- | --- |
| ~~Failover takes the lease plus an election, three to four times the base: 15–20 s at the default 5 s (17 s on round 14's final build). The same shape after a real power cut (15 s)~~ **Narrowed in round 16** to one and a half to two bases ([F62](../features/failover-window.md)): about 10 s at the default and 2 s at 1 s, with no election in a loaded minute at either ([the failover window](correctness.md#the-failover-window)). What is left is the design: the lease is the base and an election follows it, so the window is never the base alone | [Failover time](performance.md#failover-time-against-primary_failover_after), [a real power cut](correctness.md#a-real-power-cut), [round 16](correctness.md#the-failover-window) | [C7](../distributed/failover.md#the-window-and-what-a-client-sees), [F62](../features/failover-window.md) |
| Under load a group remembers about four to seven seconds of write identities, not the five minutes `retry_window` promises. A retry after that is refused `IdentityExpired`, which is safe; a first write queued that long is refused `Shedding` since round 12 and sent again under a new identity (#180) | [Nine SIGKILLs under load](correctness.md#nine-sigkills-under-load), [section 11](correctness.md#the-loader-past-what-the-cluster-commits) | [F45's limitations](../features/replica-migration.md#limitations) |
| A rebuild leaves the control group one failure short of its usual margin until the old identity's removal commits | [Section 8](correctness.md#8-an-unplaced-member-coordinates), runs 3 and 4 | [F56's limitations](../features/cluster-rebuild.md#limitations) |
| A get through a member with no placement slot costs one forward hop: 0.34 ms more than a placed member idle, 2.7 ms at the median loaded | [A member added with no rebalance](correctness.md#a-member-added-with-no-rebalance), measured in [round 13](correctness.md#a-get-through-an-unplaced-member) | Correctness, round 13. The cost of the hop, with nothing on it to remove |
| A rebalance off a live source waits `retire_after` (five minutes by default) for each set | [A member added with no rebalance](correctness.md#a-member-added-with-no-rebalance) | [Runbooks](../operations/runbooks.md) |
| ~~Nothing ships backup files between hosts~~ | [Back up, destroy and restore](correctness.md#back-up-destroy-and-restore) | **Built in round 13**: `cluster ship-backup` ([F59](../features/backup-shipping.md)) |
| The archive map is bounded by nothing: ~~1.2 GiB a node at 11.8 million partitions~~ 615 MiB at 11.8 million since [O83](../appendix/optimizations.md#o83-the-partition-index-held-forty-eight-bytes-a-partition), which no budget counts; at a terabyte a node about 45 GiB. ~~Rows are a tenth of a node's resident memory under the bench~~: that was mostly openraft's channels ([#191](../appendix/resolved/raft-channels-preallocated.md)), fixed in round 15 | [Memory](performance.md#memory), [memory at ten times the dataset](performance.md#memory-at-ten-times-the-dataset) | Todos: [the archive map](../appendix/todos.md#a-nodes-archive-map-is-bounded-by-nothing); `archive_map_bytes` on `Stats` |
| ~~The eviction budget counts about two thirds of what rows take (2.3 GiB of 3.4 on titan), and nothing counts the WAL's index of its retained entries (0.54 GiB)~~ The eviction budget counts 85% of what rows take since [#196](../appendix/resolved/row-charge-undercount.md) (round 16: 1,319 MiB counted of 1,549 on titan), the rest the allocator's rounding of a row's String and Vec blocks; the WAL index and the eviction list are on `Stats` and counted by no budget | [Memory at ten times the dataset](performance.md#memory-at-ten-times-the-dataset), [what a row is charged](correctness.md#what-a-row-is-charged) | Todos: [what a row's allocations take](../appendix/todos.md#what-a-rows-allocations-take) |
| A step that outlasts `retained_bytes + hold_bytes` under writes never finishes: each forced purge needs its member sent another snapshot. It fails by name and harms nothing else since #192 and #194 | [A step that outlasts both retentions](correctness.md#a-step-that-outlasts-both-retentions) | Correctness, round 15; `hold_bytes` is the operator's to size for a step's transfer |
| A node under its append reserve serves reads from a copy that is behind; a read that needs more is refused `Unavailable` | [A nearly full disk](correctness.md#a-nearly-full-disk) | [#156](../appendix/resolved/wal-failure-stops-the-node.md#still-open) |
| ~~A slow disk on one node costs the cluster about a quarter of its rate at 10 ms a request~~ | [Section 12](correctness.md#12-scenarios-nobody-had-run) | **Closed in round 13**: a disk four times slower now costs about 2%, and moving its leads away gains nothing ([a slow disk, again](correctness.md#a-slow-disk-again)). What is left is margin: its groups commit on the other two while it lags |
| An ASan build needs `-Zsanitizer-recover=address`, because gxhash reads past the end of short keys within a page | [Findings](findings.md#deployment-and-lab-findings) | Findings |
| Openraft's debug tracing cannot be used on a loaded node. It writes about 150,000 lines a second | [Section 8](correctness.md#8-an-unplaced-member-coordinates), runs 3 and 4 | [Overview](overview.md#when-a-move-is-slow) |

## Not yet explored

| Scenario | Why it matters |
| --- | --- |
| The power itself cut | `sysrq b` loses the page cache but not a device's write cache. Cutting a host's power by hand is the only way to test that on this lab, and it needs a person at the host |
| Past the lab's scale: more than three nodes, a factor above three, a terabyte a node | Round 15 went ten times past the dataset, 13.5 GiB a node ([ten times the dataset](correctness.md#ten-times-the-dataset)), which is the lab's disk and memory. A terabyte is extrapolated from there. An inventory places one node per host, with one set of ports and one unit name, so more nodes need more hosts |
| The headline numbers on the benchmark host | Europa runs `powersave` and hosts the clients as well as a node. The figures on these pages are lab runs, so any that are to be quoted need a rerun as captures |
| ~~The hosts' cpu frequency governor~~ | **Run in round 16** ([not the governor either](performance.md#o64-in-round-16-not-the-governor-either)): ten fresh loads under `schedutil` and `performance` on the Zen1 hosts, every cpu's frequency sampled; the cores run at 3.1–3.3 GHz under a load whichever is set, and O64's spread is the same under both |
| ~~A partition longer than a minute, and several in a row~~ | **Run in round 13** ([longer partitions](correctness.md#longer-partitions-and-a-flapping-one)): 120 s, 300 s and ten flaps, nothing lost, a heal is a second; found #184 |

## Related

- [Findings](findings.md), for everything this testing found and what was done about it.
- [C15](../distributed/open-issues.md), the design's own list of open questions.
- [Known Issues](../appendix/known-issues.md), [Optimizations](../appendix/optimizations.md) and
  [Todos](../appendix/todos.md), where every numbered row above is kept.
