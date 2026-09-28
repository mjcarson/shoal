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
Zen1 hosts: the compactor rewriting keyword partitions whole (O79), not the WAL. What no round
closed is below.

## Bugs

| Item | Found in | What is left | Next step |
| --- | --- | --- | --- |
| [143](../appendix/resolved/silent-partition-hops.md#still-open), what is left | [Round 12](correctness.md#a-silent-partitions-first-second) | The second after a silent partition's cut runs at about 44% of the rate: the kernel's verdict needs two retransmission timeouts, about 600 ms | One timeout was measured and took 5% loss for a cut. Nothing further planned |
| [132](../appendix/known-issues.md#132-ephemeral_sorted_table-aborted-once-in-glibcs-thread-cache-teardown) | Re-examined after [#133](../appendix/resolved/read-plan-rc-across-shards.md) | A one-off heap corruption abort. It has not recurred in any suite run since, nor in 169 ASan runs of the binary in round 12 | Leave filed until it recurs |
| [142](../appendix/known-issues.md#142-two-fixture-tests-fail-intermittently-on-an-idle-host) | The workspace suite run after each section's fixes | ~~Deadlines under the suite's load (a write not committed or a leader not elected in time)~~ Round 14 found most of them were [#190](../appendix/resolved/append-answer-thrown-away.md), an append's answer thrown away after a heartbeat interval: the loaded loop went from 11 of 42 tests failed to 0 of 30. What is left is the restore stall, not recurred since [#183](../appendix/resolved/restore-driver-uncommitted-done.md) | Leave filed while a full suite run still finds something; `target/lab/r14/142/loop.sh` catches a new shape |

## Optimizations

| Item | Found in | What is left | Next step |
| --- | --- | --- | --- |
| [O64](../appendix/optimizations.md#o64-a-shorter-failover-base-halves-write-throughput-on-the-lab) | [Failover base, revisited](performance.md#revisited-and-the-throughput-is-bimodal), [rounds 12](performance.md#o64-in-round-12-what-the-mode-is-not), [13](performance.md#o64-in-round-13-the-batches-seen) and [14](performance.md#o64-in-round-14-the-journal-not-the-flush) | Whole loads still run at 42,000 to 56,000 rows a second from one bootstrap to the next. Round 14 found what paces them on the Zen1 hosts: the compactor's archive writes, 114 MB/s against 24.5 MB/s of WAL, not the WAL's syncs (F60 was built and withdrawn). The 1 s base no longer halves a load | [O79](../appendix/optimizations.md#o79-a-merge-rewrites-every-partition-it-touches-whole); what differs between bootstraps is still not named |
| [O79](../appendix/optimizations.md#o79-a-merge-rewrites-every-partition-it-touches-whole) | [O64 in round 14](performance.md#o64-in-round-14-the-journal-not-the-flush) | A merge rewrites every partition it touches whole: about sixty archive bytes per keyword byte inserted. 40 MiB segments halve it and make loads 24% faster, at a 20% higher update p99, so `segment_bytes` is an inventory key and not a default | [A large sorted partition written as fragments](../appendix/todos.md#a-large-sorted-partition-written-as-fragments), a format change |
| ~~[Lead weights from measured commit latency](../appendix/todos.md#lead-weights-from-measured-commit-latency)~~ | [F58](../features/weighted-leadership.md) | **Not needed** (round 13): a disk that degrades costs about 2% and moving its leads away gains nothing | None |
| [Rebuild and move at terabyte scale](../appendix/todos.md#rebuild-and-move-at-terabyte-scale) | [Rebuilding a node under load](correctness.md#rebuilding-a-node-under-load) | Round 14 staged the bytes half with streams throttled to 2 MiB/s and fixed what it found ([#188](../appendix/resolved/forced-purge-outruns-snapshot.md)): a step is held up to `hold_bytes` past `retained_bytes`. The cut's reads are 10× faster (O78), and streaming the cut was priced at no more than the send it could overlap ([O52](../appendix/optimizations.md#o52-a-snapshot-copies-every-record-of-the-archives-into-one-file)) | A step that outlasts `retained_bytes + hold_bytes` or `migration.timeout`, and several steps in flight onto a node, on hosts with the disk |
| ~~The per-query spans, **not filed**~~ | [O75](../appendix/optimizations.md#o75-every-query-formatted-its-metadata-into-a-tracing-span) | **Filed as [O80](../appendix/optimizations.md#o80-every-query-opens-spans-a-collector-may-never-read) and not taken** (round 14): 0.8% is inside the lab's spread, and removing it without losing the traces needs a sampled layer | None |
| glommio's cancelled timeouts | [Where the time goes](performance.md#where-the-time-goes) | About 124,000 timeouts a second are armed and cancelled on europa. None of it shows in the profile | Recorded and not pursued |
| [O69](../appendix/optimizations.md#o69-every-idle-moment-parks-an-executor) | [Spinning before parking](performance.md#spinning-before-parking) | A spin before parking cut the `membarrier` calls tenfold and changed nothing, so it was reverted | Nothing, unless a later profile shows the barriers |

## Limitations

These are confirmed on the lab and documented where they live. None is planned as a fix yet.

| Limitation | Found in | Where it is written down |
| --- | --- | --- |
| Failover takes the lease plus an election, three to four times the base: 15–20 s at the default 5 s. The same shape after a real power cut (15 s) | [Failover time](performance.md#failover-time-against-primary_failover_after), [a real power cut](correctness.md#a-real-power-cut) | [C7](../distributed/failover.md#the-window-and-what-a-client-sees), [C15](../distributed/open-issues.md#measured-at-smoke-scale-only) |
| Under load a group remembers about four to seven seconds of write identities, not the five minutes `retry_window` promises. A retry after that is refused `IdentityExpired`, which is safe; a first write queued that long is refused `Shedding` since round 12 and sent again under a new identity (#180) | [Nine SIGKILLs under load](correctness.md#nine-sigkills-under-load), [section 11](correctness.md#the-loader-past-what-the-cluster-commits) | [F45's limitations](../features/replica-migration.md#limitations) |
| A rebuild leaves the control group one failure short of its usual margin until the old identity's removal commits | [Section 8](correctness.md#8-an-unplaced-member-coordinates), runs 3 and 4 | [F56's limitations](../features/cluster-rebuild.md#limitations) |
| A get through a member with no placement slot costs one forward hop: 0.34 ms more than a placed member idle, 2.7 ms at the median loaded | [A member added with no rebalance](correctness.md#a-member-added-with-no-rebalance), measured in [round 13](correctness.md#a-get-through-an-unplaced-member) | Correctness, round 13. The cost of the hop, with nothing on it to remove |
| A rebalance off a live source waits `retire_after` (five minutes by default) for each set | [A member added with no rebalance](correctness.md#a-member-added-with-no-rebalance) | [Runbooks](../operations/runbooks.md) |
| ~~Nothing ships backup files between hosts~~ | [Back up, destroy and restore](correctness.md#back-up-destroy-and-restore) | **Built in round 13**: `cluster ship-backup` ([F59](../features/backup-shipping.md)) |
| The archive map is bounded by nothing: rows are a tenth of a node's resident memory under the bench (340 MiB of 2.9 GiB), now reported on `Stats` | [Memory](performance.md#memory), [who leads the busiest groups](performance.md#who-leads-the-busiest-groups) | Todos: [the archive map](../appendix/todos.md#a-nodes-archive-map-is-bounded-by-nothing) |
| A node under its append reserve serves reads from a copy that is behind; a read that needs more is refused `Unavailable` | [A nearly full disk](correctness.md#a-nearly-full-disk) | [#156](../appendix/resolved/wal-failure-stops-the-node.md#still-open) |
| ~~A slow disk on one node costs the cluster about a quarter of its rate at 10 ms a request~~ | [Section 12](correctness.md#12-scenarios-nobody-had-run) | **Closed in round 13**: a disk four times slower now costs about 2%, and moving its leads away gains nothing ([a slow disk, again](correctness.md#a-slow-disk-again)). What is left is margin: its groups commit on the other two while it lags |
| An ASan build needs `-Zsanitizer-recover=address`, because gxhash reads past the end of short keys within a page | [Findings](findings.md#deployment-and-lab-findings) | Findings |
| Openraft's debug tracing cannot be used on a loaded node. It writes about 150,000 lines a second | [Section 8](correctness.md#8-an-unplaced-member-coordinates), runs 3 and 4 | [Overview](overview.md#when-a-move-is-slow) |

## Not yet explored

| Scenario | Why it matters |
| --- | --- |
| The power itself cut | `sysrq b` loses the page cache but not a device's write cache. Cutting a host's power by hand is the only way to test that on this lab, and it needs a person at the host |
| Past the lab's scale: more than three nodes, a factor above three, a terabyte a node | Moves at terabyte scale are extrapolated, not measured; round 13 staged the retention half of it ([a step that outlasts the log](correctness.md#a-step-that-outlasts-the-log)). An inventory places one node per host, with one set of ports and one unit name, so more nodes need more hosts |
| The headline numbers on the benchmark host | Europa runs `powersave` and hosts the clients as well as a node. The figures on these pages are lab runs, so any that are to be quoted need a rerun as captures |
| ~~A partition longer than a minute, and several in a row~~ | **Run in round 13** ([longer partitions](correctness.md#longer-partitions-and-a-flapping-one)): 120 s, 300 s and ten flaps, nothing lost, a heal is a second; found #184 |

## Related

- [Findings](findings.md), for everything this testing found and what was done about it.
- [C15](../distributed/open-issues.md), the design's own list of open questions.
- [Known Issues](../appendix/known-issues.md), [Optimizations](../appendix/optimizations.md) and
  [Todos](../appendix/todos.md), where every numbered row above is kept.
