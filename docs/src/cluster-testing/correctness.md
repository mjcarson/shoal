# Correctness

Every test on this page ends the same way: what the cluster acknowledged is read back, through
every member, and compared with what was written. A test passes when nothing acknowledged is
missing or different. Test numbers match the directories the runs were recorded in
(`target/lab/tNN-*`).

## 1. Loading the whole dataset

```bash
L=target/deploy/release/tmdb-dataset-loader
$L load -i tmdb_cluster.yaml --dataset ~/datasets/TMDB_movie_dataset_v11.csv
```

| Run | Rows written | Time | Rate | Retries | Sample read back |
| --- | --- | --- | --- | --- | --- |
| First, europa on the Optane | 2,193,788 | 56.9 s | 38,587 rows/s | none | 10,073 of 10,073 in 45 ms |
| Second, after a destroy and a fresh bootstrap, with the test suite compiling on europa | 2,193,788 | 51.2 s | 42,860 rows/s | none | 10,073 of 10,073 in 274 ms |

Two csv rows do not parse and are skipped. Neither load needed a retry: the smaller in-flight gate
F54 shipped with holds the queue under `write_timeout` on this lab
([item 129](../appendix/known-issues.md#129-an-overloaded-group-answers-outcomeunknown-rather-than-shedding)
still stands for a client that does not hold it).

**Verdict: pass.**

## 2. Reading everything back

```bash
$L verify -i tmdb_cluster.yaml --dataset ~/datasets/TMDB_movie_dataset_v11.csv   # --read quorum
```

`verify` reads every movie in gets of 256 ids, four readers each through a different member, and
compares each row field by field with the csv. It then reads all 58,418 keyword partitions whole
and compares each one's set of sort keys with the csv's.

| What | Expected | Found | Time |
| --- | --- | --- | --- |
| Movies | 1,187,691 | 1,187,691, none missing, none different | 6.2 s |
| Keyword partitions | 58,418 holding 1,003,116 rows | all 58,418 identical | 1.6 s |

The first run reported 238 movies "different from the csv". The csv holds 513 ids more than once
(857 extra rows). The loader deals rows out to workers round robin, so which of an id's rows lands
last is not fixed, and 238 of them ended on an earlier row. `verify` now accepts any of an id's csv
rows and counts those separately. That is a property of the loader, which promises no order
between two rows of one id, not a defect.

The first attempt at this read back could not run at all: its gets of 256 ids crashed every member
they were sent through ([Resolved #133](../appendix/resolved/read-plan-rc-across-shards.md), below).

**Verdict: pass**, after #133.

## 3. Acknowledged writes, read back through every member

```bash
$L bench -i tmdb_cluster.yaml --dataset ... --duration 30 --mix insert:100 --acks acks.txt
$L verify-acks -i tmdb_cluster.yaml --acks acks.txt --every-member
```

1,048,800 synthetic inserts were acknowledged in 30 seconds (about 35,000 a second from eight
workers with 128 in flight each). Every one was then read back at `Quorum` through europa, titan
and hyperion in turn:

| Through | Found | Lost | Time |
| --- | --- | --- | --- |
| europa | 1,048,800 | 0 | 16.6 s |
| titan | 1,048,800 | 0 | 21.1 s |
| hyperion | 1,048,800 | 0 | 21.0 s |

**Verdict: pass**, after #133.

### The crash this found

The first `verify-acks` run killed europa's node with `SIGSEGV`, and the next two runs killed it
again, then titan, then hyperion: whichever member coordinated the read. Narrowing it on the
cluster, reading 20,000 ids through one member:

| Get size | Level | Client retry | Result |
| --- | --- | --- | --- |
| 1 id | `Quorum` | yes | answered, 20,000 found |
| 16, 64, 256 ids | `Quorum` | yes | the member crashed |
| 2 ids | `One` | no | the member crashed |
| 2 ids, on a standalone node | `One` | no | answered |

Every crash was at one instruction inside mimalloc, which meant a heap corrupted earlier. The node
was rebuilt with AddressSanitizer on the system allocator, swapped onto hyperion alone, and sent
the two id get. It reported a use-after-free: an `Rc<[SessionToken]>` in every split query's
read plan, allocated on one shard thread and freed on another. The fix, the ASan report and the
reasoning are on [Resolved #133](../appendix/resolved/read-plan-rc-across-shards.md). The fixed
program was rolled onto the cluster with `cluster upgrade` (18.7 s, one node at a time), and
the two id get, the 256 id read back and every test after this one ran on it.

## 4. Faults under load

Each fault test runs a 60 second mixed bench (`get:55,keyword:15,update:15,insert:15`, eight
workers, 128 in flight), injects the fault at 15 s, heals it at 35 s, and then reads every
acknowledged insert back through every member (`target/lab/fault.sh`). A baseline run with no fault
did about 70,000 operations a second, a fifth of them writes, with no failures, and read back all
558,451 of its acknowledged inserts through each member.

### Kill a follower

`systemctl kill -s SIGKILL` on titan. systemd restarts it five seconds later.

**The first run found three defects.**

- **Titan never came back.** Every restart failed within ten seconds with `AlreadyExists` on a
  temp file: the kill had landed inside an archive map save, which left its temp file behind, and
  every later save refused to create it. Titan restarted nine times in two minutes and never served
  ([Resolved #135](../appendix/resolved/leftover-temp-map.md)).
- **The fix could not be delivered.** `cluster upgrade titan` refused because titan was down,
  which is when a fix is needed ([Resolved #136](../appendix/resolved/upgrade-a-down-node.md)).
  Once it was allowed, the upgrade declared titan back five seconds before its shards had started
  ([Resolved #137](../appendix/resolved/upgrade-waits-for-groups.md)).
- **A quarter of all operations were refused `IdentityExpired`** from about 30 seconds after the
  kill to the end of the run. Every write on a query stream carried the stream's own identity, as
  old as the stream, and once any group evicted a newer stream's entry it refused every older
  stream ([Resolved #138](../appendix/resolved/stream-bundle-identity.md)).

Two members read back every acknowledged insert. The third was titan, which never came up.

**The second run, with #135 to #138 fixed:**

| Seconds after start | Operations per second | Failures | What |
| --- | --- | --- | --- |
| 0–14 | 60,000–84,000 | none | before the kill |
| 15 | 61,571 | 369 | the kill: 256 in-flight operations on titan's connections `ConnectionLost`, 113 writes `OutcomeUnknown` |
| 16–32 | 44,000–95,000 | 600–1,600 a second (about 2%) | `NotLeader`: "the replication link went down before the request was written: Connection refused", writes hopped to titan as the leader of its groups |
| 22–25 | 900–71,000 | up to 950 a second | `Unavailable`: "group … is still starting", titan's groups coming back |
| 33–37 | 61,000–95,000 | none | recovered |
| 38–40 | 35,000, 8,300, 480 | none | a stall, with the workspace test suite running on europa beside the bench |

titan restarted once. Every one of the 547,937 acknowledged inserts was read back through each
of the three members.

**Verdict: pass** for correctness. Two things are left:

- **Failover took about 17 seconds.** Only two votes were cast on each survivor in the 37 seconds
  after the kill: the groups titan led never elected new leaders. They waited out
  `primary_failover_after` (5 s by default, so an election timeout of 5 to 10 s) and openraft's
  leader lease of the same maximum, and titan was back first. That is the configured behaviour,
  and it is measured against shorter settings on the [performance page](performance.md#failover-time-against-primary_failover_after).
- **The stall at 38 to 40 s** had nothing in any node's journal and ran beside a test suite on the
  client's host. It is rerun on an idle host [below](#kill-a-follower-idle-rerun).

### Kill a follower, idle rerun

The same test with nothing else running on europa. Titan, which had come back from the previous
run leading none of its 36 groups, was killed at 15 s. The only failures were the 256 operations in
flight on titan's connections at the kill (`ConnectionLost`). There was no `NotLeader`, no stall, and
throughput stayed between 40,000 and 88,000 operations a second through the restart. All 683,354
acknowledged inserts were read back through each member.

**Verdict: pass.** It also shows that a node that has restarted once leads nothing until something
moves leadership back, which nothing does: after this run europa led 24 groups, hyperion 12 and
titan none. That is on the [performance page](performance.md#leadership-after-a-restart).

### Kill the node leading the most groups

`systemctl kill -s SIGKILL` on europa, which led 24 of the 36 groups and the control group. systemd
restarted it five seconds later.

| Seconds after start | Operations per second | Failures | What |
| --- | --- | --- | --- |
| 0–14 | 60,000–84,000 | none | before the kill |
| 15 | 64,449 | 1,328 | the kill: `ConnectionLost` on europa's connections, `OutcomeUnknown` for writes in flight, `NotLeader` |
| 16–31 | 15,000–51,000 | 5,000–15,700 a second | `NotLeader` on every write to a group europa led: "the replication link went down before the request was written: Connection refused" |
| 20 | | | europa's node restarts (08:49:12) |
| 29–31 | | | the data groups elect new leaders on titan and hyperion (08:49:21–23, 14.5 s after the kill) |
| 33–39 | 0–27,000 | a few hundred | **everything stalls, reads included**: titan and hyperion at 78–85% iowait with idle cpus |
| 40 onwards | 54,000–84,000 | none | recovered |

All 452,629 acknowledged inserts were read back through each member.

**Verdict: pass** for correctness, **fail** for availability. Writes to a crashed leader's groups
fail for the lease plus an election timeout (15–20 s at the default `primary_failover_after` of
5 s), and the two remaining nodes then stall on their disks for about seven seconds. The documented
failover objective is base + 2 s. Both are followed up on the [performance page](performance.md#failover-time-against-primary_failover_after).

### Rolling upgrade under load

A 70 second mixed bench, with `cluster upgrade --force` started 10 seconds in: every node's program
replaced and its unit restarted, one node at a time, the control leader last. Each row is one run.

| Run | Upgrade | Write refusals | Other failures | Acknowledged inserts lost | What it found |
| --- | --- | --- | --- | --- | --- |
| Before #139 | the previous build, restarted by the new one | 84,418 `NotLeader` in the 30 s after the upgrade | none | 0 | Every restarted node took its groups' leaders down, and each group waited for a lease and an election ([Resolved #139](../appendix/resolved/leadership-handoff-on-stop.md)) |
| Handoff, first cut | completed | about 100 `NotLeader` | 384 `ConnectionLost` on europa's connections | 0 of 342,315 | The handoff works, but each stopping shard logged `handed=0` after waiting out its timeout: it had taken its groups' handles before the transfer, so it never saw that it had lost the lead |
| Handoff, two phases | **hyperion never came back**: `ReadyTimeout { ready: 5, of: 6 }` on the new program and again on the reverted old one | — | five bench workers hung for good | — | Shard 2's 29.7 MB map intent log took minutes to replay, three direct reads a record ([Resolved #140](../appendix/resolved/intent-log-read-ahead.md)) |
| Read-ahead | completed | 14 `NotLeader` | `ConnectionLost` for the workers on each restarted node; three streams never reached their end | 0 | A stream that failed handed its channel, and its late answers, to the next stream ([Resolved #141](../appendix/resolved/recycled-stream-channels.md)) |
| All of the above | completed | 14 `NotLeader` | 640 `ConnectionLost` | **0 of 288,942**, read back through every member | nothing: no unexpected answer, no stream that did not end |

**Verdict: pass**, after #139 to #141. A rolling upgrade under load now costs the clients the
operations in flight on the node being restarted, as `ConnectionLost` (retriable), and a handful of
`NotLeader`. It no longer costs every write to that node's groups for 15 to 20 seconds.
Graceful restarts of titan and of europa on their own (`systemctl restart` under a 30 second bench)
cost no `NotLeader` at all.

### Partition one node

For 20 seconds under the mixed bench, `iptables` on hyperion drops every packet to and from its
peers' data and control ports (12001–12002), both ways, while its client port stays reachable
(`target/lab/partition.sh`). Nothing resets a connection, so nothing says the peer is gone.

**The first run: the whole cluster stopped.** From two seconds into the partition until five
seconds after it healed, throughput was zero, reads included, except for one second in five when
exactly 1,024 writes failed `OutcomeUnknown`. Every write hopped to hyperion, which still led twelve
groups as everyone else saw it, waited the full `write_timeout`. The bench's eight workers each
keep 128 queries outstanding, so each window filled with those writes and nothing else was sent.
All 222,819 acknowledged inserts were read back.

**With [#143](../appendix/resolved/silent-partition-hops.md) fixed**, a hop over a link that has
answered nothing for two seconds is refused `NotLeader` at once. From two seconds into the
partition the cluster served 22,000–110,000 operations a second, refusing only writes to hyperion's
groups, and all 262,583 acknowledged inserts were read back through every member.

**What is still wrong.** From about 17 seconds into the partition until 20 seconds after it healed,
throughput fell to zero for a second or two at a time, reads included, with writes timing out at
5 s again:

- europa and titan contended for hyperion's twelve groups in election rounds seven to eight seconds
  apart, a new leader stepping down at the other's higher term;
- journald on titan reported suppressing 43,954 lines from the node, most of them openraft warnings
  for every failed heartbeat to hyperion, per group, twice a second.

The log flood is fixed as
[O66](../appendix/optimizations.md#o66-a-partitioned-peer-floods-the-log): the rerun had journald
suppress nothing. [O65](../appendix/optimizations.md#o65-heartbeats-to-followers-that-just-acknowledged-replication)
did not touch it. The elections are not a load problem. They continue after the heal because
hyperion's groups kept their election timers running through the partition, and on the heal it
campaigned at a higher term and made healthy leaders on europa and titan step down.

**With [#144](../appendix/resolved/post-heal-elections.md) fixed** (Pre-Vote on every group,
`target/lab/t05d-partition-prevote`, rolled onto the running cluster by `cluster upgrade`), the
same test:

| Seconds | Before #144 (t05c) | With Pre-Vote (t05d) |
| --- | --- | --- |
| Partition, first 2–3 s | 0 | 0 (#143's *Still open*) |
| Partition, the rest | 5,000–121,000 ops/s, hyperion's groups refused | 27,000–127,000 ops/s, hyperion's groups refused |
| From the heal for 20 s | 0–85,000, zero for 1–2 s at a time, writes at 5 s | 63,000–84,000, never zero |
| journald suppressions | 50,000–60,000 per node | 0 |

All 529,759 acknowledged inserts were read back through each of the three members. The fixture
test that reproduced #144 showed hyperion's terms staying where they were while it was cut off.
On the lab no leader on europa or titan stepped down at a higher term from hyperion.

**What the rerun still showed.** Every second after the heal, a few hundred writes took exactly
the 5 s write timeout and then succeeded. Those were writes coordinated on hyperion while its
copies were waiting for or installing snapshots: the coordinator waited for its own copy to apply
an entry it could not apply. Fixed as [#145](../appendix/resolved/apply-wait-on-a-stalled-copy.md).
The first two to three seconds of a silent partition still stop pipelined clients, while the hops
to the cut-off node wait for the two second silence to be judged; that remains
[#143](../appendix/resolved/silent-partition-hops.md#still-open)'s open part.

**With #145 fixed** (t05e) the 5 s writes were gone, and the worst write in a second after the heal
was about a second: the two heartbeats the wait gives a copy that is not applying. That run also
showed why hyperion was not applying. It came back behind the purge point of its groups and was
fed thirteen snapshots over 72 s, up to three per group, because the leader purged past each
install's boundary while it ran. Its reads of those groups were refused the whole time, and a
read-back through it 35 s after the heal timed out (a second attempt later passed, nothing lost).
With the default retention raised to 100,000 entries
([O67](../appendix/optimizations.md#o67-ten-thousand-retained-entries-is-seconds-of-a-busy-group),
t05f), hyperion caught up from the log: no installs, no refused reads, and the read-back passed
through every member first time.

**Verdict:** correctness **pass**, availability **good after the first three seconds**.

### Pause one node

For 20 s under the mixed bench, a node's process is stopped with `SIGSTOP` and then resumed with
`SIGCONT` (`systemctl kill --signal` on its unit, `target/lab/stall.sh`). A paused process is not
a partition: every one of its sockets stays open, its client port included, and it answers
nothing. When it resumes, every timer it had has expired at once. This is the "GC pause" case,
and it is where [Pre-Vote](../appendix/resolved/post-heal-elections.md) is meant to earn its
keep.

| Run | Paused | Pause | After the resume |
| --- | --- | --- | --- |
| t11 | hyperion, leading 12 | 0 for 3 s, then 36,000–73,000 ops/s with its groups refused `NotLeader` until they re-elected about 16–20 s in | 47,000–80,000 ops/s; write p99 2–4.5 s every few seconds |
| t11b | europa, leading the most (`--slow-ms 1000`) | 0 for 3 s, then about 48,000 | 37,000–70,000 ops/s; slow writes only in the first 2 s, all through europa, at most 1.7 s |
| t11c | hyperion (`--slow-ms 1000`) | the same shape as t11 | 1,210 writes over a second in the 12 s after the resume, all through hyperion, up to 5.3 s |

Every acknowledged insert was read back through every member in all three runs, and no node
restarted. No leader was unseated by the resumed node's expired timers.

**What t11 and t11c showed.** The periodic write p99 spikes in t11 first looked like the leadership
handbacks ([O63](../appendix/optimizations.md#o63-leadership-never-returns-to-a-groups-placement-primary)),
which happened at the same moments. openraft's own log showed every transfer landing, none
ignored for an out-of-date log. The bench's new `--slow-ms`, which logs each slow operation with
the member it went through, took it apart in t11c. Every slow write went through the resumed node,
whose copies were catching up from the log. The coordinator was waiting for its own copy to apply
the whole backlog before answering, a case #145's stall rule had deliberately left waiting. Fixed
as [#146](../appendix/resolved/apply-wait-on-a-lagging-copy.md): the wait is bounded at two
heartbeat intervals.

**What the next rolling upgrade found.** `cluster upgrade` refused to start: *"europa is down,
not up"*. europa was running and was the control leader. When t11c's paused control leader,
hyperion, resumed, its detector still took itself for the leader and saw 20 s of silence from
everyone. That silence was its own. It called europa and titan down, and europa, which had
replaced it, committed the verdicts. titan's next report set it up again. Nothing ever reports a
leader to itself, so europa stayed down in the record. Fixed as
[#147](../appendix/resolved/paused-detector-verdicts.md).

**With #146 and #147 deployed** (t11d), the sequence that left europa down was run again: pause
the control leader for 20 s under the mixed bench, then, 20 s after that run, pause whichever node
led next. Control leadership went europa, titan, europa. After each run every member read `up`,
and every acknowledged insert (380,990, then 469,395) was read back through every member.

| Run | Paused | Writes over 1 s after the resume | The slowest |
| --- | --- | --- | --- |
| t11c (before #146) | hyperion | 1,210, for 12 s, all through hyperion | 5.3 s |
| t11d-61 | europa, the control leader | 628 in the 2 s around the resume, all through europa | 1.48 s |
| t11d-62 | titan, the control leader | 569 in the 2 s around the resume, through all three | 1.73 s |

What remains around the resume is writes sent while the node was still stopped. They sat in its
socket buffers and were answered once it ran again, which no server change can shorten.

**Verdict:** correctness **pass**; with #146 and #147, a paused node, the control leader included,
costs its own writes a second or two around the resume and leaves the record right.

### Kill every node at once

Fifteen seconds into the mixed bench, every node's process is sent `SIGKILL` at the same moment
(`target/lab/kill-all.sh`), and `Restart=on-failure` starts each one again. This is a power cut
without the power cut: the page cache survives, so it tests what the processes wrote and synced,
not what the devices kept.

**t12, the first run.** europa and titan were back in about ten seconds. Writes were refused
`NotLeader` until elections finished, and 20 s after the kill the cluster served 36,000–49,000
operations a second. **hyperion never started again**: thirteen restarts, each failing
`ShardFailed { shard: 1, error: "Rkyv(Error { inner: Failure })" }` while replaying its Movie
table's archive map intent log. Every one of the 306,375 acknowledged inserts was read back
through europa and titan.

The damaged log held, past its logical end, archive records of the same table, with framing and
checksums that an intent shares, in the tail of its last 128 KiB block. A partial flush writes a
whole buffer, and glommio recycles buffers without zeroing them. A scan of the other nodes' logs,
copied while they ran, found **titan's** Movie `Shard-5` log damaged the same way. titan would not
have started again either, and with hyperion down that would have lost the cluster's quorum.
Fixed as [#148](../appendix/resolved/stale-intent-log-tail.md), in the glommio fork and in the
reader.

**With #148 fixed** (t12b), deployed by repairing hyperion with `cluster upgrade hyperion`: its
reader stopped at a foreign frame in two shards' Movie logs, 1 and 4, and it caught up. The same
kill then brought every node back after one restart each. Service was back 17 s after the kill
(18 s in t12), at 85,000–90,000 ops/s within two seconds, and all 309,478 acknowledged inserts
were read back through every member.

**Verdict:** correctness of acknowledged data **pass**; recovery **pass** with #148.

### Corrupt an archive, then scrub and repair it

On the rebuilt cluster with the whole dataset, through `cluster admin` (the tab's command line,
scripted, added for this test):

1. **`repair Movie verify` on a healthy cluster:** all 18 Movie groups `Clean` in 46 s. Every
   group's three copies were hashed and compared.
2. **64 random bytes written into titan's largest Movie archive**, 200 MB in, in place, with the
   node running (`dd … conv=notrunc`). The node reads its archives with direct I/O, so nothing
   cached hid the damage.
3. **`repair Movie verify` again:** 17 groups `Clean`, one `Divergent`, naming titan's copy
   quarantined for `Checksum`, in 48 s. The corruption was found where it was and nowhere else.
4. **`repair Movie repair`:** titan's copy was reinstalled from europa's, verified at its
   boundary, and reported `Repaired`, in 95 s. A further `repair Movie verify` found every group
   `Clean`.
5. **Every row read back through titan alone** at `One`, which is titan's own copy
   (`verify --member 2 --read one`, the option added for this): all 1,187,691 movies and 58,418
   keyword partitions equal to the csv.

**Verdict: pass.** Detection, quarantine and repair behaved as [F44](../features/repair.md) says,
on real hardware and a real dataset.

### Back up, destroy and restore

A backup of every table from the cluster that held the dataset and the t13g bench's inserts, the
cluster destroyed, a fresh one bootstrapped, and the backup restored into it
([runbook 10](../operations/runbooks.md#10-backup-and-restore)), all through `cluster admin`:

1. **Wire version 5 activated** (`cluster upgrade --activate`), which a backup needs.
2. **`backup /optane/shoal-backup`:** 36 groups `Written` in 2 min 49 s. Each group's leader
   wrote its file to its own disk: 1.1 GB on each host, 3.2 GB in all.
3. **Every host's files gathered and copied to every host** with `rsync`, since a restore reads
   each group's file on that group's new leader. Nothing ships them, as the runbook says.
4. **`cluster destroy`, `cluster bootstrap`, activate, `restore <dir>/<op>`.**

**First run (t14): a group lost.** The command printed `done` and exited 0. A `verify` against the
csv found **66,191 movies missing**. The backup's Movie records summed to 7,893,508, the restored
table held 7,453,644, and the gap of 439,864 was exactly one group's file. On hyperion, shard 1
had died during the restore, over a clean-up that synced an install directory a restore never
creates ([#153](../appendix/resolved/install-dir-absent.md)). Its restart reset a peer connection
at the moment another group's restore was quarantining that member, and that group failed. The
command did not say so ([#154](../appendix/resolved/admin-hides-failed-groups.md)), and nothing
could finish the restore short of doing it again on a new cluster
([known issue 155](../appendix/known-issues.md#155-a-restore-whose-group-failed-cannot-be-finished)).

**With #153 and #154 fixed (t14b):** the same backup restored in 1 min 50 s, with every group
`Restored` and verified and no node restarted. The table held 7,893,508 Movie partitions and 58,418
keyword partitions, exactly the backup's records, and a `verify` found 0 missing and 0 different.

**Verdict:** backup **pass**; restore **pass** with #153, and a restore that fails part way is
still unrecoverable (155).

### Fill a node's disk

hyperion's storage moved onto a 2 GiB ext4 filesystem on a loop device, through an inventory group
of its own, so a full disk stayed inside the test and away from the host's root. Then the dataset
was loaded.

- **The disk filled 70 s in.** hyperion's WAL writes failed with ENOSPC, and openraft stopped every
  group core on the node with a fatal storage error (`when Write Log`), on groups hyperion led and
  groups it followed alike. The node stayed up, its copies serving nothing, as a dead core is
  documented to do ("until the process restarts").
- **Writes through hyperion failed from then on**, `Unavailable: writing to group …: when Write
  Log`, rather than going to the groups' new leaders elsewhere. The loader, connected to every
  member, stopped at them. Groups led elsewhere kept committing on europa and titan.
- **Nothing was corrupted.** With the filesystem grown to 4 GiB online and hyperion restarted, it
  started at once, caught up, and every movie and keyword partition read through hyperion alone at
  `One` equalled the csv.

**Verdict:** durability **pass**. Availability through a node with a full disk **fail**, filed as
[known issue 156](../appendix/known-issues.md#156-a-full-disk-stops-every-group-on-a-node-until-it-is-restarted).
Cleaning up also found `cluster destroy` unable to remove a storage path that is itself a mount
point ([known issue 157](../appendix/known-issues.md#157-cluster-destroy-fails-on-a-storage-path-that-is-a-mount-point)).
