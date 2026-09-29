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
([item 129](../appendix/resolved/overload-sheds.md) stood for a client that does not hold it,
until [section 11](#11-overload-silence-and-a-nearly-full-disk) fixed it).

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
   each group's file on that group's new leader. ~~Nothing ships them, as the runbook says.~~
   Since round 13 `cluster ship-backup` does
   ([F59](../features/backup-shipping.md), [round 13](#a-backup-shipped-and-restored)).
4. **`cluster destroy`, `cluster bootstrap`, activate, `restore <dir>/<op>`.**

**First run (t14): a group lost.** The command printed `done` and exited 0. A `verify` against the
csv found **66,191 movies missing**. The backup's Movie records summed to 7,893,508, the restored
table held 7,453,644, and the gap of 439,864 was exactly one group's file. On hyperion, shard 1
had died during the restore, over a clean-up that synced an install directory a restore never
creates ([#153](../appendix/resolved/install-dir-absent.md)). Its restart reset a peer connection
at the moment another group's restore was quarantining that member, and that group failed. The
command did not say so ([#154](../appendix/resolved/admin-hides-failed-groups.md)), and nothing
could finish the restore short of doing it again on a new cluster (item 155, since
[resolved](../appendix/resolved/restore-retry.md) and proved on the lab in
[section 7](#a-restore-finished-by-a-retry)).

**With #153 and #154 fixed (t14b):** the same backup restored in 1 min 50 s, with every group
`Restored` and verified and no node restarted. The table held 7,893,508 Movie partitions and 58,418
keyword partitions, exactly the backup's records, and a `verify` found 0 missing and 0 different.

**Verdict:** backup **pass**; restore **pass** with #153, and a restore that fails part way is
~~still unrecoverable (155)~~ finished by `restore-retry` since #155's second half
([section 7](#a-restore-finished-by-a-retry)).

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
[known issue 156](../appendix/resolved/wal-failure-stops-the-node.md).
Cleaning up also found `cluster destroy` unable to remove a storage path that is itself a mount
point ([#157](../appendix/resolved/destroy-mount-point.md), fixed).

**Rerun with #156's fix deployed**, on the same 2 GiB filesystem and the same load:

- **The disk filled 70 s in, and hyperion stopped** 115 ms after its first failed WAL batch. The
  first exit was shard 3's checkpoint write (`the checkpoint file could not be written: … No space
  left on device`), a few milliseconds ahead of the WAL check. Either path stops the node, which is
  the point: no core was left dead inside a running process. The loader's writes went to the
  groups' new leaders on europa and titan.
- **While the disk stayed full, hyperion kept failing to start**, with systemd's restart count
  climbing 3, 6, 9, 12 over a minute. Each start stopped at its first shard: the compactor's
  archive map rewrite at open (`Movie/maps/temp/Shard-0`) found no space. It never came up half
  able to write.
- **Once the filesystem grew to 4 GiB online, hyperion came back with no restart by hand.** The
  next scheduled restart started it, and 15 s later all three voters were up. A second load of
  200,000 movies ran without a failure, and every one of them read back through hyperion alone at
  `One` (`verify --member 1 --read one --movies-only`): 0 missing, 0 different.

**Verdict:** availability through a node with a full disk **pass**, in the sense #156's fix
promises: the node leaves the cluster instead of holding dead copies, and returns on its own once
space does. Shedding writes before the disk is full remains open (156).

## 5. Regression pass

The fault suite again on the rebuilt cluster with every fix above deployed (#143 to #154, O61 to
O68), one 60 s mixed bench per fault, each verified through every member
(`target/lab/suite.sh`):

| Fault | Acknowledged inserts | Lost | Seconds at zero | Refusals |
| --- | --- | --- | --- | --- |
| Kill the node leading the most groups (hyperion) | 676,227 | 0 | 0 | 110,167 `NotLeader`, 20,495 `Unavailable`, 603 `OutcomeUnknown` |
| Partition hyperion by dropped packets, 20 s | 595,558 | 0 | 3, at the partition's start | 340,039 `NotLeader`, 1,493 `OutcomeUnknown` |
| No fault (the pause's first run, which found no leader to pause) | 644,994 | 0 | 0 | none |
| Kill every node at once | 353,107 | 0 | 13 | 37,378 `NotLeader`, 573 `OutcomeUnknown` |
| Pause the control leader (hyperion), 20 s | 592,733 | 0 | 2, at the pause's start | 99,953 `NotLeader`, 773 `OutcomeUnknown` |

Every acknowledged insert was read back through every member after every fault. No node needed
more than the one restart its fault gave it, and after the pause every member read `up`. What
remains is filed: the first two to three seconds of a silent partition or a pause
([#143](../appendix/resolved/silent-partition-hops.md#still-open)), a full disk
([156](../appendix/resolved/wal-failure-stops-the-node.md)),
and a restore that fails part way (155, since [resolved](../appendix/resolved/restore-retry.md)).

## 6. A second regression pass, and a crash mid compaction

The fault suite once more, on the build with [#158](../appendix/resolved/runtime-waker-lists.md)'s
runtime fix, after the timer experiments of
[O64](performance.md#revisited-and-the-throughput-is-bimodal) had restarted each node many times
(`target/lab/suite-r158.log`):

| Fault | Acknowledged inserts | Lost | Seconds at zero |
| --- | --- | --- | --- |
| Kill the node leading the most groups (titan) | 630,714 | 0 | 0 |
| Partition hyperion by dropped packets, 20 s | 530,157 | 0 | 3 |
| Pause the control leader (titan), 20 s | 514,526 | 0 | 2 |
| Kill every node at once | 321,077 | 0 through europa; not read through the others | 13 |

The first three matched section 5. Kill-all did not: hyperion never came back. Every start
failed on *"partition 10483307249282197527 of Movie could not be read for a replicated apply"*,
31 restarts until it was stopped. The map named a record past the end of its archive, and that
archive had last been written 50 minutes before. Earlier that evening, restarts during the
timer experiments had outrun the unit's 60 s stop timeout four times, and systemd had SIGKILLed
hyperion each time while loads were running.

The cause was the compactor's write order. A pass wrote each record into its archive and each
record's map intent into the map's intent log through two writers, each writing buffers out as
they filled, and synced both only at its end. So a kill between a map buffer landing and the
archive buffer it names landing left the map pointing at bytes the disk never got. After that,
the redo of the pass could not read the partition, and every compaction of the shard failed.
Then the kill-all's restart replayed a write over it, and a replica that cannot read its archive
stops. Filed, reproduced with a crash point that dies inside the window, and fixed as
[#159](../appendix/resolved/map-ahead-of-archive.md): a job's map intents are staged and written
only after its archive is synced, and a torn entry in the intent log is skipped at load. That
hyperion stopped altogether over one partition is filed as
160, since [resolved](../appendix/resolved/unreadable-partition-stalls-one-copy.md) and proved
on the lab in [section 7](#7-a-copy-that-cannot-read-a-partition).

hyperion's torn entry had been folded into its map's snapshot by the crash-loop starts, which
the fix does not repair, so the lab was bootstrapped again on the fixed build and reloaded.

### Nine SIGKILLs under load

The failure's own trigger, repeated: an insert-heavy bench (`insert:60,update:25,get:15`) while
one node after another is SIGKILLed every 25 s and left to systemd to restart
(`target/lab/kill-loop.sh`).

| | |
| --- | --- |
| Kills | 9, europa, titan and hyperion in turn, from 23:16:42 to 23:20:05 |
| Restarts after | 3 on each node, every one by systemd, none by hand |
| Acknowledged inserts | 3,644,280, every one read back through each member alone: 0 lost |
| Corrupt reads, failed shards, skipped map entries | 0 on every node |
| Compactions failed | 0 on every node |
| Bench, 256 s | get 5,311/s, update 5,926/s, insert 14,235/s; 667,187 updates refused `NotLeader` and retried |

Three updates were refused `IdentityExpired`: a client retry of a write whose outcome was unknown,
after the group had evicted its identity. That is by design, and it is safe, since nothing is
applied twice. But under this load a group remembers about seven seconds of identities, not
the five minutes the window promises, and that is now written down in
[F45's limitations](../features/replica-migration.md#limitations).

**Verdict:** **pass**. A kill cannot be aimed at the compactor's window from the outside, so this
run shows the fixed build surviving the failure's trigger nine times, not that the window was
hit. The fixture's `mid_compaction` crash point is what aims at it.

### Rebuilding a node from its peers

hyperion's copy had to be abandoned for #159 by bootstrapping the whole lab again, because
nothing rebuilt one node then ([todos](../appendix/todos.md#rebuild-a-node-from-its-peers); since
[F56](../features/cluster-rebuild.md), `cluster rebuild`, proved in
[section 7](#rebuilding-a-node-under-load)). Tried on
the rebuilt lab with nothing running: hyperion stopped, its `Movie/`, `MovieByKeyword/` and
`wal/` removed, and its identity (`shoal-meta.json`) and control log kept.

| | |
| --- | --- |
| Back up | 12 s after the start, no restarts |
| Refilled | 2.8 GB, 4.83 million partitions, in about four minutes, fed by the groups' leaders |
| Its copy alone at `One` | 1,187,691 movies and 58,418 keyword partitions equal to the csv; all 3,644,280 inserts acknowledged in the SIGKILL run present |

**Verdict:** it works, on a caught-up cluster. It is not a procedure to hand an operator as is: a
wiped voter under its old identity votes for any candidate, which can lose a committed write if
another voter fails before the refill. The todo now says what a safe `cluster rebuild` has to
check.

### The suite on the fixed build

The fault suite a third time, on the committed build with #158 and #159, after hyperion's
rebuild (`target/lab/suite-r159.log`):

| Fault | Acknowledged inserts | Lost | Seconds at zero |
| --- | --- | --- | --- |
| Kill the node leading the most groups (hyperion) | 487,988 | 0 | 0 |
| Partition hyperion by dropped packets, 20 s | 487,999 | 0 | 3 |
| Pause the control leader (europa), 20 s | 517,025 | 0 | 3 |
| Kill every node at once | 387,733 | 0 | 12 |

Every acknowledged insert was read back through every member, kill-all included. Each node
restarted only when its fault restarted it (europa 1, titan 1, hyperion 2), and all three were
active afterwards. The seconds at zero are section 5's: a silent partition's or a pause's first
seconds ([#143](../appendix/resolved/silent-partition-hops.md#still-open)), and kill-all's
elections.


## 7. A copy that cannot read a partition

The four items the last section left: [160](../appendix/resolved/unreadable-partition-stalls-one-copy.md)
(one unreadable partition stopped its node), [161](../appendix/resolved/failed-start-empty-archive.md)
(a failed start left an empty archive), the rest of [155](../appendix/resolved/restore-retry.md)
(a restore's failed group could not be finished) and a safe rebuild of a node
([F56](../features/cluster-rebuild.md)). Each was fixed in the tree with a fixture test first and
then proved here. The proving found eight more defects, #162 to #168 and one fixed in passing, and
three optimizations. Every lab run below was on a cluster bootstrapped on the build under test, with
wire version 6 activated.

### Stalling one copy, and repairing it unasked

**How.** titan stopped, 64 random bytes written at eight to twelve places in each of its Movie
archives over 20 MB, and titan started again, then the mixed bench (`get:40,update:45,insert:15`)
for 180 s. Titan had to read the damaged records to apply the updates.

**What the first runs found.** Every run, titan stayed up, which is #160 fixed, and every
stalled copy was repaired unasked. How long that took is what the runs found wrong:

| Run | Stalled | Repaired | What went wrong, and its fix |
| --- | --- | --- | --- |
| First | 1 group, while running | after 2 min 40 s, at the second try | The repair's verify read a late cut of its own first scrub as the answer: [#162](../appendix/resolved/stale-scrub-digest.md). The cut itself took two minutes under the bench: [O70](../appendix/optimizations.md#o70-a-snapshot-cut-reads-its-records-one-at-a-time) |
| Third | 8 groups, at the start's replay | 5 in 5 minutes, 3 never | openraft's own snapshot stream replaced each repair's stream: [#163](../appendix/resolved/repair-stream-replaced.md). A held 125 MB snapshot was cut again 17 times in 97 s: [O71](../appendix/optimizations.md#o71-a-held-snapshot-is-cut-again-whenever-the-checkpoint-moves). openraft logged 3,604 refused snapshot builds in 5 minutes: [O72](../appendix/optimizations.md#o72-a-refused-snapshot-build-logs-four-lines-per-apply) |
| Fourth | 2 groups, at a restart | 1 in 5 min, 1 at the second try | A copy restarted from a repair snapshot replayed forty scrubs, each reading 320,000 records, and missed its verify's deadline: [#164](../appendix/resolved/replayed-scrub-cuts.md) |

The same damage also started two loops on titan that no read ended:

- **An archive compaction that met a corrupt record** retried every 5 s, each try leaking the
  records it had rewritten. Titan's Movie archives reached 38 GB, then 64 GB, where the others held
  3 GB: [#165](../appendix/resolved/corrupt-record-compaction-loop.md). Deployed onto that node, the
  fix reported 13 corrupt records in its first minute, and the archives fell to 37.6 GB within two.
- **A segment compaction that had to merge onto one** retried every 5 s without end, holding the
  table's checkpoint on its shard: [#166](../appendix/resolved/segment-compaction-corrupt-loop.md).

**Corruption nothing had read** was then found by a backup, whose cuts failed four groups, and
repaired by an operator: `repair Movie repair` found titan's copy corrupt in 14 of 18 Movie groups,
repaired all 14 from a verified majority and judged the other 4 clean, in 79 s. A cut that meets a
corrupt record now reports it too, which quarantines the copy (on #165's page).

**The runs on the fixed build.** The same damage, twice:

| | Stalled | Each repaired after | Writes refused `Unavailable` | Titan restarts |
| --- | --- | --- | --- | --- |
| Final run | 4 groups, while running | 20 to 38 s, under the bench | 18,853 | 0 |
| With #167 | 4 groups | 1.5 to 2.5 min, under the bench | 12 | 0 |

The final run's 18,853 refusals were one group, for 16 s. Titan led it, and its core stopped before
the lead it had asked to hand on could move. Every write of the group through any node reached the
dead core until an election: [#167](../appendix/resolved/stalled-leader-handoff.md). With the fix
the stalled leader keeps its core until another member leads, and the refusals went from 18,853 to
12. Each repair used one or two cuts, against 17 before O71.

After the final run every insert acknowledged under it was read back through each member alone
(950,263 found, 0 lost), and after the operator's repair above, every movie and keyword partition
read through titan alone at `One` equalled the csv (0 missing, 0 different).

**Verdict: pass**, on the fixed build: one unreadable partition costs one copy of one group, for a
minute or two, and no node stops.

### A failed start leaves no empty archive

The lab's nodes were started eight times on the fixed build (titan four, europa two, hyperion two),
through upgrades, the corruption runs' restarts and `SIGKILL`s. Afterwards no node held a
zero-length archive file (`find */archives -maxdepth 1 -size 0`). The six empty files per node
under `archives/intents/` are the map intent logs, which each compactor start truncates, not
archives. **Verdict: pass.**

### A restore finished by a retry

**How.** A backup of every table (36 files, 1.5 GB, in 9 s), its files copied to every host, the
cluster destroyed and bootstrapped, wire version 6 activated, and one Movie group's file made
unreadable on every host (`chmod 000`) before `restore <dir>`.

| | |
| --- | --- |
| Restore | 58 s; 1 group `Failed` at `Installing` (*"does not verify against its manifest: Permission denied"*), the command exits 1 naming it |
| The table afterwards | 66,312 movies missing, one group's rows |
| `restore-retry <op>`, the file readable again | 28 s; the failed group driven from `Installing` and `Restored`, the other 35 untouched |
| The table after the retry | 0 missing, 0 different, every keyword partition equal |

**Verdict: pass.** Before this, a restore with a failed group was a cluster to delete and
restore again whole.

### Rebuilding a node under load

**How.** `cluster rebuild hyperion --yes` while the bench ran against all three nodes for 480 s,
then every acknowledged insert read back through each member, and every row read through each
member alone at `One`.

**First run: a cluster restored minutes before.** The rebuild itself worked: down in 14 s, joined
as a new identity 22 s later, 18 sets moved in 6 minutes, 392 s in all. But a tenth of the bench's
gets answered `NotFound` for movies that exist (472,946), and afterwards hyperion's own copy was
missing 331,350 movies. About half its groups had been fed from their leaders' logs, which began at
entry one and held none of the restored rows, because a restore installs them outside the log:
[#168](../appendix/resolved/restored-rows-outside-the-log.md). The same scenario in the fixture,
a set moved onto a spare after a restore, lost 32 of 32 restored rows. The fix purges a copy's log
through every repair or restore it installs. The lab was repaired with `repair Movie repair` and
`repair MovieByKeyword repair`, after which every member alone equalled the csv.

**Second run: the same backup restored into a fresh cluster on the fixed build, then the rebuild
under the bench.**

| | |
| --- | --- |
| Committed down, joined as a new identity | 13 s, 21 s later |
| Plan | 18 of 18 sets moved, 1.4 GiB (1.8 GiB streamed, every set as a snapshot), 11 min 22 s |
| Whole rebuild | 710 s |
| Bench, 480 s | get 12,738/s, update 14,318/s, insert 4,774/s; 0 `NotFound`; no second at zero |
| Acknowledged inserts | 2,296,295, all found through each member alone |
| Each member alone at `One`, and the default read | 0 missing, 0 different; every keyword partition equal |

Why so slow, at 2.6 MB/s: no step's transfer ran slowly (82 MB streamed and installed in about
1.5 s). The plan moves one set onto the node at a time, and each step carries about 35 s of fixed
cost: the planner's 5 s tick, catch-up, two membership changes, and cuts that queue behind the
source's compaction under the bench. Two more rebuilds, once the benches had grown the data to
about 240 MB a set, moved 3.4 MiB/s both with `moves_per_node` at 1 and at 6. At that size the
per-record work is the limit: a 279 MB set's cut took 41 s under the bench, and its catch-up
36 s, on hosts already busy. Six steps at once only stretched each one. The one waste in those
runs, snapshots cut for the old identity while it was still a member, is now skipped:
[O73](../appendix/optimizations.md#o73-a-snapshot-is-cut-for-a-member-that-cannot-be-reached). At terabyte scale the step's own size would dominate instead,
and two defaults would probably stop it converging under writes; see
[F56's Limitations](../features/cluster-rebuild.md#limitations).

**Verdict: pass** on the fixed build. The first run's failure was the restore's, and it would have
hit any member added after a restore, not only a rebuilt one.

### The suite on the final build

The fault suite a fourth time, on the build with every fix above, on the cluster the rebuild had
just been proved on (`target/lab/suite-r168.log`):

| Fault | Acknowledged inserts | Lost | Seconds at zero |
| --- | --- | --- | --- |
| Kill the node leading the most groups (hyperion) | 558,988 | 0 | 0 |
| Partition hyperion by dropped packets, 20 s | 486,983 | 0 | 3 |
| Pause the control leader (hyperion), 20 s | 506,380 | 0 | 2 |
| Kill every node at once | 398,645 | 0 | 13 |

Every acknowledged insert was read back through every member, kill-all included. Each node was
active afterwards, restarted only by its faults, and none held an empty archive after the kill-all's
`SIGKILL`s. The seconds at zero are section 6's: a silent partition's and a pause's first seconds,
and kill-all's elections.

## 8. An unplaced member coordinates

The item section 7 left open: [#169](../appendix/resolved/unplaced-member-forwards.md), a member
the placement does not name refusing every query `NotInitialized`. It cost 63,012 refusals in two
seconds as a rebuilt hyperion started. It was fixed in the tree with a fixture test first, then
proved here with the same scenario. Repeating it eight times found eight more defects,
[#170](../appendix/resolved/uncached-log-reads.md) to
[#177](../appendix/resolved/blocked-plan-retry.md) (#176 was filed here and fixed in [section 9](#9-a-voter-whose-log-has-a-hole)), and one optimization,
[O74](../appendix/optimizations.md#o74-a-zen1-nodes-compactor-falls-hundreds-of-jobs-behind-under-the-bench).
Runs 1, 2, 5 and 8 were on a cluster destroyed, bootstrapped on the build under test and loaded from
the csv minutes before, so every group still held the load in its log; runs 3, 4, 6 and 7 followed
on the cluster the run before left. The records are under `target/lab/r169/`.

### Rebuilding a node under load, again

**How.** `target/lab/rebuild-exp.sh`, as in section 7: the mixed bench (`get:40,update:45,insert:15`)
through all three members for 900 s, with `cluster rebuild hyperion --yes` 25 s in. Afterwards every
acknowledged insert is read back through each member alone at `One`, and the csv is verified through
each member alone (`target/lab/r169/verify-all.sh`).

| | Run 1, #169 fixed | Run 2, #170 to #172 fixed too |
| --- | --- | --- |
| Wire version | 4, the bootstrap's (section 7 activated 6) | 6 |
| Hyperion rejoined as a new identity after | 27 s | 22 s |
| `NotInitialized` over 900 s | **0** | **0** |
| Other failures | 1,302 `NotLeader`/`Unavailable` in the stop's first 5 s, 768 dropped streams as hyperion went down | 1,236 in the stop's 4 s, one set's activation (102 `NotLeader`), 768 dropped streams |
| Plan | `Completed`, 17 moved and **1 failed**, never retried | `Completed`, 18 moved; one failed and its retry moved it |
| Whole rebuild | 1,319 s | 1,275 s |
| Acknowledged inserts, each member alone at `One` | 4,265,222, 0 lost | 4,413,157, 0 lost |
| csv, each member alone at `One` | 0 missing, 0 different, every keyword partition equal | the same |
| Hyperion's journal | 60,478 refused TLS handshakes in 12 minutes | see run 3 |

**#169 is fixed:** not one refusal, where section 7 counted 63,012 in two seconds. The pipelined
clients reconnect to hyperion's new process as soon as it listens, as before. They are now served
through it, forwarded to the placed members until the first move names it.

**What run 1 found.**

- **A step that never caught up.** The sixth set, a `Movie` group titan led, sat in the move for the
  whole 600 s window and failed, with no snapshot cut for eight minutes. It was first put down to
  the leader reading the copy's log one entry per I/O, which it did, 10 entries a second in an
  experiment: [#170](../appendix/resolved/uncached-log-reads.md), fixed (1,221 a second). But that
  was not this failure. Run 7 measured it: the snapshot cut waited behind the compactor's backlog
  ([#174](../appendix/resolved/snapshot-cut-queue.md), below).
- **The failed step was published anyway.** The set's other group had activated, and a failed
  group counted as activated, so the set was routed to hyperion while the `Movie` group's voters
  still named the removed identity. The plan therefore never retried it, and `cluster rebuild` said
  it had rebuilt the node. No data was lost, because the new copy was a learner that held every
  row. But `cluster status` said "3 of 3 copies" for a group one failure from stopping. That is
  [#171](../appendix/resolved/failed-group-publishes-its-set.md): a set with a failed group is no
  longer published, its record ends, and the plan replans it.
- **A redial storm.** Every member dialled the old identity at hyperion's address every 100 ms, and
  each attempt was a TLS handshake that could only fail: 60,478 of them on a four-core host. That is
  [#172](../appendix/resolved/identity-refusal-redials.md): a dial refused on the peer's identity now
  waits out its backoff.

**What run 2 showed.** #171 works as designed. The step that failed was replanned, the retry was fed
a snapshot at once, and it caught up in 37 s. But the step still failed, and it failed in the
**`learner`** phase: in 600 s the new copy never acknowledged a single append. That is not a slow
read. A stall with nothing logged at `info` needs openraft's own tracing, which run 3 has.

| Catch-up of each group, run 2 | Groups | Time |
| --- | --- | --- |
| Keyword groups, fed the log | 17 | 1 to 8 s |
| `Movie` groups led by europa, fed a snapshot | 11 | 2 to 63 s |
| `Movie` groups led by titan, fed a snapshot | 6 | 7 s, 10 s, 22 s, **109 s**, **184 s**, and one that failed at 600 s before its retry took 37 s |

Every slow step was titan's, and each spent its time *before* the cut began. Once a cut started, it
took about 10 s to cut, send and install. That looked like openraft deciding late to ask for a
snapshot. It was not: runs 6 and 7 showed that openraft asked at once, and the cut waited
(below).

**Runs 3 and 4: tracing the stall, and what the tracing did.** Openraft logs nothing at `info` about
a replication stream that makes no progress. So run 3 set `RUST_LOG` to debug for
`openraft::replication` and `openraft::progress` on the two leaders. Run 3 lost the failing step's
trace: journald suppressed about 1,800 lines a second. Run 4 lifted the unit's rate limit, and the
nodes then wrote about **150,000 lines a second** each. That slowed the moves being traced to a crawl,
and rsyslog copied it all to `/var/log/syslog`: 67 GB on titan and 190 GB on europa in about 25
minutes. Titan's storage is a directory on its root device, so the node failed its next start on
`ENOSPC` and crash-looped until the log was cut. Nothing was lost, and titan came back on its own once
space returned ([#156](../appendix/resolved/wal-failure-stops-the-node.md)'s fix at work). But this is
not a way to observe a lab under load. The move driver now reports a stalled destination itself: the
appends sent to it, accepted, conflicting and failed, and the last failure, every 30 s at `warn`.

Run 3 otherwise matched run 2: one step failed in 600 s and its retry moved it. 3,400,781 acknowledged
inserts were found through each member alone, and every csv row matched.

**What the restarts found: an idle cluster that never elected.** Run 4's follower exited when europa
was restarted under it. The plan ran on regardless, and `cluster admin "status <plan>"` shows it.
The follower now waits out five minutes of unreadable records instead. With titan crash-looping and
the bench stopped, the plan then stalled at 6 of 7. `cluster stats` showed each member leading 4 of
its groups: **24 of 36 groups had no leader** for over twenty minutes, with two of their three
voters up. A pre-vote over a link that was down was refused without sending anything, and a link
dials only for a frame, so on an idle cluster nothing ever brought europa's links to titan back up:
[#173](../appendix/resolved/idle-pre-vote-links.md). Installing the fixed build on europa alone took
the cluster from 12 groups led to 33 within 25 s, and the stuck move started. The last 3 elected when
titan had it too, because in those groups titan's copy was the one that had to stand.

That window also showed that **a rebuild costs the control group one failure of margin**. The old
identity stays a control voter until the removal commits, so while titan was down only europa of the
three voters was up, and the control group had no leader (`leader none`) until titan returned.

**Runs 5 to 7: counting what a stalled move sends.** The move driver now logs, every 30 s while a
destination stands still, what its appends came to. Run 6 made the stall plain:

```text
a move's destination has made no progress  stalled_secs=270  sent=14788 data=1 accepted=14787
  conflicts=1 failed=0 last_prev=Some(1092395) last_acked=None
```

One append carrying entries was ever sent: openraft's first probe, at the leader's purge point, which
conflicted as a probe to an empty copy does. Every other append was an empty heartbeat. The next step
had to be a snapshot. Run 7 taught the compactor to log how long a cut waited:

```text
taking a snapshot cut  table="Movie" group=84dfae50a3fe833b queued_ms=566758 backlog=180
```

The cut waited 9.4 minutes behind 180 queued merges on titan's `Movie` compactor, for a cut that then
took about 10 s: [#174](../appendix/resolved/snapshot-cut-queue.md). A cut is now taken ahead of
queued merges. That is safe, because the compactor is the archives' only writer and a cut between
any two jobs is consistent. The backlog is its own finding: under the bench a Zen1 node's compactor
falls hundreds of jobs behind, filed as
[O74](../appendix/optimizations.md#o74-a-zen1-nodes-compactor-falls-hundreds-of-jobs-behind-under-the-bench).

Run 7, on the build before that fix, then found two more:

- **A crash loop the #171 fix opened.** Tablet 1's move failed, and with #171 the record now ends,
  so hyperion stopped its learner copies. The retry named them again, and the keyword copy's WAL
  index pointed into a segment hyperion had reclaimed meanwhile, because a group the shard no longer
  hosts counts as purged. The shard died, the process aborted, and hyperion crash-looped 147 times:
  [#175](../appendix/resolved/stopped-group-log.md). A group stopped because the map no longer names
  it now has its log forgotten once its handle is down, and a learner copy that cannot be built is
  built again empty. A voter in the same state would still stop its node:
  [#176](../appendix/resolved/unreadable-voter-log.md), filed then and fixed in
  [section 9](#9-a-voter-whose-log-has-a-hole).
- **A blocked plan nobody could retry.** The set failed twice and the plan was blocked by name.
  Asking for the removal again was accepted and changed nothing, so one set stayed short of its
  down voter: [#177](../appendix/resolved/blocked-plan-retry.md). Asking again now forgives the plan's
  failures so far. On the lab, with the fixes installed by hand (`cluster upgrade` rightly refuses
  while a set is under the factor), `remove` asked again ran the plan to `Completed`, 18 moved, and
  every acknowledged insert (3,612,822) and csv row read back through each member alone.

| Run | Build | Stalled moves | Failed steps | Outcome | Acknowledged inserts, each member alone |
| --- | --- | --- | --- | --- | --- |
| 1 | #169 | 1 | 1, published anyway (#171) | "rebuilt" with a set wrongly published | 4,265,222, 0 lost |
| 2 | + #170 to #172 | 1 | 1, retried and moved | 18 moved | 4,413,157, 0 lost |
| 3 | + debug tracing | 1 | 1, retried and moved | 18 moved | 3,400,781, 0 lost |
| 4 | + debug tracing, rate limit lifted | – | – | abandoned; see above | – |
| 5 | + #173, the stall report | 1 (5.5 min) | 0 | 18 moved | 4,234,475, 0 lost |
| 6 | + append counts | 3 | 0 | 18 moved | 4,216,793, 0 lost |
| 7 | + the cut's wait logged | 1 (9.4 min) | 2, plan blocked (#177), then the #175 crash loop | retried by hand after #174, #175 and #177: 18 moved | 3,612,822, 0 lost |
| 8 | + #174, #175, #177, fresh cluster | 3, the longest a 147 s wait for a cut | 0 | 18 moved, 979 s | 4,258,483, 0 lost |
| 9 | + archive passes coalesced (O74), grown cluster | 6 reports, the longest a 39.5 s wait for a cut | 0 | 18 moved, **748 s** | 3,994,578, 0 lost |

**Runs 8 and 9: the fixed build.** Run 8, on a freshly loaded cluster, moved every set with no failed
step and no refusal. Its longest stall was a cut waiting 147 s behind the job running when it was
asked for. That backlog was mostly archive passes, each redundant with the next, so run 9 skips a
pass that another queued pass covers. It also logs any job that holds the compactor over 5 s. Run 9,
on the cluster run 8 left, rebuilt hyperion in 748 s, the fastest of the nine, with the longest cut
wait at 39.5 s. What still holds a cut is where the compactor's time goes on a Zen1 node:
682 segment merges ran over five seconds in that run, and one archive pass ran three minutes.
That is [O74](../appendix/optimizations.md#o74-a-zen1-nodes-compactor-falls-hundreds-of-jobs-behind-under-the-bench),
left open with those figures and applied in [section 10](#10-the-compactors-backlog-and-four-rebuilds).

Across every run the bench saw no `NotInitialized`. Its other failures were the same in each: about
1,200 to 1,600 `NotLeader` and `Unavailable` in the four or five seconds hyperion's stop handed its
leads off, and 768 streams dropped as it went down. The loader's own read-back after run 8's load
once found a movie missing that every member alone held a moment later. It was reading at `One`, and
the copy that answered had not applied the load's last writes, so it now reads back at `Quorum`.

**Verdict: pass.** A rebuilt node serves from the moment it listens (#169), and the moves that refill
it no longer stall silently, fail, get published half done, block for good or crash their
destination (#170, #171, #173 to #175, #177).

### A member added with no rebalance

**How.** `target/lab/tmdb-add.yaml` is `tmdb_cluster.yaml` at a factor of two, bootstrapped on
europa and titan. The csv was loaded, then `cluster add -i target/lab/tmdb-add.yaml hyperion`, with no
`--rebalance`, left hyperion a voter of the control group and in no placement slot: no groups, no
bytes. The bench then ran through hyperion **alone** for 180 s (`bench --inventory … --addr
172.16.2.5:12000`, which connects to one member as the admin), with all four operations. Every
acknowledged insert was read back through each member alone at `One`, then the cluster was
rebalanced onto hyperion and the read-back repeated.

| | |
| --- | --- |
| Hyperion before the bench | joined, a voter, 0 groups, 0 B |
| Bench through hyperion alone, 181 s | get 9,148/s, keyword 3,049/s, update 13,725/s, insert 4,574/s |
| Failures | **none**, of any kind |
| Acknowledged inserts, each member alone at `One` | 827,821, 0 lost |
| After `cluster rebalance` | hyperion holds 12 groups and 535 MiB; 827,821, 0 lost |

Before #169, every one of those queries was refused. Get latency through the unplaced member is 2 ms
at the median, since every share is a forward: about four times a placed member's. The rebalance
moved six sets at a little over five minutes each. That is `retire_after`, five minutes by default,
which a move off a live source waits out before its old copy goes; the rebuilds never waited because
their source was down. The TMDB inventories do not set it, and `lab.yml` sets 15 s.
`cluster rebalance` stopped following at its 30 minute limit with the plan at five of six, and the
plan finished on its own.

**Verdict: pass.**

## 9. A voter whose log has a hole

The item section 8 left filed: [#176](../appendix/resolved/unreadable-voter-log.md). A durable voter
whose WAL could not be read stopped its node at every start. #175 had built a *learner* again with no
log in that case, and a voter was left alone because an emptied voter can elect a leader that is
missing what it acknowledged.

**Reproducing it found a worse form.** In the fixture, a voter's middle segment was deleted while
its checkpoint was held behind its sealed segments, as a Zen1 node's backlogged compactor leaves it.
The node did not stop. It came back with 94 of 110 notes, at the same applied index as its peers,
and served "not found" for the other sixteen. A read of the log skipped the indexes it did not
have, and openraft's re-apply checks only the ends of each chunk it reads. So a hole inside a chunk
was applied around, in silence. A hole that covers a chunk's end gives the lab's error from #175
(`Failed to get log entries … got [None, None)`). Both are fixed:

- a read across a gap is refused;
- a hole wholly at or below the checkpoint is purged;
- a hole past the checkpoint forgets the copy's log under a **floor** on its vote. The floor is
  written, synced, before the log goes, and the copy's vote is kept. Until the copy has applied past
  the floor, it grants no vote to a candidate behind it.

The fixture test is `a_voter_whose_log_has_a_hole_is_fed_not_fatal`, with the new `HOLD_COMPACTION`
verb.

**How, on the lab.** `target/lab/r176/hole.sh`:

1. With the mixed bench (`get:40,update:45,insert:15`) running through all three members, or with
   nothing running, hyperion is killed with `SIGKILL` and stopped, so systemd does not start it.
2. One of its Shard-0 WAL segments is deleted.
3. Hyperion is started again.

Afterwards every acknowledged insert, and the csv, is read back through each member alone at `One`.
The build is `c3b8874`, which is also the build of section 10's last rebuild. The runs are under
`target/lab/r176/`.

| | Idle, third newest segment | Bench, third newest | Bench, newest sealed |
| --- | --- | --- | --- |
| What hyperion found at its start | holes in 3 `Movie` groups, each wholly below its checkpoint | the same | holes in 3 `Movie` groups, each about 6,300 entries **past** its checkpoint |
| What it did | purged each log through its checkpoint; logs and votes kept | the same | forgot each log under a floor, keeping its vote. One group was led by hyperion when it was killed, and kept its own vote |
| Fed past the floor after | – | – | 4 s, 4 s and 16 s |
| Restarts by systemd | 0 | 0 | 0 |
| Acknowledged inserts, each member alone | no bench | 1,517,309, 0 lost | 1,379,242, 0 lost |
| csv, each member alone | 0 missing, 0 different, every keyword partition equal | the same | the same |

```text
ERROR a durable log has a hole past its checkpoint; forgetting it, and its leader will feed it again
      group=4a08067dafc8c382 table=Movie from=1341493 to=1347782 checkpoint=1341492
WARN  a copy's log is forgotten; it grants no vote below its floor until it is fed past it
      group=4a08067dafc8c382 floor=T1-…/0.1348627 vote=Some(Vote { leader_id: LeaderId { term: 1, … }, committed: true })
INFO  a copy was fed past its floor, and votes as any copy again  group=4a08067dafc8c382
```

With the compactor keeping up since section 10, the third newest segment was already merged even
under the bench. So only the newest sealed segment lay past the checkpoints, and that segment is
the one the third run deleted. On the build before this fix, a node in that state either stopped at
every start or, with the hole inside a chunk, served with rows missing.

**Verdict: pass.** A voter whose log lost a segment comes back by itself. It loses nothing it
acknowledged, and it cannot vote for a leader missing any of it.

## 10. The compactor's backlog, and four rebuilds

[O74](../appendix/optimizations.md#o74-a-zen1-nodes-compactor-falls-hundreds-of-jobs-behind-under-the-bench)
was the other item section 8 left open. On titan, under the bench, the compactor fell hundreds of
jobs behind. It is a performance item, and [its page](performance.md#o74-the-compactor-under-the-bench)
has the numbers. The runs are here because every change to the compactor changes what the archives
hold, and each rebuild was checked the way section 8's were.

**How.** On a cluster destroyed, bootstrapped on the build under test and loaded from the csv,
`target/lab/rebuild-exp.sh` runs the mixed bench through all three members for 900 s and rebuilds
hyperion 25 s in. The runs are under `target/lab/o74/`.

| Run | Build | Plan | Whole rebuild | Acknowledged inserts, each member alone | csv, each member alone |
| --- | --- | --- | --- | --- | --- |
| base | `fc91fb7`, the compactor's phases logged | 18 moved | 430 s | not read back | not read back |
| c1 | + 32 reads in flight for a merge and a pass | 18 moved | 315 s | 3,926,919, 0 lost | 0 missing, 0 different |
| c2 | + passes paced a minute apart, 8 reads in flight, frames read as one span | 18 moved | 534 s | not read back | not read back |
| c3 | + a pass stops inside an archive at 16 MiB, 16 reads in flight | 18 moved | **272 s** | 3,950,753, 0 lost | 0 missing, 0 different |

c3 is the fastest rebuild on record: section 8's rebuilds 8 and 9 took 979 s and 748 s. Its longest
wait for a snapshot cut was 0.7 s, against 147.5 s in rebuild 8. What the change costs is the work of
reclaiming space. With the backlog gone, the archive passes run at the rate the bench makes
garbage, where before they were skipped behind it. That costs about 6% of throughput in steady state.

**Verdict: pass.** No run lost an acknowledged write or a row.

## 11. Overload, silence and a nearly full disk

Three defects were left open by the sections above: overload answered `OutcomeUnknown` rather than
`Shedding` ([#129](../appendix/resolved/overload-sheds.md)), the first seconds of a silent partition
still stopped every pipelined client
([#143](../appendix/resolved/silent-partition-hops.md#the-first-seconds-closed)), and a node whose
disk was nearly full could only stop ([#156](../appendix/resolved/wal-failure-stops-the-node.md#the-second-part-an-append-reserve)).
All three were fixed and each fix measured here, on builds `5efa0b9` through `d276baa`, with
`35d47a6` as the base. The runs are under `target/lab/r11/`.

**Performance first.** Every change was measured against the base, and one version of each fix was
thrown out for costing throughput:

- The gate's first cut, starting at 64 writes in flight and halving on a commit slower than half
  its budget, cost about a fifth of the overloaded load's throughput, and shed a hot keyword group
  at the loader's *default* gate, which the base took with no retry.
- #143's first cut judged a leader quiet by openraft's `last_quorum_acked`. Under saturation that
  lags for seconds on healthy groups, and the first cut refused and abandoned their writes.

### The loader past what the cluster commits

The loader at its original gate, 8 workers × 4,096 in flight, with retries unbounded so that every
arm finishes if it can, each on a destroyed and freshly bootstrapped cluster
(`target/lab/r11/fresh-sweep.sh`):

| Build | Loads | Finished | Rows a second | Retried unknown | Retried shed |
| --- | --- | --- | --- | --- | --- |
| `35d47a6`, the base | 3 | 1; 2 died on `IdentityExpired` | 29,476 | 207,029 | 0 |
| the gate switched off | 3 | 2; 1 died on `IdentityExpired` | 10,156, 6,659 | 1.3M, 2.0M | 0 |
| the gate as shipped (`d276baa`) | 2 | 2 | 28,916, 32,593 | 4,031, 2,543 | 872k, 589k |

A shed write was never applied, so a client can retry it at once with no identity to keep. The
unknown outcomes that remain are writes admitted before a group's bound came down.

The loads that died met a defect of their own, filed as
[#180](../appendix/known-issues.md#180-a-first-write-queued-past-a-groups-identity-memory-is-refused-identityexpired).
A group remembers 4,096 write identities, about four seconds of this load. A first attempt that
waited in the server's queues longer than that was refused as though it were a retry of a write
the group had forgotten. None of the gated loads met it.

**At a normal load nothing moved.** The loader at its default gate on a fresh cluster: 28,730 and
29,226 rows/s on the base, 29,010 on `5efa0b9` and 30,350 on `d276baa`, with no retry. On
`c9d6a61`, whose gate started at 64, the first load shed one hot keyword group until a row ran out
of its eight retries, and the load after it ran at 31,432 with 6,399 writes shed. That is the cut
this section threw out. Every run was read back
whole with `verify` (0 missing, 0 different). Every acknowledged insert of the bench that followed
was read back through each member alone: 0 lost in every run. The bench's own comparison is on
[Performance](performance.md#the-admission-gate-and-the-bench).

### A silent partition's first seconds

The partition test of [section 4](#partition-one-node) again: hyperion's peer ports dropped both
ways for 20 s under the mixed bench.

| Second | t05f, before | After (`c9d6a61`) |
| --- | --- | --- |
| cut −1 | 44,300 | 72,067 |
| cut | 10,784 | 25,961 |
| +1 | 372 | 22,447 |
| +2 | 0 | 121,792, hyperion's groups refused |
| +3 | 0 | 122,195 |
| +4 | 25,835, hyperion's groups refused | 121,276 |

All 620,159 acknowledged inserts were read back through each member, and no node restarted.

**Verdict: pass.** No second of the partition runs at zero.

### A nearly full disk

[Fill a node's disk](#fill-a-nodes-disk) again: hyperion on a 2 GiB loop filesystem, the whole csv
loaded (`target/lab/r11/diskfill.sh`).

| | The base, section 4 | Fixed (`d276baa`) |
| --- | --- | --- |
| At about 1.35 GB used | the disk filled 70 s in and hyperion stopped, then failed every start until the disk grew | all six shards went under the 512 MiB reserve together and handed on 12 leads in 0.6 s |
| For the rest of the load | down, restarting | up, no restart, 478 MB free for ten minutes |
| The load | stopped at writes through hyperion (first run), or went on without it | finished: 2,193,788 rows at 14,023 a second, retrying 122 unknown and 327 `NotLeader` |
| Reading hyperion's copy alone at `One` | nothing to read | served: 666,405 of 1,187,691 movies missing, **0 different**, a consistent prefix |
| Once the disk grew | the next scheduled restart | all six shards back over the reserve at the next check; 120 s later every movie through hyperion alone equalled the csv |

The run found one more wait. The loader's sample read-back carries the session tokens of its own
writes, and on hyperion's frozen copy each such read waited out its deadline for an apply that
would not come, and was retried there without end. A read that needs an index a copy under the
reserve has not applied is now refused `Unavailable` at once.

**Verdict: pass.** A nearly full node leads nothing, stops filling, serves what it holds and
recovers on its own.

### The suite

All 133 cluster fixture tests passed at six threads on `c9d6a61`, two ignored as before, and the 331
`shoal-core` unit tests. The three fixes' own tests are
`an_overloaded_group_sheds_rather_than_timing_out`, `a_silent_partitions_first_seconds_hold_no_writes`
and `a_node_under_the_append_reserve_leads_nothing_and_serves`. Each fails without its fix: 5,300
writes unknown with the gate out of reach, a write through the cut-off node answered in 5.0 s, and
"node one still leads 4 groups under the append reserve".

## 12. Scenarios nobody had run

[What is left](todo.md) listed faults no section had tried: a network that is slow or lossy rather
than cut, partitions in one direction or of the control port alone, losing a quorum, a slow disk,
clock skew, a real power cut, and a partition long enough to outlast the kernel's patience. All of
them ran on the build at `e34fd0f`, and the long partition again on `80b44ec`. The scripts are
under `target/lab/r11/`, the runs under `target/lab/r11/sc/`.

**How.** `fault.sh` as in [section 4](#4-faults-under-load): the mixed bench for 60 s (get 55,
keyword 15, update 15, insert 15), the fault at 15 s and healed 20 s later. Every acknowledged
insert was then read back through each member alone. The table gives medians of each second's
operations, errors and write p99 over the 10 s before the fault, the middle of it (18–34 s), and
40–55 s, after the heal. The faults are on hyperion unless the row says otherwise.

| Scenario | How | Before | During | After | Acknowledged inserts, each member alone |
| --- | --- | --- | --- | --- | --- |
| Delay 20 ms | `tc netem` on hyperion's egress to the peer ports (`netem.sh`) | 81k, p99 194 ms | 65k, p99 158 ms | 71k | 644,291, 0 lost |
| Delay 100 ms | the same | 69k | **22k**, p99 504 ms | 71k | 478,046, 0 lost |
| Loss 1% | the same | 76k | 72k | 65k | 623,339, 0 lost |
| Loss 5% | the same | 76k | **41k**, p99 586 ms | 69k | 540,579, 0 lost |
| One way, in | `iptables` drops what the peers send hyperion (`oneway.sh in`) | 75k | 110k, 22.7k/s refused `NotLeader` | 74k | 567,559, 0 lost |
| One way, out | drops what hyperion sends its peers | 77k | 114k, 22.3k/s refused | 65k | 565,859, 0 lost |
| Control port only | both ways, port 12002 alone | 69k | 77k | 62k | 630,033, 0 lost |
| Two of three down | titan and hyperion `SIGKILL`ed and held stopped | 69k | 113k reads, **every write refused** `NotLeader` (48k/s) | 71k, refusals tapering to none by 20 s after | 303,073, 0 lost |
| Clock +30 s | titan's clock stepped ahead with NTP off | 74k | 71k | 64k | 608,003, 0 lost |
| Clock −30 s | stepped back | 72k | 77k | 61k | 621,552, 0 lost |
| Slow disk, 10 ms | hyperion's storage on `dm-delay`, every read and write delayed (`slowdisk.sh`) | 72k | 55k, p99 210 ms | 53k | 517,372, 0 lost |
| Slow disk, 50 ms | the same | 72k | 64k, **p99 1,022 ms** | 66k | 592,599, 0 lost |

No node restarted in any run, and nothing acknowledged was lost.

**What the table shows.**

- **A slow link to one node cost the whole cluster.** At 100 ms, a third of the groups were led
  across the slow link, and every pipelined client filled its window with their writes, so the
  cluster ran at a third of its rate. Nothing moved leadership off a member whose links were slow:
  leads are balanced by count ([O63](../appendix/optimizations.md#o63-leadership-never-returns-to-a-groups-placement-primary)).
  Fixed as [#182](../appendix/resolved/slow-link-leadership.md): a node whose round trip to every
  peer is far above its baseline judges its own links slow and hands its leads on. Rerun, it did
  so a second into the delay, and the cluster served 56,000–70,000 operations a second through it.
- **One-way partitions and a control-port cut behave well.** Either direction breaks TCP, so both
  are a partition as far as a connection goes, and #143's refusals apply. They start after the
  hop silence, and throughput rises while hyperion's groups are refused. The control port alone
  changed nothing the bench could see: the data plane does not need it second to second.
- **Losing a quorum is loud and definite.** Every write was refused `NotLeader` at once, with
  753 and 174 unknown outcomes in the two seconds of the kill and none after. Reads at `One`
  through europa went on. The cluster recovered with nobody's help once the two nodes started.
- **Clock steps change nothing.** Leases, the detector and every deadline run on monotonic
  clocks. Only identities are minted from the wall clock (by the client), and the retry window is
  five minutes, far wider than the step.
- **A slow disk caps writes through its node at a second.** At 50 ms, the write p99 was 1,022 ms
  for the whole fault. A write coordinated through hyperion commits on the other two, then waits
  for hyperion's own copy to apply it, bounded at two heartbeats
  ([#146](../appendix/resolved/apply-wait-on-a-lagging-copy.md)). Applied as
  [O76](../appendix/optimizations.md#o76-a-write-through-a-lagging-copy-waits-its-whole-apply-bound):
  a copy whose last wait ran out waits one poll. Rerun at 50 ms, the p99 was a second for the
  fault's first two seconds and 107–253 ms for the rest.

### A longer partition

The partition of [section 4](#partition-one-node) held for 60 s instead of 20
(`target/lab/r11/fault2.sh`). The cluster did not recover when it healed. Refusals went on at
19,000 a second for the rest of the run, 45 s. hyperion's journal logged its last failed RPC 48 s
after the heal. The 20 s partition had taken about 6.5 s to recover, and nobody had asked why.

It was TCP. A connection whose packets are dropped stays open, its retransmission timer doubling
each try, and after a heal nothing moves until the timer next fires: tens of seconds after a
minute of backoff. Fixed as [#181](../appendix/resolved/partition-retransmit-backoff.md): every
peer connection sets `TCP_USER_TIMEOUT` (`transport.unacked_timeout`, 5 s), so the kernel aborts
it and the link dials again. On `80b44ec`, the same 60 s partition:

| Seconds after the heal | Before #181 | After |
| --- | --- | --- |
| 1 | 17,690 refused | 5,258 refused |
| 2 | 19,257 refused | none |
| 44 | 19,044 refused, the run ending | none |

1,003,763 acknowledged inserts were read back through each member, 0 lost.

### A real power cut

`echo b > /proc/sysrq-trigger` reboots a host immediately, without syncing: the page cache is lost,
as it is in a power cut, and only what the processes had synced and the device had written
survives. Each was 15 s into a 180 s bench (`target/lab/r11/powercut.sh`). The node's unit is
enabled at boot and started itself when the host came back.

| Host | Back | Acknowledged inserts, each member alone | `verify`, every movie and keyword partition |
| --- | --- | --- | --- |
| titan | host up in about 20 s, node started 2 min later | 1,205,717, 0 lost | 0 missing, 0 different |
| hyperion | the same | 1,155,375, 0 lost | 0 missing, 0 different |

After the cut, titan's groups were refused for about 15 s, until they elected elsewhere (the lease
and an election, [C7](../distributed/failover.md#the-window-and-what-a-client-sees)), and nothing
failed after that. The two minutes between host and node were `systemd-networkd-wait-online`,
host configuration. The device's write cache was not lost: the power stayed on. A cut of the power
itself remains to be done by hand.

**Verdict: pass**, with three findings, all fixed: #181, #182 and O76.

## 13. Round 12

Round 11 left five items on [what is left](todo.md): #180, the first second and a half of a
silent partition (#143), #132 and #152, which had not recurred, O64's two modes, and weighted
leadership. This round works through them on builds from `fe7d106` on. The runs are under
`target/lab/r12/`.

### A compactor job lost to a timer

Reading the compactor for #152 found the cause without a lab run. Its wait for the next job
raced a bare kanal receive against the earliest retry's timer, and kanal drops a value it has
already handed to a waiting receive when that receive is dropped. The same race was in fourteen
other places, the client's stream deadline among them. All fifteen now race a receive kept by
its receiver, and a test fails on any bare receive raced in the workspace
([Resolved #152](../appendix/resolved/kanal-receive-races.md)). The test that found it passed 36
runs of 36 on the unfixed tree, which is the rate the item had, not evidence either way. The
evidence is the reproduction of kanal's loss itself.

### A first write queued past the memory

[#180](../appendix/resolved/first-write-past-identity-memory.md) had stopped round 11's loader in
three of six runs: a first write that waited in the server while its group forgot 4,096 later
identities was refused `IdentityExpired`, which nothing retries. It is fixed. Such a write that
reached the server within five seconds of its mint is refused `Shedding`, and the loader and
`exec_with` send it again under a new identity. The fixture reproduces the refusal exactly and
passes on the fix.

The lab could not make it happen on this tree. Two builds of the node from one tree, the fix
switched off in one, both with the admission gate switched off by a lab-only variable
(`target/lab/r12/180/sweep.sh`), loaded whole at 8 × 4,096 in flight on fresh clusters:

| Run | Build | Rows a second | Retried, all unknown outcomes | `IdentityExpired` |
| --- | --- | --- | --- | --- |
| 1 | without the fix | 6,170 | 2,225,257 | 0 |
| 2 | with it | 9,895 | 1,293,249 | 0 |
| 3 | with it | 6,325 | — | 0 |
| 4 | without the fix | 4,245 | — | 0 |

Without the gate, every write piles into openraft and waits out `write_timeout` as an unknown
outcome, not an expiry: the queue that ages a write past the memory is the one in front of
admission, and here the whole load drained through the one behind it. Round 11's gate-off runs
were a different switch on an older tree. The sweep was stopped after four runs, since it could
not tell the builds apart. It found one thing: [O77](../appendix/optimizations.md#o77-an-abandoned-proposal-logs-a-warning-when-it-applies),
openraft's warning for every abandoned proposal, about a thousand lines a second on titan.

### A silent partition's first second

Round 11 left the first second and a half of a silent partition, while hops already sent waited
for the silence to be judged ([#143](../appendix/resolved/silent-partition-hops.md#the-first-second-and-a-half-closed-on-the-kernels-word)).
The replication lane now also asks the kernel. A peer that is only slow still acknowledges every
segment, and one that is cut off leaves the sender's retransmission timer backing off. The
partition test again, hyperion's peer ports dropped both ways for 20 s under the mixed bench, as
a share of the second before the cut:

| Second | Round 11 | Kernel verdict on one backoff | On two |
| --- | --- | --- | --- |
| cut | 36% | 68% | 76% |
| +1 | 31% | 130%, hyperion's groups refused | 44% |
| +2 | 169% | 145% | 191% |

The one-backoff build failed the scenarios that must not trip it. Under 5% loss on hyperion's peer
traffic it refused 125 to 323 writes in about half the seconds, where round 11 refused none; 100 ms
of delay and 1% loss were clean. The two-backoff build refused 80 writes in 20 seconds of 5% loss.
Every acknowledged insert was read back through every member after each run (629,239, 489,628
and 690,658), and no node restarted.

The one-way cuts were rerun on the one-backoff build: the second of the cut ran at 38% and 51% of
the second before, where round 11's ran at 19% and 22%.

**Verdict: pass.** No second of the partition runs below 44%, and nothing slow or lossy is taken
for a cut.

## 14. Round 13

Round 12 left [what is left](todo.md) with #142's deadlines and its restore stall, O64's spread,
two limitations that were never filed (a slow disk's cost and a get through an unplaced member),
and four scenarios nobody had run. This round works through them on builds from `1578fe7` on,
which added each member's WAL sync time, appends per sync and sync sizes to `cluster stats`
([overview](overview.md#reading-a-nodes-figures)). The runs are under `target/lab/r13/`.

### The restore stall, read from the source

[#142](../appendix/known-issues.md#142-two-fixture-tests-fail-intermittently-on-an-idle-host)'s
restore stall left one group at `Pending`, with no driver and no attempts, for five minutes.
Round 12 named two ways that could happen. Reading the driver found a third, and it is the one the
record fits. A driver whose progress commit did not land failed the group, and then reported the
group `Done` to its shard whether or not the `Failed` commit landed either. The shard never drives
a group it believes done. Fixed as [#183](../appendix/resolved/restore-driver-uncommitted-done.md):
a driver reports only a phase the control plane took, and a commit that did not land is a
hand-back.

The stall itself was not caught. `target/lab/r13/142/loop.sh` ran the restore test beside five
other heavy fixture tests at six threads, with child logs on every child, on the unfixed tree. It
passed in all six rounds, while the other five failed 16 times between them. Those are #142's
deadlines: `did not commit the write within the deadline`, `elected no leader within the
deadline`, and, twice, the lost-response test's `checkpoint never reached 5`.

### A slow disk, again

Round 11 measured a 10 ms delay on every request to hyperion's storage costing a quarter of the
cluster's rate. It left the cause unfiled and suggested moving leads off the slow node. Before
building that, this round measured whether it would help, and what the slow disk costs now
(`target/lab/r13/slow/ab.sh`). Hyperion's storage was the `dm-delay` device over a loop file, the
csv was loaded, and each arm rolled its lead weights onto the running cluster, waited 90 s, set
the delay, and ran the mixed bench for 60 s. The `away` arm weighs europa 20, titan 10 and
hyperion 1, which left hyperion leading 2 of the 36 groups instead of 7.

| Arm | Delay | Operations a second | p99 get | p99 update | Hyperion's WAL: syncs/s, ms a sync |
| --- | --- | --- | --- | --- | --- |
| even | 10 ms | 78,197 | 18.9 ms | 199.0 ms | 62, 85.5 |
| away | 10 ms | 67,527 | 14.0 ms | 213.1 ms | 68, 84.9 |
| away | 10 ms | 75,225 | 13.0 ms | 204.6 ms | 46, 188.4 |
| even | 10 ms | 70,043 | 14.4 ms | 204.1 ms | 70, 84.2 |
| even | none | 78,407 | 27.1 ms | 166.3 ms | 282, 18.9 |
| even | none | 73,233 | 25.8 ms | 178.8 ms | 222, 23.6 |
| even | 50 ms | 73,959 | 18.6 ms | 170.0 ms | 20, 314.9 |

The loop file alone makes hyperion's syncs take about 20 ms, three times titan's 7 ms. The 10 ms
delay makes them 85 ms. That is four times slower again, and it cost about 2%: 74,100 operations a
second on average against 75,800 with no delay. At 50 ms hyperion fell 341,381 entries behind and
the other two members carried the writes, still at 74,000 a second. Moving the leads away gained
nothing (71,400 on average). The load it took off hyperion went to europa, which also runs the
clients.

Round 11's quarter predates [O76](../appendix/optimizations.md#o76-a-write-through-a-lagging-copy-waits-its-whole-apply-bound),
which stopped a write through a lagging copy from waiting out that copy's apply bound. With it, a
member whose disk is slow simply falls behind. A group commits on the other two, as a factor of
three allows, and the slow copy catches up afterwards.

**Verdict: the limitation is closed by the tree as it is, and a slow-disk lead handoff is not
built.** What a slow disk still costs is margin. While it lags, a failure of either fast member
leaves its groups committing at the slow disk's pace, and `cluster stats` shows the lag as `apply
lag`.

### Longer partitions, and a flapping one

[What is left](todo.md) listed partitions longer than a minute and links that flap, since #181 had
been found at 60 s. All three ran on `4955733`, with the lab's inventory: `target/lab/r11/fault2.sh`
for the two long cuts and `target/lab/r13/part/flap.sh` for the flapping one. Each cut hyperion's
peer ports both ways (`partition.sh`) under the mixed bench, and every acknowledged insert was read
back through each member alone afterwards.

| Scenario | During the cut | After the heal | Acknowledged inserts, each member alone | Restarts |
| --- | --- | --- | --- | --- |
| 120 s | about 13,000 refusals a second, `NotLeader` from hyperion to the third of the workers it serves | refusals end in 1 s; 25–55% of the rate for 14 s while hyperion installs 12 snapshots, then the rate before; 4,795 gets refused `Unavailable` meanwhile (#184) | 1,764,524, 0 lost | none |
| 300 s | about 17,000 refusals a second, the same | refusals end in 1 s; 30–60% of the rate for at least 40 s, the run's end, while hyperion installs 18 snapshots; 12,926 gets refused `Unavailable` meanwhile (#184) | 2,860,083, 0 lost | none |
| 10 × (10 s cut, 10 s healed) | the same as a single cut, every time | after every heal: one second at 30–70% of the rate with a write p99 near 1 s, then the rate before, with no refusals | 2,694,444, 0 lost | none |

No cycle of the flapping run recovered worse than the first. Link after link was cut and dialled
again, ten times in 200 s, and nothing accumulated: no backoff that grew, no lead that stuck, no
refusal after a heal. #181's `TCP_USER_TIMEOUT` is why a heal is a second here.

Both long cuts outlasted the peers' retained log for some groups: hyperion installed 12 snapshots
after the 120 s cut and 18 after the 300 s one, and the rest of its groups were fed entries. An
install restarts its group's copy, and a read through hyperion for that group's tablets is refused
until the install ends, although the two other members hold the tablet.
That was [#184](../appendix/resolved/installing-copy-reads-elsewhere.md), found in this run and
fixed: the node's shards now route a tablet whose copy is installing to another holder that is up.
Rerun on the fix, the same 300 s cut left no get refused through 18 installs, and the rate was back
17 s after the heal.

**Verdict: pass.** Nothing acknowledged was lost, no node restarted, and a heal is a second
whatever came before it. The catch-up after a cut longer than the retention is the slow part, and
since #184 it refuses nothing.

### A backup shipped and restored

[Section 4](#back-up-destroy-and-restore) copied a backup's files between hosts by hand, and
[what is left](todo.md) listed it as a limitation. Round 13 made it a command,
[F59](../features/backup-shipping.md)'s `cluster ship-backup`, and ran the whole cycle on the
cluster the partition tests had loaded (`target/lab/r13/ship/run.sh`):

| Step | Result |
| --- | --- |
| `backup /optane/shoal-backup` | 36 groups `Written`, 3.6 GB, in 25 s; europa held 22 files, titan 32, hyperion 18 |
| `ship-backup /optane/shoal-backup/<op>` | 68 s; every host holds all 72 files, owned by `shoal` |
| `ship-backup` again | 0 transfers |
| `destroy`, `bootstrap`, activate, `restore` | every group `Restored` and verified, 179 s |
| csv through each member alone at `One` | 1,187,691 movies and 58,418 keyword partitions, 0 missing, 0 different |

**Verdict: pass.**

### A get through an unplaced member

[Section 8](#a-member-added-with-no-rebalance) measured a get through a member with no placement
slot at about four times a placed member's latency, 2 ms at the median, and the limitation was
never filed. Round 13 asked whether that is the hop or something on it. The setup was the same:
europa and titan at a factor of two, the csv loaded, and hyperion added with no rebalance. Gets
alone at `One` ran for 30 s through each member (`target/lab/r13/unplaced/run.sh`), first with one
query in flight and then at the bench's defaults. The clients run on europa.

| Through | Placed | One in flight: gets a second, p50, p99 | Loaded: gets a second, p50, p99 |
| --- | --- | --- | --- |
| europa | yes, and the clients' own host | 13,560, 0.067 ms, 0.105 ms | 155,338, 0.63 ms, 27 ms |
| titan | yes | 3,285, 0.301 ms, 0.422 ms | 58,782, 0.93 ms, 164 ms |
| hyperion | no | 1,509, 0.644 ms, 0.842 ms | 47,286, 2.67 ms, 210 ms |

Titan and hyperion are the same hardware at the same distance from the clients, so the gap
between them is the forward: 0.34 ms idle, which is one more round trip on the data lane and TLS
both ways on each end. Loaded, hyperion does that for every share on a Zen1 host, and its queues
make up the rest of the median.

**Verdict: the cost of the hop, and nothing on it to remove.** A member that holds nothing has to
ask a member that does. It is a limitation of coordinating through an unplaced member, filed as
such on [what is left](todo.md), and a client that can reach the placed members should use them.

### A step that outlasts the log

[What is left](todo.md) carried moves at terabyte scale as extrapolated, with two defaults
expected to keep a step from finishing under writes: `snapshot_timeout` and the log retention.
Nobody had hosts with the disk, so round 13 staged the retention half. The inventory's new
`replication:` block ([F51](../features/cluster-deployment.md)) shrank `retained_entries` from
100,000 to 2,000, about a second of a busy group's writes, and hyperion was rebuilt under the
mixed bench as in [section 10](#10-the-compactors-backlog-and-four-rebuilds)
(`target/lab/r13/tb/run.sh`, each member's installs counted from its journal).

| Run | Retention | Rebuild | Streamed for moved | Installs per group on hyperion | Lost |
| --- | --- | --- | --- | --- | --- |
| shrunk1 | 2,000 entries, 24 MiB | 452 s | 3.2 GiB for 879 MiB | 28 once, 6 twice, one 14 and one 16 times | 0 |
| before #185 | 2,000 entries | 991 s | 8.3 GiB for 878 MiB | 31 once, 4 twice, one 46 times; its step failed and was retried | 0 |
| #185 fixed | 2,000 entries | 187 s | 1.0 GiB for 876 MiB | 36 once | 0 |

A step that outlasted the log did finish, which was the question, but not for the reason hoped.
A group's leader went on building snapshots, and so purging its log, while a member took a
snapshot of the group. When the install finished, the entries after its boundary were gone, and
the leader cut another. One group went round that loop every 6 to 7 s for over ten minutes, and
its step finished only on a retry near the end of the bench's writes. That is [#185](../appendix/resolved/snapshot-outrun-by-purge.md),
fixed in round 13: a member taking a snapshot holds its leader's unforced snapshot builds until it
has been fed past the boundary. On the fix, every group installed one snapshot, and the rebuild
was the fastest on record here.

**Verdict: pass on the fix.** Nothing was lost in any run. What is still unproved at terabyte
scale is the `retained_bytes` half: a forced purge, which bounds a shard's disk, is not held, so a
step whose transfer outlasts a shard's retained WAL bytes still loses its log. The inventory can
now raise it.

### A rehome that forgot its count

The first workspace suite run on this round's fixes failed `local_rehome_recovers_after_each_crash_point`
on *"node two never died at before_finalize"*, one of [#142](../appendix/known-issues.md#142-two-fixture-tests-fail-intermittently-on-an-idle-host)'s
recorded shapes. Alone, with child logs, it failed one run in seven. The armed node had started one
executor and run no rehome at all, because the storage marker said one executor while the files were
laid out for two. The control thread records every topology version it observes in the marker, and
it runs while the pool rehomes the files. Its rewrite had read the marker before the previous
start's rehome recorded two, and written one back after. That is
[#186](../appendix/resolved/marker-lost-update.md), which also explains #142's *"No such file or
directory"* from a child: both rewrites staged the marker under one temporary name. Every rewrite now
holds one lock. On the fix, a unit test that races the two passes, and the rehome matrix passed ten
runs of ten alone.

### The final build

`cluster destroy`, `bootstrap` and `target/lab/r11/abload.sh` on the round's last build (`cf266ec`),
with the lab's inventory:

| | |
| --- | --- |
| Whole load | 2,193,788 rows in 50.0 s, 43,908 rows a second |
| csv, whole | 1,187,691 movies and 58,418 keyword partitions: 0 missing, 0 different |
| Mixed bench, 120 s | 113,331 operations a second; p99 get 29.4 ms, update 192.7 ms |
| Acknowledged inserts, each member alone | 687,372, 0 lost |

**Verdict: pass.** The same shape as round 12's runs: 103,900 to 122,600 operations a second.
The build with #187 (`c7c4474`) was then rolled onto the same cluster with `cluster upgrade`, one
node at a time, and hyperion alone read back the whole csv (0 missing, 0 different) and all
687,372 acknowledged inserts.

### A restarted node that never compacted

#142's oldest shape, the lost-response test's *"checkpoint never reached 5: [(0, Some(0))]"*, was
caught twice by the loaded loop with child logs. Node zero, killed and restarted, sealed segment
after segment (126 in one run) and handed none to a compactor, so no group on its shard ever moved
its checkpoint. A diagnostic added to name the group holding a sealed segment never fired. It was
not a group: the segment the node recovered at its restart had never been sealed. The writer seals
the file it holds when the generation moves, and a `ROTATE` that reached the restarted node before
any entry did moved the generation while the writer held nothing. The sweep stops at the first
unsealed segment. That is [#187](../appendix/resolved/recovered-segment-never-sealed.md), fixed and
reproduced in a unit test. On the fix, five logged rounds had no stall, where ten rounds before it
had two, and eight rounds without logs passed every test.

In service a segment rotates when appends fill it, and those appends open the recovered file
first, so the ordinary path does not meet this. An explicit rotation does: a repair's or a backup's
(`RepairRotate`) reaching a node before its first write after a restart would have left its WAL
growing until the next restart.

## 15. Round 14

Round 13 left [what is left](todo.md) with the byte half of a step that outlasts the log, a cut
written whole before it is sent, O64's spread, #142's deadlines and a handful of rows to confirm.
This round works through them on builds from `0d24968` on. Its runs are under `target/lab/r14/`.
Two things were added to the inventory's `replication:` block to stage them:
`stream_bytes_per_sec`, which throttles every node's snapshot streams, and `hold_bytes` (below).

### The byte retention

[#185](../appendix/resolved/snapshot-outrun-by-purge.md) held a group's snapshot builds while a
member took a snapshot of it, and said the forced purge that bounds a shard's WAL at
`retained_bytes` was still not held. Round 14 staged it (`target/lab/r14/tb/`): hyperion rebuilt
under the mixed bench as in round 13, once at `retained_bytes: 40MiB` with the streams as they
are, and then as an A/B at the floor of 20 MiB with every node's streams throttled to 2 MiB/s.
That makes a step take about 50 s, against a retention of about 10 s of a busy group's writes.

| Run | Rebuild | Streamed for moved | Installs per group on hyperion | Lost |
| --- | --- | --- | --- | --- |
| 40 MiB, before the fix | 200 s | 1.0 GiB for 876 MiB | 36 once | 0 |
| 20 MiB and 2 MiB/s, before the fix | 1,068 s | 1.7 GiB for 878 MiB | 35 once, 1 twice | 0 |
| 20 MiB and 2 MiB/s, the fix | 1,042 s | 1.6 GiB for 878 MiB | 36 once | 0 |

At 40 MiB nothing looped. The byte budget there holds about 19,000 of a busy group's entries,
19 s of its writes, and a step took 5 s. Throttled, one group installed a snapshot at boundary
105,068 and another at 137,034 22 s later: its leader had been forced to purge past the first.
That is [#188](../appendix/resolved/forced-purge-outruns-snapshot.md), fixed: the retention sweep
passes over a group a member is taking a snapshot of while the shard's sealed WAL is within the
new `hold_bytes` (1 GiB by default) past its budget, and a forced purge stops at the dropped
segment's last frame instead of the group's checkpoint.

The throttled arms streamed about twice what they moved in both builds. That is the log: a step
that takes 50 s feeds its new copy the set's 50 s of writes after the snapshot. And both logged
`a move's destination has made no progress` every 30 s of a slow transfer (6 and 18 times), while
the snapshot was still arriving. That was [#189](../appendix/resolved/move-stall-ignores-snapshot-bytes.md),
fixed: the bytes sent count as progress.

**Verdict: pass on the fix.** Nothing was lost in any run. What a terabyte step still meets is
`retained_bytes + hold_bytes` and `migration.timeout`, both the operator's to raise.

### A cut in disk order

Pricing [O52](../appendix/optimizations.md#o52-a-snapshot-copies-every-record-of-the-archives-into-one-file),
a cut streamed rather than written first, found that on titan under the bench a Movie set's cut
took 5.5 to 8.4 s and its send and install 1.4 s (`target/lab/r14/tb/cuts.py`). The cut read one
record at a time in key order, about 80,000 records of 700 bytes each at random offsets on a busy
device. It now reads them in disk order, in runs of up to a mebibyte
([O78](../appendix/optimizations.md#o78-a-snapshot-cut-read-one-record-at-a-time-in-key-order)).
The same rebuild under load as [section 10](#10-the-compactors-backlog-and-four-rebuilds), on the
lab's inventory:

| | |
| --- | --- |
| titan's cut of a set | 0.55 s, from 5.5–8.4 s |
| europa's | 0.16 s, from 0.5 s |
| Rebuild | 175 s, streamed 877.5 MiB for 878.1 MiB moved |
| Acknowledged inserts, each member alone | 3,465,326, 0 lost |
| csv, each member alone | 0 missing, 0 different |

**Verdict: pass**, and the fastest rebuild on the lab so far.

### #142 under load

[#142](../appendix/known-issues.md#142-two-fixture-tests-fail-intermittently-on-an-idle-host)
has tracked the suite's failures under load as deadlines since round 9. Round 14 ran round 13's
loop, the six heaviest fixture tests together at six threads (`target/lab/r14/142/loop.sh`,
`loop-rehome.sh`), on this round's build with the lab cluster stopped:

| Build | Rounds | Tests failed | Shapes |
| --- | --- | --- | --- |
| This round's, before #190 | 7 | 11 of 42 | a write not committed in time, a group with no leader in time, a lease lapsed for 30 s |
| With #190 | 5 | 0 of 30 | none |

The kept child logs showed the last shape plainly: node two led a group whose followers each
handled about 5,400 of its appends, while node two logged 76 `the replication rpc timed out` for
it. openraft gives an append a heartbeat interval, 100 ms at the fixture's base, and the answers
that came later were thrown away, so nothing committed and the lease lapsed. That is
[#190](../appendix/resolved/append-answer-thrown-away.md), fixed: an append waits at least the
election timeout. On the lab it changes nothing, since its hosts answer in time: six loads at a
1 s base ran at 42,600 to 55,000 rows a second either side of the fix, and no append timed out.

**Verdict: #142's deadlines were mostly #190.** The item stays open for the restore stall, which
has not recurred, and for what the full suite run finds next.

### Round 14's final build

On the round's last build (`96ebbe5`), with the lab's inventory (`target/lab/r14/confirm.sh`):

| | |
| --- | --- |
| Whole load | 2,193,788 rows in 45.2 s, 48,491 rows a second |
| A quiet minute under the bench | 0 vote changes; 122,408 operations a second |
| titan, leading the most groups, killed under the bench | writes it led refused `NotLeader` for 17 s (seconds 16 to 33), the rest served; 661,504 acknowledged inserts, 0 lost through each member alone |
| hyperion's peer ports cut for 20 s under the bench | the second of the cut at 47% of the second before, the next at 119%; refusals end within two seconds of the heal; 852,612 acknowledged inserts, 0 lost through each member alone |

**Verdict: pass.** The failover window is the one [C7](../distributed/failover.md#the-window-and-what-a-client-sees)
states, three to four bases, and #190's floor on an append's wait did not lengthen it: a killed
peer's calls end on the link's silence. A partition's worst second is where round 12 left it
([#143](../appendix/resolved/silent-partition-hops.md#still-open)), 47% against 44%.

## 16. Round 15

Round 14 left [what is left](todo.md) with O79's amplification designed but unbuilt, the
terabyte-scale rows that needed "hosts with the disk", and #142 open for whatever a full suite run
finds next. This round built the fragments O79 filed as [F61](../features/fragmented-partitions.md)
and measured them ([performance](performance.md#o79-in-round-15-fragments)), then took the lab's
nodes an order of magnitude past the one dataset they had always held. Nothing in the lab's data
was kept: F61 changed the archive map's format. The runs are under `target/lab/r15/`.

### Ten times the dataset

The lab's hosts have about 60 GB free each, not a terabyte, but enough for ten copies of the TMDB
dataset. The loader gained `--copies` and `--first-copy`: copy `c` offsets every id by `c·2⁴⁰` and
keeps titles and keywords, so every keyword partition grows by each copy's movies, and `verify
--copies` expects every copy's keyword rows. One cluster of the lab's inventory at F61's defaults
was grown to one, five and ten copies (`scale.sh`), each step read once the compactors drained:

| Copies | Load | Movie partitions | Archived a node, Movie / keyword | On disk a node | Rows / resident a node |
| --- | --- | --- | --- | --- | --- |
| 1 | 2,193,788 rows in 45.7 s, 48,041 a second | 1,134,767 | 633 MiB / 163 MiB | 2.1 GB | 1.1 / 3.4–3.6 GiB |
| 5 | 8,775,152 more in 191.5 s, 45,818 | 5,900,163 | 3.2 GiB / 787 MiB | 7.3 GB | 1.8–2.0 / 7.5–7.7 GiB |
| 10 | 10,968,940 more in 250.8 s, 43,729 | 11,835,067 | 6.4 GiB / 1.5 GiB | 13 GB | 0.4–0.5 / 8.0–8.2 GiB |

Loads held their rate as the data grew tenfold. 74,946 keyword partitions were chains at ten copies,
counted over the three copies of each. Memory did not hold: see [memory at ten times the dataset](performance.md#memory-at-ten-times-the-dataset).

### Rebuilding a node at ten times the dataset

hyperion rebuilt under the mixed bench on the ten copy cluster (`rebuild.sh`), with the lab's
defaults otherwise, a step at a time (`moves_per_node: 1`):

| | |
| --- | --- |
| Rebuild | 18 steps, 8.0 GiB moved and 8.5 GiB streamed in 478 s: 18 MiB/s, 21 s a step |
| Round 14's, one copy | 878 MiB in 175 s: 5 MiB/s |
| Installs on hyperion | 36 groups, once each |
| Snapshot fed, stalled, failed, forced purges | 0, 0, 0, 0 on every node |
| Bench over 2,400 s | 12,822 gets, 14,417 updates and 4,807 inserts a second; update p99 333 ms |
| Acknowledged inserts, each member alone | 11,542,676, 0 lost |
| csv, each member alone at `One` | copy 0's 1,187,691 movies and all ten copies' 58,418 keyword partitions: 0 missing, 0 different |

The bench's errors were hyperion's own connections lost when it stopped (2,176), and 261
`NotLeader` and 299 `OutcomeUnknown` answers for writes it led as it went, each a write the client
may send again. A step moved a set of about 450 MB, where a lab step at one copy moved about 50,
and the fixed costs [F56](../features/cluster-rebuild.md#performance) measured (about 35 s a step) are now the smaller share: nine times the
data moved in under three times the time.

**Verdict: pass.**

### Several steps onto one node

[F56](../features/cluster-rebuild.md#performance) found six steps at once no faster than one at a
few hundred MB a set, since the per-record work was the limit. The inventory can now name
`moves_per_node` and `migration_timeout` in its `replication:` block. The ten copy cluster, grown
by the benches since to 13.5 GiB a node, was reconfigured to six moves a node and hyperion rebuilt
the same way (`rebuild.sh`, `moves6.yaml`), on the build with #191's fix:

| | One step at a time | Six at a time |
| --- | --- | --- |
| Moved | 8.0 GiB, streamed 8.5 | 13.5 GiB, streamed 15.5 |
| Rebuild | 478 s, 18 MiB/s | 306 s, 49 MiB/s |
| Each step | 21 s | 60 s, six in flight |
| Snapshot fed, stalled, failed, forced purges | 0 | 0 |
| Acknowledged inserts, each member alone | 11,542,676, 0 lost | 6,951,849, 0 lost |
| csv, each member alone | 0 missing, 0 different | 0 missing, 0 different |
| Bench update p99 | 333 ms | 281 ms |

At 450 to 750 MB a set the steps' fixed costs and the per-record work overlap, and six at once
moved 2.7 times the bytes a second. Each step took three times as long, since the six shared the
two sources' cut and stream and hyperion's installs. The bench's refusals were as before: the
connections to hyperion while it was down, and `NotLeader` and `OutcomeUnknown` (1,080 and 984)
while its leads moved.

**Verdict: pass.** Several steps onto a node are worth taking at this size.

### A step that outlasts both retentions

Round 14 left "a step that outlasts `retained_bytes + hold_bytes`" as what a terabyte step still
meets, and said a step that outlasts both loses its log and is sent another snapshot. Round 15
staged it on the ten copy cluster (`outlast.yaml`): every node's streams throttled to 4 MiB/s,
`retained_bytes: 20MiB` and `hold_bytes: 64MiB`, so a step of about 900 MB took about four
minutes and the bench's writes passed both allowances within seconds. hyperion rebuilt under the
mixed bench (`rebuild.sh`), every node on the `jemalloc-prof` build.

It did not do what round 14 said. The forced purge never happened: every sweep logged the same
`purged` for segment after segment while the sealed WAL stayed at 262 MB, three times the bound,
and europa wrote about 580 journal lines a second for 17 minutes, 117,722 of them the retention
warning and 182,599 openraft `purge_log` commands. For eight minutes the cluster served a tenth of
its rate, with updates at 400 to 500 ms at the median, and 298,252 writes were refused
`NotLeader`. After 30 minutes three of eight steps had moved, four had failed, 18.9 GiB had streamed
for 2.6 GiB moved, and `cluster rebuild` gave up with the plan blocked on tablet 1. Nothing was
lost: 10,819,412 acknowledged inserts through each member alone, and the csv's copies, 0 missing
and 0 different.

That was [#192](../appendix/resolved/forced-build-deferred.md): #188's force relied on a flag
openraft never sets, so the forced build was held like any other and the sweep asked again every
few seconds for every segment. Fixed: the force reaches the machine through the holds, once a
group. The same plan, left blocked, was taken up by the fixed build installed on all three nodes
at once (a rolling upgrade refuses a cluster with sets under the factor, rightly):

| | Before the fix | On the fix |
| --- | --- | --- |
| Retention warnings | about 115 a second on europa | 19 in its first minutes |
| Steps moved | 3 in 30 min | 14 more in 68 min, one at a time at 4 MiB/s |
| Streamed for moved | 18.9 GiB for 2.6 | 15.4 GiB for 14.9 |

The last set, blocked after the two failures under the old build, moved once the removal was sent
again (`cluster admin remove <old> <new>`, which forgives a plan's failures,
[#177](../appendix/resolved/blocked-plan-retry.md)): 18 sets, 20.0 GB, and the plan done. Then,
through each member alone at `One`, 10,819,412 acknowledged inserts, 0 lost, and the csv's copies,
0 missing and 0 different.

**Verdict: fail before the fix, fixed.** A step that outlasts both allowances no longer livelocks
the sweep. What it still does is below.

**On the fix, under the bench.** hyperion rebuilt once more the same way on #192's build
(`target/lab/r15/rb4/`). The sweep was quiet: 4,522 and 4,501 retention warnings on europa and
titan over 30 minutes, one a force, against 117,722 on europa in 17 before. A step still could
not finish, as round 14 said it would not: each forced purge took the entries after the member's
snapshot, which needed another. After 49 minutes no set had moved, three moves had failed on their
catch-up deadline, the plan was blocked on two tablets, and the bench had been refused `NotLeader`
34,270 times (a tenth of the unfixed run's). Nothing was lost: 7,059,401 acknowledged inserts and
the csv's copies through each member alone, 0 lost, 0 missing, 0 different.

It found one more defect. hyperion refused every Movie set's snapshot from 23 minutes in to the end, 820
times, because a failed move's partial of 1.17 GB was still counted against its 2 GiB bound for
partials: [#194](../appendix/resolved/abandoned-partial-snapshots.md), fixed, a partial nothing
came for past `snapshot_timeout` is dropped when the next stream begins. The same plan, taken up on that
build by a restart of all three nodes (which also emptied the stranded partial) and a removal
sent again with the bench stopped, moved 7 sets in 57 minutes, one every seven at 4 MiB/s, with no
bound refusal; one more move failed, a learner not caught up within `migration.timeout`'s 600 s
while its set streamed at the throttle.

**Verdict:** a step that outlasts `retained_bytes + hold_bytes` under writes does not finish, and
now fails by name without harming the rest of the cluster. `hold_bytes` has to cover a step's
transfer at the write rate its groups see; at a terabyte a node that is the operator's to size.

### Rebuilding at fourteen copies on the final build

The rebuilds above ran on builds with #191 unfixed or #192 unfixed, and memory samples from the
second and third (taken by a sampler that outlived its run, untimestamped) showed titan at 12.9 to
13 GiB resident against its 8 GiB budget, on a 14 GiB host, with nothing left to evict. Which run
they came from cannot be told. So hyperion was rebuilt once more under the bench on the final build
(`rbmem.sh`, fourteen copies and the benches' inserts, each member's memory stamped every 30 s):

| | |
| --- | --- |
| Rebuild | 18 steps, 13.3 GiB moved and 13.6 GiB streamed in 596 s: 23.7 MiB/s, 30 s a step |
| Snapshot fed, stalled, failed, forced purges | 0, 0, 0, 0 on every node; 36 groups installed once each |
| Acknowledged inserts, each member alone | 7,827,707, 0 lost |
| csv, each member alone at `One` | copy 0's movies and all fourteen copies' keyword partitions: 0 missing, 0 different |
| Memory, every member | peaked at 8.0 GiB, its budget, keeping 2.0 to 2.5 GiB of rows; never above |

**Verdict: pass.** The budget holds through a rebuild on the final build.

### #142 in round 15

[#142](../appendix/known-issues.md#142-two-fixture-tests-fail-intermittently-on-an-idle-host) stays
open "while a full suite run still finds something". Round 15 ran the whole workspace at six
threads with the lab stopped, on the round's build before O83: **1,740 of 1,740 passed, eight
ignored**, the first full run to find nothing. Round 14's loop of the six heaviest fixture tests
(`target/lab/r15/142/`) then found two shapes in 66 runs:

| Loop | Runs | Failed | Shape |
| --- | --- | --- | --- |
| `loop.sh`, 5 rounds | 30 | 1 | `migration_resumes_after_each_phase_failure`: after its driver was killed at `configured`, one group's move stayed `Configured` for 240 s while the other reached `Activated`. The logs were not kept |
| `loop-keep.sh`, until a failure | 36 | 1 | `scheduled_scrub_quarantines_without_an_operator`: one group's scheduled scrubs were refused for a stale version every time, silently: [#195](../appendix/resolved/scheduled-scrub-starved.md), fixed |
| `loop-keep2.sh`, 10 rounds, on #195's fix | 60 | 0 | |

**Verdict:** #142 stays open for the stuck move, which has not recurred and is not explained; the
loop now keeps every failing round's child logs (`loop-keep.sh`), so the next one can be read.

### Round 15's final build

On the round's last code (`d320328`), with the lab's inventory (`target/lab/r15/confirm.sh`),
beside [round 14's](#round-14s-final-build):

| | Round 15 | Round 14 |
| --- | --- | --- |
| Whole load | 2,193,788 rows in 53.0 s, 41,397 rows a second | 48,491 |
| A quiet minute under the bench | 0 vote changes; 111,819 operations a second | 0; 122,408 |
| europa, leading the most groups, killed under the bench | writes it led refused `NotLeader` for 16 s (seconds 16 to 31), the rest served; 577,535 acknowledged inserts, 0 lost through each member alone | titan killed: 17 s; 0 lost |
| hyperion's peer ports cut for 20 s under the bench | the cut's first second at 53% of the second before; refusals end within two seconds of the heal; 944,577 acknowledged inserts, 0 lost through each member alone | 47%; 0 lost |

**Verdict: pass.** The load's rate is inside the bootstrap-to-bootstrap spread
[O64](performance.md#o79-in-round-15-fragments) records (41,000 to 56,000 rows a second), and the
bench's within the lab's run-to-run spread. The failover window and a partition's worst second are
where round 14 left them.

## 17. Round 16

Round 15 left [what is left](todo.md) with two open defects of its own (#193, the rebuild's dial
noise, and #196, the row charge), the failover window as the first limitation on the page, O64
with "nothing planned", and #142 waiting for its stuck move to recur. This round fixed the two
defects and measured both on the lab, narrowed the failover window from three to four bases to
one and a half to two ([F62](../features/failover-window.md)), ruled the hosts' frequency
governor out of O64 ([performance](performance.md#o64-in-round-16-not-the-governor-either)), and
ran the loop for #142 again. Nothing in the lab's data was kept: #196 changed what a sorted
partition's archived size means, and every run here starts from a fresh bootstrap. The runs are
under `target/lab/r16/`.

### The frequency governor

The first run of the round was O64's, since it changes how every later number reads if it
names the mode. It did not: ten fresh loads under `schedutil` and `performance` on the Zen1
hosts spread the same way, with the cores at 3.1 to 3.3 GHz under either
([performance](performance.md#o64-in-round-16-not-the-governor-either)). The lab stays on
`schedutil`.

**Verdict: not it.** One more candidate struck.

### A rebuild without the dial noise

Round 15's rebuilds each left about twenty failed dials a second in every journal
([#193](../appendix/resolved/rebuild-redial-thrash.md)). Read by lane over europa's journal for
those rebuilds, 22,938 of the 36,479 were on the control lane at exactly one a second per old
identity between the benches and twenty a second under them, and 13,541 on the replication lane
at about one a second, all of them `CertificateIdentity` verdicts that
[#172](../appendix/resolved/identity-refusal-redials.md) already made wait their whole backoff.
The control lane's links were keyed by address, so each heartbeat to one of the rebuilt node's two
identities threw the link to the other away, backoff and all - and the link to the live new
identity with it, twenty times a second. The replication lane's verdicts stopped growing at
`reconnect_max`, five seconds, and six shards a node made that a dial a second. The fixture test
written for it made 402 control links in twelve seconds on the unfixed tree and none on the fix.

On the fixed build, a fresh cluster of the lab's inventory loaded whole (53,126 rows a second)
and hyperion rebuilt under the mixed bench (`target/lab/r16/193/run.sh`, 25 minutes of bench):

| | |
| --- | --- |
| Rebuild | 18 sets, 883 MiB moved and 901 MiB streamed in 178 s: 4.9 MiB/s, 7.5 s a step |
| Snapshot fed, stalled, failed, forced purges | 0, 0, 0, 0 on every node; 18 groups installed once each |
| Failed dials in europa's journal | 474: 433 refused connections in the minute hyperion was stopped and wiped, at `reconnect_min` as a node that may come back is dialled; 40 verdicts over the rebuild, 7 on the control lane and 33 on the replication lane, 9, 19, 6, 5 and 1 a minute as the backoffs doubled |
| In titan's | 238: 207 refused connections and 31 verdicts |
| In hyperion's | 12 verdicts, and 25 refused control handshakes where round 15 counted 9,349 |
| Bench over 1,500 s | 14,274 gets, 16,052 updates and 5,352 inserts a second; update p99 137 ms; 1,333 `NotLeader` and 8 `Unavailable` for writes hyperion led as it went, 384 connections lost when it stopped |
| Acknowledged inserts, each member alone | 8,033,268, 0 lost |
| csv, each member alone at `One` | 1,187,691 movies and 58,418 keyword partitions: 0 missing, 0 different |

**Verdict: fixed.** A three minute rebuild leaves about forty verdict dials in a peer's journal
where an eight minute one left 9,374, and the control link to the rebuilt node stays up through
it.

### The failover window

Every round since the first measured a killed leader's groups refusing writes for about sixteen
seconds at the default base, and the pages called the window two to three bases while the lab said
three to four. Round 16 read where the sum comes from: openraft's follower lease is
`election_timeout_max`, which `group_config` set to twice the base, and the randomized timeout of
one to two bases runs *after* the lease, so a candidate stood between three and four bases after
the last heartbeat. [F62](../features/failover-window.md) makes the lease the base itself and the
timeout half a base to a whole one. Measured the way every round measured it
(`target/lab/failover-test.sh`: a fresh cluster loaded whole, a quiet minute under the mixed bench
with `vote is changing` counted in every journal, then the node leading the most groups killed
under the bench; `target/lab/r16/failover/`):

| Base | Writes refused after the kill | Round 15's final build | Vote changes in the quiet loaded minute | Load | Bench in the quiet minute | Acknowledged inserts, each member alone |
| --- | --- | --- | --- | --- | --- | --- |
| 5 s (default) | about 10 s (t=16–25), the last hundred at t=25 | 16 s (t=16–31) | 0 | 45,916 rows/s | 113,353 ops/s | 568,221, 0 lost |
| 1 s | about 2 s (t=16–17) | about 4 s (round 11's 1 s arm) | 0 | 41,680 rows/s | 133,100 ops/s | 656,669, 0 lost |

Europa led the most groups in both runs and was the node killed; the refusals were `NotLeader` at
once, never a timeout, and the first second after the kill carried a write p99 of a second as the
writes queued on europa's groups were refused.

The fixture's nineteen tests that set the failover base, judge a lease or dial an identity were
run together at six threads on the change (`target/lab/r16/fixture/`). Eighteen passed and one
found what the arithmetic had moved without meaning to: the grace an empty volatile copy grants
no vote for ([#142](../appendix/resolved/volatile-amnesiac-vote.md)) was "two election
timeouts", `election_timeout_max × 2`, four bases with the old `max` and two with the new - the
two seconds `a_restarted_volatile_leader_elects_nobody_missing_its_commits` holds an empty
leader and a lagging follower alone, so the copy that had forgotten its commits granted its vote
at the end of them and the follower that kept them hit openraft's `log_state_reader.rs:25`
assertion. The grace and the head start a non-primary gives its primary are four leases now,
the same four bases as before; the test passed four of four after it.

**Verdict: pass.** The window is what the arithmetic says, and a loaded minute at a lease of the
base elected nobody at either base. Every wait derived from the lease was read against the
window it has to outlast, and one had to be restated.

### What a row is charged

Round 15's heap profile of titan at the end of a load counted 2.3 GiB of rows where the heap
held 3.4 ([#196](../appendix/resolved/row-charge-undercount.md)). The fix charges a sorted
partition's B-tree nodes and its keys' heap beside its rows, makes the unsorted replay and the
resident-archive insert charge what eviction releases, and reports the eviction list. It was
measured twice (`target/lab/r16/196/run.sh`): a fresh cluster on the `jemalloc-prof` build,
the whole csv, a minute for the compactors, then 150 s of gets alone - which change no row and
allocate enough for jemalloc to dump again while the rows stand still - with `cluster stats`
read and titan's last dump taken inside that bench, symbolized against the binary it came from
(`heap.py`, depth 3). The first run charged each sorted entry a fixed share of a node; the
profile showed why that is not enough, and the second charges the nodes themselves.

| | Round 15 (one copy, `d320328`) | A share of a node per entry | A node per eight entries (final) |
| --- | --- | --- | --- |
| Rows the profile holds, titan | 3.0 GiB of a 3.4 GiB "rows" figure that counted the table map and the LRU too | 1,497 MiB: Movie 581 deserialized + 571 partition boxes; keyword 252 of B-tree nodes + 82 of row heap + 10 | 1,549 MiB: Movie 607 + 589; keyword 257 of nodes + 87 of row heap + 10 |
| `rows` counted by `Stats` | 2.3 GiB (77% of the rows) | 1,229 MiB (82%) | 1,319 MiB (85%) |
| Eviction list, profile / `lru` | 0.16 GiB / not reported | 82 MiB / 75.5 | 86 MiB / 75.6 |
| Table maps, profile / `table maps` | 0.29 GiB / 6–8 MiB after eviction | 144 MiB / 129.1 | 144 MiB / 129.1 |
| Resident | 3.4–3.6 GiB | 2.7 GiB | 2.7 GiB |

The keyword table's B-tree nodes are the figure that set the model: 252 MiB for about a million
rows in 58,418 partitions, 250 bytes a row where a full slot is 112, because most keyword
partitions hold a few titles in a node of eleven slots. A share of a node per entry counts a
three-row partition at a third of its node; a node per eight entries counts it whole. What is
left, about 230 MiB or 15% of the rows, is the allocator's rounding of a Movie row's thirty-odd
String and Vec blocks, filed on the todo page
([what a row's allocations take](../appendix/todos.md#what-a-rows-allocations-take)) with this
number to be judged against.

**Verdict: fixed, with the rounding filed.** The memory table now names 2,020 MiB of a 2,729 MiB
node - rows, the two maps, the WAL index and the eviction list - where round 15's left 1.4 to
1.7 GiB unnamed on a restart.

### Round 16's final build

On the round's last code, with the lab's inventory (`target/lab/r16/confirm/run.sh`), beside
[round 15's](#round-15s-final-build):

| | Round 16 | Round 15 |
| --- | --- | --- |
| Whole load | 2,193,788 rows in 51.6 s, 42,517 rows a second | 41,397 |
| A quiet minute under the bench | 0 vote changes; 140,502 operations a second | 0; 111,819 |
| europa, leading the most groups, killed under the bench | writes it led refused `NotLeader` for about 10 s (seconds 16 to 25), the rest served; 633,995 acknowledged inserts, 0 lost through each member alone | 16 s (seconds 16 to 31); 0 lost |
| hyperion's peer ports cut for 20 s under the bench | the cut's first second at 40% of the second before; refusals end within a second of the heal; 1,013,465 acknowledged inserts, 0 lost through each member alone | 53%; 0 lost |

**Verdict: pass.** The load's rate is inside O64's spread, the bench's inside the lab's, the
failover window is [F62](../features/failover-window.md)'s, and a partition's worst second is
where [#143](../appendix/resolved/silent-partition-hops.md#still-open)'s remainder left it
(40%, 53% and 47% over three rounds: the kernel's two retransmission timeouts).

