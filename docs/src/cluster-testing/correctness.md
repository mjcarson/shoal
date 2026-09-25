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
