# X10. What a stripe row costs, measured

**Reported 2026-10-04.** This is the record of spike [X10](spikes.md#x10-what-a-stripe-row-costs).
It drove rows shaped like the two a bucket generates, S3's `ObjectMeta` and `StripeMeta`, through
today's persistent unsorted tables on the lab's cluster: `tmdb_cluster.yaml`'s three nodes at a
factor of three. It measured:

- rows a second one tablet group commits, as inserts, overwrites and S7's conditional commits;
- the bytes a row costs on disk, in the WAL, in the indexes and in memory, cold and resident;
- what a commit to a row that a restart left cold costs the write, its group and a neighbour;
- an object held inline in its row, from 1 KiB to 1 MiB.

Four rounds, each leg on a cluster bootstrapped for it, then a supplement of four more rounds on the
one result the rounds left open. It ends in a recommendation, which
[S18](contract.md#q25-in-part-the-metadata-rows-2026-10-04) records as Q25, in part:
**stripe rows are not kept resident, and a stripe's commit follows the read of its row S7's
coordinator already makes**; a cold row is 39 bytes of index, so the index sets no floor under the
stripe size above X6's; and **a pool's inline threshold defaults to 16 KiB**.

Five facts decide it:

- **At depth one a cold commit costs what a warm one does**: 4.96 ms against 4.95. Every replica
  reads the row, a few hundred microseconds on the 970 EVO, inside the WAL's commit delay of 2 to
  3 ms, which a commit waits out anyway.
- **Under load the reads queue.** Thirty-two writers of cold rows in one group committed 0.62× the
  rows a second of thirty-two writers of resident ones when a Zen1 host led it, and 0.91× when
  europa did. A writer beside eight writers of cold rows in its group waited at its p99 2.02× its
  p99 alone, 1.24× of what eight writers of resident rows cost it. That is T1's second clause, and
  T1 fires on it.
- **The read S7 already makes removes the stall.** A `Quorum` read through the group's leader
  first leaves the leader's copy resident when the commit arrives: under load the commit then
  costs 1.20× a warm one (1.66× without the read), and the neighbour's p99 beside such writers is
  0.95× its p99 beside writers of resident rows. That is the remedy T1's rule chose, from depth
  one, confirmed by a supplement under load.
- **A cold row is 39 bytes of memory a replica**: its archive map entry, and nothing else. A node
  of 16 TiB at 4+2 replicating a row for every 4 MiB stripe would hold 0.31 GiB of it. Kept
  resident the same rows would be 4.1 GiB, at 484 bytes a row as the engine counts it.
- **Inline objects stay cheap to 8 KiB and bend between 16 and 32 KiB**: an even mixture keeps
  0.84× its 1 KiB rate at 8 KiB, 0.58 to 0.71 at 16 KiB and 0.32 to 0.55 at 32 KiB, with the
  network at 41% and 57% of its link. The benchmark host's single node bent an octave sooner, so
  T3 fires, at its line.

**What was found on the way**: a group commits about 4,900 small rows a second on the lab, and
thirty-two writers spread over every group commit fewer than thirty-two in one, at two to three
times the device bytes a row. The WAL keeps about 100 bytes of memory for every entry it retains, which
was 414 MB a node after the load, 2.6 times the rows' own index. A merge reads the archived row an
insert replaced whole. A group reads the rows its parked batch needs one at a time. What X10 did
not settle is under [What X10 does not settle](#what-x10-does-not-settle).

## The question

[Q25](contract.md#questions-to-answer) asks four things of the metadata rows a bucket generates
([S3](objects.md#the-two-rows)):

- where the inline threshold sits, at or under which an object is its `ObjectMeta` entry and
  nothing else;
- what a stripe row costs, on disk and in memory;
- how long a commit stalls when the row it needs is not in memory;
- what scale of bucket that allows.

[Q17](contract.md#questions-to-answer) asks how a slice that missed writes learns what it is stale
on, and prefers a bounded record in the tablet group, derived at apply. X10 answers the part of it
a table can: how many commits a second one group takes, and what rewriting one row costs by its
size.

The spike's section named three results in advance that would change the design:

- a commit to a stripe whose row is not in memory stalls its group for long enough to matter.
  Then stripe rows have to stay resident, or commits are batched around the read;
- the index memory for a row, times the rows a tebibyte written in place, exceeds what a node can
  give. That sets a floor under the stripe size;
- the inline threshold's knee on the lab's devices is far from where the benchmark host's was.

## How it was judged

The lines below were set before the harness existed and agreed with the user on 2026-10-04.
`x10 report` judges them as written. Every figure is an interval over four rounds: the lowest and
the highest round, with the median between. A ratio is taken round by round and judged by where
its whole interval lies, and a difference counts only where two intervals do not overlap, which is
[the lab's rule](../performance/benchmarking.md#before-and-after-on-the-lab).

| Trigger | Fires when |
| --- | --- |
| **T1. A cold commit stalls its group** | On the group a Zen1 host leads, at depth one, either: (i) a conditional update of a row that is cold on every replica has a median above 1.5× the same update of the same row once resident; or (ii) a writer of resident rows in that group, beside a second writer making cold commits into the same group, has its p99 above 1.5× its p99 alone, while the same writer beside cold commits into another group does not |
| **T2. The index sets a floor under the stripe size** | A cold row's index bytes, times the stripe rows a node replicates, exceed 1 GiB at a 4 MiB stripe. The node is one of 16 TiB of pool devices at 4+2, with metadata at a factor of three and every stripe written in place: `3 × 16 TiB × 4/6 ÷ stripe` rows, 8.39 million at 4 MiB. A 4 MiB stripe is X6's 1 MiB chunk floor at 4+2. The floor T2 sets is the smallest of 1, 4, 16 and 64 MiB that fits |
| **T3. The knee is far from the benchmark host's** | The knee is the smallest swept size at which an even mixture of puts and gets of inline objects, at depth 32, does fewer than 0.7× the operations a second it does at 1 KiB, in every round. On the benchmark host, jove's `f22-row-size` capture of the persistent unsorted table at half reads and depth 32, that is 8 KiB ([Row size](../tables/row-size.md#the-shape)). T3 fires if the lab's knee is at 2 KiB or below, or at 32 KiB or above |

**Statements made in advance, beside the triggers.** A design review of the plan, before any
code, found that T1 as worded could hardly fire, for two reasons:

- a depth-one commit on this cluster costs a WAL commit delay of 2 to 3 ms and a Zen1 sync, against
  a read of a few hundred bytes;
- S7's coordinator already reads a stripe's row before it commits ([S7](write-path.md#the-preferred-direction-step-by-step)).

The user's lines stand. Five more figures were named beside them, to be reported and not judged:

- cold over warm rows a second at depth 32 in one group, where cold reads queue behind each other;
- the same depth-one ratio on a group europa leads, whose reads go to the Optane;
- the commit after a read through every member, then through the leader, over the warm commit;
- the commit after a read through a follower, over a warm commit through that follower;
- what the writer beside cold commits does against one beside warm commits, which separates the
  load from the reads.

If a read through every member brings the commit within 1.25× of warm, the remedy the spike's
section names, "commits batched around the read", is the read S7 already makes.

## What was run

### The harness

`shoal-spike-rows` is a workspace crate of its own, built the way `examples/tmdb_dataset` is: a
schema in its library, a node program, `x10-node`, and a driver, `x10`, which also carries every
`shoaladm` command for the schema. It is not a subcommand of `shoal-spike`, which links no client
and no `shoaladm`. It adds no crate to the workspace's lockfile, only its own package: every
dependency is one `tmdb-dataset` already resolves. `shoaladm bench` could not drive it, because a
commit is an update conditional on what it read, and `#[shoal::db]` emits no operation kind a
schema can add to the bench ([F69](../features/driver-operation-kinds.md)). Like every spike's
code it is thrown away; nothing in its rows is a format.

**The rows** are S3's, as persistent unsorted tables:

| Table | Key | Fields | Archived |
| --- | --- | --- | --- |
| `ObjectMeta` | the path's hash, `u64` | `version` (the condition), and a list of one `ObjectEntry`: a 64 byte path, a 128 bit object id, size, geometry (4 MiB stripes, 64 KiB units, 4+2), truncate epoch, no floors, no retired ids, two times, two user pairs, and the inline bytes | 312 B with nothing inline |
| `StripeMeta` | consumer `u64`, object `u128`, stripe `u64`: three fields | `sequence` (the condition), six labels of a sequence and a tag, length, epoch, a missed mask | 176 B |
| `StripeMetaDigest` | the same | the same, and six eight byte digests | 240 B |
| `Filler` | `u64` | 4 KiB of bytes | 4,112 B |

The archived sizes are the row's rkyv form. An archive stores it inside its partition, 48 bytes
more for a stripe row and 24 for an object row (the members' own count, [below](#2-bytes-a-row)).
A stripe's commit is S7's: an update, conditional on the sequence its writer read, that moves the
sequence and rewrites the touched chunk's label. A change to an object row is an update
conditional on its version.

**Aiming at a group.** A tablet group serves every tablet whose replica set is one ordered set
of shards, and a tablet is the top twelve bits of a key's partition hash. The driver hashes a key
as the nodes do (`PartitionKeySupport::get_partition_key_from_values`, checked against the row's
own hash by a test) and keeps only keys whose tablet the chosen group serves: one in eighteen on
the lab, three nodes of six shards. It learns each group's tablets and leader from every member's
`Replication` read, and sends every write to the member leading its key's group. That is not how
a client of ~~today's~~ Shoal wrote then, since none routed by topology
([D7](../direction/shard-aware-routing.md)); since [F74](../features/client-routing.md) a client
sends each write to its group's preferred leader, as this driver did. The hop a coordinator would add is left out on
purpose, so that a cell measures a group's commit, and every table says whose group it was.

**Cells.** Every worker sends one operation, waits for the answer and sends the next, so depth is
the number of workers. A worker owns its keys, so no conditional write is ever refused by a
sibling. A cell warms up for 5 s and is measured for 15 s, except where it says otherwise. A cell
spending a budget of cold keys runs until they are spent and is counted to its last answer.
Around each cell the driver reads:

- every member's WAL counters (syncs, bytes, appends) from its `Replication` report;
- every host's device counters, through `shoaladm`'s own script ([F71](../features/bench-device-memory.md));
- every host's network counters, from its physical interfaces only. europa's link is a port of
  a bridge, which counts every byte the port does.

**Making rows cold.** A restart applies everything above a group's checkpoint again, resident,
and a checkpoint moves only past a sealed WAL segment that has been merged. So after the load the
driver writes filler rows, whose groups are not measured, until every group of the three measured
tables reports, on every member, a checkpoint equal to what it applied and to the end of its log,
with the checkpoint durable on disk and no segment waiting for a compactor. Then every node is
restarted. Archive reads are direct I/O, so a restart leaves nothing of a row in any cache.

**The legs**, each on a cluster bootstrapped for it and destroyed after:

| Leg | What it does |
| --- | --- |
| **rate** | For `StripeMeta` and `ObjectMeta` (nothing inline), aimed three ways: at a group a Zen1 host leads, at one europa leads, and at every group. 4,096 resident rows are written first for each aim. Then an insert of a new key, an overwrite of a resident row (a whole insert over it), and a commit of a resident row, at depth one and 32. Then one object row rewritten in a loop, a commit after a commit, with 0 B, 4 KiB, 64 KiB and 1 MiB inline |
| **rows** | Filler, every node restarted: the baseline. Then a load of 2,000,000 stripe rows, 1,000,000 with digests and 1,000,000 object rows, every member's figures read with them resident; filler until they are archived and checkpointed; the figures again; every node restarted; the figures cold. Then the cold commit (below) |
| **size** | `ObjectMeta` holding 1 KiB to 1 MiB inline, by octave: 512 objects written first, then puts of new objects, an even mixture, and gets, each at depth 32. Gets go to the member a worker is homed on, at `One`. The compactors catch up before the next size |

**The cold commit**, on the rows the restart left cold, on a group a Zen1 host leads and then on
one europa leads, with the order swapped in even rounds:

| Side | What it is |
| --- | --- |
| `cold`, `warm` | 1,000 commits at depth one to rows cold on every replica, then the same rows again, now resident |
| `read-leader` | A `Quorum` get through the leader, then the commit through it |
| `read-follower` | A `One` get through a follower, then the commit through that follower |
| `warm-follower` | The same rows again, committed through that follower with no read: what that path costs a resident row |
| `read-every` | A `One` get through every member, then the commit through the leader |
| depth 32 `cold`, `warm` | Up to 40,000 cold rows over 15 s, then the depth-one rows again, resident |
| neighbour | A writer at depth one over 300 resident rows of the Zen1-led group, for 10 s: alone; beside eight writers of cold rows in the same group; beside eight writers of resident rows in it; beside eight writers of cold rows in another group the same member leads |

Then 1,000 object rows, cold and then warm, in a group a Zen1 host leads.

**Rounds.** Four. The legs run rate, rows, size in odd rounds and the other way in even ones, and
every leg's cells reverse with the round. `results/x10-lab.sh` runs them from europa and writes
the facts every table is labelled with. `x10 report` merges the rounds and judges the triggers,
and its output is `shoal-spike-rows/results/x10-report.md`.

### Where, and on what

| Host | CPU | Device and filesystem | Node |
| --- | --- | --- | --- |
| europa | Ryzen 9 7945HX (Zen4); the node's shards on cpus 0 to 6; the driver pinned to cores 8 to 15 and their siblings | Intel Optane 900P, XFS at `/optane` | `/optane/shoal-x10`; `wal_commit_delay` 3 ms; `lead_weight` 2 |
| titan | Ryzen Embedded V1756B (Zen1), four cores, eight threads | Samsung 970 EVO on one PCIe lane, the XFS volume X6 fitted at `/xfs` | `/xfs/shoal-x10`; `wal_commit_delay` 2 ms |
| hyperion | The same as titan | The same, another unit | The same |

- **The cluster is `tmdb_cluster.yaml`'s**: six shards and 8 GiB a node, a dedicated control
  core, a factor of three, three voters, TLS between peers and SCRAM for clients. It was renamed
  `x10` with ports and roots of its own (`shoal-spike-rows/inventory.yml`). **titan's and
  hyperion's roots are on `/xfs`**, not in a directory of their ext4 root as tmdb's are, so their
  device counters hold the nodes' writes and not the system's. That was decided with the user.
- **1 GbE between the hosts**, 0.12 ms round trip. europa leads about half of each table's 18
  groups (`lead_weight: 2`), and every commit waits on a Zen1 host's sync, since a majority of
  three always includes one.
- **Governor `performance`** on all three hosts for every run, and titan's and hyperion's
  `e2scrub_all` timer held. Both were put back afterwards: `powersave` on europa and `schedutil`
  on the Zen1 hosts, and the timers started. No other shoal unit ran on any host. The `x10`
  cluster was destroyed after every leg.
- **One `znver1` build** of both programs, rustc 1.100.0-nightly (2026-09-04), kernel 7.0.0
  (`-31` on europa, `-34` on titan and hyperion), the glommio fork at `f4643f7`.
- europa is also the development host, and nothing else heavy ran on it during the rounds.
- **No write was refused or failed** in any of the 488 records the rounds and the supplement
  kept, so every conditional write found the sequence its writer knew.

**Where the run departed from the plan on [the spikes page](spikes.md#x10-what-a-stripe-row-costs).**

- The plan's cold rows were "a restart and then overwrites". An overwrite is an insert, and an
  insert never reads the row it replaces (`apply_insert`,
  `shoal-core/src/server/tables/persistent/unsorted.rs`). So an overwrite of a cold row costs
  what any insert does. The cold commit was S7's instead: an update conditional on the sequence
  its writer read, which has to find the row before it can judge the condition.
- The plan derived rows and index memory for each tebibyte written in place, at stripes of 4, 16
  and 64 MiB. T2 needed a node, so the arithmetic is done for a node of 16 TiB as well, and at
  1 MiB too.
- Three additions: a stripe row with a digest of each chunk, because X5 left that question to
  X10; the reads before a commit, from the design review; and a supplement after the rounds, for
  the reason in [5](#5-the-supplement-the-read-under-load).
- titan's and hyperion's roots are on `/xfs`, not tmdb's directory on the ext4 root, as decided
  with the user.

**Three things were changed after the quick run and before the rounds.** A quick run of every leg,
at a tenth of every count and two second windows, was taken first, and it found three defects in
the harness:

- europa's network figure counted every byte twice, once on its link and once on the bridge the
  link is a port of. Only interfaces backed by a device are counted now.
- A resident row's bytes were read after the filler, so they included filler rows. They are read
  right after the load now.
- In the quick run, a commit sent through a follower after a read through it took twice a
  commit through the leader, and nothing told the follower's path from the cold row. So
  `warm-follower` was added: the same rows again, resident, committed through the same follower.
  In the rounds the follower path cost no more than the leader's ([3](#3-the-cold-commit)).

## How to read the tables

Every figure is the median of four rounds, with the lowest and the highest round in brackets.
Latencies are the driver's, from the send to the answer, on europa. "Device B" are bytes the
kernel counted for the devices every storage root is on, over every host together, divided by
the operations of the whole cell, warm-up included. "WAL B a row a replica" are the bytes every
member's WAL wrote for the cell, divided by its rows and by three.

## 1. Rows a second a group

| Rows | Written | Group | Depth 1: rows/s | p50 ms | Depth 32: rows/s | p50 ms | p99 ms | Device B written a row | WAL B a row a replica |
| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| stripe | insert | a Zen1 host | 202 (201–202) | 4.94 (4.93–4.96) | 4,886 (4,873–4,914) | 6.24 (6.18–6.26) | 10.3 (10.2–10.4) | 2,833 (2,814–3,110) | 304 (302–305) |
| stripe | overwrite | a Zen1 host | 202 (201–202) | 4.94 (4.94–4.96) | 4,933 (4,883–4,950) | 6.23 (6.21–6.26) | 10.1 (10.0–10.2) | 2,027 (1,913–2,040) | 300 (297–303) |
| stripe | commit | a Zen1 host | 201 (201–201) | 4.95 (4.94–4.95) | 4,925 (4,871–4,938) | 6.23 (6.19–6.27) | 10.1 (10.1–10.4) | 1,935 (1,884–2,161) | 316 (313–319) |
| stripe | insert | europa | 202 (202–202) | 4.93 (4.93–4.93) | 4,044 (4,031–4,096) | 7.92 (7.80–8.00) | 11.0 (10.4–11.7) | 2,787 (2,711–3,166) | 305 (305–306) |
| stripe | overwrite | europa | 202 (202–202) | 4.93 (4.93–4.94) | 4,106 (4,076–4,112) | 7.75 (7.73–7.81) | 10.4 (10.4–10.6) | 2,220 (2,062–2,395) | 300 (296–302) |
| stripe | commit | europa | 202 (202–202) | 4.93 (4.93–4.94) | 4,073 (4,025–4,130) | 7.85 (7.78–7.98) | 10.4 (10.4–10.5) | 2,083 (2,009–2,106) | 317 (313–319) |
| stripe | insert | every group | 206 (205–207) | 4.78 (4.74–4.79) | 3,430 (3,423–3,455) | 8.96 (8.90–9.02) | 16.6 (16.5–16.8) | 6,383 (6,335–6,477) | 332 (331–332) |
| stripe | overwrite | every group | 208 (206–208) | 4.77 (4.75–4.78) | 3,445 (3,402–3,469) | 8.93 (8.87–9.07) | 16.4 (16.3–16.7) | 6,337 (5,868–6,633) | 328 (325–332) |
| stripe | commit | every group | 207 (206–209) | 4.76 (4.73–4.79) | 3,417 (3,407–3,420) | 9.00 (8.95–9.04) | 16.8 (16.7–16.9) | 7,074 (6,771–8,008) | 345 (339–347) |
| object | insert | a Zen1 host | 202 (201–202) | 4.95 (4.94–4.96) | 4,850 (4,840–4,877) | 6.20 (6.16–6.21) | 10.9 (10.6–12.3) | 3,795 (3,451–4,014) | 433 (431–437) |
| object | overwrite | a Zen1 host | 201 (200–201) | 4.95 (4.94–4.96) | 4,875 (4,803–4,919) | 6.21 (6.18–6.23) | 10.3 (10.2–10.6) | 2,419 (2,391–2,596) | 429 (423–436) |
| object | commit | a Zen1 host | 202 (201–202) | 4.95 (4.94–4.95) | 4,879 (4,770–4,907) | 6.21 (6.18–6.25) | 10.3 (10.1–10.6) | 2,477 (2,368–2,553) | 445 (438–449) |
| object | insert | europa | 202 (201–202) | 4.94 (4.93–4.94) | 4,013 (4,001–4,022) | 7.92 (7.88–8.03) | 11.9 (10.4–13.6) | 3,902 (3,531–3,997) | 432 (432–434) |
| object | overwrite | europa | 202 (201–202) | 4.94 (4.93–4.94) | 4,004 (4,001–4,027) | 8.00 (7.88–8.06) | 11.0 (10.5–11.6) | 2,565 (2,536–3,025) | 430 (422–436) |
| object | commit | europa | 202 (201–202) | 4.94 (4.93–4.94) | 4,001 (3,968–4,039) | 8.00 (7.88–8.09) | 10.8 (10.5–11.4) | 2,698 (2,492–2,760) | 444 (439–446) |
| object | insert | every group | 207 (207–208) | 4.74 (4.72–4.75) | 3,421 (3,346–3,429) | 8.97 (8.94–9.20) | 16.6 (16.6–17.1) | 6,745 (6,690–7,217) | 460 (459–463) |
| object | overwrite | every group | 206 (205–207) | 4.76 (4.72–4.78) | 3,398 (3,365–3,420) | 8.99 (8.91–9.15) | 17.0 (16.8–17.1) | 7,321 (6,797–8,508) | 457 (453–462) |
| object | commit | every group | 208 (207–209) | 4.73 (4.72–4.75) | 3,430 (3,343–3,457) | 8.95 (8.84–9.20) | 16.7 (16.5–17.1) | 6,559 (6,453–7,083) | 474 (467–487) |

**One write at a time costs 4.9 ms, whatever it is.** An insert of a new key, an overwrite of a
resident row and S7's commit of one cost the same within a tenth of a millisecond, on either
host's group. That 4.9 ms is the WAL's commit delay (2 ms on the Zen1 group, 3 ms on europa's)
and a Zen1 host's flush. A majority of three always includes a Zen1 host, so every commit waits
for one. A conditional update judged against a resident row costs nothing the clock can see.

**A group takes about 4,900 small rows a second**, at depth 32, when a Zen1 host leads it, and
about 4,000 when europa does, whose WAL waits 3 ms for more appends. That is one group's commit
rate on this lab, and the half of Q17 a table can answer
([below](#what-a-group-can-carry-q17)).

**Thirty-two writers spread over every group commit fewer rows than thirty-two in one**: 3,430
a second against 4,900, at 9.0 ms against 6.2. They also write 6.3 to 7.3 KB to the devices for
each row, against 1.9 to 3.9 KB. Spread over eighteen groups, each shard of each node syncs its
own small batch (2.2 appends a sync, against 1.7 to 1.9 for one group), and the 970 EVO serves
those flushes one at a time. That is [O61](../appendix/optimizations.md#o61-a-fast-device-syncs-the-wal-in-batches-too-small-to-fill-a-page)'s
finding, seen from the other side: a bucket's commits spread over many groups pay in flushes,
not in cpu.

**An overwrite reads what an insert does not.** At depth 32 in one group an overwrite read 139 to
254 bytes a row from the devices, and an insert of a new key 23 to 49. Both are blind on the
commit path. The read is the compactor's: a merge reads the archived row of every key its
segment changed, and an insert that replaced the row whole throws it away
([O91](../appendix/optimizations.md#o91-a-merge-reads-the-archived-row-an-insert-replaced-whole)).

### What a group can carry (Q17)

One object row, rewritten in a loop by a conditional update after another, at four sizes:

| Inline | Commits/s | p50 ms | p99 ms | Device B written a commit |
| ---: | ---: | ---: | ---: | ---: |
| 0 | 201 (201–201) | 4.95 (4.95–4.96) | 5.41 (5.32–5.53) | 19,204 (18,797–19,976) |
| 4 KiB | 196 (194–197) | 5.07 (5.05–5.12) | 6.42 (5.64–6.71) | 37,378 (36,083–43,286) |
| 64 KiB | 178 (178–179) | 5.35 (5.33–5.35) | 10.7 (10.5–11.0) | 236,164 (235,840–237,848) |
| 1 MiB | 29.9 (29.2–30.3) | 33.2 (32.5–34.1) | 41.7 (39.3–42.5) | 3,583,675 (3,549,796–3,634,692) |

A record the group rewrites whole costs its size in the WAL of every replica at every commit,
and twice that again when it is merged. At 4 KiB the group still commits 196 times a second
for one writer; at 64 KiB, 178; at 1 MiB, 30, with each commit writing 3.6 MB to the devices.
So a bounded record of what a slice missed, kept as a row a placement group, has to stay to a few
KiB, or live in the group's state where it is not rewritten whole. X10 did not measure the second.

## 2. Bytes a row

What 2,000,000 stripe rows, 1,000,000 with digests and 1,000,000 object rows cost each member,
the largest member's figure:

| Figure | Bytes a row |
| --- | ---: |
| A stripe row in the archives, as the members count it | 224 |
| The same with six digests | 288 |
| An object row with nothing inline | 336 |
| A stripe row on disk (`du` of the table's directories) | 272 (271–272) |
| The same with six digests | 333 (333–334) |
| An object row with nothing inline | 383 (382–385) |
| WAL, a stripe row's insert, a replica | 331 (330–331) |
| WAL, a digest row's | 396 (395–396) |
| WAL, an object row's | 468 (467–468) |
| **Index, a cold row: the archive map, at 4.03 million partitions** | **39.4 (39.3–39.4)** |
| Index, a loaded row: the archive map, loaded less the baseline | 39.5 |
| Resident memory, a cold row: the process, cold less the baseline | 204 (201–207) |
| A resident row as the budget counts it (`memory_bytes`), loaded less the baseline | 291 |
| Its entry in the table's partition index | 127 |
| Its entry in the eviction list, once evictable | 67 |
| Resident memory, a resident row: the process, loaded less the baseline | 816 (814–820) |

**A row in the archives is its rkyv form and a partition around it**: 48 bytes for a stripe row,
24 for an object row. The digests cost 64 bytes a row archived, 61 on disk and 65 in the WAL, and
nothing in any index: an index entry is the key's and not the row's.

**A cold row costs 39 bytes of memory a replica**, its archive map entry: 25 bytes a hashbrown
bucket at a fill of about two thirds. That is the index O83 measured at 54.6 bytes a partition on
the cluster testing's dataset, at another point of the map's growth. Nothing else of a cold row
is in memory: after the restart the members held 5 to 43 MB of rows, the filler's tail, and 4 MB
of table index, as at the baseline.

**But the process held 204 bytes a cold row, not 39**, and the difference is the WAL's index. A
group keeps 100,000 entries behind its checkpoint for a slow member, up to 1 GiB of sealed WAL a
shard ([O67](../appendix/optimizations.md#o67-ten-thousand-retained-entries-is-seconds-of-a-busy-group)).
So every write of the load was still in the WAL after the restart: 1.65 to 1.69 GB on disk a node,
and 414 MB of index a node, about 100 bytes an entry. That cost is set by retention and not by
rows. It is at most 10 MB a busy group, and a bucket's two tables are 36 groups a node on the lab
([O90](../appendix/optimizations.md#o90-the-wal-keeps-a-hundred-bytes-of-memory-for-every-retained-entry)).

**A resident row costs about 484 bytes as the engine counts it and 816 as the process does**:
the row as `deep_size` sees it with 17 bytes of partition, its partition index entry, and the
eviction list's entry once it is merged. The process's figure includes the allocator's slack and
the WAL index of the load's entries. The heap profile of round 15 of the cluster testing found
the budget's count a third short, which is about the gap here.

**What a tebibyte written in place costs**, every stripe of it with a row, a replica:

| Stripe | Rows a TiB | Index, cold rows | Memory, resident rows (at 484 B) |
| ---: | ---: | ---: | ---: |
| 1 MiB | 1,048,576 | 39 MiB | 484 MiB |
| 4 MiB | 262,144 | 9.8 MiB | 121 MiB |
| 16 MiB | 65,536 | 2.5 MiB | 30 MiB |
| 64 MiB | 16,384 | 0.6 MiB | 7.6 MiB |

**And what a bucket can hold**: 27 million rows a GiB of archive map a replica, objects and
stripes written in place alike. That is the "twenty million" S3 estimated from fifty bytes,
measured. Every replica of a tablet holds its rows' entries, so at a factor of three over three
nodes each node holds every row's.

## 3. The cold commit

Every row was cold on every replica: the warm sides read nothing from any device, and every cold
commit read about 670 bytes on **each** host. That is one direct read of the row's record,
rounded to the device's 512 byte block. Every replica reads the row it applies, followers as well
as the leader.

### One commit at a time

1,000 commits each, at depth one, to rows of the stripe table:

| Group led by | Side | Commits/s | p50 ms | p99 ms | Read first, p50 ms | The commit after it, p50 ms |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| a Zen1 host | cold | 200 (200–201) | 4.96 (4.94–4.97) | 6.58 (5.46–7.11) | — | — |
| a Zen1 host | warm | 201 (200–201) | 4.95 (4.95–4.96) | 5.45 (5.35–6.39) | — | — |
| a Zen1 host | read-leader | 198 (197–199) | 4.97 (4.95–4.98) | 7.95 (7.83–8.11) | 0.798 (0.775–0.837) | 4.17 (4.13–4.19) |
| a Zen1 host | read-follower | 168 (137–178) | 5.06 (5.00–5.45) | 9.98 (9.87–10.3) | 0.515 (0.502–0.529) | 4.55 (4.51–4.91) |
| a Zen1 host | warm-follower | 198 (193–200) | 4.96 (4.94–4.99) | 7.90 (7.82–10.3) | — | — |
| a Zen1 host | read-every | 185 (177–189) | 5.00 (4.98–5.05) | 9.87 (9.82–10.1) | 1.23 (1.21–1.24) | 3.81 (3.76–3.83) |
| europa | cold | 201 (201–202) | 4.94 (4.94–4.95) | 5.45 (5.28–6.31) | — | — |
| europa | warm | 202 (201–202) | 4.93 (4.92–4.94) | 5.33 (5.25–5.46) | — | — |
| europa | read-leader | 201 (200–202) | 4.94 (4.93–4.95) | 5.83 (5.30–6.61) | 0.521 (0.487–0.581) | 4.42 (4.36–4.47) |
| europa | read-follower | 199 (198–200) | 4.97 (4.94–4.98) | 6.66 (5.43–7.94) | 0.507 (0.502–0.511) | 4.47 (4.44–4.48) |
| europa | warm-follower | 199 (199–200) | 4.95 (4.94–4.97) | 7.85 (5.31–7.96) | — | — |
| europa | read-every | 195 (193–196) | 4.95 (4.95–4.97) | 9.15 (8.51–9.78) | 1.20 (1.19–1.24) | 3.76 (3.73–3.78) |

**At depth one a cold commit costs what a warm one does**: 4.96 ms against 4.95 on the Zen1 host's
group, 4.94 against 4.93 on europa's. The read is a few hundred microseconds on the 970 EVO and
tens on the Optane, and it is spent inside the WAL's commit delay, which a commit at depth one
waits out in full whatever else happens. The object table's commit was the same: 4.98 cold and
4.96 warm.

**A commit after a read costs less than a warm commit on its own**, 3.8 to 4.5 ms against 4.95.
That is not the read helping. A writer at depth one sends its next commit just after the last
one's sync, so it waits out the whole commit delay; a read first moves the commit later into the
delay's window. Only the operation's total says what the read costs: 4.97 ms through the leader,
5.0 through every member, against 4.95.

**A follower's path costs no more than the leader's at the median.** A commit sent through a
follower that had just read the row took 5.06 ms in all, and through the same follower with no
read 4.96. Its tail is longer, cold or warm: a p99 of 6.7 to 10 ms against 5.3 to 5.5 through
the leader.

### Thirty-two at a time

Up to 40,000 cold rows each, for 15 s, then the depth-one rows again, resident:

| Group led by | Side | Commits/s | Over warm | p50 ms | p99 ms |
| --- | --- | ---: | ---: | ---: | ---: |
| a Zen1 host | cold | 3,039 (2,777–3,064) | 0.62× (0.47–0.63) | 10.3 (10.2–11.0) | 19.6 (18.9–23.2) |
| a Zen1 host | warm | 4,889 (4,845–5,951) | 1.00× (1.00–1.00) | 6.21 (5.23–6.26) | 10.4 (8.82–10.4) |
| europa | cold | 3,644 (3,627–3,694) | 0.91× (0.90–0.92) | 8.85 (8.48–8.98) | 13.4 (11.9–14.9) |
| europa | warm | 4,005 (3,997–4,029) | 1.00× (1.00–1.00) | 7.96 (7.92–7.97) | 11.8 (10.4–13.4) |

**Under load a group with cold rows commits fewer**: 0.62× its warm rate when a Zen1 host leads
it, 0.91× when europa does. A batch that needs a row from disk parks, and the group applies
nothing after it until the row is read. Several cold rows in one batch are read one after another,
each resumption handling one parked command (`shoal-core/src/server/shard/groups.rs`,
`run_apply` and `resume_parked`). The leader's read is on the client's path, so the group's
commits queue behind its own reads ([O92](../appendix/optimizations.md#o92-a-group-reads-the-rows-its-parked-batch-needs-one-at-a-time)).

### A neighbour in the group

One writer at depth one over 300 resident rows of the Zen1 host's group, 10 s, beside eight others:

| Beside the writer | Its p50 ms | Its p99 ms | p99 over alone | p99 over beside warm writers | The others' commits/s |
| --- | ---: | ---: | ---: | ---: | ---: |
| nothing | 4.96 (4.95–4.96) | 5.46 (5.38–5.51) | 1.00× (1.00–1.00) | 0.61× (0.59–0.64) | 0 (0–0) |
| eight writers of resident rows in its group | 6.14 (5.09–6.17) | 8.99 (8.62–9.12) | 1.65× (1.56–1.69) | 1.00× (1.00–1.00) | 1,294 (1,284–1,525) |
| eight writers of cold rows in its group | 6.96 (6.82–8.54) | 11.0 (10.8–11.5) | 2.02× (1.99–2.09) | 1.24× (1.19–1.29) | 1,090 (964–1,103) |
| eight writers of cold rows in another group | 5.31 (5.08–5.49) | 7.06 (6.12–8.45) | 1.29× (1.11–1.57) | 0.80× (0.69–0.93) | 971 (854–1,145) |

**A writer beside cold commits in its own group waits twice as long at its tail**: p99 2.02×
its p99 alone, in every round. Most of that is the load and not the reads: beside eight writers
of resident rows its p99 is 1.65×. The cold reads add 1.24× on top of the load (1.19 to 1.29).
Beside cold commits in another group led by the same member it rose 1.29×, and in one round 1.57×.

## 4. An object held inline

Each size had 512 objects written first, then puts of new objects, an even mixture, and gets of
the 512, each at depth 32 for 15 s:

| Inline | Puts/s | Over 1 KiB | Even mixture, ops/s | Over 1 KiB | Gets/s | Puts, MiB/s | Busiest link, puts |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 KiB | 3,267 (3,159–3,343) | 1.00× (1.00–1.00) | 6,280 (6,174–6,401) | 1.00× (1.00–1.00) | 163,640 (142,776–172,346) | 3.19 (3.09–3.26) | 7.11% (7.06–7.51) |
| 2 KiB | 3,156 (3,094–3,223) | 0.97× (0.96–0.98) | 6,107 (6,004–6,194) | 0.97× (0.96–0.97) | 147,231 (129,781–155,301) | 6.16 (6.04–6.30) | 10.5% (10.4–11.1) |
| 4 KiB | 3,020 (2,969–3,076) | 0.92× (0.92–0.94) | 5,738 (5,668–5,855) | 0.92× (0.91–0.92) | 126,836 (109,297–133,866) | 11.8 (11.6–12.0) | 16.8% (16.7–17.9) |
| 8 KiB | 2,764 (2,696–2,807) | 0.85× (0.84–0.86) | 5,286 (5,139–5,357) | 0.84× (0.83–0.84) | 113,872 (95,775–120,507) | 21.6 (21.1–21.9) | 28.0% (27.6–29.4) |
| 16 KiB | 2,057 (1,721–2,404) | 0.63× (0.54–0.72) | 4,156 (3,690–4,405) | 0.66× (0.58–0.71) | 102,746 (89,108–110,111) | 32.1 (26.9–37.6) | 40.8% (36.6–47.9) |
| 32 KiB | 1,455 (993–1,849) | 0.45× (0.31–0.55) | 2,702 (2,008–3,424) | 0.43× (0.32–0.55) | 92,288 (79,457–98,755) | 45.5 (31.0–57.8) | 56.5% (44.9–71.1) |
| 64 KiB | 915 (721–1,133) | 0.28× (0.23–0.34) | 1,793 (1,439–2,156) | 0.29× (0.22–0.35) | 71,547 (62,269–76,452) | 57.2 (45.1–70.8) | 68.9% (54.6–85.5) |
| 128 KiB | 548 (497–616) | 0.17× (0.16–0.18) | 1,063 (981–1,150) | 0.17× (0.15–0.19) | 46,252 (39,690–49,931) | 68.6 (62.2–77.0) | 82.3% (74.6–89.3) |
| 256 KiB | 296 (291–321) | 0.09× (0.09–0.10) | 586 (567–589) | 0.09× (0.09–0.09) | 25,923 (23,301–27,295) | 73.9 (72.7–80.2) | 89.0% (83.7–90.4) |
| 512 KiB | 149 (148–161) | 0.05× (0.05–0.05) | 296 (292–315) | 0.05× (0.05–0.05) | 13,395 (12,416–14,030) | 74.6 (74.0–80.3) | 89.4% (84.9–90.4) |
| 1 MiB | 75.4 (73.0–78.9) | 0.02× (0.02–0.02) | 146 (145–159) | 0.02× (0.02–0.02) | 6,256 (5,905–6,469) | 75.4 (73.0–78.9) | 89.6% (88.9–90.4) |

**The knee is between 16 and 32 KiB.** The even mixture keeps 0.97× of its 1 KiB rate at 2 KiB,
0.92× at 4 KiB and 0.84× at 8 KiB, in every round. At 16 KiB it falls to 0.58 to 0.71, and at
32 KiB to 0.32 to 0.55. Puts alone fall the same way, a little sooner. The network is not why:
the busiest link carried 41% of 1 GbE at 16 KiB and 57% at 32 KiB.

**From 16 KiB, a cell runs slower when a cell that wrote ran just before it.** The mixtures run
puts, mixture, gets in odd rounds and the reverse in even ones. So in odd rounds the mixture
follows the puts, and in even rounds the puts follow the mixture. At 16 KiB puts made 2,382 and
2,404 a second when they ran first and 1,721 and 1,732 when they followed the mixture; the
mixture made 4,405 and 4,403 after the gets and 3,690 and 3,909 after the puts. That is the compactors merging
the previous cell's rows while the next one runs, and it is why the intervals there are wide.
Between sizes the harness waited for the compactors; between mixtures it did not.

**From 128 KiB the network bounds a put.** A leader sends each row to two followers, and europa
also receives every write a client sends it. The busiest link carried 69% of 1 GbE at 64 KiB and
82% at 128 KiB, and puts level at 74 to 75 MiB a second from 256 KiB, the link at 89%. Gets are bound by it from 2 KiB: two of every three cross the
link, from the member a worker is homed on to europa.

**An inline byte costs about six device bytes**: each of three replicas writes it to its WAL and
again to its archives. At 1 KiB a put costs 14 KB of device writes, most of it pages a sync
writes partly empty.

## 5. The supplement: the read under load

T1 fired on its second clause, under load, and the rule written before the run chooses its remedy
from a depth-one figure: if a read through every member first brings the commit within 1.25× of a
warm one, commits are batched around the read. At depth one it did, at 0.77×, but the stall had
not shown there. So after the rounds a supplement of four more asked the same under load. Each
round loaded 3,000,000 stripe rows, about 167,000 of them in the group a Zen1 host led, made them
cold the same way, and ran on them:

- thirty-two writers committing cold rows, with no read, after a `Quorum` read through the group's
  leader, and after a `One` read through every member; then thirty-two committing resident rows;
- the neighbour of [3](#a-neighbour-in-the-group) again, alone and beside eight writers of resident
  rows, of cold rows, and of cold rows each read at the leader first.

| Thirty-two writers | Commits/s | p50 ms | p99 ms | Read first, p50 ms | The commit after it, p50 ms | p99 ms | The commit over warm, p50 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| cold | 2,949 (2,886–3,079) | 10.4 (10.2–10.5) | 19.8 (17.6–22.3) | — | — | — | 1.66× (1.51–1.69) |
| read-leader | 2,749 (2,462–2,842) | 11.2 (10.9–12.9) | 18.3 (16.7–19.8) | 3.87 (3.64–5.00) | 7.49 (7.41–7.76) | 12.1 (11.1–12.8) | 1.20× (1.09–1.25) |
| read-every | 2,938 (2,869–3,057) | 10.8 (10.4–11.0) | 18.2 (16.4–20.5) | 3.73 (3.25–3.82) | 6.98 (6.82–7.05) | 10.6 (10.4–11.3) | 1.12× (1.00–1.14) |
| warm | 4,827 (4,441–4,949) | 6.22 (6.12–6.94) | 10.3 (10.2–11.1) | — | — | — | 1.00× (1.00–1.00) |

| Beside the writer | Its p50 ms | Its p99 ms | p99 over alone | p99 over beside warm writers | The others' ops/s |
| --- | ---: | ---: | ---: | ---: | ---: |
| alone | 4.96 (4.94–4.96) | 5.46 (5.44–5.66) | 1.00× (1.00–1.00) | 0.56× (0.54–0.60) | 0 (0–0) |
| warm | 6.14 (5.88–6.24) | 9.75 (9.29–10.2) | 1.78× (1.66–1.87) | 1.00× (1.00–1.00) | 1,274 (1,247–1,325) |
| cold | 7.26 (6.81–7.93) | 11.3 (10.7–11.8) | 2.04× (1.96–2.16) | 1.18× (1.06–1.22) | 1,073 (1,018–1,112) |
| cold-read-leader | 5.35 (5.33–5.46) | 9.27 (8.86–9.49) | 1.67× (1.63–1.74) | 0.95× (0.87–1.02) | 843 (829–874) |

**After a read at the leader, a commit under load costs 1.20× a warm one** (1.09 to 1.25), and
after a read through every member 1.12× (1.00 to 1.14). Alone it cost 1.66×. The read itself
queues under load, 3.7 to 3.9 ms where it took 0.8 at depth one, and it is the read that waits for
the disk now, not the group: the leader's copy is resident when the commit arrives, so the
leader's apply does not park. The followers still read their copies, and that is what is left of
the 1.20×.

**Beside writers that read first, the neighbour sees no stall**: its p99 is 0.95× (0.87 to 1.02)
its p99 beside the same number of writers of resident rows, where beside writers that did not
read it was 1.18×. The readers committed fewer rows a second than the warm writers, 843 against
1,274, which flatters them a little; at their rate the neighbour's p50 was 5.35 ms against 6.14.

**So the remedy is the one the rule chose, and it holds under load.** The read S7's coordinator
already makes, sent to the group's leader at `Quorum`, keeps the leader's apply off the disk.
Keeping stripe rows resident would buy the remaining 0.2× at 4.1 GiB a node at 4 MiB stripes.
[O92](../appendix/optimizations.md#o92-a-group-reads-the-rows-its-parked-batch-needs-one-at-a-time)
is the engine's half: it would let the followers read in parallel too.

## What would have changed the design

| Result named in advance | Found | So |
| --- | --- | --- |
| **T1.** A commit to a cold row stalls its group for long enough to matter | **Fires, on its second clause.** (i) At depth one a cold commit cost 1.00× a warm one (0.996 to 1.00), on both hosts' groups. (ii) A writer beside eight writers of cold rows in its own group had a p99 of 2.02× its p99 alone (1.99 to 2.09), and beside cold commits in another group 1.29× (1.11 to 1.57), which is not wholly above the line. Beside the line, as stated in advance: cold over warm rows a second at depth 32 was 0.62× on the Zen1 host's group and 0.91× on europa's, and beside writers of resident rows the neighbour's p99 was 1.65×, so the cold reads' own share is 1.24× | Stripe rows are not kept resident. By the rule written in advance, a read through every member first brought the commit to 0.77× a warm one at depth one, within 1.25×, so commits are batched around the read: the one S7's coordinator already makes, sent to the group's leader. The supplement confirmed it under load: 1.20× after a read at the leader, and the neighbour's stall gone ([5](#5-the-supplement-the-read-under-load)) |
| **T2.** The index for a row, times the rows of a tebibyte written in place, exceeds what a node can give | **Does not fire.** A cold row is 39.4 bytes of index. A node of 16 TiB at 4+2 replicates 8.39 million stripe rows at 4 MiB, 0.31 GiB of index: under the 1 GiB line. At 1 MiB it would be 1.23 GiB, over it | The index sets no floor above 4 MiB, which X6's chunk floor already sets at 4+2. Kept resident, the same rows would be 4.1 GiB, which is the cost T1's first remedy would have had |
| **T3.** The inline threshold's knee is far from the benchmark host's | **Fires, at its line.** The even mixture fell below 0.7× its 1 KiB rate at 16 KiB in three rounds and at 32 KiB in the fourth (0.713 at 16 KiB in round four), so by the rule as written the knee is 32 KiB: two octaves above jove's 8 KiB. Every round was below the line at 32 KiB, and none was at 8 KiB | The inline threshold is set from the lab's knee and not the benchmark host's: 16 KiB, the bottom of it |

## The comparison

**Where a stripe row's state lives while nobody writes it:**

| Option | What a node holds a row | A commit to it | Strengths | Weaknesses |
| --- | --- | --- | --- | --- |
| **Cold rows, each commit after S7's read at the leader** (recommended) | 39 bytes of index | 1.00× warm at depth one; 1.20× under load, with the neighbour unaffected | Memory is the index alone, 0.31 GiB for a 16 TiB node at 4 MiB stripes; the read is one the write path already makes | The read waits for the disk under load (3.7 to 3.9 ms on the 970 EVO); followers still read their copies, one at a time a group ([O92](../appendix/optimizations.md#o92-a-group-reads-the-rows-its-parked-batch-needs-one-at-a-time)) |
| Cold rows, the commit alone | 39 bytes of index | 1.00× warm at depth one; 1.66× under load, 0.62× the group's rate, and a neighbour's p99 1.18× | Nothing to do | The leader's apply parks on every cold row, and the group's other writes queue behind it |
| Stripe rows kept resident | 484 bytes as counted, 816 as the process grows | Always warm: 4,900 commits a second a group | No read anywhere, no stall | 4.1 GiB a node of 16 TiB at 4 MiB stripes, growing with every stripe ever written in place; the engine has no way to pin a row against eviction, so it is new work |

**Where the inline threshold sits:**

| Threshold | Puts a second over 1 KiB | Even mixture over 1 KiB | A put's p99 | Device bytes a put |
| --- | ---: | ---: | ---: | ---: |
| 4 KiB | 0.92× | 0.92× | 19.6 ms | 33 KB |
| 8 KiB | 0.85× | 0.84× | 21.9 ms | 59 KB |
| **16 KiB** (recommended) | 0.63× (0.54–0.72) | 0.66× (0.58–0.71) | 37 ms | 115 KB |
| 32 KiB | 0.45× (0.31–0.55) | 0.43× (0.32–0.55) | 58 ms | 221 KB |
| 64 KiB | 0.28× | 0.29× | 91 ms | 413 KB |

## Recommendation

**S3's rows as S3 describes them, with three things fixed.**
[S18](contract.md#q25-in-part-the-metadata-rows-2026-10-04) records them as Q25, in part.

- **Stripe rows are not kept resident.** A stripe row is cold whenever its stripe was last written
  long ago, and that costs a node 39 bytes of index a replica and nothing else. **A stripe's commit
  follows the read of its row that S7's coordinator already makes, sent to the group's leader at
  `Quorum`.** That read leaves the leader's copy resident when the commit arrives, so the leader's
  apply, which the client waits on, never parks. At depth one the operation costs what a warm
  commit does, and under load the commit 1.20× a warm one, with no stall a neighbour can see. The
  write path already reads the row for its sequence and labels
  ([S7](write-path.md#the-preferred-direction-step-by-step)); what this adds is where the read goes.
- **The index sets no floor under the stripe size above X6's.** At 4 MiB stripes, X6's 1 MiB chunk
  floor at 4+2, a node of 16 TiB writing every stripe in place holds 0.31 GiB of stripe row index.
  At 1 MiB it would be 1.23 GiB, which the chunk floor rules out anyway.
- **A pool's inline threshold defaults to 16 KiB**, the bottom of the knee on the lab. An inline
  object at 16 KiB is put at 0.54 to 0.72 of a 1 KiB object's rate and with 115 KB of device writes,
  against 0.31 to 0.55 and 221 KB at 32 KiB. It stays a setting of the pool, and zero turns it off
  ([S3](objects.md#small-objects-stay-inline)).
- **A bucket can hold about 27 million rows a GiB of archive map a replica**, objects and stripes
  written in place alike. That is the scale Q25 asks for, until the map is paged
  ([S1](prerequisites.md#optional)). Beside it, a busy group can hold up to 10 MB of WAL index
  ([O90](../appendix/optimizations.md#o90-the-wal-keeps-a-hundred-bytes-of-memory-for-every-retained-entry)).
- **A commit's condition is equality on one field**: a stripe row's sequence, an object row's
  version. F68's conditions need nothing more for these rows.
- **The rows' sizes, for M12 to hold the generated rows to**: a stripe row of six labels is 176 bytes
  archived (224 in its partition), and six chunk digests add 64. An object row of one entry, with a
  64 byte path and nothing inline, is 312 (336). A group commits about 4,900 of them a second on
  the lab.

## What X10 does not settle

- **The rows as M12 generates them.** These were stand-ins with the fields S3 names. M12's exit
  takes X10's figures again on the generated rows as built, against this page's: 224 and 336
  bytes archived, 39 bytes of index, 4,900 commits a second a group, a cold commit 1.00× at depth
  one and 0.62× at depth 32.
- **Whether the stripe table is shared between buckets**, which [S2](buckets.md#what-it-costs) asks
  under Q25. X10 adds two facts for it: commits spread over more groups pay in flushes (3,430 a
  second over eighteen groups against 4,900 in one), and every busy group can hold up to 10 MB of
  WAL index. Both favour fewer groups; the decision is M12's.
- **What a group's own state costs** to carry Q17's record of what a slice missed, as opposed to a
  row rewritten whole. The checkpoint holding it is rewritten whole
  ([C5](../distributed/replication.md#limitations)), and X10 measured no group state.
- ~~**Small writes in the metadata log**, Q27's other half: [X8](spikes.md#x8-one-small-write-three-ways).~~
  Recorded by [X8](small-writes.md): below 64 KiB on a device that flushes, once a slice shares
  one flush among its applies, so the stripe row carries a field of pending bytes; a threshold of
  writes in place, and another from this page's threshold of whole objects held inline.
  X10's inline cells are puts of whole objects, not writes in place.
- **The stall on a node whose rows were evicted rather than restarted.** Eviction needs memory
  pressure and leaves the archive map as a restart does, so the read is the same; X10 made rows
  cold by restarting every node and did not measure eviction.
- **Thousands of groups, or millions of rows a group.** The lab's tables are eighteen groups each,
  and a group held about 110,000 rows.

## What it did not measure

- **A coordinator's hop.** Every write was sent to its group's leader. A client of today's Shoal
  sends a write to any member, which forwards it to the leader, and that hop is not in any figure
  here.
- **Reads of cold rows as the object store's read path makes them**, beyond the read before a
  commit: [S9](read-path.md)'s lookup of a stripe row is priced only by the read-first cells.
- **Write amplification as X3 asks for it.** The WAL and the archives shared a device on every host,
  and the cells were too short to seal many segments. The load's device bytes a row, 5.1 to 6.5 KB
  over three hosts, include its merges, and nothing finer is claimed.
- **Other hardware**: a link faster than 1 GbE, an SSD with power-loss protection on the Zen1 hosts,
  or a commit delay other than the lab's.
- **Crashes.** Nothing was killed.

## What it found in the engine

None of these is a defect, so none is filed on [Known Issues](../appendix/known-issues.md). Three are
filed as optimizations.

| Finding | Where |
| --- | --- |
| Every replica reads a cold row to apply a write to it: 670 bytes on each host a commit. A follower's read delays its own apply and not the commit | `ApplyStep::NeedsLoad`, `shoal-core/src/server/tables/persistent/unsorted.rs`; `run_apply`, `shoal-core/src/server/shard/groups.rs` |
| A group reads the rows a parked batch needs one at a time, so cold commits queue behind each other under load: 0.62× a warm group's rate on the 970 EVO | [O92](../appendix/optimizations.md#o92-a-group-reads-the-rows-its-parked-batch-needs-one-at-a-time) |
| The WAL keeps about 100 bytes of memory for every entry it retains, 414 MB a node after four million writes, set by retention and not by rows | [O90](../appendix/optimizations.md#o90-the-wal-keeps-a-hundred-bytes-of-memory-for-every-retained-entry) |
| A merge reads the archived row of every key its segment changed, even one an insert replaced whole: 139 to 254 device bytes an overwritten row against 23 to 49 a new one | [O91](../appendix/optimizations.md#o91-a-merge-reads-the-archived-row-an-insert-replaced-whole) |
| Nothing times a parked apply. `stage-profile`'s `set_loaded_from_disk` is never called, and the loader's spans start traces of their own, so a write's trace does not show its read | [TODOs](../appendix/todos.md#time-a-parked-apply) |
| A commit at depth one waits out the whole WAL commit delay, so anything before it in the same operation, a read included, comes out of the delay and not on top of it | `cluster.replication.wal_commit_delay`, [O61](../appendix/optimizations.md#o61-a-fast-device-syncs-the-wal-in-batches-too-small-to-fill-a-page) |

## Related

- [X10](spikes.md#x10-what-a-stripe-row-costs) for what was planned.
- [S3](objects.md) for the rows, now with their measured costs.
- [S7](write-path.md) for the commit and the read before it.
- [S18](contract.md#q25-in-part-the-metadata-rows-2026-10-04) for the decision.
- [X6's record](device-store-ssd.md) for the form this page copies, and the chunk floor T2 is read
  against.
- [Row size and what it costs](../tables/row-size.md) for the benchmark host's knee.
- [C5](../distributed/replication.md) for the apply that parks.
