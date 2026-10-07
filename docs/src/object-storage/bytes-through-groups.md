# X3. Bytes through the tablet groups, measured

**Reported 2026-10-07.** This is the record of spike [X3](spikes.md#x3-bytes-through-the-tablet-groups).
It drove a stripe's bytes as rows of a persistent unsorted table, S7's candidate A, through
today's engine with no new code, at 64 KiB, 256 KiB, 1 MiB and 4 MiB a row, on four legs:

- the lab's three hosts at a factor of three, over 1 GbE;
- three nodes on europa at a factor of three, over loopback, sharing its Optane;
- one node on titan and one on europa, a factor of one, for scale.

It measured puts, gets, an even mixture and overwrites, each with a small table's stream beside
it; the device bytes every write caused once its merges had finished; and each node's memory and
cpu. Four rounds, then a supplement that profiled the put arm. It ends in a recommendation, which
[S18](contract.md#q14-in-part-the-cost-of-stripes-as-rows-2026-10-07) records as Q14, in part:
**keep B as the preferred direction, and do not make replicated SSD pools tables.** One of the two
triggers named in advance fires and the design moves only if both do.

Four facts decide it:

- **A writes a byte about twice, as expected.** Settled, every host of both factor-three legs
  wrote 2.03 to 2.05 device bytes a byte stored a copy at 4 MiB and 2.06 to 2.40 at 1 MiB, once
  to a WAL and once to an archive. T2 fires at 4 MiB and straddles its line at 1 MiB, by one host
  in one round that [item 215](../appendix/known-issues.md#215-a-replication-lane-busy-with-wide-rows-is-judged-silent-and-refuses-forwarded-writes)'s
  refusals had made write rows twice.
- **It is several times short of the device.** Three nodes on one Optane stored 0.27 of its rate
  a copy at 1 MiB and 0.20 at 4 MiB, in every round, against T1's line of 0.5. T1 does not fire.
- **Its nodes' cpu is the bound, and the cpu goes to copies.** A byte stored at a factor of three
  cost about nine times the cpu it cost on one node. 58 to 67% of a node's cpu went to copying
  the bytes and zeroing the pages they were copied into, and a tenth to Shoal's own code
  ([O95](../appendix/optimizations.md#o95-a-wide-rows-bytes-are-copied-and-faulted-in-on-every-write)).
- **A table beside A's rows waits for them.** A small table's reads, 2 ms at the p99 alone, took
  126 to 165 times that beside the put arm on the loopback leg and 9 to 15 times on the lab, and
  its writes up to a second on loopback and 1.4 to 2.9 s on the lab, behind the stripes in the
  executors and WAL batches they share.

It found two defects: a replication lane busy with wide rows over a full 1 GbE link is judged
silent and refuses writes forwarded to its leader (item 215), and a node written faster than it
merges holds every unmerged row past its memory budget, 23.0 GiB on 6
([item 216](../appendix/known-issues.md#216-writes-faster-than-a-nodes-merges-hold-it-past-its-memory-budget)).
On the lab A took 57 to 62 MiB/s of rows at every size, with 1 GbE the bound, as it is for any
pool there.

## The question

[Q14](contract.md#questions-to-answer) asks what orders a stripe's writes and how the bytes stay
out of the log. [S7](write-path.md#the-candidates) prefers B, where holders stage a stripe's
chunks and one conditional commit of its row decides, and keeps A, **stripes are rows**, as the
baseline every measurement is read against: a stripe's bytes are a row of a generated table,
replicated by its tablet group like any other row. A needs no device store, no pool and no
protocol of its own, and gives up erasure coding, storage pools and devices altogether. Every
byte passes through the WAL, the archives and the compactor, and a 4 KiB change rewrites the
stripe at the next merge. [X1](stripe-model.md) settled Q14's safety; its cost is X3's and X8's.

X3 asks what A costs today, with no new code in the engine, at the sizes a stripe has: 64 KiB to
4 MiB. The spike's section named the result in advance that would change the design:

- **A within reach of the devices' own rate for a replicated pool, with a write amplification
  near two.** Then a data plane is worth building for erasure coding and rotational disks only,
  and replicated SSD pools are tables.
- The opposite, A several times short, is what the preferred direction assumes, and it had never
  been measured on a cluster at these sizes.

## How it was judged

The lines below were set before the harness existed and agreed with the user on 2026-10-07.
`x3 report` judges them as written. Every figure is an interval over four rounds: the lowest and
the highest round, with the median between. A ratio is taken round by round and judged by where
its whole interval lies, and a difference counts only where two intervals do not overlap, which is
[the lab's rule](../performance/benchmarking.md#before-and-after-on-the-lab).

| Trigger | Fires when |
| --- | --- |
| **T1. A is within reach of a replicated pool's rate** | On europa's three loopback nodes at a factor of three, the put arm (inserts of new rows) at 1 MiB and at 4 MiB acknowledges at least **0.5×** the pool's ceiling in every round. The ceiling is the Optane's own sequential write rate at that size, from fio in the same round, over the three copies the one device holds |
| **T2. Write amplification near two** | The preload's device bytes written for each byte stored a copy, read once every merge of it has finished, is at most **2.5** at 1 MiB and at 4 MiB, on every host of every factor-three leg, in every round |

The design moves only if **both** fire. Half the ceiling is what an amplification of two allows
at best: a device that writes every byte twice takes half its rate in rows. Beside the triggers,
reported and not judged:

- every other size and arm, and the rate sustained once the merges are counted in;
- the lab's rate against what 1 GbE carries, and one node's against its own device;
- the WAL's bytes and the archives' apart;
- the resident set, the rows a node holds and the WAL's index;
- the small table's p99, and its worst second's p99, against the same stream alone;
- the driver's cpu, each node's cpu and each device's busy time, which say what bound a rate.

## What was run

### The harness

`shoal-spike-bytes` is a workspace crate of its own, built the way X10's `shoal-spike-rows` is: a
schema in its library, a node program, `x3-node`, and a driver, `x3`, which also carries every
`shoaladm` command for the schema. It adds no crate to the workspace's lockfile, only its own
package, and like every spike's code it is thrown away.

**The rows.** `StripeRow { key: u64, bytes: Vec<u8> }`, a persistent unsorted table: a stripe's
identity and its whole contents, written whole as A writes a stripe. `SmallRow { key, value,
note }`, about 100 bytes, is the small table beside it. A row's bytes are SplitMix64 in counter
mode from its key and the write that made it, made on the stream that sends them, as
[X13](benchmark-shape.md) found a benchmark's object bytes should be; an overwrite sends bytes
that differ from the row's old ones.

**The driver is shoal-loadgen's own.** Every arm is a `Driver` handed operation kinds
([F69](../features/driver-operation-kinds.md)), with no dataset table at all:

| Kind | One operation |
| --- | --- |
| `put` | An insert of a new row, its key from the operation's seed in the top half of `u64`, where no preloaded key is |
| `get` | A get of a preloaded row, chosen evenly; an answer without the row is a miss |
| `overwrite` | An insert over a preloaded row with new bytes: a stripe rewritten in place |
| `small_get`, `small_put` | A get, and an overwrite, of one of 10,000 small rows |

The main load is six streams spread over every member, as today's client spreads them, each
keeping its share of the depth outstanding at a bundle of one. **The depth** holds about 64 MiB
of rows in flight: 512 operations at 64 KiB, 256 at 256 KiB, 64 at 1 MiB and 16 at 4 MiB. **The
paced stream** is a second `Driver` on the same clock, half `small_get` and half `small_put` at
50 operations a second offered, a stream a member, timed from each operation's slot
([F72](../features/bench-paced-stream.md)'s pacing), **on connections of its own**: another
table's user does not share a client with the stripes. No failure is retried; every one is
counted, with its code, where it happened.

`shoaladm bench` was not used, decided with the user before the harness was written. It cannot
place three nodes on one host, since an inventory's ports, unit and remote directory are a
deployment's. It reads rows only from a dataset file, and keeps 4,096 parsed rows ahead whatever
their size, which is 16 GiB at 4 MiB
([item 214](../appendix/known-issues.md#214-a-bench-feed-keeps-4096-rows-ahead-whatever-their-size)).
And it reads a run's device counters when the run's last answer is in, before the merges that
run caused have finished ([F71](../features/bench-device-memory.md#limitations)). The driver is
the bench's, so every latency and byte here is counted the way a capture counts it.

**A leg at a row size** is a cluster brought up for it and taken down after, on which `x3 spike`
runs in order:

1. The leaders are let settle for 20 quiet seconds, and the small rows are written.
2. **preload**: three times a node's memory budget of stripe rows, keyed `0..rows`, by the
   spike's own closed loop at the same depth. Then **every merge is let finish**: no shard holds
   a segment handed to a compactor, and the WAL's segments have not fallen for 10 s. The device
   bytes the preload caused, read before it and after that, over the bytes it stored and the
   copies kept, are **T2's figure**.
3. **put**, **get**, **mix** (half gets, half puts) and **overwrite**, in that order in odd
   rounds and the other way in even ones, each 10 s of warm-up and 30 s measured with the paced
   stream beside it. Every arm that writes is let settle before the next, so no arm is charged
   another's merges, which X10 saw happen
   ([X10](stripe-row-costs.md#4-an-object-held-inline)). The put arm's acknowledged bytes a
   second are **T1's figure**.
4. **alone**: the paced stream with nothing beside it, for 10 s and 30 s, its baseline.

Around every step the driver reads every host's device counters through F71's script, which
says which device each root is on; every host's network counters on its physical interfaces;
every member's WAL counters (bytes, batches and appends) from its `Replication` report; and every
member's process cpu through its unit. Every five seconds while a step runs it samples every
member's resident set, the rows it holds, its archive map's and WAL's index bytes, and every
host's available memory. **Sustained** is a step's stored bytes over its whole length and the time
its merges then took. `x3 fio` measures every device's own sequential write rate at the start of
every round, through io_uring, direct, eight in flight, for 30 s at each of the four sizes, on
the filesystem each root is on.

**Europa's three local nodes** are `x3 local up`, which does for three nodes on one host what
`shoaladm bootstrap` does for three hosts, step for step. Each node's configuration is shoaladm's
own render of the loopback inventory, given what only a shared host needs:

- its own loopback address (127.0.0.11, .12 and .13) for every listener, on one set of ports;
- a control core of its own;
- its own directory for its leaf.

Each is claimed by the node program as `shoal`, issued a leaf naming the id its claim printed,
and run in a transient unit with the deployed unit's limits. The first runs alone until it leads
a control group of one, then the other two join, then `Initialize` places every tablet. What it
started is saved as the inventory's deployment record, so the leg's cluster is opened exactly as
a deployed one is.

**Rounds.** Four. `results/x3-lab.sh` runs fio, then the legs lab, loopback, titan and europa,
each at 64 KiB, 256 KiB, 1 MiB and 4 MiB, in that order in odd rounds and the other way in even
ones. After every leg at a size it takes the cluster down, trims every device, and rests 30 s.
`x3 report` merges the rounds and judges the triggers, and its output is
`shoal-spike-bytes/results/x3-report.md`.

### Where, and on what

| Leg | Hosts and nodes | Devices and roots | Factor |
| --- | --- | --- | --- |
| **lab** | europa (Zen4), titan and hyperion (Zen1): `tmdb_cluster.yaml`'s nodes, six shards and 8 GiB each, a dedicated control core, europa's `lead_weight` 2, WAL commit delays of 3 ms on europa and 2 ms on the others | europa: one root on the Optane 900P, XFS, `/optane/shoal-x3`. titan and hyperion: the WAL root on the 970 EVO's XFS volume `/xfs`, the archive root on a second XFS volume on the same device, `/x3-archives`, made for X3 and removed after | 3 |
| **loopback** | Three nodes on europa: each six shards on three physical cores and their siblings, a control core of its own (cpus 1, 5 and 9), 6 GiB; core 0 left to the system | One root each on the Optane, `/optane/shoal-x3-local/<node>/data` | 3 |
| **titan** | One node of the lab's Zen1 shape on titan | As on the lab | 1 |
| **europa** | One node on europa, its six shards on the cpus loopback's first node has, its control thread on cpu 0, 6 GiB | One root on the Optane | 1 |

- **The driver ran on europa** for every leg, pinned to cores 8 to 15 and their siblings on the
  lab and titan legs, clear of europa's node, and to cores 13 to 15 and their siblings on the
  loopback and europa legs. The lab and titan legs cross 1 GbE between hosts, 0.12 ms round
  trip.
- **The archive volume on titan and hyperion** was an 80 GiB logical volume in their volume
  group's free space, XFS, mounted for the run and never put in fstab: a device of its own to the
  kernel, on the same 970 EVO as `/xfs`, so `/proc/diskstats` counts the WAL's bytes and the
  archives' apart. `x3-lab.sh setup` made it and `teardown` removed it, and every change is in
  `shoal-spike-bytes/results/x3-host-changes.txt`. europa has no free space in its volume group,
  so its WAL's bytes are told from the rest by the members' own WAL counters.
- **Governor `performance`** on all three hosts for every run, and titan's and hyperion's
  `e2scrub_all` timer held; both were put back afterwards, `powersave` on europa and `schedutil`
  on the Zen1 hosts. No other shoal unit ran on any host.
- **One `znver1` build** of both programs, rustc 1.100.0-nightly (2026-09-04), kernel 7.0.0-34 on
  all three hosts, the glommio fork at `f4643f7`. The tree was `7345cfa` with the spike
  uncommitted, as `x3-facts.txt` says.
- europa is also the development host, and nothing else heavy ran on it during the rounds.

**Where the run departed from the plan on [the spikes page](spikes.md#x3-bytes-through-the-tablet-groups).**

- The plan's driver was `shoaladm bench`. It was shoal-loadgen's own driver, which the bench
  runs, handed kinds by the spike, for the reasons [above](#the-harness), decided with the user.
  The paced stream is F72's pacing in that driver, not the bench's `--paced`.
- The plan put "a node's two roots on separate devices". On titan and hyperion they are separate
  devices to the kernel and one SSD underneath, decided with the user: a second disk would have
  put the archives on a rotational one and measured another pool. europa's two roots share its
  Optane.
- Two arms were added: **overwrite**, a stripe rewritten in place, which is what a write in place
  through A is, and the merge's read of the row it replaces
  ([O91](../appendix/optimizations.md#o91-a-merge-reads-the-archived-row-an-insert-replaced-whole))
  applies to; and **alone**, the paced stream's baseline.
- "Against one node" became two legs, one node on titan and one on europa, each read against the
  factor-three leg its node's shape matches.
- europa's local nodes have 6 GiB each, not the lab's 8, so three nodes, their page cache and the
  driver fit europa's 43 GiB; the one-node europa leg has the same.
- The preload is three times a node's memory budget, so gets are not all answered from memory.

**Seven things were changed after trying the harness and before the rounds**, each from a first
run of a leg, the quick run of every leg at a tenth of every count, or the first full lab cells
of a round started and stopped for the last two:

- **Bytes stored are counted over a whole arm.** The first loopback trial divided the device
  bytes of a whole arm, warm-up included, by the bytes stored in its measured window alone, and
  read 2.98× where the preload, counted whole, read 2.05×.
- **The paced stream has connections of its own.** The first lab trial drove it through the
  main load's clients, so its small answers waited behind stripes on the same sockets, which is
  [X11's](streamed-bodies.md) finding about connections and not a node's.
- **Every failure keeps its code.** The quick run's lab leg at 256 KiB failed 316 operations, and
  the records held counts and no reason.
- **Each member's cpu and each device's busy time** are read around every step, so a rate below
  its ceiling says what it ran out of.
- **A settle is timed to when its backlog cleared**, not to the end of the quiet ten seconds it
  is then watched for.
- **A settle waits for every copy to have applied alike.** The first full round's lab leg at
  256 KiB counted 3,668 of its gets as misses. Some were of rows a follower had been
  acknowledged past and not yet applied, which a read at `One` through it answers as missing.
- **The preload sends a refused row again until it is written**, for up to two minutes, counting
  every try. The rest of those misses were rows the preload had given up on after eight tries:
  at 256 KiB on the lab it retried 33,718 times and gave up on 3,471 rows of 98,304, each refused
  as [item 215](../appendix/known-issues.md#215-a-replication-lane-busy-with-wide-rows-is-judged-silent-and-refuses-forwarded-writes) says. So a get measured a preload's hole and not a read. A quick run of that
  cell then preloaded every row with 2,316 retries, read every one back through every member at
  `One` and at `Quorum`, and missed none. That round was stopped and the four rounds started
  again from the first.

**The depth was checked** as the plan asked: the loopback leg at 1 MiB, run whole twice at the
rule and twice at double it, alternating. Puts did 215 and 223 MiB/s at the rule and 209 and 213
at double it, 0.96×, and every other arm the same or slower, so the rule was kept.

## How to read the tables

Every figure is the median of four rounds, with the lowest and the highest round in parentheses;
a figure without them was the same in every round. Rates are rows acknowledged to the driver,
in MiB of row bytes a second, over the measured 30 s of an arm. **Sustained** is a step's stored
bytes over its whole length, warm-up included, and the time its merges then took to finish.
"Over ceiling" is a rate over a device's own sequential write rate in the same round, divided by
the copies the device holds. Device bytes are what the kernel counted for the devices the roots
are on, from before a step until its merges finished, over the bytes the step stored and the
copies kept: **2.0 is every byte written once to a WAL and once to an archive**. The tables in
full, every arm at every size, are `shoal-spike-bytes/results/x3-report.md`; the ones here are
cut from it and from `x3.json`.

## 1. Bytes a second, and T1

### Loopback: the pool's own devices and cores

| Size | Put MiB/s | Ceiling a copy, MiB/s | Put over ceiling | Sustained MiB/s |
| --- | --- | --- | --- | --- |
| 64 KiB | 250 (247–263) | 813 (784–830) | 0.313 (0.303–0.318) | 205 (198–223) |
| 256 KiB | 243 (237–263) | 814 (782–830) | 0.298 (0.292–0.330) | 199 (194–222) |
| 1 MiB | 220 (218–221) | 820 (795–832) | 0.268 (0.262–0.278) | 179 (175–181) |
| 4 MiB | 165 (163–167) | 822 (791–829) | 0.202 (0.199–0.206) | 136 (133–137) |

**T1 does not fire.** At a factor of three on one Optane, the put arm acknowledged 0.27 of the
device's rate a copy at 1 MiB and 0.20 at 4 MiB, in every round, against a line of 0.5. A writes
every byte about twice ([3](#3-bytes-written-for-each-byte-stored-and-t2)), so 0.5 was the most it
could have reached; it reached about half of that at 1 MiB and two fifths at 4 MiB. Counted with
the time its merges took after, the sustained rate is lower again: 179 MiB/s at 1 MiB, 0.22 of
the ceiling. The Optane was 47 to 75% busy throughout. Nothing failed: no write was refused and
no get missed in any of the loopback leg's 80 steps.

**What bounds it is the nodes' cpu.** Each of the three nodes kept 4.1 to 4.9 of its seven cpus
busy, and between them they stored 16 MiB/s of rows a core at 1 MiB. The same node alone, on the
same cpus with the same Optane, stored 150 MiB/s a core:

| Leg | Size | Cores busy a node, of seven | MiB/s stored a core | The Optane busy |
| --- | --- | --- | --- | --- |
| loopback | 64 KiB | 4.74 (4.59–4.85) | 17.6 (17.0–19.1) | 0.721 (0.711–0.748) |
| loopback | 256 KiB | 4.84 (4.43–4.94) | 16.7 (16.0–19.8) | 0.691 (0.684–0.725) |
| loopback | 1 MiB | 4.59 (4.54–4.66) | 16.0 (15.6–16.2) | 0.627 (0.614–0.638) |
| loopback | 4 MiB | 4.12 (4.07–4.25) | 13.4 (12.8–13.7) | 0.477 (0.473–0.483) |
| europa | 64 KiB | 5.24 (4.92–5.48) | 164 (155–174) | 0.732 (0.711–0.748) |
| europa | 256 KiB | 5.43 (5.39–5.53) | 176 (169–179) | 0.687 (0.670–0.758) |
| europa | 1 MiB | 5.52 (5.51–5.55) | 150 (149–152) | 0.593 (0.590–0.606) |
| europa | 4 MiB | 5.07 (4.73–5.34) | 116 (106–130) | 0.611 (0.499–0.752) |

So storing a byte at a factor of three cost about nine times the cpu that storing it
on one node did: three copies, each costing about three times a single node's write, since a
copy is also sent, received, forwarded and agreed on. [5](#5-what-the-cpu-went-to) says where it
went.

### One node

The europa leg's single node, a factor of one on the cpus one of the loopback nodes has, took
827 MiB/s of rows at 1 MiB and 967 at 256 KiB: 0.34 to 0.40 of the Optane alone, two thirds to
four fifths of what writing every byte twice allows. It too kept 5.1 to 5.5 of its seven cpus
busy, and the Optane 59 to 73% busy. At 4 MiB it fell to 586 MiB/s, and the loopback leg to 165,
the cpu a byte stored up by about 30% on both ([5](#5-what-the-cpu-went-to)).

### The lab: 1 GbE

| Leg | Size | Put MiB/s | Sustained | Preload MiB/s | p50 ms | p99 ms | Failed | Preload retries |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| lab | 64 KiB | 59.4 (54.6–71.0) | 49.6 (45.0–57.3) | 59.2 (52.8–71.9) | 124 (108–307) | 3,463 (2,099–3,598) | 0 | 0 |
| lab | 256 KiB | 56.8 (54.5–63.7) | 49.7 (47.5–53.5) | 55.1 (48.9–59.8) | 181 (160–204) | 4,348 (3,135–4,588) | 714 (9.00–1,800) | 23,800 (21,246–25,467) |
| lab | 1 MiB | 58.4 (47.6–66.9) | 50.5 (42.0–54.8) | 52.1 (43.4–57.2) | 364 (240–430) | 4,524 (4,407–4,657) | 98.5 (10.0–247) | 3,074 (2,571–4,133) |
| lab | 4 MiB | 61.9 (60.1–75.2) | 52.1 (51.8–61.5) | 61.3 (57.1–71.2) | 893 (878–955) | 3,545 (2,353–3,928) | 0 (0–8.00) | 6.00 (5.00–67.0) |
| titan | 64 KiB | 104 (102–106) | 91.9 (89.3–95.7) | 112 (111–112) | 290 (287–298) | 656 (642–684) | 0 | 0 |
| titan | 256 KiB | 109 (105–111) | 90.8 (89.9–99.1) | 112 (112–112) | 586 (577–595) | 1,157 (1,110–1,169) | 0 | 0 |
| titan | 1 MiB | 108 (105–111) | 93.5 (90.3–99.1) | 112 (112–112) | 589 (580–599) | 1,110 (1,062–1,205) | 0 | 0 |
| titan | 4 MiB | 112 | 96.8 (93.8–100.0) | 112 (112–112) | 647 (646–651) | 788 (722–899) | 0 | 0 |

On the lab, A at a factor of three took 57 to 62 MiB/s of rows at every size, with the busiest
link at 90 to 93% of 1 GbE: each byte stored crosses a link at least twice, from its leader to its
two followers, and two writes in three cross once more, from the member a client sent it to, to
its group's leader. titan's one node took 104 to 112 MiB/s, its link full the other way. The
link is the bound, and it bounds any pool on the lab the same way: B sends a byte to each of its
holders too.

**At 256 KiB and 1 MiB the lab refused writes.** The preload of 256 KiB rows was sent again
21,246 to 25,467 times a round, and the put arm failed 9 to 1,800 writes; at 1 MiB, 2,571 to 4,133
and 10 to 247. Every refusal but a few shed at admission was `NotLeader` or `OutcomeUnknown`, naming
a replication lane that "has answered nothing" for 1.5 s while it carried rows at the link's
rate. That is the silence bound [Resolved #143](../appendix/resolved/silent-partition-hops.md)
set for a partition, met by a lane that was busy, filed as
[item 215](../appendix/known-issues.md#215-a-replication-lane-busy-with-wide-rows-is-judged-silent-and-refuses-forwarded-writes).
The small table's writes were refused with them: 75 at 256 KiB and 13 at 1 MiB. At 64 KiB nothing
was refused, and at 4 MiB a few: 5 to 67 preload retries and up to 8 writes an arm. The tail on
the lab is a queue: 64 MiB in flight through a link carrying 60 MiB/s of rows is a second's wait,
and a put's p99 was 3.5 to 4.5 s at the median.

## 2. Gets, a mixture and overwrites

| Leg | Size | Get MiB/s | Get p99 ms | Device read a byte answered | Mix MiB/s | Overwrite MiB/s | Overwrites failed |
| --- | --- | --- | --- | --- | --- | --- | --- |
| loopback | 64 KiB | 2,920 (2,890–2,946) | 60.8 (59.4–62.2) | 0.786 (0.779–0.790) | 470 (468–486) | 220 (216–225) | 0 |
| loopback | 256 KiB | 3,275 (3,268–3,279) | 108 (105–111) | 0.717 (0.715–0.720) | 457 (448–477) | 217 (210–219) | 0 |
| loopback | 1 MiB | 3,102 (3,074–3,112) | 73.7 (61.4–74.0) | 0.762 (0.760–0.768) | 406 (404–413) | 200 (196–205) | 0 |
| loopback | 4 MiB | 2,772 (2,758–2,788) | 61.6 (58.8–62.3) | 0.747 (0.739–0.749) | 310 (307–312) | 156 (153–158) | 0 |
| europa | 64 KiB | 2,700 (2,368–2,892) | 55.0 (52.5–57.8) | 0.753 (0.745–0.767) | 1,195 (1,148–1,232) | 820 (771–859) | 0 |
| europa | 256 KiB | 3,067 (2,926–3,141) | 107 (103–136) | 0.769 (0.755–0.797) | 1,371 (1,314–1,395) | 914 (905–933) | 0 |
| europa | 1 MiB | 2,997 (2,907–3,015) | 88.1 (85.3–99.9) | 0.794 (0.791–0.815) | 1,229 (1,224–1,241) | 803 (800–807) | 0 |
| europa | 4 MiB | 2,659 (2,241–3,078) | 102 (52.4–152) | 0.890 (0.741–1.05) | 932 (930–945) | 561 (559–566) | 2.00 (2.00–3.00) |
| lab | 64 KiB | 3,032 (3,015–3,044) | 227 (223–234) | 0.770 (0.765–0.779) | 118 (107–142) | 59.3 (53.7–71.4) | 0 |
| lab | 256 KiB | 3,256 (3,251–3,264) | 453 (429–455) | 0.745 (0.720–0.746) | 123 (111–133) | 57.2 (55.3–65.4) | 678 (2.00–1,669) |
| lab | 1 MiB | 3,231 (3,224–3,242) | 457 (452–470) | 0.763 (0.744–0.767) | 118 (107–136) | 59.3 (47.3–68.9) | 87.5 (0–226) |
| lab | 4 MiB | 2,961 (2,937–2,963) | 498 (493–505) | 0.740 (0.731–0.743) | 126 (122–138) | 63.0 (58.1–71.7) | 1.50 (0–8.00) |
| titan | 64 KiB | 112 (112–112) | 2,971 (381–8,147) | 0.829 (0.813–0.850) | 214 (213–216) | 101 (96.7–105) | 0 |
| titan | 256 KiB | 112 (112–112) | 11,272 (734–12,927) | 0.821 (0.794–0.854) | 219 (217–219) | 102 (99.0–104) | 0 |
| titan | 1 MiB | 112 (112–113) | 7,862 (6,267–9,085) | 0.827 (0.794–0.851) | 214 (205–215) | 104 (101–112) | 0 |
| titan | 4 MiB | 112 (112–112) | 2,174 (671–4,379) | 0.813 (0.783–0.830) | 202 (201–203) | 110 (105–112) | 0 |

**Gets reached the device's read rate.** Three times a node's memory was written first, so
three in four bytes a get answered were read from an archive: 2.2 to 2.6 GB/s read from the
Optane, at 72 to 89% of the bytes answered, and 2.7 to 3.3 GB/s answered. That is the Optane's
read rate, and a read at a factor of three costs no more than at one: it is answered by one
replica. On the lab most gets were answered by europa's own member over loopback, which is how a
client of today's Shoal spreads its streams with its driver beside a node; the lab's gets are
europa's Optane, not 1 GbE. On titan alone they were the link's 112 MiB/s.

**An overwrite reads the row it replaces, whole, on every copy.** Rewriting a preloaded row ran at
0.88 to 0.95 of a put of a new one on the loopback leg and 0.94 to 1.0 elsewhere, and wrote as
many device bytes a byte. But it read 1.0 device byte for every byte stored a copy, on every leg
and size, where a put read none: the merge that folds a row in reads the archived row it replaced
([O91](../appendix/optimizations.md#o91-a-merge-reads-the-archived-row-an-insert-replaced-whole)),
and for a stripe as a row that is the whole stripe. A stripe rewritten through A costs a read of
the stripe as well as two writes of it, on every replica. An even mixture ran at about twice the put arm's rate in all: its
puts did nearly what puts alone did, and its gets kept pace with them: a stream keeps a fixed
number outstanding and draws a get or a put for each, so its slow puts set its pace.

## 3. Bytes written for each byte stored, and T2

| Leg | Host | 64 KiB | 256 KiB | 1 MiB | 4 MiB | 1 MiB, WAL device | 1 MiB, archive device |
| --- | --- | --- | --- | --- | --- | --- | --- |
| lab | europa | 2.20 (2.19–2.21) | 2.16 (2.15–2.18) | 2.31 (2.27–2.54) | 2.04 (2.04–2.05) | 2.31 (2.27–2.54) | – |
| lab | hyperion | 2.10 (2.10–2.11) | 2.14 (2.13–2.16) | 2.31 (2.27–2.40) | 2.04 (2.03–2.05) | 1.16 (1.13–1.20) | 1.15 (1.13–1.20) |
| lab | titan | 2.10 (2.10–2.10) | 2.14 (2.13–2.16) | 2.31 (2.27–2.40) | 2.04 (2.03–2.05) | 1.16 (1.13–1.20) | 1.15 (1.13–1.20) |
| loopback | europa | 2.12 (2.11–2.12) | 2.09 (2.08–2.09) | 2.06 (2.06–2.06) | 2.04 (2.04–2.04) | 2.06 (2.06–2.06) | – |
| titan | titan | 2.06 (2.06–2.06) | 2.06 (2.06–2.06) | 2.05 (2.05–2.05) | 2.03 (2.03–2.03) | 1.03 (1.03–1.03) | 1.03 (1.03–1.03) |
| europa | europa | 2.06 (2.06–2.07) | 2.05 (2.05–2.05) | 2.05 (2.05–2.05) | 2.04 (2.04–2.04) | 2.05 (2.05–2.05) | – |

Every writing arm, every host together, settled:

| Leg | Arm | 64 KiB | 256 KiB | 1 MiB | 4 MiB |
| --- | --- | --- | --- | --- | --- |
| lab | put | 2.22 (2.22–2.23) | 2.29 (2.17–2.52) | 2.24 (2.13–2.97) | 2.08 (2.07–2.10) |
| lab | mix | 2.23 (2.20–2.24) | 2.25 (2.18–2.32) | 2.20 (2.12–2.29) | 2.07 (2.07–2.08) |
| lab | overwrite | 2.22 (2.21–2.23) | 2.26 (2.16–2.46) | 2.21 (2.10–2.31) | 2.09 (2.07–2.10) |
| loopback | put | 2.19 (2.18–2.20) | 2.16 (2.15–2.17) | 2.11 (2.10–2.13) | 2.07 (2.07–2.08) |
| loopback | mix | 2.19 (2.19–2.19) | 2.17 (2.16–2.17) | 2.12 (2.12–2.12) | 2.07 (2.07–2.07) |
| loopback | overwrite | 2.19 (2.18–2.20) | 2.16 (2.15–2.17) | 2.12 (2.11–2.13) | 2.07 (2.07–2.08) |
| titan | put | 2.11 (2.11–2.12) | 2.10 (2.10–2.11) | 2.10 (2.09–2.10) | 2.07 (2.07–2.07) |
| titan | mix | 2.12 (2.11–2.12) | 2.11 (2.10–2.11) | 2.10 (2.10–2.10) | 2.07 (2.07–2.07) |
| titan | overwrite | 2.11 (2.10–2.11) | 2.10 (2.10–2.11) | 2.10 (2.09–2.10) | 2.07 (2.06–2.07) |
| europa | put | 2.08 (2.07–2.09) | 2.07 (2.06–2.19) | 2.07 (2.07–2.08) | 2.19 (2.05–2.34) |
| europa | mix | 2.47 (2.10–2.87) | 2.21 (2.08–2.67) | 2.36 (2.08–2.74) | 2.20 (2.06–2.37) |
| europa | overwrite | 2.07 (2.06–2.08) | 2.06 (2.06–2.07) | 2.07 (2.06–2.07) | 2.05 (2.05–2.06) |

**T2 fires at 4 MiB and straddles its line at 1 MiB.** The preload's device bytes for each byte
stored a copy, settled, were 2.03 to 2.05 on every host of both factor-three legs at 4 MiB, in
every round, against a line of 2.5. At 1 MiB they were 2.06 on the loopback leg and 2.27 to 2.40 on
the lab's hosts, except europa's lab node in round 3: 2.54, over the line. That round's preload
was refused 4,133 times ([item 215](../appendix/known-issues.md#215-a-replication-lane-busy-with-wide-rows-is-judged-silent-and-refuses-forwarded-writes)),
and a refused write that had landed was sent again and stored twice: the WAL writer wrote 1.17
bytes for each byte stored. A is near two: every byte goes once to a WAL and once to an archive,
and nothing else of size.

- **On titan and hyperion, the two halves apart**: the WAL's volume wrote 1.03 to 1.16 bytes a
  byte stored, and the archives' volume 1.03 to 1.15. The WAL's own counters agree with its
  device: what the WAL writer wrote, over the bytes stored, was 1.00 except at 1 MiB on the lab,
  1.13, where the refused writes that were sent and landed were written again.
- **The rest is small and shrinks as rows grow**: 2.20 at 64 KiB on europa's lab node against
  2.04 at 4 MiB. It is the WAL's headers and partly empty pages a sync writes, the archive
  maps' saves, and the filesystems' own journals.
- **A merge kept up on the factor-three legs.** Device bytes at an arm's end were within 7% of the
  settled figure there and on titan's one node, and the backlog had cleared 6 to 16 s after an
  arm. europa's one node, writing three times as fast, was 72 to 86% of the way at an arm's end and
  took up to 18 s more, which is where [item 216](../appendix/known-issues.md#216-writes-faster-than-a-nodes-merges-hold-it-past-its-memory-budget)
  comes from.

## 4. Memory

| Leg | Size | Resident peak GiB, largest member | Rows held GiB | WAL index MiB | Least host memory free GiB |
| --- | --- | --- | --- | --- | --- |
| lab | 64 KiB | 7.93 (7.92–7.99) | 6.78 (6.09–7.18) | 10.4 (10.2–10.6) | 3.75 (2.88–4.21) |
| lab | 256 KiB | 7.95 (7.87–7.98) | 7.06 (6.99–7.32) | 2.79 (2.64–2.93) | 3.95 (2.83–4.36) |
| lab | 1 MiB | 7.98 (7.96–7.99) | 6.97 (6.77–7.11) | 0.858 (0.735–0.979) | 3.78 (3.17–3.96) |
| lab | 4 MiB | 7.96 (7.91–7.99) | 6.82 (6.65–6.93) | 0.381 (0.252–0.504) | 4.78 (4.74–4.94) |
| loopback | 64 KiB | 5.98 (5.95–6.02) | 5.19 (5.14–5.30) | 10.7 (10.5–11.4) | 18.6 (16.3–20.2) |
| loopback | 256 KiB | 5.98 (5.96–5.99) | 5.23 (5.14–5.27) | 2.79 (2.68–2.89) | 18.9 (18.0–19.7) |
| loopback | 1 MiB | 6.00 (5.98–6.02) | 4.98 (4.84–5.25) | 0.781 (0.727–0.845) | 17.7 (17.1–18.2) |
| loopback | 4 MiB | 5.98 (5.93–6.01) | 4.69 (4.46–5.03) | 0.315 (0.253–0.372) | 20.9 (20.0–21.3) |
| titan | 64 KiB | 7.78 (7.76–7.84) | 6.66 (6.08–7.09) | 10.5 (10.4–10.5) | 4.45 (3.19–4.64) |
| titan | 256 KiB | 7.78 (7.68–7.88) | 6.94 (6.49–6.97) | 2.76 (2.66–2.83) | 4.25 (3.13–4.50) |
| titan | 1 MiB | 7.88 (7.81–7.95) | 6.56 (6.27–7.02) | 0.816 (0.736–0.898) | 4.10 (3.85–4.33) |
| titan | 4 MiB | 7.90 (7.87–7.91) | 6.58 (6.33–6.67) | 0.338 (0.262–0.420) | 4.81 (4.71–4.96) |
| europa | 64 KiB | 6.02 (5.89–6.24) | 5.18 (4.99–5.39) | 12.4 (11.4–14.9) | 33.3 (32.7–33.5) |
| europa | 256 KiB | 16.4 (9.01–23.0) | 15.6 (8.09–22.1) | 6.73 (4.11–9.45) | 20.7 (16.9–29.0) |
| europa | 1 MiB | 12.7 (12.2–14.0) | 11.8 (11.3–13.2) | 1.30 (1.25–1.44) | 24.1 (22.5–24.7) |
| europa | 4 MiB | 10.6 (5.93–17.3) | 9.48 (4.75–16.1) | 0.306 (0.200–0.461) | 28.3 (20.5–33.8) |

A node held its budget in rows and no more on every factor-three leg and on titan's one node:
4.5 to 5.3 GiB of rows on the loopback nodes' 6 GiB, 6.1 to 7.3 on the lab's and titan's 8, with
the WAL's index of its retained entries 0.3 to 11 MiB a node, largest at 64 KiB. **The single
node on europa did not.** Writing 560 to 970 MiB/s, faster than it merged, it held up to 23.0 GiB
resident on a 6 GiB budget: a
row no archive holds yet cannot be evicted, so a node's rows are its budget plus everything
written since its merges last caught up
([item 216](../appendix/known-issues.md#216-writes-faster-than-a-nodes-merges-hold-it-past-its-memory-budget)).
It came back under its budget at each settle.

## 5. What the cpu went to

The rounds said the nodes' cpu bound A and not what it went to, nor why rows of 4 MiB cost more
cpu a byte than rows of 1 MiB. A supplement after the rounds, `results/x3-supplement.sh`, ran the
put arm once more on the loopback leg at 1 MiB and on europa's one node at 1 MiB and 4 MiB, and
sampled every node's threads with pidstat for 20 s and one node with perf for 10 s, both inside
the measured window. Its rates were the rounds': 216 MiB/s on the loopback leg, 821 and 561 on
the one node. One run each, so its shares are a profile, not an interval.

| Run | A node's cpus busy | user : system | Executor threads busy | libc | Kernel | `x3-node` |
| --- | --- | --- | --- | --- | --- | --- |
| loopback, 1 MiB | 5.6, 5.2 and 3.8 | 1 : 1 | 79 to 94% of a cpu on two nodes, 50 to 61% on the third | 37% | 52% | 10% |
| europa, 1 MiB | 5.6 | 1.3 : 1 | 83 to 86% | 41% | 49% | 9.7% |
| europa, 4 MiB | 5.5 | 1.5 : 1 | 79 to 85% | 48% | 43% | 9.3% |

Each executor thread on the busy nodes also spent 5 to 13% of a cpu waiting for one. The third
loopback node led as many groups as the others, twelve each, and why it ran less busy is not
known.

**58 to 67% of a node's cpu went to moving bytes, and a tenth to Shoal's own code.** By symbol:

- **Copying in user space, 36 to 48%.** One loop in libc, its AVX-512 `memmove` copying
  backwards, which is what a copy between overlapping buffers or a `Vec` grown in place does.
  It is the largest single cost on every run, and the one that grows with the row: 36% at 1 MiB
  on the loopback node, 41% on europa's node at 1 MiB and 48% at 4 MiB.
- **Zeroing fresh pages, 11 to 14%.** `kernel_init_pages`, on the page fault path: memory a node
  had just allocated touched for the first time. Under DWARF call graphs at 4 MiB the fault path
  was 19% of the node inclusive, a third of it on huge pages.
- **Copying in the kernel, 5 to 7%.** Sockets' and files' copies (`_copy_to_iter`,
  `copy_folio_from_iter_atomic`): a row received from a socket, written to a file through the
  page cache, and read back from one; which files, the profile does not say.
- **kTLS, 4% on the loopback node**, encrypting and decrypting the replication lanes; the one
  node has no peers.
- **Shoal itself, 9 to 10%**, no function of it over 1.6%: the replication lane's bookkeeping
  of requests it remembers, decoding WAL frames, digesting commands.
- **The rest, a quarter to a third**, is mostly the kernel's own paths at under 2% a symbol:
  the network stack, io_uring, interrupts and the fault path's bookkeeping.

The copy's caller was not found. perf's frame pointers stop at libc, which has none, and its DWARF
unwinding stopped at the `memmove` too, so whether the copies are the WAL's frames, the rows'
`Vec<u8>`s being moved into a batch, rkyv's serialization or a buffer grown in place is not
known. `x3-supplement-europa-4194304-callers.txt` keeps what the unwinding did find. It is filed
as [O95](../appendix/optimizations.md#o95-a-wide-rows-bytes-are-copied-and-faulted-in-on-every-write),
with what it would take to find it.

**So A's cost a byte is copies, not storage.** At a factor of three every byte is received,
copied, sent twice over kTLS and copied again on each follower, much of it into fresh memory the
kernel zeroes first. The device was 47 to 75% busy while that happened
([1](#loopback-the-pools-own-devices-and-cores)). An optimization that took the copies out would
raise A's rate, and would not change what A writes, which is every byte twice
([3](#3-bytes-written-for-each-byte-stored-and-t2)), on executors every table shares
([6](#6-a-small-table-beside-the-stripes)).

## 6. A small table beside the stripes

| Leg | Size | Read alone | Read beside puts | Over alone | Worst second | Write alone | Write beside puts | Read beside gets |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| lab | 64 KiB | 2.70 (2.38–2.92) | 25.9 (22.0–29.3) | 9.37 (8.54–11.3) | 38.9 (36.2–53.5) | 8.12 (6.83–8.49) | 2,434 (1,836–2,683) | 22.8 (17.6–221) |
| lab | 1 MiB | 2.19 (2.10–2.71) | 31.6 (26.3–49.6) | 13.4 (12.2–22.1) | 81.0 (65.1–231) | 7.02 (6.67–7.44) | 2,887 (2,347–3,232) | 17.9 (17.4–18.5) |
| lab | 4 MiB | 2.57 (2.23–2.86) | 39.0 (22.4–47.9) | 14.5 (8.96–20.6) | 98.3 (67.3–228) | 7.62 (6.73–8.23) | 1,425 (1,278–1,957) | 16.4 (15.7–17.4) |
| loopback | 64 KiB | 2.09 (1.84–2.34) | 234 (178–434) | 126 (76.4–186) | 418 (284–514) | 2.95 (2.61–3.24) | 612 (539–807) | 60.9 (59.4–62.9) |
| loopback | 1 MiB | 1.96 (1.93–2.00) | 323 (304–562) | 165 (152–290) | 624 (574–852) | 2.71 (2.63–2.76) | 948 (784–1,003) | 66.1 (54.7–72.1) |
| loopback | 4 MiB | 2.03 (1.61–2.35) | 292 (273–333) | 151 (130–170) | 439 (411–457) | 3.06 (2.38–3.19) | 868 (836–923) | 58.0 (53.2–61.7) |
| titan | 64 KiB | 3.04 (2.78–3.40) | 46.9 (33.9–64.2) | 15.8 (9.99–21.9) | 104 (70.8–122) | 8.39 (7.73–8.48) | 525 (512–562) | 10.9 (4.86–21.6) |
| titan | 1 MiB | 3.47 (3.26–3.75) | 62.1 (43.4–163) | 17.8 (13.3–43.6) | 104 (65.7–232) | 8.59 (8.24–8.97) | 796 (604–905) | 25.5 (16.1–39.8) |
| titan | 4 MiB | 2.85 (2.59–3.19) | 38.9 (32.1–50.3) | 13.6 (10.1–19.4) | 70.7 (54.7–228) | 8.10 (7.47–8.21) | 251 (184–395) | 19.1 (11.0–20.4) |
| europa | 64 KiB | 2.39 (2.26–2.99) | 51.0 (43.3–56.1) | 20.7 (16.3–23.7) | 70.4 (60.6–94.0) | 2.70 (2.60–3.36) | 144 (107–160) | 74.1 (66.7–75.6) |
| europa | 1 MiB | 2.92 (2.84–3.00) | 67.9 (67.4–69.8) | 23.5 (22.7–23.9) | 84.9 (76.0–99.5) | 3.26 (3.07–3.37) | 189 (154–197) | 70.5 (52.2–89.9) |
| europa | 4 MiB | 2.70 (2.45–2.96) | 119 (108–125) | 44.1 (36.7–51.0) | 160 (142–166) | 3.04 (2.78–3.28) | 262 (221–278) | 91.7 (45.3–134) |

**A table beside A's rows waits for them.** The paced stream, 50 small reads and writes a second
to a table of 10,000 rows of about 100 bytes on connections of its own, read with a p99 of 2 to
3 ms alone on every leg. Beside the put arm:

- **on the loopback leg its reads' p99 was 126 to 165 times that** at the median, 0.23 to
  0.32 s, and its writes' 0.6 to 0.9 s, with a worst second of 0.42 to 0.62 s for reads;
- **on europa's one node, 21 to 44 times**, and its writes 0.14 to 0.26 s;
- **on the lab, 9 to 15 times** for reads, while a small write waited 1.4 to 2.9 s at the median,
  behind the stripes in its shard's WAL and on the same link to its followers, and 89 of them were
  refused with the stripes ([1](#the-lab-1-gbe)).

Beside gets, which write nothing, its reads still waited 24 to 34 times as long on europa's legs.
A small read is answered by an executor that is also validating, copying and checksumming
stripes of 64 KiB to 4 MiB, and a glommio task gives up its core only where it awaits
([S13](isolation.md#what-exists-today)); a small write shares its shard's WAL batch and its sync
with every stripe written there. Nothing of A keeps a table's latency, which is the constraint
every page of this part inherits ([the overview](overview.md#the-constraint-every-page-inherits)).

## What would have changed the design

| Result named in advance | Found | So |
| --- | --- | --- |
| **T1.** A acknowledges at least half the pool's ceiling at 1 MiB and 4 MiB on the loopback leg | **Does not fire.** 0.27 (0.26–0.28) at 1 MiB and 0.20 (0.20–0.21) at 4 MiB, every round wholly under 0.5; sustained, counting the merges, 0.22 and 0.17 | A is not within reach of a replicated pool's rate on the hardware that could show it: four to five times short of the device a copy, and half to two fifths of what writing every byte twice allows. Its nodes' cpu is the bound, not the device |
| **T2.** A writes at most 2.5 device bytes a byte stored a copy at 1 MiB and 4 MiB on every host of every factor-three leg | **Fires at 4 MiB** (2.04, every host and round). **Straddles its line at 1 MiB**, by one host in one round: europa's lab node wrote 2.54 in round 3, where item 215's refusals had the preload send 4,133 rows again and 17% more WAL was written than rows stored. Every other host and round was 2.06 to 2.40 | A is near two: once to a WAL, once to an archive. What it writes beyond two is headers, syncs' partly empty pages and the filesystems' journals, which shrink as rows grow |
| The design moves only if both fire | **It does not move** | B stays the preferred direction. A remains the baseline it is read against, and a candidate for small writes, which is X8's |

## The comparison

**What a replicated SSD pool takes, by how it is built**, on europa's Optane at 1 MiB, against
the device's own rate a copy (820 MiB/s with three copies on it, the rounds' median):

| Built as | Rows a second a copy | Over the ceiling | Device bytes a byte a copy | A table beside it |
| --- | --- | --- | --- | --- |
| **A, three copies over three nodes** (loopback) | 220 MiB/s; 179 sustained | 0.27 | 2.06 to 2.13 | Reads 126 to 165 times slower at the p99 |
| A, one copy on one node (europa), for scale | 827 MiB/s of 2,461 | 0.34 | 2.05 to 2.08 | Reads 21 to 44 times slower |
| **B, as S7 and S6 describe it** | not built: what it would pay is one write of a whole chunk a copy, written ahead and renamed ([X6](device-store-ssd.md#1-a-whole-chunk)), and a small commit of its row | X6 put a slice's whole 1 MiB chunks at 368 to 543 MiB/s on the 970 EVO and 1,978 to 2,379 on the Optane, one to four slices | about 1, the chunk once | On executors of their own ([S13](isolation.md#shared-executors-or-dedicated-ones)); X9 measures the rest |

B's row is not a measurement of B: nothing of B exists, and [X8](spikes.md#x8-one-small-write-three-ways)
is the spike that runs it beside A. It is what X6 measured a slice's device store to take, so the
two rows are read as what each path asks of the same device: A writes a byte twice through cores
that also run every table, and B writes it once from a slice's executor.

## Recommendation

**Keep B as the preferred direction, and do not make replicated SSD pools tables.**
[S18](contract.md#q14-in-part-the-cost-of-stripes-as-rows-2026-10-07) records it as Q14, in part,
with what X3 did not settle:

- **A is near two and several times short.** It writes every byte about twice, as the plan
  expected, but the bytes do not reach the device: three nodes on one Optane stored 0.27 of the
  device's rate a copy at 1 MiB and 0.20 at 4 MiB, bound by the nodes' cpu, where B's write of a
  whole chunk is one write from a slice's executor.
- **A charges every table on its nodes.** A small table's reads waited 126 to 165 times as long
  at the p99 beside A's puts on the loopback leg, and its writes up to a second, because the
  stripes share every executor and every shard's WAL with it. B's object work runs on executors
  of its own and outside the WAL ([S13](isolation.md)).
- **A's gets are the device's.** Three in four bytes read came off the Optane at its read rate,
  through any replica. A read of B's whole chunk is the same read; nothing here argues against
  either.
- **A small write may still ride the log.** S7's threshold at which small writes go inside the
  commit stands untouched: that is the cost X8 measures, and nothing here prices a write below
  64 KiB.

## What X3 does not settle

- **B's own cost.** B is not built; X8 runs a small write through A, through B, and through B
  with the bytes in its commit, and M15 measures B's whole write.
- **Where A's cpu goes, beyond the profile.** [5](#5-what-the-cpu-went-to) names the functions
  a node spent its put arm in; an optimization that took a share of them would move A's rate,
  not the comparison, which is two writes against one.
- **A on a link faster than 1 GbE.** The lab's link bounds every pool to about 60 MiB/s of rows
  at a factor of three. Loopback stood in for a fast link.
- **Several devices a node.** Every node here had one device for its rows. A pool of several
  devices a node is where B's slices spread and A's one WAL a shard does not.
- **A rotational pool.** X7 found a disk needs its journal on an SSD and an executor of its own;
  A would put a table's WAL there.

## What it did not measure

- **A write in place smaller than its row.** A has no partial write: an overwrite here is the
  whole row, which is what A pays to change any part of a stripe. The cost of a 4 KiB change is
  the row's whole cost.
- **Erasure coding.** A has none; every copy here is whole.
- **A client that routes by topology** ([D7](../direction/shard-aware-routing.md)). Two writes in
  three took a hop to their leader, as today's client's do, which is part of A as it is today.
- **Crashes and failures.** Nothing was killed. The refusals of item 215, and ten writes shed at
  admission on europa's one node at 4 MiB, are the only failures.
- **A long run.** Each arm ran 40 s and each preload three times a node's memory; nothing here
  says what a day of writes at these rates does to a node's archives, maps or memory.

## What it found in the engine

| Finding | Where |
| --- | --- |
| A replication lane carrying wide rows over a full 1 GbE link is judged silent after 1.5 s without an answer, and refuses writes forwarded to the leader: the 256 KiB preload sent 21,246 to 25,467 rows again a round, and 7,867 puts and overwrites failed over four rounds at 256 KiB | [Item 215](../appendix/known-issues.md#215-a-replication-lane-busy-with-wide-rows-is-judged-silent-and-refuses-forwarded-writes) |
| A node written to faster than it merges holds every unmerged row past its budget, since a partition holding a write no archive has yet is not evictable: 23.0 GiB resident on a 6 GiB budget | [Item 216](../appendix/known-issues.md#216-writes-faster-than-a-nodes-merges-hold-it-past-its-memory-budget) |
| A bench feed keeps 4,096 parsed rows ahead whatever their size, 16 GiB at 4 MiB, and sizes its frames by a file's text | [Item 214](../appendix/known-issues.md#214-a-bench-feed-keeps-4096-rows-ahead-whatever-their-size) |
| A small table's operations wait behind wide rows on the executors and in the WAL they share | Not a defect: what S13's executors and lane are for ([S13](isolation.md)) |
| A row's bytes are copied in user space and land in freshly faulted pages on every write: 36 to 48% of a node's cpu in one `memmove` loop and 11 to 14% zeroing pages, more at 4 MiB than at 1 MiB, with Shoal's own code 9 to 10% | [O95](../appendix/optimizations.md#o95-a-wide-rows-bytes-are-copied-and-faulted-in-on-every-write) |

## Related

- [X3](spikes.md#x3-bytes-through-the-tablet-groups) for what was planned.
- [S7](write-path.md#the-candidates) for candidate A and the direction it is read against.
- [S18](contract.md#q14-in-part-the-cost-of-stripes-as-rows-2026-10-07) for the decision.
- [S13](isolation.md) for what keeps a table's latency, which A does not.
- [X6's record](device-store-ssd.md) for what a slice's device store takes on the same devices.
- [X10's record](stripe-row-costs.md) for the form this page copies, and the rows A's would sit
  beside.
- [F69](../features/driver-operation-kinds.md), [F71](../features/bench-device-memory.md) and
  [F72](../features/bench-paced-stream.md) for the driver's kinds, the device script and the
  pacing this spike drove through.
