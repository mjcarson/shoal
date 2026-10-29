# A 2+1 get crosses the object lane

| | |
| --- | --- |
| **Entry** | [O104](../../appendix/optimizations.md#o104-an-object-lane-copies-a-reads-bytes-twice-and-a-stages-once-more), filed from F86 before the comparison |
| **Priority** | [9 of 9](../store-optimizations.md#in-order) |
| **Status** | ~~to diagnose: O104 is a candidate, not an established cause~~ diagnosed on 2026-10-22. At 2+1 a node's object executor is its busiest thread, 77 to 85% of a cpu at sixteen in flight, and the lane is about half of that ([Each thread's cpu](#each-threads-cpu)). O104's two copies are about 8% of its time. The lane's TCP over loopback and its kTLS are a third, and no other store in the comparison encrypted between its nodes ([Where the executor's time goes](#where-the-executors-time-goes)). ~~Which change follows is the user's choice~~ The user chose both copy fixes, the store's own (O127) first, and a setting for plaintext lanes, [F103](../../features/plaintext-peer-lanes.md) (2026-10-22). Built by [F105](../../features/store-reads-in-place.md) and [F106](../../features/lane-copies.md) and measured on 2026-10-23: on europa's loopback the 2+1 gets ran 1.05 to 1.26 times as fast, every range apart ([Before and after](#before-and-after)) |

## What the comparison found

On europa's loopback, gets of 1 MiB at sixteen in flight:

| | Shoal, three copies | Shoal, 2+1 | Ceph, 2+1 | MinIO, 2+1 | RustFS, 2+1 |
| --- | --- | --- | --- | --- | --- |
| Gets a second | 2,331 to 2,405 | 1,749 to 1,779 | 2,996 to 3,055 | 3,035 to 3,093 | 1,199 to 1,225 |
| Each of Shoal's devices | 738 to 815 MiB/s read, 83 to 85% busy | 557 to 596 MiB/s, 47 to 50% busy | | | |
| europa's cpus busy | 18% | 19% | | | |

At 2+1 Shoal was behind Ceph by at least 40% and MinIO by at least 41%
([without the link](../store-comparison.md#without-the-link-europas-optane-over-loopback)). With three
copies its devices bound it, at 83 to 85% busy. At 2+1 they were half busy and the host's cpus four
fifths idle, so neither bound it.

**Measured again** in the comparison (2026-10-25 and 26, [Shoal measured again](../store-comparison.md#shoal-measured-again)): on
europa's loopback the 1 MiB gets at 2+1 ran 1.76 to 1.90 times as fast as the first run's, within
Ceph, and within MinIO at sixteen in flight and behind it by at least 11% at one; with three copies
2.2 to 2.5 times, ahead of Ceph and SeaweedFS. F107's cache answered them too, beside the copies F105,
F106 and F109 to F111 took out, and the comparison does not separate the two.

## Why it would help

With three copies over three nodes the coordinator holds every stripe and reads its own copy
([Reads from a pool of copies follow leadership](read-placement.md)). At 2+1 an object of 1 MiB
lies in one data chunk, on whichever node placement put it, so about two gets in three fetch the
chunk from another node over the object lane before the coordinator sends it on. O104 counts what
that costs: the holder copies the bytes into its response's body, and the requester copies the
payload into the answer's `Vec`, two copies a mebibyte on each node besides the device's.

A node does its object work on one executor, a single cpu (`resources.object_cpus`). Four fifths of
europa's cpus were idle, but one busy cpu a node would not show in that figure, and the capture
records each process's cpu, not each thread's. So the lane's copies on that one executor are a
candidate for the bound, and nothing more yet. A frame written as two parts and an answer decoded
in place, as O104 proposes, would take the copies off it.

## What would show it worked

First, whether the lane is the bound:
- **Each thread's cpu.** `get100/s1m/c16` at 2+1 on the loop leg, with every node's threads sampled
  (`pidstat -t`) while it runs. If an object executor's cpu is near one whole cpu, the executor
  bounds the gets.
- **O104's own.** `shoaladm bench` `get100` at 1 MiB and 4 MiB on the lab and over loopback, each
  member's cpu a gibibyte read.

Then, for O104's change, the same arm before and after. It worked if the 2+1 gets' intervals are
above the before captures', disjoint, and the object executor's cpu a gibibyte read falls.
`get100/s1m/c16` with three copies must not get worse.

## Each thread's cpu

Measured on the lab's loop leg on 2026-10-22, on F98's build (`l104-loop-ec21` and
`l104-loop-rep3`, under `shoal-spike/results/stores-ab/`; the threads in `l104-threads.txt` beside
them).
- **The arms.** Gets of 1 MiB at one in flight and sixteen, two captures a group, each on devices
  made fresh.
- **The sampling.** Every node is a process on europa. `pidstat -t` sampled every thread each
  second. Each figure is a thread's mean over its arm's measured seconds, an idle second counting
  as nothing.
- **The threads.** Each node has three shards, which are glommio's `unnamed-2` to `unnamed-4`. Its
  one object executor is `shoal-objects-0`, on the control core's other thread. Its control thread
  is `shoal-control-1`.
- **What was kept.** The chain's own sampler named its processes, and pidstat applies that filter
  to every thread's own name too, so it kept each node's total and none of its threads. A second
  sampler of every active task started part way through the second 2+1 capture, so that capture's
  threads cover 33 seconds at one in flight and 60 at sixteen. The two three-copy captures are
  whole.
- **Nothing was lost**, and the rates are the comparison's.

| Sixteen in flight | Three copies | 2+1 |
| --- | --- | --- |
| Gets a second | 2,388 to 2,436 | 1,766 to 1,805 |
| p50 and p99, ms | 6.5 to 6.7, 10.3 | 8.7 to 8.9, 14.5 to 14.8 |
| Each device busy | 87 to 88% | 46 to 49% |
| Each node, every thread | 138 to 144% of a cpu | 166 to 174% |
| Its object executor | **42 to 44%**: 27 to 28 user, 15 to 16 system | **77 to 85%**: 27 to 28 user, 50 to 58 system |
| Each of its shards | 18 to 46% | 22 to 43% |
| Its control thread | 2% | 2% |
| The driver | 106 to 115% | 74% |

At one in flight the object executor runs at 9 to 11% of a cpu with three copies and 19 to 20% at
2+1.

What this says:
- **At 2+1 the object executor is a node's busiest thread.** It runs at four fifths of a cpu at
  sixteen in flight, where with three copies it runs under half. By the page's test it does not
  bound the gets alone, since it is not at one whole cpu. A single executor that busy still queues
  what it is handed, so it can be much of a get's wait.
- **The lane is about half of it.** With three copies, each node reads 800 MiB/s off its device at
  0.43 of a cpu. At 2+1 its executor reads 600 MiB/s, sends 400 of them over the lane and receives
  400 more, at 0.81 of a cpu. Its user time is about 0.27 of a cpu in both. Its system time grows
  from 0.16 to 0.53.

## Where the executor's time goes

perf sampled every node's process at 999 Hz for 20 seconds of the first measured run of a third
2+1 capture (`p104-loop-ec21`, at 1,759 and 1,763 gets a second; `p104-perf.txt` beside it). Of
104,988 samples, the object executors took 48% and the shards 50%. The executors' samples by what
they were doing:

| Work | Share of the executors' samples |
| --- | --- |
| Copies in user space (libc's `memmove`) | 20% |
| TCP over loopback, both ends | 18% |
| kTLS's AES-GCM, both ends | 15% |
| Zeroing and allocating pages in the kernel | 8% |
| io_uring and the device reads | 7% |
| Entering and leaving the kernel, and its mitigations | 6% |
| Scheduling and timers | 5% |
| glommio | 3% |
| Memory cgroup accounting | 2% |
| Zeroing buffers in user space (`memset`) | 2% |
| CRC-64/NVME of each unit | 1% |
| The rest | 13% |

What this says:
- **O104's two copies are about 8% of the executor's time.** Each mebibyte a holder reads is copied
  twice before it leaves the store: `read_exact` copies each piece out of glommio's buffer, and
  `read_chain` copies each unit's part into the range. Each mebibyte the lane carries is copied once
  more on each node, by `encode_body` and `decode_parts`. At 2+1 that is about 2 GiB of copies a
  second on each executor, two fifths of them O104's.
- **The store's own two copies are about 12%**, and every read pays them, with the lane or without
  it ([O127](../../appendix/optimizations.md#o127-a-holder-copies-every-byte-it-reads-twice-before-it-leaves-the-store)).
  [F105](../../features/store-reads-in-place.md) took them off (2026-10-23): a range the file gives
  whole is answered from the device's buffers, and a local read copies it once to cross to its
  shard. [F106](../../features/lane-copies.md) took O104's copies off after it: the holder writes
  the answer from those buffers, and the asker keeps the bytes in the body its link read; a stage's
  bytes are one allocation every holder's request shares. Both are measured
  [below](#before-and-after).
- **The lane's TCP and its kTLS are a third of the executor's time.** On loopback the sending
  thread carries both ends of TCP. The kernel encrypts and decrypts every byte of the lane, which
  runs under mutual TLS because shoaladm renders `cluster.tls` for every deployment.
- **No other store in the comparison encrypted between its nodes.**
  - MinIO and RustFS spoke plain HTTP.
  - SeaweedFS ran with no `security.toml`.
  - Ceph's daemons used its messenger's `crc` mode. `ms_cluster_mode` and `ms_client_mode` are
    `crc secure` in v20.2.0's `global.yaml.in`, crc first.

  The comparison's statement that it measured TLS on no store was wrong for Shoal's lanes
  ([what it did not measure](../store-comparison.md#what-it-did-not-measure)).

So O104's change would take less than a tenth of the work off the busiest thread. Three changes
could follow: O104's copies, the store's own copies (O127), or a lane without TLS where the network
is trusted ([todos](../../appendix/todos.md#the-rest-of-encryption)). ~~Which one, if any, is the
user's choice, made with these figures.~~ The user chose both copy fixes, the store's own first
(2026-10-22), and a setting that lets a deployment's lanes run without TLS, off by default:
[F103](../../features/plaintext-peer-lanes.md) built it as an inventory's `plaintext_lanes`. On this
page's loopback at 2+1 (2026-10-23), gets plaintext ran 1.09 to 1.24 times as fast, their object
executors spending 0.77 to 0.86 of the cpu a GiB they spent under TLS
([F103's figures](../../features/plaintext-peer-lanes.md#on-europas-loopback)).

## Before and after

[F105](../../features/store-reads-in-place.md) took the store's copies off and
[F106](../../features/lane-copies.md) the lane's. They were measured together on 2026-10-23
(`w2-loop-ec21`, `w2-loop-rep3` and `w2-ssd-ec21`, under `shoal-spike/results/stores-ab/`), run as
[F105's page](../../features/store-reads-in-place.md#performance) says. *Before* is F104's build and
*after* the commit of F106's stage half, which carries both; each feature's own step is on its page.

The gets on the loop leg, operations a second, and the cpu a GiB their object executors spent,
before → after:

| Copies | Size | In flight | Before | After | After ÷ before | p99 ms | Object executors, cpu-seconds a GiB |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 2+1 | 64 KiB | 1 | 1,696 [1,689–1,717] | 1,781 [1,774–1,812] | **1.05, ahead** | 0.76 → 0.71 | 3.53 → 3.37 (0.95) |
| | | 16 | 13,230 [13,163–13,358] | 16,586 [16,422–16,790] | **1.25, ahead** | 1.86 → 1.39 | 2.66 → 2.36 (0.89) |
| | 1 MiB | 1 | 434 [429–441] | 547 [533–553] | **1.26, ahead** | 2.80 → 2.26 | 1.36 → 1.02 (0.75) |
| | | 16 | 1,800 [1,779–1,824] | 2,262 [2,251–2,291] | **1.26, ahead** | 14.3 → 11.5 | 1.37 → 1.07 (0.78) |
| Three | 64 KiB | 1 | 1,933 [1,929–1,959] | 2,081 [2,062–2,120] | **1.08, ahead** | 0.65 → 0.60 | 1.74 → 1.71 (0.98) |
| | | 16 | 14,725 [14,648–14,808] | 19,721 [19,547–19,941] | **1.34, ahead** | 1.75 → 1.22 | 1.26 → 1.19 (0.95) |
| | 1 MiB | 1 | 559 [555–564] | 739 [734–745] | **1.32, ahead** | 2.06 → 1.54 | 0.55 → 0.22 (0.40) |
| | | 16 | 2,412 [2,388–2,440] | 2,579 [2,577–2,586] | **1.07, ahead** | 10.5 → 8.85 | 0.53 → 0.25 (0.47) |

The puts ran 4 to 8% faster at 2+1, the 64 KiB ones at one in flight within, and 7 to 8% faster
with three copies, every range apart.

By [the test above](#what-would-show-it-worked), it worked:
- **The 2+1 gets ran 5 to 26% faster, every range apart**, and every get with three copies did too.
- **The object executors' cpu a GiB read fell** at 2+1, by 22 and 25% for the 1 MiB gets and by 5
  and 11% for the 64 KiB ones.
- **Three copies' 1 MiB gets at sixteen in flight were not worse**: 7% faster, apart.

Beside the comparison's other stores, a guide and never a verdict: those captures ran on
2026-10-19, before the settle waited out Shoal's reclamation
([Resolved #301](../../appendix/resolved/a-settle-waits-for-reclamation.md)).
- **At 2+1 the 1 MiB gets at sixteen in flight** are behind Ceph by at least 23%, where they were
  behind by 39%, and behind MinIO by at least 24%, where 39%. They are ahead of RustFS by at least
  83%, where 45%.
- **At 2+1 and one in flight** they are behind Ceph by at least 32%, where 45%, MinIO by 40%, where
  MinIO was at least 2.1 times as fast, and RustFS by 0.2%, where 20%.
- **With three copies** the 1 MiB gets are behind Ceph by at least 7.1% at one in flight, where
  29%, and by 30% at sixteen, where 34%. They are behind SeaweedFS by at least 29%, where 46%, and
  by 2.3%, where 7.8%.

On the SSD leg, a node a host over 1 GbE at 2+1, the gets before → after, with the cpu a GiB the
three nodes' processes spent (no thread was sampled across hosts):

| Size | In flight | Before | After | After ÷ before | p99 ms | Nodes, cpu-seconds a GiB |
| --- | --- | --- | --- | --- | --- | --- |
| 64 KiB | 1 | 413 [392–432] | 427 [413–432] | 1.03, within | 3.80 → 3.71 | 79.0 → 69.4 (0.88) |
| | 16 | 1,985 [1,950–2,026] | 2,019 [2,017–2,024] | 1.02, within | 14.3 → 13.8 | 39.3 → 33.6 (0.85) |
| 1 MiB | 1 | 62.3 [60.1–64.1] | 67.6 [65.3–69.7] | **1.08, ahead** | 26.7 → 23.6 | 26.0 → 21.0 (0.81) |
| | 16 | 132 [130–134] | 128 [127–129] | **0.97, behind** | 413 → 441 | 17.1 → 14.7 (0.86) |

What it says:
- **The nodes spent 12 to 19% less cpu a GiB on every get**, and 9 to 14% less on every put, each
  range apart.
- **The 1 MiB gets at one in flight ran 8% faster.** Capture beside capture it was 8 and 9%, though
  each of the after's captures drew fewer of the gets to europa's own node than the before's
  capture it is set beside ([each capture](../../features/store-reads-in-place.md#on-the-ssd-leg)).
- **At sixteen in flight the gets were europa's link's**, full in every capture. The 1 MiB gets'
  3% is the draw of how many bytes each get took across it
  ([F106's SSD leg](../../features/lane-copies.md#on-the-ssd-leg)).
- **The 1 MiB puts at one in flight ran 6% faster**, apart, and the others were within.

Beside the comparison's other stores on SSDs, a guide again: the 1 MiB gets at one in flight are
behind Ceph by at least 9.9%, where 17%, MinIO by 39%, where 44%, and RustFS by 13%, where 20%. At
sixteen in flight every store is bound by europa's link
([where Shoal is better, and where worse](../store-comparison.md#where-shoal-is-better-and-where-worse)).

### Where the executor's time goes since

perf sampled every node's process again as [above](#where-the-executors-time-goes), 20 seconds of a
capture of each build on 2026-10-23 (`p2-perf.txt` beside the captures):
- F104's build, `p2-loop-ec21-before`, at 1,783 gets a second;
- F106's stage half, `p2-loop-ec21-o104`, at 2,215.

The samples each node's object executor took for each GiB the gets moved:

| Work | Before | After |
| --- | --- | --- |
| Copies in user space (libc's `memmove`) | 295 | 45 |
| TCP over loopback, both ends | 265 | 260 |
| kTLS's AES-GCM, both ends | 222 | 255 |
| Zeroing and allocating pages in the kernel | 118 | 123 |
| io_uring and the device reads | 104 | 33 |
| Entering and leaving the kernel, and its mitigations | 91 | 50 |
| Copies in the kernel | 20 | 78 |
| CRC-64/NVME of each unit | 15 | 86 |
| Zeroing buffers in user space (`memset`) | 22 | 0 |
| The rest | 311 | 228 |
| **All** | **1,463** | **1,158** |

What this says:
- **The executor spends 21% less for each GiB.** That matches its threads' cpu a GiB in the table
  above, 0.78 of what it was.
- **The copies in user space are about a sixth of what they were.** What is left is mostly a local read's
  one copy to cross to its shard
  ([O129](../../appendix/optimizations.md#o129-a-local-read-still-copies-its-range-once-to-cross-to-its-shard-and-scrubs-and-records-copy-theirs)),
  which [F110](../../features/store-checks-in-place.md) takes off: a local read hands its shard the
  buffer it filled. Lab 3b measures it.
- **The device reads cost a third of what they did.** A range is one run of pieces, where each
  unit was a read of its own.
- **The first touch of the bytes moved.** Before, a copy in user space was the first to read what
  the device wrote, and what came after it found the bytes cached. Since, the CRC of each unit is
  the first, then the kernel's copy and kTLS as the lane sends them. Those three grew by 162
  samples a GiB, against the 273 the copies and zeroing gave up.
- **The lane's TCP and kTLS are now 45% of the executor's samples**, about the same for each GiB
  as before. A plaintext lane ([F103](../../features/plaintext-peer-lanes.md)) takes off the kTLS.
- **The coordinating shard's copies are now the largest cost of a 2+1 get.** The shards took 1,363
  samples a GiB, where they took 1,531, and less of it was TCP (161, where 236) and kTLS (25, where
  47). Their copies in user space stayed at 313 and 322, about a quarter of their samples
  ([O128](../../appendix/optimizations.md#o128-a-coordinator-copies-a-reads-range-twice-before-it-answers-and-checks-it-again)).
  [F109](../../features/reads-from-stripe-buffers.md) takes them off: a get's range is answered as
  the parts its stripes were read in. Lab 3b measures it.

## What it could cost

A reply that borrows its buffer outlives the frame's allocation only as the answer it is (O104).

The glommio fork's vectored write, under F106, gave a table's strong reads across hosts a tail of
about 110 ms beside gets that fill a link, where it was about 17. ~~Its cause is not yet known.~~
A frame became one segment, and one a full link dropped waited out the kernel's 200 ms loss
probe. A node's replication and control lanes now set a minimum RTO of 5 ms, and a client's
connection holds it until it carries an object
([Resolved #304](../../appendix/resolved/lone-frame-loss-probe.md)).

## Related

[O104](../../appendix/optimizations.md#o104-an-object-lane-copies-a-reads-bytes-twice-and-a-stages-once-more);
[Reads from a pool of copies follow leadership](read-placement.md);
[F86. Replicated pools](../../features/replicated-pools.md); [F90. Erasure coded pools](../../features/erasure-coded-pools.md).
