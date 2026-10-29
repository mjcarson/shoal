# Exploratory spikes

**No spike here has run.** This page is the list of what has to be measured or modelled before the
[milestones](milestones.md) of block volumes can be more than a guess, and how each would be done.

A milestone plan is a set of claims: that the lane is safe without a commit a write, that it is fast
enough to be worth a protocol of its own, that a geometry, a tract and a copy-on-write cost what the
pages say, that a volume leaves the tables alone. The pages before this one make those claims as
preferences and hang each on a question ([W27](contract.md#questions-to-answer)). A spike is what
settles a question, in the sense the object storage part gave the word
([its spikes](../../object-storage/spikes.md#what-a-spike-is)): it answers a question and says in
advance what result would change the design, its code is thrown away, it is never a prototype of the
feature, and it reports what it did not measure. X23 is the one exception, as X1 was: the model is
kept, and its schedules are acceptance tests.

## How each is written

| Field | Holds |
| --- | --- |
| **Question** | The question, and the `Q` it serves |
| **What would change the design** | The result, stated before the run, that would move a preference |
| **Method** | What is built and what is run |
| **Where** | Host, device, filesystem, governor |
| **Records** | The table it prints |
| **Depends on** | Other spikes, a prerequisite, a gate |
| **Cost** | An afternoon, days, or a week. A guess, and labelled as one |

**Where a spike runs** is the object storage part's lab and its rules
([Where a spike runs](../../object-storage/spikes.md#where-a-spike-runs)): titan and hyperion (Zen1,
the 970 EVO), europa (the Optane), 1 GbE; built for `znver1` in a target directory of its own; a
scratch directory on the device measured, never the tmdb cluster's; the tmdb node stopped and the
governor at `performance`, both put back; sides alternated by round, four rounds or more, a difference
counted only when the intervals do not overlap; every table labelled by host, cpu, governor, device and
filesystem. Disks are not used: an attached pool is SSD-only in these gates.

## The spikes at a glance

| # | Spike | Settles | Needs | Cost |
| --- | --- | --- | --- | --- |
| X23 | The lane's model | Q43, Q44, Q48, and P27–P34 | Nothing | One to two weeks |
| X24 | One 4 KiB write, three ways | Q46, the forwarding threshold, the lane's case | The lab | A week |
| X25 | A volume pool's geometry | Q47 | The lab | Days |
| X26 | Write working sets and takeover time | Q45 | X24's spike build | Days |
| X27 | ublk beside NBD, on the tokio client | Q49 | X24's spike build | Days to a week |
| X28 | The lane beside the reference cell | Q51 | X24's spike build | Days |
| X29 | A snapshot's copy-on-write | Q50 | The lab | Days |
| X30 | RBD, DRBD and Mayastor, read at the source *(optional)* | W15's readings | Nothing | Days |

## The spikes

### X23. The lane's model

| Field | |
| --- | --- |
| **Question** | Do the lane's rules, as [W17](writer-lane.md) to [W21](snapshots-and-clones.md) state them, keep P27 to P34 under every interleaving of writers, takeovers, crashes, partitions, moves, rebuilds and snapshots; and is every rule needed? Q43, Q44 and Q48 |
| **What would change the design** | If the safe policy breaks a property, the rule the schedule shows missing is added to its page before M29. If an unsafe setting's schedule breaks nothing, the rule it turns off is not needed and its page says so. If the partition that leaves the old writer with every position of a stripe breaks P27 even as a cut a stripe, the choice is between the cut as stated and a token from the majority's side on every write, which would end "no commit a write", and it goes back to the user |
| **Method** | [W26](model.md)'s model in `shoal-model/src/volume/`, its oracles, its safe policy and its unsafe settings, each setting's schedule saved; `volume_search` over three layouts for at least 1,200,000 runs on europa and half again on titan and hyperion from a `znver1` build, compared block by block |
| **Where** | Anywhere; the search on europa, titan and hyperion |
| **Records** | Every setting's verdict and saved schedule; the search's blocks by host; what each repair was |
| **Depends on** | Nothing |
| **Cost** | One to two weeks |

### X24. One 4 KiB write, three ways

| Field | |
| --- | --- |
| **Question** | What does a 4 KiB write cost at queue depths one and thirty-two through the object store as built, through the lane write-through, and through the lane write-back with a FLUSH every N writes; and from what size does forwarding by one holder beat fanning out from the host on 1 GbE? Q46, and the threshold of [W17](writer-lane.md#forwarding-large-writes-optional) |
| **What would change the design** | If the lane write-through is less than twice as fast as the object store's write in place on the 970 EVO at depth one, its case rests on fast devices alone and goes back to the user before M30. If write-back is within 10% of write-through on the Optane, it is not worth offering on devices like it. If forwarding never wins below a stripe, the hybrid is dropped |
| **Method** | A spike crate in X8's manner: a holder on one object executor a host with X6's journal and committer, answering lane records over a socket; a writer on europa that numbers, fans out and forwards; the object store's `patch100` arm on the same devices beside it. Every write read back |
| **Where** | titan and hyperion (the 970 EVO), europa (the Optane), 1 GbE; `performance` governor |
| **Records** | By device, size, depth and mode: p50, p99 and p99.9, IOPS, device bytes a byte, the host link's use |
| **Depends on** | The lab |
| **Cost** | A week |

### X25. A volume pool's geometry

| Field | |
| --- | --- |
| **Question** | Which geometry does an attached pool cut a volume by: 1 MiB stripes of 4 KiB units, 4 MiB of 64 KiB with a label a block, or 4 MiB of 4 KiB with a header of several blocks? Q47 |
| **What would change the design** | If 64 KiB units with a label a block cost a 4 KiB write no more than 10% and a read nothing, an attached pool keeps buckets' default geometry. If 1 MiB stripes' listings make a rebuild or a light scrub of a group too slow, 4 MiB stripes with a larger header |
| **Method** | X6's harness (`shoal-spike device`) with the lane record and the three chunk formats: random 4 KiB writes, their applies and reads, a group's listing, and the metadata a TiB |
| **Where** | titan (the 970 EVO), europa (the Optane) |
| **Records** | By geometry: write and read latency and IOPS, device bytes a byte at apply, a listing's time, headers a TiB |
| **Depends on** | The lab |
| **Cost** | Days |

### X26. Write working sets and takeover time

| Field | |
| --- | --- |
| **Question** | How many tracts does a real workload dirty a second and keep dirty at once, at tracts of 4 MiB to 256 MiB, and how long does a takeover take with that many dirty? Q45 |
| **What would change the design** | If a working set dirties so many tracts a second that their commits load the row's group past [W23](isolation.md#table-writes-a-volume-makes)'s bound, the bitmap grows coarser or marks are batched longer. If a takeover with a typical dirty set takes more than tens of seconds, first touch needs more in parallel or smaller tracts. If a volume's snapshots and bitmap outgrow one row, the command of several writes in one tablet becomes required for volumes |
| **Method** | Block traces (`blktrace`) on ext4 and XFS over a local device of fio's random and sequential writes, PostgreSQL under pgbench, a kernel build and a virtual machine's boot and update, replayed against the tract sizes; then takeovers of X24's spike with the traced dirty sets |
| **Where** | europa for the traces; the lab for the takeovers |
| **Records** | By workload and tract size: tracts dirtied a second, dirty at once at p50 and p99; a takeover's time by dirty tracts |
| **Depends on** | X24's spike build |
| **Cost** | Days |

### X27. ublk beside NBD, on the tokio client

| Field | |
| --- | --- |
| **Question** | What does the host device cost a request, by ublk's copy mode and against NBD and against the client in process; and does the tokio client's hop from a queue's thread matter? Q49 |
| **What would change the design** | If the hop between the queue's thread and tokio costs more than a tenth of a 4 KiB write on the Optane, an io_uring-native client becomes required. If zero copy gains under 5%, the default copy is kept. If NBD is within 10% of ublk, it becomes a cheap second front end |
| **Method** | A ublk server spike with a null target that answers at once and X24's lane as a real one; the default copy, user copy, and zero copy with automatic registration; NBD with the same targets; queue depths one and thirty-two; the host's cpu a request. titan's and hyperion's 7.0.0-34 kernels checked for `ublk_drv` first |
| **Where** | europa (the Optane), titan |
| **Records** | By mode and depth: latency at p50 and p99, IOPS, the host's cpu a request |
| **Depends on** | X24's spike build |
| **Cost** | Days to a week |

### X28. The lane beside the reference cell

| Field | |
| --- | --- |
| **Question** | What may a volume cost the queries beside it, and what node ceiling holds that? Q51 |
| **What would change the design** | If no useful rate holds [W23](isolation.md#the-budget)'s budget with the lane on the object executors, it needs executors apart from buckets', or a lower default ceiling. If the tables' own device breaks the budget far past what F85's load did, an attached pool on a node's WAL device is refused by default instead of warned of. If members fall silent below the link's capacity, the ceiling's default drops |
| **Method** | X9's method: the reference cell alone, then beside X24's lane at rising rates and depths, random 4 KiB and sequential 1 MiB, with and without a lane queue apart from the object foreground, on separate devices and on the tables', with a bucket arm beside it; the detector's log read for silence |
| **Where** | The lab |
| **Records** | The cell's read and write p99 ratio and the bucket arm's, by rate, depth, queue and device; members called silent |
| **Depends on** | X24's spike build |
| **Cost** | Days |

### X29. A snapshot's copy-on-write

| Field | |
| --- | --- |
| **Question** | What does the first write after a snapshot cost, copying the chunk against a reflink of it, and what does a stream of snapshots cost the foreground? Q50 |
| **What would change the design** | If copies push a write's p99 past twice its own under a stream of snapshots, a reflink becomes required on XFS, and an attached pool on ext4 is offered no snapshots. If reflinks fragment reads past X6's line, copies stay |
| **Method** | X6's harness: a 1 MiB chunk copied and reflinked (`FICLONERANGE`) at its first write after a snapshot, at depths one and thirty-two; the foreground beside snapshots every few seconds; reads after many reflinks |
| **Where** | titan (XFS on the 970 EVO), europa (XFS on the Optane) |
| **Records** | The first write's latency each way; the foreground's p99 ratio; read latency after reflinks |
| **Depends on** | The lab |
| **Cost** | Days |

### X30. RBD, DRBD and Mayastor, read at the source

*(Optional: no gate waits on it.)*

| Field | |
| --- | --- |
| **Question** | Are [W15](prior-art.md)'s readings right at pinned releases, and what do RBD and DRBD cost at W25's arms on the lab's own devices? |
| **What would change the design** | A borrowed mechanism read differently at the source, RBD's handover or DRBD's activity log most likely, changes W18's or W16's version of it. An RBD on the lab far better at depth one than the lane reopens where the writer runs |
| **Method** | [X14](../../object-storage/ceph-and-s3-sources.md)'s method: releases pinned by commit, the files W15 names read and cited by line, predictions written before. Optionally RBD through cephadm as the store comparison deploys Ceph, and DRBD 9 between two hosts, every host change recorded and put back |
| **Where** | Reading anywhere; the lab for the runs |
| **Records** | Each W15 claim confirmed, corrected or still recalled; RBD's and DRBD's figures at W25's arms |
| **Depends on** | Nothing |
| **Cost** | Days |

## What has to be on the record before each gate

| Gate | Decided first |
| --- | --- |
| M29 | Q43, Q44 and Q48 in the model, by X23 |
| M30 | Q46 with the user, by X24; Q47 by X25; Q51 by X28 |
| M31 | Q45 by X26; Q43 confirmed on what M30 built |
| M32 | Q48 confirmed on what M31 built |
| M33 | Q49 by X27 |
| M34 | Q50 by X29 |

## Related

[W27](contract.md#questions-to-answer) for the questions; [the milestones](milestones.md) for the
gates; [the object storage part's spikes](../../object-storage/spikes.md), whose shape and rules this
keeps; [What's left to do](whats-left-todo.md) for the order drawn.
