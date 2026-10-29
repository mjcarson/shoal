# Exploratory spikes

**No spike here has run.** This page is the list of what has to be measured or modelled before the
[milestones](milestones.md) of a file tree can be more than a guess, and how each would be done. They
follow the volumes' ([volumes' spikes](../volumes/spikes.md)), whose lane a file's bytes travel, and keep
the object storage part's meaning of a spike ([its spikes](../../object-storage/spikes.md#what-a-spike-is)):
it answers a question and says in advance what result would change the design, its code is thrown away
but for X31's model, and it reports what it did not measure.

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
([Where a spike runs](../../object-storage/spikes.md#where-a-spike-runs)): titan, hyperion and europa, built
for `znver1`, scratch directories on the devices measured and never the tmdb cluster's, the tmdb node
stopped and the governor at `performance` and both put back, sides alternated by round, every table
labelled by host, cpu, governor, device and filesystem.

## The spikes at a glance

| # | Spike | Settles | Needs | Cost |
| --- | --- | --- | --- | --- |
| X31 | The namespace's model | Q53, Q57 in part, and P36–P43 | Nothing | One to two weeks |
| X32 | Metadata operations, priced | Q54, Q58's default, Q57 in part, the budget | The lab | A week |
| X33 | A directory part's size | Q55 | X32's spike build | Days |
| X34 | FUSE, classic and over io_uring | Q59 | The lab | Days |
| X35 | A call-back, priced | Q56 | The volumes' X24 spike build | Days |
| X36 | CephFS, JuiceFS and InfiniFS, read at the source *(optional)* | W29's readings | Nothing | Days |

## The spikes

### X31. The namespace's model

| Field | |
| --- | --- |
| **Question** | Do the namespace's rules, as [W30](atomic-commands.md) to [W35](mounts-and-locks.md) state them, keep P36 to P43 under every interleaving of operations, coordinators' crashes, drivers, renames, splits, mounts' ends and call-backs; and is every rule needed? Q53, and Q57 in part |
| **What would change the design** | If the safe policy breaks a property, the rule the schedule shows missing is added to its page before M35. If an unsafe setting's schedule breaks nothing, its rule is not needed and its page says so. If two crossing renames of directories can make a cycle even under the rename row, the row's scope widens before M37 |
| **Method** | [W40](model.md)'s model in `shoal-model/src/namespace/`, its oracles, its safe policy and its unsafe settings, each setting's schedule saved; `namespace_search` over three layouts on europa, titan and hyperion, compared block by block |
| **Where** | Anywhere; the search on the lab's three hosts |
| **Records** | Every setting's verdict and saved schedule; the search's blocks by host; each repair |
| **Depends on** | Nothing |
| **Cost** | One to two weeks |

### X32. Metadata operations, priced

| Field | |
| --- | --- |
| **Question** | What does each namespace operation cost on the lab, one tablet's and across groups, with inodes co-located by minted prefixes and with the owner in the sort key instead; what does an open recorded as a row cost; and what metadata load holds [W37](isolation.md#the-budget)'s budget on the table shards? Q54, Q58's default, and Q57 in part |
| **What would change the design** | If co-location buys under 30% over the owner in the sort key, it is dropped and no supplied hash is needed ([W31](namespace.md#alternatives-rejected)). If no useful rate holds the budget on the table shards with the queue and limits, executors of the namespace's own become required before M36. If recording an open costs under a tenth of a create, opens are recorded and no mount is asked before a reclamation |
| **Method** | A spike build of the one-tablet command in one table's apply and of a coordinator; mdtest's easy and hard shapes, a small-file arm and an `untar`, at depths 1 to 32; the reference cell beside each at rising rates, with and without the queue and the limits; against JuiceFS's 1,209 µs create on TiKV ([W29](prior-art.md#juicefs)) as a mark, not a budget |
| **Where** | The lab: the 970 EVO's WAL on titan and hyperion, the Optane on europa |
| **Records** | By operation, layout and depth: latency at p50 and p99, rate, commits and groups an operation; the cell's p99 ratio by rate; an open row's cost |
| **Depends on** | The lab |
| **Cost** | A week |

### X33. A directory part's size

| Field | |
| --- | --- |
| **Question** | What does a lookup and a create cost by a part's size, 256 to 16,384 entries, cold and resident, and what does compaction rewrite a create? Q55 |
| **What would change the design** | The split threshold is set from where a cold lookup's time turns. If a part of a few thousand entries is already too slow cold, a sorted condition needs archived seeks, an optional row that becomes required ([W28](prerequisites.md#optional)) |
| **Method** | X32's spike build: parts filled to each size, evicted, then lookups and creates; device bytes and compaction bytes a create |
| **Where** | titan (the 970 EVO), europa (the Optane) |
| **Records** | By part size: a cold and a resident lookup's latency, a create's, and the bytes compaction rewrote |
| **Depends on** | X32's spike build |
| **Cost** | Days |

### X34. FUSE, classic and over io_uring

| Field | |
| --- | --- |
| **Question** | What does a FUSE mount cost an operation on the lab's 7.0 kernels, classic against over io_uring; what does the writeback cache save; does an inode's invalidation write its dirty and mapped pages back first; and does the daemon's tokio client cost its queues anything? Q59 |
| **What would change the design** | If invalidation does not write dirty pages back, the writeback cache stays off and the daemon gathers writes itself ([W36](fuse.md#the-kernels-caches)). If io_uring is not enabled or gains under 10%, classic FUSE is kept. If the tokio hop costs more than a tenth of a small file's create, an io_uring-native client becomes required, as the volumes' X27 asks for ublk |
| **Method** | A FUSE spike daemon with a null namespace answering at once, and with X32's spike build behind it; creates, `stat`s, listings, 4 KiB and 1 MiB reads and writes; the writeback cache on and off; a shared writable mapping written, invalidated and read by a second mount |
| **Where** | europa, titan |
| **Records** | By transport and cache: an operation's latency and rate, the host's cpu an operation; whether each invalidation wrote back |
| **Depends on** | The lab |
| **Cost** | Days |

### X35. A call-back, priced

| Field | |
| --- | --- |
| **Question** | What does a call-back cost, by the holder's dirty bytes from none to 256 MiB, what do two mounts writing one file in turn sustain, and what does a forced revoke cost? Q56 |
| **What would change the design** | The dirty cap a file is set from how long a call-back takes at it. If a call-back at a few MiB dirty takes more than tens of milliseconds, keeping a write lease past a close until called back becomes a gate's work, not optional |
| **Method** | The volumes' X24 spike build as a file's lane, a spike coordinator granting and calling back write leases, and two writers taking turns at chosen dirty bytes |
| **Where** | The lab |
| **Records** | A call-back's time by dirty bytes; turns a second between two mounts; a revoke's time |
| **Depends on** | The volumes' [X24](../volumes/spikes.md#x24-one-4-kib-write-three-ways) spike build |
| **Cost** | Days |

### X36. CephFS, JuiceFS and InfiniFS, read at the source

*(Optional: no gate waits on it.)*

| Field | |
| --- | --- |
| **Question** | Are [W29](prior-art.md)'s readings right at pinned releases, and what do CephFS and JuiceFS cost at W39's arms on the lab's devices? |
| **What would change the design** | A borrowed mechanism read differently at the source, CephFS's rename between ranks or its caps' recall, JuiceFS's sessions and sustained inodes, InfiniFS's split, changes the page that borrowed it. A CephFS or JuiceFS on the lab far better at metadata than X32's figures reopens Q58 |
| **Method** | [X14](../../object-storage/ceph-and-s3-sources.md)'s method: releases pinned by commit, the files W29 names read and cited by line, predictions written before; optionally CephFS through cephadm, as the store comparison deploys Ceph, and JuiceFS over Redis and TiKV, every host change recorded and put back |
| **Where** | Reading anywhere; the lab for the runs |
| **Records** | Each W29 claim confirmed, corrected or still recalled; CephFS's and JuiceFS's figures at W39's arms |
| **Depends on** | Nothing |
| **Cost** | Days |

## What has to be on the record before each gate

| Gate | Decided first |
| --- | --- |
| M35 | Q53 in the model, by X31 |
| M36 | Q54 and Q58's default by X32; Q55 by X33; Q61 by design |
| M37 | Q53 confirmed on what M36 built; Q57 by X31 and X32 |
| M38 | Q56 by X35 |
| M39 | Q59 by X34; Q58 asked again |

## Related

[W41](contract.md#questions-to-answer) for the questions; [the milestones](milestones.md) for the gates; [the
volumes' spikes](../volumes/spikes.md), which come first; [What's left to do](whats-left-todo.md) for the order
drawn.
