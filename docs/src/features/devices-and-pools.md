# F85 — M14, devices and pools on one node

## Context

[M14](../object-storage/milestones.md#m14-devices-and-pools-on-one-node) is the object store's
fourth gate, and the first that gives a pool somewhere to keep bytes. [F80](object-wire.md) made
pools committed policy with nothing behind them: every object was held in its row, and a larger put
was refused `TooLarge`. This gate builds what sits under a pool on one node:
- devices and their slices ([S4](../object-storage/pools-and-devices.md#a-device-has-slices));
- the pool map and placement ([S5](../object-storage/placement.md));
- the device store ([S6](../object-storage/device-store.md));
- the executors that own the slices and the memory object work draws from
  ([S13](../object-storage/isolation.md));
- readiness, which says what a pool is short of ([S14](../object-storage/operations.md));
- devices in the inventory, the fixture and the bench ([S15](../object-storage/performance.md),
  [S16](../object-storage/testing.md)).

**No client's byte reaches a device at this gate.** The store is driven by tests, the fixture's
verbs and the lab's harness; M15 wires the write path to it.

Every prerequisite had landed:
- the claim that tells an empty directory from a marked one ([Resolved #46](../appendix/resolved/unmarked-directory-refused.md));
- a member's failure domain ([F81](failure-domains.md)), and the optional rack and zone ([F83](rack-and-zone-domains.md));
- free bytes for every storage root ([F82](root-capacity.md));
- the store's shape on SSDs ([X6](../object-storage/device-store-ssd.md)), and where object work runs ([X9](../object-storage/table-latency.md)).

Four decisions were taken with the user on 2026-10-12:

- **D1, where object executors run.** On cpus named in `resources.object_cpus`, taken off the shards
  one thread at a time. The lab names cpu 1, the control core's other thread, which the control
  thread's whole-core reservation left idle, so no shard is given up.
- **D2, a pool whose level no member names.** A pool by `rack` on a cluster with no racks places by
  F83's fallback, each host its own rack, and readiness says so by name rather than refusing.
- **D3, the apply's flush.** `apply_sync: shared` is the default on XFS and ext4, and `each` can be
  chosen per device. The default needed evidence on a real device first:
  `shared_flush_covers_every_completed_apply` replays `dm-log-writes` logs, and passed on both
  filesystems ([below](#the-shared-flushs-evidence)). Had it failed, the default would have been `each`.
- **D4, known issue 212.** Resolved here: F58's lead score now takes its logarithm from the frozen
  table placement uses ([Resolved #212](../appendix/resolved/lead-score-logarithm.md)).

The other decisions were mine. They are recorded with their reasons under **Design choices**.

## What it does

### Configuration

A node names its devices under `storage.devices`, and the cpus its object executors run on under
`resources`:

```yaml
resources:
  cores: 6
  object_cpus: [1]            # one object executor a cpu; off the shards, one thread at a time
  object_memory: 256MiB       # the object budget, carved out of node_memory before the rows'
storage:
  devices:
    - {path: /xfs/shoal-tmdb-pool, class: ssd}                   # one slice
    - {path: /mnt/nvme1/shoal, class: ssd, slices: 2, apply_sync: each, journal: 64MiB}
  pools:
    fast: {class: ssd, redundancy: {replicas: 3}, placement_groups: 4}
```

**A device's fields.** Each field of `DeviceSpec` (`shoal-proto/src/shared/devices.rs`):

| Field | Meaning | Default |
| --- | --- | --- |
| `path` | Where its filesystem is mounted. It must exist, and is never created | — |
| `class` | The label its pools select it by | — |
| `slices` | How many executors' worth it is cut into | 1, at most 16 |
| `weight` | Its share of a pool | its size |
| `reserve` | Free bytes nothing written ahead may take | a cluster's `migration.disk_reserve`, else 1 GiB |
| `apply_sync` | `shared` or `each` | `shared` on XFS and ext4 |
| `journal` | A slice's journal, written ahead in four segments | 256 MiB |
| `free_files` | Files a slice keeps written ahead for each chunk length | 8 |

Unknown keys are refused, in a device and in `storage` itself, so `storage.device:` is an error
rather than a node with no devices. Devices are a node's own facts: they never reach the schema's
fingerprint, and the control group commits what a node reported, not what another node's file
said.

**Object cpus.** Each cpu in `object_cpus` runs one executor, and is removed from the shards'
candidates before they are chosen (`cpus_reserving`). These are refused by name (`ObjectCpuRefused`):
- cpu 0, the control cpu, a cpu that is not online, and a duplicate;
- a `cores` count the remaining cpus cannot meet;
- devices with no object cpu.

A cpu is judged as a shard's is, by whether it is online, and not by the starting thread's
affinity: the lab starts a benchmark's process under `taskset` for its client, and glommio pins
every executor past that mask. A cpu a cgroup forbids fails, by name, when its executor is pinned.
The control core's other thread is allowed when named. Its executor's blocking pool (renames and
directory syncs) is placed on that cpu's SMT sibling, or on the cpu itself when a shard holds the
sibling.

**The object budget.** `object_memory` defaults to 256 MiB. Each object executor's 16 MiB of
registered I/O memory is counted beside it. When `node_memory` is set, the rows' budget is
`node_memory − object_memory − io_memory` a cpu, and a node where that leaves the rows nothing is
refused at start.

**A pool's placement groups.** `PoolSpec` gains `placement_groups`, a power of two up to 2^16 and
1 unless set. A pool's chunk is also capped at 480 units, so every unit's checksum fits the chunk's
one header block.

### Devices and slices

A node claims its devices at start, after its storage roots and before any executor starts
(`server/devices/claim.rs`).

**Markers.** Each claimed directory carries a marker:
- the device: `<path>/shoal-device.json`, holding its format, its id (a UUID v7), the node, the cluster, its class and its slices;
- each slice: `<path>/slice-N/shoal-slice.json`, holding the slice's id, the device's id and the index;
- each slice is also held with `slice-N/shoal.lock` under `flock`, as a root's directory lock is.

Every marker is written by the primary marker's sequence: a temporary file, a sync, a rename and a
sync of the directory. The primary marker records each device's path and id (`StorageMeta.devices`),
so a node can name a device that went missing.

**The claim's outcomes**, at the device and at each slice:

| Directory | Outcome |
| --- | --- |
| Empty | A new device, or a new slice, with a new id. Unlike a table root, an empty device is new even if it was known before: a replaced disk is a new device whose chunks are rebuilt, never the old one with its chunks gone |
| Marked | Held to this node, this cluster and this device; refused by name if it is another's |
| Files with no marker | Somebody's data, refused by name (`DeviceRefused`) |

**A path that cannot be served.** These are reported, and the node still starts:
- A known device whose path is missing, or answers `EIO` at the claim, is reported **failed** under its
  known id.
- A known device emptied and claimed again is a new device, and its old id is failed too: its chunks
  are gone, to be rebuilt by M16, and never just down.

**Refused at the claim.** Each of these is refused by name:
- a device nested in or around a storage root or another device;
- two devices on one filesystem (`st_dev`), which would make one disk two failure domains, unless a
  test's override allows it;
- a change to `slices` while any slice holds a chunk. With none held, the slices are made again.

**The probe** (`server/devices/probe.rs`, a port of X6's `facts.rs`) reads what each device is on:
- **The filesystem**, from `statfs`. tmpfs is refused, since glommio turns direct I/O off there in
  silence. btrfs is refused, since every overwrite allocates. XFS and ext4 are accepted; anything
  else is served with a warning and `apply_sync: each`.
- **The disk**, from sysfs, through a device mapper's slaves and a partition's parent: whether the
  kernel calls it rotational, and what its write cache does.

The probe warns and never overrules:
- A device the kernel calls rotational under class `ssd` or `nvme`, or one it calls solid state under
  `hdd`, keeps its class and carries a warning naming both.
- A rotational device is warned that M19's rules are not yet enforced.

**The test override.** The repository's test directories are on btrfs and `/tmp` is tmpfs, so a test
names the facts it stands in for (`probe::set_override`, or `SHOAL_TEST_DEVICE_FACTS` for a
fixture's child), and every claim under one logs a warning that it did.

### Reports, commands and reads

**Reports.** Each status report carries `devices`, one `DeviceReport` a device:
- its id, path, class, size, free bytes, reserve, `major:minor`, filesystem and rotational flag;
- its warnings and its health, `serving` or `failed` with a reason;
- for each slice: its id, its index, the chunks it holds and the bytes staged on it.

The figures are read once a tick beside F82's roots, and a slice's from the atomics its executor
updates. An executor's number is never reported. The leader keeps the figures in its view of the
member (`NodeCapacity.devices`), uncommitted, as it keeps a root's.

**Two control commands**, both refused until the cluster activates wire 8
(`POOL_MAP_WIRE_VERSION`, the [F68](conditional-writes.md) precedent):
- **`ObserveDevices{node, incarnation, devices}`** commits what a device *is*: its id, class, slices,
  its size as an integer fixed at its first report, and its operator weight. The node proposes it at
  start after `ObserveMember`, fenced by its incarnation. It is kept off the member's record, which
  rides openraft's membership entries.
- **`ReportDevices{node, incarnation, seq, failed}`** commits which devices have failed. The leader
  proposes it when the set changes. One whose `(incarnation, seq)` is no newer than the last applied
  is answered applied and changes nothing, which closes the X→Y→X reordering a report replayed out
  of order would cause.

Applying `ObserveMember` copies the member's domains onto its devices. Tombstoning a member retires
them.

**Reads.** Two admin reads, added to `ADMIN_READS` and advertised:
- `devices`: the pool map, its generation, and every member's devices as last reported;
- `pool_status`: every pool's readiness.

`Readiness` gains `pools`. On a standalone node, `ShoalPool::devices()`, `pool_status()` and
`object_store()` answer the same questions.

### The pool map

The pool map (`shoal-proto/src/shared/poolmap.rs`) is
`PoolMap{generation, devices: BTreeMap<DeviceId, DeviceRecord>, exceptions, fitted}`.

**A device's record** is `DeviceRecord{id, node, seat, failed, eras}`. Each **era**
(`Era{from, until, class, domains, slices, weight}`) holds what placement reads of a device for a
span of generations; its weight is the operator's, or the device's size when it was claimed. A
pool's fitted weights are kept beside the devices, per pool, with the generations they hold for
(`FittedEra` in `fitted`). So `View::new(map, name, pool, generation)` sees the map as it was at
any generation kept, and a reader's old generation is computed again rather than stored. Every
weight is an integer.

**What moves the generation.** It moves when a device joins or leaves, is reweighted, or changes
its class, domains or slices. It does not move when a device or its member goes down or a device
fails: where a placement group belongs has not changed, only whether its chunk can be read. Failed
is committed as a set, so M16 rebuilds a failed device at once rather than waiting out a grace.

**A seat** is minted from the device's id when the device is first committed, and lives in the map
and nowhere else, never in a marker. `replace_in_seat` passes a seat to a successor, which then draws
exactly its predecessor's groups. M20's `replaces` is what will name one.

**Where it lives.**
- On a cluster: `ControlState.pool_map` (`serde(default)`). Every change bumps the topology version, so
  `Core::publish` pushes it.
- In the shards: it rides `TabletMap` as a shard-only `pool_map`, never in the client's frame.
- On a standalone node: derived at start, and written to `shoal-poolmap.json` at the primary root.

### Placement

Placement (`shoal-proto/src/shared/pool_placement.rs`) is pure, and the same on every node and
build.

**Keys.**
- **The stripe's key.** `objects::stripe_key(consumer, object, stripe)` is the `StripeMeta` row's own
  partition key: `GxHasher` over the three in order, as `#[derive]` hashes a composite key. It is
  tested equal to a generated schema's.
- **The placement group.** `pg_of(stripe_key, groups)` takes the key's top twelve bits as the tablet
  and the next `log2(groups)` as its sub-range, so a placement group is always inside one tablet and
  that tablet's group orders every write to it.
- **The draws.** `group_key`, `slice_key(seat, index)` and `mix64` follow X2's spike.

**The score** is weighted rendezvous, as X2 chose (`shared/rendezvous.rs`). `neglog2(u)` reads
`LOG2_TABLE`, 4,097 literal constants of `log2(1 + i/4096)` with 48 fraction bits, and interpolates.
The score is `neglog2(u) as f64 * inv_w`, with `inv_w = slices / weight` computed once a view; the
lowest score wins. libm is never used. Tests hold the table to a CRC, to published values, and to
within one unit of libm's at every entry.

**`View` and `place`.**
- The candidates are the eras live at the generation with the pool's class, every slice drawn in one
  order: device id, then index.
- Each failure domain at the pool's level (through F83's `domain_at`) keeps its best slice. A
  device is in exactly one domain at any level, so two chunks never share one; a member naming no
  level at or below the pool's stands in a domain of its own, `node:<id>`, and a pool by `device`
  makes each device one. The `width` lowest domains are the answer.
- An exception overrides the slice its rule names.
- An infeasible pool is reported as `Infeasible`, never placed.
- `PlacementCache` keeps answers by `(pool, consumer, pg, generation)` ([O87](../appendix/optimizations.md)).

**Fitted weights and exceptions.**
- **Fitted weights** port X2's `fit()`: 24 rounds that pull each device's draw toward its share,
  rounded to integers. The leader fits each pool again whenever the map's generation moves, and
  commits the weights (`SetPoolWeights`), like exceptions, only while the pool holds nothing.
- **Exceptions** port X2's: the overfull slices' groups moved to the least full, keeping the domain
  rule. They are committed (`SetPoolExceptions`) only while a pool holds nothing, which is all of
  M14.

### Readiness

`readiness(view, pool, status)` reports for each pool (`PoolReadiness`):
- its devices of the class, and how many are serving: not failed, on a member that is up;
- the failure domains at its level, against the width it needs, with the fallback named, for
  example "rack falls back to host on 3 members";
- its free bytes above each serving device's reserve, and the devices at their reserve;
- the capacity a pool exactly as wide as its domains can use: its smallest domain's free bytes
  times its width, since every placement group puts a chunk in every domain.

`short` names each thing it lacks, for example "it places a stripe on 6 distinct hosts and its
devices span 3". A pool with nothing short is ready. Refusing a write to a pool that is not ready
is M15's.

### The device store

A slice's directory (`server/objects/store/`):

```text
slice-N/
  shoal-slice.json  shoal.lock
  flush                                 4 KiB, written ahead; what the shared flush syncs
  journal/segment-0..3                  written ahead, reused as a free list
  free/<file bytes>/<number>            written ahead with zeros, a chunk and its header block each
  chunks/<consumer>/<tablet>-<sub>/<object>/<stripe>.<position>
```

#### The chunk

A stripe chunk is one file: a 4 KiB header block, then its units at aligned offsets. The header
(format 1) holds:
- magic and format;
- the identity: consumer, object, stripe and position;
- the unit's bytes and the number of units;
- the label of the write it holds, and how many of its bytes hold data;
- a CRC of the header itself;
- every unit's CRC.

A unit's checksum is `combine(crc64(unit), crc64(identity ‖ unit index), 37)`, so bytes that verify
at one place fail at every other (P15). A unit wholly past `length` reads as zeros without touching
the device. `chunk_format_is_frozen` holds the encoding to known bytes.

#### The journal

A slice's journal is four segments of 64 MiB each by default. They are written ahead with zeros once,
and reused as a **free list**, not a ring: a segment is written again only once every record in it
has been dropped, so a record held for long pins its own segment and nothing else. A segment's first
block names its generation. With no segment free, a stage is refused `JournalFull`, which a caller
retries.

**A record's block checks itself under a seed.** Its CRC covers the slice's id and the segment's
generation before the block's own bytes. So a record from an older use of the segment, or a client's
bytes that happen to look like a block, never reads as a record. A start then scans **every 4 KiB
block** of every segment for records, rather than walking from one record to the next. That finds a
record acknowledged after an earlier one tore, which a chain would lose at the tear.

There are two kinds of record:
- **Part of a chunk.** The record block, then the whole chunk header the apply will write, then the
  units' new values. It holds new values and never a patch, so writing it twice writes the same bytes.
- **A whole chunk.** The record block, naming the file of the pool the chunk was written into. The
  record is the index, so a start finds every staged whole chunk without walking `chunks/`.

**The committer** is X8's, with its counters, and an error is propagated to every waiter rather than
panicking. One `fdatasync` covers every record whose write completed before it began. It never syncs
with nothing new.

#### Stage, apply, read and discard

| Operation | What it does |
| --- | --- |
| Whole stage | Write the chunk into a file of the pool, then its sync (`each`) or the record's sync, which flushes the device; then the record through the committer |
| Whole apply | Rename the file over the chunk and sync its directory |
| Whole discard | Drop the record; the file goes back to the pool |
| Partial stage | Write the record. Refused `CannotMake` when the label it expects is not one the slice can make, a chunk that does not exist among them (`have: None`): a chunk's first write is whole |
| Partial apply | Verify every unit of the record, write them and the record's header in place, then `fdatasync` the chunk (`each`) or join a flush begun after (`shared`), then drop the record. A record a later one stands on is applied first |
| Read | The chunk at the label asked for, or the records chaining to it laid over the chunk. Every unit is verified, and a unit that fails is never returned |

A per-chunk `RwLock` keeps a read and an apply of one chunk apart, and every loop yields every
64 KiB of input. A failing unit with a record that covers it is applied again from that record
before the read answers.

**Space.** A stage takes no new space while its pool has a file, since the pool and the journal are
written ahead, and an apply never does. The device's reserve gates refilling them. A whole stage that
finds the pool empty makes one file for itself alone while the device has room above its reserve, and
is refused `NoSpace` by name once it has none.

**The free pool.** It keeps `free_files` files for each chunk length of the pools the device's class
serves. Each file is made under a temporary name, written with zeros, synced, and renamed in whole.
Files are refilled on the background queue, and a start removes temporary files left behind.

**A failing device.** Any I/O error marks the device failed, and every slice with it. It is reported,
and every later operation on it is refused `DeviceFailed`. The node and its tables go on.

#### What a start finds

Recovery is a pure classification (`record::classify`) over a record, the chunk's header, and
whether the units it covers verify:

| Outcome | When | What a start does |
| --- | --- | --- |
| **Applied** | The chunk carries the label the record makes, and every covered unit verifies | Drops the record |
| **Torn** | The chunk carries that label and a covered unit fails, or carries the label the record expects and a covered unit fails | Applies it again from the record |
| **Staged** | The label it expects can be made | Keeps it |
| **Inert** | Otherwise | Keeps it, and reports it |

A whole-chunk record whose file holds the chunk is staged. One whose file does not verify is
dropped, since its stage was never acknowledged.

There are no tombstones. A discarded loser comes back after a crash until M15's row excludes it
again, which P16 makes safe.

#### The shared flush

Under `apply_sync: shared`, an apply is not made durable by a sync of its own chunk. Instead it joins
a sync of the slice's `flush` file, through a committer of its own, and that sync must have begun
after the apply's writes completed. On XFS and ext4, a sync of an unchanged file still flushes the
device's cache (`xfs_file_fsync` and `ext4_sync_file` both issue `blkdev_issue_flush` when there is
no log to force). That flush covers every write the device completed before it.

This is correct only if a chunk's writes need nothing but the cache flushed. That holds because a
chunk file is only ever overwritten within extents that are already written:
- a whole chunk's file was zero-written and synced when the pool made it;
- a partial apply writes inside a chunk that exists;
- no write extends a file or meets an unwritten extent, so there is no metadata to commit.

`shared_flush_covers_every_completed_apply` is the evidence ([Performance](#performance)). The
device is chosen as the flush's target, never the journal's, since at M19 a journal can live on
another device.

### Object executors and the budget

`ObjectStore::start(plan)` starts one executor a cpu in `object_cpus` (`server/objects/executor.rs`):
- `Placement::Fixed(cpu)`, with its blocking pool placed explicitly on the sibling cpu;
- three task queues, each with a 100 µs latency goal: foreground (1,000 shares), apply (500) and
  background (100);
- the node's slices dealt round-robin over the executors. Each slice is opened on its owner, and
  every request for it goes there. The owner table is the node's own and never sent to a peer.

It starts in `ShoalPool::ready` once the shards answer, as X9's executor did, so the rings it
registers never take a locked-memory budget a shard was owed. It stops in `exit` before the control
plane, closing every slice once every sync asked of it is answered.

**The handle.** `ObjectStore` is `Clone + Send`, over kanal. It offers `stage_whole`, `stage_units`,
`apply`, `discard`, `read` and `chunk_label`, and `owners`, `served_on`, `budgets` and `slices` for
tests and the harness.

**The budget** (`budget.rs`). `object_memory` is split over the executors, and each request draws its
bytes on the executor before any is allocated. A draw that does not fit waits up to 250 ms, then is
shed `Budget`, a retryable code. `try_draw_stream(window)` charges a window and 4 MiB of socket
buffers, for M15's streams. A `Permit` gives its bytes back when dropped.

### Operations

**The inventory** (`shoaladm/src/deploy/inventory.rs`) carries `devices` at three levels, and the most
specific list wins whole:
- the deployment;
- a group;
- a node.

`Resources` carries `object_cpus` and `object_memory`. Both are rendered into the node's file.

`validate` refuses:
- devices with no object cpu;
- a device that cannot be (its own `check`);
- a device inside or around a storage root, the remote directory, or another device on its host.

**Deploying.**
- `preflight` treats `shoal-device.json` as a claimed node's marker, and refuses a device directory
  that is missing, since a device is a mounted disk that deploy never creates.
- `stage` hands each device's directory, and not its contents, to the node's user.
- `destroy` and a wipe empty a device only when it holds Shoal's marker, and name one that holds
  anybody else's files.

**`status`** draws a devices section under the roots, one line a device:
- its path, class, slices and `major:minor`;
- its size and free bytes;
- its slices' staged bytes and chunks, summed;
- its health, its warnings beside it.

**`pools`** is a cluster action, read-only like `grants`. It draws each pool's policy as placement
reads it, its devices and domains against what it needs, its free and usable bytes, and what it is
short of. This closes F80's todo for a `pools` command. shoalctl draws both through the same model.

**The bench** (`shoaladm bench`) moves each device beside itself (`<path>-bench`), at every level it
is named, and never into `--bench-storage`'s one directory: a device is a mounted disk.
- `check_disjoint` holds the copy's devices apart from the inventory's roots and devices.
- The bench makes its devices without `-p` before bring-up, and records each one so it is removed
  once empty on every way out.
- The wipe guard reads device markers as it reads roots'.
- A capture's provenance gains `pool_shape`: each pool's policy, and each node's devices by class,
  slices, filesystem and disk, never a path or a size. `compare` refuses two captures of different
  shapes as a fact, which a run may waive by name.
- The inventory's shape digest keeps class and slices and drops paths.

### The fixture

`ClusterBuilder::devices(id, &[(name, class, slices)])` gives a node devices under a directory of its
own (`device_dir(id, name)`). A child given devices reads them from a file the fixture writes, stands in XFS for its
facts through `probe::set_override`, and gets an object cpu: the control core's other thread, else a
data core's other thread. (`SHOAL_TEST_DEVICE_FACTS` does the same for a child started some other
way.) Two verbs:
- `DEVICES` answers the node's device reports;
- `STORE <json>` stages, applies, reads or discards on a slice, through the node's `ObjectStore`.

`FAULT_DIR <device> lost | torn | full` arms F70's faults under a device as under a root. A torn
write ends the child with 137 after syncing the prefix, so the crash tests run on a standalone child
and start it again.

### The spike's code goes, and its measurements come back

- **X9's spike code is removed:** `shoal-core/src/server/x9.rs`, the `x9` feature of shoal-core, shoal
  and shoal-bench, `x9_report.rs`, the harness's marks, and shoal-core's optional `rusty_erasure`
  and `crc-fast`. The lockfile keeps both, through shoal-spike.
- **`object-load` replaces it** in shoal-bench (`workloads/harness/object_load.rs`). Asked for by
  `SHOAL_OBJECT_LOAD_DEVICE`, it gives the workload's server a device, a 4+2 pool of 64 KiB units with
  1 MiB chunks, and an object executor on `SHOAL_OBJECT_LOAD_CPU`. It then stages and applies whole
  chunks on that executor at `SHOAL_OBJECT_LOAD_RATE_MIB`, rewriting a ring of chunks, and reports
  the measured phase apart.
- **`shoal-spike store`** retakes X6's figures from the product's own code:
  - a small write staged and applied at each sync, rested and loaded, at each depth;
  - whole 1 MiB chunks beside one file synced each;
  - a placement group of a million chunk files listed cold.
- **`shoal-spike fanout`** sizes the real `PoolMap` beside X2's model of it.

### Found on the way

- **A dropped glommio file is closed lazily**, on the reactor's next I/O cycle, so one dropped as its
  executor stops is never closed. The first `shared_flush` run found a closed slice still holding its
  flush file, its four segments and the chunks its last applies touched, and the device could not
  be unmounted. The store now closes every file it is done with (`store::close`), the committer
  closes its file on `stop`, the journal its segments on `close`, and `Slice::close` gives the
  reactor one turn for a file dropped on an error's way out. The test asserts that nothing is left
  open.
- **io_uring tears a ring down after its executor is gone**, so a filesystem can be busy for a moment
  after the last executor using it stopped. The test's teardown retries its unmount. A node that
  stops its object executors and then has a device unmounted under it will meet the same thing.

## Design choices

**Eras, not snapshots.** S5 asked that a placement group's slices be computable at any kept
generation. Storing each generation whole costs a map a change. An era a device costs what changed,
and `View::at` reads the one live at a generation. A device's domains moving (F83's rack added) is
an era too, which is why `ObserveMember` refreshes them.

**Integer weights, and an integer table.** The map crosses the control group as JSON. A float weight
that did not round-trip to the bit, or a libm that differed in its last bit, would make two nodes
draw differently at a near tie: X2 measured libm choosing differently for 4 of 39 million chunks.
Every weight is a `u64`, the logarithm is a frozen table, and the one `f64` operation after it is a
correctly rounded multiply. That is also #212's fix.

**The stripe's key is the row's.** A placement group must be a sub-range of the tablet that orders
its writes. Reproducing `StripeMeta`'s own partition hash makes that true by construction, and a
test holds it to a generated schema's value.

**Two commands for devices.** Identity (`ObserveDevices`) changes once in a device's life and moves
the generation. Runtime state (`ReportDevices`) changes often and must not. Keeping them apart keeps
reports from moving placement, and the `seq` on reports fixes the reordering `ReportShards` allows.

**A seat outside the marker.** The seat is minted when the map first commits the device, from its
id, and lives in the map alone. M20's replacement has one source of truth, and a disk moved between
hosts carries nothing that could claim a seat.

**The device path is never created.** An unmounted disk leaves its mount point empty, or absent. A
claim that created it would take the root filesystem for a new device and fill it. Deploy refuses
a missing device directory, and the bench makes its own without `-p`, so a missing parent (an
unmounted disk) is refused there too.

**A journal of free segments with a seeded scan.** S6's ring pinned every later record behind one
held long, and a chain walked from the head stops at a tear. A free list pins only the segment a
held record is in, and a seeded CRC makes every block self-identifying, so the start scans blocks.

**The record is the index of whole chunks.** X6's whole chunk is written into a file of the pool.
Naming that file in the record means a start recovers every staged chunk from the journal alone, and
never waits on a walk of `chunks/`, which X6 timed at 37.5 s cold for a million. The chunks a slice
holds are counted for its report by a walk on the executor's blocking pool, in the background, after
the slice serves.

**The shared flush on the device, through a file of its own.** Syncing the journal would flush the
same device today, but at M19 a journal can live on another device. A 4 KiB file of the slice's own
names the device the chunk is on.

**Executors of their own, started late.** X9 found a step of a 1 MiB unit on the table shards moved
the reference cell's read p99 two to four times, and the same work on a core of its own moved it
nothing measurable. Starting in `ready`, after the shards have registered their buffers, is what
X9's executor did for the locked-memory budget.

**The budget is drawn on the executor.** A draw that waits uses a glommio timer. Drawing on the
caller would put a glommio timer on a tokio thread. The request carries its size, and the executor
draws before it allocates.

**`object-load` drives the store through the node's own handle.** The bytes are made on the
client's runtime, where M15's stream will be, so the object executor does the store's own work and
nothing a spike stood in for.

## Alternatives rejected

| Alternative | Why not |
| --- | --- |
| Object work on a third queue of every table shard | X9: the reference cell's read p99 moved 2.0 to 3.8 times at 1 MiB units, with or without a latency goal |
| A seat in the device's marker | Two sources of truth for M20's replacement, and a disk moved between hosts would carry a seat into another node's map |
| Down or failed as a generation | Every member's restart would move placement. Placement says where chunks belong; health says whether they can be read |
| One command for identity and health | A report every tick would move the generation, or the generation would ignore a reweight |
| `f64` weights in the map | JSON round-trips need not be exact to the bit, and a near tie would split nodes |
| A strict ring journal | One record held for long blocks every later stage, and a walk from the head cannot get past a torn record |
| Tombstones for discarded stages | P16 makes a resurrected loser safe until M15's row excludes it; a tombstone a discard is a write a discard. Filed in todos as an option |
| `apply_sync: each` by default | Shared covers several applies with one flush: in `shoal-spike store`'s quick run on europa's Optane at depth 8, 16 applies took 4 flushes. The evidence test passed on XFS and ext4 |
| A clone (`FICLONERANGE`) for a partial write | X6: it failed every condition but one |
| Creating a missing device directory | Takes an unmounted disk's mount point on the root filesystem for a new device |
| A placement group by hashing `(object, stripe)` alone | It would not be a sub-range of the tablet that orders its writes |

## Limitations

- **No client's byte reaches a device.** The write path is M15's, and so is refusing a write to a pool
  that is not ready.
- **A failed device is reported, not rebuilt.** M16 rebuilds it, and moves placement groups.
- **No listing API.** A placement group's chunks are a directory, and M16's walk lists them.
- **No `replaces`.** A device's seat passes to a successor only through `replace_in_seat`; nothing in
  the inventory names one until M20.
- **Exceptions are committed only while a pool holds nothing.** Moving chunks to honour a new
  exception is M16's.
- **Fitted weights are recomputed only when a pool's device set changes.**
- **Rotational devices are warned about, and M19's rules are not enforced.**
- **The wizard does not probe a host's devices.** An inventory's devices are written by hand or kept
  as the wizard read them (filed in todos).
- **A bench override that changes devices is not made on the hosts.** The bench makes its devices
  once, at the run's start.
- **The object budget is drawn per request.** M15's streams use `try_draw_stream`; nothing draws a
  stream's window today.
- **A slice's files are opened per operation.** There is no open-file cache (filed in todos).
- **Discarded stages have no tombstones**, so one comes back after a crash until M15 excludes it.
- **S6's ext4 warnings are not built**: neither the warning for small objects on ext4 nor the refusal
  of ext4 on a rotational device. Both are M19's, with the rotational rules.
- **A device's `journal` is a size.** S4 drew `journal: <dir>` for M19's journal on another device,
  which will need a key of its own.

## Invariants to uphold

- **The answer is the same on every node and build.** Integer weights, the frozen table, one order of
  candidates. Never take a weight as a float, or a logarithm from libm, anywhere a placement or a
  lead is drawn.
- **A placement group never spans two tablets.** `pg_of` takes the tablet's twelve bits first, and
  `stripe_key` must equal `StripeMeta`'s partition key.
- **The generation moves only when where a group belongs could change.** Down and failed never move
  it.
- **A device's path is never created**, by the node, deploy or the bench.
- **A stage is durable before it is reported, holds new values only, and takes no new space.**
- **A chunk changes in place only by applying a staged write, and a record is dropped only after its
  apply is durable.**
- **No unit that fails its checksum is returned, merged into, or used as a source.**
- **The shared flush is correct only while chunk files are overwritten within written extents.** Never
  fallocate a chunk or its pool's file, extend one, or write one through anything that leaves an
  unwritten extent, without making `apply_sync: each` the rule for it.
- **A slice is served by one executor**, and its owner is never sent to a peer.
- **Every file the store is done with on its way out is closed explicitly**, and `Slice::close` leaves
  nothing open.
- **Under `apply_sync: shared` a whole stage's file is made durable by its record's sync**, which
  flushes the device only because the journal is on the chunk's device. M19 may put a journal on
  another device: a whole stage there syncs its file or joins the slice's own flush first.
- **Nothing walks `chunks/` on the reactor.** A start's count runs on the blocking pool.
- **No object buffer is allocated before its bytes are drawn.**

## Performance

### The shared flush's evidence

`shared_flush_covers_every_completed_apply` (`shoal/tests/shared_flush.rs`, root only, ignored) puts
a `dm-log-writes` target under XFS and then under ext4, and drives a slice under
`apply_sync: shared`:
- sixteen whole chunks;
- eight writers applying one unit at a time over their own chunks;
- each acknowledgement marked in the log.

It then replays the log onto a zeroed image, to every state a crash at the last flush before an
acknowledgement could leave, plus the writes the device made durable itself. It mounts each state,
so the filesystem replays its own journal, and reads every chunk. Every acknowledged apply was in its
chunk at its label or a later one, with that label's bytes. On europa, 2026-10-12:

| Filesystem | Acknowledged applies | States judged | Log entries | Flushes | Applies the state before did not yet hold |
| --- | --- | --- | --- | --- | --- |
| XFS | 112 | 39 | 1,904 | 152 | 105 |
| ext4 | 112 | 28 | 2,436 | 161 | 111 |

The last column is what makes it evidence: most applies were not durable one judged state earlier,
so a replay that could not tell one flush from the next would have failed.

### The pool map's frame

`shoal-spike fanout`, JSON as the topology frame is pushed, every device a 1 TB `ssd` on a host of
its own rack. Bytes are the same on every host; the encode times are europa's (Ryzen 9 7945HX,
powersave), median of runs:

| Shape | Members | Devices | Slices | Frame bytes | Bytes a device | Over the tablet frame at 64 members | Encode µs |
| --- | --- | --- | --- | --- | --- | --- | --- |
| lab | 3 | 3 | 3 | 961 | 320 | 0.05× | 0.6 |
| lab-2 | 3 | 6 | 6 | 1,890 | 315 | 0.11× | 1.2 |
| 6x12 | 6 | 72 | 72 | 22,377 | 311 | 1.27× | 13.3 |
| 6x12-slices | 6 | 72 | 288 | 30,801 | 428 | 1.75× | 17.7 |
| 64x12 | 64 | 768 | 768 | 239,064 | 311 | 13.59× | 143.0 |

X2 predicted 2,747 bytes for lab-2 and 301 bytes a device: the map as built is 1,890 and 315. A change
of every device's domains keeps an era a device, 135 bytes each at lab-2.

### On the lab

Taken after this commit, on `tmdb_cluster.yaml`'s hosts; recorded below by the follow-up commit.

## Tests

| Test | What breaks if the feature is reverted |
| --- | --- |
| `shoal/tests/devices.rs` `empty_directory_is_a_new_device` | An empty device directory is not claimed as new, or an emptied one keeps its old id or its slices' |
| `devices.rs` `unmarked_directory_with_files_is_refused` | A directory of somebody's files is taken as a device |
| `devices.rs` `mislabelled_device_is_warned_about_and_not_overruled` | Detection overrules a class, or says nothing |
| `cluster_fixture` `every_device_reports_its_free_bytes` | A device's size and free bytes are missing from the report or the leader's view |
| `cluster_fixture` `slices_of_one_device_share_its_failure` | A device's slices are counted as two failure domains, or one fails without the other |
| `pool_placement` `placement_never_shares_a_failure_domain` | Two chunks of a placement group land in one domain at the pool's level, or on one device |
| `pool_placement` `placement_moves_only_what_changed` | A device joining or leaving moves groups it neither gains nor loses |
| `pool_placement` `placement_is_the_same_on_every_build` | A draw or a score moves off its frozen values |
| `pool_placement` `replacement_in_its_seat_moves_nothing_else` | A successor in its predecessor's seat moves another device's groups |
| `pool_placement` `a_placement_group_is_a_sub_range_of_its_tablet`, `stripe_key_is_frozen` | A group spans two tablets, or the stripe's key moves |
| `pool_placement` `consumers_draw_apart`, `the_cache_answers_by_generation` | Two buckets place alike, or a cached answer outlives its generation |
| `pool_placement` `readiness_names_what_a_pool_is_short_of`, `fitted_weights_even_out_unequal_devices`, `exceptions_keep_the_domain_rule_and_even_the_fill` | Readiness, fitting or exceptions stop doing what S5 and S14 ask |
| `poolmap` `the_generation_moves_once_a_change_and_never_for_nothing`, `a_device_never_moves_between_nodes`, `the_pool_map_round_trips_as_json` | The generation moves on health, or a device changes node, or the map does not survive the control group |
| `rendezvous` `the_log2_table_is_frozen`, `the_log2_table_agrees_with_published_values`, `every_log2_entry_is_within_one_of_libms`, `neglog2_follows_the_logarithm`, `the_draw_and_score_are_frozen` | The table, the interpolation or the draw changes |
| `buckets` `stripe_key_is_the_stripe_rows_partition_key` | Placement's key stops being the stripe row's |
| `cluster_fixture` `staged_write_survives_a_crash_and_applies_once` | A stage acknowledged before a crash is lost, or applied twice |
| `cluster_fixture` `torn_apply_is_written_again_from_the_journal` | A torn apply is returned torn after a restart |
| `device_store` `apply_needs_no_space` | An apply allocates, and fails on a full device |
| `device_store` `unit_checksum_binds_bytes_to_their_place` | A unit's bytes verify at another place |
| `device_store` `read_and_apply_never_overlap` | A read returns half an apply |
| `cluster_fixture` `tables_survive_pool_device_loss` | A lost device stops the node or its tables |
| `device_store` `object_buffers_never_evict_rows` | Object buffers count against the rows' budget |
| `device_store` `object_work_past_its_budget_is_shed_by_name` | Object work grows past its budget, or is shed without a name |
| `device_store` `a_slice_is_served_by_one_executor` | A slice is opened on two executors |
| `cluster_fixture` `pool_readiness_names_what_is_short` | `pool_status` stops naming a missing domain or a device at its reserve |
| `owned` `bench_copy_moves_every_device` | The bench's copy keeps an inventory's device, at any level |
| `shoal-loadgen` `compare_refuses_another_pool_shape` | Two captures of different pool shapes compare as alike |
| `shared_flush` `shared_flush_covers_every_completed_apply` | The shared flush acknowledges an apply a crash loses, or a closed slice keeps a file open |
| `slice` `a_whole_chunk_stages_applies_and_reads`, `units_stage_over_their_base_and_apply_in_order`, `a_start_recovers_what_was_staged`, `a_torn_apply_is_written_again_at_the_start` | The store's own operations, on one executor |
| `chunk` `chunk_format_is_frozen`, `a_units_checksum_is_bound_to_its_place`; `record` `a_record_is_classified_by_the_chunk_it_stages`, `a_record_reads_only_under_its_seed` | A format changes, or recovery classifies otherwise |
| `claim` `an_empty_device_is_claimed_and_kept`, `another_nodes_device_is_refused`, `a_missing_device_fails_under_its_id`, `nested_and_shared_devices_are_refused`; `probe` `the_store_refuses_tmpfs_and_btrfs_by_name`, `a_directory_is_probed` | The claim's outcomes, or the probe's refusals |
| `budget` `a_budget_holds_what_is_drawn_and_no_more`; `devices` `a_device_reads_as_documented`, `a_device_that_cannot_be_is_refused` | The budget, or a device's configuration |
| `cpus` `object_cpus_are_refused_by_name` | An object cpu that cannot be had is taken, or refused without its name, or a blocking pool lands on a shard's cpu |
| `conf` `the_object_budget_comes_out_of_node_memory` | Object work's memory is not taken out of `node_memory` before the rows', or a budget that leaves the rows nothing starts |
| `control::types` `device_commands_wait_for_wire_8_and_apply_in_order` | A device is committed below wire 8, a failure moves the generation, or an older report clears a newer failure |
| `inventory` `devices_resolve_whole_and_are_held_clear_of_every_root` | An inventory's devices merge across levels, or one inside a root, the remote directory or another device is deployed |
| `deploy_render` `a_group_split_renders_the_roots_the_engine_claims` | A rendered device or object cpu does not reach the engine, or the claim does not mark it |
| `hosts` `bench_devices_are_made_beside_and_removed_empty`; `model` `devices_are_drawn_a_line_each`; `actions` `pools_are_read_and_drawn` | The bench makes a device with `-p` or removes one that holds anything; `status` or `pools` stop drawing devices |
| `object_load` `the_plan_reads_from_its_variables`, `percentiles_are_read_from_the_samples`; `object_load_report` `the_load_runs_beside_the_cell_and_reports_its_measured_window` | The object load is not driven on the cpu asked for, or its report loses the measured window |
| `placement` `the_lead_score_takes_no_logarithm_from_libm` | [Resolved #212](../appendix/resolved/lead-score-logarithm.md) |

## Related

- [S4](../object-storage/pools-and-devices.md), [S5](../object-storage/placement.md),
  [S6](../object-storage/device-store.md), [S13](../object-storage/isolation.md),
  [S14](../object-storage/operations.md), [S15](../object-storage/performance.md),
  [S16](../object-storage/testing.md): the design this builds.
- [X2](../object-storage/placement-simulation.md), [X6](../object-storage/device-store-ssd.md),
  [X8](../object-storage/small-writes.md), [X9](../object-storage/table-latency.md): the spikes it was
  chosen by.
- [F80](object-wire.md): pools as policy. [F81](failure-domains.md),
  [F83](rack-and-zone-domains.md): the domains placement reads. [F82](root-capacity.md): the free
  bytes a device is reported with. [F70](storage-faults.md): the faults the fixture arms under a
  device.
- [Resolved #212](../appendix/resolved/lead-score-logarithm.md): the lead score on the same table.
