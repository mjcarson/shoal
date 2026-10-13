# F82. Free bytes for every storage root

## Context

A node writes under more than one directory when its configuration says so: the default
latency path holds its marker, WAL, control log and the tables' maps, the throughput path its
archives, and `storage.tables` can give a table a pair of its own. `Storage::roots` has listed
each distinct one since [Resolved #43](../appendix/resolved/marker-every-root.md), and every one
of them is claimed, locked and carries a mirror of the marker. Capacity read only one of them.
[F46](capacity-rebalancing.md) reported `statvfs` of the default latency path, once a tick, as
`StatusReport::free_bytes`. The leader kept that one figure in memory for the planner, and the
receiver of a snapshot stream checked the reserve against the same path. A node whose archives
lived on another disk could fill that disk while every check read the other one.

[S1](../object-storage/prerequisites.md#required) lists every root's free bytes as a required
prerequisite of [M14](../object-storage/milestones.md#m14-devices-and-pools-on-one-node).
[S4](../object-storage/pools-and-devices.md#capacity-and-weight) says what M14 reads: a node
reports **the size and the free bytes of each device**, the leader keeps them in memory and never
commits them, and a device that is nearly full is skipped by "the same reserve check a snapshot
stream meets today, made for each device". [X2](../object-storage/placement-simulation.md#recommendation)
draws a device by its weight, which is its size unless the planner fits another. With one figure a
node, a write would find a full disk only after its commit
([S7's schedule 13](../object-storage/write-path.md#the-schedules-that-shaped-it), [P7](../object-storage/contract.md#the-contract)).

What reads the new figures was decided with the user on 2026-10-12, before anything was built:

- every root is reported, kept in the leader's view and shown in `status`;
- the rebalance planner and the snapshot receiver check the reserve on every root a move writes
  to;
- the append reserve ([Resolved #156](../appendix/resolved/wal-failure-stops-the-node.md)) stays
  on the WAL's root, and that is recorded with its reason
  ([todos](../appendix/todos.md#the-append-reserve-on-a-nodes-other-roots)).

## What it does

### A node measures every root

`Storage::root_uses` (`shoal-core/src/server/conf.rs`) lists the roots in the order the claim
takes them, the primary first, each with what the node writes under it:

| Use | What is written there |
| --- | --- |
| `primary` | the marker, the hosting file, the WAL, the control log, and every snapshot being received |
| `latency` | tables' intent logs and archive maps |
| `throughput` | tables' archives |

A root can have several uses. One path for everything is `primary,latency,throughput`, and the
default throughput path is listed whether or not a table still writes there, since the claim
holds it either way.

`control::capacity::measure` reads each root's filesystem: its size from `statvfs`, its free
bytes as `capacity::free_bytes` has always read them (a test's override, then a full disk F70
armed under the path, then `statvfs`), and the device it is on, `major:minor` from the root's
`st_dev`. A root that cannot be read is reported with nothing free, no size and no device. The
control thread measures every root once a tick (`tick_stats`), and the tick's report and the
node's [F52](cluster-stats.md) figures both use that one measurement.

### The report carries them

`StatusReport::roots` is a `Vec<RootCapacity>` (`shoal-proto/src/shared/capacity.rs`,
`shoal::shared::capacity`):

```json
{"path": "/hdd/shoal-tmdb", "device": "8:1", "size_bytes": 6001039839232,
 "free_bytes": 5885028098048, "uses": ["throughput"]}
```

`StatusReport::free_bytes` stays, now the tightest root's free bytes, which is what a leader from
before F82 plans with. A report with no roots comes from a build before F82, and a leader on F82
reads its one figure as a root holding everything (`RootCapacity::whole_node`). The leader keeps
every member's roots in `NodeCapacity`, in memory and never committed, as F46 kept its one figure.
A change in any root's figures marks the capacity moved, which has the plans looked at early.

### `Members` and `status` show them

`Members` answers every member's `roots` beside `free_bytes`, now the tightest root's. A node
answers by the rule `Stats` already followed: the leader answers from its table, and any other
node answers its own measurement and nothing for the other members
([Resolved #232](../appendix/resolved/former-leader-capacity.md), found on the way). `shoaladm
status` and the `shoalctl` cluster tab keep the `free` column, the tightest root, and add a section
listing each member's roots when any member reports one:

```text
roots                                path                         uses                       device         size       free  used
e8973813-…                           /optane/shoal-tmdb           primary,latency            259:2      260.6GiB   246.5GiB    5%
e8973813-…                           /hdd/shoal-tmdb              throughput                 8:1          5.5TiB     5.4TiB    1%
```

The share used is the size less the free bytes, over the size, so it counts the filesystem's
reserved blocks as used. On titan's ext4 root at the bootstrap that read 30% where `df` said 28%. `cluster stats` keeps its `free` column and metric, now the tightest root's figure.

### A move is judged on every root

Every root a move writes to keeps the disk reserve free. A root the move's bytes land on keeps
those bytes too. That is a `primary` root, where a stream is assembled, or a `throughput` root,
where it is installed into archives. A root that is only `latency` keeps the reserve alone, since
a set's maps and intent logs are a small part of it. `RootCapacity::need` is that rule and
`short_root` applies it, in `shoal-proto`. The planner and the receiver both call them, so they
cannot judge a move differently.

- **The planner** (`control/planner.rs`) is given each member's roots on `NodeInput::roots`. A
  set's move carries one group of every table to the destination, so every one of the
  destination's roots is judged. A member that has reported nothing is taken at its word, as before.
  A blocked reason names the member, the root and its uses:

  ```text
  disk reserve: 9c8b… has too little free on root /hdd/shoal (throughput) to take the set and the 1073741824 byte reserve
  ```

  It no longer carries the free bytes. A plan's blocked reason is committed again whenever it
  changes, and a figure that moved with every report would have re-committed a blocked plan every
  `plan_interval`.
- **The receiver** (`shard/snapshots.rs::begin_snapshot`) measures the roots one group's stream
  and install write: the primary, and the table's own latency and throughput roots
  (`Storage::install_roots`). A volatile table installs into memory, so it writes only the
  primary. Its refusal names the root and its figures, and counts in `refused_reserve`.

### The append reserve stays on the WAL's root

`ShardReplication::check_disk` reads the free bytes of the root the WAL is on, as before.
`capacity::free_bytes` is still the function it calls. A node whose archive root fills goes on
leading; [item 91](../appendix/known-issues.md) is what a compactor meets on a full disk.

### The fixture splits roots

`ClusterBuilder::split_roots()` gives every child a throughput root of its own, a second
temporary directory beside the one it is started in, through `StagedCluster::throughput_root`.
`Cluster::throughput_dir(i)` names it, and `FAULT_DIR <that path> full <bytes>` fills it alone.

## Design choices

- **Roots, not devices.** A configuration names directories, and a root's figures are its
  filesystem's. Devices are M14's: a `storage.devices` list ~~that does not exist yet~~
  ([S4](../object-storage/pools-and-devices.md)), which [F85](devices-and-pools.md) built. The
  device each root is on is reported, so a reader that adds figures up counts a device once. S4's
  prerequisite reads "every root, which here means every device", and ~~when M14 adds devices they
  are measured the same way~~ since F85 a pool device is measured the same way: its size from `statvfs` and its free bytes through
  `capacity::free_bytes`, a test's override and F70's full disk included
  (`capacity::size_and_free`), reported once a tick beside the roots.
- **Charged by use.** A table given a pair of its own (`storage.tables`) can keep its maps and
  intent logs on a small fast device and its archives on a large one. Charging every root a whole
  set would block every move onto such a node for good, though a set's maps are a small part of
  it. A root that only holds maps and
  intent logs keeps the reserve; one that holds staging or archives keeps the set as well. On a
  node with one root, that is exactly F46's check.
- **Every root of the destination, at the planner.** A replica set's move carries one group per
  table, and every table writes under the primary and its own pair. So a move writes every root the
  destination has, and the planner needs no table's paths.
- **The table's own roots, at the receiver.** One stream is one group of one table, and the
  receiver has the configuration. It judges what that table writes, with the same rule.
- **Reported, never committed**, by F46's reasoning: figures that move every tick would fill the
  log with what nobody replays. The plan derived from them is what is committed.
- **The tightest root is the one figure.** `free_bytes` on the report, on `Members` and on a
  node's `Stats` figures is the least free of the node's roots: the one that fills first, and the
  one that stops the node taking a move.
- **A use this build does not know is read, not refused.** `RootUse` ends in
  `#[serde(other)] Unknown`, so a root a later build names another way never makes a whole report
  undecodable to an older leader. An unknown root keeps the reserve and nothing more.

## Alternatives rejected

- **Summing the roots into one figure.** A set's bytes do not spread over a node's roots in
  proportion to their room. The partial lands under the primary whole, and the archives under one
  throughput root. A sum would place a move on a node whose archive disk is full because its WAL
  disk is empty.
- **One entry per device in the report.** Two roots on one filesystem are two paths with different
  uses, and a root is what an operator configures and recognizes. The device is in each entry
  instead.
- **Every root charged the set's bytes.** Simpler, and it blocks every move onto a node with a small
  latency device, as above.
- **Each table's roots in the report, for the planner to resolve.** Every report would carry
  every table's paths, and the answer would be the same: a set's move writes all of them.
- **Extending the append reserve to every root.** Decided with the user. That reserve guards the
  WAL, whose failure stops the node ([Resolved #156](../appendix/resolved/wal-failure-stops-the-node.md)).
  What a full archive root costs is a compactor's, which is item 91's to fix, and making a node
  hand on its leads for it is a change of what the reserve means.
- **A per-root `FREE_BYTES` override in the fixture.** F70's `full` fault already arms one
  directory, and it fills as it is written, as a real disk does. The process-wide override stays
  as it was, applying to every root.

## Limitations

- **A root is a directory.** ~~M14's devices and slices are not built, and nothing reads a root's
  size yet: M14's placement weight is its first reader.~~ [F85](devices-and-pools.md) built M14's
  devices and slices beside the roots, and a device's placement weight reads the device's own size,
  fixed at its first report, not a root's: nothing reads a root's size yet.
- **Two roots on one filesystem are judged one at a time.** Each is checked against the same free
  bytes, and nothing sums what a move lands on both. A stream's partial and the archives it is
  installed into coexist until a checkpoint carries the install, so a node with one root needs
  about twice the stream, and the check asks for it once. That gap predates F82: [item 233](../appendix/known-issues.md#233-a-snapshot-install-holds-its-stream-twice-and-the-reserve-check-counts-it-once).
- **A table's own root is charged the whole set.** The planner judges every root by the set's
  bytes, though only that table's share lands on a root of its own. `shoaladm` renders no
  `storage.tables`, so no deployed node has one ([todos](../appendix/todos.md#a-tables-own-root-charged-its-own-bytes)).
- **The append reserve reads the WAL's root alone**, as above.
- **A follower answers its own capacity only.** `status` through a follower shows that member's
  roots and no other's, and names the leader to ask.
- **The override is process-wide.** `FREE_BYTES` sets every root's free bytes at once, and a
  root's size stays its filesystem's. A test that fills one root arms a full disk under it.
- **The fixture's split roots share a filesystem.** They are two directories under one temporary
  root, so their device and size are the same. A full disk under one is what tells them apart.
- **A path that is not UTF-8 is reported lossily.**
- **A root on a rotational disk is reported, not paced.** The lab's archives on its rotational
  disks shed a full load's writes
  ([O100](../appendix/optimizations.md#o100-a-tables-archives-on-a-rotational-disk-slow-a-load-to-a-tenth-and-shed-its-writes)).
  F82 says how full such a root is, and nothing about how fast it takes writes.
- **Through a rolling upgrade a leader before F82 plans by the tightest root**, which is stricter
  than F82's rule on a node with a small latency root. A member before F82 reports one figure, read
  as a root holding everything.

## Invariants to uphold

- **The planner and the receiver judge by one rule.** `RootCapacity::need` and `short_root` are
  the rule. Changing what a use is charged changes both, and a receiver stricter than the planner
  turns a plan's steps into refused streams.
- **`capacity::free_bytes` is the one source of a free figure**, and `measure` reads through it.
  A test's override and F70's full disk change every reader at once: the report, `Stats`, the
  receiver and the append reserve.
- **A root that cannot be read reports nothing free.** The planner places nothing on it and the
  receiver takes nothing onto it, as F46's one figure did.
- **`Storage::roots` is the one list of a node's roots**, and `root_uses` is built on it. A new
  place a node writes, ~~M14's devices among them,~~ is added there, so that it is claimed, locked,
  mirrored and measured alike. [F85](devices-and-pools.md#devices-and-slices)'s devices are the
  exception, and on purpose: a device is claimed by a claim of its own, carries a device marker
  rather than a mirror of the primary's, and is refused if it is nested in or around a root. It is
  measured through the same `capacity::free_bytes`.
- **`RootUse::Unknown` stays, last, under `#[serde(other)]`.** Removing it makes an older leader
  refuse a newer node's every report.
- **The report's `free_bytes` stays the tightest root's** while a leader from before F82 may run.
- **Capacity is never committed, and nothing in `apply` reads it.**
- **Only the leader answers another member's capacity** ([Resolved #232](../appendix/resolved/former-leader-capacity.md)).

## Performance

Nothing here is on a query's path. Once a tick (500 ms) the control thread runs a `statvfs`
and a `stat` for each root. The receiver measures the one to three roots a stream writes once,
at its begin. No before-and-after was taken, by the rule that only work added to a query's path
earns one.

The status report grows by a root's JSON, about 128 bytes each. `cargo run -p shoal-spike
--release -- fanout` priced it, with every figure at full width, against the same report with no
roots, which is what a build before F82 sends:

| members | report bytes, before | with two roots | bytes/s in at the leader, before | with two roots |
| --- | --- | --- | --- | --- |
| 3 | 323 | 579 | 1,292 | 2,316 |
| 8 | 548 | 804 | 7,672 | 11,256 |
| 16 | 908 | 1,164 | 27,240 | 34,920 |
| 32 | 1,628 | 1,884 | 100,936 | 116,808 |
| 64 | 3,068 | 3,324 | 386,568 | 418,824 |

That is 8% more at the leader at 64 members, and 32 KB/s. A node of one root, every node the lab
deploys, adds half of it.

### The tmdb cluster

The lab's three hosts, `tmdb_cluster.yaml`, built for znver1 at F82's tree. The user decided the
cluster's data did not need to be kept, and that the layout was to be put back once checked.

- **`verify --read quorum`, before**, on F81's build: all 1,187,691 movies, 1,188,009 release
  rows and 58,418 keyword partitions found, none missing or different.
- **Destroyed, and bootstrapped with every node's archives on a disk of their own.** The
  inventory was `tmdb_cluster.yaml` with `throughput: /hdd/shoal-tmdb` on both groups:
  europa's 6 TB disk and titan's and hyperion's 14 TB ones, each XFS. The bootstrap took 19 s.
  `status` listed two roots a member:

  ```text
  roots                                path                         uses                       device         size       free  used
  10c94c8a-…                           /optane/shoal                primary,latency            252:0       97.9GiB    67.8GiB   30%
  10c94c8a-…                           /hdd/shoal-tmdb              throughput                 8:1         12.7TiB    12.5TiB    1%
  e8973813-…                           /optane/shoal-tmdb           primary,latency            259:2      260.6GiB   247.6GiB    5%
  e8973813-…                           /hdd/shoal-tmdb              throughput                 8:1          5.5TiB     5.4TiB    1%
  ```

  Each root's device was what `stat` gave on its host: europa's Optane `259:2`, the root volume
  titan's and hyperion's `/optane/shoal` sits on `252:0`, every `/hdd` `8:1`. Every size and free
  figure was `df -B1`'s at the line's precision. Read exactly through `stats --json`, europa's
  tightest root was `df`'s to the byte, and titan's and hyperion's within 49 KB and 86 KB, the
  WAL's writes between the two reads.
- **Loaded, and the archives landed on their own disk.** Each `/hdd` root came to hold 804 to
  957 MB of archives. Its filesystem's free bytes fell by as much: titan's by 892,989,440 bytes
  against `du`'s 892,910,854, europa's by 804,474,880 against 804,389,112. The primary held the
  WAL and maps, about 1.14 GB on each host.
- **The load did not finish on that layout.** Three passes each stopped within 86 to 124 s, on a
  shed write or a lapsed lease. Each started near 42,662 rows/s and fell to 2,000 to 9,600, and
  the rotational disks stayed 90 to 98% busy after the client stopped. A shard's compactor writes
  its archives on the shard's own executor, and X7 found a rotational device wants an executor of
  its own. That is filed as [O100](../appendix/optimizations.md#o100-a-tables-archives-on-a-rotational-disk-slow-a-load-to-a-tenth-and-shed-its-writes)
  and is no part of F82. On the restored layout, the same build loaded the whole dataset in
  38.0 s.
- **`status` reached the control leader every time**, so a follower's answer
  ([Resolved #232](../appendix/resolved/former-leader-capacity.md)) was not seen on the lab. The
  fixture test is its evidence.
- **Destroyed again** (which removed every `/hdd/shoal-tmdb`), **bootstrapped from
  `tmdb_cluster.yaml`**, and loaded. `status` listed one root a member, `primary,latency,throughput`.
  The first load met [item 225](../appendix/known-issues.md#225-a-load-that-had-every-row-acknowledged-never-finished)
  for the third time: every row acknowledged in 50 s, then `0 rows/s` for ten minutes. The
  loader's 42 sockets, captured before it was stopped, were idle, with nothing queued either way.
  Run again at once, it wrote 3,382,336 rows in 38.0 s at 89,114 rows/s.
- **`verify --read quorum`, after**: the same counts, none missing or different. Every unit was
  active and every host's governor was as it had been.

## Tests

| Test | What breaks if F82 is reverted |
| --- | --- |
| `the_documented_shape_is_read` (`shoal-proto/src/shared/capacity.rs`) | A root's JSON loses a field, or a report naming only a path and free bytes is refused |
| `an_unknown_use_is_read_and_not_refused` (same) | A use a later build names refuses the report, or a report from before F82 does not stand for everything |
| `a_move_is_judged_on_every_root_by_its_uses` (same) | The tightest root is not the least free, a latency-only root is charged the set, or a short root is not found |
| `every_root_is_listed_with_its_uses` (`shoal-core/src/server/conf.rs`) | A root is missing, listed twice or given the wrong uses, or an install is judged on the default's roots for a table with its own, or on a table's roots for a volatile one |
| `every_root_is_measured_on_its_own` (`shoal-core/src/server/control/capacity.rs`) | A root's size, free bytes or device is not its own, a full disk under one root moves another's figure, or an unreadable root is reported with room |
| `the_planner_drains_balances_and_blocks` (`shoal-core/src/server/control/planner.rs`, extended) | An archive root short of the set does not block by its path, a latency-only root is charged the set, or one under the reserve alone does not block |
| `every_root_reports_its_free_bytes` (`shoal/tests/cluster_fixture.rs`) | The leader's `Members` lacks a member's second root, a root's uses, size or device, or the tightest figure; a full archive root moves the primary's figure; `Stats` does not follow the tightest root; a node hands on its leads for a full archive root |
| `a_full_archive_root_refuses_a_stream_and_blocks_a_drain_by_name` (same) | A move straight onto a full archive root is installed, or a drain onto it is planned rather than blocked by the root's name |
| `a_former_control_leader_answers_no_capacity_it_no_longer_hears` (same) | A former leader answers stale capacity for other members, or none for itself (item 232) |
| `the_cluster_model_reads_the_admin_frames` (`shoaladm/src/cluster/model.rs`, extended) | A member's roots are not read or not drawn with their share used, or a frame from before F82 draws a roots section |

**Established by reproducing it.** Each half was reverted in turn and
`a_full_archive_root_refuses_a_stream_and_blocks_a_drain_by_name` run against it.

- With the receiver judging the primary alone (`install_roots(&table, true)`), the spare took the
  stream onto its full archive root:

  ```text
  the spare never refused a stream for its archive root: {..."bytes_received":144871,...,"installed":1,...,"refused_reserve":0,...}
  ```
- With the planner given each member's first root alone, the drain planned two moves onto the
  spare and never reached `Blocked`. The receiver refused them instead:

  ```text
  NotReady("plan 441c… never reached Blocked: {"blocked":null,...,"phase":"Running",...,"steps":[{"bytes":139384,...,"state":"Moving","tablet":0,...},{"bytes":146832,...,"state":"Moving","tablet":2,...}]}")
  ```

## Related

- [S1's row](../object-storage/prerequisites.md#required), which this delivers, and
  [S4's capacity and weight](../object-storage/pools-and-devices.md#capacity-and-weight), which M14
  builds on it.
- [F46](capacity-rebalancing.md) for the planner, the receiver's reserve and the one figure this
  replaces, and [F70](storage-faults.md) for the full disk the tests arm.
- [Resolved #43](../appendix/resolved/marker-every-root.md) and
  [Resolved #46](../appendix/resolved/unmarked-directory-refused.md) for how every root is claimed.
- [Resolved #156](../appendix/resolved/wal-failure-stops-the-node.md) for the append reserve.
- [Resolved #232](../appendix/resolved/former-leader-capacity.md), found on the way, and
  [item 233](../appendix/known-issues.md#233-a-snapshot-install-holds-its-stream-twice-and-the-reserve-check-counts-it-once),
  filed on the way.
