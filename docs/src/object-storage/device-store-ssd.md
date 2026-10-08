# X6. The device store on SSD, measured

**Reported 2026-10-04.** This is the record of spike [X6](spikes.md#x6-the-device-store-on-ssd).
It measured S6's device store on the lab's SSDs as a slice's executor would drive it:

- a whole stripe chunk as a file of its own, against a slot in a shared file;
- the journal a slice stages partial updates in;
- a partial write applied in place, against one staged and cloned;
- removing chunks, listing a placement group cold, and one unit read cold;
- what a run of clones does to a chunk;
- one device under one to eight executors.

It ran on europa's Optane under XFS, and on titan's and hyperion's 970 EVOs under XFS, ext4 and
btrfs side by side on one device, four rounds each. It ends in a recommendation, which
[S18](contract.md#q22-in-part-the-device-store-on-ssd-2026-10-04) records as the choice:
**S6's store, with each whole chunk written into a file the slice keeps written ahead**,
partial updates journalled and applied in place, XFS preferred and btrfs refused, and one slice
for each SSD.

Five facts decide it:

- **None of the four results named in advance came out on the filesystems the store accepts.**
  A file a chunk is viable from 1 MiB on a device that flushes its cache (0.51 and 0.52 of a
  shared file's rate on the two 970 EVOs, the line being 0.5) and from 256 KiB on the Optane.
  One core drives both SSDs for 64 KiB units and whole chunks under XFS and ext4; only on btrfs,
  which is refused, did one slice fall short. The clone
  fails every condition but one. A cold walk that reads the label of every chunk of a million
  takes 37.5 s on the 970 EVO.
- **A fresh file's sync is the dear step**, because it commits the file's allocation as well as
  its data: 7.2 ms on the 970 EVO against 3.2 ms to overwrite the same bytes. A file from a pool
  written ahead is an overwrite, and lifts a file a chunk to 0.71 of a shared file at 1 MiB.
- **A small write in place costs two flushes**: 2 ms rested to 6 ms loaded at 4 KiB on the 970
  EVO, and 81 µs on the Optane, whose cache writes through. A clone costs two to three times
  that, and leaves the chunk at fifteen hundred extents after a thousand writes.
- **Writing the journal ahead with zeros earns its keep**: 2.3 times an appending journal's
  commits at one stager on the 970 EVO, sixteen times at 64 stagers on the Optane. Allocating
  ahead without writing earns nothing.
- **btrfs writes 81 bytes for every byte of a small write**, and copies on every overwrite, so the
  store's rule that applying needs no new space cannot hold on it.

**What was found on the way**: the 970 EVO flushes in two regimes, 0.9 ms rested and 3 ms after
sustained synced writes, which is why titan and hyperion differed where they did; both run at one
PCIe lane; glommio's directory `fdatasync` makes a rename durable; and glommio's reactor stays on
its cpu when completions come back in under a few tens of microseconds. What X6 did not settle is
under [What X6 does not settle](#what-x6-does-not-settle).

## The question

[Q22](contract.md#questions-to-answer) asks four things of the device store:

- how stripe chunks lie on a slice;
- how an update is applied;
- what its `fdatasync` strategy is;
- which filesystems it accepts.

Its preferred answer is S6's: a journal written ahead for small updates, and whole files for
large ones. [Q27](contract.md#questions-to-answer) asks what one small write in place costs, and
whether small writes should ride the metadata log instead. X6 answers the device half of Q27;
[X8](spikes.md#x8-one-small-write-three-ways) answers the end-to-end half ([reported](small-writes.md)). X6 also asks how many
slices an SSD needs for its cores to drive it, one core to a slice.

The spike's section named four results in advance that would change the design:

- creating, syncing and renaming a chunk costs so much that a file a chunk is not viable at
  small sizes;
- one core falls well short of what an SSD can do;
- a clone of a range makes a partial write one write instead of two, and its sync is as cheap as
  an overwrite's;
- listing a placement group of a million chunks is slow enough that a light scrub needs an index
  of its own.

## How it was judged

The thresholds below were set in the plan, before the harness existed, and `shoal-spike device
report` judges them as written. Every comparison is between intervals: four rounds, each side's
lowest and highest round. A difference counts only where two intervals do not overlap, which is
[the lab's rule](../performance/benchmarking.md#before-and-after-on-the-lab). A ratio is taken
round by round, so a slow round slows both of its sides, and is judged by where its whole interval
lies.

| Trigger | Fires when |
| --- | --- |
| **T1. A file a chunk is not viable at size S** | At six in flight, on XFS, the better of the two file-a-chunk sides that sync the directory once for six reaches less than 0.5× layout B's chunks a second. Layout B is the same bytes in a slot of one shared file written ahead, under one fdatasync for six. It is judged on throughput, not on one writer's latency: on a device whose cache needs flushing, a file a chunk needs two flushes (the file, then the directory) where a slot needs one, so its latency approaches twice B's whatever the filesystem does. The smallest S that passes is a floor under the chunk size, for Q25. The trigger fires if that floor is above 1 MiB |
| **T2. One core falls well short of an SSD** | For random reads of 64 KiB units and whole chunks of 1 and 4 MiB, all three hold: one slice reaches less than 0.7× the best number of slices; the gain survives the control that holds the total depth fixed; and, for reads, one slice gains nothing from depth past 32. The number offered to the inventory wizard is the smallest count whose median is within 0.9× the best and whose p99 is within 1.25× the best's. 4 KiB reads are judged the same way and reported, not used as the trigger |
| **T3. The clone is the way to apply** (judged on XFS; btrfs reported) | All five hold. (a) At 256 KiB and 1 MiB, the clone writes at most 0.6× the journal's device bytes. (b) The fdatasync after a clone is no worse than an in-place apply's, at p50 and p99, with equal flushes. (c) The clone's stage, and its stage and apply together, are no worse than the journal's at six in flight. (d) After a thousand clones into a 64 MiB chunk, a sequential read of it runs at least 0.8× and a cold unit read takes at most 1.2× a fresh chunk's. (e) The journal's writes into chunks that have been cloned into are no worse than into chunks that have not. If all five hold, the filesystems narrow to XFS and btrfs, and the fork's clone call becomes required |
| **T4. A light scrub needs an index of its own** | The cheapest walk that yields each chunk's name, length and label, taken cold on the 970 EVO's XFS, takes more than 60 s for a placement group of a million chunks, or costs more than twice as much a chunk at a million as at a hundred thousand. If only the walks that read the label from the header fail, and the walk that reads it from an extended attribute passes, the label moves out of the header: a detail of Q22, not an index |

**One condition was changed before the rounds ran.** The plan's third condition for T2 was
that one slice be at least 85% busy, executor and io_uring workers together, at its best depth.
The quick runs showed an executor thread on its cpu nearly all the time whatever its load, when
the device answers within a few tens of microseconds
([O89](../appendix/optimizations.md#o89-a-reactor-waiting-on-a-fast-device-does-not-sleep)).
So "busy" could not tell a core that runs out from one that waits. It was replaced, before the
first round, by the depth sweep's plateau. The control that holds the total depth fixed, which
the plan had for reads alone, was added for writes too (four batches of six across the slices).

Two more statements were made in advance about the journal's files. If appending runs within 10%
of a file written ahead at 6 and 64 stagers on every filesystem, writing ahead is not worth its
zeros. If a file allocated ahead matches one written ahead on its first lap, the zeros need not
be written at all.

## What was run

### The harness

`shoal-spike device` is a subcommand of the existing spike binary, in `shoal-spike/src/device/`,
as X2's `placement` is. It adds no crate to the workspace's lockfile: glommio and libc were
already its dependencies. It drives files the way a slice's executor would, with the glommio
fork's `DmaFile` on an executor pinned to one core, direct I/O throughout, and its blocking
thread on that core's SMT sibling. That last is not the fork's default: `Placement::Fixed`
puts the blocking thread on the executor's own cpu (`glommio/src/executor/mod.rs:530`), and the
blocking thread is where every rename, unlink, mkdir and clone goes. The spike writes bytes and
never parses them: there is no replay, no index and no format in it. Like every spike's, it is
thrown away.

Chunks are laid out as S6 lays them out:
`slice-<i>/chunks/<consumer>/<placement group>/<object>/<stripe>.<position>`, each a 4 KiB header
block followed by its units. The bytes are seeded noise, filled once into one buffer a size and
written from there, so no allocation or fill is timed. Every offset and length is a multiple of
4 KiB, the filesystems' block, which is also at least every lab device's logical block.

**Before any measurement, probes** check what the measurements lean on:

- that direct I/O reaches the device without a sync;
- which device counts a cache flush (the whole disk, not the LV's device mapper device);
- whether a directory's `fdatasync`, which is all glommio's `Directory::sync` issues, makes a
  rename durable as an `fsync` does;
- whether the filesystem clones a range;
- what a hop to the blocking thread costs;
- what an `fdatasync` with nothing to sync costs.

**Counting.** Each side snapshots the kernel's block counters before and after
(`/sys/class/block/<dev>/stat`): bytes from the filesystem's own device, flushes from the whole
disk. A side starts after a `syncfs` and a quarter of a second's settling, and ends with a timed
`syncfs`, the *drain*, whose bytes count toward the side, so work a filesystem defers past the
call is not lost. CPU is the process's (every thread, including the io_uring workers the kernel
runs for a sync, an allocation or a cold open) and the executor thread's own.

**Rounds.** Four of them. The sides of a cell run back to back, in the declared order in odd
rounds and reversed in even ones, and the order of the filesystems on a host alternates by round
the same way (`shoal-spike/results/x6-lab.sh`). Each cell writes about 128 MiB unless a size or
a count says otherwise. `device report` merges the rounds into intervals and judges the
triggers; its output is `shoal-spike/results/x6-report.md`.

The eight measurements:

| Measurement | What one side does | Cells |
| --- | --- | --- |
| **1. A whole chunk** | **A**: create a staged file, optionally allocate it to length (**A+**), write header and units (up to eight 1 MiB pieces in flight), `fdatasync`, rename to the chunk's name, sync the directory. **B**: write the same bytes into a slot of one shared file filled with zeros, `fdatasync`. `-each`: every chunk syncs for itself. `-batch`: six chunks of one object at once, then one sync for the six. `-replace`: the rename lands on a chunk that is there. `-dironly`: no file sync, only the directory's, which leans on the journal's ordering and is labelled so | 64 KiB to 64 MiB; one writer and six, six meaning six in flight on one executor |
| **2. The journal** | Records of a 4 KiB header and a payload. Each stager writes its own record and a committer syncs whatever has completed (**each**); or one writer copies the pending records into one buffer and writes and syncs them (**coalesced**). Files **appended**, **allocated** ahead and never written, or **written ahead** with zeros and overwritten as a ring, which is S6's journal | 4, 16 and 64 KiB; 1, 6 and 64 stagers; 3 s a cell |
| **3. A partial write** | Whole units at unit-aligned places in 4 MiB chunks, so nothing is read and merged. **J**: a journal record, group-committed, then units and header written in place and the chunk synced. **C-punch**: header and units staged in a slot of a shared staging file, group-committed, `FICLONERANGE` into the chunk, the header written, the chunk synced, the slot punched. **C-cow**: the same without the punch, so the next stage into the slot copies on write. **C-perslot**: a staging file of its own a writer, synced alone. **J′**: J on chunks that have been cloned into eight times each. Every side has chunks of its own, and one pass over its slots runs first so each starts in the state it leaves them in | 4 KiB to 1 MiB; one and six in flight, to different chunks |
| **4. Removing chunks** | A thousand chunks, cold, removed one by one through glommio, then the directory synced; sixteen objects' directories removed whole; and `unlinkat` on the executor's own thread for reference | 1 MiB and 4 MiB chunks, a header written and the rest allocated |
| **5. Listing a placement group** | Walks on a plain thread pinned to the core: **names** (`getdents64`); **statx** for lengths, in directory order and in inode order; the label from a 40-byte **xattr**; the label read from the **header** block by direct I/O, one at a time and 32 at a time on an executor. Cold (every cache dropped three times) and warm. Directories opened with `O_NOATIME`, so a walk writes nothing | 100,000 and 1,000,000 chunks; *deep*, 64 chunks an object, and *wide*, one |
| **6. One unit at a random offset** | **cold**: every cache dropped and the device woken by another read, then open, read header and unit together, close; **dentry-warm**: the inode cached; **open**: the file open and its header in memory | 2,048 chunks of 4 MiB, aged two minutes; units of 4 KiB to 1 MiB |
| **7. Fragmentation** | A thousand writes of 4 to 64 KiB into one chunk, by clone (staged, cloned, punched) or in place; extents counted by FIEMAP; a sequential read of the whole chunk and fifty cold reads of one 64 KiB unit, each against a fresh chunk | 4 MiB and 64 MiB chunks |
| **8. Slices** | One to four executors on one device, eight on europa, each on its own physical core with its own directory and a 1 GiB population, started and stopped on one clock. Random reads of 64 KiB and 4 KiB units at 32 deep, and the same with the total depth held at 32 (`-T`); whole chunks of 1 and 4 MiB written six at a time, and four batches of six held across the slices (`-T`); one slice at depths 1 to 128. fio, with io_uring and direct I/O on the same filesystem, gives the device's ceiling | 2 s warm-up and an 8 s window for reads, 1 s and 4 s for writes |

### Where, and on what

| Leg | Host | CPU | Device | Link | Filesystem and mount | What it ran |
| --- | --- | --- | --- | --- | --- | --- |
| **europa xfs** | europa | Ryzen 9 7945HX (Zen4); slices on cpus 8 to 15 | Intel Optane 900P 280 GB (`SSDPED1D280GA`, firmware E2010480); write cache *write through*, so the kernel issues no flush and a sync costs only what the filesystem writes | PCIe 3.0 ×4 | XFS on `nvme0n1p1` at `/optane`, made 2026-10-03: reflink, rmapbt, `relatime,inode64,logbufs=8,logbsize=32k` | Everything |
| **titan xfs** | titan | Ryzen Embedded V1756B (Zen1); slices on cpus 2, 4, 6, 0 | Samsung 970 EVO 500 GB (firmware 1B2QEXE7); write cache *write back*, so every sync is a flush | **PCIe 3.0 ×1**, of the drive's ×4 | XFS on an 80 GiB LV made for X6, reflink, defaults | Everything |
| **titan ext4** | titan | the same | the same device | the same | ext4 on a second 80 GiB LV, `lazy_itable_init=0,lazy_journal_init=0`, `relatime` | Everything |
| **titan btrfs** | titan | the same | the same device | the same | btrfs on a third 80 GiB LV, data single, metadata DUP, `ssd,discard=async,space_cache=v2` (its defaults) | Everything |
| **hyperion xfs**, **hyperion ext4** | hyperion | as titan | as titan, another unit | **PCIe 3.0 ×1** | as titan's | Measurements 1, 2, 3 and 8, as a repeat of titan's |

- **The three filesystems on titan share one device**, which is what separates the filesystem
  from the device. europa's Optane and the 970 EVO differ in both, and every comparison across
  them says so.
- **The 970 EVOs run at one PCIe lane.** Each drive negotiates ×1 in these Zen1 boards
  (`current_link_width`), so a sequential write tops out near 725 MB/s on either host. That is the
  link, not the drive's flash: the write cache's knee, after 8 to 10 GiB, only takes it to about
  668 MB/s (`device slc`, `results/x6-<host>-slc.md`). Every titan and hyperion rate is bounded by
  that lane, and what one core can drive is judged against fio on the same link.
- **Governor `performance`** on every host for every run, put back afterwards: `schedutil` on
  titan and hyperion, `powersave` on europa. `shoal-tmdb` was inactive on all three before and
  after; europa's Optane, which held its data directory, had been reformatted the day before. On titan and
  hyperion the weekly `e2scrub_all` timer, which snapshots and reads every ext4 LV, was held for
  the runs and started again after.
- **One `znver1` build** ran on all three hosts, as the lab's rule asks. rustc 1.100.0-nightly
  (2026-09-04), kernel 7.0.0 (`-31` on europa, `-34` on titan and hyperion), the glommio fork at
  `f4643f7`.
- Each filesystem was proved by a quick run of every measurement before its first round, and
  trimmed before each round.

**Where the run departed from the plan on [the spikes page](spikes.md#x6-the-device-store-on-ssd).**
The plan named ext4 on titan and hyperion and btrfs on europa, with XFS once fitted. Instead,
europa's Optane had become XFS, and the three filesystems were put side by side on one 970 EVO.
hyperion repeated titan's core measurements. The listing gained the label walks, the partial
write the staging variants, the probes the directory sync, and the slices measurement the depth
controls. A checksum's CPU is projected from X5's rate rather than spent, since adding the
checksum crate is M13's to do.

**Two things went wrong and were redone.** A byte cap on the slices measurement's write cells
stopped the Optane inside its two-second warm-up, so those cells measured nothing. The first
rounds were stopped, the cells were bounded by time instead, and the slices measurement was run
again on every leg. The records of the other measurements of each completed round were kept: the
code they ran did not change. Then the first rounds on hyperion showed a file a chunk at the edge
of T1, which the plan had not anticipated. A supplement, `chunk-recycle`, was added and run after the
rounds: a file a chunk taken from files written ahead with zeros, as the WAL recycles its segments,
measured beside a fresh file and layout B. A third run, after the rounds, was the run-order check
on hyperion that explains where titan and hyperion differed (`results/x6-order.sh`, below).

## What the probes found

The probes ran before every run on every leg, and gave the same answer each time.

| Probe | europa, Optane, XFS | 970 EVO, XFS | 970 EVO, ext4 | 970 EVO, btrfs |
| --- | --- | --- | --- | --- |
| A 1 MiB direct write, unsynced | 1024 KiB on the device at once | the same | the same | the same |
| A 4 KiB overwrite and its `fdatasync` | 11 µs, no flush, 4 KiB written | 0.85 to 1.1 ms, one flush, 4.6 to 5.2 KiB | 0.85 to 0.89 ms, one flush, 4.0 KiB | 3.0 to 3.2 ms, one flush, **72 KiB** |
| An `fdatasync` of a file with nothing to write | 11 µs, no flush | **0.12 to 0.60 ms, one flush** | **0.12 ms, one flush** | 25 to 43 µs, no flush |
| A rename, then the directory's `fdatasync` | 75 µs, 2 KiB | 2.1 to 2.3 ms, 2 KiB, one flush | 2.9 to 3.1 ms, 17 to 18 KiB, one flush | 2.8 to 3.2 ms, 68 KiB, one flush |
| The same with the directory's `fsync` | 75 µs, 3 KiB | 2.1 to 2.2 ms, 3 KiB, one flush | 2.7 to 2.9 ms, 20 KiB, one flush | 3.3 ms, 82 KiB, one flush |
| `FICLONERANGE` of one block | supported | supported | **refused**, `EOPNOTSUPP` | supported |
| An empty `spawn_blocking` | 13 µs | 27 µs | 27 µs | 27 µs |

Three things follow from it.

- **glommio's `Directory::sync` is enough.** It issues `fdatasync` on the directory, never
  `fsync`. On ext4 an `fdatasync` commits only what the inode's last data change needs, and a
  rename changes no size, so it might have returned without committing the rename. It does not.
  On every filesystem here the two calls wrote the same and flushed once. ext4's
  `ext4_fsync_journal` forces a full commit for anything that is not a regular file; that is
  recalled, not read here, and the probe is the evidence. Shoal's
  own directory syncs (`server/wal/mod.rs`, `replication/snapshot.rs`, `control/store.rs`,
  `shard/repair.rs`, `shard/migrate.rs`) are therefore durable, and there is nothing to file.
- **On ext4 and XFS a sync with nothing to sync still flushes the device's cache.** A committer
  that syncs on a timer, or for a stage that wrote nothing new, pays a flush each time. btrfs
  skips it, and a cache that writes through needs none.
- **btrfs writes 72 KiB to make 4 KiB durable**: it copies the block and commits its log tree.
  It is fourteen to eighteen times what XFS and ext4 write, and three times as slow on the same
  device.

## Two things about the lab's 970 EVOs

**They flush in one of two regimes.** A cache flush on a 970 EVO costs about 0.9 ms when the
drive has rested for a couple of minutes, and about 3 ms once it has taken a minute or so of
synced writes. It stays at 3 ms until it rests again. titan and hyperion agreed to a median of
1.00 over every cell both ran (whole chunk, journal and slices had 91 to 100% of cells within
10%), except the first cells of the partial write. Those ran first in hyperion's order, after a
rest, and after the fragmentation measurement in titan's. Run on hyperion four more times,
alternating a rest with the fragmentation measurement first, a 4 KiB journalled write took 2.0 ms
after a rest and 6.1 ms after the fragmentation run. Its apply's `fdatasync` took 0.9 ms and 3.0
ms. Those are titan's two figures exactly, with hyperion's drive at 34 to 39 °C where titan's
read 50 °C during its runs (`results/x6-hyperion-order-*`), so heat is not the cause. So every comparison below is between sides of one cell, run back
to back in one regime. An absolute figure on the 970 EVO that involves a sync is the regime it
was taken in, and the tables say which when it matters. The loaded regime is what a busy node
sees; the rested one is a best case. It is also why
[the cluster testing](../cluster-testing/performance.md) recorded a 970 EVO sync at 3 ms where
X6's probes saw 0.9.

**They run at one PCIe lane**, which the lab table now says: about 860 MiB/s of reads and 720 of
writes, whatever the drive could do at four.

## 1. A whole chunk

**The best file-a-chunk side, divided by layout B**, at six chunks in flight with one sync for
six. The ratio is chunks a second, taken round by round; the median is shown, and the range where
it matters. Below 0.5 is T1's line.

| Chunk | Optane, XFS | 970 EVO, XFS (hyperion / titan) | 970 EVO, ext4 (hyperion / titan) | 970 EVO, btrfs |
| --- | --- | --- | --- | --- |
| 64 KiB | 0.31 | 0.25 / 0.25 | 0.20 / 0.21 | 0.42 |
| 256 KiB | 0.62 | 0.34 / 0.34 | 0.29 / 0.28 | 0.54 |
| **1 MiB** | 0.85 | **0.51** [0.47–0.61] / **0.52** [0.51–0.55] | **0.43 / 0.43** | 0.77 |
| 4 MiB | 0.95 | 0.81 / 0.76 | 0.70 / 0.70 | 1.20 |
| 16 MiB | 0.98 | 0.92 / 0.94 | 0.88 / 0.93 | 1.50 |
| 64 MiB | 1.00 | 1.02 / 1.02 | 0.96 / 0.97 | 1.57 |

**One chunk at a time**, where the steps show (µs at the median, 970 EVO loaded, hyperion's):

| | create | allocate | write | `fdatasync` | rename | directory sync | the whole cycle | flushes | layout B's cycle |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| XFS, 64 KiB | 115 | 93 | 200 | 7,210 | 64 | 4,050 | 11,729 | 2 | 3,349 (1 flush) |
| XFS, 1 MiB | 116 | 83 | 1,375 | 7,552 | 64 | 4,055 | 13,252 | 2 | 4,866 |
| ext4, 64 KiB | 112 | 51 | 153 | 7,287 | 70 | 7,125 | 14,805 | 2 | 3,329 |
| Optane XFS, 64 KiB | 45 | 29 | 69 | 46 | 27 | 43 | 260 | 0 | 66 |

**Recycled files.** The supplement measured a third way beside A+ and B in one run: a file from
a pool written ahead with zeros, overwritten, synced and renamed into place. R÷B, six in flight:

| Chunk | Optane, XFS | 970 EVO, XFS (hyperion / titan) | 970 EVO, ext4 (hyperion / titan) | 970 EVO, btrfs |
| --- | --- | --- | --- | --- |
| 64 KiB | 0.65 | 0.42 / 0.40 | 0.33 / 0.33 | 0.48 |
| 256 KiB | 0.84 | 0.57 / 0.57 | 0.49 / 0.49 | 0.54 |
| 1 MiB | 0.93 | 0.71 / 0.84 [0.48–1.22 over both] | 0.64 / 0.64 | 0.77 |
| 4 MiB | 0.97 | 0.91 / 0.90 | 0.86 / 0.85 | 1.16 |
| 16 MiB | 1.00 | 0.97 / 0.97 | 0.96 / 0.92 | 1.40 |

The 1 MiB cell on the 970 EVO's XFS spans the drive's two regimes within the cell, which is why
its range is wide on both hosts; its sides still ran back to back. On btrfs a recycled file
gains nothing over a fresh one, since an overwrite there is an allocation.

What these say:

- **A fresh file's `fdatasync` is the dear step**: 7.2 ms on the 970 EVO against 3.2 ms for the
  same bytes overwritten in layout B's file, because it commits the file's allocation in the
  filesystem's journal as well as the data. The directory's sync adds 4 ms on XFS and 7 ms on
  ext4, whose commit writes 17 KiB where XFS's writes 2. That is two flushes and two journal
  commits where B pays one flush.
- **A recycled file's sync is an overwrite's**: 1.3 ms against A+'s 4.9 ms at 64 KiB, six in
  flight in the same run, so only the directory's sync is left on top of B. It lifts a file a
  chunk from 0.43 of B to 0.71 and 0.84 at 1 MiB on the 970 EVO's XFS, and to 0.57 at 256 KiB,
  and from 0.31 to 0.65 at 64 KiB on the Optane.
- **On btrfs a fresh file beats layout B from 4 MiB.** Overwriting B's slot is not an overwrite
  there: every write is copied to new blocks and checksummed, so B pays an allocation and a fresh
  file pays nothing extra. It also means btrfs never offers the cheap path S6's journal relies
  on (below).
- **A directory sync for six is better than six**: on the 970 EVO's XFS at 64 KiB, 346 chunks a
  second against 244 on hyperion and 361 against 256 on titan, and 15% more at 1 MiB. On the
  Optane, where a sync costs no flush, it made little difference. A batch is the chunks of one
  object.
- **Leaning on the journal's order alone** (`A+-dironly`, no file sync) recovers much of the
  gap, 0.69 to 0.77 of B at 1 MiB. It is not durable by POSIX, which promises nothing for a file
  whose data was not synced, and is not recommended; it says where the cost is.

## 2. The journal

**Records committed a second**, each stager writing its own record and a committer syncing
whatever had completed. A record is a 4 KiB header block and the payload. The 970 EVO is in the
loaded regime throughout.

| Payload, stagers | Optane XFS: written ahead / appended / allocated | 970 EVO XFS (titan) | 970 EVO ext4 (titan) | 970 EVO btrfs |
| --- | --- | --- | --- | --- |
| 4 KiB, 1 | 25,310 / 8,783 / 12,634 | 316 / 134 / 137 | 312 / 140 / 138 | 121 / 120 / 120 |
| 4 KiB, 6 | 94,743 / 14,346 / 36,781 | 1,688 / 414 / 452 | 1,052 / 415 / 818 | 341 / 358 / 336 |
| 4 KiB, 64 | 318,751 / 19,686 / 63,035 | 12,414 / 2,611 / 3,476 | 8,034 / 2,781 / 3,849 | 2,475 / 2,524 / 2,409 |
| 64 KiB, 1 | 14,401 / 6,633 / 9,168 | 292 / 129 / 132 | 287 / 131 / 135 | 112 / 114 / 113 |
| 64 KiB, 64 | 37,539 / 17,766 / 33,793 | 6,408 / 2,505 / 2,739 | 4,391 / 2,372 / 2,748 | 1,897 / 1,960 / 1,959 |

- **Writing ahead earns its zeros everywhere but btrfs**: 2.3 times an appended journal's rate at
  one stager on the 970 EVO, 2.9 on the Optane, and 16 times at 64 stagers on the Optane. An
  append changes the file's size, so every sync commits the filesystem's journal too; ext4's
  appended 4 KiB record cost the device 32 KiB against the written-ahead record's 8.
- **Allocating ahead is not writing ahead.** On the 970 EVO an allocated journal runs at an
  appended one's rate, since its first write into each block converts it, which is the same
  journal commit. So the zeros have to be written, once, when the journal is made.
- **One stager's commit costs one flush**: 3.1 ms at the median on the 970 EVO, 38 µs on the
  Optane. At 64 stagers a sync carries 32 records on average.
- **Coalescing the pending records into one write** gains at 64 stagers on ext4 (13,971 against
  8,034 at 4 KiB) and nowhere much else; a group commit of separate writes is enough.
- **btrfs gains nothing from writing ahead**: an overwrite is a new allocation there, and a 4 KiB
  record cost the device 125 KiB.

## 3. A partial write

This is the device half of Q27: what one small write in place costs. Stage plus apply, at the
median; then the device's bytes for each byte of payload. The 970 EVO's figures are hyperion's
run-order check, four runs, so that each regime is measured apart: only the 4 KiB cells, which
run first, differ between them. ext4 ran only the journal's side, so its small cells caught the
drive rested.

| | Optane XFS: journal / clone | 970 EVO XFS, rested: journal / clone | 970 EVO XFS, loaded: journal / clone | 970 EVO ext4, rested: journal | 970 EVO btrfs: journal / clone |
| --- | --- | --- | --- | --- | --- |
| 4 KiB, one in flight | **81 µs** / 333 µs | **2.0 ms** / 6.4 ms | 6.1 ms / 14.7 ms | 1.9 to 2.5 ms | 16.7 ms / 16.8 ms |
| 4 KiB, six in flight | 145 µs / 965 µs | 5.7 ms / 16.4 ms | 20.9 ms / 39.6 ms | 7.0 ms | 34.3 ms / 26.6 ms |
| 64 KiB, one | 138 µs / 439 µs | | 6.7 ms / 18.1 ms | 2.2 to 2.7 ms | 17.6 ms / 17.6 ms |
| 1 MiB, one | 910 µs / 962 µs | | 9.8 ms / 20.5 ms | 9.6 ms | 24.6 ms / 21.3 ms |
| 1 MiB, six | 4.8 ms / **3.1 ms** | | 27.3 ms / 52.6 ms | 45.9 ms | 65.3 ms / 57.6 ms |
| Device bytes ÷ payload, 4 KiB | 4.0 / 12.8 | 4.0 / 11.7 | 4.0 / 11.7 | 4.0 | 84 / 82 |
| Device bytes ÷ payload, 1 MiB | 2.0 / 1.1 | | 2.0 / 1.1 | 2.0 | 2.4 / 1.4 |

- **The journal's write in place is two flushes**: one for the stage, one for the apply. On the
  970 EVO that is 2 ms rested and about 6.5 ms loaded at one in flight; on the Optane, whose cache
  writes through, 81 µs. X8 adds the network and the commit to it, ✅ and
  [did](small-writes.md): 7.0 ms for a staged 4 KiB write on the lab, 1.5 ms on the Optane.
- **The clone is dearer in every way but bytes at large sizes.** Its stage writes into a hole or a
  shared block, which is an allocation; its apply remaps extents in a transaction; and the
  `fdatasync` after it commits that transaction: 2.9 ms against the journal apply's 0.9 ms on the
  rested 970 EVO, 58 µs against 12 µs on the Optane. Only at 1 MiB, six in flight on the Optane,
  where the journal's double write meets the device's bandwidth, does the clone finish first.
- **A chunk cloned into stays dearer**: the journal's writes into chunks that had been cloned into
  took 22.6 ms against 14.9 at 64 KiB, six in flight on the 970 EVO's XFS.
- **btrfs writes 81 bytes for every byte of a small write**, journal or clone, and 17 ms for one
  write in place.

## 4. Removing chunks

| µs a chunk, unlink to drained, 1 MiB chunks | Optane XFS | 970 EVO XFS | 970 EVO ext4 | 970 EVO btrfs |
| --- | --- | --- | --- | --- |
| One by one through glommio, then the directory synced | 72 | 194 | 98 | 161 |
| Sixteen objects' directories, removed whole | 67 | 185 | 99 | 165 |
| `unlinkat` on the executor's own thread | 50 | 138 | 53 | 127 |

Removing is cheap next to writing: a thousand chunks take a fifth of a second on the 970 EVO.
Going through glommio's blocking thread adds about 40 µs a chunk on Zen1 (the hop is 27 µs at the
median). btrfs defers its work past the unlinks: its drain took 54 ms a thousand.

## 5. Listing a placement group

**Seconds, cold, for a placement group of a million chunks**, deep (64 chunks an object) / wide
(one):

| Walk | Optane XFS | 970 EVO XFS (titan) | 970 EVO ext4 (titan) | 970 EVO btrfs |
| --- | --- | --- | --- | --- |
| names | 0.83 / 8.3 | 4.2 / 20.3 | 3.2 / **120** | 5.1 / 27.3 |
| lengths (`statx`, inode order) | 5.0 / 12.4 | 12.9 / 27.4 | 10.8 / **128** | 17.1 / 36.7 |
| labels from an xattr | 6.5 | 15.7 | 13.5 | 21.4 |
| **labels from the header**, 32 in flight | **15.8** / 23.0 | **37.5** / 52.3 | 34.8 / **154** | 46.4 / 66.4 |

- **T4's walk, the header's label read for every chunk, takes 37.5 s cold for a million chunks on
  the 970 EVO's XFS**, and 15.8 s on the Optane. It reads 4.6 KiB a chunk. Its cost a chunk at a
  million is the same as at a hundred thousand, to within 5%. A 16 TiB device of 4 MiB chunks
  would take two and a half minutes.
- **Reading the inodes in order of their numbers** made no difference on any filesystem here.
- **A label in an extended attribute** would make the walk two and a half times cheaper. It
  would also make every apply change the inode's metadata, which was not measured.
- **ext4 lists a wide placement group very slowly**: a million object directories of one chunk
  take two minutes for their names alone, reading 4.5 KiB from the device for each one where XFS
  reads 1.0. XFS takes 20 s on the same device.
- Every warm walk wrote nothing, which is the check that `O_NOATIME` kept the walks read-only.

## 6. One unit at a random offset

| µs at the median | Optane XFS: cold / file open | 970 EVO XFS | 970 EVO ext4 | 970 EVO btrfs |
| --- | --- | --- | --- | --- |
| 4 KiB unit | 184 / 16 | 933 / 127 | 1,128 / 116 | 1,139 / 146 |
| 64 KiB unit | 212 / 42 | 882 / 330 | 1,126 / 327 | 1,355 / 388 |
| 1 MiB unit | 576 / 404 | 1,883 / 1,286 | 2,104 / 1,287 | 2,645 / 1,637 |
| of which the cold open | 162 | 597 to 775 | 810 to 963 | 715 to 720 |

- **A cold open costs more than the read it precedes** at 64 KiB and below: 0.6 ms on the 970
  EVO's XFS and 0.8 ms on ext4, for the directory and inode reads (about 50 KiB on XFS and 150 on
  ext4). A node that keeps a hot chunk's file open saves it.
- The header and the unit are read together, so the header adds nothing a cold read notices.

## 7. Fragmentation

After a thousand writes of 4 to 64 KiB into one chunk:

| | Optane XFS: clone / in place | 970 EVO XFS: clone / in place | 970 EVO ext4: in place | 970 EVO btrfs: clone / in place |
| --- | --- | --- | --- | --- |
| Extents, 64 MiB chunk, fresh → after | 7 → 1,502 / 3 → 3 | 14 → 1,506 / 15 → 15 | 6 → 6 | 3 → 1,499 / 3 → **1,499** |
| Sequential read ÷ a fresh chunk's | 0.97 | 1.00 (the link limits both) | 1.00 | 1.00 |
| Cold 64 KiB unit read ÷ a fresh chunk's | **2.38** (467 µs) / 1.00 | **2.59** (1,312 µs) / 1.04 | 1.09 | 1.09 / 1.09 |

A clone splits an extent in three, so a chunk's extent map grows by about one and a half extents a
clone. A sequential read barely notices, but a cold read of one unit has to load the map first,
and takes more than twice as long. On btrfs a write in place fragments exactly as a clone does,
because it is one.

## 8. One device, several slices

| MiB/s | Optane XFS: fio / 1 slice / best | 970 EVO XFS (titan): fio / 1 slice / best | 970 EVO ext4 (titan) | 970 EVO btrfs |
| --- | --- | --- | --- | --- |
| 64 KiB reads | 2,553 / **2,553** / 2,554 (2) | 857 / **862** / 862 (2) | 857 / 862 / 862 | 859 / 862 / 862 |
| 4 KiB reads | 2,299 / **1,204** / 2,307 (8) | 842 / **479** / 844 (2) | 822 / 479 / 844 | 842 / 321 / 846 |
| 4 KiB, total depth 32 | 1,208 / 1,830 (2) / 2,299 (8) | 480 / 683 (2) / 734 (4) | 479 / 683 / 733 | 325 / 518 / 650 |
| 1 MiB chunks written | 2,501 / 1,978 / 2,379 (8) | 722 / 368 / 543 (4) | 745 / 342 / 567 | 637 / 236 / 463 |
| 1 MiB, four batches on one slice | 2,226 | 528 | 565 | 337 |
| 4 MiB chunks written | 2,325 / 2,418 (4) | 606 / 675 (4) | 594 / 696 | 357 / 639 |
| 4 MiB, four batches on one slice | 2,394 | 630 | 630 | **399** |

- **One slice drives these SSDs for 64 KiB units and whole chunks.** One core reads 64 KiB units
  at fio's ceiling on both devices. Whole chunks with one batch of six in flight fall short of
  four slices, but one slice holding four batches does as well as four slices: the limit is how
  many chunks are in flight, not the core.
- **On btrfs one slice does fall short for 4 MiB chunks**, and T2's conditions hold there: one
  slice wrote 357 MiB/s against four slices' 639, and holding four batches on the one slice took
  it only to 399. The slice's own thread was 5% busy, so what ran out was not its core's cpu but
  something each slice has one of, such as its blocking thread or its ring's io_uring workers. X6
  did not trace which. On XFS and ext4 the same control closed the gap.
- **4 KiB units would need two.** A Zen1 core does about 123,000 random 4 KiB reads a second
  through glommio at depth 32, and 141,000 at 128: about 8 µs of cpu a read. fio reaches 215,000.
  A Zen4 core does 309,000 against the Optane's 588,000. With the total depth held at 32, two
  slices beat one by 43% on Zen1 and 52% on Zen4. So it is the core, not the queue.
- **A slice's thread time says little about its need when the device is fast.** With 4 KiB reads
  at depth one on the Optane, completing in 17 µs, the executor thread slept 0.03 times a read
  and was on its cpu 99% of the time, at 55,000 reads a second. At 64 KiB, completing in 43 µs,
  it slept once a read and was 46% busy. On the 970 EVO, with 100 µs reads, it slept every read.
  glommio's own runtime, without its waiting, was a quarter to a half of the thread time. A
  checksum's cost, added from X5's rate, never brought a slice near a whole core.

## What would have changed the design

| Result named in advance | Found | So |
| --- | --- | --- |
| **T1.** A file a chunk is not viable at small sizes | **No, at the line.** Its floor is 1 MiB on the 970 EVO's XFS, where it reaches 0.51 of layout B (0.47 to 0.61 over hyperion's rounds, so not wholly below 0.5), and 256 KiB on the Optane. ext4 falls below at 1 MiB (0.43), with its floor at 4 MiB. From files recycled from a pool written ahead, it reaches 0.71 at 1 MiB and 0.57 at 256 KiB on the 970 EVO's XFS, and 0.93 and 0.65 at 1 MiB and 64 KiB on the Optane | A stripe chunk stays a file of its own. Its file is taken from a pool written ahead, which the trigger did not require and the measurement says is worth having. Chunks under 1 MiB on a device that flushes pay twice the shared file's cost and more |
| **T2.** One core falls well short of an SSD | **No on XFS and ext4**, for 64 KiB units and whole chunks: one slice reaches fio's ceiling for reads on both devices, and closes its gap on writes by holding more batches in flight. **Yes on btrfs, for 4 MiB chunks**, where more batches on one slice did not close it and the slice's thread was 5% busy, so the limit there is per slice and not the core's cpu. For 4 KiB units, yes everywhere: a Zen1 core does 123,000 reads a second against the device's 215,000, and the gap survives a fixed total depth | One slice a device is what the wizard offers for an SSD on XFS or ext4. btrfs, where the trigger fired, is refused for other reasons. A pool whose chunk unit is 4 KiB on a fast device would want two. X4 and X5 already put the unit at 16 KiB or more |
| **T3.** A clone makes a partial write one write, as cheap to sync | **No.** It halves the bytes only at 1 MiB (0.52 to 0.55 of the journal's on the Optane), while its sync costs three to eight times an overwrite's, its stage costs more, a chunk after a thousand clones is read cold at 2.4 to 2.6 times a fresh one's, and the journal's own writes into a cloned chunk get dearer. Conditions (b) to (e) fail on every leg that clones; (a) fails at 256 KiB | The journal and the apply in place stay. No clone call is added to the fork, and the filesystem need not share blocks |
| **T4.** A light scrub needs an index of its own | **No.** A cold walk that reads every chunk's header takes 37.5 s for a million on the 970 EVO's XFS, 15.8 on the Optane, at the same cost a chunk as at a hundred thousand | A light scrub walks the placement group's directory and reads each header. The label stays in the header |
| Appending runs within 10% of writing ahead | **No**: 0.2 to 0.4 of its rate at 6 and 64 stagers on the 970 EVO, 0.06 to 0.47 on the Optane | The journal is written ahead, as S6 has it |
| Allocating ahead matches writing ahead on its first lap | **No**: an allocated journal ran at an appended one's rate on the 970 EVO | Its zeros are written when it is made |

## The comparison

**Where a whole chunk goes**, the 970 EVO's XFS at 1 MiB and six in flight unless it says otherwise:

| Option | Performance | Strengths | Weaknesses |
| --- | --- | --- | --- |
| **A file a chunk from a pool written ahead** (recommended) | 0.71 of a shared file; 0.93 on the Optane | A whole-chunk write is an overwrite and a rename; the directory listing is the inventory; removing a chunk is returning its file | A pool to keep: space written ahead and not yet used, and a file whose old bytes a short write would leave behind |
| A fresh file a chunk, S6 as written | 0.51; 0.85 on the Optane | Nothing to keep beside the chunks | Its sync commits an allocation: 7.2 ms against an overwrite's 3.2 on the 970 EVO |
| Chunks in slots of large shared files | the floor | One flush a batch, no rename, no directory | Needs an index that survives a crash, and a compaction for what is deleted; S6 rejected both, and the measurement gives them no reason to return above 1 MiB |

**How part of a chunk is applied:**

| Option | Performance | Strengths | Weaknesses |
| --- | --- | --- | --- |
| **Journal written ahead, then in place** (recommended) | 2 ms rested, 6 ms loaded on the 970 EVO at 4 KiB; 81 µs on the Optane | Two cheap syncs; any filesystem; idempotent replay | The bytes written twice |
| Stage and clone | 2.1 to 3.2 times slower on the 970 EVO at one in flight; 3.6 to 6.7 times on the Optane at six, except at 1 MiB | Half the bytes at 1 MiB | An allocation to stage and a transaction to apply; fragments the chunk; XFS or btrfs only |

**Filesystems**, for a device that holds slices:

| | XFS | ext4 | btrfs |
| --- | --- | --- | --- |
| A fresh chunk's cycle, 64 KiB | 11.7 ms | 14.8 ms | 17.1 ms |
| A rename made durable | 2.1 ms, 2 KiB | 2.9 ms, 17 KiB | 3.2 ms, 68 KiB |
| A small write in place | 2 ms rested | 2 ms rested | 17 ms, 81 bytes a byte |
| Writing the journal ahead | pays | pays | does nothing: every overwrite is new blocks |
| Cold walk of a wide placement group of a million | 52 s | 154 s | 66 s |
| Clone | yes | no | yes |
| **So** | **accepted, preferred** | **accepted**, with its wide directories and dearer commits | **refused** |

## Recommendation

**The device store S6 describes, with one change: a whole chunk is written into a file the slice
keeps written ahead, not a new one.** [S18](contract.md#q22-in-part-the-device-store-on-ssd-2026-10-04)
records it as the choice.

- **A stripe chunk is a file of its own**, at the path S6 gives it, so a directory listing is the
  placement group's inventory.
  - A whole-chunk write takes a file from the slice's **pool of files written ahead with zeros**
    to the chunk's length, overwrites it, syncs it, and renames it over the chunk. One directory
    sync covers the chunks of one object written together.
  - A chunk that is removed gives its file back to the pool instead of being unlinked.
  - On a device that flushes its cache, a chunk below 1 MiB costs more than twice what a slot in
    a shared file would, and below 256 KiB even from the pool. That is a floor for Q20's
    geometry to respect, not a reason to share files: above it, a file a chunk is within 30% of
    the shared file's floor and keeps S6's simplicity.
- **Part of a chunk is staged in a journal written ahead with zeros and overwritten as a ring.**
  Each record is a header block and its units. One `fdatasync` covers every record whose write
  has completed, and the apply then writes units and header in place and syncs the chunk. This is
  S6's design, measured: two cheap syncs, any filesystem.
- **No clone.** The apply is not a `FICLONERANGE`, and the fork gains no clone call (S1's optional
  row is closed as not needed).
- **The `fdatasync` strategy**:
  - one sync for a batch of stages, as a group commit;
  - one for each apply;
  - one directory sync for a batch of whole chunks;
  - a committer never syncs when nothing new has completed, because on ext4 and XFS that still
    flushes the device;
  - glommio's `Directory::sync`, an `fdatasync`, is enough for a rename on all three filesystems.
- **Filesystems**: XFS is preferred, and ext4 accepted. ext4 lists a wide placement group five
  times slower and commits a rename at greater cost. btrfs is refused for a device: a small write
  in place costs 17 ms and 81 bytes a byte there, writing ahead buys nothing, and an apply needs
  new space, which breaks S6's rule that applying needs none. tmpfs stays refused.
- **One slice for each SSD**, which is what the inventory wizard offers. A pool with 4 KiB units
  on a fast device would want two. The slice's blocking thread goes on its core's sibling, not on
  the core itself ([S13](isolation.md)).
- **A light scrub walks the placement group's directories and reads every header.** The label
  stays in the header, and no index is kept for the scrub.
- **What one small write in place costs on a device**, the half of Q27 X6 owns, is two flushes:
  2 ms rested to 6 ms loaded on the 970 EVO at 4 KiB with one in flight, and 81 µs on the
  Optane. Whether a small write should ride the metadata log instead is X8's to decide, against
  that floor. ✅ [X8](small-writes.md) decided it: below 64 KiB on a device that flushes, once a
  slice shares one flush among its applies, and never on the Optane ([Q27](contract.md#q27-and-q14-in-part-one-small-write-three-ways-2026-10-08)).

## What X6 does not settle

- **The chunk size and the chunk unit**: Q20's geometry and Q25. X6 gives the floor above.
- **The pool**: its size, how it is refilled and trimmed, what a write shorter than a chunk
  leaves behind in a recycled file, and how the pool is found again after a crash. That is M14's
  to design. Whatever a recycled file held is unreadable through the store, because a unit's
  checksum binds it to its place and its label ([P15](contract.md#the-contract)), but M14 states
  it.
- **The journal's files**: their size and number, and when an applied record's space is reused.
- ~~**Small writes in the metadata log**, the other half of Q27: [X8](spikes.md#x8-one-small-write-three-ways).~~
  Recorded by [X8](small-writes.md) ([Q27](contract.md#q27-and-q14-in-part-one-small-write-three-ways-2026-10-08)), which also found an apply's flush worth
  sharing: 1.70× for B's small writes on the 970 EVO.
- **Rotational disks**: [X7](spikes.md#x7-the-device-store-on-hdd), which reuses this harness.
  The 970 EVO's two regimes are a warning for it: a device's state can decide a figure.
- **Whether the store survives a crash as designed**: M14's acceptance tests, with
  [F70](../features/storage-faults.md)'s faults.
- **Keeping chunk files open.** A cold open costs more than a 64 KiB read on the 970 EVO. Whether
  a slice keeps a cache of open chunk files is [S9](read-path.md)'s and M14's.
- **What object work costs a table beside it**: [X9](spikes.md#x9-table-latency-beside-object-work).

## What it did not measure

- **Crashes.** Nothing was killed or torn: durability is argued from which syncs were issued, not
  shown.
- **A write smaller than a unit**, which reads and merges the unit first.
- **Real checksums.** Their cpu was projected from X5's rate.
- **Layout B as a real design**: its index and compaction were not built, so B is a floor.
- **Tuning**: ext4's fast commit, XFS's log size, btrfs's `nodatacow`, discard options.
- **The flash's own writes.** The hosts have no SMART tools, so device bytes are what the host
  asked for, not what the NAND wrote.
- **Temperature, cell by cell**: it was read only for the run-order check.
- **Other hardware**: an Intel cpu, ARM, an SSD with power-loss protection (the Optane stands in:
  its flush is free), more than one device a host, and the 970 EVO at its full four lanes.

## What it found in the glommio fork

None of these is a defect of Shoal, so none is filed on [Known Issues](../appendix/known-issues.md).
Each is written down because M14's store meets it.

| Finding | Where |
| --- | --- |
| `Placement::Fixed` puts an executor's blocking thread on the executor's own cpu, so every rename, unlink, mkdir and clone takes time from the reactor. Shoal's shards, built by a pool with `MaxSpread`, do not | `glommio/src/executor/mod.rs:530` |
| Rename, unlink and mkdir go to the blocking thread, a hop of 13 µs on Zen4 and 27 µs on Zen1, though io_uring has offered all three since 5.15 | `glommio/src/sys/uring.rs:1572-1618` |
| A directory has `fdatasync` and no `fsync`, and there is no rmdir. The first is enough (the probe); the second goes through std | `glommio/src/io/directory.rs:148` |
| `DmaFile::create` opens write-only, so a file it made cannot be a clone's source | `glommio/src/io/dma_file.rs:274` |
| On btrfs it cannot find the block device behind the anonymous device number and falls back to a 512 B alignment and no polling | `glommio/src/sys/sysfs.rs:114-122` |
| Its reactor does not sleep when completions come back fast: at 17 µs a read the executor thread slept 0.03 times a read and stayed on its cpu, at 43 µs it slept once a read ([O89](../appendix/optimizations.md#o89-a-reactor-waiting-on-a-fast-device-does-not-sleep)). Observed, not traced to a line | `shoal-spike device slices`, its depth rows |

## Related

- [X6](spikes.md#x6-the-device-store-on-ssd) for what was planned.
- [S6](device-store.md) for the design it measured, now updated.
- [S18](contract.md#q22-in-part-the-device-store-on-ssd-2026-10-04) for the decision.
- [S4](pools-and-devices.md#inventories) for the slices the wizard offers.
- [S11](scrub.md) for the light scrub.
- [S1](prerequisites.md#what-the-lab-needs-fitted) for the XFS filesystem fitted for it.
- [X5's record](checksums.md) for the form this page copies, and the checksum's rate.
- [O89](../appendix/optimizations.md#o89-a-reactor-waiting-on-a-fast-device-does-not-sleep) for
  the reactor that does not sleep.



