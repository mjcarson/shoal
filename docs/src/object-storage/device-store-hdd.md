# X7. The device store on HDD, measured

**Reported 2026-10-06.** This is the record of spike [X7](spikes.md#x7-the-device-store-on-hdd).
It ran S6's device store on the rotational disks fitted to the lab that day, through X6's harness
with what a disk needs and six measurements of its own:

- a WD140EDFZ of 14 TB in titan and in hyperion;
- a WD6001FZWX of 6 TB in europa.

It ran four rounds a leg on each, on XFS and on ext4. Each filesystem was made over the whole
disk before every leg, and the host's SSD stood beside it. A supplement added after the first
round ran the cells that share one arm with the disk's write cache on and off.

[S18](contract.md#q23-what-a-rotational-device-needs-2026-10-06) records the answer to Q23.

**A rotational device runs with its write cache off, journals on an SSD of its node, and has an
executor of its own.** It also:

- applies a whole batch at once, which bounds a read's wait;
- reads whole chunks of 4 MiB or more;
- runs on XFS, not ext4;
- takes one slice.

Seven facts decide it:

- **H1 fires: a stage cannot be acknowledged from the disk.** A 4 KiB journal record and its
  sync take one revolution, 8.4 ms, when nothing else touches the disk. Beside applies in offset
  order they take 42 to 251 ms at the median, and 28 to 67 ms even with the disk's cache off. On the SSD beside it the same stage takes 0.92 ms on the 970 EVO and 37 µs on the Optane, and
  its median stays near that whatever the disk is doing.
- **A disk's volatile write cache is a liability either way.**
  - On the 14 TB disks a cache flush that meets a read stalls the read for 100 ms, and fio shows
    the same with no harness in the way. With the cache on, the foreground's small write takes
    447 ms at the median beside 64 KiB reads at 20 a second; with it off, 49 ms.
  - On europa's WD Black the sync of a 4 KiB overwrite returns in 0.38 to 0.41 ms with the cache
    on, a twentieth of a revolution, so the acknowledged block cannot be on its sector yet.
  - With the cache off every disk syncs in exactly one revolution.
- **H2 fires: a disk's slice must not share an executor with an SSD's.** Sharing one raised the
  SSD slice's stage or read p99 from 4 ms to 367 ms on titan's XFS, and its read p99 from 72 µs
  to 9.7 ms on europa's ext4, where the control on a second executor stayed level. Europa's XFS
  was the exception, at 1.1×. A disk's operation costs its own executor 55 to 96 µs of a core.
- **Offset order buys nothing; the batch's size bounds a read.**
  - Applies one at a time in offset order did no better than in arrival order, by FIEMAP or by
    inode, on any disk.
  - With the cache on, the 14 TB disks kept 8 of the 48 applies a second offered. A whole batch
    in flight kept the offer.
  - Where H3 could be judged, a foreground read waited less than one batch, so it does not fire.
- **H4 fires on europa's disk: a byte budget does not keep the foreground clear.** In the
  supplement's rounds a 10 MiB/s scrub raised a small write's p99 2.65 times with the cache on and
  1.70 with it off, wholly, and 2.0 times on its ext4. In the main rounds every XFS leg was at the
  line, 1.2 to 3.1 times at the median, and on the 14 TB disks with the cache off it stayed at or
  under the line. No budget of 10 MiB/s or more stayed wholly within 1.25× on any XFS leg.
- **A disk reads whole chunks.** Across the whole platter, as XFS lays a population out, a random
  read reaches half the sequential rate only at 4 MiB, on every disk. H5 does not fire on XFS: a
  file a chunk keeps X6's floor of 1 MiB, and finding a chunk cold costs 1.4 to 1.5 times reading
  it open, at the line. On the 14 TB disks' ext4 the floor is 4 MiB.
- **ext4 is not for a disk.** Listing a placement group of a million one-chunk objects did not
  finish in half an hour on ext4, at 7.8 ms an object directory, where XFS took 87 s. ext4 also
  quadrupled the small chunk's floor on the 14 TB disks, and is where a shared executor hurt
  most.

**What was found on the way**:

- The WD140EDFZ's characteristics page says 5400 rpm, but its sync takes one revolution at
  7200 rpm, 8.33 ms.
- The block layer merges concurrent flushes, a third of a flush each at six writers.
- An XFS filesystem spreads a population over the whole platter, while an empty ext4 keeps it
  near its parent's group: ext4's random figures were measured across 0.1 to 2 GiB and XFS's
  across 4 to 12 TiB.

What X7 did not settle is under [What X7 does not settle](#what-x7-does-not-settle).

## The question

[Q23](contract.md#questions-to-answer) asks what a rotational device needs that an SSD does
not. [S6](device-store.md#what-a-rotational-device-changes) names three choices it holds:

- where a disk's journal lives, on the disk or on an SSD of the same node;
- whether a disk's slice gets an executor to itself;
- how a disk is read: whole units, read ahead.

[S13](isolation.md#io-on-a-slice) adds a fourth, which M19 owns: applies of committed writes are
batched and, on a rotational disk, issued in offset order, and a foreground read waits for no
more than one batch. [S11](scrub.md#schedule-and-budget) and [S10](recovery.md#budgets) add a
fifth: a byte budget for each device, which on a disk is the arm's time.

The spike's section named three results in advance that would change the design:

- a stage's sync on the disk itself slow enough, above about 20 ms, that a small write cannot be
  acknowledged from it: then a journal on an SSD is required for a rotational pool;
- applies in place, reads and scrubs on one arm interfering enough that a disk needs an executor
  to itself, or a different layout;
- many small chunks costing a seek each to create and to find: then a rotational pool sets its
  inline threshold and stripe size apart from an SSD pool's, or shares files.

## How it was judged

The plan's three results became five triggers before the harness ran, each a line
`shoal-spike device report` judges as written. Every comparison is between intervals over four
rounds, each side's lowest and highest round, and a ratio is taken round by round, as
[X6](device-store-ssd.md#how-it-was-judged) judged its own. A threshold fires when a figure's
whole interval lies past it; one that straddles it is at the line. XFS legs are judged, ext4 legs
reported.

| Trigger | Fires when | Then |
| --- | --- | --- |
| **H1. A stage cannot be acknowledged from the disk** | A 4 KiB journal record and its group commit, at one stager, on S6's journal written ahead, has a median above **20 ms**: with the disk otherwise idle, or while applies in offset order run on it | A rotational pool's journal is required on an SSD of the same node, and a journal disk shared by several devices becomes one failure domain ([S5](placement.md#failure-domains)) |
| **H2. A disk needs an executor of its own** | One executor drives an SSD's slice and the disk's together, and the SSD slice's p99, of its 64 KiB reads or its 4 KiB stages, rises above **1.25×** its p99 with the executor to itself, while the control, the disk's slice on another executor, does not | A rotational slice never shares an executor with an SSD's ([S13](isolation.md#who-owns-a-slice)) |
| **H3. Offset order does not bound a read's wait** | Applies batched in offset order and offered at half of what the disk takes, and a foreground 64 KiB read's p99 lies past one batch's median plus an idle read's p99: S13's premise that a read waits for no more than one batch | M19's slice holds applies behind a waiting read, rather than submitting a batch whole |
| **H4. A byte budget cannot protect the foreground** | At a deep scrub's budget of 10 MiB/s, a small foreground write's p99 lies above **1.25×** its p99 with no scrub | A disk's scrub adapts to the foreground, in the arm's idle time, and S11's fixed budget is not enough on its own. The largest budget within 1.25× is handed to X12 |
| **H5. Small chunks cost a seek each** | (a) The smallest chunk at which a file a chunk from the pool reaches **0.5×** layout B, six in flight, is above **1 MiB**, X6's floor on the 970 EVO; or (b) a cold 64 KiB unit read, open and read, takes more than **1.5×** the same read with the file held open, which is what a shared file kept open would pay | A rotational pool sets a larger chunk size, stripe size and inline threshold than an SSD pool, or shares files |

X6's T1 and T4 are judged again as X6 wrote them and reported for a disk. Reported without a
trigger, as the plan asked: the sequential and random rates by size and depth, which give how far
a disk should read ahead; whether syncs that arrive together are merged into one flush; what the
disk's write cache costs, on and off; and one slice against two.

## What was run

### The harness

`shoal-spike device` is X6's harness ([X6](device-store-ssd.md#the-harness)), run on the disks
with what a disk needs and with measurements of X7's own. It still adds no crate to the
workspace's lockfile, still writes bytes it never parses, and is still thrown away. What changed
for a disk:

- **Facts a disk's figures need.** The kernel cuts a SATA disk's model at sixteen characters and
  has no `firmware_rev` for it, so the model is read from udev's name for the disk and the
  firmware from `device/rev`. Every table now carries the rotation rate, read from the disk's own
  block device characteristics page, whether it takes forced unit access (none of the three
  does, so every `fdatasync` is a whole cache flush), its scheduler (mq-deadline), the block
  layer's queue (64) and the disk's own (NCQ, 32). The counters add the time the disk had a
  request in flight and the requests the block layer merged.
- **The host's SSD beside it.** `--ssd-dir` names a directory on the host's SSD, the 970 EVO's XFS
  volume on titan and hyperion and the Optane on europa. The sides that put a journal or a slice
  there count that device.
- **Bounded for a device that answers in milliseconds.** A whole-chunk or partial-write side stops
  after twenty seconds whatever its count, and its count is what it wrote. The listing walks the
  two populations of a hundred thousand each round, drops the walk that reads one header at a
  time, and caps every walk at half an hour; the millions are walked once a filesystem. The
  slices measurement runs one slice and two, and fio one job. The clone sides and the
  fragmentation measurement are not run: X6 rejected the clone, and a write in place does not
  fragment.
- **Populations spread as a slice's are.** The read population's objects are spread over
  placement groups, and every population records its span: where its files start on the device,
  from the 5th to the 95th percentile, as a fraction of it. XFS gives every new directory the next
  allocation group, so its populations spread from near 0% to past 90% of the disk; that span is
  the seek every random figure on XFS was measured across.

X7's own measurements:

| Measurement | What one side does | Cells |
| --- | --- | --- |
| **seq** | A file written sequentially from its start, then read back; random reads over sixteen files of 1 GiB each in a directory of its own, and inside the first file alone (the short seek); fio, one job, beside them | Pieces of 128 KiB, 1 and 4 MiB at depths 1, 4 and 32; random reads of 4 KiB to 16 MiB at depths 1, 4 and 32 (16 MiB at 8 at most); 8 s windows |
| **sync** | Each writer a file of its own written ahead: a 4 KiB overwrite at its next place, then `fdatasync`, in a loop | 1, 2, 4 and 6 writers; 5 s |
| **contend** | One executor, as one slice is. An applier writes 64 KiB units and their chunk's header in place into 1,024 chunks of 4 MiB spread over 64 placement groups, a batch at a time, then syncs every chunk the batch touched at once. Beside it, open loops issue a 64 KiB read of a random chunk at 20 a second and a 4 KiB journal record, on the disk and on the SSD, at 20 a second each, every one timed from its slot. The applies are offered at half of what the disk takes with a whole batch in flight, measured first (`capacity`) | Batches of 32 and 128; sides `idle`, `arrival-qd1`, `offset-qd1` (one apply at a time, by FIEMAP's physical offset), `offset-ino` (by inode and offset), `kernel` (the batch in flight); 3 s warm-up and 20 s |
| **scrub** | The same population. A deep scrub reads whole chunks in order through a bucket that fills at its budget, beside open loops of small writes made as S6 makes them (a journal record on the disk, then the unit and header in place and the chunk synced) at 10 a second, and 64 KiB reads at 20 | Budgets of 0, 10, 20, 40 and 60 MiB/s and none; 20 s each |
| **shared** | An SSD's slice, open loops of 64 KiB reads at 1,000 a second and 4 KiB stages at 200, on one executor; the disk's slice doing everything a busy disk does at once (applies a batch of 32 at a time, reads four deep, whole 1 MiB chunks written into files written ahead and renamed, their directory synced) | `ssd-alone`; `shared`, both slices on the one executor; `separate`, the disk's slice on an executor of its own, the control; 20 s each |
| **wcoff** | `sync`, and the written-ahead journal and the journal's partial write at 4 and 16 KiB, with the disk's write cache off: the lab script runs `hdparm -W0` and has the kernel rescan the disk before it, and turns the cache on again after. Its sides end in `-wt` | After every XFS leg |

**Rounds and legs.** A leg is one filesystem in one round. Every leg makes its filesystem on the
disk's one partition, which spans the disk, so XFS and ext4 see the same zones and the same seek
spans: XFS first in odd rounds and ext4 first in even ones, as the lab's rule alternates sides.
ext4 is made with `lazy_itable_init=0,lazy_journal_init=0`, so nothing initialises in the
background; on 14 TB that is about twelve minutes of writing inode tables before the leg. Four rounds a
leg, `shoal-spike/results/x7-lab.sh`, each host a systemd unit of its own, the three at once.
`device report` merges the rounds and judges the triggers; its output is
`shoal-spike/results/x7-report.md`.

**The write cache supplement**, added after round 1 (below), ran on one XFS leg a host after the
rounds:

1. fio alone, with no harness in the way: 64 KiB random reads and 8 KiB synced writes, each at 20
   a second, alone and together, with the disk's write cache on and then off.
2. Four rounds of `contend` and `scrub`, the cache on then off in odd rounds and off then on in
   even ones, every side named `-wb` or `-wt`.

### Where, and on what

| Leg | Host | CPU | Disk | SSD beside it | What it ran |
| --- | --- | --- | --- | --- | --- |
| **titan hdd xfs**, **titan hdd ext4** | titan | Ryzen Embedded V1756B (Zen1); the executor on cpu 2, its blocking thread on 3; `shared`'s second executor on 4 | WDC WD140EDFZ-11A0VA0, firmware 81.00A81, 12.7 TiB, CMR, SATA 6 Gb/s, 512 B logical over 4 KiB physical | Samsung 970 EVO, an XFS volume at `/xfs` | Everything, then the supplement |
| **hyperion hdd xfs**, **hyperion hdd ext4** | hyperion | as titan | the same model, another unit | as titan | The core set (`seq`, `sync`, `journal`, `partial`, `chunk`, `chunk-recycle`, `contend`, `scrub`, `wcoff`); then `listing-1m` once on each filesystem, and the supplement |
| **europa hdd xfs**, **europa hdd ext4** | europa | Ryzen 9 7945HX (Zen4); the executor on cpu 8, its blocking thread on 24 | WDC WD6001FZWX-00A2VA0 (a WD Black), firmware 01.01A01, 5.5 TiB, CMR, 7200 rpm, 46,241 hours powered on | Intel Optane 900P, XFS at `/optane` | Everything, then the supplement |

- **Each disk is one GPT partition from 1 MiB to its end**, made by `x7-lab.sh setup` after
  wiping what the disks held before: titan's and hyperion's were members of an old ZFS pool,
  europa's an XFS filesystem. Every change to a host is in
  `shoal-spike/results/x7-host-changes.txt`.
- **Every leg made its filesystem over the whole partition.** XFS took `mkfs.xfs` defaults
  (`crc`, `finobt`, `sparse`, `rmapbt`, `reflink`): thirteen allocation groups of 1 TiB on the
  14 TB disks and six on the 6 TB. ext4 was made with `lazy_itable_init=0,lazy_journal_init=0`.
  Both used `relatime`.
- **Where a population lies differs by filesystem**, and every table that reads one says so:
  - XFS gives each new directory the next allocation group, so the read population spanned
    12 TiB of titan's disk and 4 TiB of europa's, from near its start to past 90%.
  - An empty ext4 keeps a directory that is not near the root beside its parent: the same
    population spanned 0.12 GiB on titan and 2 GiB on europa.

  ext4's random figures are therefore short seeks on an empty filesystem, which a full disk would
  not keep. The sequential files lay at 31% of the 14 TB disks and 73% of europa's under XFS,
  and at their start under ext4, so XFS's sequential rates are a middle zone's.
- **Write cache on**, as the drives ship, except in `wcoff` and the supplement's `-wt` sides.
  Turning it off is `hdparm -W0`, then a rescan so the kernel reads `write through` and issues
  no flush. The lab script put it back on every way out, and every disk was left with it on.
- **Governor `performance`** for every run, put back afterwards: `schedutil` on titan and
  hyperion, `powersave` on europa. The e2scrub and fstrim timers were held and started again.
  shoal-tmdb was inactive on all three before and after.
- **One `znver1` build** for all three hosts. rustc 1.100.0-nightly (2026-09-04), kernel
  7.0.0-34, the glommio fork at `f4643f7`.
- **Temperatures** were read at every leg's end: 47 to 51 °C on the 14 TB disks and 48 to 50 °C on
  europa's.

**Where the run departed from the plan** on [the spikes page](spikes.md#x7-the-device-store-on-hdd):

- *One executor driving one, two and four disks* could not run, with one disk a host. `shared`
  stands in for the half that decides the design, an SSD's slice beside a disk's, and each disk
  operation's cost to its executor projects the rest.
- *The journal on the host's SSD* is a side of `journal` and `partial` (`ssd-written-ahead`,
  `J-ssd`).
- *A sync's cost, and whether several merge* is `sync`.
- *A read's tail while applies run* is `contend`, which also times stages beside the applies.
- *A foreground write's tail while a scrub reads* is `scrub`.
- *The write cache* was not in the plan. `wcoff` ran it from the first round, and the supplement
  was added after round 1, when titan's and hyperion's figures for `contend` and `scrub` came out
  six to twenty times europa's with the disk less than half busy.
- The million-chunk listing ran on hyperion rather than titan, the same model of disk, since
  hyperion's set finished first.
- Europa's supplement was run twice. The first run overlapped a build of the report on europa,
  the development host, and its records were set aside and not used. The second ran with nothing
  else on the host. Europa's first ext4 leg overlapped a `cargo check` of the spike, seconds long,
  and was kept: nothing in that round stands apart from the other three.

**Four corrections to `device report` were made after the first rounds were read, before any
verdict was written.** Each made it follow the rule as the plan stated it:

- The plan's H2 names the SSD's stages as well as its reads. The report at first judged reads
  alone.
- H2's control "does not rise" by the lab's rule, which counts a rise only where the whole
  interval is past the line. The report at first asked every round of the control to stay under
  it.
- A `contend` side counts only batches that end inside its window, so it is now called behind
  its offer only when it falls short by more than the one batch the window's edges can cost.
- T4's words name the cheapest walk for each chunk's name, length and label. X6's report judged
  a deep placement group alone, and now judges a wide one too. Of X6's own records, that changes
  the verdict printed for its ext4 and btrfs legs, not for the 970 EVO's XFS on which X6 judged it.

The report also names each probe by the cache it ran under.

## What the probes found

The probes ran before every run, on every leg. Medians over the rounds, the cache on, then off:

| Probe | WD140EDFZ (titan, hyperion), XFS | WD140EDFZ, ext4 | WD6001FZWX (europa), XFS | WD6001FZWX, ext4 |
| --- | --- | --- | --- | --- |
| The `fdatasync` after a 4 KiB overwrite, cache on | 8.05 ms, one flush | 8.09 ms, one flush | **0.39 ms**, one flush | **0.40 ms**, one flush |
| The same, cache off | 28 µs, no flush: the write itself waited for the platter | | 54 µs, no flush | |
| An `fdatasync` of a clean file, cache on | 0.49 ms, one flush | 0.49 ms | 51 µs | 51 µs |
| A rename, then the directory's `fdatasync`, cache on | 8.23 ms, two flushes | 16.6 ms | 0.51 ms | 1.64 ms |
| The same, cache off | 8.24 ms | | 7.71 ms | |
| An empty `spawn_blocking` | 26 µs | 26 µs | 13 µs | 13 µs |

Every other probe answered as it did on the SSDs. Direct I/O reached the disk unsynced. The flush
was counted on the whole disk, as X6 found. glommio's directory `fdatasync` wrote what an
`fsync` did, so it makes a rename durable. And XFS cloned a block where ext4 refused, which X7
does not use.

## Three things about the lab's disks

**The WD140EDFZ spins at 7200 rpm, though it says 5400.** Its block device characteristics page,
and `hdparm`, report a rotation rate of 5400 rpm. One block overwritten again and again cannot
be made durable faster than a revolution, since the block has to come round under the head each
time. It took 8.05 to 8.39 ms on both units and both filesystems: the sync's with the cache on, and
the write's own with it off. A revolution at 7200 rpm is 8.33 ms; at 5400 it is 11.1. The `sync` cell, a 4 KiB write and its sync in a loop,
settled at 8.35 ms with one writer, which is the revolution and no more. Every figure here is the
disk's measured behaviour, and the table names it by its measured speed, 7200 rpm.

**The WD Black acknowledges a flush before the block can be on its sector.** With the cache on,
the `fdatasync` after a 4 KiB overwrite returned in 0.39 ms, and a 4 KiB journal record and its
sync in 0.83 ms: a twentieth and a tenth of the 8.33 ms revolution. The block cannot have reached
its own place on the platter in that time. Either the drive keeps the data somewhere it treats as
durable, or it does not honour the flush. X7 cannot tell which without cutting the power, which
it did not do. With the cache off the same write and sync took 8.35 ms, the revolution.

**The WD140EDFZ stalls a read behind a flush for a tenth of a second.** On its own, a flush with
little in the cache costs the revolution. But when 64 KiB reads arrive at 20 a second beside
4 KiB journal stages at 20 a second, each with its sync, the stages got through only 9 flushes a
second. A read waited about 100 ms, and the disk was 45% busy. fio, with no harness involved, did
the same:

| fio, one job each, 30 s, 20 a second offered | WD140EDFZ (hyperion): cache on | cache off | WD6001FZWX (europa): cache on | cache off |
| --- | --- | --- | --- | --- |
| 64 KiB random reads alone: read p50 | 6.1 ms | 5.9 ms | 6.1 ms | 6.4 ms |
| 8 KiB synced writes alone: a second | 20.3 | 20.3 | 20.2 | 20.2 |
| Both together: reads a second, read p50 | **10.1, 100.1 ms** | 19.0, 0.4 ms | 19.0, 6.8 ms | 19.0, 6.5 ms |
| Both together: synced writes a second | **10.0** | 20.4 | 20.4 | 20.4 |

With the cache on, the two streams together got half of what was offered, and a read took a
tenth of a second. With it off, both got their rate. A cache flush on SATA is a command the disk
does not queue, so nothing else runs while it does. Why this drive's flush takes 100 ms with reads
among the writes, where alone it takes one revolution, was not traced. The WD Black, whose
flush does not wait for the platter, showed none of it. The rounds of `contend` and `scrub` on
the 14 TB disks are therefore figures for this drive with its cache on, and the supplement takes
them again with it off.

## 1. Sequential and random, and how far to read ahead

Medians over four rounds, from `seq`. XFS spreads the random population over the platter;
ext4's lay in 0.1 to 2 GiB, so its random figures are short seeks.

| | WD140EDFZ, XFS (titan / hyperion) | WD140EDFZ, ext4 (titan / hyperion) | WD6001FZWX, XFS | WD6001FZWX, ext4 |
| --- | --- | --- | --- | --- |
| Sequential read, 1 MiB at depth 1, MiB/s | 194 / 193 (file at 31%) | 206 / 211 (at 0.2%) | 151 (at 73%) | 213 (at 0.4%) |
| Sequential write, 1 MiB at depth 1, MiB/s | 188 / 188 | 200 / 208 | 152 | 215 |
| fio, sequential read, MiB/s | 171 / 166 | 191 / 202 | 205 | 209 |
| Random 4 KiB reads a second, depth 1 / 32 | 66 / 233 | 175 / 717 | 81 / 293 | 162 / 528 |
| A random 64 KiB read, depth 1, p50 | 13.4 / 13.9 ms | 5.4 / 5.5 ms | 12.7 ms | 6.6 ms |
| The same inside one 1 GiB file | 5.4 / 5.3 ms | 5.2 / 5.1 ms | 6.1 ms | 6.2 ms |
| Random 4 MiB reads, depth 1, MiB/s | 102 / 99 | 178 / 175 | 114 | 160 |
| A random read reaches 0.5× sequential at | **4 MiB** | 1 to 4 MiB | **4 MiB** | 4 MiB |
| ... and 0.8× at | never (16 MiB: 0.70) | 4 MiB | 16 MiB | 16 MiB |

- **A random read costs a seek and a rotation:** 13 ms for 64 KiB across a platter laid out by
  XFS, 5 to 6 ms inside a gibibyte. The disk reads 4 KiB at 66 to 81 a second at depth 1 and three
  and a half times that at depth 32, where the scheduler and the disk's own queue reorder them.
- **A disk reads a chunk whole.** Read whole across the platter, a chunk reaches half the
  sequential rate at 4 MiB on both disks, and four fifths only at 16 MiB on europa's and never on
  the 14 TB disks. A read of one 64 KiB unit gets 4.5 to 4.9 MiB/s of a disk that streams 150 to
  200. So S6's way of reading a disk is the whole chunk, and Q20's geometry gives a rotational
  pool a chunk of 4 MiB or more.

## 2. Syncs and the write cache

From `sync` and `wcoff`, on XFS: each writer a file of its own, a 4 KiB overwrite and its
`fdatasync` in a loop.

| Writers | WD140EDFZ (titan / hyperion): syncs a second, p50, flushes a sync | cache off | WD6001FZWX | cache off |
| --- | --- | --- | --- | --- |
| 1 | 118 / 116, **8.35 ms**, 1.00 | 120 / 120, **8.35 ms** | 1,020, **0.83 ms**, 1.00 | 119, **8.35 ms** |
| 6 | 278 / 235, 21 / 25 ms, **0.335** | 187 / 270 | 1,411, 3.3 ms, 0.39 | 283 |

- **One writer's sync is one revolution** on the 14 TB disks with the cache on or off, and on
  europa's with it off: 8.35 ms, 120 a second. Europa's, with the cache on, returns ten times
  sooner, which is the second of the facts above.
- **The block layer merges syncs that arrive together.** Six writers' syncs cost a third of a flush
  each, since a flush that starts covers every write completed before it. So six writers get two
  to three times one writer's syncs, not six.

## 3. The journal

S6's journal as X6 measured it, a 4 KiB header block and the payload, beside the same journal on
the host's SSD. The 4 KiB record's median, and records a second:

| | WD140EDFZ, XFS (titan) | WD140EDFZ, ext4 (titan) | WD6001FZWX, XFS | WD6001FZWX, ext4 |
| --- | --- | --- | --- | --- |
| Written ahead, 1 stager | **8.39 ms**, 113/s | 8.36 ms | 0.83 ms, 1,011/s | 0.81 ms |
| Written ahead, cache off, 1 stager | 8.36 ms | | 8.40 ms, 119/s | |
| Appended, 1 stager | 41.7 ms, 24/s | 41.8 ms | 1.08 ms | 1.87 ms |
| On the SSD beside it, 1 stager | **0.92 ms** (970 EVO) | 0.92 ms | **37 µs** (Optane) | 37 µs |
| Written ahead, 64 stagers | 2,216/s (hyperion 4,234) | 6,060/s | 5,870/s | 5,701/s |
| Appended, 64 stagers | 267/s | 310/s | 1,175/s | 914/s |

- **A stage on an idle disk is one revolution**, 8.4 ms, below H1's 20 ms on every disk, and the
  WD Black's 0.83 ms is the cache it does not wait for.
- **Writing ahead earns its zeros five times over on a disk.** An appended record changes the
  file's size, so its sync commits the filesystem's log as well: 41.7 ms on the 14 TB disks, five
  revolutions, against one. That is X6's finding with the seeks added.
- **The SSD's journal is ten to two hundred times sooner** than the disk's, idle.

## 4. A partial write

The journal's way, J, beside J-ssd: the record staged in a journal on the host's SSD, then applied
in place on the disk. Stage and apply, at the median; the acknowledgement waits for the stage
alone (S7):

| | WD140EDFZ, XFS (titan / hyperion): J | J-ssd | WD6001FZWX, XFS: J | J-ssd |
| --- | --- | --- | --- | --- |
| 4 KiB, one in flight: stage | 11.4 / 12.8 ms | **0.93 / 0.96 ms** | 0.95 ms | **49 µs** |
| 4 KiB, one in flight: stage and apply | 29.8 / 31.6 ms | 9.0 / 9.6 ms | 3.0 ms | 1.9 ms |
| 4 KiB, cache off: stage and apply | 24.7 / 25.0 ms | 9.1 / 8.7 ms | 26.2 ms | 9.5 ms |
| 4 KiB, six in flight: stage and apply | 51.7 / 54.5 ms | 43.1 / 36.9 ms | 14.2 ms | 9.2 ms |
| 64 KiB, one: stage and apply | 31.3 / 32.4 ms | 10.1 / 10.7 ms | 1.7 ms | 1.2 ms |
| 1 MiB, one: stage and apply | 46.7 / 46.8 ms | 19.7 / 21.9 ms | 53.7 ms | 18.5 ms |

- **A stage on the SSD takes the disk out of the acknowledgement.** The small write's stage falls
  from 11 to 13 ms to under a millisecond on the 970 EVO. Its apply is then one revolution on the
  disk, 8 to 9 ms, with nothing ahead of it on the arm.
- **On the disk, the stage and the apply take turns on one arm.** J costs three to four
  revolutions at 4 KiB: the journal's write, its sync, the seek to the chunk, and the apply's
  sync.
- On ext4 the 14 TB disks were slower still, 45.9 and 56.9 ms for J and 28.4 and 31.6 for J-ssd.

## 5. A whole chunk

X6's T1: a file a chunk against a slot of a shared file written ahead (layout B), six in flight
with one sync for six. The recycled file (R), from a pool written ahead, is what X6 recommended.
R ÷ B, round by round:

| Chunk | WD140EDFZ, XFS (titan / hyperion) | WD140EDFZ, ext4 (titan / hyperion) | WD6001FZWX, XFS | WD6001FZWX, ext4 |
| --- | --- | --- | --- | --- |
| 64 KiB | 0.29 / 0.29 | 0.13 / 0.13 | 0.45 | 0.20 |
| 256 KiB | 0.37 / 0.35 | 0.20 / 0.17 | 0.58 | 0.28 |
| **1 MiB** | **0.62** [0.43–0.67] / **0.93** | 0.40 / 0.40 | **1.10** | 0.60 |
| 4 MiB | 0.82 / 0.70 | 0.67 / 0.68 | 0.99 | 0.71 |
| 16 MiB | 1.12 / 0.88 | 0.97 / 0.94 | 1.07 | 0.91 |
| Floor, where R ÷ B first stays above 0.5 | **1 MiB** | **4 MiB** | **256 KiB** | 1 MiB |

- **A file a chunk keeps X6's floor of 1 MiB on XFS**, so H5 (a) does not fire there. A fresh
  file a chunk (X6's T1 itself, A+) has the same floor.
- **A small chunk costs revolutions:** one 64 KiB chunk at a time took 41.7 ms as a fresh file on
  the 14 TB disks' XFS (the file's sync, its allocation's commit, and the directory's), five
  revolutions, against 8.65 ms, one, for a slot of the shared file.
- **ext4 moves the floor to 4 MiB on the 14 TB disks**, and holds a small chunk to half of
  XFS's rate or less: its rename and directory sync cost two revolutions where XFS's cost one
  (the probes).

## 6. Finding a chunk

X6's measurement 6 on a disk: one unit at a random place, cold (every cache dropped, so the open
reads the directory and inode from the disk) against the file held open. H5 (b) is cold ÷ open at
64 KiB:

| | WD140EDFZ, XFS (titan) | WD140EDFZ, ext4 (titan) | WD6001FZWX, XFS | WD6001FZWX, ext4 |
| --- | --- | --- | --- | --- |
| Cold: open, then header and unit, p50 | 22.1 ms (open 1.7 ms) | 9.1 ms (open 1.6 ms) | 17.3 ms (open 0.9 ms) | 14.1 ms (open 0.9 ms) |
| The file held open, p50 | 14.7 ms | 6.5 ms | 11.9 ms | 9.4 ms |
| Cold ÷ open, by round | 1.52 [1.44–1.57] | 1.42 [1.29–1.52] | 1.46 [1.35–1.53] | 1.50 [1.39–1.86] |
| The population's span | 12 TiB | 0.12 GiB | 4 TiB | 2 GiB |

- **Finding a chunk costs a disk half its read again, at the line.** A cold open itself reads in
  1 to 2 ms, since XFS keeps an inode near its directory. The rest is the header and the unit read
  apart once the head has moved, and every interval straddles 1.5×. H5 (b) does not fire.
- A slice that keeps its hot chunks open saves that half. The reads on a disk are seeks either
  way.

## 7. Listing a placement group

X6's T4 on a disk. Each walk is cold, with every cache dropped, and stops after half an hour. The
populations of a hundred thousand were walked every round; those of a million once on hyperion,
the same model as titan's:

| Walk | WD140EDFZ, XFS | WD140EDFZ, ext4 | WD6001FZWX, XFS | WD6001FZWX, ext4 |
| --- | --- | --- | --- | --- |
| Deep 100K, label from the header, 32 in flight: µs a chunk | 172 (titan) | 45 (titan) | 67 | 29 |
| Wide 100K, names only: µs an object | 50 (titan) | **4,437** (titan) | 28 | **5,633** |
| Deep 1M, names / xattr label / header label, s | 42 / 58 / **217** | 27 / 39 / **65** | | |
| Wide 1M, names, s | **87** | **stopped at 1,800 s after 229,959 objects** | | |

- **T4 fires on a disk as X6 wrote it**: the header walk of a deep million takes 217 s on XFS,
  past X6's 60 s, and the names of a wide million 87 s. The walk that reads the label from an
  extended attribute passes at 58 s, but listing a wide group fails before any label is read. On a
  disk a light scrub is minutes, not seconds: about 12 minutes for a 14 TB disk of 4 MiB chunks,
  reading every header.
- **ext4 lists a wide placement group at 4.4 to 7.8 ms an object**, a seek for each object
  directory, where XFS takes 28 to 87 µs. A million objects of one chunk did not finish in half
  an hour. On SSDs X6 found ext4 five times slower than XFS here; on a disk it is ninety to two
  hundred times.

## 8. Removing chunks

A thousand chunks of 4 MiB, cold, one by one through glommio, then the directory synced: 754 µs a
chunk on the 14 TB disks' XFS, 317 µs on europa's, 394 to 492 µs on ext4. Removing stays cheap
next to writing, as on the SSDs.

## 9. One disk, one slice or two

| MiB/s | WD140EDFZ, XFS (titan): 1 slice / 2 | WD6001FZWX, XFS: 1 / 2 |
| --- | --- | --- |
| 64 KiB random reads, 32 deep a slice | 41.5 / 25.6 | 36.8 / 22.4 |
| 4 MiB chunks written, six at a time | 108 / 111 | 120 / 111 |
| fio, one job: 64 KiB random reads, 32 deep | 36.2 | 33.8 |

A second slice on one disk read 40% less, since two slices' readers spread their seeks over two
populations, and wrote no more. One slice is every disk can use, and it used under 4% of its
core. T2 does not fire.

## 10. Reads and stages beside applies

`contend`, on XFS, the cache on as the rounds ran. One executor applies 64 KiB units in batches
of 32 at half of what the disk took with a whole batch in flight (`capacity`). Beside it, a
64 KiB read and 4 KiB stages on the disk and on the SSD each arrive at 20 a second. Medians, ms:

| | WD140EDFZ (titan / hyperion) | WD6001FZWX |
| --- | --- | --- |
| Capacity, applies a second, whole batches | 93 / 96 | 128 |
| Idle: read p50, p99 | 81, 181 / 73, 140 | 13.6, 46 |
| Idle: the disk's stage p50 | **283 / 210** | 14.2 |
| Offset order, one at a time: applies done of those offered | **8 of 46 / 8 of 48** | 64 of 64 |
| ... a batch's p50; a read's p99 | 3,403, 122 / 3,390, 116 | 303, 235 |
| ... the disk's stage p50; the SSD's stage p50 | **247**, 1.0 / **251**, 1.0 | **42**, 0.57 |
| Arrival order, one at a time: applies done; read p99 | 8, 127 / 8.8, 117 | 64, 229 |
| The batch in flight: applies done; a batch's p50 | 39, 781 / 42, 696 | 64, 386 |
| ... a read's p99; the disk's stage p50; the SSD's stage p99 | 340, **420**, 211 / 353, **391**, 236 | 205, **150**, 1.7 |

- **A stage on the disk is not acknowledged in 20 ms once applies run beside it.** Beside
  applies in offset order it took 42 ms at the median on the WD Black and 250 ms on the 14 TB
  disks. Even idle beside the reads, the 14 TB disks' stage took 210 to 283 ms, which is the
  flush stalling behind a read, above. On the SSD the stage stayed at 0.6 to 1.0 ms. **H1 fires**
  on every XFS leg, and on ext4: 41 to 306 ms.
- **The order of a batch one at a time made no difference.** By FIEMAP's physical offset, by inode
  and offset, or as the applies arrived, the batches took the same time, and the reads beside them
  waited the same, to within each other's intervals on every disk. The kernel's scheduler and the
  disk's queue order a whole batch in flight at least as well. On the 14 TB disks with the cache
  on, one apply at a time kept only 8 of the 46 to 48 offered, since each waited behind a flush.
- **A read waited less than a batch.** Where the applies kept their offer (europa, both
  filesystems, and the 14 TB disks with the batch in flight), a read's p99 was 0.16 to 0.71 of one
  batch's median plus an idle read's p99. **H3 does not fire** where it can be judged; on the 14 TB
  disks' one-at-a-time sides it is not judged, since they never ran the load.
- **A batch's size is a read's wait.** A batch of 32 in flight took 0.2 to 0.8 s and one of 128
  0.6 to 1.2 s, and a read's p99 rose with it: 176 to 353 ms at 32, 441 to 694 ms at 128.
- **The SSD's stage on the same executor was hurt by the disk's applies** on the 14 TB disks'
  sides with the batch in flight: a p99 of 211 to 236 ms against 4 ms alone. That is H2's finding
  inside a measurement that was not looking for it.

## 11. A foreground beside a scrub

`scrub`, the cache on as the rounds ran: a small write made as S6 makes it (a 4 KiB stage on the
disk's journal, then the unit and header in place and the chunk synced) at 10 a second, and 64 KiB
reads at 20, beside a deep scrub reading whole chunks at a budget. The write's p99 ÷ its p99 with
no scrub, round by round:

| Budget | WD140EDFZ, XFS (titan / hyperion) | WD140EDFZ, ext4 (titan / hyperion) | WD6001FZWX, XFS | WD6001FZWX, ext4 |
| --- | --- | --- | --- | --- |
| None: the write's p50, p99, ms | 474, 691 / 432, 635 | 355, 572 / 363, 552 | 14.6, 25 | 10.1, 35 |
| 10 MiB/s | 1.37 [1.00–1.60] / 1.16 [0.87–1.51] | 1.05 [1.00–1.11] / 1.00 [0.94–1.34] | **3.10** [0.73–3.14] | **2.02** [1.57–3.06] |
| 20 MiB/s | 1.16 / 1.19 | 1.31 / 1.20 | 3.09 | 2.22 |
| 60 MiB/s | 1.17 / 1.20 | 1.28 / 1.27 | 14.6 | 4.19 |
| No bound: the scrub's MiB/s | 16 / 17 | 21 / 24 | 68 | 88 |

- **In these rounds H4 is at the line on every XFS leg and fires on europa's ext4.** On europa's
  XFS a 10 MiB/s scrub tripled the small write's p99 in three rounds of four, and one round's
  baseline outlier put that interval across the line; the supplement's four rounds on the same
  disk put it wholly past it (section 13).
- **On the 14 TB disks the scrub barely moved a foreground that was already a tenth of a second
  slow.** With the cache on the scrub never got past 17 to 24 MiB/s whatever its budget.
- No budget of 10 MiB/s or more stayed wholly within 1.25× on any XFS leg. The supplement takes
  the 14 TB disks again with the cache off, where the scrub gets its budget.

## 12. One executor, an SSD's slice and a disk's

`shared`: the SSD's slice takes 64 KiB reads at 1,000 a second and 4 KiB stages at 200. The disk's
slice runs applies a batch of 32 at a time, reads four deep and whole 1 MiB chunks renamed. p99,
µs:

| | WD140EDFZ + 970 EVO, XFS (titan) | ext4 (titan) | WD6001FZWX + Optane, XFS (europa) | ext4 (europa) |
| --- | --- | --- | --- | --- |
| SSD read p99: alone / shared / separate | 618 / 637 / 605 | 626 / **59,752** / 619 | 72 / 82 / 73 | 72 / **9,688** / 73 |
| SSD stage p99: alone / shared / separate | 3,551 / **367,215** / 3,726 | 3,969 / **356,640** / 3,918 | 89 / 103 / 91 | 89 / **9,174** / 93 |
| Shared ÷ alone, the worse of the two | **106** [95–186] | **102** [2.8–212] | 1.15 [0.99–1.22] | **134** [56–407] |
| The disk's executor, own core: busy, µs a disk operation | 0.6%, 86 | 0.8%, 91 | 0.6%, 59 | 0.8%, 55 |

- **H2 fires on three legs of four.** The SSD slice's stage, or its read, rose about a hundredfold
  on the executor it shared with the disk's slice, and not at all with the disk's slice on a
  second executor. Europa's XFS rose 1.15×, under the line.
- **Why was not traced.** The disk's slice does nothing CPU-bound: its own executor was under 1%
  busy. What one executor shares is its io_uring, the workers the kernel runs that ring's syncs
  on, and the one blocking thread its renames go to. A disk's sync or rename takes milliseconds to
  tenths of a second where an SSD's takes a millisecond. On titan's XFS only the SSD's stages
  suffered, whose sync waits on such a worker. On ext4 the SSD's direct reads suffered too, as if
  the executor itself had blocked submitting the disk's work. Europa has four times titan's cpus,
  and io_uring sizes a ring's pool of such workers by the cpus, four to a cpu as recalled from the
  kernel's source and not read here. X7 records the finding and leaves its mechanism, which M19
  meets, to a trace.
- **A disk barely uses its executor.** At every operation the shared cells offered it, the disk's
  slice took 55 to 91 µs of its thread an operation, under 1% of the core. One core has the cpu
  for a hundred such disks. Whether disks' slices may share an executor among themselves, with
  no SSD among them, was not measured with one disk a host.

## 13. The write cache supplement

`contend` and `scrub` again, on one XFS leg a host after the rounds: four rounds, the cache on
(`-wb`) then off (`-wt`) in odd rounds and the other way in even ones. Medians, ms unless they say
otherwise:

| | WD140EDFZ (titan): on / off | WD140EDFZ (hyperion): on / off | WD6001FZWX (europa): on / off |
| --- | --- | --- | --- |
| Idle: the disk's stage p50 | 190 / **13.9** | 233 / **13.4** | 14.8 / 12.0 |
| Idle: a read's p50 | 76 / 22 | 71 / 23 | 14.1 / 16.1 |
| A small write beside reads, no scrub: p50, p99 | 446, 644 / **50, 126** | 447, 576 / **49, 80** | 14.4, 28 / 41, 62 |
| Offset order, one at a time, batch 32: applies a second | 7.2 / **41.6** | 8.0 / **43.2** | 60.8 / 38.4 |
| ... the disk's stage p50 | 252 / 54 | 241 / 67 | 79 / 28 |
| The batch in flight, 32: applies a second; the disk's stage p50 | 40, 390 / 42, 36 | 42, 367 / 43, 39 | 61, 77 / 54, 28 |
| ... the SSD's stage p99, the same executor | 234 / **2.9** | 217 / **3.9** | 0.94 / 15.8 |
| Capacity, a batch of 32 / of 128 in flight, applies a second | 96, 128 / 83, 77 | 96, 141 / 90, 102 | 122, 154 / 112, 102 |
| An unbounded scrub beside them, MiB/s | 18 / **72** | 17 / **72** | 73 / 88 |
| A 10 MiB/s scrub's write p99 ÷ none | 1.13 [0.95–1.19] / 0.97 [0.39–1.32] | 1.25 [1.12–1.39] / 1.44 [1.02–1.52] | **2.65** [1.73–3.57] / **1.70** [1.42–1.84] |

- **On the 14 TB disks the cache is the whole difference**, as fio said:
  - the stage falls from about 200 ms to 13 ms idle;
  - a small write beside reads falls from 446 ms to 50 ms at the median;
  - one apply at a time keeps its offer;
  - a scrub gets its budget, 72 MiB/s unbounded where it got 17.
  - The SSD's stage on the same executor follows how long the disk's operations take. Its p99
    falls from 234 ms to 3 ms when the 14 TB disks' flushes are gone. On the WD Black it rises
    from 0.94 to 15.8 ms as each write waits for its sector. That is H2's finding again, and it
    follows the disk's operations' time, not their kind.
- **On the WD Black, turning the cache off costs what its cache was hiding.** A small write beside
  reads takes 41 ms, not 14, and one apply at a time keeps 38 of 61 a second. Its cache-on figures
  are those of a drive whose sync returns before its platter could hold the block.
- **The cache off costs a whole batch's throughput on every disk**: 7 to 14% fewer applies a
  second at 32, and 27 to 40% fewer at 128. Each write now waits for its sector, and the disk can
  no longer gather a batch in its cache. That is the price of an acknowledgement that means the
  bytes are on the platter.
- **With the cache off the stage beside applies is still 28 to 67 ms**, above H1's 20 ms on every
  disk. The journal goes to the SSD with the cache on or off.
- **H4 fires on europa's disk**, with the cache on and with it off: a 10 MiB/s scrub raised the
  small write's p99 2.65 and 1.70 times, wholly. In the rounds the same leg was at the line only
  because one round's baseline had an outlier. On the 14 TB disks a 10 MiB/s scrub stays below
  or at the line, with the cache on or off.


## What would have changed the design

| Result named in advance | Found | So |
| --- | --- | --- |
| **H1.** A stage cannot be acknowledged from the disk: above 20 ms at the median, idle or beside applies in offset order | **Yes.** Idle, a stage is one revolution, 8.4 ms, on every disk with an honest flush. Beside applies in offset order it took 42 ms on the WD Black and 247 to 251 ms on the 14 TB disks; with the cache off, 28 ms on the WD Black and 54 to 67 ms on the 14 TB disks. On the SSD beside it, 0.6 to 1.0 ms | **A rotational device journals on an SSD of its node.** A journal shared by several devices makes them one failure domain ([S5](placement.md#failure-domains)) |
| **H2.** A disk needs an executor of its own: an SSD slice's p99 above 1.25× on a shared executor, wholly, and not on the control | **Yes, on three legs of four.** About a hundredfold on titan's XFS and ext4 and on europa's ext4; 1.15× on europa's XFS. The control stayed level | **A rotational slice never shares an executor with an SSD's.** A disk needs under 1% of a core, so the cost is a core a node gives disks, not a core a disk |
| **H3.** Offset order does not bound a read's wait: a read's p99 past one batch plus an idle read's | **No**, where judged: 0.16 to 0.71 of the bound. One apply at a time kept 8 of 48 offered on the 14 TB disks with their cache on, so it was not judged there | Applies stay batched and below the foreground (S13). **Offset order is dropped**: by FIEMAP, by inode or as they arrived, one at a time, the batches took the same, and a whole batch in flight did as well or better. The batch's size is what bounds a read |
| **H4.** A byte budget cannot protect the foreground: at 10 MiB/s the write's p99 above 1.25×, wholly | **Yes, on europa's disk**: 2.65× with the cache on and 1.70× with it off in the supplement's XFS rounds, 2.02× on its ext4. At the line on the main rounds' XFS legs (1.16 to 3.10 at the median), and at or under it on the 14 TB disks with the cache off (0.97, 1.44). No budget of 10 MiB/s or more stayed wholly within 1.25× on any XFS leg | **A disk's scrub adapts to the foreground**: paced by the arm's idle time, with S11's byte budget as its ceiling. X12 finds the pacing and the ceiling a disk ships with |
| **H5.** Small chunks cost a seek each: a file a chunk's floor above 1 MiB, or a cold find above 1.5× | **No on XFS:** the floor stays at 1 MiB (256 KiB on the WD Black), and a cold find costs 1.42 to 1.52 times an open one, at the line. **Yes on ext4** on the 14 TB disks, whose floor is 4 MiB | A file a chunk stays. A rotational pool's chunk is set by how a disk reads, 4 MiB or more, not by the cost of making one |
| X6's T1 | Floor 1 MiB on XFS on every disk; 4 MiB on the 14 TB disks' ext4 | As H5 |
| X6's T4: a light scrub's walk over 60 s for a million | **Fires on a disk:** the header walk of a deep million took 217 s on XFS, the names of a wide million 87 s; on ext4 the wide listing did not finish in 30 minutes | The label stays in the header for now; whether a rotational pool's light scrub keeps an index is M17's, with these figures |

## The comparison

**Where a rotational device's journal goes**, at 4 KiB, one in flight, on the 14 TB disks' XFS:

| Option | Acknowledged after | Strengths | Weaknesses |
| --- | --- | --- | --- |
| **On an SSD of the node** (recommended) | 0.93 ms to stage; the apply follows in 8 to 9 ms, off the acknowledgement's path | The stage never waits for the arm; the apply has the arm to itself | A second device in every write's path; a journal SSD lost loses every chunk its devices had not yet applied, so the devices it serves are one failure domain |
| On the disk itself | 11 to 13 ms idle, 250 ms beside applies with the cache on, 54 to 67 ms with it off | One device, one failure | The stage and the apply take turns on one arm; a stage waits for whatever the arm is doing |

**The disk's write cache:**

| | Cache on | Cache off (recommended) |
| --- | --- | --- |
| A sync | One revolution on the 14 TB disks; 0.4 ms on the WD Black, which cannot be on the platter | One revolution, 8.35 ms, on every disk |
| A flush among reads (14 TB disks) | Stalls everything for about 100 ms | No flush is issued |
| A stage idle beside reads; a small write (14 TB disks) | 233 ms; 447 ms | 13 ms; 49 ms |
| A whole batch of applies in flight, 32 / 128 | 96 to 122 / 128 to 154 a second | 7 to 14% fewer / 27 to 40% fewer |
| What an acknowledgement means | Whatever the drive's firmware does with a flush | The write is on the platter when it completes |

**Filesystems, for a rotational device:**

| | XFS | ext4 |
| --- | --- | --- |
| A rename made durable | 8.2 ms, one revolution | 16.6 ms, two |
| A file a chunk's floor (14 TB disks) | 1 MiB | 4 MiB |
| Listing a million objects of one chunk | 87 s | did not finish in 30 minutes |
| A shared executor (H2) | 106× on titan, 1.15× on europa | 102× and 134× |
| A population on an empty filesystem | Spread over the platter | Kept near its parent, so short seeks until the disk fills |
| **So** | **required** | **refused for a rotational device** |

## Recommendation

**S6's device store, with five things a rotational device needs.**
[S18](contract.md#q23-what-a-rotational-device-needs-2026-10-06) records it as the answer to Q23.

1. **The disk's volatile write cache is off.**
   - The node reads the kernel's view of each device's disk (`queue/write_cache`). It starts a
     rotational device's slices only when the cache writes through, or when the device's
     configuration says the cache is on deliberately, which the node then names in a warning, as
     S4's mislabelled class is named.
   - The inventory wizard and `shoaladm` turn the cache off and keep it off across a power
     cycle.
   - The figures: a sync becomes the revolution it should be on every disk, and on the 14 TB disks
     a small write falls from 447 ms to 49 ms beside reads.
2. **Its journal is on an SSD of the same node**, named in the device's configuration; a
   rotational device without one is refused.
   - Several devices may share one journal SSD, and they then count as one failure domain, as S6
     already says.
   - A stage is then 0.9 ms on the 970 EVO and 49 µs on the Optane, and the disk's arm does only
     applies, reads and scrubs.
3. **A rotational device's slices have executors of their own**, never shared with an SSD's.
   One slice a disk: a second read 40% less.
4. **Applies are issued a batch at a time, the whole batch in flight**, below the foreground.
   - The batch is bounded by the time a disk takes to apply it, since that bounds a foreground
     read's wait. Where H3 was judged, a read waited 0.16 to 0.71 of one batch's median plus an
     idle read.
   - There is no offset order: the block layer and the disk's own queue order a batch in flight,
     and one at a time in offset order bought nothing.
   - S13's test becomes `rotational_applies_are_batched_and_bounded`.
5. **A disk reads whole chunks, and a rotational pool's chunk is 4 MiB or more.** Across the
   platter a random read reaches half the sequential rate at 4 MiB and not before. That is a floor
   for Q20's geometry on a rotational pool, above the 1 MiB that making a chunk as a file needs.

And beside those:

- **XFS for a rotational device; ext4 is refused there.** X6 accepted ext4 for an SSD, and that
  stands.
- **A deep scrub on a disk is paced by the arm's idle time**, with a byte budget as its ceiling,
  since even 10 MiB/s raised a small write's p99 1.7 to 2.7 times on europa's disk. How it paces,
  and the ceiling a disk ships with, are X12's to find from these figures.
- **A light scrub of a disk takes minutes for a million chunks.** Whether it needs an index is
  M17's.

## What X7 does not settle

- **Why a shared executor hurts.** The finding is firm; its mechanism (the ring's sync workers,
  the blocking thread, or a submission that blocks) was not traced. Whether disks' slices may
  share an executor among themselves needs two disks a host.
- **Whether the WD Black's flush is durable.** Only cutting its power during a run would say. With
  the cache off the question does not arise.
- **Why the WD140EDFZ's flush stalls a read for 100 ms.** Measured, by fio as well as the harness;
  not explained.
- **The apply batch's bound**, in time or in applies: M19's, against a foreground read's budget.
- **How a disk's scrub paces itself by the arm's idle time, and its ceiling**: X12, with these
  figures.
- **A light scrub's index for a rotational pool**: M17.
- **The geometry**: Q20 and Q25. X7 adds a floor of 4 MiB under a rotational pool's chunk.
- **A full disk.** Every population lay on an empty filesystem. ext4's short seeks would grow as it
  filled, and XFS's spread is already the whole platter.

## What it did not measure

- **Crashes and power cuts.** Durability is argued from which syncs were issued and what the
  cache does, not shown.
- **Shingled disks, SAS disks, a RAID controller, or a drive with power-loss protection.** All
  three disks are CMR SATA disks on the motherboard's controller.
- **More than one disk a host.** One executor driving several disks is projected from what one
  disk's operations cost a core.
- **Schedulers other than mq-deadline, and queue tuning.** No `none`, no BFQ, no change to
  `nr_requests` or the NCQ depth.
- **Tuning of either filesystem**: XFS's log size or placement, ext4's commit interval.
- **Real checksums**, as in X6.
- **The 14 TB disks at 5400 rpm.** They do not run at it.

## What it found

None of these is a defect of Shoal, so none is filed on [Known Issues](../appendix/known-issues.md).
Each is written down because M19 meets it.

| Finding | Where |
| --- | --- |
| The WD140EDFZ's characteristics page and `hdparm` say 5400 rpm; it turns at 7200 | `device/vpd_pgb1`; the probes and `sync` |
| The WD140EDFZ, with its cache on, stalls a read behind a flush for about 100 ms | fio, `results/x7-<host>-flush-*.json`; `contend` and `scrub` |
| The WD6001FZWX acknowledges a sync in 0.4 ms with its cache on | The probes, `journal` |
| `hdparm -W0` alone leaves the kernel issuing flushes until the disk is rescanned and `queue/write_cache` reads `write through` | `x7-lab.sh`'s `write_cache` |
| An empty ext4 keeps a deep directory's files beside its parent, and XFS spreads them over every allocation group, so figures on an empty filesystem differ in their seeks | Every population's span |
| One executor shared by an SSD's slice and a disk's raised the SSD's tail about a hundredfold on three legs of four | `shared`, `contend` |
| The kernel's `device/model` cuts a SATA model at sixteen characters, and a SATA disk has no `firmware_rev` | `facts.rs` |

## Related

- [X7](spikes.md#x7-the-device-store-on-hdd) for what was planned.
- [X6's record](device-store-ssd.md) for the harness and the SSDs, and the form this page copies.
- [S6](device-store.md#what-a-rotational-device-changes) for the design it measured, now updated.
- [S18](contract.md#q23-what-a-rotational-device-needs-2026-10-06) for the decision.
- [S13](isolation.md#io-on-a-slice) for the order of a slice's work and the applies' test.
- [S11](scrub.md#schedule-and-budget) and [X12](spikes.md#x12-recovery-and-scrub-rates) for the
  scrub's budget.
- [M19](milestones.md#m19-rotational-devices) for what it gates.
- [S1](prerequisites.md#what-the-lab-needs-fitted) for the disks fitted for it.
