# X15. A short chunk's file, measured

~~**Status: planned, not yet run.** The question, the method and what was expected are written
down before the run, as the lab's rule asks. Results replace this note.~~

**Reported 2026-10-20.** This is the record of spike [X15](spikes.md#x15-a-short-chunks-file). It
placed a put's stripe chunk three ways on every host's SSD and disk, four rounds each:
- as the store is built;
- from a pool of files of the chunk's class written ahead (B);
- in a file made for the chunk and synced by itself (A).

It ends in a recommendation, which [F96](../features/chunk-file-classes.md) builds: **a chunk's file
is made for it, allocated to its class and synced by itself, and nothing is written ahead.**

Four facts decide it:
- **A fresh file placed more chunks a second than a pool of the class in every cell on every
  host.** Each figure is the median of a cell's rounds:
  - 1.12 to 1.86 times on europa's Optane;
  - 1.42 to 1.80 times on the 970 EVOs;
  - 1.25 to 2.40 times on the disks.

  Every SSD cell's intervals are disjoint. On the disks, five cells of twenty-four overlap, all of
  them at one in flight.
- **It writes about one device byte a byte put.**

  | Chunk | Fresh file | Pool of the class | Store as built |
  | --- | --- | --- | --- |
  | 64 KiB | 1.13 to 1.29 | 2.25 to 2.47 | 65 |
  | 4 MiB | 1.00 | 2.00 | 2.0 |
- **Its own sync cost what the shared flush did.** At 64 KiB with one chunk in flight on titan's
  970 EVO, the fresh file's sync took 2.9 ms and the shared flush 2.8. X6 measured the same pair at
  7.2 ms and 3.2. The pool's chunk paid at its directory syncs instead: 7.0 ms against 2.0, with its
  refill's writes on the device beside them, and 4.6 flushes a chunk against 2.0.
- **At eight in flight a pool runs dry.** Two thirds to four fifths of the chunks found no file
  ready and made their own, writing it with zeros before their bytes. That is the cost the pool
  existed to avoid.

Against the store as built, a fresh file of the chunk's class placed 1.4 to 15.6 times as many
chunks a second. The gain is largest at 64 KiB on the Optane, and smallest at 4 MiB, where the
class is the whole chunk and only O102's doubling is saved.

## The question

Where should a stripe chunk's file come from, once it is sized to what the chunk holds?

[O107](../appendix/optimizations.md#o107-a-stripe-shorter-than-its-chunk-costs-its-device-the-whole-chunk)
is the largest gap [the store comparison](store-comparison.md) found. Every whole chunk, however
short, is written into a file of its pool's whole chunk length, 4 MiB and a header block. That file
is taken from a pool the slice wrote ahead with zeros, and the pool is then refilled with another
file of zeros. A 64 KiB stripe therefore costs each holder about 4 MiB of device writes: 196 device
bytes per byte put with three copies, against Ceph's 3.5. F96, the change this decides
([the order of work](store-optimizations.md#the-order-of-work)), sizes a chunk's file to a
power-of-two class of its units. That leaves the question of where the
file comes from:

- **(B) A pool of the class, written ahead.** Every durability rule stays as F85 built it. The data
  is made durable by the shared flush, and space is taken ahead of the stage. The refill still
  writes the class's length again as zeros, so a byte costs about two.
- **(A) A file made for the chunk.** It is allocated to its class, written, and synced by its own
  `fdatasync`, with nothing written ahead. A byte costs about one, which also ends
  [O102](../appendix/optimizations.md#o102-a-whole-chunk-costs-its-device-twice-its-bytes)'s doubling
  for every new object.

[X6](device-store-ssd.md#1-a-whole-chunk) chose the pool for its sync. On the 970 EVO a fresh
file's `fdatasync` took 7.2 ms against 3.2 ms for the same bytes overwritten, because it commits
the file's allocation in the filesystem's journal. But X6's recycled file had no refill behind it,
and the store's refill makes a fresh file for every chunk taken, so the product pays the dear sync
anyway, in the background. [F85's retake](../features/devices-and-pools.md#on-the-lab) put the
store's path at 0.36 to 0.39 of one file synced, below X6's fresh file at 0.51. The user chose to
measure both before building either.

## How it was judged

- **Every figure is the median of four rounds**, with the lowest and highest beside it. A
  difference counts only when two intervals are disjoint; a ratio is taken round by round.
- **Every side is read against `today`**, the store as built, in the same cell, back to back.
- **What decided F96's source:**
  - (A) unless `class-fresh`'s rate is below `class-pool`'s, disjoint, at eight in flight on the
    970 EVO or on a disk;
  - or unless `class-fresh` writes more device bytes per byte than `class-pool` anywhere.

  Neither happened.

## What was expected

Written before the first round:

1. **Device bytes per byte put at 64 KiB:** `today` about 65, `class-pool` about 2.1,
   `class-fresh` under 1.5 (its data, its header and the filesystem's journal).
2. **One in flight, 970 EVO:** `class-pool`'s cycle below `class-fresh`'s at its median. It
   overwrites and shares a flush, where a fresh file pays an allocation's sync. But its rate is
   within `class-fresh`'s or below it, because each chunk's refill takes the device for a fresh
   file's sync too.
3. **Eight in flight, 970 EVO:** `class-fresh` ahead of `class-pool`, with fewer flushes per chunk.
4. **Europa's Optane**, where a sync costs no flush and bytes are the bound: `class-fresh` ahead at
   every size and depth.
5. **The disks**, write cache off, where every sync is a seek: `class-fresh` ahead at every size.
6. **4 MiB, the whole chunk:** `today` and `class-pool` alike, and `class-fresh` writing half their
   bytes.

## What was run

`shoal-spike device short` (`shoal-spike/src/device/short.rs`), a measurement of X6's harness run
only when named. Each side places a put's chunk the way a slice does:
1. its header block and units are written with direct I/O;
2. they are made durable;
3. the file is renamed over the chunk's path;
4. the chunk's directory and the file's source directory are synced, as `apply_whole` syncs both.

| Side | Its file | Made durable by | Behind it |
| --- | --- | --- | --- |
| `today` | a pool file of the whole 4 MiB chunk | the shared flush: one `fdatasync` of a 4 KiB file of the slice's for every write completed before it began | the pool refilled with a file of zeros, 4 MiB |
| `class-pool` (B) | a pool file of the chunk's class | the shared flush | the pool refilled with a file of the class |
| `class-fresh` (A) | a file made for the chunk and allocated to its class | its own `fdatasync` | nothing |

**The sides follow the store:**
- Each pool starts full, with the store's default of eight files, as a slice at rest has it.
- The refill makes one file at a time in a task queue below the foreground's, as a slice's
  background queue does. A chunk that finds its pool empty makes its file itself, as
  `begin_whole` does, and is counted as `held`.
- A side's device counters run until its pool is full again, so the zeros a chunk cost are counted
  to it whenever they are written.

**The grid:**
- chunks of 64 KiB, 256 KiB, 1 MiB and 4 MiB, each a power of two of 64 KiB units, so a class is
  its chunk's own length;
- one chunk in flight, and eight, on one pinned executor;
- each cell's count from X6's byte budget, 600 chunks at most, a side capped at 20 seconds on a
  disk.

**Where.** On each host's SSD and on its disk:
- europa's Optane;
- titan's and hyperion's 970 EVO, on the XFS volume `/xfs`;
- the WD disks at `/hdd`, with their write cache off as a deployed node keeps them.

Each host ran four rounds under the `performance` governor with its tmdb node stopped, in
`shoal-spike/results/x15-lab.sh`, the sides' order reversed in even rounds. The records are under
`shoal-spike/results/x15/`, merged in `shoal-spike/results/x15-report.md`.

## 1. Chunks a second

**A fresh file beside a pool of the class**, `class-fresh ÷ class-pool`, the median of the rounds'
ratios. A cell marked * overlaps: its two sides' intervals are not disjoint.

| Chunk, in flight | Optane (europa) | 970 EVO (titan / hyperion) | Disk (europa / titan / hyperion) |
| --- | --- | --- | --- |
| 64 KiB, 1 | 1.12 | 1.53 / 1.53 | 2.06 / 1.69 / 1.47* |
| 64 KiB, 8 | 1.41 | 1.59 / 1.63 | 2.24 / 1.62 / 1.56 |
| 256 KiB, 1 | 1.22 | 1.57 / 1.57 | 1.55* / 1.25* / 1.55 |
| 256 KiB, 8 | 1.55 | 1.69 / 1.69 | 1.73 / 1.81 / 1.83 |
| 1 MiB, 1 | 1.59 | 1.45 / 1.44 | 1.86* / 1.66 / 1.26* |
| 1 MiB, 8 | 1.86 | 1.79 / 1.79 | 2.13 / 2.03 / 2.40 |
| 4 MiB, 1 | 1.62 | 1.42 / 1.44 | 1.95 / 1.96 / 2.24 |
| 4 MiB, 8 | 1.75 | 1.80 / 1.80 | 1.95 / 1.64 / 2.12 |

The overlapping cells are a disk's spread between rounds, not a lead the other way. In two of them
one round of four went below 1, at 0.98 (hyperion, 64 KiB) and 0.85 (titan, 256 KiB). No cell's
median is below 1.25.

**A fresh file beside the store as built**, `class-fresh ÷ today`:

| Chunk, in flight | Optane (europa) | 970 EVO (titan / hyperion) | Disk (europa / titan / hyperion) |
| --- | --- | --- | --- |
| 64 KiB, 1 | 7.01 | 2.20 / 2.19 | 3.84 / 2.69 / 2.46 |
| 64 KiB, 8 | 15.55 | 4.18 / 4.23 | 6.50 / 5.77 / 4.85 |
| 256 KiB, 1 | 5.63 | 2.09 / 2.09 | 2.88 / 2.07 / 3.18 |
| 256 KiB, 8 | 10.93 | 4.37 / 4.43 | 5.29 / 5.35 / 5.29 |
| 1 MiB, 1 | 3.62 | 1.92 / 1.91 | 2.68 / 2.16 / 1.62 |
| 1 MiB, 8 | 4.19 | 3.13 / 3.18 | 3.59 / 4.02 / 4.37 |
| 4 MiB, 1 | 1.52 | 1.44 / 1.47 | 1.89 / 2.27 / 2.22 |
| 4 MiB, 8 | 1.81 | 1.78 / 1.77 | 2.05 / 1.54 / 1.47 |

The store as built writes a 4 MiB file whatever the chunk holds, so its rate barely moves with the
chunk's size: 76 to 127 chunks a second on the 970 EVOs, whether the chunk is 64 KiB or 1 MiB. The
4 MiB row is [O102](../appendix/optimizations.md#o102-a-whole-chunk-costs-its-device-twice-its-bytes)'s
case alone, since there the class is the whole chunk.

## 2. Device bytes a byte put

Each cell's median. The range runs across the six legs and both depths.

| Chunk | `today` | `class-pool` | `class-fresh` |
| --- | --- | --- | --- |
| 64 KiB | 65.4 to 65.6 | 2.25 to 2.47 | 1.13 to 1.29 |
| 256 KiB | 17.1 | 2.07 to 2.12 | 1.04 to 1.08 |
| 1 MiB | 5.0 | 2.02 to 2.03 | 1.01 to 1.02 |
| 4 MiB | 2.0 | 2.00 to 2.01 | 1.00 |

- **A fresh file writes past its bytes** only its header block, 4 KiB a chunk, and the
  filesystem's journal: 13 to 29% more at 64 KiB, and nothing measurable at 4 MiB.
- **A pool of the class writes every byte twice**: its zeros, then its data.
- **The store as built writes the whole 4 MiB chunk twice**, whatever the stripe held.

## 3. Where a chunk's time goes

One 64 KiB chunk in flight, µs at the median of the rounds. A flush is a cache flush the kernel
sent the device. The Optane writes through its cache, and the disks ran with theirs off, so neither
was sent one.

| Device | Side | Made durable | Directory syncs | Whole cycle | Flushes a chunk | Chunks a second |
| --- | --- | --- | --- | --- | --- | --- |
| 970 EVO, titan | `today` | 2,345 | 5,877 | 9,103 | 4.23 | 82.3 |
| | `class-pool` | 2,772 | 6,960 | 8,371 | 4.57 | 118 |
| | `class-fresh` | 2,894 | 2,033 | 5,380 | 2.00 | 181 |
| Disk, titan | `today` | 47 | 33,187 | 62,570 | none | 11.0 |
| | `class-pool` | 46 | 26,813 | 54,204 | none | 17.5 |
| | `class-fresh` | 12,518 | 8,311 | 37,512 | none | 29.0 |
| Optane, europa | `today` | 22 | 429 | 1,011 | none | 501 |
| | `class-pool` | 20 | 125 | 298 | none | 3,129 |
| | `class-fresh` | 47 | 59 | 274 | none | 3,517 |

What they say:
- **On the 970 EVO a fresh file's sync cost what the shared flush did**: 2.9 ms against 2.8, the same
  at every size and on both hosts. X6 measured the same step at 7.2 ms against an overwrite's 3.2,
  one chunk at a time on hyperion's drive. X15 does not say why the two differ.
- **A pool's chunk paid at its directory syncs instead**: 7.0 ms against 2.0 on the 970 EVO, and
  26.8 ms against 8.3 on the disk. The refill was writing a file of zeros on the same device and
  syncing it, and making it in the directory those syncs commit.
- **On a disk with its write cache off, the shared flush costs nothing**, 46 µs, since there is no
  cache to flush. A fresh file's sync costs a journal commit there, 12.5 ms. The pool still loses:
  its refill's zeros and their sync take the disk's arm between chunks.
- **On the Optane every step is cheap**, and the refill is what separates the sides. The pool of the
  class placed 3,129 chunks a second and a fresh file 3,517. The store as built, whose refill
  writes 4 MiB a chunk, placed 501.
- **Even at one in flight the store as built fell behind its own refill**: on titan's 970 EVO, 113
  of 600 chunks at 64 KiB found its pool empty.

## 4. Eight in flight

A pool is refilled one file at a time. With eight chunks in flight it empties, and a chunk that
finds it empty makes its own file, writing it with zeros before its bytes, as `begin_whole` does.
The pool of the class ran dry for 458 to 473 of 600 chunks at 64 KiB, about 94 to 100 of 128 at
1 MiB, and 21 to 22 of 32 at 4 MiB, on every leg. The chunks that waited for a file waited:

| Leg, 64 KiB | `class-pool`: taking a file | `today`: taking a file | `class-fresh`: making and allocating one |
| --- | --- | --- | --- |
| Optane, europa | 0.8 ms | 14 ms | 0.2 ms |
| 970 EVO, titan / hyperion | 12.8 / 13.0 ms | 40 / 40 ms | 0.4 / 0.4 ms |
| Disk, europa / titan | 83 / 58 ms | 218 / 301 ms | 0.3 / 0.5 ms |

(The first round's medians.) A fresh file sent 0.8 to 1.0 flushes a chunk at eight in flight on the
970 EVOs, against 1.9 to 2.1 for the pool of the class.

## What would have changed the design

| Expected before the first round | Found |
| --- | --- |
| 1. Device bytes a byte at 64 KiB: `today` about 65, `class-pool` about 2.1, `class-fresh` under 1.5 | **Yes**, the pool a little higher: 65.4 to 65.6, 2.25 to 2.47, and 1.13 to 1.29. The pool's refill makes its files through the filesystem's journal too |
| 2. One in flight, 970 EVO: `class-pool`'s cycle below `class-fresh`'s, its rate within or below | **The cycle, no; the rate, yes.** `class-fresh`'s cycle was the shorter, 5.4 ms against 8.4: its own sync cost what the shared flush did, and the pool's directory syncs cost three and a half times its. Its rate was 1.53 times the pool's |
| 3. Eight in flight, 970 EVO: `class-fresh` ahead, with fewer flushes a chunk | **Yes**: 1.59 to 1.80 times, with 0.8 to 1.0 flushes a chunk against 1.9 to 2.1 |
| 4. The Optane: `class-fresh` ahead at every size and depth | **Yes**: 1.12 to 1.86 times, every cell disjoint |
| 5. The disks: `class-fresh` ahead at every size | **Yes, at every median**: 1.25 to 2.40 times. Five cells of twenty-four overlap, all at one in flight |
| 6. 4 MiB: `today` and `class-pool` alike, `class-fresh` writing half their bytes | **Yes**: the pool at 0.88 to 1.18 of the store as built, and `class-fresh` at 1.00 device byte a byte against 2.0 |
| The decision: (A) unless `class-fresh` falls below `class-pool`, disjoint, at eight in flight on the 970 EVO or a disk, or writes more device bytes anywhere | **Neither**: (A) |

## Recommendation

**A chunk's file is made for it, and nothing is written ahead.**
[F96](../features/chunk-file-classes.md) builds it:
- **A whole chunk's file is made in full before anything names it.** The file is allocated to its
  class with `fallocate`. Its units are written, then the zeros up to its class, then its header.
  It is synced by its own `fdatasync`.
- **Then it is named.** A put's chunk is renamed over the chunk's path. A staged chunk is renamed
  into the pool and named by its journal record.
- **The refill is retired.** A file that a removed chunk gives back is kept, up to `free_files` a
  length, and taken before a fresh one is made. It is already written, so it saves the
  allocation's commit, which X15 did not measure (below).
- **A write that takes a chunk past its class stages the chunk whole** into a file of the new
  class, the user's choice before the run. A file of the chunk's own length would rewrite it at
  every append; a class is rewritten once a doubling.

## What it does not settle

- **The object's directory is made before the clock.** A put of a new object makes its directory
  and syncs its parent, which costs every side the same.
- **Only placed chunks are measured**, a put's. A whole chunk staged under a journal record, as a
  write's is, adds the record's sync to either side.
- **One slice on one executor**, as X6's measurement 1 had it.
- **A recycled file of the class beside a fresh one.** F96 takes a file a removed chunk gave back
  before it makes one. X6 measured a recycled file's sync at 1.3 ms against a fresh one's 4.9, at
  64 KiB with six in flight; X15's sides did not include one.
- **Why a fresh file's sync cost X15 under half what it cost X6** on the same drive.
- **Why the two sides parted at 4 MiB on titan's and hyperion's disks, at eight in flight.** There
  they do the same work, yet the pool's side ran at 0.90 and 0.88 of the store's, every round below
  one, though the order alternated. Each side writes in a directory of its own, which XFS may place
  at a different radius of the disk; nothing here tests that.
- **A full device.** A fresh file needs free space at its stage, where a pool's file took its space
  ahead. F96 refuses the stage `NoSpace` before anything can commit on it; X15 did not run a device
  near full.

## Related

- [F96. A chunk's file sized to what it holds](../features/chunk-file-classes.md), which builds the
  recommendation.
- [O107](../appendix/optimizations.md#o107-a-stripe-shorter-than-its-chunk-costs-its-device-the-whole-chunk)
  and [O102](../appendix/optimizations.md#o102-a-whole-chunk-costs-its-device-twice-its-bytes).
- [A short stripe writes a whole chunk](store-optimizations/short-stripes.md), the comparison's
  page.
- [X6. The device store on SSD](device-store-ssd.md), whose harness this extends.
- [S6. The device store](device-store.md).
