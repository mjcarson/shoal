# F96. A chunk's file sized to what it holds

Since this feature a stripe chunk's file holds the **class** of units its chunk holds, the fewest a
power of two that hold its bytes, not its pool's whole chunk. It is a **fresh** file made for the
chunk and synced by itself, or a file a removed chunk gave back; nothing is written ahead. A 64 KiB
stripe now costs each holder its own 64 KiB and a header block, where it cost about 4 MiB: the
largest gap [the store comparison](../object-storage/store-comparison.md) found
([O107](../appendix/optimizations.md#o107-a-stripe-shorter-than-its-chunk-costs-its-device-the-whole-chunk)),
and the doubling of every whole chunk's bytes
([O102](../appendix/optimizations.md#o102-a-whole-chunk-costs-its-device-twice-its-bytes)).

## Context

[F85](devices-and-pools.md) built the device store as [S6](../object-storage/device-store.md)
designed it and [X6](../object-storage/device-store-ssd.md) measured it:
- **A whole chunk was written into a file the slice wrote ahead with zeros**, taken from a pool of
  its chunk length and renamed into place. X6 measured a fresh file's `fdatasync` at 7.2 ms on the
  970 EVO against 3.2 ms to overwrite the same bytes, since a fresh file commits its allocation.
- **The pool was refilled behind every chunk taken** with another file of zeros, so a device wrote a
  whole chunk's length twice ([O102](../appendix/optimizations.md#o102-a-whole-chunk-costs-its-device-twice-its-bytes)).
- **Every file was the pool's whole chunk**, 4 MiB and a header block for a replicated pool, however
  short its stripe. A put of 64 KiB cost each holder about 4 MiB of writes, and the comparison
  counted 196 device bytes a byte put with three copies, against Ceph's 3.5
  ([A short stripe writes a whole chunk](../object-storage/store-optimizations/short-stripes.md)).

The [F90 todo](../appendix/todos.md#a-short-chunk-in-a-file-of-its-own-length) sketched a pool a
length class. Four decisions were taken with the user before anything was built:
- **Spike both ways of sourcing a short file first** ([X15](../object-storage/short-chunk-files.md)):
  a pool of the class written ahead, or a fresh file synced by itself.
- **The writer grows a chunk**: a write that takes a chunk past its file's class stages it whole.
- **A wire version gates the change** (16), so a cluster upgrades in place.
- **Each change is measured before and after on the lab** with the comparison's own arms.

## What it does

### A chunk's class

`class_units(length, unit_bytes, chunk_units)` (`shoal-proto/src/shared/objects.rs`) is the fewest
units a power of two that hold `length`, never fewer than one and never more than the chunk's.
`Geometry::class_of` gives a position's class and `Geometry::outgrows` says whether a write that takes
a stripe from one length to another takes a chunk that holds bytes past its class. A replicated
pool's 4 MiB chunk has classes of 1, 2, 4, 8, 16, 32 and 64 units of 64 KiB; a 4+2 pool's 1 MiB data
chunks 1 to 16.

### Where its file comes from

`Slice::begin_whole` sizes the file `BLOCK + class × unit`, then:
- **takes a file of that length a removed chunk gave back**, if the pool holds one: whole already,
  so it is overwritten and made durable as before, by the shared flush, its own sync, or its record's;
- **or makes a fresh one**, `free/<length>/<n>.tmp`, while the device has room above its reserve,
  else `NoSpace` before anything could commit on it. The file is allocated to its length
  (`allocate`, `fallocate` through glommio, zeros where a filesystem cannot), its units and then the
  zeros up to its class are written, then its header, and it is synced by its own `fdatasync`, which
  commits its allocation with its bytes. A put's chunk is then renamed over its path; a staged one
  is renamed into the pool first, `free/<length>/<n>`, and named by its record. On any failure, or
  an abort, the temporary file is removed: no pool takes a file half written.

The fixture's `FAULT_DIR <dir> full <bytes>` refuses that allocation past its bytes, as a full disk
would: the glommio fork's `pre_allocate` asks a test's I/O hook with `IoOp::Allocate`, where it
asked `Meta`.

The refill is retired. The pool keeps at most `free_files` files a length, the ones removed chunks
give back; a file given back past that is removed. `FreePool::open` still removes a `.tmp` a crash
left, so no file of the pool has an extent its writes left unwritten.

### Growing a chunk

**A chunk is never extended in place.**
- **The writer stages a write whole** where it takes a chunk past its class: `write_stripe` reads
  every unit the chunk holds that the write does not carry, in one read at the row's label, and
  stages the chunk whole into a file of its new class. A small write rides its commit only while it
  stays inside its class, so a fold never needs a larger file after its commit. An erasure coded
  stripe goes to its whole mode when any position's class grows (`plan_write`).
- **The holder refuses a stage of units past its file** by name, `Invalid`, an existing code: a
  units stage reaching more units than the file it would apply into has room for, read from the
  file's size. A rebuild whose units copy is refused so copies the chunk whole.
- **An apply never writes past its file**, refused by name with the file as it was.

### Reads

A read takes from a base file only the units its header holds; a unit past them is a record's, or
zeros. A rotational slice's span is read up to the base's last unit and laid over past it.

### The wire

Wire 16 (`CLASS_WIRE_VERSION`). No frame moved, so a 16 build speaks 15 to an older peer and a
cluster upgrades in place; a holder sizes a file by class only once its object executor's installed
map has activated 16 (`Lanes::classed`), and a node of no cluster at once. A cluster node is told
apart by its plan to dial, never by its map's members: an executor holds a map of no members and
the floor's activation until its node installs the first, and must not size by class then.
`shoaladm`'s bootstrap and `upgrade --activate` activate the newest wire, as before.

### Figures

A slice's `chunk_bytes` counts each file at its own length: an apply over a chunk counts the new
file's length and takes the old one's away, and a reclaimed chunk is counted out at its file's.

## Design choices

- **Fresh files, not class pools written ahead**, measured by X15 on every host's SSD and disk:
  - a fresh file placed 1.12 to 2.40 times the chunks a second of a pool of the class, each cell's
    median, every SSD cell disjoint and five disk cells of twenty-four overlapping at one in flight;
  - at 1.13 to 1.29 device bytes a byte at 64 KiB, where the pool wrote 2.25 to 2.47;
  - its own sync cost what the shared flush did on the 970 EVO, 2.9 ms against 2.8, and the pool
    paid at its directory syncs, behind its refill, and ran dry at eight in flight.
- **A file a removed chunk gave back is still taken first.** It is whole already, so it is
  overwritten and made durable as F85 made a pool's file, with no allocation to commit. X15 did not
  measure one; X6 measured a recycled file's sync at 1.3 ms against a fresh one's 4.9.
- **Powers of two**, so a chunk grown by appends is written whole at most once a doubling; a file of
  the chunk's own length would rewrite it at every append past it.
- **The writer grows a chunk, not the holder.** A holder that copied its chunk into a larger file
  could not when the chunk was still a staged whole in the pool, which is what the next append meets
  most often, and one that recorded a stage of units as a whole chunk would change what a holder
  records, which X1's model holds.
- **The holder's refusal reads the file's real room**, so a file written before F96, a whole chunk
  long, takes units an older coordinator stages there.
- **A wire gate**, so every coordinator applies the writer's rule before any holder makes a file a
  class long.

## Alternatives rejected

- **A pool of each class written ahead** (B): every rule as F85 built it, at two device bytes a byte;
  X15's figures above.
- **A file of the chunk's own length**: each append past it a whole rewrite, quadratic in the appends.
- **The holder re-homes a chunk into a larger file**: it cannot when the chunk is a staged whole, and
  the stage of units it would follow holds the chunk's read lock.
- **The holder promotes a stage of units to a whole chunk**: no network for the copy, but a change to
  what a holder records, which the model would have to take first.
- **No gate, a limitation**: a rolling upgrade's older coordinators would have appends refused past
  their class and folds failed.

## Limitations

- **A replaced chunk's old file is not recycled.** A whole chunk over another unlinks the old file,
  after its previous state's link, where O102 proposed giving it to the pool: since nothing is
  written ahead a recycled file saves only a fresh file's allocation.
- **A chunk written before F96 keeps its whole chunk's file** until it is rewritten whole. The writer
  judges growth by the class of a chunk's length, so a small write past that class on such a chunk is
  staged whole though its file has room.
- **A whole chunk staged on a device that flushes its cache costs two more flushes**: its fresh file's
  `fdatasync` and its pool directory's, beside its record's. The store's own figures put a 1 MiB
  whole stage at 0.88 to 0.91 of before on the 970 EVO
  ([O123](../appendix/optimizations.md#o123-a-whole-chunk-staged-into-a-fresh-file-pays-its-own-sync-and-its-pool-directorys-beside-its-records)).
- **A growth costs a read and the chunk over the network** at each class it crosses, about the chunk
  once more over its life.
- **X1's model draws whole writes and records the units they changed**, where the product records
  the whole chunk: the product's record is the more conservative.
- **A file shorter than the units its header holds**, which nothing the store writes leaves, fails a
  read as an unreadable file does, rather than being set aside.
- **No build of 15 was run beside one of 16.** The roll rests on no frame having moved at 16, and the
  gate on the fixture's row, which activates 15 and then 16 on builds of 16.
- **The pool keeps up to `free_files` files of every length a reclaimed chunk had.** A slice that
  reclaims chunks of all seven classes of a 4 MiB chunk can hold eight of each, about 64 MiB, where
  F85's pool held eight whole chunks, 32 MiB, written ahead whether anything was reclaimed or not.
  Nothing trims the pool, so a length no later stage asks for keeps its files.
- **The store's `chunk_lengths` names a length to its pool and nothing more.** The pool serves any
  length on demand, so the option is kept for its callers rather than for what it does.

## Invariants to uphold

- **A chunk is never extended in place.** A units stage and an apply write only inside their file;
  a write past a chunk's class is staged whole.
- **Every byte of a fresh file is written, and the file synced by itself, before anything names
  it**: its chunk path, or the pool and then a record. The shared flush stays correct only while a
  chunk is overwritten within extents already written.
- **A file a fresh write half made never reaches the pool**: removed on any failure, and a `.tmp` a
  crash left removed at the next open.
- **A read never reads past the units its base holds.**
- **No holder sizes a file by class below wire 16**, nor a cluster node's executor before its first
  map.

## Performance

### X15

[X15](../object-storage/short-chunk-files.md) chose the file's source; its figures are on its page.
What F96 gains over the store as built is X15's `class-fresh` beside its `today`, chunks a second,
the median of four rounds:

| Chunk, in flight | Optane (europa) | 970 EVO (titan / hyperion) | Disk (europa / titan / hyperion) |
| --- | --- | --- | --- |
| 64 KiB, 1 | 7.01 | 2.20 / 2.19 | 3.84 / 2.69 / 2.46 |
| 64 KiB, 8 | 15.55 | 4.18 / 4.23 | 6.50 / 5.77 / 4.85 |
| 1 MiB, 8 | 4.19 | 3.13 / 3.18 | 3.59 / 4.02 / 4.37 |
| 4 MiB, 8 | 1.81 | 1.78 / 1.77 | 2.05 / 1.54 / 1.47 |

Device bytes a byte put fell from 65 to 1.13 to 1.29 at 64 KiB, and from 2.0 to 1.00 at 4 MiB.

### The store's own figures

`shoal-spike store --only whole,short`, three rounds a side under `performance` with each host's node
stopped (`results/store-host.sh`), at the commit before F96 with F96's harness and at F96, on
europa's Optane and titan's 970 EVO. Both sections stage a whole chunk and apply it, one at a time:
a write's path, where a put's chunk is placed without a record. The records are
`shoal-spike/results/store/store-f96-{before,after}-*`.

**Short chunks of a 4 MiB pool**, chunks a second, each round's figure:

| Chunk | Optane, before | Optane, after | 970 EVO, before | 970 EVO, after |
| --- | --- | --- | --- | --- |
| 64 KiB | 449, 458, 467 | 1,638, 1,642, 1,675 | 64, 75, 77 | 90, 90, 91 |
| 256 KiB | 414, 441, 441 | 1,149, 1,162, 1,169 | 71, 72, 76 | 83, 83, 84 |
| 1 MiB | 321, 323, 328 | 518, 528, 530 | 58, 58, 58 | 67, 68, 68 |

Device bytes a byte fell from 65.5, 17.1 and 5.0 to 1.35 to 1.39, 1.09 to 1.10 and 1.02 on both.

**Whole chunks of a 1 MiB pool**, MiB a second, the shared flush:

| | Optane | 970 EVO |
| --- | --- | --- |
| Before | 484, 486, 493 | 75, 76, 76 |
| After | 502, 507, 508 | 67, 68, 68 |

- **A short chunk staged is 1.2 to 3.6 times as fast**: 3.6 times at 64 KiB on the Optane, where a
  sync costs no flush, and 1.2 to 1.4 times on the 970 EVO, every round's apart.
- **A whole chunk staged at its pool's full length is slower on the 970 EVO**, 0.88 to 0.91 of
  before in every round, and 4% faster on the Optane. A staged fresh file is made durable by its own
  `fdatasync` and its pool directory's, beside its record's sync, where a pool's file was made
  durable by the record's sync alone; on a device that flushes its cache at every sync, those are
  two more flushes a stage. A put's placed chunk pays neither, as X15 measured: filed as
  [O123](../appendix/optimizations.md#o123-a-whole-chunk-staged-into-a-fresh-file-pays-its-own-sync-and-its-pool-directorys-beside-its-records).

### On the lab

The store comparison's arms were run before this commit (`33e9f5c1`) and at it (`af3a190b`), with
each side's node built at its commit and checked against its captures' `program_sha256`.
`stores-lab.sh ab` took four captures of Shoal alone, in the order before, after, after, before.
Each capture ran on the leg's devices made fresh, with the tmdb node stopped and every governor at
`performance`. Every arm ran four times, 30 s each after 5 s of warmup. A side's figure is the
median of its eight runs, with the lowest and highest beside it. A difference is a result only where
the two sides' ranges are disjoint. The records are in `shoal-spike/results/stores-ab/f96-<leg>-<group>/`,
and `results/stores-ab-report.py` prints these tables from them.

**SSDs over 1 GbE, three copies** (2026-10-20): europa's Optane, and titan's and hyperion's 970
EVOs. Operations a second:

| Arm | Before | After | After ÷ before | Device bytes a byte put |
| --- | --- | --- | --- | --- |
| Puts, 64 KiB, one in flight | 30.6 [26.9–39.0] | 44.8 [28.2–45.1] | 1.46, within | 198 → 5.1 |
| Puts, 64 KiB, sixteen | 67.2 [59.8–108] | 263 [251–270] | **3.92, ahead** | 197 → 4.8 |
| Puts, 1 MiB, one | 21.2 [15.7–24.2] | 17.9 [15.8–23.9] | 0.84, within | 14.4 → 4.0 |
| Puts, 1 MiB, sixteen | 55.6 [53.1–71.2] | 73.3 [72.4–75.0] | **1.32, ahead** | 14.3 → 3.2 |
| Puts, 16 MiB, one | 2.90 [2.77–2.93] | 2.93 [2.83–2.97] | 1.01, within | 5.8 → 3.1 |
| Puts, 16 MiB, sixteen | 4.60 [4.33–4.97] | 4.53 [4.37–4.73] | 0.99, within | 6.0 → 3.0 |
| Gets, 64 KiB, one | 608 [573–642] | 615 [586–654] | 1.01, within | |
| Gets, 64 KiB, sixteen | 3,327 [2,973–3,798] | 3,273 [3,109–3,427] | 0.98, within | |
| Gets, 1 MiB, one | 119 [111–130] | 118 [109–128] | 0.99, within | |
| Gets, 1 MiB, sixteen | 222 [200–243] | 218 [197–245] | 0.98, within | |

- **A 64 KiB put writes 4.8 to 5.1 device bytes a byte, down from 197 to 198.** That is under the 10
  the comparison's page asked for. Three copies of a 64 KiB chunk and its header block account for
  3.19. The rest is the rows' WAL and the filesystem's journal.
- **At sixteen in flight, puts are faster and the ranges are disjoint**: 3.9 times at 64 KiB, and
  1.32 times at 1 MiB.
  - Before, at 64 KiB, the 970 EVOs were 95% busy writing 287 MiB/s. After, they were 94% busy
    writing 24 MiB/s, so bytes no longer bound them.
  - Each 970 EVO took 7.4 device writes a put, where Ceph's took 4.2 at twice the rate
    ([O124](../appendix/optimizations.md#o124-a-64-kib-put-keeps-its-970-evo-busy-at-a-tenth-of-its-bytes)).
- **At one in flight, puts are within.**
  - At 64 KiB the medians' ratio is 1.46. The p99 was 31 ms in six of the after side's eight runs;
    the before side's was 36 to 83 ms.
  - At 1 MiB the ratio is 0.84, but the runs it rests on carried other work. Three of each
    capture's four runs follow a run of 64 KiB puts at sixteen in flight, whose objects are deleted
    and reclaimed during them (`reclaim_grace` 5 s, against the bench's settle of 10 s).
  - Since F96 that run puts about 9,000 objects, where it put 2,700 to 3,800 before. The 1 MiB
    runs that followed it made about 1,150 device writes a second on titan for the same bytes as
    the run that followed other work, which made 330.
  - That run placed 23.9 objects a second in both after captures, against 21.6 and 20.9 before.
- **16 MiB puts are within, at half the device bytes**: 6.0 fell to 3.0 at sixteen in flight.
  europa's 1 GbE link bounds them on this leg, as it bounded every store's in the comparison. The
  disk leg judges them
  ([A whole chunk is written twice](../object-storage/store-optimizations/whole-chunk-twice.md)),
  and there they ran 1.58 times as fast at sixteen in flight (below).
- **Gets are unchanged**, every arm within. On an SSD F96 changes a read only by bounding it by its
  file; on the disks the files' sizes moved the gets as well (below).
- **Nothing was lost.** The read-back after every run checked 8,393 objects after and 8,579 before.
  The check at each capture's end checked 3,072. None were lost.

**SSDs over 1 GbE, 2+1** (2026-10-20), on the same devices. Operations a second:

| Arm | Before | After | After ÷ before | Device bytes a byte put |
| --- | --- | --- | --- | --- |
| Puts, 64 KiB, one in flight | 40.3 [25.4–41.2] | 41.0 [25.5–42.5] | 1.02, within | 68 → 3.7 |
| Puts, 64 KiB, sixteen | 119 [110–170] | 301 [280–304] | **2.52, ahead** | 68 → 3.3 |
| Puts, 1 MiB, one | 22.7 [16.2–27.9] | 20.6 [18.0–26.7] | 0.91, within | 5.9 → 2.7 |
| Puts, 1 MiB, sixteen | 84.2 [80.4–86.7] | 95.1 [93.1–96.1] | **1.13, ahead** | 5.8 → 2.1 |
| Puts, 16 MiB, one | 5.03 [4.67–5.13] | 4.88 [4.07–5.10] | 0.97, within | 2.9 → 1.6 |
| Puts, 16 MiB, sixteen | 6.98 [6.90–7.07] | 6.95 [6.90–7.03] | 1.00, within | 3.0 → 1.5 |
| Gets, 64 KiB, one | 411 [409–412] | 426 [331–441] | 1.04, within | |
| Gets, 64 KiB, sixteen | 1,956 [1,940–1,985] | 2,057 [2,017–2,088] | **1.05, ahead** | |
| Gets, 1 MiB, one | 60.4 [58.0–61.8] | 63.3 [61.7–64.0] | 1.05, within | |
| Gets, 1 MiB, sixteen | 129 [127–132] | 132 [131–134] | 1.02, within | |

- **A 64 KiB put writes 3.3 to 3.7 device bytes a byte, down from 68.** At 2+1 a 64 KiB stripe
  stores its first data chunk and a parity chunk as long. Each is a unit and a header block, and the
  two account for 2.13.
- **At sixteen in flight, puts are faster and the ranges are disjoint**: 2.5 times at 64 KiB, and
  1.13 times at 1 MiB.
  - At 64 KiB the 970 EVOs went from 94% busy writing 171 to 174 MiB/s to 93 to 94% busy writing
    19 MiB/s.
  - Each took 6.0 to 6.1 device writes a put. With three copies it took 7.3 to 7.5, though here each
    holds a chunk of only two puts in three
    ([O124](../appendix/optimizations.md#o124-a-64-kib-put-keeps-its-970-evo-busy-at-a-tenth-of-its-bytes)).
  - At 1 MiB they went from 94% busy writing 165 to 168 MiB/s to 85% busy writing 63 MiB/s.
- **At one in flight, puts are within.**
  - At 64 KiB the first run of every capture was the slowest, at 25 to 27 a second on both sides.
    The other three runs placed 40 to 42 a second on both sides. The 970 EVOs wrote 53 MiB/s before
    and 3 MiB/s after, so their bytes did not bound these puts.
  - At 1 MiB, as with three copies, three of each capture's four runs follow a run of 64 KiB puts at
    sixteen in flight. Those runs deleted 9,700 to 10,600 objects after and 4,200 to 6,000 before.
    The 1 MiB runs that followed made 700 to 1,100 device writes a second on titan after, against
    270 to 560 before.
  - The 1 MiB run that followed other work placed 26.6 and 26.7 objects a second after, against
    27.8 and 27.9 before.
- **16 MiB puts are within, at half the device bytes**: 3.0 fell to 1.5 at sixteen in flight. The
  970 EVOs went from 49% busy to 33%.
- **Gets of 64 KiB at sixteen in flight were 5% faster, with the ranges disjoint. The cause is not
  known.**
  - F96 changes a read only by bounding it by its file, and the device read the same bytes for
    each byte served, 1.06.
  - Each of these runs followed a run of 16 MiB puts, which wrote 4.0 GiB to each 970 EVO before
    F96 and 2.0 GiB after. What the device was still doing after that is one candidate, but it was
    not measured.
  - The gain was 7% between the first pair of captures and 2% between the second.
- **Nothing was lost.** The read-back after every run checked 8,662 objects after and 8,643 before.
  The check at each capture's end checked 3,072. None were lost.

Beside the comparison's own captures of the other stores, which ran on another day, 64 KiB puts are
still behind Ceph, SeaweedFS and MinIO, but by much less. They are now within RustFS at 2+1. At
1 MiB and sixteen in flight, Shoal is now ahead of Ceph in both groups. The figures are on
[A short stripe writes a whole chunk](../object-storage/store-optimizations/short-stripes.md#before-and-after).

**Disks, three copies** (2026-10-21): each host's disk, a WD6001FZWX in europa and a WD140EDFZ in
titan and hyperion, their write caches off, each with its journal on the host's SSD. Each node's
rows and logs are on its SSD, as on the SSD leg. Operations a second:

| Arm | Before | After | After ÷ before | Device bytes a byte put |
| --- | --- | --- | --- | --- |
| Puts, 64 KiB, one in flight | 7.05 [6.53–7.90] | 10.2 [9.47–11.1] | **1.44, ahead** | 199 → 5.6 |
| Puts, 64 KiB, sixteen | 17.8 [15.1–20.6] | 55.5 [48.7–69.2] | **3.12, ahead** | 203 → 4.6 |
| Puts, 1 MiB, one | 6.52 [6.13–7.30] | 6.32 [3.63–9.10] | 0.97, within | 14.3 → 3.6 |
| Puts, 1 MiB, sixteen | 15.6 [13.3–17.7] | 35.2 [26.2–39.0] | **2.27, ahead** | 14.3 → 3.2 |
| Puts, 16 MiB, one | 1.43 [1.03–1.57] | 1.28 [1.13–1.83] | 0.90, within | 5.5 → 3.1 |
| Puts, 16 MiB, sixteen | 2.65 [2.27–2.93] | 4.20 [3.83–4.57] | **1.58, ahead** | 6.1 → 3.1 |
| Gets, 64 KiB, one | 126 [124–131] | 411 [366–461] | **3.26, ahead** | |
| Gets, 64 KiB, sixteen | 491 [439–551] | 2,680 [2,413–3,167] | **5.46, ahead** | |
| Gets, 1 MiB, one | 38.2 [34.7–41.2] | 46.2 [42.0–49.7] | **1.21, ahead** | |
| Gets, 1 MiB, sixteen | 156 [143–162] | 165 [162–169] | 1.06, within | |

- **A 64 KiB put writes 4.6 to 5.6 device bytes a byte, down from 199 to 203.** That is under the
  10 the comparison's page asked for. Each disk wrote 1.28 to 1.53 bytes a byte put, down from 65
  to 71; a 64 KiB chunk and its header block are 1.06. Each host's SSD wrote 0.21 to 0.35, for the
  journal and the rows.
- **At sixteen in flight, puts are faster at every size and the ranges are disjoint**: 3.1 times at
  64 KiB, 2.3 times at 1 MiB and 1.6 times at 16 MiB. At one in flight, 64 KiB puts are 1.44 times
  as fast, also disjoint.
  - Before, at 64 KiB, each disk was 97 to 99% busy writing 69 to 73 MiB/s, nearly all of it the
    4 MiB files written ahead. After, each wrote 4.1 MiB/s, titan's and hyperion's 87 to 91% busy
    and europa's 65%.
  - Each disk made 143 to 148 writes a second on both sides, of 490 to 530 KiB before and 28 to
    29 KiB after. A put took 2.6 to 2.7 of each disk's writes, down from 8.1. So what bounds a
    64 KiB put on the disks is now their writes, not their bytes, as
    [O124](../appendix/optimizations.md#o124-a-64-kib-put-keeps-its-970-evo-busy-at-a-tenth-of-its-bytes) found of the 970 EVOs.
  - At 16 MiB each disk wrote 1.01 to 1.05 bytes a byte put, down from 2.0. It was 97 to 99% busy
    on both sides, writing 66 to 68 MiB/s where it had written 79. With its write cache off it
    took 98 to 100 writes a second of about 690 KiB, where it had taken 149 to 153 of about 540.
- **At one in flight, 1 MiB and 16 MiB puts are within.** Their slowest runs carried other work.
  - In each after capture, two of the four 1 MiB runs placed 3.6 to 5.0 objects a second. The
    other two placed 7.7 to 9.1, and every run before placed 6.1 to 7.3. In the two slow runs
    titan's disk read 53 to 63 times a second, where in the others it read 5 to 15, and its SSD
    wrote 241 to 267 times a second, where in the others it wrote 63 to 79.
  - Both slow runs followed a run of 64 KiB puts at sixteen in flight. That run deletes what it
    put: 1,625 to 2,374 objects since F96, where it deleted 533 to 734 before. Most likely the slow
    runs ran beside that deletion's reclamation. One of the fast runs followed such a run too, so
    this was not established.
  - The 16 MiB runs show the same. The first in each after capture placed 1.77 and 1.83 objects a
    second, and the other three 1.13 to 1.33. In the other three titan's disk read 33 to 40 times a
    second, where in the first it read 9 to 13.
- **64 KiB gets ran 3.3 times as fast at one in flight and 5.5 times at sixteen.** Their p99 fell
  from 38.6 to 9.1 ms and from 258 to 13.8 ms. 1 MiB gets ran 1.21 times as fast at one in flight,
  and are within at sixteen.
  - The disks wrote nothing during the gets on either side, and read the same 34 KiB a request.
    Each disk served 2.6 to 8.2 times as many reads a second.
  - At sixteen in flight europa's disk served 2,203 reads a second, 35% busy; before, it served
    468, 95% busy. [X7](../object-storage/device-store-hdd.md#1-sequential-and-random-and-how-far-to-read-ahead) measured this disk's random
    4 KiB reads at 293 a second at depth 32 across its platter, and at 528 within 2 GiB. So most of
    these reads were served without a seek.
  - What F96 changed is the files. The 1,024 objects of 64 KiB the arm reads are files of 68 KiB,
    68 MiB in all on each disk. Before, each was a file of 4 MiB, 4 GiB in all. Whether the disk's
    cache or its read-ahead served the reads was not measured.
- **Nothing was lost.** The read-back after every run checked 7,296 objects after and 7,708 before.
  The check at each capture's end checked 3,072. None were lost.

Beside the comparison's own captures of the other stores, which ran on another day, 64 KiB puts on
the disks are still behind Ceph and SeaweedFS. At sixteen in flight they are behind by at least 3.8
times and 36%, where the comparison had at least 12.6 and 5.2 times. 16 MiB puts at sixteen in
flight are within both, and 64 KiB and 1 MiB gets at sixteen are ahead of both. The figures are on
[A short stripe writes a whole chunk](../object-storage/store-optimizations/short-stripes.md#before-and-after)
and [A whole chunk is written twice](../object-storage/store-optimizations/whole-chunk-twice.md#before-and-after).
**Disks, 2+1** (2026-10-21), on the same devices. Operations a second:

| Arm | Before | After | After ÷ before | Device bytes a byte put |
| --- | --- | --- | --- | --- |
| Puts, 64 KiB, one in flight | 9.88 [9.40–10.2] | 12.4 [12.1–12.9] | **1.25, ahead** | 133 → 4.0 |
| Puts, 64 KiB, sixteen | 28.4 [27.4–29.7] | 72.0 [70.9–77.8] | **2.54, ahead** | 131 → 3.4 |
| Puts, 1 MiB, one | 8.95 [8.13–9.33] | 8.93 [5.07–11.1] | 1.00, within | 9.5 → 2.5 |
| Puts, 1 MiB, sixteen | 24.6 [23.8–27.5] | 53.6 [41.9–55.7] | **2.17, ahead** | 9.0 → 2.1 |
| Puts, 16 MiB, one | 2.82 [2.73–2.93] | 2.47 [2.33–3.00] | 0.88, within | 2.7 → 1.6 |
| Puts, 16 MiB, sixteen | 6.00 [5.80–6.27] | 6.87 [6.53–7.13] | **1.14, ahead** | 3.0 → 1.5 |
| Gets, 64 KiB, one | 127 [124–131] | 330 [315–340] | **2.59, ahead** | |
| Gets, 64 KiB, sixteen | 1,479 [1,462–1,490] | 1,832 [1,809–1,875] | **1.24, ahead** | |
| Gets, 1 MiB, one | 33.9 [32.7–34.5] | 39.2 [36.9–39.9] | **1.16, ahead** | |
| Gets, 1 MiB, sixteen | 122 [119–124] | 121 [120–123] | 1.00, within | |

- **A 64 KiB put writes 3.4 to 4.0 device bytes a byte, down from 131 to 133.** Each disk wrote 0.82
  to 1.10 bytes a byte put, down from 41 to 50. A 64 KiB stripe at 2+1 is two chunks, each a unit
  and a header block, 2.13 over the three disks. Each host's SSD wrote 0.23 to 0.34.
- **At sixteen in flight, puts are faster at every size and the ranges are disjoint**: 2.5 times at
  64 KiB, 2.2 times at 1 MiB and 1.14 times at 16 MiB. At one in flight, 64 KiB puts are 1.25 times
  as fast, also disjoint.
  - At 64 KiB each disk was 96 to 98% busy before, writing 67 to 81 MiB/s. After, each was 81 to
    92% busy writing 3.5 to 4.2 MiB/s.
  - Each disk made 135 to 174 writes a second on both sides. A put took 1.8 to 2.4 of each disk's
    writes, down from 5.4 to 6.1, though each disk holds a chunk of only two puts in three. As with
    three copies, the disks' writes bound the puts now, not their bytes.
  - At 16 MiB each disk wrote 0.50 bytes a byte put, the 1.5 that 2+1 needs over three disks, down
    from 1.0.
- **At one in flight, 1 MiB and 16 MiB puts are within, and their slowest runs carried other work,
  as with three copies.**
  - In each after capture, two of the four 1 MiB runs placed 5.1 to 7.4 objects a second and the
    other two 10.5 to 11.1. Every run before placed 8.1 to 9.3. In the two slow runs titan's disk
    read 43 to 47 times a second, where in the others it read 6 to 9, and its SSD wrote 317 to 344
    times a second, where in the others it wrote 81 to 111.
  - Both slow runs followed a run of 64 KiB puts at sixteen in flight, which deleted 2,392 to 2,695
    objects since F96, where it deleted 951 to 1,046 before. One of the fast runs followed such a
    run too.
  - The 16 MiB runs show the same. The first in each after capture placed 2.93 and 3.00 objects a
    second, and the other three 2.33 to 2.57. In the other three titan's disk read 28 to 35 times a
    second, where in the first it read 7 to 10.
- **64 KiB gets ran 2.6 times as fast at one in flight and 1.24 times at sixteen.** Their p99 fell
  from 20.4 to 6.1 ms and from 54.4 to 15.5 ms. 1 MiB gets ran 1.16 times as fast at one in flight,
  and are within at sixteen.
  - For 64 KiB gets at sixteen in flight the disks went from 73 to 91% busy to 11 to 44%, for the
    same bytes read a byte served. As with three copies, the objects' files are 68 KiB where they
    were 4 MiB.
  - At sixteen in flight europa's 1 GbE link received 105 MiB/s for 64 KiB gets and 111 for 1 MiB,
    its ceiling. The bench's driver is on europa, and titan's and hyperion's larger disks hold four
    fifths of the short objects' data chunks
    ([O116](../appendix/optimizations.md#o116-an-erasure-coded-stripes-first-data-chunk-favours-the-heaviest-device)). So the link bounds these gets now, not the
    disks.
- **Nothing was lost.** The read-back after every run checked 8,152 objects after and 7,261 before.
  The check at each capture's end checked 3,072. None were lost.

Beside the comparison's own captures of the other stores, which ran on another day, 64 KiB puts at
2+1 and sixteen in flight are now ahead of MinIO and RustFS, by at least 56% and 9.5%. The
comparison had Shoal behind both, by at least 28% and 46%. They are still behind Ceph, by at least
5.8 times where it was 15.7. 64 KiB gets at sixteen in flight are ahead of all three, and 16 MiB puts
are within or ahead of all three at both depths.

**Disks, writes in place, three copies** (2026-10-21). The first run found
[item 283](../appendix/resolved/chained-records-decided-together.md): under sixteen writes in flight, a holder's decide
discarded small writes its disk had not applied yet. Both sides were then built again with its fix:
`1b4b46ed` is this commit's parent with the fix, and `83b9dd36` is this commit with it. The arm
writes 4 KiB in place into 1 MiB objects. Writes a second:

| In flight | Before | After | After ÷ before | Device bytes a byte written |
| --- | --- | --- | --- | --- |
| One | 23.5 [12.0–32.1] | 31.8 [27.7–33.6] | 1.35, within | 627 → 213 |
| Sixteen | 106 [97.2–122] | 117 [91.3–124] | 1.10, within | 178 → 153 |

- **Writes in place are no worse**: within at both depths, the medians higher after.
- **A few writes were refused `NotReady`**, alike on both sides: 16 and 14 in one capture on each,
  in three of the sixteen runs at sixteen in flight. A client tries a `NotReady` again, and the
  bench counts it as refused. Why they were refused was not traced.
- **Nothing was lost or left unreadable.** The check at each capture's end read all 1,024 objects
  back; at the build before the fix, 417 of them could not be read.

**Europa's loopback, three copies** (2026-10-21): three nodes on europa over 127.0.0.11 to 13,
every root and pool device on a loop device of its own on the Optane. The writes in place ran from
worktrees with item 283's fix on both sides, as on the disks. Operations a second:

| Arm | Before | After | After ÷ before | Device bytes a byte put |
| --- | --- | --- | --- | --- |
| Puts, 64 KiB, one in flight | 122 [115–123] | 198 [196–199] | **1.63, ahead** | 198 → 4.8 |
| Puts, 64 KiB, sixteen | 185 [183–186] | 1,481 [1,477–1,491] | **8.02, ahead** | 195 → 4.6 |
| Puts, 1 MiB, one | 94.3 [93.7–95.2] | 73.8 [73.1–123] | 0.78, within | 14.8 → 4.4 |
| Puts, 1 MiB, sixteen | 146 [144–157] | 398 [361–403] | **2.73, ahead** | 14.6 → 3.1 |
| Gets, 64 KiB, one | 1,959 [1,944–1,978] | 1,905 [1,341–1,921] | 0.97, behind | |
| Gets, 64 KiB, sixteen | 14,938 [14,899–14,972] | 14,885 [14,813–14,928] | 1.00, within | |
| Gets, 1 MiB, one | 565 [554–575] | 567 [566–571] | 1.00, within | |
| Gets, 1 MiB, sixteen | 2,338 [2,327–2,370] | 2,344 [2,318–2,365] | 1.00, within | |
| Writes in place, 1 MiB, one | 303 [300–304] | 303 [300–305] | 1.00, within | 168 → 168 |
| Writes in place, 1 MiB, sixteen | 1,552 [1,498–1,562] | 1,545 [1,481–1,554] | 1.00, within | 165 → 165 |

**Europa's loopback, 2+1** (2026-10-21):

| Arm | Before | After | After ÷ before | Device bytes a byte put |
| --- | --- | --- | --- | --- |
| Puts, 64 KiB, one in flight | 196 [192–197] | 198 [196–198] | 1.01, within | 67.7 → 3.4 |
| Puts, 64 KiB, sixteen | 501 [498–502] | 1,523 [1,516–1,531] | **3.04, ahead** | 64.1 → 3.1 |
| Puts, 1 MiB, one | 106 [104–109] | 79.8 [78.3–123] | 0.75, within | 5.6 → 2.9 |
| Puts, 1 MiB, sixteen | 329 [327–428] | 539 [472–550] | **1.64, ahead** | 5.4 → 2.1 |
| Gets, 64 KiB, one | 1,675 [1,644–1,684] | 1,567 [1,142–1,586] | 0.94, behind | |
| Gets, 64 KiB, sixteen | 13,340 [13,307–13,373] | 13,213 [12,993–13,335] | 0.99, within | |
| Gets, 1 MiB, one | 432 [429–436] | 429 [422–434] | 0.99, within | |
| Gets, 1 MiB, sixteen | 1,794 [1,768–1,810] | 1,772 [1,762–1,779] | 0.99, within | |
| Writes in place, 1 MiB, one | 393 [384–404] | 386 [381–397] | 0.98, within | 76.7 → 76.7 |
| Writes in place, 1 MiB, sixteen | 2,401 [2,363–2,444] | 2,397 [2,352–2,433] | 1.00, within | 72.3 → 72.3 |

- **64 KiB puts at sixteen in flight ran 8.0 times as fast with three copies and 3.0 at 2+1**, and
  1 MiB puts 2.7 and 1.6 times, the ranges disjoint. A 64 KiB put writes 4.6 device bytes a byte
  with three copies and 3.1 at 2+1, where it wrote 195 and 64.
- **At one in flight, 1 MiB puts are within, and three of each after capture's four runs carried
  the run before them.**
  - Those three each followed a run of 64 KiB puts at sixteen in flight, which since F96 puts and
    deletes 51,000 to 53,600 objects, where it deleted about 6,500 with three copies and 18,000 at
    2+1. They placed 73 to 81 objects a second, the devices making 6,100 to 7,900 reads a second
    beside them, the reclamation of what that run deleted
    ([O126](../appendix/optimizations.md#o126-a-holder-syncs-two-directories-for-every-chunk-it-reclaims)).
  - The fourth followed the run at one in flight, about 6,950 deleted, and placed 122 and 123 a
    second, faster than every run before F96, which placed 94 to 109.
- **64 KiB gets at one in flight were 3% and 6% slower, the ranges disjoint, for the same reason.**
  - Each of their runs follows a run of 1 MiB puts at sixteen in flight, which since F96 deletes
    12,500 to 14,100 objects with three copies, where it deleted 5,060 to 5,750. At 2+1 it deletes
    16,500 to 19,200, where it deleted 11,460 to 15,030.
  - Beside that reclamation the devices made 5,800 to 9,800 reads a second, where they made 3,550
    to 5,870. They read 1.15 to 1.34 bytes for each byte the gets were served, where 1.07 to 1.17.
  - Every other get is within.
- **Writes in place are no worse**: within at both depths in both groups, at the same device bytes.
- **Nothing was lost.** The read-back after every run checked 3,984 to 4,042 objects a capture.
  Each capture's end check read every object back.

Beside the comparison's own captures of the other stores, which ran on another day, 1 MiB is the
only size of these that the comparison ran on this leg:
- with three copies, 1 MiB puts at sixteen in flight are still behind Ceph and SeaweedFS, but by at
  least 33% and 35%, where the comparison had at least 4.0 and 4.1 times;
- at 2+1 they are now ahead of RustFS by at least 3.3%, where they were behind it by at least 20%.
  They are behind Ceph and MinIO by at least 39% and 42%, where the comparison had at least 2.6
  and 2.8 times.

With the loop leg F96's legs are all in, and its two rows of the comparison's table are struck
([the optimizations](../object-storage/store-optimizations.md#in-order)).

**Measured again in the comparison** (2026-10-25 and 26), at `257592b3` with every change since
beside F96 ([Shoal measured again](../object-storage/store-comparison.md#shoal-measured-again)),
these guides became its verdicts. Every 64 KiB put wrote 3.3 to 5.6 device bytes a byte. The 64 KiB
puts are behind every store but RustFS on SSDs, by at least 22% to 3.0 times, and behind Ceph and
SeaweedFS on disks; the 1 MiB puts are within a few per cent of the other stores on SSDs, and on one
host behind Ceph and MinIO by at least 6.1% to 40% and ahead of RustFS.

## Tests

| Test | What fails if the feature is reverted |
| --- | --- |
| `shoal-proto` `objects::tests::a_chunks_file_is_its_class` | A class is the fewest units a power of two, capped at the chunk, and a write outgrows only past a holding chunk's class |
| `shoal-core` `store::slice::tests::a_short_chunk_is_a_file_of_its_class` | A whole chunk's file is its class's length on an SSD and a disk, counted at that length, and reads back with zeros past its bytes |
| `shoal-core` `store::slice::tests::a_chunk_is_never_extended_in_place` | A stage of units past a file is refused by name; an apply past a file made smaller is refused with the file as it was |
| `shoal-core` `store::slice::tests::a_reclaimed_class_file_is_counted_out_by_its_length` | A reclaimed chunk is counted out at its file's length |
| `shoal-core` `store::slice::tests::a_fresh_whole_leaves_no_temporary_and_the_pool_keeps_only_its_target` | A placed, staged, aborted or short whole chunk leaves no temporary file, a fresh file is made for it, and the pool keeps no file past its target |
| `shoal-core` `store::slice::tests::a_start_after_each_step_of_a_fresh_whole` | A start removes a temporary file at each step a crash could leave one, keeps a file adopted before its record as the pool's and a recorded one as its stage's, and numbers past every file it found |
| `shoal-core` `store::slice::tests::a_whole_stage_beside_its_journal_flushes_its_device` | Rewritten: a fresh file's stage asks no shared flush, and a pool file's beside a journal elsewhere does |
| `shoal-core` `store::slice::tests::a_start_recovers_what_was_staged` | Rewritten: a discarded whole stage's file goes to a pool short of its target |
| `shoal-core` `erasure::tests::a_chunk_past_its_class_rewrites_the_stripe_whole` | An erasure coded write past a chunk's class is a whole stripe, one inside it a partial write |
| `shoal` `objects` `a_small_object_grown_by_appends_climbs_its_classes` | Appends across classes on copies whose small writes ride their commits read back, and end in files of their class |
| `shoal` `objects` `an_erasure_coded_object_grown_by_appends_climbs_its_classes` | The same on 4+2 |
| `shoal` `objects` `short_stripe_stores_no_padding` | A short erasure coded stripe's files are their classes |
| `shoal` `device_store` `apply_needs_no_space` | A whole stage past a full device takes a file a removed chunk gave back, or is refused `NoSpace` |
| `shoal` `cluster_fixture` `chunk_files_take_their_class_once_wire_16_is_activated` | Below wire 16 a short stripe's files are whole chunks, and past it their class |
| `shoal-core` `objects::lane::tests::a_cluster_node_sizes_files_by_class_only_past_wire_16` | A node of no cluster sizes by class at once; a cluster node's executor does not before its first map, nor below 16, and does at 16 |
| `shoal` `shared_flush` `shared_flush_covers_short_chunks_and_their_growth` (ignored; sudo, `dm-log-writes`) | On XFS and ext4, chunks in files of their class, applied inside it and grown whole into the chunk's class, every acknowledgement there at the flush before it, no file shorter than its header holds, and a slice started on every state with no temporary file left: 128 acknowledgements over 51 states on XFS and 44 on ext4 |
| `shoal` `shared_flush` `shared_flush_covers_every_completed_apply` (ignored) | Now also starts a slice on every replayed state; it held 112 acknowledgements over 37 states on XFS and 28 on ext4 with fresh files |
| `shoal` `cluster_fixture` `rotational_stage_is_acknowledged_from_its_journal_device` | Rewritten: no temporary file is left once a disk's whole stage is acknowledged, where it waited for the pool's refill |
| `shoal-model` `stripe_model` `writes_staged_whole_preserve_acknowledged_bytes` | Writes staged whole, half of them by their op, keep every acknowledged byte at every layout |

X1's search ran every configuration to 2,000 seeds on europa's 32 threads, once at the commit before
F96 and once with it:
- **the 1,432 blocks both ran are the same**, so the old configurations' schedules and digests did
  not move;
- **the `whole` group's ten configurations** found no violation and no stall: writes staged whole
  at three copies, 2+1 and 4+2, alone, grouped, released, and with small writes in their commits.

The records are not committed, as X1's are not.

## Related

- [X15. A short chunk's file](../object-storage/short-chunk-files.md), which chose fresh files.
- [A short stripe writes a whole chunk](../object-storage/store-optimizations/short-stripes.md) and
  [A whole chunk is written twice](../object-storage/store-optimizations/whole-chunk-twice.md).
- [F85. Devices and pools](devices-and-pools.md), [S6. The device store](../object-storage/device-store.md).
- [X6. The device store on SSD](../object-storage/device-store-ssd.md).
