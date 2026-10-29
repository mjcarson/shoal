# F102. A cache of the units reads verified

Since this feature a node may keep the units its object reads verified in memory,
`resources.object_cache` of it across its object executors, and answer a read of the same units
again without its device. ~~It keeps none unless it names a size.~~ Since
[F107](cache-default-share.md) it keeps an eighth of its memory unless it names a size, on a node
that claims devices, and gives the cache's units back before rows. A unit is held by its slice, its
chunk, the label it was read at and its index, and a chunk's bytes at a label never change once
written, so a unit held is never wrong for a reader that names its label.

## Context

The store comparison found every other store answering its repeated gets from memory
([O121](../appendix/optimizations.md#o121-every-get-reads-its-bytes-off-the-device-however-often-the-object-is-read),
[A get reads the device every time](../object-storage/store-optimizations/read-cache.md)):
- **SSDs, 64 KiB.** The other stores read 2 to 16% of the bytes they served off their devices, and
  Shoal 107 to 184%.
- **Disks, 64 KiB.** Shoal's gets at one in flight ran at 130 a second with a p99 of 27 to 45 ms,
  where the others read at most 3 MiB/s off any disk and answered at 1.3 to 2.1 ms.

A slice read a chunk's range with direct I/O for every request, by S6's design: the page cache was
rejected, since object bytes held there are held twice and counted by nobody
([S6](../object-storage/device-store.md#alternatives-rejected)). O121 filed the shape: a cache of chunk
units by chunk and label, on the executor that owns the slice, a read taking from it only bytes at
a label its row names. F91 had filed its narrow form for disks
([a cache of chunks a disk read](../appendix/todos.md#a-cache-of-chunks-a-disk-read)).

O121 filed the cache inside the object budget. The budget is 256 MiB unless set, split over a
node's object executors, and each lab node's copies of the comparison's 1 MiB gets come to 1 GiB.
On 2026-10-22 the user chose a budget of the cache's own.

## What it does

- **The setting.** `resources.object_cache`, a size, carved out of `node_memory` beside
  `object_memory` (`Resources::object_reserved`). The shards share what is left, and a node where
  the cache leaves the rows nothing is refused at its start by name. An inventory names it in the
  `resources` of a node, a group or the deployment.
- **Where it lives.** Each object executor keeps its share in a `UnitCache`
  (`server/objects/store/cache.rs`), held in a `RefCell` on the executor's own thread. Every slice
  the executor owns is given it as it opens (`Slice::set_cache`). The node's cache is shared out by
  the slices each executor owns, so an executor that owns none, a worker beside a disk's, keeps
  none.
- **A reader's read.** A reader's request for a chunk's range (`ChunkRequest::Read`, from this node
  or over the object lane) looks in the cache first. That happens once the slice has said it is
  serving, and before the chunk's file is opened or its header read.
  - **A hit:** for each label the reader asks for, in its order, if every unit the range covers is
    held at that label, the range is cut from them and answered at that label. Its range is drawn
    from the object budget as a device read's is, and it counts in the slice's `read_bytes`, not
    its `read_ios`.
  - **A miss:** the slice reads as before. Every unit `read_chain` verified is put in at the label
    it made, under the chunk's read lock: whole, its overlays laid over, and on a disk out of its
    one span.
- **What passes it by.** These read the device and fill nothing (`serve_as`, reached in its class
  through the executors' gate since [item 299](../appendix/resolved/a-gate-keeps-a-requests-class.md)):
  - a rebuild's, a move's and a scrub's requests (`Background`, `Scrub`);
  - a parity stage's read of its own units;
  - `Slice::read`.

  A deep scrub's `Verify` reads its device directly, as it always did.
- **What drops from it.** Every change a slice makes to a chunk drops the chunk's units at every
  label:
  - a whole chunk renamed over it (`apply_whole`: a put's chunk placed, a whole write applied, a
    rebuild's or a move's whole copy);
  - units written in place (`write_units`: an apply, a fold, a repair);
  - the chunk set aside (`quarantine_locked`);
  - a reclamation (`remove_chunk`).
- **Its bound.** Units are given up for room by a clock: a unit read since the hand last passed is
  spared once.
- **Its figures**, kept for each executor and summed for the node (`ObjectStore::cache_figures`):
  hits, misses, the units filled, evicted and dropped, the bytes held and the bound. The fixture's
  `OBJECTS` verb carries them.
- **On the lab.** `stores-lab.sh ab` takes one side's size as `AB_<side>_CACHE`.

## Design choices

- **A budget of its own**, the user's choice. A cache sized to the object budget would hold the
  comparison's 64 KiB working set and not its 1 MiB one, and a put would have had to push readers'
  units out to draw its buffers. A node names the cache it can afford beside the rows it keeps.
- **Keyed by label, so never stale.** A label's tag is a try's own, a whole chunk never lands over a
  newer one, and an object's id is never registered again once retired. So the bytes held at a
  label are the bytes the device holds at it for as long as the slice makes the label at all. A
  reader one write behind is answered at the label it asks for, as `previous_chain` answers it. A
  retry of a put in flight places its chunk at the put's label again, with the same bytes, and the
  rename drops what was held all the same. Dropping a chunk's units at its changes is for memory, not
  for correctness.
- **Whole units, verified.** A read verifies every unit it reads by its checksum, and the cache
  keeps exactly those. A hit needs neither the device nor a check, and a range inside a unit read
  for another range is a hit.
- **Looked in before the file is opened.** Every read opened its chunk's file and read its 4 KiB
  header before reading a unit, and a hit skips both.
- **On the executor that owns the slice**, so its state is that thread's alone, with no lock, and a
  remote reader's request finds the same cache a local one does.
- **A reader's reads only.** Recovery reads a chunk once, where readers may read it again; filling
  the cache with recovery's reads would push out what readers read.
- **Dropped where the slice already marks a change.** Every change to a chunk's bytes passes through
  four functions under the chunk's write lock, where the chunk's previous state is kept or dropped
  ([F86](replicated-pools.md)), and the cache drops there too.
- **A clock, not a list in order of use.** A hit sets one bit and moves nothing.
- **Off by default.** As with F100's lease and F101's margin, the lab measures it, and the default is
  the user's decision with the figures.

## Alternatives rejected

- **The page cache.** S6 rejected it, and nothing here changes why: object bytes would be held twice
  and counted by nobody.
- **A share of the object budget**, as O121 was filed. No memory would move from the rows, but at
  128 MiB an executor it holds the comparison's 64 KiB working set and not the 1 MiB one, and a
  put's buffers would have had to push units out, tying a reader's cache to a writer's back
  pressure. The user chose a budget of its own.
- **Whole chunks**, F91's narrow form. A 4 MiB chunk on a disk would be held whole for a reader of
  64 KiB of it, where units hold what was read.
- **Headers and open files alone** ([todos](../appendix/todos.md)). They would take the open and the
  header read off a miss, but every miss would still read its units. Both stay open beside this.
- **On the coordinating shard.** A coordinator sees every holder of its stripes, but object work
  never runs on a table shard (S13), and a coordinator's cache would have to follow every holder's
  applies.

## Limitations

- ~~**A working set larger than the cache gains nothing.**~~ **A cache gains the reads it holds,
  in proportion, and a node holds only what it reads**: the gets it coordinates, and the reads
  other nodes ask of its slices. On the lab's disks 256 MiB a node held about half of what europa
  read and most of what titan and hyperion read. It ran 1 MiB gets at one in flight 1.79 times as
  fast, where 2 GiB ran them 3.11 ([On the disk leg](#on-the-disk-leg)). Each miss also costs a
  copy of its units into the cache and a turn of the clock. On an SSD that was not so until
  [F105](store-reads-in-place.md): each unit was read into a buffer of its own, which the cache
  took. A read since answers from the device's buffers, which a cache never holds. On europa's loopback a cache of 4 MiB
  a node held one 1 MiB read in a hundred. Those gets ran within the range of none, 2% slower at
  the median, and their p99 at sixteen in flight rose from 10.3 ms to 11.1
  ([A cache that holds almost nothing](#a-cache-that-holds-almost-nothing)).
- **Its bookkeeping is not counted:** the maps' entries and the clock's places, about a hundred
  bytes a unit, outside the bound.
- **A failed slice's units stay held** until the clock gives them up. A failed slice refuses every
  read before it looks, so they are never served.
- **A hit finds nothing wrong with its device.** A read off the device reports a unit that fails its
  checksum and sets its chunk aside ([F89](scrub-and-repair.md)). A unit that went bad on the device
  after a read verified it is answered from memory, with the bytes it held, until the chunk changes
  or the clock gives it up. Until then only a scrub finds it.
- **A remote read still crosses the object lane.** The cache is the holder's, so a get at 2+1 of a
  chunk on another node is served from that node's memory and then copied over the lane
  ([O104](../appendix/optimizations.md#o104-an-object-lane-copies-a-reads-bytes-twice-and-a-stages-once-more)).
- **One cache for every pool on a node.** No pool can keep its chunks out of it.
- **A read ahead of an apply is kept only until the apply.** A reader of a label a staged record
  makes, before the slice applies it, fills the cache, and the apply drops what it filled with the
  rest of the chunk's units.
- ~~**Not yet measured on the lab.** Its before and after follows.~~ Measured on 2026-10-22
  ([Performance](#performance)).
- **It cannot hurry a get that its network link holds back.** On the lab's SSD and disk legs every
  get another host coordinated crossed the bench driver's 1 GbE link, and at sixteen in flight that
  link was full with or without the cache ([On the SSD leg](#on-the-ssd-leg),
  [On the disk leg](#on-the-disk-leg)). Behind a full link the gets that cross it wait longer. The
  cache answered the gets europa coordinated, on the driver's own host, in a few milliseconds,
  which left more of the sixteen in flight queued on the link: on the disks 1 MiB gets' p99 rose
  from 257 ms to 341.

## Invariants to uphold

- **A unit is put in only once a read verified it, and only at the label the read made.** Never from
  a write's bytes.
- **A hit answers only at a label the reader asked for, with every unit of its range at that one
  label.**
- **Every change a slice makes to a chunk's bytes drops the chunk's units, under the chunk's write
  lock.** A new path that changes a chunk calls `Slice::uncache` beside `drop_previous` or
  `keep_previous`.
- **A background read and a scrub's never take from the cache or fill it**, and a scrub verifies the
  device. Until [item 299](../appendix/resolved/a-gate-keeps-a-requests-class.md) the object executors' gate handed
  `serve` the request unwrapped, so through either dispatch a rebuild's read did both.
- **The slice says it is serving before a hit is taken**, so a failed device never answers from
  memory, and a hit's range is drawn from the object budget as every object buffer is.
- **Only the executor that owns a cache touches it.**

## Performance

Measured on the lab on 2026-10-22 (`f102-ssd-rep3`, `f102-ssd-rep3-patch`, `f102-hdd-rep3`,
`f102-loop-rep3`, `f102-loop-ec21` and `f102-loop-rep3-miss`, under `shoal-spike/results/stores-ab/`).
The legs were:
- the SSD leg with three copies;
- the disk leg with three copies;
- europa's loopback, with three copies and at 2+1.

How it was run:
- **The sides** ran one build, F102's commit (`4dd51f8f`), its node the same binary in every capture:
  - *off*, with no cache;
  - *2 GiB*, a cache of 2 GiB a node. That holds every working set of the comparison's gets, 1,024
    objects of each size.
  - *256 MiB*, a cache of 256 MiB a node, on the SSD and disk legs. That is a quarter of the 1 MiB
    working set.
  - *4 MiB*, a cache of 4 MiB a node, on the loopback with three copies. That holds almost none of
    what a node reads.
- **The arms** are the comparison's gets at 64 KiB and 1 MiB, one in flight and sixteen. On the SSD
  leg its puts at both sizes ran beside them, and 1 MiB patches, writes in place, after them.
- **The order of the captures:**
  - off, 2 GiB, 256 MiB, 256 MiB, 2 GiB, off on the SSD and disk legs;
  - off, 2 GiB, 2 GiB, off on the loopback and for the patches;
  - off, 4 MiB, 4 MiB, off for the cache of 4 MiB.

  Each capture ran on devices made fresh.
- **A side's figure** is the median of its eight runs, with the lowest and highest.
- **Nothing was lost.** The check at each capture's end read 2,048 objects back, and 1,024 after
  the patches.

### On europa's loopback

All three nodes are on one host, so no get crosses a network link. Gets a second:

| Group | Arm | In flight | Off | 2 GiB | 2 GiB ÷ off | p50 ms | p99 ms |
| --- | --- | --- | --- | --- | --- | --- | --- |
| Three copies | 64 KiB | 1 | 1,952 [1,928–1,982] | 2,617 [2,574–2,635] | **1.34, ahead** | 0.50 → 0.38 | 0.65 → 0.49 |
| | | 16 | 15,057 [15,006–15,093] | 16,596 [16,385–16,784] | **1.10, ahead** | 1.04 → 0.94 | 1.70 → 1.62 |
| | 1 MiB | 1 | 566 [565–568] | 1,137 [1,124–1,149] | **2.01, ahead** | 1.68 → 0.79 | 2.03 → 1.28 |
| | | 16 | 2,363 [2,348–2,369] | 4,682 [4,565–4,793] | **1.98, ahead** | 6.75 → 3.18 | 10.6 → 6.46 |
| 2+1 | 64 KiB | 1 | 1,694 [1,629–1,700] | 2,192 [2,180–2,199] | **1.29, ahead** | 0.59 → 0.45 | 0.75 → 0.61 |
| | | 16 | 13,434 [13,390–13,518] | 15,324 [15,237–15,427] | **1.14, ahead** | 1.17 → 1.02 | 1.82 → 1.71 |
| | 1 MiB | 1 | 429 [425–433] | 704 [699–706] | **1.64, ahead** | 2.38 → 1.47 | 2.82 → 2.06 |
| | | 16 | 1,776 [1,749–1,808] | 2,361 [2,346–2,376] | **1.33, ahead** | 8.92 → 6.58 | 14.7 → 14.8 |

- **Device reads.** The devices read 1.00 to 1.06 bytes for every byte served with no cache, and
  0.00 to 0.01 with one.
- **Every range is disjoint.**
- **At 2+1 the gain is smaller.** A get there gathers its chunks from two nodes, one of them over
  the object lane, and the cache leaves the lane where it was
  ([O104](../object-storage/store-optimizations/object-lane.md#where-the-executors-time-goes)).

### On the SSD leg

Gets a second. The last column is the bytes the devices read for every byte served, every host's
devices together.

| Arm | In flight | Off | 2 GiB | 2 GiB ÷ off | 256 MiB | 256 MiB ÷ off | Device bytes read a byte, off → 2 GiB → 256 MiB |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 64 KiB | 1 | 596 [531–638] | 744 [647–855] | **1.25, ahead** | 735 [683–794] | **1.23, ahead** | 1.07 → 0.02 → 0.04 |
| | 16 | 3,373 [3,147–3,641] | 3,668 [3,048–4,395] | 1.09, within | 3,527 [3,308–3,818] | 1.05, within | 1.06 → 0.00 → 0.00 |
| 1 MiB | 1 | 128 [99.3–135] | 173 [140–203] | **1.35, ahead** | 152 [141–168] | **1.18, ahead** | 1.01 → 0.04 → 0.33 |
| | 16 | 250 [240–261] | 250 [201–302] | 1.00, within | 233 [219–256] | 0.93, within | 1.00 → 0.00 → 0.29 |

**The ranges are the leadership draw's.**
- **Each fresh deployment draws again which node leads which groups.** A get is coordinated by its
  group's leader, which reads its own copy
  ([O119](../appendix/optimizations.md#o119-a-get-from-a-pool-of-copies-reads-its-coordinators-own-copy-so-each-disk-serves-the-gets-its-node-leads)).
- **Only some gets cross europa's 1 GbE link.** The bench driver is on europa, so a get europa
  coordinates never crosses the link. One that titan or hyperion coordinates does.
- **Each capture's draw is known.** Each member counts the strong reads it coordinates
  ([F99](strong-read-rounds.md)). In the captures with no cache, europa's count matched its share of
  the device reads to within two points.

Each capture's gets a second, the median of its four runs, with europa's share of the gets after
each:

| Capture | 64 KiB, 1 in flight | 64 KiB, 16 | 1 MiB, 1 | 1 MiB, 16 |
| --- | --- | --- | --- | --- |
| Off, first | 586 (51%) | 3,184 (49%) | 125 (53%) | 243 (54%) |
| 2 GiB, first | 701 (46%) | 3,075 (46%) | 151 (46%) | 202 (45%) |
| 256 MiB, first | 792 (60%) | 3,810 (59%) | 162 (56%) | 252 (56%) |
| 256 MiB, second | 733 (52%) | 3,363 (51%) | 144 (49%) | 220 (49%) |
| 2 GiB, second | 853 (67%) | 4,386 (66%) | 201 (63%) | 300 (63%) |
| Off, second | 634 (59%) | 3,638 (58%) | 132 (56%) | 258 (57%) |

What this says:
- **At sixteen in flight the gets are bound by the link.**
  - **The link was full.** It received 118 MB/s in every run at 1 MiB, and 103 to 117 MB/s at
    64 KiB.
  - **The draw sets the 1 MiB rate.** Every capture's rate is the link's 112 or so 1 MiB gets a
    second divided by the share titan and hyperion coordinated, to within 3%: 205 predicted at 45%
    and 202 measured, 304 and 300 at 63%.
  - **No cache moves that.** The sides' ranges at sixteen in flight are their captures' draws.
- **At one in flight the cache takes the device read off each get.** A line through each side's two
  captures, read at a draw of 55%, gives gets a second:

  | Arm | Off | 2 GiB | 256 MiB |
  | --- | --- | --- | --- |
  | 64 KiB, one in flight | 610, a mean of 1.64 ms | 767, 1.31 ms (+26%) | 756, 1.32 ms (+24%) |
  | 64 KiB, sixteen | 3,488 | 3,680 (+6%) | 3,589 (+3%) |
  | 1 MiB, one | 129, 7.70 ms | 178, 5.67 ms (+38%) | 160, 6.20 ms (+24%) |
  | 1 MiB, sixteen | 247 | 257 (+4%) | 248 |

  Two captures that drew alike say the same. Off's second and 256 MiB's first both drew 56 to 60%.
  Beside off, 256 MiB ran 64 KiB gets at one in flight 25% faster, a mean of 1.26 ms against 1.57,
  and 1 MiB gets 23% faster.
- **At one in flight a 1 MiB get's p50 follows the draw.**
  - **Two kinds of get.** One europa coordinates takes about 2 ms, and one across the link about 9.
    The p50 falls in whichever kind is the majority.
  - **With no cache** the captures drew 53% and 56% for europa, and the p50 was 2.37 ms.
  - **With 2 GiB** the first capture drew 46%, and the p50 was 6.03 ms.
  - **The mean** fell from 7.71 ms to 5.79.
- **256 MiB held most of what each node read.** A node reads only the gets it coordinates.
  - **Titan and hyperion** coordinated about a quarter each, and their caches held nearly all of
    their share of the 1 MiB objects.
  - **Europa** coordinated about half, so its cache held about half of its share. Its device served
    70 to 100% of the reads that still went to a device.
- **Puts and patches did not get worse.**
  - **Puts** at 64 KiB and 1 MiB were within on every side, at one in flight and sixteen. Each wrote
    the same device bytes for every byte put as before, 3.12 to 5.05.
  - **1 MiB patches** drop the units of every chunk they write. With 2 GiB they ran at 0.96 and
    0.94 times off's rate at one and sixteen in flight. The ranges overlap and are a third wide,
    as the runs within every capture are.

### On the disk leg

Each host's disk held the copies, its journal on the host's SSD. Gets a second. The last column is
the bytes the disks read for every byte served, every host's disks together.

| Arm | In flight | Off | 2 GiB | 2 GiB ÷ off | 256 MiB | 256 MiB ÷ off | Device bytes read a byte, off → 2 GiB → 256 MiB |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 64 KiB | 1 | 460 [416–467] | 752 [683–759] | **1.63, ahead** | 735 [662–743] | **1.60, ahead** | 1.07 → 0.01 → 0.05 |
| | 16 | 2,785 [2,747–2,811] | 3,298 [3,247–3,424] | **1.18, ahead** | 3,356 [3,293–3,431] | **1.20, ahead** | 1.06 → 0.00 → 0.00 |
| 1 MiB | 1 | 52.7 [49.6–53.5] | 164 [90.3–166] | **3.11, ahead** | 94.4 [73.6–95.8] | **1.79, ahead** | 1.01 → 0.05 → 0.36 |
| | 16 | 190 [189–193] | 229 [223–233] | **1.20, ahead** | 229 [224–236] | **1.20, ahead** | 1.00 → 0.00 → 0.32 |

What this says:
- **Every range is disjoint**, at both sizes and both depths, with either cache.
- **The draw was even.** Europa coordinated 45 to 47% of the gets in the captures with no cache,
  and 50 to 54% in the others. At one in flight each side's two captures ran within 1.4% of each
  other.
- **At one in flight the cache takes a disk's read off each get.**
  - **1 MiB gets** took a mean of 18.9 ms with no cache, 6.0 with 2 GiB and 10.5 with 256 MiB.
    With no cache a get waits on a disk wherever it is coordinated, so the draw hardly moves it.
    Off's captures drew 47% and 45% and ran at 52.7 and 52.5 a second.
  - **64 KiB gets** took a mean of 2.16 ms with no cache and 1.33 with 2 GiB. Their p99 fell from
    4.15 ms to 2.21.
  - **Each arm's first run filled the cache.** 1 MiB gets at one in flight ran at 90 and 95 a
    second in the first run of each 2 GiB capture, and at 163 to 166 in its others. The ranges
    include those runs.
- **256 MiB held about two thirds of the 1 MiB reads.** The disks read 0.36 bytes for every byte
  served at one in flight.
  - **Europa** coordinated about half the gets, over twice what its cache held, so about half of
    its reads missed. Its disk did 77 to 87% of the reads that still went to a disk.
  - **Titan and hyperion** coordinated about a quarter each, and their caches held most of their
    share.
- **At sixteen in flight the link decides the rate, as on the SSD leg.**
  - **The link was full** at 1 MiB, receiving 116 to 118 MB/s in every capture.
  - **Each capture's rate** is the link's 111 or so 1 MiB gets a second divided by the share of
    gets whose bytes crossed it, to within 3%.
  - **With no cache that share was larger than the draw.** Europa's disk read 41 to 42% of the
    bytes, where europa coordinated 45 to 47% of the gets. [F101](disk-read-margin.md)'s margin
    sends a read to another holder once its own disk's queue is more than four deeper, and those
    reads crossed the link too. With a cache europa answered its own gets from memory.
  - **So half of the 1.20 is the draw and half is those reads.** The cache's captures drew five
    points more for europa.
  - **64 KiB gets** ran 1.18 and 1.20 times as fast. The draw's five points account for 11% of
    that. The rest is the link running fuller, receiving 113 to 115 MB/s against 105.
- **The tail at sixteen in flight rose.** With 2 GiB, 1 MiB gets' p50 fell from 77.2 ms to 10.6,
  and their p99 rose from 257 ms to 341, outside off's range. A get europa answered from memory
  took a few milliseconds, which left more of the sixteen in flight queued on the link.

### A cache that holds almost nothing

A cache too small for what its node reads still pays for every miss: the units copied in and a turn
of the clock. Europa's loopback with three copies measured it, with no cache beside 4 MiB a node.
Gets a second:

| Arm | In flight | Off | 4 MiB | 4 MiB ÷ off | Device bytes read a byte | p99 ms |
| --- | --- | --- | --- | --- | --- | --- |
| 64 KiB | 1 | 1,967 [1,964–1,980] | 2,029 [2,024–2,055] | **1.03, ahead** | 1.06 → 0.86 | 0.65 → 0.65 |
| | 16 | 14,954 [14,813–15,116] | 15,074 [15,042–15,273] | 1.01, within | 1.06 → 0.86 | 1.73 → 1.74 |
| 1 MiB | 1 | 570 [562–574] | 561 [552–566] | 0.98, within | 1.00 → 0.99 | 2.02 → 2.11 |
| | 16 | 2,392 [2,337–2,428] | 2,333 [2,311–2,367] | 0.98, within | 1.00 → 0.99 | 10.3 → 11.1, **higher** |

- **At 1 MiB it held one read in a hundred**, and every other read missed. Gets ran within the
  range of none, 2% slower at the median. Their p99 at sixteen in flight rose from 10.3 ms to 11.1,
  outside off's range.
- **At 64 KiB it held about one read in seven**, and gets at one in flight ran 3% faster.
- **Memory did not move.** Each node's peak was 0.37 to 0.40 GiB with no cache and 0.38 to 0.41
  with 4 MiB.
- **This is the leg where a get costs least**, with no link and an Optane under every node, so a
  miss's own cost shows most here.

### Memory

A node holds the units it reads, up to its size. Each node's peak resident memory:

| Leg | Off | 2 GiB | 256 MiB |
| --- | --- | --- | --- |
| SSDs | 1.99 to 2.08 GiB | 2.23 to 2.83 GiB | 2.28 to 2.31 GiB |
| Disks | 1.78 to 1.86 GiB | 2.04 to 2.45 GiB | 2.06 to 2.10 GiB |
| Loopback, three copies | 0.37 to 0.41 GiB | 0.75 to 0.81 GiB | |
| Loopback, 2+1 | 0.39 to 0.41 GiB | 0.76 to 0.80 GiB | |

**No 2 GiB cache filled.** A node read its share of the working sets, 0.2 to 0.8 GiB here.

### The table rows beside it

The user chose a share of node memory as the cache's default, once the rows were measured under the
memory it takes (2026-10-22). Measured on the lab on 2026-10-23 (`f104-ssd-rep3-rows`,
`f104-loop-rep3-probe` and `f104-loop-rep3-rows`, under `shoal-spike/results/stores-ab/`), with
[F104](memory-in-a-capture.md)'s counts.

How it was run:
- **The load.** The comparison's 1 MiB gets at sixteen in flight, which fill a cache. Beside them
  the TMDB `Movie` table's reads ran paced, 500 a second on four workers. The table was preloaded
  whole, 1,188,548 rows, beside the comparison's 1,024 posters of 1 MiB.
- **The sides**, one build (F104's commit, `628c3681`):
  - *off*, no cache;
  - *an eighth*, a cache of an eighth of node memory.

  The order was off, an eighth, an eighth, off, four runs each.
- **The legs**, both with three copies:
  - the SSD leg at 8 GiB a node, where the rows fit with room: a cache of 1 GiB;
  - europa's loopback at 2,304 MiB a node: a cache of 288 MiB. With the rows and no cache each node
    reached 2.09 to 2.11 GiB at 8 GiB (`f104-loop-rep3-probe`), so 2,304 MiB leaves them just
    under it.
- **Nothing was lost.** The check at each capture's end read 1,024 objects back.

Each side's figures, the median of its eight runs with the lowest and highest:

| Leg | Side | Peak resident a node | Partitions evicted past node memory | Rows a node | Paced reads p50 ms | p99 ms | Gets a second |
| --- | --- | --- | --- | --- | --- | --- | --- |
| SSDs, 8 GiB | Off | 3.70 GiB [3.68–3.70] | none | 1.01 GiB | 1.97 [1.27–2.27] | 6.02 [5.29–6.15] | 220 [206–236] |
| | An eighth | 4.26 GiB [4.23–4.29] | none | 1.01 GiB | 2.11 [1.47–2.60] | 6.24 [5.46–14.4] | 239 [218–261] |
| Loopback, 2,304 MiB | Off | 2.11 GiB [2.11–2.12] | none | 1.01 GiB | 1.07 [0.40–1.25] | 2.85 [2.12–3.03] | 2,427 [2,413–2,440] |
| | An eighth | 2.18 GiB [2.17–2.25] | 1,963,529 and 1,914,162, each in its capture's first run, none after | 0.48 GiB after | 1.06 [0.57–1.60] | 3.05 [2.67–6.03] | 4,147 [4,098–4,220] |

No shard evicted a partition for its own budget on either leg: the cache's carve-out took each
loopback node's budget for rows from 1.98 to 1.70 GiB, still above the 1.01 GiB they held.

What this says:
- **Where the rows fit with room, the cache cost them nothing.**
  - On the SSDs each node held the gets it coordinated, about a third of the 1 GiB working set,
    and its resident memory rose by about as much.
  - The paced reads' ranges overlap.
  - The gets moved with the leadership draw, as in [F102's run](#on-the-ssd-leg). The devices read
    0.04 bytes for every byte served, where they read 1.01.
- **Where the rows nearly fill a node's memory, the cache's fill evicts them.**
  - On the loopback each node's cache filled to 288 MiB in the first run of each capture that had
    one, and the process passed the node's memory.
  - Each shard then evicted 40% of its rows at a time, as a pass past the node's memory does
    ([Resolved #149](../appendix/resolved/node-memory-budget.md)). The nodes evicted 1.96 million
    partitions in one capture and 1.91 million in the other, 1.78 and 1.75 GiB of rows as the
    shards count them. That was about half of each node's rows: 0.48 GiB of 1.01 were held after.
  - In that run the paced reads' p99 was 4.68 and 6.03 ms, against 2.12 to 3.03 with no
    cache. After it, with resident memory back under the node's, it was 2.67 to 3.40.
  - The gets ran 1.71 times as fast, the caches answering 84% of them, and their p99 fell
    from 10.7 ms to 8.56.

~~Which share the default takes is the user's to choose with these figures: an eighth, another,
or none.~~ The user chose an eighth, the caches giving units back before any row is evicted
(2026-10-23), which [F107](cache-default-share.md) built. On the loopback's nodes at 2,304 MiB it
evicted no row: the caches gave units back as they filled, keeping 28 to 138 MiB a node of their
288, and the gets ran 1.27 times as fast as with none
([F107's figures](cache-default-share.md#on-europas-loopback-its-rows-nearly-filling-each-node)).

### Beside the comparison's captures

These ran on another day, so they are a guide only.
- **Europa's loopback, three copies.** 1 MiB gets at one and sixteen in flight moved:
  - against Ceph, from behind by at least 29% and 35% to ahead by at least 23% and 20%;
  - against SeaweedFS, from behind by at least 46% and 10% to ahead by at least 1.8% and 71%.
- **Europa's loopback, 2+1.** 1 MiB gets at one and sixteen in flight:
  - are still behind Ceph, by at least 13% and 20% where they were 46% and 39% behind;
  - are still behind MinIO, by at least 24% and 21% where they were 2.1 times and 40% behind;
  - moved against RustFS from behind by at least 21% to ahead by at least 22% at one in flight,
    and from ahead by at least 42% to ahead by at least 91% at sixteen.
- **The SSDs.** 64 KiB gets at one in flight:
  - are behind Ceph by at least 28%, where they were 46% behind;
  - are behind SeaweedFS by at least 20%, where they were 41% behind.
- **The disks.**
  - 64 KiB gets at one in flight are behind Ceph by at least 36% and SeaweedFS by at least 30%,
    where they were 2.5 and 2.3 times behind.
  - 1 MiB gets at one in flight moved against Ceph from behind by at least 32% to ahead by at
    least 7.6%, and against SeaweedFS from behind by at least 6.6% to ahead by at least 54%.

**Taken as verdicts since** (2026-10-25 and 26), with the default eighth of node memory rather than
2 GiB, when [the comparison measured Shoal again](../object-storage/store-comparison.md#shoal-measured-again):
64 KiB gets at one in flight are behind Ceph by at least 28% and SeaweedFS by 20% on the SSDs, and
by 35% and 29% on disks. On europa's loopback 1 MiB gets with three copies are ahead of Ceph by at
least 33% and 52% at one and sixteen in flight, and of SeaweedFS by 10% and 2.1 times; at 2+1 they
are within Ceph, and within MinIO at sixteen in flight.

## Tests

| Test | What fails if the feature is reverted |
| --- | --- |
| `shoal-core` `objects::store::cache::tests::a_range_is_answered_whole_from_one_label_or_not_at_all` | A range is answered only with every unit it covers held at one label asked for, the first in the reader's order, cut at the chunk's end; another label and another slice's copy are never answered |
| `shoal-core` `objects::store::cache::tests::a_changed_chunk_drops_every_label_it_is_held_at` | A chunk's change drops its units at every label, and nothing of another chunk |
| `shoal-core` `objects::store::cache::tests::the_cache_holds_its_bound_and_gives_up_what_was_not_read` | The units held never pass the bound, a unit read since the hand passed is spared, and a cache of no room holds nothing |
| `shoal-core` `objects::store::slice::tests::a_read_of_units_read_before_is_answered_without_the_device` | A reader's second read of a unit reads no device; a range reaching units no read verified goes to the device and is kept; a label the chunk is not at is never answered; a background read neither takes nor fills, asked of `serve` with its class on (item 299's way past the dispatch). With the lookup taken out, *a read of a unit held went to the device*: six reads of it where five were |
| `shoal-core` `objects::store::slice::tests::every_change_to_a_chunk_drops_its_cached_units` | Units applied in place, a whole chunk placed over it, the chunk set aside and a reclamation each drop the chunk's units. With the drop after units applied in place taken out, *an apply in place left the chunk's units*: four |
| `shoal-core` `objects::lane::tests::a_background_read_leaves_the_cache_as_it_was` | Since [item 299](../appendix/resolved/a-gate-keeps-a-requests-class.md): a rebuild's read through its holder's own executor and across the lane takes nothing from the cache and fills nothing, where a reader's read fills it |
| `shoal-core` `conf::tests::` ~~`an_object_cache_is_none_unless_named_and_comes_out_of_node_memory`~~ `an_object_cache_comes_out_of_node_memory` since F107 | No cache unless named; a size parses; it is reserved beside the object budget, and one that leaves the rows nothing is refused |
| `shoal` `cluster_fixture` `a_repeated_get_is_answered_from_its_holders_cache` | Through three nodes, a second get is answered from a holder's cache, and a write in place is read back as written while the holders drop the chunk's units. With the lookup taken out, *the second get was answered by no cache: 0 hits, then 0*; with the drop after units applied in place taken out, *no cache dropped the written chunk's units* |
| `shoaladm` `deploy::render` (extended) | A node's `object_cache` is rendered as named and left out unnamed |

`SHOAL_TEST_OBJECT_CACHE=64MiB` runs the object suites again with every node keeping a cache, as
`SHOAL_TEST_ROTATIONAL=1` runs them on disks: the standalone `objects` rows, and the fixture's rows
of writes in place, rebuilds, moves, scrubs, erasure coded reads and reclamation, whose reads must
never come back stale. Its first run, on 2026-10-22:
- **Standalone.** `objects` and `device_store` all passed, 31 and 17.
- **The fixture.** Of 74 object rows, 70 passed. Three failed only under load, as each had before,
  and are recorded on [Tests That Fail Under Load](../appendix/flaky-tests.md).
- **A row that needs its disks.** `copies_on_disks_are_read_by_queue` failed alone too: *node 1's
  disk served none of the reads node zero's queued*, since node zero's cache answered every read
  it queued. Both of its rows count what each node's disk read, so they keep no cache whatever the
  run asks (`.object_cache(0)`).

## Related

- [O121](../appendix/optimizations.md#o121-every-get-reads-its-bytes-off-the-device-however-often-the-object-is-read) and its page,
  [A get reads the device every time](../object-storage/store-optimizations/read-cache.md).
- [S6. The device store](../object-storage/device-store.md), whose page cache stays rejected;
  [S9. The read path](../object-storage/read-path.md); [S13's memory](../object-storage/isolation.md#memory).
- [F85](devices-and-pools.md), the object budget this sits beside, and
  [F86](replicated-pools.md), the previous state its invalidation sits beside.
