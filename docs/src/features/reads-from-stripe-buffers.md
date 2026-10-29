# F109. A get answered from its stripes' buffers

## Context

[F105](store-reads-in-place.md) and [F106](lane-copies.md) took a read's copies off the object
executors and the lane: a holder answers from the buffers its device read into, and an asker keeps
the bytes in the body of the frame its link read. What was left of a get's copies was on the table
shard that coordinated it
([O128](../appendix/optimizations.md#o128-a-coordinator-copies-a-reads-range-twice-before-it-answers-and-checks-it-again)).
Lab 2's profile put them at about a quarter of the shards' samples in 2+1 gets of 1 MiB at sixteen
in flight, the largest cost left in such a get
([where the executor's time goes since](../object-storage/store-optimizations/object-lane.md#where-the-executors-time-goes-since)).

Mapping the read path for this found three copies in an erasure coded get, where O128 had argued
two:
- **`read_parts`** laid each data chunk's part into a `Vec` of its own
  (`shard/objects/erasure.rs`);
- **`read_stripes`** appended each stripe to the range, and wrote a hole's zeros into it
  (`shard/objects/read.rs`);
- **`answer_reply`** copied the range behind the answer's head, the one buffer the client relay
  framed every frame from (`shard/objects/mod.rs`).

A pool of copies skipped the first. Then the read took the range's CRC-64/NVME in one call over up
to 4 MiB, with no yield, on a shard that serves table queries beside it.

This is wave three's first copy to go
([the plan](../object-storage/store-optimizations.md#wave-three)).

## What it does

**A read's range is the parts it was read in** (`RangeParts`, `server/objects/range.rs`). Each part
is one of two things:
- **A holder's answer where it left it** (`ChunkBytes`): a buffer of its own from a slice on this
  node, or the body of the frame a lane read from another node's holder.
- **A run of zeros**, by its length: a hole, or bytes a floor hides.

How a range is gathered:
- **A pool of copies' stripe is one part**: its holder's answer, its pending bytes laid over it
  first, as before.
- **An erasure coded stripe is its data chunks' parts**, each moved out of what `gather` brought
  back, a decoded one included.
- **`read_stripes` appends each stripe's parts**, and a hole or what a floor hides as a run of zeros.
  Two runs side by side are one.

**The checksum is folded from the parts**, still on the coordinating shard:
- each part's bytes are checksummed 64 KiB at a time (`STEP`), the executor offered back between
  steps, as an object executor's loops do;
- a run of zeros is folded in by its length alone, with no pass (`checksum::zeros`);
- the parts' checksums are folded by Shoal's combine, which is exact (`Chain`).

**The answer carries its range as parts** (`Reply::range`):
- `ReplyKind::Object` no longer counts a head's bytes, since its archive is now only the answer's
  head and tail.
- The client relay writes that archive as the opener.
- Each data frame is its preamble and the slices of the parts the frame covers, in one vectored
  write. A run of zeros is slices of one static block of a mebibyte, never written, so its pages
  are the kernel's zero page.

**The backup's object reader hands its archive the parts.** The archive writes them a part at a
time on its blocking pool, a hole from the same block.

**A stripe answers exactly what was asked of it, or the read fails by name.** Before, a short
answer would have moved every later stripe's bytes, under a checksum taken of them as they came.
No holder answers short: each pads past its chunk's bytes to the length asked. So this is a check
of what was relied on, and no behaviour changes.

**A write that reads a chunk's units before staging them** (F96's whole stage, a unit's old value)
takes a stripe's bytes in one piece (`RangeParts::into_contiguous`). A pool of copies answers one
part, which is moved out, never copied.

## Design choices

- **The parts ride beside the head, not inside the reply's kind.** `ReplyKind` is `Copy`, and every
  relay matches on it. A field every reply carries, empty for all but an object read's, keeps that
  match as it was and adds no allocation to an empty one.
- **The zero block is a default data frame long**, so a frame of a hole is one slice. It is a
  `static`, never written.
- **A window over the parts is a plain iterator of byte slices** (`RangeParts::window`). The relay
  maps it to `IoSlice`s, the archive writes it, and a test concatenates it. A frame's slices are
  gathered into a `Vec` per frame, a few at most against a mebibyte written.
- **The checksum keeps one running digest inside a part and the combine across parts.** That is a
  multiplication per part, not one per step.
- **`gather`'s contract is unchanged.** It answers every position with exactly the range's bytes, a
  position past its share as allocated zeros, because a write's and a rebuild's decodes need them
  contiguous. A read moves what it brought back and copies none of it.
- **`RangeParts` lives beside the object executors' types** (`server/objects/`). The coordinator,
  the messages, the client relay and the archive writer all name it, and `shard::objects` is
  private to the shard.

## Alternatives rejected

- **Holders answering their range's checksum, so the coordinator takes none.** A holder checksums
  every unit it reads as it verifies it. It could make the range's checksum from those, and from the
  pieces at the range's edges where it starts or ends inside a unit. That would take the
  coordinator's last pass. But it changes the lane's answer, which needs a wire version activated as
  F96's classes were. Filed as
  [O132](../appendix/optimizations.md#o132-a-coordinator-checksums-every-byte-of-a-reads-range-its-holders-checked-already),
  for lab 3b's profile to decide.
- **Checksumming the frames as they are written.** The answer's head carries the checksum and is
  written first, so it has to be known before any data frame.
- **The parts as `bytes::Bytes`.** A part has one owner, the reply, so a reference count buys
  nothing. And `ChunkBytes` is what the lane and the store hand over already.
- **A buffer the coordinator owns, which holders write into.** The bytes arrive on object executors
  and in lanes' frame bodies. A buffer of the coordinator's would be filled by a copy across
  executors, the cost this removes.
- **Zeros as an allocated buffer, as before.** That is an allocation, a pass to zero it and a pass
  to checksum it, for bytes whose checksum is known from their length.

## Limitations

- **The coordinator still reads every byte once,** to checksum it
  ([O132](../appendix/optimizations.md#o132-a-coordinator-checksums-every-byte-of-a-reads-range-its-holders-checked-already)).
- **The client copies every object answer twice:** its frames' buffer into a `Vec`, then that into
  the range a caller is given
  ([O133](../appendix/optimizations.md#o133-a-client-copies-every-object-answer-out-of-the-buffer-its-frames-filled)).
- **These still copy, as they did:**
  - an inline object's range, from its entry: at most the pool's inline threshold;
  - a degraded read's sources, into the decoder's own buffers;
  - a position past its stripe's share, as allocated zeros;
  - a short share's padding, which on a frame's body may reallocate it.
- **An answer holds its stripes' buffers until its last frame is written**, a lane's frame bodies
  among them. That is the same bytes the copied range held, kept in a few buffers instead of one.
- ~~**Not yet measured.** Lab 3b takes F109 to F111 before and after on the loop leg (below).~~
  Measured in lab 3b ([Performance](#performance)).

## Invariants to uphold

- **A range is its parts in order, and nothing else.** No part is empty and no two runs of zeros are
  adjacent, so a window names the same bytes whichever parts it spans.
- **A part is never changed once it is in a range.** Pending bytes are laid over a holder's answer
  before it becomes a part, while the read holds it alone.
- **A stripe answers exactly what was asked of it, or the read fails.** Never append an answer of
  another length.
- **The server streams an object answer exactly when the client expects data frames.** The relay
  streams one whose range is not empty, and the client opens a stream for an answer whose `length`
  is above zero; `length` is the range's length. A rule that let them differ would leave a client
  waiting for frames that never come.
- **The checksum is the parts', taken before the head is written**, since the head carries it.
- **A reply is `Send`.** `ServerMsg` asserts `Send` with an `unsafe impl` the compiler does not
  check (item 133's class), so `what_a_query_carries_between_shards_is_send` holds `Reply` to it. A
  part that held an `Rc` would fail that test, not run.
- **The zero block is never written.**

## Performance

Lab 3b measured it on 2026-10-25 on europa's loopback, every node on europa's Optane and every host at
kernel 7.0.0-34. Its sides were cumulative, from the build before F109 (`ceb2f640`) to F112, in the
order before, F109, F110, F111, F112, F112, F111, F110, F109, before, four runs a capture: gets and
puts of 64 KiB and 1 MiB at one and sixteen in flight, at 2+1 (`w3-loop-ec21`) and with three copies
(`w3-loop-rep3`). Every task's cpu was sampled each second (`pidstat`, read by `stores-ab-cpu.py`),
and every node was profiled through one arm, a 1 MiB get at sixteen in flight at 2+1 (`perf`, read
by `stores-ab-perf.py`; `shoal-spike/results/stores-ab/w3-perf.txt`).

F109 was to work if each table shard's cpu a GiB fell, the 2+1 gets were no slower, and three
copies' gets at sixteen in flight were no worse ([the plan](../object-storage/store-optimizations.md#wave-three)).
**It did, and the 1 MiB gets ran faster.** F109 beside the build before it, the rate's ratio and the
table shards' cpu a GiB as a share of the build before's:

| Arm | 2+1 | Three copies |
| --- | --- | --- |
| get 1 MiB, 1 in flight | 1.09 times as fast, apart; shards 0.89, the ranges overlapping | 1.06, apart; shards 0.94, overlapping |
| get 1 MiB, 16 in flight | 1.22, apart (2,297 → 2,800 a second); shards 0.67, apart | 1.37, apart (4,589 → 6,300); shards 0.69, apart |
| get 64 KiB, 1 and 16 in flight | 1.00 and 1.01, within; shards 0.99 and 0.98 | 1.00, within; shards 0.99 and 0.98 |
| put, every size and depth | 1.00, within | 0.99 to 1.00, within |

- **The nodes' whole cpu a GiB** of a 1 MiB get at sixteen in flight fell from 2.59 cpu-seconds to
  1.93 at 2+1, and from 1.66 to 1.16 with three copies.
- **The tail fell with it.** The 1 MiB gets' p99 at sixteen in flight went from 15.9 to 13.6 ms at
  2+1 and from 6.72 to 4.65 with three copies, every run apart.
- **The copies are gone from the profile.** libc's `memmove` was 23.2% of the table shards' samples
  before F109 and 2.0% with it.
- **The checksum's pass is what is left**: 2.8% of the shards' samples with F109, and 4.5% once
  F111 had taken the lanes' copies too. That is about 0.04 cpu-seconds a GiB of that get, which does
  not pay for the wire change that would take it off ([O132](../appendix/optimizations.md#o132-a-coordinator-checksums-every-byte-of-a-reads-range-its-holders-checked-already)).

**On the SSD leg at 2+1** (`w3-ssd-ec21-w3b`, the build before F109 beside F111, two captures a
side), where europa's link bounds the gets at sixteen in flight, the check was that nothing got
worse. Every rate was within the build before's, at 0.98 to 1.09, and every p99 but one. The 1 MiB
puts at sixteen in flight had a p99 of 372 ms where it was 344, every run apart, at the same rate,
and no kind of connection resent more. Lab 3c ran F111 again beside F113, and its two captures had 370
to 385 ms, so it repeated. Which of F109 to F111 it belongs to is not known: the SSD leg ran them
together, and on loopback the same puts' p99 fell a little at each ([O137](../appendix/optimizations.md#o137-a-1-mib-puts-p99-at-sixteen-in-flight-across-hosts-is-8-longer-with-f109-to-f111)).

## Tests

| Test | What fails if the feature is reverted |
| --- | --- |
| `shoal-proto` `shared::checksum::tests::the_checksum_of_zeros_needs_no_pass` | A run of zeros' checksum, made from its length, is not one call's over the zeros, or does not fold into a chain |
| `shoal-core` `server::objects::range::tests::a_window_is_the_bytes_the_range_stands_for` | A data frame written from the parts is not byte for byte the frame the copied range gave, at any frame size: bytes from a slice and from lanes, a hole past a zero block, an overlaid stripe, a short last stripe's zeros; or an empty part or two adjacent runs are kept |
| `shoal-core` `server::objects::range::tests::a_ranges_checksum_is_its_bytes_checksum` | The checksum folded from the parts, a step at a time, is not the checksum of the bytes they stand for |
| `shoal-core` `server::objects::range::tests::one_part_is_moved_out_whole` | A write's stripe of one part is copied rather than moved |
| `shoal-core` `server::messages::tests::what_a_query_carries_between_shards_is_send` | A reply, with a range on it, is not `Send` |
| `shoal-client` `client::objects::tests::a_read_whose_bytes_do_not_match_its_checksum_is_refused` | A read whose bytes do not hold its answer's checksum or length is handed back; untested before |
| `a_hole_reads_as_zeros` (`shoal/tests/objects.rs`) | One read from a stripe's bytes into a hole does not read back as written |
| `a_striped_object_round_trips`, `an_erasure_coded_object_round_trips`, `short_stripe_stores_no_padding`, `range_read_touches_only_the_chunks_it_needs` (`shoal/tests/objects.rs`) | Ranges across stripes, across a data chunk's end, of a short last stripe, or of whole stripes do not read back as written |
| `read_overlays_pending_bytes`, `degraded_read_decodes_only_what_it_needs`, `a_striped_object_round_trips_across_nodes`, `an_erasure_coded_object_round_trips_across_nodes` (fixture) | An overlaid stripe, a decoded part, or parts in lanes' frame bodies do not read back as written |
| `objects_survive_a_backup_and_restore_by_archive` (fixture) | The backup's archive does not hold the parts' bytes |

## Related

- [O128](../appendix/optimizations.md#o128-a-coordinator-copies-a-reads-range-twice-before-it-answers-and-checks-it-again),
  the finding it builds.
- [F105](store-reads-in-place.md) and [F106](lane-copies.md): the store's and the lane's copies,
  taken before this.
- [F80](object-wire.md), the object frames, and [F73](bodies-across-frames.md), the streams they
  ride.
- [F90](erasure-coded-pools.md): `gather`, whose contract this leaves alone.
- [Resolved #133](../appendix/resolved/read-plan-rc-across-shards.md): why a reply's `Send` is
  checked.
