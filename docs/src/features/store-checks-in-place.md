# F110. The store's checks in place, and a local read handed over whole

## Context

[F105](store-reads-in-place.md) answered a read from the buffers its device put the bytes in. It
left the copies its own page listed
([O129](../appendix/optimizations.md#o129-a-local-read-still-copies-its-range-once-to-cross-to-its-shard-and-scrubs-and-records-copy-theirs)):
- **A local read copied its range once to cross to its shard** (`Span::into_vec`). A glommio
  buffer is `!Send`, so the bytes a shard on another thread was handed were a copy, made on the
  object executor. This is every read of a stripe that a holder on the coordinator's own node
  answers.
- **A scrub's units and a verification's** were copied out of glommio's buffers by `read_exact`
  before their checksums were taken.
- **A chunk's header was copied twice to read it**: once by `read_exact`, and once more to zero its
  own checksum's field before checking it.
- **A staged record's units were copied twice on their way back:** `read_exact` copied them out of
  the journal's device buffers, then each was copied into a buffer of its own. An apply copied each
  a third time, into a fresh DMA buffer, to write it.

Lab 2's profile, after F105 and F106, put the object executors' copies in user space at 45 samples
a GiB in 2+1 gets, mostly the local read's crossing
([where the executor's time goes since](../object-storage/store-optimizations/object-lane.md#where-the-executors-time-goes-since)).
This is wave three's second copy to go
([the plan](../object-storage/store-optimizations.md#wave-three)).

## What it does

**A local read hands its shard the buffer it filled.**
- A read whose answer crosses to a shard is the store's `serve`, as against a served lane's
  `answer`. It reads its run of units in one read into a buffer of the heap's (`read_run`).
- It verifies each unit there, as F105's reads do.
- It answers with that buffer as `ChunkBytes::Shared`: a `bytes::Bytes` whose owner holds the
  buffer, the range of it the read answers, and the read's draw on its executor's budget.
- The shard keeps the bytes as a part of its read's range ([F109](reads-from-stripe-buffers.md)),
  and the client relay writes them from there. **The draw is given back when the shard drops
  them**, after the client was written to, where before it went back once the executor's copy was
  made.

A read answered on its executor, a served lane's, keeps F105's pieces in the device's buffers,
which [F106](lane-copies.md)'s vectored write sends.

**The store's checks read where the device put the bytes** (`read_in_place`, one aligned read
each):
- a scrub's units (`verify_inner`) and a verification's (`verifies`), still a unit at a time;
- five chunk header reads and the journal's segment head.

A header's own checksum is fed its block as three slices, around eight zero bytes where the
checksum's field is, so the block is never copied to zero it.

**A record's units stay in the journal device's buffers** (`RecordUnits`):
- the read chain verifies them and lays a range from them there;
- an apply writes each unit back to its chunk from the buffer it was read and verified in
  (`write_in_place`), through the fork's `From<ReadResult> for DmaBuffer`.

**Bytes another owns are copied before they change.** `ChunkBytes::Shared` gives its bytes for
change (`DerefMut`, `resize`) only once they are copied into a buffer of their own. Only a read of a
pool of copies does that, laying a pending write over a holder's answer.

**The glommio fork gained two things** (`../glommio`, branch `ZeroCopyDmaStreamWriter`, commit
`9dc4717`):
- **`DmaFile::read_into_at(buf, pos)`** reads into a caller's buffer and gives it back, cut to what
  was read:
  - the test hook is asked first, as every read's is;
  - it is never shared with a scheduled read of an overlapping range;
  - a future dropped while the kernel writes leaves the buffer alive in its source until the
    completion is consumed;
  - a short read gives the buffer back shorter.
- **`OwnedDmaBuffer`**, a buffer that is always the heap's, aligned for Direct I/O, and `Send`.

## Design choices

- **Whether the answer crosses is the serving path's to say.** The store's `serve` answers a
  request whose reply goes to another thread, and `answer` one a lane writes on its executor. The
  read under them takes a flag (`crosses`), so a lane's read keeps the ring's registered buffers.
- **One read of the run into one buffer.** A `Bytes` is one slice, and so is everything that reads a
  `ChunkBytes`. The run is the range's units, from the first to the last the chunk's file gives, at
  most a read's range and two units. A device takes it as one read, which a disk wants
  ([F91](rotational-devices.md)).
- **The draw crosses with the bytes.** An executor's budget counts what its reads hold, and a local
  read now holds its buffer until the shard is done with it, so the draw rides inside the bytes'
  owner.
- **A check reads a unit at a time, as before.** A scrub's draw and its device's queue depth do not
  move (`scrub_stays_inside_its_byte_budget`), and the copy is what goes.
- **A copy that is kept stays a copy.** A kept previous state (`read_previous_units`) and a cache's
  units ([F102](read-unit-cache.md)) outlive the read, so they are copied as before.

## Alternatives rejected

- **Lending the ring's registered buffers to the shard.** They belong to the executor's ring, a
  bounded pool its reads draw on, and a shard on another thread would free them there.
- **An answer of several pieces** (`ChunkBytes` holding a list). Every reader of `ChunkBytes` reads
  it as one slice, the lane's encoders and the coordinator's checks among them.
- **Pieces read into the parts of one buffer.** That is several reads holding one buffer at once,
  which one owner of it cannot lend, and a read into part of a buffer the fork does not have.
- **Copying on the shard instead of the executor.** The same copy, on a thread that serves tables
  beside it.

## Limitations

- **A read whose answer crosses no longer uses the ring's registered buffers.** The kernel pins a
  heap buffer's pages for each read, where a registered buffer is pinned once (`read_fixed`). ~~Lab 3b
  weighs it against the copy.~~ Lab 3b found them about even: the object executors' cpu a GiB was
  0.97 to 1.08 of F109's. With three copies, every get local, a 64 KiB get at one in flight cost them
  0.44 cpu-seconds a GiB where it cost 0.41 ([O136](../appendix/optimizations.md#o136-a-local-read-into-a-heap-buffer-forgoes-the-rings-registered-buffers)).
- **A local read's draw is held until its client is written to.** The budget counts more of what a
  node holds under load, and a budget that sheds sheds sooner.
- **A cache's fill still copies the units it keeps**, since it outlives the read
  ([O130](../appendix/optimizations.md#o130-a-lanes-buffers-are-fresh-allocations-a-cache-hit-is-copied-to-be-sent-and-answers-go-a-frame-a-call)).
- **A range a record or a kept previous state gives any part of is still laid out once**, in a
  buffer of its own, as F105's was.
- **The journal's read of a whole segment at a start, and a kept previous state's units, keep
  their copies.**
- ~~**Not yet measured.** Lab 3b takes F109 to F111 before and after on the loop leg.~~ Measured in
  lab 3b ([Performance](#performance)), where it moved nothing.

## Invariants to uphold

- **A buffer handed to another thread is always the heap's** (`OwnedDmaBuffer`). It is never a
  ring's registered buffer or a shared read's result, which belong to their executor. Its `Send` is
  an `unsafe impl` that holds only for storage of its own.
- **Bytes another owns are never changed where they are.** `ChunkBytes::Shared` is copied before
  any change, and its owner's buffer is read only.
- **A read's draw lives as long as its bytes**, wherever they went.
- **An apply writes a record's unit only after verifying it**, and from the buffer it verified.
- **A check keeps none of the device's buffer past the check.**

## Performance

Lab 3b measured it on 2026-10-25 on europa's loopback, every node on europa's Optane and every host at
kernel 7.0.0-34. Its sides were cumulative, from the build before F109 (`ceb2f640`) to F112, in the
order before, F109, F110, F111, F112, F112, F111, F110, F109, before, four runs a capture: gets and
puts of 64 KiB and 1 MiB at one and sixteen in flight, at 2+1 (`w3-loop-ec21`) and with three copies
(`w3-loop-rep3`). Every task's cpu was sampled each second (`pidstat`, read by `stores-ab-cpu.py`),
and every node was profiled through one arm, a 1 MiB get at sixteen in flight at 2+1 (`perf`, read
by `stores-ab-perf.py`; `shoal-spike/results/stores-ab/w3-perf.txt`).

F110 was to work if each object executor's cpu a GiB fell, and three copies' gets, every one local,
were no slower ([the plan](../object-storage/store-optimizations.md#wave-three)). **The gets were no
slower, and the executors' cpu did not fall.** F110 beside F109:
- **Every rate within**, at 0.98 to 1.03.
- **At 2+1 the object executors' cpu a GiB was 0.97 to 1.01 of F109's**, every range overlapping.
- **With three copies it was 0.99 to 1.08.** The 64 KiB gets at one in flight were apart at 1.07,
  0.44 cpu-seconds a GiB [0.43–0.45] where they were 0.41 [0.40–0.41]
  ([O136](../appendix/optimizations.md#o136-a-local-read-into-a-heap-buffer-forgoes-the-rings-registered-buffers)); every other range overlapped.
- **The profile hardly moved.** The executors' `memmove` was 13.2% of their samples with F109 and
  12.9% with F110. In a 2+1 get most of the bytes reach an executor over the lane, and the copy F110
  took off a local read was a small part of its work.

So on these arms a local read's heap buffer, read without the ring's registered buffers, cost about
what the copy it saved did. The scrub's, the verification's and the journal's reads were not on
these arms.

## Tests

| Test | What fails if the feature is reverted |
| --- | --- |
| glommio `io::dma_file::test::a_buffer_read_into_crosses_to_another_thread` | A buffer a read filled cannot be read and freed on a thread that is no executor's |
| glommio `io::dma_file::test::a_short_read_into_a_buffer_is_cut_to_it` | A read into a buffer past the file's end is not given back shorter, or empty |
| glommio `io::dma_file::test::a_read_into_a_buffer_outlives_its_dropped_future` | A read into a buffer dropped before or after it was submitted breaks a read after it |
| glommio `io::hook::test::a_hook_fails_and_tears_only_its_directory` | A read into a buffer is not refused by a hook that lost its directory |
| `shoal-core` `server::objects::lane::tests::a_local_reads_bytes_hold_their_draw_until_dropped` | A local read's bytes are copied to cross, or its draw goes back before its caller drops them, or stays after |
| `shoal-core` `server::objects::lane::tests::a_read_across_the_lane_stays_in_its_frame` | The same range through the holder's own executor crosses as a copy, not the buffer it filled |
| `shoal-core` `server::objects::store::chunk::tests::chunk_format_is_frozen` | A header's checksum, fed as three slices, misses a byte flipped before its field, in it or after it, or reads past the block |
| `shoal-core` `server::objects::store::slice::tests` (F105's rows among them) | A range, a laid-over range or a held span reads other bytes, or a record's units apply other bytes |
| `device_store.rs`, under `SHOAL_TEST_ROTATIONAL=1` too | A scrub of a flipped unit, a verification, a staged record's apply or a header read goes wrong on the store as built |
| `objects.rs`, under `SHOAL_TEST_ROTATIONAL=1` too, and the fixture's object rows | An object's bytes, read locally and across lanes, are not what was written |

## Related

- [O129](../appendix/optimizations.md#o129-a-local-read-still-copies-its-range-once-to-cross-to-its-shard-and-scrubs-and-records-copy-theirs),
  the finding it builds.
- [F105](store-reads-in-place.md), whose reads in place this extends to a local read's crossing and
  the store's checks; [F106](lane-copies.md), the lane's half.
- [F109](reads-from-stripe-buffers.md), whose parts the bytes become on the shard.
- [F102](read-unit-cache.md): the cache, whose units are still copies.
