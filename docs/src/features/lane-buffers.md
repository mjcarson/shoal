# F111. The lane's buffers: answers written together, a hit from the cache, a piece where it was read

## Context

[F106](lane-copies.md) took the object lane's copies of a read's bytes off. It left four costs in
the lane's buffers
([O130](../appendix/optimizations.md#o130-a-lanes-buffers-are-fresh-allocations-a-cache-hit-is-copied-to-be-sent-and-answers-go-a-frame-a-call)):
- **Each answer was a frame and a call of its own.** A served lane wrote its answers one `sendmsg` at
  a time, however many were ready together.
- **A read the cache of read units answered was copied** into a buffer of the span's own before it
  was written ([F102](read-unit-cache.md) keeps copies).
- **A whole chunk's piece arrived in a body and was copied again** into the device's aligned buffer
  to be written.
- **Every request body was a fresh allocation**, whose pages the kernel zeroes as it maps them.

This is wave three's third copy to go
([the plan](../object-storage/store-optimizations.md#wave-three)).

## What it does

**A served lane writes every answer ready together in one call** (`write_answers`, `write_batch`).
- The writer takes the first answer it is owed, then every one already waiting, in the order they
  were owed: at most 64 answers, and no more once 4 MiB are gathered (`BATCH_ANSWERS`,
  `BATCH_BYTES`).
- Their frames go out as one vectored write: each frame's header, its head, then a read's bytes
  where its store left them. The fork's write takes 1,024 slices a call and is called again for any
  more (`codec::write_slices`).
- Each answer's room, draw and place among the answers owed go once the call is written.
- A lane's figures count the answers and the calls (`answers_written`, `answer_writes`).

**A hit from the cache of read units is slices of the units it holds** (`CachedRange`).
- The cache keeps each unit as `bytes::Bytes`, and a hit is a slice of each unit the range covers,
  never a copy.
- A served lane writes the slices as they are.
- A local reader's hit inside one unit crosses to its shard as that one slice. One across units is
  copied once, as before, since a reply carries one slice.
- A change to a chunk drops the cache's units and leaves a reader's slices as they were.

**A whole chunk's piece is written from the buffer its lane read it into** (`read_request`,
`WholeWriter::piece_aligned`).
- The served lane reads a request's head first. For a piece of a whole chunk it reads the piece's
  fixed fields and its checksums, which say how long its units are.
- It reads the units straight into a buffer from its executor's DMA allocator, the ring's
  registered memory or the heap past it.
- The piece is decoded without its units (`ChunkRequest::decode_piece_head`), which must be exactly
  the bytes its checksums cover.
- The stream's task writes the buffer as it is. A lane's figures count the pieces read so
  (`aligned_pieces`).
- A piece of this node's own put still arrives as shared bytes and is copied, since its bytes are
  every holder's and aligned for none.

**Every other request body is read into the lane's arena** (`RequestBody::read_from_arena`,
`read_rest_from_arena`).
- The arena is a `BytesMut` a lane keeps, at least a frame long (`ARENA_BYTES`, 1 MiB).
- Each body is split off it whole. Once every body of an allocation is dropped, the arena takes
  that allocation back, whose pages are mapped already.
- A body is handed out only once a read filled every byte of it, as F25's rule is, and a read that
  fails leaves the arena holding nothing.

## Design choices

- **A batch is what is ready, never what may come.** The writer never waits to gather more, so an
  answer alone is written at once and the batch only gathers what a burst left waiting.
- **The batch has two bounds.** 64 answers keeps a call under the vectored write's slices. 4 MiB
  keeps a batch behind a slow peer from holding every answer's room until its last byte is written.
- **A piece's units are read apart only on a served lane.** That is where they cross the wire, and
  where a buffer of the device's can be filled by the read itself; a local put's pieces come as one
  shared copy every holder writes.
- **The piece's decode is the body's.** `decode_piece_head` runs every check `decode_shared` does
  and refuses anything but a piece. A body that cannot be right ends the lane as before.
- **The arena reclaims, never frees early.** `BytesMut::reserve` takes an allocation back only once
  nothing else holds any of it, so a body a request still holds keeps its bytes.

## Alternatives rejected

- **Waiting a moment to gather answers** (a Nagle of our own). It would add the wait to every lone
  answer, where gathering what is already ready costs none.
- **A ring of answer buffers kept per lane.** The bytes of an answer are its store's buffers already
  (F105, F106); only the heads would be kept, a few dozen bytes each.
- **The cache holding the device's buffers.** A device's buffer is drawn from the executor's
  registered memory and belongs to its read; the cache copies once at its fill, as before.
- **Reading every body into an aligned buffer.** Only a piece's units are written to a device as
  they are; a stage of units goes to the journal through its own buffers.

## Limitations

- **The client relay and the asking link still write a frame a call**
  ([O134](../appendix/optimizations.md#o134-a-client-relay-and-an-asking-link-write-a-frame-a-call)).
- **A batch holds its answers' rooms until the whole call is written**, so one slow peer holds up
  to 4 MiB of answers rather than one.
- **The arena takes an allocation back only once it runs out of room** and nothing else holds any
  of it. A request that keeps a slice of its body, as a stage of units does until its journal write,
  keeps its allocation from the arena that long.
- **A cache's unit can outlive its eviction** while a reader holds a slice of it; the cache counts it
  out of its bound when it lets go of it.
- **An answer body that crosses to a shard is still a fresh allocation.** Lab 3b's profile cannot
  say what that costs. The kernel zeroing the pages it allocates (`kernel_init_pages`, as Ubuntu's
  `init_on_alloc` does) was 7.0% of the object executors' samples and 10.8% of the shards', for the
  sockets' buffers and the process's fresh memory alike. The profile kept no call stacks to tell
  them apart ([O135](../appendix/optimizations.md#o135-pages-zeroed-as-they-are-allocated-are-a-tenth-of-a-get-path-threads-samples)).
- ~~**Not yet measured.** Lab 3b takes F109 to F111 before and after on the loop leg.~~ Measured in
  lab 3b ([Performance](#performance)).

## Invariants to uphold

- **A served lane has one writer, which writes answers in the order they were owed.** A batch is a
  run of them, never a reordering.
- **Every answer's room, draw and place among the answers owed go only once its bytes are written**,
  and an answer that cannot be framed or written ends the lane (Resolved #307), batch or not.
- **A piece's units are read only once its head said how long they are**, and only that many: a
  frame's bytes are never read past.
- **A body is handed out only once a read filled every byte of it**, from the arena as from a fresh
  allocation.
- **A cached unit's bytes never change while any slice of them is held.**

## Performance

Lab 3b measured it on 2026-10-25 on europa's loopback, every node on europa's Optane and every host at
kernel 7.0.0-34. Its sides were cumulative, from the build before F109 (`ceb2f640`) to F112, in the
order before, F109, F110, F111, F112, F112, F111, F110, F109, before, four runs a capture: gets and
puts of 64 KiB and 1 MiB at one and sixteen in flight, at 2+1 (`w3-loop-ec21`) and with three copies
(`w3-loop-rep3`). Every task's cpu was sampled each second (`pidstat`, read by `stores-ab-cpu.py`),
and every node was profiled through one arm, a 1 MiB get at sixteen in flight at 2+1 (`perf`, read
by `stores-ab-perf.py`; `shoal-spike/results/stores-ab/w3-perf.txt`).

F111 was to work if the object executors' cpu a GiB fell again, the puts were no slower and the
gets at sixteen in flight no worse ([the plan](../object-storage/store-optimizations.md#wave-three)).
**It did.** F111 beside F110:
- **At 2+1, the 1 MiB gets at sixteen in flight ran 1.11 times as fast**, every run apart (2,885 →
  3,194 a second). The executors' cpu a GiB was 0.92 of F110's for them, apart, and 0.94 for the
  1 MiB puts at sixteen in flight, the ranges overlapping. Every other rate was within.
- **With three copies, the 1 MiB puts ran 1.02 and 1.04 times as fast**, every run apart, the
  executors' cpu a GiB 0.89 at sixteen in flight, apart. The 64 KiB gets, answered from the cache as
  slices, cost the executors 0.84 and 0.83 of the cpu a GiB, apart (0.44 → 0.37 cpu-seconds at one
  in flight, below F109's 0.41), and every get's rate was within.
- **The executors' `memmove` fell from 12.9% of their samples to 4.8%.** Their largest costs left are
  the kTLS encryption, 24%, and TCP over loopback, 23%.

**F109 to F111 together, beside the build before F109, on loopback:**
- the 1 MiB gets ran 1.13 and 1.39 times as fast at 2+1, at one and sixteen in flight, and 1.07 and
  1.36 with three copies;
- the nodes' cpu a GiB of those gets at sixteen in flight was 0.69 of the build before's at 2+1
  (2.59 → 1.78 cpu-seconds) and 0.74 with three copies;
- the 1 MiB puts at sixteen in flight ran 1.03 and 1.04 times as fast;
- every 64 KiB arm was within.

On the SSD leg every rate was within the build before's, and so was every p99 but the 1 MiB puts' at
sixteen in flight ([F109's figures](reads-from-stripe-buffers.md#performance)).

## Tests

| Test | What fails if the feature is reverted |
| --- | --- |
| `shoal-core` `server::objects::lane::tests::answers_ready_together_go_in_one_write_within_a_byte_bound` | Forty answers ready together are not written in three calls of at most 4 MiB, or one is answered with other bytes, or one keeps its place among the answers owed after it is written |
| `shoal-core` `server::objects::lane::tests::a_whole_chunk_across_the_lane_is_the_file_its_own_store_writes` | A piece's units across the lane are copied rather than read into the device's buffer, or the chunk's file differs byte for byte from the one its own store writes through its copying path |
| `shoal-core` `server::objects::store::cache::tests::a_hit_is_slices_of_the_units_held` | A hit is a copy of the units held, or a change to the chunk changes the bytes a reader holds |
| `shoal-core` `server::request_body::tests::an_arena_gives_a_body_the_room_its_last_one_gave_back` | A body's room is not given to the next once every holder dropped it, or a body still held loses its bytes, or a body past the arena's room is not read whole |
| `shoal-core` `server::request_body::tests::an_arena_body_cut_short_leaves_the_arena_empty` | A body whose stream ended early is handed out, or leaves bytes no read filled in its arena |
| `shoal-core` `server::objects::lane::tests` (every row) | A lane's requests, answers, stalls and ends behave otherwise with its reads and its writes batched |
| `a_repeated_get_is_answered_from_its_holders_cache` (fixture) | A get answered from a holder's cache across a lane does not read back what was written |

## Related

- [O130](../appendix/optimizations.md#o130-a-lanes-buffers-are-fresh-allocations-a-cache-hit-is-copied-to-be-sent-and-answers-go-a-frame-a-call),
  the finding it builds.
- [F106](lane-copies.md): the lane's copies of a read's bytes, taken before this.
- [F102](read-unit-cache.md), the cache of read units.
- [F25](read-buffers-are-filled-not-zeroed.md): a body read unzeroed, whose rule the arena keeps.
- [Resolved #307](../appendix/resolved/a-lane-whose-writer-stops-ends.md): a lane whose answer cannot
  be written ends.
