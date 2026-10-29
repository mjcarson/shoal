# F106. A read's bytes cross the object lane uncopied

Since this feature a holder writes a read's answer as its head and the buffers its store read the
bytes into, in one vectored write, and the node that asked keeps the bytes in the buffer its link
read the frame into. Neither end copies them, and the asking end no longer zeroes a buffer before
reading into it. A stage's bytes go the other way the same: one allocation shared by every holder
it is sent to, written as a part of its frame, and taken as a slice of the frame's body where it
arrives. Under it, glommio's streams write every part of a frame in one `sendmsg`, where each part
was a call of its own.

## Context

[F86](replicated-pools.md) gave the object lane the simplest encoding: a holder encoded a read's
answer into a body of its own, copying the bytes, and the asking link copied them out of the body
it read, after zeroing it ([O104](../appendix/optimizations.md#o104-an-object-lane-copies-a-reads-bytes-twice-and-a-stages-once-more)). O104's diagnosis
(2026-10-22) put those two copies at about 8% of a node's busiest thread in 2+1 gets on one host,
and the store's own two at 12%
([where the executor's time goes](../object-storage/store-optimizations/object-lane.md#where-the-executors-time-goes)).
The user chose both copy fixes, the store's first (2026-10-22): [F105](store-reads-in-place.md)
answers a read from the buffers the device put it in, and this writes the answer from them.

Reading the path found a third cost. Every peer frame is written vectored (`codec::write_frame`),
and its comment said a frame's parts went out in one call. glommio's `TcpStream` had no vectored
write, so the trait's default wrote the first part alone each call: a frame was a `send` for each
part, and under kTLS a record for each.

## What it does

- **A vectored write in the glommio fork.** `TcpStream` and `UnixStream` implement
  `poll_write_vectored` as one non-blocking `sendmsg` of at most 1,024 buffers, Linux's
  `UIO_MAXIOV`, and wait for room under the write's timeout as a write of one buffer does. Every
  vectored writer gains it: peer links' frames, the client relays' answers and the object lane.
- **The holder's answer.** A read served on a lane is answered with the span its store read
  (`serve::answer`, `Answered::Bytes`). The lane's writer writes the answer's head and label
  (`ChunkReply::bytes_head`) and then the span's pieces, as one frame. An answer holds its buffers,
  their draw on the budget and the room its request took until the frame is written. A read is
  drawn once, by the store (`served_draws`), where the lane drew it and the store drew it again.
- **The asker's answer.** A link decodes a read's answer where it read it
  (`ChunkReply::decode_owned`): the bytes stay in the frame's body behind their label, a
  `ChunkBytes::Framed`. A read through the holder's own executor answers a buffer of its own,
  `ChunkBytes::Owned`. Either compares by its bytes.
- **No zeroing.** A link reads a frame's body into a buffer it does not zero first
  (`codec::read_body`), on [F25](read-buffers-are-filled-not-zeroed.md)'s argument for a request's body.
- **A stage's bytes, shared.** A piece's and a unit's bytes are `bytes::Bytes`, which shoal-proto
  depends on since F106.
  - A put's pipeline, a whole stage and a rebuild make a piece's bytes once, and every holder's
    request shares them, where each was a copy.
  - A request goes out as parts (`ChunkRequest::encode_parts`), its carried bytes parts of their
    own, written by the link as one vectored frame.
  - The serving lane reads a request's body unzeroed (F25's `RequestBody`) and decodes it in place
    (`ChunkRequest::decode_shared`): a piece's and a unit's bytes are slices of the body. A piece is
    written to its chunk from there; units a slice stages or folds are copied once into buffers of
    their own.
- **The readers** take the bytes where they are: a stripe read appends them to the range it
  answers, and an erasure coded gather decodes from them. A rebuild and a write's read of old units
  copy them where they need a buffer of their own.

## Design choices

- **The bytes stay where each end's read put them.** On the holder they are the device's buffers
  (F105); on the asker, the frame's body. A copy at either end would be the one this removes.
- **One frame, written as a vector.** The head and the pieces go to the socket together, so under
  kTLS the kernel fills records across their boundaries.
- **An answer holds what it uses until it is written.** Its buffers and their draw are the read's,
  and its room is its request's: a peer that stops reading its answers stops its own requests and
  holds no more of the holder's budget than its lane's room. Before F106 an answer gave both back
  once it was queued, so a holder read every request such a peer sent, and kept the encoded answers
  outside its budget.
- **The wire is unchanged.** `bytes_head` and the bytes are the body `encode_body` makes, which
  writes its head with `bytes_head`, and `decode_owned` reads every answer `decode_parts` reads. A
  request's parts, side by side, are its body: both are written by one encoder into a sink, one
  buffer or parts. No version moves, and a node on either side of this change talks to one on the
  other.
- **`bytes` in shoal-proto.** It is no runtime and no channel, and the client and the engine use it
  already. Sharing a buffer among requests is what it is for.

## Alternatives rejected

- **io_uring's send operations for the vectored write.** glommio's add `MSG_ZEROCOPY` to a send,
  which kTLS refuses.
- **Copying a frame's parts into one buffer before the write.** That is the copy the read's
  answer drops, made again for the call's sake.
- **A read's answer as `bytes::Bytes`.** The link reads a frame's body into an aligned buffer, since
  most frames are archives read in place, and an aligned buffer becomes `Bytes` only by a copy.
- **A reply that borrows its frame.** The answer crosses from the link's task to its waiter on a
  channel, so it owns the body rather than borrowing it.

## Limitations

- ~~**The coordinator still copies a read's range twice** before it answers, and checksums it again
  ([O128](../appendix/optimizations.md#o128-a-coordinator-copies-a-reads-range-twice-before-it-answers-and-checks-it-again)).~~
  Since [F109](reads-from-stripe-buffers.md) the coordinator answers from the parts its stripes
  were read in, and checksums them once.
- ~~**A local read still copies its range once** to cross to its shard
  ([O129](../appendix/optimizations.md#o129-a-local-read-still-copies-its-range-once-to-cross-to-its-shard-and-scrubs-and-records-copy-theirs)).~~
  Since [F110](store-checks-in-place.md) it hands its shard the buffer it filled.
- ~~**A frame's body is a fresh allocation**, a served cache hit is copied to be sent, and each answer
  is a frame and a call of its own
  ([O130](../appendix/optimizations.md#o130-a-lanes-buffers-are-fresh-allocations-a-cache-hit-is-copied-to-be-sent-and-answers-go-a-frame-a-call)).~~
  Since [F111](lane-buffers.md) a served lane writes the answers ready together in one call, a cache
  hit as slices of its units and a piece from the buffer it was read into, and reads its other bodies
  into an arena; an answer body that crosses to a shard is still a fresh allocation.
- ~~**A stage's bytes are still copied** for each holder it goes to: the lane's request half
  follows.~~ Shared since the stage half (2026-10-23).
- **Units a slice stages or folds are copied once** out of the frame's body, into the buffers the
  store's records take.
- ~~**Not yet measured on the lab.** Its before and after follows with F105's.~~ Measured with F105
  on 2026-10-23 ([Performance](#performance)).
- ~~**A strong table read across hosts, beside gets that fill a link, has a tail of about 110 ms**
  since the fork's vectored write, where it was about 17: a p99.9 in every run of every build
  with it, its cause not yet known.~~ Fixed the same day. A frame is one segment since the vectored
  write, and the kernel probed a lone segment's loss only after its 200 ms minimum RTO. A node's
  replication and control lanes now set a minimum of 5 ms, and a client's connection holds it
  until it carries an object
  ([Resolved #304](../appendix/resolved/lone-frame-loss-probe.md)).

## Invariants to uphold

- **The wire is unchanged byte for byte**: a read's head and its bytes are `encode_body`'s body,
  `decode_owned` and `decode_parts` read every answer alike, a request's parts are its body, and
  `decode_shared` and `decode` read every request alike.
- **A request's carried bytes are never changed once shared**: every holder's request reads the
  same allocation.
- **A body holds only bytes a read filled**: `read_body` hands out a buffer only once `read_exact`
  filled every byte of it.
- **A served answer holds its buffers, their draw and its room until its frame is written.**
- **No `MSG_ZEROCOPY` on a peer's socket**, and at most 1,024 buffers a `sendmsg`.

## Performance

Measured on the lab on 2026-10-23, in two runs:
- **The object store's**, F105's before and after, the same captures F105's page reads, run as
  [it says](store-reads-in-place.md#performance). This page has F106's step: F105's commit
  (`958742ba`) to that of F106's stage half (`f80e0da4`), which carries the fork's vectored write,
  the read half and the stage half.
- **The table path's**, the grid's reference cell before and after the fork's vectored write, which
  every answer to a client goes through.

[O104's page](../object-storage/store-optimizations/object-lane.md#before-and-after) has F105 and
F106 together.

### On europa's loopback

Operations a second, F105 → F106:

| Copies | Arm | Size | In flight | F105 | F106 | F106 ÷ F105 | p50 ms | p99 ms |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 2+1 | Get | 64 KiB | 1 | 1,666 [1,644–1,712] | 1,781 [1,774–1,812] | **1.07, ahead** | 0.60 → 0.56 | 0.77 → 0.71 |
| | | | 16 | 13,392 [13,287–13,537] | 16,586 [16,422–16,790] | **1.24, ahead** | 1.17 → 0.95 | 1.81 → 1.39 |
| | | 1 MiB | 1 | 519 [509–526] | 547 [533–553] | **1.05, ahead** | 1.99 → 1.87 | 2.39 → 2.26 |
| | | | 16 | 2,117 [2,089–2,148] | 2,262 [2,251–2,291] | **1.07, ahead** | 7.46 → 6.96 | 12.6 → 11.5 |
| | Put | 64 KiB | 1 | 434 [399–438] | 463 [426–468] | 1.07, within | 2.28 → 2.14 | 2.71 → 2.56 |
| | | | 16 | 2,528 [2,500–2,582] | 2,717 [2,695–2,752] | **1.07, ahead** | 6.26 → 5.84 | 9.55 → 9.13 |
| | | 1 MiB | 1 | 218 [217–220] | 235 [233–236] | **1.08, ahead** | 4.56 → 4.22 | 5.40 → 5.02 |
| | | | 16 | 547 [544–548] | 564 [555–565] | **1.03, ahead** | 29.3 → 28.5 | 35.8 → 34.5 |
| Three | Get | 64 KiB | 1 | 1,963 [1,929–1,989] | 2,081 [2,062–2,120] | **1.06, ahead** | 0.50 → 0.47 | 0.65 → 0.60 |
| | | | 16 | 14,817 [14,713–14,947] | 19,721 [19,547–19,941] | **1.33, ahead** | 1.05 → 0.79 | 1.73 → 1.22 |
| | | 1 MiB | 1 | 732 [719–739] | 739 [734–745] | 1.01, within | 1.29 → 1.28 | 1.56 → 1.54 |
| | | | 16 | 2,581 [2,579–2,588] | 2,579 [2,577–2,586] | 1.00, within | 6.15 → 6.16 | 8.79 → 8.85 |
| | Put | 64 KiB | 1 | 428 [403–430] | 458 [430–459] | **1.07, ahead** | 2.32 → 2.16 | 2.75 → 2.61 |
| | | | 16 | 2,132 [2,109–2,183] | 2,274 [2,260–2,343] | **1.07, ahead** | 7.54 → 7.03 | 11.5 → 11.0 |
| | | 1 MiB | 1 | 201 [200–203] | 218 [217–220] | **1.09, ahead** | 4.94 → 4.55 | 5.80 → 5.33 |
| | | | 16 | 399 [395–403] | 435 [432–438] | **1.09, ahead** | 40.1 → 36.8 | 46.5 → 42.9 |

The cpu a GiB moved cost, in cpu-seconds, F105 → F106, with the ratio:

| Copies | Arm | Size | In flight | Nodes | Object executors | Shards |
| --- | --- | --- | --- | --- | --- | --- |
| 2+1 | Get | 64 KiB | 1 | 16.2 → 13.6 (0.84) | 3.58 → 3.37 (0.94) | 12.4 → 10.0 (0.81) |
| | | | 16 | 11.6 → 9.27 (0.80) | 2.68 → 2.36 (0.88) | 8.82 → 6.84 (0.78) |
| | | 1 MiB | 1 | 2.51 → 2.28 (0.91) | 1.05 → 1.02 (0.97) | 1.42 → 1.21 (0.86) |
| | | | 16 | 2.77 → 2.48 (0.90) | 1.16 → 1.07 (0.92) | 1.59 → 1.37 (0.87) |
| | Put | 64 KiB | 1 | 78.4 → 69.2 (0.88) | 15.1 → 14.5 (0.96) | 54.4 → 46.0 (0.85) |
| | | | 16 | 52.6 → 45.3 (0.86) | 10.8 → 10.0 (0.93) | 36.5 → 30.0 (0.82) |
| | | 1 MiB | 1 | 8.07 → 7.19 (0.89) | 2.90 → 2.72 (0.94) | 4.48 → 3.81 (0.85) |
| | | | 16 | 6.77 → 6.23 (0.92) | 2.79 → 2.65 (0.95) | 3.56 → 3.14 (0.88) |
| Three | Get | 64 KiB | 1 | 13.7 → 11.3 (0.83) | 1.74 → 1.71 (0.98) | 11.7 → 9.41 (0.80) |
| | | | 16 | 9.63 → 7.43 (0.77) | 1.25 → 1.19 (0.95) | 8.32 → 6.18 (0.74) |
| | | 1 MiB | 1 | 1.42 → 1.29 (0.91) | 0.22 → 0.22 (1.00) | 1.16 → 1.03 (0.89) |
| | | | 16 | 1.44 → 1.29 (0.89) | 0.25 → 0.25 (0.99) | 1.17 → 1.02 (0.87) |
| | Put | 64 KiB | 1 | 86.3 → 77.1 (0.89) | 21.6 → 20.6 (0.95) | 54.3 → 46.3 (0.85) |
| | | | 16 | 56.7 → 48.6 (0.86) | 13.2 → 12.3 (0.93) | 36.9 → 30.0 (0.81) |
| | | 1 MiB | 1 | 9.24 → 8.23 (0.89) | 3.95 → 3.69 (0.94) | 4.53 → 3.80 (0.84) |
| | | | 16 | 7.89 → 6.88 (0.87) | 3.77 → 3.26 (0.87) | 3.61 → 3.13 (0.87) |

What this says:
- **At 2+1 every arm ran faster**, by 3 to 24%, seven of the eight with their ranges apart. The
  64 KiB puts at one in flight overlap.
- **With three copies every arm but the 1 MiB gets ran 6 to 33% faster**, each range apart, and the
  1 MiB gets were within. A get from a pool of three copies reads its own node's copy, so its bytes
  cross no lane.
- **The shards spent 11 to 26% less cpu a GiB on every arm**, at either redundancy, and the object
  executors up to 13% less.
- **Where a get's bytes cross no lane, what F106 changed on its path is the fork's vectored write.**
  Every frame to a client is a header and a body at least, a `send` each before F106 and one
  `sendmsg` since. It saves a call a part, so it saves most where frames are small. With three
  copies the shards spent 20 and 26% less cpu a GiB on the 64 KiB gets, and 11 and 13% less on the
  1 MiB gets, whose rate did not move: at sixteen in flight their devices were 93 to 99% busy on
  either side.
- **A put's stage is one allocation every holder's request shares**, written as parts and read in
  place. The puts ran 3 to 9% faster, their shards spending 12 to 19% less cpu a GiB and their
  object executors 4 to 13% less.

### The table path

The grid's reference cell (`macro/grid/unsorted/r50/1024`) on titan (Zen1, four cores) under
`performance`, by [the lab's procedure](../performance/benchmarking.md#before-and-after-on-the-lab),
on 2026-10-23 (`shoal-spike/results/stores-ab/w2-vec/`):
- **Two builds of `shoal-workload`**, each for znver1 in a worktree of its own:
  - F105's commit, against the glommio fork before its vectored write (`a29c550`);
  - F106's stage half, against the fork after it (`2b88b86`).

  Of F106 only the fork's vectored write is on the cell's path: a standalone node opens no peer
  lane.
- **X9's scratch configuration**: two shards on cpus 2 and 4, and the client on 6 and 7.
- **The host held still**: titan's tmdb node stopped, locked memory unlimited, and the storage wiped
  before every run.
- **Six rounds**, the first side alternating.

Each side's median with its lowest and highest:

| Figure | Before | After | After ÷ before |
| --- | --- | --- | --- |
| Operations a second | 8,404 [8,259–8,501] | 8,465 [8,327–8,586] | 1.01, within |
| Read p50 µs | 179 [175–201] | 171 [169–176] | 0.96, within |
| Read p99 µs | 430 [405–470] | 398 [389–418] | 0.93, within |
| Read mean µs | 193 [187–215] | 183 [180–187] | **0.95, apart** |
| Write p50 µs | 7,041 [6,921–7,139] | 6,952 [6,913–7,046] | 0.99, within |
| Write p99 µs | 14,998 [13,291–15,946] | 15,414 [13,582–17,254] | 1.03, within |

What this says:
- **The cell's rate was within.** Half its operations are writes, each waiting on a sync of the
  device, 7 ms at the median, and they bound it.
- **Its reads were quicker.** Their mean was 5% less, the ranges apart, and their p50 was lower in
  each of the six rounds. A read's answer is a frame of two parts or more, a `send` each before
  F106 and one `sendmsg` since.

### On the SSD leg

A node on each host, over 1 GbE, at 2+1, from the captures F105's page reads. No thread was sampled
on this leg, so the cpu is each capture's own count of the three nodes' processes. Operations a
second, F105 → F106:

| Arm | Size | In flight | F105 | F106 | F106 ÷ F105 | p99 ms | Nodes, cpu-seconds a GiB |
| --- | --- | --- | --- | --- | --- | --- | --- |
| Get | 64 KiB | 1 | 428 [403–450] | 427 [413–432] | 1.00, within | 3.68 → 3.71 | 77.2 → 69.4 (0.90) |
| | | 16 | 2,043 [1,985–2,103] | 2,019 [2,017–2,024] | 0.99, within | 13.8 → 13.8 | 38.4 → 33.6 (0.87) |
| | 1 MiB | 1 | 71.8 [69.0–73.6] | 67.6 [65.3–69.7] | 0.94, within | 24.8 → 23.6 | 22.0 → 21.0 (0.96) |
| | | 16 | 136 [131–139] | 128 [127–129] | **0.95, behind** | 428 → 441 | 15.8 → 14.7 (0.93) |
| Put | 64 KiB | 1 | 39.4 [39.1–40.1] | 41.5 [40.6–42.2] | **1.05, ahead** | 63.8 → 62.1 | 670 → 580 (0.86) |
| | | 16 | 300 [297–302] | 302 [299–306] | 1.01, within | 74.0 → 72.3 | 245 → 224 (0.91) |
| | 1 MiB | 1 | 25.5 [25.0–26.0] | 26.6 [26.4–26.9] | **1.04, ahead** | 82.1 → 78.9 | 68.7 → 59.9 (0.87) |
| | | 16 | 94.8 [92.5–96.8] | 97.5 [95.8–98.6] | 1.03, within | 345 → 346 | 36.9 → 33.4 (0.90) |

What this says:
- **The nodes spent 4 to 14% less cpu a GiB on every arm**, every range apart. Where it went is not
  known on this leg.
- **The puts at one in flight ran 4 and 5% faster**, their ranges apart. Each of F106's captures
  ran more of them than any capture of the other two builds.
- **The 1 MiB gets at sixteen in flight were 5% behind, and each deployment's draw made it.**
  - Europa's link received 111 to 112 MiB/s in every capture, full.
  - F106 sends the same bytes as before, so at a full link only the bytes each get takes across it
    can move the rate. Those depend on where the deployment put the gets' coordinators and chunks.
  - F106's two captures took 0.87 MiB of europa's link a get. The other four took 0.81 to 0.85
    ([each capture](store-reads-in-place.md#on-the-ssd-leg)).
- **The 1 MiB gets at one in flight were within**, and alike at a like share. F106's capture whose
  gets europa's own node answered 44% of ran 69.3 a second, and F105's at 47% ran 69.5.
- **The tail of a put at one in flight did not move.** It is twice as long under TLS as in
  plaintext at 64 KiB ([O131](../appendix/optimizations.md#o131-a-puts-tail-at-one-in-flight-across-hosts-is-twice-as-long-under-tls)),
  and F106's frames, each written whole, left its p99 at 62.1 ms.

## Tests

| Test | What fails if the feature is reverted |
| --- | --- |
| glommio `net::tcp_socket::tests::a_vectored_write_sends_its_parts_in_order` | A socket with room takes only a frame's first part in one call, or the parts arrive out of order |
| glommio `net::tcp_socket::tests::a_vectored_write_waits_for_a_slow_reader` | Parts of 3 MiB written past a full socket are lost or reordered, or the write does not wait for room |
| glommio `net::tcp_socket::tests::a_vectored_write_takes_more_parts_than_one_call` | A call takes more than 1,024 parts, or 2,000 parts arrive out of order |
| glommio `net::unix::tests::a_unix_vectored_write_sends_its_parts_in_order` | A Unix stream's call takes one part of three |
| `shoal-proto` `peer::chunk::tests::a_reads_bytes_stay_where_their_frame_was_read` | A read's head and bytes are not `encode_body`'s body, or a decoded read's bytes are copied out of their body, or a body shorter than a label is taken |
| `shoal-proto` `peer::chunk::tests::chunk_bytes_are_their_bytes_wherever_they_are` | Bytes in a frame and in a buffer of their own compare unequal, or do not resize in place |
| `shoal-proto` `peer::chunk::tests::every_answer_round_trips` (extended) | An answer decoded in place differs from one decoded from a copy |
| `shoal-proto` `peer::chunk::tests::every_requests_parts_are_its_body` | A request's parts are not its body for a piece, a background stage, a stage of units, a fold, a parity stage or a read, a request decoded from a shared body differs, or its carried bytes are copied out of the body |
| `shoal-core` `peer::codec::tests::a_body_is_every_byte_its_peer_sent` | A body read unzeroed in uneven pieces is not the bytes sent, or a short one is handed out |
| `shoal-core` `objects::lane::tests::a_read_across_the_lane_stays_in_its_frame` | A read across the lane is copied out of its frame, or a range inside units, a staged unit laid over or a rebuild's read answers other bytes |
| `shoal-core` `objects::lane::tests::a_peer_that_reads_no_answers_holds_a_bounded_budget` | A holder reads every request of a peer that reads no answers, its answers hold no draw, or a draw stays after the peer goes |
| `shoal` `cluster_fixture` `an_object_crosses_its_lanes_under_ktls` | A 2+1 object's chunks read back otherwise through any node over lanes under kTLS; skips by name without the kernel's TLS module |

## Related

- [O104](../appendix/optimizations.md#o104-an-object-lane-copies-a-reads-bytes-twice-and-a-stages-once-more);
  [A 2+1 get crosses the object lane](../object-storage/store-optimizations/object-lane.md).
- [F105](store-reads-in-place.md), whose span a served read is written from;
  [F86](replicated-pools.md), the lane; [F25](read-buffers-are-filled-not-zeroed.md), a body read
  unzeroed.
- [O128](../appendix/optimizations.md#o128-a-coordinator-copies-a-reads-range-twice-before-it-answers-and-checks-it-again),
  [O129](../appendix/optimizations.md#o129-a-local-read-still-copies-its-range-once-to-cross-to-its-shard-and-scrubs-and-records-copy-theirs) and
  [O130](../appendix/optimizations.md#o130-a-lanes-buffers-are-fresh-allocations-a-cache-hit-is-copied-to-be-sent-and-answers-go-a-frame-a-call): the copies left.
