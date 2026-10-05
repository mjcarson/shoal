# F73. More than one frame for one query

A bundle longer than the server's frame is now sent as an opener and a run of bounded data
frames, and an answer longer than one data frame comes back the same way, between peers that
agreed to it at the hello. Small answers queued on a connection are written between a long
answer's frames rather than after all of it, and a long stream travels on connections the client
sets apart for it. The frames are a layer of their own - an opener, data frames with `LAST` on the
final one, a receiver that judges every frame against its stream - which today's bundles and
answers are carried over, and which M13's object frames will carry bytes over without another
framing change.

## Context

This is the S1 prerequisite "more than one frame for one query on the client wire"
([S1](../object-storage/prerequisites.md#required)), landing before
[M13](../object-storage/milestones.md#m13-the-wire-and-the-baseline). Before it:

- **a body was read whole**. The server allocated a bundle's length and filled it before
  anything was routed (`shoal-core/src/server/request_body.rs`), and the client read an answer
  the same way;
- **a frame was the ceiling**. A frame is bounded at 64 MiB, a client's hello always offered
  exactly that, and an answer past it was `ResponseTooLarge` whatever either side could hold;
- **an answer was one frame a query**, written whole, so a 60 MiB answer held every answer
  queued behind it on its connection for as long as its bytes took;
- **`Flags::LAST`**, "the last frame for its query", had been reserved since F10 and set by
  nothing.

An object is larger than any buffer either peer should hold (R13), so a write has to be carried
in, and a read answered by, a sequence of bounded frames. The prerequisite waited on
[Q26](../object-storage/contract.md#questions-to-answer) and on spike
[X11](../object-storage/streamed-bodies.md), which measured what such a sequence costs: the frame
size, the window, and what it does to a small query sharing its connection. The user chose the
shape: a generic stream layer, and today's bundles and answers carried over it, so that the layer
is used and tested before any object exists.

## What it does

### The frames

| Frame | Body | When |
| --- | --- | --- |
| A `Queries` opener: `Queries` with `Flags::STREAMED` | its trace context and read options as a whole bundle carries them, then the bundle's id (16 bytes) and the archive's length (8 bytes), and no archive | a bundle past the server's frame, to a server that granted streams |
| A `Response` opener: `Response` with `Flags::STREAMED` | the query id, the session token if there is one, then the answer's length | an answer past one data frame, to a client that asked for streams |
| `Data`, message type 27 | an id (16 bytes), the offset of its bytes in the stream (8 bytes), and the bytes; `Flags::LAST` on the final one | after an opener, in order |

The id is the opener's: a bundle's id for a request, a query id for an answer. The 24 bytes after
a data frame's header sit where a response's query id and the start of its body do, so the
client's fixed preamble read is unchanged. A data frame's bytes are never an archive and never
validated as one; the receiver puts them at their offset and nothing else.

### Who speaks streams

Streams are offered and granted at the hello, the way read options were
([F41](read-consistency.md)): `CLIENT_CAP_STREAMS` is bit one of the capability byte, and the
last reserved byte of both handshake bodies, offset 15, is spent on the largest body each side
assembles, as a power of two (`stream::body_bound`). A client offers its own frame bound now
instead of a hard-coded 64 MiB. A peer from before F73 writes and reads zero in both places, asks
for nothing and is granted nothing, and is sent exactly what it always was: its bundles and
answers are one frame each, and an answer past its frame is `ResponseTooLarge` as before.

### The receiver

`shoal-proto/src/shared/protocol/stream.rs` holds what both peers agree on, with no I/O:

| Type | What it is |
| --- | --- |
| `Inbound<T>` | Every stream one direction of a connection has open, by id, with its sink. Judges each data frame: a known id, the offset equal to the bytes received so far, never past the declared length, `LAST` exactly at the end, no empty frame. Refused streams are drained. At most 16 open |
| `Hold` | `Reserved`: the declared length is taken at the opener, as an assembled bundle or answer is. `Windowed`: the bytes count against a window until the sink releases them, which M13's stager will be |
| `Splitter` | Cuts a length into frames of one size, in order, `LAST` on the last |
| `StreamFault` | What ends the connection: a duplicate id, too many open, nothing declared, a declaration past the advertised bound, an unknown id, a gap, bytes past the end, `LAST` short, the end without `LAST`, an empty frame |

**What ends what.** A stream whose frames break its rules ends the connection, as a bad header
does: a peer out of step cannot be resynchronized. A stream the receiver refuses by policy is
answered by name and drained, and the connection goes on: a bundle past the server's bound is
`RequestTooLarge` (error code 21, reserved until now), and one past the shard's assembly budget
is `Shedding`.

### The server

- **Reading.** `client_rx_relay` opens a stream for a `Queries` opener, reads each data frame
  straight into the next part of a `BodyAssembly` (`request_body.rs`), and routes the bundle once
  its last byte is in, exactly as one read in a single frame: the shards cannot tell the two
  apart. `BodyAssembly` is the only other way to a `RequestBody`, and keeps its promise: its
  length only grows by a read that filled the bytes, and it becomes a body only when full.
- **The budget.** Every bundle being assembled takes its declared length from its shard's
  `networking.max_assembling_bytes` at its opener, and gives it back when it is routed or its
  connection ends.
- **Writing.** `write_replies` hands every answer to an `Outbox` (`shard/outbox.rs`). An answer
  longer than one data frame, to a client that asked for streams and within what it assembles, is
  a stream: an opener, then data frames written straight from the archive. Whole frames - small
  answers, topology frames, admin answers, refusals - are written before the next data frame,
  unless whole frames have had a data frame's worth of bytes since the last one, so a small
  answer waits behind at most one data frame and a stream is never starved. Up to four streams
  take turns, and two answers of one id are never open at once.

### The client

- **Sending.** A bundle that fits the server's frame is written exactly as before. One past it is
  an opener and data frames of `StreamConfig::request_frame_bytes`, when the server granted
  streams and assembles bundles that long; otherwise it is refused before a connection is taken,
  by the frame as it always was, or by the server's body bound.
- **Receiving.** `TcpProxy` opens an assembly for a `Response` opener, an `AlignedVec<16>` of the
  declared length so the archive stays aligned, fills it frame by frame, and hands it on at
  `LAST` as one response: nothing above the proxy knows it was streamed. An answer nobody waits
  for is drained, not assembled. `ShoalResponse::wire_bytes` counts the opener and every data
  frame's preamble ([F69](driver-operation-kinds.md)).
- **Connections set apart.** X11 found that a small request's p99 on a connection carrying a
  stream at 1 MiB frames was several times its p99 on a connection of its own, and that
  `TCP_NOTSENT_LOWAT` did not close the gap (T1). So a client keeps
  `StreamConfig::dedicated_connections` (two by default), opened when first needed, for bundles it
  streams and for sends a caller marks `SendOptions::bulk` because their answers are long.
  `Shoal::connections` says how many of each are open.

### Settings

| Where | Setting | Default |
| --- | --- | --- |
| `networking` | `stream_frame_bytes`: payload bytes of a streamed answer's data frames | 1 MiB, X11's frame, cut to what `max_frame_bytes` leaves after a data head |
| `networking` | `max_request_body_bytes`: the longest bundle assembled, a power of two | `max_frame_bytes`; a cluster node refuses more (item 208) |
| `networking` | `max_assembling_bytes`: bytes one shard's connections may hold in bundles being assembled | four bundles of the longest |
| `StreamConfig` | `max_frame_bytes`, `max_body_bytes`, `request_frame_bytes`, `dedicated_connections` | 64 MiB, 1 GiB, 1 MiB, 2 |

None of the server's appears in `shoal.yml`, and every default keeps the ceiling that existed:
the benchmark configuration does not change.

## Design choices

**A capability bit and the last reserved byte, not a new client wire version.** The client lane
is read by `shoaladm upgrade` across a rolling upgrade, and the schema fingerprint folds in the
client wire version, so a version bump would have stopped an upgrade at its first gate and made
every bench comparison across F73 need a waiver. F41 set the precedent, and S12 had planned the
object types as an addition behind a bit.

**An opener flag, not a type of its own.** A `Queries` opener is a `Queries` frame: the trace
context and read options are read by the same code in the same order, and the bundle routes the
same way. M13's `ObjectOp` and `ObjectAnswer` take the flag the same way, so `Data` is S12's
`ObjectData` and the object types move up to 28 and 29.

**The opener's id, not a stream number.** The client already finds a waiter by the bundle's id,
and a refusal names it. Two answers of one bundle share it, so the writer never interleaves two
streams of one id, which costs nothing: a bundle's answers come from shards that answer at
different times anyway.

**One frame for a request that fits.** A pooled connection's write side carries one bundle at a
time, so splitting a bundle that fits buys no interleaving and costs a frame a mebibyte.

**Interleave between ids, small frames first, never starve.** Bytes already written to a socket
cannot be taken back ([C2](../distributed/transport.md#design-choices)); the frame boundary is
where a writer can choose. X11 showed that this alone does not bound a small answer's wait, since
the kernel's buffers hold megabytes ahead of it, which is why long streams also go on connections
of their own. Its repeat with the server writing small frames first
([item 213](../appendix/resolved/x11-setup-fifo.md)) found that a low water mark on the socket makes
the order count, about three times across the network; a node sets none
([O94](../appendix/optimizations.md#o94-a-nodes-sockets-set-no-tcp_notsent_lowat)).

**Reserve, never wait, for an assembling stream.** An assembled bundle takes its whole length at
its opener and is refused when the budget is short. Counting it against a window would let one
stream stop the read that is its only way to finish. The window `Inbound::may_read` keeps is for
sinks that release as they consume; there is none yet, and M15's stager is the first.

## Alternatives rejected

- **A client wire version 5**, and handshake bodies grown to carry the bounds: the two costs in
  the first design choice, for a field that fits a byte.
- **Credits, as HTTP/2's `WINDOW_UPDATE`.** A receiver that is reading has TCP's window; one that
  wants a range asks for it (S12's ranges). Credits need `Cancel` wired and state on both sides.
- **Splitting every bundle past a data frame**, as answers are: see the fourth design choice.
- **Streaming between peers.** A forwarded bundle and its answer stay one peer frame; the peer
  lane's bound and item 208 are a separate question, and a cluster node's request body is held to
  its frame until 208 is fixed.
- **Growing the receiver's buffer as frames arrive** rather than declaring the length up front:
  every growth is a copy of what arrived, and an undeclared length cannot be admitted against a
  budget. An object `put` of unknown length is M13's, as a windowed sink, not an assembly.
- **`TCP_NOTSENT_LOWAT` on every client socket** instead of connections set apart: X11 measured
  it, and at 16 and 128 KiB it narrowed the gap without closing it, and under kTLS on the sending
  side it made the small request's tail worse.

## Limitations

- **A refused bundle still sends every byte.** The server answers at the opener, but nothing tells
  the client to stop before its last data frame; `Cancel` is still reserved
  ([todos](../appendix/todos.md#cancel-and-what-it-would-actually-buy)).
- **An assembling stream holds its reservation for as long as its client takes to send it.** A
  stalled client holds bytes of its shard's budget until its connection ends.
- **A cluster node assembles nothing past its frame.** `max_request_body_bytes` above
  `max_frame_bytes` is refused on a cluster node by name, because one row past a peer frame is a
  log entry no append carries ([item 208](../appendix/known-issues.md#208-a-write-that-fits-a-client-frame-can-make-a-log-entry-no-peer-frame-carries)).
  Answers stream on a cluster as anywhere.
- **An unexpectedly long answer shares its connection.** Only a bundle the client streams, or one
  its caller marks bulk, goes on a connection set apart; a get that happens to return many rows
  is interleaved on whichever connection it was sent on.
- **The client reserves an answer's declared length at its opener**, up to its body bound, for each
  of up to four streams a connection writes at once.
- **A data frame is one write.** The reply writer hands a whole 1 MiB data frame to the socket in
  one vectored write, so under kTLS, where the kernel encrypts inside the call, a shard writing a
  long answer holds its core about a millisecond a frame on Zen1, as X11 measured. That is less
  than a whole frame of up to 64 MiB, which is what a long answer cost before F73, and the change
  that bounds it is filed ([todos](../appendix/todos.md#write-object-frames-under-ktls-in-pieces)).
- **Nothing measures the window yet.** `Inbound::may_read` and `Hold::Windowed` are built and
  tested and read by nothing until M15's stager.

## Invariants to uphold

- A stream is judged frame by frame, and a frame that breaks it ends its connection.
- An opener and a data frame are sent only to a peer that granted `CLIENT_CAP_STREAMS`, and a
  stream is never longer than the receiver's advertised bound.
- A bundle that fits the server's frame is framed exactly as before streams.
- A `RequestBody` exists only once reads have filled every byte of it, however many frames they
  took.
- Bytes reserved by an assembling stream never stop the reader; only a windowed sink's do.
- Every whole frame queued on a connection is written before the next data frame, unless whole
  frames have had a data frame's worth since the last one.
- Two streams of one id are never open in one direction of a connection at once.
- A cluster node's request body is never past its frame while item 208 is open.

## Performance

Every answer now goes through the `Outbox`, so F73 is on every query's path, and it was compared
before and after on the lab ([the procedure](../performance/benchmarking.md#before-and-after-on-the-lab)).
The committed change (`09084c8`) was compared against `a12c91e`, the commit before it, both built
for `znver1` and run by `shoal-workload` on hyperion. The conditions:

- hyperion: Zen1 V1756B, 4 cores and 8 threads, kernel 7.0.0-34, `performance` governor;
- the lab's tmdb node on that host stopped (it was already);
- two shards, with physical core 3 (cpus 3 and 7) left to the client, and storage on `/opt/shoal`,
  wiped before every run;
- tracing at `Warn`;
- four rounds, each running both sides back to back, with the side that went first alternating.

**This is an A/B, not a capture.** The committed `shoal.yml` is sized for a sixteen-core host.

| Workload | Figure | Before, median [range] | After, median [range] | |
| --- | --- | ---: | ---: | --- |
| `get_ephemeral` | ops/s | 51,594 [44,019–53,721] | 51,015 [47,476–55,218] | within noise |
| `get_ephemeral` | get p99 | 571 µs [511–589] | 570 µs [479–631] | within noise |
| `transport/send_one/small` | ops/s | 49,422 [43,246–52,804] | 47,808 [44,909–50,661] | within noise |
| `transport/send_one/small` | get p99 | 584 µs [496–646] | 599 µs [549–637] | within noise |
| `grid/unsorted/r50/1024` | ops/s | 8,157 [8,101–8,296] | 8,261 [8,222–8,320] | within noise |
| `grid/unsorted/r50/1024` | read p99 | 487 µs [471–527] | 523 µs [480–566] | within noise |
| `grid/unsorted/r50/1024` | write p99 | 14.9 ms [14.0–16.0] | 13.7 ms [13.4–14.2] | within noise |
| `insert_ephemeral` | ops/s | 109,245 [94,247–126,326] | 110,715 [81,015–113,057] | within noise |
| `insert_ephemeral` | insert p99 | 97.9 ms [73.1–102.9] | 87.9 ms [84.9–110.5] | within noise |
| `encryption/depth/plain/1048576/8` | ops/s | 1,549 [1,452–1,602] | 1,418 [1,362–1,561] | within noise |
| `encryption/depth/plain/1048576/8` | get p50 | 4.43 ms [4.12–4.96] | 5.20 ms [4.61–5.70] | within noise |
| `encryption/depth/plain/1048576/8` | get p99 | 12.4 ms [10.3–14.1] | 11.8 ms [10.4–12.2] | within noise |
| `encryption/depth/tls/1048576/8` | ops/s | 917 [847–954] | 908 [842–978] | within noise |
| `encryption/depth/tls/1048576/8` | get p99 | 16.5 ms [13.3–16.9] | 15.5 ms [14.8–16.8] | within noise |

Every other figure, each side's p50 per operation, was within noise too. Following the compare
tool's rule for the macro layer, a difference counts only when the two sides' run intervals are
disjoint.

The two `encryption` arms are the ones that exercise the change: each get answers one 1 MiB row,
whose archive is longer than one 1 MiB data frame, so after F73 every answer is an opener and two
data frames written through the `Outbox`, where before it was one frame. That was established by
reading the decision in `write_replies`, not by watching the frames: the server sends through
io_uring, where `strace` cannot count its writes. The plaintext arm's first medians moved against
F73 (0.92 times the ops/s, 1.17 times the p50) inside overlapping intervals, so both arms were
repeated over eight rounds:

| Workload, 8 rounds | Figure | Before, median [range] | After, median [range] | |
| --- | --- | ---: | ---: | --- |
| `encryption/depth/plain/1048576/8` | ops/s | 1,558 [1,407–1,608] | 1,518 [1,338–1,532] | within noise |
| `encryption/depth/plain/1048576/8` | get p50 | 4.64 ms [4.21–5.51] | 4.65 ms [4.38–5.78] | within noise |
| `encryption/depth/plain/1048576/8` | get p99 | 11.1 ms [9.7–15.5] | 11.7 ms [10.7–13.5] | within noise |
| `encryption/depth/tls/1048576/8` | ops/s | 941 [833–960] | 914 [850–981] | within noise |
| `encryption/depth/tls/1048576/8` | get p50 | 7.53 ms [6.78–10.00] | 7.93 ms [7.07–9.82] | within noise |
| `encryption/depth/tls/1048576/8` | get p99 | 16.2 ms [15.2–20.0] | 16.8 ms [13.0–19.0] | within noise |

The p50 came back to 1.00 times. **No cost was measured**, on small answers or on streamed ones.
What this cannot show is the benefit: no workload sends a small query on a connection carrying a
long answer, which is what the interleaving is for. That is covered by
`a_small_answer_is_written_between_the_frames_of_a_large_one`, and was measured by X11 on its own
harness ([X11, section 3](../object-storage/streamed-bodies.md#3-a-small-request-beside-a-stream)),
not by a benchmark of Shoal. The workload that would is filed
([todos](../appendix/todos.md#a-workload-with-small-queries-beside-a-streamed-answer)).

## Tests

| Test | Where | What breaks if this is reverted |
| --- | --- | --- |
| `stream::tests` (14) | `shoal-proto/src/shared/protocol/stream.rs` | A stream in order is not kept to its end, any of the faults is accepted, a refused stream is not drained or its id stays taken, reserved bytes stop the reader or windowed ones do not, the splitter misses or repeats a byte or misplaces `LAST`, a data preamble does not round trip, or the body bound does not survive its byte |
| `a_hello_without_streams_reads_as_none`, `every_message_type_round_trips_through_its_discriminant`, `flag_bits_are_stable` | `shoal-proto/src/shared/protocol/tests.rs` | The body bound moves off offset 15, an older peer's zero reads as a bound, `Data` moves off 27, or `STREAMED` off bit 7 |
| `an_assembled_body_holds_every_byte_of_every_frame`, `an_assembly_short_of_its_length_does_not_finish`, `an_assembly_refuses_a_read_past_its_length` | `shoal-core/src/server/request_body.rs` | An assembled body changes a byte, a short one becomes a body, or a read past its length or cut short leaves bytes nothing wrote inside it |
| `outbox::tests` (5) | `shoal-core/src/server/shard/outbox.rs` | A small answer waits for a whole stream, a flood of small answers starves one, two streams of one id interleave, more than the interleave open at once, or the read relay's count of unstarted answers is wrong |
| `stream_settings_default_with_no_config` and three more | `shoal-core/src/server/conf.rs` | A default moves a ceiling, a data frame past the frame or under a page is accepted, or a cluster node takes a body past its frame |
| `stream_config_refuses_what_a_server_cannot_be_told` | `shoal-client/src/client/builder.rs` | A body bound that is not a power of two, or a frame too small, reaches a hello |
| `a_streamed_answer_is_assembled_between_other_frames` | `shoal-client/src/client.rs` | An answer's data frames are not assembled around other frames, its framing is not counted, or it lands off a sixteen byte boundary |
| `a_bundle_larger_than_one_frame_round_trips` | `shoal/tests/streamed_bodies.rs` | A bundle past the server's frame is refused, or its answer past the client's frame is |
| `an_answer_larger_than_one_frame_round_trips` | the same | An answer past the client's frame is refused or changed |
| `an_answer_past_the_clients_body_bound_is_refused_by_name` | the same | An answer past what the client assembles is streamed, or refused without naming the bound |
| `long_streams_go_on_connections_set_apart` | the same | A streamed or bulk bundle goes on the shared pool, or a small one opens a connection set apart |
| `a_small_answer_is_written_between_the_frames_of_a_large_one` | the same | A small answer queued during a long one waits for all of it |
| `a_malformed_data_frame_ends_one_connection_and_others_are_answered` | the same | An unknown id, a gap, bytes past the end or a short `LAST` is accepted, or ends more than its connection |
| `a_stream_over_the_request_bound_is_refused_by_name` | the same | A stream past the bound is accepted, refused without naming its id, or ends its connection |
| `the_ack_grants_streams_only_to_a_client_that_asked` | the same | A client that asked for nothing is granted streams, or one that asked is not told the bound |
| `a_data_frame_without_the_capability_closes_one_connection` | the same | A data frame is read on a connection that was not granted streams |
| `a_stream_round_trips_over_tls` | `shoal/tests/tls.rs` | A stream does not survive the kernel's record layer both ways |
| `a_long_answer_is_streamed_through_every_node` | `shoal/tests/cluster_fixture.rs` | A long answer, its node's own or forwarded, is not streamed to a client with a small frame |

The existing `errors.rs` tests of `ResponseTooLarge` are unchanged and still pass: their raw
hellos ask for no streams, which is the guard that a client from before F73 is served as it was.

## Related

- [X11](../object-storage/streamed-bodies.md), which set the frame, the window's role and the
  connections set apart; [Q26, in part](../object-storage/contract.md#q26-in-part-streamed-bodies-2026-10-05).
- [S12](../object-storage/wire-and-client.md), the object frames this layer carries.
- [Wire protocol](../architecture/wire-protocol.md) and [Transport](../distributed/transport.md).
- [F10](framing-and-protocol-evolution.md), which reserved `LAST`; [F41](read-consistency.md),
  the capability byte; [F26](archive-routed-requests.md), the request body shards share.
