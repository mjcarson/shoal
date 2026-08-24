# F25. Read buffers are filled, not zeroed

## Context

Both ends of a Shoal round trip allocated a buffer, wrote zeroes over every byte of it, and then
overwrote every one of those bytes with the read on the next line.

The server, in `client_rx_relay`:

```rust
// allocate a buffer that is exactly the right size
let mut data = BytesMut::zeroed(header.body_len());
// wait for messages from our client
if let Err(error) = tcp_rx.read_exact(&mut data).await {
```

The client, in `TcpProxy::read_frame`:

```rust
// Create an aligned vec to act as a pool of bytes
let mut aligned_buff = AlignedVec::<16>::with_capacity(frame.rest_len);
// resize our aligned vec
aligned_buff.resize(frame.rest_len, 0);
self.reader.read_exact(&mut aligned_buff).await?;
```

Filed as [O29](../appendix/optimizations.md) and
[O37](../appendix/optimizations.md), two entries about one defect on the two ends of one round
trip. [Item 34](../appendix/resolved/unvalidated-length-prefix.md) named the server's when it was
an *unbounded* allocation, and [F10](framing-and-protocol-evolution.md) fixed the bound and left
the zeroing; O37 went unfiled until the read path was walked in code for
[Row size and what it costs](../tables/row-size.md), which counts both of them among the walks a
payload takes per round trip.

O29 had been blocked on nothing for four features. What kept it was a real obstacle rather than an
absent one, and the entry said so: the fix looks like a one line swap for `BytesMut::with_capacity`
plus an `unsafe` `set_len` and is not, because `ServerMsg::Client` carries the buffer by value
across a channel and several call sites construct that message. Removing the zeroing makes a
buffer's initialization state a property of a message type, and nothing guaranteed it.

**Two things the entries said turned out to be wrong**, and both are recorded on the optimizations
page beside the claims they replace rather than in place of them.

*The memset was never in the `decode` stage.* `f24-routing`'s stage breakdown was read as locating
O29, on the grounds that `decode` grew ×199 across the width axis and contains the deserialize the
memset sits beside. It cannot contain it: `base`, the stamp every offset is measured from, is taken
in `client_rx_relay` **after** `read_exact` returns, and `decode` is the span from `bundle_dequeued`
to `decoded`. The allocation and the read both happen before the bundle's clock starts, so they
fall in **`net_in`** — the span from the client's write to `base` — mixed in with real wire time.
The stage layer has never measured either entry and cannot, as it stands.

*The server's half was not a `memset` at every size.* `BytesMut::zeroed(len)` is
`BytesMut::from_vec(vec![0; len])`, and `vec![0u8; n]` is `alloc_zeroed` — calloc, not an
unconditional write. A size the allocator serves out of its heap really is memset; one it serves
with a fresh `mmap` arrives zeroed from the kernel and costs nothing extra. So O29's *Impact* was
not asymptotic in the row width as filed. The client's `AlignedVec::resize(len, 0)` has no such
qualification: it is an unconditional write of zeroes at every size, over the larger of the two
payloads. **O37 is the asymptotic one, and O29 is not.**

## What it does

Neither buffer is written twice. The read is the only thing that writes either of them.

**On the server**, a new type in `shoal-core/src/server/request_body.rs`:

```rust
pub struct RequestBody {
    /// The body itself, filled by the read that built this
    data: BytesMut,
}
```

The field is private and `RequestBody::read_from` is the only constructor there is, so a
`RequestBody` that exists is a buffer some read filled to its full length. `ServerMsg::Client`
carries one of these instead of a bare `BytesMut`, and the relay is one call:

```rust
let data = match RequestBody::read_from(&mut tcp_rx, header.body_len()).await {
```

**On the client**, a `read_payload` helper beside `TcpProxy::read_frame` that hands the socket the
allocation as the uninitialized memory it is, and lets tokio count what the reader filled:

```rust
let mut read_buf = ReadBuf::uninit(spare);
// a socket hands over whatever it has rather than whatever was asked for, so this loops
while read_buf.filled().len() < len {
    let before = read_buf.filled().len();
    std::future::poll_fn(|cx| Pin::new(&mut *reader).poll_read(cx, &mut read_buf)).await?;
    // a read that delivered nothing is the end of the stream, not a shorter payload
    if read_buf.filled().len() == before {
        return Err(Errors::IO(std::io::Error::from(ErrorKind::UnexpectedEof)));
    }
}
```

and only then claims the length. A stream that ends mid payload is an `UnexpectedEof`; a partly
filled buffer never leaves the function on either end.

## Design choices

**The two ends are fixed differently, and the difference is forced rather than chosen.** The
client is tokio, which has [`ReadBuf`] — a reader is handed `&mut [MaybeUninit<u8>]` and reports
back how much of it it initialized, so `set_len` is discharged by a *check*: the loop only ends
once `ReadBuf` says `len` bytes are filled. The server is glommio behind
`futures::io::AsyncRead`, which takes a `&mut [u8]` and reports nothing. There is no check
available there, so the guarantee has to be **structural** instead: make the read the only way the
value can be built, which is what the private field buys. That asymmetry is why one half is a type
and the other is a function.

**`RequestBody` is a type rather than a comment.** The entry's own account of why this was hard is
that the buffer's initialization state becomes a property of a message several call sites
construct. A newtype answers exactly that: there is one constructor, `messages.rs` cannot build the
variant any other way, and a future call site that wants to put bytes in a `ServerMsg::Client` has
to go through a read to do it. This is the same shape as `StableBytes` in
`server/tables/partitions.rs` — an invariant stated in the type system with the argument written on
it, rather than an `unsafe` block with a comment hoping to be read.

**The client's two reads stay split.** The preamble and the payload are read separately so the
archive lands at offset zero of a sixteen byte aligned allocation, which is what makes turning it
into a response a pointer cast. Merging them would put the payload at offset 24 and end the zero
copy read silently. This change does not touch that, and the test that guards it —
`the_response_payload_lands_on_a_sixteen_byte_boundary` — is unchanged and still passes.

**The benchmark measures both shapes in one build.** `wire_codec/width/request/body` and
`wire_codec/width/response/body` each run a `zeroed` arm and a `uninit` arm at the five widths, so
one capture adjudicates the change rather than two captures on either side of it. The `zeroed` arms
stay afterwards as **controls** for a shape neither end has any more, the way
`wire_codec/request/decode/header` is kept as a control for a cost that does not grow.

## Alternatives rejected

**Recycling a buffer per connection**, so the zeroing is paid once and amortized over every request
after it. This is the design that avoids `unsafe` entirely, and it does not: a `BytesMut` whose
bytes were split off and handed to a shard reclaims its allocation on the next `reserve`, but the
reclaimed capacity is uninitialized *from Rust's point of view* however many times it has been
written, so exposing it to a reader needs the same `set_len`. The version that really is safe keeps
the buffer at a fixed length and copies the body out of it, which trades a memset for a memcpy of
the same bytes.

**Decoding in the relay** and sending `Queries` over the channel instead of a buffer. This removes
the ownership transfer that made the fix hard in the first place. It also moves the deserialize off
the shard's queue onto the relay task and changes what the `decode` stage means, which is a larger
change to the shape of the pipeline than the cost being removed justifies.

**Teaching the glommio fork a `poll_read` over `MaybeUninit`.** `glommio` is a path dependency at
`../glommio` and its `NonBuffered` stream already reads straight into the caller's slice with
`yolo_recv`, so a variant taking `&mut [MaybeUninit<u8>]` and returning how much it wrote would let
the server discharge the same runtime check the client does and retire the structural argument
entirely. It is the better end state and it is a change to another repository, so it is filed in
[TODOs](../appendix/todos.md) rather than taken here.

**Fixing the error frame's `vec![0u8; msg_len + ..]`** in the same pass. It is the same shape and
it is bounded at four kibibytes by the protocol's message bound, so it is not what either entry is
about. Left alone deliberately, and named here so the next reader knows it was seen.

## Limitations

- **This removes a write, not a copy.** The payload is still walked by the kernel copy into the
  buffer, by the deserialize behind it, and by everything
  [Row size and what it costs](../tables/row-size.md) counts. One hop of about six comes off each
  end, and the entries it closes were never the largest on that list.
- **The saving is not asymptotic in the row width**, on either end, and the benchmark says so. Both
  arms copy the same bytes, so the ratio between them is bounded by what zeroing costs against what
  copying costs — a write against a read plus a write — and it settles rather than growing.
- **No stage can see it.** The body read happens before the bundle's clock starts, so it lands in
  `net_in` beside real wire time. Nothing in the nineteen stages isolates it, and the micro layer is
  the only instrument that can adjudicate either entry until that changes.
- **The macro layer cannot see it either.** The entry's own estimate is 0.3% of a wide insert. No
  claim is made here about a `grid` arm, and none should be read into one.
- **The server's guarantee is structural, not checked.** Nothing at runtime verifies that
  `read_exact` filled the buffer; what is verified is that no other code path can build the value.
  A second constructor added to `request_body.rs` would silently undo the whole argument, which is
  why it is written on the type.

## Invariants to uphold

- **`RequestBody`'s field stays private, and `read_from` stays its only constructor.** No
  `From<BytesMut>`, no `pub fn new`, no public field. This is the entire safety argument for the
  `set_len` inside it.
- **The client's `set_len` stays behind the `filled().len()` check.** It is sound because
  `ReadBuf` counts what a reader initialized; a version that assumed the read filled the buffer
  would be the server's argument without the server's protection.
- **A partly filled buffer never leaves either read.** Both return an error and drop the buffer,
  because a short body handed onward is uninitialized memory reaching `rkyv::access`, not a small
  request.
- **The client's preamble and payload stay two reads.** See `TcpProxy::read_frame`'s own invariant
  block, which this change did not weaken.
- **Neither allocation may move to a path with a weaker alignment.** `rkyv::access` validates
  pointer alignment, and the response buffer's sixteen byte alignment is what the zero copy read
  rests on. `BytesMut::with_capacity` and `BytesMut::zeroed` allocate the same way, which is why
  swapping one for the other does not disturb this — a future change to a stack buffer or a
  sub-slice would.

## Performance

Measured by the two new pairs, both shapes in one build, at the five widths the codec groups
already sweep. Each arm allocates a body of that width and then fills it, so the arms differ in the
zeroing and in nothing else.

**Not captured yet.** The code, the benchmark and this page land in one commit and the capture
follows on a clean tree, because a capture taken against a dirty tree records a commit that does not
contain the bytes it measured and can never be located in history afterwards. The numbers go in the
commit after this one, and until they do this section is the only claim this page makes about size:
none.

What the instrument will say is worth writing down first, so that the capture confirms a prediction
rather than supplying one. Two shapes:

- **The saving is real at every width above the smallest, and it does not grow.** Both arms copy
  the same bytes, so the ratio between them is bounded by what writing zeroes costs against what
  reading-and-writing costs, and it should settle rather than diverge.
- **The request pair should be the weaker of the two**, because `alloc_zeroed` sometimes declines to
  write at all, and the response pair the stronger, because `AlignedVec::resize` always writes.

The macro layer is not expected to move and no arm of it is quoted here. O29's own estimate was 0.3%
of a wide insert, which is below what that layer resolves, and a change that cannot be seen there
should say so rather than go looking for a number that agrees with it.

## Tests

| Test | What breaks if this is reverted |
| --- | --- |
| `request_body::tests::a_body_read_in_chunks_holds_every_byte_it_was_sent` | A fill that does not fill. The reader hands over seven bytes at a time, so a read that took its first return for the whole body leaves a tail nobody wrote |
| `request_body::tests::a_body_whose_stream_ends_early_is_an_error_and_not_a_short_read` | A truncated body reaching a shard as uninitialized bytes rather than as a failed connection |
| `request_body::tests::an_empty_body_reads_without_touching_the_reader` | A zero length bundle allocating, or blocking on a read that will never be satisfied |
| `client::tests::a_payload_that_arrives_in_pieces_is_read_whole` | The same on the client, over a 64 KiB payload delivered in 4 KiB pieces with a flush between them. The payload has no zero byte in it, so an unwritten tail is visible rather than plausible |
| `client::tests::a_connection_that_closes_mid_payload_is_an_error` | A half written payload leaving `read_frame` with a tail the allocator supplied |
| `client::tests::the_response_payload_lands_on_a_sixteen_byte_boundary` | The alignment the zero copy read rests on, unchanged by this and re-asserted at seven awkward lengths |
| `client::tests::an_error_frame_does_not_disturb_the_response_read` | The dispatch between the two frame kinds staying between the two reads |

## Related

- [Optimizations](../appendix/optimizations.md) — O29 and O37, the two entries this closes, and the
  corrections it records beside them
- [Row size and what it costs](../tables/row-size.md) — the walk table both of them are on
- [F10](framing-and-protocol-evolution.md) — bounded the allocation and left the zeroing
- [Resolved #34](../appendix/resolved/unvalidated-length-prefix.md) — where the server's half was
  first named, and whose *Still open* section this closes
- [Resolved #79](../appendix/resolved/micro-only-capture-current.md) — the renderer defect that
  stood between this change and a capture of it
- [Request lifecycle](../architecture/request-lifecycle.md) — the path both buffers are on
