# 34. The request length prefix is unvalidated

## Symptom

A client could name the server's allocation size. The first eight bytes of a request frame were
read as a `u64` and handed straight to `BytesMut::zeroed` before a byte of the body had arrived,
so a corrupt or hostile prefix asked a shard for up to `usize::MAX` bytes. The read that followed
was `.unwrap()`, and the read before it `panic!`d on any non-EOF error, so a truncated frame took
the shard down rather than the connection — along with every other client that shard was serving.

## Cause

The protocol had no maximum message size, because it had no place to put one. The length field
was the whole header, and a bound that the format does not carry is a number each peer would have
to invent separately and neither could enforce on the other. That is why this was filed as a
format problem rather than as a missing bounds check.

```rust
// parse the upcoming messages size
let len = u64::from_le_bytes(len_bytes) as usize;
// allocate a buffer that is exactly the right size
let mut data = BytesMut::zeroed(len);
```

`shard.rs:66-69`, `client_rx_relay`

## Evidence

**Established by reading the source, then reproduced.** The reading is the four lines above and
their two `panic!` sites, which is what the original entry recorded.

The reproduction is
`a_hostile_length_prefix_closes_one_connection_and_the_server_keeps_serving`, in
`shoal/tests/framing.rs`. It starts a server, proves it answers, opens a raw socket beside the
healthy client, writes a header claiming `u32::MAX` bytes and nothing else, and then asserts two
things: that the raw connection is closed, and that **the healthy client still answers a query**.
The second assertion is what the defect was actually about. Against the unfixed tree the second
half never runs, because the shard the raw socket landed on is gone.

## The fix

Part of [F10](../../features/framing-and-protocol-evolution.md), which replaced the bare length
with an eight-byte header:

```
 ┌─────────┬─────────┬──────────┬────────────────────┐
 │ version │  type   │  flags   │   length (u32 LE)  │
 └─────────┴─────────┴──────────┴────────────────────┘
```

Three things closed this item, and the first is the one that made the other two possible:

1. **The length is a `u32`, and the format has a bound.** `Networking::max_frame_bytes` defaults
   to 64 MiB and is exchanged in the handshake, so each peer knows the other's. Four gibibytes is
   already an absurd frame; the field is paid on every one of *N* response frames per bundle
   rather than once per bundle, and narrowing it is what paid for the version, type and flag bytes.
2. **The check happens in a pure decoder, in front of the allocation.** `decode_request` takes a
   `&[u8; 8]` and cannot allocate even if it wanted to, so there is no path from a length to an
   allocation that does not pass the bound first.
3. **Both relays stopped panicking.** Five `panic!`/`unwrap` sites in `client_rx_relay` and
   `client_tx_relay` became a logged `break`, which ends one connection.

## Alternatives rejected

**A hardcoded maximum on the server side only.** The smaller change, and it would have stopped the
allocation. It would not have stopped the panic, and it leaves the client free to write a bundle
the server will refuse — which the client then discovers as a closed socket rather than as an
error naming both sizes. A bound the format carries is enforceable in both directions.

**A required `max_frame_bytes` key in `shoal.yml`.** `shoal.yml` is committed and is the config
every frozen benchmark was captured against; changing it invalidates the baseline. A serde default
means the file did not have to change at all, and
`a_config_without_a_frame_bound_gets_the_default` is what says so.

**Removing `BytesMut::zeroed` at the same time.** The zeroing is pure waste — `read_exact`
overwrites every byte of it on the next line — and the original entry says so. Removing it needs
`ServerMsg::Client` to stop carrying a `BytesMut`, which ripples into `server/messages.rs` and
`handle_client`, and none of that is about the unbounded allocation. It is now *bounded* waste,
which is the part this item was about, and the rest is filed as
[O29](../optimizations.md#o29-a-request-body-is-zeroed-and-then-immediately-overwritten).

## Invariants to uphold

**The bound is checked before the allocation, not after.** `decode_request` returns
`FrameTooLarge` rather than returning a length for the caller to check, precisely so that there is
no call site that could forget.

**`length` counts every byte after the header.** A decoder that started treating it as a payload
length would make `decode_response`'s underflow check meaningless.

**Neither relay panics.** Both are per-connection tasks on a shard that serves many connections. A
panic in either is not a failed request, it is a failed shard.

**Each side checks its own bound before it allocates and the peer's before it writes.** Dropping
the writer-side check turns a legible error at the caller back into a connection that died.

## Still open

Two things this fix deliberately did not do:

- **A response too large to frame closes the connection with nothing on the wire to say why.** The
  server logs it and drops the client. Better than the panic, but not the fix — that needs an
  error channel, filed as
  [item 61](../known-issues.md#61-a-response-too-large-to-frame-closes-a-connection-silently).
- **`BytesMut::zeroed` still zeroes a buffer `read_exact` immediately overwrites**, now bounded.
  [O29](../optimizations.md#o29-a-request-body-is-zeroed-and-then-immediately-overwritten).

The rest of [item 16](../known-issues.md#16-panics-on-the-hot-path) is also still open — this
removed five of its sites, all in the two relays, and the table there is still long.

## Tests

| Test | What breaks if the fix is reverted |
| --- | --- |
| `shoal/tests/framing.rs::a_hostile_length_prefix_closes_one_connection_and_the_server_keeps_serving` | One peer can name a shard's allocation size and then kill it |
| `shoal/tests/framing.rs::a_frame_of_an_unknown_type_closes_one_connection` | An unknown type byte is read as a length |
| `shared::protocol::tests::a_length_over_the_bound_is_refused` | The bound stops being checked in the decoder |
| `shared::protocol::tests::a_payload_that_does_not_fit_is_refused_on_encode` | A body past a `u32` truncates and the header disagrees with its own bytes |
| `client::tests::a_frame_over_our_bound_is_refused_before_it_is_allocated_for` | The client allocates whatever a server names |
| `server::conf::tests::a_config_without_a_frame_bound_gets_the_default` | A tree with an old `shoal.yml` gets no bound at all |
| `server::conf::tests::a_config_can_set_its_own_frame_bound` | The bound stops being configurable |

## Related

- [F10. Framing and protocol evolution](../../features/framing-and-protocol-evolution.md) — the
  change this was part of
- [Wire Protocol](../../architecture/wire-protocol.md) — the format as built
- [item 16](../known-issues.md#16-panics-on-the-hot-path) — the panics, five of which went with
  this
- [item 61](../known-issues.md#61-a-response-too-large-to-frame-closes-a-connection-silently) —
  what is left of the write side
- [D2. Framing and protocol evolution](../../direction/framing.md) — the design
