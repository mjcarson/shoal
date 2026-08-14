# F10. Framing and protocol evolution

## Context

The wire protocol had two frames, four hardcoded call sites, no shared constant, and no module of
its own. It carried no version, no message type, no bound on its length, and no way for a peer to
say which schema it was compiled from. Everything in this list was blocked on the same eight
bytes:

| Wanted | Needs a frame that says |
| --- | --- |
| A pool health check that detects a dead peer | `Ping` / `Pong` |
| A client proving who it is | `Auth` / `AuthResponse` |
| A client learning which shard owns which tablet | `Topology` |
| A server saying a read failed rather than returning nothing | `Error` |
| A server draining a connection before it closes | `GoAway` |
| A client abandoning a query it will never read | `Cancel` |

This is [D2](../direction/framing.md), which ranked it **A1** — four other pages in that chapter
cannot start without it, and it is a flag day whose only deployments today are the integration
tests, `shoal-bench` and `shoalctl`, all compiled from this tree. That is the cheapest it will
ever be.

Three things in `framing.md` turned out to be wrong against the code, and the first of them
changes the argument for the whole change:

**The header costs zero bytes, not eight.** The old request frame was an 8-byte `u64` length; the
new one is the 8-byte header. The old response preamble was 16 bytes of query id plus an 8-byte
`u64` length; the new one is 8 bytes of header plus 16 bytes of query id. Narrowing the length to
a `u32` pays for the version, the type and the two flag bytes exactly, with nothing left over.
`framing.md`'s "800 bytes per 100-query bundle" is 0 bytes, and the frozen performance baseline is
not invalidated by frame size. `the_preamble_sizes_are_unchanged` is where that claim lives as an
assertion rather than as prose.

**`wire_codec` cannot catch a misaligned payload.** `framing.md` says the benchmark "is the only
thing that would catch a header that accidentally made the payload unaligned". It is not: a
criterion benchmark measures nanoseconds, and a misaligned rkyv access is a `bytecheck` failure or
undefined behaviour rather than a slowdown. The benchmark was built anyway, because
[O1](../appendix/optimizations.md) and [O2](../appendix/optimizations.md) are blocked on it. The
alignment guard is a test.

**`framing.md` never says what `length` counts.** It counts every byte after the header,
*including* a response frame's query id. That is what lets a peer skip a frame whose type it does
not know, which is the only reason a type byte is worth carrying.

## What it does

Every frame in both directions now starts with the same eight bytes:

```
 ┌─────────┬─────────┬──────────┬────────────────────┐
 │ version │  type   │  flags   │   length (u32 LE)  │
 │  (1 B)  │  (1 B)  │  (2 B)   │       (4 B)        │
 └─────────┴─────────┴──────────┴────────────────────┘
 request  : [header][rkyv Queries]
 response : [header][query id 16 B][rkyv ResponseKinds]
```

Twelve message types are defined; four are wired. `Hello`, `HelloAck`, `Queries` and `Response`
are constructed today. `Auth`, `AuthResponse`, `Ping`, `Pong`, `Topology`, `Error`, `GoAway` and
`Cancel` exist as reserved discriminants, so that the features that need them are a call site
rather than a second flag day.

A connection opens with a handshake. The client writes a `Hello` naming the protocol version, a
64-bit fingerprint of the schema it was built from, and the largest frame it will accept. The
server answers with a `HelloAck` carrying the same three things — and either accepts, or refuses
with a reason. A refusal is still a `HelloAck`, so a client that was turned away learns why
instead of seeing a reset.

The fingerprint is computed at compile time by the derive macros, folding table names and order,
each row's field names, declared types, archived sizes and alignments, field positions and query
roles, each declared projection, and the protocol version. Two peers built from schemas that
differ anywhere in that list refuse each other by name.

A configured `max_frame_bytes` bounds every frame. Each side checks its own bound before it
allocates and the peer's bound before it writes, which closes
[item 34](../appendix/resolved/unvalidated-length-prefix.md) on both sides at once.

The four hardcoded call sites became one module, `shoal-core/src/shared/protocol.rs`, with one
encoder and one decoder and the sizes as constants.

## Design choices

**The header layout is fixed for all protocol versions.** The version byte is at offset 0 and the
length is a little-endian `u32` at offsets 4..8, and neither ever moves. This is what lets a peer
read a frame written in a version it does not speak, report which version it saw, and drain
exactly the right number of body bytes before replying. Without it the version byte would be
decorative, because a peer that cannot parse the rest of the header cannot resynchronize —
`a_hello_of_an_unsupported_version_is_refused_with_an_ack` is the test that depends on it.

**Message type discriminants are explicit and start at 1.** Two reasons, both of which a future
change has to keep: a zeroed buffer must never decode as a valid message type, and inserting a
variant must never silently renumber the wire.
`every_message_type_round_trips_through_its_discriminant` turns an insertion into a test failure
rather than a compatibility break.

**Unknown flag bits are preserved, never rejected.** That is the entire mechanism by which a bit
can be spent without bumping the version byte. A decoder that masked unknown bits off would turn
the first use of bit 4 into a break.

**The handshake bodies are fixed bytes, not rkyv archives.** The whole purpose of the exchange is
to detect that the peer's schema — and with it, potentially, its rkyv layout — does not match
ours. Decoding it with rkyv makes the detector depend on the thing it detects: a peer built
against a different rkyv would report "corrupt archive" rather than "your schema is different", or
at worst mis-read. Fixed bytes also mean the handshake has a known size before it is read and no
alignment requirement at all. Cassandra, Postgres and Kafka all use fixed bytes for the frame that
negotiates the version, for the same reason. `framing.md` is silent on this and should not have
been.

**The fingerprint mixes archived size and alignment alongside the spelling of each type.** The
spelling alone can lie. A type alias whose definition changes from `u32` to `u64` keeps every
declaration reading `Id`, and that is the one dangerous direction — two peers agreeing when they
should not. Two spellings of the same type disagreeing is the safe direction and is left as is.
`widening_a_type_behind_an_alias_changes_the_fingerprint` is what says so.

**The fingerprint lives on `QuerySupport`.** It is the only trait both peers see: the client is
`Shoal<S: QuerySupport>`, and the server reaches the identical constant through
`<D as ShoalDatabase>::ClientType`. It is required rather than defaulted, so a hand-written
implementation cannot silently opt out of the only check that catches this failure. The strongest
compile-time guarantee available to this system is a runtime handshake field.

**The two bounds are exchanged, not configured.** `Shoal::new` takes an address and has no config
object, so there is no place to configure a client-side bound. The server's bound comes from
`conf.networking.max_frame_bytes`; the client's is a compile-time constant. Each side learns the
other's in the handshake.

**`max_frame_bytes` has a serde default.** `shoal.yml` is committed and is the config every frozen
benchmark was captured against. A required key would have invalidated that baseline for a setting
nobody has ever needed to change.

**The whole per-connection sequence runs in a task of its own.** The handshake, the broadcast and
both relays. A handshake done inline in the accept loop would let one client that connects and
then says nothing park that loop — and with `cores: 1`, which
`utils::build_single_shard_config` sets, that is every subsequent connection to the server.

**The connection's write relay is owned by the task that runs its read relay.** The two halves of
a split stream keep the stream alive between them, so a read relay that ends on its own leaves the
write relay parked on an empty channel holding a socket nobody will ever read from again. This was
found by a test that hung rather than by reading.

**The protocol module knows nothing about any async runtime.** The server reads with glommio and
the client with tokio, and the two share every decision here and none of the I/O, because there is
no I/O left to share: each call site is a `read_exact` of a fixed-size array, one pure call into
the module, and a `read_exact` of the body. The module's whole dependency list is `core` and
`uuid`, so it moves into a client-only crate under [D5](../direction/runtimes.md) unchanged. That
also means D5's crate split was *not* a prerequisite, contrary to `framing.md`.

## Alternatives rejected

**Kafka's per-API-key versioning.** Every request carries an API key and a version for that key,
so a cluster can be upgraded while old clients keep working, indefinitely and granularly. It is
the most permissive evolution model in wide use, and far more machinery than a system whose client
and server are built from the same commit needs. Recorded because if Shoal ever ships a client
independently of a server, this is the model to revisit.

**A self-describing envelope — protobuf, CBOR, or MessagePack — around the rkyv payload.** Buys
introspectable frames and mature tooling. Costs a parse on every frame, on the path whose whole
value is that there is no parse.

**Negotiating an rkyv layout version rather than a protocol version.** Tempting, because the real
hazard is a layout mismatch. It couples the wire contract to a dependency's internal versioning
and it does not catch the actual failure — two peers on identical rkyv with different *schemas*.
The fingerprint catches that; a layout version does not.

**A length-delimited stream framing crate (`tokio-util`'s `LengthDelimitedCodec`).** Would replace
the client's hand-rolled read loop with a maintained one, but it owns its buffers, which is the
alignment problem below, and the server side is glommio and cannot use it anyway.

**Adding the type byte now and the version byte later.** The expensive part is the flag day, and
it is paid per break rather than per field.

**A defaulted `SCHEMA_FINGERPRINT` on the traits.** Would have avoided touching three hand-written
test implementations. A default of `0` lets a future hand-written implementation opt out of the
guarantee by doing nothing, which is exactly the failure mode this is trying to remove.

**Draining nothing before refusing a handshake.** The refusal closes the connection anyway, so
draining looks pointless. It is not: closing a socket that still has unread bytes queued sends a
reset, which discards the very reply the server went to the trouble of composing.

## Limitations

**A response too large to frame closes the connection with nothing on the wire to say why.** The
server logs it and drops the client. This is strictly better than the panic it replaced, which
took the shard and every other client on it, but it is not the fix — the fix is an error channel,
which is out of scope here and filed as
[item 61](../appendix/known-issues.md#61-a-response-too-large-to-frame-closes-a-connection-silently).

**A 64-bit fingerprint can collide.** Two different schemas that hash the same would shake hands
and exchange archives of mismatched layout — the exact undefined behaviour the fingerprint exists
to prevent, now with a false sense of safety. Three things make this acceptable rather than
alarming: `bytecheck` remains the second line of defence on both paths and catches most structural
differences even when the fingerprint agrees; the input space is a handful of schemas per
deployment rather than an adversarial one; and **a hostile peer can trivially forge a
fingerprint**. This is a mistake detector, not authentication. Authentication is
[D3](../direction/authentication.md).

**Eight of the twelve message types are defined and unwired.** `Ping` and `Pong` exist but
`is_valid` still calls `peer_addr`; `Cancel` exists but a dropped result stream still leaks its
slot; `GoAway` exists but nothing drains. Those are their own features, and the point of defining
the discriminants now is that none of them is a flag day.

**The handshake proves nothing about identity.** Any peer can send any fingerprint. See D3.

**`BytesMut::zeroed` still zeroes a buffer that `read_exact` immediately overwrites.** It is now
*bounded* waste, which is the part item 34 was about, but it is still waste. Removing it needs
`ServerMsg::Client` to stop carrying a `BytesMut`, which ripples into `server/messages.rs` and
`handle_client`. Filed as [O29](../appendix/optimizations.md#o29-a-request-body-is-zeroed-and-then-immediately-overwritten).

**A client learns only one server's frame bound.** `peer_max_frame_bytes` is a single value shared
across the pool, so a pool spanning servers configured differently would keep whichever bound was
learned last. Today every connection in a pool goes to one address, so this cannot happen yet; it
becomes real with [D7](../direction/shard-aware-routing.md)'s per-shard endpoints.

## Invariants to uphold

**The client's two-read structure must not be merged into one read.** The client reads its
preamble into a stack array and its payload into a freshly allocated `AlignedVec<16>`. That is
what puts the archive at offset zero of a sixteen-byte-aligned allocation, which is what makes
turning it into a response a pointer cast rather than a parse. A single read of preamble plus
payload lands the payload at offset 24, and offset 24 of a sixteen-byte-aligned allocation is
never itself sixteen-byte aligned. Merging the two `read_exact` calls looks like an obvious
optimization, which is exactly why
`the_response_payload_lands_on_a_sixteen_byte_boundary` parameterises over seven awkward payload
lengths rather than one.

**The eight header bytes mean the same thing in every protocol version.** A future version may add
meaning to the flag bits or change what follows the header. It may not move the version byte or
the length field.

**`length` counts everything after the header, including the response's query id.** A decoder that
started treating it as the payload length would make `decode_response`'s underflow check
meaningless and every response frame sixteen bytes short.

**Message type discriminants are never renumbered and never reused.** Append.

**Unknown flag bits are round-tripped.**

**The client always speaks first.** The client writes `Hello` then reads; the server reads `Hello`
then writes. If both waited to read, every connection would deadlock and nothing in the frame
layout would show it.

**The handshake completes before the read half is handed to the proxy.** Moving it after
`into_split()` is the natural-looking refactor, and it would send the `HelloAck` down the response
path, where it decodes as a response to a query nobody sent and hits the "missing stream channel"
branch.

**`connect` stays under a deadline.** `bb8`'s connection timeout bounds its retry loop and
`pool.get()`, not `connect` itself. Before the handshake, `connect` could not block at all,
because it neither read nor wrote. It can now, and the `tokio::time::timeout` around it is the
only thing standing between a stalled server and a `Shoal::new` that never returns.

**A refused handshake is answered before the socket is closed, and the body is drained first.**
See *Alternatives rejected*.

**The protocol module depends on `core` and `uuid` and nothing else.** Adding a runtime dependency
to it — even an async trait that names one — is what would stop it moving under D5.

## Performance

The frame is the same size it was, so there is nothing to compare against the baseline. The new
cost is one header encode per frame written and one header decode per frame read, both of which
are branch-and-shift over a fixed eight-byte array with no allocation.

`wire_codec`, the criterion benchmark this change built, is what would show otherwise. It measures
the header alone, the request path at 1/10/100 queries per bundle, and the response path at
16/256/1024/4096 rows — the same row counts `partitions.rs` uses, so the two can be read against
each other.

Two of its groups exist for reasons other than this change. `wire_codec/request/decode` runs the
validated `access` and the unchecked `access_unchecked` as separate functions, and the difference
between them is what `bytecheck` costs on the request path — the question
[O1](../appendix/optimizations.md) is blocked on. `wire_codec/response/encode` at four row counts
is [O2](../appendix/optimizations.md)'s. Building the benchmark once answers three questions.

The handshake adds one round trip per connection, paid in `Shoal::new` and in `bb8`'s replacement
of a connection, never on a query path. With `min_idle(10)` that is ten round trips at client
construction, against a `connection_timeout` of five seconds.

## Tests

| Test | What breaks without the feature |
| --- | --- |
| `shared::protocol::tests::every_message_type_round_trips_through_its_discriminant` | A variant inserted into `MessageType` renumbers the wire silently |
| `shared::protocol::tests::the_preamble_sizes_are_unchanged` | The claim that the header costs zero bytes stops being checked |
| `shared::protocol::tests::flag_bits_are_stable` | A flag constant moves and an older peer misreads it |
| `shared::protocol::tests::unknown_flag_bits_are_preserved` | The first use of a new flag bit becomes a compatibility break |
| `shared::protocol::tests::a_length_over_the_bound_is_refused` | A peer names its own allocation size again |
| `shared::protocol::tests::a_response_shorter_than_its_query_id_is_refused` | `payload_len` underflows on a short frame |
| `shared::protocol::tests::a_payload_that_does_not_fit_is_refused_on_encode` | A body length past a `u32` truncates and the header disagrees with its own bytes |
| `shared::protocol::tests::an_unknown_version_is_refused` | A peer from another version is mis-parsed instead of refused |
| `shared::protocol::tests::a_header_of_an_unknown_version_is_still_readable` | The version byte becomes decorative |
| `shared::protocol::tests::the_separator_stops_concatenation_colliding` | Renaming a pair of adjacent fields passes the handshake |
| `shared::protocol::tests::widening_a_type_behind_an_alias_changes_the_fingerprint` | A type alias that widened is a false agreement |
| `client::tests::the_response_payload_lands_on_a_sixteen_byte_boundary` | The two client reads are merged and the zero-copy response path ends silently |
| `client::tests::a_frame_over_our_bound_is_refused_before_it_is_allocated_for` | The client allocates whatever a server names |
| `shoal/tests/framing.rs::a_hostile_length_prefix_closes_one_connection_and_the_server_keeps_serving` | One peer can kill a shard and every client on it |
| `shoal/tests/framing.rs::a_frame_of_an_unknown_type_closes_one_connection` | An unknown type byte is read as a length |
| `shoal/tests/framing.rs::a_response_frame_sent_to_the_server_closes_one_connection` | The direction of travel is inferred rather than declared |
| `shoal/tests/framing.rs::a_hello_of_an_unsupported_version_is_refused_with_an_ack` | A version mismatch becomes a reset instead of a message |
| `shoal/tests/framing.rs::a_hello_naming_a_different_schema_is_refused_with_an_ack` | The server stops naming its own fingerprint in a refusal |
| `shoal/tests/handshake.rs::a_client_built_from_a_different_schema_is_refused` | Two peers built from different schemas exchange archives |
| `shoal/tests/handshake.rs::a_matching_client_can_still_query` | The handshake refuses everybody and the test above still passes |
| `shoal/tests/fingerprint.rs::a_reordered_row_changes_the_schema_fingerprint` | The case `bytecheck` cannot catch stops being caught |
| `shoal/tests/fingerprint.rs::a_declared_projection_changes_the_schema_fingerprint` | A projection added to a table leaves the fingerprint where it was |
| `shoal/tests/fingerprint.rs::the_identity_projection_borrows_its_tables_fingerprint` | A row and its own projection describe different schemas |
| `server::conf::tests::a_config_without_a_frame_bound_gets_the_default` | `shoal.yml` needs a new key and the frozen baseline is invalidated |

## Related

- [Wire Protocol](../architecture/wire-protocol.md) — the format as built, in detail
- [D2. Framing and protocol evolution](../direction/framing.md) — the design this came from, and
  the three places it was wrong
- [Resolved #34](../appendix/resolved/unvalidated-length-prefix.md) — the unbounded allocation this
  closed
- [D3](../direction/authentication.md), [D4](../direction/encryption.md),
  [D6](../direction/connection-pool.md), [D7](../direction/shard-aware-routing.md) — the four
  pages this unblocks
- [D5. Runtimes and crate structure](../direction/runtimes.md) — where the protocol module goes
  next, and why it can move unchanged
- [F5. Validated archives](validated-archives.md) — `bytecheck`, which is the second line of
  defence the fingerprint sits in front of
