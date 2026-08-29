# F35 — A trace context on the wire

## Context

[Resolved #89](../appendix/resolved/fragmented-query-traces.md) made every span the *server* opens
for one query into one trace — socket read to socket write, with the disk read inside it — and
stopped at the socket deliberately. The client's spans were a second trace in a second process, and
nothing on the wire said the two belonged together, so a slow query showed up in a collector as two
unrelated traces with no way to join them. That page filed the join in
[Todos](../appendix/todos.md) with two possible shapes and a verdict; this is that verdict built.

The gap mattered most where it was hardest to see. A cold get's latency is mostly a disk read, and
the client's view of the same query is a pool wait plus a socket round trip. Neither trace could be
asked *which* of those a slow query spent its time in, because neither contained the other.

## What it does

A request frame can now carry a W3C trace context between its header and its payload. The client
puts the span it is sending from into those bytes, and the server hangs its root span off them, so
a query is one trace from `Shoal::send_stamped` to the rows coming back:

```
Shoal::send_stamped                      the caller: frames the bundle and writes it
├── Shoal::request                       the server: opened when the frame lands
│   └── Coordinator::route               … and the whole server subtree under it
├── Shoal::response                      the caller: one per frame the reader task routes
└── ShoalResultStream::next              one per response handed back
```

Three pieces:

**The wire.** `Flags::TRACE_CONTEXT` (bit 4, the first of the twelve free ones to be spent) says a
26 byte block follows the header: a version byte, a 16 byte trace id, an 8 byte span id and the W3C
trace flags. `request_preamble_traced` builds it, `Header::request_payload_len` takes it back off
the body length, and `PROTOCOL_VERSION` went to 3.

**The client.** Behind the `otel` feature, `current_trace_context` resolves the caller's current
span through `tracing-opentelemetry` and hands its ids to the codec. The two framing sites —
`Shoal::send_stamped` and `ShoalQueryStream::send` — pass whatever it returns.

**The return half.** A query's answers arrive in a detached reader task shared by every query on a
connection, which inherits nothing from any of them. `Waiter` now parks the span the query was sent
in, so `Shoal::response` and both `next()`s are in the trace their send opened — the same move the
server makes with `QueryMetadata.span`.

## Design choices

**The flag bit, not a field on `Queries<S>`.** Both shapes cost a flag day, because
`PROTOCOL_VERSION` is mixed into `SCHEMA_FINGERPRINT` either way. What separates them is what the
*next* optional block costs: a peer that does not know a flag bit round trips it rather than
refusing it, so a second one is a call site instead of another version bump. The rkyv field is also
in the wrong place — it would be inside the archive every shard reads with `unarchive_queries`,
which is the hot path, rather than in a preamble the relay reads once.

**A read of its own, never the front of the body buffer.** The payload is an rkyv archive accessed
in place, and `RequestBody::read_from` allocates exactly the body length so it lands at offset 0.
Twenty-six bytes in front of it would misalign every pointer in the archive. So `decode_request`
stops at the header and the context is a second `read_exact` into a stack array — which is also why
the client's write stays two `IoSlice`s, with `RequestPreamble` holding one fixed size buffer for
both shapes rather than a `Vec`.

**A version byte inside the context.** The frame header already carries a protocol version, so this
is not how the wire evolves. It is there so a decoder can refuse 26 bytes that are not a trace
context, which the flag bit alone cannot tell it.

**`parent: None` stays on `Shoal::request`.** The parent this feature sets is an *OpenTelemetry*
parent, resolved by the OTLP layer; the registry's parent is a separate mechanism and has never
heard of the other process. Removing `parent: None` would let the relay task inherit whatever the
connection task happened to be in the day somebody instruments it — which is what that line was
always guarding against.

**A remote parent, so the client's sampler decides.** The context is built with `is_remote` set,
which a parent based sampler defers to. A trace sampled at one end and not the other is worse than
either answer.

## Alternatives rejected

**A field on `Queries<S>`.** Much less code, and it is the one the todo entry rejected: it moves
`SCHEMA_FINGERPRINT` for a field that is empty whenever nobody is tracing, and it puts the context
inside the archive rather than ahead of it. See above.

**Prefixing the context onto the body buffer.** One read instead of two, and it breaks rkyv's
zero-copy access outright — every archived pointer would be 26 bytes out of alignment. This is the
reason the request preamble is variable length rather than the body being variable shape.

**A 25 byte context with no version byte.** Smaller, and it leaves a decoder unable to tell a
malformed frame from one it does not understand. The byte buys a real refusal.

**Making the client's OpenTelemetry dependency unconditional.** `shoal-core` carries the three otel
crates already, so the server pays nothing new; the client is the half that is meant to build
without an engine, and `shoalctl` and `shoal-client-check` are both in that arm. Gating it is safe
in a way gating `stage-profile` would not be, because the **reading** side is unconditional: a
server understands the flag bit whether or not anything it serves ever sets one, so a client with
the feature off can never desynchronize the two.

**A span per response *in addition to* parking the send's.** Rejected on the same cost argument
[Resolved #89](../appendix/resolved/fragmented-query-traces.md) made: `#[instrument]` defaults to
`INFO`, and a new root span per bundle would be another registry slab insert on the send path. The
send's own span is already there and already covers the write, so it is what gets parked.

## Limitations

**The client's half is off by default.** A client built without `otel` sets no flag bit, writes no
extra bytes and is joined to nothing. That is the arm every deployment that has never configured a
collector is in, and it is why `trace_propagation.rs` carries `#![cfg(feature = "otel")]` rather
than failing without one.

**Which means the feature reaches that test from another crate.** `cargo test -p shoal` compiles
the binary away; `cargo test --workspace` runs it, because `shoal-bench`'s `workloads` feature
enables `shoal/otel` and cargo unifies features across a workspace build. That is a real dependency
and not a happy accident — dropping `shoal/otel` from `shoal-bench` would stop this test running
and nothing would fail.

**`sample_ratio` means something narrower on the server now.** It governs traces that arrived with
no remote parent. A traced client decides for both processes.

**Nothing propagates in the response direction.** The client keeps its own span and needs nothing
from the server, so the response preamble is unchanged. A server that wanted to *add* to a caller's
trace rather than hang off it would need the other half.

**`Shoal::request` still exports shorter than its children.** It is entered over the body read, so
its end time is the read rather than the response. That is inherited from Resolved #89 and is not
made worse here, but a reader of a collector sees the same shape one level up now: a client send
span that ends at the write, with the server's subtree extending past it.

**Parking the span extends its lifetime past the query.** `channel_map` is a `papaya` map, and
removing an entry defers reclamation rather than dropping it, so the span a query was sent in closes
some time after the query ends. Harmless — the trace id is fixed when the span is created — but it
is why `trace_propagation.rs` drops the client before it reads what was exported.

## Invariants to uphold

**A trace context that names no parent is never built.** `TraceContext::new` returns `None` for an
all-zero trace id or span id, and `decode` refuses one. This is the load bearing refusal of the
whole feature rather than a tidiness check: `tracing` turns a parent it cannot resolve into
`Attributes::new_root`, so a zero parent does not produce an orphan somebody would notice — it
silently starts a **new trace**, which is the exact failure a trace context exists to stop.

**`Waiter.span` must never be empty.** The client's half of the same trap. Anything constructing a
`Waiter` outside a send has to supply a real span.

**The context is read separately from the payload, always.** The payload is accessed in place and
has to start at the beginning of its own allocation. A change that folded the context into
`RequestBody` would break every archived pointer in the bundle.

**`Header::request_payload_len` is the only way to size a request body.** It is what subtracts the
context and what refuses a frame too short to hold the one it claims; `body_len()` on a traced
frame is 26 bytes more than the archive.

**Every span on the query path stays at one level.** The two new client spans are `INFO`, like every
other span on that path. A parent a filter can drop independently of its children re-roots all of
them — the whole of [item 90](../appendix/resolved/divergent-layer-filters.md).

**The client and the server pin the same `opentelemetry` major.** Two majors in one process are two
incompatible `SpanContext` types. `shoal-client` names 0.28 and 0.29 because `shoal-core` does.

## Performance

**Not measured, and no capture was taken.** Nothing under `shoal-bench/src/workloads/`, `shoal.yml`
or the seed changed, so no workload fingerprint moved and the corpus still describes the tree.

What it costs, argued:

- **26 bytes per request frame, and only when the client is tracing.** A caller with no
  OpenTelemetry layer resolves an invalid context, builds none, and writes the same eight byte
  preamble it always did — asserted by `an_untraced_preamble_is_byte_identical`.
- **One extra `read_exact` per traced frame**, of a stack array, on a socket that has just been read
  from. Untraced frames read nothing extra, because the flag is what says the bytes are there.
- **Two `INFO` spans per response on the client**, `Shoal::response` and
  `ShoalResultStream::next`. This is the real cost and it is on a per-query path, so it is filed as
  [O45](../appendix/optimizations.md) rather than waved through. `tracing.level` remains the knob:
  the committed `shoal.yml` names `Warn`, at which neither callsite is enabled.

## Tests

| Test | What breaks if the feature is reverted |
| --- | --- |
| `trace_propagation::one_query_spans_the_client_and_the_server` | The whole feature. One trace id across `Shoal::send_stamped`, the five server spans and both client response spans, and `Shoal::request` naming the client's span as its parent rather than merely sharing a trace with it |
| `protocol::tests::a_trace_context_round_trips` | Both ids and the flags surviving the wire in the order they were written — an id shifted by a byte is a parent that resolves to nothing, which is a new trace rather than an error |
| `protocol::tests::a_trace_context_that_names_no_parent_is_refused` | The refusal that stops a zero id from silently starting a new trace, on both the building and the decoding side |
| `protocol::tests::an_unknown_trace_context_version_is_refused` | A decoder being able to tell these 26 bytes from 26 bytes it does not understand |
| `protocol::tests::a_traced_request_preamble_round_trips` | The flag being set, the length counting the context, and the payload length coming back out from under it |
| `protocol::tests::an_untraced_preamble_is_byte_identical` | A caller that is not tracing paying nothing — the same eight bytes, and no flag bit |
| `protocol::tests::a_traced_frame_too_short_for_its_context_is_refused` | `request_payload_len` refusing rather than underflowing into a body of nearly `usize::MAX` bytes |
| `protocol::tests::the_trace_context_flag_is_its_own_bit` | The flag staying bit 4 and colliding with none of the four spent before it |
| `protocol::tests::the_preamble_sizes_are_unchanged` | The untraced preamble staying eight bytes, and the traced one staying thirty four |
| `server::trace::tests::adopting_a_remote_parent_joins_the_peers_trace` | A span handed a peer's context resolving to the peer's trace id rather than one of its own |
| `server::trace::tests::an_unsampled_peer_stays_unsampled` | The sender's sampling decision travelling, rather than being made again on the server |

## Related

- [Resolved #89](../appendix/resolved/fragmented-query-traces.md) — the server's half, and why it
  stopped at the socket
- [Item 90](../appendix/resolved/divergent-layer-filters.md) — the other way a parent goes missing
- [Observability](../operations/observability.md) — what is instrumented, and the trace tree
- [F34](benchmark-tracing.md) — what installs a subscriber, and what a level costs
- [F10](framing-and-protocol-evolution.md) — the header, the flag bits and the version byte this
  spends
- [Wire Protocol](../architecture/wire-protocol.md) — the frame layout
