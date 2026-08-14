# D4. Encryption in transit

## Context

This is the page where the chapter's central tension is argued in full, because encryption is the
one item here that directly contradicts the thing the current branch exists to provide.

The response path is zero-copy. The socket payload is read into an `AlignedVec<16>` and
`ShoalResponse` holds a pointer into that buffer, so reading a row is a pointer cast rather than a
parse ([The Client](../api/client.md#shoalresponse)). **Every naive way of adding TLS destroys
that**, because a TLS library conventionally owns its plaintext buffer and hands the application a
slice of it — which then has to be copied into aligned memory before rkyv can access it. One full
copy of every row returned, on the path built to have none.

The good news is that this is avoidable, and the way to avoid it is a specific API choice made at
the start rather than an optimization applied later. That choice is what this page is about.

## What exists today

Plaintext, in both directions, with no configuration to change it. See
[D3](authentication.md#what-exists-today) — the two absences are the same absence.

The read that has to be protected:

```rust
let mut aligned_buff = AlignedVec::<16>::with_capacity(len);
aligned_buff.resize(len, 0);
self.reader.read_exact(&mut aligned_buff).await?;
```

`shoal-core/src/client.rs:524-527`, `TcpProxy::start`

The property is that the bytes rkyv will read land, once, in memory the client allocated and
aligned. Any option that cannot preserve that is paying a copy per response.

## The options

| Option | Zero-copy read survives | Handshake | Notes |
| --- | --- | --- | --- |
| **Plaintext** | Yes | — | Only defensible on a network that is trusted *and* isolated. Worth stating as an assumption rather than leaving implied |
| **rustls, buffered API** | **No** | userspace | rustls decrypts into its own buffer and hands out a slice; the client copies into the `AlignedVec` |
| **rustls, unbuffered API** | **Yes** | userspace | Decrypts in place into a caller-supplied buffer — hand it the `AlignedVec<16>` |
| **kTLS** | Yes | userspace | Handshake in userspace, record layer in the kernel, so `read()` returns plaintext into any buffer. NIC offload on capable hardware |
| **WireGuard / IPsec between hosts** | Yes | out of process | Zero application code. No per-connection identity, so [D3](authentication.md) gains nothing |
| **A service mesh sidecar** | Yes, out of process | out of process | Also zero application code, and worse: every packet takes an extra userspace hop through a proxy, which is latency in exactly the place this system cares about |

Three of these need expanding.

### rustls, and why the API choice is the whole decision

rustls exposes two shapes. The conventional one wraps a stream and manages its own plaintext
buffer; the unbuffered one hands the caller each state transition and lets the caller supply the
buffers, including decrypting a record in place. With the unbuffered API the client can decrypt
directly into the `AlignedVec` it was going to allocate anyway, and `ShoalResponse` keeps working
unchanged — the pointer it holds points into aligned memory that happens to have arrived
encrypted.

The cost that remains is real but different in kind: **AES-GCM over every byte**, at hardware rates
on any CPU with AES-NI. For a store returning rows measured in hundreds of bytes, the per-record
overhead — a fixed cost per TLS record, plus the framing — matters more than the throughput term,
which is an argument for larger response frames rather than against encryption.

**The framing interaction is worth stating.** TLS records have a maximum size around 16 KiB, so a
response larger than that spans records and cannot be decrypted in one pass into one buffer without
the client tracking record boundaries. That is exactly what the unbuffered API asks the caller to
do, and it composes with [D2](framing.md)'s length prefix rather than fighting it, but it means the
read loop grows a state machine where it currently has two `read_exact` calls. This is the largest
piece of real work on this page.

### kTLS

Handshake in userspace with rustls, then hand the negotiated keys to the kernel and let the socket
do the record layer. From then on `read()` returns plaintext, into whatever buffer the caller
names — so the current `read_exact(&mut aligned_buff)` works verbatim, and the client-side code
change is nearly zero. On NICs that support it, the record layer offloads to hardware and the CPU
cost approaches plaintext.

Two obstacles. It is Linux-only, which Shoal already is. And **the server side runs on glommio and
io_uring**, which has no kTLS integration — enabling kTLS on a socket is a `setsockopt` glommio does
not expose, and whether it composes with io_uring's submission path is unverified. So kTLS is the
right destination and the wrong starting point.

### Node-level encryption

WireGuard, IPsec, or a mesh sidecar move the problem out of Shoal entirely: no application code, no
handshake, no protocol change. It is genuinely the cheapest option and it should not be dismissed —
for a deployment that already runs one, turning it on is the correct answer and this page's
recommendation is redundant.

It fails on two counts for a general answer. A sidecar proxy adds a userspace hop per packet, which
is latency in the one place a thread-per-core store cannot afford it. And none of the three gives
Shoal a per-connection peer identity, so [D3](authentication.md) gets nothing from them and would
still need SCRAM.

## Recommendation

**rustls with the unbuffered API, decrypting in place into the response buffer. kTLS later.
Configured per listener, defaulting to off.**

| | |
| --- | --- |
| **Rank** | **B**, ahead of [D3](authentication.md) because it decides it |
| **Impact** | Argued — and the cost is the one thing on this page that genuinely needs a number before it lands |
| **Difficulty** | XL — reaches the client's read loop, the server's write path, and the configuration |
| **Depends on** | ~~[D2](framing.md) for the handshake~~ — **satisfied** by [F10](../features/framing-and-protocol-evolution.md); [D1](transport.md) having declined QUIC, which would have supplied this |
| **Blocks** | [D3](authentication.md)'s mTLS path |
| **Tradeoff** | Major — AES over every byte, and a read loop that becomes a state machine |
| **Benchmark** | `transport/*`, unbuilt — and here that is a **blocker**, not a caveat |

Three parts to the recommendation, and the third is as important as the first two.

**Unbuffered rustls, chosen at the start.** Reaching for the buffered API and copying "for now" is
how the zero-copy path would be lost — it would work, it would be measurably slower, and the
measurement that would catch it does not exist. The alignment-preserving version is not
meaningfully harder if it is the first thing written.

**kTLS as a later optimization, not a first implementation.** The handshake code is shared between
the two, so the rustls work is not thrown away when kTLS arrives. Revisit when glommio can enable
it on a socket.

**Per-listener configuration, defaulting to off.** `Networking` grows a TLS section alongside
`interface` and `port` (`shoal-core/src/server/conf.rs:132-138`). This is not a hedge — it is what
keeps the benchmark harness and the integration tests on the plaintext path, so that captures stay
comparable against the frozen `B1-performance` baseline
([Benchmarking](../operations/benchmarking.md)). A build where every capture silently included TLS
would invalidate every number in the book.

## What it costs

- **AES-GCM over every byte in both directions**, at hardware rates, plus a per-record fixed cost
  that is proportionally worse for small responses.
- **A handshake per connection**, up to 50 of them (`client.rs:141-142`) — the same cost
  [D3](authentication.md#what-it-costs) accounts for, and the same mitigation: session resumption.
- **A read loop that becomes a state machine.** Today it is two `read_exact` calls; with unbuffered
  rustls it is a record-boundary-aware loop. That is where a bug would live, and it is on the path
  with the least test coverage in the client
  ([Test Coverage](../appendix/test-coverage.md)).
- **A certificate lifecycle**, which is an operational cost rather than a code one, and which
  [D3](authentication.md) needs anyway if mTLS is taken.

## What it breaks

- **The zero-copy property, if the wrong API is chosen.** This is the whole page. It is listed here
  as something that breaks rather than as a design note because it breaks *silently* — the copied
  version is correct, and nothing in the repository would notice.
- **`AlignedVec::with_capacity(len)` sized from the frame length.** The ciphertext is longer than
  the plaintext by the AEAD tag and the record framing, so the length the client reads and the
  buffer it allocates stop being the same number. Getting that wrong is a subtle sizing bug, and it
  interacts with the frame bound that
  [item 34](../appendix/resolved/unvalidated-length-prefix.md) added — a ciphertext is longer than
  the plaintext it carries, so a record that fits under `max_frame_bytes` in one form may not in
  the other.
- **Comparability of captures**, unless the per-listener default holds. An encrypted capture and a
  plaintext one are not the same measurement, the same rule that already applies to `hotpath` and
  `stage-profile` builds ([Performance Baseline](../operations/performance-baseline.md)).

## Prerequisites

[D2](framing.md), for the handshake and because the record-boundary logic has to be written against
a framing layer that has a bounded length. [D5](runtimes.md)'s crate split, so a client-only crate
can pull rustls without pulling glommio.

## How it would be measured

**This is the one item in the chapter where the missing benchmark is a genuine blocker rather than
a caveat.** Everything else here is a capability question — it either exists or it does not, and
the cost is a startup or a per-connection one. Encryption is a per-byte tax on the hot path, and
the whole recommendation above turns on whether it is a few percent or a large fraction.

What is needed, in order:

1. The `transport/{send_one,send_batched,stream,stream_unordered}` workloads
   ([TODOs](../appendix/todos.md#benchmark-coverage-the-harness-does-not-have)), which would give
   the client a number at all for the first time.
2. `tracing` spans and `hotpath` scopes in `client.rs`
   ([O28](../appendix/optimizations.md#o28-the-client-takes-two-guards-on-its-response-map-for-every-query-it-sends)),
   so the share attributable to the read loop can be separated from the rest.
3. The same workloads run plaintext and encrypted, as a control pair — the pattern
   [F9](../features/ephemeral-tables.md) established for storage, applied to the wire.

A plaintext-versus-TLS pair is a **precondition** for taking this, not a follow-up. Until it
exists, the honest statement is that nobody knows what encryption costs this system.

## Related

- [D1. The transport](transport.md) — QUIC would have supplied this, and why it was declined anyway
- [D2. Framing and protocol evolution](framing.md) — the handshake, and the length field that
  record framing interacts with
- [D3. Authentication](authentication.md) — what mTLS gives once this exists
- [The Client](../api/client.md#shoalresponse) — the zero-copy property being protected
- [Benchmarking](../operations/benchmarking.md) — why the per-listener default matters
