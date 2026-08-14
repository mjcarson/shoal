# D1. The transport

## Context

This question has already been asked once in this repository, and answered twice. The branch
history records it:

```
214a0bf  we need to change to quic I think because udp is not reliable on its own
74c8e24  migrating to tcp instead of udp
3fcb669  mostly converted to tcp with some errors on shutdown and very very ugly
```

The move off UDP was forced — a datagram protocol with no delivery guarantee cannot carry a
request/response store without reimplementing most of TCP badly. QUIC was named as the destination
and TCP was taken as the stop on the way. This page argues that TCP is the destination, and says
what would have to change for that to be wrong.

Everything else in this part depends on the answer. QUIC would supply TLS
([D4](encryption.md)), stream multiplexing ([D6](connection-pool.md)), and connection identity
([D3](authentication.md)) as transport features. Declining it means building each of those on top
of a framing layer that does not exist yet ([D2](framing.md)) — so this is the page that decides
how much work the rest of the chapter is.

## What exists today

TCP, with every shard binding the same port. `Shard::spawn_client_listener` runs on every shard and
binds `interface:port`:

```rust
let tcp_sock = TcpListener::bind(self.conf.networking.to_addr())?;
```

`shoal-core/src/server/shard.rs:422`, `Shard::spawn_client_listener`

glommio's `TcpListener::bind` sets `SO_REUSEPORT`, so all N shards hold the same address and the
kernel load-balances incoming connections across them. `set_nodelay(true)` on both sides
(`client.rs:66` in `ShoalConnectionManager::connect`, `shard.rs:139` in `client_acceptor`), so
nothing waits on Nagle.

Above that, the client does its own multiplexing. A bundle goes out on whichever of up to 50
pooled write halves bb8 hands it, and responses come back demultiplexed by query id through a
shared `papaya` map rather than by the connection they arrived on
([The Client](../api/client.md#split-connections)). **The application layer already provides
independent logical streams over a pool of sockets.** That fact is what decides this page.

## The options

| Option | What it buys | What it costs |
| --- | --- | --- |
| **TCP, as now** | Zero work, kernel-optimal path, `read()` lands bytes wherever the caller says | No TLS, no ping, no shard steering — all of which become D2/D3/D4/D7 |
| **QUIC** (quinn, s2n-quic) | Streams without head-of-line blocking, TLS 1.3 built in, 0-RTT resumption, connection migration, connection IDs | Userspace congestion control and mandatory per-packet AEAD; no glommio integration; ends the zero-copy read |
| **TCP + kTLS** | Encryption with the record layer in the kernel, so `read()` still returns plaintext into the caller's buffer; NIC offload on capable hardware | Handshake still userspace; no glommio path exists. This is [D4](encryption.md), not a transport change |
| **io_uring registered buffers, multishot receive** | Fewer syscalls and no per-read buffer setup on the server side | Server-only, glommio has to expose it, and it changes nothing the client sees |
| **RDMA or a kernel-bypass stack** (DPDK, Seastar's native stack) | The lowest latency achievable, and the one Seastar built for | Requires specific NICs, exclusive device ownership, and an operational story Shoal does not have. Would have to reimplement flow control |

## Recommendation

**Stay on TCP.**

| | |
| --- | --- |
| **Rank** | **D** — declined, and recorded rather than dropped |
| **Impact** | Argued — no measurement of either transport under this workload exists |
| **Difficulty** | XL — a transport change reaches the wire format, both peers, and the deployment |
| **Depends on** | nothing |
| **Blocks** | nothing. Declining it makes [D2](framing.md), [D3](authentication.md), and [D4](encryption.md) into real work rather than free |
| **Tradeoff** | Major — declining QUIC is a bet that the datacenter assumption holds |
| **Benchmark** | `transport/*`, unbuilt. Nothing here has a number |

Three arguments, in order of force.

**QUIC's headline feature is already implemented in userspace.** The reason to want QUIC in a
database is independent streams: one slow response must not block another behind it on the same
socket. Shoal already has that. A bundle's responses are routed by query id through `channel_map`,
across a pool of 50 connections, so a slow query occupies neither a connection nor a position in
anyone else's queue (`client.rs:183-206`, `Shoal::track_response`). Adopting QUIC would replace a
working application-layer multiplexer with a transport-layer one, delete no code — the id-keyed map
is also what makes a *bundle* addressable, which QUIC streams do not do — and pay a per-packet
crypto cost for the privilege.

**QUIC's crypto is mandatory, and it ends the zero-copy read.** There is no unencrypted QUIC. Every
byte is decrypted in userspace into a buffer the QUIC implementation owns and then handed to the
application, so the client can no longer do this:

```rust
let mut aligned_buff = AlignedVec::<16>::with_capacity(len);
aligned_buff.resize(len, 0);
self.reader.read_exact(&mut aligned_buff).await?;
```

`shoal-core/src/client.rs:524-527`, `TcpProxy::start`

That read, and the pointer into it that `ShoalResponse` holds, is the entire zero-copy property of
the response path. Under QUIC the payload arrives in stream-reassembly buffers and has to be copied
into aligned memory before rkyv can access it — one full copy of every row returned, on the path
this branch exists to make copy-free. [D4](encryption.md) shows that TLS *can* be made to avoid
this by decrypting in place into a caller-supplied buffer; QUIC's stream reassembly cannot, because
a stream's bytes may arrive out of order across packets and the assembled result is by construction
a copy.

Add to that userspace congestion control and per-packet ACK processing on a thread-per-core engine
that reserves core 0 for coordination and gives every other core to a shard. GSO, GRO, and NIC
offload narrow the CPU gap against TCP; they do not close it, and they arrive as deployment
requirements rather than as code.

**The deployment target does not need what QUIC is for.** Connection migration matters when a
client's address changes. 0-RTT matters when the handshake is a large fraction of a short
connection's life; Shoal's pool holds connections for up to 1800 s (`client.rs:145`). Middlebox
traversal and ossification-resistance matter on the public internet. Head-of-line blocking at the
transport matters when loss is non-negligible. Inside a datacenter, on a pooled long-lived
connection, none of the four is the thing standing between Shoal and better latency.

## What it costs

Declining QUIC is not free, and the page is not worth writing if it does not say so.

- **TLS becomes real work.** QUIC would have given encryption and peer identity as a property of
  the transport. [D4](encryption.md) is a page long because that gift is being refused.
- **A ping has to be invented.** QUIC has connection-level keepalive and liveness. On TCP,
  detecting a dead peer needs a protocol message, which needs [D2](framing.md) — this is precisely
  what the `client.rs:82` TODO records as blocked.
- **The datacenter assumption is now load-bearing.** It should be stated in the deployment
  documentation rather than assumed, because [D3](authentication.md) and [D4](encryption.md) both
  reason from it.

**The trigger that would reverse this decision**, stated so it can be recognised rather than
re-litigated: *clients outside the datacenter, or a multi-tenant control plane that needs
per-tenant connection identity at the transport.* Either one makes QUIC's properties load-bearing
instead of decorative. Neither is on the roadmap.

## What it breaks

Nothing — this is the recommendation to change nothing. What it *forecloses* is worth listing,
because a later reader will find these missing and should know they were declined rather than
overlooked: transport-level 0-RTT, connection migration across client address changes, and per-query
flow control below the application layer.

## Prerequisites

None. This page exists to be decided first, not built first.

## How it would be measured

It cannot be, today, and this is one of the places where that matters most. Adjudicating TCP
against QUIC needs the `transport/{send_one,send_batched,stream,stream_unordered}` workloads
([TODOs](../appendix/todos.md#benchmark-coverage-the-harness-does-not-have)) *and* a QUIC
implementation to measure them against — the second of which is the thing being decided. So the
recommendation is made on structure rather than on measurement, and it is graded `Argued`
accordingly.

What *could* be measured cheaply, and would inform this without building anything: the share of
end-to-end latency that is socket time at all. `client.rs` has no spans
([O28](../appendix/optimizations.md#o28-the-client-takes-two-guards-on-its-response-map-for-every-query-it-sends)),
but the server side already stamps `mark_socket_written` in `client_tx_relay`
(`shoal-core/src/server/shard.rs:85-127`) and F6's stage breakdown joins client and server stamps.
If the wire is a small fraction of the total, no transport change is worth its risk, and that
number is close to reach.

## Related

- [D2. Framing and protocol evolution](framing.md) — everything TCP does not give for free
- [D4. Encryption in transit](encryption.md) — the TLS work that QUIC would have absorbed
- [D7. Shard-aware routing](shard-aware-routing.md) — `SO_REUSEPORT` steering is a TCP-specific
  obstacle, but QUIC's connection IDs would need eBPF steering instead, so neither transport makes
  shard-awareness free
- [D9. Lessons from other databases](prior-art.md#quic-adopters-and-non-adopters) — who looked at
  QUIC and why most did not move
- [Wire Protocol](../architecture/wire-protocol.md) — the framing this transport carries
