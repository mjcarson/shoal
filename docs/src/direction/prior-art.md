# D9. Lessons from other databases

## Context

Every problem in this part has been solved several times, and the solutions disagree with each
other for reasons that are usually about deployment rather than about engineering. This page
records what those reasons are, so that the recommendations on the other eight pages can be checked
against something other than argument.

Each entry says **what to copy** and **what not to**. The second half matters more: the systems
here are mostly older and more general than Shoal, and their designs carry compromises Shoal has
not made yet and should not make by imitation.

## The systems

### ScyllaDB

The closest analogue by a wide margin, and the one to read first. Seastar gives it a shard per
core, a shared-nothing execution model, and an aversion to cross-core traffic — the same shape as
Shoal's glommio shards and `kanal` channels. It also made the same migration Shoal made: **from a
vnode consistent-hash ring to tablets**, ownership stored per tablet rather than derived from a
hash, for the same reasons recorded in
[items 11, 12, 37](../appendix/resolved/tablet-ring.md).

Its drivers are shard-aware, and the mechanism went through three generations that map exactly onto
the options in [D7](shard-aware-routing.md#1-a-way-to-reach-a-chosen-shard): discover the landed
shard and reconnect until every shard is covered; a dedicated shard-aware port; and source-port
selection, where the client chooses a source port that steers it to the shard it wants in a single
connect. It puts its topology metadata under Raft, which is what makes a tablet movable rather than
merely nameable.

**Copy:** the topology push, the stale-routing fallback, and the per-shard port. Also the sequencing
— Scylla shipped tablets before it shipped tablet-aware clients, which is the order
[D7](shard-aware-routing.md) recommends.

**Do not copy:** CQL's per-connection stream id, a signed 16-bit field, which caps in-flight
requests per connection at 32768 and is a constraint drivers work around. Shoal's per-bundle UUID
has no ceiling and does not need one.

### Cassandra

The protocol lesson. Its native protocol frame header is, structurally, what
[D2](framing.md) proposes: a version, flags, a stream id, an opcode, and a length. That header let
Cassandra evolve its protocol across fifteen years and five major versions without a flag day,
because **the very first byte tells a peer whether it can understand the rest**. That is the entire
argument for the version byte, and it is the strongest evidence in this chapter.

Its authentication is SASL over `AUTHENTICATE` / `AUTH_RESPONSE` / `AUTH_CHALLENGE` /
`AUTH_SUCCESS`, a multi-round exchange that [D3](authentication.md) should copy rather than invent.

Topology discovery is a *pull*: clients read the cluster's own system tables. Scylla replaced that
with a push, which is less machinery and lower latency to a correct map.

**Copy:** the header, and the SASL flow. **Do not copy:** the pull-based topology discovery, or the
choice to make the protocol text-adjacent — CQL's string-typed query surface is what SHQL is for on
Shoal, and it is a front end rather than the wire.

### FoundationDB

The strongest lesson for [D6](connection-pool.md) and [D8](typed-queries.md), and it is about where
complexity should live. FoundationDB's client library is deliberately fat: it owns routing, retry,
and transaction bookkeeping, and the server is correspondingly simpler. That is the opposite of the
current Shoal split, where the coordinating shard does the routing and the client does almost
nothing.

Its retry model is the part worth internalising. **FoundationDB retries the transaction, not the
request** — the retry loop is the API's central idiom — which is why it can retry anything safely.
Shoal has no transaction, so [D6](connection-pool.md) cannot inherit that and has to divide its
queries into idempotent and not. Recording *why* the easy answer is unavailable is more useful than
the answer would have been.

The second lesson is testing. FoundationDB's deterministic simulation — the whole system run
against a simulated network, disk, and clock, with injected failures — is what lets it claim
correctness under partition and process death. Shoal's client has
[no test at all](../appendix/test-coverage.md#the-streaming-client-apis) for its streaming APIs,
which is how [item 60](../appendix/known-issues.md#60-a-result-stream-that-is-not-drained-to-the-end-leaks-its-slot-in-the-client)
went unnoticed. **Of everything on this page, a harness that can kill a connection underneath an
in-flight query is the single most valuable thing to copy.**

**Do not copy:** the architecture wholesale. FoundationDB's layered design and its coordinator/proxy
topology solve a distributed-transaction problem Shoal does not have.

### Aerospike

The purest comparison on this page for deployment shape: a datacenter store, latency-obsessed,
explicitly not built for the edge. Two things it settled that this chapter is re-deriving.

**The partition map lives in the client.** Aerospike clients hold the full map, refresh it on a
background interval, and route every single-key operation directly to the owner. It is the proof
that [D7](shard-aware-routing.md)'s model works at scale rather than merely in theory. Its
namespaces are cut into **4096 partitions** — the same number as Shoal's tablets, arrived at by the
same reasoning: far larger than any node count so the split is even, small enough that the map is
trivially resident (`shoal-core/src/server/ring.rs:27-31`).

**A fixed header with a version and a type**, eight bytes, ahead of every message. Which is
[D2](framing.md), independently derived by a system with the same constraints.

**Do not copy:** the tend-interval refresh. Polling was the right answer before there was a way to
push on an existing connection; [D2](framing.md) provides one.

### Redis Cluster and RESP3

The counter-example on routing, and the source of [D7](shard-aware-routing.md)'s safety property.
Redis Cluster clients route by hash slot, and when they are wrong the **server corrects them**:
`MOVED` says the slot has permanently relocated, `ASK` says it is mid-migration. Client-side routing
is an optimization over a path that stays correct — which is exactly the degradation
[D7](shard-aware-routing.md#5-staleness-which-is-what-makes-it-safe) requires, and the reason
token-aware clients are safe to deploy at all.

RESP3's push messages are the model for topology updates arriving on a connection that is also
carrying queries, rather than through a side channel or a poll.

**Do not copy:** the protocol's text orientation. RESP is human-readable, which is a real
operational virtue for a system whose clients are hand-written — and worth nothing here, where
every client is generated from a schema and the payload is a zero-copy archive.

### TiKV and gRPC

What it costs to buy a general-purpose RPC layer. gRPC gives multiplexing, deadlines, flow control,
TLS, and auth for free — very nearly the list [D2](framing.md) through [D6](connection-pool.md) are
proposing to build by hand. It also gives a per-RPC overhead that cannot be removed, which is why
TiKV added a `BatchCommands` layer whose only purpose is to amortize it by packing many requests
into one call.

**That is the evidence for Shoal's raw framing.** A store measuring microseconds cannot afford a
protocol stack it did not choose, and the moment it batches to hide that stack's overhead it has
rebuilt the multiplexing the stack was supposed to provide. Shoal already batches for its own
reasons; it should not also be batching to pay for HTTP/2.

**Copy:** nothing directly. **Note:** the temptation to adopt gRPC will recur every time
[D2](framing.md) looks like work, and this is the answer.

### DragonflyDB

The honest counterweight to [D7](shard-aware-routing.md), and the entry most likely to be skipped by
someone who has already decided. Dragonfly is thread-per-core and shared-nothing — the same
execution model — and it deliberately **does not** expose shard-awareness to clients. Its argument
is that moving work between cores inside one process is cheap enough that pushing the topology to
every client is complexity that is not repaid.

Shoal's situation is closer to Dragonfly's than the resemblance to Scylla suggests. Scylla's clients
are usually on other machines, its shard counts are large, and the hop it saves crosses a network.
The hop [D7](shard-aware-routing.md) would save is a `kanal` send between two cores of the same box
(`shoal-core/src/server/comms.rs:46-58`). **That is the reason D7 is ranked last and gated behind a
measurement.**

### Kafka

The most permissive protocol-evolution model in wide use: every request carries an API key and a
version *for that key*, and a client asks the broker which versions it supports before using one.
A cluster can be upgraded underneath clients indefinitely, one message type at a time.

It is also far more machinery than Shoal needs, and the reason is deployment: Kafka's clients are
written by other people, in other languages, on other release cycles. Shoal's client and server are
generated from the same schema and usually built from the same commit.

**Copy:** nothing now. **Revisit** if Shoal ever ships a client independently of a server — that is
the condition that makes per-message versioning worth its cost, and it should be recognised rather
than re-argued.

### Postgres and MongoDB

Both landed on **SCRAM-SHA-256** for password authentication, from different directions and after
trying other things. It is the boring correct answer, and [D3](authentication.md) should not spend
design effort re-deciding it: the server stores a salted iterated derivation, the client proves
knowledge without transmitting the password, and the exchange authenticates the server to the client
as well.

Postgres is also worth noting for what it does *not* do: no client-side routing, because there are
no shards to route to. That is a reminder that half this chapter exists only because Shoal is
partitioned.

### QUIC adopters and non-adopters

HTTP/3 moved. Databases largely did not, and the reason is consistent across the ones that
evaluated it: **userspace QUIC costs materially more CPU per byte than TCP**, because congestion
control, loss recovery, and per-packet AEAD all run in the application. GSO, GRO, and NIC offload
have narrowed that gap and have not closed it, and they arrive as deployment requirements rather
than as code.

The properties QUIC sells — connection migration, 0-RTT over lossy paths, resistance to middlebox
ossification, transport-level stream independence — are edge properties. Every one of them is worth
less inside a datacenter on a long-lived pooled connection. That is the whole of
[D1](transport.md)'s argument, and the industry's split along exactly the edge/datacenter line is
the evidence for it.

## The comparison

| System | Transport | Framing | Auth | Encryption | Client routing | Typed client |
| --- | --- | --- | --- | --- | --- | --- |
| **Shoal, today** | TCP | Length prefix, no version or type | None | None | None | Runtime enum match |
| **Shoal, proposed** | TCP ([D1](transport.md)) | 8-byte header, versioned ([D2](framing.md)) | mTLS or SCRAM ([D3](authentication.md)) | rustls in place ([D4](encryption.md)) | Tablet map, pushed ([D7](shard-aware-routing.md)) | `Query::Response` ([D8](typed-queries.md)) |
| **ScyllaDB** | TCP | CQL, versioned | SASL | TLS | Shard- and token-aware | Generated, per driver |
| **Cassandra** | TCP | Native protocol, versioned | SASL | TLS | Token-aware, pulled | Generated, per driver |
| **FoundationDB** | TCP | Custom binary | TLS identity | TLS | In the client library | Layer-dependent |
| **Aerospike** | TCP | 8-byte header, versioned | Internal or external | TLS | Full partition map, polled | Per client |
| **Redis Cluster** | TCP | RESP, text-oriented | AUTH / ACL | TLS | Slot map, with `MOVED` fallback | None |
| **TiKV** | HTTP/2 (gRPC) | protobuf | TLS | TLS | Via placement driver | protobuf-generated |
| **DragonflyDB** | TCP | RESP | AUTH | TLS | **Deliberately none** | None |
| **Kafka** | TCP | Per-API-key versioning | SASL | TLS | Partition leader map | Per client |

Two things stand out from the table.

**Every system here versions its framing.** Shoal is the only row with no version field, and it is
the only row where a peer mismatch is undefined behaviour rather than a refused connection. That is
[D2](framing.md)'s ranking justified by consensus rather than by argument.

**Client-side routing is not universal.** Four of the nine do it in a form Shoal would recognise,
one refuses on principle, and the rest sit in between. It is the one item in this chapter where the
prior art genuinely disagrees, which is precisely why [D7](shard-aware-routing.md) is gated behind a
measurement instead of a precedent.

## Related

- [D1. The transport](transport.md) — the QUIC decision this page's last entry supports
- [D2. Framing and protocol evolution](framing.md) — Cassandra's and Aerospike's headers
- [D3. Authentication](authentication.md) — Cassandra's SASL flow, Postgres and MongoDB on SCRAM
- [D6. A production connection pool](connection-pool.md) — FoundationDB on retries and on testing
- [D7. Shard-aware routing](shard-aware-routing.md) — Scylla for, Dragonfly against
- [items 11, 12, 37](../appendix/resolved/tablet-ring.md) — Shoal's own vnodes-to-tablets migration
