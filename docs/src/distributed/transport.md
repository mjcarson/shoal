# C2. The inter-node transport

## Context

Every shard can already reach every other shard, and it does so through one enum with one variant.
This page adds the second variant. It is the seam [TODOs](../appendix/todos.md#distribution) named
— "Adding a `Remote` variant is the seam" — and the page is longer than that sentence because the
variant drags three questions behind it: who owns a connection to another node, how a message that
is a `Bytes` and a `Span` on one machine becomes a frame on the wire, and what happens to the rule
that a shard replies to a client directly when the client's socket is on another machine.

## What exists today

**One variant, one arm.**

```rust
pub enum ShardContact {
    /// This shard is on our current node
    Local(usize),
}
```

`shoal-core/src/server/shard.rs:824`

```rust
match contact {
    ShardContact::Local(shard) => match self.shards.get(*shard) {
        Some((tx, _)) => tx.send(msg).await?,
        None => panic!("Who is {contact:#?}"),
    },
}
```

`shoal-core/src/server/comms.rs:40-53`

`Comms` holds one unbounded `kanal` channel pair per shard, cloned into every shard
([Thread per Core](../architecture/thread-per-core.md#the-channel-mesh)). `ShardInfo::mesh_id`
(`shard.rs:853`) unwraps the `Local` index and is what routing hands back.

**What crosses the mesh.** `ServerMsg` (`shoal-core/src/server/messages.rs:178`) has eleven
variants. Four of them are what a distributed query path is made of, and the shape of each decides
how it goes on a wire:

| Variant | Carries | Crosses a node? |
| --- | --- | --- |
| `Query { meta, body: Bytes, offset, keys }` | A validated bundle by refcount, an index into it, and the keys this shard owns | **Yes** — this is a forward |
| `Gathered { meta, response }` | One shard's share of a split query, back to the splitter | **Yes** — a share may come from another node |
| `NewClient { client, client_tx }` | The sender half of a client's socket relay | **No** — a `kanal` sender cannot leave the process |
| `Partition(..)` | A glommio `ReadResult` | **Never** — the `Send` invariant, [Thread per Core](../architecture/thread-per-core.md#the-send-escape-hatch) |

`QueryMetadata` (`messages.rs:20`) carries the client id, the bundle id, the index, `end`, a
`gather: Option<ShardContact>` (`messages.rs:35`), a `Span`, and the stage stamps. Every field but
the span and the stamps is plain data; the span is what [F35](../features/wire-trace-context.md)
already knows how to put on a wire.

**The coordinator routes without deserializing** (`Shard::send_to_shard`, `shard.rs:1204`,
[F26](../features/archive-routed-requests.md)): it freezes the request into a `Bytes`, validates
it once with `Queries::access`, and sends each owning shard a clone of the refcount plus an offset
and its keys. The owner reads its query out with `unarchive_queries`, which is `unsafe` because it
skips validation on the strength of the coordinator having done it
([Wire Protocol](../architecture/wire-protocol.md#validation)).

**Responses bypass the coordinator.** `NewClient` is broadcast to every shard on the node
(`shard.rs:776`), each stores the sender in `client_map` (`shard.rs:1795`), and the owning shard
writes to the client's socket relay directly ([Request Lifecycle](../architecture/request-lifecycle.md#design-notes)).
Only a *split* query goes back through the splitter, as `Gathered`, to be merged
(`Shard::handle_gathered`, `shard.rs:1648`).

**The framing module is runtime-free and has room.** Twelve message types, five of them reserved
and unwired; sixteen flag bits, five claimed; a 26-byte trace context behind a flag
([Wire Protocol](../architecture/wire-protocol.md#the-header)). The module depends on `core` and
`uuid` and nothing else, which is why the same encoder and decoder can serve a glommio server and a
tokio client — and a glommio peer.

**TLS is already in the kernel.** rustls does the handshake and kTLS does the record layer
([F14](../features/encryption-in-transit.md)), and the handshake already produces a peer
certificate that nothing yet checks against a CA for identity.

## The design

### The second variant

```rust
pub enum ShardContact {
    /// This shard is on our current node
    Local(usize),
    /// This shard is on another node, reached over that node's peer listener
    Remote { node: NodeId, shard: u16 },
}
```

`mesh_id()` stays, and stays valid only for `Local`; every caller that has a `Remote` in hand wants
the pair, not an index. `ShardInfo` grows nothing — its `contact` is where the node lives.

### Who owns a connection

**The sending shard does.** Each shard holds, in a field of its own, one outbound peer connection
per remote node — opened lazily on the first `Remote` send to that node, kept for the life of the
shard, and re-opened on failure. With `S` shards on a node and `N` nodes in the cluster, a node
holds `S × (N − 1)` outbound peer connections, and nothing about any of them is shared across
threads. That is the thread-per-core rule applied to sockets, and it is the only design on this
page that does not put a lock on the data path.

```
 node A                                          node B
 ┌─────────┐                                     ┌─────────┐
 │ shard 0 │──── A0→B ───────────────────────────▶│ accept  │──kanal──▶ shard 3 (the frame names it)
 │ shard 1 │──── A1→B ───────────────────────────▶│ (any)   │──kanal──▶ shard 7
 │ shard 2 │──── A2→B ───────────────────────────▶│         │
 └─────────┘                                     └─────────┘
   each connection is one shard's, on both ends: the sender's to write, the acceptor's to read
```

**Inbound is shared-nothing too, one hop later.** Every shard binds `cluster.port` with
`SO_REUSEPORT`, exactly as every shard binds the client port today
([Thread per Core](../architecture/thread-per-core.md#accepting-connections)), and the kernel
picks which shard accepts a peer connection. The accepting shard runs the read relay for it and
**forwards every frame over the local mesh to the shard the frame names**. So a `Forward` from A0
to B3 that the kernel handed to B5 costs one `kanal` send more than one it handed to B3. That hop
is the one [D7](../direction/shard-aware-routing.md#why-this-is-ranked-below-everything-else) argues
is worth about a microsecond on the same machine and gates behind a measurement; the same
argument applies here, and per-shard peer ports (D7's option 1) are recorded as the optimization to
measure, not built.

The relay is the existing `client_rx_relay` shape (`shard.rs:120`): read a header, read the
context if the flag says so, read the body, post a `ServerMsg` onto a channel. The difference is
which channel — the named shard's rather than the acceptor's own.

### The peer handshake

A peer connection opens the way a client connection does, one fixed-size frame each way, the
peer speaking first:

```
PeerHello    body, 48 B : cluster id (16) | node id (16) | fingerprint u64 | protocol version u8 | reserved [u8; 7]
PeerHelloAck body, 48 B : cluster id (16) | node id (16) | fingerprint u64 | reason u8           | reserved [u8; 7]
```

Fixed bytes, not rkyv, for [F10](../features/framing-and-protocol-evolution.md#design-choices)'s
reason: the exchange detects a schema mismatch and must not decode with the thing it detects. The
reasons a peer is refused — unsupported version, schema mismatch, **wrong cluster** — are the
handshake's three plus one, and a refusal is still an ack with `REFUSED` set so the refused peer
learns why. The node id in the ack is what a joiner writes into its marker as the seed's identity,
and the cluster id is what it adopts ([C1](node-identity.md#two-identities)).

When `cluster.tls` is set the handshake runs inside TLS, mutually authenticated against
`cluster.tls.ca`, and the `NodeId` in the `PeerHello` **must match the certificate's** — a
certificate names a node, and a node presenting another node's name is refused. Without TLS the
node id is taken on trust, inside whatever boundary the operator drew.

### The message types

Appended at 13 and up, never renumbered, exactly as the twelve before them:

| Type | Direction | Carries | Page |
| --- | --- | --- | --- |
| `PeerHello`, `PeerHelloAck` | both | above | here |
| `Forward` | coordinator → owner | the bundle body, the offset, the keys, and the return address: the coordinator's `(node, shard)`, the client id, the bundle id, the index, `end`, and `gather` | here |
| `Forwarded` | owner → coordinator | the sealed response bytes, unchanged, plus the client id, the bundle id and the index | here |
| `Replicate` | primary → follower | the intent bytes with their `(tablet, epoch, seq)` header | [C5](replication.md) |
| `ReplicateAck` | follower → primary | `(tablet, epoch, seq, pos)` | [C5](replication.md) |
| `CatchUp` | follower → primary | `(tablet, from_seq)` | [C7](failover.md) |
| `StreamBegin`, `StreamPartition`, `StreamEnd` | primary → new replica | a tablet's partitions at a `seq`, then its tail | [C7](failover.md), [C8](rebalancing.md) |
| `Raft` | control plane → control plane | an opaque `openraft` message | [C3](membership.md) |
| `PeerPing`, `PeerPong` | both | a sequence number and a send time | [C3](membership.md) |
| `Admin` | client → any node | a control-plane request, answered from the control plane | [C9](operations.md) |

Every one of them is `[header][trace context?][payload]`, the request frame's shape, and every one
carries the trace context whenever the request that caused it did. A trace of a forwarded query
therefore has the client's span, the coordinator's `Coordinator::route`, and the owner's
`Shard::handle_query` under **one trace id across three processes** — F35's path, one segment
longer.

### The bundle is forwarded as bytes

`ServerMsg::Query` carries the validated bundle by refcount. A `Forward` carries **the same
bytes**, written from the `Bytes` with a vectored write, the same way a response is written from
its `AlignedVec` today (`client_tx_relay`, `shard.rs:319`). The owner does not receive a query; it
receives a bundle and an offset, exactly as a local owner does.

What it cannot inherit is the coordinator's validation. `unarchive_queries` is `unsafe fn` because
its precondition — these bytes were validated by `Queries::access` and have not changed since — is
an argument about one process's memory (`Wire Protocol`, [Validation](../architecture/wire-protocol.md#validation)).
A socket is not that. **The receiving node revalidates the bundle once with `Queries::access`
before any shard on it reads a query out**, and from then on the local rule applies unchanged.
That is one `bytecheck` walk per forwarded bundle, paid on the owner rather than the coordinator,
and [O1](../appendix/optimizations.md#o1-queries-are-fully-deserialized-on-arrival)'s `wire_codec`
benchmark already says what a walk costs at 1, 10 and 100 queries per bundle.

A bundle naming partitions on three nodes is forwarded to three nodes, so the coordinator writes
its bytes three times. That is what [F26](../features/archive-routed-requests.md) removed on the
single node — the per-shard clone — coming back at node granularity, and it is the honest cost of
routing from the coordinator; [D7](../direction/shard-aware-routing.md) is the design that would
move it to the client.

### Responses stop bypassing the coordinator across nodes

`client_map` holds `kanal` senders, which cannot leave the process, so a remote owner cannot write
to a client socket on the coordinator's node. It answers with a `Forwarded` to the coordinator
shard named in the `Forward`, and **the coordinator shard relays the bytes to the client's socket
relay without looking at them** — the sealed `AlignedVec` goes from the owner's `rkyv::to_bytes`
to the client's aligned read with one extra socket in between and no parse. The "responses bypass
the coordinator" design note in [Request Lifecycle](../architecture/request-lifecycle.md#design-notes)
becomes node-local: within a node it still holds.

A `Gathered` share from a remote owner is a `Forwarded` with `gather` set; the coordinator merges
it in `handle_gathered` as it merges a local share, after unsealing it. Merging needs the rows, so
a cross-node split query pays one deserialization per remote share that a local split does not —
the coordinator would have had `ResponseKinds` in hand, and now has bytes.

### Backpressure, from the first line

Every channel in the single node is unbounded and that is
[item 15](../appendix/known-issues.md#15-no-backpressure-anywhere). A peer connection is the one
place a remote process can grow a local queue, so the channel between a peer read relay and the
shard it feeds is **bounded**, and the relay stops reading from the socket when it is full. TCP
does the rest: a node that cannot keep up stops draining its receive window, the sender's writes
block, and the sender's shard sees a full outbound buffer and answers its own client with
`ErrorCode::Shedding` — the code [F11](../features/error-channel.md) reserved for exactly this and
nothing has used. Item 15 stays open for the local channels; the peer path does not join it.

## Alternatives rejected

**gRPC, or any RPC framework.** [D9](../direction/prior-art.md#tikv-and-grpc)'s TiKV lesson: a
general-purpose RPC layer gives multiplexing, deadlines and TLS for free and a per-call overhead
that cannot be removed, which TiKV then batches to hide. Shoal already batches; it should not also
batch to pay for HTTP/2. The framing module exists, is runtime-free, and has reserved room.

**A separate internal wire format.** Two codecs to keep in step, two version bytes, and a
fingerprint check written twice. The only thing the peer protocol needs that the client protocol
lacks is message types, and those are appended.

**One connection per node pair, shared by every shard on the node.** Halves the connection count
and puts a lock, or a channel, between every shard and the socket. That is the design the whole
server is built to avoid, and the connection count it saves — `S` per remote node — is small.

**Per-shard peer ports, so a frame lands on the shard it names.** D7's option 1, and probably right
eventually. It is not built first because its value is the local hop it saves, and nobody has
measured that hop ([D7](../direction/shard-aware-routing.md#how-it-would-be-measured)). Accept
anywhere and relay is correct with no measurement; the ports are an optimization on top of it.

**Forwarding the deserialized query.** Would let the owner skip revalidation, and would mean
serializing a query the coordinator deliberately never deserialized — undoing F26 for every
cross-node query. Bytes plus one `bytecheck` is cheaper than deserialize plus serialize plus
deserialize.

**Letting the remote owner open a connection back to the client.** The client has a pool of
connections to whichever nodes it was given, and a response arriving on a connection the client
did not send the query on lands in a `TcpProxy` whose `channel_map` may not have the id
([The Client](../api/client.md)). The coordinator holds the socket; the coordinator answers.

**A shared cluster secret.** [C1](node-identity.md#alternatives-rejected).

## What it costs

- **One socket write per remote destination per bundle**, of the bundle's bytes. A bundle that
  routes to `k` nodes is written `k` times. On the single node it was written zero times.
- **One `bytecheck` per forwarded bundle**, on the owner. Measured by `wire_codec/request/decode`
  ([F10](../features/framing-and-protocol-evolution.md#performance)).
- **One local mesh hop per inbound frame** that the kernel handed to the wrong shard, which is
  `(S − 1) / S` of them.
- **One extra socket on every cross-node response**, and one deserialization per remote share of a
  split query.
- **`S × (N − 1)` connections per node**, each a TLS handshake when `cluster.tls` is set, paid at
  first use and at reconnect.

## What it breaks

- **`unarchive_queries`'s safety argument at a node boundary.** It is re-established by
  revalidating on arrival, and the page says so where the `unsafe` is, but the argument now has
  two cases and a reader of the single-node one has to know the other exists.
- **`Gather`'s assumption that every share reports.** A node that dies with a share outstanding
  leaks the gather forever, which is [item 33](../appendix/known-issues.md#33-collected-split-query-state-has-no-expiry)
  made reachable by something other than a bug. [C6](reads.md#deadlines-on-a-gather) puts a
  deadline on it.
- **"Responses bypass the coordinator"** becomes a statement about one node.
- **`ShardInfo::mesh_id` panics on a `Remote`.** Every caller is on the local path and should be;
  a new caller that is not gets a panic rather than a wrong index.

## Invariants to uphold

- **A peer connection is owned by one shard and touched by no other thread.** The outbound side
  belongs to the sender; the inbound side belongs to whichever shard accepted it. Nothing pools
  them.
- **A `ServerMsg::Partition` never becomes a frame.** There is no `MessageType` for it, and the
  encoder has no arm for it. The `Send` invariant (`thread-per-core.md`) is a comment on the
  single node; on the wire it is an enum with no variant, which is stronger.
- **A forwarded bundle is validated on the node that receives it before any shard there reads a
  query out of it.** The `unsafe` precondition on `unarchive_queries` is re-established per
  process, never assumed across one.
- **Every peer frame that descends from a client request carries that request's trace context**,
  so that a trace stays one trace across nodes. A frame that drops it starts a new trace silently,
  which is the failure [F35](../features/wire-trace-context.md) was built to prevent.
- **Peer message types are appended and never renumbered.** The same rule as the first twelve.
- **The peer read relay stops reading when its channel is full.** The first unbounded queue fed by
  a remote process is the first way one node takes down another.

## Prerequisites

[C1](node-identity.md) for the identities the handshake exchanges.
[F10](../features/framing-and-protocol-evolution.md) for the header and the reserved room,
[F14](../features/encryption-in-transit.md) for kTLS, [F35](../features/wire-trace-context.md) for
the context that crosses a hop, [F26](../features/archive-routed-requests.md) for the bundle being
a `Bytes` in the first place, [F11](../features/error-channel.md) for `Shedding`.

## How it would be measured

`macro/cluster/hop/{local,remote}` — the same single-partition get, over the ephemeral controls
([F9](../features/ephemeral-tables.md)) so storage is out of the number, in three placements: the
partition owned by the accepting shard, by another shard on the same node, and by a shard on
another node. The first two are the experiment D7 said was "constructible today" and nobody has
run; the third is what this page adds. It is the first cluster capture worth taking, and
[M2](milestones.md#m2-the-inter-node-transport) takes it.

## Acceptance tests

| Test | Asserts | Milestone |
| --- | --- | --- |
| `a_get_whose_partitions_live_on_the_other_node_is_answered` | Two nodes with a static config-only split; the rows come back correct and in the order the query named them | M2 |
| `a_bundle_spanning_both_nodes_is_answered_once_per_query` | The reorder buffer sees exactly one response per index when shares come from two nodes | M2 |
| `one_trace_id_spans_client_and_both_nodes` | `trace_propagation.rs`'s `SERVER_PATH` extended with a `CLUSTER_PATH`; every span shares one trace id | M2 |
| `a_peer_from_another_cluster_is_refused_with_an_ack` | The ack names both cluster ids | M2 |
| `a_peer_built_from_a_different_schema_is_refused_with_an_ack` | Same, for fingerprints | M2 |
| `a_peer_whose_certificate_names_another_node_is_refused` | With `cluster.tls`, the `PeerHello` node id must match the certificate | M2 |
| `a_frame_the_kernel_gave_the_wrong_shard_reaches_the_right_one` | Asserted on span attributes: the accepting shard and the handling shard differ | M2 |
| `a_forwarded_bundle_is_validated_on_arrival` | A corrupt body forwarded by a hostile peer is refused on the owner with `Error`, not read unchecked | M2 |
| `a_slow_peer_sheds_rather_than_growing_a_queue` | A paused destination node makes the sender's client see `Shedding`, and the sender's memory does not grow | M2 |
| `a_partition_message_has_no_frame` | A compile-time test: the encoder's `match` on `ServerMsg` has no `Partition` arm and is exhaustive | M2 |

## Related

- [C1. Nodes, identity, and the cluster configuration](node-identity.md) — the identities in the handshake
- [C5. Replication and the write path](replication.md), [C7](failover.md), [C8](rebalancing.md) — the pages that spend the message types listed here
- [Request Lifecycle](../architecture/request-lifecycle.md) — the single-node path this extends hop by hop
- [Wire Protocol](../architecture/wire-protocol.md) — the framing, the fingerprint, and the validation rule
- [F26](../features/archive-routed-requests.md) — why a bundle is bytes, and what forwarding them costs
- [F35](../features/wire-trace-context.md) — the context that has to cross every hop
- [D7. Shard-aware routing](../direction/shard-aware-routing.md) — the client half, and the hop this page declines to optimize yet
- [item 15](../appendix/known-issues.md#15-no-backpressure-anywhere), [item 33](../appendix/known-issues.md#33-collected-split-query-state-has-no-expiry) — the two open items a transport makes sharper
