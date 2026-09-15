# 95. `ShoalPool::transport()` reported shard zero's links and called them the node's

## Symptom

The method's doc said it gathered every shard's peer links, and its loop ran once: the pool
asked shard zero over its mesh channel and returned what shard zero held. A node with one
shard was reported whole. A node with four reported whatever shard zero happened to dial - for
the `local_shard` hop arm, which forwards nothing from any shard, an empty list, and for a node
whose other shards forward, a fraction of the frames with no sign that it was one.
`macro/cluster/hop/local_shard`'s artifact recorded `transport.links: []` on a node that served
two hundred reads, which was true of shard zero and would be read as true of the node.

## Cause

`ShoalPool` had held a sender to every shard's mesh channel since the fixture needed to fail one
(`shard_txs`), and `replication()` already walked them; `transport()` was written at M2 against
`control_tx`, the sender to shard zero alone, with a comment that the relay to the rest was not
built yet. Nothing built it, and the consumers - the bench harness's `transport_facts` and the
fixture's `TRANSPORT` and `WIRE` commands - flattened the views they got, so a single view read
as the whole node.

## Evidence

**Reproduced.** `the_transport_view_names_every_shard` in `shoal/tests/pool.rs`, a two-shard
standalone pool asking for its transport view, against the tree at `dbdd13c`:

```text
thread 'the_transport_view_names_every_shard' panicked at shoal/tests/pool.rs:328:5:
assertion `left == right` failed: the transport view did not come from every shard
  left: [0]
 right: [0, 1]
```

The item itself was established by reading the source, and the artifact above is what a
reader would have met.

## The fix

`transport()` walks `shard_txs` the way `replication()` does and returns one
`ShardTransportView` per shard, in shard order, each naming its shard. A shard whose channel is
closed or that does not answer within five seconds is reported by its index as
`ServerError::ShardFailed` rather than the call failing as `NotClustered` - a shard that is
wedged is exactly the one whose links matter, and a node must never be reported as though the
shards that answered were all of it.

The bench record says which shards answered: `TransportFacts.shards` lists them and every
`LinkFacts` carries the `shard` that held it, both `#[serde(default)]` so a capture from before
this reads back with an empty list and shard zero, which is what it recorded. The fixture's
`WIRE` command already iterated every view; `TRANSPORT` serializes what it is given.

## Alternatives rejected

**Have shard zero relay the request across the mesh and fold the answers.** That is the design
the M2 comment described. It puts a fan-out and a wait on a shard's loop for a question the
pool can ask directly, and the pool already had the senders.

**Fail the whole call when one shard does not answer, as before.** The old error was
`NotClustered`, which is what a standalone node says, so a wedged shard on a cluster node read
as a configuration mistake. Naming the shard is the point of the item's fix direction.

## Invariants to uphold

- **One view per shard, in shard order.** `views[i].shard == i`; a consumer may index by shard.
- **A missing answer is an error naming the shard**, never a shorter list. A reader of the bench
  record can tell a node with no links from a shard that held none only because the list of
  answering shards is complete or the capture failed.
- **`TransportFacts.shards` and `LinkFacts.shard` stay `#[serde(default)]`**: the committed
  captures predate them.

## Still open

- The per-shard view is a snapshot taken shard by shard, five seconds apart at worst; a link
  that moved between two shards' answers is counted where each shard saw it.

## Tests

| Test | Where | What breaks if this is reverted |
| --- | --- | --- |
| `the_transport_view_names_every_shard` | `shoal/tests/pool.rs` | A two-shard pool answers with one view, `[0]` |
| `slow_peer_has_bounded_bytes_and_independent_lanes` | `shoal/tests/cluster_fixture.rs` | The bulk link's counters read off the first view of a one-shard node, which still has to be there |
| `mixed_versions_exchange_real_cluster_operations` | `shoal/tests/cluster_fixture.rs` | `wire_of` reads the negotiated version off every shard's links |

## Related

[F38. Inter-node transport](../../features/inter-node-transport.md), which filed this with the
hop arms; [C2. Transport](../../distributed/transport.md); [Resolved #96](ping-interval-consumer.md),
filed beside it.
