# 94. An answer owed to a client that had left ended the shard

## Symptom

A client that closed its connection with answers still owed - a bundle written and the socket
dropped before the responses came back - could end the shard that owed them, and with it every
other client that shard was serving. The shard reported `KanalSend(ReceiveClosed)` to the pool
and stopped; the pool reported a failure; every connection that had landed on that shard was
refused from then on.

[Item 32](../known-issues.md#32-a-disconnected-client-is-never-cleaned-up-anywhere) had filed the
leak beside this: nothing retired a departed client's channel from any shard's `client_map`, so
every shard held every connection's sender for the life of the process. The leak was known. That
the same departure could take a shard down was not, until [F38](../../features/inter-node-transport.md)
made it common: a peer link is a client to the node it dials, and a lane that is cut and
reconnects - which the bounded-lanes test does on purpose - leaves answers owed to a connection
that is gone on every reconnect.

## Cause

`Shard::reply_sealed` in `shoal-core/src/server/shard.rs` sent an answer with

```rust
client_tx.send((query_id, span, stamps, archived)).await?;
```

and matched a missing map entry with `panic!("{} Missing client channel? {client}")`. The `?`
is the defect. A `kanal` send fails when the receiver is gone, and the receiver is the
connection's write relay, which [F10](../../features/framing-and-protocol-evolution.md) made
end when the read relay ends - that is, when the client leaves. So a client leaving mid-query
turned the next answer owed to it into a `ServerError` that propagated out of the shard's event
loop, and the loop is the shard. The `panic!` arm was unreachable for an ordinary client, because
nothing removed a map entry (item 32), and it became reachable the moment something did.

## Evidence

**Reproduced.** `shoal/tests/client_disconnect.rs` opens a raw handshaken connection, writes a
bundle of two hundred inserts to a persistent table and closes the socket at once, twenty times
over so the window is hit on every shard, and then checks that a handshake still answers and
`pool.failure()` is `None`. With the `?` restored on the current tree the test fails on the
eighteenth round:

```
thread 'a_client_that_leaves_before_its_answers_does_not_end_the_shard' panicked at
shoal/tests/client_disconnect.rs:96:13:
round 18: the server never answered a handshake; the shards report Some((1, "KanalSend(ReceiveClosed)"))
```

Shard 1 died of the send, and the next connection the kernel put on shard 1's listener was never
answered. The `panic!` arm restored on its own does not fail the test, which is the second half
of the cause: an ordinary client never loses its map entry, so only a peer connection - which
the peer listener now retires - could have reached it.

## The fix

`reply_sealed` treats both as what they are, an answer with nowhere to go:

- a send that fails is logged at `DEBUG` - "an answer was owed to a client that had left" - and
  the answer dropped;
- a missing map entry is logged the same way and the answer dropped, in place of the panic.

And the retirement item 32 asked for exists for the connections that made this reachable: the
peer listener broadcasts `ServerMsg::ClientGone(conn)` when a peer lane ends
(`shoal-core/src/server/peer/listener.rs`), and every shard removes the entry
(`shard.rs`, the `ClientGone` arm), so a peer that reconnects a thousand times leaves a thousand
closed channels behind rather than a thousand senders.

The `Reply` the channel carries became a struct - `id`, `index`, `end`, `kind`, `span`,
`stamps`, `archived` - in the same change, because a sealed answer owed to a *peer* has to be
framed with its index and whether it ends the stream, and a tuple of four could not say.

## Alternatives rejected

- **Keeping the `?` and retiring the channel first.** Retirement is a broadcast and arrives when
  it arrives; an answer produced between the socket closing and the `ClientGone` landing would
  still have found a live sender with a dead receiver. The send has to tolerate the gap.
- **Failing the query loudly - an `ERROR`, a counter.** A client leaving with answers owed is not
  a server fault, and a peer lane cut on purpose does it by the hundred. `DEBUG` is the level at
  which "something ordinary happened" is written.
- **Broadcasting `ClientGone` from the client relay too, in this change.** It is what item 32
  asks for and it is the same one-line broadcast, but it changes what every ordinary disconnect
  costs every shard - a message per shard per connection - and the bench captures that would
  show the cost are the benchmark host's to take. Left open, on purpose, on item 32.

## Invariants to uphold

- **An answer is never allowed to end the shard.** Every path from `reply_sealed` down to the
  socket treats a dead receiver as a dropped answer. Anything that reintroduces a `?` on a send
  to a client channel reintroduces this defect; the test below is what says so.
- **A missing map entry is a retired client, not a bug.** Once `ClientGone` exists, an answer
  can lawfully arrive after retirement. The map entry's absence is a state, not a violation.
- **Retirement is a broadcast to every shard**, because `NewClient` was. A shard that misses it
  keeps a closed channel, which is harmless; a shard that never gets one keeps an open sender,
  which is item 32.

## Still open

The remainder of [item 32](../known-issues.md#32-a-disconnected-client-is-never-cleaned-up-anywhere):
an ordinary client's departure still retires nothing. `client_rx_relay` ends and tells nobody,
and every shard keeps that connection's sender and map entry until the process exits. The
broadcast is the one the peer listener already does; what is owed before it is added is a
measurement of what it costs a disconnect-heavy client.

## Tests

| Test | What breaks if the fix is reverted |
| --- | --- |
| `client_disconnect::a_client_that_leaves_before_its_answers_does_not_end_the_shard` | Twenty rounds of a raw client writing two hundred inserts and closing at once; a later handshake going unanswered and `pool.failure()` naming a shard with `KanalSend(ReceiveClosed)` |
| `cluster_fixture::slow_peer_has_bounded_bytes_and_independent_lanes` | The data lane cut and the node still answering: with the `?`, the first answer owed across the cut lane ends the shard that owed it |

## Related

- [Item 32](../known-issues.md#32-a-disconnected-client-is-never-cleaned-up-anywhere), the leak
  this was filed beside and the half that stays open
- [F38](../../features/inter-node-transport.md), which made the panic reachable and the send
  failure common
- [F10](../../features/framing-and-protocol-evolution.md), which made a disconnect end the write
  relay - the change that turned the `?` from a latent into a live defect
- [Item 15](backlog-bounds.md), the unbounded channel a late answer used to be sent into -
  still unbounded, though a connection that owes `max_queued_replies` answers is no longer read
