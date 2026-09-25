# 32. A disconnected client was never cleaned up

*The second half of this item. [Resolved #94](disconnected-client-cleanup.md) fixed the defect
found beside it: an answer owed to a departed client ended its shard. It also retired the
connections that peer lanes open. This page covers the ordinary client.*

## Symptom

Every connection a client opened stayed on every shard until the process exited. Each shard
kept one `client_map` entry and one `kanal` sender for it. A client that opened a pool of
fifty, did its work and exited left fifty entries on every shard, and the next client left
fifty more. Nothing in the process ever took them back.

## Cause

`client_acceptor` broadcasts `ServerMsg::NewClient` to every shard when a connection finishes
its handshake, because any shard may answer a query from any connection. Nothing broadcast the
reverse. When the connection ended, `client_rx_relay` returned and its task cancelled the write
relay, which closed the socket ([F10](../../features/framing-and-protocol-evolution.md)). The
senders that the other shards held were never touched.

`ServerMsg::ClientGone` has existed since F38, and its arm removes the map entry and the
topology subscription and forgets the client's gathers. The peer listener sent it when a lane
ended. The client acceptor never did.

## Evidence

**Reproduced against the unfixed tree.** `a_client_that_leaves_is_forgotten_by_every_shard`
opens eight handshaken connections to a two-shard server and waits until every shard reports
all eight. It then closes them and waits five seconds for every shard to report none. It could
not see this before, because nothing reported what a shard held. `ShardTransportView` gained
`clients`, the length of that shard's `client_map`, for this test.

```text
test a_client_that_leaves_is_forgotten_by_every_shard ... FAILED
every shard should have let its closed connections go, but they hold [8, 8]
```

## The fix

**When a client connection ends, `client_acceptor` broadcasts `ServerMsg::ClientGone(client)`
to every shard.** It does this after cancelling the write relay, which is the same point at
which the peer listener retires a lane. A failed broadcast is logged at `ERROR` and does not end
anything else.

`Shard::subscribe` now also ignores a client that it has no map entry for. The topology push
already dropped a subscriber with no channel, so this only means that a `Subscribe` which
arrives after retirement is not stored at all.

## Performance

A disconnect now costs one message per shard, which is exactly what `NewClient` already costs
per connect. The message is handled by a `HashMap::remove`, a `HashSet::remove` and a sweep of
the client's gathers. None of it is on the query path, and a connection that stays open costs
nothing more than before.

[Resolved #94](disconnected-client-cleanup.md#alternatives-rejected) deferred this change until
a capture could show what it costs a client that churns connections. No workload does that:
every benchmark client holds one pool for the whole run, so no existing capture could move.
That capture is still the benchmark host's to take, and a connection-churn workload is filed
in [Todos](../todos.md) as the benchmark that would take it. It was not taken here: this change
was made on the development host, whose numbers are not committed.

## Alternatives rejected

- **Telling only the shards that ever answered the client.** That would save messages on a
  wide node, but every shard would then have to record which clients it had answered. That is
  bookkeeping on the answer path, paid on every answer, to save a message per shard per
  disconnect. `NewClient` goes to every shard, so `ClientGone` does too.
- **Waiting for `GoAway`** ([D2](../../direction/framing.md#message-types)). `GoAway` is what a
  peer says before it closes cleanly. It cannot cover a socket that died, which is the case this
  item is about. The two are complementary: `ClientGone` is what the server tells itself.
- **Dropping entries on the next failed send.** A shard that never answers the client never
  tries a send, so its entry would outlive the client indefinitely.

## Invariants to uphold

- **Retirement is a broadcast to every shard, because announcement is.** A connection announced
  to a set of shards has to be retired to the same set, or the ones left out keep its sender.
- **`ClientGone` is sent after `NewClient` on the same channels.** A shard's queue is FIFO, so a
  retirement can never overtake the announcement it retires. A path that retires a client over
  some other channel breaks this.
- **An answer or a subscription after retirement is not an error.** In-flight queries may still
  produce answers after `ClientGone` lands. [Resolved #94](disconnected-client-cleanup.md)
  already drops those at `DEBUG`, and `subscribe` drops a late subscription.

## Still open

Gathers that start *after* a client's retirement, from a share forwarded before it, are not
forgotten by `ClientGone`. They expire at `networking.query_deadline` through the gather sweep
([Resolved #33](gather-expiry.md)), which bounds them.

## Tests

| Test | What breaks if the fix is reverted |
| --- | --- |
| `a_client_that_leaves_is_forgotten_by_every_shard` (`shoal/tests/client_disconnect.rs`) | Every shard still holds all eight closed connections five seconds after they closed. |
| `a_client_that_leaves_before_its_answers_does_not_end_the_shard` | Still passes. It guards [Resolved #94](disconnected-client-cleanup.md), which retirement makes reachable more often. |

## Related

- [Resolved #94](disconnected-client-cleanup.md), the half of this item that was fixed first.
- [F38](../../features/inter-node-transport.md), where `ClientGone` and the peer retirement came from.
