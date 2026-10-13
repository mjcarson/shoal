# 236. A replaced replication link's late answer could complete another RPC

A shard's replication link to a peer is dropped and dialled afresh when the member's address
changes ([F40](../../features/replication.md)). Correlation ids started again at one on every link,
and an answer was routed to whichever link held the member's key. So an answer the old link read
after the switch could complete the new link's RPC of the same id, which can belong to another
group. Found by reading the code while designing [F84](../../features/replication-lane-handoff.md),
which rewrote the map the links are kept in, and fixed with it.

## Symptom

None was seen on the lab or in the fixture: it needs a member's data address to change while an
RPC to it is in flight. When it happens, an `AppendEntriesResponse` that one group's leader asked
for completes another group's append of the same id. openraft takes the answer as that follower's,
so a leader can count a follower as matched at an index it never stored. It can also count a
follower as refusing an append it took, or answer a vote with another group's verdict. The RPC
the answer was meant for waits out its deadline and is reported unreachable, which openraft
retries.

## Cause

`ReplicationLink` minted its correlation ids from a `next_id` of its own, starting at 1. The map
replaced a link whose target no longer matched the member's advertised address, and dropped
only its own handle: an RPC in flight held the old link through an `Rc`, so the old link's task
went on reading the connection. Every answer reached the shard as a `LinkEvent` naming its node,
and `ShardNetwork::answered` completed the pending RPC of that id on whatever link the map held
for the node, by then the new one. The ids of the two links overlapped from the first request.

## Evidence

**Reproduced** by `a_late_answer_from_a_replaced_link_completes_nothing`, written first and run on
europa against `d84ea4c` before any of F84's changes to the network:

- an RPC for group one is in flight on a link to an address nothing answers on;
- the member's address changes, so the next RPC, for group two, goes on a new link;
- the old link's answer to id 1 is then delivered for the member.

Group two's RPC was completed by it:

```text
test server::replication::network::tests::a_late_answer_from_a_replaced_link_completes_nothing ... FAILED
thread 'server::replication::network::tests::a_late_answer_from_a_replaced_link_completes_nothing' panicked at shoal-core/src/server/replication/network.rs:2594:13:
group two's rpc was completed by group one's answer: Ok([103, 114, 111, 117, 112, 32, 111, 110, 101, 39, 115, 32, 97, 110, 115, 119, 101, 114])
```

The bytes are "group one's answer". With the fix, the second RPC waits out its own deadline and
the late answer completes nothing.

## The fix

**One counter for every link of a shard's network.** `Shared::next_id` is an `Rc<Cell<u64>>`
that every `ReplicationLink` the network opens shares, so an id names one RPC across all of them.
A late answer from a replaced link is routed to the link that replaced it and finds no RPC of
that id there, so it is dropped. The RPC it answered, on the old link, waits out its deadline and
is reported unreachable. That is how an unknown outcome was always reported, and openraft retries
it.

## Alternatives rejected

**Draining the old link before replacing it.** Waiting for every RPC on a link to finish before
dialling the new address would hold appends to a member that moved for a whole deadline, and
the old address may never answer.

**Routing an answer by the link it came in on.** A link's events could carry an identity of the
link, and the network could keep replaced links until their RPCs drained. It moves the same
answer to the same RPC as the fix does, with a second table to keep, for an answer whose only
use is to complete an RPC the caller has already given up on by the time a move is noticed.

**Ids per link with an epoch in them.** A link's ids could start from a high epoch of its own.
One counter is the same guarantee with nothing to choose.

## Invariants to uphold

- **A correlation id names one RPC across every link of a shard's network.** Any new way of
  minting ids for replication RPCs - a second network, a link opened outside `ShardNetwork::link`
  - draws on the same counter.
- **An answer whose id no pending RPC holds is dropped.** It is never matched to another RPC by
  any other field.

## Still open

Nothing of this item. The old link's RPCs still wait out their deadlines once it is replaced,
which is the cost of an address change and was before.

## Tests

| Test | Where | What breaks if this is reverted |
| --- | --- | --- |
| `a_late_answer_from_a_replaced_link_completes_nothing` | `shoal-core/src/server/replication/network.rs` | A late answer from a replaced link completes the new link's RPC of the same id |
| `an_answer_completes_the_rpc_on_its_own_slots_link` | `shoal-core/src/server/replication/network.rs` | Two links to one node hand out the same id, and an answer on one completes the other's RPC |

## Related

- [F84](../../features/replication-lane-handoff.md), which keys the links by member and found this.
- [F40](../../features/replication.md), the replication lane and its links.
- [Resolved #143](silent-partition-hops.md), whose silence a link's pending RPCs feed.
