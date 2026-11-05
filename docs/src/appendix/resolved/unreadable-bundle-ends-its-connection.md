# 362. A bundle that did not validate ended its shard, and with it its node

## Symptom

One well-framed `Queries` frame whose body was not a valid archive of the schema's queries, from any
client that had shaken hands, ended the shard that read it. A deployed node ends with any shard
(`node.rs`: "a dead shard ends the node, so its supervisor restarts it"), so every connection on every
shard of that node went with it, and the connection that sent the frame was never answered. A forward
from a peer did the same to the node it landed on, both when its body did not validate and when an
entry named a query past the bundle's end.

A client of a newer build, or a peer of one in a rolling upgrade, whose bundle names a query this
build does not know would end the node rather than be refused, which is what
[F10](../../features/framing-and-protocol-evolution.md)'s framing promises for a frame that cannot
be right.

## Cause

`handle_client` (`shoal-core/src/server/shard.rs`) validates a bundle with `Queries::access(&body)?`,
and the shard's loop awaits it with `?`, so a failed validation was returned from the loop as the
shard's failure. The validation is the only one a bundle gets: every shard it routes to reads its own
query straight out of the same bytes without walking them again, which is sound only because this ran
first. Nothing upstream validates the body: the connection's read relay checks the frame's header,
sections and length, and hands the body on raw, a streamed bundle too once it is assembled.

`handle_forward` validated a peer's forward the same way, `access(&body)?`, and returned
`ProtocolError::MalformedForward` as an error for an entry past the bundle's end, despite the comment
beside it that said it "ends this bundle"; and it checked each entry's offset inside the loop that
routes them, so a forward whose third entry was bad had its first two routed already.

## Evidence

**Established by reproducing it**, both ways.

- `a_bundle_that_does_not_validate_closes_one_connection_and_the_server_keeps_serving`
  (`shoal/tests/framing.rs`) sends a well-framed `Queries` frame of 64 bytes of `0xFF` down a
  handshaken raw socket. On the tree before the fix nothing answered it and nothing closed it for
  20 s, and the pool reported its shard failed on the validation:

  ```text
  EVIDENCE answered: Err(Elapsed(()))
  EVIDENCE failure: Some((0, "Rkyv(Error { ... InvalidSubtreePointer { ... }, trace: StructCheckContext { struct_name: \"ArchivedQueries\", field_name: \"queries\" } } } })"))
  ```

  A healthy client's round trip after it never came back.
- `a_forward_that_does_not_validate_ends_no_node` (`shoal/tests/cluster_fixture.rs`) has a spare node,
  placed nowhere so it forwards every query, send its next forward with its bundle's bytes overwritten
  (`faults::corrupt_forwards`, `CORRUPT_FORWARDS 1`). On the tree without the forward's half of the
  fix, the node it landed on went down under it:

  ```text
  a read through a forward no node could read was answered Err(Server { ..., code: OutcomeUnknown, msg: "the link to this node went down after the query was sent" })
  ```

On the fix the raw connection is answered with one error frame, `InvalidRequest`, its id nil, and
then closed; the pool reports no failure and the healthy client is answered. The forwarded read is
answered `InvalidRequest` by name, every node lives, and the next read through the spare is answered.

## The fix

**The validation stays exactly where it is; its failure is the sender's, never the shard's.**

- **A client's bundle that does not validate refuses its connection** (`refuse_unreadable`): one WARN,
  then a reply of a new kind, `ReplyKind::Fail { code: InvalidRequest }`, which the connection's write
  relay writes as an error frame for the connection itself (a nil id, since nothing of an unreadable
  bundle names a query) and then shuts its end of the socket, so the client reads its end at once and
  its pool replaces the connection. The shard returns `Ok` and serves on. What the connection sends
  after that bundle is dropped unread (`refused_clients`, until its `ClientGone`).
- **A peer's forward that this node cannot read is refused entry by entry** (`refuse_forward`). Every
  entry's bundle id and index ride the forward's preamble and entries, outside the archive, so each is
  answered to its origin as a forwarded error with `InvalidRequest` (`ReplyKind::Stale`, whose payload
  carries a code), which the origin answers its client with in the query's own variant, rather than
  sending the query elsewhere as it does a stale refusal. The body and **every** entry's offset are
  checked before any entry is routed, so none is half sent. Warned of once every ten seconds at most,
  with how many were refused since.

Nothing unvalidated is ever read: the refusals name only what the frame carries outside its archive.

## Alternatives rejected

- **Answer the client's bad bundle per query, as `refuse_generated` does.** A per-query answer is built
  in the query's own variant from its table, and a body that does not validate has no table to read.
- **Close the connection without a word.** The client would see a socket end and retry the same bytes
  on its next connection, never learning why; an error frame first says what was wrong.
- **Keep the connection open after the refusal.** The read relay would go on reading frames of a client
  whose last bundle could not be read, out of step with it on what it sent; a protocol error ends its
  connection, as F10's hostile frames do.
- **Let the origin's forward time out** (`forward_timeout`, 5 s) rather than answer each entry. Each
  entry's index is known without the body, and a definite refusal by name is what the forward lane
  owes ([F42](../../features/primary-failover.md)).
- **Validate the body in the read relay**, before any shard. It would move work off the shard loop but
  the relay knows no schema's query types, and a bundle a relay validated would still have to be
  trusted by every shard it reaches, which is what the single validation on the coordinating shard
  already gives.

## Invariants to uphold

- **Nothing a client or a peer sends ends a shard.** A frame that cannot be read is refused to its
  sender and ends at most its own connection.
- **A bundle is validated once, where it is coordinated, and a forward again where it lands** (the
  process boundary), before any of it is read or routed.
- **A refusal names only what the frame carries outside its archive**: a connection, or a forward's
  bundle id and indexes.
- **`ReplyKind::Fail` is the last frame its connection writes**, and never queued to a peer relay.

## Still open

- **An archived index cannot overflow.** The review that found this item asked whether a bundle's
  base index plus its queries could overflow on a shard; rkyv archives a `usize` as 32 bits here, so
  the sum of the two always fits a 64-bit `usize`, and a bundle sent with `usize::MAX` as its base was
  answered normally. Nothing was filed.
- **A refused client is told why once**, and a client that retries the same bytes on its next
  connection is refused again there; nothing counts refused connections in a node's figures.

## Tests

| Test | Where | Breaks if |
| --- | --- | --- |
| `a_bundle_that_does_not_validate_closes_one_connection_and_the_server_keeps_serving` | `shoal/tests/framing.rs` | A bundle that does not validate ends its shard again, goes unanswered, or leaves its connection open |
| `a_forward_that_does_not_validate_ends_no_node` | `shoal/tests/cluster_fixture.rs` | A forward its landing node cannot read ends that node again, or its client is not answered `InvalidRequest` by name |
| `unplaced_member_forwards_every_query` | `shoal/tests/cluster_fixture.rs` | Validating every entry before routing any stops a good forward being served |

## Related

[F10](../../features/framing-and-protocol-evolution.md), framing and what a hostile frame costs;
[F26](../../features/archive-routed-requests.md), the single validation every shard's read relies on;
[F38](../../features/inter-node-transport.md) and [F45](../../features/replica-migration.md), forwards
and their refusals; [Resolved #169](unplaced-member-forwards.md), the spare that forwards every query.
