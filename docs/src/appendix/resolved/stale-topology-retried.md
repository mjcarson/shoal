# 220. A query refused as routed by a stale map was never retried by the client

## Symptom

A bundle sent with a retry budget (`SendOptions::retry`, `exec_with`, `send_one_with`) whose query
was refused `StaleTopology` came back to the caller as that failure on its first try, whatever
budget it had left. `StaleTopology` is what a node answers a query of a tablet no group of its
serves at its map's version: a copy that retired under a move, or a node that never held it
([F45](../../features/replica-migration.md)). The code's own documentation says the opposite:

> The coordinator sends it once to another holder; a client that meets it retries under the same
> identity.

`shoal-proto/src/shared/protocol/error.rs`, `ErrorCode::StaleTopology`

Before [F74](../../features/client-routing.md) a client met it rarely: a node that coordinates a
query reroutes a peer's stale refusal itself, and a client's own query reached a node by the
kernel's choice, whose ring was as new as the node's map. A client that routes by topology sends
a query to the node its own map names, which is older than the cluster's for a moment after every
move, so the refusal reaches it on the path it now takes.

## Cause

`retriable` (`shoal-client/src/client.rs`) lists the codes a try is repeated on, and
`StaleTopology` was not among them. It was added to the error codes by F45 with its documentation
saying a client retries it, and the list `exec_with` consults was written by
[F42](../../features/primary-failover.md) before the code existed; nothing joined the two.

## Evidence

**Reproduced** by a unit test written before the fix, run against the unfixed tree:

```text
$ cargo test -p shoal-client --lib a_stale_topology_refusal_is_tried_again
running 1 test
test client::tests::a_stale_topology_refusal_is_tried_again ... FAILED

failures:

---- client::tests::a_stale_topology_refusal_is_tried_again stdout ----

thread 'client::tests::a_stale_topology_refusal_is_tried_again' (1792348) panicked at shoal-client/src/client.rs:4346:9:
a stale route is not tried again

test result: FAILED. 0 passed; 1 failed; 0 ignored; 0 measured; 28 filtered out; finished in 0.00s
```

Found by reading the client's retry list against the error codes while planning F74, which made
the path common.

## The fix

`retriable` repeats `StaleTopology` under the same identity, beside `NotLeader`, `Unavailable`,
`QuorumUnavailable`, `ConnectionLost`, `OutcomeUnknown`, `Timeout` and `Shedding`, and the code
lists in `SendOptions::retry` and `exec_with`'s documentation name it. It is a definite refusal:
nothing accepted the query, so a repeat is a first try wherever it lands, and `outcome_unknown`
stays false for it.

F74 adds what makes the repeat land somewhere else: a try refused `StaleTopology` is not sent the
same way again while the client's map has not moved; its runs go through the endpoints, whose node
routes by its own newer map (`Shoal::reaim`).

## Alternatives rejected

| Alternative | Why not |
| --- | --- |
| Correct the documentation to say a client does not retry it | The refusal is definite and the next try is routed by a newer map; a caller with a budget asked for exactly this |
| Retry it only in a client that routes by topology | A client that does not route can meet it too, on a node whose ring a move has just changed, and the repeat is as safe there |
| Count it as an unknown outcome | Nothing accepted the query, and a definite refusal counted unknown would turn a later refusal into `OutcomeUnknown` (Resolved #125) |

## Invariants to uphold

- **A code is retriable only if a repeat under the same identity is safe**: nothing was applied,
  or whatever was applied answers its first result. `StaleTopology` is a refusal before any group
  accepted the query.
- **A retried stale route must not be sent the same way again before the map moves**, or it is
  refused again until the budget is gone. F74's `reaim` sends it through the endpoints.
- **The retry list and the error codes' documentation agree.** A new code that says a client
  retries it goes into `retriable` in the same change.

## Still open

Nothing of this item. The general gap - that the list and the codes are joined only by reading -
is what the invariant above names.

## Tests

| Test | What breaks if the fix is reverted |
| --- | --- |
| `shoal-client` `client::tests::a_stale_topology_refusal_is_tried_again` | `retriable` refuses to repeat `StaleTopology`, and `outcome_unknown` is held false for it |
| `cluster_fixture` `a_routed_client_survives_a_killed_member` | a routed client's retried writes and reads across a member's death and return |

## Related

[F74](../../features/client-routing.md), which made the refusal common;
[F45](../../features/replica-migration.md), which added the code;
[F42](../../features/primary-failover.md), which added the retry;
[Resolved #125](retry-unknown-outcome.md), which decides what a retry reports.
