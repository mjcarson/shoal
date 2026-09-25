# 128. A hopped write reported "the replication rpc timed out" for a leader that answered

## Symptom

The [TMDB loader](../../features/tmdb-dataset-deployment.md) stopped against a healthy three-node
cluster on the lab, six seconds into a load:

```text
Error:
   0: the load stopped; loading is idempotent, so run it again to finish
   1: a write failed: Server { query_id: Some(01a0d6ff-4863-7bd2-a4d0-dd80f14c1c4f), index: Some(13531), code: OutcomeUnknown, msg: "the replication rpc timed out" }
```

`cluster status` afterwards reported every member up, writes admitted and no open plans. The
journals showed no election and no crash. The words named a network failure, and none had
happened.

## Cause

A shard that holds a replica of a tablet but does not lead its group gets `ForwardToLeader` from
openraft and hops the write to the leader, once (`propose_through` in `server/shard/groups.rs`).
The hop is `ShardPeer::propose`, an RPC over the replication lane, and it was given `remaining`,
what was left of the write's budget. `rpc_at` used that one value for two things:

- the `deadline_ms` in the request head, which the leader's `ReplicateKind::Propose` arm uses as
  its own budget for `propose_through`, and
- the forwarder's own `glommio::timer::timeout` on the answer.

The leader works until its budget is gone and then answers "group X did not commit the write
within the deadline". That answer leaves the leader at the moment the forwarder's timer, started
earlier, has already fired. The forwarder always won the race, and turned its own timeout into
`RpcFailure::Unreachable("the replication rpc timed out")` and `ProposalOutcome::Unknown`. Both
answers are `OutcomeUnknown`, so the client got the right code with the wrong reason: a leader
that was up and answering was reported as unreachable.

The load's failure itself was overload, which this item does not fix. The loader kept eight
workers × 4096 = 32k writes outstanding, with each movie's keyword rows added to the queue, and a
write that waited in that queue past `replication.write_timeout` (5s) could not commit in time.
The server does not shed that load before a write times out, which is filed as
[item 129](../known-issues.md#129-an-overloaded-group-answers-outcomeunknown-rather-than-shedding).
The loader now retries the codes that say to try again
([F54](../../features/tmdb-dataset-deployment.md)).

## Evidence

**Reproduced against the unfixed tree** on the development host with
`a_hopped_write_reports_the_leaders_outcome`. It uses three nodes at a factor of three, cuts the
replication lanes out of the group's leader so that it takes a proposal and cannot commit it,
leaves the lane from a follower into the leader up, and writes through that follower:

```text
---- a_hopped_write_reports_the_leaders_outcome stdout ----
the hop did not carry the leader's answer back: Err(Server { query_id: Some(01a0d708-96c2-7c10-b462-2cbbd07adda6), index: Some(0), code: OutcomeUnknown, msg: "the replication rpc timed out" })
```

This is the same message the loader got. The lab's journals from the failed load (05:17:29.6 load
start, 05:17:35.8 the loader's disconnect, about one `write_timeout` apart) show the same shape:
the forwarders on hyperion and titan logged "failed to write a response: Broken pipe" for the
bundle once the loader had gone, and no member logged an election.

## The fix

**The forwarder tells the leader less time than it waits itself.** `ShardPeer::propose` now sends
through `ReplicationLink::rpc_budgeted`, which takes the budget put in the head separately from the
forwarder's own deadline. The budget is `hop_budget(remaining)` (`server/replication/network.rs`):
`remaining` minus a tenth of it, capped at 250ms (`HOP_MARGIN`). A 5s write gives the leader
4.75s, and a 500ms write gives it 450ms. `rpc_at` passes the same value for both, so every other
RPC is unchanged.

A leader that cannot commit in time now says so ("group X did not commit the write within the
deadline"). A leader that is shedding or no longer leads says that. Only a leader that really
cannot be reached still reads as "the replication rpc timed out".

## Alternatives rejected

- **Have the leader take a margin off what it was told.** The leader does not know how far the
  request travelled or how long it queued before it was read. The forwarder is the side holding
  both clocks' starting point, so it is the side that picks the split.
- **Wait longer on the forwarder than `remaining`.** This would make a hop outlive the write's
  budget and the bundle's deadline that `remaining` was cut from
  ([C2](../../distributed/transport.md): a forwarded write counts down from the origin's budget,
  never up from a fresh one).
- **A fixed margin.** 250ms off a 500ms `write_timeout` (which the fixture uses) leaves the leader
  half its budget. A tenth scales down with short budgets, and the cap keeps a long budget from
  giving away seconds.
- **Map the timeout to a different code.** The code was already right: an unanswered proposal may
  have committed. What was wrong was that the answer carrying the reason never had a chance to
  arrive.

## Invariants to uphold

- **A peer that acts until its budget runs out and then answers must be told a budget shorter
  than the caller's wait.** `rpc_budgeted` exists for this. A new RPC of that shape (a proposal,
  or any wait-then-answer request) goes through it, not through `rpc_at`.
- **`hop_budget(r) < r` for every `r > 0`, and `hop_budget(0) == 0`.** A budget equal to the wait
  brings the race back, and an underflow would hand the leader a huge budget.
- **The receiver's deadline is the head's `deadline_ms` and nothing more.** The
  `ReplicateKind::Propose` arm in `server/shard/groups.rs` must not add slack of its own to it.

## Still open

- [Item 129](../known-issues.md#129-an-overloaded-group-answers-outcomeunknown-rather-than-shedding):
  an overloaded group lets a write wait out its deadline instead of refusing it as `Shedding`.

## Tests

| Test | Breaks if reverted |
|------|--------------------|
| `cluster_fixture::a_hopped_write_reports_the_leaders_outcome` | The hop hands the leader the whole of `remaining`, and the client reads "the replication rpc timed out" instead of the leader's "did not commit" |
| `server::replication::network::tests::a_hop_leaves_the_leader_less_than_it_waits` | `hop_budget` stops taking a margin, stops capping it, or underflows on a zero budget |

## Related

- [F40 — replication](../../features/replication.md): the hop and `propose_through`.
- [F42 — primary failover](../../features/primary-failover.md): the one-hop rule, and how
  `RpcFailure` is mapped to `NotLeader` and `OutcomeUnknown`.
- [F54 — the TMDB dataset as a deployed database](../../features/tmdb-dataset-deployment.md): the
  loader whose failed run found this, and its retry.
- [Resolved #125](retry-unknown-outcome.md): how the client's own retry reports an unknown outcome.
