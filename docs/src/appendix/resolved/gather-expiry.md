# 33. Collected split-query state had no expiry

## Symptom

A get naming partitions on several shards was answered in pieces, and the shard that split it
kept a `Gather` until every piece arrived. If one never did - a shard that stalled, a peer whose
answer was lost, anything on the list [item 33](../known-issues.md) kept - the gather stayed
resident forever and the client waited forever. There was no deadline on a bundle anywhere: not
on a gather, not on a local share, and the only sweep that existed expired *forwarded* queries on
a cluster node and answered them `OutcomeUnknown`, which a standalone node never ran and a local
share never met.

## Cause

`Shard::handle_gathered` in `shoal-core/src/server/shard.rs` removed a gather from `gathering`
when its `outstanding` count reached zero, and nothing else ever removed one. The map was keyed
by `(bundle, index)` with no notion of which attempt or which shard a share answered for, so
even a late or repeated share had nowhere to be judged; the sweeper that did exist was spawned
in the peer branch of `Shard::init` and walked `Peers.pending` alone.

## Evidence

**Reproduced.** `shoal/tests/gather_expiry.rs` starts a two shard standalone server with a one
second `networking.query_deadline`, holds every share both shards would send with the new
`HoldShares` verb for six seconds, and sends a get over thirty-two keys. With the hold in place
and the expiry sweep commented out - the tree as it was, plus the verb - the get never returned
and the test's own four second timeout ended it:

```text
Error: Io(Custom { kind: Other, error: "the split get never returned: the gather did not expire" })
test result: FAILED. 0 passed; 1 failed; ... finished in 4.04s
```

With the sweep in place the same get is answered `Timeout` - "the query did not complete within
its deadline; 0 of 2 shares arrived" - between 0.9 and 3 seconds, the gather is gone, the
expiry is counted once, and the shares released at six seconds are counted late and change
nothing.

## The fix

[F41](../../features/read-consistency.md) rebuilt the gather. `shoal-core/src/server/shard/gather.rs`
holds `Gathers`, a map of `Gather`s each with a **slot** per share sent, an **attempt** minted per
bundle on the coordinating shard, a **deadline** from the bundle's budget, and the table and end
flag needed to answer an expiry in the query's own variant. `arrive(key, attempt, slot, share,
failed)` judges a share by identity: no key or an older attempt is `Late`, a covered slot is
`Duplicate`, otherwise it is merged and the gather completes when every slot is covered or one
has failed. `expire(now)` takes every gather past its deadline; `forget_client` takes a departed
client's.

The sweeper moved out of the peer branch into `Shard::init` and runs on every node, at a tenth
of the shortest deadline in play with a fifty millisecond floor. On each tick `sweep_gathers` runs
first: an expired gather is answered `Timeout` once, its pending forwards are forgotten through
`Peers::forget` so the forward sweep cannot answer the query again, and then `sweep_deadlines`
expires what is left of the forwards, each now at the sooner of its own timeout and its bundle's
deadline. A share that arrives from a peer after its pending was forgotten is still put to the
gather, which is what counts it late rather than dropping it unseen.

The budget is `networking.query_deadline`, ten seconds, on a standalone node as on a cluster one,
or a shorter one a bundle names in its read options; a forward carries the milliseconds remaining
and the serving node counts down from its own arrival.

## Alternatives rejected

**A per-gather timer task.** One glommio timer per split query would have been the obvious shape
and would have put a task on the executor for every gather in flight; the tick already existed
for the forwards and a sweep over a map is what both use.

**Answering an expiry `OutcomeUnknown`.** That is what the forward sweep says, because a forwarded
write may have applied. A gather is a read and nothing behind it may have applied; `Timeout` says
what happened and nothing more.

**Leaving the count-down and adding only a deadline.** A gather that expired and then received
its last share would have merged it into nothing and logged a warning, and a duplicate share
would have decremented the count twice. Judging by slot and attempt is what makes expiry safe to
add.

## Invariants to uphold

- **A gather's key leaves the map on completion or on expiry, never both.** Whichever comes
  first takes it; anything after finds no key and is late.
- **The tick sweeps gathers before forwards.** An expired gather forgets its pendings; if the
  forward sweep ran first it would answer the same query a second time.
- **Every path into `handle_gathered` names an attempt and a slot** - a local share through
  `QueryMetadata.read`, a remote share through the answer head, a failed forward through its
  `Pending`. A share with neither cannot be judged and must not exist.
- **Coverage is the slot.** A share with no rows covers; a missing share never does; the
  completion rule reads slots and never row counts.
- **The sweeper is spawned unconditionally in `init`.** A standalone node splits queries across
  its shards and its gathers expire the same way.

## Still open

- ~~A read is not retried within its budget: the attempt identity exists for it and the reroute is
  M6's ([F41](../../features/read-consistency.md#limitations)).~~ Done at M6
  ([F42](../../features/primary-failover.md)): a share the link never wrote is sent to another
  holder once, under the same attempt and slot.
- ~~The local mesh queues are still unbounded~~ (an admission bound since
  [Resolved #15](shard-mesh-admission.md)); a deadline bounds how long a client waits, not how
  much a shard holds behind what it admitted.
- [Item 32](../known-issues.md#32-a-disconnected-client-is-never-cleaned-up-anywhere)'s remainder:
  a departed ordinary client's gathers are dropped now, its channel is not yet retired.

## Tests

| Test | What breaks if this is reverted |
| --- | --- |
| `a_standalone_gather_expires_at_the_query_deadline` | The reproduction: a held split get never returns |
| `gather_timeout_completes_once_and_discards_late_replies` (unit) | A gather expires twice, or a late, stale-attempt or duplicate share is merged |
| `gather_timeout_completes_once_and_discards_late_replies` (fixture) | A cluster gather answers twice or a late remote share is merged after the answer |
| `a_failed_share_completes_at_once_and_an_empty_share_still_covers` | A failed share waits for the rest, or an empty share stops covering |
| `a_gone_client_drops_only_its_own_gathers` | A departed client's gathers stay resident |
| `a_config_without_a_query_deadline_gets_the_default` | The committed `shoal.yml` stops loading or the deadline stops being read |

## Related

[F41](../../features/read-consistency.md), [C6](../../distributed/reads.md),
[Resolved #16, 51](partition-load-failure.md), [Timeouts](../todos.md#timeouts).
