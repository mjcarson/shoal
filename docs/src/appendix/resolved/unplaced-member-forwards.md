# 169. A member the placement does not name refused every client query

## Symptom

On the lab, `cluster rebuild hyperion` under the mixed bench cost 63,012 failed operations in two
seconds, every one of them refused `NotInitialized`:

```text
this node holds no tablets: the placement has not been initialized, or does not name it
```

The bench's pipelined clients reconnected to hyperion's new process as soon as it listened. The new
identity was a member of an initialized cluster and held the whole map. But no placement slot named
it until the first move of its replacement plan did, and until then it refused everything it was
sent. The client does not retry `NotInitialized`, since the code says the cluster is not set up, so
each refusal was a failure. The same happens to any member added with `cluster add` and no
rebalance: a client connected to it is refused everything, for as long as nothing moves onto it.

## Cause

A shard's coordinator refused a whole bundle when `Shard::placed` was false, and `placed` meant
"a ring names this node". `TabletMap::ring_for` answered `None` for a node that neither the
placement nor a configuration nor a move named, and `Ring::with_placement` refuses a placement that
leaves out the node building the ring (`PlacementMissingSelf`). That was built for the joiner
before `Initialize`, which really has nowhere to send a query. A member admitted after `Initialize`
fell into the same case. Yet it already did the thing it needed for most of its queries: a placed
node forwards every tablet it holds no copy of to the tablet's holder, and an unplaced member holds
no copy of anything.

## Evidence

**Reproduced in the fixture before the fix.** `unplaced_member_forwards_every_query`
(`shoal/tests/cluster_fixture.rs`) initializes three nodes at factor three and leaves a fourth,
joined afterwards, with no move. Against the unfixed tree the first write through the fourth node:

```text
a write through the spare was refused: Server { query_id: Some(01a0de75-…), index: Some(0),
code: NotInitialized, msg: "this node holds no tablets: the placement has not been initialized,
or does not name it" }
```

The lab figure (63,012 refusals in 2 s) comes from the bench's per-second samples in
[section 7](../../cluster-testing/correctness.md#rebuilding-a-node-under-load). The fixed build was
run through the same rebuild, and through a member added with no rebalance, in
[section 8](../../cluster-testing/correctness.md#8-an-unplaced-member-coordinates).

## The fix

The fix splits *routes* from *holds tablets*.

- **`TabletMap::initialized`**: the map now says whether an operator initialized the placement.
  Before `Initialize`, the placement is the bootstrapper alone, and that alone cannot tell a
  bootstrapped cluster from an initialized one.
- **`TabletMap::coordinates(me)`**: true for a node a ring names, and for every member once the
  placement is initialized.
- **`Ring::coordinator`**: builds the ring for a member the placement does not name. It uses the
  same slot list and the same assignment as `Ring::with_placement`, which is now one function they
  share (`Ring::assign`). The node's executors come first and own nothing, and every tablet goes to
  the remote slot a placed node sends it to.
- **`read_ring_for`**: starts from that ring and runs its per-tablet holder choice unchanged. A
  tablet goes to its primary while the primary is up, and to another holder when it is down.
- **The shard**: it keeps `placed`, which still decides which groups it hosts, and gains `routes`.
  Its coordinator refuses only when `routes` is false, which is a joiner before `Initialize`.
- **Readiness**: the view reports `routes`, and an unplaced member's `default_writes` is the
  map's admission rather than a shortfall. A write through it is coordinated like any other.

## Alternatives rejected

- **A retriable refusal instead.** Answering `Unavailable`, which the client retries, would have
  turned the failures into retries. But every query sent to that node would still cost a round trip
  and a backoff, for as long as the node stays unplaced. With no rebalance, that is forever. The
  node has the map, so it can simply forward.
- **Place the member at once.** Putting it into the placement when it is admitted would change
  every tablet's primary. That means empty copies serving reads and a rebalance nobody asked for.
  Placement stays the operator's decision, made through a plan.
- **Forward before `Initialize` too.** A joiner could forward to the bootstrapper, which serves
  alone until then. But data written before `Initialize` is not what the initialized placement
  holds. `NotInitialized` is the honest answer while no operator has placed anything, and the
  existing membership test keeps checking that.

## Invariants to uphold

- **The coordinator ring and the placed ring apply one assignment.** Both go through
  `Ring::assign`. If the two ever disagreed about a tablet's slot, a forward from an unplaced
  member would land on a shard that does not hold the tablet. It would then come back
  `StaleTopology`, or be answered from nothing.
- **`placed` decides groups and `routes` decides refusals.** An unplaced member must host no group:
  `rebuild_groups` still reads `placed`, so forwarding never creates a copy.
- **`NotInitialized` means that no operator initialized the placement**, never that this node is
  not in it.

## Still open

- **A node that has not received its first map** still has the default map, with no placement,
  and refuses `NotInitialized`. It has only just started listening, so this is a short window. The
  lab run on the fixed build counted the refusals in it: see
  [section 8](../../cluster-testing/correctness.md#8-an-unplaced-member-coordinates).

## Tests

| Test | What breaks if this is reverted |
| --- | --- |
| `unplaced_member_forwards_every_query` (`shoal/tests/cluster_fixture.rs`) | A write, a `One`, `Quorum` or session read through a member no placement slot names is refused `NotInitialized` |
| `readiness_distinguishes_process_control_and_data` (`shoal/tests/cluster_fixture.rs`) | A joiner before `Initialize` forwards instead of refusing `NotInitialized` |
| `ring::tests::a_coordinator_routes_every_tablet_to_its_placed_slot` | The coordinator ring sends a tablet to a slot a placed node would not, or owns a tablet locally |
| `map::tests::an_unplaced_member_coordinates_every_tablet_remotely` | An unplaced member has no ring, routes a tablet other than to its primary, or reads from a down holder |
| `map::tests::a_joiner_is_unplaced_until_initialized` | A joiner coordinates before `Initialize` |

## Related

- [F39](../../features/membership.md), where `NotInitialized` and the joiner came from.
- [F45](../../features/replica-migration.md), the moves that bring a member into the rings.
- [F56](../../features/cluster-rebuild.md), whose lab run found it.
