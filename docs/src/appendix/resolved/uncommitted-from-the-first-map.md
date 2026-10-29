# 325. A node just rolled onto a build answered a query of a table not yet committed `StaleTopology`

A schema grows by appending tables and buckets ([F93](../../features/operations-and-the-real-cluster.md)).
A node rolled onto a build that appends one serves it only once the cluster's `ExtendSchema` commits
it, and until then refuses a query of it by name, `UncommittedTable`, at the front door. That
refusal is final, where `StaleTopology` tells a client to refresh its map and try again.

Filed from W4's workspace run (2026-10-27); fixed in the flaky-test plan's first wave the same day.

## Symptom

`an_appended_table_rolls_through_a_cluster_holding_objects` rolls each node in turn onto a build
whose schema appends a table (`Clip`) and a bucket. Right after each node rejoins, it writes a
`Clip` row through that node and expects `UncommittedTable`. In W4's workspace run the write was
refused *StaleTopology: tablet 3338 of Clip is not served on this node at map version 18: its copy
retired here or was never here*, and the row passed at three threads.

## Cause

A shard flags the tables its build appends that the cluster has not committed (`Shard::uncommitted`)
whenever it installs a pushed map (`install_map`). It is built, though, with the map its node starts
with (`setup.initial_map`), the committed state as of the start, and it began with no flags at all.
The authorizer beside them was compiled from that map. So between a node's start and its shards'
first pushed map, a query of an appended table passed the front door, was routed by the first map,
found no group serving its tablet, and was refused as a stale topology: *at map version 18* is the
map the restarted node started with.

## Evidence

**Established by reading the source and the run's message.** The shard's constructor sets
`uncommitted: Vec::new()` beside an authorizer compiled from `setup.initial_map`, and `install_map`
is the only place the flags were computed. The refusal names the version of the map the node
started with, and it came right after the node's restart, before any push could have reached it.

No row arranges the window: a fault that held a node's pushed maps would need the shards to queue
the newest one and install it when the hold ends, since the control plane pushes only on a change.
The fix is the one place the flags are now computed, which a unit test holds, and the rolling row
meets the window under load.

## The fix

**A shard takes its uncommitted tables from the map it starts with**, as it takes its authorizer,
and from every map pushed after it. Both go through one rule, `TabletMap::uncommitted`: the build's
tables the committed record does not name, none before the map is initialized, and none at all
when every table is committed, so a build whose schema is the committed one keeps its check at the
front door to one length load.

## Alternatives rejected

- **Judge a query's table at the routing step.** The front door is where every client query is
  refused by name before anything routes it, and the routing step is shared with peers' forwards,
  which never name an uncommitted table.
- **Answer `StaleTopology` and let the client's retry find `UncommittedTable`.** A client refreshes
  its map and tries again on `StaleTopology`, which costs a round for a query that cannot succeed,
  and a client with no retries left reports the wrong refusal.

## Invariants to uphold

- **Everything a shard derives from a map, it derives from the map it starts with too.** Its ring,
  its authorizer and its uncommitted tables come from `initial_map` before any push.
- **An uncommitted table is refused at the front door by name**, from a shard's first message on.

## Still open

- No row holds a node's pushed maps to arrange the window; the rolling row meets it under load.

## Tests

| Test | What fails if the fix is reverted |
| --- | --- |
| `a_map_names_the_tables_a_build_appends_until_they_are_committed` (`shoal-core`, `map`) | The rule both places now share: the appended table named once the map is initialized, none before, none once committed |
| `an_appended_table_rolls_through_a_cluster_holding_objects` (`cluster_fixture`) | Under load, a write of the appended table right after a node's restart is refused `StaleTopology`, as in W4's run |

## Related

- [F93](../../features/operations-and-the-real-cluster.md), the appended table's refusal.
- [Tests That Fail Under Load](../flaky-tests.md), where the row was recorded.
