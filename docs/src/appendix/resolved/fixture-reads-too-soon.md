# 280, 287, 290. Fixture rows read what the cluster had not yet settled

Each of these rows asserted something true of a cluster that has settled, and read it a moment
before it had. An idle host settles inside that moment; a loaded one does not. In each, what the
engine answered was right: the row read too soon. Item 142's `unplaced_member_forwards_every_query`
was the same, and is fixed here too.

Filed from the record of [Tests That Fail Under Load](../flaky-tests.md) (2026-10-22 on, 287 on
2026-10-17); fixed in the flaky-test plan's second wave (2026-10-27).

## Symptom

- **280**, `removed_device_id_is_never_accepted_again`: *a put after the removal is short of a
  holder* (`left: 2, right: 3`), in ten loaded runs and five of six runs alone in one.
- **287**, `object_writes_survive_control_quorum_loss`: *node 1's pool map changed without a control
  quorum*, once. The two maps differed only in pool `fast` pinned.
- **290**, `device_faults_do_what_they_say`: *node two ended otherwise than a torn write ends it*
  (`left: None, right: Some(137)`), in seven loaded runs.
- **142**, `unplaced_member_forwards_every_query`: *initialized: false* with every member up, in four.

## Cause

- **280.** A put waits `straggler_wait` (200 ms) for a holder past `k + f` and marks a slower one
  missed, which recovery rebuilds a moment later. Node two had just been started again, its groups
  catching up, so it was that holder often enough. The row counted each put's holders at once.
- **287.** A replicated pool is pinned once, by the control leader's `PinPool` at the generation it
  is first ready at, about when the row's first put succeeds. The row read every node's pool map
  right after that put, and the pin reached node one after the read.
  `pool_map_holds_nothing_for_a_stripe` read its maps the same way and checked only node zero's pin.
- **290.** `failure()` reports a child gone when its output ends, and `exit_code()` asked
  `try_wait` once. A process's files are closed as it exits, before it can be waited on, so the read
  between the two found no code.
- **142.** `wait_joined` is the control group's word that a node joined. ~~Its shards hold an
  initialized map a moment after, and the row read readiness at once.~~ Its control thread holds
  the initialized state a moment after, which is what `READINESS` reads, and the row read it at
  once. The shards hold the map that state makes a moment later still, and a shard without it
  refuses every query `NotInitialized`, by name.

## Evidence

**Established by reading the source and the runs' messages**, each named on its item:
- item 287's two printed maps differed in the pin alone;
- item 290's cause is in `node.rs`;
- item 280's run alone counted two holders where recovery's rebuild later made three;
- item 142's readiness was read straight after `wait_joined`.

## The fix

- **280**: the row waits up to 120 s for every put after the removal to have three holders
  (`wait_chunks`). That is the claim it makes: nothing is placed on the removed device, and each put
  ends on three.
- **287**: `settled_pool_maps` reads every node's map until each names pool `fast` pinned and all are
  the same bytes, and both rows take their maps from it.
- **290**: `Node::exit_code_within` waits up to a bound for the child to be reaped once its output
  has ended, and the torn row reads the code with it. `exit_code` stays for the rows that poll or
  that assert a node is still up.
- **142**: the row reads `READINESS` until ~~its shards report an initialized map~~ the control
  thread holds the initialized state, up to 30 s, before it asserts the node is not placed. The
  confirmation loop (2026-10-27, `w6`) found that this is not the shards' map: a write through the
  spare was refused *this node has no placement to route by*. Every write and read through the
  spare now asks again on `NotInitialized`, for up to 30 s.

## Alternatives rejected

- **Lengthen nothing and retry the row.** Each failure was a read too soon, and a retried row reads
  too soon again under the same load.
- **Have a put wait for every holder** (280). The straggler wait is what keeps a slow holder from
  holding a put; recovery's rebuild is the design ([F86](../../features/replicated-pools.md)).
- **Make `exit_code` wait** (290). Two rows ask it whether a node is still up, and a wait would turn
  their answer into a delay.

## Invariants to uphold

- **A row reads a cluster's state once the change it waits for has reached every node it reads**,
  not when the command that causes it returns.
- **A child's exit code is read once it can be reaped.**

## Still open

- Nothing of these four. Each row's claim is unchanged.

## Tests

| Test | What fails if the fix is reverted |
| --- | --- |
| `removed_device_id_is_never_accepted_again` (`cluster_fixture`) | Under load, and alone at times, a put's holders counted before recovery rebuilds the slow one |
| `object_writes_survive_control_quorum_loss` and `pool_map_holds_nothing_for_a_stripe` (`cluster_fixture`) | Under load, a pool map read before the pin reached its node |
| `device_faults_do_what_they_say` (`cluster_fixture`) | Under load, the torn node's code read before it can be reaped |
| `unplaced_member_forwards_every_query` (`cluster_fixture`) | Under load, readiness read before the control thread holds the initialized state, or a query sent through the spare before its shards hold the map |

## Related

- [Resolved #116](map-version-test-snapshot.md), a row that read a version once and expected it to
  stay.
- [Item 142](../known-issues.md#142-two-fixture-tests-fail-intermittently-on-an-idle-host), whose
  row this fixes one of.
- [Tests That Fail Under Load](../flaky-tests.md).
