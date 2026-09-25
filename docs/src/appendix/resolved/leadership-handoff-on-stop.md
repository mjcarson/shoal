# 139. A planned stop took its groups' leaders down with it

## Symptom

A rolling upgrade under a mixed load ([cluster testing](../../cluster-testing/correctness.md#rolling-upgrade-under-load))
refused 84,418 writes `NotLeader` in the 30 seconds after it finished: a fifth of the writes the
bench sent. Every node the upgrade restarted led groups, and each of those groups refused writes
until the two members left could elect a new leader.

## Cause

`node::serve` stops on `SIGTERM` by calling `ShoalPool::exit`, and each shard's `stop_groups` shut
its groups down without looking at which ones it led. A group whose leader stops is in the same
state as one whose leader crashed: its followers will not vote while the leader's lease is valid,
which is `election_timeout_max` (twice `primary_failover_after`) since the last acknowledgement,
and then an election timeout has to pass. At the default base of five seconds, that is 15 to 20
seconds of refused writes for every planned restart of a node that leads anything. openraft has
the transfer that avoids it (`trigger().transfer_leader`), and repair already used it.

## Evidence

**Reproduced against the unfixed tree** with `a_stopped_leader_hands_its_groups_off`, in a worktree
of `b681dcc` with only the test and the fixture's new `EXIT` verb copied in. Three nodes at the
default failover base, node one leading the key's group, a writer through node zero across a
graceful stop of node one:

```text
panicked at shoal/tests/cluster_fixture.rs:17700:5:
writes to the group node one led were still refused 5s after it stopped
```

On the fixed tree, the same test's longest run of refused writes across the stop is zero, out of
109 writes.

## The fix

Stopping is two phases (`shoal-core/src/server/shard/groups.rs`):

1. `stop_groups` marks the shard stopping, so every new write through it is refused `NotLeader`
   (retriable) rather than parked. It then hands every group it leads to the voter whose log
   matches furthest (`hand_off_leadership`: `transfer_leader`, then wait until this member no
   longer leads, all groups at once, bounded by `HANDOFF_TIMEOUT`, three seconds), and posts
   `HandedOff`.
2. On `HandedOff` the shard takes the groups' handles and shuts them down, as before, and posts
   `GroupsDown`.

The handles stay in their slots through the first phase. A first cut took them first, so the new
leaders' messages could not reach the old one, which therefore never saw that it had lost the lead.
Every transfer then ran out its three seconds and logged `handed=0`, although the clients saw
almost no refusals. The fixture has an `EXIT` verb and `Cluster::stop`, which stop a child the way
`SIGTERM` stops a deployed node.

On the lab, a forced rolling upgrade under the same mixed load then refused 14 writes
`NotLeader` in total, against 84,418.

## Alternatives rejected

- **Letting the upgrade transfer leadership over the admin interface before each restart.** It
  would only help `cluster upgrade`. A `systemctl restart`, a `cluster restart` or an operator's own
  script would still take the leaders down, and the node itself is the one that knows what it
  leads.
- **Waiting for the transfers without bound.** A target that is itself down, or a group that cannot
  commit, would hold the stop until the supervisor kills the process, which loses the clean
  shutdown as well as the handoff. Three seconds is a few election rounds on any base.
- **A shorter default failover base.** It narrows the unplanned case too, which is measured on the
  [performance page](../../cluster-testing/performance.md#failover-time-against-primary_failover_after),
  but a planned stop should not have to wait for any failure detection at all.

## Invariants to uphold

- **A stopping shard refuses writes before it hands off.** A write proposed through a leader in
  the middle of a transfer could be accepted and then lost with the leadership. Refusing it makes
  the client send it to the new leader.
- **The groups keep their handles until they have handed off.** Their messages are delivered
  through those handles.
- **The handoff is bounded.** Past `HANDOFF_TIMEOUT` the shard stops anyway, and a group that did
  not move waits for its lease and an election, as it would have without this.

## Still open

- A crash is still the lease plus an election timeout: 15 to 20 seconds at the default base
  ([C7](../../distributed/failover.md)).
- A node that comes back leads nothing, and nothing moves leadership back to it
  ([performance](../../cluster-testing/performance.md#leadership-after-a-restart)).

## Tests

| Test | What breaks if this is reverted |
| --- | --- |
| `a_stopped_leader_hands_its_groups_off` (`shoal/tests/cluster_fixture.rs`) | Writes to a gracefully stopped leader's group are refused for a lease and an election |
| Rolling upgrade under load ([cluster testing](../../cluster-testing/correctness.md#rolling-upgrade-under-load)) | Tens of thousands of writes are refused `NotLeader` per upgrade |

## Related

- [F42](../../features/primary-failover.md), primary failover.
- [F55](../../features/cluster-upgrade.md), the rolling upgrade this was found by.
