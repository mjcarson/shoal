# 145. A write through a node whose copy was installing a snapshot waited out the whole write timeout

## Symptom

After the lab's partition test healed, with [#144](post-heal-elections.md) fixed, the cluster
served 63,000–84,000 operations a second with no stalls
([cluster testing](../../cluster-testing/correctness.md#partition-one-node)). But every second, a
few hundred writes took *exactly* 5 s, the `write_timeout`, and then **succeeded**. When the bench
stopped sending, about 370 writes still in flight were answered over the next five seconds, all at
5,001 ms. The same shape is in the run from before #144's fix. The no-fault baseline's worst write
in a minute was 1.3 s.

## Cause

A coordinator whose copy of a group follows hops a write to the leader. Once the leader answers
that it committed, the coordinator waits for its own copy to apply the entry before it answers the
client (`propose_through` in `shoal-core/src/server/shard/groups.rs`). That is what makes a `One`
read through the same node see the write. The wait was bounded only by what was left of the
write's deadline.

After 20 seconds of partition, hyperion was behind the purge point of its groups, and it spent the
seconds after the heal waiting for snapshots and then installing them. A copy that is installing
applies nothing until the install ends ([F43](../../features/node-recovery.md)), and a copy
waiting for a snapshot to start applies nothing at all. So every write coordinated on hyperion to
one of those groups was committed within milliseconds and then held for the rest of its five
seconds. It was answered as the success it was, 5 s late, holding a slot in the client's window
the whole time.

## Evidence

**Reproduced** with `a_write_through_an_installing_copy_is_answered_at_commit` in
`shoal/tests/cluster_fixture.rs`. Three nodes, a 3 s write timeout, node two left behind the purge
point and restarted with every install held for ten seconds (`install_hold_ms`). A write through
node two to a key of a group it is installing. Against the unfixed tree:

```text
a write through an installing copy was answered in 3.001454626s: Ok(0x77747c002210)
a write through an installing copy waited 3.001454626s, where the write timeout is 3s
```

With the fix: answered in 127.7 ms, `Ok`, and the leader reads the new value.

The lab attribution is by reading the timings rather than by tracing a write. Every 5 s write
succeeded, and the bench does not retry. Hyperion's reads answered `Unavailable: group … is
installing a snapshot` from about 10 s after the heal until the run ended. And the partition's
leader handbacks ([O63](../optimizations.md#o63-leadership-never-returns-to-a-groups-placement-primary))
happened only at a few moments, not every second.

## The fix

`wait_applied_here` replaces the plain wait. It ends at the first of:

- this copy applied the write's index (as before);
- this copy is **installing** a snapshot, which `MachineState::installing` says, and so will
  apply nothing until the install ends;
- this copy's applied index has **not moved for two heartbeat intervals** (a fifth of the
  failover base, 1 s by default). A follower that is keeping up hears a commit within one
  heartbeat under no load and at once under load, so a copy that does not move in two is not
  applying: it is waiting for a snapshot, or stuck;
- the deadline.

Whichever ends it, the outcome is what the leader answered. `propose_through` now takes the copy's
`state`, from both callers: the coordinator's `propose` and the hop receiver, which is the leader
and never waits.

## Alternatives rejected

- **Drop the apply wait.** Then a `One` read through the node that accepted a write could miss it
  in the ordinary case, a follower a few milliseconds behind. The wait is cheap when the copy is
  applying, and it is what [F40](../../features/replication.md) promises.
- **A fixed short bound on the wait.** A slow disk on a follower that *is* applying would lose
  read-your-writes it could have kept by waiting. Stall detection gives up only on a copy that is
  not moving.
- **Refuse the write, or answer `OutcomeUnknown`, when the local copy is stalled.** The write
  committed. Any answer but success would make a client retry a write that happened, and pay
  the identity machinery to recognise it.
- **Only the installing check.** It misses the window before an install begins, when the copy is
  behind the purge point and applies nothing either. On the lab that was the first few seconds
  after the heal.

## Invariants to uphold

- **The wait never changes the outcome.** It only decides when a committed write is answered.
- **A write answered before its apply here still carries its session token**, and a `Session`
  or `Quorum` read is what sees it for certain. A `One` read through the same node sees it
  whenever that node's copy was applying.
- **The stall window is at least one heartbeat interval.** Anything shorter would give up on
  an idle healthy follower that simply has not heard the commit yet.
- **No `RefCell` borrow of the state is held across the wait's `.await`.**

## Still open

- A `One` read through a node whose copy was stalled may miss a write that node acknowledged.
  That was already so for a read through any *other* follower. The session token is the answer
  for a client that needs more.

## Tests

| Test | What breaks if this is reverted |
| --- | --- |
| `a_write_through_an_installing_copy_is_answered_at_commit` (`shoal/tests/cluster_fixture.rs`) | A write through a node whose copy of the group is installing waits the whole write timeout |
| Partition one node ([cluster testing](../../cluster-testing/correctness.md#partition-one-node)) | A steady trickle of 5 s writes for as long as the healed node catches up |

## Related

- [F43](../../features/node-recovery.md), node recovery and the installing copy.
- [F41](../../features/read-consistency.md), session tokens.
- [Resolved #144](post-heal-elections.md), the same partition test's previous finding.
