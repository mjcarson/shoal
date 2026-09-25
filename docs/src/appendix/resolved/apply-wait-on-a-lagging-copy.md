# 146. A write through a node catching up from the log waited for its whole backlog

## Symptom

On the lab, hyperion was paused with `SIGSTOP` for 20 s under the mixed bench and then resumed
([cluster testing](../../cluster-testing/correctness.md#pause-one-node)). For about twelve seconds
after it resumed, writes through it took up to 5.3 s. All of them succeeded, and writes through
the other two members were fast. In the per-second series it showed as write p99s of 2–4.5 s
every few seconds, which first looked like leadership being handed back.

## Cause

A coordinator waits for its own copy of a group to apply a committed write before it answers, so
a `One` read through the same node sees it. [#145](apply-wait-on-a-stalled-copy.md) ended that
wait on a copy that is installing a snapshot or whose applied index has stood still for two
heartbeats. A copy catching up from the log after a stall is neither. It applies continuously,
thousands of entries behind, and a write proposed through it is applied only after all of them.
So every write through the resumed node waited for that node's whole backlog, up to the deadline.

## Evidence

**Found on the lab** with the bench's new `--slow-ms`, which logs every operation slower than
the threshold with the member it went through and when it was sent
(`target/lab/t11c-stall-hyperion`): 1,210 writes slower than a second from the resume until
twelve seconds after it, up to 5.3 s each, every one of them through member 1, hyperion, and
none through the other two. The
leadership handbacks at the same time ([O63](../optimizations.md#o63-leadership-never-returns-to-a-groups-placement-primary))
all landed, and none was ignored for an out-of-date log.

**Reproduced** with `a_write_through_a_lagging_copy_is_answered_within_two_heartbeats` in
`shoal/tests/cluster_fixture.rs`. Three nodes at a 1 s base. Node two is paused while node zero
takes 15,000 single writes to a key of a group node zero leads, then resumed, and twenty writes go
through node two at once. Against the tree with #145's stall rule, twice:

```text
the slowest of twenty writes through the catching-up node took 1.639001813s
the slowest of twenty writes through the catching-up node took 1.832193504s
```

With the fix, three runs: 316, 326 and 367 ms, which is the 200 ms bound plus a hop and a commit
on a debug build.

## The fix

`wait_applied_here` (`shoal-core/src/server/shard/groups.rs`) waits at most **two heartbeat
intervals**, a fifth of the failover base and 1 s by default, whatever the copy is doing. It still
ends at once when the index is applied or the copy is installing. #145's stall tracking is gone:
it watched for the same window, and a bound on the whole wait covers a stalled copy and a lagging
one alike.

## Alternatives rejected

- **Judge the lag rather than the time**: end the wait at once when this copy's applied index is
  more than some number of entries behind the commit. A number of entries means a different
  time on every host and every load, and the question a client cares about is time.
- **Keep waiting for a copy that is applying.** That is #145's rule, and it cost up to the whole
  write timeout per write on a node that had just come back, holding each client's pipeline for
  it.
- **Answer every write at the commit** and drop the wait. A `One` read through the accepting node
  would then miss the write in the ordinary case of a follower a few milliseconds behind, which
  the wait covers at almost no cost.

## Invariants to uphold

- **The wait decides when a committed write is answered, never what it is answered.**
- **Its bound is at least one heartbeat interval**, or an idle healthy follower that has not
  heard the commit yet is answered before it applies.
- **A write answered before its apply here carries its session token.** A `Session` or
  `Quorum` read sees it; a `One` read through a node that was behind may not, as through any
  other follower.

## Still open

- Nothing about this wait. The lab rerun is in the
  [chapter](../../cluster-testing/correctness.md#pause-one-node).

## Tests

| Test | What breaks if this is reverted |
| --- | --- |
| `a_write_through_a_lagging_copy_is_answered_within_two_heartbeats` (`shoal/tests/cluster_fixture.rs`) | A write through a node catching up from the log waits for its whole backlog |
| `a_write_through_an_installing_copy_is_answered_at_commit` (`shoal/tests/cluster_fixture.rs`) | The installing case, which the bound also covers but the installing check answers at once |
| Pause one node ([cluster testing](../../cluster-testing/correctness.md#pause-one-node)) | Seconds-long writes through a node resumed after a stall |

## Related

- [Resolved #145](apply-wait-on-a-stalled-copy.md), the same wait on a copy that is installing.
- [F40](../../features/replication.md), where the wait was promised.
