# 190. A leader threw away every append answer that took longer than a heartbeat interval

Filed and fixed in one change, from round 14 of the lab testing
([#142 under load](../../cluster-testing/correctness.md#142-under-load)). It was most of what
[#142](../known-issues.md#142-two-fixture-tests-fail-intermittently-on-an-idle-host) had been
tracking as deadlines under the suite's load since round 9.

## Symptom

The fixture suite's heavy tests, run six at a time as the suite runs them
(`target/lab/r14/142/loop.sh` and `loop-rehome.sh`), failed in every round. Across seven rounds,
five of the six tests failed at least once, each with a write that did not commit or a group that
elected no leader within its deadline. The rehome crash matrix failed in five of the seven, as it
had in 18 of round 13's 26. Alone, every one passed. With its setup writes retrying for 30 s, the rehome test
still failed:

```text
the lease of 94411189…/3 on group 12490ac0ff138c1e lapsed: no quorum acknowledged it within 2s
```

It lapsed for the whole 30 s. The node's journal had 76 `the replication rpc timed out` failures
for that group, and both followers' journals had about 5,400 append requests from it, handled.

## Cause

openraft gives each `append_entries` call a budget of one heartbeat interval (`RPCOption`, built
from `Config::heartbeat_interval` in `ReplicationCore`), a tenth of the failover base: 100 ms in
the fixture, 500 ms at the default 5 s. `GroupPeer::send_append` waited exactly that long for the
answer, and on expiry dropped the pending request and reported the member unreachable.

A follower on a loaded host that took longer than a tenth of the base to answer therefore had
every answer thrown away. It applied the entries, and its leader never learned what it had
matched. The leader committed nothing, so a write through it waited out its deadline. The lease,
which the members' acknowledgements renew, lapsed, so a write through it was refused `NotLeader`.
openraft sent the same entries again, which added to the load that made the answers late. Nothing
got the group out of it until the load fell.

## Evidence

**Established by running it**, in the fixture on the development host, at `b7f4d1c`. The same loop
of six heavy tests at six threads, before and after:

| Build | Rounds | Tests failed |
| --- | --- | --- |
| Before, with and without the fixture's settle and retried setup writes | 7 | 11 of 42 |
| The fix | 5 | 0 of 30 |

On the lab, at a 1 s failover base, where round 11 first saw O64's halved load rate, the fix made
no difference either way (`target/lab/r14/floor/ab.sh`, six fresh clusters, interleaved): 49,878,
52,470 and 54,989 rows a second before it, 42,553, 52,593 and 54,630 on it, and no append timed
out in either arm. The lab's release builds answer inside 100 ms under a whole load. What the
fixture's debug builds, six clusters to a host, do not.

## The fix

**An append or heartbeat waits at least the election timeout for its answer**
(`ShardNetwork::append_floor`, `shoal-core/src/server/replication/network.rs`). `send_append`
waits the longer of openraft's budget and the failover base, which the shard sets with the rest of
the base's timers (`set_failover_base`). A peer that has gone silent is still given up on as
before: the link checks every 100 ms whether the peer has sent anything within the hop silence,
and ends the call if not. Only a peer that is up and slow is waited for.

Votes and pre-votes keep openraft's budget: an election wants a quick failure more than a late
answer.

openraft's own heartbeat worker wraps its heartbeat in a timeout of one interval, and that is not
ours to change. Since [O65](../optimizations.md#o65-heartbeats-to-followers-that-just-acknowledged-replication)
a replication acknowledgement renews a lease as a heartbeat's does, and those are the answers that
now arrive.

## Alternatives rejected

- **A longer heartbeat interval.** It is a tenth of the base so a follower hears from its leader
  often enough to trust its lease; lengthening it lengthens every failover.
- **Overriding `stream_append` to pipeline appends.** It would send more while an answer is late.
  It would not stop the late answer from being thrown away, which is the defect.
- **Waiting without bound.** A slow peer's call would never end if the peer died mid-answer and
  its link never noticed. The election timeout bounds it, and the silence check ends it sooner.

## Invariants to uphold

- **An append's answer is waited for as long as it can still matter**: at least the election
  timeout, since until then the leader is still the leader its followers know.
- **A silent peer is given up on by the link's silence, not by the append's budget.** The floor
  must never be the only way a dead peer's call ends.
- **Elections keep openraft's budget.** A vote that is late is worse than one refused.

## Still open

- openraft's heartbeat worker still times out at one interval. On a loaded host its heartbeats
  fail while appends succeed, which is now harmless because the appends renew the lease.

## Tests

| Test | What breaks if the fix is reverted |
| --- | --- |
| The fixture loop (`target/lab/r14/142/loop-rehome.sh 5`) | Heavy tests run six at a time fail on writes not committed and groups with no leader |
| The lab A/B at a 1 s base (`target/lab/r14/floor/ab.sh`) | Nothing on the lab's hosts, which answer in time: it shows the floor costs a whole load nothing |

No unit test: the defect is a follower answering later than a heartbeat interval under real
load, which a unit test's network does not do. The loop is the reproduction.

## Related

- [#142](../known-issues.md#142-two-fixture-tests-fail-intermittently-on-an-idle-host), the
  deadlines this was most of.
- [O64](../optimizations.md#o64-a-shorter-failover-base-halves-write-throughput-on-the-lab),
  first found at a 1 s base.
- [#128](hop-deadline-margin.md), a forwarded proposal's budget.
