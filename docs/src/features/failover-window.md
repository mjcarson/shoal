# F62. A failover window of one and a half to two bases

A dead leader used to be replaced between three and four times `primary_failover_after` after
its last heartbeat, 15 to 20 seconds at the default. The base is now the follower's lease of its
leader, and an election follows within half a base to a whole one, so the window is one and a
half to two bases: 7.5 to 10 seconds at the default, and the same margin against a slow leader
in heartbeats as before.

## Context

[F42](primary-failover.md) documented the window as two to three times the base and rejected a
shorter lease. The lab measured it at three to four
([failover time](../cluster-testing/performance.md#failover-time-against-primary_failover_after)):
about 16 s at the default in every round from the first to
[round 15](../cluster-testing/correctness.md#round-15s-final-build). Round 16 traced where the
sum comes from. openraft's follower lease is `election_timeout_max`, not a setting of its own,
and the election timer does not run beside the lease: a follower's vote is expired when
`now > last heartbeat + lease + timeout` (`utime.rs`), with the timeout drawn from
`[election_timeout_min, election_timeout_max)`. `group_config` set the pair to the base and
twice it, which made the window `[max + min, 2·max)` = `[3b, 4b)`. The "two to three" the pages
carried added the lease to the timeout's *lower* bound only.

The user chose to change the default rather than measure it and file it.

## What it does

`group_config` (`shoal-core/src/server/shard/groups.rs`) derives a data group's timers from the
base `b` as:

| Timer | Was | Is | At 5 s | At the fixture's 1 s |
| --- | --- | --- | --- | --- |
| Heartbeat | `b/10` | `b/10` | 0.5 s | 100 ms |
| Follower lease (`election_timeout_max`) | `2b` | `b` | 5 s | 1 s |
| Election timeout | `[b, 2b)` | `[b/2, b)` | 2.5–5 s | 0.5–1 s |
| A candidate stands after | `[3b, 4b)` | `[1.5b, 2b)` | 7.5–10 s | 1.5–2 s |
| Heartbeats a leader may miss before a follower votes for another | 20 | 10 | | |
| Heartbeats before a follower stands itself | 30–40 | 15–20 | | |

What is derived from `election_timeout_max` was read one wait at a time. A returning leader
waits out one lease before standing, `b` now ([Resolved #103](../appendix/resolved/returning-leader.md)):
that wait *is* the followers' lease and has to move with it. The head start a non-primary gives
its primary to initialize a group, and the grace an empty volatile copy grants no vote for
([Resolved #142](../appendix/resolved/volatile-amnesiac-vote.md)), are waits *past* a window,
and keep their time: four leases now, `4b`, where they were two of the old double leases. The
first run of the fixture's failover tests on the change found the second one out - the grace had
halved to the two seconds `a_restarted_volatile_leader_elects_nobody_missing_its_commits` holds
an empty leader and a lagging follower alone, and the copy that had forgotten its commits granted
the vote at the end of them, openraft's `log_state_reader.rs:25` assertion on the follower that
had kept them. [Resolved #190](../appendix/resolved/append-answer-thrown-away.md)'s
floor on an append's wait is set from the base itself (`set_failover_base`), not from
`election_timeout_min`, so a loaded follower's late answer is waited for as long as before.

Measured on the lab (`target/lab/failover-test.sh`, a fresh cluster loaded whole, a quiet minute
under the mixed bench with elections counted from every journal, then the node leading the most
groups killed under the bench), beside round 15's final build:

| Base | Writes refused after the kill | Before F62 | Vote changes in the quiet loaded minute | Load | Acknowledged inserts, each member alone |
| --- | --- | --- | --- | --- | --- |
| 5 s (default) | about 10 s (t=16–25) | 16 s (t=16–31, round 15) | 0 | 45,916 rows/s | 568,221, 0 lost |
| 1 s | about 2 s (t=16–17) | about 4 s (round 11) | 0 | 41,680 rows/s | 656,669, 0 lost |

Both inside the window the arithmetic gives, `[1.5b, 2b)`, and the loads inside O64's spread.

## Design choices

**The base is the lease.** `primary_failover_after` now names one thing a reader can hold on to:
how long a follower goes on believing in a leader it has not heard from. The window is that plus
the election, and the election is bounded by the base. The setting's old meaning - the lower
bound of the election timeout, with the lease an unstated twice it - was what made the window
three to four times a number called "failover after".

**Ten heartbeats of lease.** Raft's own guidance is an election timeout of about ten heartbeat
intervals. A lease of `b` at a heartbeat of `b/10` is that; the candidate itself stands after
fifteen to twenty. The old configuration waited twenty heartbeats to vote and thirty to forty to
stand, which no measurement had asked for.

**Half a base of randomization.** The election timeouts spread over `[b/2, b)`, half a base. At
the default that is 2.5 s against a broadcast of tens of milliseconds, and at the fixture's
second it is 500 ms; a split vote needs two candidates within a round trip and a disk sync of
each other, and pre-vote ([Resolved #144](../appendix/resolved/post-heal-elections.md)) makes a
split cost a round trip rather than a term.

## Alternatives rejected

**A lease of its own, apart from `election_timeout_max`.** openraft has no such setting, and
carrying a patched openraft for one constant is not worth what it costs; the ratio in
`group_config` is the lever the library gives.

**Keeping the randomization at a whole base (`min = b/2`, `max = 1.5b`).** A window of
`[2b, 3b)`, ten to fifteen seconds at the default. The randomization is not what was short, and
the lease is what the setting should name.

**Meeting the written objective of base plus two seconds.** With lease = `max` and the timeout
added after it, the window is at least `max + min`, so "base plus a constant" would make `max`
and `min` both well under the base: a lease of six heartbeats or fewer at the default heartbeat.
The objective was written before the arithmetic was, and a window of one and a half to two bases
with a lease of ten heartbeats is the design this page states instead. The milestones page says
so.

**Leaving the default and documenting the arithmetic.** Considered with the user, who chose the
change: a planned stop hands its leads off ([Resolved #139](../appendix/resolved/leadership-handoff-on-stop.md)),
so only a crash pays the window, and it had paid twice what the design needs.

## Limitations

- **A shorter lease is a shorter tolerance of a slow leader.** A leader whose heartbeats are
  delayed by a whole base now loses its followers' votes where it had two bases. Pre-vote keeps a
  single slow follower from raising the term, and a leader that is slow to every follower for a
  base is one the group is better off replacing. What the lab's quiet loaded minute and the
  loaded fixture loop showed at the new lease is under Performance below; O64's spread is
  unrelated to the timers
  ([O64](../appendix/optimizations.md#o64-a-shorter-failover-base-halves-write-throughput-on-the-lab)).
- **The window is still not "base plus two seconds".** It is one and a half to two bases; the
  objective as written is withdrawn rather than met.
- **A base under 100 ms is refused as before**, and the election timeout's lower bound has a
  floor of 50 ms, so at the smallest base the ratio is what it is at every other.
- **A cluster mid-upgrade fails over at the old build's window.** A follower on this build
  stands at one and a half to two bases; one on the build before refuses every vote until two
  bases have passed, so while both are voters a leader lost is replaced no sooner than the old
  window allows, and pre-vote keeps the new build's earlier candidates from raising the term
  meanwhile. Once the upgrade completes the window is this page's.

## Invariants to uphold

- `election_timeout_max` is the follower lease. A wait that *is* the lease (a returning leader's)
  is one of it and moves with it; a wait that has to outlast a whole failover (the head start,
  the amnesiac grace) is four of it, twice the window, and its time is what matters. Change the
  ratio here and check each against the window, not against its old multiple.
- `set_failover_base` sets #190's append floor from the base, never from
  `election_timeout_min`: a loaded follower's answer must be waited for at least a base.
- openraft's own constraint holds: heartbeat + `heartbeat_min_interval` + a tick stays under
  `election_timeout_min`, which at `b/10 + b/10 + b/500` against `b/2` it does at every base.
- The window a page states is an interval, `[max + min, 2·max)` from the last heartbeat, never a
  multiple of the base alone.

## Performance

[Round 16](../cluster-testing/correctness.md#the-failover-window) on the lab, beside round 15's
16 s: about 10 s at the default base and about 2 s at 1 s, with no election in a loaded minute at
either and nothing lost. The loads under the new timers ran at 45,900 and 41,700 rows a second,
inside the bootstrap-to-bootstrap spread O64 records, and the quiet minutes' benches at 113,000
and 133,000 operations a second, inside the lab's run-to-run spread. Under load in the fixture,
the loop of six heavy tests at six threads (`target/lab/r16/142/loop-keep.sh`) ran eight rounds
on the new lease, 48 runs, with one failure that was [#197](../appendix/resolved/retired-driver-holds-slot.md)
and none in the six rounds after its fix; each round's children ran 1,087 to 1,575 elections
between them, all of them asked for by the tests' kills, restarts, isolations and partitions.
That count is the baseline a later round compares against: a round where it jumps is the one to
read, since the loop counts every election and not only the ones nobody asked for.

## Tests

| Test | What breaks if this is reverted |
| --- | --- |
| `a_group_config_keeps_the_timers_its_base_derives` (`shoal-core/src/server/shard/groups.rs`) | The timeouts are not half the base and the base |
| `a_dead_primary_fails_writes_only_until_its_election` (`shoal/tests/cluster_fixture.rs`) | A dead leader's group elects nobody within two failover bases and two seconds, where it was allowed four |
| The loaded loop of six fixture tests at six threads (`target/lab/r16/142/loop-keep.sh`) | A spurious election under load at the fixture's base, as a round's vote changes far above the 1,087–1,575 the tests themselves ask for |

## Related

- [C7](../distributed/failover.md), the window and what a client sees.
- [F42](primary-failover.md), the failover this narrows.
- [Cluster testing, round 16](../cluster-testing/correctness.md#17-round-16).
