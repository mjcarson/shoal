# 129. An overloaded group answered `OutcomeUnknown` rather than shedding

## Symptom

The [TMDB loader](../../features/tmdb-dataset-deployment.md) at its original gate, eight workers
× 4,096 queries in flight, stopped against a healthy three-node lab cluster about fifty seconds
into the load:

```text
a write failed: Server { …, code: OutcomeUnknown, msg: "group 2201a562745f2069 did not commit the write within the deadline" }
```

Every member was up and nobody was elected. The cluster was simply given more writes than it
commits in `replication.write_timeout` (5 s), and it answered the writes at the back of the queue
`OutcomeUnknown`: the leader might still commit them, so a client may retry one only under the
same identity. `Shedding`, the refusal that says nothing was applied, never came.

## Cause

A write on a cluster node is proposed through its group's leader with `client_write`, which
appends it to openraft's queue and returns once it commits. The only bound on that queue was
`replication.pending_bytes`, 64 MiB of proposed and unanswered bytes per group on the proposing
shard. At the loader's row sizes that is tens of thousands of writes, far more than a group
commits in 5 s, so it never fired first. And a write that hopped to its leader (the
`ReplicateKind::Propose` arm of `handle_replication`) was counted against no bound on the leader
at all. So overload came back as timeouts: every write that joined the queue behind more than
five seconds of commits waited out its deadline in openraft and was answered unknown.

## Evidence

**Reproduced against the unfixed tree** with `an_overloaded_group_sheds_rather_than_timing_out`.
Three nodes at a factor of three, `write_timeout` 1 s. The followers of one group complete each
append 100 ms after the last (the new fixture verb `SLOW_WAL`), so the group commits at a bounded
rate. Then 4,000 writes are sent to it through its leader and 4,000 through a follower at once:

```text
through the leader: {"OutcomeUnknown": 3400, "ok": 600}; through a follower: {"OutcomeUnknown": 4000}
```

**And on the lab**, on a freshly bootstrapped cluster running `ebf237e`, with the loader at 8 × 4,096
(`target/lab/r11/abload.sh`, arm `base`): two loads in a row stopped at 50 and 51 s on
`OutcomeUnknown` "did not commit the write within the deadline".

## The fix

Each group has a **gate** on each shard (`ProposalGate`, `server/replication/admission.rs`). A
leader hands a write to openraft only through it, both its own writes and those hopped to it.
The gate lets a bounded number of the group's writes into openraft at once. The rest wait in it,
in arrival order, and nothing has been appended for them. A write whose turn does not come
within a quarter of its budget is refused `Shedding`, which is definite
(`propose_through` in `server/shard/groups.rs`).

The bound follows what the group commits. Each write that passes the gate reports how long it
spent in openraft. One that took more than half its budget halves the bound, at most once per
such interval. One that took less raises it by one, so the bound roughly doubles each round while
commits are quick. It starts at 64, never falls under 8 and never rises over 1,024. So the queue
in openraft holds about what the group commits in half the budget, whatever the host, the device
or the load. A write that leaves the gate has three quarters of its budget left, and the bound is
halved long before a commit takes that long.

After the fix, the same test gives:

```text
through the leader: {"Shedding": 3936, "ok": 64}; through a follower: {"Shedding": 4000}
```

**On the lab**, the loader at 8 × 4,096 with retries unbounded (`--retries 1000000`), each run on
a destroyed and freshly bootstrapped cluster (`target/lab/r11/fresh-sweep.sh`). The gate was
switched off in two runs of the fixed build by setting its bounds out of reach:

| Build | Runs | Outcome of each | Rows a second | Retried: unknown | Retried: shed |
| --- | --- | --- | --- | --- | --- |
| `ebf237e`, before | 3 | 1 finished, 2 stopped on `IdentityExpired` | 29,476 (the one that finished) | 207,029 | 0 |
| fixed, gate off | 3 | 2 finished, 1 stopped on `IdentityExpired` | 10,156 and 6,659 | 1.3M and 2.0M | 0 |
| fixed, #143's first cut | 5 | all finished | 23,420, 24,667, 24,616, 19,384, 12,027 | 512, 268, 0, 384, 221,343 | 1.1M–1.96M |
| fixed | 1 | finished | 24,050 | 0 | 1.56M |

Five of the gated runs were on the first cut of [#143's quiet leader](silent-partition-hops.md),
which judged a leader quiet by openraft's `last_quorum_acked`. Under this load a follower's
acknowledgements queue behind its appends, so that judgement refused and abandoned writes a
healthy group would have committed. The run at 12,027 retried about five million writes on codes
the loader did not yet count separately. The check now judges the members' nodes on the network,
and the one run since shed and nothing else: no unknown outcome and no `NotLeader`.

What the gate buys under overload is that the load finishes, and that a client is told the
truth: a write was not applied, rather than may have been. Without it, the same load finished once
in three runs on each build. The runs that stopped did so on
[item 180](../known-issues.md#180-a-first-write-queued-past-a-groups-identity-memory-is-refused-identityexpired):
a write that waited in the server's queues longer than its group remembers identities was
refused as though it were a late retry.

**Nothing is lost at a normal load.** On the same lab, with the loader at its default gate and the
120 s mixed bench, both read back through each member (`target/lab/r11/abload.sh`):

| | Before (`ebf237e`) | Fixed |
| --- | --- | --- |
| Load at the default gate | 28,730 and 29,226 rows/s | 29,010 rows/s |
| Mixed bench | 109,061 and 105,042 ops/s | 105,099 ops/s |
| Bench p99, get / update | 30.5–30.8 / 180–190 ms | 33.0 / 210 ms |
| `verify`, and acknowledged inserts through each member | 0 missing, 0 different; 0 lost | 0 missing, 0 different; 0 lost |

The bench's spread between two runs of the same build (4%) is larger than any difference between
the builds.

## Alternatives rejected

- **A bound on the age of the oldest unanswered proposal.** This was the first cut, and it needs
  no estimate of anything. But a bundle arrives all at once, so every write in it is admitted
  before any of them has aged, and the test's burst of 4,000 still ended unknown.
- **A fixed count bound.** 1,024 in flight is a fifth of a second for a group committing 5,000 a
  second, and more than five seconds for one committing 200. The test's slowed group still
  answered 424 writes unknown at a bound of 1,024. Whatever a fixed number is right for, some host
  or load commits slower.
- **A bound from an estimated commit rate.** An idle group has no rate to estimate from, so a
  burst to one would be judged against nothing, or against a guess. The latency each write
  reports is the same information, measured on the writes themselves.
- **Halving at a quarter of the budget, and waiting at the gate for half.** Tried first. On a
  device whose cost is per sync rather than per entry, which is every device, a smaller bound
  means smaller batches and a lower commit rate. With the stricter mark, the test's group spiralled
  down to 8 in flight and committed 96 writes. A commit that takes half the budget has still met
  its deadline. Halving there keeps the batches large while the gate still guarantees the
  remaining quarter.
- **Shedding on the proposing shard only.** A write hopped to the leader would then join
  openraft's queue unbounded, as it did before. The gate is on the leader, where the queue is, and
  counts the leader's own writes and hopped ones in one line.
- **Keeping `pending_bytes` as the only bound.** It stays, as the memory bound it always was. It
  says nothing about time.

## Invariants to uphold

- **Nothing is appended for a write refused at the gate.** `Shedding` is definite only because
  the refusal comes before `client_write`. A permit is taken only where the write is about to be
  appended, when this shard leads or its lease has not started. It is let go on
  `ForwardToLeader`, since nothing was appended then either.
- **A permit lives until the write's outcome is known.** Dropping it early lets more writes into
  openraft than the bound says. Holding it past the outcome starves the gate. It is a local of
  `propose_through`, which returns with the outcome.
- **A place handed over is counted once.** `ProposalPermit::drop` hands its place to the oldest
  live waiter without touching `in_flight`. A waiter whose wait ran out closes its channel before
  it looks for a place handed to it (`enter`), so a place is never lost between a hand-over and a
  timeout.
- **The bound is judged on time in openraft, never on time at the gate.** Otherwise a queue at
  the gate would halve the bound that drains it.

## Still open

- The gate's bound and its shed count are not on the replication report, so an operator cannot
  see a group shedding except through the clients' `Shedding` counts.
- A write hopped from a follower waits in the same line as the leader's own writes. Under a burst
  through the leader, the hops arriving behind it are all shed: fair by arrival, not by path.

## Tests

| Test | What breaks if this is reverted |
| --- | --- |
| `an_overloaded_group_sheds_rather_than_timing_out` (`shoal/tests/cluster_fixture.rs`) | A group given more writes than it commits within `write_timeout` answers them `OutcomeUnknown`, through its leader and through a follower |
| `slow_tablet_does_not_block_other_tablets` (the same file) | A group with no quorum holds more than the gate's least bound to an unknown outcome, or sheds before anything pends |
| `a_full_gate_hands_places_over_and_sheds_the_late`, `a_waiter_that_gave_up_is_passed_over`, `the_bound_follows_how_long_commits_take` (`server/replication/admission.rs`) | The gate's hand-over, its refusal of a late write, and its bound's movement |
| The lab's load at 8 × 4,096 (`target/lab/r11/abload.sh`) | The loader stops on `OutcomeUnknown` about fifty seconds in |

## Related

- [Resolved #128](hop-deadline-margin.md), the wrong reason the same load's first failure carried.
- [C5](../../distributed/replication.md), the outcomes a write is owed.
- [Distributed cluster testing, section 11](../../cluster-testing/correctness.md#11-overload-silence-and-a-nearly-full-disk).
