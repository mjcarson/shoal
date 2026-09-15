# 110. The kill arm's client failed a steady share of its operations for as long as node one was dead

## Symptom

The failover arm's client does not retry, by design, so an outage is visible as errors. From
the kill at eight seconds to the restart at sixteen the client saw about two hundred errors a
second against fourteen hundred operations, every second, and they stopped only when node one
was back and re-elected. The item read the arm's `p50` - back to a quarter of a millisecond
inside two seconds - as the data groups having failed over, and a seventh of the operations
still failing as a write whose primary was dead being refused rather than proposed through node
zero's own replica: "route by health" broken for writes.

## Cause

The reading was wrong in both halves, and the timeline kept no error code to say so.

The operations that keep succeeding are the six sevenths that never touch a group node one
led: every read is `One` from the local replica, and the writes to the other groups commit as
before. The `p50` came back because those are most of the run. The seventh that fails is every
write to a group node one led, and each fails at once with `NotLeader`: node zero holds a
replica of that group and proposes through it, its openraft answers `ForwardToLeader` naming
node one's shard, the hop is refused unsent because the link is down, and
[F42](../../features/primary-failover.md) turns an unsent hop into `NotLeader` on purpose so a
client that retries can go elsewhere. A follower cannot commit a write; nothing but an election
ends the refusals, and at the cluster's default failover base of five seconds the survivors
stand only once their lease of node one lapses - `election_timeout_max`, twice the base - plus
a timeout of their own: three to four times the base, fifteen to twenty seconds. The smoke
arm's kill lasts eight and a half. No election could have happened inside it, and node one's
return before its own lease lapsed is [item 103](returning-leader.md)'s
shape on top. The F42 page's smoke table says as much - "until the survivors elect, which at a
five second base is ten to fifteen seconds after the kill" - and the item read the table's
`during` p50 as the election.

## Evidence

**Reproduced, with the codes.** The timeline now keeps each failure's error code, and a smoke
run of `macro/cluster/failover/kill` on this host against the tree at `bcf9aa4` with the
codes recorded (`--allow-dirty`, into a scratch directory under `target/`, deleted):

| Window | Ops | Errors | By code |
| --- | ---: | ---: | --- |
| `before` (0 - 8.0 s) | 13,067 | 13 | `OutcomeUnknown` 13 |
| `during` (8.0 - 21.4 s) | 11,105 | 1,819 | `NotLeader` 1,780, `Unavailable` 39 |
| `after` (21.4 - 24.6 s) | 163 | 3 | `NotLeader` 3 |

Two hundred and twenty errors a second through the outage, `NotLeader` by name, and the
`during` p50 at 294 µs; node one restarted at 16.7 s and the outage ended at 21.4 s, thirteen
seconds after the kill, which is the lease and an election at the default base.

**The design, pinned by a test.** `a_dead_primary_fails_writes_only_until_its_election` in
`shoal/tests/cluster_fixture.rs` kills node one at a failover base of one second and writes a
key it led through node zero without a retry, every quarter second: thirteen refusals over
3.96 s, every one `NotLeader` and answered inside two seconds, then the write served in 12 ms,
then a hundred writes across the groups with no failure. That is the lease and an election at
base one, on this host, twice.

## The fix

No change to the write path: it does what [F42](../../features/primary-failover.md) says, and
a follower proposing a dead leader's write through its own replica would be a write nobody
commits. What changed is that the arm now says what it saw. Every failed operation on the
timeline carries its error code's name - or `client` for a failure the client had itself -
and every window of a fault, background, backup, catch-up or migration record carries
`errors_by_code` beside `errors`, mirrored into the explorer's index and defaulted so the
committed captures read back as they were; a run's counts carry `failed/<code>` beside
`failed`. The item this page closes could not have been filed as it was against a capture that
said `NotLeader`.

## Alternatives rejected

**Propose through the local replica when the node holds one**, the item's fix direction. The
node already does, and the replica is a follower: its group's log is the dead leader's, and
openraft answers a follower's proposal with the leader to forward to. Committing there would
be a second leader.

**Have a `Down` verdict start an election.** The control plane's detector commits node one
`Down` well inside the lease, and a group's followers could be told to stand on it. They would
be refused: openraft's vote handler rejects any candidate while the receiver's lease of the
current leader has not lapsed, and no API expires a follower's lease early. The lease is the
failover floor by construction.

**Shorten the lease by narrowing `election_timeout_max` towards the base.** A tuning of the
failover window C7 records as a decision, with a split-vote cost, and not what this item is
about. Filed under C15's measured-at-smoke-scale remainder rather than changed here.

## Invariants to uphold

- **A write to a dead leader's group is refused at once, `NotLeader`**, never waited out to a
  timeout; the test asserts the refusal inside two seconds. A client with a retry covers it.
- **The failover window is lease plus election**, three to four times the base. An arm or a
  test that expects writes to a dead leader's group to succeed sooner is expecting a second
  leader.
- **Every failed timeline sample names its code.** `TimelineSample::code_of` is the one place a
  client error becomes a name; a new failure path in a driver goes through it.

## Still open

- The failover window itself: base plus two seconds was the objective as set, and three to
  four times the base is what the lease gives; C15 carries it.
- The kill arm's smoke schedule kills node one for less time than the default base takes to
  elect, so its `during` window is the whole outage and its `after` window is the returning
  leader's re-election (item 103); a smoke run of it measures the lease and nothing else.

## Tests

| Test | Where | What breaks if this is reverted |
| --- | --- | --- |
| `a_dead_primary_fails_writes_only_until_its_election` | `shoal/tests/cluster_fixture.rs` | A write to a dead leader's group waits rather than being refused, fails with anything but `NotLeader` or `Unavailable`, is refused past four failover bases, or a write after the election fails |
| `fault_capture_preserves_outage_time_series` and the window tests | `shoal-bench/src/workloads/harness/fault.rs` | A window's `errors_by_code` is not cut from the samples' codes |

## Related

[F42. Primary failover](../../features/primary-failover.md), whose rule this is;
[C7. Failover](../../distributed/failover.md); [Resolved #103](returning-leader.md),
the returning leader; [C10](../../distributed/performance.md), the arm's record.
