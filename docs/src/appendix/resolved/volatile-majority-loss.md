# 109. A volatile group's survivor tripped an openraft debug assertion when a majority lost its memory log at once

## Symptom

An ephemeral table's group keeps its log in memory, so a member that restarts comes back with
none. When two of a group's three voters restarted at once they came back empty together and
could elect each other - a candidate with an empty log wins against another with an empty log -
and the new leader appended fresh entries at indexes the surviving third voter had committed.
The survivor's `has_log_id` then met a `prev_log_id` below its committed index with a different
leader id and asserted, a `debug_assert!`, so the group's `RaftCore` task panicked on the
survivor and the group was dead on that shard until the process restarted; the shard did not
die, so the pool reported no failure. Seen by [F45](../../features/replica-migration.md)'s
crash matrix when both drivers of an ephemeral table's group died together.

## Cause

Two things, and the first is the one that matters. A restarted volatile copy is
indistinguishable to openraft from a fresh one: no log, no vote, `is_initialized` false. So
`start_group` treated it as new - the placement primary initialized the group again, writing
the bootstrap membership at index zero, and stood for election; the other empty copy granted,
since the candidate's log was as good as its own; and a leader was elected whose log began
where the survivor's had a hundred committed entries. The second is that nothing noticed a
core that had died: `Group.raft` stayed `Some`, the metrics stayed at their last value, and
the report said `up`.

## Evidence

**Reproduced.** `two_volatile_voters_lost_at_once_do_not_kill_the_survivor` in
`shoal/tests/cluster_fixture.rs`: three nodes at a factor of three and a base of one second,
twenty rows written to the ephemeral table through node zero and agreed by digest, nodes one
and two killed at once and restarted at once, and the survivor's view of every volatile group
polled for a leader at a term above the one before - or for the survivor still leading - with
its copies alive throughout. Against the tree at `3735cab` with the two changes neutralised,
twice, the test failed at its twenty-second deadline:

```text
the volatile groups elected nobody after two voters came back empty: …
```

with one group still at term 1 led by a restarted node in the survivor's view: the empties
had initialized the group again and elected between themselves at the old term, and the
survivor was left following a leader whose log was not its own, with no election of its own
coming. The assertion itself did not fire in those two runs; F45's crash matrix is where it
was seen, with the child's panic quoted on the item. With the fix, four runs: every group
elected within a few seconds - the survivor leading each - the twenty rows read back, ten more
written and agreed on every node, no child dead.

## The fix

Three parts in `shoal-core/src/server/shard/groups.rs`:

- **A copy remembers that it held a volatile group.** The first time a volatile group comes up
  on a shard, a marker named by the group is written under `wal/Shard-N/volatile/`; at the next
  start `rebuild_groups` reads the markers and a copy of a marked group that is empty is
  `held_before`: empty because its memory went, not because the group is new.
- **Such a copy does not initialize the group again, and does not stand.** `start_group`
  gives it the head start the non-primaries already had - `elect` off for two election
  timeouts - and initializes only if, after that, nobody has: the members that kept their
  memory lead and feed it, and if none did the group is new again after the grace, which is
  the ephemeral contract said out loud.
- **Such a copy grants no vote to a candidate as empty as itself.** `grants_to_empty_candidate`
  is judged on the replication lane before openraft sees the request: a volatile copy that
  held the group before and holds no log now refuses a candidate whose last log index is at
  or below the bootstrap entry, and grants to one with a real log - the survivor - until two
  election timeouts have passed with nobody feeding it, after which it grants as any copy
  would. Two empties cannot elect each other over a survivor; a group where every member lost
  its memory elects after the grace.

And the shard notices a dead core: on every sweep `probe_cores` asks each group's handle a
question only a running core answers, bounded to two hundred milliseconds, and a `Fatal`
answer is kept as `core_dead` on the slot and in the report, where `up` is false for as long
as it is.

## Alternatives rejected

**Restart the survivor empty on detection**, the way a `Rebuild` quarantine does. It throws
away the one copy that had the data, and the data is exactly what the rule keeps.

**Truncate the survivor's committed entries under a rule the follower accepts.** That is
openraft's engine, and the assertion is its statement that a committed entry is not truncated.

**Read the node's incarnation for "restarted".** The fixture stages every marker before the
first start, so a fresh fixture node is at incarnation two and every copy looked restarted; a
fresh cluster's volatile groups then waited out the grace before their first election. The
per-group marker says what the incarnation cannot: whether *this* shard held *this* group.

**Detect the dead core through openraft's `running_state`.** It records a core that stopped
with an error, not one that panicked; the probe answers for both.

## Invariants to uphold

- **The `volatile/` markers are written on every up and read once, when the shard starts.**
  `Replication.held_volatile` is that one scan; `rebuild_groups` reads it and never the disk
  again, so a group rebuilt within a run - the bootstrapper's single-member groups replaced by
  the placement's, say - is not taken for one that lost its memory. The first cut of this
  scanned on every rebuild and stalled a fresh cluster's bootstrap on exactly that, which the
  suite caught and [Resolved #106](isolated-member-term-inflation.md)'s commit corrected.
- **A group of one voter initializes itself whatever it held before**: nobody else can feed it.
- **An empty held-before copy neither initializes nor grants to an empty candidate for the
  grace**, two election timeouts. Shorter and two empties elect each other again; a group
  where every member lost its memory is new after it.
- **The grace is judged from the copy's `up_since`**, so a copy that came up late is not judged
  by a group that came up early.
- **`up` is false for a copy whose core is gone**, and `core_dead` says why.

## Still open

- A group where every member lost its memory is dead for the grace, two election timeouts,
  before it is new again.
- A dead core is reported and not restarted; the copy serves nothing until the process does.
- A restarted volatile copy that is fed by snapshot rather than log is
  [F43](../../features/node-recovery.md)'s path, unchanged - which means it is `installing`,
  and unreadable, for as long as the snapshot takes to land in memory. Before this it had
  re-initialized and led an empty group of its own, so a read through it was served at once
  and served nothing; `installing_tablet_never_serves_partial_state` reads the ephemeral table
  through a returning node in that window and met it once under the suite's load.

## Tests

| Test | Where | What breaks if this is reverted |
| --- | --- | --- |
| `two_volatile_voters_lost_at_once_do_not_kill_the_survivor` | `shoal/tests/cluster_fixture.rs` | The empties initialize the group again and elect between themselves; the survivor follows a foreign log or its core dies; the rows are lost |
| `an_empty_volatile_copy_grants_no_vote_to_an_empty_candidate` | `shoal-core/src/server/shard/groups.rs` | The grant rule's cases, one by one |
| `volatile_replication_uses_common_encoding`, `a_volatile_group_purges_its_log` | `shoal/tests/cluster_fixture.rs` | A fresh volatile group's first election, and a whole restart's, still happen |
| `scheduled_scrub_quarantines_without_an_operator`, `corrupt_follower_is_quarantined_and_repaired_from_a_verified_source` | `shoal/tests/cluster_fixture.rs` | A `Rebuild` restart of a volatile copy is still fed by its leader |

## Related

[F40. Replication](../../features/replication.md), whose volatile groups these are;
[F9. Ephemeral tables](../../features/ephemeral-tables.md), the contract;
[F44. Repair](../../features/repair.md), whose `Rebuild` restarts a copy empty on purpose;
[Resolved #99](durable-log-reversion.md), which lets a leader feed an empty member;
[Resolved #105](volatile-groups-never-purged.md).
