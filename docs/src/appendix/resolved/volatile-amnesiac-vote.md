# 142. A volatile leader restarted empty voted for a follower missing what it had committed

Item 142 remains open on [Known Issues](../known-issues.md#142-two-fixture-tests-fail-intermittently-on-an-idle-host)
for its deadlines. This page is its one failure that came from inside openraft, and that failure
was a lost write, not a timing.

## Symptom

`migration_resumes_after_each_phase_failure` failed about one run in four, now and then with
openraft's debug assertion in a child node:

```text
thread 'unnamed-2' panicked at openraft-0.10.0-alpha.34/src/raft_state/log_state_reader.rs:25:13:
assertion failed: Some(log_id.to_ref()) <= self.committed().to_ref()
```

In `has_log_id`, the assertion states that a log id at an index below the copy's committed index
matches what the copy committed. It fired when a leader's append reached a follower that had
committed a different entry at the same index. That is two histories of one group, and one of them
had a committed entry the other did not.

## Cause

The group was the ephemeral table's, whose log lives in memory. The test kills the control
leader at each phase of a move, and at `reconfiguring` that node also led the group. Read from the
child logs of a failing run (`target/child-logs-mig`, `SHOAL_CHILD_LOG`):

1. Node `3e27` led the group at term 2 and committed index 47 with node `e567`. The third voter,
   `7dce`, held up to 46.
2. `3e27` was killed and restarted. Its copy came back with no log and no vote (`get_initial_state:
   vote: <T0-…>, last_log_id: None`). Item 109's marker recognised it as a copy that had held the
   group, so it did not initialize the group again (*"a volatile copy came back empty; it waits to
   be fed"*).
3. `7dce` stood at term 3 with last log 46. `e567` refused it on the log (*"reject vote-request:
   by last_log_id … 0.46 < … 0.47"*). The empty `3e27` granted.
   [Resolved #109](volatile-majority-loss.md)'s rule was: an empty copy that held the group grants
   nothing to an *empty* candidate, and grants to any candidate with a real log, on the view that
   such a candidate is the survivor.
4. `7dce` led with the votes of `7dce` and `3e27`. It then probed `e567` from index 15, and its
   entries at the indexes `e567` had committed carried term 3. `e567`'s core asserted.

Raft is safe because every majority that could elect a leader shares at least one voter with the
majority that committed an entry, and that voter refuses a candidate that lacks the entry. A copy
that forgot what it acknowledged no longer refuses. So `3e27`'s memory was part of the majority
that committed 47, and its vote made a majority without it. In a release build the assertion is
off, and index 47 was simply replaced: an acknowledged write to a table at a replication factor of
three was lost to one node's restart. The ephemeral contract loses what a *majority* forgot, not
what one member did.

The #109 rule was right about empty candidates. Its mistake was treating "has a log" as "has
everything I acknowledged". An empty copy cannot know what it acknowledged, so it cannot tell a
survivor from a laggard.

## Evidence

**Established from the logs, then reproduced.** The logs above came from the tenth run of a loop
of the migration test (two failures in twelve runs) on the tree at `5fa2b9a`.
`a_restarted_volatile_leader_elects_nobody_missing_its_commits` in `shoal/tests/cluster_fixture.rs`
builds the same situation on purpose. It uses three nodes at a factor of three, reads at `Quorum`,
and runs these steps:

1. Cut the data lanes between the group's leader L and one follower, C.
2. Write twenty rows of the group through L, so L and the other follower, K, commit them.
3. Kill L and cut K's data lanes.
4. Heal C's lanes to L and restart L empty.
5. Wait two seconds, heal K, and read every row back through K.

Only the data lanes are cut, so C is never judged isolated and stands for election. On the tree
before the fix, K's core met the same assertion in both runs taken:

```text
thread 'unnamed-2' (1380770) panicked at …/openraft-0.10.0-alpha.34/src/raft_state/log_state_reader.rs:25:13:
thread 'unnamed-2' (1381000) panicked at …/openraft-0.10.0-alpha.34/src/raft_state/log_state_reader.rs:25:13:
```

A first cut of the test isolated C on every lane instead, and passed on the unfixed tree. An
isolated node does not stand for a while after it heals
([the reserve change in `5fa2b9a`](wal-failure-stops-the-node.md)). Its reads were also served at
`One` from K's own copy, which held the rows whoever led.

With the fix the test passed three runs of three, in 7 s each. `migration_resumes_after_each_phase_failure`,
which failed two of twelve runs before, passed eight of eight. Item 109's test and the volatile
tests named in its table passed, as did `installing_tablet_never_serves_partial_state`.

## The fix

`grants_to_empty_candidate` in `shoal-core/src/server/shard/groups.rs`, which is judged on the
replication lane before openraft sees a vote or pre-vote. It applies to a volatile copy that held
the group before (item 109's `volatile/` marker) and holds no log past the bootstrap entry. Such a
copy now:

- **grants nothing for the grace** (~~two election timeouts~~ four leases from the copy's `up_since`,
  four bases either way: two of the old double leases, and since [F62](../../features/failover-window.md)
  twice the window a failover takes), so it counts
  as a member that is down. The members that kept their memory elect among themselves, and a
  majority of them can do that alone. One node restarting therefore never makes a leader of a copy
  that is missing what the node acknowledged;
- **after the grace, grants to a candidate with a real log.** That is item 109's survivor, when
  most of the group lost its memory at once and nobody else can lead;
- **after half a grace more, grants to any candidate**, empty or not, because the whole group lost
  its memory. The ephemeral contract applies: a majority forgot, so the data is gone.

A copy that has been fed a log again judges as openraft does, because what it holds came from a
leader.

## Alternatives rejected

- **Keep granting at once, but only to candidates whose log looks long enough.** An empty copy
  has nothing to compare a candidate's log with. Any rule it can apply at once is a guess about
  what it acknowledged.
- **Persist the vote and the last acknowledged log id of every volatile group.** That would make a
  volatile copy's vote as safe as a durable one's, like [#176](unreadable-voter-log.md)'s floor.
  But the floor would have to be synced before every acknowledgement, which is exactly the write
  an ephemeral table exists to avoid. A floor written now and then is not safe, since it trails
  what was acknowledged.
- **Ask the other members what was committed before voting.** The member that could answer may be
  the one that is unreachable, and the answer would be a second election protocol beside Raft's.
- **Remove a restarted volatile copy from the voters and add it back as a learner.** It is safe,
  but costs two membership changes per group per restart of a node. A group already tolerates one
  member down, and refusing votes for the grace is that member being down.

## Invariants to uphold

- **A copy that may have forgotten an acknowledgement never grants a vote before the grace.** One
  member's lost memory has to cost what one member down costs, and nothing more.
- **The grace is the same one item 109's head start waits**, two election timeouts. A copy that
  initializes the group after its head start is still refused by the other empties for half a
  grace more. The copies with a log therefore get a full election timeout in which only they can
  win.
- **Only a volatile copy with the marker is judged.** A fresh group's copies have lost nothing and
  elect at once.

## Still open

- A group whose members all lost their memory is dead for one and a half graces (six base
  timeouts, 30 s at the default 5 s base) instead of one. A whole-cluster restart pays that once
  for its ephemeral tables.
- After the grace, a candidate with a real log is still granted. If most of the group lost its
  memory, which is the contract's loss, a laggard can still win over a slower survivor. The
  survivor's election timeout is shorter than the window it is given, so this needs a survivor
  that did not stand for a whole election timeout.
- The other failures item 142 tracks are deadlines under the suite's load, and stay filed there.

## Tests

| Test | What breaks if this is reverted |
| --- | --- |
| `a_restarted_volatile_leader_elects_nobody_missing_its_commits` (`shoal/tests/cluster_fixture.rs`) | A restarted empty leader elects the follower that missed its commits; the rows are lost and the follower that kept them asserts |
| `an_empty_volatile_copy_grants_no_vote_for_the_grace` (`shoal-core/src/server/shard/groups.rs`) | The rule's cases one by one: nothing for the grace, a real log after it, any candidate after half a grace more |
| `two_volatile_voters_lost_at_once_do_not_kill_the_survivor` (`shoal/tests/cluster_fixture.rs`) | Item 109's case: two empties elect each other over the survivor |
| `migration_resumes_after_each_phase_failure` (`shoal/tests/cluster_fixture.rs`) | Where the assertion was first seen |

## Related

- [Resolved #109](volatile-majority-loss.md), whose grant rule this corrects.
- [Resolved #176](unreadable-voter-log.md), the same guarantee for a durable copy that lost its
  log, which can afford a synced floor.
- [F9. Ephemeral tables](../../features/ephemeral-tables.md), the contract.
