# 337. A cluster's first initialization was asked once, and a control election under it failed the start

A cluster is placed by one `Initialize`, sent once its members have joined
([F39](../../features/membership.md)). The fixture's start sends it once every node has joined, while
the leader is still promoting the joiners to voters. `shoaladm`'s bootstrap sends it once every node
is a voter ([F51](../../features/cluster-deployment.md)). The control plane answers an
administrative request it found no leader for `NotLeader`, a refusal its sender may ask again. A
proposal searches for its leader for a few seconds within its 10 s deadline (`propose`).

Found in the flaky-test plan's third workspace run after the confirmation loops (2026-10-29), the
third run to fail this way.

## Symptom

`a_restore_refuses_a_backup_without_its_record`, in workspace run three at `98824393`, failed before
it backed anything up:

```text
Error: ChildFailed("the initialization was not applied: {\"error\":\"NotLeader: no control leader could be reached; the cluster may lack a quorum. the voters are [NodeId(0e6d26a7-…), NodeId(750d58cd-…)] and this node reaches [NodeId(31668e3a-…), NodeId(750d58cd-…), NodeId(0e6d26a7-…)]; if a majority of the voters is gone for good, stop a survivor and run force_recover on its directory, which keeps it alone and tombstones the rest\"}")
```

Two voters, and the third member still being promoted. `an_installing_copy_is_refused_a_walk` in
F90's run and `a_restore_refuses_a_bucket_missing_an_archive` in #322's failed in the same setup with
the same answer ([Tests That Fail Under Load](../flaky-tests.md#left-the-page)).

## Cause

**The initialization was asked once.** The fixture's start (`Cluster::initialize`) took the first
answer, and the child's helper that sends every administrative verb asked again only on
`StaleVersion` ([Resolved #298](fixture-setup-under-load.md)'s item 118). The control group's
elections time out in 150 to 300 ms, and a member a loaded host held past that while it was promoted
stood for election. A proposal in the middle of that election searched for its leader, found none in
time, and answered `NotLeader`, as it is built to. The control plane was right, and the start
treated a refusal to ask again as final.

**`shoaladm`'s bootstrap does the same**, found by reading: `initialize` sends its request once,
and so do `activate` and `extend_schema`, which run once the leader's node has restarted. Its
bootstrap records every node before it initializes, so one that met an election could not be
finished: a second `bootstrap` is refused as deployed, and `deploy` takes the upgrade path, which
never initializes.

## Evidence

**Established by reading the source and the runs' messages, then reproduced.** The reproduction is a
row written for it, `an_initialization_rides_out_a_moment_with_no_control_leader`. It starts a
cluster uninitialized, cuts every control lane, and asks a follower of the control group to
initialize; a thread of its own heals the lanes three seconds on. A control link that is down fails
its request at once, so the follower's search for a leader ends with none. The members report every
five seconds there, so the silence calls nobody down. With the child's new retry turned off, two
runs of two failed:

```text
Error: ChildFailed("the initialization was not applied: {\"error\":\"NotLeader: no control leader could be reached; the cluster may lack a quorum. the voters are [NodeId(5cdc0b6b-…), NodeId(9a7533a5-…), NodeId(e4bc5d52-…)] …
test result: FAILED. 0 passed; 1 failed; 0 ignored; 0 measured; 281 filtered out; finished in 4.16s
```

With it on, three runs of three passed, in 5.4 s each. A first version of the row cut the lanes for
twelve seconds, past a proposal's deadline. Once healed, its leader called a member down for the
silence, and the initialization asked again was refused `NotUp`. That refusal is right, so the row
keeps its silence inside one report interval. The output is in `target/lab/i337/`.

## The fix

- **The fixture's child asks an administrative request again on `NotLeader`**, under the same
  operation, for `ELECTION_RETRY_WINDOW` (15 s), in all four of its helpers: the verbs' own, a
  planned operation's, `REPAIR`'s and `MOVE`'s. A commit an earlier try did
  not see is answered as it was. The window stays under the fixture's 30 s wait for a command's
  reply, with a proposal's 10 s deadline inside it: a reply after that wait would be read as the next
  command's. `StaleVersion` is asked again for `ADMIN_RETRY_WINDOW`, 10 s, as before.
- **`shoaladm` rides out a control election in the steps that follow joins and restarts**
  (`riding_elections`): the bootstrap's `Initialize`, under the operation id `cluster.json` keeps,
  and `activate` and `extend_schema`, each asked again for `ELECTION_RIDE` (60 s) at
  `ELECTION_PAUSE` (500 ms), reading the version again from its second try on. Every other answer is
  the step's, as before.

## Alternatives rejected

- **Search for the leader until the proposal's deadline in the engine.** The engine answers as it is
  built to. An operator asking a cluster that lost its quorum for good would wait 10 s for an answer
  that now comes in seconds and names the way out. A caller that means to ride out an election asks
  again under its operation.
- **Retry in the client's `admin`.** The interactive `shoaladm admin` command, where an operator
  should see `NotLeader` and its hint at once, sends through it too.
- **Wait for every voter before the fixture's initialization.** That narrows the window. An
  election can still follow a promotion, and the start would still take its first answer.
- **One window for both codes in the child.** `StaleVersion` is a view behind its leader, asked
  again at once; an election is a leader not yet there.

## Invariants to uphold

- **A refusal its sender may ask again is asked again by a step that has an operation id**: the
  fixture's helpers and `shoaladm`'s deploy steps, `NotLeader` and `StaleVersion` alike, the
  same operation each time.
- **A child's command answers within the fixture's 30 s wait**, so no window inside it reaches that
  wait, counting a last proposal's deadline.
- **A row that cuts the control lanes keeps its silence inside the detector's interval**, or a member
  is called down when they heal.

## Still open

- `shoaladm`'s `add --rebalance`, `rebuild`'s removal and the bench's events send their admin
  requests once. They run once a cluster is settled, so no election is expected under them.
- Nothing finishes a bootstrap that failed after recording its nodes on a build before this one:
  `destroy` and bootstrap again.

## Tests

| Test | What fails if the fix is reverted |
| --- | --- |
| `an_initialization_rides_out_a_moment_with_no_control_leader` (`cluster_fixture`) | A follower asked to initialize while no control leader can be reached answers `NotLeader`, and the start fails on it |
| `a_deploy_step_rides_out_a_control_election` (`shoaladm`, `deploy::ops`) | A deploy step takes its first `NotLeader` as final, or asks again a refusal that is not one |
| `permanent_quorum_loss_requires_explicit_recovery`, `fourth_data_node_does_not_change_control_voter_count` and `mixed_table_bundle_resolves_each_table_policy` (`cluster_fixture`) | Unchanged: each still reads a minority's `NotLeader`, now once the child's window is over |

## Related

- [Resolved #298](fixture-setup-under-load.md), whose item 118 made the child ask `StaleVersion` again.
- [F39](../../features/membership.md), the control group and its promotions;
  [F51](../../features/cluster-deployment.md), the bootstrap.
- [Tests That Fail Under Load](../flaky-tests.md#left-the-page).
