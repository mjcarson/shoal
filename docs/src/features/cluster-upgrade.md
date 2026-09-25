# F55. A rolling upgrade with `shoalctl cluster upgrade`

## Context

[F51](cluster-deployment.md) deploys a cluster from an inventory and can start, stop and restart
its units. It had no way to put a new build of the node program on a running cluster, though.
[Runbook 7](../operations/runbooks.md#7-rolling-upgrade) was the only path, and an operator ran it
by hand on every host: stop the node, install the build, start it, wait for `Readiness` and for
`Replication.lag_max` to reach zero, move to the next node, and finally `Activate { wire }` from
the cluster tab. Each step is simple, but together they are long and easy to get wrong. The easy
mistakes are moving on before a node has caught up, restarting a second node while a set is short
a copy, and activating before every member speaks the new version.

This feature makes the runbook a program. **It replaces the node program and nothing else.** A
schema change is still a new cluster and a restore ([F48](rolling-compatibility.md)), and nothing
here migrates an archive, a marker or a WAL format.

## What it does

```
shoalctl cluster upgrade -i <inventory> [NODE...] [--force] [--activate] [--rollback]
```

The program installed is the inventory's `server:`, the same file `bootstrap` and `add` copy. To
upgrade, rebuild it (or point `server:` at another build) and run `upgrade`.

1. **Gate.** The cluster has to be healthy enough to lose one node at a time. `judge_health`
   refuses, and names the reason, unless all of these hold:
   - every recorded node is an `up` member in the `member` phase;
   - default writes are admitted;
   - no replica set is under its factor;
   - no plan is unfinished.
2. **Order.** The nodes run in the record's order, with the control leader last
   (`upgrade_order`), so the control group elects a new leader once instead of up to once per
   node. `NODE...` narrows the run to those nodes.
3. **Per node, one at a time:**
   - Read the installed program's owner, its sha256, and whether a `.prev` sits beside it, all in
     one round trip. If the node already runs this exact program, skip it unless `--force` was
     given. This is what lets a rerun after a failure resume where it stopped.
   - Push the program to the login's temp dir under the digest check `stage` uses. The code is
     shared: `push_binary` and `install_binary`, taken out of `stage`. Then install it beside the
     node's program as `<program>.candidate`, owned by the node's user.
   - Run `<candidate> --version` as that user. A build for a newer cpu dies of SIGILL here and
     is refused by name before anything the node runs has changed.
   - Keep the running program as `<program>.prev`, unless it is the same build. Rename the
     candidate over it. The running process keeps its own inode.
   - `systemctl restart` the unit, then wait:
     - until the unit is `active` (`failed` stops the wait at once);
     - until every recorded node is an up member again, with the voters bootstrap asked for (the
       wait `bootstrap` and `add` use);
     - until **the node itself** answers and reports `caught_up`: default writes admitted,
       `lag_max == 0` and no snapshot installing. `Readiness` and `Replication` are node-local,
       so this is the restarted node's own view, not a peer's.
   - **If the node does not come back**, swap `.prev` back in, restart, and wait again. Then
     stop the upgrade with an error that names:
     - the node and the step it failed at;
     - whether the revert brought it back;
     - the last fifty lines of the journal from the failed run.
4. **After every node**, print the activated wire version and the lowest and highest a member
   reports. With `--activate`, if the lowest is above the activated version, send
   `Activate { wire: lowest }`. Without it, print the command to run instead. Activation is the
   point past which no node can roll back, so it is never done unless asked. Running
   `upgrade --activate` again on an upgraded cluster restarts no node and only activates.
5. **`--rollback`** runs the same loop and the same gate. For each node it swaps `<program>`
   with `<program>.prev`, restarts it and waits for it. If the node does not come back, it swaps
   the two again. A swap can be undone, which a delete could not. A node with no `.prev` is
   refused by name. `--rollback` conflicts with `--force` and `--activate`.

## Design choices

- **Rename rather than stop, copy, start.** The program is replaced while the old process still
  runs, so the node is down only for the restart. A crash between two steps leaves one whole
  program in place, either the old one or the new one.
- **The `--version` run is the SIGILL check.** It is the cheapest way to start the program on
  that host without touching the node's directory. `claim` would do it too, but `claim`
  reads the marker, and on a claimed directory it counts as a start ([F51](cluster-deployment.md)
  notes this).
- **The digest decides both the skip and the `.prev`.** When the installed digest is the new
  one, the node is skipped, and under `--force` `.prev` is left alone. A forced rerun never
  overwrites the program a rollback would go back to with the program it just installed.
- **The run user comes from the program's owner**, not from a preflight. `preflight` refuses a
  claimed directory by design, and `stage` installed the program owned by the user the node
  runs as.
- **Leader last**, read from the `Members` frame at the gate. Leadership may move during the
  run, and then the order is only a heuristic. Getting it wrong costs one extra election, not
  correctness.
- **The waits read the model the cluster tab draws**, as every other deployment wait does
  ([F51](cluster-deployment.md)'s invariant). A model defect fails an upgrade rather than
  hiding in a view.

## Alternatives rejected

- **Stop every node, install, start every node.** Simpler, but the cluster is down for the
  whole restart, and a build that fails to start takes every node with it.
- **Activating automatically at the end.** The activation is the rollback point. An operator
  who wants a soak period between the last restart and the point of no return would have to
  remember to turn it off. It is opt-in instead: the command prints the line to run.
- **Stopping and leaving a failed node for inspection.** One node down in a cluster at factor
  three still serves, but a second failure from anything else during the investigation costs
  a quorum. Swapping back keeps the cluster whole. The journal of the failed run is captured
  before the revert's own run adds to it.
- **Rolling back by deleting the new program and copying the old one over.** The old program
  would then live nowhere but the operator's machine. Keeping `.prev` on the host makes a
  rollback independent of what the operator has built since.
- **Suspending the node's removal grace with `Maintenance` around the restart.** `Maintenance`
  only suspends a grace that has already opened; for a member that is still `up` it is
  refused as `WrongPhase`. See Limitations.

## Limitations

- **The program only.** The rendered `shoal.yml`, the unit, the certificates and every storage
  root are left as they are. A build that needs a new configuration key needs it rendered by
  hand, and a build that changes the schema id is refused at the join. That failure is caught
  as "did not come back" and reverted.
- **One node at a time, with no failure domains.** The inventory has no notion of a rack or
  zone, so the upgrade cannot restart a whole domain at once as runbook 7 allows.
- **A short `auto_remove_after` can race it.** The restarted node is `down` until it reports
  again. An `auto_remove_after` shorter than a restart could open, and expire, a grace in
  between. The defaults are minutes and a restart is seconds, but nothing checks this.
- **`.prev` is one generation.** Two upgrades without a rollback between them leave the first
  build nowhere on the host.
- **`caught_up` wants `lag_max == 0`.** A node under steady write load can show a lag of a few
  entries on most polls. The wait polls once a second for three minutes, so a single zero is
  enough, but a heavily loaded cluster could time out and revert a healthy node.
- **After an activation, `--rollback` fails.** The older build refuses to start below the
  activated wire version. The wait fails and the swap is reverted, so the node comes back on
  the new build, and the error explains why.

## Invariants to uphold

- **Nothing the node runs changes before the candidate has run `--version` on that host.** The
  candidate and the temp copy are the only files written up to then.
- **`.prev` is written only when the installed digest differs from the new one.**
- **Every replacement is a rename**, and every rename is within the program's own directory, so
  a crash leaves one whole program at the path the unit runs.
- **One node at a time, and the next starts only once the last is `caught_up` as it sees
  itself** and every recorded node is up with the voters.
- **An upgrade never activates unless `--activate` was given, and a rollback never does.**
- **`stage` and `upgrade` push and install the program through the same two functions**
  (`push_binary`, `install_binary`), so a digest check added to one is in both.

## Performance

None claimed. A node is down for its restart and its catch-up: about the length of a
`systemctl restart`, plus however long its groups take to reach their leaders. The push costs
what `stage`'s did, once per node.

## Tests

| Test | Where | What breaks if this is reverted |
| --- | --- | --- |
| `an_unhealthy_cluster_is_refused_by_name` | `shoalctl/src/deploy/upgrade.rs` | A node is restarted while another is down or leaving, writes are refused, a set is short a copy, or a plan is moving data |
| `the_leader_is_upgraded_last` | `shoalctl/src/deploy/upgrade.rs` | The leader is restarted mid-run, a narrowed run restarts other nodes, or a typo in a node name is ignored |
| `caught_up_needs_no_lag_and_no_install` | `shoalctl/src/deploy/upgrade.rs` | The next node is restarted while this one is still behind its leaders |
| `only_a_higher_common_version_is_activated` | `shoalctl/src/deploy/upgrade.rs` | `--activate` sends a version a member does not speak, or one already activated |
| `upgrade_parses_its_nodes_and_refuses_a_forced_rollback` | `shoalctl/src/cli.rs` | The node list is not positional, or a rollback can activate |
| `a_deployed_cluster_serves_every_row_from_every_node` | `shoal-bench/tests/deploy_smoke.rs` | Gated on `SHOAL_DEPLOY_INVENTORY`: after the adds, `upgrade --force` restarts every node through the whole wait and every row is read back through every node; a plain `upgrade` then skips them all |

## Related

[Runbook 7](../operations/runbooks.md#7-rolling-upgrade), which this is as a program;
[F51](cluster-deployment.md), whose `stage` it shares the push with;
[F48](rolling-compatibility.md), whose negotiation makes a mixed cluster work and whose
`Activate` ends the window; [shoalctl](../operations/shoalctl.md).
