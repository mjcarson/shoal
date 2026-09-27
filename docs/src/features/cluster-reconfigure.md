# F57. Rendering a deployed cluster's files again with `shoalctl cluster reconfigure`

## Context

`cluster upgrade` ([F55](cluster-upgrade.md)) replaces a node's program and never its `shoal.yml`.
So a change to what an inventory says, or to what the renderer writes, reached no deployed node.
The lab added `node_memory` to its nodes' files by hand
([Resolved #149](../appendix/resolved/node-memory-budget.md)), and ran
[O61](../appendix/optimizations.md#o61-a-fast-device-syncs-the-wal-in-batches-too-small-to-fill-a-page)'s
WAL commit delay by editing europa's file on the host, since an inventory had no key for it.
Both were filed as todos: re-rendering a deployment's node files, and a per-group commit delay.
The todo said re-rendering would need the admin's credential, which the tool deliberately does not
keep. It does keep it: bootstrap mints one into the deployment's state directory
(`admin.password`), which is what every command authenticates with.

## What it does

`cluster reconfigure -i <inventory> [node...] [--force]` goes through the deployed nodes one at a
time, the control leader last, as `upgrade` does. For each node it:

1. reads the `shoal.yml` the node runs on, and keeps the entry it was deployed with: `bootstrap:
   true` for the node that minted the cluster, the seeds it was given for a joiner;
2. renders the file afresh from the inventory, with that entry and the deployment's credential;
3. compares the two with the credential taken out, since its salt is new every render. A node
   whose file is unchanged is left alone unless `--force`;
4. otherwise writes the new file, owned by the user the node runs as, restarts the node and waits
   until it is back and caught up, exactly as an upgrade does;
5. and if the node does not come back on the new file, writes the old one back, restarts it
   again, and stops, naming the node.

The inventory gains a key for the WAL group commit delay, **`wal_commit_delay`**, at each of its
three levels: the deployment, a group and a node, resolved most specific first like storage. It is
rendered as `cluster.replication.wal_commit_delay`, and refused above the engine's 10 ms. It
belongs to a device, so a group is the level to set it at. The lab's inventory sets it on europa's
group alone:

```yaml
groups:
  bd795i:
    storage:
      latency: /optane/shoal-tmdb
    wal_commit_delay: 3ms
```

The inventory wizard carries it through an edit at every level, and gained a field for
`failover`, the other key it could only carry.

## Design choices

- **Keep the entry the node was deployed with.** A node that bootstrapped the cluster and one that
  joined it were rendered differently. Rendering every node as a joiner, or as the bootstrap one,
  would change a file for no reason. The entry is read from the file itself, which the deployment
  owns.
- **Compare values, not text.** The two files are compared as YAML values with `auth.users` taken
  out. A textual comparison would find every file changed, because of the salt, and restart every
  node every time.
- **Only a changed node is restarted.** The command is meant to be rerun after every inventory
  edit, and a restart costs a node's leads a transfer. A rerun after a partial run resumes where it
  stopped.
- **The leader last, and one at a time,** for the same reason an upgrade does: a restart hands the
  node's leads on, and the control leader's handoff is an election.

## Alternatives rejected

- **Rendering files in `cluster upgrade`.** An upgrade that also changed configuration could not
  be rolled back by swapping the program back. Two commands keep the two changes apart.
- **Asking for the admin's password.** The deployment already holds it, from bootstrap. Asking
  would make a script of the command impossible for no gain.
- **Editing a node's file in place, key by key.** The renderer is the one definition of a node's
  file. A second, key by key, would drift from it.

## Limitations

- A key the renderer does not write cannot be set this way, and a key added to a file by hand is
  dropped by the next reconfigure of that node. The lab's hand-added `node_memory` is rendered
  since #149, so nothing on the lab was lost.
- The wizard has no form field for `wal_commit_delay`; it carries whatever an inventory names.
- A node that is down is not reconfigured: the health judgement refuses the run, as for an upgrade.

## Invariants to uphold

- **A node's entry is never changed by a reconfigure.** A node that joined is never rendered as the
  one that bootstraps.
- **A failed node is left on the file it had.** Nothing is left half-written: the file is written
  beside its target and renamed, as every deployment write is.
- **The credential is always derived from the deployment's password**, never read back from the
  host's file.

## Performance

Nothing on a node's path. A reconfigure restarts only the nodes whose file changed, and each
restart is a planned stop, which hands its leads on first
([Resolved #139](../appendix/resolved/leadership-handoff-on-stop.md)). On the lab, reconfiguring
europa's group for the 3 ms delay restarted europa alone. hyperion and titan were reported as
already rendered.

## Tests

| Test | What breaks if this is reverted |
| --- | --- |
| `only_the_credential_is_ignored` (`shoalctl/src/deploy/upgrade.rs`) | Every node is restarted on every run, or a real change is missed |
| `a_group_split_renders_the_roots_the_engine_claims` (`shoal-bench/tests/deploy_render.rs`) | A group's `wal_commit_delay` does not reach the engine as the duration it named, or one over 10 ms is accepted |
| `a_draft_round_trips_an_inventory` (`shoalctl/src/wizard/form.rs`) | The wizard drops `wal_commit_delay` or `failover` from an inventory it edits |
| `the_failover_base_is_a_field` (the same file) | The failover base cannot be typed, blanked or refused in the wizard |

## Related

- [F55](cluster-upgrade.md), the rolling upgrade this mirrors.
- [F53](inventory-wizard.md), the inventory and its groups.
- [O61](../appendix/optimizations.md#o61-a-fast-device-syncs-the-wal-in-batches-too-small-to-fill-a-page), the commit delay.
