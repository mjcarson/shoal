# F59. Shipping a backup to every host with `shoaladm ship-backup`

## Context

A backup ([F49](backup-and-recovery.md)) writes each group's file on the disk of the node that led
the group when it was cut: `<path>/<op>/<table>/<group>-<boundary>.snap`, with a manifest beside
it. A restore reads each group's file on the node that leads that group in the *new* cluster,
which is rarely the same node. So a restore needs every file on every node. Nothing in the cluster
moved them, and [runbook 10](../operations/runbooks.md#10-backup-and-restore) said so: *"Copy the
`<path>/<op>` directory out of the failure domain yourself; nothing ships it."* The lab did this
by hand with `rsync` in [section 4](../cluster-testing/correctness.md#back-up-destroy-and-restore)
of the cluster testing, and [what is left](../cluster-testing/todo.md) carried it as a limitation.

## What it does

```bash
shoaladm admin -i <inventory> backup /optane/shoal-backup      # prints the op
shoaladm ship-backup -i <inventory> /optane/shoal-backup/<op>  # every host holds it all
shoaladm destroy -i <inventory> --yes
shoaladm bootstrap -i <inventory>
shoaladm admin -i <inventory> restore /optane/shoal-backup/<op>
# or into another cluster's hosts, deployed or not yet
shoaladm ship-backup -i <old> /optane/shoal-backup/<op> --to <new>
```

`ship-backup` (`shoaladm/src/deploy/ship.rs`):

1. Lists what every host of the inventory holds under the directory, and of `--to`'s inventory
   when given, over ssh with `sudo find`. A host holding none of it is an empty list, not a
   failure: it led none of the groups.
2. Plans the copies. Each file a receiver lacks is read from the first host, in inventory order,
   that has it, and one stream runs per sender and receiver. A file a receiver already holds is
   never sent.
3. Runs each stream as `ssh <sender> sudo tar -cf - -T -` piped into `ssh <receiver> sudo tar
   --skip-old-files -xf -`, through the operator's machine, with the sender's list of files on its
   stdin. Nothing is stored locally.
4. Lists every receiver again, and fails naming any that does not hold every file.

The files keep their owner, which is the node's system user on every host of a deployment, so the
restore's node can read them.

## Design choices

- **A command of the deployment tool, not of the cluster.** The cluster has a bulk lane that
  streams snapshots between nodes, and a restore could fetch a missing file from whichever member
  wrote it. But the old cluster is usually gone by the time of a restore, which is the point of a
  backup, and a new cluster's nodes know nothing of the old one's hosts. The operator's machine
  knows both inventories and reaches every host already.
- **Every host gets every file.** A group's new leader is not known until the new cluster is up,
  and it moves. A copy on every host costs disk (3.6 GB a host on the lab) and makes the restore
  indifferent to leadership.
- **Streams through the operator's machine rather than host to host.** Hosts need no ssh access
  to each other, and a deployment's only trust is the operator's key on each host.
- **`--skip-old-files`, and a plan built from listings.** Running it twice sends nothing, and an
  interrupted run is finished by running it again. Nothing is overwritten, so a file that differs
  from its twin is left alone. The restore verifies every file against its manifest before
  trusting a record (F49), which is where a damaged copy is caught.
- **`attach`, not `open`.** It reads the inventory without its deployment checks, so it works
  after `destroy` deleted the cluster's local state, and before the new cluster is bootstrapped.

## Alternatives rejected

- **Gathering to a local directory, then pushing.** This needs the whole backup's size on the
  operator's machine and writes it twice. The plan reads each file once per receiver that lacks
  it, and nothing is stored.
- **`rsync`.** It is what the lab used by hand. It is not on every host, and the listing is all
  the diff this needs, since backup files are never rewritten.
- **Having the backup write every file to every member.** That would multiply a backup's writes
  by the member count during the backup, when the cluster is serving, and still leave a new
  cluster on new hosts without them.

## Limitations

- **The operator's machine carries every byte**, twice over its link: in from a sender and out to
  a receiver. On the lab that was 7.2 GB in 68 s through europa's 1 GbE, europa being a node as
  well.
- **Streams run one at a time.** Parallel streams would be faster on a network where the
  operator's link is not the limit.
- **The backup's path has to be the same on every host.** It is by construction, since `Backup`
  names one path, and the restore names one too.
- **No copy out of the failure domain.** This spreads the backup over the cluster's own hosts,
  which a backup is meant to survive. Keeping a copy elsewhere is still the operator's, and
  `--to` an inventory of other hosts is one way to do it.

## Invariants to uphold

- **A file on a receiver is never rewritten.** `--skip-old-files` is what makes a rerun safe, and
  a restore verifies each file against its manifest, never against its twin.
- **Success means every receiver holds every file any host held.** The command lists again after
  copying and fails otherwise, so a script can go straight to `restore`.
- **Nothing is written on the operator's machine.**

## Performance

On the lab, a backup of the loaded TMDB cluster (36 groups, 3.6 GB, 25 s) was held 22, 32 and 18
files by europa, titan and hyperion. `ship-backup` left each holding all 72 in 68 s, and a second
run sent nothing. Restored into a freshly bootstrapped cluster in 179 s, every group `Restored`,
and the csv verified through each member alone at `One`: 0 missing, 0 different
([cluster testing, round 13](../cluster-testing/correctness.md#a-backup-shipped-and-restored)).

## Tests

| Test | What breaks if the feature is reverted |
| --- | --- |
| `shoalctl` `deploy::ship::tests::each_receiver_is_sent_what_it_lacks` | A receiver is sent a file it holds, a file twice, or misses one only a later host holds |
| `shoalctl` `deploy::ship::tests::a_backup_directory_splits` | A relative or rootless directory is accepted, or the parent and name are split wrongly |
| `shoalctl` `deploy::ship::tests::the_scripts_name_the_backup` | The listing prints paths the tar cannot read back, or a path is not quoted |
| The lab run above (`target/lab/r13/ship/run.sh`) | A restore after `destroy` has files missing on the new leaders |

## Related

- [F49](backup-and-recovery.md), the backup and the restore.
- [Runbook 10](../operations/runbooks.md#10-backup-and-restore), which now names this command.
- [F51](cluster-deployment.md), the deployment it belongs to.
