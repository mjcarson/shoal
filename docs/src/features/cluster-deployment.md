# F51. Deploying a cluster with `shoalctl cluster`

## Context

After [F50](cluster-operations.md), everything a cluster needs on its first day existed, and
none of it was a program. [C14](../distributed/deploying.md) opened with "there is no `shoal`
server binary and no `shoalctl` binary", and [runbooks 1 and 2](../operations/runbooks.md) were
procedures an operator followed by hand:
- write one `shoal.yml` per node, bootstrap or seeds, with its advertise address and the
  cluster's admins;
- issue each node a leaf naming an id that did not exist until the node's first claim;
- copy a server program that had to be written first, against the schema;
- start the nodes in order, wait for `Members`, send `Initialize` once, and wait for
  `Readiness`.

The one remote launcher, shoal-bench's `--remote`, stands a node up for one capture of one arm
and kills it afterwards; it does not describe a cluster somebody keeps.

Two physical hosts made the gap concrete. hyperion and titan are 4c/8t Zen1 machines reached
from the development host over keyless ssh. The ask was for `shoalctl` to bootstrap new clusters
and add nodes to existing ones, **for any schema** (TMDB as well as the bench schema), so that
Shoal can be tested against a real cluster from now on.

It found one defect on the way. The cluster tab's model had counted no voters and no learners
in any cluster since F50, because it read as numbers two fields the server writes as lists
([Resolved #114](../appendix/resolved/cluster-tab-voter-count.md)).

## What it does

### Two generic entry points, one per half

A schema is a compile-time construct on both halves, and the client refuses a server with
another schema fingerprint at the hello. So each half has one generic entry point, and a
schema's own program is three lines:

| Half | Entry point | Commands | Links |
| --- | --- | --- | --- |
| Server | `shoal::server::node::main::<Db>()` | `serve --conf`, `claim --conf` | the engine |
| Tool | `shoalctl::cli::main::<DbClient>()` | `tui`, `cluster …` | the client alone ([F15](client-server-split.md)) |

Two pairs are built:
- **Bench:** `shoal-bench`'s `shoal-node` and `shoal-benchctl`.
- **TMDB:** `shoal/examples/tmdb_node.rs` and `shoalctl/examples/tmdbctl.rs`. Both include one
  tables file, `shoalctl/examples/tmdb/tables.rs`, so they cannot drift apart.
- **TMDB dataset** ([F54](tmdb-dataset-deployment.md)): the `tmdb-dataset` crate's
  `tmdb-dataset-node` and `tmdb-dataset-loader`, one crate and one schema type. The tool half is
  `shoalctl::cli::run` with a `load` of its own beside the shoalctl commands.

`tmdbctl` with no arguments still opens the terminal UI at `127.0.0.1:12000`.

### `serve` and `claim`

**`serve`** is the program C14 told every operator to write:
- `Conf::from_file`, `trace::setup`, `ShoalPool::start` and `ready`, then a ready line;
- it refuses a `--conf` path that does not exist, rather than serving the defaults;
- it holds the pool up and exits on a dead shard, so the supervisor restarts it;
- on SIGTERM or SIGINT it exits the pool cleanly.

**`claim`** prints `{"node", "cluster", "slots"}` for the storage directory and starts nothing.
It is `shoal::server::claim`, which is what `ShoalPool::start` does before its first shard:
- the same `resolve_executors` (validate the block, place the control core, resolve the cpus);
- the same `claim_root` (lock the directory, `StorageMeta::claim` for the configured intent).

The one thing `claim` does not read is `cluster.tls`, since the leaf is issued for the id it
returns. So a leaf naming `shoal-node://<id>` is issued **before the node's first start**. F50
had listed that as explicitly unsupported ("first-boot certificate provisioning").

### The inventory and the state

An inventory is YAML, one per cluster (`shoalctl/src/deploy/inventory.rs`). It names:
- the cluster, and the server program to copy;
- the remote directory (default `/opt/shoal-deploy/<name>`);
- the ports;
- the factor and the voters;
- default resources and each node's own, and since [F53](inventory-wizard.md) a named group's
  that a node takes by naming it;
- since F53, the storage directories (`latency`, `throughput`) for the deployment, a group or a
  node, each directory resolved on its own. ~~Every node's data lives in `<remote_dir>/data`.~~
  That is now only the default;
- the admin principal;
- the system user nodes run as (`user`, created where missing);
- `retire_after`, the one move setting rendered;
- `failover`, rendered as `cluster.primary_failover_after`, the base every group's election
  timeout and lease derive from; the engine's five seconds if absent. Added by the
  [cluster testing](../cluster-testing/performance.md#failover-time-against-primary_failover_after)
  chapter to measure failover against it. The wizard keeps an inventory's value when it edits one
  and has no field for it yet;
- the hosts, as a name, an ssh target and an address;
- optionally, the bootstrap set.

`validate` refuses:
- voters other than 1, 3 or 5;
- a factor above the bootstrap set;
- duplicate names or addresses;
- colliding ports;
- a server program that has not been built;
- any unknown key;
- since [F53](inventory-wizard.md), a node naming a missing group, and any storage directory
  `destroy` could not safely delete: relative, shallow, holding `..`, nested, or overlapping the
  remote directory's `bin` or `tls`.

An address that is not given is resolved locally, and a loopback answer is refused. A
machine's own `/etc/hosts` commonly maps its name to `127.0.1.1`, which no peer can dial.
`cluster new` resolves the same way while the inventory is written, and will not save a node
whose name answers only loopback ([Resolved #127](../appendix/resolved/wizard-loopback-address.md)).
The `server` program has to be an executable file. A source file is refused by name.

What the deployment mints is kept on the operator's machine under `~/.shoal/clusters/<name>/`
(`$SHOAL_DEPLOY_HOME` overrides the root). The directory is `0700` and every secret in it `0600`:
- the authority's certificate and key;
- the admin password (`$SHOAL_ADMIN_PASSWORD` overrides it);
- `cluster.json`: the cluster id, each deployed node's name, id, address and ssh target, and
  the `Initialize` operation id.

### `cluster bootstrap`

For the inventory's bootstrap set, in order:
1. **Preflight every host before touching any.** One ssh round trip reports the user, the cpus,
   whether sudo needs a password, whether systemd and the kernel `tls` module are there, and
   ~~whether the remote directory already holds a claimed marker~~ whether any of the node's
   storage directories holds a marker ([F53](inventory-wizard.md)). A claimed marker is refused
   unless `--wipe` is given. The inventory's `user` is created as a system user where it is
   missing.
2. **Stage each node.**
   - Create the directories, owned by the user the node runs as.
   - scp the program to the login's `/tmp`, compare its `sha256sum` against the local file,
     and `install` it into place as that user's.
   - Write the node's `shoal.yml` (`deploy/render.rs`), mode `0600` because it carries the
     admin's derived SCRAM credential.
   - The first node gets `bootstrap: true`. The rest get `seeds` naming its control address.
3. **Claim as the node's user, then issue the leaf** for the id the claim printed (`deploy/pki.rs`). Its SANs are
   the host name, the address and `shoal-node://<id>`. Write it, its key and the authority.
4. **Start the first node** under its systemd unit (`deploy/unit.rs`), connect as the admin, and
   wait until it is an up member with one voter.
5. **Start the rest**, and wait until every one is up with `min(control_voters, n)` voters.
6. **`Initialize`** in inventory order, under an operation id written to `cluster.json` before
   the request goes out, then wait for `Readiness` to admit default writes.

A cluster that `cluster.json` says was deployed is refused a second bootstrap.

### `cluster add <node> [--rebalance]`

For a node the inventory lists and the state does not:
- Read the cluster's `Members` frame through any deployed node. The joiner's seeds are the
  committed control address of every member whose phase is `member`.
- Preflight, stage with those seeds, claim, leaf and start it, as bootstrap does.
- Wait until every deployed node is up with its voters.
- With `--rebalance`, send `Rebalance` and follow its plan record through
  `components::follow_once`, the function the cluster tab follows it with. Without it, the node
  is told it holds nothing yet.

### And the day-to-day

- `status`: every node's id and address, whether its unit is active, and the cluster tab's
  lines as a member draws them.
- `start`, `stop`, `restart [node]`: systemctl on one node or every deployed node.
- `logs <node> [-n]`: its journal.
- `upgrade [node...]`: since [F55](cluster-upgrade.md), runbook 7 - a new program on every
  node, one at a time, with a swap back for a node that does not come back.
- `rebalance`.
- `destroy --yes`: visits every host the inventory lists, deployed or not. It disables and
  deletes the unit, deletes the remote directory and, since [F53](inventory-wizard.md), every
  storage directory the node resolves, and deletes the local state.
- `tui --inventory`: the terminal UI against the first node that answers, authenticated as the
  admin.

### The lab

`shoalctl/inventories/lab.yml` is europa, hyperion and titan at a factor of three.
`lab-add.yml` is hyperion and titan at a factor of two, with europa listed for `add`. Both hold
four shards per host. The program has to be built for the oldest cpu, not `native`:

```sh
CARGO_TARGET_DIR=target/deploy RUSTFLAGS="-C target-cpu=znver1" \
    cargo build --release -p shoal-bench --bin shoal-node --bin shoal-benchctl
target/deploy/release/shoal-benchctl cluster bootstrap -i shoalctl/inventories/lab.yml
```

## Design choices

- **A generic `main` in the engine, not a binary per schema in the repository.** The program
  C14 asked every operator to write was the same twelve lines each time. Writing it once, with
  `claim` beside `serve`, is what makes the deployment schema-independent: shoalctl copies
  whatever program the inventory names and never compiles one.
- **`claim` is the start's own code.** `resolve_executors` and `claim_root` were cut out of
  `ShoalPool::start` rather than written again. A claim that resolved the cpus differently from
  the start would issue a leaf for an identity with another slot count.
- **The server's configuration is a mirror, pinned by a test.** shoalctl cannot link
  shoal-core's `Conf`, so `render.rs` serializes its own structs. `deploy_render.rs` in
  shoal-bench, which links both, parses every file rendered as a `Conf` and runs the engine's
  cluster validation on it. `Conf` refuses unknown keys in every section rendered, so a rename on
  either side fails there.
- **A system unit through sudo.** The node outlives the deployment, comes back after a reboot,
  logs to the journal, and is stopped by the SIGTERM `serve` answers. `LimitMEMLOCK=infinity`
  is set for io_uring, and `ExecStartPre=+/sbin/modprobe tls` for the peer lanes.
- **A system user of the node's own.** io_uring charges the memory a ring locks to the user
  that created it and checks it against the caller's `RLIMIT_MEMLOCK`. The unit's
  `LimitMEMLOCK=infinity` lets the node lock what it needs, and that total is then charged to
  its user. The first lab deployment ran the europa node as the operator: every glommio test on
  europa then died at glommio's io_uring probe (`Failed to register a probe`), and passed again
  the moment the node was destroyed. `user: shoal` in the lab inventories keeps the node's
  budget apart from the operator's; without `user`, a node runs as the ssh login, which is fine
  on a host nobody develops on.
- **Mutual TLS with the binding on, always.** Every deployed cluster is the one F50 recommends:
  a leaf per node, `bind_identity: true`. The deployment is what made the leaf-per-node cost
  nothing.
- **The authority is rebuilt from its key.** Only the key and the certificate written at mint
  are kept. A later leaf is signed by an issuer rebuilt from the key under the same
  distinguished name, and a verifier matches a leaf's issuer by that name and checks the
  signature against that key. This keeps rcgen's certificate parser (`x509-parser`) out of the
  build. `pki.rs`'s test issues from a rebuilt authority and loads the leaf under the
  certificate written at mint.
- **`Initialize`'s operation id is persisted before it is sent**, so a deployment that died
  after sending it resends the same operation. It is answered `Repeated` rather than refused as
  a second placement.
- **Every wait reads the cluster tab's model.** `cluster::poll`, lifted out of the tab, is
  what bootstrap and add wait on. The deployment and the tab cannot disagree about what "up"
  means, and one of them disagreeing with the server is how item 114 was found.
- **ssh in batch mode, and every command line a value.** A host that would prompt is refused.
  `Host::ssh_command`, `scp_command` and `write_script` return `Vec<String>`s the unit tests
  read, the way `RemoteSpec` does.

## Alternatives rejected

- **Extending shoal-bench's `--remote`.** It stands up a node per arm, under a staged file
  whose marker the driver minted, and kills it after the capture: the lifetime of a
  measurement. A cluster somebody deploys outlives every command that touched it.
- **nohup and a pid file.** No system change is needed, but a node does not survive a reboot,
  its log is a file nobody rotates, and stopping it is a `kill` of a pid that may have been
  reused.
- **A shared leaf with `bind_identity: false`.** It avoids the claim-before-leaf ordering, and
  gives up what F50 built: any member could speak as another.
- **Starting each node once in plaintext to learn its id**, as C14 described. It is a start
  whose only purpose is to write a marker, on a node that briefly serves without the binding.
- **Linking `Conf` into shoalctl.** It would put the engine back into the client half, which
  F15 split out and `shoal-client-check` exists to keep out.
- **Compiling the server on each host.** The hosts have no toolchain, and a build per host is
  a cluster of several builds. The digest check refuses exactly that for a copied program.

## Limitations

- **One node per host per cluster.** The inventory refuses two nodes on one address, since
  they would share its ports. Two clusters can share hosts only on different ports, and
  `lab.yml` and `lab-add.yml` do not.
- **Clients connect in plaintext.** The deployment secures the peer lanes. `networking.tls` for
  clients is not rendered, so the admin's SCRAM exchange crosses the network unencrypted
  (SCRAM never sends the password itself).
- **The program is the operator's to build**, for the oldest cpu among the hosts. A build for a
  newer one is caught at the claim by its SIGILL (exit 132) and refused by name, before
  anything starts. Since [F55](cluster-upgrade.md), `upgrade` refuses it the same way, from a
  `--version` run on the host, before the node's program is replaced.
- **Losing the state directory loses the authority**, and with it the ability to issue a leaf
  to a node added later. The cluster itself is untouched. Nothing backs the directory up.
- **`add` never decommissions, and `destroy` is all or nothing.** Removing one node is still
  runbook 4 through the cluster tab.
- **A rebalance step takes at least `retire_after`.** A move finishes only once its source has
  reclaimed the retired copy. At the engine's default of five minutes, the first lab rebalance
  onto europa moved four sets in twenty minutes and was still planning more when the smoke
  test's timeout ended it. So the inventory renders `retire_after`, and the lab's is `15s`. At
  that, the smoke test's whole run took 144 seconds, five-set rebalance included. It is the only
  tuning key rendered.
- **A claim counts as a start.** `StorageMeta::claim` bumps the marker's incarnation, so a
  freshly deployed node's first `Members` row says incarnation 2.

## Invariants to uphold

- **`server::claim` and `ShoalPool::start` claim through the same two functions.** A leaf is
  issued for the identity the claim returns, and it is only right if the start returns the
  same one. `deploy_render.rs` asserts `pool.identity().node` equals the claim's.
- **Claim before leaf.** A leaf is never issued for an id that no claim of that directory
  printed.
- **`Initialize` is sent at most once per cluster** under the id in `cluster.json`, and never by
  `add`.
- **The rendered file is a `Conf`.** A field added to the mirror has to be one `Conf` accepts,
  in the section `Conf` has it in; `deploy_render.rs` is the check.
- **shoalctl links no engine.** `cargo tree -p shoalctl | grep -c glommio` is 0, and every
  dependency added here is one the lockfile already resolved.
- **Every wait reads the model the tab draws**, so a model defect fails a deployment rather
  than hiding in a view.
- **Every file under the node's directory is its user's**, written through sudo and handed over
  before its rename, and `claim` runs as that user: a directory claimed by one user and served by
  another is refused at the lock. Since [F53](inventory-wizard.md) that includes every storage
  root outside the remote directory, which `stage` hands over on its own.

## Performance

None claimed. Bootstrap of the three-node lab took seventeen seconds end to end on the first
run, most of it the program copied three times. The live smoke test on `lab-add.yml` -
bootstrap at factor two, a thousand rows written and read back through every node, `add europa
--rebalance` moving five sets at `retire_after: 15s`, a read through europa, `destroy` - took
144 seconds. Moving `resolve_executors` and `claim_root` out
of `ShoalPool::start` is behavior-preserving, but it is under the macro, hotpath and stages
layers' source paths, so every capture before it reads as not describing the current code.

## Tests

| Test | Where | What breaks if this is reverted |
| --- | --- | --- |
| `a_rendered_node_claims_starts_and_initializes` | `shoal-bench/tests/deploy_render.rs` | A rendered bootstrap file is no longer a `Conf` (its `retire_after` included), a claim disagrees with the start, or the rendered admin cannot `Initialize` |
| `a_deployed_cluster_serves_every_row_from_every_node` | `shoal-bench/tests/deploy_smoke.rs` | Gated on `SHOAL_DEPLOY_INVENTORY`: bootstrap, rows through every node, `add --rebalance`, `destroy` on real hosts |
| `an_inventory_defaults_to_the_documented_cluster` | `shoalctl/src/deploy/inventory.rs` | The defaults stop being the documented ports, factor, voters and paths |
| `an_inventory_that_cannot_be_a_cluster_is_refused` | `shoalctl/src/deploy/inventory.rs` | An impossible cluster is deployed rather than refused |
| `a_node_resolves_off_the_loopback` | `shoalctl/src/deploy/inventory.rs` | A node advertises `127.0.1.1` to its peers |
| `state_is_private_and_minted_once` | `shoalctl/src/deploy/state.rs` | The key or password become readable, or the password changes between runs |
| `a_leaf_names_its_node_under_a_rebuilt_authority` | `shoalctl/src/deploy/pki.rs` | A node added later gets a leaf that no node trusts, or that names no node |
| `a_node_file_says_how_it_enters_and_holds_no_password` | `shoalctl/src/deploy/render.rs` | A joiner bootstraps, or the password lands on a host |
| `the_unit_runs_the_deployed_program` | `shoalctl/src/deploy/unit.rs` | The unit runs another program, as root, or without the tls module |
| `commands_are_batch_mode_and_quoted` | `shoalctl/src/deploy/remote.rs` | ssh prompts, a path with a space splits, or an owned file is renamed into place before it is handed over |
| `a_command_reports_its_output` | `shoalctl/src/deploy/remote.rs` | A failure is read as success |
| `members_are_ready_when_every_node_is_up_and_voting` | `shoalctl/src/deploy/ops.rs` | A wait ends before the voters exist |
| `seeds_are_the_placeable_members_control_addresses` | `shoalctl/src/deploy/ops.rs` | A joiner seeds through a leaving member or its client port |

## Related

[C14. Deploying a cluster](../distributed/deploying.md), which this makes a program;
[Runbooks 1 and 2](../operations/runbooks.md); [shoalctl](../operations/shoalctl.md);
[F50](cluster-operations.md), whose leaf binding this issues for;
[F15](client-server-split.md), which keeps the tool client-only;
[Resolved #114](../appendix/resolved/cluster-tab-voter-count.md), found here.
