# F53. An inventory wizard, and storage per node group

## Context

[F51](cluster-deployment.md) made deploying a cluster a program, but the program starts from an
inventory an operator writes by hand. The keys were learned from `lab.yml` and the F51 page, and
a mistake in them (a factor larger than the bootstrap set, a typo in a key, a port used twice)
was found only when `bootstrap` refused the file.

Every node was also given the same storage. `render.rs` wrote `<remote_dir>/data` into both
`storage.default.filesystem.latency_sensitive.path` and `throughput_sensitive.path`, whatever the
host. The engine had split the two since before F51: intent logs, archive maps and the marker go
under the latency path, the archives under the throughput path, and
[Resolved #43](../appendix/resolved/marker-every-root.md) locks and marks every distinct root. So
a host with an NVMe for the logs and a large disk for the archives could be served, but not
deployed.

The ask was twofold:
- an interactive way to build an inventory, to make a new cluster easier to deploy;
- per-node storage directories, defined for **groups** of nodes, so that hosts sharing hardware
  do not repeat the same settings.

## What it does

### Node groups

An inventory gains three keys:

```yaml
storage:                          # every node, unless its group or itself says otherwise
  latency: /srv/shoal/logs
  throughput: /srv/shoal/archive
groups:                           # named settings a node takes by naming one
  small:
    resources: {cores: 4, memory: 4Gi}
    storage: {latency: /mnt/nvme/shoal, throughput: /mnt/bulk/shoal}
nodes:
  - {name: hyperion, address: 172.16.2.5, group: small}
  - {name: titan, address: 172.16.2.4, group: small, storage: {throughput: /mnt/hdd/shoal}}
  - {name: europa, address: 172.16.2.10}
```

A node now carries `group` and `storage` beside its existing `resources`. Settings are resolved
by `Inventory::resolve_storage` and `Inventory::resolve_resources`
(`shoalctl/src/deploy/inventory.rs`), and each reports the level it took its value from:
- **Storage is merged field by field.** The node is checked first, then its group, then the
  deployment. An unset `latency` is `<remote_dir>/data`. An unset `throughput` is the resolved
  latency.
- **Resources are replaced whole.** A node's own resources win, otherwise its group's,
  otherwise the deployment's. This is F51's existing rule, extended by one level.

Resolved, a `Node` carries `storage: NodeStorage { latency, throughput }`. Its `roots()` lists
the distinct directories with the primary first, the same order the engine's `Storage::roots`
gives. Everything that touches a node's data uses them:
- `render` writes the two paths into `shoal.yml`;
- `stage` creates every root and hands it to the node's user;
- preflight reports a node as claimed if *any* root holds `shoal-meta.json`;
- `--wipe` and `destroy` delete every root.

`destroy` resolves each node's storage without resolving its address, so a host that no longer
resolves is still cleaned up.

`validate` now also refuses:
- a node naming a group that is not listed;
- a group name outside `[A-Za-z0-9_-]`;
- a `remote_dir`, or any node's resolved storage directory, that is not absolute, contains `.`
  or `..`, or is fewer than two components deep (`/` and `/mnt` are refused by name);
- a storage directory that is the remote directory, holds it, or overlaps its `bin` or `tls`;
- a node whose latency and throughput directories are nested one inside the other (equal is
  fine).

`validate` is split in two. `validate_shape` judges everything the file says, and `validate`
adds the check that the server program exists.

### `cluster new`

`shoalctl cluster new -o <file> [--from <inventory>]` (`shoalctl/src/wizard.rs`) is a full-screen
form over six pages:

| Page | What it edits |
| --- | --- |
| Cluster | the name, the server program, the remote directory, the user, the admin, the tracing level |
| Shape | the factor, the voters, `retire_after`, the three ports |
| Defaults | the deployment's resources and storage |
| Groups | a list of groups, each with its resources and storage |
| Nodes | a list of hosts: name, ssh target, address, group, bootstrap, and the node's own resources and storage |
| Review | every issue, what each node resolves to and from which level, and the file that will be written |

**The draft is judged on every key**, by the code `bootstrap` judges the file with:
- a field that does not parse is an error on that field;
- `validate_shape`'s refusal is placed on the page and field it names;
- a server program that does not exist yet (checked against the output file's directory, the
  way `load` resolves a relative path) is a warning, never an error, and so is one that exists
  but is not executable, such as a source file. `validate` refuses the second at bootstrap;
- a node given no address has its name resolved here, off the event loop, through the same
  `lookup` and `dialable` that `bootstrap`'s `resolve` uses. The Nodes page shows the answer. A
  name that resolves only to loopback is an error on the address field, and a name that does not
  resolve is a warning ([Resolved #127](../appendix/resolved/wizard-loopback-address.md)).

The sidebar counts the errors on each page. `s` on the review writes the file only when there
is no error, asks before replacing an existing file, writes a `.partial` first and renames it
into place, and prints the `bootstrap` command.

**`p` on the Nodes page probes the selected host.** It makes one read-only ssh round trip in
batch mode, off the event loop (`wizard/probe.rs`), and reports:
- the host's cpus and memory;
- the free space on the filesystem each of the node's roots would be created on (the nearest
  existing ancestor is measured);
- any root that already holds a marker.

`--from` loads an inventory to edit. It is loaded without being judged, so a broken file can be
opened and fixed. A relative `server` is kept as written when the output is in the same
directory. Anywhere else it is made absolute, so it still names the same program.

## Design choices

- **Storage merges per field, resources replace whole.** The storage fields are `Option`s at
  every level, so an unset one is visibly unset, and a group that names only fast logs should
  not have to repeat the archive path. `Resources.memory` has a serde default, so a level that
  names no memory cannot be told from one that names `4Gi`, and a field-by-field merge would
  silently hand a node the default memory. Keeping F51's whole-replace rule for resources avoids
  that, and the wizard says so on every resource field. Once a group or node sets any resource,
  its blank memory reads `4Gi`, not `inherited`.
- **One group per node.** A node names at most one group. Several groups per node would need a
  precedence order between groups, which is exactly the kind of setting whose value an operator
  cannot see at a glance. The review table shows every resolved value with its source instead.
- **`throughput` defaults to the resolved latency, not to the next level's throughput.** An
  inventory with no storage at all renders exactly what F51 rendered, and a group that names
  only `latency` moves the whole node there. The alternative, a group's latency combined with the
  deployment's default `<remote_dir>/data` for archives, splits a node across two disks nobody
  chose.
- **The paths are used as given.** `destroy` and `--wipe` delete exactly the directories the
  inventory names, so validation refuses the ones where that is dangerous.
- **The form is pure, the view draws.** `wizard/form.rs` holds the draft and returns an
  `Outcome` for every key. `wizard/view.rs` draws from it, and nothing else draws. This is the
  cluster tab's convention ([F50](cluster-operations.md)). Every rule in the wizard is a unit
  test with no terminal, and the drawing is tested on ratatui's `TestBackend`.
- **Groups are named by a draft identity, not by name.** A node's group choice holds the group's
  `id`, so renaming a group one keystroke at a time never re-points a node at another group that
  shares a prefix.
- **No new dependencies.** The wizard uses ratatui, crossterm, serde_yaml and kanal, which
  shoalctl already had. `tempfile` is a new dev-dependency at the version the lockfile already
  resolved for three other crates. F51's invariant that shoalctl links no engine and grows the
  graph by nothing holds.

## Alternatives rejected

- **Line prompts on stdin.** They are simpler and scriptable, but an inventory is a whole
  judged at once (the factor has to fit the bootstrap set, a group has to exist before a node
  names it), and prompts walk it one question at a time with no way back. The operator chose the
  full-screen form.
- **A prompt crate (`inquire`, `dialoguer`).** Neither is in the lockfile, and F51 holds
  shoalctl to what the lockfile already resolves.
- **A per-cluster subdirectory under each storage path** (`<path>/<cluster>`). It would make
  `destroy` unable to delete anything but its own, but the directory in `shoal.yml` would not be
  the one the operator typed. The rules on dangerous paths buy most of the same safety without
  the surprise.
- **Per-table storage in the inventory.** The engine takes `storage.tables.<name>` with its own
  pair of paths. It was left out because no inventory needs it yet, and it would add a table
  list to every level. See [todos](../appendix/todos.md).

## Limitations

- **Changing a deployed node's storage moves nothing.** A node's paths are rendered at stage
  time, which is `bootstrap` or `add`. An inventory edited afterwards describes directories the
  running node does not use until it is staged again, and a node staged onto new directories
  starts empty. Moving a node's data is the rehome's job
  ([F47](local-rehome.md)), and the deployment does not drive it.
- **Per-table storage is not rendered.** Every table of a node uses its two directories.
- **A blank address is judged by this machine's resolver.** `bootstrap` resolves on whichever
  host runs it. An inventory written on one host and bootstrapped from another can resolve
  differently, and a name's answer is cached for the whole session
  ([Resolved #127](../appendix/resolved/wizard-loopback-address.md)).
- **A probe reads, it does not decide.** It reports free space and cpus, and neither is checked
  against the resources the node is given.
- **The wizard edits the keys it knows.** An inventory is re-serialized from its parsed form, so
  `--from` drops every comment in the source file (`lab.yml`'s notes included), and writes
  default values out explicitly (the ports, `control_core_shared: false`).

## Invariants to uphold

- **`NodeStorage::roots()` is the engine's `Storage::roots()`**, the same directories in the
  same order, primary first. Preflight's claimed check, `stage`'s ownership, and `destroy`'s
  deletion all read it. `a_group_split_renders_the_roots_the_engine_claims` compares the two on
  a rendered file.
- **Every directory `destroy` deletes passed `check_dir`.** A new path-valued key in the
  inventory has to go through `check_dir` and `check_clear_of` in `validate_shape`, or `rm -rf`
  will be run on a path nobody checked.
- **An inventory with no storage and no groups renders what F51 rendered**: both paths
  `<remote_dir>/data`. `a_group_supplies_what_a_node_does_not_set` asserts it.
- **The wizard judges with `validate_shape`, never its own copy of the rules.** A refusal added
  to the inventory reaches the wizard with no change to the wizard. If its words name no field,
  it is placed on the review page, so it is never lost.
- **The form draws nothing and touches no file.** I/O is `wizard::save` and `wizard::probe`.
  A save and a probe are reached through an `Outcome`. A name's resolution is started by the loop
  from `Wizard::unresolved`, and its answer comes back into `Wizard::resolutions`. Nothing in
  `build` or a key handler resolves a name.

## Performance

Not applicable. Nothing on a node's path changed: a node given the same two directories is
served exactly as before, and one given two is served the way `Conf` always served a split.

## Tests

| Test | Where | What breaks if this is reverted |
| --- | --- | --- |
| `a_group_supplies_what_a_node_does_not_set` | `shoalctl/src/deploy/inventory.rs` | The node → group → deployment order, the per-field storage merge, throughput falling back to latency, or the F51 default |
| `a_group_replaces_resources_whole` | `shoalctl/src/deploy/inventory.rs` | A group's resources are merged by field and hand a node the deployment's memory |
| `a_storage_root_that_destroy_could_misuse_is_refused` | `shoalctl/src/deploy/inventory.rs` | `destroy` runs `rm -rf` on `/`, `/mnt`, a relative path, the remote directory, `bin`, `tls` or a nested root, or a missing group is deployed |
| `a_node_file_names_the_directories_its_group_gives_it` | `shoalctl/src/deploy/render.rs` | The rendered `shoal.yml` stops naming the resolved directories under their own writers |
| `a_group_split_renders_the_roots_the_engine_claims` | `shoal-bench/tests/deploy_render.rs` | The deployment's roots and the engine's disagree, so a root is left unowned, unwiped or unchecked |
| `a_draft_round_trips_an_inventory` | `shoalctl/src/wizard/form.rs` | `--from` changes an inventory it was only asked to open, `lab.yml` and a groups inventory included |
| `a_cluster_typed_in_builds_its_inventory` | `shoalctl/src/wizard/form.rs` | Keys stop building the inventory they describe, or a new node stops following its predecessor's group |
| `an_issue_lands_on_its_field` | `shoalctl/src/wizard/form.rs` | An error is reported on the wrong page or field, or an unbuilt program refuses the file |
| `a_group_in_use_cannot_be_deleted` | `shoalctl/src/wizard/form.rs` | Deleting a group strands its nodes, or a rename loses them |
| `losing_or_replacing_work_is_asked_about` | `shoalctl/src/wizard/form.rs` | Esc drops a changed draft, a save replaces a file unasked, or a draft with an error is written |
| `a_blank_resource_says_what_it_means` | `shoalctl/src/wizard/form.rs` | A blank resource says `inherited` where the whole-replace rule makes it the default |
| `a_source_names_its_level` | `shoalctl/src/wizard/form.rs` | The review stops saying where a value came from |
| `a_loopback_only_name_is_refused_before_saving` | `shoalctl/src/wizard/form.rs` | A node whose name resolves only to loopback saves clean and `bootstrap` refuses it ([#127](../appendix/resolved/wizard-loopback-address.md)) |
| `a_source_file_is_not_a_server_program` | `shoalctl/src/wizard/form.rs` | A source file named as the server program raises no warning |
| `a_resolution_tells_loopback_from_nothing` | `shoalctl/src/wizard/probe.rs` | The wizard cannot tell a loopback answer from no answer |
| `a_probe_reads_what_its_script_prints` | `shoalctl/src/wizard/probe.rs` | The probe's script stops running, or its output is misread |
| `a_relative_server_is_rebased_onto_the_new_inventory` | `shoalctl/src/wizard.rs` | `--from` into another directory names another program |
| `the_review_page_shows_what_each_node_resolves_to` | `shoalctl/tests/wizard.rs` | The review draws the wrong resolution or source, a column truncates it, or a page panics at 80×24 |
| `a_saved_inventory_loads` | `shoalctl/tests/wizard.rs` | The saved file is not an inventory `Inventory::load` accepts, is not the one built, or leaves a `.partial` |

The wizard was also driven in tmux against `lab.yml`: a group was added, two nodes were put in
it, europa was probed, and the file was saved and loaded by `tmdbctl cluster logs -i`. Driving it
found three things the unit tests had not:
- a truncated resources column;
- `inherited` shown on a group's blank resource fields;
- `--from` keeping a relative `server` that no longer named the program.

## Related

[F51. Deploying a cluster](cluster-deployment.md), whose inventory this extends;
[shoalctl](../operations/shoalctl.md);
[Resolved #43](../appendix/resolved/marker-every-root.md), the marker in every root that
preflight's claimed check reads;
[F47. Local rehome](local-rehome.md), the only thing that moves a node's files;
[F50](cluster-operations.md), whose model/view split the wizard follows.
