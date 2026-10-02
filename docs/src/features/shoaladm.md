# F63. `shoalctl` and `shoaladm`, and a deployment built from a Rust project

## Context

A Shoal schema is a compile-time construct, so every program that serves it or talks to it is
built against it. Before this feature that was the operator's job four times over. To deploy a
database somebody had written as a `#[shoal::db]` struct they wrote a node program
(`shoal::server::node::main::<Db>()`) and a tool program (`shoalctl::cli::main::<DbClient>()`),
built the node **by hand for the oldest cpu among the hosts** with a `RUSTFLAGS` line copied out
of a page, pointed an inventory's `server:` at the artifact, deployed with `mytool cluster
bootstrap`, and typed `mytool tui` to see a row. The [lab inventory](../../../shoaladm/inventories/lab.yml)
opened with the cargo line to remember, and a build for the wrong cpu was caught only when the
node died of SIGILL on the host ([F51](cluster-deployment.md)).

The user asked for three things. Split `shoalctl`, the terminal UI, from `shoaladm`, the tool
for everything else (deploy, upgrade, backup, and the rest), so that nobody types `shoalctl
tui`. Let `shoaladm` deploy a database from nothing but a Rust project directory: find the
`#[shoal::db]` struct in `main.rs`, refuse to guess between two without `--db`, inspect every host
it deploys to and compile a node with the native features of that host's cpu, over keyless ssh
and passwordless sudo. And, in the user's words, "users do not need to remember a list of cargo
commands to compile the right things - one tool will do all of that", with a config at
`~/.config/shoal/config.yaml` naming a default inventory and every compiled program kept under
`~/.local/shoal/bin` by a descriptive name.

## What it does

### Two programs, and what each builds

| Program | What it is | What it builds, and when |
| --- | --- | --- |
| `shoaladm` | The admin tool: `deploy`, `bootstrap`, `add`, `upgrade`, `rebuild`, `reconfigure`, `rebalance`, `admin`, `ship-backup`, `status`, `stats`, `start`, `stop`, `restart`, `logs`, `destroy`, `new`, `build`, `config`, and since [F66](dataset-benchmarks.md) `bench run`/`list`/`show`/`compare` | For a command that connects to a cluster: the schema's admin program, then the node program once per cpu class among the hosts |
| `shoalctl` | The terminal UI, with no subcommand: `shoalctl [-i inventory \| --addr addr]` | The schema's terminal UI |

Both are ordinary binaries that know no schema. Run in a project directory (or given
`--project`), each reads the project through `cargo metadata`, parses `src/main.rs` and the
modules it declares (then `src/lib.rs`, which is how `examples/tmdb_dataset` is found) for a
struct carrying `#[shoal::db]`, generates a wrapper crate under the project's own `target/`, has
cargo build the program it needs, installs it, and **replaces itself with it**, the arguments
passed on whole with `--project` and `--db` made explicit. The program it hands over to is the
per-schema library entry point that every tool program was before, `shoaladm::cli::main_blocking::<DbClient>()`
or `shoalctl::cli::main_blocking::<DbClient>()`, so a schema that writes its own program (the
TMDB loader, the bench pair) is the same thing written by hand. Cargo is incremental, so a
project that has not changed costs a second or two before the command runs.

Run outside any project, each looks up the schema the inventory's cluster was deployed from
(the state remembers it) and runs the program a previous build installed for it, saying so.

### `shoaladm deploy`

One command makes a cluster run the project. It resolves the inventory (`-i`, else the
project's `inventory.yml`, else the config's `default_inventory`), and:

1. reads the project and finds the database, refusing two without `--db`, a
   `#[shoal::db(client)]` half by name, and a private struct;
2. probes every host of the bootstrap set over read-only ssh for its architecture, vendor,
   family, model and flags, refuses a host of another architecture, and maps each to the
   `-C target-cpu` it is built for: `znver1` for the V1756B on hyperion and titan, `znver4` for
   the 7945HX on europa. The inventory's `target_cpu` on a node, its group or the deployment
   overrides the probe;
3. builds the node once per cpu class, under that cpu's flags, in a target directory of its
   own, and installs each as `<package>-<Db>-node-<cpu>`;
4. bootstraps the cluster as [F51](cluster-deployment.md) does, shipping each host the build for
   its class - or, if the cluster exists, runs the rolling upgrade of
   [F55](cluster-upgrade.md), skipping every node already on its build.

Every build happens before any host is touched, so a build that fails changes nothing anywhere.
The old `server:` line still works: an inventory that names a built program deploys it as
before, and one that names a `project:` builds from there instead of the directory the command
runs in. The two together are refused.

### The wrapper crate

`<target>/shoal-build/<package>/` holds one generated crate with three binaries, `<package>-node`,
`<package>-adm` and `<package>-ctl`, written again only when their text changes:

- its `[dependencies]` are the project's, read from the metadata with their sources (a path
  made absolute, a git branch, tag or revision, a registry), features, renames, targets and
  optionality, plus `mimalloc` for the node and `shoaladm` and `shoalctl` sourced **beside the
  project's `shoal`** - the sibling directory of a path dependency, the same repository and
  reference of a git one, the same version of a registry one - each only where the project does
  not depend on it already;
- its `[features]` are the project's, so `cfg(feature = …)` in the schema file keeps its
  meaning, and the workspace's `[patch]` table is copied with its paths made absolute;
- the workspace's `Cargo.lock` is copied beside it, so every version is the project's own;
- a `main.rs` schema is brought in as `#[path = "…/src/main.rs"] mod schema;`: the file is
  compiled where it is, its own `mod x;` declarations resolve under `src/` as they always did,
  and its `fn main` becomes an unused `schema::main`. A library schema is reached by depending
  on the package.

A build for a cpu runs cargo with the process's `RUSTFLAGS` less any `-C target-cpu` and plus
its own, and `CARGO_ENCODED_RUSTFLAGS` unset, in the project directory so its toolchain file and
cargo config apply. The native builds (the admin program, the terminal UI) leave the environment
alone.

### The config and the installed programs

`~/.config/shoal/config.yaml` (`$XDG_CONFIG_HOME/shoal/config.yaml` when set) holds
`default_inventory` and, optionally, `bin_dir`. `shoaladm config show` prints it and `shoaladm
config default-inventory <file>` writes it, absolute, through a `.partial` and a rename at
`0600`. Every command that takes `-i` resolves the flag, then `inventory.yml` in the project,
then the config's default, and says how to set one otherwise; `shoalctl` with nothing at all
falls back to `127.0.0.1:12000` as it always did.

`~/.local/shoal/bin` (`$SHOAL_BIN_DIR`, or the config's `bin_dir`) holds what the tools build,
installed through a `.partial` and a rename at `0755`:

| Role | Name | Example |
| --- | --- | --- |
| node, one per cpu class | `<package>-<Db>-node-<target-cpu>` | `tmdb-dataset-Tmdb-node-znver1` |
| admin program | `<package>-<Db>-adm` | `tmdb-dataset-Tmdb-adm` |
| terminal UI | `<package>-<Db>-ctl` | `tmdb-dataset-Tmdb-ctl` |

On the hosts the node has one name whatever its cpu, `<package>-<Db>-node`. The cluster's
record keeps that name (`program`) from its bootstrap, and the schema (`schema`) when the
program was built from a project; a record written before this feature learns the name from a
host's unit the first time it is asked.

### The inventory wizard

`shoaladm new` takes the `Server program` field as optional, beside `Project` and `Database`
fields on the cluster page and a `Target cpu` field at the deployment, group and node levels,
and closes by naming `shoaladm deploy -i <file>` and `shoaladm config default-inventory`.

## Design choices

- **A per-schema program, built and handed over, rather than a schema-less protocol.** The
  reserved admin-only fingerprint was the other design (below). Building the schema's own
  program keeps the hello's check on every connection, changes nothing on the wire, and makes
  the generic binary one line of logic thick: find, build, `exec`. The cost is a compile of
  the engine per project on first use, which is the cost the node build pays anyway.
- **The wrapper is generated, the project is not touched.** A `[[bin]]` added to the user's
  `Cargo.toml` would be a diff in their repository made by a tool. A crate under their
  `target/`, rewritten from the metadata on every run, is theirs to delete.
- **`#[path]` over `include!`.** An included file's `mod x;` resolves relative to the including
  file, so a schema with modules would break; a `#[path]` module owns the directory its file
  is in (`DirOwnership::Owned { relative: None }` in rustc), so `mod extra;` finds `src/extra.rs`.
  `a_main_included_by_path_keeps_its_modules` has cargo itself check that on a crate with no
  dependencies.
- **`cargo metadata`, not a manifest parser.** What cargo says a package is called, depends on
  and puts its artifacts under is what the wrapper uses; workspace inheritance, `[patch]` and
  renames are cargo's problem, already solved. `serde_json` reads its output.
- **A short cpu table, a level fallback, and rustc as the judge.** AMD by family (23 → Zen 1 or
  2 by model, 25 → Zen 3 or, with `avx512f`, Zen 4, 26 → Zen 5), Intel by a dozen models, and
  the psABI levels `x86-64-v2` to `v4` from the flags for anything else; the name chosen is
  checked against `rustc --print target-cpus` and demoted to the level when the toolchain does
  not know it, so a part newer than the toolchain never becomes a flag rustc refuses.
- **One target directory per cpu class.** `RUSTFLAGS` is part of cargo's fingerprint, so two
  classes sharing a directory rebuild the engine on every alternation.
- **Every build before any host.** `prepare` runs after the record check and before the
  preflight loop, in `bootstrap`, `add`, `rebuild` and `upgrade` alike.
- **The on-host program name is decided once.** A unit's `ExecStart` names it, so it is kept
  in the record at bootstrap and never derived again from an inventory that may since have
  moved from `server:` to `project:`.
- **`deploy` is bootstrap-or-upgrade.** The state says which; the user chose one verb over
  two for the common case.
- **Programs live under the operator's home, not under `target/`.** `target/` is cargo's to
  clean and the project's to move; what the tools run and ship is theirs, so it is installed
  by name where a later run from another directory finds it.

## Alternatives rejected

- **A reserved admin-only fingerprint.** One `u64` the server accepts beside its own, marking
  the connection admin-only and refusing query frames on it, would have let one installed
  `shoaladm` run `status` and `backup` against any cluster with no project and no toolchain.
  The user chose against it: it is a wire change, and it gives up the schema check on the
  connection that does the most dangerous things. The design is recorded here because it is
  the smaller one, and the one to reach for if a schema-less admin tool is ever wanted.
- **Compiling on the hosts.** They have no toolchain, and a build per host is a cluster of
  several builds ([F51](cluster-deployment.md) said so). This feature builds per *class* on
  the operator's machine and ships each host its class's program, under the digest check.
- **`-C target-cpu=native` from every host's own rustc.** There is none there. A table and a
  fallback is what `native` would have computed, minus tuning for parts the table does not
  name, which the level covers for correctness.
- **A single build for the oldest cpu, found by the probe.** Simpler, and what the lab did by
  hand; but the user asked for each host's native features, and the per-class map costs one
  more build, not one more mechanism.
- **Depending on the project's package for a `main.rs` schema.** A binary target cannot be
  depended on. `#[path]` is the one way to compile a `main.rs` into another crate.
- **Copying `main.rs` into the wrapper.** It would need the copy kept in step, and the
  project's `mod` files would still have to be found; `#[path]` reads the original.

## Limitations

- **A `main.rs` schema is compiled as a module of the wrapper.** `crate::` paths in it resolve
  to the wrapper's root and fail; a crate-level `#![feature]` in it is refused by rustc (only
  `shoal-core` needs one, so no schema should); the project's `build.rs` does not run for the
  wrapper; the database struct has to be `pub`, and the scan says so; a `#[path]` on a `mod`
  declaration is not followed by the scan, though rustc follows it in the build.
- **A renamed `shoal` dependency breaks the derive macros' `::shoal::` paths**, which was true
  before this feature and is not the wrapper's doing.
- **No cross-compiling.** A host of another architecture than the machine running `shoaladm`
  is refused by name; filed in [TODOs](../appendix/todos.md#build-and-packaging).
- **The first run compiles the engine twice or more**: the admin program natively and the
  node once per cpu class, each in its own target directory under the project. Minutes, not
  seconds; every later run is incremental.
- **A registry `shoal` needs `shoaladm` and `shoalctl` published at the same version.** The
  wrapper asks for them beside `shoal`; nothing on crates.io answers yet.
- **The state directory stays at `~/.shoal/clusters/<name>/`**, where [F51](cluster-deployment.md)
  put it; moving it would orphan every deployed cluster's authority. The config and the
  programs are where the user asked for them.
- **A record written before this feature learns its program name from a host's unit**, over
  ssh, the first time a command needs it; the sed it runs is tested under `sh` against a unit as
  `systemctl cat` prints one, and was run by hand on europa against the lab's cluster, but no
  test drives the ssh round trip itself.
- **The lab inventories keep `server:`**, since the bench schema lives in a module of a
  workspace crate rather than at a project's root, and `shoal-benchctl` is its program
  written by hand.

## Invariants to uphold

- **Every command that ships a program calls `prepare` before it touches a host.** A failed
  build has to change nothing anywhere; `bootstrap`, `add`, `rebuild` and `upgrade` each do,
  and a new command that stages must too.
- **`push_binary`, `claim` and `upgrade_node` take the node's own program**, never the
  inventory's, since two nodes may hold two builds.
- **The on-host name comes from the record.** `Deployment::server_name` reads `record.program`
  first; nothing derives the name a unit runs from the inventory once a cluster exists.
- **The wrapper adds a dependency only where the project lacks it.** A project that depends
  on `mimalloc`, `shoaladm` or `shoalctl` itself (the TMDB dataset crate does) must not be
  given it twice; the first lab run failed on exactly that.
- **`RUSTFLAGS` for a cpu build replaces only the cpu.** Everything else the operator set
  stays; `CARGO_ENCODED_RUSTFLAGS` is unset because it would win over both.
- **`cargo tree -p shoaladm | grep -c glommio` is 0, and so is `shoalctl`'s.** The node is
  built by cargo in the project, never linked here; both tools depend on `shoal` with
  `default-features = false`.
- **The generic binaries hand over and never recurse.** The program they `exec` is the
  per-schema entry point, which has no front-end logic.
- **A cpu name reaches cargo only after rustc accepted it.** `cpu::decide` refuses an override
  the toolchain does not know and demotes a table name it does not know.

## Performance

None claimed for the engine: no engine path changes. What the tool costs, measured on europa
against the lab on 2026-09-30 while a second `tmdb` cluster ran on the same hosts:

| What | Time | Notes |
| --- | --- | --- |
| `shoaladm deploy` from `examples/tmdb_dataset`, cold | **3 min 19 s** wall, 1,686 s of cpu | The admin program (`tmdb-dataset-Tmdb-adm`, 14.9 MB) built and installed, three hosts probed, the node built for `znver1` (hyperion, titan) and `znver4` (europa), 36 MB each, and the three-node cluster bootstrapped and initialized. Three release builds of the engine in three target directories (690–711 MB each), on the 16-core Zen 4 |
| `shoaladm deploy` again, nothing changed | **4.5 s** | Three incremental builds (0.16–0.19 s each), the probes, and the upgrade path: every node "already runs this program", 0 of 3 upgraded |
| `shoaladm status` from `/tmp`, no project at hand | **1.4 s** | The installed `tmdb-dataset-Tmdb-adm` run as it was last built, named by the record's schema |
| `shoalctl` from the project | one incremental build | `tmdb-dataset-Tmdb-ctl` (11.7 MB) built beside the admin program in the same native target directory and opened |

The tmdb cluster the [cluster testing](../cluster-testing/overview.md) chapter uses ran on the
same hosts throughout, on its own ports; the second cluster was destroyed afterwards. What the
build costs is the engine's compile time; nothing here changes what a node does once it runs.

## Tests

| Test | Where | What breaks if this is reverted |
| --- | --- | --- |
| `one_database_is_found_in_main`, `two_databases_need_a_name`, `halves_and_privacy_are_judged`, `declared_modules_are_followed`, `the_library_is_the_fallback` | `shoaladm/src/project.rs` | A database is not found, two are guessed between, a client half or a private struct is deployed, a module is not followed, or a library schema is missed |
| `cargo_metadata_is_read` | `shoaladm/src/project.rs` | This crate's own metadata stops reading back, or a workspace root or a bare directory is taken for a project |
| `the_manifest_carries_the_projects_dependencies`, `the_tools_come_from_beside_shoal` | `shoaladm/src/build.rs` | A path, git, registry, renamed, optional or targeted dependency is written wrongly, a dev dependency leaks in, the features or the patch table are dropped, a tool is sourced from somewhere other than beside `shoal`, or one the project already has is added twice |
| `the_sources_name_the_database` | `shoaladm/src/build.rs` | A wrapper names the wrong type, or a library schema is included by path |
| `a_main_included_by_path_keeps_its_modules` | `shoaladm/src/build.rs` | cargo itself: a `main.rs` schema's `mod x;` stops resolving under `src/` |
| `rustflags_replace_the_cpu` | `shoaladm/src/build.rs` | `native` reaches a node build, or the operator's other flags are dropped |
| `names_are_descriptive` | `shoaladm/src/build.rs` | An installed program stops saying what it is |
| `amd_parts_are_named_by_family`, `intel_parts_are_named_or_levelled`, `names_are_judged_against_the_toolchain`, `another_architecture_is_refused`, `the_script_reads_this_machine` | `shoaladm/src/cpu.rs` | The lab's two parts map wrongly, an unknown part gets no level, an unknown name reaches cargo, another architecture is built for, or the probe misreads `/proc/cpuinfo` |
| `a_config_reads_back_with_its_defaults`, `an_inventory_is_resolved_in_order`, `a_program_is_installed_whole_and_executable`, `the_bin_dir_has_three_sources` | `shoaladm/src/config.rs` | A missing config is an error, a relative default is read against the wrong directory, the resolution order changes, or a program is installed half-written or not executable |
| `the_project_is_global_and_the_inventory_optional`, `upgrade_parses_its_nodes_and_refuses_a_forced_rollback` | `shoaladm/src/cli.rs` | `--project`/`--db` stop being global, `-i` becomes required, a command that connects is run by the generic binary, or the `cluster` prefix comes back |
| `the_ui_needs_no_subcommand` | `shoalctl/src/cli.rs` | `shoalctl` needs `tui` again, or an address and an inventory are both taken |
| `an_inventory_that_cannot_be_a_cluster_is_refused` | `shoaladm/src/deploy/inventory.rs` | `server` and `project` are both accepted, a project without a manifest is, or an inventory naming neither is refused |
| `state_is_private_and_minted_once` | `shoaladm/src/deploy/state.rs` | An old record without `schema` or `program` stops reading, or a new one does not keep them |
| `the_unit_runs_the_deployed_program` | `shoaladm/src/deploy/unit.rs` | The unit stops running the program by the name it is given |
| `programs_are_named_by_where_they_come_from` | `shoaladm/src/deploy/programs.rs` | A built program's on-host name stops being its file name, a project's stops being `<package>-<Db>-node`, the record forgets the schema, or an unprepared node gets a program |
| `a_units_program_is_read_off_its_exec_start` | `shoaladm/src/deploy/ops.rs` | The sed a pre-F63 cluster's program name is learned through stops matching `ExecStart`, run for real under `sh` |
| `a_deployed_cluster_serves_every_row_from_every_node` | `shoal-bench/tests/deploy_smoke.rs` | Gated on `SHOAL_DEPLOY_INVENTORY`: the bench pair's flattened commands stop deploying |
| The lab run above | by hand | `shoaladm deploy` from `examples/tmdb_dataset` stops building two classes and bootstrapping |

## Related

[F51](cluster-deployment.md), whose deployment this builds the program for;
[F53](inventory-wizard.md), whose form gains the project fields;
[F54](tmdb-dataset-deployment.md), the project the lab run deploys;
[F55](cluster-upgrade.md), which `deploy` runs onto an existing cluster;
[shoaladm](../operations/shoaladm.md) and [shoalctl](../operations/shoalctl.md), the two
operations pages; [C14](../distributed/deploying.md).
