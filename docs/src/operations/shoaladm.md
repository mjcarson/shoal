# shoaladm

`shoaladm` deploys a Shoal cluster from a Rust project and operates it over ssh: bootstrap,
add, upgrade, rebuild, reconfigure, rebalance, backup and restore, the units, the journal,
destroy. It is one installed binary that knows no schema. Run in the project that defines a
`#[shoal::db]` struct, it builds whatever the command needs - the schema's admin program, and
the node program once per cpu class among the hosts - and hands the command over
([F63](../features/shoaladm.md)). Nobody types a cargo command.

The terminal UI that queries the database is [shoalctl](shoalctl.md), the same way round:
`shoalctl` in the project builds the UI for its schema and opens it.

## Deploying a project

```bash
cargo install --path shoaladm --path shoalctl     # once, from this repository
cd my-database                                     # a project with `#[shoal::db] pub struct MyDb`
shoaladm new -o inventory.yml                      # the hosts, in a form; leave Server program blank
shoaladm deploy                                    # build, probe, build per cpu, bootstrap
shoalctl                                           # the terminal UI, as the cluster's admin
shoaladm status
shoaladm deploy                                    # after a change: the rolling upgrade
```

What `deploy` does, in order:

1. **Finds the project.** The current directory or `--project <dir>` has to hold a
   `Cargo.toml`, or the command says so. `cargo metadata` reads it.
2. **Finds the database.** `src/main.rs` and the modules it declares are parsed for a struct
   carrying `#[shoal::db]`; if none is there, `src/lib.rs` and its modules. One is the
   database. Two are refused until `--db <name>` says which. A `#[shoal::db(client)]` half
   cannot be served and is refused by name, and so is a struct that is not `pub`.
3. **Builds the admin program** for the schema, `<package>-<Db>-adm`, and replaces itself with
   it. Everything from here runs in that program.
4. **Resolves the inventory**: `-i <file>`, else `inventory.yml` in the project, else the
   config's `default_inventory`.
5. **Probes every host** of the bootstrap set over read-only ssh (`uname -m` and the first
   block of `/proc/cpuinfo`) and decides the cpu each program is built for: AMD Zen 1 to 5 by
   family and model, a dozen Intel parts by model, and otherwise the x86-64 level the flags
   reach (`x86-64-v2`, `v3`, `v4`). A name the local `rustc` does not know is demoted to the
   level. A host of another architecture is refused. `target_cpu` on a node, its group or the
   deployment in the inventory overrides the probe.
6. **Builds the node once per cpu class**, under `RUSTFLAGS="… -C target-cpu=<cpu>"` with any
   `target-cpu` the environment had removed, in a target directory of its own per class, and
   installs each as `<package>-<Db>-node-<cpu>`.
7. **Bootstraps** as [F51](../features/cluster-deployment.md) describes, shipping each host its
   class's build under the digest check - or, when the cluster exists, **upgrades** it one node
   at a time, the leader last, as [F55](../features/cluster-upgrade.md) describes, skipping
   every node already on its build.

Every build finishes before any host is touched. The first run compiles the engine once per
program and once per class, which is minutes; later runs are incremental.

The `[build] …` lines on stderr say which program is being built for what, and cargo's own
output follows. The whole thing is one command; if it fails, the errors above the last line
name what to fix in the project.

## The inventory

An inventory is what [F51](../features/cluster-deployment.md) and [F53](../features/inventory-wizard.md)
describe, with three fields around the program:

```yaml
name: lab
# one of these, or neither:
server: ../target/deploy/release/my-node      # a node program you built yourself
project: .                                     # the project it is built from, relative to this file
db: MyDb                                       # only when the project defines more than one
target_cpu: znver1                             # override the probe for every node
nodes:
  - name: hyperion
    address: 172.16.2.5
    target_cpu: x86-64-v3                      # or for one node, or on a group
```

Neither `server` nor `project` means the project the command runs in. Both is refused. A
`server` is deployed as it is, so an inventory written before F63 works unchanged; build that
program for the oldest host's cpu, never `native`, or it dies of SIGILL at its claim and the
command says so.

### Where each node stands

Since [F81](../features/failure-domains.md) every rendered `shoal.yml` names the host its node
stands on, `cluster.failure_domains.host`, which the control group commits on the node's member
record. It is the node's own setting, else its group's, else the node's name:

```yaml
groups:
  chassis:
    failure_domains: {host: chassis-1}   # two blades in one chassis fail together
nodes:
  - {name: blade-a, group: chassis}
  - {name: blade-b, group: chassis}
  - {name: europa}                       # stands on host europa, its name
  - {name: loop-a, ssh: europa, failure_domains: {host: europa}}   # a second node on europa
```

Two nodes on one machine name the machine, in the node or in a group, or the inventory takes
each node's name and counts them as two hosts. A host the engine would refuse, empty or holding
whitespace, is refused by the inventory naming the node, and a level but ~~`host`~~ `host`, `rack`
or `zone` is refused when the file is read. `status` shows each member's host and how many hosts
the members stand on. A change is a restart: `reconfigure` writes the new files and restarts the
nodes whose file moved. The wizard has no field for it and keeps an inventory's as they are.

Since [F83](../features/rack-and-zone-domains.md) a node or a group can also name the rack its
host stands in and the zone its rack stands in, field by field as the host is:

```yaml
groups:
  shelf-1:
    failure_domains: {rack: shelf-1, zone: hall-a}   # every node of the group
nodes:
  - {name: blade-a, group: shelf-1}
  - {name: blade-b, group: shelf-1, failure_domains: {rack: shelf-2}}   # its own rack, the group's zone
  - {name: europa}                                    # no rack or zone: its host stands for both
```

A rack or zone that neither the node nor its group names is left out of the node's file, and the
engine reads it as the level below's. `validate`, which every command runs, refuses a hierarchy that
does not nest, naming both nodes: two nodes on one machine in two racks or zones (one naming a rack
and one naming none among them), or one rack in two zones. A rack's name is the cluster's, so racks
in different zones are named apart. Once a member names a rack or a zone, `status` adds a column for
it after `host` and counts them beside the hosts, `racks 2  zones 1`. **A node on a build before
F83 refuses a file that names either**, so `upgrade` every node before an inventory naming one is
reconfigured onto the cluster. [Failure domains and failover](failure-domains.md) draws the
levels, says what each layout survives, and why `bootstrap:` should interleave them.

### Users and grants

Every node accepts the inventory's `admin`, who holds every table. Since
[F77](../features/authorization.md) an inventory can name more principals, and what each may do:

```yaml
admin: admin
users: [reader, loader]          # each gets a password minted into the deployment's state
grants:                          # seeded into the cluster by its bootstrap, and only then
  reader:
    tables:
      '*': [read]
  loader:
    tables:
      Movie: [read, write]
    buckets:                     # a bucket the schema declares, by its marker (F79)
      '*': [read, write]
```

A user's password is minted the first time a deployment needs it, kept beside the admin's as
`user-<name>.password` in the deployment's state directory, and rendered into every node's file as
a derived credential. A user may do only what it was granted: the `grants` here seed a bootstrap,
and on a cluster that exists `shoaladm admin grant <user> <table|*> <privileges>`, `grant <user>
bucket <bucket|*> <privileges>` and `revoke` change them - editing the inventory's afterwards
changes no grant. A deploy refuses a grant on a table or bucket the schema does not serve, and a
table grant on a bucket's generated table ([F79](../features/bucket-schema.md)). `SHOAL_DEPLOY_USER=<user>` makes every connection a command opens
for itself authenticate as that user instead of the admin, which is how an operator sees the
cluster as a user does; the deployment's own steps (a bootstrap's initialization, an upgrade, a
rebuild, a bench's restarts) stay the admin's. The wizard has no fields for either yet and keeps an
inventory's as they are.

### Pools and buckets

A schema that declares a bucket is deployed with the bucket bound to a storage pool
([F80](../features/object-wire.md)), at the deployment's level, rendered into every node's
`storage`:

```yaml
pools:
  fast:
    class: ssd
    redundancy: {replicas: 3}
    failure_domain: host
    inline: 16KiB                # at or under this an object stays in its row; at most 16MiB
    small_writes: 64KiB          # a write in place under this rides its commit; 0 for none
buckets:
  Posters: {pool: fast}
```

A deploy refuses an inventory whose buckets are not each bound, to a pool it defines. It also
refuses one naming a bucket the schema does not declare, and a pool whose facts do not hold. The
bootstrap commits them once, with each binding's consumer id: editing them afterwards changes
nothing on a running cluster. A bootstrap also activates the wire version the build speaks once the
first node initializes, since a cluster below wire 7 refuses every object write
([F68](../features/conditional-writes.md)), one below wire 8 commits no device
([F85](../features/devices-and-pools.md)), one below wire 9 writes no stripe and pins no pool
([F86](../features/replicated-pools.md)), and one below wire 12 writes nothing to an erasure coded
pool ([F90](../features/erasure-coded-pools.md)), and one below wire 13 commits no device's journal
([F91](../features/rotational-devices.md)). `small_writes` defaults to 64 KiB where every device a
pool places on has a volatile write cache and to none otherwise; the lab's inventory sets it.
A pool of `redundancy: {erasure: {data: 2, parity: 1}}` stores each 4 MiB stripe as `data` chunks
of its bytes and `parity` chunks computed from them, on as many failure domains: the lab's three
hosts take 2+1 by host, or up to 4+2 by device over their six devices. Such a pool names no
`small_writes` other than 0, since every write to it is staged with its parity, and its `stripe`,
if named, is a whole number of `data` chunks of whole units, which a deploy says when it is not. ~~The wizard keeps ~~both~~ them as they are and has no
fields for them yet.~~ Since [F93](../features/operations-and-the-real-cluster.md#the-tools) the
wizard's Storage page edits the deployment's devices, the pools and the bindings, each one line of
this file's own YAML, and devices and object cpus at every level, judging them as a deploy does
against the buckets the project's schema declares. A bucket a schema appends is bound here before
the build is rolled on, and `upgrade --activate` commits its binding.

A pool's devices are named at the deployment, a group or a node, the most specific list winning
whole, beside the cpus their object executors run on ([F85](../features/devices-and-pools.md)):

```yaml
resources:
  object_cpus: [1]              # the control core's other thread on the lab
groups:
  v1000:
    devices:
      - {path: /xfs/shoal-tmdb-pool, class: ssd}   # a mounted disk; never created
```

A deploy refuses devices with no object cpu, and a device inside or around a storage root, the
remote directory or another device on its host. Its preflight refuses a device directory that is
missing, since a device is a disk mounted there, and reads `shoal-device.json` as it reads a root's
marker; `stage` hands each device's directory, and not what is in it, to the node's user. `destroy`
and a wipe empty a device only when it holds Shoal's marker, and name one that holds anybody else's
files.

**A disk** ([F91](../features/rotational-devices.md)) names its journal, a directory on an SSD of
its host, and a node serving disks beside SSDs names two object cpus:

```yaml
groups:
  v1000:
    resources: {cores: 5, memory: 8Gi, control_core_shared: false, object_cpus: [1, 7]}
    devices:
      - {path: /xfs/shoal-tmdb-pool, class: ssd}
      - {path: /hdd/shoal-tmdb-bulk, class: hdd, journal: /xfs/shoal-tmdb-journal}
pools:
  bulk: {class: hdd, media: rotational, redundancy: {replicas: 3}, failure_domain: host}
```

A deploy holds a journal clear of the roots, the devices and the remote directory, and its preflight
refuses one that is missing, since it is never created. For every device on a disk the kernel calls
rotational, unless the device says `write_cache: on`, the preflight turns the disk's volatile write
cache off with `hdparm -W0` and a rescan, then checks that `queue/write_cache` reads `write through`.
Writing that file alone would change only what the kernel believes. It then installs
`/etc/udev/rules.d/61-shoal-write-cache-<serial>.rules`, which does the same at every boot. A host
with no `hdparm`, or a cache that will not turn off, is refused by name. `destroy` and a wipe empty a
journal directory holding Shoal's marker as they empty a device, and leave a disk's cache off. A
bench moves a journal beside the inventory's as it moves a device. `status` adds `journal on <dir>`
to a disk's line.

### Nodes that share a machine

Several nodes may stand on one machine, each named with the same `ssh` target and an `address` of
its own ([F86](../features/replicated-pools.md#operations)). The cpu probe reads the machine's
`/etc/machine-id` beside its topology, and every node behind one target is deployed apart:

- its unit is `shoal-<cluster>-<node>.service`, where a node alone on its machine keeps
  `shoal-<cluster>`, so a deployment that never shares a machine is unchanged;
- its remote directory is `/opt/shoal-deploy/<cluster>/<node>`;
- its client, peer and control listeners bind its own address rather than every interface;
- its control thread runs on `resources.control_core`, cpu 0 unless it names another; nodes on one
  machine may share one, each keeping its shards off it with `exclude_cores`.

A deploy refuses two nodes on one machine when one's shards or object cpus - by the machine's
topology - land on any cpu of the other's, its control core included, when their roots or devices
overlap, and two ssh targets that reach one machine. Host changes, the governor and the limits, are made once a machine. Each node
names its own host domain, its name by default, so a pool of three copies places one on each.
`shoaladm/inventories/loopback.yml` is three nodes on europa over `127.0.0.11–13`, the object
store's loopback leg, which `bench run` takes like any inventory.

### Scan settings

What every node does for a scan's pages ([F116](../features/scan-admission.md)) and how fast their
archived reads may go ([F119](../features/scan-pacing.md)), each key as a node's `shoal.yml` writes
it under `scans` ([Configuration](../getting-started/configuration.md#scans)):

```yaml
scans:
  ceiling: 64MiB          # a second, every shard's scans of one device together; 100MiB unless set
  trickle: 4MiB           # a second, while a get or a write is reading the device; 2MiB unless set
  pages_in_flight: 3      # scan pages one shard reads at once; 2 unless set
  enabled: true           # whether nodes answer scans at all
```

The section is rendered into every node's file as written, and each node's start judges it: a key
the engine does not read, such as `cieling`, refuses the start by name rather than leaving the
default in place unsaid. Unset keys are the engine's defaults, and an inventory with no `scans` renders
none. A change is a restart: `reconfigure` writes the new files and restarts the nodes whose file
moved. The wizard has no fields for it and keeps an inventory's as it is. `scan_memory`, the buffers a
node's scan pages may hold, is a node's `resources` setting and is not rendered by the inventory yet.

## The config

`~/.config/shoal/config.yaml` (`$XDG_CONFIG_HOME/shoal/config.yaml` when that is set):

```yaml
# Written by `shoaladm config`
default_inventory: /home/me/my-database/inventory.yml   # relative paths are relative to this file
bin_dir: /home/me/.local/shoal/bin                       # optional; this is the default
```

| Command | What it does |
| --- | --- |
| `shoaladm config show` | Print the file, or the defaults and where the file would be |
| `shoaladm config default-inventory <file>` | Set the inventory every command reads when given none |

A missing file is the defaults. Every command that takes `-i` resolves the flag, then
`inventory.yml` in the project, then `default_inventory`, and says how to set one otherwise.
`shoalctl` with nothing at all opens `127.0.0.1:12000`, as it always did.

## The installed programs

Everything the tools build lands in `~/.local/shoal/bin` (`$SHOAL_BIN_DIR`, or the config's
`bin_dir`), written through a `.partial` and a rename so a program being run is never half
written:

| Role | Name | Example |
| --- | --- | --- |
| node, one per cpu class | `<package>-<Db>-node-<target-cpu>` | `tmdb-dataset-Tmdb-node-znver1` |
| admin program | `<package>-<Db>-adm` | `tmdb-dataset-Tmdb-adm` |
| terminal UI | `<package>-<Db>-ctl` | `tmdb-dataset-Tmdb-ctl` |

On the hosts a node has one name whatever its cpu, `<package>-<Db>-node`, kept in the
cluster's record from its bootstrap. The record also keeps which package and database the
cluster was deployed from, so `shoaladm status -i <inventory>` or `shoalctl -i <inventory>` run
from a directory that is not a project finds the installed program for it and runs that,
saying it was not rebuilt.

The generated crate the builds come from is `<target>/shoal-build/<package>/`, under the
project's own target directory: a manifest with the project's dependencies, `node.rs`,
`adm.rs` and `ctl.rs`, and a copy of the project's `Cargo.lock`. It is rewritten from the
project on every run and is safe to delete.

## The commands

Every command takes `--project <dir>` and `--db <name>` anywhere on the line, and `-i <file>`
where the resolution above does not name the inventory.

| Command | What it does |
| --- | --- |
| `deploy [--wipe]` | Make the cluster run this project: bootstrap it if it does not exist, upgrade it if it does |
| `build [--target-cpu <cpu>...] [-i <inv>]` | Build without deploying: the node for this machine, for the named cpus, or for every host of an inventory (probed, nothing else touched), and the admin program and terminal UI |
| `new -o <inv> [--from <inv>]` | Build or edit an inventory in a full-screen form that judges it on every key ([F53](../features/inventory-wizard.md)) |
| `bootstrap [--wipe]` | Stage, claim, issue a leaf, and start every bootstrap node under systemd, then `Initialize` once and wait for writes ([F51](../features/cluster-deployment.md)) |
| `add <node> [--wipe] [--rebalance]` | Join a listed node through every member, built for its cpu, and optionally follow a `Rebalance` onto it |
| `rebalance` | Send `Rebalance` and follow its plan |
| `admin <operation...> [--timeout-secs]` | Send any operation the cluster tab's command line takes (`repair <table> [verify\|repair]`, `backup [table] <dir>`, `restore <dir>`, `restore-retry <op>`, `decommission <node>`, `status <op>`, `grant <principal> <table\|*> <privileges>`, `grant <principal> bucket <bucket\|*> <privileges>`, `revoke ...`, ...) without the tab's preview, and follow its record until it is done; `grants` prints a line a principal and table or bucket ([F77](../features/authorization.md), [F79](../features/bucket-schema.md)); `pools` prints each pool's policy as placement reads it, its devices and domains against what it needs, its free bytes and what it is short of ([F85](../features/devices-and-pools.md)), and since [F86](../features/replicated-pools.md) the generation it is pinned at, or `unpinned`, and since [F88](../features/recovery-and-moves.md#operations) a recovery line: the generation its groups move to, the moves admitted, the groups behind it, the stale and overflowed positions, and what was rebuilt, backfilled, switched and aborted, and since [F89](../features/scrub-and-repair.md#operations) a scrub line: when the oldest light and deep pass finished, the groups pending, the chunks and bytes verified, and what was found stale, corrupt, unexplained, disagreeing and lost, and since [F90](../features/erasure-coded-pools.md#what-an-operator-sees) its redundancy (`2+1`, or `3 replicas`) after its class and the stripes whose parity a deep pass checked against their data and found disagreeing (`parity checked N mismatched M`); `fail-device <device>` has the device's node fail it as an I/O error would, so its placement groups move off it, and since [F92](../features/reclamation-and-device-lifecycle.md#the-operators-surface) it stays retired; `scrub <pool> [light\|deep]`, deep unless light is asked for, scrubs a pool's placement groups now and is followed until no group of it is pending; since F92 `drain-device <device>` copies a device's groups off it while it serves, `remove-device <device> [--lost]` removes a retired or emptied drained device for good, or on the word that it is lost, refused by name otherwise, `reweight-device <device> <weight>` gives it a weight its groups move by, and `pools` gains a reclaim line: objects, stripes and floors reclaimed, removals, slices released, unexplained chunks removed, retired objects pending and reclamations deferred, and the devices draining; since [F93](../features/operations-and-the-real-cluster.md#the-tools) `scrub <pool> [light\|deep] [bucket <name> \| device <id> \| slice <id> <index>]` narrows a scrub, `extend-schema <id>` commits a schema that appends tables or buckets, `resolve-stripe <bucket> <path> <stripe> accept\|lost <sequence>` settles a stripe whose row is behind its chunks, `buckets [name]` prints each bucket's objects, bytes and operations by kind, `devices` every member's devices, and a kind is sent only to a node that advertises it |
| `ship-backup <path>/<op> [--to <inv>]` | Copy every host's files of a backup to every host, or to another inventory's hosts ([F59](../features/backup-shipping.md)); refused while no host holds the backup's record, `backup.json`, which a restore needs ([Resolved #322](../appendix/resolved/backup-outcome-record.md)) |
| `status` | Every node's id, address and unit, then the cluster tab's lines: since [F85](../features/devices-and-pools.md) a devices section under the roots, a line a device with its size, free bytes, slices' chunks and staged bytes, and its health and warnings |
| `stats [--table <t>] [--watch [s]] [--json] [--basic]` | Every member's standing, groups, tablets, bytes and rates, and every plan's progress, from the control leader ([F52](../features/cluster-stats.md)). Members are named by hostname. On a terminal it charts the figures full screen, with a help page on `?`; `--basic`, `--json` and a pipe print them ([F64](../features/stats-tui.md)). The view opens on a home tab: the cluster's queries, reads, writes and errors a second, read and write speed, the slowest p99 and memory, six charts, and a table of every member's answers by kind, speeds, waits and memory ([F65](../features/query-figures-home-tab.md)). `--json` carries each member's `codec` since [F90](../features/erasure-coded-pools.md#what-an-operator-sees): the bytes its object executors encoded, decoded and folded, the nanoseconds each took and the kernel that ran |
| `start`/`stop`/`restart [node]` | systemctl on one node or every deployed node |
| `upgrade [node...] [--force] [--activate] [--rollback]` | Replace every node's program with this build, one node at a time and the leader last ([F55](../features/cluster-upgrade.md)); since [F93](../features/operations-and-the-real-cluster.md) its admin connections present the committed schema's fingerprint to nodes of the old build, each node's file takes the inventory's pools and bindings with its new program, `--activate` also sends `ExtendSchema` with the inventory's pools when this build appends tables or buckets to the committed schema, and `--rollback` warns that a node rolled back below a committed extension stops at its start |
| `reconfigure [node...] [--force]` | Render every node's `shoal.yml` again and restart the ones whose file changed ([F57](../features/cluster-reconfigure.md)) |
| `rebuild <node> --yes` | Rebuild one node from its peers under a new identity ([F56](../features/cluster-rebuild.md)) |
| `logs <node> [-n N]` | The node's journal |
| `destroy --yes` | Delete every node, its data and the local state; a device is emptied only when it holds Shoal's marker, and never removed, since it is where a disk is mounted |
| `config show` / `config default-inventory <file>` | The config above |
| `bench run --dataset <dir> [-i <inv>] [--spec bench.yml] ...` | Benchmark the project's schema against a folder of `<Table>.csv`/`.json`/`.jsonl`, on a cluster of its own copied from the inventory (`<name>-bench`, every root, port and directory moved, and every device beside itself as `<path>-bench`, made for the run and removed once empty), or on the inventory's own with `--attach`. Workloads (`--workloads`, ~~`--mixes`~~) `insert100`, `read100`, `rw50`, `read90` at bundle sizes, chosen in a wizard on a terminal when none is named ([F67](../features/bench-run-wizard.md)), optional events (`kill`, `rebalance`, ..., and for an object arm since [F88](../features/recovery-and-moves.md#operations) `kill`, `stop`, `device_fail` and `device_add`, and since [F89](../features/scrub-and-repair.md#operations) `deep_scrub`), `--profile` for a heap profiled node, object arms over a folder's `<Bucket>.yaml` descriptions (`--objects put100,get100,rw50 --object-sizes 4KiB,1MiB --concurrency 8`, [F80](../features/object-wire.md)), writes in place since [F86](../features/replicated-pools.md) (`patch100`, `patch100-32k`, or a `patch:N` weight, each rewriting a preloaded object's own bytes), every acknowledged object read back and a loss failing the run, the bytes every slice still held staged a minute after an object arm's last answer recorded as `staged_left`, `--stop-unit` and `--governor` put back on every way out. On a terminal it draws the stats view with a bench tab ([F66](../features/dataset-benchmarks.md)) |
| `bench list` / `bench show <capture>` / `bench compare <baseline> <candidate> [--allow <fact>]` | The captures under the project's `target/shoaladm-bench/`; compare refuses two that differ in anything that moves their numbers, a capture's pool shape among them since [F85](../features/devices-and-pools.md) (`--allow pool-shape`) |

The commands that connect to a cluster (`deploy` through `reconfigure`, and `bench run`) run in
the schema's admin program; `new`, `build`, `config`, `ship-backup`, the units, `logs`,
`destroy` and `bench list`/`show`/`compare` run in `shoaladm` itself and need no schema.

## A schema's own program

The per-schema program `shoaladm` builds is one line, and a program with commands of its own
writes the same line by hand and flattens the commands beside its own. The TMDB dataset
loader carries `load` beside them and the terminal UI as `tui`
([F54](../features/tmdb-dataset-deployment.md)); `shoal-benchctl` is the same for the bench
schema:

```rust
#[derive(clap::Subcommand)]
enum Command {
    Load(LoadArgs),
    Tui(shoalctl::cli::TuiArgs),
    #[command(flatten)]
    Shoaladm(shoaladm::cli::Command),
}

Command::Tui(args) => shoalctl::cli::run::<MyDbClient>(&cli.project, args).await,
Command::Shoaladm(command) => shoaladm::cli::run::<MyDbClient>(&cli.project, command).await,
```

`shoaladm` links no engine: `cargo tree -p shoaladm | grep -c glommio` is 0. The node program it
deploys is built by cargo in the project, never here.

## What it assumes

Keyless ssh to every host as a user with passwordless sudo, `systemctl` and the kernel `tls`
module there, and a machine of the hosts' architecture to build on with a nightly toolchain
(the engine needs one). Cross-compiling is not supported: a host of another architecture is
refused.

## Limitations

- A `main.rs` schema is compiled as a module of the generated crate: `crate::` paths in it
  fail, a crate-level `#![feature]` is refused, the project's `build.rs` does not run, and the
  struct has to be `pub`. A library schema has none of these.
- The first run is minutes: the engine is compiled once for the admin program and once per cpu
  class. Every later run is incremental.
- The cpu table names a dozen Intel parts and every Zen; anything else builds for the x86-64
  level its flags reach, which runs everywhere it should but is not tuned for the part.
- The state stays at `~/.shoal/clusters/<name>/`, where F51 put it.
