# Distributed cluster testing

## Context

Every cluster feature from [F36](../features/cluster-harness.md) to
[F55](../features/cluster-upgrade.md) was proved in the process fixture: real Shoal nodes, but all
of them children of one test binary on one host, on the glibc allocator, over loopback, with
proxies standing in for the network. This chapter is the other half. A real dataset is deployed
with the real deployment tool onto three physical hosts. It is driven with real clients over a
real network, faults are injected from outside the process, and everything the cluster says is
checked against what was written.

The fixture is still the regression suite. What this chapter adds is the evidence a fixture cannot
give. The first two runs here found a defect that had been in the tree since F41 and that every
fixture test had run over without noticing: a get naming two partitions crashed the node that
coordinated it ([Resolved #133](../appendix/resolved/read-plan-rc-across-shards.md)).

## The lab

| Host | CPU | Memory | Storage the node uses | Network |
| --- | --- | --- | --- | --- |
| europa (172.16.2.10) | AMD Ryzen 9 7945HX, 16 cores / 32 threads, `powersave` governor | 43 GiB | Intel Optane SSD 900P (`SSDPED1D280GA`), btrfs, `/optane/shoal-tmdb` | 1 GbE |
| titan (172.16.2.4) | AMD Ryzen Embedded V1756B (Zen1), 4 cores / 8 threads, `schedutil` | 14 GiB | Samsung 970 EVO, ext4, `/optane/shoal` (a directory on the root device) | 1 GbE |
| hyperion (172.16.2.5) | AMD Ryzen Embedded V1756B (Zen1), 4 cores / 8 threads, `schedutil` | 14 GiB | Samsung 970 EVO, ext4, `/optane/shoal` (a directory on the root device) | 1 GbE |

The hosts are unequal on purpose: that is the "physical capture on unequal hardware" that
[C15](../distributed/open-issues.md#measured-at-smoke-scale-only) listed as having a launcher and no
run. Round trip time between them is about 0.12 ms. Europa is also the development host, so it
runs the clients and the builds, and its numbers carry that noise.

The cluster is `tmdb_cluster.yaml` at the repository root, deployed with
[F51](../features/cluster-deployment.md)'s `cluster bootstrap`: replication factor 3, three control
voters, six cores and 8 GiB per node with a dedicated control core, since round 12 europa at
`lead_weight: 2` and the Zen1 hosts at a 2 ms `wal_commit_delay`, mutual TLS on the peer lanes,
SCRAM for clients, and nodes running as the system user `shoal` under systemd with
`Restart=on-failure`. The schema is [F54](../features/tmdb-dataset-deployment.md)'s: `Movie`
(unsorted, by id) and `MovieByKeyword` (sorted, by keyword then title and id). The dataset is
`TMDB_movie_dataset_v11.csv`, 1,188,548 movies, which the loader writes as 2,193,788 rows.

Europa's group started on `/opt/shoal`, on a root device that was 98% full. It was moved to the
Optane before the first test here. Everything below ran on the Optane unless it says otherwise.

## Building and deploying

```bash
# the node and the loader, for the oldest cpu in the lab (Zen1), never native
CARGO_TARGET_DIR=target/deploy RUSTFLAGS="-C target-cpu=znver1" \
    cargo build --release -p tmdb-dataset
L=target/deploy/release/tmdb-dataset-loader
$L cluster bootstrap -i tmdb_cluster.yaml
$L cluster upgrade -i tmdb_cluster.yaml        # after every fix, one node at a time
$L cluster destroy -i tmdb_cluster.yaml --yes  # between tests that need an empty cluster
$L cluster rebuild -i tmdb_cluster.yaml hyperion --yes   # a node from its peers, as a new identity (F56)
$L cluster admin -i tmdb_cluster.yaml "restore-retry <op>"  # a restore's failed groups, again (#155)
$L cluster admin -i tmdb_cluster.yaml "remove <node> <replacement>"  # again: retries a blocked plan (#177)
$L cluster ship-backup -i tmdb_cluster.yaml /optane/shoal-backup/<op>  # every file to every host (F59)
$L cluster add -i target/lab/tmdb-add.yaml hyperion   # a member with no placement slot (section 8)
$L bench -i target/lab/tmdb-add.yaml --addr 172.16.2.5:12000 …   # through that one member, as the admin
```

## The driver

The loader is the test driver. Beside `load` it has three commands, all in
`examples/tmdb_dataset/src/bench.rs`:

| Command | What it does | What it proves |
| --- | --- | --- |
| `load` | Writes the whole csv, retrying the codes that say to try again, then reads a sample back | The dataset can be loaded, and at what rate |
| `verify` | Reads every movie back and compares it field by field with the csv, then reads every keyword partition whole and compares its set of sort keys | Nothing written was lost, changed or misplaced, on either table |
| `bench` | Drives a mix of `get`, `keyword` (a partition read, limited to 50 rows), `update` (an overview rewritten to the value it has) and `insert` (a synthetic movie above id 2⁴⁰) for a fixed time, printing a line a second with throughput, p50, p99 and max per kind, and the failures by code. `--slow-ms` also logs each operation slower than the threshold, with the member it went through and when it was sent | Throughput and latency under a mix, and what a fault does to both second by second |
| `verify-acks` | Reads back every synthetic insert a `bench` run was acknowledged for, through one member, each member in turn, or all of them | No acknowledged write was lost, whatever happened during the run |

Updates rewrite a value the row already has, so a `verify` after a `bench` still matches the csv.
Synthetic ids are offset by `--run` and by worker, so no two runs write the same row.

Faults are injected from outside the process with `systemctl kill -s SIGKILL`, `SIGSTOP` and
`SIGCONT`, `iptables` rules that drop a peer's traffic, and `tc netem` delay and loss. Every rule is
removed at the end of the test that added it. [Section 12](correctness.md#12-scenarios-nobody-had-run)
added the rest:

| Script (`target/lab/r11/`) | What it injects |
| --- | --- |
| `netem.sh <host> add "<args>" \| del` | `tc netem` on one host's egress, filtered to the peer ports (12001–12002) so ssh and clients are untouched |
| `oneway.sh <host> in \| out \| control \| heal` | a partition one way only, or of the control port alone |
| `slowdisk.sh setup \| delay <r> <w> \| teardown` | hyperion's storage on a `dm-delay` device over a loop file, its delay changed live |
| `powercut.sh <host> <dir> <run>` | `sysrq b` 15 s into a bench: a reboot without sync, then every row verified |
| `fault2.sh <dir> <run> <secs> <inject> <heal>` | `fault.sh` with the fault held for any length |
| clock skew | `timedatectl set-ntp false; date -s "+30 sec"` on titan, and back |

Round 12's scripts are under `target/lab/r12/`: `180/sweep.sh` (fresh clusters on two builds of
the node, with the admission gate switched off by a lab-only environment variable that was never
committed), `o64.sh` (fresh clusters loaded whole, the page cache dropped on every host first or
not), `leadab.sh` (lead weights rolled onto one cluster with `cluster reconfigure`, then the mixed
bench), and `132/loop.sh` (an ASan build of one test binary run until it aborts).

Round 13's are under `target/lab/r13/`: `142/loop.sh` (the restore test beside five other heavy
fixture tests at six threads, with child logs, round after round), `slow/ab.sh` (lead weights
rolled onto a cluster whose hyperion is on the `dm-delay` device, then the bench under a delay),
`o64/o64.sh` and `o64/batches.py` (fresh loads with the storage figures sampled, and each Zen1
node's sync sizes averaged per load), `part/flap.sh` (a partition cut and healed on a cycle),
`unplaced/run.sh` (gets through placed and unplaced members, one in flight and loaded),
`ship/run.sh` (a backup shipped with `cluster ship-backup`, the cluster destroyed and the backup
restored), and `tb/run.sh` (a rebuild under load at a shrunk retention, with each member's snapshot
installs counted from its journal).

The runs that compare builds use `abload.sh` (a fresh cluster, the csv, `verify`, the bench, every
acknowledged insert back through each member), `fresh-sweep.sh` (the loader past what the cluster
commits, on fresh clusters), `benchab.sh` and `benchab2.sh` (arms rolled onto one cluster in turn)
and `profab.sh` (the same, with a titan profile). **Interleave arms and reverse their order**: on
this lab the first arm after an upgrade wins by 2–6% whichever build it is
([performance](performance.md#the-admission-gate-and-the-bench)).

Each run is recorded by `target/lab/record.sh`, which runs `vmstat 1` on every host beside it. Disk
writes are counted per device from `/proc/diskstats` before and after (`target/lab/diskstats.sh`).
These scripts are scratch and are not committed. What they measured is on these pages.

## Reading a node's figures

`cluster stats` prints, since round 11, each member's row memory against its eviction budget and
its resident memory, and the cluster's ten busiest groups with the member leading each. Since
round 12 it also prints each member's storage pipeline: WAL syncs and bytes a second, the WAL's
segments, the sealed segments waiting on a compactor, entries committed and not yet applied, and
bytes proposed and not yet answered, with each shard's applied writes a second (quietest to busiest)
and the groups each shard leads. Since round 13 the storage table also has each member's mean WAL
sync (`sync ms`, a batch's write and `fdatasync` together), the appends one sync carries
(`per sync`), and the share of the interval's syncs in each size bucket (under 4 KiB, 16 KiB,
64 KiB, 256 KiB and 1 MiB, then the rest). These tell a slow device from small batches: a failing
disk shows as `sync ms`, and a group commit settled on small batches shows as the first buckets
filling. `load --series <secs>` prints the load's rate over each
interval, so a load that changes pace partway through shows where. A node
that judges its own links slow says so in its journal (`this node's links are slow, and it hands
its leads on`), as does one under its append reserve (`under the append reserve`).

Since round 15 the memory table also has each member's `archive maps`, `table maps` and `wal
index`, the bytes the shards' archive map indexes, tables' partition indexes and WAL entry indexes
hold, estimated from their sizes; none counts against a budget. What is left of `resident` past the rows and the two
is counted by nothing, and a node whose left over grows under load wants a heap profile:

```bash
# the node program with jemalloc and sampled heap profiling built in: an allocation sampled every
# 512 KiB on average, the live samples dumped every 2 GiB allocated to /var/tmp/shoal-heap.*
CARGO_TARGET_DIR=target/prof RUSTFLAGS="-C target-cpu=znver1 -C force-frame-pointers=yes" \
    cargo build --release -p tmdb-dataset --bin tmdb-dataset-node --features jemalloc-prof
# an inventory whose `server:` names target/prof/release/tmdb-dataset-node, rolled on with
# `cluster upgrade`; then a dump's live bytes by allocation site, symbolized against the binary
python3 target/lab/r15/prof/heap.py /var/tmp/shoal-heap.<pid>.<n>.i<n>.heap \
    target/prof/release/tmdb-dataset-node 3
```

That is what named [#191](../appendix/resolved/raft-channels-preallocated.md). A cluster grows past
the one dataset with `load --copies <n> --first-copy <c>`, each copy under ids of its own, and
`verify --copies <n>` expects every copy's keyword rows.

## When a move is slow

A move that makes no progress says so itself, every 30 s, on the node leading the group:

```text
a move's destination has made no progress  group=… stalled_secs=270 sent=14788 data=1
  accepted=14787 conflicts=1 failed=0 last_prev=Some(1092395) last_acked=None
```

`data` counts the appends that carried entries, as opposed to heartbeats. `last_acked` is the highest
index the copy accepted. `failed` and `last_error` say whether the link is at fault. The compactor
logs how long each snapshot cut waited (`taking a snapshot cut … queued_ms= backlog=`) and every job
that held it over 5 s (`a compaction job ran long … kind= secs=`). These are the tools that found
[#174](../appendix/resolved/snapshot-cut-queue.md). Since
[O74](../appendix/optimizations.md#o74-a-zen1-nodes-compactor-falls-hundreds-of-jobs-behind-under-the-bench)
a long job also says where its time went: `frames` and `read_ms` for a merge's segment,
`loaded` and `load_ms` for the partitions it read (or the records a pass copied), `apply_ms`,
`written` and `write_ms`, `sync_ms`, `fold_ms`, and `archives` emptied. `target/lab/o74/summary.py`
averages them per host and kind from each host's journal (`target/lab/o74/journal.sh`).

A copy that lost part of its log says so at its start, by group
([#176](../appendix/resolved/unreadable-voter-log.md)): `a durable log has a hole below its
checkpoint` (purged, nothing lost) or `… past its checkpoint; forgetting it` under a floor on its
vote, then `a copy was fed past its floor` once its leader has fed it. The replication report
carries `floor` on a group while it holds.

**Do not turn on openraft's debug tracing on a loaded lab node.** `RUST_LOG` at debug for
`openraft::replication` wrote about 150,000 lines a second a node under the bench. rsyslog copied it
to `/var/log/syslog` until titan's root device, which holds its storage, was full
([section 8](correctness.md#8-an-unplaced-member-coordinates), runs 3 and 4).

## How to read the numbers

These are not captures. The rule in [Benchmarking](../performance/benchmarking.md) holds: a number
that is committed or compared belongs to the benchmark host, and this lab is not it. Europa runs
`powersave` and also hosts the clients, and the Zen1 hosts run `schedutil`. Every number here is
one run on this lab, labelled as such. It says what shape an answer has (whether a fault costs one
second or ten, which host saturates first, whether a change moved something by 5% or 5×), not what
Shoal costs.

## The pages

- [Correctness](correctness.md): the load and full read back, acknowledged writes under load, the
  crash the first runs found, and the fault tests.
- [Performance](performance.md): load and mixed workload throughput and latency, where each host
  spends its time, and the optimizations tried here with their outcomes.
- [Findings](findings.md): every defect and optimization this testing found or measured, with
  what was done about each.
- [What is left](todo.md): the open defects, optimizations still to apply or measure,
  limitations the lab confirmed, and scenarios not yet run.

## Related

- [C11](../distributed/testing.md), the fixture and the protocol model, which remain the regression
  suite.
- [C14](../distributed/deploying.md), deploying a cluster with `shoalctl`.
- [C15](../distributed/open-issues.md), the open questions this chapter answers some of.
