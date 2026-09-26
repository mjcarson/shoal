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
voters, six cores and 8 GiB per node with a dedicated control core, mutual TLS on the peer lanes,
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
removed at the end of the test that added it.

Each run is recorded by `target/lab/record.sh`, which runs `vmstat 1` on every host beside it. Disk
writes are counted per device from `/proc/diskstats` before and after (`target/lab/diskstats.sh`).
These scripts are scratch and are not committed. What they measured is on these pages.

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

## Related

- [C11](../distributed/testing.md), the fixture and the protocol model, which remain the regression
  suite.
- [C14](../distributed/deploying.md), deploying a cluster with `shoalctl`.
- [C15](../distributed/open-issues.md), the open questions this chapter answers some of.
