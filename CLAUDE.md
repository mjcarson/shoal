# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build Commands

```bash
# You can't build just shoal on its own you can either build an example or the tests
# To build the examples you can use
cargo build --example tmdb
# and the TMDB dataset's deployable pair, a node program and a loader (F54)
cargo build --release -p tmdb-dataset

# the benchmark workloads are the other thing that instantiates a schema
cargo build --release --bin shoal-workload

# Build with release optimizations (recommended for benchmarking)
cargo build --release

# Run tests
cargo test

# Run a specific test
cargo test <test_name>

# the client compiles a schema with no engine in its graph. this is D5's regression test: it
# fails to resolve `glommio` if a server path creeps back into the client half of #[shoal::db]
cargo check -p shoal-client-check --no-default-features

# the explorer's native window is behind a non-default feature, so `--workspace --all-targets`
# does not compile it. this is the same idiom as the line above, for the same reason
cargo check -p shoal-top --features native

# and the browser half, which is the target that actually gets used. RUSTFLAGS is set explicitly
# because the shell exports `-C target-cpu=native`, which reaches wasm32 and beats every config file
cd shoal-top && RUSTFLAGS="-C target-cpu=generic" \
    cargo check --target wasm32-unknown-unknown --lib --features web

# the client half of the trace context is behind a feature, so `cargo test -p shoal` compiles
# `trace_propagation.rs` away. a `--workspace` run does build it, because `shoal-bench` enables
# `shoal/otel` and cargo unifies features - this is the form that runs it on its own (F35)
cargo test -p shoal --features otel --test trace_propagation

# and the same property on the two programs somebody runs (F63): the admin tool and the UI
cargo build -p shoaladm -p shoalctl

# the TMDB node on jemalloc with sampled heap profiling built in (round 15 of the cluster testing):
# built by nobody else, and what names a node's memory when `cluster stats` cannot. Dumps land in
# /var/tmp/shoal-heap.*; target/lab/r15/prof/heap.py reads them against the binary
cargo check -p tmdb-dataset --features jemalloc-prof

# the two feature-gated shoal-bench binaries are built by nobody else, and the one test behind a
# feature that starts a server runs against a scratch copy of shoal.yml (item 97)
cargo check -p shoal-bench --features stage-profile,hotpath --all-targets
cargo test -p shoal-bench --features stage-profile --test stage_join

# the protocol model (F36) is pure: no engine, no runtime, no shoal crate in its graph. it tests
# while shoal-core does not compile, and `cargo tree -p shoal-model` must never mention glommio
cargo test -p shoal-model
cargo run -p shoal-model --example regenerate_schedules   # after a model change; tests only load

# the cluster fixture (F36) re-executes this binary as its children, so it is its own test target.
# since F37 its servers are cluster nodes: each child runs a control thread on a core the fixture
# allocates; since F39 they join each other through node zero and the M1, M2 and M3 acceptance
# tests live here; since F40 every tablet is replicated between them and the nine M4 tests too;
# since F42 the thirteen M6 tests kill, stall, isolate and restart them; since F43 the eight M7
# tests leave a node behind the purge point, throttle and cut the snapshot streams that feed it,
# and crash it at every point of an install (CRASH_AT); since F46 the seven M9b tests drain,
# expire, block and rebalance them (DECOMMISSION, REMOVE, MAINTENANCE, REBALANCE, PLAN_STATUS,
# FREE_BYTES); since F47 the two M9c tests restart a node at another core count on its lease and
# crash the rehome at every point (restart_with_cores, ChildOverrides, REHOME, HOSTING,
# SHARD_DIRS), one of them on a standalone child, which answers the command loop too; since F50
# a `.peer_tls()` cluster runs mutual TLS under a fixture authority whose leaves name their
# nodes, RELOAD_TLS reloads one, and the rotation test skips by name without `modprobe tls`;
# since F52 STATS [table] reads every member's figures and every plan's pace, and the two stats
# tests count a cluster's writes and partitions and follow a rebalance through it.
# every test allocates whole cores, so the suite is what a loaded machine makes it: a failure
# that passes alone was a timeout, and the child logs are under SHOAL_CHILD_LOG=<dir> (one file
# per child, DEBUG, hundreds of MB each - point it under target/, never at a tmpfs). run it at
# six threads: at the default thirty-two the children die at glommio's io_uring probe on the
# development host (the fencing failure filed as item 100 was a race of the clone's own, since
# resolved). SHOAL_CHILD_LOG wants an absolute path - a child runs in its own directory - and a
# targeted run: one suite run under it wrote 54 GB and failed twenty-two tests on the I/O
cargo test -p shoal --test cluster_fixture -- --test-threads 6
SHOAL_CHILD_LOG=$PWD/target/child-logs cargo test -p shoal --test cluster_fixture -- <one test>

# conditional writes on every table kind of a standalone server (F68); the replicated half - a
# race through three nodes, a refused retry, a compaction and restart, the wire 7 gate - is four
# `conditional` tests in the fixture. a cluster refuses one until `activate 7`
cargo test -p shoal --test conditional_writes
cargo test -p shoal --test cluster_fixture -- --test-threads 6 conditional

# the fixture's storage faults (F70): `FAULT_DIR <dir> torn <bytes> | full <bytes> | lost | clear`
# arms one in a child, through the I/O hook the glommio fork gained for it. Their match to a real
# device behind device-mapper needs passwordless sudo, losetup, dmsetup and mkfs.ext4, and is ignored
cargo test -p shoal --test cluster_fixture -- --test-threads 6 device_faults
cargo test -p shoal --test kernel_faults -- --ignored --nocapture

# item 33's reproduction (F41): a standalone two shard get whose shares are held expires at the
# bundle deadline. run against the tree with the gather sweep disabled it never returns
cargo test -p shoal --test gather_expiry

# the tablet groups' WAL under openraft's storage suite, plus the rotation, stream and checkpoint
# tests (F40). the memory log the ephemeral tables replicate through runs the same suite
cargo test -p shoal-core wal

# the control plane's two conformance suites and its crash test (F37): the glommio runtime under
# openraft's own suite, the control store under openraft's storage suite, and a torn append
cargo test -p shoal-core control

# the Q1/Q13 spike (F37): not a benchmark and never a capture. prints the idle and durable tables
# the decision record in docs/src/distributed/protocol.md carries, labelled by host and governor
cargo run -p shoal-spike --release
# and its second question (F39): what a topology push and the members' status reports cost as
# the cluster grows, which is the Q13-at-M3 record on the same page
cargo run -p shoal-spike --release -- fanout

# the X4 spike: erasure coding crates fed the same buffers, checked and timed on one core. NOT a
# workspace member - it is its own workspace with its own Cargo.lock, so no erasure coding crate
# reaches the workspace's lockfile before M18 and no workspace build needs a C toolchain. isa-l
# builds the ISA-L it bundles, which needs `sudo apt install nasm autoconf automake libtool
# pkgconf`. Build it for the lab's Zen1 hosts in a target dir of its own, and natively for europa
# with the C kernels of reed-solomon-erasure native too (from a clean target dir: its build script
# does not rerun when that variable changes). The tables are on
# docs/src/object-storage/erasure-coding-crates.md
cd shoal-spike-erasure && CARGO_TARGET_DIR=../target/lab/x4/znver1 RUSTFLAGS="-C target-cpu=znver1" \
    cargo build --release
cd shoal-spike-erasure && RUST_REED_SOLOMON_ERASURE_ARCH=native CARGO_TARGET_DIR=../target/lab/x4/native \
    cargo build --release
target/lab/x4/native/release/shoal-spike-erasure --quick --core 8          # proves every adapter runs
target/lab/x4/znver1/release/shoal-spike-erasure all --core 2 --out titan-znver1.json   # on the host
target/lab/x4/native/release/shoal-spike-erasure report *.json            # the page's summaries

# the X5 spike: every checksum candidate fed the same buffers, held to published check values,
# compared across hosts, builds and ways of feeding it, and timed on one pinned core. NOT a
# workspace member, for X4's reason: no candidate reaches the workspace's lockfile before M13 adds
# the chosen one. An x86-64 level build needs `+aes`: the levels do not carry it and gxhash 3
# refuses to compile without it. `--features gxhash3-hybrid` builds for znver1 and dies of SIGILL
# on the Zen1 hosts, which is one of its findings. The tables are on
# docs/src/object-storage/checksums.md
cd shoal-spike-checksum && CARGO_TARGET_DIR=../target/lab/x5/znver1 RUSTFLAGS="-C target-cpu=znver1" \
    cargo build --release
cd shoal-spike-checksum && CARGO_TARGET_DIR=../target/lab/x5/x86-64-v3 \
    RUSTFLAGS="-C target-cpu=x86-64-v3 -C target-feature=+aes" cargo build --release
target/lab/x5/znver1/release/shoal-spike-checksum --quick --core 8          # proves every adapter runs
target/lab/x5/znver1/release/shoal-spike-checksum all --core 2 --out titan-znver1.json   # on the host
target/lab/x5/znver1/release/shoal-spike-checksum report *.json            # the page's summaries

# the X2 spike: S5's placement candidates simulated over generated pool maps, from the lab's three
# hosts to fifty of twenty-four, for fill, movement, feasibility and exceptions. Pure and seeded, so
# `placement` prints the same tables on every host (75 seconds on europa's 32 threads); `lookups`
# times one answer a candidate on a pinned core, the only host-dependent figure, and `fanout` sizes
# the pool map's frame beside the tablet map's. A subcommand of shoal-spike, so it builds the engine:
# for the Zen1 hosts in a target dir of its own. The tables are on
# docs/src/object-storage/placement-simulation.md, the raw ones in shoal-spike/results/
cargo run -p shoal-spike --release -- placement > shoal-spike/results/x2-placement.md
cargo run -p shoal-spike --release -- placement --only lab --per-tablet 1,4   # a quick look
CARGO_TARGET_DIR=target/lab/x2/znver1 RUSTFLAGS="-C target-cpu=znver1" cargo build --release -p shoal-spike
target/lab/x2/znver1/release/shoal-spike placement lookups --core 2 --label znver1   # on the host
target/lab/x2/znver1/release/shoal-spike fanout                                      # on the host

# the X6 spike: S6's device store measured on a filesystem as a slice's executor drives it - a
# whole chunk against a slot of a shared file, the journal, a partial write journalled against
# cloned (FICLONERANGE), removing, listing a placement group cold, a unit read cold, fragmentation
# after clones, and one device under one to eight executors. A subcommand of shoal-spike. It runs
# as root (it drops caches and locks memory), refuses tmpfs and the root filesystem, and writes
# records that `report` merges across rounds and judges against the four triggers. The lab's run
# is shoal-spike/results/x6-lab.sh: four rounds, the filesystems' order alternating, seven hours on
# titan for three filesystems and forty minutes more for `--only chunk-recycle`. The 970 EVO's
# flush has two regimes (0.9 ms rested, 3 ms after synced writes), so a figure that involves a
# sync depends on what ran before it: x6-order.sh is the check. /xfs on titan and hyperion is the
# lab's fitted XFS, an LV beside the root; europa's Optane at /optane is XFS since 2026-10-03.
# The tables are on docs/src/object-storage/device-store-ssd.md
CARGO_TARGET_DIR=target/lab/x6/znver1 RUSTFLAGS="-C target-cpu=znver1" cargo build --release -p shoal-spike
sudo target/lab/x6/znver1/release/shoal-spike device quick --dir /optane/x6/quick   # proves every measurement runs
sudo /var/tmp/x6/shoal-spike device all --dir /xfs/x6 --round 1 --out titan-xfs.json   # on the host
target/lab/x6/znver1/release/shoal-spike device report shoal-spike/results/x6-*.json   # intervals and verdicts

# the X10 spike: rows shaped like the object store's two generated rows (S3's ObjectMeta and
# StripeMeta) through today's persistent unsorted tables on the lab's cluster - rows a second a
# group, bytes a row on disk and in memory cold and resident, a commit to a row a restart left
# cold, and an object held inline from 1 KiB to 1 MiB. Its own workspace crate, shoal-spike-rows
# (a schema, a node program and `x10`, the driver with every shoaladm command flattened in), since
# the cold commit is an update shoaladm bench cannot drive. Its inventory is tmdb_cluster.yaml
# renamed x10 with its own ports and roots (titan and hyperion on /xfs). results/x10-lab.sh runs
# four rounds of three legs (rate, rows, size), each on a cluster bootstrapped for it, under the
# performance governor, the driver pinned clear of europa's node; about three and a half hours.
# LEGS=remedy runs the supplement on the read before a commit, about an hour. The tables are on
# docs/src/object-storage/stripe-row-costs.md
CARGO_TARGET_DIR=target/lab/x10/znver1 RUSTFLAGS="-C target-cpu=znver1" cargo build --release -p shoal-spike-rows
QUICK=1 ROUNDS=1 OUT=target/lab/x10/quick sh shoal-spike-rows/results/x10-lab.sh   # proves every leg runs
sh shoal-spike-rows/results/x10-lab.sh                                              # the four rounds
LEGS=remedy sh shoal-spike-rows/results/x10-lab.sh                                  # the supplement
target/lab/x10/znver1/release/x10 report shoal-spike-rows/results/x10.json         # intervals and verdicts

# a cluster on real hosts from a project (F63): run in the project that defines the schema,
# shoaladm finds the #[shoal::db] struct, probes every host's cpu over ssh, builds the node once
# per cpu class and the schema's admin program, installs them under ~/.local/shoal/bin, and
# bootstraps the cluster - or upgrades one that exists. no cargo line to remember; the first
# run compiles the engine once per program and class (3m19s on europa for the tmdb example
# against the lab's two classes), later runs are incremental. shoalctl does the same for the UI
cargo build --release -p shoaladm -p shoalctl
cd examples/tmdb_dataset && ../../target/release/shoaladm new -o inventory.yml   # leave Server program blank
../../target/release/shoaladm deploy
../../target/release/shoaladm status
../../target/release/shoalctl
# the inventory every command reads when given none lives in ~/.config/shoal/config.yaml
../../target/release/shoaladm config default-inventory inventory.yml
# the bench schema lives in a module of a workspace crate, so its pair is written by hand and
# its inventory names a `server:` built for the oldest cpu among the hosts - never native, which
# the shell exports and which dies of SIGILL on the Zen1 hosts. its own target dir, so the
# native build is left alone
CARGO_TARGET_DIR=target/deploy RUSTFLAGS="-C target-cpu=znver1" \
    cargo build --release -p shoal-bench --bin shoal-node --bin shoal-benchctl
# an inventory is written in a form (F53) that judges it as you type; --from edits one. a node's
# storage directories can be set per deployment, per named group of nodes, or per node
target/deploy/release/shoal-benchctl new -o target/wizard/my.yml --from shoaladm/inventories/lab.yml
target/deploy/release/shoal-benchctl bootstrap -i shoaladm/inventories/lab.yml
target/deploy/release/shoal-benchctl status -i shoaladm/inventories/lab.yml
# a rebuilt node program onto a running cluster, one node at a time and the leader last (F55);
# --activate ends the rolling window, --rollback swaps every node back onto its .prev before it
target/deploy/release/shoal-benchctl upgrade -i shoaladm/inventories/lab.yml
target/deploy/release/shoal-benchctl destroy -i shoaladm/inventories/lab.yml --yes
# the rendered shoal.yml parsed and validated as a Conf, claimed, started and initialized
cargo test -p shoal-bench --test deploy_render
# and against the hosts: bootstrap, rows through every node, add --rebalance, destroy. a
# rebalance step takes at least the inventory's retire_after (15s on the lab, five minutes by
# default). the lab's nodes run as the system user `shoal`: a node running as you on europa
# spends your io_uring locked-memory budget and every glommio test here dies at its probe
SHOAL_DEPLOY_INVENTORY=$PWD/shoaladm/inventories/lab-add.yml \
    cargo test --release -p shoal-bench --test deploy_smoke -- --nocapture

# Run with hotpath profiling enabled (attribution only, never a baseline number)
cargo build --release --bin shoal-workload --features hotpath

# Run the TMDB example. Needs no config, no dataset and no flags - it starts its own
# server against a temp dir under target/ and reads the rows back four ways.
cargo run --example tmdb

# The real dataset as a deployed database (F54): examples/tmdb_dataset is a crate with the
# node program and a loader that is also this schema's admin program and UI. It needs
# TMDB_movie_dataset_v11.csv (538MB, from kaggle, not in this repo) and a deployed cluster
# (`shoaladm deploy` in that directory, above); --addr loads a node started by hand instead.
# The rows/sec it prints is not a measurement.
cargo build --release -p tmdb-dataset
target/release/tmdb-dataset-loader load -i examples/tmdb_dataset/inventory.yml --dataset ~/datasets/TMDB_movie_dataset_v11.csv --limit 10000
# conditional writes raced through every member, judged against the committed order (F68)
target/release/tmdb-dataset-loader contend -i examples/tmdb_dataset/inventory.yml --keys 16 --workers 24

# benchmark any schema against a dataset folder (F66): one <Table>.csv/.json/.jsonl a table, the
# table opted in with #[shoal_table(db = "...", dataset)] and serde::Deserialize. Run in the
# schema's project, it copies the inventory into a cluster of its own (<name>-bench, every root,
# port and directory moved), runs every workload at every bundle size, and tears it down again. Proving
# a change on the lab: isolate it the way a side cluster is, and never point it at tmdb without
# --attach (read-only unless --yes-write)
cd examples/tmdb_dataset && SHOAL_BIN_DIR=$PWD/../../target/lab/<f>/bin SHOAL_DEPLOY_HOME=$PWD/../../target/lab/<f>/state \
    ../../target/debug/shoaladm bench run -i <side inventory> --dataset <folder> --workloads read100,rw50 \
    --bundles 1,16 --runs 2 --duration 10 --allow-neighbours        # --dry-run prints the plan first
# leave --workloads out on a terminal and a wizard chooses the whole run, explaining each choice;
# ctrl-s saves it as a spec that `--spec` runs again (F67). With no terminal the four defaults run
../../target/debug/shoaladm bench list | show <label> | compare <baseline> <candidate>
# every run of a capture keeps each host's device counters (/proc/diskstats before and after, by
# the device each root is on) and each member's memory (F71); `--paced <table> --paced-rate <N>`
# drives one table at an offered rate beside the main load, with windows of its own (F72). The
# script runs under `sh -c`: a login shell of zsh, europa's, ties `path` to PATH
cd examples/bench_dataset && ../../target/debug/shoaladm bench run -i <inventory> --dataset dataset \
    --workloads read100,insert100 --paced Review --paced-rate 50 --stop-unit shoal-tmdb
# the driver, the dataset, the capture and compare, engine-free like shoaladm
cargo test -p shoal-loadgen
cargo tree -p shoal-loadgen | grep -c glommio     # 0
# the whole path against a node in process, nothing schema specific in it, and a real profile build.
# its `stats` test starts a cluster of one node and holds every metric of the stats view that
# says it moves (`Metric::under_load`) to a real run (F67)
cargo test -p bench-dataset
cargo test -p bench-dataset --test profile_build -- --ignored
```

## Benchmarking

**A schema's own workload on its own cluster is `shoaladm bench`** ([F66](docs/src/features/dataset-benchmarks.md)),
above under the build commands; it is meant to replace `shoal-bench`, which stays until it is
retired (`docs/src/appendix/todos.md#retiring-shoal-bench`). Everything else in this section is
`shoal-bench`, the workspace crate that runs the engine's benchmarks, stores the results with their
provenance, compares them, and generates the book's results page.

```bash
# capture a full run: micro + macro + hotpath + stages, into docs/perf/runs/
cargo run -p shoal-bench --release -- run --label <label>

# a subset while iterating, filtered the way `cargo test` filters tests
cargo run -p shoal-bench --release -- list get_key
cargo run -p shoal-bench --release -- run --label <label> --layer micro get_key

# the named groups, what each answers, and what a capture of it would cost
cargo run -p shoal-bench --release -- list --groups

# one question rather than the whole suite. --group combines with or, and intersects
# with --layer and the filters. it selects only: nothing runs concurrently, ever
cargo run -p shoal-bench --release -- run --label <label> --group conf/storage

# a whole capture at a fraction of the data, for checking a workload runs at all
cargo run -p shoal-bench --release -- run --label <label> --scale smoke --runs 2

# what workloads exist, and what each one isolates
./target/release/shoal-workload list

# compare against the frozen baseline and the trailing one, which is the default pair
cargo run -p shoal-bench --release -- compare <label>

# what has been captured, and whether it still describes the current code
cargo run -p shoal-bench --release -- status

# regenerate the results pages, or check that the committed ones are current
cargo run -p shoal-bench --release -- render
cargo run -p shoal-bench --release -- render --check

# the interactive explorer (F29). unlike `render`, this draws any number of captures on one chart,
# against either a swept fact or the capture timeline. a native window needs a display, so on a
# machine reached over ssh the second form is the one that works
cargo run -p shoal-bench --release -- explore
cargo run -p shoal-bench --release -- explore --serve      # http://127.0.0.1:8321
cargo run -p shoal-bench --release -- explore --index-only  # target/explore/index.json
```

`--serve` needs `wasm-bindgen` at **exactly** the version the lockfile resolved; it refuses to build
with the `cargo install` line to run rather than producing a bundle that fails in the browser. The
index is written to `target/explore/` and must never be committed - `gather` computes `Page::dirty`
from every uncommitted path outside `docs/src/performance/`, so a committed index would break
`render --check` permanently.

`docs/perf/baselines/B1-performance.json` is frozen and never overwritten — `promote` refuses it
with no override. `shoal.yml` is the benchmark config and is committed — changing it invalidates
the baseline. The benchmarks assume the CPU governor is `performance`. `run` refuses a dirty tree
unless given `--allow-dirty`, because a measurement of bytes that exist in no commit cannot be
located in history afterwards.

### Commit first, then capture

**A capture taken on a dirty tree is marked `uncommitted` in its provenance and stays that way
forever.** There is no fixing it up afterwards: the label records the commit it ran against, and if
that commit does not contain the bytes that were measured, the number can never be located in
history. So the order is always:

1. **Commit the change.** All of it — a capture against a half-committed tree is the same problem.
2. **Capture**, on a clean tree, with the `performance` governor.
3. **`render`**, and commit the regenerated pages under `docs/src/performance/` as a follow-up.

`--allow-dirty` exists for throwaway captures while iterating on a workload, never for one whose
numbers are going to be committed or quoted. A capture taken that way should be deleted from
`docs/perf/runs/` rather than left in the corpus, because everything in that directory appears on
the freshness table as though it were a real measurement.

### When a capture is needed

Not on every change. Take one when:

- **A workload was added, removed or changed.** Its own source fingerprint moved, so every existing
  capture is correctly reported as no longer describing it.
- **`shoal.yml`, the seed, or anything in `shoal-bench/src/workloads/` changed.** Same reason, and
  this includes adding a field to `ScaleFacts` or a row type to the shared schema — both change the
  fingerprint of *every* workload.
- **A change claims a performance effect.** `docs/src/appendix/optimizations.md` will not accept an
  entry as acted on until a benchmark exists that would show the difference, and names it.

Do **not** take one for a docs-only change, a renderer change, or a test-only change. For a renderer
change, `render` and `render --check` are the verification; the artifacts underneath are untouched.

### Before and after on the lab

**Benchmarks run on the lab nodes**, hyperion or titan (Zen1, 4 cores and 8 threads, no cargo),
since 2026-09-30. The committed captures and the frozen baseline came from another host, so a
lab number is an A/B, not a capture. Any change that adds work to a node's query path gets one,
before and after, and a figure that costs too much is sampled or moved behind a profiling
feature. The procedure is in `docs/src/performance/benchmarking.md#before-and-after-on-the-lab`,
and F65's page is the worked example:

1. **Commit the change.** Build `shoal-workload` at it and at the commit before it, each in a
   worktree beside the repo (glommio is a sibling path dependency), with
   `RUSTFLAGS="-C target-cpu=znver1"` and its own `CARGO_TARGET_DIR`. `scp` both to the host.
2. **Use a scratch conf** under `target/lab/<feature>/`, sized for four cores: 2 shards,
   `exclude_cores: [3]`, storage on `/opt/shoal`, tracing at `Warn`, no remote sink.
3. **Prepare the host**:
   - `sudo systemctl stop shoal-tmdb` on it;
   - `sudo cpupower frequency-set -g performance`;
   - afterwards, restore `schedutil`, start the node, and check `status` shows 3 of 3.
4. **Run the sides back to back**, the first alternating by round, for four rounds or more, and
   wipe `/opt/shoal` before each run. A difference is a result only when the two sides' run
   intervals are disjoint; repeat a suspicious one with more rounds.
5. **Quote the numbers on the feature's page**, labelled by host, cpu and governor. Nothing goes
   into `docs/perf/runs/` unless the user asks for a lab corpus.

A full capture was **seventy-five minutes**, measured by `F20-conf` on 2026-08-22 — the first one
anybody timed. This file said four to five hours until then, and that figure was never a
measurement. [F22](docs/src/features/row-size-benchmarks.md) then added 165 macro arms, so the
projection is now **about two hours** — and that figure is a projection again, scaled from the
measured one by arm count, until somebody times a capture of the current set. While iterating, `--scale smoke --runs 2` runs the whole set at a hundredth of the
data and proves every workload still runs — which is what you want before spending the hour on the
real one. Budget twenty minutes for a macro-only smoke pass; the criterion layer is what makes a
full smoke run take longer than you expect.

The macro layer is three hundred and ninety-nine **workloads** living in `shoal-bench/src/workloads/`,
each generating its own rows from `--seed` — there is no dataset to fetch
([F8](docs/src/features/purpose-built-workloads.md)). They come in three kinds and the differences
matter:

- **Isolating workloads** drive one path each, so a difference between two captures can be
  attributed to something. Eight of them are storage-free controls over ephemeral tables, each
  paired with a persistent workload it is read against
  ([F9](docs/src/features/ephemeral-tables.md)) — a pair differs in the storage engine and in
  nothing else, and each pair has a test asserting that, so a constant changed in one half has to
  change in the other.
- **The grid** is two hundred and twenty-nine workloads driving a *mixture* of reads and writes,
  swept across row width, read share, key distribution and load depth
  ([F17](docs/src/features/workload-grid.md), extended by
  [F22](docs/src/features/row-size-benchmarks.md)). It answers what a caller's workload costs. The
  width axis is sixteen widths against all four tables at three mixtures, plus a rung at each width
  with one query outstanding.
  **A regression is never attributed to a grid arm** — the grid says a mixture got slower, the
  isolating pairs say which half.
- **The cluster overhead arm** is one workload, `macro/cluster/overhead/nodes/1`: the grid's
  reference cell served by a server with a `cluster:` block ([F37](docs/src/features/node-identity-control-plane.md)).
  It is read beside `macro/grid/unsorted/r50/1024` and nowhere else; `--group cluster` selects the pair.
- **The replication arms** are three workloads ([F40](docs/src/features/replication.md)):
  `macro/cluster/overhead/nodes/3`, the reference mixture on three nodes of three shards
  replicating to nobody, and `macro/cluster/replication/{durable,volatile}`, the same placement
  at a factor of three on the persistent and the ephemeral table. They are read against each
  other and never against `nodes/1`, whose shard count they do not share. An arm asking for more
  copies than it places nodes is refused before a server starts.
- **The read arms** are seven workloads ([F41](docs/src/features/read-consistency.md)):
  `macro/cluster/reads/{one,barrier,session}`, one get at the reference depth on the replication
  arms' placement differing only in what the read asks for, and
  `macro/cluster/fanout/{get,filter,limit,empty}`, a six key get split over the same three nodes
  at a factor of one in four shapes. Every read arm's capture carries `cluster.reads` - the
  barrier and application waits apart from the round trip - and `empty` reads keys it never
  wrote on purpose (`Workload::expects_rows`). The cluster arms' port blocks are numbered among
  the cluster arms and stay under 32768; one numbered among every workload sat in the ephemeral
  range and lost its control port to a `TIME_WAIT`.
- **The failover arm** is one workload ([F42](docs/src/features/primary-failover.md)):
  `macro/cluster/failover/kill`, the durable replication cell driven for a fixed time by a
  client that does not retry, with node one killed a third of the way through and started
  again two thirds through by the harness (`Workload::fault`). Its capture carries
  `cluster.fault` - the marks, the client's first failure and recovery, three windows with a
  distribution each and a per second series - so the outage is never averaged into the run.
  Node zero is the driver's own process and is never the one killed.
- **The rebalance arms** are four workloads ([F46](docs/src/features/capacity-rebalancing.md)):
  `macro/cluster/rebalance/{add,decommission,remove,capacity_blocked}`, the kill arm's
  placement and mixture with a plan the control leader drives from a third of the way through -
  a `Rebalance` onto a spare, a `Decommission` onto it, an expiry after node one is killed for
  good (`FaultSpec::restart: false`) under a five second grace, and a `Decommission` with no
  spare that stays blocked by name. Each carries `cluster.rebalance` with `p99_ratio_permille`,
  the number M9b's two-times budget is judged on; the blocked arm's `outcome` is `unfinished`
  by construction.
- **The rehome arm** is one workload ([F47](docs/src/features/local-rehome.md)):
  `macro/rehome/shrink`, the `nodes/1` arm seeded at twelve executors and started again at
  eight (`ConfOverrides.restart_shards`, applied by the harness to the server that comes back),
  so the start between runs a rehome of four executors' files; its capture carries
  `cluster.rehome`, the pool's report with `millis` for the hold. It is read on its own, never
  against the reference cell: eight executors hosting twelve slots is another server.
- **The background arms** are two workloads ([F44](docs/src/features/repair.md),
  [F49](docs/src/features/backup-and-recovery.md)): `macro/cluster/background/repair`, the
  kill arm's placement and mixture with nothing killed and a `Repair` of the reference table
  in verify mode asked for a third of the way through (`Workload::background`), whose capture
  carries `cluster.background` - the marks, the groups, what the scrubs hashed and read, three
  windows and a per second series - so a scrub's cost to the foreground is `during` read
  against `before`; and `macro/cluster/background/backup`, the same with the wire version
  activated and a `Backup` of the table asked for instead, whose capture carries
  `cluster.backup` - the marks, the files' counts, bytes and records, the same windows and
  series. The backup's files go under the workload's own storage root, which the next run wipes.
- **The configuration sweep** is fifty-eight workloads under `macro/conf/`, each one the grid's
  reference cell `macro/grid/unsorted/r50/1024` with **exactly one field** of the server
  configuration moved ([F20](docs/src/features/configuration-sweeps.md)). It answers what a setting
  in `shoal.yml` is worth. Every sweep contains the value the committed `shoal.yml` resolves to, and
  a test fails if one stops bracketing it — so retuning that file is a test failure rather than a
  sweep that quietly stops covering the configuration everything else is measured under.

They are compiled into a second binary of that crate, `shoal-workload`, which the runner builds and
spawns; the runner half still builds with `--no-default-features` and no engine at all, which is
what lets a capture be judged while `shoal-core` will not compile. **A workload's identifier is the
key every comparison joins on, so renaming one orphans every capture taken before the rename.** Add
and deprecate instead — and **append**, never interleave: a workload's position in
`workload_ids::IDS` decides the TCP port a capture gives it. Adding a workload means one file, one
line in `workloads::all()`, one line in `workload_ids::IDS`, and a family in
`shoal-bench/src/render/family.rs` — a test fails if you forget any of the last three.

A full capture is about two hours and its macro layer is most of it, so **use a group**
rather than a prefix ([F21](docs/src/features/benchmark-groups.md)) when a question is narrower than
the whole set. `list --groups` prints the fourteen declared sets, what each answers, and what a
capture of it would cost — projected onto `FULL_MACRO_CAPTURE_SECS`, which is hand-maintained and
has not been re-measured since the macro layer grew by 165 arms, so every projection it prints is
currently low; `--group grid` is the grid alone, `--group
isolating` is everything that drives one path, `--group conf/storage` is the writer knobs. A group
**selects and never schedules** — a capture still runs one `shoal-workload` process at a time, and
must, because two servers at once share a page cache, a device queue and a set of cores. `--scale
smoke` runs anything at a hundredth of the data.

**Everything under `docs/src/performance/` except `benchmarking.md` and `baseline.md` is
generated** — never edit those pages by hand, run `shoal-bench render`, which writes all eleven or
none. Each page opens with four mandatory blocks (what it measures, how to read it, what would make
it wrong, what it cannot tell you) that come from its family
([F18](docs/src/features/results-pages.md)). See `docs/src/performance/benchmarking.md` for the
runbook, `docs/src/performance/baseline.md` for the frozen numbers and hardware, and
`docs/src/features/bench-runner.md` for how the tool is put together.

## Fixing a Bug

**Every bug fix updates the docs in `docs/src/`, in the same change as the code.** The reasoning
behind a fix is worth more than the fix, and a page that still describes the broken behaviour is
worse than no page. This is not optional and does not need to be asked for.

1. **Find the item.** Open `docs/src/appendix/known-issues.md` and locate the defect. Keep its
   number — numbers are shared between the known and resolved pages and are **never reused**, so a
   number appears on exactly one of them. A defect that is not filed yet gets the next free number.
2. **Reproduce before fixing.** Write the test first, run it against the unfixed tree, and keep the
   actual failure output. That output goes into the **Evidence** section, which always states
   whether the defect was established by reading the source or by reproducing it. A test written
   after the fix and assumed to cover it is not evidence.
3. **Fix it**, then move the item:
   - delete it from `known-issues.md` — or, if only part of it was fixed, leave the open remainder
     there and say so on both pages, as items 9, 20, and 24 do;
   - add a row to `docs/src/appendix/resolved-issues.md`;
   - write `docs/src/appendix/resolved/<slug>.md`.
4. **The resolved page always uses the same sections**, in this order: **Symptom, Cause, Evidence,
   The fix, Alternatives rejected, Invariants to uphold, Still open, Tests, Related.** Two of these
   carry most of the value. *Alternatives rejected* — a fix is only understandable next to what it
   is not. *Invariants to uphold* — this is the section someone reads before changing that code
   again, so state what the fix depends on, not what it does. The *Tests* table names what breaks
   if the fix is reverted, one row per test.
5. **Register the page** in `docs/src/SUMMARY.md`, in item-number order.
6. **Sweep the rest of the docs for anything the fix just made false.** `grep -rn "<feature>"
   docs/src` and read every hit. The pages that go stale most often are
   `tables/query-execution.md` (its Limitations list), `tables/table-types.md`,
   `tables/partitions.md`, `api/shql.md`, `api/derive-macros.md`, `appendix/glossary.md`,
   `appendix/todos.md`, `appendix/optimizations.md`, and `appendix/test-coverage.md`. Check the
   "Still open" sections of other resolved pages too — a follow-up one of them called for may be
   what you just did.
7. **File what you found on the way.** New defects go to `known-issues.md` at the next free number;
   performance findings go to `optimizations.md` as a new `O` number; work that is a missing
   feature rather than a defect goes to `todos.md`. An optimization you deliberately did not take
   is recorded, not dropped.
8. **Re-run and re-count.** `cargo check --workspace --all-targets` and `cargo test --workspace`,
   then update the baseline counts in the headers of `known-issues.md` and `test-coverage.md`, and
   the per-binary counts in `test-coverage.md`.

Code style for the fix itself follows the rest of the repo: a docstring on every function, struct,
enum, and method, and an inline comment for each step inside them.

## Adding a Feature

**Every feature updates the docs in `docs/src/`, in the same change as the code.** A bug fix earns
a page when it had a wrong mental model behind it; a feature earns one when it introduces a model
that was not there before, because the next person to touch that code will reason from the model
rather than from the diff. This is not optional and does not need to be asked for.

1. **Find what it was filed as.** Most features are already described in
   `docs/src/appendix/todos.md`, often with a design sketch. Read it first — it is usually right
   about the hard part, and where it is wrong that is worth saying on the page you write.
2. **Take the next free `F` number.** They are shared across
   `docs/src/features/delivered-features.md` and never reused, the same rule issue and `O` numbers
   follow.
3. **Build it**, then write `docs/src/features/<slug>.md`. **The page always uses the same
   sections**, in this order: **Context, What it does, Design choices, Alternatives rejected,
   Limitations, Invariants to uphold, Performance, Tests, Related.** Three of these carry most of
   the value. *Alternatives rejected* — a design is only understandable next to what it is not.
   *Limitations* — the gap between what a feature looks like and what it does, written down rather
   than discovered. *Invariants to uphold* — what the feature depends on, not what it does. The
   *Tests* table names what breaks if the feature is reverted, one row per test.
4. **Add a row to `delivered-features.md`** and register the page in `docs/src/SUMMARY.md`, in
   `F` number order.
5. **Sweep the rest of the docs for anything the feature just made false.** `grep -rn "<feature>"
   docs/src` and read every hit. A feature makes *more* pages stale than a fix does, because it
   changes what the system can do rather than what it does correctly. The pages that go stale most
   often are `api/shql.md`, `api/derive-macros.md`, `tables/query-execution.md`,
   `tables/table-types.md`, `tables/partitions.md`, `appendix/glossary.md`, `introduction.md`, and
   `operations/shoalctl.md`. Check the "Still open" sections of the `resolved/` pages and the
   entries in `optimizations.md` too — a feature often closes one.
6. **Say so where the old claim was.** A rule the feature changed is struck through and kept with
   what replaced it, not deleted: `resolved/sort-keys.md` and `optimizations.md` both do this. A
   reader who learned the old rule needs to find out it moved.
7. **File what you found on the way**, the same way a fix does: `known-issues.md`,
   `optimizations.md`, `todos.md`. A piece of the feature you deliberately did not build is
   recorded in `todos.md` with the reason, not dropped.
8. **Re-run and re-count.** `cargo check --workspace --all-targets` and `cargo test --workspace`,
   then update the baseline counts in the headers of `known-issues.md` and `test-coverage.md`, and
   the per-binary counts in `test-coverage.md`.

Code style is the same as for a fix.

## Planning Docs

**Every planning doc draws the order of its work as a mermaid diagram**, in the same change that
writes the plan. A plan is a chapter or page that lays out work not yet done: prerequisites,
spikes, milestones. It ends with a page that draws all of it, so a reader can see what can start
now, what waits on what, and what is already done, without reading every page.
[What's left to do](docs/src/object-storage/whats-left-todo.md) is the worked example.

1. **Draw every piece of work, spikes included.** That means prerequisites, the lab or hardware
   a step needs fitted, each spike, the decisions or gates, and the milestones. Split a spike
   where only part of it waits on something, as X6's XFS leg and X12's rotational half are split.
2. **An arrow points from a piece of work to what requires it.** Work with no arrow between it is
   parallel and sits on one level. Draw an arrow only where no longer path already implies it,
   and say on the page where the full list of requirements lives.
3. **Keep the levels honest.** Mermaid's layout pulls a box with no inputs down beside the first
   thing it feeds, which reads as "later". Hold a box that can start now on the first level with
   the link's length: `X2 ---> Gate` spans two levels, and each extra dash is one more.
4. **Mark what is done.** A done box is green with a ✅ at the start of its label:
   `classDef done fill:#2e7d32,stroke:#1b5e20,color:#ffffff` and
   `F69["✅ F69 Operation kinds"]:::done`. **Whoever finishes a piece of work turns its box green
   in the same change that finishes it**, the way a finished row on a prerequisites page gets
   its ✅ and its old text struck through.
5. **Mark what is optional.** An optional box says `(optional)` in its title and has a dashed
   border: `classDef optional stroke-dasharray: 5 5` and `:::optional`. Its arrows are dashed too
   (`-.->`), meaning *helps* rather than *requires*. Whether something is required or optional
   follows the same rule as prerequisites, and the reason is written on the page that lists it.
   Optional work that feeds nothing goes in a subgraph of its own, "Optional, any time".
6. **Keep it readable at book width.** A level wider than about six boxes does not fit a top to
   bottom diagram. Draw that part left to right (`flowchart LR`), or split the page into two
   diagrams at a gate both share, rather than let one shrink. Keep labels short and wrap them
   with `<br>`.
7. **Never put `#` in a label.** Mermaid reads `#…;` as an entity: write "Item 202", not "#202".
8. **Look at it rendered.** Run `mdbook build docs` and open the page in a browser. mdbook-mermaid
   renders the fences there (`docs/mermaid.min.js`), so it is the only place a fence that does
   not parse, or a layout that does not read, shows. Check the navy and the light theme; the
   done colour is chosen to read in both.

## Architecture Overview

Shoal is a high-performance, distributed database with persistence, built on three crates:

### Crate Structure

Seven implementation crates plus a facade, a spike and a deployable example. The split is
[F15](docs/src/features/client-server-split.md) and the rule it enforces is simple: **nothing
outside `shoal-proto`, `shoal-client`, `shoal-core` and `shoal-derive` names any of them — callers
go through `shoal`.**

- **shoal-proto** - The wire format and everything both peers agree about: `shared/protocol/`
  (framing, handshake, auth frames), `shared/queries/` (including the SHQL parser),
  `shared/responses.rs`, `shared/traits.rs`, `shared/auth/` (SCRAM), `shared/tls.rs` (rustls
  config, no I/O), `client/errors.rs`, `stamps.rs`. **Links no async runtime.** Do not add tokio,
  glommio or kanal here
- **shoal-channel** - `KeptReceiver`, a kanal receiver whose receive in progress survives the
  future that waited on it ([Resolved #152](docs/src/appendix/resolved/kanal-receive-races.md)).
  **Never race a bare kanal `recv()`** (`select!`, `timeout`, `race`): a dropped receive loses a
  value already handed to it. Race a kept receiver's `next`; `tests/no_raced_receives.rs` scans
  the workspace for the bare form. Depends on kanal alone
- **shoal-client** - The tokio client: `Shoal<S>`, the three streaming modes, the `bb8` pool.
  Links no storage engine
- **shoal-core** - The database engine:
  - `server/` - Shard management, query handling, TCP listener
  - `server/database.rs` - `ShoalDatabase`, the trait a schema implements to be served
  - `server/routing.rs` - `ShardRouting`, which splits a query across the shards owning its keys
  - `server/tables/storage/` - Filesystem storage with DMA (glommio-based)
  - `pub use shoal_proto::shared` - so the whole of `server/` names the protocol unchanged.
    Depends on `shoal-proto`, and **never** on `shoal-client`
- **shoal-derive** - Procedural macros. Emits `::shoal::` paths only and depends on no shoal crate
- **shoal** - The facade. `default-features = false` drops `shoal-core` and leaves a client
- **shoal-client-check** - A schema that compiles against the client alone. Not a library: it
  exists to fail if a server path creeps back into the client half of `#[shoal::db]`
- **shoal-top** - The benchmark explorer ([F29](docs/src/features/benchmark-explorer.md)):
  `index.rs` holds the portable index types and depends on `serde` alone, and everything behind the
  `ui` feature draws them with `egui`/`egui_plot`. **`shoal-bench` enters it with
  `default-features = false`**, which is what keeps egui out of `cargo tree -p shoal-bench
  --no-default-features`. It must never depend on `shoal-bench`: that crate pulls `walkdir`, which
  does not build for `wasm32-unknown-unknown`, and the explorer's primary target is a browser
- **shoal-spike** - The Q1/Q13 spike ([F37](docs/src/features/node-identity-control-plane.md)): a
  binary, `publish = false`, that drives N openraft groups on one pinned glommio executor through
  a counting loopback network and prints what they cost; `fanout`
  ([F39](docs/src/features/membership.md)) prices the topology push and the status reports
  instead, and since X2 the pool map's frame beside it; `placement`
  ([X2](docs/src/object-storage/placement-simulation.md)) simulates S5's placement candidates in
  `src/placement/`, which is pure and names no engine type. Depends on `shoal` with the engine, and
  is the one place the control store is driven with three members in a group
- **shoal-spike-erasure** - The X4 spike ([erasure coding crates](docs/src/object-storage/erasure-coding-crates.md)):
  every erasure coding candidate S18 pinned behind one trait, checked against every loss pattern
  and timed on one pinned core. **Not a workspace member**: its manifest carries an empty
  `[workspace]`, so it has its own `Cargo.lock`, the erasure crates stay out of the workspace's
  until M18 adds the chosen one, and `isa-l`'s C build (nasm, autotools, a `pkg-config` pinned at
  0.3.22 so libisal-sys's source fallback is reachable) never touches a workspace build. Deleted
  when M18 lands
- **shoal-spike-checksum** - The X5 spike ([checksums](docs/src/object-storage/checksums.md)):
  every checksum candidate S18 pinned behind one trait, held to published check values, compared
  across hosts, builds and ways of feeding it, and timed on one pinned core, with a CRC combine of
  its own written from zlib's method. **Not a workspace member**, for X4's reason: no candidate
  reaches the workspace's lockfile until M13 adds the chosen one. Deleted when M13 lands
- **shoal-spike-rows** - The X10 spike ([what a stripe row costs](docs/src/object-storage/stripe-row-costs.md)):
  a schema of four persistent unsorted tables shaped like S3's generated rows (`ObjectMeta`,
  `StripeMeta`, a `StripeMeta` with a digest a chunk, and filler), its node program `x10-node`, and
  `x10`, a driver that aims writes at one tablet group's tablets and at its leader, reads every
  member's `Replication` and `Stats` and every host's device and network counters around each cell,
  and carries every `shoaladm` command. A workspace member, since it adds no crate to the lockfile;
  thrown away like every spike's code
- **shoal-model** - The deterministic protocol model ([F36](docs/src/features/cluster-harness.md)):
  the contract P1–P6 as executable checks over a Raft-shaped tablet group, with saved schedules
  under `shoal-model/schedules/`. Depends on `serde` and `serde_json` alone and names no shoal
  crate; keep it that way, since a schedule that needs the engine to replay is worth nothing
- **tmdb-dataset** - The TMDB dataset as a deployable database ([F54](docs/src/features/tmdb-dataset-deployment.md)),
  under `examples/tmdb_dataset/`: the library is the schema, `tmdb-dataset-node` is its
  `node::main`, and `tmdb-dataset-loader` is `load -i <inventory>` beside every shoaladm command
  and the UI as `tui`. Both binaries use the one `Tmdb` type, so they cannot disagree about the
  fingerprint. `shoaladm deploy` run in its directory finds `Tmdb` in the library and builds the
  same node itself
- **shoal-loadgen** - The schema generic benchmark ([F66](docs/src/features/dataset-benchmarks.md)):
  the dataset folder and its streaming readers, the preload and insert pool, the spec and its arm
  matrix, the seeded picker, per-second hdrhistogram windows, the driver, event window cutting,
  the capture and compare. Generic over `S: DatasetSupport`; the per-table pieces come from the
  table derives. **Links no engine** - `cargo tree -p shoal-loadgen | grep -c glommio` is 0
- **bench-dataset** - `examples/bench_dataset/`, a small catalog schema with a committed dataset
  (and a `dataset-bad/` refused by name) that the whole `shoaladm bench` path is tested against
- **shoaladm** - Deploying and operating a cluster over ssh ([F51](docs/src/features/cluster-deployment.md)),
  the inventory wizard ([F53](docs/src/features/inventory-wizard.md)), the cluster tab's model,
  and since [F63](docs/src/features/shoaladm.md) building a schema's programs from its project:
  `project` scans `src/main.rs` then `src/lib.rs` for `#[shoal::db]`, `build` generates a wrapper
  crate under the project's `target/shoal-build/` and runs cargo, `cpu` probes a host and names
  its `-C target-cpu`, `config` reads `~/.config/shoal/config.yaml` and installs programs under
  `~/.local/shoal/bin`. The binary knows no schema: for a command that connects it builds the
  schema's admin program and `exec`s it. Since F66 `bench/` is `shoaladm bench`: the bench's own
  cluster (`owned.rs`, every root moved and checked disjoint), host changes undone on every way out
  (`hosts.rs`), events, `--profile` builds (a wrapper of their own, `build::Flavor`), and the run on
  a thread of its own drawn in the stats view. **Links no engine** - `cargo tree -p shoaladm | grep -c glommio`
  is 0 - and every dependency is one the lockfile already resolves
- **shoalctl** - The terminal UI, a library entered by `shoalctl::cli::main_blocking::<DbClient>()`
  and a binary that builds that program for the project's schema and runs it (F63). Depends on
  `shoaladm` for the cluster tab's model and for connecting to a deployed cluster; links no engine

### Key Abstractions

**Table Types:**
- `PersistentSortedTable<T, Storage, TableNames>` - Partitioned + sorted with disk persistence
- `PersistentUnsortedTable<T, Storage, TableNames>` - Partitioned only with disk persistence
- `EphemeralSortedTable<T>` / `EphemeralUnsortedTable<T>` - In-memory only. **Aliases**, not
  implementations: the same two tables with `NoStorage` as the engine, so they cannot drift from
  the persistent pair ([F9](docs/src/features/ephemeral-tables.md)). A schema writes one generic
  and the macro fills the rest in. Nothing they hold is ever evicted, and nothing survives a
  restart.

**Macros:**
- `#[derive(ShoalSortedTable)]` - Generates query structs (Get, Insert, Update, Delete) and trait impl for sorted tables
- `#[derive(ShoalUnsortedTable)]` - Same for unsorted tables
- `#[shoal::db]` (`#[shoal::db(client)]` for the client half alone; there is no `shoal_db`) - Attribute macro that rewrites table field types (adds `<Self>` to storage and `TableNames`; for an ephemeral table there is no storage generic to add it to, so `Self` is pushed as one) and generates `TableNames` enum, `*Client` struct, `QueryKinds`/`ResponseKinds` enums. It classifies a field by looking for `Sorted`/`Unsorted`/`Persistent`/`Ephemeral` in the type name, so a table type named anything else panics during expansion

**Field Attributes:**
- `#[shoal_table(db = "DbName", dataset)]` - `dataset` lets the table be loaded from a dataset file
  and benchmarked by name (F66); the row must also derive `serde::Deserialize`
- `#[shoal(partition)]` - Partition key (required)
- `#[shoal(sort)]` - Sort key (sorted tables only)
- `#[shoal(filter)]` - Filterable field
- `#[shoal(update)]` - Updatable field
- `#[shoal_table(db = "DbName")]` - Links table to database schema

### Server Model

- One shard per CPU core (cpu 0 reserved for coordination; a cluster node also reserves the
  control core's whole physical core for its control thread unless `cluster.control_core_shared`)
- Since [F47](docs/src/features/local-rehome.md) a shard is two things: an *executor*, one per
  core, that owns the `Shard-N` files, and a *slot*, the shard a peer names. A cluster node's
  slots (`cluster.slots`, one per core by default) are claimed once into the marker's `shards`
  and never move - every group identity is hashed from them - and `shoal-hosting.json` says
  which executor hosts each slot and owns each tablet (`server/hosting.rs`); a standalone node
  hosts per tablet. Changing `resources.cores` is a rehome (`server/rehome/`) run before any
  shard starts under `shoal-rehome.json`, a manifest resumed at its step: fold, copy the moving
  archived records, move the moving groups' WAL entries and sidecars through `GroupStore`,
  reclaim, finalize. A peer never learns an executor number: `peer::listener::dispatch_target`
  is the one place a slot becomes one, and a group's address on this node is `spec.me(node)`,
  never the executor id. More cores than slots is refused; grow past them with a `Replace`.
- Since [F44](docs/src/features/repair.md) every archive record is `[size][gxhash64][payload]`
  behind a format 2 header, written by `write_record` and verified by `ArchiveMap::read_record`
  and nowhere else; a scrub is `Command::scrub`, a command whose tablet is `SCRUB_TABLET`,
  applied in committed order and never handed to a compactor; a quarantine is
  `MachineState::quarantined`, persisted under `wal/Shard-N/quarantine/`, decided locally and
  committed through the node's report; `Repair` is driven by the group's leader in
  `shard/repair.rs` with every phase committed before the step it names, and a durable copy is
  repaired by restarting its group from its held checkpoint with the received file pending
- A `cluster:` block makes the node a cluster member ([F37](docs/src/features/node-identity-control-plane.md)):
  `server/meta.rs` mints a `NodeId` and a `ClusterId` into a format 3 marker, `server/control/`
  runs an embedded `openraft 0.10.0-alpha.34` group on a glommio `AsyncRuntime` written there, and
  `ShoalPool::topology()` reports what it committed. Absent, none of that exists
- Since [F39](docs/src/features/membership.md) the group is the cluster's membership: a node with
  `seeds` joins through them as a learner, the leader promotes voters up to `control_voters`, a
  `TabletMap` built from the committed state is pushed whole to every shard (`server/map.rs`)
  and to every subscribed client, admin operations ride the client connection, writes are
  admitted against the write quorum, and the leader's phi-accrual detector (`control/detector.rs`)
  commits a silent member `Down`. Never hold a `RefCell` borrow across an `.await` on the control
  core, and never retry a proposal on a metrics change without a backoff - both starve the one
  executor RaftCore, the links and the loop share
- Since [F46](docs/src/features/capacity-rebalancing.md) a member has a `phase` beside its
  `health` - `Member`, `Leaving`, `Removing`, `Removed` - and `is_placeable` is the one
  placement check; a `Down` verdict under `auto_remove_after` opens a grace the leader counts
  in committed eighths (`GraceElapsed`) and expires into a removal plan; `Decommission`,
  `Remove`, `Maintenance` and `Rebalance` are operations recording plans (`control/plan.rs`)
  whose steps the pure planner (`control/planner.rs`) derives from the sets as served, the
  members' weights and the archived bytes and free bytes they report - kept in the leader's
  memory, never committed - and the leader issues as ordinary moves; a drained member is
  tombstoned before it leaves the control group and its identity is refused at every door.
  Every stream a node sends draws on one token bucket (`stream_bytes_per_sec`), a shard
  installs `concurrent_streams` at once, and `disk_reserve` is checked by the planner and again
  by the receiver at the begin
- Since [F41](docs/src/features/read-consistency.md) a read is served at `One` or `Quorum`: a
  `Quorum` read obtains a `ReadIndex` barrier from its group's leader - its own handle when it
  leads, `ReplicateKind::ReadBarrier` over the lane when not - and waits for its own apply
  through it on a spawned task that posts `ServerMsg::ReadReady`; a committed write's answer
  carries a `SessionToken` a later read is served past. Every gather (`server/shard/gather.rs`)
  has a slot per share, an attempt per bundle and a deadline (`networking.query_deadline`), the
  sweeper runs on every node, and a read refusal is answered as a response, never through the
  metadata's carried failure, which cannot reach a sealed answer. The read options and token
  sections ride the wire only between peers that granted `CLIENT_CAP_READ_OPTIONS` at the hello
- Since [F40](docs/src/features/replication.md) every tablet is replicated: a shard hosts one
  `openraft` group per table and replica set the map derives (`server/map.rs`, `GroupSpec`),
  its log is the shard's shared format 2 WAL (`server/wal/`, one fsync per batch across groups),
  a write is a `Command` proposed through the local replica and applied in committed order by
  `apply` on every replica (`server/shard/groups.rs`), and a cluster node writes no intent log.
  Never await a group's `apply` on the shard loop - `Raft::new` re-applies the checkpoint on the
  caller's task, so groups start on spawned tasks and the loop only receives; only the placement
  primary initializes a fresh group; and a sealed WAL segment is handed to a compactor only once
  every group applied past it and deleted only once every group purged past it
- Uses `glommio` LocalExecutor for thread-per-core async I/O
- `kanal` channels for lock-free inter-shard communication
- Consistent hash ring for partition routing

### Client Model

- `Shoal<S>` wraps TCP connection pool (min 10, max 50 connections)
- Three streaming modes: `send()`, `stream()`, `stream_unordered()`
- UUID-based query tracking for response routing

### Wire Protocol

Every frame begins with an eight byte header - version, message type, two flag bytes and a
32-bit length that counts every byte after it - and a connection opens with a handshake that
carries the schema fingerprint ([F10](docs/src/features/framing-and-protocol-evolution.md),
`docs/src/architecture/wire-protocol.md`).

- Client→Server: `[8-byte header][26-byte trace context, if flagged][rkyv-serialized Queries]`
- Server→Client: `[8-byte header][16-byte query id][rkyv-serialized ResponseKinds]`

## Configuration (shoal.yml)

```yaml
resources:
  cores: 12                    # Number of cores for shards
  exclude_cores: [12, 13, 14, 15]  # Physical cores to exclude, both SMT threads of each
  memory: "4Gi"                # Memory limit; exceeding it evicts 40% of current usage
storage:
  default:
    filesystem:
      latency_sensitive:
        path: "/opt/shoal"
      throughput_sensitive:
        path: "/opt/shoal"
tracing:
  level: Info                  # Trace/Debug/Info/Warn/Error/Off
```

## Usage Pattern

```rust
// 1. Define table with derive macros
#[derive(ShoalUnsortedTable)]
#[shoal_table(db = "MyDb")]
pub struct MyTable {
    #[shoal(partition)]
    pub id: u64,
    #[shoal(filter)]
    pub name: String,
}

// 2. Define database schema
#[shoal::db]
pub struct MyDb {
    pub my_table: PersistentUnsortedTable<MyTable, FileSystem>,
}

// 3. Start server
let conf = Conf::from_file("shoal.yml")?;
let pool = ShoalPool::<MyDb>::start(conf)?;

// 4. Create client and send queries
let client = Shoal::<MyDbClient>::new("127.0.0.1:12000").await?;
let mut results = client.send(queries).await?;
```

## Key Dependencies

- `rkyv` - Zero-copy serialization (all data types must derive rkyv traits)
- `glommio` - Thread-per-core async runtime with DMA filesystem support
- `gxhash` - Fast hashing for partition keys
- `kanal` - Lock-free channels
- `bb8` - Connection pooling
