# Building Shoal

Two requirements are not recorded anywhere in the repository, and both will stop a fresh
clone from building.

## Requirement 1: a nightly toolchain

`shoal-core` opens with a feature gate:

```rust
#![feature(trivial_bounds)]
```

`shoal-core/src/lib.rs:1`

`trivial_bounds` is unstable, so Shoal requires nightly Rust. There is no `rust-toolchain.toml`
in the repo, so nothing tells `rustup` to select one — you must have nightly as your default
or invoke `cargo +nightly` explicitly. This book was written against `1.96.0-nightly`.

The feature is needed because of how heavily the codebase leans on associated types through
`rkyv`. Trait bounds like `<R as Archive>::Archived: Deserialize<R, ...>` appear on nearly
every table method, and some of the generated impls produce bounds that the compiler
considers trivially true or trivially false, which stable rejects.

> Adding a `rust-toolchain.toml` pinning nightly would make this failure mode
> self-explanatory. See [TODOs](../appendix/todos.md#build-and-packaging).

## Requirement 2: a sibling glommio checkout

The workspace does not use the published glommio crate:

```toml
glommio = { path = "../glommio/glommio", version = "0.10" }
```

`Cargo.toml`

This is a **path dependency to a sibling directory**. Shoal expects a glommio checkout at
`../glommio` relative to the repository root:

```
parent/
├── shoal/          # this repository
└── glommio/        # a glommio checkout, patched
    └── glommio/    # the inner crate
```

Shoal depends on APIs not present in an unmodified glommio release — `DmaFile::close_rc`
(used by `StreamWriter`, `.../fs/stream.rs:313`) and `PoolThreadHandles`
(`shoal-core/src/server.rs:1`) among them. Substituting the crates.io release will not
compile.

## Building

You cannot build the `shoal` library on its own in a useful way — it is a façade whose types
are only instantiated once a concrete schema exists. Build an example or the tests instead:

```bash
# build the bundled example
cargo build --example tmdb

# build the benchmark workloads, which are the other thing that instantiates a schema
cargo build --release --bin shoal-workload

# release build, which is what you want for any measurement
cargo build --release

# everything, including tests and examples
cargo check --workspace --all-targets
```

`.cargo/config.toml` in the workspace sets `-Ctarget-cpu=native`. `gxhash`, Shoal's hash
function everywhere, depends on AES-NI intrinsics, so builds are not portable across machines
with different instruction sets. Its *output* is: a given gxhash version hashes a key to the same
value on every platform it supports, which is what lets a directory move between machines
([Resolved #65](../appendix/resolved/gxhash-pin.md)).

**A program deployed to other machines is built for the oldest of them**, in its own target
directory so the native build is left alone. `RUSTFLAGS` beats every config file, which is what
makes this work, and the environment's `native` export is what makes it necessary:

```bash
# the lab's hosts are Zen1 (hyperion, titan) and Zen4 (europa); Zen1 has AES-NI and AVX2
CARGO_TARGET_DIR=target/deploy RUSTFLAGS="-C target-cpu=znver1" \
    cargo build --release -p shoal-bench --bin shoal-node --bin shoal-benchctl
```

A native Zen4 build dies of SIGILL on a Zen1 host. `shoalctl cluster` meets that at the node's
`claim`, before anything starts, and refuses it by name
([F51](../features/cluster-deployment.md)).

> This page claimed that before the file existed. The flag was in a `[build]` table in the
> workspace `Cargo.toml`, where **cargo silently ignores it** — which is why the docs and
> `shoal_looper.sh` both passed `RUSTFLAGS` by hand. Any measurement taken before
> `.cargo/config.toml` was added was built without it.

## Tests

```bash
cargo test                  # everything
cargo test insert           # a single test by name
```

Integration tests live in `shoal/tests/`. They spin up a real `ShoalPool` against a
`tempfile::TempDir` and talk to it over a real TCP socket — there is no in-process test
harness. ~~Each test grabs a unique port from a global counter starting at 13000.~~ Each test
starts its server on port `0` and reads the port the shards bound from `ShoalPool::ready`
([Resolved #38, 58, 88](../appendix/resolved/pool-readiness.md)); the helpers in
`shoal/tests/utils.rs` configure two cores and a 100 MiB memory limit.

Because tests spawn real per-core executors, they are sensitive to the machine's core count. The
cluster fixture in `shoal/tests/cluster/` ([F36](../features/cluster-harness.md)) additionally
allocates whole physical cores per child and records when the machine is too small to isolate them.

## Features

| Crate | Feature | Effect |
| --- | --- | --- |
| `shoal-core` | `server` *(default)* | Pulls in `glommio` and compiles the server half. Without it you get a client-only build. |
| `shoal-core` | `hotpath` | Enables the `hotpath` profiler, activating `#[hotpath::measure]` / `#[measure_all]` attributes scattered through the hot path. |
| `shoal` | `hotpath` | Same, **forwarded to `shoal-core/hotpath`** — it was not, and the resulting profile was empty. |
| `shoal-core` | `bench` | Re-exports crate private internals as `tables::bench_exports` so the criterion benches can reach them. Not a supported API. |
| `shoal` | `bench` | Same, forwarded. Required to build `shoal/benches`. |
| `shoal-core` | `shql-complete` | SHQL autocompletion support for clients. |
| `shoal-client` | `stage-profile` | Records when a response came off the socket. Has to be enabled with the server's half, or the two disagree about whether stamps exist at all. |
| `shoal-client` | `otel` | Puts a W3C trace context on every request frame, so a caller's spans and the server's are one trace ([F35](../features/wire-trace-context.md)). Off by default, and safe to leave off: the client sets no flag bit and writes no extra bytes, and a server understands the bit either way. |
| `shoal` | `otel` | Same, forwarded to `shoal-client/otel`. There is no server half to forward — `shoal-core` reads the context unconditionally. |

```bash
cargo build --release --bin shoal-workload --features hotpath   # a profiling build
cargo bench -p shoal --features bench                     # the micro benchmarks
cargo test -p shoal --features otel                       # including the two-process trace test
```

**`otel` is off by default and a `cargo test --workspace` still builds it**, because `shoal-bench`
enables `shoal/otel` and cargo unifies features across a workspace build. That is the one feature
here whose reach is decided by a crate other than the one you are building.

See [Observability](../operations/observability.md) for what `hotpath` reports and
[Benchmarking](../performance/benchmarking.md) for how to run either.

## Running the examples

There are two, and the difference between them is what they need off disk.

### `tmdb` — nothing set up

~~The `tmdb` example is both a demo and the benchmark harness.~~ Since
[F8](../features/purpose-built-workloads.md) it is only a demo, and it needs **nothing set up**:

```bash
cargo run --example tmdb
```

No config, no dataset, no flags. It starts a server against a temporary directory under
`target/`, writes a dozen movies, and reads them back four ways — a keyed get, a projection, a
filter, and the same query written in SHQL.

The benchmark harness it used to be is now twenty-three purpose-built workloads in `shoal-bench`.
See
[Benchmarking](../performance/benchmarking.md) for how to run them and what the numbers mean.

### `tmdb-dataset` — the real dataset, deployed

~~`cargo run --release --example tmdb_dataset`~~ The dataset example started its own server and
needed a config, a storage flag and a tracing flag to do it. Since
[F54](../features/tmdb-dataset-deployment.md) it is a crate, `examples/tmdb_dataset/`, with a
node program to deploy and a loader to fill the deployed cluster:

```bash
# built for the oldest cpu among the hosts, never native, in its own target dir
CARGO_TARGET_DIR=target/deploy RUSTFLAGS="-C target-cpu=znver1" \
    cargo build --release -p tmdb-dataset
# an inventory whose server is target/deploy/release/tmdb-dataset-node, then the cluster
target/deploy/release/tmdb-dataset-loader cluster new -o tmdb.yml
target/deploy/release/tmdb-dataset-loader cluster bootstrap -i tmdb.yml
# the dataset, into every member, then a sample read back
target/deploy/release/tmdb-dataset-loader load -i tmdb.yml \
    --dataset ~/datasets/TMDB_movie_dataset_v11.csv
```

The dataset is `TMDB_movie_dataset_v11.csv`, about 538 MB and 1.19 million movies, from
[Kaggle](https://www.kaggle.com/datasets/asaniczka/tmdb-movies-dataset-2023-930k-movies).
Nothing in this repository fetches it. `--limit` loads a slice, and `--workers`, `--batch` and
`--in-flight` size the pipeline. A row that will not deserialize is skipped and counted. A write
that fails with a code saying to try again (`OutcomeUnknown`, `Shedding`, `NotLeader` and the
like) is sent again after a backoff, up to `--retries` times. Any other failed write stops the
load, and running it again finishes it, since every insert is keyed by the movie. A large retry
count means the pipeline is deeper than the cluster commits: lower `--in-flight`.
`--addr <host:port>` loads one node started by hand instead of a deployed cluster.

**It is not a benchmark**, despite printing a rows-per-second figure. There is no warmup,
repetition, percentile or baseline behind that number. Measuring Shoal is `shoal-bench`'s job;
see [Benchmarking](../performance/benchmarking.md).

Note that the checked-in `shoal.yml` points storage at `/opt/shoal`, which must be writable
(`sudo mkdir -p /opt/shoal && sudo chown $USER /opt/shoal`). A
root the server cannot create is refused with its path, `cannot use the storage directory
/opt/shoal: Permission denied` ([Resolved #126](../appendix/resolved/storage-directory-unusable.md)).
 It is also the benchmark configuration, so changing it invalidates the recorded
baseline ([Performance Baseline](../performance/baseline.md)). It used to carry a
typo that silently disabled core exclusion; a misspelled resource key now fails the load
instead ([Configuration](configuration.md#the-exluded_cores-typo--fixed)).

## Design notes

The path dependency on a patched glommio is the defining constraint on this codebase. It is
why Shoal can use `close_rc` and `PoolThreadHandles`, and it is also why Shoal cannot be
published to crates.io as-is. Any change requiring new glommio behaviour is a two-repository
change.

## Limitations

- No `rust-toolchain.toml`, so the nightly requirement surfaces as a confusing compile error.
- No vendoring or git dependency for glommio, so the build is not reproducible from this
  repository alone.
- `-Ctarget-cpu=native` plus `gxhash` means binaries are not portable between machines; a
  deployed one is built for an explicit cpu, above.
