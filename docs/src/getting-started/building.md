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
with different instruction sets.

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
harness. Each test grabs a unique port from a global counter starting at 13000
(`shoal/tests/utils/utils.rs:11-16`) and configures two cores and a 100 MiB memory limit.

Because tests bind real ports and spawn real per-core executors, they are sensitive to the
machine's core count and to ports already in use.

## Features

| Crate | Feature | Effect |
| --- | --- | --- |
| `shoal-core` | `server` *(default)* | Pulls in `glommio` and compiles the server half. Without it you get a client-only build. |
| `shoal-core` | `hotpath` | Enables the `hotpath` profiler, activating `#[hotpath::measure]` / `#[measure_all]` attributes scattered through the hot path. |
| `shoal` | `hotpath` | Same, **forwarded to `shoal-core/hotpath`** — it was not, and the resulting profile was empty. |
| `shoal-core` | `bench` | Re-exports crate private internals as `tables::bench_exports` so the criterion benches can reach them. Not a supported API. |
| `shoal` | `bench` | Same, forwarded. Required to build `shoal/benches`. |
| `shoal-core` | `shql-complete` | SHQL autocompletion support for clients. |

```bash
cargo build --release --bin shoal-workload --features hotpath   # a profiling build
cargo bench -p shoal --features bench                     # the micro benchmarks
```

See [Observability](../operations/observability.md) for what `hotpath` reports and
[Benchmarking](../operations/benchmarking.md) for how to run either.

## Running the example

~~The `tmdb` example is both a demo and the benchmark harness.~~ Since
[F8](../features/purpose-built-workloads.md) it is only a demo, and it needs **nothing set up**:

```bash
cargo run --example tmdb
```

No config, no dataset, no flags. It starts a server against a temporary directory under
`target/`, writes a dozen movies, and reads them back four ways — a keyed get, a projection, a
filter, and the same query written in SHQL.

The benchmark harness it used to be is now fifteen purpose-built workloads in `shoal-bench`. See
[Benchmarking](../operations/benchmarking.md) for how to run them and what the numbers mean.

Note that the checked-in `shoal.yml` points storage at `/opt/shoal`, which must exist and be
writable. It is also the benchmark configuration, so changing it invalidates the recorded
baseline ([Performance Baseline](../operations/performance-baseline.md)). It used to carry a
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
- `-Ctarget-cpu=native` plus `gxhash` means binaries are not portable between machines.
