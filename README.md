# Shoal

A thread-per-core, partitioned database in Rust whose schema is a Rust type. It runs as a single
node or as a replicated cluster.

Shoal is under active development on the `DistributedShoal` branch. The standalone engine and
the cluster both work, and are exercised by 1,790 tests and a three-host lab, but it is not a
general-purpose database yet: see [what it is not](#what-it-is-not-yet).

## What it is

- **The schema is Rust.** A table is a struct with a derive macro, and a database is a struct
  of tables with `#[shoal::db]`. There is no runtime DDL and no dynamic typing; adding a table
  means recompiling. The macros generate a typed client, its query and response types, and the
  server's dispatch.
- **Rows are read where they lie.** [rkyv] is both the wire format and the on-disk format. A
  partition read off disk is filtered, projected and sent back without being deserialized, and
  a client reads the rows straight out of the bytes that came off the socket.
- **One shard per core, nothing shared.** Each core runs its own [glommio] executor with its own
  tables, write-ahead log and files, using direct I/O through io_uring. Shards talk over
  channels, never over locks.
- **Four table types.** Sorted tables keep many rows per partition in sort-key order; unsorted
  tables keep one row per partition. Each comes persistent, with a write-ahead log compacted into
  archives, or ephemeral, in memory only.
- **Five query kinds.** Insert, get, update, delete and exists. A get names its partition keys
  and can add:
  - filters on marked fields;
  - a projection, so only some fields are read;
  - a sort-key range or set within a sorted partition;
  - a limit.

  Queries travel in bundles that may mix tables and kinds. The client can wait for a whole
  bundle, stream its answers in order, or stream them as they finish. SHQL, a small
  `select ... where` text syntax, parses into the same queries.

## What a cluster adds

A node whose configuration has a `cluster:` block joins a cluster through its seeds. From there:

- **Membership and placement** are agreed by an embedded Raft group. A silent member is called
  down, and a duplicate identity is fenced off.
- **Every tablet is replicated.** Each set of copies is its own Raft group. By default a write is
  acknowledged once a majority has fsynced it, and a read can ask for the local copy, a quorum
  barrier, or its own session's writes.
- **The cluster heals itself**:
  - a failed primary is replaced within 7.5 to 10 seconds at the default settings;
  - a returning node is caught up from the log or a snapshot;
  - a corrupt copy is quarantined and repaired from a verified majority.
- **Operators can reshape it**:
  - replica sets move under capacity-aware rebalance, decommission and removal plans;
  - a node's core count can change across a restart;
  - a rolling upgrade negotiates the wire version before activating it;
  - backup and restore work into a new cluster identity.
- **Security** is opt-in: mutual TLS between nodes, with certificate rotation, and SCRAM-SHA-256
  and TLS 1.3 for clients.
- **Every node reports what it is doing**: what its clients were answered, by kind, with read
  and write speed and p50/p99 latency; its memory, storage pipeline and placement; and the pace
  of every plan.

## What it is not, yet

- **Linux only, and a nightly toolchain.** glommio needs io_uring, and `shoal-core` uses an
  unstable feature.
- **No transactions.** A bundle of queries is a batch, not a transaction.
- **No secondary indexes and no scans.** A partition is found only by its partition key, so
  every get names its keys, and every SHQL query needs a `WHERE`.
- **No authorization.** Authentication and encryption exist, and both are off unless configured.
  An authenticated client can read and write every table.
- **No metrics endpoint.** A cluster's figures are an admin read that `shoaladm stats` polls.
  Nothing is exported to a monitoring system, and a standalone node answers no admin reads.

[Known Issues](docs/src/appendix/known-issues.md) lists what is wrong today.

## Getting started

Shoal builds against a patched glommio checked out beside it, and on nightly Rust:

```bash
git clone git@github.com:mjcarson/shoal.git
git clone git@github.com:mjcarson/glommio.git   # must sit at ../glommio relative to shoal
cd shoal
cargo +nightly run --example tmdb
```

The example needs no config, no dataset and no flags. It starts a two-shard server under
`target/`, writes a dozen movies, and reads them back four ways: by key, as a projection,
through a filter, and with SHQL. Keep data off tmpfs, which cannot take direct I/O.

A schema, a server and a client, condensed from that example:

```rust
#[derive(Debug, Clone, PartialEq, Archive, Serialize, Deserialize, ShoalUnsortedTable, DeepSizeOf)]
#[rkyv(derive(Debug))]
#[shoal_table(db = "Tmdb")]
pub struct Movie {
    #[shoal(partition)]
    pub id: u64,
    #[shoal(filter)]
    pub year: u64,
    #[shoal(update)]
    pub tagline: String,
    pub title: String,
}

#[shoal::db]
pub struct Tmdb {
    pub movie: PersistentUnsortedTable<Movie, FileSystem>,
}

// one shard per configured core
let conf = Conf::from_file("shoal.yml")?;
let mut pool = ShoalPool::<Tmdb>::start(conf)?;
pool.ready(Duration::from_secs(30))?;

// a typed client, generated from the schema
let client = Shoal::<TmdbClient>::new("127.0.0.1:12000").await?;
client.send_one(Movie { id: 348, year: 1979, title: "Alien".into(), tagline: "...".into() }).await?;
let answer = client.send_one(MovieGet::new(vec![348])).await?;
if let Some(rows) = answer.access::<Movie>()? {
    for movie in rows.iter() {
        println!("{} ({})", movie.title, movie.year);
    }
}
```

[Building Shoal](docs/src/getting-started/building.md) covers the toolchain and the glommio
checkout, and [Configuration](docs/src/getting-started/configuration.md) covers `shoal.yml`.

## Running a cluster

`shoaladm` deploys and operates a cluster over ssh, building the schema's programs from its own
project. `shoalctl` is the terminal UI.

```bash
cargo build --release -p shoaladm -p shoalctl
cd examples/tmdb_dataset                   # any project with a #[shoal::db] struct
../../target/release/shoaladm new -o inventory.yml
../../target/release/shoaladm deploy       # probes each host's cpu, builds, bootstraps or upgrades
../../target/release/shoaladm status
../../target/release/shoaladm stats        # full screen: totals, charts and a table of members
../../target/release/shoalctl
```

`deploy` builds the node once per CPU class among the hosts, installs it as a systemd unit, and
bootstraps the cluster, or upgrades it one node at a time when it already exists. Other commands:

- `add`, `rebuild` and `rebalance` change which nodes hold the data;
- `upgrade` and `reconfigure` roll out a new build or a new configuration;
- `admin` sends repair, backup and the rest of the cluster tab's operations;
- `ship-backup` copies a backup to every host, and `destroy` removes the cluster.

Read [shoaladm](docs/src/operations/shoaladm.md) and the [runbooks](docs/src/operations/runbooks.md)
before operating a cluster you care about.

`examples/tmdb_dataset` is a complete deployable schema: the TMDB movie dataset, with a loader
and a `bench` command that drives a timed mix of reads and writes against a deployed cluster.

## Workspace

| Crate | What it is |
| --- | --- |
| `shoal` | The facade every user depends on. `default-features = false` gives a client with no engine. |
| `shoal-core` | The engine: shards, routing, storage, replication, the control plane. |
| `shoal-client` | The tokio client, its connection pool and its streams. |
| `shoal-proto` | The wire format, queries, responses, SCRAM and TLS configuration. No async runtime. |
| `shoal-derive` | The table, projection and `#[shoal::db]` macros. |
| `shoal-channel` | A channel receiver that is safe to race in `select!` and timeouts. |
| `shoaladm`, `shoalctl` | Cluster deployment and operations, and the terminal UI. |
| `shoal-bench`, `shoal-top` | The benchmark runner and workloads, and the results explorer. |
| `shoal-model` | A deterministic model of the replication protocol, with saved schedules. |
| `shoal-spike` | Measurements the control plane's design was decided on. |
| `shoal-client-check` | A schema built against the client alone, which fails if server code leaks into it. |

## Performance

`shoal-bench` runs micro benchmarks, around four hundred purpose-built workloads and per-query
stage profiles. It stores each capture with its provenance, compares captures, and renders the
[results pages](docs/src/performance/overview.md) in the book. A change that adds work to a
node's query path is also measured before and after on a lab node, as an A/B
([benchmarking](docs/src/performance/benchmarking.md#before-and-after-on-the-lab)).

## Documentation

The book in `docs/` describes how Shoal works internally, and is written for people changing it.
Every feature has a page recording what it does, the alternatives rejected, its limitations and
the invariants it depends on. Every resolved defect has one too.

```bash
mdbook serve docs
```

Start with the [introduction](docs/src/introduction.md), the
[delivered features](docs/src/features/delivered-features.md) (F1 to F65), and
[Distributed Shoal](docs/src/distributed/overview.md).

## License

MIT.

[rkyv]: https://rkyv.org/
[glommio]: https://github.com/DataDog/glommio
