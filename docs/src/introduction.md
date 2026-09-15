# Introduction

This book documents how Shoal works *internally*. It is written for people changing Shoal,
not for people using it. It covers the design choices behind the system, the trade-offs
those choices imply, the limitations that follow from them, and the places where the code is
unfinished or wrong.

Every non-trivial claim in this book cites a `file:line`. Line numbers drift; treat them as
a starting point rather than an address.

## What Shoal is

Shoal is a thread-per-core database written in Rust, built on three ideas:

- **The schema is a Rust type.** Tables are structs annotated with derive macros. There is no
  runtime DDL, no catalog, and no dynamic typing. Adding a table means recompiling.
- **Data is never parsed if it can be read in place.** Shoal uses [rkyv] for both its wire
  format and its on-disk format. A partition read off disk can be filtered without ever
  being deserialized ([Partitions](tables/partitions.md)).
- **One shard per core, no shared state.** Each core runs an independent [glommio] executor
  owning its own tables, its own write-ahead log, and its own files. Shards talk over
  channels, never over locks ([Thread per Core](architecture/thread-per-core.md)).

Durability comes from a per-shard write-ahead log — called the *intent log* throughout the
codebase — which is periodically compacted into *archives* of partition data, indexed by an
*archive map* ([Storage Overview](storage/overview.md)).

## What Shoal is not

It is worth being blunt about the boundaries, because the crate descriptions ("a distributed
database") oversell the current state:

- **Distributed, as a cluster of nodes.** A node with a `cluster:` block joins a cluster
  through its seeds; the cluster agrees on its members, its placement and its tables under an
  embedded Raft group, fences a duplicate identity, calls a silent member down, and forwards a
  query's shares to the nodes that hold them over bounded, optionally encrypted lanes
  ([Distributed Shoal](distributed/overview.md)). A node with no `cluster:` block is a
  standalone database and none of this exists in it.
- **Replicated, on a cluster node.** A tablet has `min(replication_factor, nodes)` copies on
  distinct nodes, each a Raft group; a default write is acknowledged once a majority has
  fsynced it, a default read is the local copy's committed state, and a strong read is a
  barrier from the leader. A primary that dies is replaced by its group's own election, a node
  that returns is fed a snapshot, a corrupt copy is quarantined and repaired from a verified
  majority, replica sets move between nodes under plans, a rolling upgrade negotiates and
  activates the wire, and a cluster is backed up and restored into a new identity
  ([F36](features/cluster-harness.md) through [F50](features/cluster-operations.md)). A
  standalone node still holds one copy on one disk.
- **No transactions.** There is no atomicity across queries, no isolation between them, and
  no rollback. A bundle of queries is a batch, not a transaction.
- **No secondary indexes.** A partition is only ever found by its partition key; there is no
  scan, which is why a `WHERE` clause is mandatory. *Within* a sorted partition a sort key can be
  matched or bounded, so a large partition can be paged through
  ([F1](features/sort-key-ranges.md)) — but that narrows what a partition returns, not which
  partitions can be reached ([Query Execution](tables/query-execution.md)).
- **A core-count change is a restart, not a migration.** The files of the executors that no
  longer run are moved onto the ones that do before a shard starts, and the start is held for
  it ([F47](features/local-rehome.md)); a cluster node's slots, the shard count its peers
  record, never move, and are the ceiling on its executors. Growing past them is a `Replace`.
- **Encryption and authentication are both optional and both off by default.** A server can require
  SCRAM-SHA-256 and refuse a client that cannot do it ([F12](features/authentication.md)), and it
  can encrypt its listener with TLS 1.3 ([F14](features/encryption-in-transit.md)). Neither is on
  unless a config asks, so the default is still that anything which can reach the port can read and
  write any table — and even an authenticated, encrypted connection can, because there is still no
  authorization ([Wire Protocol](architecture/wire-protocol.md#limitations)).

What it would take to close the rest of that, and five other things the client cannot do, is
designed in [Direction](direction/overview.md) — which is a design record, not a roadmap. Two of
its nine entries have since been built, in whole ([F10](features/framing-and-protocol-evolution.md),
[F11](features/error-channel.md)) and in half ([F12](features/authentication.md)).

Shoal is best understood as a fast partitioned key-value store with a persistence layer, run
either as one node or as a cluster of them. The cluster - replication with a primary per
tablet, membership under Raft, failover, recovery, repair, migration, rebalancing, upgrades,
backup and the tests and benchmarks that prove each - is described in
[Distributed Shoal](distributed/overview.md), with what is still open on its
[last page](distributed/open-issues.md).

## A note on the current branch

This book was written against the `ZeroCopyResponses` branch, during a rewrite of the intent
log writer. That rewrite is now finished: acknowledgement waits for an `fdatasync` covering
the record, the flush watermark only advances over contiguously completed writes, and every
write to the log is block aligned. See [Durability model](storage/overview.md#durability-model)
and [Intent Log](storage/intent-log.md).

Deleted rows also used to come back, in three separate ways: compaction pruned a partition
without removing its archive map entry, unsorted deletes never looked at disk in the first
place, and a partition could be marked evictable while its delete was still in an open intent
log — dropping the tombstone that was the only thing hiding the archived row. All three are
fixed ([#4](appendix/resolved/unsorted-disk-consultation.md),
[#5](appendix/resolved/resurrected-deletes.md)).

Other parts of the branch are still rough. Read [Known Issues](appendix/known-issues.md)
before drawing conclusions about anything else, and
[Resolved Issues](appendix/resolved-issues.md) before changing anything the fixes above depend
on.

The workspace compiles — `cargo check --workspace --all-targets` passes with warnings only,
and `cargo test --workspace` passes with 71 integration tests and 94 unit tests.

## Crate map

| Crate | Role |
| --- | --- |
| `shoal-proto` | The wire format and everything both peers agree about: queries, responses, the traits a schema implements, SCRAM, the TLS configuration. **Links no async runtime.** |
| `shoal-client` | The tokio client, its connection pool and its result streams. Links no storage engine. |
| `shoal-core` | The database: shards, the ring, the storage engines, `ShoalDatabase`. Depends on `shoal-proto`, and never on `shoal-client`. |
| `shoal-derive` | Proc macros. `#[derive(ShoalSortedTable)]`, `#[derive(ShoalUnsortedTable)]`, and the `#[db]` attribute that generates a database's dispatch layer. Emits `::shoal::` paths and depends on no shoal crate at all. |
| `shoal` | The user-facing façade over the four. **Everything outside those four names this and nothing else.** `default-features = false` drops the engine and leaves a client. |
| `shoalctl` | A terminal UI, generic over a compiled-in schema. A client: no glommio, no io_uring. |
| `shoal-client-check` | Not a library. A schema that compiles against the client alone, which fails to build if a server path creeps back into it. |
| `shoal-top` | The benchmark explorer's index types and its `egui` interface ([F29](features/benchmark-explorer.md), [F30](features/plot-axis-units.md), [F33](features/chart-readout.md)). `shoal-bench` enters it with `default-features = false` and gets the `serde` structs alone, so the benchmark runner never grows a graphics dependency. Named for what it is meant to become: a live view of a running server. |

That shape is [F15](features/client-server-split.md), and the reason for it is worth one line:
before it, opening a connection meant compiling a storage engine. `shoal-core` was one crate
holding both peers, with a `server` feature that looked like it separated them and could not.

Inside `shoal-core` the split is:

| Module | Role |
| --- | --- |
| `server/` | Shard lifecycle, the ring, inter-shard messaging, tables, and storage. |
| `server/database.rs` | `ShoalDatabase`, the trait a schema implements to be served. |
| `server/routing.rs` | `ShardRouting`, which splits a query across the shards that own its partitions. |

`shoal-core::shared` still resolves — it is a re-export of `shoal_proto::shared`, so the engine
names the wire format the way it always has.

## Suggested reading order

If you are new to the codebase, read in this order:

1. [Overview](architecture/overview.md) — how the pieces fit together.
2. [Request Lifecycle](architecture/request-lifecycle.md) — one query, end to end. This is
   the page that makes the rest legible.
3. [Storage Overview](storage/overview.md) — the three on-disk structures.
4. [Derive Macros](api/derive-macros.md) — where all the generated code comes from, since
   roughly half of what runs at query time does not appear in the repo as source.
5. [Known Issues](appendix/known-issues.md) — before you trust anything, and
   [Resolved Issues](appendix/resolved-issues.md) before you change anything: each page there
   ends with the invariants its fix depends on.

If what you are about to change is the client or the wire it speaks, read
[Direction](direction/overview.md) first. It is the only forward-looking part of this book, and it
exists because the six things most often asked of the client all land on the same missing eight
bytes of frame header.

The single most informative file in the repository is
`shoal-derive/src/traits/db.rs`, which generates the `ShoalDatabase` impl that dispatches
every query to its table.

[rkyv]: https://rkyv.org/
[glommio]: https://github.com/DataDog/glommio
