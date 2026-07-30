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

- **Not distributed.** `ShardContact` has exactly one variant, `Local(usize)`
  (`shoal-core/src/server/shard.rs:169`). All routing is to shards on the current process.
  There is no node discovery, no inter-node transport, and no cluster membership.
- **No replication.** Every partition lives on exactly one shard, in one copy, on one disk.
- **No transactions.** There is no atomicity across queries, no isolation between them, and
  no rollback. A bundle of queries is a batch, not a transaction.
- **No secondary indexes and no range scans.** The only access path is by partition key.
  Sort keys exist in the storage layer but are not yet usable as a query predicate
  ([Query Execution](tables/query-execution.md)).
- **No rebalancing.** Shard count is baked into the on-disk file layout. Changing it between
  restarts strands data ([Partitioning and the Ring](architecture/partitioning.md)).

Shoal is best understood as a fast single-node partitioned key-value store with a
persistence layer, on top of which distribution has not yet been built.

## A note on the current branch

This book was written against the `ZeroCopyResponses` branch, during a rewrite of the intent
log writer. That rewrite is now finished: acknowledgement waits for an `fdatasync` covering
the record, the flush watermark only advances over contiguously completed writes, and every
write to the log is block aligned. See [Durability model](storage/overview.md#durability-model)
and [Intent Log](storage/intent-log.md).

Other parts of the branch are still rough. Read [Known Issues](appendix/known-issues.md)
before drawing conclusions about anything else — the most serious open defect is that
compaction can resurrect deleted rows
([#5](appendix/known-issues.md#5-pruned-partitions-leak-a-stale-archive-map-entry)).

The workspace compiles — `cargo check --workspace --all-targets` passes with warnings only,
and `cargo test --workspace` passes with 14 integration tests and 32 unit tests.

## Crate map

| Crate | Role |
| --- | --- |
| `shoal-core` | Everything of substance: server, shards, storage engines, client, shared traits and query types. |
| `shoal-derive` | Proc macros. `#[derive(ShoalSortedTable)]`, `#[derive(ShoalUnsortedTable)]`, and the `#[db]` attribute that generates a database's dispatch layer. |
| `shoal` | The user-facing façade. Re-exports `shoal-core` and `shoal-derive` under one name, plus a benchmarking harness (`bencher`). |
| `shoalctl` | A terminal UI, generic over a compiled-in schema. |

Inside `shoal-core` the split is:

| Module | Role |
| --- | --- |
| `shared/` | Types on both sides of the wire: queries, responses, and the core traits. |
| `server/` | Shard lifecycle, the ring, inter-shard messaging, tables, and storage. |
| `client/` | The async client, connection pool, and result streams. |

## Suggested reading order

If you are new to the codebase, read in this order:

1. [Overview](architecture/overview.md) — how the pieces fit together.
2. [Request Lifecycle](architecture/request-lifecycle.md) — one query, end to end. This is
   the page that makes the rest legible.
3. [Storage Overview](storage/overview.md) — the three on-disk structures.
4. [Derive Macros](api/derive-macros.md) — where all the generated code comes from, since
   roughly half of what runs at query time does not appear in the repo as source.
5. [Known Issues](appendix/known-issues.md) — before you trust anything.

The single most informative file in the repository is
`shoal-derive/src/traits/db.rs`, which generates the `ShoalDatabase` impl that dispatches
every query to its table.

[rkyv]: https://rkyv.org/
[glommio]: https://github.com/DataDog/glommio
