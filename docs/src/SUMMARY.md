# Summary

[Introduction](introduction.md)

# Getting Started

- [Building Shoal](getting-started/building.md)
- [Configuration](getting-started/configuration.md)

# Architecture

- [Overview](architecture/overview.md)
- [Thread per Core](architecture/thread-per-core.md)
- [Partitioning and the Tablet Map](architecture/partitioning.md)
- [Request Lifecycle](architecture/request-lifecycle.md)
- [Wire Protocol](architecture/wire-protocol.md)

# Storage

- [Storage Overview](storage/overview.md)
- [The Intent Log](storage/intent-log.md)
- [Archives and the Archive Map](storage/archives-and-map.md)
- [Compaction](storage/compaction.md)
- [Recovery](storage/recovery.md)

# Tables

- [Table Types](tables/table-types.md)
- [Partitions](tables/partitions.md)
- [Query Execution](tables/query-execution.md)
- [Memory and Eviction](tables/memory-and-eviction.md)

# API

- [Derive Macros](api/derive-macros.md)
- [The Client](api/client.md)
- [SHQL](api/shql.md)

# Operations

- [Observability](operations/observability.md)
- [shoalctl](operations/shoalctl.md)
- [Tuning](operations/tuning.md)

# Performance

- [Performance](performance/overview.md)
  - [Read/write mixtures](performance/grid.md)
  - [Row size](performance/row-size.md)
  - [Table types and what storage costs](performance/table-types.md)
  - [Access patterns and load depth](performance/access-patterns.md)
  - [Transport and encryption](performance/transport.md)
  - [Reading many partitions at once](performance/fanout.md)
  - [Configuration and what each setting is worth](performance/configuration.md)
  - [The micro layer](performance/micro.md)
  - [Where the time goes](performance/attribution.md)
  - [Every workload](performance/all-workloads.md)
- [Benchmarking](performance/benchmarking.md)
- [Performance Baseline](performance/baseline.md)

# Features

- [Delivered Features](features/delivered-features.md)
  - [F1. Sort-key range predicates](features/sort-key-ranges.md)
  - [F2. Projections on get queries](features/projections.md)
  - [F3. A three layer performance harness](features/performance-harness.md)
  - [F4. Archives are validated once, not once per read](features/validated-archives.md)
  - [F5. The flushed sweep runs on a wakeup, not on every message](features/flushed-sweep-gate.md)
  - [F6. A per query stage breakdown](features/stage-breakdown.md)
  - [F7. A benchmark runner that renders its own results](features/bench-runner.md)
  - [F8. Purpose-built workloads, in the crate that judges them](features/purpose-built-workloads.md)
  - [F9. Ephemeral tables, and the benchmarks that need them](features/ephemeral-tables.md)
  - [F10. Framing and protocol evolution](features/framing-and-protocol-evolution.md)
  - [F11. The error channel](features/error-channel.md)
  - [F12. Authentication](features/authentication.md)
  - [F13. The transport workloads](features/transport-workloads.md)
  - [F14. Encryption in transit](features/encryption-in-transit.md)
  - [F15. The client is a crate that cannot start a database](features/client-server-split.md)
  - [F16. The client builder](features/client-builder.md)
  - [F17. The workload grid](features/workload-grid.md)
  - [F18. Results pages that explain themselves](features/results-pages.md)
  - [F19. Charts that name their colours in one place](features/chart-legends.md)
  - [F20. What each setting is worth](features/configuration-sweeps.md)
  - [F21. Benchmark groups](features/benchmark-groups.md)

# Direction

- [Overview](direction/overview.md)
  - [D1. The transport](direction/transport.md)
  - [D2. Framing and protocol evolution](direction/framing.md)
  - [D3. Authentication](direction/authentication.md)
  - [D4. Encryption in transit](direction/encryption.md)
  - [D5. Runtime portability](direction/runtimes.md)
  - [D6. A production connection pool](direction/connection-pool.md)
  - [D7. Shard-aware routing](direction/shard-aware-routing.md)
  - [D8. Compile-time guarantees](direction/typed-queries.md)
  - [D9. Lessons from other databases](direction/prior-art.md)

# Appendix

- [Known Issues](appendix/known-issues.md)
- [Optimizations](appendix/optimizations.md)
- [Test Coverage](appendix/test-coverage.md)
- [Resolved Issues](appendix/resolved-issues.md)
  - [1-3. Acknowledged writes were not durable](appendix/resolved/durability.md)
  - [4. Unsorted updates and deletes never consult disk](appendix/resolved/unsorted-disk-consultation.md)
  - [5. Deleted rows came back](appendix/resolved/resurrected-deletes.md)
  - [6. Memory accounting collapsed to zero on a partition load](appendix/resolved/memory-accounting.md)
  - [7, 10. `limit` was ignored by persistent sorted tables](appendix/resolved/sorted-limit.md)
  - [8. Sort keys were accepted and ignored](appendix/resolved/sort-keys.md)
  - [9. Orphaned update intents panicked, then were dropped silently](appendix/resolved/orphaned-update-intents.md)
  - [11, 12, 37. The ring panicked on an empty lookup and never smoothed load](appendix/resolved/tablet-ring.md)
  - [13. Eviction logging can underflow](appendix/resolved/eviction-log-underflow.md)
  - [14. Empty rotated intent logs were never deleted](appendix/resolved/empty-rotated-logs.md)
  - [16, 51. A partition read that failed panicked its shard and stranded its queries](appendix/resolved/partition-load-failure.md)
  - [18, 50. Core exclusion was ignored and shard placement was random](appendix/resolved/excluded-cores-typo.md)
  - [20. The storage tests were entirely commented out](appendix/resolved/storage-tests.md)
  - [24. A bad query could leave the terminal in raw mode](appendix/resolved/shoalctl-panic.md)
  - [25. `CLAUDE.md` described a Shoal that no longer existed](appendix/resolved/claude-md-drift.md)
  - [26, 39. A multi-partition get answered in an arbitrary order](appendix/resolved/partition-order.md)
  - [31. Multi-log recovery discarded already-replayed intents](appendix/resolved/multi-log-recovery.md)
  - [34. The request length prefix is unvalidated](appendix/resolved/unvalidated-length-prefix.md)
  - [44. A compaction discarded a damaged log's tail in silence](appendix/resolved/compaction-tail-loss.md)
  - [45. The storage marker's format field was written and never read](appendix/resolved/storage-marker-format.md)
  - [48. A query that does not parse was answered with silence](appendix/resolved/query-error-display.md)
  - [57. A missing archive was created empty rather than reported](appendix/resolved/missing-archive.md)
  - [56, 61. A response cannot say that a read failed](appendix/resolved/response-error-channel.md)
  - [54. `#[shoal::db]` needed crates the caller had never heard of](appendix/resolved/macro-emits-three-crates.md)
  - [67, 68. Chart labels collided, and the scope prefix strip never matched](appendix/resolved/chart-labels.md)
- [TODOs and Unbuilt Work](appendix/todos.md)
- [Review, August 2026](appendix/review-2026-08.md)
- [Glossary](appendix/glossary.md)
