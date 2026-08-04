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
- [Benchmarking](operations/benchmarking.md)
- [shoalctl](operations/shoalctl.md)

# Features

- [Delivered Features](features/delivered-features.md)
  - [F1. Sort-key range predicates](features/sort-key-ranges.md)

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
  - [20. The storage tests were entirely commented out](appendix/resolved/storage-tests.md)
  - [24. A bad query could leave the terminal in raw mode](appendix/resolved/shoalctl-panic.md)
  - [26, 39. A multi-partition get answered in an arbitrary order](appendix/resolved/partition-order.md)
  - [31. Multi-log recovery discarded already-replayed intents](appendix/resolved/multi-log-recovery.md)
  - [44. A compaction discarded a damaged log's tail in silence](appendix/resolved/compaction-tail-loss.md)
  - [45. The storage marker's format field was written and never read](appendix/resolved/storage-marker-format.md)
- [TODOs and Unbuilt Work](appendix/todos.md)
- [Glossary](appendix/glossary.md)
