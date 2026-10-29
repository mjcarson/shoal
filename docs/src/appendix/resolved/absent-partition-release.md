# 318. A query parked on a partition that does not exist logged a WARN that the partition could not be read

A table asks its archive map what it can say about a partition before it reads anything. Since
[F76](../../features/paged-archive-map.md) paged the map, the answer can be that a run may hold the
partition and only a read of the run's page can say. The query then parks while the shard's loader
reads that page. When the page does not hold the partition, the loader reports it absent and the
table releases the parked queries to answer as an empty partition does. Nothing failed: the
partition was never written, or a compaction pruned it.

The loader logged that read at DEBUG, on purpose. The table logged the same release at WARN, as a
partition that "could not be read", whatever the release carried.

Found reading the journals of the [store comparison](../../object-storage/store-comparison.md)'s
second run, and reproduced by a test.

## Symptom

```text
WARN shoal_core::server::tables::persistent::unsorted: msg="Releasing queries parked on a partition that could not be read" table=PostersObjectMeta partition_id=10731079895591866599 released=1
```

The comparison's loop leg logged it 63,874 times in three hours, all on the bucket's
`PostersObjectMeta` table, and none with a failure behind it. 36,097 came in the four captures' 4 KiB
puts at sixteen in flight and 25,710 in their 4 KiB half-and-half arms at sixteen. Its disk leg logged
it 324 times. Every operation of those runs was answered, and nothing was lost.

A put of a new object reads its path's row first, so every new path is a lookup of a partition never
written. Each run of the map has a filter that rules out about ninety-nine such keys in a hundred; a
key a filter cannot rule out parks on a page read. On the loop leg about one 4 KiB put in 130 took
this path: 36,097 releases beside 4,602,035 measured puts at sixteen in flight, and 25,710 beside
3,579,759 in the half-and-half arms.

## Cause

`fail_partition` (`tables/persistent/unsorted.rs` and `sorted.rs`) releases the queries parked on a
read that did not produce a partition. It is called with an error for a read that failed, and with
none for a partition no archive holds (`client_error` answers `None` for `PartitionNotFound`). It
logged one WARN before it looked at either:

```rust
event!(
    Level::WARN,
    msg = "Releasing queries parked on a partition that could not be read",
    ...
);
```

The loader had made the same distinction for the same read since
[Resolved #16](partition-load-failure.md): a pruned partition at DEBUG, "expected on any workload
that deletes", and a read it gave up on at ERROR. The release kept one level for both. Before the map
was paged, only a compaction pruning a partition between a probe and its read reached it, and that
was rare. Since F76 every lookup a run's filter cannot rule out, on a page the map's cache does not
hold, reaches it.

## Evidence

**Reproduced by `a_query_released_by_an_absent_partition_logs_no_warning`**
(`shoal/tests/absent_partition_logs.rs`), written before the fix. A server whose archive map has a
tiny delta, no page cache and filters that rule nothing out (`filter_bits(0)`) holds rows in a sorted
and an unsorted table, and is started again so nothing is resident. Forty gets of keys never written
are each answered empty. A layer over the process's events counts the loader's absent reads and the
releases logged at WARN, and the test requires at least one absent read, so it cannot pass without
taking the path.

On the tree before the fix:

```text
thread 'a_query_released_by_an_absent_partition_logs_no_warning' panicked at shoal/tests/absent_partition_logs.rs:230:5:
assertion `left == right` failed: 74 reads found their partition was not there, and their releases were logged as failed reads
  left: 74
 right: 0
```

Every get was answered empty, as it should be. Only the level was wrong.

## The fix

**A release logs at WARN only when it carries an error.** Both tables' `fail_partition` log a release
with an error at WARN, with the error's code, and one without at DEBUG as "Releasing queries parked on
a partition no archive holds", beside the loader's own DEBUG for the read. What the released queries
answer did not change.

## Alternatives rejected

- **Not logging the release at all.** A release is the end of a parked query's wait, and at DEBUG it
  still ties the parked queries to the read that let them go, as `link_released` does in a trace.
- **Throttling the WARN.** It would keep calling a partition never written a failed read, only less
  often.
- **Keeping the page of every run cached.** It is what `page_cache_bytes` is for, and a cache never
  holds every page of a map larger than it.

## Invariants to uphold

- **A WARN about a partition read is a read that failed.** A partition no archive holds is no failure,
  at the loader or at the table.
- **Both persistent tables release parked queries the same way.** They park and release through code
  of their own, so a change to one is made to the other, as here.
- **A released query answers with the release's error, and with none for a partition no archive
  holds**, which is what makes it empty rather than failed.

## Still open

- **A lookup a filter cannot rule out still costs a page read**, about one in 130 new keys on the
  loop leg. That is F76's design, not this defect.

## Tests

| Test | What fails if the fix is reverted |
| --- | --- |
| `a_query_released_by_an_absent_partition_logs_no_warning` (`shoal/tests/absent_partition_logs.rs`) | Gets of partitions never written, each read off a page and found absent, log their releases at WARN as failed reads |
| `a_get_whose_partition_cannot_be_read_does_not_hang` (both `persistent_*_table.rs`) | A release with an error stops answering with it (unchanged by this fix) |

## Related

- [Resolved #16, 51](partition-load-failure.md): the release path, and the loader's classification
  of a pruned partition as no failure.
- [F76](../../features/paged-archive-map.md): the paged map that made a lookup's absence something a
  page read finds.
- [The store comparison](../../object-storage/store-comparison.md#what-it-found), whose second run's
  journals found it.
