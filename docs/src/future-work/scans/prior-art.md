# W3. Lessons from ScyllaDB and other stores

## Context

Every part of a scan has been built before, by stores whose shape is close to Shoal's or deliberately
far from it. ScyllaDB is the closest: a shard per core, partitions placed by a hash, rows ordered
inside a partition, and since 6.0 tablets ([D9](../../direction/prior-art.md#scylladb),
[C12](../../distributed/prior-art.md#scylladb)). This page records what each store does with a scan
and why, and says for each what Shoal takes and what it does not. It extends D9 and C12 and does not
repeat them.

Every fact below was read on 2026-10-25 from the page or source file its link names; `GH:` is a path
in [ScyllaDB's repository](https://github.com/scylladb/scylladb) at its default branch that day. A
claim that was not read is marked **recalled**. ~~None of this has been run on the lab; reading
ScyllaDB's scan path at a pinned release, and running one beside a foreground, is
[X20](spikes.md#x20-scylladbs-scan-path-at-the-source)'s, optional.~~ Since
[X20](scylladb-scan-sources.md) (2026-10-29) every claim of ScyllaDB's source and documents here has
been read again at `scylla-2026.3.4` (`daef9dad`): fourteen held as written, four held with
something left out, and two were wrong in part, struck and replaced below. X20's page cites each by
file and line; nothing in the table of what Shoal takes moved.

## The systems

### ScyllaDB

**A full scan is the token ring cut into ranges.** Partitions are stored in the order of their tokens,
a Murmur3 hash, so a full scan is `SELECT ... WHERE token(pk) >= ? AND token(pk) <= ?` over sub-ranges
of the ring. ScyllaDB's own advice in 1.6 was to run N = nodes × cores × 3 queries at once over
M = 100 × N sub-ranges, pulled from a queue in random order so every shard starts working
([1.6](https://www.scylladb.com/2017/02/13/efficient-full-table-scans-with-scylla-1-6/)); its worked
example went from 42,110 rows a second serially to 510,752 in parallel on three nodes of eight vCPUs
([parallel](https://www.scylladb.com/2017/03/28/parallel-efficient-full-table-scan-scylla/)). The
order is a hash's, and nobody minds: the same position [W2](ordering.md) takes.

**Since tablets, a scan is a tablet at a time, on the shard that owns it.** The 2025 advice reads
`system.tablets`, scans one tablet at a time with a routing key so the request reaches the replica
shard that holds it, keeps each replica shard "busy with at least 2 inflight requests", and "highly
recommends" `BYPASS CACHE` ([tablets](https://www.scylladb.com/2025/05/13/efficient-full-table-scans-with-scylladb-tablets/)).
Tablet routing tells a driver nothing for a range scan, so a scanner reads the tablets itself
(`GH:docs/dev/tablets-routing-v2.md`). This is the shape [W6](distribution.md) takes, with Shoal's
route table in place of `system.tablets`.

**A page is bounded in rows and in bytes, and a short page is never the end.** Drivers default to
5,000 rows a page ([python driver](https://python-driver.docs.scylladb.com/stable/query-paging.html));
the server cuts a page at a "built-in (non-optional) size limit of 1MB" (`GH:docs/dev/paged-queries.md`),
`query_page_size_in_bytes` today (`GH:db/config.cc`). A page that reached the byte limit first is
short, and pages can be empty; a query is exhausted only when a replica returned fewer rows than asked
and the read was not short (`GH:service/pager/query_pagers.cc`), so a client must go on until the
paging state is null. A driver that stopped at empty filtered pages was a bug
([issue 8203](https://github.com/scylladb/scylla/issues/8203)). `query_tombstone_page_limit`, 10,000,
cuts a page after that many tombstones even when it is empty, since 5.2
([5.2](https://www.scylladb.com/2023/05/04/scylladb-open-source-5-2-with-raft-based-schema-management/)).
The paging state carries the last partition and clustering key, the rows remaining, the query's id and
the last replicas, so the next page goes to the same ones (`GH:service/pager/paging_state.hh`).

**Readers are saved between pages, as a cache.** Since 2.2 a query's state is kept between pages, and
since 3.0 a range scan's shard readers are saved when a page fills
([2.2](https://www.scylladb.com/2018/07/13/efficient-query-paging/),
[3.0](https://www.scylladb.com/2018/11/01/more-efficient-range-scan-paging-with-scylla-3-0/)). The
cache is per shard, keyed by the query's id from the paging state; a lookup removes the entry, an
entry lives ten seconds, and a saved reader is registered with the admission semaphore as an inactive
read it may evict (`GH:replica/querier.hh`, `GH:replica/querier.cc`). On a miss a new reader is made.
3.0 measured 30% more range-scan throughput, and up to 39% fewer bytes and 73% fewer disk operations
for a read.

**Admission is a semaphore a scheduling group a shard.** User reads may hold 100 reads and 2% of a
shard's memory; system and maintenance reads 10 each, and 2% of memory each too (X20). Each read costs one
count and 128 KiB; a read is admitted only if resources are free and ~~no admitted read waits on cpu
alone~~ fewer than two admitted reads wait on the cpu alone (`reader_concurrency_semaphore_cpu_concurrency`,
X20); reads are serialized past twice the memory limit and killed past four times; an inactive read
is evicted ~~only~~ to let a waiting one in, and when its ten seconds run out (X20)
(`GH:docs/dev/reader-concurrency-semaphore.md`).

**Concurrency grows when results are thin.** A replica reads a range across its shards with read-ahead
that starts at one shard and doubles when it reaches a shard with nothing buffered, up to the shard
count (`GH:readers/multishard.cc`); a coordinator reads one vnode range in its first round and doubles
the ranges each round while rows remain (`GH:service/storage_proxy.cc`). Neither is kept across pages.
With tablets the multishard reader is skipped: each piece is read on its owning shard in turn
(`GH:replica/multishard_query.cc`).

**`BYPASS CACHE`** makes a read neither use nor fill the row cache, "mostly useful for range scans
which typically process large amounts of data with no temporal locality", since OSS 3.1
([CQL extensions](https://docs.scylladb.com/manual/master/cql/cql-extensions.md)).

**Isolation is by scheduling group.** CPU shares start at 1,000 for statements and compaction and 200
for streaming, "a.k.a. maintenance", with controllers moving them at runtime, and scheduling groups
isolate I/O as well as cpu (`GH:docs/dev/isolation.md`). Workload prioritization gives service levels
shares of their own: an OLTP load at 1,000 shares beside an OLAP load at 10 lost 5 to 10% of its
throughput ([2019](https://www.scylladb.com/2019/05/23/workload-prioritization-running-oltp-and-olap-traffic-on-the-same-superhighway/)),
an Enterprise feature until 2025.1 made every feature source-available
([FAQ](https://www.scylladb.com/source-available-faq)).

**A range scan has its own timeout**, `range_request_timeout_in_ms`, 10 s beside a read's 5 s
(`GH:db/config.cc`), and `USING TIMEOUT` overrides either since 4.4.

**`ALLOW FILTERING` filters on the coordinator**, after the rows the key restrictions name are fetched
([forum](https://forum.scylladb.com/t/when-to-use-filtering-and-when-not/55)), and a query that would
filter must say so.

**Aggregates run where the rows are, since 5.1.** `SELECT COUNT(*)` runs on all nodes and all shards
in parallel, 68 s down to 2 s for about 2×10<sup>8</sup> rows on three nodes of 32 vCPUs
([5.1](https://www.scylladb.com/product/release-notes/scylladb-open-source-5-1)), through a
super-coordinator that splits the ranges by owner ([2023](https://www.scylladb.com/2023/06/20/how-scylladb-distributed-aggregates-reduce-query-execution-time-up-to-20x/)).

**Tablets split and merge by size**, at twice and half a target, 5 GB by default, and are balanced over
nodes and shards ([tablets](https://docs.scylladb.com/manual/stable/architecture/tablets.html)). Shoal's
4096 tablets are fixed, and split by nobody yet ([Partitioning](../../architecture/partitioning.md#why-the-id-comes-from-the-high-bits)).

| From ScyllaDB | Shoal |
| --- | --- |
| A scan in hash order, nobody minding | **Copied** ([W2](ordering.md)) |
| A tablet at a time, routed to its owning shard, two in flight a shard | **Copied**, driven by the client from its route table, a page carrying many tablets of one shard ([W6](distribution.md)) |
| A page bounded in rows and bytes; short and empty pages; the end by state | **Copied**, with a bound on work added so a filter cannot read a tablet whole in one page ([W4](cursor-and-pages.md)) |
| `BYPASS CACHE` | **Copied by construction**: a scan reads through the walk, which makes nothing resident, and there is no clause to forget ([W5](tablet-reader.md#never-resident)) |
| Admission by count and memory, by class | **Copied**: a scan class with a count, a budget and a device ceiling a shard ([W7](isolation.md)) |
| A scheduling group with low shares | **Copied**: a scan queue of 100 shares with a goal ([W7](isolation.md#cpu)) |
| Saved readers between pages | **Not yet**: priced by [X17](spikes.md#x17-a-page-rebuilt) first, and only ever a cache ([W5](tablet-reader.md#saved-positions-optional)) |
| Concurrency that doubles round by round | **Not copied**: a page fills from as many of its shard's tablets as it takes, so nothing needs to grow |
| Filtering on the coordinator | **Not copied**: a scan filters on the shard, where the rows are ([W5](tablet-reader.md)) |
| `ALLOW FILTERING` on `SELECT` | **Not copied as a clause**: a scan is a statement and a call of its own ([W10](api.md)) |
| Parallelized aggregation | **Later**, optional ([milestones](milestones.md#optional-any-time)) |
| Tablets that split by size | **Not now**: a cursor survives a split, which is all a scan asks ([W2](ordering.md#invariants-to-uphold)) |

### Other stores

**Cassandra.** Murmur3 is the default partitioner, and the ordered ones "have been included for
backward compatibility only" ([cassandra.yaml](https://cassandra.apache.org/doc/latest/cassandra/managing/configuration/cass_yaml_file.html)).
`ByteOrderedPartitioner` "allows ordered scans by primary key", and is discouraged: load has to be
balanced by hand, sequential writes make hot spots, and a layout that balances one table skews another
([DataStax](https://docs.datastax.com/en/cassandra/3.x/cassandra/architecture/archPartitionerBOP.html)).
*Shoal*: range placement rejected for the same reasons ([W2](ordering.md#alternatives-rejected)).

**DynamoDB.** A `Scan` call reads at most 1 MB, counted before its filter, and pages by
`LastEvaluatedKey`; capacity is charged on the items read whatever the filter keeps; a parallel scan
names `Segment` and `TotalSegments`, items falling into segments by their partition key's hash
([Scan](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Scan.html)). AWS suggests a
segment for every 2 GB, smaller pages with `Limit`, a shadow table, and backoff, so a scan does not
disturb other traffic ([best practices](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/bp-query-scan.html)).
*Shoal*: a page's work bound counts reads, not results, for the same reason; segments are tablet
ranges ([W10](api.md#the-client)); and a scan's disturbance is the server's to bound, not the caller's.

**Bigtable.** A table is "a sorted key-value map", split into tablets of contiguous rows that split
and move under load ([overview](https://docs.cloud.google.com/bigtable/docs/overview)); lookups,
prefixes and ranges of row keys are efficient and anything else is a full scan, and hashed keys are to
be avoided because they lose ordering ([schema design](https://docs.cloud.google.com/bigtable/docs/schema-design)).
`SampleRowKeys` returns keys that "delimit contiguous sections of the table of approximately equal
size", to split a job ([API](https://docs.cloud.google.com/bigtable/docs/reference/data/rpc/google.bigtable.v2));
salting a key with a hash modulo N keeps a point read one read and makes a scan N parallel scans,
returned unsorted ([salting](https://cloud.google.com/blog/products/databases/cloud-bigtable-schema-optimization-key-salting)).
*Shoal*: the opposite starting point, hash first, and spread is salting by declaration
([W8](declared-order.md#spread)).

**CockroachDB.** A hash-sharded index puts a hidden shard column ahead of a sequential key to spread
its writes, at "a small performance impact on reading sequential data or ranges of data", since a
query scans each bucket and combines them ([docs](https://docs.cockroachlabs.com/docs/stable/hash-sharded-indexes)).
*Shoal*: the same trade as spread, made per table.

**Azure Cosmos DB.** Hierarchical partition keys of up to three levels let a query naming a prefix go
"to only the subset of physical partitions" that hold it, five of a thousand in its example, while a
filter on a middle level alone fans out to all of them ([docs](https://learn.microsoft.com/en-us/azure/cosmos-db/hierarchical-partition-keys)).
*Shoal*: placement fields are the same idea, and the same caveat holds: an order field without its
placement value is not a prefix.

**Spanner** stores rows "in sorted order by primary key", in splits of contiguous rows, co-locates a
child's rows with its parent's by interleaving, and asks for keys that do not increase monotonically
([docs](https://docs.cloud.google.com/spanner/docs/schema-and-data-model)). **FoundationDB**'s data
model "is an ordered key-value store", ordered by raw bytes, and its tuple layer keeps that order so
leading elements serve as prefixes for range reads ([data modeling](https://apple.github.io/foundationdb/data-modeling.html)).
*Shoal*: the order-preserving encodings [W8](declared-order.md#how-a-user-tells-shoal-how-to-step)
writes are the tuple layer's kind.

**Ceph RGW and S3**, as [X14](../../object-storage/ceph-and-s3-sources.md) read them at source: RGW's
bucket index is omap on index shards chosen by a hash of the name, so an ordered listing asks every
shard and merges their answers, and an index entry is written pending before an object's head and
completed after it; S3 lists in UTF-8 byte order, 1,000 keys a page, continued by an opaque token,
with a prefix and a delimiter, and a listing sees a write once it is acknowledged. *Recalled*, not
read: RGW also offers a listing that skips the merge and returns entries unordered, for buckets with
many shards. *Shoal*: an unordered listing first, and an ordered one at a later gate
([W9](bucket-listing.md)).

## Implementation reading list

| Source | What to read it for | Before |
| --- | --- | --- |
| `GH:readers/multishard.cc`, `GH:replica/multishard_query.cc` | How a range is read across shards, and why tablets skip it | [M23](milestones.md#m23-the-walk-rebuilt-for-load) |
| `GH:replica/querier.hh`, `GH:replica/querier.cc` | What a saved reader holds and when it is dropped | A saved position, if X17 asks for one |
| `GH:docs/dev/reader-concurrency-semaphore.md` | Admission by count and memory, and inactive reads | [M23](milestones.md#m23-the-walk-rebuilt-for-load) |
| `GH:service/pager/query_pagers.cc`, `GH:service/pager/paging_state.hh` | When a scan is over, and what a cursor carries | [M24](milestones.md#m24-scans-on-one-node) |
| [Tablet scans, 2025](https://www.scylladb.com/2025/05/13/efficient-full-table-scans-with-scylladb-tablets/) and its code, `github.com/fee-mendes/tablet-fullscans` | A client-side tablet scanner | [M25](milestones.md#m25-scans-across-a-cluster) |

## The comparison

| Store | A full scan's order | Its parts | A page | What keeps it from the foreground | Ordered access |
| --- | --- | --- | --- | --- | --- |
| ScyllaDB | Token (hash) | Token ranges; tablets since 6.0 | Rows and 1 MB; short pages | Scheduling groups, a semaphore, `BYPASS CACHE` | Clustering keys inside a partition |
| Cassandra | Token (hash) | Token ranges | Rows | Thread pools (recalled) | Clustering keys; ordered partitioners, discouraged |
| DynamoDB | Hash of the partition key | Segments | 1 MB before the filter | Capacity, charged to the caller | Sort keys inside a partition |
| Bigtable | Row key | `SampleRowKeys` ranges | A stream of rows (recalled) | Tablet splitting and moving | Row key order, natively |
| CockroachDB | Primary key | Ranges | Rows and bytes (recalled) | Admission control (recalled) | Key order; hash-sharded indexes trade it away |
| **Shoal, proposed** | **Tablet, then hash** | **Tablets, many a page** | **Rows, bytes and work; short pages** | **A scan class: queue, budget, idle device, admission** | **Sort keys inside a partition; a declared order across partitions** |

## Sources

All read 2026-10-25.

- ScyllaDB: [1.6 full scans](https://www.scylladb.com/2017/02/13/efficient-full-table-scans-with-scylla-1-6/),
  [parallel full scans](https://www.scylladb.com/2017/03/28/parallel-efficient-full-table-scan-scylla/),
  [tablet full scans](https://www.scylladb.com/2025/05/13/efficient-full-table-scans-with-scylladb-tablets/),
  [paging in 2.2](https://www.scylladb.com/2018/07/13/efficient-query-paging/),
  [range-scan paging in 3.0](https://www.scylladb.com/2018/11/01/more-efficient-range-scan-paging-with-scylla-3-0/),
  [5.2](https://www.scylladb.com/2023/05/04/scylladb-open-source-5-2-with-raft-based-schema-management/),
  [distributed aggregates](https://www.scylladb.com/2023/06/20/how-scylladb-distributed-aggregates-reduce-query-execution-time-up-to-20x/),
  [workload prioritization](https://www.scylladb.com/2019/05/23/workload-prioritization-running-oltp-and-olap-traffic-on-the-same-superhighway/),
  [5.1 release notes](https://www.scylladb.com/product/release-notes/scylladb-open-source-5-1),
  [source-available FAQ](https://www.scylladb.com/source-available-faq),
  [CQL extensions](https://docs.scylladb.com/manual/master/cql/cql-extensions.md),
  [tablets](https://docs.scylladb.com/manual/stable/architecture/tablets.html),
  [python driver paging](https://python-driver.docs.scylladb.com/stable/query-paging.html),
  [filtering](https://forum.scylladb.com/t/when-to-use-filtering-and-when-not/55),
  [issue 8203](https://github.com/scylladb/scylla/issues/8203); in the repository,
  `docs/dev/paged-queries.md`, `docs/dev/reader-concurrency-semaphore.md`, `docs/dev/isolation.md`,
  `docs/dev/tablets-routing-v2.md`, `db/config.cc`, `service/pager/query_pagers.cc`,
  `service/pager/paging_state.hh`, `service/storage_proxy.cc`, `replica/querier.hh`,
  `replica/querier.cc`, `replica/multishard_query.cc`, `readers/multishard.cc`.
- Cassandra: [cassandra.yaml](https://cassandra.apache.org/doc/latest/cassandra/managing/configuration/cass_yaml_file.html),
  [ByteOrderedPartitioner](https://docs.datastax.com/en/cassandra/3.x/cassandra/architecture/archPartitionerBOP.html).
- DynamoDB: [Scan](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Scan.html),
  [query and scan best practices](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/bp-query-scan.html).
- Bigtable: [overview](https://docs.cloud.google.com/bigtable/docs/overview),
  [schema design](https://docs.cloud.google.com/bigtable/docs/schema-design),
  [data API](https://docs.cloud.google.com/bigtable/docs/reference/data/rpc/google.bigtable.v2),
  [salting](https://cloud.google.com/blog/products/databases/cloud-bigtable-schema-optimization-key-salting).
- [CockroachDB hash-sharded indexes](https://docs.cockroachlabs.com/docs/stable/hash-sharded-indexes);
  [Cosmos DB hierarchical partition keys](https://learn.microsoft.com/en-us/azure/cosmos-db/hierarchical-partition-keys);
  [Spanner schema and data model](https://docs.cloud.google.com/spanner/docs/schema-and-data-model);
  [FoundationDB data modeling](https://apple.github.io/foundationdb/data-modeling.html).

## Related

[D9](../../direction/prior-art.md) and [C12](../../distributed/prior-art.md), which this extends;
[S17](../../object-storage/prior-art.md) and [X14](../../object-storage/ceph-and-s3-sources.md) for Ceph
and S3; [W2](ordering.md) for the order and [W7](isolation.md) for the isolation these lessons shaped.
