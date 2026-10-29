# X20. ScyllaDB's scan path, at the source

**Read 2026-10-29.** This is the record of spike [X20](spikes.md#x20-scylladbs-scan-path-at-the-source),
optional: no gate waits on it. It read ScyllaDB's source at the newest release that day,
`scylla-2026.3.4`, where [W3](prior-art.md#scylladb) had read the repository's default branch or
recalled. The run beside a foreground is [below](#what-was-run).

Of W3's twenty claims of ScyllaDB's source and documents, **fourteen held as written, four held with
something W3 left out, and two were wrong in part**: a read is admitted while up to two others need
only the cpu, not none, and a saved reader is evicted when its time runs out, not only to let a
waiting read in. **Neither changes the design**: Shoal's scan admission is a count of pages and a
budget of bytes, never a reader's cpu ([F116](../../features/scan-admission.md),
[F118](../../features/scan-budget.md)), and Shoal saves no reader between pages, which is
[X17](spikes.md#x17-a-page-rebuilt)'s question to answer. The predictions written before the
reading ([x20-predictions.md](../../../../shoal-spike/results/x20-predictions.md)) got thirteen of
nineteen right; four of the six it got wrong expected something to have moved since W3's reading
that had not.

## The question

[W3](prior-art.md) says what ScyllaDB does with a scan, and the scans plan copies five of its
mechanisms: a scan in hash order, a tablet at a time routed to its owning shard, pages bounded in
rows and bytes with short pages that are not the end, admission by count and memory by class, and
a scheduling group of low shares. W3 read them on 2026-10-25 from the repository's default branch
and from ScyllaDB's posts, and marked what it had not read as recalled. The spike's question is
whether those readings are right at a pinned release, and what ScyllaDB's own tablet-era full scan
costs its foreground on the lab.

**What would have changed the design**, named on the spike page before the reading: a borrowed
mechanism read differently at the source than in the documents, the saved readers' memory bound or
the semaphore's admission most likely, changing [W5](tablet-reader.md)'s or [W7](isolation.md)'s
version of it.

## How it was judged

[X14](../../object-storage/ceph-and-s3-sources.md)'s method: a release pinned by its commit, every
claim read at a path and a line, predictions written first. Four labels:

- **Confirmed**: the source says what W3 says.
- **Confirmed, with more**: it does, and something W3 left out bears on the copy.
- **Corrected**: the source says otherwise, in whole or in part.
- **Still recalled**: what W3 cited is not in the repository (a post, a release note, a driver's
  documentation), so the reading neither confirms nor corrects it.

## What was read

| Source | Pinned at | What |
| --- | --- | --- |
| ScyllaDB | `scylla-2026.3.4`, commit `daef9dad4a013b2310341fd0354f18b330dadfa2`, the newest tag that was not a candidate on 2026-10-29 (2026.4.0 had candidates only) | `docs/dev/` (paging, the semaphore, isolation, tablet routing), `docs/cql/cql-extensions.md`, `db/config.cc`, `service/pager/`, `service/storage_proxy.cc`, `service/mapreduce_service.cc`, `service/tablet_allocator.cc` and its `_fwd.hh`, `replica/` (the querier and its cache, the multishard query, the database's semaphores), `readers/multishard.cc`, `reader_concurrency_semaphore.hh`, `cql3/statements/select_statement.cc` |

The repository was cloned sparse and shallow at the tag into `target/lab/x20/scylladb`, and nothing
was built from it:

```sh
git clone --depth 1 --branch scylla-2026.3.4 --filter=blob:none --sparse \
    https://github.com/scylladb/scylladb target/lab/x20/scylladb
```

## The claims

| W3's claim | At `scylla-2026.3.4` | Label |
| --- | --- | --- |
| A tablet-era scan reads `system.tablets` itself: tablet routing tells a driver nothing for a range scan | "For requests that are not single-partition (e.g. range scans), the driver sends an arbitrary `tablet_version_block`. It will be ignored and no routing information will be returned" (`docs/dev/tablets-routing-v2.md:108`) | Confirmed |
| A page is cut at a built-in, non-optional 1 MB, `query_page_size_in_bytes` | `docs/dev/paged-queries.md:13-15`; `query_page_size_in_bytes`, `1 << 20`, changeable live (`db/config.cc:1134`) | Confirmed |
| A query is exhausted only when a replica returned fewer rows than asked and the read was not short | `_exhausted = (replica_row_count < page_size && !results->is_short_read()) \|\| _max == 0` (`service/pager/query_pagers.cc:422`): or once the query's own row limit is spent | Confirmed, with more |
| `query_tombstone_page_limit` cuts a page after 10,000 tombstones | `db/config.cc:1132`, 10,000 | Confirmed |
| The paging state carries the last partition and clustering key, the rows left, the query's id and the last replicas | `service/pager/paging_state.hh:31-41`: those, the rows left in two 32-bit halves, the last replicas by token range, and besides them the read repair decision, the rows fetched of the last partition, the clustering key's bound weight and the partition region the read stopped in | Confirmed, with more |
| Saved readers are a cache a shard, keyed by the query's id; a lookup removes its entry; an entry lives ten seconds; a saved reader is an inactive read its semaphore may evict | The cache is a member of each shard's database (`replica/database.hh:1782`), an `unordered_multimap` by query id (`replica/querier.hh:323`); a lookup must match the read's ranges as well as its id, and erases what it finds (`replica/querier.cc:192-213`); ten seconds (`querier.cc:190`), set in code (`database.hh:2210`), not a configuration option; each entry registered as an inactive read with that time to live (`querier.cc:261`, `:299`) | Confirmed, with more |
| User reads hold 100 reads and 2% of a shard's memory; system and maintenance reads 10 each | `replica/database.hh:1689-1699`: 100 and 2% for statements, 10 and 2% for streaming (maintenance), 10 and 2% for system, and a queue of 2% of memory over 1,000 (`:1694`); `docs/dev/reader-concurrency-semaphore.md:26-28` says the same | Confirmed, with more: the other two groups hold 2% of memory each too |
| Each read costs one count and 128 KiB | `docs/dev/reader-concurrency-semaphore.md:38` | Confirmed |
| A read is admitted only if no admitted read waits on cpu alone | The document says so (`:39`) and, a few lines up, that a request is not issued while more than "a small constant (by default 2)" need the cpu (`:17-19`). The code refuses admission once the admitted reads needing only the cpu reach `reader_concurrency_semaphore_cpu_concurrency` (`reader_concurrency_semaphore.cc:1458-1459`, `:1542-1543`), 2 by default (`db/config.cc:1561`), 1 for view updates (`:1573`) | **Corrected in part**: up to two may wait on the cpu |
| Reads are serialized past twice the memory limit and killed past four times | `reader_concurrency_semaphore_serialize_limit_multiplier`, 2, and `_kill_limit_multiplier`, 4, both changeable live (`db/config.cc:1557`, `:1559`) | Confirmed |
| An inactive read is evicted only to let a waiting one in | The document says so (`:55`); the semaphore evicts one for three reasons, a permit's shortage, its time to live expiring, and by hand (`reader_concurrency_semaphore.hh:179-183`), as for a table that goes away (`evict_inactive_reads_for_table`, `:524`) | **Corrected in part**: also when its time runs out |
| A replica's multishard read-ahead starts at one shard and doubles at a shard with nothing buffered, up to the shard count | `_concurrency = 1` (`readers/multishard.cc:1022`); doubled, capped at the shard count, when crossing to a shard whose reader has an empty buffer (`:1110-1114`) | Confirmed |
| A coordinator reads one vnode range in its first round and doubles each round while rows remain, not kept across pages | A factor of 1 for each call (`service/storage_proxy.cc:6708`), doubled while rows and partitions remain (`:6687`), a local of the call, so a page starts again at 1; the rows-a-range estimate beside it is now always 0 (`:6707`) | Confirmed |
| With tablets the multishard reader is skipped: each piece is read on its owning shard in turn | The multishard reader refuses a table whose sharding is not by vnode (`replica/multishard_query.cc:220-225`); `do_query_tablets` splits the ranges by tablet and reads each on its shard, one after another, stopping at a short read or a spent limit (`:802-835`) | Confirmed |
| `BYPASS CACHE` neither uses nor fills the row cache, mostly for range scans, since 3.1 | `docs/cql/cql-extensions.md:6-14`; the version is the post's | Confirmed; the version still recalled |
| CPU shares start at 1,000 for statements and compaction, 200 for streaming, with controllers moving them, and scheduling groups isolate I/O too | `docs/dev/isolation.md:15-24`: default, memtable, memtable_to_cache, compaction, memory_compaction, statement and gossip at 1,000, streaming at 200; controllers move them (`:28`); "scheduling groups (for CPU-time and IO isolation)" (`:8`) | Confirmed, with more: five more groups at 1,000 |
| A range scan's timeout is 10 s beside a read's 5 s; `USING TIMEOUT` overrides either, since 4.4 | `db/config.cc:1143`, `:1145`; `docs/cql/cql-extensions.md:70-81` | Confirmed; the version still recalled |
| `ALLOW FILTERING` filters on the coordinator, after the rows the key restrictions name are fetched | The filtering pager fetches a page from the replicas and filters its rows as it builds the result (`restrictions_filter`, `service/pager/query_pagers.cc:270-280`); a read that filters is counted apart (`cql3/statements/select_statement.cc:443`) | Confirmed |
| Aggregates run where the rows are, through a super-coordinator that splits the ranges by owner; 68 s down to 2 s | `service/mapreduce_service.cc:266-276`, its `retrying_dispatcher` the super-coordinator; the figure is the release notes' | Confirmed; the figure still recalled |
| Tablets split and merge at twice and half a target, 5 GB by default | `default_target_tablet_size`, 5 GiB (`service/tablet_allocator_fwd.hh:16`); twice and half it (`service/tablet_allocator.cc:847-853`) | Confirmed |

**Still recalled**, as W3 left them: the python driver's 5,000 rows a page, 1.6's and the 2017 posts'
advice and figures, 3.0's 30% and 73%, 2019's 5 to 10%, and 2025's "two in flight" advice. None of
them is in the repository.

## What it changes

**Nothing in the design.** The two corrections are both in the semaphore, and neither is a
mechanism the plan copies as ScyllaDB built it:

- **Admission by cpu need.** ScyllaDB admits a read while at most two admitted reads need only the
  cpu, so a shard's cpu is never idle behind a read waiting on its disk. Shoal's scan class admits
  a count of pages a shard and draws their bytes from a budget ([F116](../../features/scan-admission.md),
  [F118](../../features/scan-budget.md)), and its pages yield in steps in a queue of their own
  ([F117](../../features/scan-class.md)). A page waiting on its device does not hold its core, so
  the count is what bounds the work in flight, not the cpu.
- **Saved readers' life.** A saved reader goes when its ten seconds are up as well as when a waiting
  read needs its room. Shoal saves no reader between pages; whether a saved position would save a
  third of a page is [X17](spikes.md#x17-a-page-rebuilt)'s question, and if it does, a time to live
  beside eviction under pressure is ScyllaDB's shape to take.

W3's table of what Shoal takes stands as it is.

## What the predictions got wrong

[x20-predictions.md](../../../../shoal-spike/results/x20-predictions.md) was written before any file
was read, and was not edited after. Six of its nineteen were wrong:

- **Four expected a change that had not happened**: the semaphore's counts and memory share, which
  are W3's; `ALLOW FILTERING` pushed to the replicas, where it is the coordinator's; the tablet
  target at 10 GB, where it is 5 GiB; and the querier cache's time to live a configuration option,
  where it is set in code.
- **One confirmed what W3 had wrong**: that no admitted read may wait on the cpu, where two may.
- **One placed a detail wrongly**: the coordinator's range concurrency a member of its read
  executor, where it is a local of each call.

It was right that an inactive read is evicted on its time to live too, about the paging state's
fields beyond W3's, the scheduling groups beyond the four W3 named, both multipliers being
settings, and the aggregates' service under a new name. A reading of a default branch four days
before the tag held up better than a guess at what changes between releases.

## What was run

### The post's scanner is a simulation

The spike page named the 2025 post's tablet scanner, `fee-mendes/tablet-fullscans` (commit
`7a95c8a`), to run beside the foreground. Read before running it, as the plan asked: it scans no
cluster. It builds a random tablet map of its own (`full_scan_tablets.py:375`), schedules each
tablet's sub-ranges with at most `--inflight` requests a replica shard, two by default
(`ShardScheduler`, `:169-209`), and for each request sleeps a random tenth of a second
(`:288-291`, "Simulate processing"); it imports no driver. It is the post's scheduling, shown, and
it bears out the post's "two in flight a replica shard". So the run drives a scanner of the same
shape written for it, `shoal-spike/results/x20/x20_client.py`: the table's tablets read from
`system.tablets`, each tablet's token range one request, given to one of its replicas in turn and
sent to that host with a key inside the tablet as its routing key, two in flight a replica shard,
`BYPASS CACHE`, pages of a thousand rows.

### The run

Prepared and not yet taken: three ScyllaDB nodes of the release's own image on X16's hosts and cpus,
X16's cell from a Python client of ScyllaDB's own driver, and the scanner above, by
[x20-lab.sh](../../../../shoal-spike/results/x20-lab.sh). Its figures come with its own change.

## Related

[W3](prior-art.md) for the claims; [X14](../../object-storage/ceph-and-s3-sources.md) for the method;
[X17](spikes.md#x17-a-page-rebuilt) for saved positions; [W7](isolation.md) for Shoal's admission.
