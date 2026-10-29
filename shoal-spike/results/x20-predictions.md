# X20's predictions, written before the source was read

Spike X20 ([ScyllaDB's scan path, at the source](../../docs/src/future-work/scans/spikes.md#x20-scylladbs-scan-path-at-the-source))
reads ScyllaDB at a pinned release and checks every claim [W3](../../docs/src/future-work/scans/prior-art.md#scylladb)
made of it, which W3 read on 2026-10-25 at the repository's default branch or recalled. These are what
each claim is expected to look like at the release, written on 2026-10-29 before any of its files was
read, so a reading can be held to what was expected of it, as X14's were
([x14-predictions.md](x14-predictions.md)).

**The release**: `scylla-2026.3.4`, the newest tag that is not a candidate on 2026-10-29, commit
`daef9dad4a013b2310341fd0354f18b330dadfa2`. 2026.4.0 had candidates only.

| W3's claim | Expected at the release | Where it should be read |
| --- | --- | --- |
| A tablet-era scan reads `system.tablets` itself, since tablet routing tells a driver nothing for a range scan | Confirmed: routing is by token, a range scan names no token | `docs/dev/tablets-routing-v2.md` |
| A page is cut at a non-optional 1 MB | Confirmed, the limit a setting, `query_page_size_in_bytes`, 1 MiB | `docs/dev/paged-queries.md`, `db/config.cc` |
| A query is exhausted only when a replica returned fewer rows than asked and the read was not short | Confirmed, in the pager's handling of a page's result | `service/pager/query_pagers.cc` |
| `query_tombstone_page_limit` cuts a page after 10,000 tombstones | Confirmed, 10,000 | `db/config.cc` |
| The paging state carries the last partition and clustering key, the rows left, the query's id and the last replicas | Confirmed, with fields W3 did not list (a per-partition row count, a tracing id) | `service/pager/paging_state.hh` |
| Saved readers: a cache a shard, keyed by the query's id, a lookup removing its entry, ten seconds a life, each saved reader an inactive read its semaphore may evict | Confirmed; the ten seconds a setting (`querier_cache_entry_ttl`?) | `replica/querier.hh`, `replica/querier.cc`, `db/config.cc` |
| A semaphore a scheduling group a shard: user reads 100 and 2% of memory, system and maintenance 10 each; a read 1 count and 128 KiB | Corrected in part: the counts and the memory share moved since the document was written, the 128 KiB kept | `docs/dev/reader-concurrency-semaphore.md`, `replica/database.cc` |
| A read is admitted only with resources free and no admitted read waiting on cpu alone | Confirmed | `reader_concurrency_semaphore.cc`, the document |
| Reads are serialized past twice the memory limit and killed past four times | Confirmed as settings, a multiplier each | `db/config.cc` (`reader_concurrency_semaphore_serialize_limit_multiplier`, `_kill_limit_multiplier`) |
| An inactive read is evicted only to let a waiting one in | Corrected in part: also on its ttl and under memory pressure | the semaphore's source |
| The multishard reader's read-ahead starts at one shard and doubles at a shard with nothing buffered, up to the shard count | Confirmed | `readers/multishard.cc` |
| A coordinator reads one vnode range in its first round and doubles each round while rows remain, not kept across pages | Confirmed, the concurrency a member of the read executor | `service/storage_proxy.cc` |
| With tablets the multishard reader is skipped, each piece read on its owning shard in turn | Confirmed | `replica/multishard_query.cc` |
| `BYPASS CACHE` neither uses nor fills the row cache, since 3.1 | Confirmed | `docs/cql/cql-extensions.md` |
| CPU shares 1,000 for statements and compaction and 200 for streaming, with controllers moving them, and scheduling groups isolate I/O too | Confirmed in shape, the shares a few groups more than W3 named (`gossip`, `memtable`, `memtable_to_cache`) | `docs/dev/isolation.md`, `main.cc` |
| `range_request_timeout_in_ms` 10 s beside a read's 5 s; `USING TIMEOUT` overrides either | Confirmed | `db/config.cc`, `docs/cql` |
| `ALLOW FILTERING` filters on the coordinator, after the rows the key restrictions name are fetched | Corrected: the filter is pushed to the replicas, which return only the rows it keeps | `cql3/statements/select_statement.cc`, `query/` |
| Aggregates run where the rows are, through a super-coordinator that splits ranges by owner | Confirmed, under a name moved since (`mapreduce_service`) | `service/mapreduce_service.cc` |
| Tablets split and merge at twice and half a target, 5 GB by default | Corrected: the target moved, 10 GB | `db/config.cc`, `docs/dev/` or the architecture page |

## What the run is expected to show

X20's run, optional, is three ScyllaDB nodes on the lab, a stress load at a fixed rate on one table
and the 2025 post's tablet scanner reading another beside it. **Expected**: the foreground's read p99
rises by less than 1.5 times during the scan with the scanner at two pages in flight a shard and
`BYPASS CACHE`, more without it, which would put ScyllaDB near W7's budget but not inside it.
