# X20. ScyllaDB's scan path, at the source

**Read and run 2026-10-29.** This is the record of spike [X20](spikes.md#x20-scylladbs-scan-path-at-the-source),
optional: no gate waits on it. It read ScyllaDB's source at the newest release that day,
`scylla-2026.3.4`, where [W3](prior-art.md#scylladb) had read the repository's default branch or
recalled, and ran that release on the lab beside [X16](walk-beside-the-foreground.md)'s foreground
([below](#what-was-run)).

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

**The run** put three nodes of that release on X16's hosts and cpus, and X16's cell beside a full scan
of a table of three million kibibyte rows, scanned as ScyllaDB's 2025 advice has it: a tablet a
request, routed to a replica's shard, two in flight a replica shard, `BYPASS CACHE`. Four rounds with
the scan carrying its keys, as X16's carried keys and counts, and two carrying every row:

- **ScyllaDB's scan read the table 40 to 52 times as fast as Shoal's walk beside the same cell**: 3.1
  GB in 2.15 s, 1.4 million rows a second, about 460 MiB a second a node off the same devices, where
  X16's walk took 86 to 112 s. It reads a tablet in its sstable's order, where the walk reads a page's
  archived partitions one at a time~~: the lever X16 named, [X17](spikes.md#x17-a-page-rebuilt)'s runs
  in disk order~~. [W42](scan-speed.md) breaks the gap down: a synced write's flushes on the scan's
  device, the members' shares, one read in flight a page and the cpu a partition; runs come last, once
  merges lay a tablet's records together.
- **Unpaced, it did not hold [W7's budget](isolation.md#the-budget) either**: while it ran, the cell's
  read p99 rose 2.34 times and its insert p99 2.15 times, and its rate fell by a quarter, where
  Shoal's walk cost 1.13 and 1.60 times for forty to fifty times as long. A scan in disk order would
  read far past the ceiling X16's walk never reached, so once X17 lands it is
  [X18](spikes.md#x18-table-reads-paced-by-idle-time)'s pace that holds the budget, not the walk's
  slowness.
- **A scan carrying its rows to one client fills that client's link**: 168.6 MiB a second into europa,
  about two thirds of it over its one 1 GbE port, and the cell on the same client went from a read p99
  of 1.15 ms to about 200 ms, the kernel's minimum retransmission timeout, at a tenth of its rate.
  Shoal's client connections keep a 5 ms minimum, and [W7](isolation.md#network) puts a scan on
  connections of its own; [M24](milestones.md#m24-scans-on-one-node)'s evidence now names the case
  of one link.
- **Before any scan, ScyllaDB's cell read faster than Shoal's**: a read p99 of 1.15 ms from a Python
  client, against X16's 6.6 to 7.7 ms, and 2.7 ms even during the scan. Filed as
  [O140](../../appendix/optimizations.md#o140-a-resident-reads-p99-beside-the-cells-inserts-is-six-times-scylladbs-on-the-same-hosts);
  ScyllaDB answers a write before its commitlog is synced, which leaves its shards and devices freer
  than Shoal's synced writes leave its own.
- **The prediction was wrong**: under 1.5 times expected, 2.34 measured.

Every host was identical to its snapshot afterwards.

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

**The run's one prediction was wrong as well.** It expected the cell's read p99 to rise by less than
1.5 times beside a scan at two in flight a shard with `BYPASS CACHE`, near W7's budget but outside
it; it rose 2.34 times, outside by more than Shoal's walk is. Its "more without `BYPASS CACHE`" was
not run. It named a stress load at a fixed rate and the post's scanner; the run drove X16's closed
loop and a scanner of the post's shape, since the post's simulates one (below).

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
`BYPASS CACHE`, pages of a thousand rows, in four processes of its own.

### The cluster and the foreground

Three ScyllaDB nodes from the release's own image, `scylladb/scylla:2026.3.4` (its amd64 manifest on
Docker Hub `sha256:2a3cf99b55d7...`, one image on every host by its id), on X16's hosts and cpus:
europa in docker, which it had; titan and hyperion in podman, installed for the run and purged
after, hyperion given the image titan pulled, since it cannot reach the internet. Every change to a
host is in [x20-host-changes.txt](../../../../shoal-spike/results/x20-host-changes.txt), and every
host was identical to its snapshot afterwards.

| Host | Shards | Cpus | Memory | Data on |
| --- | --- | --- | --- | --- |
| europa | 6 | 1-6 | 6 GiB | the Optane, `/optane/scylla-x20` (XFS) |
| titan | 5 | 2-6 | 6 GiB | the 970 EVO, `/xfs/scylla-x20` (XFS) |
| hyperion | 5 | 2-6 | 6 GiB | the 970 EVO, `/xfs/scylla-x20` (XFS) |

Developer mode (no tuning of the hosts), `--overprovisioned 0`, so each shard keeps its cpu, and the
release's own `scylla.yaml`, so a write is answered before its commitlog is synced
(`commitlog_sync: periodic`, every 10 s, `conf/scylla.yaml:60-61`; "periodic syncs are acknowledged
immediately", `db/config.cc:934`). More memory than X16's 4 GiB nodes, since ScyllaDB sizes its row
cache and its readers' semaphores by it and wants about a gibibyte a shard. One keyspace, three
copies, tablets on, and two tables of X16's shapes: `cell`, 100,000 rows of a kibibyte, and `big`,
3,000,000, loaded at `QUORUM` and flushed. `big` is 64 tablets, an sstable each, 3.1 GB on every node
at 1,109 bytes a partition ([x20-cluster.txt](../../../../shoal-spike/results/x20-cluster.txt)).
Each round started after every node was restarted, so `big` was read from its sstables with an empty
row cache, as X16's `Big` was from its archives after a restart.

The foreground was X16's cell from Python: four closed-loop workers, each one request at a time, half
reads of a preloaded key at `ONE` and half inserts of a new key at `QUORUM`, the driver token and
shard aware (`scylla-driver` 3.29.12). It ran in a container of its own on europa pinned to cpus
12-15 and 28-31, X16's driver cpus, the scan's four processes beside it in the same container. The
scan began 30 s in and the foreground ran on for 25 s after it ended. A Python client's latencies
are its own as well as the cluster's, so what compares with X16 is the ratio of the cell's p99
during the scan to its p99 before, not the milliseconds.

The scan's 64 tablets were dealt over their three replicas in turn, which put them on 12 of the
cluster's 16 shards, 24 requests in flight. Four rounds had it carry its keys alone, as X16's scan
carried keys and counts, and two more every row to the client (`SELECT=rows`), all by
[x20-lab.sh](../../../../shoal-spike/results/x20-lab.sh). The rounds are in
[x20-runs](../../../../shoal-spike/results/x20-runs) and the tables come from
[x20-report.py](../../../../shoal-spike/results/x20-report.py).

### The figures

The scan, a round a row:

| Round | Carried | The scan | Rows a second | MiB a second carried | A tablet, p50 / max | Errors |
| --- | --- | --- | --- | --- | --- | --- |
| 1 | keys | 2.14 s | 1,400,864 | 10.7 | 0.64 / 0.88 s | 0 |
| 2 | keys | 2.18 s | 1,376,532 | 10.5 | 0.63 / 0.88 s | 0 |
| 3 | keys | 2.16 s | 1,389,724 | 10.6 | 0.64 / 0.85 s | 0 |
| 4 | keys | 2.14 s | 1,402,519 | 10.7 | 0.63 / 0.85 s | 0 |
| 1 | rows | 17.11 s | 175,358 | 168.6 | 5.18 / 9.51 s | 0 |
| 2 | rows | 17.09 s | 175,529 | 168.7 | 5.80 / 9.09 s | 0 |

The cell, a round and a kind a row, its 30 s before the scan beside the scan's own time:

| Round | Carried | Kind | p50 before → during | p99 before → during | Rate during over before | p99 during over before |
| --- | --- | --- | --- | --- | --- | --- |
| 1 | keys | read | 0.57 → 0.68 ms | 1.15 → 2.81 ms | 0.75 | 2.44 |
| 1 | keys | insert | 0.58 → 0.68 ms | 1.07 → 2.32 ms | 0.74 | 2.17 |
| 2 | keys | read | 0.57 → 0.66 ms | 1.15 → 2.63 ms | 0.74 | 2.28 |
| 2 | keys | insert | 0.57 → 0.67 ms | 1.07 → 2.17 ms | 0.76 | 2.03 |
| 3 | keys | read | 0.56 → 0.65 ms | 1.15 → 2.74 ms | 0.74 | 2.38 |
| 3 | keys | insert | 0.57 → 0.67 ms | 1.07 → 2.29 ms | 0.74 | 2.14 |
| 4 | keys | read | 0.57 → 0.66 ms | 1.16 → 2.68 ms | 0.74 | 2.31 |
| 4 | keys | insert | 0.58 → 0.68 ms | 1.09 → 2.41 ms | 0.73 | 2.22 |
| 1 | rows | read | 0.56 → 3.31 ms | 1.14 → 199.03 ms | 0.09 | 174.51 |
| 1 | rows | insert | 0.57 → 3.85 ms | 1.07 → 150.56 ms | 0.10 | 140.94 |
| 2 | rows | read | 0.57 → 3.38 ms | 1.17 → 197.02 ms | 0.09 | 169.03 |
| 2 | rows | insert | 0.58 → 3.92 ms | 1.07 → 186.91 ms | 0.09 | 174.03 |

Medians over the rounds, the range beside each in brackets:

| Carried | Rounds | The scan | Read p99 during over before | Insert p99 during over before | The cell's rate during over before |
| --- | --- | --- | --- | --- | --- |
| keys | 4 | 2.15 [2.14, 2.18] s | 2.34 [2.28, 2.44] | 2.15 [2.03, 2.22] | 0.74 [0.74, 0.75] |
| rows | 2 | 17.10 [17.09, 17.11] s | 171.77 [169.03, 174.51] | 157.48 [140.94, 174.03] | 0.09 [0.09, 0.10] |

After the scan the cell was back where it began in every round: a read p99 of 1.13 to 1.17 ms and an
insert p99 of 1.08 to 1.12 ms.

### Beside X16

The scan carrying its keys, beside [X16's tables](walk-beside-the-foreground.md#the-cell-beside-a-whole-scan)
of Shoal's walk beside the same cell on the same hosts, its before side (F115) and after side (F119):

| | Shoal's walk, before / after | ScyllaDB's scan |
| --- | --- | --- |
| The whole table, beside the cell | 112 / 86.3 s | 2.15 s |
| Partitions a second, beside the cell | 26,700 / 34,500 | 1,395,000 |
| The cell's read p99, before → during | 6.58 → 7.24 / 6.54 → 7.33 ms | 1.15 → 2.71 ms |
| Read p99, during over before | 1.13 / 1.12 | 2.34 |
| Insert p99, during over before | 1.60 / 1.63 | 2.15 |

Alone, X16's walk read 83,900 and 85,700 partitions a second, about 28 MiB a second a node; ScyllaDB's
scan, beside the cell, read about 460 MiB a second a node, a third of the table's 3.1 GB on each in
2.15 s.

### What the run says

- **A scan in its table's order reads at many times the walk's rate on the same devices.** A tablet's
  46,875 rows took 0.63 s at p50, 13 µs a row on one of 24 requests in flight, read from one sstable
  in token order. X16's walk reads a page's archived partitions one at a time, 732 in 154 ms on the
  Zen1 hosts alone and 346 ms beside the cell, 210 and 470 µs a partition on one of 32 pages in
  flight. That is X16's finding with a number beside it~~: runs in disk order are X17's lever~~, and the
  devices give a scan far more than the walk takes of them; [W42](scan-speed.md) says which levers
  reach it, and in what order. The rate is a lower bound on what the
  cluster serves, since the scan's own client may have been its limit ([below](#what-it-cannot-tell)).
  Evidence for [Q33](contract.md#questions-to-answer) and Q37: a scan at many times the walk's rate
  ran on ScyllaDB's shards, in the same scheduling group as its foreground.
- **Unpaced, at that rate, a scan doubles the foreground's tail while it runs.** The cell's read p99
  2.34 times and its insert p99 2.15 times, its rate down a quarter, for the scan's 2.15 s: ScyllaDB
  is outside W7's budget too, by more than Shoal's walk is, for a fortieth to a fiftieth of the time.
  Its scan's reads ran in the statement group beside the foreground's, since no service level was
  set. For [Q34](contract.md#questions-to-answer): a scan in disk order would read past the 100 MiB
  a second a device that X16's walk never reached, so the pace X18 sets is what holds the budget once
  X17 lands, and X18 has to measure it against a scan that fast, not against today's walk.
- **A scan's rows to one client fill that client's link, and the client's own foreground waits on
  the kernel's retransmission timer.** Carrying every row, the scan moved 168.6 MiB a second into
  europa: about a third of its tablets were read on europa's own node, so about two thirds of those
  bytes, some 112 MiB a second, crossed europa's one 1 GbE port, about that port's line rate. The
  cell on the same client went from a read p99 of 1.15 ms to 197 to 199 ms, a median of 3.3 ms, at a
  tenth of its rate. A tail at 200 ms is Linux's minimum retransmission timeout: a lone segment lost
  beside a full link waits it out, as [Resolved #304](../../appendix/resolved/lone-frame-loss-probe.md)
  found of Shoal's own lanes. No TCP counters were read during these rounds, so that cause is
  inferred from the tail's shape, not measured. Shoal's client already holds `networking.rto_min`,
  5 ms, on a foreground connection, and [W7](isolation.md#network) puts a scan on connections of its
  own that keep the kernel's. Neither shares out the link, which is why M24's evidence now asks for
  the budget with the foreground on the scan's own client.
- **ScyllaDB's foreground was faster than Shoal's before any scan.** At the same level on the same
  hosts, ScyllaDB's cell had a read p99 of 1.15 ms before the scan and 2.7 ms during it; X16's had
  6.6 ms before a scan and 7.7 ms alone, its median 0.43 ms, below ScyllaDB's 0.57. A Python client
  adds to ScyllaDB's figures, not to Shoal's. Its inserts are answered unsynced, so the insert tails
  are not comparable, and a shard that syncs no write may answer its reads sooner: filed as
  [O140](../../appendix/optimizations.md#o140-a-resident-reads-p99-beside-the-cells-inserts-is-six-times-scylladbs-on-the-same-hosts),
  to be measured, not taken as the cause.

None of it changes the design: a scan's pages are paced by their devices' idle time and admitted
by a count and a budget ([F116](../../features/scan-admission.md) to
[F119](../../features/scan-pacing.md)), and W7 carries a client's scan on connections of its own,
which is what each finding argues for.

### What it cannot tell

- **The client shared its cpus.** The scan's four processes and the foreground ran in one container
  on four cores' eight threads, each scanner process decoding about 350,000 rows a second. The scan's
  rate is what the cluster served this client, a lower bound on what it can serve, and part of the
  cell's tail during the scan may be its own client's.
- **The insert tails mean different things.** ScyllaDB's inserts are answered before their commitlog
  is synced, Shoal's after its WAL is; X16's insert tail is the scan's reads beside the WAL's
  flushes on one device, which ScyllaDB's inserts never wait on.
- **The during window is short**: 2.15 s, about 5,400 reads and as many inserts in each keys round,
  so its p99 is each kind's 54th slowest. The four rounds' ratios agree within 0.2.
- **One replica a tablet, dealt in turn**, each request at `ONE` to its replica's shard. A scanner that
  chose the least busy replica, or left the shard to the driver, would load the cluster otherwise.
- **No service level.** The scan ran in the statement group beside the foreground; ScyllaDB's
  workload prioritization, which [W3](prior-art.md#scylladb) reports took 5 to 10% from an OLTP load
  beside an OLAP one, was not tried.
- **Developer mode and 6 GiB a node**: the hosts were not tuned as ScyllaDB's own setup would tune
  them, and each node had half as much memory again as X16's.

### On the way

- **A bound statement cannot carry a range's routing key.** In `scylla-driver` 3.29.12 a
  `BoundStatement`'s `routing_key` comes from its bound partition key and has no setter (`property
  'routing_key' of 'BoundStatement' object has no setter`), and a token range binds none: the first
  client's every scanner thread failed on it. The scanner builds a statement a tablet, its tokens in
  its text and a key inside the tablet as its routing key, and primes the driver's tablet map from
  `system.tablets`, since a driver learns a tablet only from a single-partition request's answer
  (`docs/dev/tablets-routing-v2.md:108`).
- **A scanner in the foreground's process takes the foreground's interpreter.** With the scan's
  threads in the foreground's own process, carrying rows, the cell answered 64 reads a second during
  the scan, a fiftieth of its rate before; with them in four processes of their own, as in the
  rounds above, about 310, a tenth. Its read p99 was 217 ms, and 197 to 199: the interpreter cost the
  cell its rate, and the link its tail. The run with the threads in one process, and the first
  client's, came from earlier versions of the client and are quoted here, not kept.

## Related

[W3](prior-art.md) for the claims; [X14](../../object-storage/ceph-and-s3-sources.md) for the method;
[X16](walk-beside-the-foreground.md) for the walk beside the same cell;
[X17](spikes.md#x17-a-page-rebuilt) for saved positions and runs in disk order; [W42](scan-speed.md)
for what closes the gap;
[X18](spikes.md#x18-table-reads-paced-by-idle-time) for the pace; [W7](isolation.md) for Shoal's
admission and its connections; [Resolved #304](../../appendix/resolved/lone-frame-loss-probe.md) for
a lone segment's loss beside a full link.
