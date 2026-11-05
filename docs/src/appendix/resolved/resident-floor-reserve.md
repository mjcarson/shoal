# 338. A node below its resident floor evicted its rows at every check

## Symptom

[X16](../../future-work/scans/walk-beside-the-foreground.md#what-was-found-on-the-way)'s first quick
pass ran its side cluster's nodes at `node_memory: 1Gi`. Every node sat at 1.56 to 1.77 GiB resident
while its rows held 23 to 115 MiB, so every shard evicted two fifths of its rows at every check: 146,000
partitions a member beside a scan of `Big` and 172,000 beside one of `Keyword`. The cell's reads went
to disk for rows the node had just evicted, and nothing the node evicted brought it under. At 4 GiB the
same cluster held 1.58 to 2.02 GiB and evicted nothing. No line in any log said why.

## Cause

A node judges its budget against the process's resident set, read from `/proc/self/statm` at most
every 250 ms on every shard ([Resolved #149](node-memory-budget.md)), because almost nothing a node holds
is rows. Some of what it holds is no row's and stays whatever is evicted. That is its **floor**: its
executors and their io_uring buffers, its groups, its indexes and caches, and the allocator's pages.
`Shard::over_memory` (`shoal-core/src/server/shard.rs`) knew nothing of a floor. A process past
`node_memory` had every shard evict 40% of its rows (`MemoryPressure::Resident`), however little the rows
held against the excess. A node whose `node_memory` was below its floor was past its budget for good.
Every shard evicted at every check, four times a second, each pass taking two fifths of what the
foreground had read back since the last. #149's own *Still open* named the shape and left it.

## Evidence

**Reproduced in process.** The tests were written first, in a binary of their own
(`shoal/tests/resident_floor.rs`, since `faults::override_node_memory` is process wide). A single shard
of a 64 MiB budget holds 10,000 rows of a kibibyte, archived by a restart and read back. Its node's memory
is then moved to a mebibyte, far below what the process holds without a row. Against the tree before
the fix, the node ran a pass at every check and evicted nearly every row in two seconds. A node whose
two mebibytes its floor alone filled started as if nothing were wrong:

```text
---- a_node_past_its_floor_holds_its_rows_to_a_reserve stdout ----
thread 'a_node_past_its_floor_holds_its_rows_to_a_reserve' panicked at shoal/tests/resident_floor.rs:219:5:
assertion `left == right` failed: a node past its floor ran 7 passes by its resident memory, evicting 9721 of 10000 partitions
  left: 7
 right: 0

---- a_node_whose_floor_leaves_its_rows_nothing_refuses_to_start stdout ----
thread 'a_node_whose_floor_leaves_its_rows_nothing_refuses_to_start' panicked at shoal/tests/resident_floor.rs:288:5:
a node whose floor fills its memory started on 127.0.0.1:36999
```

**The floor named on the lab** (2026-11-05). Three tmdb nodes ran on europa over loopback
(`shoaladm/inventories/loopback.yml`, X16's shape at 4 GiB a node), each bootstrapped fresh. Each node was
read at ready, after loading 100,000 movies and walking every tablet, and after two minutes at rest, three
ways:

- **A:** mimalloc as deployed.
- **B:** mimalloc with `MIMALLOC_ALLOW_LARGE_OS_PAGES=0`.
- **C:** jemalloc with sampled profiling, dumped every 128 MiB allocated. A dump at a clean stop
  (`prof_final`) was tried first and is no use for this: the stop has freed the tables, and it held 0.04 GiB.

The script is scratch under `target/lab/i338/`. Each reading is `/proc/<pid>/status`, `smaps_rollup`'s
`AnonHugePages` and the node's own figures (`stats --json`):

| A node, MiB | At ready | After the load | At rest | Huge pages at rest | Unnamed at rest |
| --- | --- | --- | --- | --- | --- |
| A: mimalloc as deployed | 409–436 | 1,161–1,181 | 1,145–1,181 | 712–776 | 814–850 |
| B: mimalloc, no huge pages | 149–157 | 917–957 | 929–969 | 0 | 598–638 |
| C: jemalloc | 149–157 | 814–823 | 827–835 | 0 | 495–504 |

Every arm held the same rows (230 MiB as the shards count them), the same indexes (90 MiB) and the same
eviction list (12 to 13 MiB). "Unnamed" is the resident set less all four, which no figure named.

- **Huge pages are about a quarter of a gibibyte of every node's floor.** mimalloc 2 on Linux defaults
  `allow_large_os_pages` to 2 and madvises its arenas `MADV_HUGEPAGE`, and europa runs THP `madvise` with
  khugepaged's `max_ptes_none` at 511. A 2 MiB page is resident once any byte of it is touched, and a free
  inside it returns nothing. The same nodes held 260 to 280 MiB more at ready with them than without, and
  200 to 230 MiB more at rest. Europa's own tmdb node held 1.82 GiB of its 1.85 resident in huge pages.
  Filed as [O157](../optimizations.md#o157-a-nodes-heap-on-transparent-huge-pages), not taken here.
- **What the heap held at the end of the load**, by C's last interval dump of each node
  (`lg_prof_interval:27`, symbolized by `shoal-spike/results/heap.py`, 0.87 to 0.88 GiB live):

  | Part | MiB a node |
  | --- | --- |
  | Rows and the tables' maps (the shards count about 270 of it) | 420–458 |
  | openraft's engine queue, `EngineOutput::new(4096)` a group | 156–162 |
  | The WAL's index and its log (its figure says 56) | 123–135 |
  | glommio's io memory, 10 MiB a shard and 16 the object executor's | 67 |
  | Replicated commands decoded | 42–46 |
  | openraft's runtime stats, 27 MiB of it one table a process | 39–40 |
  | Each group's remembered requests | 6–11 |

  openraft's queue is #191's shape again: a ring allocated to its bound, its pages resident as load passes
  through. It and the runtime stats are filed as
  [O158](../optimizations.md#o158-what-openraft-holds-of-every-nodes-floor). The pinned figure
  (`VmPin`, 198 MiB a node in B) counts glommio's 66 MiB of io memory once a ring it is registered on,
  three times over.
- **The fix on the same cluster** (2026-11-05, the build before the fix against this one, each at
  `node_memory: 768Mi`, below the ~830 MiB a node holds unnamed at rest and above its floor at ready).
  The fixed nodes logged 189 to 260 MB resident at ready, and one warning each ten seconds while past
  their floor:

  | A node | Before the fix | Fixed |
  | --- | --- | --- |
  | Passes during the load | 358–380, by its resident memory | 0 by its resident memory, 13–14 past its floor |
  | Partitions evicted during the load | 212,335–216,219 | 173,397–180,877 |
  | Passes in two minutes at rest | 1,645–1,816, each evicting nothing | none; 1,498–1,672 checks counted past the floor |
  | Rows held at rest | 77–80 MiB, its eviction list empty | 111–119 MiB, its reserve held |
  | The walk's partitions found resident, keywords / releases | 7,663–8,956 / 10,746–14,023 | 15,096–15,806 / 15,064–16,799 |
  | Resident at rest | 916–984 MiB | 1,039–1,056 MiB |

  The last row is the reserve the fix keeps past the floor, the overshoot it bounds. At `node_memory:
  1Gi` both builds ended the load under their budget (989 to 1,023 MiB), and their passes rightly ran
  while the rows covered the excess. Every arm's figures, logs and script are under `target/lab/i338/`,
  which is scratch.

## The fix

- **The eviction list keeps what it names summed.** It was a bare `LruCache` shared by a shard and its
  tables. It is now an `EvictionList` (`shoal-core/src/server/memory.rs`) that keeps the charge of every
  partition it names as each is named, taken out or evicted (`put`, `pop`, `pop_lru`, `promote`). A check
  reads what eviction could free without walking the list. The `ShoalDatabase` trait, the derive and the
  two persistent tables carry it in place of the cache.
- **Every shard publishes its rows and what eviction could free of them.** Each shard has a slot of its
  own on a cache line of its own (`NodeRows`). One figure is every row it counts with its list
  (`memory_usage + lru_bytes`). The other is what the list names with the list itself
  (`EvictionList::bytes + lru_bytes`). A row past its last archive is in the first and never in the
  second. The shard publishes at every check of the node's memory and once before it reports itself
  ready.
- **A check past the node's memory is judged before it evicts** (`memory::judge`), by what eviction could
  free:
  - Past `node_memory` by no more than the node's evictable rows hold, evicting can bring it under, and
    the pass runs as #149 has it (`MemoryPressure::Resident`).
  - Past it by more, the node is **past its floor**. The check is counted
    (`EvictionFigures::floor_checks`) and warned of, once every ten seconds at most across the node,
    naming its resident set, its evictable rows, its floor and the reserve.
  - Each shard then holds its evictable rows to a **reserve** of an eighth of its budget
    (`ROW_RESERVE_SHARE`), evicting only while they are above it. Such a pass is counted under its own
    cause (`MemoryPressure::Floor`: `floor_passes`, `floor_partitions`, `floor_bytes`) and evicts down to
    three fifths of the reserve (`memory::floor_need`), as a budget's pass leaves three fifths of its
    rows.
  - F107's caches still give units back before either.
- **A node whose floor leaves its rows nothing is refused at ready** (`Resources::check_floor`, called by
  `ShoalPool::check_floor`). Once every shard has answered, and before the object executors start, so
  their rings are counted once, the pool reads the process's resident set and logs it at INFO. It refuses
  the node `InvalidConfig` when that set, less every row its shards published, plus object work's
  reservation and the scan budget reaches `node_memory`. This check takes out every row, evictable or not,
  so a restart that read rows back is judged leniently. The refusal names the figure to raise
  `node_memory` past, and the shards are told to stop.
- **The figures reach a capture and `cluster stats`.** A member's memory sample keeps `floor_checks` and
  `evicted_at_floor`, `evicted_bytes` counts the floor's passes, and the stats view's memory group gains
  `evicted, floor` and `floor checks`.
- **A heap profile has a symbolizer again.** `shoal-spike/results/heap.py` reads a `jemalloc-prof` node's
  `heap_v2` dumps and names their stacks through `addr2line`. Round 15's `target/lab/r15/prof/heap.py`,
  which the docs cited, was scratch and is gone.

## Alternatives rejected

- **Judge the floor and the reserve by every row a shard counts.** This change's first cut did, and the
  lab showed why not. At 768 MiB each node's last 80 MiB of rows were past their last archive, so its
  eviction list was empty and every row it counted was held. Each shard's rows passed its reserve on
  unarchivable rows alone, so every check counted a floor pass that evicted nothing: 1,452 to 1,488 a node
  in two minutes at rest. Under traffic, the same rule would have evicted every evictable row the
  foreground read back at every check, which is the defect. Rows eviction cannot free are floor.
- **Sum the eviction list at the check.** It holds a partition a node keeps, 11.8 million on the lab's
  ten copies, and walking it on a shard's loop four times a second would hold that loop for milliseconds.
  The sum is kept as the list changes, at a subtraction an operation.
- **Stop evicting past the floor.** The simplest rule, and the first one considered. Rows would grow back
  to their full budgets, so a node whose floor grew under load could hold its floor and `node_memory`
  besides: on a host sized to the budget, the kernel's OOM killer before the node's own budget. The
  reserve bounds what is kept past the floor to an eighth of the rows' budgets.
- **Keep the 40% passes past the floor, rate limited.** Closest to the old behaviour, but every period
  would still evict rows the foreground reads back, which is the defect.
- **Hold each shard's rows at what they held when the floor was crossed**, by lowering its budget to
  them. A budget is judged on every message, not at the quarter second check
  ([known issue 59](../known-issues.md#59-a-shard-that-cannot-free-anything-keeps-trying-on-every-message-in-silence)'s
  shape), so a read that loaded one partition past it would begin a pass. The reserve is judged at the
  check.
- **Judge the node's budget on its rows rather than its resident set.** #149 rejected that for good
  reason: almost nothing a node holds is rows, and that memory is what the budget exists to bound. The
  resident set still judges whether the node is over. The rows only judge whether evicting them could
  help.
- **Subtract a floor measured at ready from the resident set.** The floor is not fixed. On the lab it
  doubled with a load of 100,000 rows, and huge pages make what a free returns depend on what else
  shares its 2 MiB. A floor read once would be wrong within minutes.
- **Refuse at the configuration's check, before anything starts.** The floor is what the running
  process holds, which is known only once its executors, rings and groups exist.
- **Turn mimalloc's huge pages off here.** That is where the floor's largest cuttable part is (arm B),
  but it changes every node's memory backing and wants its own before and after on the lab: filed as
  [O157](../optimizations.md#o157-a-nodes-heap-on-transparent-huge-pages).

## Invariants to uphold

- **A check past `node_memory` is judged by what eviction could free, never by every row**: a row past
  its last archive is in no eviction list. `memory::judge` is consulted before every `Resident` pass, and
  past the floor a shard evicts only its evictable rows above its reserve.
- **The eviction list is changed only through `EvictionList`'s methods**, so its sum is what it names.
  A table that took a partition out of it any other way would leave the sum high, and the node would
  judge itself able to free what it cannot.
- **A shard publishes at every check and before it reports ready**, so a judgement reads every shard at
  most one check old, and the pool's floor check reads every row as the shards started.
- **The floor check at ready runs before the object executors start**: their rings are in
  `object_reserved`, and counting them in the resident set as well would refuse nodes that fit.
- **The budget is still judged on the kernel's figure** (#149's invariant): the rows decide only whether
  a pass can help, never whether the node is over.
- **Past the floor nothing is judged on every message**: the reserve is applied at the rate-limited check,
  never as a lowered budget.

## Still open

- **The floor grows with a node's data**, and nothing past the reserve bounds it. A node whose floor
  grows past `node_memory` after its start keeps its reserve and warns. It is not refused, since a node
  that served cannot be refused without an outage.
- **The reserve is a constant**, an eighth of a shard's budget, chosen to bound the overshoot, not from a
  measurement of what a node past its floor reads.
- **A pass a node's evictable rows can cover may still free nothing the kernel sees.** On a heap of huge
  pages a free inside a 2 MiB page returns nothing ([O157](../optimizations.md#o157-a-nodes-heap-on-transparent-huge-pages)),
  so such a node runs #149's passes until its evictable rows no longer cover the excess, and only then
  holds its reserve. On the lab at 1 GiB that was 88 to 286 passes a node through a load.
- **The node's own figures undercount what it holds beside its rows.** The WAL's index figure says
  56 MiB where the profile holds 123 to 135, and the rows' charge about 270 where the heap holds 420
  to 458 ([Resolved #196](row-charge-undercount.md)'s remainder). The judgement reads the eviction
  list's charge, which is exactly what a pass frees by the shards' count, so these leave it alone, but
  an operator reading `cluster stats` still sees a large unnamed share.

## Tests

| Test | Where | Breaks if |
| --- | --- | --- |
| `a_node_past_its_floor_holds_its_rows_to_a_reserve` | `shoal/tests/resident_floor.rs` | A node past its floor evicts at every check again, evicts below its reserve, or stops counting the checks |
| `a_node_its_rows_can_bring_under_still_evicts_them` | same | The floor's judgement swallows a pass the node's rows could bring it under with (#149's) |
| `a_node_whose_floor_leaves_its_rows_nothing_refuses_to_start` | same | A node whose floor fills its memory starts, or is refused without naming the setting and its resident set |
| `a_node_past_its_floor_evicts_only_above_its_reserve` | `shoal-core/src/server/memory.rs` | The judgement past the floor evicts at or under the reserve, or a pass there leaves anything but three fifths of it |
| `a_node_its_rows_can_bring_under_is_judged_as_before` | same | A node its rows can bring under, or one inside its memory, is judged past its floor |
| `a_nodes_rows_are_every_shards_last_published` | same | The board sums stale or out of range slots, or mixes a shard's rows with what eviction could free of them |
| `an_eviction_lists_bytes_follow_what_it_names` | same | The list's sum drifts from what it names when a partition is named again, taken out by key or evicted |
| `a_floor_that_leaves_the_rows_nothing_is_refused` | `shoal-core/src/server/conf.rs` | The start's check counts rows as floor, forgets object work or scans, or refuses a node with no budget |
| `a_floor_check_is_counted_apart_from_its_passes` | `shoal-proto/src/shared/protocol/stats.rs` | A check or pass past the floor is counted as a resident pass, not summed over shards, or unreadable from an older build |

## Related

- [Resolved #149](node-memory-budget.md), which judged a node's budget on its resident set and left this
  open.
- [Resolved #196](row-charge-undercount.md), what a row is charged.
- [F104](../../features/memory-in-a-capture.md), evictions by cause.
- [F107](../../features/cache-default-share.md), the caches that give units back first.
- [X16](../../future-work/scans/walk-beside-the-foreground.md#what-was-found-on-the-way), which found it.
- [Memory and eviction](../../tables/memory-and-eviction.md).
