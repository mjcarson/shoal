# F107. A cache of read units by default, given back before rows

Since this feature a node keeps [F102](read-unit-cache.md)'s cache of read units unless it names
otherwise: an eighth of `node_memory`, on a node that claims devices. And when the process passes
the node's memory, the caches give units back before any shard evicts a row. A node that names a
cache keeps what it names, and one that names `0` keeps none.

## Context

F102 built the cache off unless named, and its figures were the user's to judge a default by: on
every leg a cache that held the working set ran gets at one in flight 1.25 to 3.1 times as fast
([F102's figures](read-unit-cache.md#performance)). The user chose a share of node memory, once
the table rows were measured under the memory it takes (2026-10-22).
[F104](memory-in-a-capture.md) counted what that run needed, and lab 1 ran it on 2026-10-23
([the table rows beside it](read-unit-cache.md#the-table-rows-beside-it)):
- **Where the rows fit with room**, 8 GiB nodes on SSDs, an eighth evicted no row and the paced
  table reads were within.
- **Where the rows nearly filled a node**, 2,304 MiB on the loopback, an eighth took each node past
  its memory as it filled. The shards evicted about half their rows in that run, the table reads'
  p99 rose to 4.7 and 6.0 ms against 3.0 at most without it, and the gets ran 1.71 times as fast.

The user chose an eighth, the caches giving units back before rows are evicted (2026-10-23).

## What it does

- **The default.** A node's start settles `resources.object_cache` where the file names none
  (`Resources::settle_object_cache`): an eighth of `node_memory` (`DEFAULT_CACHE_SHARE`) on a node
  that claims devices and runs object executors, and none on any other. The rows' budgets, the start's
  check and the object plan all read it once it is settled, so the cache is carved out of the
  node's memory beside `object_memory` as one named is.
- **Named stays named.** A size is that size, and `0` is none.
- **Given back first.** A shard that finds the process past `node_memory` asks the node's caches to
  give units back before it evicts (`ObjectStore::yield_caches`):
  - each cache's bound falls below what it holds by its share of the excess and an eighth of what
    it holds besides;
  - its executor gives the units past the bound back, those read least recently first
    (`UnitCache::shrink`), and asks the allocator to return the memory to the system
    (`malloc_trim`);
  - the shard evicts no row on that check.

  Only once the caches hold nothing does a shard past the node's memory evict rows, as before.
- **Counted.** A check a cache gave units back on is counted in the shard's evictions
  (`EvictionFigures::cache_yields`), summed in a node's figures, and kept in a capture beside each
  member's memory (`cache_yields`); `stores-ab-report.py --memory` prints it.

## Design choices

- **Settled at the start, not written.** shoaladm renders an unnamed cache unnamed, as it always
  did, so a node takes the default when it starts on this build, and an operator's file still
  says what it said.
- **Only where a slice can hold units.** A node with no device has nothing to cache, and a node
  with no node budget has no share to take: each keeps none. So the fixture's clusters, a
  standalone node and shoal-bench's servers are unchanged.
- **The caches yield in the shard's own check.** The shard decides to evict when the process passes
  the node's memory ([Resolved #149](../appendix/resolved/node-memory-budget.md)), so it is where
  the caches are asked first. A bound is an atomic another thread can lower, and only lowered:
  every shard of a node may ask at once, and the same excess lowers it once.
- **An eighth more than the excess.** The process's memory falls only as far as the allocator gives
  freed memory back, so a cache gives a margin besides the excess, and a check that still finds the
  node past its memory asks again.
- **The allocator asked to give memory back.** A cache's units are small allocations, which glibc
  keeps in its arenas after they are freed; `malloc_trim` returns them to the system, so the
  process's resident memory falls by what the cache gave.

## Alternatives rejected

- **An eighth with no yielding**, the share as lab 1 measured it. On a node whose rows nearly fill
  its memory, the cache's first fill evicted half of them.
- **A fixed size.** It is too much for a small node and too little for a large one; a share follows
  the node.
- **Evicting rows and the cache together.** The rows are a table's working set, and the cache is a
  copy of bytes on a device.
- **A cache that grows back once memory frees.** A bound raised again refills to the size that took
  the node past its memory; filed, with what it would need.

## Limitations

- **A lowered bound is never raised** until the node restarts. A node that passed its memory once
  keeps a smaller cache ([todos](../appendix/todos.md#a-cache-that-grows-back-after-it-gave-units-back)).
- **The allocator's trim is glibc's.** With another allocator, as the jemalloc build has, freed
  units may stay resident, and the caches give everything before rows are evicted.
- **A node stays past its memory for a check or more** while its caches give units back: four
  checks a second each shard.
- **A node with no `node_memory` keeps none**, as before; it has no budget to take a share of.
- **An upgrade changes a running node.** A node restarted on this build with no `object_cache` in its
  file keeps an eighth of its memory; one that should keep none names `0`.
- ~~**Not yet measured on the lab.** Its run follows: the default beside `0`, and the node whose rows
  nearly fill it.~~ Measured on 2026-10-23 ([Performance](#performance)).

## Invariants to uphold

- **A named cache is as named**, zero meaning none, and an unnamed one is settled once, at the
  start, before any budget is read.
- **A shard past the node's memory evicts rows only once the caches hold nothing.**
- **A cache's bound only falls**, so every shard asking at once lowers it once.
- **A cache never holds more than its bound**: a fill and a shrink keep to the bound as read now.

## Performance

Measured on the lab on 2026-10-23 (`w2-loop-rep3-tight` and `w2-ssd-rep3-default`, under
`shoal-spike/results/stores-ab/`).

How it was run:
- **One build**, the commit of item 301's fix (`6bfea9e7`), which carries F107. The sides differ in
  the inventory alone:
  - *default*, naming no `object_cache`: an eighth of each node's memory;
  - *none*, naming `0` (`AB_none_CACHE=0MiB`).
- **Lab 1's load** ([the table rows beside it](read-unit-cache.md#the-table-rows-beside-it)):
  - the comparison's 1 MiB gets at sixteen in flight, which fill a cache;
  - beside them, the TMDB `Movie` table's reads, paced at 500 a second on four workers;
  - the table preloaded whole, 1,188,548 rows, beside 1,024 posters of 1 MiB.
- **The order of the captures** was default, none, none, default, four runs each, with three
  copies.
- **A side's figure** is the median of its eight runs, with the lowest and highest.
- **Nothing was lost.** The check at each capture's end read 1,024 objects back.

### On europa's loopback, its rows nearly filling each node

Each node's memory was lab 1's 2,304 MiB, which leaves the rows just under it with no cache; an
eighth is 288 MiB.

| Side | Highest resident, any node | Rows evicted | Caches gave back | Caches held, three nodes | Gets a second | p99 ms | Paced reads p50 ms | p99 ms |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| None | 2.10 GiB [2.10–2.10] | none | – | – | 2,586 [2,582–2,587] | 8.75 [8.69–8.87] | 0.83 [0.34–1.14] | 1.55 [1.01–1.83] |
| Default | 2.22 GiB [2.21–2.22] | none | 7 and 9 checks | 303 and 184 MiB | 3,278 [3,120–3,444], **1.27, ahead** | 8.33 [8.28–8.39] | 0.83 [0.27–1.06] | 2.07 [1.42–2.51] |

What this says:
- **No row was evicted.** Lab 1's eighth, which gave nothing back, evicted about half of each
  node's rows in each capture's first run.
- **The caches gave units back instead.** Each node's cache did so on two or three checks, all in
  its capture's first run as it filled, and on none after. They kept 28 to 138 MiB a node of the
  288 they were given, and no node's resident memory passed 2.22 GiB of its 2.25.
- **The gets ran 1.27 times as fast as with no cache**, the caches answering 18 and 29% of them. Lab
  1's eighth answered 84% and ran 1.71 times as fast, at the cost of half the rows.
- **The two captures kept different caches**, 303 and 184 MiB across the nodes, so 3,436 and 3,124
  gets a second. A bound is never raised, so what a node keeps depends on how far its first fill
  took it past its memory.
- **The paced reads' p99 rose**, a median of 2.07 ms against 1.55, their ranges overlapping, and
  their p50 was the same. In lab 1 the eighth's p99 was 4.68 and 6.03 ms in the run its shards
  evicted, against 2.12 to 3.03 with no cache.

### On the SSD leg, its rows fitting with room

Each node's memory was 8 GiB, as in lab 1, so an eighth is 1 GiB. A node on each host, over
1 GbE; the bench's client is on europa.

| Side | Highest resident, any node | Rows evicted | Caches gave back | Caches held, three nodes | Gets a second | p99 ms | Paced reads p50 ms | p99 ms |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| None | 3.70 GiB [3.68–3.72] | none | – | – | 290 [269–313] | 324 [313–331] | 2.03 [1.29–2.23] | 7.40 [6.39–8.42] |
| Default | 4.18 GiB [4.16–4.21] | none | none | 1.00 GiB | 206 [200–211], 0.71, behind | 357 [345–385] | 1.70 [1.49–2.17] | 7.64 [7.33–101] |

With three copies a get is read from the copy of the node that leads its row's group. Each capture
is a deployment of its own and draws again which node leads which groups:

| Capture | Side | Europa's share of the strong reads | Gets a second | Gets the other two nodes led, a second |
| --- | --- | --- | --- | --- |
| 1 | Default | 44.9% | 203 | 111.3 [110.9–112.0] |
| 2 | None | 63.8% | 309 | 111.8 [111.0–112.3] |
| 3 | None | 59.1% | 273 | 111.6 [111.3–111.8] |
| 4 | Default | 46.5% | 208 | 111.8 [111.2–112.2] |

What this says:
- **No row was evicted, and no cache gave anything back.** Each node's rows held 1.01 GiB under a
  budget of 6.72 GiB, where it was 7.72 with no cache, and no node passed 4.21 GiB of its 8.
- **The caches held 1 GiB across the three nodes**, each the objects whose gets it led. They
  answered every get from each capture's second run on, and the devices read 0.04 bytes for each
  byte served, where they read 1.01.
- **The gets were europa's link's and the draw's, not the cache's.**
  - Europa's link received 112.1 to 112.5 MiB/s in every capture, full.
  - In every run of either side the gets the other two nodes led ran 110.9 to 112.3 a second, so
    the link carried those.
  - The rest were the gets europa's own node led, answered over its loopback. Each deployment
    draws how many: europa took 45 and 47% of the strong reads in the default's captures, and 59
    and 64% in none's.
  - So the report's 0.71 is that draw. Lab 1 met the same draw with F102's eighth, the other way
    ([the table rows beside it](read-unit-cache.md#the-table-rows-beside-it)).
- **The paced reads were within**: a p50 of 1.70 ms against 2.03, and a p99 of 7.64 against 7.40.
  - One run of the default's had a p99 of 101 ms.
  - In every run of either side, at least a thousandth of the reads took 110 ms or more: their
    p99.9 was 110.7 to 112.2 ms. In that run more than a hundredth did.
  - Lab 1's rows run on F104's build, the same load, had a p99.9 of 17 to 22 ms on both sides.
    A bisect of the builds put it on the glommio fork's vectored write, which F106 brought: a
    frame became one segment, and one a full link dropped waited out the kernel's 200 ms loss
    probe. Fixed since by a minimum RTO on the replication and control lanes and on a client's
    connection until it carries an object
    ([Resolved #304](../appendix/resolved/lone-frame-loss-probe.md)).

## Tests

| Test | What fails if the feature is reverted |
| --- | --- |
| `shoal-core` `conf::tests::a_nodes_cache_is_an_eighth_of_its_memory_unless_named` | An unnamed cache is not an eighth of node memory on a node with devices and executors, or is not carved out of the rows' budget; one is kept without a device, a node budget or an executor; or a named one, zero included, changes |
| `shoal-core` `objects::store::cache::tests::a_lowered_bound_gives_units_back` | A bound lowered from another thread is not given back to, the unit read is not spared, or a fill passes the lowered bound |
| `shoal-core` `objects::lane::tests::the_caches_give_their_units_back_before_rows` | A node's caches do not lower their bounds by the excess and an eighth, their executors keep the units past it, or caches that hold nothing say they gave |
| `shoal-proto` `stats::tests::a_cache_yield_is_counted_apart_from_evictions` | A yield is not counted, is counted as a pass that evicted, does not add up over shards, or a node's figures from before F107 do not read |
| `shoal` `device_store` `object_buffers_never_evict_rows` (rewritten) | The rows' budget on a node with devices does not carve out the default cache |

## Related

- [F102. A cache of the units reads verified](read-unit-cache.md), whose default this is; its run of
  the table rows beside it.
- [F104. Memory in a capture](memory-in-a-capture.md), which counts evictions and now the caches'
  yields.
- [Resolved #149](../appendix/resolved/node-memory-budget.md): the node's memory budget.
- [The rest of wave two](../object-storage/store-optimizations.md#the-rest-of-wave-two).
