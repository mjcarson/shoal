# 299. A rebuild's reads used the cache of read units, and a scrub's listing ignored its device's ceiling

An object executor holds a rebuild's, a move's and a scrub's request at its slice's gate before
serving it ([F88](../../features/recovery-and-moves.md), [F89](../../features/scrub-and-repair.md)).
The gate handed back the request it wrapped, without its class. Both of its callers then served
that as a reader's request. As a result:
- **A rebuild's or a move's read used the cache.** It took units from the cache of read units and
  filled it, which [F102](../../features/read-unit-cache.md) says recovery never does.
- **A light scrub's listing was not paced.** It read every chunk's header without drawing on its
  device's recovery ceiling, which F89 says it does.

Found by reading the served path while planning O104's copies (2026-10-22), reproduced by two
tests, and filed and fixed in one change.

## Symptom

No run showed it:
- the lab's benches neither rebuild nor scrub while they measure;
- F102's test of a background read called `serve` with the request still wrapped, a way in that
  passes no dispatch.

Two tests through the dispatches show it:
- **The cache.** A rebuild's read of a chunk on a holder that keeps a cache filled the cache with
  the chunk's four units. Every later rebuild's read of it, through the holder's own executor or
  across the lane, was answered from memory. The bytes were right, since a unit is held by the
  label it was read at, but recovery's reads pushed out what readers had read. Once a reader's
  read filled the cache, a rebuild copied from memory, not from the device it was rebuilding from.
- **The scrub.** With a device ceiling of one 4 KiB block a second, a light scrub's listing of
  three chunks should take about two seconds. It answered in a millisecond through the holder's
  executor and in 42 ms across the lane, having drawn nothing.

## Cause

`executor::gate` (F89, `c7005464`) did three things:
- it matched `Background(inner)` and `Scrub(inner)`;
- it waited for the slice's gate, the background or the scrub one;
- it returned `*inner`, the request unwrapped.

Its two callers are the executor's dispatch, for a request from this node's shards
(`executor.rs`), and the lane's, for one from another node (`lane.rs`). Each served what `gate`
returned:
- a piece of a whole chunk by its stream;
- anything else through `serve::serve`.

`serve` answers a request as a reader's unless the request is wrapped:
- its `Background` and `Scrub` arms answer the inner request with `foreground` false;
- its `Scrub(List)` arm lists with each header read paced.

The wrapper was gone by then, so neither arm was reached from either dispatch:
- **F89's paced listing**, from its first commit. A listing's gate draws nothing
  (`moved_bytes` of a `List` is 0), since its blocks are drawn as it reads each header.
- **F102's class**, from F102 on.

F102's slice test passed because it reached `serve` by the one way in that kept the wrapper.

## Evidence

**Reproduced** by two tests in `shoal-core/src/server/objects/lane/tests.rs`, written before the
fix and run against `0d8ea4a2`:

```text
---- server::objects::lane::tests::a_background_read_leaves_the_cache_as_it_was stdout ----
assertion `left == right` failed: a background read was served as a reader's, through the executor and across the lane, before and after a reader's read (hits, misses, units filled)
  left: [(0, 1, 4), (1, 0, 0), (1, 0, 0), (1, 0, 0)]
 right: [(0, 0, 0), (0, 0, 0), (0, 0, 0), (0, 0, 0)]
---- server::objects::lane::tests::a_scrub_listing_draws_on_its_devices_ceiling stdout ----
a scrub's listing read its headers without drawing on its device's ceiling: [1.005057ms, 41.909253ms]
```

The first test's four triples, as hits, misses and units filled:
1. **A rebuild's read through B's own executor:** it looked, missed and filled the chunk's four
   units, `(0, 1, 4)`.
2. **The same read across the lane:** a hit, `(1, 0, 0)`.
3. **Both again, after a reader's read:** each a hit.

## The fix

**`gate` keeps the request's class.** It answers the request as it is served (`served_as`):
- a piece of a whole chunk bare, since its stream writes it whatever its class;
- anything else still wrapped, so `serve` answers it by its class.

A rebuild's or a move's read now reads its device and leaves the cache as it was, and a scrub's
listing draws a block of its device's ceiling for each header it reads.

Nothing else moved:
- **Budgets:** `lane::draws` sees through a wrapper.
- **Seals and refusals:** `serve_as` checks a request's chunk and scope, which see through a
  wrapper too, and checks a refused stage again on the inner request.
- **A move's listing** stays unpaced, as it was. A wrapped `Background(List)` is answered by
  `serve_as`'s plain `List` arm.

## Alternatives rejected

- **Returning the class beside the request, and a `serve` that takes it.** Two places would then
  say what a class means. `serve_as` already says it, by its arms, and its other callers send it
  wrapped requests.
- **Drawing a listing's header blocks in its gate**, as `limit` blocks. That would draw for chunks
  the listing may not hold, and all before it reads any.
- **A test of `serve` alone.** That is how the defect hid: a class's effect has to be tested
  through the dispatch that carries it.

## Invariants to uphold

- **A gated request reaches `serve` in its class.** Only a piece of a whole chunk is unwrapped,
  for its stream. `serve_as`'s arms are the one place a class is read.
- **A test of what a class does goes through a dispatch**, the executor's or the lane's, not
  `serve` directly.

## Still open

Nothing of this item. A device's ceiling counts nothing a figure shows, so the listing's pace is
tested by the time it takes, a bound load can only lengthen.

## Tests

| Test | What fails if the fix is reverted |
| --- | --- |
| `shoal-core` `objects::lane::tests::a_background_read_leaves_the_cache_as_it_was` | A rebuild's read through the holder's own executor and across the lane, before and after a reader's read, takes nothing from the holder's cache and fills nothing, where the reader's read misses and fills the chunk's four units. Reverted: `[(0, 1, 4), (1, 0, 0), (1, 0, 0), (1, 0, 0)]` |
| `shoal-core` `objects::lane::tests::a_scrub_listing_draws_on_its_devices_ceiling` | With a ceiling of one block a second, a light scrub's listing of three chunks takes at least 1.8 s through the holder's executor and across the lane, and lists every chunk at its label. Reverted: 1 ms and 42 ms |

## Related

- [F102. A cache of the units reads verified](../../features/read-unit-cache.md);
  [F89, scrub and repair](../../features/scrub-and-repair.md);
  [F88, recovery and moves](../../features/recovery-and-moves.md).
- [A 2+1 get crosses the object lane](../../object-storage/store-optimizations/object-lane.md):
  O104's page, whose plan reads this path.
