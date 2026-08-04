# 11, 12, 37. The ring panicked on an empty lookup and never smoothed load

Three filed defects with one cause. All were reported against the vnode ring, and all were
symptoms of the same mismatch: consistent hashing was solving a problem Shoal does not have,
while failing at the one it does. Item 37 was found still open during the doc sweep for the
other two, and the same replacement had already fixed it.

## Symptom

**The filed half.** A query routed before any `ServerMsg::Join` had been processed panicked the
shard on an `.unwrap()`. A client connecting during the startup window was enough.

**The unfiled half, which is worse.** The same window has a silent failure mode that was never
reported. `Shard::init` spawns the client listener *before* it broadcasts its join
(`shard.rs:423-425`), and every shard is its own coordinator. A shard holding a *partial* ring —
one join processed out of sixteen — does not panic. It routes **successfully, to the wrong
shard**, and the write lands where no later lookup will look for it. No error, no warning, and
the data is simply not there afterwards. The panic was the visible symptom of a window whose
other outcome was undetectable.

**The balance half.** Each shard laid down 1000 vnodes but received the share of a single one.
On a 16 shard node the busiest shard owned about 3.5 times the mean.

**The idempotence half (item 37).** `Ring::add` appended to `self.shards` and laid down 1000
vnodes every time it was called, with no check for a name it already knew. A `Join` delivered
twice for one shard counted it twice and repointed its vnodes at the new index, orphaning the
old entry. The ring's shape depended on exactly-once delivery, which nothing guaranteed — it
held only because `join_cluster` had a single call site.

## Cause

Three independent mistakes in one file, and one design mismatch underneath them.

**The panic.** `find_shard` walked to the next vnode at or above the key and fell back to the
lowest vnode when the key was past the last one:

```rust
None => self.ring.range((Included(&0), Excluded(&partition))).next().unwrap(),
```

`ring.rs:61-65`. The fallback is correct wrap-around and its `next()` is `None` in exactly one
case: an empty ring. Each shard built its ring from broadcasts, so an empty ring was a real
state and not a defensive impossibility.

**The balance.** Every shard laid its vnodes down at the same fixed stride, with only the
starting offset varying by name hash:

```rust
let mut vnode = hasher.finish();
for _ in 0..1000 {
    vnode = vnode.wrapping_add(RING_JUMP);
    self.ring.insert(vnode, shard_id - 1);
}
```

`ring.rs:36-43`, with `RING_JUMP = u64::MAX / 1000`. Since `1000 × RING_JUMP ≈ u64::MAX`, every
shard's vnodes formed an evenly spaced comb wrapping the ring exactly once, and **all N combs
shared one period**. The ring therefore repeated every `RING_JUMP`, with exactly N vnodes per
period at offsets fixed by the N name hashes. A key's owner was decided by its position within a
period, so each shard owned the identical arc in all 1000 periods — the load distribution of a
single vnode, at the cost of a `BTreeMap` of 1000 × N entries per shard.

**The idempotence.** `Ring::add` had no check for a name it already held:

```rust
self.shards.push(shard);
let shard_id = self.shards.len();
```

`ring.rs:32-34`. Every call appended and laid down a fresh 1000 vnodes, so a repeated `Join`
counted its shard twice and left the earlier `shards` entry with nothing on the ring pointing at
it. It held only because `join_cluster` had exactly one call site — an invariant enforced by
where the code happened to be called from, not by anything in the ring.

Underneath all three: a consistent hash ring exists to minimise *movement* when membership changes.
Shoal has no membership changes. Shard count is fixed at startup and already welded into the
on-disk layout (`Shard-N-active`, `maps/Shard-N`). The ring was paying a 16,000-entry `BTreeMap`
walk per partition key for a property nothing used.

## Evidence

**Reproduced**, before any fix, by three tests written against the vnode ring and run on it:

```
running 3 tests
test server::ring::tests::empty_ring_does_not_panic ... FAILED
test server::ring::tests::ring_does_not_repeat_every_jump ... FAILED
test server::ring::tests::vnodes_smooth_the_load ... FAILED

---- server::ring::tests::empty_ring_does_not_panic stdout ----
thread panicked at shoal-core/src/server/ring.rs:65:18:
called `Option::unwrap()` on a `None` value

---- server::ring::tests::ring_does_not_repeat_every_jump stdout ----
thread panicked at shoal-core/src/server/ring.rs:156:9:
1000/1000 keys route the same one RING_JUMP away, so the ring repeats

---- server::ring::tests::vnodes_smooth_the_load stdout ----
thread panicked at shoal-core/src/server/ring.rs:126:9:
busiest shard owns 3.54x the mean, so vnodes are not smoothing load
```

Two numbers are worth keeping. The imbalance measured **3.54×**, against a predicted ~3.4× —
the busiest of N shards owns about `H_N/N` of the ring when the vnodes are effectively one
each, which is `21.1%` against a `6.25%` mean at N=16. And the repeat measured **1000 out of
1000**: not a weak periodicity but a total one, confirming the vnode count changed nothing
whatsoever about ownership.

The silent misroute in the same window was established by reading `Shard::init` and was not
reproduced — it is a race, and a test that lost it would prove nothing.

## The fix

The ring was replaced by a **tablet map**, which inserts one indirection and makes the second
step a lookup instead of a hash:

```
partition key ──gxhash──> token ──top bits──> tablet id ──array lookup──> owner
                                  (arithmetic)            (stored, movable)
```

```rust
pub struct Ring {
    /// The shard that owns each tablet, indexed by tablet id
    tablets: Vec<u16>,
    pub shards: Vec<ShardInfo>,
}
```

Four things follow, and three of them are structural rather than checks:

- **The empty ring became unbuildable.** `Ring::new(shard_count)` is the only constructor,
  `Default` is gone, and a zero shard count is a `ShoalError::NoShards`. `find_shard` is
  therefore total: it indexes an array that `new` filled completely into a shard list `new`
  refused to leave empty. The `.unwrap()` did not become a `?` — the state it failed on cannot
  be constructed.
- **The partial ring became unbuildable too**, which is what closes the silent misroute. Shard
  count is known at `shard::start` from `cpus.len()` *before any shard spawns*, so every shard
  builds a complete, identical map in `Shard::new` and no shard ever routes against a map that
  is still filling. This is the half that was never filed and it is the more important half.
- **Balance became exact.** Tablet `i` goes to shard `i % shard_count`, so shares differ by at
  most one tablet — 256 each at 4096 tablets over 16 shards, against the measured 3.54× before.
- **The lookup became a shift and two indexed loads** into an 8 KiB array, replacing a `range()`
  walk over 16,000 `BTreeMap` entries. That closes [O6](../optimizations.md#routing-and-memory),
  which had predicted both the win and that it had to be taken together with item 12.

`ServerMsg::Join` is kept as the membership seam. `Ring::add` now ignores a shard already in the
map — which node-locally is every join — and warns for an unknown one, because placing it would
need tablet migration and a rebalancer, neither of which exists. **That is item 37 fixed as a
side effect**: `add` is idempotent because ignoring a known name is the whole of its node-local
behaviour, so exactly-once `Join` delivery is no longer something the ring's shape depends on.

**The `find_shard` signature did not change**, so `group_by_shard`, `SortedQuery::split_by_shard`
and `UnsortedQuery::split_by_shard` are untouched.

A separate consequence worth naming: because `Ring::new` builds `ShardInfo::new(i)` in order,
`shards[i].mesh_id() == i` on every shard. The old ring stored an *arrival order* index, so two
shards genuinely held different indices for the same shard and their agreeing required an
argument (it was correct, but it needed one). Now the maps are simply identical.

Finally, `StorageMeta` (`server/meta.rs`) records the shard count a storage directory was written
by, checked once in `ShoalPool::start` before any shard spawns. Changing `resources.cores` between
restarts now refuses to start instead of silently stranding every partition that moved — a trap
that predates this change and was previously only a documented limitation.

## Alternatives rejected

**Keep the ring; return `Option` from `find_shard` and hash `(name, i)` per vnode.** This is the
small fix, and it is what the two items literally asked for. It was rejected on three counts: it
leaves the 16,000-entry `BTreeMap` walk on the hot path; it gets ~1.07× balance where a tablet
map gets exactly 1.00×; and — decisively — it leaves the silent misroute standing, because a
partial ring is still a valid ring. Making callers handle `None` also spreads a startup concern
across three query modules that have nothing to do with startup.

**Tablet ids from `partition % TABLET_COUNT`.** Rejected because it cannot be split. Taking the
id from the *top* bits means a tablet later divides in two by consuming one more bit, its keys
staying contiguous and no other tablet disturbed. Modulo reshuffles every assignment the moment
the tablet count changes. Nothing splits tablets today; this was chosen now because it is the one
decision here that is expensive to reverse.

**Store a replica set per tablet now** (`Vec<SmallVec<[u16; 3]>>` instead of `Vec<u16>`), so
replication later widens data rather than changing shape. Rejected as premature: it costs the
flat array's cache behaviour, makes every call site index `[0]` and so *read* as though it
handles RF>1 when it does not, and anticipates only a small part of replication — write fan-out,
quorum acknowledgement, read repair and consistency levels are the actual work, and a list in the
ring shapes none of them. Widening `u16` later is a compiler-checked change confined to one file.

**Re-key storage by tablet in this change**, so a tablet becomes a movable unit and core-count
changes stop stranding data. This is genuinely the most future-proof option and it is on the
expensive-to-reverse side, being an on-disk format. Rejected on sequencing: what a tablet-keyed
layout should look like depends on how migration streams data, and that protocol does not exist
yet, so building it now risks migrating twice. Filed in [todos](../todos.md#rebalancing) with the
tablet id — the name that makes it possible — now in place.

**A mapping-version field in `StorageMeta`, to detect directories written by the old ring.**
Rejected deliberately: there is no migration off the vnode ring and none is wanted. An old
directory has no marker, gets one written, and its data is mis-mapped. Existing directories are
removed, not upgraded.

## Invariants to uphold

- **The tablet map is complete before any query can be routed.** This is what makes `find_shard`
  total and what closes the misroute window — both by construction, neither by a check. Anything
  that reintroduces incremental map construction (dynamic membership, a rebalancer, a remote
  join) reintroduces *both* defects, and must make routing wait for a complete map rather than
  route against a partial one.
- **A tablet id comes from the high bits of the partition key.** The low bits must be the ones
  varying within a tablet, or splitting a tablet stops dividing that tablet and starts
  redistributing everything. `Ring::tablet_of` is the only place this is decided.
- **`shards[i].mesh_id() == i`.** `Ring::new` builds `ShardInfo::new(i)` in order and the tablet
  map stores indices into that list. `group_by_shard` deduplicates on `mesh_id()`, so a
  construction that broke this alignment would silently route to the wrong shard.
- **`TABLET_COUNT` is a power of two and far larger than any shard count.** The first is what
  makes the id a bit-shift and a split a single extra bit; the second is what keeps `i %
  shard_count` even.
- **A storage directory is only ever read back by the shard count that wrote it.** Ownership
  derives from the shard count and a shard's data is stored under its own name, so the two must
  agree. `StorageMeta::claim` enforces this; there is no migration behind it.

## Still open

**Tablet assignment is derived, not persisted.** `Ring::new` recomputes `i % shard_count` on
every start, so the map is only movable in principle — nothing can currently move a tablet and
have that survive a restart. Persisting the map is the first half of rebalancing and is filed in
[todos](../todos.md#rebalancing).

**Shard count is still part of the on-disk format.** Intent logs are `Shard-N-active` and each
shard has its own archive map, so changing `resources.cores` still cannot work — it is now a
startup error rather than silent loss, which is a better failure but not a fix. Tablet-keyed
storage is the path and is filed with its reasoning.

**`StorageMeta` only guards the default storage root.** A per-table storage override pointing at
a different directory is unguarded, so a mixed configuration can still strand the overridden
table's data silently. Filed as
[item 43](../known-issues.md#43-the-storage-marker-only-guards-the-default-storage-root).

**`StorageMeta` does not guard a directory written before it existed.** A directory with no
marker is claimed rather than refused, and every directory written before this change has none —
while this change is also what moved every partition to a different shard. So the one case the
marker most needed to catch is the one case it cannot see. Filed as
[item 46](../known-issues.md#46-an-unmarked-storage-directory-is-claimed-rather-than-refused).
The marker's `format` field is now checked, which was a separate hole in the same guard
([item 45](storage-marker-format.md)) and does not help with this one: that is a marker that is
wrong, this is a marker that is absent.

**`Ring::add` cannot place an unknown shard.** It warns and ignores. That is the honest behaviour
until a rebalancer exists, but it means the multi-node seam is a seam and not a partial
implementation.

## Tests

| Test | Fails without |
| --- | --- |
| `empty_ring_is_not_constructable` (`shoal-core/src/server/ring.rs`) | Item 11. `Ring::new(0)` must reject the state that used to panic, since the fix is that the state cannot exist |
| `tablets_are_evenly_owned` (`ring.rs`) | Item 12. Per-shard tablet counts differing by more than one, across seven shard counts |
| `every_shard_owns_a_tablet` (`ring.rs`) | A shard count that starves a shard entirely, checked for every count up to 64 |
| `tablet_id_comes_from_the_high_bits` (`ring.rs`) | The split invariant. Catches a change to `tablet_of` that takes low bits, which would work today and break the first split |
| `find_shard_is_stable_across_rings` (`ring.rs`) | Two shards disagreeing about an owner — the property the old arrival-order index needed an argument for |
| `find_shard_handles_the_whole_key_space` (`ring.rs`) | An indexing error at either end of the key space |
| `a_different_shard_count_is_refused`, `the_same_shard_count_is_allowed_back`, `an_unclaimed_directory_is_claimed` (`server/meta.rs`) | The storage marker, at unit level |
| `a_changed_shard_count_refuses_to_start`, `the_same_shard_count_restarts` (`shoal/tests/storage_meta.rs`) | The same, end to end through `ShoalPool::start` — a real server must refuse the changed count and accept the unchanged one |
| `get_across_shards_returns_every_row`, `get_applies_its_limit_across_shards` (`shoal/tests/persistent_sorted_table.rs`) | Nothing directly — they pin that replacing the routing function did not break multi-shard fan-out, which is the main risk of this change |

## Related

- [Partitioning and the Tablet Map](../../architecture/partitioning.md) — the tablet model in full
- [O6](../optimizations.md#routing-and-memory) — the lookup cost this removed, and what it got right and wrong
- [Rebalancing](../todos.md#rebalancing) — tablet-keyed storage, and persisting the map
- [Test Coverage](../test-coverage.md)
