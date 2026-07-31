# 9. Recovery and compaction panicked on orphaned update intents

Fixed for unsorted tables, which is where both panics lived. The sorted path never had an
equivalent — its `apply_intents` has always seeded from the partition's archive copy.

## Symptom

A shard that would not start. Both panics were on the startup path, which is the worst place
for one: a shard that cannot start cannot be recovered without deleting data.

## Cause

Two sites, the same underlying assumption — that an update always has a base row within reach:

```rust
// TODO handling a partition missing
None => panic!("Missing partition?"),
```

fired during startup replay when an update's base partition was not resident and `scan`'s
`load_partition_direct` found nothing.

```rust
UnsortedIntents::Update(update) => match &mut maybe_partition {
    Some(partition) => partition.update(&update),
    None => panic!("Applying update to no partition?"),
},
```

fired during compaction when a log contained an update whose insert had been compacted in an
earlier generation. `apply_intents` for unsorted tables started from `None` rather than from
the partition's current archive copy, so a perfectly ordinary sequence — insert in generation
3, compact, update in generation 4 — crashed the shard when generation 4 was compacted.

The second was made freshly reachable by [item 4](unsorted-disk-consultation.md): once an
update against an archive-only partition could succeed, it wrote exactly the orphaned intent
that panicked. The two had to be fixed together.

## The fix

`UnsortedPartition::apply_intents` seeds from `loaded.remove(&key)`, matching the sorted
implementation:

```rust
// start from this partitions current archive copy if it has one, since an
// update can target a row whose insert was compacted generations ago
let mut maybe_partition = loaded.remove(&key);
```

`.../persistent/unsorted.rs:1037`

and both sites now warn and skip the intent instead of panicking
(`.../persistent/unsorted.rs:1014`, `:1056`).

## Alternatives rejected

**Keep the panic but make it a `ServerError`.** It is the same outcome — a shard that will not
start — dressed up. An intent whose base row is genuinely gone is a data loss that has already
happened; refusing to start does not undo it and does prevent recovering everything else.

## Invariants to uphold

- **Compaction must seed from the partition's current archive copy.** An intent log holds a
  delta, not a whole partition, and the insert an update refers to may be generations old.
- **Startup must not panic on a malformed or orphaned intent.** The startup path is the
  recovery path; a crash there is unrecoverable without deleting data.

## Still open

An intent whose base row is genuinely gone is dropped with only a `warn!`, and nothing counts
it. That is the same observability gap as the mid-log corruption case in
[Recovery](../../storage/recovery.md#truncation-and-corruption), and it is tracked there rather
than here.

## Related

- [Recovery](../../storage/recovery.md)
- [Compaction](../../storage/compaction.md#3-apply)
