# 313. A walk of a long chain of records nested a call for each, and overflowed its executor's stack

A slice keeps a write it staged over another as a record that stands on the label it was laid over,
until its committed apply writes it into the chunk. Two places walked a chunk's records down to the
label its file carries:
- **the apply**, which applies every record beneath the one asked for first;
- **the chain**, which every read, stage, verification and confirmation asks where a label's bytes
  come from.

Each did it by calling itself once for each record beneath. Each call was a boxed future, so no
future grew with the chain, but polling it nested a poll for each one. A chain a few hundred records
deep overflowed the object executor's thread, and a stack overflow ends the process: the whole node,
not the one apply.

Found by F110's run of the object suite with every device a disk, which aborted on an object
executor, and reproduced on one slice.

## Symptom

```text
thread 'shoal-objects-0-88' has overflowed its stack
fatal runtime error: stack overflow, aborting
```

The node's process is gone, with every table shard and object executor in it. Nothing before it is
logged.

It takes a chunk whose staged records outrun its applies, one standing on the next. A disk's slice
issues its applies in batches once its foreground is done ([F91](../../features/rotational-devices.md)),
so a stripe written in place many times between batches leaves a chain. A read or a stage at the top
of it walks the whole chain too.

## Cause

`Slice::apply_locked` applied a record beneath first by calling itself, up to
`MAX_RECORD_UNITS` (960) records deep:

```rust
if let Some(beneath) = beneath {
    self.apply_locked(id, beneath, depth + 1).await?;
}
```

`Slice::chain` did the same for each label a record may be laid over:

```rust
for base in bases {
    if let Some(mut beneath) = self.chain(id, base, depth + 1).await? {
```

Both returned a boxed future, so each level's state was on the heap. But a future that awaits
another polls it from inside its own poll, so a chain of `n` records is `n` nested polls on the
executor's stack, a frame or more each. An object executor's thread has Rust's default stack of 2 MiB.
`chain` also opened the chunk's file and read its header again at every level.

## Evidence

**Reproduced by `a_long_chain_of_records_applies_in_order`** (`objects/store/slice/tests.rs`),
written before the fix. On one slice it stages a chunk of four 4 KiB units whole and applies it.
Then it stages 900 records, each a unit over the label before, reads at the last label, and applies
the last.

On the tree before the fix, at F110's commit and the same at `ceb2f640`, the commit before F109:

```text
test server::objects::store::slice::tests::a_long_chain_of_records_applies_in_order ...
thread 'unnamed-1' (492563) has overflowed its stack
fatal runtime error: stack overflow, aborting
```

With the apply taken out of the test, the read at the last label overflowed by itself, so both
walks are the defect. With the fix the test passes in six seconds, its 900 records read and applied.

The disk pass's abort fits it but is not established by it. It came once in four runs of F110's
object suite on disks, while `small_writes_from_eight_writers_on_one_stripe`, whose eight writers
write in place on one stripe, was running. Without a backtrace, which the abort does not print and
no debugger on the host could take, the walk it overflowed in is not known.

## The fix

**Neither walk calls itself.**
- **The apply** (`apply_locked`) reads the chunk's label once, follows the records down from the
  label asked for to the first that stands on a label the chunk carries, and applies them from the
  bottom in a loop (`apply_one`), each made durable before the one above it is written. The
  refusals are the recursion's: `CannotMake` where no record beneath exists and the chunk is at
  another label, and `NotStaged` for a label nothing makes.
- **The chain** keeps the records on its path in a vector, each with the labels it may be laid over
  and how many of them were tried. It goes back past a record whose labels all led nowhere, as the
  recursion's returns did, and lays the path over the base it finds, the deepest record first. It
  reads the chunk's header once, where it read it at every level.

Both keep the bound of `MAX_RECORD_UNITS` records.

## Alternatives rejected

- **A larger stack for the object executors.** It moves the depth that overflows and keeps a stack
  frame for each record. The bound a chain has is 960 records, whatever the stack.
- **Refusing a stage that would make a chain deeper than some bound.** It would refuse writes a disk
  only fell behind on, and a client would retry them into the same chain.
- **Applying a record at once when what it stands on is unapplied.** It undoes the batching that a
  disk's applies rely on ([F91](../../features/rotational-devices.md)).

## Invariants to uphold

- **No walk of a chunk's records nests a call for each record.** A new walk keeps its path in a
  vector, as these two do.
- **A chain is applied from the bottom, each record durable before the one above it is written.**
- **A chain is read as one snapshot of the chunk's label.** Its callers hold the chunk's lock, so the
  label cannot move during the walk.
- **A chain's depth stays bounded by `MAX_RECORD_UNITS`.**

## Still open

- **The disk pass's abort is attributed by fit.** A stack overflow there that recurs after this fix
  has another cause.
- **It shortened a disk's rounds of a write in place**, since a confirmation no longer reads a
  chunk's header at every level of its chain. That hid [item 312](write-rounds-bounded-by-time.md),
  a write that gave up after sixty-four rounds lost to other writers, which F110's tree still failed
  one run in eight. That item is fixed on its own page.

## Tests

| Test | What fails if the fix is reverted |
| --- | --- |
| `a_long_chain_of_records_applies_in_order` (`objects/store/slice/tests.rs`) | A read at the top of a chain 900 records deep, or its apply, overflows the executor's stack and ends the test binary; or the chain reads other bytes, or applies out of order |
| `units_stage_over_their_base_and_apply_in_order` (`objects/store/slice/tests.rs`) | A short chain applies out of order, or reads other bytes |
| `a_record_applies_over_any_label_it_may_be_laid_over` (`objects/store/slice/tests.rs`) | A record laid over one of its `also` labels is not found or not applied (item 239) |
| `a_record_a_later_one_stands_on_is_never_offered_alone` (`objects/store/slice/tests.rs`) | A chained record is decided apart from the one standing on it (item 283) |

## Related

- [Resolved #239](record-bases.md): a record keeps every label it may be laid over, which the chain
  tries in turn.
- [Resolved #283](chained-records-decided-together.md): records one stands on are decided together.
- [F91](../../features/rotational-devices.md): a disk's applies issued in batches, which let chains
  grow.
- [F110](../../features/store-checks-in-place.md), whose disk pass found it.
