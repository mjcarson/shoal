# 239. A holder could not apply a write it staged over a label its chunk later passed

A stage that carries a row's pending bytes may be laid over the label the row names or over any
label those bytes fold from ([F86](../../features/replicated-pools.md#the-small-write-in-its-commit)).
A slice took such a stage over the first of those labels it could make, and recorded only that one.
When the chunk then reached another of them, a fold applied after the stage arrived, the committed
write could not be applied. The position stayed behind while its row called it current, and every
later write to it was refused. Found by F86's patch leg on the lab, and fixed before its figures
were taken.

## Symptom

`shoaladm bench` writing 4 KiB in place into 32 objects of 1 MiB, eight writes in flight, on the
lab's three nodes with `small_writes: 64KiB`:

- whole runs did nothing for their fifteen seconds ("done: idle"), every write stuck;
- writes were refused `NotReady`, "no current holder of stripe 0 ... answered", the holders
  answering with labels the row neither named nor folded from;
- 256 to 384 KiB were left staged a minute after a run's last answer;
- the read-back after a run could not read one object: `{"Unavailable": 1}`.

The same arms with `small_writes: 0`, every write staged, did none of this.

## Cause

A small write's commit leaves its bytes pending in the row, and the next write carries those
pending units in its own stage. That stage can be laid over the row's named label or over any
label the pending bytes fold from: it rewrites every unit in which those labels differ.
`stage_units` tried each label in turn and recorded the first it could make as the record's
`expects`, forgetting the rest. On a holder the fold had not reached yet, that was the oldest, the
base. The fold then arrived and applied, moving the chunk to the row's named label. Applying the
committed write found the chunk at a label other than the one its record expected, and refused
`CannotMake`. It never applied: the chunk sat at the fold's label, the row named the write's, and
each later stage, which expected the write's label, was refused too.

Two consequences followed from the one fact. A read found the position's holder at a label the row
did not name and could not fold from, so with both holders stuck it failed. And the stuck records,
and each one staged over them, stayed staged.

## Evidence

**Reproduced** on a standalone node by `small_writes_from_eight_writers_on_one_stripe`, written
first: eight writers each make forty 4 KiB writes in place in a 128 KiB range of their own of one
stripe and read their range back after each. Against `4566a64` it failed in its first rounds:

```text
writer 0 round 1: Object { ..., outcome: NotReady, msg: "no current holder of stripe 0 of contended.bin
answered, its row at 3 naming [[Label { sequence: 3, .. }, Label { sequence: 2, .. }], [...]]:
Holder { .. }: Held { label: Some(Label { sequence: 1, tag: 7896423198143595577 }) }; Holder { .. }:
Held { label: Some(Label { sequence: 1, tag: 7896423198143595577 }) }" }
```

The row's labels in the message were added to find this. A log of every stage, apply and discard
on the slice then showed six writes at sequence 2 staged over `(0, …)`, because the fold making
`(1, 52)` had not reached the slice. The fold applied, and the committed `(2, 7)` was refused:

```text
stage pos 1 makes (2, 7) expects (1, 52) also [(0, 0)] -> Ok(())
stage pos 1 makes (1, 52) expects (0, 0) also [] -> Ok(())
apply pos 1 (1, 52) -> Ok(())
apply pos 1 (2, 7) -> Err(CannotMake { expects: Some(Label { sequence: 0, tag: 0 }), have: Some(Label { sequence: 1, .. }) })
```

With the record keeping every label it may be laid over, every write and read succeeded, and the
test then found dozens of records left staged on both slices, the ones applies had passed over. Dropping those is the
second half of the fix. `a_record_applies_over_any_label_it_may_be_laid_over` is the same sequence
on one slice, deterministic; run against `4566a64` it fails at the apply:

```text
applied over the label the fold made: CannotMake { expects: Some(Label { sequence: 1, tag: 31 }), have: Some(Label { sequence: 2, tag: 62 }) }
```

## The fix

**A record keeps every label it may be laid over.** `Record::also` holds the further labels its
stage allowed, newest first, up to six, in bytes 160 to 255 of its block with their count at byte
113. A record with none is the block an older build wrote, so the record format is unchanged. The
chain a read or a stage follows, a start's classification, the read's repair and the apply all
take a chunk at any of a record's labels (`Record::stands_on`).

**An apply drops every record it passed.** A slice applies a label only once its write committed,
so the row has reached that sequence. No other record at or below it can then commit, and the chunk
never moves back to apply one, so `drop_passed` drops them all, returning a whole chunk's file to
its pool.

## Alternatives rejected

**Recording the newest label the slice can make.** The fold had not been staged when the write
arrived, so the newest label the slice could make was the base. The label a chunk reaches next is
not known when a record is written.

**Refusing a stage until the fold arrives.** The writer would need every fold before its stage,
which is the round a small write in its commit exists to save. Refused, it is retried, and the
writers behind it wait in turn.

**Applying the fold and then re-staging the write.** The holder does not have the write's bytes
once its stage is refused, and asking the writer again is a second round per holder.

## Invariants to uphold

- **A record carries every unit in which its labels differ.** That is what makes applying it over
  any of them the same chunk. A stage that does not carry every pending unit may be laid over the
  label it expects and no other.
- **A chunk applies a label only once its write committed.** `drop_passed` depends on it: a record
  at or below an applied sequence is dropped because no commit can name it.
- **A record with no further labels is the block an older build wrote.** Byte 113 stays zero and
  bytes 160 to 255 stay zeros for it.
- **More than six further labels keeps the six newest.** A record whose chunk reaches only an older
  one waits for its row's decide, as an inert record does.

## Still open

- The row's pending chain grows with each small write merged before a clear, and a stage carries
  it as its further labels. Past six, the oldest are not kept. A holder that far behind is the
  missed position a clear marks.

## Tests

| Test | Where | What breaks if this is reverted |
| --- | --- | --- |
| `small_writes_from_eight_writers_on_one_stripe` | `shoal/tests/objects.rs` | Writes stick and reads fail under eight concurrent small writes to one stripe, or records stay staged |
| `a_record_applies_over_any_label_it_may_be_laid_over` | `shoal-core/src/server/objects/store/slice/tests.rs` | A committed write staged over the base cannot be applied once a fold moves its chunk, or a passed record stays staged |
| `a_record_keeps_the_further_labels_it_may_be_laid_over` | `shoal-core/src/server/objects/store/record.rs` | A record's further labels do not survive its journal, or a record with none is not an older build's block |
| `a_record_is_classified_by_the_chunk_it_stages` | `shoal-core/src/server/objects/store/record.rs` | A start calls a record over one of its further labels inert, or misses a torn one |

## Related

- [F86](../../features/replicated-pools.md), whose patch leg found it.
- [Resolved #240](moved-row-not-ready.md), a write whose row moved answered `NotReady`, found beside it.
- [X1](../../object-storage/stripe-model.md#what-the-search-found-and-the-repairs), whose holder
  keeps every committed record a label the row names stands on, the rule `drop_passed` keeps.
