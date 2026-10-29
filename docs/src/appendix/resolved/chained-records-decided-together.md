# 283. A holder's decide discarded a committed write that later writes were staged over

A holder asks a stage's row about it once its stager has had its time, and discards it when the row
is past its sequence and names it nowhere ([F86](../../features/replicated-pools.md#deciding-what-a-stager-left)).
A disk's applies wait behind their batches, so under load a holder can hold a record for each of
several small writes at once, each record standing on the one before, while the row clears past
the older ones. The decide judged each record alone. It discarded the older ones as excluded,
though they were committed writes the newer records needed. The newer records could then never
apply, the position stayed at the label it had, and every later read and write of it failed. Found
by [F96](../../features/chunk-file-classes.md#on-the-lab)'s disk leg, and filed and fixed in one change.

## Symptom

`shoaladm bench` on the lab's disk leg, three copies of 1 MiB objects with `small_writes: 64KiB`,
writing 4 KiB in place (`patch100`), on the build before F96 (`33e9f5c1`), 2026-10-21:

- at one in flight, about 33 writes a second, and nothing wrong;
- at sixteen in flight, about twenty seconds in, every write stopped answering ("idle"). Fifteen
  were refused `NotReady`, and 102 MB were left staged a minute after the run's last answer;
- every run after it was refused `NotReady`, every write;
- the read-back at the capture's end could not read 417 of the 1,024 objects: `{"Unavailable": 417}`.
  None held other bytes than were put;
- all three nodes logged 6,047 folds that did not apply, all between 06:05 and 06:06:
  - 1,829 of them "no record on the slice makes" a label;
  - 4,218 "the stage expects" one label "and the slice holds" another, "from which it cannot make it";
- at 06:25 hyperion declared a stripe lost: "a stripe has no current copy that verifies; it is lost
  and nothing is rebuilt".

One chunk's three positions show the shape. On each, the first failure is a fold's own apply
finding its record gone, and every later one is a record expecting the label that record made:

```text
europa   position 1: no record on the slice makes Label { sequence: 1, .. }
europa   position 1: the stage expects Some(Label { sequence: 1, .. }) and the slice holds Some(Label { sequence: 0, tag: 0 }) (six times)
titan    position 0: no record on the slice makes Label { sequence: 5, .. }
titan    position 0: the stage expects Some(Label { sequence: 5, .. }) and the slice holds Some(Label { sequence: 3, .. }) (four times)
titan    position 0: the stage expects Some(Label { sequence: 7, .. }) and the slice holds Some(Label { sequence: 3, .. })
hyperion position 2: no record on the slice makes Label { sequence: 5, .. }
hyperion position 2: the stage expects Some(Label { sequence: 5, .. }) ... (four times)
```

## Cause

A small write rides its commit: its bytes go into the row's pending bytes, each holder journals a
record of them when asked to fold, and answers once the record is durable. Its apply follows. The
writer then clears the row with the `k + f` holders that folded, so that the row names the write's
label at their positions ([F86](../../features/replicated-pools.md#the-small-write-in-its-commit)).
The next small write's fold is staged over that label: its record expects it, and nothing else.

On a disk, applies wait behind their batches ([F91](../../features/rotational-devices.md)), and at
sixteen in flight they fell further and further behind the folds. A holder then held a chain of
records, the record making label 5 standing on the one making 3, which stood on the one making 1,
with its chunk still at 0. The row named label 5 at its position, since the holder had folded each
write.

Once a record was older than `decide_after`, its holder's node asked the row about it alone
(`DecideTask::run`). The record making label 1 was not named at its position, the row's pending
bytes did not fold from it (the clears had taken them out of the row), and the row was past its
sequence. So it was discarded, as an excluded stage is. The record making 3 went the same way. The
applies waiting behind the batch then found no record for labels 1 and 3, and the record for label
5 could never be laid over a chunk at 0. The position was stuck at a label its row did not name.

That broke [P16](../../object-storage/contract.md#the-contract), which says a committed write not
yet applied is referred to by every later change staged over it.
[X1](../../object-storage/stripe-model.md#what-the-search-found-and-the-repairs)'s holder keeps
every committed record a named label stands on. The decide kept only those the row's pending
bytes fold from, and could not see that another record of its own slice stood on this one.

What followed came from that one fact:
- **A position stuck.** It could make no label its row named, and a read asked another holder.
- **Reads failed.** With every holder of a stripe stuck the same way, a read found none, and the
  read-back's 417 objects were `Unavailable`.
- **Writes failed.** A write that needed a unit it did not carry read it from the stripe first,
  failed the same way, and was refused `NotReady`.
- **A stripe declared lost.** With every holder of a stripe stuck, its positions came to be marked
  missed. A rebuild of one then found no other position its row called current to copy from, and
  declared the stripe lost. Its `corrupt` list was empty because it tried no source.

## Evidence

**Reproduced**, first on the lab and then by two tests written before the fix.

`a_holder_behind_its_folds_keeps_the_records_its_chain_needs` holds node two's disk arm six
seconds an apply (`SLOW_ARM 6000`), with `decide_after` at two seconds. It makes three small writes
through node zero, each folded and cleared before the next. Against `a3d0469e` it failed:

```text
node two's chunk stayed at Some((0, 0)), where node zero's is at Some((5, 1807236853986292075))
```

Node two's log (`SHOAL_CHILD_LOG`, a second run) has the sequence the lab's logs imply:

```text
06:41:07.274944 a stage was decided by its row ... label=Label { sequence: 1, .. } decided=Discarded
06:41:08.274824 a stage was decided by its row ... label=Label { sequence: 3, .. } decided=Discarded
06:41:10.423125 a folded small write did not apply ... error=no record on the slice makes Label { sequence: 1, .. }
06:41:16.425069 a folded small write did not apply ... error=no record on the slice makes Label { sequence: 3, .. }
06:41:16.425322 a folded small write did not apply ... error=the stage expects Some(Label { sequence: 3, .. }) and the slice holds Some(Label { sequence: 0, tag: 0 }) ...
06:41:16.425621 a stage was decided by its row ... label=Label { sequence: 5, .. } decided=Undecided("Refused { code: CannotMake, ...
```

The last line repeats every second until the test gave up.

`a_record_a_later_one_stands_on_is_never_offered_alone` stages three records in a chain on one
slice. Against the slice before the fix, the slice offers all three to their rows:

```text
  left: [(ChunkId { .. }, Label { sequence: 2, tag: 62 }), (ChunkId { .. }, Label { sequence: 3, tag: 93 }), (ChunkId { .. }, Label { sequence: 4, tag: 124 })]
 right: [(ChunkId { .. }, Label { sequence: 4, tag: 124 })]
```

## The fix

**A slice offers its row no record that another record of the same chunk stands on**
(`Slice::undecided`). Such a record is committed, since a writer stages only over a label its
row names or its pending bytes fold from. The record above it is offered instead:
- **if the row names it**, it is applied, which applies the chain beneath it, and the apply's
  `drop_passed` drops what it passed;
- **if it is excluded**, it is discarded, and the record beneath it is offered at the next poll and
  judged alone.

**A fold's apply that a later apply passed no longer warns.** Applying label 5 over the chain
applies 1 and 3 beneath it. Their own applies, still waiting behind the batch, then find no record,
and their chunk at or past their sequence. That is the apply having happened, not the record lost,
so the warning that said "its record stays for its row to decide" is no longer written for it.

## Alternatives rejected

- **Asking the holder whether its chain to the named label passes through the record.** That is a
  new request and a round per record, to ask a slice about its own records, which it can answer
  before it offers them.
- **A longer `decide_after` on disks.** It postpones the race and does not remove it. Under a
  sustained load a disk's backlog of applies grows past any fixed time.
- **A clear that waits for applies rather than folds.** A fold answers once its record is durable
  so that a small write never waits for a disk's batch, which is the latency the small write in its
  commit exists to save ([X8](../../object-storage/small-writes.md)).
- **Keeping every record the row is past, until a scrub.** Most such records are the stages of
  writes that lost their row, which the decide exists to discard, and a stager's time is how long
  that takes.

## Invariants to uphold

- **A record another record of its chunk stands on is never offered to its row alone.** Judged
  alone, a committed record its row has cleared past looks like an excluded stage.
- **A record that stands on another is committed.** The filter depends on it. A writer stages only
  over a label its row names or its pending bytes fold from, and a fold's sources are the row's
  pending bytes.
- **A record's dependents are decided first.** A dependent applied applies the record beneath it.
  A dependent discarded leaves the record beneath to be offered and judged alone.

## Still open

- ~~**The lab's run of writes in place on the disk leg at the fix.** F96's before and after of
  `patch100` stopped at its first capture, the before side's, so F96 is not yet judged on writes in
  place.~~ Run again with the fix on both sides (2026-10-21,
  [F96's page](../../features/chunk-file-classes.md#on-the-lab)). Every capture's end check read all
  1,024 objects back, and no chunk's last failed fold left it behind the label it expected.
- **Records a rebuild passed stay staged, and their applies warn.** A holder too slow to fold
  within `clear_wait` is marked missed and rebuilt past its records. Their applies still run, and
  each finds its chunk ahead: "the stage expects" label 5 "and the slice holds" 23, thousands of
  times on europa in that run. They stay until their row decides them. A rebuild could drop the
  records it passed, as an apply's `drop_passed` does.
- **A few writes in place were refused `NotReady`** at sixteen in flight in that run, alike
  before F96 and after it: 30 in three runs of sixteen. Not traced.
- **Folds on a disk outpace its applies at sixteen in flight.** The backlog grows for as long as
  the load lasts, held by the journal's room and the bytes a slice may have staged. Each held
  record is now kept until it applies.

## Tests

| Test | Where | What breaks if this is reverted |
| --- | --- | --- |
| `a_holder_behind_its_folds_keeps_the_records_its_chain_needs` | `shoal/tests/cluster_fixture.rs` | Node two's decide discards the records its later folds stand on, and its chunk stays at the put's label while the row names the third write's |
| `a_record_a_later_one_stands_on_is_never_offered_alone` | `shoal-core/src/server/objects/store/slice/tests.rs` | A slice offers its row a record a later one stands on, which the decide then discards |

## Related

- [Resolved #239](record-bases.md), whose records keep every label they may be laid over and
  whose `drop_passed` drops what an apply passed. This fix keeps what an apply has not yet reached.
- [F86](../../features/replicated-pools.md#deciding-what-a-stager-left), the decide, and the small
  write in its commit.
- [F91](../../features/rotational-devices.md), a disk's applies a batch at a time.
- [P16](../../object-storage/contract.md#the-contract), and X1's holder, which keeps every
  committed record a named label stands on.
- [F96](../../features/chunk-file-classes.md#on-the-lab), whose disk leg found it.
