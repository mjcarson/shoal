# 251. A change of a slice's journal size zeroed its records

A slice's journal is four segments written ahead to a fixed length
([F85](../../features/devices-and-pools.md#the-journal)). Its open made a segment again whenever the
segment's length was not the one its device asked for, on the reasoning that a short segment is one
a crash caught being written ahead. A segment of another whole length is something else: one written
under another `journal` size. A node started after its device's journal size was changed zeroed
every segment, every staged and unapplied record in them with it, and said nothing. Found by
[F91](../../features/rotational-devices.md) while it gave a journal a directory of its own, where a
journal's size and place both move, and filed and fixed in one change.

## Symptom

A device whose configuration changed `journal: 64MiB` to the default 256 MiB, restarted while a
slice held a record - a stage whose commit had not reached it, or a committed write not yet applied -
came up holding no record. A stage acknowledged before the restart was gone: a read at its label
was answered with the chunk's older label, and an apply of it was refused `NotStaged`. Nothing in
the log said a record was lost; the start logged a segment made, as it does on a first start.

## Cause

`Journal::open` judged each segment by `metadata.len() == segment_bytes`, the length the device's
configuration asks for now. Any other length went to the branch that makes a segment: a staged file
zero-filled to the new length and renamed over the old segment. A short segment, the case the
comment named, holds no record yet. A segment of another full length holds whatever the last run
wrote there. F85's configuration made `journal` a setting any operator may change, and nothing held
a changed size back until the journal was empty.

## Evidence

**Reproduced** by `a_journal_is_resized_only_once_it_holds_nothing`, written with F91 and run against
the open as it was before (`journal.rs` judging a segment by the length asked, its one changed line
put back): a record staged in a journal of 1 MiB segments, the slice closed, and opened again asking
for 2 MiB.

```text
thread 'unnamed-1' panicked at shoal-core/src/server/objects/store/slice/tests.rs:907:9:
assertion `left == right` failed
  left: 2097152
 right: 1048576
test result: FAILED. 0 passed; 1 failed
```

The segment had been made again at the new length. With that assertion set aside, the next one
showed what it cost:

```text
thread 'unnamed-1' panicked at shoal-core/src/server/objects/store/slice/tests.rs:908:9:
assertion `left == right` failed
  left: []
 right: [Label { sequence: 2, tag: 62 }]
```

The acknowledged stage was gone, on europa, 2026-10-16.

With the fix the journal keeps its 1 MiB segments and the record while it holds one, and the start
after its apply makes it again at 2 MiB.

## The fix

**A journal is opened at the length its segments have**, read from its first segment, whatever the
device asks. A slice whose journal is at another length than asked recovers it as it is. If the
recovery leaves it holding no record, the slice closes it, removes its segments and opens it again at
the length asked. Otherwise it keeps the length it has, warns that it does, and resizes at a start
that finds it empty. A crash between the removal and the open finds the segments missing and makes
them, which loses nothing, since the journal held no record.

## Alternatives rejected

**Refuse the node's start while a journal of another length holds a record.** Loud, but the only
remedy would be to put the old size back, start, let the records apply and change it again, which is
what keeping the old length until the journal is empty does by itself.

**Copy the live records into segments of the new length.** A rewrite of the journal at a start, with
its own crash window, for a change an operator makes once.

**Record the segment length in a marker and trust it.** F91 does write it in a slice's journal marker
when the journal is elsewhere; the segments' own length is the one fact that is always there, the
journal under a slice included.

## Invariants to uphold

- **A segment is never zeroed while it may hold a record.** Only a segment shorter than two blocks,
  or not a whole number of them, is taken for one a crash caught being written ahead.
- **A journal is resized only by a start whose recovery left it holding no record.**

## Still open

Nothing of this defect.

## Tests

| Test | Where | What breaks if this is reverted |
| --- | --- | --- |
| `a_journal_is_resized_only_once_it_holds_nothing` | `shoal-core/src/server/objects/store/slice/tests.rs` | A change of a journal's size zeroes its records, or a journal holding none is never made at the new size |
| `a_start_recovers_what_was_staged` | `shoal-core/src/server/objects/store/slice/tests.rs` | A journal at its asked length loses what a start should find |

## Related

- [F85](../../features/devices-and-pools.md#the-journal), which built the journal.
- [F91](../../features/rotational-devices.md), which moved a journal to an SSD and renamed the size
  `journal_size`, and found this on the way.
