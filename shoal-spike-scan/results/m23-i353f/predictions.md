# Item 353's fix, its attribution run: predictions, written before it (2026-11-02)

Item 353 (`docs/src/appendix/known-issues.md`) is attributed: the light scan steps past 100 µs on their
own cpu are the steps' own work. `Big`'s planned captures pass 100 µs 32 to 34 times in 10,000 on the
Zen1 hosts at a mean of 14 µs, `Keyword`'s on the same cpus 0.6 to 1 time at 11 µs; a long `Big`
capture looked at 10 to 14 keys against every capture's 9, at 160 to 200 µs of its own cpu with no
fault. This run, six of M23's exit's rounds on the counts' build, times every capture by its parts by
the wall (`CaptureClock`): its keys looked at, kept, selected and sorted; its plan of each key; its
archive's view (`ArchiveMap::scan_range`: the delta's entries in its range copied, a cursor a run);
and the rest of its step. Each class counts its long captures, past 100 µs on their own cpu, by the
part that took the most of each (`capture_parts.held_by`), with the keys planned, the delta entries
copied and the runs.

## What is known before the run

- A planned `Big` page's capture plans at most its page's limit of keys and copies nothing; its keys
  part goes through every resident key of its tablet, about nine, which a long capture barely exceeds.
- Its view copies the delta's entries in `[start, end]`, the rest of its tablet when the page did not
  fill on resident keys, and takes one cursor a run, each a small allocation. The delta holds at most
  16,384 entries a shard's map before it is written as a run; a shard serves about 820 of `Big`'s
  tablets, so a tablet's share of a full delta is about 20 entries.
- Every node runs on mimalloc. Item 353's run counted 3 TLB shootdowns a second on europa's shards'
  cpus against 12 to 26 on the Zen1 hosts', and no long Zen1 capture took a page fault.

## The outcomes, and what each would show

1. **The view, by its runs**: the long captures held by their view, with more runs a capture than
   every capture's mean. Bound: the cursors made at the scan's first read, off the loop.
2. **The view, by its delta**: held by their view, copying many more delta entries than the mean.
   Bound: a view copies at most a page's worth, the page ending at the last entry copied.
3. **The view, by neither**: held by their view at the same runs and delta entries as every capture:
   the allocator's slow path inside the view's allocations, or memory the view touches cold. Not a
   listed bound: brought to the user.
4. **The plan**: held by their plan at about the same keys planned: a lookup's or a key's own case.
   Bound: the plan ended at a deadline, the page at its last planned key.
5. **The keys**: held by their keys at about nine keys looked at: a tablet's map gone sparse, its
   whole capacity walked. Bound: a sparse map shrunk.
6. **Spread, or the rest**: no part holds most of them, or the rest does: the allocator, or the
   dispatch. Brought to the user.

## Predicted

- **The view holds most of `Big`'s long captures on the Zen1 hosts**, at least two in three, since the
  plan of nine keys and their keys are the parts `Keyword`'s captures share and its captures do not
  pass. This is the most confident of these.
- **Within the view, neither its runs nor its delta set it apart (outcome 3)**: a long capture's view
  takes the same runs and about the same delta entries as every capture's, and its time is the
  allocator's or cold memory's, which the Zen1 hosts' smaller caches and more frequent shootdowns
  make longer. Less confident: outcome 1 is the one that would show a long view with more runs, and
  the record holds no count of `Big`'s runs to rule it out.
- **Europa's `Big` captures pass about once in 10,000, held by the same part as the Zen1 hosts'.**
- **`Keyword`'s few long captures are spread over the parts**, too few to name one.
- **No long capture is held by its rest**: the dispatch and the page made are a few lookups and a
  move.

Any of these the run contradicts is said so on item 353's record, beside what it showed.
