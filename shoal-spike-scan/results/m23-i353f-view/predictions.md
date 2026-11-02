# Item 353's fix, the view's run: predictions, written before it (2026-11-02)

Six of M23's exit's rounds with captures timed by their parts (`results/m23-i353f/`) named no part:
each long capture had one part of 100 to 300 µs, the plan's about as often as the view's. Two rounds
with mimalloc's purge off (`results/m23-i353f-purge/`) took the Zen1 hosts' long `Big` captures from
21.8 to 11.8 a 10,000, those the plan held from 9.1 to 2.3, those the view held from 10.9 to 8.4. This
run is two rounds on the build that ships the decided fix, every node's unit at
`MIMALLOC_PURGE_DELAY=100`, with each capture's view taken apart as the map's scan starts: its delta's
copy (its ranges joined, the entries in them copied), its cursors over the runs and their merge, and
the rest of the view (the call to the table's storage, the view made around its scan, what the
capture dropped as its plan ended).

## What is known before the run

- A view copies about ten delta entries and makes about three run cursors on every capture, long or
  not, and allocates for each: the delta's deque grown as it is collected, a source vector grown by its
  runs, a cursor's range deque a run, the merge's heads, the joined ranges. The rest of the view
  allocates nothing.
- Every node's environment is read from `/proc` after each restart (`env.txt`), so the purge delay
  each ran at is on the record.

## Predicted

- **The plan's long captures stay near the purge-off run's**, about 2 to 4 a 10,000 on the Zen1 hosts,
  since a purge 100 ms after a page empties is ten times rarer than at 10 ms. Less confident than the
  next: a rarer purge may hand back more pages at once.
- **The view's long captures are spread over its delta and its runs, about as each allocates, and its
  rest holds few**: another of the allocator's slow paths inside an allocation, not a part's own work.
  If so, no listed bound fits, and it goes to the user.
- **The view's long captures stay at about 8 a 10,000 on the Zen1 hosts**, as in the purge-off run.
- **Europa's and `Keyword`'s long captures stay under 2 a 10,000.**
- **No change to the cell's p99s or to a node's peak memory beyond a percent of the first run's.**

Any of these the run contradicts is said so on item 353's record, beside what it showed.
