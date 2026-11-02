# Item 353's fix, the allocation run: predictions, written before it (2026-11-02)

The view run (`results/m23-i353f-view/`, `-view2/`, two rounds at the shipped purge delay of 100 ms)
counted the Zen1 hosts' long `Big` captures at 19.9 a 10,000: the plan's 6.6, the view's 11.6, 16 of
those 21 in the view's copy of the delta's entries. This run is two rounds on the build where every
collection a capture's step makes is allocated once at its size: a capture's keys and plan make at
most 2 allocations and a scan's start at most 4, whatever the keys, runs and entries, where they made
1 to 5 and 6 to 15 (`shoal-core`'s `alloc_count` tests). A step on the loop still allocates about
eight times: its kept keys, its captures, its range, the scan's four, and the page's figures.

## Predicted

- **The Zen1 hosts' long `Big` captures fall, but not to `Keyword`'s rate**: to 8 to 14 a 10,000. A
  capture makes about half the allocations it did, and a slow path's chance is per allocation, so the
  rate falls about as the allocations do; those left are the allocator's events in the allocations a
  capture still makes.
- **The view's delta copy holds fewer of them**, since it allocates once rather than as it fills; the
  view's runs and the plan hold about as many as before.
- **Europa's and `Keyword`'s long captures stay under 2 a 10,000**, and the cell's p99s and a node's
  peak memory stay within a percent of the view run's.
- **Both preloads complete.** Less confident: item 357 is not understood.

Any of these the run contradicts is said so on item 353's record, beside what it showed.
