# Delivered Features

Capabilities that have been built, kept for the same reason [Resolved Issues](../appendix/resolved-issues.md)
are kept: the reasoning behind a feature is worth more than the feature. Each page states what
the thing does, the choices its design rests on, what was rejected on the way, what it still
cannot do, and — the part that matters when changing this code later — the invariants it depends
on.

Item numbers are prefixed `F` and are **never reused**, the same rule [Known
Issues](../appendix/known-issues.md) and [Optimizations](../appendix/optimizations.md) follow.

| # | Feature | What it does |
| --- | --- | --- |
| F1 | [Sort-key range predicates](sort-key-ranges.md) | A sorted get or exists can bound its rows by a range of sort keys instead of naming them, in memory and in an archive, which makes an exclusive lower bound a cursor and paging a large partition cost a page |
| F2 | [Projections on get queries](projections.md) | A get can ask to be answered with a named subset of a tables fields instead of whole rows, which reads only those fields out of an archive rather than deserializing every field of every row it returns |
| F3 | [A three layer performance harness](performance-harness.md) | Criterion micro-benchmarks, the `tmdb` example, and a working `hotpath` profile, captured into committed JSON and judged against a frozen baseline and a trailing one, so an optimization can be shown to have worked rather than argued to have |
| F4 | [Archives are validated once, not once per read](validated-archives.md) | A partition read off disk is validated when the read lands and held in a form that carries that fact, so a query against an evicted partition seeks it directly instead of re-running rkyv's validator over the whole buffer first — and the archived read path became reachable from a test for the first time |
| F5 | [The flushed sweep runs on a wakeup, not on every message](flushed-sweep-gate.md) | A shard asks "what is durable now?" when a write has landed or a log is due to rotate, instead of after every message it handles, which took that sweep from 711,638 calls to 21,279 in the same workload without moving when a rotation fires |

## How a feature gets written down

Not every change earns a page. A bug fix gets one when it had a wrong mental model behind it; a
feature gets one when it introduced a model that was not there before, because the next person to
touch that code will reason from the model rather than from the diff. The sections are always the
same:

**Context** what was not possible, and why it mattered. **What it does**, across every front end
that can reach it. **Design choices** and **Alternatives rejected**, together, because a design is
only understandable next to what it is not. **Limitations**, so that the gap between what it looks
like and what it does is written down rather than discovered. **Invariants to uphold**, which is
the section to read before changing the code it describes. **Performance**, naming what got
cheaper and what did not. **Tests**, naming what fails if the feature is reverted.
