# 25. `CLAUDE.md` described a Shoal that no longer existed

## Symptom

`CLAUDE.md` is the file an agent reads before touching this repository, and it is the only
document in the tree that is read *first* and *always*. Four of its claims were false. Two of them
were false in the worst way available to a reference document — they named an API and a number,
both plausible, both wrong:

```rust
let conf = Conf::new("shoal.yml")?;
```

```yaml
  memory: "4Gi"                # Memory limit (triggers LRU eviction at 60%)
```

There is no `Conf::new`. Eviction does not trigger at 60% of anything.

The other two — `exluded_cores` for `exclude_cores`, and `EphemeralTable` listed as a usable table
type — were fixed earlier and by other work, and are described on their own pages.

## Cause

Nothing keeps `CLAUDE.md` in step with the code. It is not compiled, its code blocks are not
doctests, and no test reads it. Every other document in `docs/src/` has at least the weak
protection of being maintained by whoever writes the change it describes; `CLAUDE.md` is written
once and then only appended to.

The two claims fixed here drifted by ordinary renaming:

- `Conf::new` became `Conf::from_file` (`shoal-core/src/server/conf.rs:324`) when loading a config
  from a path stopped being the only way to build one.
- The 60% figure appears nowhere in the source and probably never did. What the shard actually
  does is trigger on the limit being exceeded at all (`shard.rs:985`) and then free 40% of
  *current usage* (`:850`) — a different trigger and a different quantity, so the sentence was
  wrong twice in six words.

## Evidence

**Established by reading the source**, and then confirmed against it line by line during the
August 2026 review ([Review](../review-2026-08.md)). `grep -rn "fn new" shoal-core/src/server/conf.rs`
returns nothing that takes a path; `Conf::from_file` is the only constructor from a file. The
eviction figures come from reading `Shard::start`'s loop and `Shard::evict_data`, both quoted
above.

There is no reproduction because there is nothing to run: the defect is that a document said
something untrue. That is worth stating rather than glossing, because it is the reason this class
of defect survives so long — nothing fails.

## The fix

Two edits to `CLAUDE.md`:

```diff
-  memory: "4Gi"                # Memory limit (triggers LRU eviction at 60%)
+  memory: "4Gi"                # Memory limit; exceeding it evicts 40% of current usage
```

```diff
-let conf = Conf::new("shoal.yml")?;
+let conf = Conf::from_file("shoal.yml")?;
```

The eviction line is deliberately not a restatement of the mechanism. It says what an operator
setting the number needs to know — that the limit is a threshold rather than a target, and that
crossing it frees a proportion rather than an amount — and leaves the mechanism to
[Memory and Eviction](../../tables/memory-and-eviction.md).

## Alternatives rejected

**Delete the eviction comment rather than correct it.** A YAML example with an uncommented
`memory` key is not wrong, and cannot go stale. Rejected because the thing a reader gets wrong
about `resources.memory` is precisely that it looks like a cap and behaves like a trigger; a key
with no comment invites that reading instead of correcting it.

**Make `CLAUDE.md`'s Rust block a doctest.** It would have caught `Conf::new`. Rejected because
the block is a four-step sketch spanning a server start and a client round trip — making it
compile means making it a real example, which is what `shoal/examples/tmdb.rs` already is. The
useful version of this idea is the *reverse*: shrink the sketch until it is short enough to point
at `tmdb.rs` instead of restating it. Filed in [TODOs](../todos.md).

**Add a test that greps `CLAUDE.md` for identifiers that no longer exist.** Rejected as more
mechanism than the problem justifies, and unsound — it would pass on prose that is wrong without
naming anything, which is what the 60% claim was.

## Invariants to uphold

- **`CLAUDE.md` names APIs it does not exercise.** Renaming a public constructor, a config key, or
  a table type means grepping `CLAUDE.md` as well as `docs/src/`. Nothing else will notice.
- **The eviction comment describes a trigger, not a budget.** If eviction ever gains a high and low
  watermark — which is the obvious next shape for it — that line has to change, because "evicts 40%
  of current usage" is exactly the thing a watermark pair would replace.
- **This page is not a licence to duplicate `docs/src/` into `CLAUDE.md`.** The reason all four
  rows of item 25 were drift is that `CLAUDE.md` restated things that live somewhere else. Every
  claim it makes that it does not have to make is a future entry on this page.

## Still open

Nothing of item 25 remains. The two rows fixed elsewhere kept their references:
[`exclude_cores`](excluded-cores-typo.md) and
[the ephemeral table types](../../features/ephemeral-tables.md).

The general problem — a document that nothing checks — is untouched, and is the reason
[item 21](../known-issues.md#21-constant-and-comment-mismatches) exists for comments and this page
exists for `CLAUDE.md`. Both are the same defect in different files.

## Tests

| Test | What it pins |
| --- | --- |
| *none* | There is no test. `CLAUDE.md` is not compiled, not doctested, and not read by anything in the suite — which is the cause above, restated as a coverage gap |

The nearest thing to coverage is `shoal-core/src/server/conf.rs`'s five config tests, which fail if
`from_file` is renamed again — but they would not notice `CLAUDE.md` still naming the old spelling,
which is the whole point.

## Related

- [18, 50. Core exclusion was ignored and shard placement was random](excluded-cores-typo.md) —
  the `exluded_cores` row of this item, fixed there in both files
- [F9. Ephemeral tables](../../features/ephemeral-tables.md) — the `EphemeralTable` row, fixed by
  making the claim true rather than by changing it
- [21. Constant and comment mismatches](../known-issues.md#21-constant-and-comment-mismatches) —
  the same defect one layer down, in source comments
- [Memory and Eviction](../../tables/memory-and-eviction.md) — what the corrected comment points at
- [Review, August 2026](../review-2026-08.md) — the sweep that closed this
