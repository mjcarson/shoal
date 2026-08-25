# 20. Orphaned source files

The second half of item 20, and the one that stayed open when
[the storage tests](storage-tests.md) were turned back on. With this the item is closed in full.

## Symptom

`shoal-core/src/server/cursor.rs` and `shoal-core/src/server/response.rs` were not declared in
`shoal-core/src/server.rs` and so were not compiled. Both referenced APIs that no longer existed —
`crate::ShoalRow`, `rkyv::AlignedVec` — and neither would have built if anything had tried.

## Cause

They were left behind by refactors that moved what used them and did not remove them. Nothing
declares a module by accident, so nothing warns when one stops being declared: an undeclared file
is simply not part of the crate, and `cargo check` has no opinion about files it never reads.

## Evidence

**Established by reading, and the interesting half by reading the file rather than the entry.**
The page recorded more than "these are dead". It said `response.rs` was **not merely dead**,
because it defined

```rust
pub struct Responses<'a, R> {
    data: Vec<Option<Vec<&'a AlignedVec>>>,
    ...
}
```

— a response that *borrows* its rows instead of owning them, which is precisely the shape
[O2](../optimizations.md#o2-every-returned-row-is-copied-at-least-twice) was blocked on. The entry
warned that whoever started O2 would either rediscover the file by accident or reimplement it, and
would have no way to tell whether it was a design that had been tried and abandoned or one that
was never finished — because nothing compiled it, so it could not even be said whether it still
type-checked.

Opening it while starting O2 answered that, and the answer was the opposite of what the warning
implied. **The whole file is twenty-eight lines**: a struct, a `with_capacity` that fills in a
`PhantomData`, and nothing else. There is no borrowing design in it. There is no method that reads
a row, no lifetime that has to be discharged anywhere, and nothing that would have informed the
design that replaced it. The "prior art" was a name and a field.

## The fix

Both files deleted, in the change that closed the entry they were held for
([F27](../../features/grouped-responses.md)).

## Alternatives rejected

**Declaring `response.rs` behind `#[allow(dead_code)]`**, which is what the entry offered as the
other acceptable outcome. Right for a file carrying a design worth keeping honest; wrong for this
one, because there is no design in it to keep honest and compiling it would only have meant fixing
twenty-eight lines of stub against APIs it was written before.

**Keeping it until F27 landed, to check the real design against.** The real design borrows through
`RowRef<'a, T>`, whose whole point is that its archived type *is* the row's archived type. That has
no relationship to `Vec<Option<Vec<&AlignedVec>>>` beyond both containing an ampersand.

## Invariants to uphold

**A file that is not declared is not checked, and nothing will tell you.** This is the property
that let two files rot in the tree across several refactors. There is no test that would have
caught it and none is added here, because the check that would catch it — walking `src/` for `.rs`
files nothing declares — is a lint rather than a test, and the two files it would have found are
now gone.

**An entry that describes code should be re-read against the code before it is acted on.** The
value of this item was almost entirely in a claim about `response.rs` that reading the file
disproved. It was filed in good faith from the struct definition, which is the part of the file
that looks like a design.

## Still open

Nothing. Both files are gone and the entry is closed in full.

## Tests

| Test | Breaks if |
| --- | --- |
| `cargo check --workspace --all-targets` | either file is restored without being declared, or declared without being fixed |

There is no test naming these files, and there should not be: a deleted file is not something a
test can assert about. What would have caught the original defect is a lint over undeclared
sources, which is filed in [TODOs](../todos.md) rather than written here.

## Related

- [F27](../../features/grouped-responses.md) — the feature that closed the entry these were held
  for, and the reason the file was finally opened
- [O2](../optimizations.md#o2-every-returned-row-is-copied-at-least-twice) — the entry
  `response.rs` was kept as prior art for
- [Row size and what it costs](../../tables/row-size.md#the-payload-is-walked-about-six-times-per-round-trip)
  — the copy accounting this was found while tracing
