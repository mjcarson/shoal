# 74. A configuration knob the page did not name disappeared from it

Filed and fixed while building [F22](../../features/row-size-benchmarks.md), which added the first
configuration sweep the page's reading order did not already name.

## Symptom

Nothing, until a knob was added. `macro/conf/` arms are selected, run, and written into the capture
whatever they are called; the page that reads them keeps only the knobs it names in one of two
hand-maintained lists. A knob absent from both was captured at full cost and then silently left off
[Configuration and what each setting is worth](../../performance/configuration.md) — no row in the
verdict table, no bar on either chart, and no line anywhere saying a sweep had been dropped.

## Cause

`shoal-bench/src/render/pages/configuration.rs`, in `in_order`. Its doc comment stated the contract
plainly:

> Anything the order does not name is appended rather than dropped, so a knob added to the sweep
> without this list being updated is late rather than missing — the same rule `arms::table_kinds`
> follows.

The body did not do that:

```rust
let mut picked: Vec<&(String, u32, Vec<Arm<'_>>)> = Vec::new();
// the named knobs first, and within a knob the read shares in ascending order
for knob in order {
    let mut matching: Vec<&(String, u32, Vec<Arm<'_>>)> = sweeps
        .iter()
        .filter(|(name, _, _)| name == knob)
        .collect();
    matching.sort_by_key(|(_, read_pct, _)| *read_pct);
    picked.extend(matching);
}
picked
```

There is no second pass. `arms::table_kinds`, which the comment cites as following the same rule,
does have one — the comment was written from the intention and the code from the loop, and nothing
compared them.

## Evidence

**Reproduced.** `a_knob_the_order_does_not_name_is_appended` hands `in_order` three sweeps and an
order naming two of them. Against the unfixed function it returns two, dropping `brand_new_knob`
without error.

The route by which it was found is worth recording, because it is not the route the test takes.
F22 repeats the `latency_buffer` sweep at 8 KiB and 64 KiB, which arrive as separate sweeps named
`latency_buffer @ 8 KiB` and `latency_buffer @ 64 KiB`. Making `in_order` honour its own doc comment
made the storage section's spread chart fail:

```
error: rendering Configuration and what each setting is worth:
chart-conf-storage-spread has 11 groups, which is more than the 8 the canvas holds
```

That is the *fix* being wrong rather than the defect — the two section functions are each handed one
half of the configuration on purpose, so appending there pulled the five resource sweeps onto the
storage chart. It is on this page because it is what the fix has to get right.

## The fix

Two functions rather than one flag.

`in_order` keeps its strict behaviour and its doc comment now describes it, because that is what the
two section functions want: `storage()` is handed `STORAGE_KNOBS` and must return the storage half
and nothing else.

`in_order_with_rest` is the one that honours the original contract, and `what_to_set` — the caller
that is handed *both* lists and builds the recommendation table — is the caller that uses it.

The match became a prefix check at the same time, so a sweep named `<knob> @ <width>` sorts beside
the sweep it repeats instead of at the end of the page.

## Alternatives rejected

**A boolean parameter.** `in_order(sweeps, order, append_rest)` is one function and two behaviours
decided at every call site by a bare `true` or `false`. Two named functions say which contract a
caller is asking for.

**Deleting the sentence from the doc comment.** It would have made the comment true and left the
defect. The sentence is right about what the recommendation table needs: that table is the one
artifact claiming to say what every setting is worth, and a knob missing from it is a claim that is
quietly false.

**Deriving the order from the sweeps rather than declaring it.** Removes the possibility of a knob
being unnamed, and gives up the reading order — which is deliberate, storage before resources and
the durability barrier first, because a reader arriving at that table is choosing settings rather
than reading alphabetically.

## Invariants to uphold

- **`in_order` is for a section, `in_order_with_rest` is for the whole page.** A section handed the
  appending variant pulls the other half of the configuration onto its chart, which the group cap
  turns into a rendering failure — and would otherwise turn into a chart about the wrong thing.
- **The prefix match is what keeps a width repeat beside its own knob.** Making it an equality check
  again sends every repeat to the end of the page, under `in_order_with_rest`, and drops them
  entirely under `in_order`.
- **A doc comment describing a guard is not a guard.** This is the second one found in the same
  change; see [item 73](stage-artifact-overwrite.md), where two constants claimed a drift test that
  did not exist.

## Still open

Nothing from this item. The related gap — that `every_sweep_covers_the_shipped_default` asserts the
sweep brackets what `shoal.yml` says while nothing asserts `shoal.yml` says what the page
recommends — is [F20's](../todos.md#what-f20-left-undone) and is unchanged.

## Tests

| Test | What it pins |
| --- | --- |
| `pages::configuration::tests::a_knob_the_order_does_not_name_is_appended` | The recommendation table holds every sweep in the capture. This is the reproduction |
| `pages::configuration::tests::a_section_takes_only_the_knobs_it_names` | A section still gets its own half, which is what the fix had to not break |
| `pages::configuration::tests::a_width_repeat_follows_the_sweep_it_repeats` | A repeat sorts beside its knob rather than at the end |

## Related

- [F20](../../features/configuration-sweeps.md) — the sweep and the page this is about
- [F22](../../features/row-size-benchmarks.md) — the change that added the first unnamed sweep
- [Resolved #73](stage-artifact-overwrite.md) — found in the same change, the same shape: a comment
  describing a guarantee nothing enforced
