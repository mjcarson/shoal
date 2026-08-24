# Resolved #79. A capture of one layer decided what every page drew

## Symptom

`shoal-bench render` draws all eleven generated pages from one capture, chosen once for the whole
render. A capture does not have to measure everything: `--layer micro` produces no macro layer at
all, and `--group` or a filter produces part of one. Whichever capture was chosen decided what
**every** page drew, so a capture of one layer emptied the pages of the others.

Rendering with a micro-only capture as the current one takes
[Read/write mixtures](../../performance/grid.md) from 59,287 bytes to 3,459, and the page then
says:

> No capture in `docs/perf/runs/` measured a read/write mixture, so there is nothing to draw here
> yet. Take one with `shoal-bench run --label <name>`, which captures every layer.

while eleven captures with a macro layer sit in that directory. The same happens to
[Row size](../../performance/row-size.md) (142,176 → 5,305),
[Configuration](../../performance/configuration.md), [Table types](../../performance/table-types.md),
[Access patterns](../../performance/access-patterns.md),
[Transport](../../performance/transport.md) and [Reading many partitions](../../performance/fanout.md).

## Cause

`render.rs`, in `gather`:

```rust
// which capture the current numbers come from: the caller's choice, or the most recent one
// that produced a micro layer
let current = match &args.current {
    Some(label) => label.clone(),
    None => timeline
        .iter()
        .rev()
        .find(|snapshot| snapshot.micro.is_some())
        .map(|snapshot| snapshot.label.clone())
        .unwrap_or_default(),
};
```

One label, resolved once, used by every page. The seven macro pages then read
`page.current().and_then(|current| current.macro_layer.as_ref())`, and a capture selected for
having a micro layer has no reason to have a macro one.

**This is [item 77](../known-issues.md) from the other side.** That item files the *converse* — a
macro-only capture can never *become* current, however exactly it covers the arms a page draws,
because the selection screens on the micro layer. Both are the same conflation: the choice answers
"which capture is the reference" when the question every page is actually asking is "which capture
measured the layer I draw".

The second half of it is completeness rather than presence. A capture narrowed by a filter has the
layer and holds only the arms the filter selected, so drawing a page from one silently drops every
arm it excluded. Nothing screened on that either, and `CaptureMeta` has recorded `complete` per
layer since the meta file existed.

## Evidence

**Established by rendering**, twice, and then by a unit test.

The end-to-end reproduction needs no fabricated artifact, because a micro-only capture is already
committed: `inline-probe`, which has a micro layer and a meta and nothing else. Naming it is
enough:

```
$ shoal-bench render --current inline-probe --out /tmp/repro79
wrote 11 pages under /tmp/repro79 (23 captures, 329769 bytes)
```

against 773,488 bytes for the same command without the flag. The seven macro pages collapse, each
to its "nothing has been captured" text.

Then in `render/page.rs`, against the tree before the fix:

```
---- render::page::tests::a_capture_with_no_macro_layer_does_not_become_the_macro_source stdout ----
assertion `left == right` failed
  left: None
 right: Some("micro-only")

---- render::page::tests::a_partial_capture_does_not_outrank_a_complete_one stdout ----
assertion `left == right` failed
  left: None
 right: Some("full")
```

## The fix

The default is resolved **per layer**, and at the point of use rather than once for the render.
`Page::current` holds only what the caller asked for with `--current`, and `Page::current_for`
answers each page's real question:

```rust
pub fn current_for(&self, layer: Layer) -> Option<&Snapshot> {
    // a capture the caller named is an instruction rather than a preference, so it wins
    // whenever it measured this layer at all
    if let Some(named) = self
        .timeline
        .iter()
        .find(|snapshot| snapshot.label == self.current && snapshot.has(layer))
    {
        return Some(named);
    }
    // otherwise the most recent capture that measured the whole of this layer
    self.timeline
        .iter()
        .rev()
        .find(|snapshot| snapshot.complete.contains(&layer) && snapshot.has(layer))
        // and a tree that only ever captured part of it draws the newest of those, because a
        // page built from a filtered capture still beats a page built from nothing
        .or_else(|| self.timeline.iter().rev().find(|snapshot| snapshot.has(layer)))
}
```

`Snapshot` gained `complete`, a set of the layers whose `LayerRecord` said so, read from the
capture's own meta in `gather`. `Page::current()` is now `current_for(Layer::Micro)`, so the micro
page is unchanged; the seven macro pages take `Layer::Macro`; and the hotpath and stage sections of
`attribution.md` and `row-size.md`, which already scanned the timeline for a snapshot with their
layer, now go through the same function and gain the completeness screen they never had.

**No committed page changes.** `f24-routing` is the newest capture and produced all four layers
complete, so every layer resolves to the same label the single `current` resolved to before. That
is what made this safe to take alongside another change: it alters what happens to the *next*
narrow capture and nothing about the current numbers.

## Alternatives rejected

**Drawing each arm from the newest capture that measured it.** This is what
[item 77](../known-issues.md) considers and rejects, and the rejection still holds: it mixes
captures *within* one page and one table, which [Baseline](../../performance/baseline.md) forbids
because two captures are two machines' worth of conditions. Per *layer* is a different line — a
page draws one layer, so no page ever mixes two captures — and it is the line taken here.

**Letting a page say "a newer capture covers these rows".** Item 77's own fix direction, and still
the right answer to item 77, which is about a capture that is newer *and* comparable being
invisible. It is not the answer to this: a page that has drawn nothing has no rows to annotate.

**Refusing to render when the newest capture is partial.** A hard error rather than a screen. It
would have caught this, and it also breaks the case the fallback above exists for — a tree whose
only capture of a layer is a filtered one, where a partial page is better than an empty one and a
failed render is worse than both.

**Screening on `partial` from `CaptureStatus` instead of `complete` from the meta.** `partial` is a
property of the whole capture, so a full capture of one layer taken alongside a filtered capture of
another would be screened out of both. `LayerRecord::complete` is already per layer and already
written by every capture.

## Invariants to uphold

- **No page may reach `Page::current` (the field) to decide what to draw.** It holds the caller's
  explicit `--current` and is empty when there was none. `current_for` is the accessor; the field
  is an input to it.
- **A page draws one layer from one capture.** Mixing two captures inside a page or a table is what
  [Baseline](../../performance/baseline.md) forbids, and per-layer resolution must never become
  per-arm resolution.
- **`--current` stays an instruction.** A caller naming a capture gets it wherever it measured the
  layer, complete or not. The screens decide the default only.
- **`Snapshot::complete` is read from the capture's own meta, never inferred.** A capture with no
  meta file contributes no completeness claim and falls to the last clause, which is correct: the
  older captures in `docs/perf/runs/` predate the meta file.
- **A new page that draws a layer calls `current_for` with that layer.** A page added with
  `page.current()` would silently re-acquire this defect for whatever it draws.

## Still open

[Item 77](../known-issues.md) is **not** closed by this, and the two should not be confused. A
macro-only capture can now become the macro pages' source, which is half of what 77 asks for; what
it is actually about is a page having no way to say *these rows in particular have been re-measured
since*, so that a reader looking at a stale figure can find the correction that is already
committed. That still has no answer.

## Tests

| Test | What breaks if this is reverted |
| --- | --- |
| `render::page::tests::a_capture_with_no_macro_layer_does_not_become_the_macro_source` | The defect itself: a micro-only capture taking the seven macro pages down with it |
| `render::page::tests::a_partial_capture_does_not_outrank_a_complete_one` | A filtered capture becoming the source for a page and dropping every arm the filter excluded |
| `render::page::tests::a_named_capture_wins_over_the_newest_complete_one` | `--current` degrading from an instruction into a preference |
| `render::pages::tests::every_page_renders_with_nothing_captured` | A tree with no captures at all, which is what a clean checkout building the book depends on |

## Related

- [Known Issues](../known-issues.md) — item 77, the converse of this and still open
- [F21](../../features/benchmark-groups.md) — the groups that make a narrow capture the ordinary
  thing to take, which is what makes this reachable
- [F18](../../features/results-pages.md) — the eleven pages and how each one is built
- [F25](../../features/read-buffers-are-filled-not-zeroed.md) — the change that needed a micro-only
  capture and found this in the way
