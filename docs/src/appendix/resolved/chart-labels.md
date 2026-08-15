# 67, 68. Chart labels collided, and the scope prefix strip never matched

Two defects in the same layer, found the same way and fixed together: the charts on
[Benchmark Results](../../operations/benchmark-results.md) place their text without measuring it,
and neither of these was visible in any test that did not draw a real capture.

## Symptom

**67.** The encryption charts drew two series labels on top of each other. Every curve is labelled
at its right-hand end rather than in a legend, so two curves that end at similar costs put their
names in the same place — unreadable, and neither name recoverable.

**68.** Every label on every hotpath profile chart carried a redundant `shoal_core::` prefix,
spending twelve of a forty-six character budget on a string identical for every scope in the
profile, and pushing the part that identifies a scope closer to being truncated.

## Cause

**67.** `shoal-bench/src/render/chart/encryption.rs`, in `draw_overhead`. Each label was placed as
the loop drew its curve:

```rust
if let Some((x, y, _)) = curve.last() {
    chart.draw_series(std::iter::once(Text::new(
        series_label(*key),
        (*x * 1.08, *y),
        super::label_font(11),
    )))?;
}
```

`*y` is the curve's last data point. Nothing consulted where the other labels had gone, because
each was drawn before the next curve was known.

**68.** `shoal-bench/src/render/chart/hotpath_scopes.rs`, in `shorten`:

```rust
let trimmed = name.strip_prefix("shoal::").unwrap_or(name);
```

`hotpath` names a scope after the crate it is compiled in, and that crate is `shoal_core` — the
scopes in every capture are `shoal_core::server::shard::handle_query` and its like. `"shoal::"` is
not a prefix of `"shoal_core::"`, so the strip has never once matched and `unwrap_or` returned the
name unchanged every time.

## Evidence

**67 — established by reproducing it.** Taking a second capture containing the encryption sweeps
failed `chart_geometry::stacked_labels_have_room`:

```
encryption_by_row: two labels in the column at x=661 are 2 apart at y=171,
which is closer than the text is tall
```

It is data-dependent, which is why it lay dormant from [F14](../../features/encryption-in-transit.md)
until [F15](../../features/client-server-split.md): `every_chart()` draws from every capture
committed to the tree, and until a second capture with encryption sweeps existed there was only
one set of curves to collide. Moving the new capture aside made the test pass and putting it back
made it fail again, which is what established the cause rather than the symptom.

**68 — established by reading a capture.** Every scope name in
`docs/perf/runs/*.hotpath.json` begins `shoal_core::`. The unit test agreed with the broken code
because it was written against `shoal::server::shard::handle_query`, a name no capture has ever
contained.

## The fix

**67.** Label placement moved out of the drawing loop. Every curve's end is collected first, then
sorted by height and pushed apart so no two are closer than the text is tall, then drawn:

```rust
let plot_px = 400.0 - 16.0 - 46.0;
let min_gap = (high - low) * (12.0 / plot_px);
ends.sort_by(|left, right| left.1.partial_cmp(&right.1).unwrap_or(Ordering::Equal));
```

The gap is computed in data units from the pixels the text occupies, because the label is placed
in data space and the chart's y range is not fixed. Labels stay in the order their curves ended
in, so a reader can still tell which is which.

**68.** `shorten` strips any of the three crate prefixes:

```rust
const PREFIXES: [&str; 3] = ["shoal_core::", "shoal_client::", "shoal_proto::"];
```

All three are listed rather than just the one in use, because F15 made it possible for a
`#[hotpath::measure]` site to live in another crate.

## Alternatives rejected

**Scoping `chart_geometry` to one capture instead of every one.** It would have made 67 stop
firing without fixing anything, and the test's value is precisely that it draws from what is
committed — a chart that is fine for one dataset and broken for the next is the failure mode.

**A legend instead of end-of-curve labels for 67.** It removes the collision by removing the
labels, at the cost of the property the current design was chosen for: a reader never has to match
a colour to a name. Deconflicting the labels keeps both.

**Truncating from the tail rather than the front for 68.** The tail is what identifies a scope —
`handle_query` — and the front is what every scope shares. That choice was already right; only the
prefix list was wrong.

## Invariants to uphold

- **A chart places labels only after every label's position is known.** Anything placed inside the
  loop that draws the series cannot avoid a collision, because it cannot see what comes next.
- **`chart_geometry` draws from every capture in the tree, and that is the point.** A new capture
  is allowed to fail it; that is the test finding a layout that does not survive real data.
- **The prefix list in `shorten` covers every crate a `#[hotpath::measure]` site can live in.** A
  scope that keeps its prefix shortens differently and reads as a different scope on the chart.
- **A test over label geometry uses names that occur in captures.** Both of these were agreed with
  by a test written against something plausible rather than something real.

## Still open

Nothing from these two. The wider issue they are instances of — that plotters is built here
without a font backend and therefore *estimates* text extents rather than measuring them — is
described in the header of `shoal-bench/tests/chart_geometry.rs` and is not fixable from this side.

## Tests

| Test | What breaks without it |
| --- | --- |
| `chart_geometry` — `stacked_labels_have_room` | 67, over every chart drawn from every capture committed to the tree |
| `hotpath_scopes` — `a_long_scope_is_shortened_from_the_front` | 68's shortening, now spelled with a prefix that occurs in real data |
| `hotpath_scopes` — `a_scope_from_any_crate_loses_its_prefix` | 68 recurring if a measured scope moves crates, and a name from outside the workspace being mangled |

## Related

- [F15. The client is a crate that cannot start a database](../../features/client-server-split.md)
  — the change during which both were found
- [F14. Encryption in transit](../../features/encryption-in-transit.md) — which added the charts 67
  was latent in
- [F7. A benchmark runner that renders its own results](../../features/bench-runner.md) — which
  built this layer
