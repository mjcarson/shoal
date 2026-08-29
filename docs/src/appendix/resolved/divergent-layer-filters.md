# 90. Setting `RUST_LOG` could fragment every exported trace

The subscriber's two layers were filtered from two different sources. A per-layer filter decides
what a layer can **see**, not only what it emits, so the two disagreeing meant the OTLP layer could
be shown a child whose parent it had never been shown — and a parent it cannot find is a new trace
id, not an orphan.

## Symptom

Latent rather than observed, and it says so. With `tracing.level: Info` in `shoal.yml` and no
`RUST_LOG`, the two filters admit the same set and nothing goes wrong. The failure needs the two to
disagree, and the most likely way for them to disagree is somebody setting `RUST_LOG` — which is
what a person does when they are trying to work out why their traces look wrong.

`RUST_LOG=shoal_core::server::shard=info` would have kept `Coordinator::send_to_shard` on the
console while dropping it from the OTLP layer, at which point every `Shard::handle_query` naming
it as a parent is exported as the root of a fresh trace. The console output would look correct
throughout.

## Cause

`shoal-core/src/server/trace.rs` built two filters from two sources:

- `setup_local` used `EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(conf.level…))`
  — `RUST_LOG` when set, the configured level otherwise;
- `setup_remote` used `conf.level.to_filter()` — the configured level, always.

Both were attached with `.with_filter(...)`, which makes them *per-layer* filters rather than
global ones. `tracing-opentelemetry` documents the consequence in the function it happens in:

```rust
// A span can have an _explicit_ parent that is NOT seen by this `Layer` (for which
// `Context::span` returns `None`. This happens if the parent span is filtered away
// from the layer by a per-layer filter. In that case, we fall-through to the `else`
// case, and consider this span a root span.
```

`tracing-opentelemetry-0.29.0/src/layer.rs:773-793`. The fall-through matters: a span created with
an explicit parent is not *contextual*, so it skips the `is_contextual` branch and lands on
`OtelContext::new()` — an empty context, which is a brand new trace. Every hop in Shoal's query
path uses an explicit parent, because every one of them crosses a channel
([item 89](fragmented-query-traces.md)), so all of them are exposed to this.

## Evidence

**Established by reading the source**, and said so rather than claimed as reproduced. The two
filters were read in `trace.rs`, and the consequence was checked in the dependency rather than
inferred: `parent_context` at `tracing-opentelemetry-0.29.0/src/layer.rs:773-810` is quoted above
from the installed crate, and `Span::child_of_with` at `tracing-0.1.41/src/span.rs:497-508` is the
same fall-through one level down.

No collector run demonstrates it, and one would need a config in which the two filters disagree,
which no committed config produces. That is the reason it is worth fixing rather than the reason
it is not: the state is reachable by an environment variable and invisible on the console.

## The fix

One source, read once. `filter_directives` decides the directives — `RUST_LOG` when it is set to
anything non-blank, the configured level otherwise — and `setup_with` passes the resulting string
to both `setup_local` and `setup_remote`, each of which builds an `EnvFilter` from it.

`EnvFilter` is not `Clone`, which is why this returns a string rather than a filter. Two filters
built from one set of directives cannot disagree; two built from two sources can.

The stray `println!("setup remove -> {otlp:#?}")` on the setup path went at the same time. It ran
before the subscriber existed, so it could never have been an event, and it printed the sink
configuration to stdout — which
[F34](../../features/benchmark-tracing.md) says is the stream a benchmark harvests the hotpath
profile from.

## Alternatives rejected

**Filter the registry once instead of filtering each layer.** It removes the divergence by
removing the per-layer filters, and it also removes the thing the split was for: the console layer
wants per-target control so that the OTLP exporter's own `DEBUG` chatter can be turned on without
turning the whole workspace up. A shared source keeps both.

**Leave the remote layer unfiltered and let the registry's `max_level_hint` decide.** The remote
layer would then export everything the console layer admits, which makes `RUST_LOG=debug` on one
target a debug-level export of the whole process to a collector. Filtering is what stops that.

**Make `conf.level` the floor and `RUST_LOG` only able to raise it.** Attractive, and a different
change: it alters what `RUST_LOG` means for every existing deployment, and it does not fix this on
its own — two filters from one rule can still be two different filters.

## Invariants to uphold

**Every layer in this subscriber is built from `filter_directives`, and nothing else.** A layer
filtered from another source can be shown a child whose parent it was not shown, and that child is
exported as the root of a new trace with no warning anywhere.

**A layer added later needs a filter from that same string, or no filter at all.** No filter is
safe; a different one is not.

**`directives_from` is the testable half and reads no environment.** The environment is read in
`filter_directives` and nowhere else, so a test of the decision does not have to mutate global
state that every other test in the binary shares.

## Still open

Nothing from this item. The related gap is that a collector can accept an export and drop every
span without saying so, which is [item 87](../known-issues.md#87-a-collector-that-rejects-every-span-is-indistinguishable-from-one-that-accepts-them)
and is not about filtering.

## Tests

| Test | What breaks if the fix is reverted |
| --- | --- |
| `server::trace::tests::one_source_decides_what_every_layer_filters_on` | `RUST_LOG` replacing the configured level entirely, a blank override not counting as an override, and both layers reading the one decision |

The rest of the property is held by the types rather than by a test: `setup_local` and
`setup_remote` take `&str` directives, so there is no longer a second source for either of them to
read.

## Related

- [Item 89](fragmented-query-traces.md) — the other way a parent goes missing, and why it matters
- [Observability](../../operations/observability.md) — the empty-parent trap, written down
- [F34](../../features/benchmark-tracing.md) — the subscriber this filters, and what installs it
