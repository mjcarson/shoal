# 54. `#[shoal::db]` needed crates the caller had never heard of

## Symptom

A crate that wrote a schema had to declare `glommio`, `uuid` and `kanal` as its own dependencies,
even though it mentioned none of them and had no reason to know they existed. The failure was at
least loud — `cannot find module or crate glommio in this scope` — but it pointed at the
`#[shoal::db]` attribute rather than at the manifest, and nothing in
[Derive Macros](../../api/derive-macros.md) said a word about it.

Two manifests carried the workaround with a comment saying it was wrong. `shoalctl/Cargo.toml`
listed three crates under `# this shouldn't be required by the user`, and `shoal-bench/Cargo.toml`
listed the same with a pointer to this item.

## Cause

The macros emitted absolute paths into `shoal_core` and into the third-party crates its trait
signatures name. `shoal-derive` had 215 `shoal_core::` tokens inside `quote!` blocks, plus 76
bare `rkyv::`, 9 `kanal::`, 6 `uuid::` and 1 `glommio::`. A path emitted by a macro is resolved
in the *caller's* crate, so every one of those was a dependency the caller had to supply.

The single `glommio` token was `shoal-derive/src/traits/db.rs:321`:

```rust
medium_priority: glommio::TaskQueueHandle,
```

one parameter of `ShoalDatabase::new`, and the entire reason a schema-defining crate had to
declare a storage engine.

**Nothing had noticed because nothing had ever tried.** Every schema in this repository lived
inside `shoal` — the `tmdb` example, the integration tests — and `shoal` already depended on all
of them, so the requirement was invisible from the only place it was ever exercised.

## Evidence

**Established by reproducing it**, while building [F8](../../features/purpose-built-workloads.md).
Writing the workload schema in `shoal-bench` was the first time a schema was defined outside
`shoal`, and it failed on each missing crate in turn.

One claim in the original item was wrong, and is corrected here rather than carried forward. It
said the generated code "names `glommio`, `uuid` and `deepsize2` by path". `deepsize2` is never
emitted by any macro — there is no `deepsize` token anywhere in `shoal-derive`. It arrives as a
supertrait bound at `shared/traits/sorted.rs:10`, `unsorted.rs:10` and `traits.rs:551`, which the
schema author satisfies by writing `#[derive(DeepSizeOf)]` by hand. The same is true of `rkyv`:
a schema writes `#[derive(Archive, Serialize, Deserialize)]` itself. The item's list should have
read `glommio`, `uuid` and `kanal`.

## The fix

Both halves of what this item predicted, in [F15](../../features/client-server-split.md).

**The macros emit `::shoal::` and nothing else.** All 387 sites were re-pointed, and the facade
re-exports each crate from the one whose trait signatures it appears in — so a schema cannot
resolve a different `rkyv` than `RkyvSupport` was compiled against, which is a second bug the old
arrangement made possible. The leading `::` is the crate-root form, so a local module named `shoal`
cannot shadow it.

**And the split removed `glommio` rather than re-exporting it.** The client and the protocol are
crates of their own now, and `shoal --no-default-features` links no engine at all — so `shoalctl`
does not merely stop *declaring* glommio, it stops *compiling* it.

`glommio` is gone from the manifests of `shoal`, `shoalctl` and `shoal-bench`. `gxhash` and
`kanal` are gone from `shoal`'s. `shoal-derive` also dropped its own unused `shoal-core` path
dependency, which takes the engine off the critical path of building the proc macro.

## Alternatives rejected

**Documenting the requirement in [Derive Macros](../../api/derive-macros.md).** The original item
already dismissed this, correctly: the fix "would make the requirement disappear rather than need
documenting". A manifest requirement that a reader must be told about is one the macro should not
have imposed.

**A `#[shoal(crate = "...")]` attribute**, so a caller could name the facade. It is what several
proc-macro crates do, and it solves a problem nobody has: there is one facade, and generated code
that cannot find it is broken rather than misconfigured. It would also have to be tested.

**An interpolated crate root in the derive** — `fn root() -> TokenStream` used as
`#root::shared::traits::…`. It makes every emitted path two tokens and unreadable in the source of
the macro, which is worse than the problem: the emitted path is the thing a reviewer needs to be
able to read.

## Invariants to uphold

- **Generated code names `::shoal::` and never an implementation crate.** The table in
  `shoal-derive/src/utils.rs` lists every prefix the macros emit, which crate the facade takes it
  from, and whether it is legal in the client half of `#[shoal::db(client)]`.
- **The facade declares none of the re-exported crates itself.** Each comes from the crate whose
  trait signatures name it, so there is exactly one version of each in the graph.
- **A new `::shoal::server::`, `::shoal::storage::` or `::shoal::tables::` path in the client half
  is a bug.** It compiles in the macro and fails in the caller, which is the worst place to find
  out — `shoal-client-check` is what catches it.

## Still open

**`rkyv` and `deepsize2` are still a schema author's own dependencies**, and the open remainder of
this item stays on [Known Issues](../known-issues.md). Neither is macro-emitted, so re-pointing
paths could not reach them: a schema writes `#[derive(Archive, Serialize, Deserialize)]` and
`#[derive(DeepSizeOf)]` by hand, and those derives expand to absolute paths into their own crates.

Closing the remainder means either emitting the `DeepSizeOf` impl from `ShoalSortedTable` by
summing the row's fields — which hand-rolls a memory accounting impl the server's eviction budget
depends on being right — or removing the bound from the client-visible traits. Both are filed in
[TODOs](../todos.md) rather than done here, and `shoal::deepsize2` and `shoal::rkyv` exist as
escape hatches in the meantime.

## Tests

| Test | What breaks without it |
| --- | --- |
| `cargo check -p shoal-client-check --no-default-features` | A schema compiling in a crate that declares none of the macro-named crates. This is the direct regression test: a re-emitted `glommio::` or `uuid::` path fails to resolve |
| `cargo build -p shoalctl` | The same, on a program somebody runs, with `shoal-core` gone from its manifest |
| `shoal-client-check` — the seven behavioural tests | That the client half still emits everything it should, so a `db(client)` cannot pass by emitting nothing |
| `grep -rn 'shoal_core::\|[^:]rkyv::\|[^:]uuid::\|[^:]glommio::\|[^:]kanal::' shoal-derive/src` | Returns nothing. Not a test, but it is how the 387-site sweep is verified rather than read |

## Related

- [F15. The client is a crate that cannot start a database](../../features/client-server-split.md)
  — the change that closed this
- [D5. Runtime portability](../../direction/runtimes.md) — which predicted the second fix
- [F8. Purpose-built workloads](../../features/purpose-built-workloads.md) — where this was
  reproduced
- [item 65](../known-issues.md) — the two `gxhash` majors, found while deciding which crate should
  re-export it
