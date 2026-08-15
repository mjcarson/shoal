# F15. The client is a crate that cannot start a database

## Context

The ask was "support different async runtimes — tokio, glommio, and others".
[D5](../direction/runtimes.md) argues that this names the wrong problem, and the argument is worth
restating because it is what this feature is:

> **The problem behind the ask is not that the client is tokio-only. It is that the client is not
> tokio-only.**

A pure client — a TUI, a benchmark runner, an application that never starts a server — was forced
to compile and link glommio, and therefore io_uring, and therefore Linux, for no reason it could
see. `shoalctl` is a terminal UI that opens a connection and sends queries. It has no shard, no
storage and no partitions, and it compiled a storage engine.

There was a feature that looked like it already fixed this:

```toml
default = ["server"]
# Enables the server features for Shoal
server = ["glommio"]
```

It did not work, and could not have. `shoal-core/src/lib.rs` declared `pub mod server;` with no
`#[cfg]`; `server.rs` opened with `use glommio::PoolThreadHandles;`; and `shared/traits.rs` —
the module the client depends on for `QuerySupport`, `RkyvSupport` and the query and response
types — imported `glommio::TaskQueueHandle` at module scope, because `ShoalDatabase::new` takes
one. Nine of `ShoalDatabase`'s methods carried `#[cfg(feature = "server")]` and four did not, and
the imports their signatures needed carried nothing at all. **Turning the feature off produced a
build failure rather than a client**, and nothing had ever tried: no build in the workspace passed
`--no-default-features` to `shoal-core`, so the feature had never once been exercised.

Two manifests already said so in comments. `shoalctl/Cargo.toml` carried `deepsize2`, `kanal` and
`glommio` under `# this shouldn't be required by the user`, and `shoal-bench/Cargo.toml` carried
the same three pointing at
[item 54](../appendix/known-issues.md#54-shoaldb-needs-three-crates-the-caller-has-never-heard-of).

D5 ranks this **A2** — "a mechanical change with no behaviour and no design risk" — and ranks the
runtime abstraction itself **C**, with no demonstrated caller. **This is the split only.** Step 2
is untouched and still stands at C.

## What it does

There are three implementation crates where there was one.

| Crate | Holds | Links |
| --- | --- | --- |
| `shoal-proto` | The wire format, the query and response types, the traits a schema implements, SCRAM, the TLS configuration | **No async runtime at all.** 19 dependencies |
| `shoal-client` | `Shoal<S>`, the three streaming modes, the connection pool | tokio, bb8, papaya, kanal — and no engine |
| `shoal-core` | The shard, the storage engines, the ring, `ShoalDatabase` | glommio, and the protocol crate |

`shoal-core` depends on `shoal-proto` and **not** on `shoal-client`. That direction is the proof
the split is real, and it needed no work: nothing in the engine had ever named `crate::client`.

The `server` feature moved to the facade, where for the first time it means something:

```toml
shoal = { version = "0.1.0", default-features = false }
```

gives a client. `shoal::shared`, `shoal::traits` and `shoal::client` are all still there;
`shoal::server`, `shoal::storage`, `shoal::tables`, `shoal::ShoalPool` and `shoal::Conf` are not,
and neither is glommio.

A schema says which half it wants:

```rust
// a server, unchanged
#[shoal::db]
pub struct Tmdb { .. }

// a client, which emits no `ShoalDatabase` impl, no `ShardRouting` impl, and not the struct
#[shoal::db(client)]
pub struct Tmdb {
    pub movies: PersistentSortedTable<Movie, FileSystem>,
}
```

`shoalctl` is now the second form. Its manifest has no `shoal-core` line and no `glommio` line,
and `cargo tree -p shoalctl` contains neither glommio nor io_uring.

**Generated code names `::shoal::` and nothing else.** It used to emit `shoal_core::`, `rkyv::`,
`uuid::`, `glommio::` and `kanal::` by path, so a crate writing a schema had to declare crates it
had never heard of. The facade now re-exports each from the crate whose trait signatures it appears
in, which closes the body of [item 54](../appendix/resolved/macro-emits-three-crates.md).

## Design choices

**Two crates, not one.** D5 offers "`shoal-proto`, or `shoal-client`", singular. Two were built
because they answer to different constraints: `shoal-proto` is the crate that could be published
and has to stay free of a runtime, and `shoal-client` is an implementation that happens to use
tokio. Merging them would mean the wire format's dependency list contains a runtime forever, which
is precisely the property that made this defect invisible for as long as it was.

**`shoal-core` keeps `pub use shoal_proto::shared;`, permanently.** This is not a compatibility
shim. From inside the engine, `shared` still means exactly what it meant, so the whole of
`server/` — hundreds of `crate::shared::traits::…` sites — is a zero-line diff. That single
`pub use` is the largest churn reduction available here and the reason the change is reviewable.

**Downstream names the facade, never an implementation crate.** Twenty-six files across
`shoal/tests`, the benches, the examples, `shoalctl` and `shoal-bench` say `shoal::` where they
said `shoal_core::`. Nothing outside the four implementation crates names `shoal_proto` or
`shoal_client`, which is what lets a fourth crate be added later without a second workspace-wide
rename.

**`split_by_shard` moved to a server-side extension trait rather than moving `Ring` clientward.**
It was a method on `ShoalQuerySupport`, a *shared* trait, and its signature named `Ring` and
`ShardInfo` — placement, which a server decides and a client is not told. It is now `ShardRouting`
in `shoal-core::server::routing`, implemented for `SortedQuery`, `UnsortedQuery` and the generated
`QueryKinds`. The bound sits on `ShoalDatabase::ClientType`:

```rust
type ClientType: QuerySupport<QueryKinds: ShardRouting> + Sized;
```

so only something that owns a ring ever asks for it — which is what makes `db(client)` fall out
for free rather than needing a second mechanism. D5 offered moving `Ring` into the client instead,
because [D7](../direction/shard-aware-routing.md) wants a client-side tablet map anyway; that was
declined because it puts routing in the client *now* to serve a feature that does not exist yet,
and D7 can move a trait impl as easily as it could have moved a trait method.

**`#[shoal::db(client)]` is an argument, not a second macro or a `cfg`.** The alternative was
emitting `#[cfg(feature = "server")]` around the server half, evaluated in the *caller's* crate —
which would have required every schema crate to declare a feature named `server` and wire it up,
a silent manifest requirement of exactly the class item 54 already was. Of the eight emissions the
`db` macro makes, two change: `traits::db::add` is skipped, and `query_kinds::add` takes a flag so
the `ShardRouting` block is not emitted. The other six are byte-identical in both halves, and
`rewrite_table_fields` still runs in the client half even though its output is discarded, so both
halves see identical input.

**`Errors::Channel(ChannelError)` rather than a string.** `Errors` had to move into `shoal-proto`,
because `QuerySupport::succeeded` and `shared::responses` both name it — but it carried
`KanalSend(kanal::SendError)` and `KanalReceive(kanal::ReceiveError)`, and the protocol crate must
not name whichever channels a particular client is built on. The orphan rule blocks putting the
`From` impls in `shoal-client`. A `Channel(String)` variant was the obvious move and the wrong one:
kanal's two error types are fieldless two-variant enums, one byte and `Copy`, so stringifying them
trades a free value for a heap allocation. `ChannelError` is an equivalent owned enum and the seven
`?` sites map explicitly.

**The derive emits literal paths, not an interpolated crate root.** A `fn root() -> TokenStream`
interpolated as `#root::shared::traits::…` was considered and rejected: it makes every emitted path
two tokens and unreadable in the source, which is the opposite of reviewable, and it implies a
configurability (`#[shoal(crate = "...")]`) nobody asked for. The 387-site sweep is verified by
grep returning nothing rather than by reading it.

## Alternatives rejected

**Gating the existing modules properly instead of splitting.** Adding `#[cfg(feature = "server")]`
to `pub mod server;` and untangling `shared/traits.rs` would have fixed the build failure without
moving anything. It is less work, and it leaves the client and the server sharing a version, a
release and a dependency set — which is the thing that made this defect invisible. D5 rejects it
and this agrees.

**Keeping `shoal-core`'s `server` feature.** The nine `#[cfg(feature = "server")]` attributes
became dead the moment `ShoalDatabase` moved into a server-only crate, and dead was the good case.
The bad case is that they are actively misleading: a reader who sees them believes a client-only
`shoal-core` exists, which is the exact false belief this feature was built to correct. They are
gone, `glommio` is no longer optional there, and the name survives on the facade meaning one thing.

**`gxhash` re-exported from `shoal-proto` at the workspace pin.** The workspace pins gxhash 3 with
`deterministic`; `shoal-core` pins 2.2. Re-exporting the workspace one would have put two majors in
the graph with the facade's choice deciding how every partition key is hashed — and changing that
silently rehashes every persisted dataset. `shoal-proto` pins the same major the engine does, so
there is exactly one. The disagreement itself is filed as
[item 65](../appendix/known-issues.md).

**A `Runtime` trait, or an `async-compat` shim.** Both are D5's step 2, both are ranked C there,
and neither is built. The argument is D5's and is not restated here.

## Limitations

**Still one runtime, and tokio is it.** Nothing here is generic over a runtime. D5's step 2 is
deliberately not built, because the compelling non-tokio caller — an application already on
thread-per-core, colocated with a shard — is better served by an in-process path that skips the
socket entirely, which is a different feature.

**A `#[shoal::db(client)]` schema must name its table and storage types in field position only,
and never in a `use`.** Because the client half never emits the struct,
`PersistentSortedTable<Movie, FileSystem>` is read for its names by `syn` and discarded — neither
type reaches resolution. An import of either fails in a client build even though the field type
does not. This is pinned by `shoal-client-check` and demonstrated by `shoalctl`.

**`rkyv` and `deepsize2` remain a schema author's own dependencies.** Item 54 named `glommio`,
`uuid` and `deepsize2`; the first two were macro-emitted and are closed, but `deepsize2` is a
supertrait bound satisfied by a hand-written `#[derive(DeepSizeOf)]`, and `rkyv`'s derives are
written by hand too. Re-pointing macro paths cannot reach either. The remainder stays open on
[item 54](../appendix/known-issues.md).

**A published client is still blocked, but by one thing instead of three.** The workspace depends
on a local glommio fork by path, which cannot be published — but only `shoal-core` needs it now,
so `shoal-proto` and `shoal-client` are unblocked by this change. `#![feature(trivial_bounds)]`
was the risk that would have kept them nightly-only; see *Performance* for what was found.

**`cargo test --workspace` does not prove the client-only graph.** Cargo unifies features across
workspace members, so a workspace run compiles one `shoal` with `server` on and glommio *is* in
that build. The graph proof is a build command, not a test — see *Tests*.

## Invariants to uphold

- **Nothing outside `shoal-proto`, `shoal-client`, `shoal-core` and `shoal-derive` names an
  implementation crate.** Callers go through `shoal`. This is what lets a fourth crate be added
  without a workspace-wide rename.
- **`shoal-core` never depends on `shoal-client`.** The reverse dependency would drag tokio into
  every server binary and recreate the coupling in the mirror image.
- **`shoal-proto` takes no async runtime and no channel crate.** Not tokio, not glommio, not kanal.
  A type that needs one belongs in `shoal-client` or `shoal-core`.
- **There is exactly one `rkyv`, one `uuid`, one `gxhash` and one `deepsize2` in the graph, and the
  facade declares none of them** — each is re-exported from the crate whose trait signatures it
  appears in. Two of any of them and `Archive` impls stop unifying, and the diagnostics are a wall.
- **`gxhash` comes from `shoal-proto`, pinned to the major `shoal-core` uses.** A client hashes its
  own partition keys, so it cannot be gated behind the engine; and the client and the ring must
  hash a key identically.
- **The non-generic per-frame functions in `shared/protocol.rs` carry `#[inline]`.** Their callers
  are in other crates now and this workspace builds with `lto = false`, so removing one turns a
  header codec into a real call on every frame. See *Performance*.
- **`stage-profile` is all three crates or none.** `Stamp` is in `shoal-proto`, `ClientStamps` in
  `shoal-client` and `StageStamps` in `shoal-core`; a `const _: () = assert!(size_of == 0)` in the
  client fires if they drift.
- **A `db(client)` schema names storage types in field position only** — see *Limitations*.
- **The client half of the derive emits no `::shoal::server::`, `::shoal::storage::` or
  `::shoal::tables::` path.** The table in `shoal-derive/src/utils.rs` lists which prefixes are
  legal in which half; a new one on the wrong side compiles in the macro and fails in the caller.

## Performance

[D5](../direction/runtimes.md) says **"none needed for the split; nothing measurable changes"**.
That is true at the source level — no algorithm, data structure or allocation changed — and it is
wrong at the codegen level, so it was measured. `pre-d5-split` and `post-d5-split` are both in
`docs/perf/runs/`.

**Why a pure move can cost anything.** This workspace has no `[profile.release]` at its root
([item 66](../appendix/known-issues.md)), so every release build is `lto = false`,
`codegen-units = 16`. Under that profile rustc inlines across a crate boundary only for functions
that are generic, `const`, or carry `#[inline]`. Most of the moved surface is generic — `RkyvSupport`,
`QuerySupport`, `Queries<S>`, `SortedQuery<T>`, `Response<T>` — and monomorphizes in the consuming
crate, so it was never at risk. The non-generic per-frame codec in `shared/protocol.rs` was, and it
carried **no `#[inline]` attributes at all**.

**The header codec got faster, and the reason is the interesting part.**

| Benchmark | before | after | |
| --- | --- | --- | --- |
| `wire_codec/request/decode/header/{1,10,100}` | 3.32 ns | 0.65 ns | **−80.3%** |
| `wire_codec/header/decode` | 3.32 ns | 0.70 ns | **−78.8%** |
| `wire_codec/header/decode_response` | 5.42 ns | 1.52 ns | **−72.0%** |
| `wire_codec/header/response_preamble` | 4.72 ns | 3.67 ns | **−22.2%** |
| `wire_codec/response/decode/access/{256,1024,4096}` | | | **−8.5% to −9.0%** |

These calls were *already* cross-crate before the split — `shoal/benches/wire.rs` has always been a
separate crate from `shoal-core`, as has the client, which decodes a header on every frame. The
codec had no `#[inline]` because everyone who wrote it only ever saw it called from inside its own
crate. **The split did not create that cost; it created the reason to look.** `header/encode` and
`header/request_preamble` are unchanged at 2.62 ns, both `const fn` — though so is `decode`, which
gained 79%, so "const fn inlines anyway" is not a rule this repository should rely on.

**One reproducible regression, on one path.** `wire_codec/request/decode/deserialize/1` went
29.52 → 38.28 ns, confirmed at 38.91 ns by a second capture. It dilutes with bundle size
(+29.7% / +5.8% / +1.7% at 1 / 10 / 100 queries), which is the shape of a fixed per-call cost.
`#[inline]` on `RkyvSupport::serialize` and `deserialize` recovered `encode/serialize/1`
(70.58 → 64.04 ns) and did nothing for this one. It is real, unexplained, and filed as
[O32](../appendix/optimizations.md) rather than papered over.

**End to end, nothing moved.** The first post-split macro capture showed 32 metrics worse and 4
better out of 783, including three workloads with disjoint wall-clock intervals — which by this
repository's own standard is a result. **It did not reproduce.** Re-running the same code over the
same workloads:

| Over `transport` + `fanout`, 306 metrics | first capture | re-run |
| --- | --- | --- |
| not a result (intervals overlap) | 280 | 292 |
| disjoint, better | 2 | 8 |
| disjoint, worse | **24** | **6** |

`fanout/resident/256` went +5.65% → +0.61%, `transport/send_batched/large` +5.18% → −1.80%,
`transport/stream/large` +2.08% → +1.04%. A 24:2 skew becoming 6:8 on identical code is a noisy
capture, and the honest conclusion is that **one macro capture is not enough to convict a change
of a few percent** — which is worth writing down, because the first one nearly convicted this one.

**No stored capture was invalidated.** Every `#[hotpath::measure]` site is under `server/`, which
did not move, so no scope name changed and every prior hotpath capture still joins. That is worth
stating because a reader would reasonably assume otherwise from a change that renamed two thirds
of the crate's modules.

## Tests

| Test | What breaks without it |
| --- | --- |
| `cargo check -p shoal-client-check --no-default-features` | The graph proof: a schema compiles with no engine in its subgraph. This is D5's stated check, and it would have failed for three separate reasons before this |
| `cargo build -p shoalctl` | The same property on a program somebody runs, with `shoal-core` gone from its manifest entirely |
| `shoal-client-check` — `a_schema_parses_shql_without_a_server` | The `parse` arm of the generated `QuerySupport` impl, the largest emission the client half keeps |
| `shoal-client-check` — `a_malformed_query_is_refused` | `ShqlParseError` reaching a client that has no engine |
| `shoal-client-check` — `a_query_round_trips_through_the_wire_format` | `RkyvSupport::serialize`/`access` across the new crate boundary — the archived layout and its reader agreeing |
| `shoal-client-check` — `every_table_and_projection_is_named` | The `TableNames` enum and the projection enums, which are separate emissions from the client struct |
| `shoal-client-check` — `the_fingerprint_is_a_constant` | `traits/fingerprint.rs`, the only client-half emission reaching into `shoal::shared::protocol` |
| `shoal-client-check` — `a_tables_fields_are_described` | `TableSchemaSupport`, which shql completion needs before a client has connected to anything |
| `shoal-client-check` — `a_row_hashes_its_own_partition_key` | `PartitionKeySupport`, and so `gxhash` not being gated behind the engine |
| `shoalctl/tests/completion.rs` | A `#[shoal::db(client)]` schema driving a real TUI, in the crate that motivated the split |
| `hotpath_scopes` — `a_scope_from_any_crate_loses_its_prefix` | The scope prefix strip, if a `#[hotpath::measure]` site ever moves out of `shoal-core` |
| `chart_geometry` — `stacked_labels_have_room` | The encryption chart's label deconfliction, over every capture committed to the tree |
| The 171 `shoal-proto` and 6 `shoal-client` unit tests | The protocol and the client, at their new addresses. The totals did not change; the attribution did |

## Related

- [D5. Runtime portability](../direction/runtimes.md) — the design page, whose step 1 this is
- [D7. Shard-aware routing](../direction/shard-aware-routing.md) — which wanted `Ring` on the
  client side; `split_by_shard` is now a server-side trait it can move whole
- [F10. Framing and protocol evolution](framing-and-protocol-evolution.md) — whose module was
  written to be runtime-free so it could move here unchanged, and did
- [item 54](../appendix/resolved/macro-emits-three-crates.md) — the same defect from the macro's
  side, closed but for `deepsize2` and `rkyv`
- [item 65](../appendix/known-issues.md) — the two `gxhash` majors this had to navigate
- [item 66](../appendix/known-issues.md) — the release profile nothing has been reading
