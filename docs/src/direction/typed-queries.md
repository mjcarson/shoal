# D8. Compile-time guarantees

## Context

Shoal generates its whole client surface from a schema. Every table, every query struct, every
response variant is written by a macro that knows exactly which query produces which response — and
then throws that knowledge away. What reaches the caller is a schema-wide enum and a turbofish:

```rust
let response = client.send_one(TestRecordGet::new(vec![key])).await?;
let access = response.access::<TestRecord>()?.unwrap().first().unwrap();
```

`shoal/tests/persistent_sorted_table.rs:80-88`

Nothing checks that `TestRecord` is what a `TestRecordGet` answers with. Naming the wrong type
compiles, and fails at runtime with a string that does not say which types were involved. **The
information needed to make that a compile error was present in the macro and discarded**, which is
what makes this page worth writing: it is not new capability, it is capability already computed and
then dropped.

This is also the only page in the chapter that is entirely additive. Nothing here changes the wire,
the server, or the untyped path that SHQL and `shoalctl` need.

## What exists today

**A query erases itself on the way in.** Each generated query struct converts into the schema-wide
`QueryKinds` through a plain `From` impl (`shoal-derive/src/traits/from_query/sorted.rs`,
`unsorted.rs`). After that conversion there is no type-level record of which query it was.

**A response is matched, not typed, on the way out.** `FromShoal::retrieve` is the generated
runtime check:

```rust
fn retrieve(archived: &#response_name) -> Result<&ArchivedOption<ArchivedVec<...>>, Errors> {
    ...
    Err(shoal_core::client::Errors::WrongType("Wrong Type!".to_owned()))
}
```

`shoal-derive/src/traits/from_shoal.rs:27-35`

The error is `Errors::WrongType(String)` (`shoal-core/src/client/errors.rs:11`) carrying the
literal `"Wrong Type!"` — no expected type, no actual type, no query id.

**One place already does this right, and it shows what is possible.** `exists` is constrained by a
marker trait so only an exists query can be passed:

```rust
/// Marker trait for Exists queries
///
/// This trait is used to constrain the `exists` method on the Shoal client
/// to only accept Exists queries at compile time, preventing incorrect usage.
pub trait ExistsQuery {}
```

`shoal-core/src/shared/traits.rs:41-44`

And projections are already typed **in the query-building direction**:

```rust
pub fn projection<P>(mut self) -> Self
where
    P: shoal_core::shared::traits::ShoalProjection<Row = #name>,
```

`shoal-derive/src/structs/get.rs:107-109`

whose own doc comment states the asymmetry outright — *"The response comes back as the projection
rather than as the row, so it is retrieved with `response.access::<P>()`"* — a convention on the
way back where there is a constraint on the way in.

**The bounds are copy-pasted.** A ten-line `CheckBytes` `where` clause appears verbatim on
`Shoal::new`, `exec`, `send_one`, `exists`, `skip`, both `next` implementations, and both
`wait_for_next_response` implementations — eight public methods, around eighty lines that say one
thing:

```rust
where
    for<'a> <<S as QuerySupport>::ResponseKinds as Archive>::Archived:
        rkyv::bytecheck::CheckBytes<
            Strategy<
                rkyv::validation::Validator<
                    rkyv::validation::archive::ArchiveValidator<'a>,
                    rkyv::validation::shared::SharedValidator,
                >,
                rkyv::rancor::Error,
            >,
        >,
```

`shoal-core/src/client.rs:989-999`, `ShoalResultStream::skip`

## The proposals

Ordered by value over cost. The first two are worth doing on their own; the rest are cheap
follow-ons.

### 1. `trait ShoalQuery { type Response; }`

The core of the page. Each generated query struct declares what it answers with:

```rust
impl ShoalQuery for MovieGet     { type Response = Movie; }
impl ShoalQuery for MovieInsert  { type Response = (); }
```

and the client gains a typed entry point beside the untyped one:

```rust
pub async fn send_one<Q: ShoalQuery + Into<S::QueryKinds>>(
    &self, query: Q,
) -> Result<TypedResponse<Q::Response>, Errors>
```

so `access()` takes no turbofish and cannot name the wrong type. It generalises what `ExistsQuery`
already does for one operation to all of them, and it is the return-side mirror of the projection
bound that already exists — **half the machinery is written; this is the other half.**

Projections need `Response` to depend on the projection chosen, which is a builder producing a
different type rather than mutating a field. That is a real change to the generated `Get` and the
main cost of this item.

### 2. A sealed trait for the bounds

```rust
pub trait ClientSchema: QuerySupport
where
    for<'a> <Self::ResponseKinds as Archive>::Archived: CheckBytes<...>,
{}

impl<T> ClientSchema for T where T: QuerySupport, ... {}
```

Every method's ten-line clause becomes `S: ClientSchema`. Purely mechanical, zero runtime cost, no
API break for callers who never wrote the bound themselves, and it deletes eighty lines that are
currently the client's worst readability problem. **Cheap enough to do before anything else on this
page, or independently of it.**

### 3. Type the error

`Errors::WrongType(String)` becomes a variant naming both types via `std::any::type_name`. Trivial,
independent, and worth doing whether or not (1) ever happens — `"Wrong Type!"` is not a diagnostic.

### 4. Typestate on `ShoalQueryStream`

`ShoalQueryStream<Q, Open>` and `ShoalQueryStream<Q, Closed>`, so sending after `close` is a
compile error rather than `Errors::StreamAlreadyTerminated`
(`shoal-core/src/client/errors.rs:37`). Small, cheap, and it removes one runtime error from an API
whose state machine is otherwise invisible.

### 5. A typed result stream — the hard one

A bundle is a `Vec<S::QueryKinds>` by construction (`shoal-core/src/shared/queries.rs`), so a
heterogeneous bundle's responses cannot share a type. Three shapes:

| Shape | Verdict |
| --- | --- |
| Type only homogeneous bundles — `Queries<S>` built from one query type yields `TypedStream<Q::Response>` | **Recommended.** Covers the common case and costs one extra builder |
| Tuple- or HList-typed bundles — `(MovieGet, KeywordGet)` yields `(Response<Movie>, Response<MoviesByKeyword>)` | **Rejected.** Elegant for two or three queries, a compile-time explosion beyond, and bundles here run to hundreds |
| Leave streams untyped, type only `send_one` | Acceptable fallback if the homogeneous builder proves awkward |

## Recommendation

**Take (2) and (3) immediately, (1) as the substantial piece, (4) and (5) opportunistically.**

| | |
| --- | --- |
| **Rank** | **B** for (1); **A**-tier by cost for (2) and (3), which are near-free |
| **Impact** | Argued, and not a performance claim at all — every one of these is compile-time only |
| **Difficulty** | S for (2), (3), (4). L for (1), because projections make `Response` depend on a builder step. M for (5) |
| **Depends on** | nothing. Entirely additive, and parallel to every other page here |
| **Blocks** | nothing |
| **Tradeoff** | None — the untyped path stays for SHQL and `shoalctl` |
| **Benchmark** | none needed, and none possible; nothing here exists at runtime |

Two things this page must not overclaim.

**Typing cannot remove the server's runtime match.** The wire carries an enum by construction — a
bundle is a `Vec<QueryKinds>` and a response is a `ResponseKinds`, and the server dispatches over
them (`shoal-derive/src/traits/db.rs`). The goal is to make the *client's* view total, not to
eliminate dispatch. A design that tried would end up encoding the schema in the type of every frame
and would still have to match at the boundary.

**The guarantee that matters most is not in the type system.** No amount of client-side typing helps
when the peer was compiled from a different schema: both sides are individually consistent and only
their agreement is wrong. That check is [D2](framing.md)'s schema fingerprint — a handshake field.
It is worth saying on this page because "stronger compile-time guarantees" is the ask that a
fingerprint actually answers, and the type system cannot.

## Alternatives rejected

**A fully session-typed protocol** — typestate over the entire request/response exchange, so an
out-of-order operation is unrepresentable. Two objections, and the second is fatal. The compile
errors would be worse than what they replace, on a client whose error messages are already its
weakest point. And SHQL is dynamic by design: `QuerySupport::parse` matches a table name as a
string at runtime and yields a `QueryKinds` with no static type at all
(`shoal-derive/src/structs/client.rs`). `shoalctl` is built on that path. A protocol that cannot
express an untyped query cannot serve the tool most likely to use it.

**Making `access::<T>()` infallible by construction**, returning `&ArchivedVec<T::Archived>` rather
than a `Result`. Attractive with (1) in place, but the response still has to be checked for the
error variant [D2](framing.md) adds, so a `Result` survives anyway — just for a better reason.

## What it costs

Compile time and generated code size. (1) adds an impl per query struct and (5) adds a builder;
neither is large, but `#[shoal::db]` already generates around 260 lines per table for SHQL parsing
alone, and this is on top.

## What it breaks

- **Nothing at runtime**, which is the point of the page.
- **Callers who wrote the `CheckBytes` bound themselves** would need to switch to `ClientSchema`.
  In practice that is only code inside this workspace.
- **`send_one`'s signature**, if the typed version replaces rather than joins the untyped one. It
  should join it — `send_one_typed`, or a `typed()` adapter — because `shoalctl` needs the untyped
  form and a client that forces a static type on a dynamically parsed query is a client `shoalctl`
  cannot use.

## Prerequisites

None. This page can be started at any point, and (2) and (3) could land this week.

## How it would be measured

It would not. Every item here is compile-time, and the correct check is a set of
`compile_fail` doctests: naming the wrong row type in `access`, passing a non-exists query to
`exists`, projecting another table's type, and sending on a closed stream. The first and last do
not currently fail to compile; the middle two already do
(`shoal-core/src/shared/traits.rs:41-44`, `shoal-derive/src/structs/get.rs:107-109`) and have no
test asserting it, which is worth fixing regardless of whether the rest of this page happens.

## Related

- [Derive Macros](../api/derive-macros.md) — what is generated, and where the type information is
  discarded
- [The Client](../api/client.md#shoalresponse) — `access` and the `exists` marker trait
- [F2. Projections on get queries](../features/projections.md) — the one place a query's response
  type is already constrained
- [D2. Framing and protocol evolution](framing.md) — the schema fingerprint, which is the guarantee
  this page cannot provide
- [item 54](../appendix/known-issues.md#54-shoaldb-needs-three-crates-the-caller-has-never-heard-of)
  — the other generated-code ergonomics problem
