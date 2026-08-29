# D5. Runtime portability

> **Step 1 is delivered.** The split shipped as
> [F15](../features/client-server-split.md): `shoal-proto` holds the wire format and links no
> async runtime, `shoal-client` holds the tokio client, and `shoal --no-default-features` builds a
> client with no glommio in its graph. **Step 2, the runtime abstraction itself, is not built and
> still stands at rank C** — this page's recommendation was to leave it there until somebody asks,
> and nobody has. What follows is the argument as written, with the claims the work overturned
> struck through rather than deleted.

## Context

The ask is "support different async runtimes — tokio, glommio, and others". **The problem behind
the ask is not that the client is tokio-only. It is that the client is not tokio-only.** A pure
client — a TUI, a benchmark runner, an application that never starts a server — is forced to
compile and link glommio, and therefore io_uring, and therefore Linux, for no reason it can see.

That is the defect. Runtime abstraction is the feature; **decoupling the client from the server is
the fix**, it is independent of every other item in this chapter, it changes no behaviour, and it
is the only entry here with no design risk at all. This page argues that they are two pieces of
work and that only one of them is clearly worth doing.

## What exists today

`shoal-core` declares a feature that looks like it separates the two halves:

```toml
default = ["server"]
# Enables the server features for Shoal
server = ["glommio"]
```

`shoal-core/Cargo.toml`

It does not work. Two unconditional lines defeat it:

```rust
pub mod client;
pub mod server;
```

`shoal-core/src/lib.rs:3-4`

```rust
use glommio::TaskQueueHandle;
```

`shoal-core/src/shared/traits.rs:4`

`pub mod server;` is declared with no `#[cfg]`, and `server.rs` opens with a glommio import. Worse,
`shared/traits.rs` — the module the client depends on for `QuerySupport`, `RkyvSupport`, and the
query and response types — imports `glommio::TaskQueueHandle` at module scope, because
`ShoalDatabase::new` takes one. Only the *methods* of `ShoalDatabase` are `#[cfg(feature =
"server")]`-gated; the imports and the module declarations are not. Turning the feature off does
not produce a client-only build; it produces a build failure.

**This is no longer the state of the tree.** The feature is gone from `shoal-core`, which *is* the
server and has no configuration in which it does not need an engine; nine dead `#[cfg]` attributes
went with it. The name now lives on the `shoal` facade, where it means whether to link `shoal-core`
at all. One further detail this page did not have: four of `ShoalDatabase`'s methods carried no
`#[cfg]` either, so the gating was incoherent as well as ineffective ([F15](../features/client-server-split.md)).

The consequence is visible downstream, and somebody already wrote down that it was wrong:

```toml
# this shouldn't be required by the user
deepsize2 = "0.1"
kanal = { workspace = true }
glommio = { workspace = true }
```

`shoalctl/Cargo.toml:19-22`

`shoalctl` is a terminal UI that opens a `Shoal<S>` and sends queries. It has no server, no shard,
and no storage. It declares glommio anyway. `shoal-bench` carries the same three with a comment
pointing at
[item 54](../appendix/known-issues.md#54-shoaldb-needs-three-crates-the-caller-has-never-heard-of),
which is the same problem seen from the macro's side.

So the true dependency surface of "a Shoal client" today is tokio with `full`, glommio, bb8, kanal,
papaya, rkyv, lru, intmap, bytes, deepsize2, winnow, and the whole OpenTelemetry stack — none of
it optional. ~~and tonic~~ `tonic` left with the three unused OpenTelemetry dependencies it came in
with, which is 512 lines off `Cargo.lock` and one fewer major of `rustls` in the graph, and changes
nothing about the argument below: the surface is still everything a server needs.

There is a second, subtler coupling in the same direction. `ShoalQuerySupport` is a *shared* trait
implemented by the generated `QueryKinds`, and one of its methods takes server types:

```rust
fn split_by_shard<'a>(&self, ring: &'a Ring, found: &mut Vec<(&'a ShardInfo, Self)>);
```

`shoal-core/src/shared/traits.rs:108`, `ShoalQuerySupport::split_by_shard`

~~`Ring` and `ShardInfo` live in `shoal-core::server`. The client's query enum carries a method whose
signature names the server's routing types — which is why the client cannot be separated from the
server without moving it, and, separately, why [D7](shard-aware-routing.md) finds the routing logic
already compiled into every client.~~

**Resolved the other way round.** Rather than move `Ring` clientward, `split_by_shard` moved off
the shared trait onto `ShardRouting`, a server-side extension trait in `shoal-core::server::routing`.
The bound sits on `ShoalDatabase::ClientType`, so only something that owns a ring ever asks for it —
which is also what makes `#[shoal::db(client)]` work without a second mechanism. D7 can still move
`Ring` to the client; it now moves a trait impl rather than a trait method.

## The options

### Step 1: split the crate

A new crate — `shoal-proto`, or `shoal-client` — holding the things both peers need and neither
runtime owns:

| Moves | Stays in `shoal-core` |
| --- | --- |
| `Queries`, `QueryKinds`, `ResponseKinds`, `Response`, `ResponseAction` | `server/`, storage, the shard |
| `QuerySupport`, `RkyvSupport`, `ShoalQuerySupport`, `ShoalResponseSupport`, `ShoalProjection` | `ShoalDatabase`, and its `TaskQueueHandle` |
| The [D2](framing.md) framing module — built as `shoal-core/src/shared/protocol.rs`, depending on `core` and `uuid` alone so it moves unchanged | The listener and the relays |
| `Shoal<S>` and the client machinery | |

`shoal-core` depends on it. `shoalctl` and `shoal-bench` depend on it and drop glommio. `Ring` and
`ShardInfo` either move too — [D7](shard-aware-routing.md) wants the client to hold a tablet map
anyway, so this is not a stretch — or `split_by_shard` moves off the shared trait onto a server-side
extension trait.

This is a mechanical change with no behaviour in it, and it closes item 54 as a side effect: the
generated code's `glommio` path disappears from a client-only schema.

### Step 2: the runtime abstraction itself

Only after the split does this become a real question, and there are three answers in wide use.

**A `Runtime` trait with associated I/O types.** `type TcpStream`, `type JoinHandle`, `spawn`,
`sleep`, `timeout`. Maximum flexibility; the caller picks at the type level and two runtimes can
coexist in one binary. The cost is that every signature in the client grows a runtime parameter,
every I/O type becomes an associated type, and the error messages — already the client's worst
ergonomic problem, given the ten-line `CheckBytes` bound copy-pasted onto eight public methods
([D8](typed-queries.md)) — get materially worse.

**Mutually exclusive feature flags.** `runtime-tokio`, `runtime-glommio`, `runtime-smol`, one
concrete implementation selected at compile time behind a common module path. This is what `sqlx`
and `redis-rs` do. Public signatures stay concrete, error messages stay readable, and the cost is
that a binary gets one runtime — which for a database client is nearly always true anyway.

**Stay tokio-only, and say so.** What `reqwest` does. tokio is the default runtime of the Rust
datacenter; a client that works there works for almost everyone.

## Recommendation

**Split the crate now. Prefer feature flags over a `Runtime` trait. Keep tokio as the default and
the only shipped backend until someone asks for another.**

| | |
| --- | --- |
| **Rank** | **A2** for the split — a mechanical change with no behaviour and no design risk. **C** for the runtime abstraction, which has no demonstrated caller |
| **Impact** | Argued for the abstraction. The split is not a performance change at all — it is a dependency and packaging one |
| **Difficulty** | M for the split (moving modules and one trait method). L for feature-flagged runtimes. XL for a `Runtime` trait |
| **Depends on** | nothing |
| **Blocks** | ~~[D2](framing.md) and~~ [D7](shard-aware-routing.md) only in the sense of where their code should live. D2 turned out not to be blocked at all: its module has no runtime dependency to split away from, so it was written where it is and moves later ([F10](../features/framing-and-protocol-evolution.md)) |
| **Tradeoff** | None for the split. Contained for feature flags — one runtime per binary |
| **Benchmark** | ~~none needed for the split; nothing measurable changes~~ **Wrong, and measured.** True at the source level — no algorithm, data structure or allocation changed. False at the codegen level: this workspace has no `[profile.release]` at its root ([item 66](../appendix/known-issues.md)), so every release build is `lto = false`, and a call that was intra-crate becomes a real call unless it is generic, `const`, or `#[inline]`. Captured before and after; see [F15](../features/client-server-split.md)'s Performance section |

The split is recommended **on its own merits and regardless of whether the abstraction is ever
built**. It closes item 54, deletes three lines from two manifests that somebody already flagged as
wrong, removes io_uring from the dependency tree of every client, and makes it possible to publish
a client crate at all — which is currently blocked anyway by a path dependency on a local glommio
fork ([TODOs](../appendix/todos.md#build-and-packaging)).

The abstraction is ranked `C` because **the compelling non-tokio case is not the one the ask
implies**. A glommio client would exist for one reason: an application already running on
thread-per-core that wants to query a Shoal server it colocates with. For that caller, a glommio
TCP client is the wrong answer to the right question — the right answer is an in-process path that
skips the socket entirely and hands a `Queries` to the local shard's channel, which is a different
feature and belongs in [TODOs](../appendix/todos.md) rather than here. Building runtime portability
to serve a case better served another way is how an abstraction ends up with one real
implementation and a trait nobody needs.

## Alternatives rejected

**`async-compat` or a similar shim**, letting tokio-based code run on another executor. It works,
and it is a per-future wrapper on the hot path of a system that counts microseconds. Declined for
the same reason a mesh sidecar is declined in [D4](encryption.md): a general adapter in the fast
path is a cost paid forever for flexibility used once.

**Gating the existing modules properly rather than splitting the crate.** Adding `#[cfg(feature =
"server")]` to `pub mod server;` and untangling `shared/traits.rs`'s imports would fix the build
failure without moving anything. It is less work and it leaves the client and the server sharing a
version, a release, and a dependency set — which is the thing that made this defect invisible for
as long as it was.

## What it costs

Only the split has a cost, and it is churn: every `use shoal_core::shared::...` in the workspace
moves, and `shoal-derive` has to emit paths into the new crate. The facade crate `shoal` is the
right place to absorb that — re-exporting both so that downstream code and generated code name
`::shoal::...` and never see the boundary, which is also the fix item 54 asks for.

## What it breaks

- **Every import path outside `shoal`**, once. Mitigated by the facade, and it is a compile error
  rather than a behaviour change, which is the good kind.
- **The assumption that one crate is both peers.** Today a schema compiles into a binary that could
  be either. After the split, a client-only crate exists that cannot start a server, which is the
  point but is a new thing to keep working — a test that builds a schema against the client crate
  alone is what keeps the glommio import from creeping back.

## Prerequisites

None. This is the one item in the chapter that can start today.

## How it would be measured

It would not, and that is not a gap. The split changes no code path; it changes which crates a
binary links. The correct check is a build, not a benchmark: **a test crate that declares only the
client crate, writes a schema, and compiles** — which would fail today for three separate reasons
and is the regression test for all of them.

## Related

- [item 54](../appendix/known-issues.md#54-shoaldb-needs-three-crates-the-caller-has-never-heard-of)
  — the same defect seen from the macro's side
- [D2. Framing and protocol evolution](framing.md) — which wants somewhere runtime-free to live
- [D7. Shard-aware routing](shard-aware-routing.md) — which needs `Ring` on the client side, the
  same move this page requires for a different reason
- [D8. Compile-time guarantees](typed-queries.md) — the other half of the client's ergonomics
- [TODOs](../appendix/todos.md#build-and-packaging) — the packaging constraints this sits inside
