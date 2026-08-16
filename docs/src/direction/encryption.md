# D4. Encryption in transit

> **This has since been built**, as [F14](../features/encryption-in-transit.md). The page is kept as
> the design record — the [chapter overview](overview.md#the-d-number) says why a `D` page outlives
> the `F` page that supersedes it — and it is worth reading precisely because it was **wrong about
> the mechanism** and the build had to correct it before it could start.
>
> **This page's central technical claim was wrong**, and it is corrected in place below rather than
> quietly edited. The claim was that rustls' unbuffered API decrypts a record in place into a
> buffer the caller supplies, so that handing it the `AlignedVec<16>` preserves the zero-copy read.
> **No released rustls does that.** The correction is in [The
> options](#the-options) and in [rustls, and why the API choice is the whole
> decision](#rustls-and-why-the-api-choice-is-the-whole-decision); it moves the recommendation from
> rustls to kTLS, which this page had ranked second on an obstacle that turns out not to apply.
> Nothing else on the page changed — the argument about *what has to be protected* was right, and
> it is what caught the error.

## Context

This is the page where the chapter's central tension is argued in full, because encryption is the
one item here that directly contradicts the thing the current branch exists to provide.

The response path is zero-copy. The socket payload is read into an `AlignedVec<16>` and
`ShoalResponse` holds a pointer into that buffer, so reading a row is a pointer cast rather than a
parse ([The Client](../api/client.md#shoalresponse)). **Every naive way of adding TLS destroys
that**, because a TLS library conventionally owns its plaintext buffer and hands the application a
slice of it — which then has to be copied into aligned memory before rkyv can access it. One full
copy of every row returned, on the path built to have none.

The good news is that this is avoidable, and the way to avoid it is a specific API choice made at
the start rather than an optimization applied later. That choice is what this page is about.

## What exists today

Plaintext, in both directions, with no configuration to change it. ~~See
[D3](authentication.md#what-exists-today) — the two absences are the same absence.~~ **They were
not the same absence**, which is the thing this chapter got most wrong: authentication landed
without encryption ([F12](../features/authentication.md)), so what is left here is this page alone.
The practical consequence is that a SCRAM exchange is currently visible on the path — the username
in clear, and the salt, nonces and proof of an exchange an observer can grind offline.

The read that has to be protected:

```rust
let mut aligned_buff = AlignedVec::<16>::with_capacity(frame.rest_len);
aligned_buff.resize(frame.rest_len, 0);
self.reader.read_exact(&mut aligned_buff).await?;
```

`shoal-core/src/client.rs:1063-1066`, `TcpProxy::read_frame`

The citation moved. This page, [D1](transport.md), [the chapter
overview](overview.md#the-constraint-every-page-inherits) and [The Client](../api/client.md) all
still named `client.rs:524-527`, `TcpProxy::start`, which is where this read lived before
[F10](../features/framing-and-protocol-evolution.md) split frame decoding out of the relay loop.
Only this page's copy is corrected here; the other three are stale in the same way and are filed
rather than fixed in passing.

The property is that the bytes rkyv will read land, once, in memory the client allocated and
aligned. Any option that cannot preserve that is paying a copy per response.

## The options

| Option | Zero-copy read survives | Handshake | Notes |
| --- | --- | --- | --- |
| **Plaintext** | Yes | — | Only defensible on a network that is trusted *and* isolated. Worth stating as an assumption rather than leaving implied |
| **rustls, buffered API** | **No** | userspace | rustls decrypts into its own buffer and hands out a slice; the client copies into the `AlignedVec` |
| ~~**rustls, unbuffered API**~~ | ~~**Yes**~~ **No** | userspace | ~~Decrypts in place into a caller-supplied buffer — hand it the `AlignedVec<16>`~~ **It does not.** Plaintext comes out of a `Vec<u8>` rustls owns, so this row is the buffered row with more work. See below |
| **rustls handshake + own record layer** | Yes | userspace | `dangerous_into_kernel_connection` yields the traffic secrets; decrypt with `aws-lc-rs` directly into the `AlignedVec<16>`. Means owning nonces, sequence numbers, padding, key updates and alerts |
| **kTLS** | Yes | userspace | Handshake in userspace, record layer in the kernel, so `read()` returns plaintext into any buffer. NIC offload on capable hardware |
| **WireGuard / IPsec between hosts** | Yes | out of process | Zero application code. No per-connection identity, so [D3](authentication.md) gains nothing |
| **A service mesh sidecar** | Yes, out of process | out of process | Also zero application code, and worse: every packet takes an extra userspace hop through a proxy, which is latency in exactly the place this system cares about |

Three of these need expanding.

### rustls, and why the API choice is the whole decision

~~rustls exposes two shapes. The conventional one wraps a stream and manages its own plaintext
buffer; the unbuffered one hands the caller each state transition and lets the caller supply the
buffers, including decrypting a record in place. With the unbuffered API the client can decrypt
directly into the `AlignedVec` it was going to allocate anyway, and `ShoalResponse` keeps working
unchanged — the pointer it holds points into aligned memory that happens to have arrived
encrypted.~~

**This was checked against the source and it is false.** rustls does expose two shapes, and the
unbuffered one does hand the caller each state transition and let it own the *ciphertext* buffer.
What it does not do is decrypt into that buffer:

```rust
pub struct ReadTraffic<'c, 'i, Data> {
    conn: &'c mut UnbufferedConnectionCommon<Data>,
    // for forwards compatibility; to support in-place decryption in the future
    _incoming_tls: &'i mut [u8],
    // owner of the latest chunk obtained in `next_record`, as borrowed by `AppDataRecord`
    chunk: Option<Vec<u8>>,
}

// TODO deprecate in favor of `Iterator` implementation, which requires in-place decryption
pub fn next_record(&mut self) -> Option<Result<AppDataRecord<'_>, Error>> {
    self.chunk = self.conn.core.common_state.received_plaintext.pop();
```

`rustls-0.23.43/src/conn/unbuffered.rs:328-361`

The input buffer is held with a `_` prefix, against a future the two comments name. `AppDataRecord`
lends its payload out of `chunk`, and `ChunkVecBuffer::pop` returns an owned `Vec<u8>`. So the
plaintext of every record lands in a heap allocation rustls made, and reaching the `AlignedVec` from
there is a copy — **the same copy the buffered row was rejected for**, bought at the price of
driving a state machine by hand.

The distinction the page missed is that the unbuffered API's subject is *allocation and lifetime*,
not *placement*. It exists so that a `no_std` or embedded caller can decide when and where memory is
taken, which is a different property from deciding where a decrypted byte lands. Reading the row's
promise as the second when the crate offers the first is an easy mistake to make from the
documentation alone, and only the source settles it.

**What survives.** The section title is still right: the API choice is the whole decision. It is a
choice between three mechanisms rather than between two rustls shapes, and the ranking below is
redone on that basis.

The cost that remains is real but different in kind: **AES-GCM over every byte**, at hardware rates
on any CPU with AES-NI. For a store returning rows measured in hundreds of bytes, the per-record
overhead — a fixed cost per TLS record, plus the framing — matters more than the throughput term,
which is an argument for larger response frames rather than against encryption.

**The framing interaction is worth stating**, and it is the part of this section that got *more*
important rather than less. TLS records have a maximum size around 16 KiB, so a response larger than
that spans records and cannot be decrypted in one pass into one buffer without the client tracking
record boundaries. ~~That is exactly what the unbuffered API asks the caller to do~~ — it is what
any userspace record layer asks the caller to do, and the unbuffered API does not help with it,
because the boundaries it exposes are between rustls' own `Vec`s rather than inside a buffer the
caller placed. It composes with [D2](framing.md)'s length prefix rather than fighting it, but it
means the read loop grows a state machine where it currently has two `read_exact` calls. This is the
largest piece of real work on this page, **and it is work only the two userspace options have to
do** — under kTLS the boundaries are the kernel's problem and the read loop does not change at all.

There is a second-order trap underneath it that is worth recording, because it is what any
in-place scheme has to solve and it is not obvious until it is drawn. Decrypting record `i+1` so
that its plaintext lands immediately after record `i`'s requires writing its 5-byte record header
at `end_of_plaintext_i - 5` — which is *inside* plaintext `i`. The five bytes have to be saved and
restored around each record, and the records have to be read strictly one at a time so that no
record's header is written before the previous record has been decrypted. Five bytes per 16 KiB is
nothing; discovering the constraint after the loop is written is not.

### kTLS

Handshake in userspace with rustls, then hand the negotiated keys to the kernel and let the socket
do the record layer. From then on `read()` returns plaintext, into whatever buffer the caller
names — so the current `read_exact(&mut aligned_buff)` works verbatim, and the client-side code
change is nearly zero. On NICs that support it, the record layer offloads to hardware and the CPU
cost approaches plaintext.

~~Two obstacles. It is Linux-only, which Shoal already is. And **the server side runs on glommio and
io_uring**, which has no kTLS integration — enabling kTLS on a socket is a `setsockopt` glommio does
not expose, and whether it composes with io_uring's submission path is unverified. So kTLS is the
right destination and the wrong starting point.~~

**Both obstacles were overstated, and the second was the reason this option was ranked below
rustls.** Taking them in turn.

Linux-only stands and costs nothing. Shoal is Linux-only already, because glommio is.

`setsockopt` being unexposed is not an obstacle at all on the half that matters. The property being
protected is on the **client**, which is tokio, where the socket's file descriptor is one
`AsRawFd::as_raw_fd()` away and `setsockopt(TCP_ULP)` is an ordinary `libc` call on it. The server
does run on glommio — and `glommio` is a **path dependency on a local fork** (`../glommio`, root
`Cargo.toml`), so a `setsockopt` it does not expose is a function this repository can add rather
than a capability it lacks. The sentence assumed an upstream crate.

~~What remains is the one thing the paragraph got right, reduced to its true size: **whether kTLS
composes with io_uring's submission path on the server is unverified.**~~ **It has since been
verified, and it composes.** A spike was written before any of this was built, because it was the
only unverified fact the corrected recommendation rested on. Three phases, all passing:

| Phase | What it establishes |
| --- | --- |
| **A** — kTLS over blocking sockets | rustls completes a TLS 1.3 handshake, `dangerous_extract_secrets` yields the keys, two `setsockopt` calls per direction hand them to the kernel, and both peers then exchange plaintext with bare `read`/`write`. Negotiated `TLS13_AES_256_GCM_SHA384` |
| **B** — the same socket under glommio | the descriptor is adopted with `FromRawFd` and read and written through io_uring. **This is the composition the page called unverified.** It works |
| **C** — a MiB response, read the client's way | ~64 TLS records, reassembled by the kernel; the server writes `[preamble][archive]` with `write_vectored` exactly as `client_tx_relay` does; the client reads with the same two `read_exact` calls it uses today, into a 16 byte aligned buffer, and the payload comes back byte for byte identical |

Phase C is the one that decides it. It is the `macro/transport/*/large` regime, run through the
real read shape, and **not one line of the client's read path had to change** — which is the claim
this page has been making since it was written and could not previously substantiate.

Two things the spike found that are not obvious and belong in the build:

- **Session tickets have to be off.** A TLS 1.3 server sends `NewSessionTicket` *after* the
  handshake. On a socket the kernel has taken over, that is a non application record, and a plain
  `read` cannot return it — it fails with `EIO` instead. `send_tls13_tickets = 0` on the server
  config. This is the classic kTLS trap and it costs resumption, which is one of the three
  mitigations [D3](authentication.md#what-it-costs) proposes for the fifty-handshake problem. **The
  two interact and the page had not noticed**: taking kTLS makes D3's first mitigation unavailable
  in the form it was written.
- **glommio already exposes what is needed.** `TcpStream` implements both `AsRawFd`
  (`glommio/src/net/tcp_socket.rs:427`) and `FromRawFd` (`:432`). The fork is not required for the
  socket option after all — the sentence above about a `setsockopt` glommio "does not expose" was
  wrong twice over.

The kernel side is present on the reference machine: `CONFIG_TLS=m` and `CONFIG_TLS_DEVICE=y`, so
both the software record layer and NIC offload are available. The `tls` module is **not** autoloaded
by `setsockopt` — a machine that has never used kTLS answers `ENOENT` until `modprobe tls` has run,
which is a deployment note rather than a code one and is exactly the kind of thing that would
otherwise be discovered in production.

rustls is still what performs the handshake. `dangerous_into_kernel_connection`
(`rustls-0.23.43/src/conn/kernel.rs`) exists for exactly this: drive an unbuffered connection to
`WriteTraffic`, then take the `ConnectionTrafficSecrets` out and give them to something else — the
kernel, or a record layer of one's own. So the handshake work is shared between all three userspace
options and none of it is thrown away whichever wins.

### Node-level encryption

WireGuard, IPsec, or a mesh sidecar move the problem out of Shoal entirely: no application code, no
handshake, no protocol change. It is genuinely the cheapest option and it should not be dismissed —
for a deployment that already runs one, turning it on is the correct answer and this page's
recommendation is redundant.

It fails on two counts for a general answer. A sidecar proxy adds a userspace hop per packet, which
is latency in the one place a thread-per-core store cannot afford it. And none of the three gives
Shoal a per-connection peer identity, so [D3](authentication.md) gets nothing from them and would
still need SCRAM.

## Recommendation

~~**rustls with the unbuffered API, decrypting in place into the response buffer. kTLS later.
Configured per listener, defaulting to off.**~~

**kTLS, with rustls performing the handshake. Configured per listener, defaulting to off.**

The first and second clauses swapped and the third did not move. The reason is the whole of the
correction above: the option this page recommended cannot do the thing it was recommended for, and
the option it deferred can do it with *less* code than either alternative rather than more.

| | |
| --- | --- |
| **Status** | **Built**, as [F14](../features/encryption-in-transit.md), on the corrected recommendation rather than the original one |
| **Rank** | **B**, ~~ahead of [D3](authentication.md) because it decides it~~ — it did not decide it. D3's SCRAM half shipped first and without this. What this decides is whether there is a *second* mechanism, and whether the first one runs in clear |
| **Impact** | Argued — and the cost is the one thing on this page that genuinely needs a number before it lands |
| **Difficulty** | XL — reaches the client's read loop, the server's write path, and the configuration. **Unchanged in grade and redistributed in shape**: under kTLS the client's read loop is the one part that does *not* change, and the weight moves onto the handshake, the `setsockopt` dance, and a glommio fork |
| **Depends on** | ~~[D2](framing.md) for the handshake~~ — **satisfied** by [F10](../features/framing-and-protocol-evolution.md); [D1](transport.md) having declined QUIC, which would have supplied this |
| **Blocks** | [D3](authentication.md)'s mTLS path, which is all that is left of D3; channel binding for the SCRAM half, which [F12](../features/authentication.md) had to decline; and the re-auth ticket, which is only safe behind this ([TODOs](../appendix/todos.md)) |
| **Tradeoff** | Major — AES over every byte, and ~~a read loop that becomes a state machine~~ a dependency on a kernel feature and on a forked runtime. The state machine was the price of the userspace options; this one's price is that the thing doing the crypto is no longer in the build |
| **Benchmark** | ~~`transport/*`, unbuilt — and here that is a **blocker**, not a caveat~~ — **built** ([F13](../features/transport-workloads.md)), and built at eight workloads rather than four *because of this page*: a row-width axis was added on the argument below, since a per-byte tax is invisible at 256 bytes. The blocker is cleared and the TLS arms are one axis away |

Three parts to the recommendation, and the third is as important as the first two.

~~**Unbuffered rustls, chosen at the start.** Reaching for the buffered API and copying "for now" is
how the zero-copy path would be lost — it would work, it would be measurably slower, and the
measurement that would catch it does not exist. The alignment-preserving version is not
meaningfully harder if it is the first thing written.~~

**The instinct here was right and it was aimed at the wrong thing.** The danger it names is real:
the copied version is correct, it is measurably slower, and nothing in the repository would notice.
What the paragraph got wrong is that reaching for the *unbuffered* API is also how the zero-copy
path is lost, because it copies too — it just does so behind a state machine that looks like the
careful choice. A version of this mistake that shipped would have been harder to find than the
buffered one, not easier, because the code would read as though the property had been protected.

**kTLS first, because it is the only option that keeps the read loop.** Under kTLS the socket
returns plaintext into whatever buffer the caller names, so `read_exact(&mut aligned_buff)` at
`shoal-core/src/client.rs:1063-1066` is untouched, `ShoalResponse` is untouched, and there is no
record-boundary state machine to get wrong on the path with the least test coverage in the client.
The two userspace options both require writing that state machine; one of them additionally
requires owning nonce construction, sequence numbers, padding, key updates and alerts, which is
security-sensitive code this repository has no reason to own.

~~**The one thing to verify before committing to it**, and the only real question left on this page:
whether a kTLS socket works under glommio's io_uring submission path on the server.~~ **Verified —
it composes**, at a MiB across sixty four records, through `write_vectored` on the server and two
unchanged `read_exact` calls on the client. See [kTLS](#ktls) for what the spike covered and the two
traps it turned up. There is no unverified fact left under this recommendation.

**The handshake is shared whichever way that goes**, so it is the first thing to build and none of
it is wasted. `dangerous_into_kernel_connection` is the seam all three options meet at.

**Per-listener configuration, defaulting to off.** `Networking` grows a TLS section alongside
`interface` and `port` (`shoal-core/src/server/conf.rs:138-157`, which has since grown a third
field). This is not a hedge — it is what
keeps the benchmark harness and the integration tests on the plaintext path, so that captures stay
comparable against the frozen `B1-performance` baseline
([Benchmarking](../performance/benchmarking.md)). A build where every capture silently included TLS
would invalidate every number in the book.

## What it costs

- **AES-GCM over every byte in both directions**, at hardware rates, plus a per-record fixed cost
  that is proportionally worse for small responses.
- **A handshake per connection**, up to 50 of them (`client.rs:141-142`) — the same cost
  [D3](authentication.md#what-it-costs) accounts for, and the same mitigation: session resumption.
  **This is now additive rather than hypothetical**: a deployment with authentication on already
  pays three round trips and a PBKDF2 derivation per connection, and nothing measures either
  ([O30](../appendix/optimizations.md)).
- ~~**A read loop that becomes a state machine.** Today it is two `read_exact` calls; with unbuffered
  rustls it is a record-boundary-aware loop. That is where a bug would live, and it is on the path
  with the least test coverage in the client
  ([Test Coverage](../appendix/test-coverage.md)).~~ **Not under the corrected recommendation.**
  This was the largest code cost on the page and kTLS deletes it outright — the kernel owns the
  record layer, so the two `read_exact` calls stay two `read_exact` calls. It is still exactly what
  either userspace option costs, and it is the sharpest reason to prefer the kernel one.
- **A dependency on the kernel and on a forked runtime**, which is what replaces it. The crypto
  moves out of the build and into `CONFIG_TLS`, so "which cipher suite is this connection using" is
  answered by the kernel rather than by a crate lock, and enabling it on the server side means
  carrying a `setsockopt` in the local glommio fork. A deployment on a kernel without the `tls`
  module has to fall back, which means the userspace path cannot be deleted even once this works.
- **A certificate lifecycle**, which is an operational cost rather than a code one, and which
  [D3](authentication.md) needs anyway if mTLS is taken. Note that
  [F12](../features/authentication.md) already built the negotiation this would slot into:
  `AuthMechanism::MutualTls` is defined and refused, and turning it on is a new arm in two matches
  rather than a new exchange.

## What it breaks

- **The zero-copy property, if the wrong API is chosen.** This is the whole page. It is listed here
  as something that breaks rather than as a design note because it breaks *silently* — the copied
  version is correct, and nothing in the repository would notice. **This entry was written as a
  warning and it turned out to be a prediction about this page itself**: the API named in the
  recommendation was the wrong one, the version built on it would have been correct, and the only
  thing that caught it was reading the crate's source rather than its documentation. A design note
  cannot be the defence against a failure this quiet. The defence is a test that asserts the
  archive's address is 16-byte aligned, on a response large enough to span records, which is the
  first thing to write whichever mechanism wins.
- **`AlignedVec::with_capacity(len)` sized from the frame length.** The ciphertext is longer than
  the plaintext by the AEAD tag and the record framing, so the length the client reads and the
  buffer it allocates stop being the same number. Getting that wrong is a subtle sizing bug, and it
  interacts with the frame bound that
  [item 34](../appendix/resolved/unvalidated-length-prefix.md) added — a ciphertext is longer than
  the plaintext it carries, so a record that fits under `max_frame_bytes` in one form may not in
  the other.
- **Comparability of captures**, unless the per-listener default holds. An encrypted capture and a
  plaintext one are not the same measurement, the same rule that already applies to `hotpath` and
  `stage-profile` builds ([Performance Baseline](../performance/baseline.md)).

## Prerequisites

[D2](framing.md), for the handshake and because the record-boundary logic has to be written against
a framing layer that has a bounded length. [D5](runtimes.md)'s crate split, so a client-only crate
can pull rustls without pulling glommio.

~~**One prerequisite is added by the correction, and it comes before both of those:** a spike that
answers whether a kTLS socket works under glommio's io_uring submission path.~~ **Done, and it
passed** — see [kTLS](#ktls). It was written before anything else because getting the answer wrong
costs an XL feature built on the wrong mechanism, which had already happened once on this page. It
took an afternoon and it changed two things about the design, which is the argument for spiking a
load-bearing assumption rather than reasoning about it.

[D5](runtimes.md)'s crate split is also now a *weaker* prerequisite than this section claims. It was
listed so that a client-only crate could pull rustls without pulling glommio; under kTLS the client
pulls rustls for the handshake alone, and the record layer it would otherwise need is in the kernel.
The split is still wanted, and it is no longer load-bearing for this page.

## How it would be measured

**This is the one item in the chapter where the missing benchmark is a genuine blocker rather than
a caveat.** Everything else here is a capability question — it either exists or it does not, and
the cost is a startup or a per-connection one. Encryption is a per-byte tax on the hot path, and
the whole recommendation above turns on whether it is a few percent or a large fraction.

What is needed, in order:

1. ~~The `transport/{send_one,send_batched,stream,stream_unordered}` workloads
   ([TODOs](../appendix/todos.md#benchmark-coverage-the-harness-does-not-have)), which would give
   the client a number at all for the first time.~~ **Done**
   ([F13](../features/transport-workloads.md)) — and this page changed what got built. Four
   workloads at one row width would have answered this page's question with a number near zero,
   because a per-byte tax does not show up on a 256-byte response. They were built at eight, with
   row width as a second axis, and the first smoke run confirmed the two regimes are real: at 256
   bytes the four modes spread nearly 4×, at a MiB they collapse into one number because the wire
   is the whole cost.
2. `tracing` spans and `hotpath` scopes in `client.rs`
   ([O28](../appendix/optimizations.md#o28-the-client-takes-two-guards-on-its-response-map-for-every-query-it-sends)),
   so the share attributable to the read loop can be separated from the rest.
3. The same workloads run plaintext and encrypted, as a control pair — the pattern
   [F9](../features/ephemeral-tables.md) established for storage, applied to the wire.

~~A plaintext-versus-TLS pair is a **precondition** for taking this, not a follow-up. Until it
exists, the honest statement is that nobody knows what encryption costs this system.~~

**The pair exists and has been read.** `f14-encryption` is the capture; the numbers are in
[F14](../features/encryption-in-transit.md#performance) and the charts in
[Benchmark Results](../performance/transport.md#what-encryption-costs). The short version, and
it vindicates the argument this section makes:

| Row width | What TLS cost, one query deep |
| ---: | ---: |
| 256 B | +6.7%, not separable from noise |
| 4 KiB | +11.9%, not separable |
| 64 KiB | +25.7% |
| 1 MiB | **+77.7%** |

**A set measured only at 256 bytes would have said encryption was free**, which is precisely what
this page predicted and precisely why [F13](../features/transport-workloads.md) was built at two
widths rather than one. The marginal cost is ≈1 µs per response plus 0.159 ns per byte — 6.3 GB/s.

The finding this page did *not* predict is that the overhead collapses under concurrency: the same
MiB row costs +77.7% at one outstanding query and +4.8% at thirty-two, because twelve shards encrypt
in parallel and the bottleneck moves to the wire. What does not collapse is throughput, which TLS
costs about a tenth of. **Encryption is most expensive where the system is least busy.**

**The plaintext half of that pair now exists.** [F13](../features/transport-workloads.md) built the
four modes at two row widths, so what remains is the TLS axis on top of them — the same
control-pair shape [F9](../features/ephemeral-tables.md) established for storage, applied to the
wire, with `macro/transport/*/large` as the arm that decides whether this feature is affordable.

**Both halves of the pair now exist** ([F14](../features/encryption-in-transit.md)): sixteen arms,
eight plaintext and eight encrypted, differing in the wire and in nothing else. **Neither has been
captured**, so the sentence above still stands in full — the harness can now answer the question and
nobody has asked it.

**The correction adds a second question the same pair can answer**, and it is worth taking while
the harness is being touched rather than later. The recommendation is now kTLS *because* it is the
only mechanism that avoids a copy per response — but nobody has measured what that copy costs
either. A third arm running userspace TLS, against the same workloads, turns "kTLS is worth the
kernel dependency" from an argument into a number. It is one more axis on a set that is already
gaining one, and it is the only chance to measure the rejected option against the chosen one while
both are still cheap to build.

## Related

- [D1. The transport](transport.md) — QUIC would have supplied this, and why it was declined anyway
- [D2. Framing and protocol evolution](framing.md) — the handshake, and the length field that
  record framing interacts with
- [D3. Authentication](authentication.md) — what mTLS gives once this exists
- [The Client](../api/client.md#shoalresponse) — the zero-copy property being protected
- [Benchmarking](../performance/benchmarking.md) — why the per-listener default matters
