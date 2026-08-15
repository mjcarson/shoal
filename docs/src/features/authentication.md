# F12. Authentication

## Context

Anything that could reach the port could read and write any table. That was stated in the
[Wire Protocol](../architecture/wire-protocol.md#limitations) page and it was exactly true — there
was no credential, no identity, and no seam where one would go.

The usual objection is that a datacenter store behind a firewall does not need authentication, and
[D3](../direction/authentication.md#context) answers it: **identity is what makes an audit log
meaningful**, and **identity is what makes multi-tenancy possible at all**. A store with no notion
of who is connected cannot later grow per-table authorization, per-client quotas, or a way to
answer "which service filled this table with garbage". Per-table authorization has sat in
[TODOs](../appendix/todos.md#per-table-authorization) for exactly this reason: a design for it
written before there was any notion of a principal would have been a design for nothing.

D3 was blocked on a handshake and a message type, and that block is gone.
[F10](framing-and-protocol-evolution.md) landed the framed handshake, and `MessageType::Auth = 3`
and `AuthResponse = 4` have been defined and unwired since. What was left was a call site.

**This is half of D3.** That page recommends mTLS as the primary identity and SCRAM-SHA-256 as the
fallback for deployments with no PKI, and mTLS is not a credential an application checks — it is a
property of a TLS connection, and there is no TLS ([D4](../direction/encryption.md) is unbuilt). So
the half that needs no PKI is what got built, behind a negotiation step that makes mTLS a new
variant rather than a new handshake. The D3 page records which half is still open.

## What it does

**A server requires nothing by default.** A config with no `auth` section behaves exactly as every
server did before this, which is what keeps the benchmark harness, the integration suite and the
[frozen baseline](../operations/performance-baseline.md) comparable.

A server that wants credentials says so:

```yaml
auth:
  required: true
  users:
    reader:
      # derived at startup and then dropped; convenient, and a password in a file
      password: "hunter2"
    service:
      # what a deployment that does not want a password on disk writes instead
      scram_sha_256:
        salt: "mreM7hc9ajmfASxDVtMmYA=="
        iterations: 4096
        stored_key: "mclPl9SbAkgUFigfCfZXDextXlX9lbyXp2EkZKstVVg="
        server_key: "1nuhjMmXMbqkuTxZEZzCClfB/wX0AfVCw/pPihZ48Io="
```

The second stanza is produced by `cargo run --example scram_credential -- <username>`, which reads
the password off stdin rather than off the command line and prints exactly the block above.

A client that has credentials says so with a second constructor. `Shoal::new` is unchanged, and
every one of its call sites — fifteen of them across the workloads, the tests and the examples —
still compiles and still connects:

```rust
let client = Shoal::<MyDbClient>::with_credentials(
    "127.0.0.1:12000",
    Credentials::scram("reader", "hunter2"),
)
.await?;
```

On the wire, a connection now opens like this:

```text
 client                                                            server
   │  Hello         fingerprint, max frame, mechanisms I can do       │
   ├─────────────────────────────────────────────────────────────────►│
   │  HelloAck      fingerprint, max frame, reason, mechanism I chose  │
   │◄─────────────────────────────────────────────────────────────────┤
   │  Auth          n,,n=<user>,r=<cnonce>                             │
   ├─────────────────────────────────────────────────────────────────►│
   │  AuthResponse  Challenge  r=<cnonce||snonce>,s=<salt>,i=<iters>   │
   │◄─────────────────────────────────────────────────────────────────┤
   │  Auth          c=biws,r=<cnonce||snonce>,p=<proof>                │
   ├─────────────────────────────────────────────────────────────────►│
   │  AuthResponse  Success    v=<server signature>                    │
   │◄─────────────────────────────────────────────────────────────────┤
```

The last four frames only happen when the `HelloAck` named a mechanism. A server that requires
nothing writes a zero there, which is the byte every server wrote before this existed.

Three things it produces that did not exist before:

- **A `Principal`** — a name and how it was proved — which the connection task logs and nothing
  else consults yet. Authorization is what it exists for and is deliberately still unbuilt.
- **A refusal at connect time** rather than a closed socket, in two shapes: a `HelloAck` carrying
  `RefusalReason::NoCommonAuthMechanism` for a client that offered nothing the server accepts, and
  an `AuthResponse` with `AuthStatus::Failed` for one that offered something and could not prove it.
- **Mutual authentication.** SCRAM proves the server to the client as well, and the client checks
  the final `v=` signature rather than assuming it.

## Design choices

**The mechanism fields came out of reserved bytes, not off the end of a body.** `Hello` had four
reserved bytes and `HelloAck` had three, written as zeroes and ignored on read. Two of the first
and one of the second now carry a mechanism bitmap and a selected mechanism, and zero means "none"
in both directions — so this landed without touching the version byte or the size of either frame.
That is what those bytes were reserved *for*, and spending them this way is the mechanism by which
a field can be added without a flag day. A field that will not fit in what is left needs a new
protocol version, not a longer body.

**The server picks; the client offers.** The client sends a set of mechanisms it can do and the
server names exactly one, walking its own preference order rather than the client's bits. This is
Cassandra's `AUTHENTICATE`-names-the-mechanism shape ([D9](../direction/prior-art.md#cassandra)),
and the reason it matters is that the reverse — a client naming what it will use — lets a client
that offers only the weaker of two mechanisms talk a server into accepting it.

**A `HelloAck` naming a mechanism the client cannot read decodes as `None`, and the client then
refuses itself.** This is safe in exactly one direction and it is this one: the client is the peer
that has to *do* the mechanism, so a name it cannot read leaves it with nothing to send. A server
never reads that field.

**The crypto is not in `shared::protocol`.** That module states as an invariant that its whole
dependency list is `core` and `uuid`, so it can move into a client-only crate when
[D5](../direction/runtimes.md) splits the crates. `protocol::auth` therefore holds the frame layout
and nothing else — the payloads are opaque bytes to it — and `shared::auth` holds the mechanism,
the credential store and the six crypto dependencies. The split is not cosmetic: putting SHA-256
in `protocol` would have made D5's crate split a dependency negotiation rather than a move.

**The messages are RFC 5802's text, byte for byte**, not a binary encoding of the same
construction, even though both peers are built from this repository. The proof is computed over the
concatenation of the message strings, so the encoding *is* part of the cryptography — and matching
the RFC is what lets RFC 7677's own test vector be a test here rather than this implementation
being checked only against itself.

**An unknown user is answered with a manufactured credential rather than a rejection.** The store
never returns "no such user": an unknown name gets a salt derived from its own bytes and a
per-process key, the default iteration count, and keys no password derives to. The exchange runs to
its end and fails at the proof, which is exactly where a wrong password fails. Deriving the fake
salt from the username means probing the same name twice gets the same challenge, the way a real
user would; deriving it from a per-process key means it cannot be computed by anyone who has not
already got into the server.

**Every comparison of a secret is `subtle::ConstantTimeEq`.** The client proof and the server
signature are both values the peer chose the bytes of, and a short-circuiting comparison of one is
a way to guess it a byte at a time.

**Credentials live on the connection manager.** `bb8` already treats `connect` as the place a
connection becomes usable, so a connection the pool opens to replace a dead one re-authenticates
with no code anywhere else — the compensating gain [D3](../direction/authentication.md#what-it-costs)
predicted, and it arrived exactly as predicted.

**The exchange runs before the stream is split.** Everything it reads would otherwise be handed to
the relays on the server and to the response proxy on the client, which would decode a challenge as
a response to a query nobody sent. This is the same constraint the handshake already had, and
`shoal/tests/auth.rs` pins it with a raw socket that sends a bundle of queries instead of a proof.

**`UserCredential` is two optional fields rather than the two-variant enum it wants to be.** The
`config` crate cannot deserialize an externally tagged enum whose variant carries a struct — it
answers `does not have variant constructor scram_sha_256` for the spelling every YAML example would
use — so the shape that reads correctly won over the shape that models correctly, and
`UserCredential::to_stored` carries the check the type would have made unnecessary.

## Alternatives rejected

**A shared bearer token in `Hello`.** One string every client holds, no round trips, and it would
have fit in the reserved bytes. Rejected for the reason [D3](../direction/authentication.md#the-options)
gives: one credential for everyone is no audit trail and no revocation short of a restart, and on a
plaintext link — which is every link, since D4 is unbuilt — it is a password on the wire.

**mTLS, by doing D4 first.** This is D3's actual recommendation and it is the better answer for
service-to-service traffic, because there is no secret to distribute and rotation is certificate
rotation. It was not taken because it is D4 plus D3 rather than D3, and D4's own design — rustls
decrypting in place into the response buffer so that TLS does not cost the zero-copy path — is
unbuilt and load-bearing. `AuthMechanism::MutualTls` is defined, refused, and is the arm this
becomes.

**A binary encoding of SCRAM.** Both peers are ours, the text messages are the largest thing in the
handshake, and parsing `r=`/`s=`/`i=` out of a comma-separated string is more code than reading four
length-prefixed fields. Rejected because the proof is a signature over those exact bytes, so a
private encoding would have made the RFC's test vector inapplicable — and the vector is the only
thing standing between "this implements SCRAM" and "this implements what I thought SCRAM was".

**A `Mechanism` trait with `ScramClient` as its only implementor.** The seam D3 asked for is real,
but mTLS is the one mechanism with *nothing to send* — the identity is established before the first
frame could be written — so a trait shaped around `step(&[u8]) -> Vec<u8>` would be shaped around
the one case that does not fit it. The state machines have the same `first` / `step` / `finish`
shape and the call sites dispatch on `AuthMechanism` with one arm each, which is a seam that costs
nothing and does not guess at the second implementation.

**Changing `Shoal::new` to take credentials.** Fifteen call sites, every one of which wants
`Credentials::none()`, and [D6](../direction/connection-pool.md)'s builder is where this argument
belongs. `with_credentials` is a second constructor and both delegate to one private `connect`, so
the ten-line `where` clause exists once.

**Storing passwords and comparing them.** Not seriously considered, and named here because it is
what "add authentication" most often means in practice. The server holds a salted, iterated
derivation and could not produce a user's password if it were asked to.

## Limitations

- **No SASLprep.** RFC 5802 says to normalize a password with SASLprep before deriving from it and
  this does not, so two clients that disagree about the Unicode normalization of a non-ASCII
  password derive different keys. It costs a dependency, and SASLprep is the identity function on
  ASCII. Filed in [TODOs](../appendix/todos.md).
- **No channel binding.** The gs2 header is `n` — "this client does not support it" — because
  binding needs a TLS layer to bind *to*. A `y` there without support is exactly the downgrade `y`
  exists to detect, so claiming it would be worse than not offering it. ~~This closes with D4.~~
  **D4 has been built** ([F14](encryption-in-transit.md)) and this is still not closed: the
  `tls-exporter` binding is available and unwired, and F14 turned up the ordering trap it will hit
  — the exporter has to be taken from rustls *before* `dangerous_into_kernel_connection` consumes
  the session. Filed in [TODOs](../appendix/todos.md).
- **No authorization.** A `Principal` is produced, logged, and consulted by nothing. Who may read
  which table is a server-side catalog problem and stays in
  [TODOs](../appendix/todos.md#per-table-authorization), now unblocked rather than blocked.
- **Three round trips per connection, and the pool opens ten before it is idle.** `min_idle` is 10
  and `max_size` is 50, and every one of them pays a full SCRAM exchange plus a PBKDF2 derivation
  on both ends. This is the "fifty handshakes, not one" cost D3 named, and none of its three
  mitigations — TLS session resumption, a re-auth ticket, a configurable `min_idle` — are built.
  See *Performance* below for what is and is not known about the size of it.
- **Credentials are read once, at startup.** There is no way to add, remove or rotate a user
  without restarting the server, and no `Ping`-driven re-authentication of a connection that is
  already open. Filed in [TODOs](../appendix/todos.md).
- **A password in a config file is still a password in a config file.** The `password:` key exists
  because it is what an operator reaches for first, and it is derived at startup and dropped — but
  the file it came from is still on disk. The derived stanza is the answer, and `stored_key` in it
  is *also* a secret: it cannot be turned back into a password and it can be replayed as a login.
- **No rate limiting.** Nothing bounds how many times a peer may open a connection and guess. The
  cost of a guess is one PBKDF2 derivation on the server, which is the wrong side of that trade.
  Filed in [Known Issues](../appendix/known-issues.md).

## Invariants to uphold

**Zero means "no mechanism" in both handshake bodies.** An older peer wrote those reserved bytes as
zeroes and read them as nothing, and that is the only reason this landed without a version bump. A
future change that gives zero a meaning in either field breaks every peer built before it.

**The discriminants of `AuthMechanism`, `AuthStatus` and `RefusalReason` are never renumbered and
never reused.** They are on the wire. `every_auth_mechanism_round_trips_through_its_discriminant`
and `every_auth_status_round_trips_through_its_discriminant` are what turn an insertion into a
failing test rather than a compatibility break.

**Both auth types start at 1, so a zeroed buffer decodes as neither.** In particular `AuthStatus`
has no zero variant, which is what stops a zeroed buffer reading as a successful login.

**The mechanism is selected in exactly one place** — `server_handshake`, through
`CredentialStore::select`. A second place that could choose one is a second place a downgrade could
be negotiated.

**`shared::protocol` stays on `core` and `uuid`.** The auth frames are in it and the auth
*mechanism* is not, and that boundary is what keeps [D5](../direction/runtimes.md)'s crate split a
move rather than a redesign. Nothing that hashes, signs or draws randomness belongs in
`protocol::auth`.

**An unknown user and a wrong password fail identically** — same variant, same wire message, same
number of round trips, same work done. `CredentialStore::lookup` returning a decoy rather than an
`Option` is what makes that structural instead of a property of how the calling code is written.
`ScramServer::verify` also refuses on the `known` flag *after* the proof check, so a decoy that
somehow verified still cannot authenticate.

**The exchange completes before the stream is split**, on both ends.

**`MAX_AUTH_PAYLOAD_LEN` is the bound that applies to auth frames, not the connection's
`max_frame_bytes`.** These frames are read from a peer that has proved nothing, and the connection
bound is 64 mebibytes. A reader that used the larger of the two hands an unauthenticated peer an
allocation channel.

**A refusal is written before the socket closes**, and it says the same sentence for every cause a
peer can trigger. Which failure it actually was belongs in the server's log, through
`ServerError::Auth`, where nobody untrusted is reading.

**The client verifies the server's final signature.** Deleting `ScramClient::finish`'s check would
leave every test in this feature passing and every client authenticating itself to anything that
answers the port.

## Performance

**Nothing on the query path changed.** The exchange happens once per connection, before the stream
is split, and every byte after it goes through code that was not touched. The response read is
still two reads into an `AlignedVec<16>` and the request write is still vectored.

**Connect cost is not measured, and no existing benchmark can see it.** The macro layer measures
queries against an already-warm pool, so three round trips and two PBKDF2 derivations per
connection are invisible to every number in [Benchmark Results](../operations/benchmark-results.md).
This is the one measurement [D3](../direction/authentication.md#how-it-would-be-measured) said the
~~planned~~ `transport/*` workloads would still not provide: what is needed is a *connect* workload,
time to first successful query from a cold client. That prediction held —
[F13](../features/transport-workloads.md) built them and they open their pool before sampling, so a
handshake is still outside every number they report. It stays filed as
[O30](../appendix/optimizations.md) rather than guessed at.

What can be said without a benchmark: 4096 iterations of PBKDF2-HMAC-SHA-256 is on the order of a
millisecond of CPU, it is paid on both peers, and `min_idle = 10` means a client startup does ten
of them concurrently against shards that are each single-threaded. That is a startup cost, not a
steady-state one, and it is zero for a server with no `auth` section — which is every server the
benchmarks run against.

## Tests

| Test | What breaks if this is reverted |
| --- | --- |
| `the_rfc_7677_vector_round_trips` | The implementation stops being SCRAM and starts being what this repository thinks SCRAM is |
| `the_right_password_authenticates` | The mechanism cannot accept anybody |
| `the_wrong_password_is_refused` | The mechanism accepts everybody |
| `an_unknown_user_is_refused_identically` | A login becomes a way to ask which accounts exist |
| `an_unknown_user_gets_a_plausible_challenge` | The decoy's salt or iteration count gives the answer the proof check refuses to give |
| `a_tampered_proof_is_refused` | The proof check stops being a check |
| `a_server_that_replaces_the_nonce_is_refused` | A captured challenge can be replayed at a client |
| `a_client_that_echoes_the_wrong_nonce_is_refused` | The two halves of the exchange stop being bound together |
| `a_server_that_cannot_sign_is_refused` | The mutual half is gone and a client authenticates to anything |
| `a_channel_binding_request_is_refused` | A client asking for binding is silently answered without it |
| `messages_out_of_order_are_refused` | A success arrives before a proof and is believed |
| `a_username_with_structural_characters_round_trips` | A comma in a username injects a field into the signed message |
| `a_stored_credential_carries_no_password` | The salt stops being per-credential, or a key starts printing in a log line |
| `a_stored_credential_round_trips_through_yaml` | A derived credential in a config file stops loading |
| `credentials_offer_what_they_can_do` | A password starts appearing in debug output |
| `a_store_that_requires_nothing_selects_nothing` | An open server starts demanding proof |
| `a_store_selects_from_its_own_preference_order` | The client's bit order decides the mechanism instead of the server's list |
| `every_auth_mechanism_round_trips_through_its_discriminant` | A variant inserted into the enum renumbers the wire silently |
| `every_auth_status_round_trips_through_its_discriminant` | The same, and a zeroed buffer can read as a success |
| `a_mechanism_set_keeps_bits_it_cannot_name` | A newer client's mechanism bit becomes a compatibility break |
| `an_auth_frame_round_trips` / `an_auth_response_round_trips` | The frame layout drifts, or a refusal stops being flagged in its header |
| `an_auth_payload_past_the_bound_is_refused` | The handshake becomes an allocation channel inside the frame bound |
| `an_auth_body_that_is_too_short_is_refused` | A short body is indexed into rather than refused |
| `an_unknown_mechanism_in_an_ack_reads_as_none` | A mechanism name from the future is read as one this build can do |
| `auth_defaults_to_off` | Every existing deployment, benchmark and test stops connecting |
| `a_password_in_the_config_is_derived` | The server starts holding passwords |
| `a_derived_credential_in_the_config_loads` | The only way to configure a user is to put a password in a file |
| `an_unknown_mechanism_is_rejected` | A misspelled mechanism yields a server that requires proof and can never grant it |
| `the_right_credentials_connect_and_query` | The exchange leaves a byte unread and the connection is unusable afterwards |
| `a_client_with_no_credentials_is_refused` | A server that requires authentication accepts anonymous clients |
| `credentials_against_an_open_server_are_ignored` | Adding credentials to a client breaks it against every server that has not turned authentication on |
| `the_server_names_the_mechanism_it_selected` | The ack stops carrying the negotiation and the client cannot know what to send |
| `queries_before_the_proof_are_refused` | A bundle from an unauthenticated peer is executed |
| `an_oversized_auth_frame_is_refused` | An unauthenticated peer picks the server's allocation size |
| `a_refusal_is_flagged_in_its_header` | A client has to read a body to learn it was refused |

## Related

- [D3. Authentication](../direction/authentication.md) — where this was designed, and the mTLS half
  that is still open
- [D4. Encryption in transit](../direction/encryption.md) — what closes the channel binding
  limitation and adds the second mechanism
- [D6. A production connection pool](../direction/connection-pool.md) — the builder credentials
  belong in, and the `min_idle` that decides how many exchanges happen at once
- [D9. Lessons from other databases](../direction/prior-art.md#cassandra) — Cassandra's SASL flow,
  which this copies
- [F10. Framing and protocol evolution](framing-and-protocol-evolution.md) — the handshake and the
  reserved bytes this spends
- [F11. The error channel](error-channel.md) — the other feature built out of F10's reserved slots
- [Wire Protocol](../architecture/wire-protocol.md) — the frame layouts as built
- [Configuration](../getting-started/configuration.md) — the `auth` section
- [The Client](../api/client.md) — `with_credentials`
