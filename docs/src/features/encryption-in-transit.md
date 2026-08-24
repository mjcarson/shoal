# F14. Encryption in transit

## Context

Everything Shoal sent crossed the wire in clear, in both directions, with no configuration to
change it. The practical consequence was sharper than "no confidentiality": the SCRAM exchange
[F12](authentication.md) shipped ran in the open, so an observer on the path saw the username, and
saw the salt, nonces and proof of an exchange it could then grind offline.

[D4](../direction/encryption.md) is the design page, and it is the item the direction chapter's
recommended order had left for last. It had been blocked twice and by the time this was taken it
was blocked by nothing: framing closed with [F10](framing-and-protocol-evolution.md), and the
benchmark precondition it named — *"a plaintext-versus-TLS pair is a **precondition** for taking
this, not a follow-up"* — closed with [F13](transport-workloads.md).

**The hard part was never encryption. It was encrypting without ending the zero-copy read.** The
response path reads the socket straight into an `AlignedVec<16>` and turns it into a row with a
pointer cast, which is the property the `ZeroCopyResponses` branch exists for. Every conventional
way of adding TLS destroys it, because a TLS library owns its plaintext buffer and hands the
application a slice that then has to be copied into aligned memory.

## What it does

A server encrypts its listener when its config says to, and does not otherwise:

```yaml
networking:
  interface: "127.0.0.1"
  port: 12000
  tls:
    cert: "/etc/shoal/server.pem"
    key: "/etc/shoal/server.key"
```

**A config with no `tls:` section behaves exactly as every server did before this**, which is what
keeps the benchmark harness, the integration suite and the frozen
[baseline](../performance/baseline.md) comparable. The same shape `auth:` already had.

A client says what it trusts:

```rust
let client = Shoal::<MyDbClient>::with_options(
    "127.0.0.1:12000",
    ClientOptions::new()
        .credentials(Credentials::scram("reader", "hunter2"))
        .tls(TlsClientOptions::new("/etc/shoal/ca.pem")),
)
.await?;
```

Underneath, rustls performs a TLS 1.3 handshake and then **hands the negotiated keys to the
kernel**. From that moment rustls is out of the data path: the kernel owns the record layer,
`read()` returns plaintext into whatever buffer the caller names, and

```rust
let aligned_buff = read_payload(&mut self.reader, frame.rest_len).await?;
```

`shoal-client/src/client.rs`, `TcpProxy::read_frame` — is **unchanged** by TLS. *(It has since
changed for an unrelated reason: it used to `resize(frame.rest_len, 0)` before the read, and
[F25](read-buffers-are-filled-not-zeroed.md) took that out. The claim this paragraph makes is about
encryption, and it still holds — under kTLS the kernel writes plaintext into whatever buffer the
caller names, and it does not care whether the caller wrote zeroes there first.)* So are `TcpProxy`,
`ShoalResponse`, `client_rx_relay` and `client_tx_relay`. The zero-copy property survives because
nothing on the response path had to learn that TLS exists.

There is a new `Networking::tls()` builder, a `ClientOptions` type, and `Shoal::with_options`.
`Shoal::new` and `Shoal::with_credentials` are unchanged wrappers, so no existing call site moved.

## Design choices

**kTLS, and the page that recommended otherwise was wrong.** D4 recommended rustls' *unbuffered*
API on the strength of its documentation, which describes it as letting the caller supply the
buffers. It does — for ciphertext. Reading the crate's source settled it:

```rust
pub struct ReadTraffic<'c, 'i, Data> {
    conn: &'c mut UnbufferedConnectionCommon<Data>,
    // for forwards compatibility; to support in-place decryption in the future
    _incoming_tls: &'i mut [u8],
    // owner of the latest chunk obtained in `next_record`, as borrowed by `AppDataRecord`
    chunk: Option<Vec<u8>>,
}
```

`rustls-0.23.43/src/conn/unbuffered.rs:328-361`. Plaintext comes out of a `Vec<u8>` rustls owns, so
the unbuffered API costs exactly the copy the buffered one was rejected for, bought at the price of
driving a state machine by hand. D4 is corrected in place with the citation; the lesson it draws is
the general one, that **a design page arguing from a dependency's prose rather than its source can
be confidently wrong**.

**The decision was made by a spike rather than by argument**, because the same page had already
been wrong once. Three phases, written before any of this: rustls handshake → `setsockopt` →
plaintext `read`/`write`; the same descriptor adopted by glommio and driven through io_uring; and a
MiB response — ~64 records — written with `write_vectored` and read with the client's own two
`read_exact` calls, byte for byte identical into a 16-byte-aligned buffer. All three passed, and
the third is the one that decided it: **not one line of the client read path had to change.**

**Session tickets are off, and that is not free.** A TLS 1.3 server sends `NewSessionTicket` after
the handshake; on a socket the kernel has taken over that is a non-application record and a plain
`read` fails with `EIO`. `send_tls13_tickets = 0`. The cost is session resumption, which is the
*first* of the three mitigations [D3](../direction/authentication.md#what-it-costs) proposes for
the fifty handshakes a client pool opens. **kTLS and that mitigation are incompatible in this
form**, and neither page had noticed.

**One TLS record is read at a time during the handshake, never more.** This is not tidiness. A read
that pulled in the peer's last handshake record *and* the first bytes it sent afterwards would
leave those bytes in a userspace buffer that is dropped when the handshake ends, and the kernel has
no way to be told about them. It was found by writing the naive version: the server ate the client's
`Hello` along with its `Finished`, and the connection completed its TLS handshake and then hung
until the deadline.

**The attachment is read back rather than assumed.** `ktls::enable` calls `getsockopt(TCP_ULP)` and
refuses if the kernel does not report `tls`. A `setsockopt` that returned zero and left the socket
in plaintext would produce a connection that works perfectly and encrypts nothing, and nothing
above that layer could tell.

**A server refuses to start rather than falling back.** A config asking for TLS on a machine where
`modprobe tls` has never run fails at startup, before anything binds. Falling back to plaintext is
the one behaviour this feature must never have.

**`Networking` gained `deny_unknown_fields`.** It was the only config section without it, and it is
where it matters most: a misspelled `tls:` key under a section that ignored it produces a server
that starts, listens, and serves every query in clear.

**The crypto is not reachable from `shared::protocol`**, which stays on `core` and `uuid` so
[D5](../direction/runtimes.md)'s crate split remains a move. `shared::tls` sits beside
`shared::auth` for the same reason and by the same rule.

## Alternatives rejected

| Option | Why not |
| --- | --- |
| **rustls, buffered API** | One copy of every response into the `AlignedVec`. This is what D4 was written to avoid, and it remains the thing to avoid |
| **rustls, unbuffered API** | The recommendation this feature was planned around. It copies too — see above. It would have been *harder* to notice, because the code reads as though the property had been protected |
| **rustls handshake + our own record layer** | Genuinely in-place, and it means owning nonce construction, sequence numbers, padding, key updates and alerts. Security-sensitive code with no reason to be ours, to buy what the kernel gives for two `setsockopt` calls |
| **`ring` as the crypto provider** | `aws-lc-rs` is rustls' default and ships the AES-GCM assembly a userspace fallback would want. Under kTLS this decides handshake cost only, since the kernel does the bulk work — the choice is now cheap insurance rather than a throughput argument |
| **A cargo feature gate for TLS** | Followed F12's precedent and added the dependencies unconditionally. A non-default feature is not built by `--all-targets` and would rot; TLS is off at *runtime*, which is what D4 actually asks for |
| **WireGuard / IPsec / a mesh sidecar** | Still the right answer for a deployment that already runs one, and still gives no per-connection peer identity, so [D3](../direction/authentication.md) gains nothing from it. A sidecar also adds a userspace hop per packet |
| **A committed test certificate** | Generated per run with `rcgen` instead. A checked-in fixture means private key material in git and a hard expiry that fails the suite years from now for a reason nobody will connect to it |

## Limitations

- **Key updates are not handled, and the confidentiality limit is therefore not enforced.** Once
  kTLS is on, rustls is out of the data path and neither peer generates a `KeyUpdate`, so in
  practice none occur — which also means the AES-GCM message limit rustls normally enforces is not
  enforced by anything. Both peers being ours is what makes this survivable. The
  `KernelConnection` is retained per connection precisely so that handling one later is a call site
  rather than a redesign. Filed in [TODOs](../appendix/todos.md).
- **No session resumption**, per the tickets decision above. Every connection in a pool of fifty
  pays a full handshake.
- **The client trusts exactly one certificate authority**, named by path. No native root store and
  no webpki bundle — a datacenter store talks to a private CA, and trusting the public roots as
  well would mean accepting any certificate on the internet for the name asked for.
- **Linux only, and a kernel with `CONFIG_TLS`.** Shoal is Linux-only already because glommio is,
  but this adds a *module* requirement: `setsockopt` does not autoload it, so `modprobe tls` is a
  deployment step. See [Configuration](../getting-started/configuration.md).
- **No mTLS.** `AuthMechanism::MutualTls` is still defined and still refused. It is unblocked by
  this and not built.
- **No channel binding.** F12's gs2 header is still `n`. Also unblocked by this and not built.
- **Nothing measures it yet.** The control pair exists; the capture does not. See *Performance*.

## Invariants to uphold

- **The socket is idle when the kernel takes it.** The TLS handshake completes with nothing
  buffered, no Shoal byte has crossed, and no read has gone past the last handshake record. The
  record-at-a-time read is what guarantees the last of those.
- **`send_tls13_tickets = 0`.** Turning tickets on breaks every connection this server accepts,
  milliseconds after each one appears to succeed.
- **TLS 1.3 only, AES-GCM only.** The cipher list is the two suites `ktls` can build key material
  for. A suite outside it negotiates a session that completes and then cannot be handed to the
  kernel.
- **The `KernelConnection` outlives the socket**, so a key update has somewhere to be handled.
- **A frame is not a record and must not be made one.** The kernel reassembles across record
  boundaries and the client never sees one. Nothing may reintroduce a constraint that a response
  fit in a record — that constraint belonged to the userspace designs and does not apply here.
- **TLS comes before the Shoal handshake, which comes before authentication.** All three are inside
  the same deadline.

## Performance

**Measured.** `f14-encryption` is the first capture to hold the control pair, and the first to hold
the sweeps that say how the cost *behaves* rather than merely whether it exists. The charts are on
[Benchmark Results](../performance/transport.md#what-encryption-costs); what follows is what
they mean.

### The row-width prediction was right

At a single outstanding query — a service time with nothing queued behind it — the cost rises
monotonically with the row:

| Row | Plaintext p50 | TLS p50 | Overhead | Separated? |
| ---: | ---: | ---: | ---: | --- |
| 256 B | 32.26 µs | 34.42 µs | +6.7% | no |
| 4 KiB | 34.98 µs | 39.13 µs | +11.9% | no |
| 64 KiB | 44.47 µs | 55.88 µs | +25.7% | **yes** |
| 1 MiB | 216.03 µs | 383.81 µs | **+77.7%** | **yes** |

This is exactly what [F13](transport-workloads.md) built a row-width axis to catch, and it is worth
being explicit about how close it came to being missed: **a set measured only at 256 bytes would
have reported encryption as costing 6.7% and not been separable from noise.** The prediction that a
per-byte tax is invisible in one regime and the whole cost in the other holds, with the two widest
points the only ones the disjointness rule calls results.

Fitting the two widest points gives **≈1 µs fixed per response plus 0.159 ns per byte — a marginal
6.3 GB/s.** Single-stream bandwidth falls from 4.10 GB/s to 2.43 GB/s. That is the shape a hardware
AEAD gives: a small constant, and a slope you only see once there are enough bytes for the slope to
matter.

### The cost shrinks as load rises, which is the finding worth keeping

| 1 MiB row | depth 1 | depth 8 | depth 32 |
| --- | ---: | ---: | ---: |
| Overhead | +77.7% | +8.7% | +4.8% |

and across independent clients, at the same width: +73.6% at one, +53.9% at two, +25.3% at four,
+15.8% at eight.

**Encryption is most expensive exactly where the system is least busy.** At depth one the crypto
sits in the critical path of a single round trip and there is nothing to hide it behind. Under
concurrency twelve shards encrypt in parallel and the bottleneck moves to the wire, so the same
per-byte work is absorbed rather than added.

The sober half of that is throughput, which does not improve with hiding:

| | plaintext | TLS | cost |
| --- | ---: | ---: | ---: |
| 1 MiB, depth 8 (peak) | 6.73 GB/s | 6.11 GB/s | −9.2% |
| 1 MiB, depth 32 | 6.53 GB/s | 5.55 GB/s | −15.0% |
| 256 B, depth 32 (peak) | 351k q/s | 320k q/s | −9.0% |

So: **a latency cost between 5% and 78% depending entirely on how wide the rows are and how busy
the server is, and a throughput cost of about a tenth.** A deployment returning small rows under
load pays almost nothing. One returning MiB rows to an idle server pays nearly double.

The transport control pair D4 called a precondition agrees, at the coarser grain a wall clock gives:
+7% to +21% across the eight modes, with `send_one/large` at +14.6%.

### Two things the sweep found that are not about encryption

**Depth 128 is past saturation and its numbers must not be read.** Throughput *falls* from depth 32
to depth 128 — 351k to 277k queries a second at 256 bytes — and the overhead inverts, with TLS
measuring 23% to 47% *faster*. That is congestion, not cryptography.

What makes it worth recording is that **those inverted points pass the disjointness rule**: the runs
are cleanly separated, so the macro layer calls them results. The rule detects *reliably* different
and cannot detect *meaningfully* different, and a saturated workload is reliably weird. Filed as
[O31](../appendix/optimizations.md); the charts draw the points rather than hiding them, and the
analysis is what has to say they are noise.

**The client sweep did not measure what it was built to measure.** It was added to expose the
per-connection handshake — the cost [O30](../appendix/optimizations.md) says nothing can see,
because every workload opens its pool before it samples. It does not, for the same reason: `bb8`
fills `min_idle` during `Pool::build()`, so all ten connections of every client have handshaked
before the first sample is taken. The sweep measures steady state with *n* pools. **O30 stays
open**, and closing it needs a workload that times connection establishment itself rather than one
that opens more connections.

### What bounds the noise

The two sweeps overlap at exactly one configuration — one client, one query deep — so each width
was measured twice by independent workloads. They agree to **0.3–3.0%**, which is a free estimate of
run-to-run noise and is why the 256 B and 4 KiB overheads are not separable: they are the same size
as the noise floor. Any claim below about 5% in this capture is not a claim.

## Tests

| Test | What breaks if this is reverted |
| --- | --- |
| `a_socket_reports_the_tls_ulp_once_it_is_attached` | the only assertion anywhere that tells a kTLS socket from a plaintext one — it is what would catch this being quietly replaced by a userspace implementation |
| `the_response_buffer_lands_on_a_sixteen_byte_boundary_over_tls` | a MiB response still arrives whole, at the start of a buffer this client aligned |
| `a_mib_response_over_tls_matches_its_plaintext_bytes` | ~64 records reassembled by the kernel, byte for byte |
| `a_query_round_trips_over_tls` | the floor |
| `scram_over_tls_authenticates` | TLS and F12's exchange compose, in that order |
| `tls_does_not_authenticate_on_its_own` | encryption is not authentication |
| `a_plaintext_client_is_refused_by_a_tls_server` | turning encryption on cannot silently keep serving plaintext clients |
| `a_tls_client_is_refused_by_a_plaintext_server` | the converse, which is the likelier accident |
| `a_client_that_does_not_trust_the_certificate_is_refused` | the certificate is checked, not merely presented |
| `the_nonce_splits_into_a_leading_salt_and_a_trailing_iv` | the kernel's salt/iv split, which decrypts nothing if reversed |
| `the_sequence_number_is_big_endian` | one record decrypts and nothing after it |
| `a_key_of_the_wrong_length_is_refused`, `a_cipher_and_key_that_disagree_are_refused` | a key is never silently truncated to a weaker one |
| `each_cipher_has_its_own_struct_length`, `every_info_names_tls_1_3` | the `setsockopt` structs match what the kernel reads |
| `a_server_config_extracts_secrets_and_sends_no_tickets` | the two settings the whole feature depends on and that no call site shows |
| `only_kernel_supported_ciphers_are_offered` | a suite that cannot be handed to the kernel |
| `a_certificate_and_key_load_from_pem`, `a_pem_file_with_no_certificate_is_refused`, `a_missing_certificate_names_the_path_it_looked_for`, `a_client_refuses_an_authority_it_cannot_read` | certificate loading, and errors that name what they looked at |
| `an_explicit_server_name_overrides_the_address`, `an_invalid_server_name_is_refused` | what a certificate is checked against |
| `tls_defaults_to_off` | a server encrypts only when asked, which is what keeps the baseline comparable |
| `a_tls_section_is_read`, `a_tls_section_needs_both_a_certificate_and_a_key` | the config section |
| `a_misspelled_networking_key_is_refused`, `a_misspelled_tls_key_is_refused` | a typo is a startup failure rather than a silently plaintext server |
| `the_plaintext_arms_kept_their_original_names` | the eight pre-existing workload identifiers, and every capture that joins on them |
| `a_tls_arm_differs_from_its_plaintext_twin_only_in_the_wire` | the control pair measures the wire and not something else that moved |
| `only_the_tls_arms_configure_a_tls_server` | the axis is wired to the thing it names |

**These tests need `modprobe tls`.** The integration tests skip loudly when the module is absent
rather than failing, the same way the `stage-profile` tests sit outside a default run. A skipped
encryption test that reported green would be this feature's own worst failure mode, which is why
the skip prints and why the server refuses to start rather than falling back.

## Related

- [D4. Encryption in transit](../direction/encryption.md) — the design, and the correction to it
- [D3. Authentication](../direction/authentication.md) — mTLS, and the resumption mitigation this
  took away
- [D1. The transport](../direction/transport.md) — QUIC would have supplied this, and its stream reassembly is
  the copy this avoided
- [F12. Authentication](authentication.md) — the exchange this now runs underneath
- [F13. Transport workloads](transport-workloads.md) — the plaintext half of the control pair
- [F9. Ephemeral tables](ephemeral-tables.md) — the control-pair shape
- [The Client](../api/client.md) — the zero-copy property this protects
- [Configuration](../getting-started/configuration.md) — the `tls` section and the module it needs
