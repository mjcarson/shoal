# F13. The transport workloads

## Context

The client is the one layer of this system whose total has never been bounded.

Every macro workload before these measured a path through the *engine* and charged the client's
share of it to the engine, because nothing separated the two. A `macro/get_resident` sample is a
`send`, a serialize, a pool checkout, a socket write, a server round trip, a socket read, an
`AlignedVec` allocation and an `rkyv` access, reported as one number attributed to the read path.
How much of it is the read path has never been known.

[Todos](../appendix/todos.md) filed the `transport/*` workloads as "the only thing that could say
how much of a measured latency is the harness's own", and then filed a stronger claim on top of it:
they are what **every page of [Direction](../direction/overview.md) is blocked on** — nine design
pages arguing about a transport nobody has measured.

[D4](../direction/encryption.md) is where that became a blocker rather than a caveat. Encryption is
a per-byte tax on the response path, and the entire recommendation of that page turns on whether it
is a few percent or a large fraction. The page says so itself: a plaintext-versus-TLS pair is a
**precondition** for taking the work, not a follow-up. That pair cannot exist until there is a
transport workload to be an arm of.

## What it does

Eight workloads in `shoal-bench/src/workloads/transport.rs`, on two axes.

**Mode** — the four ways a caller can talk to a Shoal server, over an identical query mix:

| Identifier | Timing | Isolates |
| --- | --- | --- |
| `macro/transport/send_one/{small,large}` | `per_query` | `Shoal::send` with one query in the bundle, awaited on its own |
| `macro/transport/send_batched/{small,large}` | `per_batch` | `Shoal::send` with many queries in the bundle |
| `macro/transport/stream/{small,large}` | `per_batch` | `Shoal::stream`, responses in the order they were sent |
| `macro/transport/stream_unordered/{small,large}` | `per_batch` | `Shoal::stream_unordered`, responses as they arrive |

A caller picking between these four today is picking blind. `send_one` against `send_batched` is
what bundling is worth; `stream` against `stream_unordered` is what the ordered stream's
head-of-line buffering costs.

**Row size** — `small` is a 256-byte payload, `large` is a MiB.

These are not the same measurement at two scales. They are two regimes, and which one a system is
in decides which costs matter:

- At **256 bytes** a response is one frame, one allocation and a round trip. The fixed
  per-response costs dominate and the per-byte costs are invisible.
- At **a MiB** the per-byte costs dominate and everything fixed disappears into the noise.

A per-byte tax such as encryption is nearly free in the first regime and is the entire cost in the
second. **A set with only the small arm would have reported that encryption is cheap and been
wrong by an order of magnitude.** The size axis is what makes these workloads able to answer the
question they were built for.

The smoke run that first exercised all eight shows the split directly. At 256 bytes the modes
separate — `send_batched` at 0.77 ms against `send_one` at 2.94 ms, a near-4× spread that is
entirely per-response overhead. At a MiB all four collapse into 17–19 ms, because at that width
the wire is the whole cost and the mode barely registers.

## Design choices

**One file, two axes.** The eight share `transport.rs` with mode and size as parameters, the shape
`keyed_get.rs` settled on for one axis and `fanout.rs` already uses for two. Eight files would
drift, and a pair that drifts stops answering its question. The identifiers are leaked at startup
for the reason `Fanout::all` leaks its own: eight short strings read for the life of the process,
which is what lets the parameters live in the struct rather than forcing eight hand-written types.

**Nothing about the server is pinned.** `ConfOverrides::default()` on all eight. These measure the
client, so a workload naming a shard count or a memory limit would be holding still something it is
not about — the opposite of the fanout curve, which pins its shard count precisely because
placement is a confound for it.

**`send_batched` has no driver behind it.** `driver::drive` streams and `driver::drive_per_query`
sends one query at a time, so neither exercises the call this mode is about: a bundle handed to
`send` and drained to its end. It is the one mode with a loop of its own.

**The driver grew two parameters rather than a second copy.** `drive_with` takes a `StreamMode` and
an in-flight gate; `drive` is now a call to it with the values every other workload wants, so no
existing caller changed. The ordered and unordered result streams are different types with no
shared trait, so a private `Results` enum tells them apart in one place. It is named `StreamMode`
and not `Ordering` because `driver.rs` already imports `std::sync::atomic::Ordering`, and two
things called `Ordering` in one file is how the wrong one gets used.

**The large arm derives three constants from the row width.** Each of them is a hang or a hard
failure if it does not:

- **The seed bundle.** An insert bundle carries every row it inserts, so a hundred MiB rows in one
  bundle is a hundred-megabyte frame against a 64 MiB bound. `seed_batch` sizes it from the row
  width, capped at `driver::BATCH` so a narrow row still seeds the way everything else does.
- **The in-flight gate.** The gate bounds outstanding *responses*, not only outstanding queries. At
  the driver's default of 4096 the MiB arm would hold four gigabytes of responses in memory at once
  and would be measuring the allocator.
- **The concurrency and the query batch.** Same reason, for the per-batch modes, which hold a whole
  bundle's responses at once per slot.

**512 rows at the large size.** Far fewer than the small arm's 200,000, because the subject is the
width of a row and not how many there are. 512 MiB sits under the 4 GiB limit `shoal.yml` sets, so
every read is answered from memory and the arm measures the transport rather than a mixture of
transport and eviction.

## Alternatives rejected

**Four workloads without the size axis**, which is what [Todos](../appendix/todos.md) specified.
Rejected because it cannot do the job the entry itself claims for it. The todo says these are what
`Direction` is blocked on, and the page most blocked on them is [D4](../direction/encryption.md),
whose question is a per-byte cost. Four 256-byte workloads would answer it with a number near zero.

**A size axis on `send_one` alone**, to keep the capture short. Rejected because the modes are not
interchangeable at a MiB — the reason to know what `stream` costs on a wide row is exactly that
bundling and ordering behave differently when one response fills the pipe.

**Reusing `keyed_get`'s rows and only varying the transport.** Rejected because `keyed_get` is a
read-path workload whose numbers are already committed against the frozen baseline; widening its
rows would move them. A separate workload with its own seed streams leaves every existing capture
alone.

**Raising `driver::IN_FLIGHT` and letting the large arm use it.** Rejected: the gate is a memory
bound, not a throughput knob, and a global change to it would alter every workload that already
depends on the current value.

## Limitations

- **These measure a client against a server on loopback.** There is no network in the number. A
  real link adds latency the client does not control, and the mode differences these show would
  compress against it.
- **`send_one` is the only service time.** The other three saturate, so their samples are batch
  completion times and their percentiles describe the batching. The number worth reading off them
  is the wall clock. A comparison never joins one timing to the other.
- **The two sizes are two points, not a curve.** They bracket the regimes rather than showing where
  the crossover is. A workload that wanted the crossover would need the fanout treatment — a swept
  parameter and a query budget that falls as it rises.
- **Neither arm isolates the client from the server.** A sample still includes the server's work.
  What the pair gives is the *shape* of the client's contribution across two regimes, not a
  subtraction. Separating them needs the client-side `tracing` spans and `hotpath` scopes
  [O28](../appendix/optimizations.md) asks for, which do not exist.
- **The large arm's rows all fit in memory by construction.** That is deliberate — it keeps
  eviction out of a transport measurement — but it means these say nothing about a wide row that
  has to be read off disk.

## Invariants to uphold

- **A seed bundle must fit in a frame.** `seed_batch` is what guarantees it, and
  `a_seed_bundle_fits_in_a_frame` is what catches a change to it. Without this the large arm fails
  partway through seeding, after minutes of work, with a refused frame.
- **A seed bundle must never be empty.** A batch of zero builds an empty bundle forever and the
  seed never finishes — a hang, not an error. `a_seed_bundle_is_never_empty` covers the whole range
  of row widths a `u32` frame bound can express.
- **The gate must bound bytes, not queries.** Anything that raises `LARGE_IN_FLIGHT`,
  `LARGE_CONCURRENCY` or the large query batch has to be checked against the row width it will be
  multiplied by. `the_large_arm_bounds_what_it_holds` asserts the product stays under 128 MiB.
- **The two sizes of one mode may differ in the row width and in nothing else.** That is what makes
  the pair isolate a per-byte cost. `the_sizes_of_one_mode_differ_only_in_the_row` asserts it.
- **These are appended to `workloads::all()` and to `workload_ids::IDS`, never inserted.** A
  workload's position in that list decides the port a capture gives it, so inserting moves every
  workload after it. The ephemeral controls are appended for the same reason.
- **The identifiers are the join key of every comparison.** Renaming one orphans every capture
  taken before the rename. Add and deprecate.

## Performance

Nothing got faster. This is a measurement feature: it adds eight workloads and changes no engine
code.

What it costs a capture is eight more workloads times five runs, so forty more server lifecycles.
The large arm is the expensive half — 512 MiB seeded and two gigabytes moved over the wire per run
— and it is the reason a full capture grew noticeably rather than marginally.

What it buys is the first bounded number for the client transport, and the first workload set that
can distinguish a fixed per-response cost from a per-byte one. Every entry in
[Optimizations](../appendix/optimizations.md) about the client — [O28](../appendix/optimizations.md)
and [O30](../appendix/optimizations.md) among them — was previously unadjudicable for want of
exactly this.

## Tests

| Test | What breaks if this is reverted |
| --- | --- |
| `every_mode_and_size_is_minted` | a mode or a size silently missing from a capture |
| `an_id_names_its_mode_and_size` | an identifier that does not say what it measured |
| `the_sizes_of_one_mode_differ_only_in_the_row` | a second axis leaking into the pair, so a per-byte cost is read off a gap that is not one |
| `a_seed_bundle_fits_in_a_frame` | the large arm failing partway through seeding with a refused frame |
| `a_seed_bundle_is_never_empty` | a seed phase that hangs instead of finishing |
| `the_large_arm_bounds_what_it_holds` | four gigabytes of outstanding responses, measuring the allocator |
| `only_the_single_send_is_per_query` | a batch completion time reported as a service time |
| `a_smoke_run_still_has_rows_to_read` | a smoke run with too few rows for the key walk to mean anything |
| `workload_ids::the_declared_ids_are_the_registered_ones` | a workload registered but never run, and never missed |
| `seed_batch` doctest | the frame-derived batch silently becoming a constant again |

## Related

- [F8. Purpose-built workloads](purpose-built-workloads.md) — the harness these are built on, and
  the `todos.md` entry that specified them
- [F14. Encryption in transit](encryption-in-transit.md) — the TLS half of the pair these eight are
  the plaintext half of. It added a `Wire` axis over these rather than a second set of workloads, so
  the eight identifiers here are byte identical to what they were and every capture still joins
- [F9. Ephemeral tables](ephemeral-tables.md) — the control-pair pattern, which the TLS arms will
  follow
- [D4. Encryption in transit](../direction/encryption.md) — the page these unblock, and why the
  size axis exists
- [Direction](../direction/overview.md) — the chapter that is step-0 blocked on client measurement
- [Benchmarking](../operations/benchmarking.md) — how to run them
- [Optimizations](../appendix/optimizations.md) — O28 and O30, which these make adjudicable
