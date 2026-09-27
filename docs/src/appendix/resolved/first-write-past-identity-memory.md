# 180. A first write queued past a group's identity memory was refused `IdentityExpired`

## Symptom

The TMDB loader, at 8 × 4,096 writes in flight on a fresh lab cluster with retries unbounded,
stopped early in the load on:

```text
code: IdentityExpired, msg: "the identity of this write is older than the 300s retry window, or older than an identity group c5724bd0b94902bf has forgotten; a retry this late is not answered its first result"
```

This happened in two of three runs of the build before the admission gate, and in one of three
with the gate switched off. `IdentityExpired` is not retriable, so a write that was never applied
failed for good.

## Cause

A group remembers the result of its last `REMEMBERED_REQUESTS` (4,096) write identities
(`MachineState::remember`). Evicting one raises `expired_before` to the time that identity was
minted, and `propose_write` refuses any identity minted before it. That is what keeps a retry of
a write whose first result is gone from being applied twice
([F45](../../features/replica-migration.md#a-retry-identity-with-a-time-and-a-window)).

The check cannot tell a retry from a first attempt. The loader mints each bundle's identity as
it sends it. A write that then waits in the server's queues in front of admission (the
coordinator's mesh, a forward, the shard loop) while its group applies and forgets 4,096 writes
minted after it reaches the check older than the watermark, and is refused as a late retry. At
about 1,000 writes a second per group, 4,096 identities are four seconds. The admission gate
([#129](overload-sheds.md)) bounds the queue behind admission, not the one in front of it.

## Evidence

**Established by running it**, first on the lab in round 11
([section 11](../../cluster-testing/correctness.md#the-loader-past-what-the-cluster-commits)), then
reproduced in the fixture by `a_first_write_queued_past_the_memory_is_refused_retriably`,
written before the fix. The test does this:

1. It mints an identity.
2. It writes 4,200 notes to the same group under identities minted after it.
3. It sends a note under the first identity, which is exactly a write that queued while its
   group forgot what came after it.

Against the tree with the new branch disabled:

```text
assertion `left == right` failed: a first write that queued past the memory was answered Err(Server { …, code: IdentityExpired, msg: "the identity of this write is older than the 300s retry window, or older than an identity group 5be901f531640e96 has forgotten; a retry this late is not answered its first result" })
  left: Some(IdentityExpired)
 right: Some(Shedding)
```

The lab runs on both builds are in
[round 12](../../cluster-testing/correctness.md#a-first-write-queued-past-the-memory).

## The fix

**A forgotten identity that reached the server within seconds of being minted is refused
retriably.** `QueryMetadata::received_ms` is the time the server received the query. In
`propose_write`, a write refused as expired is answered `Shedding`, not `IdentityExpired`, when
all three of these hold:

- its identity was *forgotten* (`MachineState::forgotten`), rather than past the retry window;
- it is still inside the window;
- it arrived within `FRESH_AT_RECEIPT_MS` (5 s) of its mint time.

It is still refused, and nothing is proposed.

**A client sends it again as a new write.**

- The loader already re-stages a refused row into a new bundle, under a new identity, and it
  retries `Shedding`.
- `Shoal::exec_with` now mints a new identity for a retry when the bundle is one query, the
  identity was not pinned by the caller, and no earlier try can have applied
  (`sent_again_as_new`).
- A bundle of several queries keeps its identity, because some of its queries may have applied
  under it.

## Alternatives rejected

- **Remember identities for a time rather than a count.** This was the item's own suggestion. A
  group would keep every identity minted within some seconds of its newest, so a first write
  queued less than that would never be forgotten. It fails on two counts:
  - **Every replica must forget identically.** Apply answers a remembered identity `Duplicate`
    and applies anything else. A replica that remembered an identity another had forgotten
    would diverge on a late retry. Eviction by wall-clock time is not identical across replicas.
    It would need a leader's time stamped into every command, and the retry sidecar would have
    to hold the whole longer memory at every checkpoint.
  - **The sidecar costs too much.** It is written whole for every group of a shard at each
    checkpoint. On the lab that is about 1 MB a second a node at 4,096 identities, and would
    be about 2.6 MB at ten seconds' worth, on hosts whose disks are the limit. Written
    incrementally, it costs what every write adds, about 1.5 MB a second a node.
- **Bound the wait in front of admission.** Shedding a write that has waited more than a few
  seconds in the shard's queue keeps it from ever reaching the check late, but the memory under
  load is four seconds or less, and it shrinks as the rate grows. Any fixed bound is wrong at
  some rate. The answer that depends on nothing but the write itself is how old it was on
  arrival.
- **A "first attempt" flag from the client.** Server-side hops and the client's own resends
  after an unknown outcome carry the same identity, and a pinned identity may be a resend the
  library cannot see. A flag the client sets cannot be trusted to mean what the check needs.

## Invariants to uphold

- **A forgotten identity is never applied.** This change only chooses the refusal's code. Both
  answers are definite, and nothing is proposed. So the guarantee against a second effect does
  not depend on `received_ms`, `FRESH_AT_RECEIPT_MS` or any clock. A clock that is wrong makes a
  write be answered `Shedding` where `IdentityExpired` was due, or the other way round, and
  nothing else.
- **A client re-mints only when no try can have applied.** That means one query, no pinned
  identity, and no earlier try that returned an unknown outcome. A new identity is a new write,
  and after a try that may have applied it is a second effect. `sent_again_as_new` is the one
  place this is decided.
- **A genuine late retry sent within five seconds of its mint** is answered `Shedding` until
  those five seconds pass, and `IdentityExpired` after. Its client keeps the identity, because
  its earlier try's outcome was unknown, so it cannot loop past that.

## Still open

- A client that never retries (a stream) still sees the refusal. `Shedding` tells it to send
  the write again. `IdentityExpired` told it the write was lost.
- A client whose clock is more than five seconds behind the server's gets `IdentityExpired`, as
  before, since its writes never arrive fresh. Round 11's clock step was 30 s on a server host,
  and the client's clock is what matters here.
- The memory is still 4,096 identities, so a retry across a failover of ten seconds or more
  under load is still refused by name
  ([F45's limitation](../../features/replica-migration.md#limitations)).

## Tests

| Test | What breaks if the fix is reverted |
| --- | --- |
| `a_first_write_queued_past_the_memory_is_refused_retriably` (`shoal/tests/cluster_fixture.rs`) | The queued first write is refused `IdentityExpired`. It also fails if the fresh branch answers a late retry of the same identity with anything but `IdentityExpired`, or if the write is applied |
| `only_a_lone_unapplied_write_is_sent_under_a_new_identity` (`shoal-client`) | `exec_with` re-mints a bundle of several queries, a pinned identity, or a write whose earlier try may have applied |
| `retry_identity_survives_snapshot_and_migration` (`shoal/tests/cluster_fixture.rs`) | An identity past the window is no longer `IdentityExpired` through every node |

## Related

- [F45](../../features/replica-migration.md), the expiry this refines.
- [#129](overload-sheds.md), the gate that bounds the queue behind admission.
- [#138](stream-bundle-identity.md), where a stream's identity was minted once for the
  stream and expired with it.
