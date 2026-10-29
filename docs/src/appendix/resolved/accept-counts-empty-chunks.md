# 265. `ResolveStripe`'s `Accept` could not accept a short erasure coded stripe, and its stripe number wrapped

`ResolveStripe` is an operator's word on a stripe whose row is behind its chunks
([F93](../../features/operations-and-the-real-cluster.md)): `Accept` moves the row to the label
`k + f` holders agree on, and `Lost` declares the stripe lost. `Accept` counted a position only when
its holder answered a label past the row, so a short erasure coded stripe's data chunks that were
never stored, current by definition ([F90](../../features/erasure-coded-pools.md)), counted against
it: a stripe of two chunks or fewer at 4+2 could never be accepted, and the refusal said to declare
it lost. And the bound on the operator's stripe number multiplied it unchecked. Filed by a code
review of F87 to F93.

## Symptom

At 4+2 and the default `f = 1`, `Accept` needs five positions, and a stripe holding one or two
chunks of bytes has at most four that store anything: every object of 2 MiB or less at 1 MiB chunks,
and any short last stripe. `Accept` answered "no 5 holders of stripe 0 of … agree on a label past
its row …: declare it lost instead", so an operator's only answer destroyed data the holders held.
Where enough positions did agree, a never stored one was marked missed. A stripe number past what
`stripe * stripe_bytes` holds panicked a debug build's shard, and in release wrapped onto another
stripe's start, so a crafted number passed the bound and `Lost` committed a lost row for a stripe
that does not exist.

## Cause

`resolve` built its count from the holders' answers alone and never asked the geometry, where a
put (`check_put`), the erasure coded write's plan and its `Ask::Empty` each leave out a position by
`chunk_length(…) == 0`. The bound was `stripe * stripe_bytes >= reach` on an admin request's
`u64`, never checked.

## Evidence

**Reproduced** by two unit tests, written first against the count and the bound as `resolve` had
them, each moved unchanged into a function of its own (`accepted`, `has_stripe`) so a test reaches
it without a cluster whose row is behind its chunks, which takes a `force_recover`.

```text
---- a_stripe_past_the_multiply_is_no_stripe stdout ----
panicked at shoal-core/src/server/shard/objects/resolve.rs:276:5:
attempt to multiply with overflow

---- an_accept_counts_a_short_stripes_empty_chunks stdout ----
panicked at shoal-core/src/server/shard/objects/resolve.rs:337:73:
accepted
```

## The fix

- **A position whose holder answered no label and whose chunk holds no bytes at the row's length
  counts for every label** and is current in the commit; one that holds bytes and answered nothing
  is missed, as before.
- **The stripe's start is a checked multiply**: a number it cannot hold is no stripe, refused by
  name as any stripe past the object's reach is.

## Alternatives rejected

- **Taking the stripe's length from the holders' headers rather than the row.** A holder's
  `Confirm` answers a label and no length, and a holder of a chunk that holds nothing has no header
  to answer from; the row's length is what a put and a write judge the same positions by.
- **Refusing `Accept` on erasure coded pools.** It is the one answer that keeps the bytes.

## Invariants to uphold

- **A position is counted the same way by every path that counts it.** A put, a write and an
  accept each treat a chunk that holds no bytes as current, through `Geometry::chunk_length`.
- **An operator's number is never multiplied unchecked.**

## Still open

`Accept` commits the row's own `length`, and an extending write whose commit was lost, and whose
label it accepts, moved the stripe's length past it. Not examined further: filed in
[the todos](../todos.md#what-f93-left-for-later) beside F93's other remainders rather than as a
defect, since no extending write is known to have been lost this way.

## Tests

| Test | What breaks if the fix is reverted |
| --- | --- |
| `shard::objects::resolve::tests::an_accept_counts_a_short_stripes_empty_chunks` | A short stripe's empty chunks count against it, and an accept is refused or marks them missed |
| `shard::objects::resolve::tests::a_stripe_past_the_multiply_is_no_stripe` | A stripe number past the multiply panics in debug and wraps onto another stripe in release |

## Related

[F93](../../features/operations-and-the-real-cluster.md) for `ResolveStripe`;
[F90](../../features/erasure-coded-pools.md) for a short stripe's chunks;
[Resolved #262](behind-asked-of-holders-first.md), whose refusal names the resolution.
