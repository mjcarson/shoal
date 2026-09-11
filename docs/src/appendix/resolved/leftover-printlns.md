# 17. Leftover debug `println!`s

## Symptom

`shoal-core/src/` printed to stdout from inside the engine, bypassing the tracing level filter:
six lines in `PersistentSortedTable::exists`, one of which `{:#?}`-printed an entire partition; a
"Compacting ->" line on every rotation; "listening on ..." from `Networking::to_addr`, once per
shard and *before* the bind it announced; and a line naming the trace sink while the subscriber
was being installed. A test run's output was mostly them, and an operator's stdout carried lines
no configuration could turn off.

## Cause

Debugging that shipped. The last two survived longest because each had a use nothing else
provided: the `listening on` line was the only way a test learned which port a server bound, and
the sink line ran before there was a subscriber to emit it through.

## Evidence

**Established by reading the source**, when filed and at each partial fix. Today
`grep -rn 'println!' shoal-core/src` finds nothing outside a test module; the one `eprintln!`
left, in `trace.rs`, reports that a subscriber could not be installed, which is the one moment a
subscriber cannot report anything.

## The fix

In four parts over four changes. The six `exists` lines and the compaction line went first, and
the item was marked mostly fixed. The sink line went with
[Resolved #90](divergent-layer-filters.md), which decided the subscriber's filters once and had
no reason left to announce them. The `listening on` line went with
[Resolved #38, 58, 88](pool-readiness.md): once `ShoalPool::ready` reports the address every
shard actually bound, a line printed before the bind by a function called to format a string is
worse than nothing, and the tests that read it no longer need to.

## Alternatives rejected

**Turning the last two into `tracing` events.** The item's own suggestion for what remained.
The bind announcement would still have been a claim made before the bind; the readiness event is
the honest version and lives on the pool, not in a string formatter. The sink line had no reader
once the subscriber's decision was made in one place.

## Invariants to uphold

- **Nothing in `shoal-core/src` prints to stdout.** stdout is what `shoal-workload` harvests a
  hotpath profile from ([F34](../../features/benchmark-tracing.md) found the sink line in it),
  and what a test child reports its endpoints on.
- **`Networking::to_addr` formats a string and does nothing else.** A caller reads the bound
  address from `ShoalPool::bound_addr` or `ready`.

## Still open

Nothing of this item. Whether the tests should install a subscriber so that engine events reach
them at all is [item 69](../known-issues.md#69-shoalctl-and-the-tests-still-install-no-tracing-subscriber).

## Tests

| Test | What breaks if the fix is reverted |
| --- | --- |
| `pool::a_port_of_zero_resolves_to_one_every_shard_binds` | The address a test learns is the one `ready` reports, which is the only source left |

No test asserts that stdout is quiet; a `grep` over the crate is the check, and it is quoted above.

## Related

- [Resolved #38, 58, 88](pool-readiness.md), where the last line went
- [Resolved #90](divergent-layer-filters.md), where the sink line went
- [Observability](../../operations/observability.md#debug-output-that-is-not-tracing)
