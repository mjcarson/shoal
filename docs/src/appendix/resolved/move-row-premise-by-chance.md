# 354. A move row's premise failed by chance: twelve objects all placed on one device

## Symptom

In the workspace run of [item 350](encode-copy-deadline.md)'s change (2026-11-01, 2,946 tests),
`move_serves_reads_and_writes_throughout` failed before the move it is about began:

```text
thread 'move_serves_reads_and_writes_throughout' panicked at shoal/tests/cluster_fixture.rs:28098:5:
node two's second device held every object before the move
```

It passed when run again alone, in 14 s, and twice more after the fix.

## Cause

**The row's premise held only by chance.** The row puts twelve objects of one stripe each into a
pool of three copies on three hosts with two devices each, then loses node two's first device and
watches its groups move to the second. It asserted first that the move had something to carry: that
not every object's copy on node two was on the second device.

Where each copy lands is a draw. A stripe's placement group is its key's bits (`pg_of`), and its
key is a hash of its consumer, object and stripe (`stripe_key`), so the twelve objects fall in
twelve groups with no relation between them. Each group's copy on node two is that host's best
slice by weighted rendezvous over its two devices (`View::rule_indices`). The fixture's devices
are directories on one filesystem, so each device's weight is the same filesystem's size and the
pool is not fitted (`fit_weights` returns none for devices of one weight). Each group's copy lands
on either device half the time, and all twelve land on the second about one run in 4,096
(2⁻¹²). The workspace run met that.

Nothing in the engine was wrong. Placement did what it promises: draws in proportion to weight,
independently per group.

## Evidence

**Established by reading the source**, beside the run's failure above: `stripe_key`
(`shoal-proto/src/shared/objects.rs`), `pg_of` and `View::rule_indices`
(`shoal-proto/src/shared/pool_placement.rs`), `fit_weights`, which fits nothing for devices of one
weight, and `DeviceFacts::capacity_weight`, a filesystem's size. A node's devices are committed
together by one `ObserveDevices` (`ControlPlane::observe_devices`), so the pool cannot have been
ready with node two's second device alone. Not reproduced: the draw depends on object ids minted from
each put's own time.

## The fix

**The row puts objects until node two's first device holds one**, twelve at least and 48 at most,
and then asserts that it holds one. It reads the first device rather than counting the second: a
copy still staged on node two is on neither device, so a count of the second could look short of
every object while the first held nothing. The chance of 48 objects missing the first device is
2⁻⁴⁸. The writer cycles through every object it put, so a few more objects only lengthen its rounds.

## Alternatives rejected

- **Weights that tilt placement toward the first device** in the row's devices: the row is about a
  move under writes, and a lopsided pool would make it a row about weights.
- **Objects chosen by path until one lands there**, by computing placement in the test: the row
  would depend on the placement rule's internals, which `put`'s answer does not name.
- **The assertion dropped**: the move then may carry no object, and the row would pass without
  testing what it names.

## Invariants to uphold

- **A row that needs data on a device puts until the device holds it**, never a fixed count whose
  spread is a draw.
- **A row reads where a chunk is in place, not where it is missing**: a staged copy is on no
  device yet.

## Still open

- **`an_erasure_coded_move_decodes_what_its_old_holder_lost` asserts the same premise** with eight
  objects, about one run in 256 by placement alone. That is its first message, *node two's first
  device held nothing to move*, on
  [item 282](../known-issues.md#282-an_erasure_coded_move_decodes_what_its_old_holder_lost-misses-its-deadline-under-load),
  whose second message, a move short of its deadline, is unexplained.

## Tests

| Test | What breaks if the fix is reverted |
| --- | --- |
| `move_serves_reads_and_writes_throughout` (`shoal/tests/cluster_fixture.rs`) | Twelve objects put alone; about one run in 4,096 they all land on node two's second device and the row fails before its move |

## Related

- [F88, recovery and moves](../../features/recovery-and-moves.md), whose S10 row this is
- [Tests that fail under load](../flaky-tests.md), where the run is recorded
- [Item 282](../known-issues.md#282-an_erasure_coded_move_decodes_what_its_old_holder_lost-misses-its-deadline-under-load),
  whose first message is the same premise
