# 333. A device removed as lost was still asked as a holder, and the moves off it crawled

A device removed as lost is tombstoned, its era ended and its id never accepted again, and its node
refuses to serve it by name ([F92](../../features/reclamation-and-device-lifecycle.md#a-devices-life)).
Its placement groups then move off it, and each switch names the slice it left for its release past
the reclamation grace ([F92](../../features/reclamation-and-device-lifecycle.md#what-a-move-leaves)).

Found planning [item 250](empty-groups-move-without-a-slot.md)'s fix (2026-10-28), by reading the
source, then reproduced.

## Symptom

- **Rebuilds asked the removed device.** In the flaky-test plan's evidence loop `w8`, nodes one and
  two asked a rebuild of a device removed as lost 275 times in two minutes, each refused
  `DeviceFailed` (*the device failed: device 01a17f9b-… was removed by process …, lost: wipe it and
  add it as a new device*).
- **The moves off it crawled once reclamation ran.** A device removed as lost while its node was
  up, with a reclamation grace of two seconds: 120 of about 2,000 placement groups had moved after
  fifteen minutes, none in flight.

## Cause

A removal tombstones a device and ends its era, and never sets `failed`, which is its node's report
of an I/O failure. Three checks of whether a slice still serves read `failed` alone:

- **`holder_serving`** (`shard/objects/recover.rs`) named the removed slice a holder that can take a
  rebuild, so every poll asked it.
- **`Mover::serving`** decided what a copy does with a position no holder gave bytes for: marked
  missed if its old slice is gone, the move given up otherwise. A position whose only copy was on
  the removed device gave the move up.
- **`Mover::gone`** lets a release skip a slice that never serves again. A removed slice was sealed
  first instead. Its node refused the removed device, and the release failed.

The releases did more than fail. Each poll's work put a group's release ahead of the moves after
it, from one budget of sixteen tasks a shard. Read from the figures, not traced: the releases of the
slices the moved groups left, failing and asked again at every poll, held that budget, and the moves
still to come waited behind releases that could never succeed. Nothing was in flight while 1,889
groups were behind.

## Evidence

**Established by reading the source, then reproduced** by
`a_slice_on_a_device_removed_as_lost_is_released_without_a_word`, written first: two devices a host,
eight objects put, a reclamation grace of two seconds, and node two's first device removed as lost
while its node is up. On the tree before, the moves never settled within its 900 s:

```text
the groups moved off the removed device never came: [{…,"moves":0,…,"recovery":{…,"behind":1889,"moves_aborted":0,"moves_switched":120,…}]
```

The run's whole output is in `target/lab/i250/repro-before-full.txt`.

## The fix

- **A device serves nothing once it failed, was retired, or was removed**
  (`DeviceRecord::serves_nothing`), and the three checks read it: a rebuild never asks a removed
  slice, a copy with no source but it marks the position missed, and a release of it is done
  without a seal.
- **A poll orders releases after every move** ([item 250](empty-groups-move-without-a-slot.md)'s
  order), so a release that keeps failing for any reason never holds a move back.

## Alternatives rejected

- **Set `failed` at a removal.** `failed` is a node's report, cleared by the report after a restart
  that finds the device whole, and the readiness of a pool reads it as one. A removal is an
  operator's committed fact, which `tombstone` already records.
- **Read `DeviceRecord::refusal`**, which already says when a node must not serve a device. It
  formats a reason, and these checks run for every holder of every rebuild and copy.

## Invariants to uphold

- **Wherever a slice is judged able to take a request, its device is judged by `serves_nothing`**,
  never by `failed` alone.
- **A release of a slice that serves nothing commits without a word to it.**
- **No release is ordered ahead of a move.**

## Still open

- `Reclaimer::sweep` and a pool's readiness read `failed` and `tombstone` in their own ways, and
  neither reads `retired`; neither met a case here.

## Tests

| Test | What fails if the fix is reverted |
| --- | --- |
| `a_slice_on_a_device_removed_as_lost_is_released_without_a_word` (`cluster_fixture`) | The moves off a device removed as lost crawl behind releases that fail at its seal, and its slices are never released |
| `a_removed_device_serves_nothing_though_it_never_failed` (`poolmap`) | A removed device, which never failed, is taken as serving |

## Related

- [F92](../../features/reclamation-and-device-lifecycle.md), a device's life and the releases.
- [F88](../../features/recovery-and-moves.md), the moves and the rebuilds.
- [Resolved #250](empty-groups-move-without-a-slot.md), whose order puts releases last.
