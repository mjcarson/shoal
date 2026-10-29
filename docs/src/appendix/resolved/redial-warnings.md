# 319. A node whose peer refused its connections logged every redial of every link at WARN

A peer link dials lazily, on its first frame, and dials again after a failure. It backs off up to
`reconnect_max`, but never for longer than until a frame wants to go, and never for less than
`reconnect_min`, 100 ms by default. A replication link always has a heartbeat waiting, so while its
peer refuses every connection it dials about ten times a second for as long as the peer is gone.

Each failed dial logged a WARN, "a peer link could not be made". The link then told its shard the
link was down, and the shard logged a second WARN, "a replication link went down", for a link that
had never been up. Neither was throttled.

Found in europa's journal while reading the [store comparison](../../object-storage/store-comparison.md)'s
second run, and reproduced by a test.

## Symptom

On 2026-10-24 a node of the lab's `storesssdec21-bench` cluster on europa was left running from 06:33
until it was stopped at 18:59, its peers gone, after the run it belonged to was cut short
([Resolved #306](lab-run-restarted-over-its-cluster.md)). In the minute from 16:00 UTC it logged
32,703 lines:

```text
WARN shoal_core::server::peer::link: msg="a peer link could not be made" node=323525aa-… lane=replication error=IO(Os { code: 111, kind: ConnectionRefused, message: "Connection refused" })
WARN shoal_core::server::shard: msg="a replication link went down" node=323525aa-… slot=Some(3) reason="IO(Os { code: 111, kind: ConnectionRefused, … })"
```

That was 16,052 failed dials on replication lanes, each with its "went down", about 3,200 for each of
the peer's five slots, and 599 failed dials on the control lane. journald suppressed about 6,350
more of the unit's lines every 30 seconds. From 15:45 to 18:59 the journal kept 5.8 million lines of
that unit, and europa's journal now begins at 15:45 that day: nothing before it is left, the
comparison's first-run node logs of 2026-10-18 and 19 among them.

A cluster with one node down meets the same: every member's links to it dial again at the floor for
as long as the outage lasts, and say so twice a dial.

## Cause

`run` (`peer/link.rs`) logged every failed dial at WARN, with no memory of the dials before it:

```rust
event!(Level::WARN, msg = "a peer link could not be made", %node, %lane, ?error);
```

`LinkEvent::Down` was delivered both for a connection that dropped and for a dial that never
connected, with nothing to tell the two apart, and the shard's `handle_peer_event` logged every
replication lane's Down at WARN as a link that "went down". The bulk and data lanes' Downs did the
same.

The redial's cadence is on purpose: "a frame waiting out a backoff is a caller waiting out a backoff"
([Resolved #172](identity-refusal-redials.md) bounded it for verdicts on identity, which no frame
changes). The defect is the log, not the dials.

## Evidence

**Reproduced by `a_refused_peer_is_warned_of_once_not_at_every_dial`** (`shoal-core`'s
`server::peer::tests`), written before the fix. A data link dials a port nothing listens on while a
frame is queued every 20 ms for a second and a half, as a heartbeat would be, and a layer counts the
failed dials the link logs and the ones logged at WARN. It requires at least five failed dials, so it
cannot pass without the redials.

On the tree before the fix:

```text
thread 'server::peer::tests::a_refused_peer_is_warned_of_once_not_at_every_dial' panicked at shoal-core/src/server/peer/tests.rs:1264:5:
assertion `left == right` failed: 16 failed dials logged as warnings
  left: 16
 right: 1
```

The journal's counts above are from `journalctl -u shoal-storesssdec21-bench.service` on europa.

## The fix

**The first failed dial of a run is a warning, and the ones after it are not.**
- **The link counts its failed dials** since it was last up. The first is logged at WARN as before.
  The dials after it are logged at DEBUG, until `DIAL_WARN_EVERY` (10 s) has passed since the last
  warning, when one WARN, "a peer link still cannot be made", says how many failed since.
- **A link made again after failed dials says so at INFO**, with how many failed, and its count
  starts over.
- **`LinkEvent::Down` says whether the link was up** (`connected`): true for a connection that
  dropped, false for a dial that never connected. The shard logs a replication, bulk or data link
  that went down at WARN only when it was up, and a dial that never connected at DEBUG, since the
  link logged it already.

What the link does is unchanged: when it dials, what it delivers, and what its owners do with a
Down.

## Alternatives rejected

- **Dialling less often while a peer refuses.** A refused connection is a peer that may be starting
  again at the same address, and a frame waiting out a backoff is a caller waiting out a backoff:
  the floor is what lets a restarted node's writes through at once.
- **One throttle for the whole process**, as F108's refusals have. It would let one link's failures
  through and hide which peer every other warning was about.
- **Dropping the shard's "went down" altogether.** A link that was up and dropped is worth a warning
  in its shard's own words, which name the slot.

## Invariants to uphold

- **A link's failed dials are warned of once a run, then at most every `DIAL_WARN_EVERY`.** A new
  log on that path counts against the same run.
- **A Down says whether its link was up.** An owner logs "went down" only for one that was, and a
  new owner of link events reads `connected` before it logs a Down as a failure.
- **The redial's cadence is the transport's, not the log's.** Nothing here changes when a link
  dials.

## Still open

- **The warning every ten seconds is not tested**: the test runs for a second and a half, and the
  interval is a constant.
- **A link that connects and drops again and again** still logs "a peer link dropped" at every drop.
  It was not seen here.

## Tests

| Test | What fails if the fix is reverted |
| --- | --- |
| `a_refused_peer_is_warned_of_once_not_at_every_dial` (`shoal-core`, `server::peer::tests`) | Every failed dial to a peer that refuses is logged at WARN, sixteen in a second and a half; or a failed dial's Down is delivered as a connection that dropped |
| `identity_verdicts_wait_out_the_backoff` (`shoal-core`, `server::peer::tests`) | Unchanged: which failures wait out the whole backoff ([Resolved #172](identity-refusal-redials.md)) |

## Related

- [Resolved #172](identity-refusal-redials.md) and [Resolved #193](rebuild-redial-thrash.md): the
  redial's backoff for verdicts on identity.
- [Resolved #306](lab-run-restarted-over-its-cluster.md): the run whose leftover node logged this.
- [F108](../../features/object-work-in-flight.md): the throttled warnings of an object lane's
  refusals.
