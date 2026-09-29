# 193. A rebuild dialled the old identity's address twenty times a second

Filed in round 15 of the lab testing and fixed in round 16
([cluster testing](../../cluster-testing/correctness.md#17-round-16)).

## Symptom

For the whole of a rebuild ([F56](../../features/cluster-rebuild.md)), every other member's
journal filled with failed dials of the rebuilt node's old identity, and the rebuilt node's with
the handshakes it refused:

```text
WARN shoal_core::server::peer::link: msg="a peer link could not be made" node=1731724f…
     lane=control error=Shoal(CertificateIdentity { claimed: NodeId(1731724f…), certified: Some(NodeId(b51056c2…)) })
WARN shoal_core::server::control::listener: msg="refused a control peer at tls" … UlpUnavailable(ENOTCONN)
```

About twenty a second each side: 9,374 and 9,349 lines in an eight minute rebuild, 53,588 and
55,423 in a forty-five minute one. Europa's journal over round 15's rebuilds held 36,479 failed
dials, 22,938 of them on the control lane and 13,541 on the replication lane, and 35,000 of them
were `CertificateIdentity` verdicts - the dial that [Resolved #172](identity-refusal-redials.md)
already makes wait its whole backoff.

## Cause

Two causes, one a lane.

**The control lane's links were keyed by address.** `PeerNetwork::link` (`control/network.rs`)
cached one `ControlLink` per control address, and replaced it whenever the identity a caller
expected there differed from the cached link's. A rebuilt node answers its old identity's
address, and the old identity is a control voter until its removal commits, so openraft
heartbeats both identities at one address every 50 ms. Each heartbeat to one replaced the link
to the other with a fresh `Link`, whose backoff started over at `reconnect_min` and which dialled
on its first frame. The link to the old identity dialled, heard the new identity's certificate,
and was thrown away before its backoff could grow; the link to the new identity was torn down
twenty times a second while it was *up*, and every control RPC in flight on it failed
"cancelled". In the fixture, the control leader made 402 links in twelve seconds to reach the
two identities, and its link to the new one had sent one frame.

**The replication lane's verdicts stopped growing at `reconnect_max`.** A shard's links are
keyed by identity, so their backoff grew as #172 meant - and stopped at five seconds. Six shards
a node dialling the old identity every five seconds is one dial a second a node, for the length
of a rebuild: 13,541 lines on europa. A verdict does not change with the next dial the way a
refused connection does, and the ceiling that is right for a peer that may come back is not the
ceiling for one that answered that it is somebody else.

## Evidence

**Established from the lab's journals, then reproduced in the fixture.** Europa's journal
counted by lane and error over round 15's rebuilds (`journalctl -u shoal-tmdb`): the control lane
at exactly one failed dial a second per old identity outside the bench (`.947` every second),
the replication lane at the cap. The fixture test `a_rebuilt_identity_is_dialled_at_its_backoff`
was written first and run on the address-keyed links:

```text
the control leader made 402 links in 12 s to reach two identities at one address
  (before {"2743d77f…": 1, "43ce1b34…": 2}, after {"2743d77f…": 1, "43ce1b34…": 2})
```

The per-link `dials` did not move between the two readings, because every replaced link took
its counter with it; the links *made* is the figure that shows the thrash, and it is what the
test asserts on. On the fix the same run makes none, and the old identity's link is dialled
seven times in the twelve seconds as its backoff doubles.

**On the lab**, hyperion rebuilt under the mixed bench on the fixed build
([round 16](../../cluster-testing/correctness.md#a-rebuild-without-the-dial-noise)), 18 sets in
178 s: europa's journal held 40 failed dials that were verdicts over the rebuild - 7 on the
control lane and 33 on the replication lane, 9, 19, 6, 5 and 1 in its five minutes as the backoffs
doubled - where round 15's eight minute rebuild held 9,374; titan's 31; and hyperion refused 25
control handshakes where it had refused 9,349. The 433 refused connections in europa's journal
were the minute hyperion was stopped and wiped, redialled at `reconnect_min` as a node that may
come back is.

## The fix

- **Control links are keyed by address and identity** (`PeerNetwork::link`), so a rebuilt node's
  two identities at one address each keep a link, and a link's backoff outlives the next
  heartbeat to the other. `PeerNetwork::forget_node` drops an identity's links once the member
  is `Removed` or tombstoned, from `ping_members`, so nothing is held for an identity nobody
  dials. `PeerNetwork::views` reports the links and how many have been made, through
  `ControlRequest::Links` and `ShoalPool::control_links`, which the fixture reads as
  `CONTROL_LINKS`.
- **A verdict's backoff grows to `VERDICT_BACKOFF_MAX`** (60 s, or `reconnect_max` if that is
  longer) rather than to `reconnect_max` (`peer/link.rs`, `run`). A dial that fails another way
  keeps the transport's ceiling. Nothing in `is_identity_verdict` changed: the rule that a verdict
  is the peer's own answer, never something read from the map, stands.

## Alternatives rejected

- **A tombstone written when the removal plan starts, ending the links at every door.** A live
  `Remove` - a decommission of a member that is up - moves the member's sets with the member
  still serving, and a tombstone would cut it off. The judges read the tombstone; making it
  earlier for a rebuild alone would need a second kind of tombstone.
- **Skipping a `Removing` member in `ping_members`.** The pings feed the detector and the
  reachability report, and a decommissioning member is up and worth pinging. The pings were also
  not the noise: they came once a second, the heartbeats twenty times.
- **Reading the member's phase in the link.** #172's rule: nothing inferred from the map goes
  into a dial's judgement of its failure. The map arrives later than the peer's own answer, and
  a link that stopped dialling on the map's word would not notice the map being wrong.
- **A longer `reconnect_max` for every failure.** A refused connection wants the early wake a
  queued frame gives it, since the node may be starting; only a verdict is the same on the next
  dial.

## Invariants to uphold

- A control link is looked up by `(control address, identity)`, never by the address alone.
  Two identities at one address are two members as far as the control group is concerned, and
  each must keep its own backoff.
- A link that is replaced takes its counters with it. A test that wants to see links being
  replaced reads `made`, not `dials`.
- `is_identity_verdict` classifies the peer's own answer and nothing else. The ceiling a verdict
  grows to is the one place the two kinds of failure are told apart after the classification.
- An identity's control links are forgotten once it is `Removed` or tombstoned, and not before:
  a `Removing` member is still dialled.

## Still open

- Every group that still names the removed identity queues frames for it, which costs a frame's
  encoding and a dropped queue per heartbeat until the move removes it from the group. #172's
  note, unchanged.
- A seed is dialled under the nil identity, so a joiner that later learns the node at a seed's
  address holds two links to it, the seed's idle beside the member's: one parked task per seed
  address, for the node's run.
- The failed dials while the old identity's node is stopped and wiped are refused connections,
  not verdicts, and redial at `reconnect_min` as they should: about ten a second a link for the
  minute or two the wipe takes.

## Tests

| Test | What breaks if this is reverted |
| --- | --- |
| `a_rebuilt_identity_is_dialled_at_its_backoff` (`shoal/tests/cluster_fixture.rs`) | The control leader makes hundreds of links in twelve seconds to reach a rebuilt node's two identities, or the old identity is dialled more than its backoff allows, or the new identity's link is dialled while it is up |
| `identity_verdicts_wait_out_the_backoff` (`shoal-core/src/server/peer/tests.rs`) | The classification a verdict's ceiling depends on changes |

## Related

- [Resolved #172](identity-refusal-redials.md), the verdict this ceiling extends.
- [F38](../../features/inter-node-transport.md), the links and their backoff.
- [F56](../../features/cluster-rebuild.md), the rebuild that reuses an address under a new
  identity.
- [Cluster testing, round 16](../../cluster-testing/correctness.md#17-round-16).
