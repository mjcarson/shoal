# 172. A link refused on who its peer is redialled ten times a second

## Symptom

In the rebuild of [section 8](../../cluster-testing/correctness.md#8-an-unplaced-member-coordinates),
hyperion's node logged 60,478 refused TLS handshakes in about twelve minutes:

```text
WARN shoal_core::server::peer::listener: msg="refused a peer"
     error=Tls(UlpUnavailable(Os { code: 107, kind: NotConnected, … }))
```

Every other member, hyperion's new identity included, kept dialling the old identity at the
address hyperion now answered as the new one:

```text
WARN shoal_core::server::peer::link: msg="a peer link could not be made"
     node=84d28e9b… lane=replication error=Shoal(CertificateIdentity { claimed: 84d28e9b…,
     certified: Some(0935612f…) })
```

That is about 84 full TLS handshakes a second landing on a four-core Zen1 host that was also
installing the moves, and each one could only fail.

## Cause

A link that fails to dial waits a backoff that doubles from `reconnect_min` (100 ms) to
`reconnect_max` (5 s). But only the first 100 ms of it is a real wait: after that, any frame
queued on the link ends the wait, because "a frame waiting out a backoff is a caller waiting out a
backoff". openraft queues a heartbeat to every member of every group it leads, and the old
identity stayed a voter of each set until that set's move reached it. So every link to the old
identity redialled every 100 ms, on every shard, on both lanes, for as long as the rebuild ran.

That rule is right for a peer that is down or restarting: its next start should be found at once.
It is wrong for a peer that answered and said it is someone else. That answer is the same on the
next dial.

## Evidence

**Established from the lab's logs and the source.** The counts are from hyperion's journal for
the rebuild's first twelve minutes. The classification the fix adds is pinned by
`identity_verdicts_wait_out_the_backoff`. The lab count on the fixed build is in
[section 8](../../cluster-testing/correctness.md#8-an-unplaced-member-coordinates).

## The fix

`is_identity_verdict` (`peer/link.rs`) marks a dial that failed because the peer's certificate or
hello named another node (`CertificateIdentity`, `PeerIdentity`), or because the peer refused us
as mismatched, removed or of another cluster. Such a failure waits its whole grown backoff, with
no early wake for a queued frame, so a link to an identity that is gone settles at one dial every
five seconds. Every other failure keeps the early wake:

- a refused connection or a timeout;
- `UnknownNode`, which a joiner meets until the map reaches its peer;
- `Fenced`, which the next start may clear.

## Alternatives rejected

- **Stop dialling a member the map calls removed.** The map is the leader's word and arrives
  later than the peer's own answer. A link would also still dial through a rebuild's whole window,
  before the removal is committed.
- **Raise `reconnect_min`.** That slows the reconnect after every ordinary restart, to spare a case
  the peer has already named.

## Invariants to uphold

- **A failure the next start can change keeps the early wake.** A restarting node, and a joiner
  its peers have not heard of yet, are found as soon as a frame wants to go.
- **A verdict is the peer's own answer about who it is.** Nothing inferred from the map goes in
  `is_identity_verdict`.

## Still open

- Nothing stops the groups that still name a removed identity from queuing frames for it, which
  costs a frame's encoding and a dropped queue per heartbeat. A move removes the identity from
  each group, so this lasts only for a rebuild's window.

## Tests

| Test | What breaks if this is reverted |
| --- | --- |
| `identity_verdicts_wait_out_the_backoff` (`shoal-core/src/server/peer/tests.rs`) | An identity verdict is retried at once, or a refused connection, an unknown joiner or a fenced run waits out its backoff |
| `address_change_is_observed_and_a_stale_clone_is_fenced` (`shoal/tests/cluster_fixture.rs`) | A node that moved address is not found again, or a stale clone is not fenced |

## Related

- [F38](../../features/inter-node-transport.md), the links and their backoff.
- [F50](../../features/cluster-operations.md), the certificates that bind an identity.
- [F56](../../features/cluster-rebuild.md), whose rebuild reuses an address under a new identity.
