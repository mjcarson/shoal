# 310. No connection probed its peer, so one whose peer vanished while it owed nothing was never noticed

Every peer connection set `TCP_USER_TIMEOUT` ([Resolved #181](partition-retransmit-backoff.md)),
which aborts a connection whose sent data goes unacknowledged for the transport's timeout. That
bounds only data the connection sent. A connection that owed nothing had nothing for the timeout to
see: an asker whose requests were sent and acknowledged, waiting on their answers, or a node's end
of a client's idle connection. If its peer vanished without a reset reaching it, it stayed
established for good, and so did every request it carried. No peer lane and no client connection
set keepalive.

Found while reading the transport for
[item 305](../known-issues.md#305-a-captures-1-mib-puts-across-hosts-stalled-whole-and-its-check-could-not-read-10-objects),
and filed and fixed in one change, the last of the silent waits wave three set out to bound.

## Symptom

- **A link that looks up to a peer that is gone.** Its requests are neither answered nor refused,
  until something sends on it. The send goes unacknowledged, and `TCP_USER_TIMEOUT` aborts the link
  then.
- **A node's end of a client's connection, held open for a client that is gone,** with its relays
  and buffers.
- Both only when the peer disappears without its kernel's reset reaching this end: a host lost, a
  network that drops packets, or a reset that was itself dropped.

## Cause

A TCP connection with nothing outstanding sends nothing, so a peer that vanished is noticed only
when this end next writes, and the user timeout counts only unacknowledged writes. Keepalive is
what probes an idle connection, and nothing set it:
- `peer::set_unacked_timeout` was the only option on a dialled or accepted lane (`peer/link.rs`,
  `peer/listener.rs`, `control/listener.rs`);
- a client's connection had `TCP_NODELAY` alone at both ends.

## Evidence

**Established by reading the source.** A peer that vanishes silently cannot be made in a test here
without dropping packets in the kernel, which needs root and the host's firewall. The fixture's
blackhole keeps its userspace proxies' connections open, so their kernels would acknowledge every
segment and answer every probe.

What a test can show is that no connection probed, read from the sockets. On the tree without the
fix:
- **`both_ends_of_a_clients_connection_are_kept_alive`** (`shoal/tests/objects.rs`) reads
  `SO_KEEPALIVE` from both ends of a client's connection to a node in process:

  ```
  the node's end of a client's connection: [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
  ```

- **`every_lane_and_client_connection_is_kept_alive`** (fixture) reads every connection of each
  node with `ss -o`: an idle connection that probes shows a keepalive timer, and none showed any.

  ```
  node 0 holds connections that never probe their peers: ["127.0.0.1:35334 to 127.0.0.1:28002 (\"\")", ...]
  ```

## The fix

- **Every peer lane probes its peer after `cluster.transport.keepalive`**, 10 seconds by default.
  `set_keepalive`, in `shoal-proto`'s `shared::sockets`, sets:
  - `SO_KEEPALIVE`;
  - `TCP_KEEPIDLE` at the idle;
  - `TCP_KEEPINTVL` at a fifth of it, a second at least;
  - `TCP_KEEPCNT` at three.

  It is called beside every `set_unacked_timeout`, on a dialled link, an accepted data or object
  lane and an accepted control lane. With `TCP_USER_TIMEOUT` set too, the kernel aborts the
  connection once its probes go unacknowledged for that timeout. The link goes down, every request
  it carried is refused by name, and it is dialled again.
- **A client's connection probes after a minute at both ends** (`sockets::CLIENT_KEEPALIVE`): the
  node lets go of a client that vanished, and the client's reader ends on a node that did.
- **Zero leaves keepalive off**, as zero leaves the user timeout at the kernel's own.

## Alternatives rejected

- **An application ping on every lane.** The control lane pings already, and the replication lanes
  carry heartbeats. An object lane carries requests only when there is work, and a ping would add a
  frame kind to answer, where the kernel's probe needs nothing from either process.
- **A deadline on every request a link carries.** The coordinators bound their gathers by
  `query_deadline` already, and Resolved #307 and #308 bound the rest. What is left is the link
  itself: its waiters held, and every later request sent into a socket whose peer is gone, until a
  write times out.
- **A shorter keepalive.** A peer is probed after ten seconds idle and given up about five seconds
  of unanswered probes later, so a vanished host's links go down in about fifteen seconds. A
  shorter idle would probe busy clusters' quiet lanes for nothing, and the setting is there for a
  deployment that wants it.

## Invariants to uphold

- **Every connection between nodes sets both the user timeout and keepalive**, at every site that
  makes one. A new lane or a new acceptor calls both, or a vanished peer is noticed by one kind of
  link and not another.
- **The kernel answers probes**, so keepalive says only that the peer's host is there, never that
  its process is answering. A wedged process with a live kernel is bounded by the lanes' own bounds
  (the stall bound, Resolved #307) and the coordinators' deadlines, not by this.

## Still open

- **The orphaned stream task** found in the same reading is filed, not fixed: a whole chunk's
  stream task on a holder waits on its next piece for good when its sender stops, holding a pool
  file. It leaks; it does not stall a request.

## Tests

| Test | What fails if the fix is reverted |
| --- | --- |
| `shoal-proto` `shared::sockets::tests::keepalive_is_set_on_the_socket` | A connection given keepalive does not probe after its idle, every fifth of it, three times; or zero sets it |
| `both_ends_of_a_clients_connection_are_kept_alive` (`shoal/tests/objects.rs`) | Either end of a client's connection to a node does not probe, or not after a minute |
| `every_lane_and_client_connection_is_kept_alive` (fixture) | A connection of a cluster node, a lane on either port or a client's, shows no keepalive timer while it is idle |

## Related

- [Resolved #181](partition-retransmit-backoff.md): the user timeout, which keepalive's probes are
  bounded by.
- [Resolved #307](a-lane-whose-writer-stops-ends.md), [#308](an-object-attempt-gives-up.md) and
  [#309](a-full-feed-leaves-its-connection-read.md): the other silent waits from the same reading.
- [Item 305](../known-issues.md#305-a-captures-1-mib-puts-across-hosts-stalled-whole-and-its-check-could-not-read-10-objects).
- [The configuration page](../../getting-started/configuration.md): `cluster.transport.keepalive`.
