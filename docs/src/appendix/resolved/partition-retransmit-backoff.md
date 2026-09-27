# 181. A silent partition's links stayed down for most of a minute after it healed

## Symptom

On the lab, hyperion's peer ports were dropped both ways for 60 s under the mixed bench
([cluster testing, section 12](../../cluster-testing/correctness.md#12-scenarios-nobody-had-run)).
When the rules came off, the cluster did not recover. Writes to hyperion's groups and writes through
hyperion went on being refused `NotLeader`, about 19,000 a second, for the rest of the run: 45 s
after the heal. hyperion's journal showed its replication RPCs timing out ("the replication link
failed: the replication rpc timed out") until 48 s after the heal. The same test at 20 s had
healed in about 6.5 s, which is why nobody had noticed.

## Cause

A partition that drops packets leaves every TCP connection open. Each side's sent data goes
unacknowledged, and the kernel retransmits it on a timer that doubles every try: a fifth of a
second, then 0.4, 0.8, and so on up to two minutes. It gives up on the connection only after about
fifteen minutes (`tcp_retries2`). So after a partition of a minute the next retransmission is tens
of seconds away. When the partition heals, nothing moves on the connection until that
retransmission fires. The peer links are long-lived connections that nothing else closes: the
link is up as far as Shoal knows. The recovery time was therefore set by the kernel's backoff,
and it grew with the length of the partition.

## Evidence

**Established on the lab**, by the length of the partition:

| Partition | After the heal, until refusals stop |
| --- | --- |
| 20 s (`r11/143-partition`) | about 6.5 s |
| 60 s (`r11/sc/partition-60s`) | more than 45 s, the rest of the run; the journal's last failed RPC 48 s after the heal |

The fixture cannot show it. Its `blackhole` is a proxy on loopback, which acknowledges every
segment at the kernel however little it forwards, so no retransmission timer ever backs off.

## The fix

Every peer connection, on all four lanes and on both ends, has `TCP_USER_TIMEOUT` set to
`transport.unacked_timeout`, 5 s by default (`peer::set_unacked_timeout`, called by
`link::connect`, `peer_acceptor` and `control_acceptor`). The kernel then aborts a connection
whose sent data has gone unacknowledged for that long. The link sees its connection fail, goes
down, and dials again. A dial is bounded by `handshake_timeout`, and redials back off to at most
`reconnect_max`. After the heal the link is back within one dial, whatever the partition's
length.

The same 60 s partition with the fix (`r11/sc/partition-60s-181`): refusals fell from 18,000 to
5,000 in the second after the heal, and to none the second after that. Every one of 1,003,763
acknowledged inserts was read back through each member.

## Alternatives rejected

- **Closing a link from Shoal when it is judged silent.** The hop silence of
  [#143](silent-partition-hops.md) already knows a link is silent, a second and a half in. But a
  paused process (`SIGSTOP`) is silent too, and its connections are healthy: the kernel
  acknowledges for it. Aborting on unacknowledged data is a judgement of the network, which is
  what this is.
- **TCP keepalive.** It probes only an idle connection. These carry heartbeats every tenth of the
  failover base, so they are never idle.
- **Lowering `tcp_retries2` on the hosts.** It works, and it is host configuration that a
  deployment would have to remember, for every TCP connection on the host.

## Invariants to uphold

- **Every peer connection carries the timeout, on both ends.** A connection set on one end only
  heals on that end's side; the other end's replies wait out the backoff.
- **The timeout counts unacknowledged data, never silence.** A connection whose peer is busy,
  paused or has nothing to say is acknowledged by the peer's kernel and is left alone.
- **It is longer than any pause the kernel can cause** on a healthy link, so it aborts only a
  connection whose packets are not arriving.

## Still open

- For the first 5 s of a partition the connection is still up, so hops wait for the hop silence
  ([#143](silent-partition-hops.md)) and not for the abort.
- The client port is not covered: a client connection is the client's to keep alive.

## Tests

| Test | What breaks if this is reverted |
| --- | --- |
| `unacked_timeout_is_set_on_the_socket` (`server/peer/tests.rs`) | A peer connection is left on the kernel's default of about fifteen minutes |
| The 60 s partition ([cluster testing, section 12](../../cluster-testing/correctness.md#a-longer-partition)) | A long silent partition's links stay down for most of a minute after the heal |

## Related

- [Resolved #143](silent-partition-hops.md), the partition's first seconds.
- [C2](../../distributed/transport.md), the transport and its lanes.
