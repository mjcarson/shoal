# 304. Since F106 a frame was one segment, and one a full link dropped waited out the kernel's 200 ms loss probe

[F106](../../features/lane-copies.md) brought the glommio fork's vectored write, which sends a
frame's parts in one `sendmsg` where each part had been a call of its own. With `TCP_NODELAY` a call
is a segment, so a small frame went from two to four segments to one. When a link that was full
dropped such a frame, nothing after it on its connection said so for a while. The kernel's probe for
a lone segment's loss waits two round trips and its minimum retransmission timeout, 200 ms by default.
A table's strong reads across hosts, beside gets that filled a link, came to have a tail of about
110 ms where it had been about 17.

Found by lab 2 of the store optimizations (2026-10-23), filed as item 304, bisected and fixed the same
day.

## Symptom

- **The paced table reads' tail.** On the SSD leg with three copies, the TMDB `Movie` table's reads
  were paced at 500 a second beside 1 MiB gets at sixteen in flight, which keep europa's 1 GbE link
  full.
  - Their p99.9 was 110.6 to 215 ms in all 72 runs of builds with the vectored write.
  - It was 16.4 to 33.6 ms in all 24 runs of builds without it.
  - Their p99 rose by about a millisecond, and in six runs to about 100 ms.
- **The strong reads' rounds.** The captures count each barrier's wait and each round's appends by
  bucket ([F99](../../features/strong-read-rounds.md)).
  - With the vectored write, 0.45% of the barriers took over 33 ms, where 0.11% did without it.
  - Of the rounds' appends, 0.68% took over 33 ms between being sent and answered, where 0.27% did.
  - No follower ever took over 25 ms to answer one.
- **On europa's loopback**, where nothing is dropped, the same load's p99.9 stayed at 1.5 to 4.0 ms.
- **No get was slower.** Every capture's gets were europa's link's.

## Cause

Linux's tail loss probe (`tcp_schedule_loss_probe`) is how a connection finds a loss that nothing sent
after it will reveal. It fires two smoothed round trips after the last send. When only one packet is
in flight it adds the minimum retransmission timeout, 200 ms by default, to allow for a receiver that
delays its acknowledgement. With more than one packet in flight it adds 2 ms.

- **Before the vectored write**, a frame left a part at a time, each part a segment.
  - A loss of any segment but its last was revealed by the segments behind it, through their
    selective acknowledgements, within a round trip.
  - With several in flight, the probe came 2 ms after two round trips.
  - The hosts' counters show those frames drawing about four times the loss probes the later builds
    drew (`w2-bisect-tcp.txt`).
- **Since**, a small frame is one segment: a strong read's heartbeat, its answer, and a table read's
  answer to its client.
  - Where the switch's port into europa was full and dropped one, its connection learned of it only
    from the next frame's acknowledgement, or from the probe, 200 ms and two round trips on.
  - A strong read whose round or answer met such a loss waited from tens of milliseconds to about
    230.

The vectored write did not lose more: retransmits ran at 115 to 155 for every 10,000 segments on every
build. It made each loss slower to find.

## Evidence

**Established by bisecting builds on the lab and trying the cause there, then reproduced by a test**
before the fix.

The paced reads' p99.9, each build's runs (2026-10-23, under `shoal-spike/results/stores-ab/`):

| Captures | Build | p99.9 ms |
| --- | --- | --- |
| `w2-ssd-rep3-tail` | F104 | 16.4–17.9 |
| | F105 | 16.9–19.0 |
| | F106 | 111.0–121.1 |
| | F107, no cache | 110.8–111.7 |
| `w2-ssd-rep3-bisect` | F105 | 16.8–33.6 |
| | F105 on the fork after its vectored write | 111.0–112.3 |
| | F106's read half | 111.0–215.0 |
| | F106 | 110.6–113.2 |
| `w2-ssd-rep3-preempt` | F106 | 110.8–111.7 |
| | F106, its shards preempted every 10 ms | 111.5–120.3 |
| `w2-ssd-rep3-rto1` to `rto4` | F106, every host's `net.ipv4.tcp_rto_min_us` at 200,000 (1 and 4) | 111.0–111.8 |
| | F106, at 5,000 (2 and 3) | 13.3–14.4 |

What each run showed:
- **The bisect.** F105's code built against the fork after its vectored write has the tail, and
  against the fork before it has none. The vectored write alone brings it.
- **The shards' preemption** was tried first, since a busy executor reaps a stream's readiness every
  100 ms by default. It made no difference.
- **The minimum RTO** removed the tail. Every host was set to 5 ms between captures, in the order
  default, low, low, default. The gets the other two nodes led ran 111.3 to 111.9 a second in all
  four, europa's link's; how many gets europa led moved the rest, by each deployment's draw.

The test, first `every_connection_retransmits_on_the_minimum_rto` and since the fix was scoped
`lanes_of_rounds_retransmit_on_the_minimum_rto`, reads each fixture node's connections from outside it
with `ss`. Against the unfixed tree:

```text
thread 'every_connection_retransmits_on_the_minimum_rto' panicked at shoal/tests/cluster_fixture.rs:29697:9:
34 of node 0's 34 connections retransmit on the kernel's 200 ms minimum: [SocketTimer { local: "127.0.0.1:39319",
peer: "127.0.0.1:35642", rto_ms: 201.0 }, SocketTimer { local: "127.0.0.1:28001", peer: "127.0.0.1:47398",
rto_ms: 213.0 }, ... SocketTimer { local: "127.0.0.1:39319", peer: "127.0.0.1:35596", rto_ms: 204.0 }]
```

The lab's own tmdb cluster read the same: its peer connections on europa at `rto:201`.

## The fix

- **The lanes of rounds set their minimum retransmission timeout** (`TCP_RTO_MIN_US`, Linux 6.15 and
  later) to `cluster.transport.rto_min`, 5 ms unless named and `0ms` the kernel's own.
  - These are the replication and control lanes (`peer::lane_takes_rto_min`), whose frames are a
    group's heartbeats, appends, barriers and their answers.
  - A dialled lane sets it beside `set_nodelay` (`peer::link`).
  - The control listener sets it at the accept. The peer listener sets it once the hello names a
    replication lane.
- **A client's connection holds `networking.rto_min` until it carries an object operation**, 5 ms
  unless named. The shard reads the kernel's minimum at the accept and sets the floor
  (`ClientRto::hold`), and the connection's first object operation gives the kernel's back
  (`ClientRto::give_back`). A table's answer is one segment, and an object's is bulk.
- **The data, bulk and object lanes keep the kernel's 200 ms**, as a client's connection does once
  it has carried an object.
- **The start refuses a value the kernel would not take**, 2 ms to 200 ms or zero, by the setting's
  name (`conf::check_rto_min`). A refused `setsockopt` would leave the lanes at the 200 ms the setting
  exists to avoid.
- **A kernel without the option says so once, at WARN**, and its lanes go on without it.

The floor bounds only the probe, and a timeout on a fast path. Each connection's timeout still
follows its own round trip above it, so a slow path is not sent again early.

**First built on every connection** (e99379f1), a client's included (`networking.rto_min`), and
measured on the lab that way (2026-10-23). F107's build ran beside it in the order before, after,
after, before.
- **The rows' load on the SSDs** (`w2-ssd-rep3-fix`), with no cache, the median of eight runs a
  side:

  | | Before | Every connection at 5 ms |
  | --- | --- | --- |
  | Paced reads' p99.9, ms | 112 [111–121] | **13.7 [13.4–14.3]** |
  | p99 | 7.78 [7.03–8.23] | **5.50 [5.08–5.94]** |
  | p50 | 1.95 [1.12–2.23] | 2.04 [1.29–2.40] |
  | 1 MiB gets a second, sixteen in flight | 223 [220–231] | 210 [189–232], within: europa's link's |

- **The comparison's arms at 2+1** (`w2-ssd-ec21-fix`, one capture before, two after). The gets at
  one in flight paid for it:
  - 64 KiB gets at one in flight ran 0.61 times as fast, their p99 7.6 ms where it was 3.1;
  - 1 MiB gets at one in flight ran 0.89 times as fast;
  - the puts at one in flight kept their rate, and their tail halved: a p99 of 32.2 ms where it was
    62.5 at 64 KiB, and 55.9 where it was 80.9 at 1 MiB;
  - at sixteen in flight every arm was within.
- **A connection idle past its timeout restarts its congestion window**
  (`net.ipv4.tcp_slow_start_after_idle`), and on a fast LAN the floor is that timeout. A client's
  pooled connection idles a few milliseconds between gets at one in flight, so each answer of 64 KiB
  or 1 MiB went out in slow start again.
- **One capture's 1 MiB puts stalled whole** on that build, at one in flight and then at sixteen,
  and its check found 10 of 246 objects unreadable, `ConnectionLost`. No node logged anything
  about it ([item 305](../known-issues.md#305-a-captures-1-mib-puts-across-hosts-stalled-whole-and-its-check-could-not-read-10-objects)).

The user chose to scope the floor to the lanes of rounds (2026-10-23), so a client's answers and the
lanes of bytes keep the kernel's window after an idle. **Scoped so** (b90ae1fe), on the lab beside
F107's build, in the order before, after, after, before:
- **The rounds' tail went.** Barriers over 33 ms were 3 of 121,160, where 677 of 134,536 were.
- **The table reads' tail did not.** Their p99.9 was 111 ms [111–112], beside 112 [111–112]
  (`w2-ssd-rep3-scoped`).
- **O131's put tail at one in flight did not move either**: 61.9 ms against 62.2.
- **Every arm of the comparison was within**, 0.98 to 1.01, with the same p99s
  (`w2-ssd-ec21-scoped`).

So the table reads' tail is a client's answers: a read led across the full link answers its client
in one segment, and a dropped one waits the kernel's probe. The user chose to have a client's
connection hold the floor until it carries an object (2026-10-23).

**As kept** (7154dbf2), on the lab beside F107's build, in the order before, after, after, before
(2026-10-24):

| | Before | After |
| --- | --- | --- |
| Paced reads' p99.9, ms (`w2-ssd-rep3-client`) | 111 [111–113] | **14.1 [13.4–14.3]** |
| p99 | 7.40 [6.82–8.13] | 6.37 [5.33–6.90] |
| p50 | 2.15 [1.29–2.57] | 1.90 [1.55–2.56] |
| The comparison's arms at 2+1 (`w2-ssd-ec21-client`) | – | every one within, 0.93 to 1.02, the same p99s |
| 64 KiB puts at one in flight, p99 ms | 62.8 | 62.6 |

The table reads' tail went, at no cost to the comparison's arms. O131's put tail did not move:
a put's answer is one segment too, on a connection that carries objects and so keeps the kernel's
probe.

## Alternatives rejected

- **A floor on every connection**, as first built. It took the reads' tail off, and cost the gets at
  one in flight 11 to 39% across hosts, by the window's restart after an idle (above).
- **The lanes of rounds alone**, as scoped first. The rounds' tail went, and the table reads' did not,
  since their answers to the client are lone segments too (above).
- **A floor everywhere, with the hosts' slow start after idle turned off.** It is a host-wide setting
  every program shares, which a deployment would have to make.
- **Writing a frame a part a call again.** It brings back the luck of a frame with segments behind
  it, not a reason. Each part would again be a call and, under kTLS, a record, which F106 took off:
  the table path's reads were 5% quicker on the mean without them.
- **The host's `net.ipv4.tcp_rto_min_us`, or a route's `rto_min`.** That is what the lab tried. It
  changes every program on the host, the window's restart with it.
- **A shorter delayed acknowledgement on the receiver** (`TCP_DELACK_MAX_US`). The probe's allowance
  is the sender's, and it does not depend on what the receiver does.
- **A shorter wait in the application**, such as the replication RPC's 100 ms silence poll. The wait
  is the kernel's: the frame was still in its retransmission queue.

## Invariants to uphold

- **A lane of rounds sets its minimum RTO before its first round**, dialled or accepted. A new lane
  that carries rounds is named in `peer::lane_takes_rto_min`.
- **A client's connection holds the node's minimum until its first object operation**, which gives
  the kernel's back once. A connection that carries objects carries bulk.
- **A lane of bytes keeps the kernel's**, as does a client's connection once it has carried an
  object, since a short timeout restarts an idle connection's window.
- **A setting is zero or what the kernel takes**, refused at the start otherwise.
- **A frame is one segment**, so a lost one is found by what follows it or by the probe. On a lane of
  rounds, the probe's floor bounds a lone frame's tail; nothing else does.

## Still open

- ~~**A client's answers are lone segments too, and keep the kernel's probe.** A table read's answer
  across a full link waits up to 200 ms if it is dropped.~~ A client's connection holds the floor
  until it carries an object (above).
- **An object's answer, and a client's request, keep the kernel's probe.** A put's answer is one
  segment on a connection that carries objects, and `shoal-client` sets no minimum on its own end
  ([todos](../todos.md#a-clients-connections-take-a-minimum-rto)). ~~O131's put tail at one in
  flight is that: a floor on every connection halved it, and the fix as kept leaves it.~~ O131's put
  tail at one in flight is not this probe. Lab 3a (2026-10-24) found it is each connection's window
  restarting after an idle. A floor shortened it too, by lowering the delayed ACKs' cap, and the fix
  as kept leaves it ([O131](../optimizations.md#o131-a-puts-tail-at-one-in-flight-across-hosts-is-twice-as-long-under-tls)).
  Since [F112](../../features/object-congestion.md) (2026-10-25) the connections that carry objects
  ~~run~~ may run under BBR, which keeps their windows through an idle, and since
  [F113](../../features/slow-start-after-idle.md) every node's host keeps them by its own setting.
- **A kernel before 6.15 has no such option.** A node on one keeps the tail, and says so once.
- **The puts' stall seen on the first build** is filed as
  [item 305](../known-issues.md#305-a-captures-1-mib-puts-across-hosts-stalled-whole-and-its-check-could-not-read-10-objects),
  not yet reproduced and not yet known to be this fix's.

## Tests

| Test | What fails if the fix is reverted |
| --- | --- |
| `shoal` `cluster_fixture` `lanes_of_rounds_retransmit_on_the_minimum_rto` | A fixture node's control lane, or each of its replication lanes, retransmits on the kernel's 200 ms, or each lane of bytes takes the minimum. Told apart by their ports; a client's connection is `objects`'s row, since `ss` shows its timeout coming down a round trip at a time |
| `shoal` `objects` `a_connection_holds_the_minimum_rto_until_it_carries_an_object` | A client's connection to a node in this process does not hold the node's minimum before it carries an object, or does not hold the kernel's after; read through its descriptors |
| `shoal-core` `peer::tests::only_the_lanes_of_rounds_take_the_minimum_rto` | A lane other than replication and control takes the minimum, or one of them does not |
| `shoal-core` `peer::tests::rto_min_is_set_on_the_socket` | The option is not set, does not read back as set, or zero is not left to the kernel |
| `shoal-core` `conf::tests::a_minimum_rto_is_five_milliseconds_unless_named` | An unnamed minimum is not 5 ms, a named one changes, or one the kernel refuses is accepted |

## Related

- [F106. A read's bytes cross the object lane uncopied](../../features/lane-copies.md), whose vectored
  write made each frame one segment.
- [Resolved #181](partition-retransmit-backoff.md): `TCP_USER_TIMEOUT`, the other kernel timer every
  peer connection sets.
- [O131](../optimizations.md#o131-a-puts-tail-at-one-in-flight-across-hosts-is-twice-as-long-under-tls):
  a put's tail at one in flight under TLS, measured before F106, on frames of several segments.
- [Lab 2 of the store optimizations](../../object-storage/store-optimizations.md#the-rest-of-wave-two).
