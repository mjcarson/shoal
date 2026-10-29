# F112. BBR on Shoal's object sockets

## Context

A put at one in flight across hosts had a tail twice its median's distance: on the lab's SSD leg at
2+1, a p99 of 63 ms for 64 KiB and 81 ms for 1 MiB
([O131](../appendix/optimizations.md#o131-a-puts-tail-at-one-in-flight-across-hosts-is-twice-as-long-under-tls)).
Lab 3a found the cause on 2026-10-24, with every bench connection's kernel state sampled beside its
runs:
- **A connection that carries objects sits idle between puts**, past its retransmission timeout of
  about 200 ms: the client's connections, and the object lanes between nodes.
- **Linux restarts the congestion window of such a connection** while
  `net.ipv4.tcp_slow_start_after_idle` is 1, halving it for each timeout idle, down to ten
  segments. The holders' lanes had a median window of 10 under it, and 68 to 124 without it.
- **So each put's bytes went out in a window cut back to its start.** With that setting off on every
  host, the same build's puts at one in flight ran 1.08 to 1.12 times as fast, with p99s of 30.3 and
  50.7 ms. No other arm moved.

Linux skips that restart for a congestion control that sets its own window (`cong_control`), as BBR
does. On 2026-10-24 the user chose BBR on Shoal's object sockets. The alternative was a host setting
that changes every connection on a host.

## What it does

**Since [F113](slow-start-after-idle.md) (2026-10-25) BBR is a deployment's choice.** Every key below
is null, the host's own congestion control, unless named: lab 3b measured BBR slowing the gets across
hosts and resending 15 to 29 times the segments where they met at one link, and F113 keeps every
window through an idle with the host's setting instead. Named, everything below holds.

**An object lane runs under ~~BBR~~ the named congestion control at both ends**:
`cluster.transport.object_congestion`, ~~`bbr` unless named~~ the host's unless named.
- The dialing end sets `TCP_CONGESTION` after its connect, beside its keepalive and timeouts.
- The accepting end sets it once the hello says the lane is an object lane, before the lane is
  handed to its object executor.
- `peer::lane_takes_object_congestion` is the rule: the object lane alone.

**A client's connection runs under ~~BBR~~ the named congestion control at both ends once it carries
an object operation.**
- **The node's end:** `networking.object_congestion`, ~~`bbr` unless named~~ the host's unless named.
  The shard's client reader sets it at the connection's first object frame, where it gives back
  item 304's minimum RTO (`ObjectCongestion`, beside `ClientRto`).
- **The client's end:** `ShoalBuilder::object_congestion`, ~~`bbr` unless named~~ the host's unless
  named. An object attempt sets it on the connection it took, once (`ConnShared::carry_objects`).
- A connection that carries no object keeps the host's.

**A host that refuses it leaves the connection the host's**, said once a process at WARN and never
ending a connection (`sockets::set_object_congestion_of`). A process not running as root may take
only a congestion control whose module is loaded, `ENOENT` otherwise, and that its host allows in
`net.ipv4.tcp_allowed_congestion_control`, `EPERM` otherwise.

**shoaladm loads and allows it**, from an inventory's `object_congestion`: ~~`bbr` unless named, null
for each host's~~ each host's unless named.
- Each node's file names it in both sections only when ~~it is not `bbr`~~ the inventory names one.
- Each node's unit runs two steps as root before its node starts, neither required:
  `modprobe tcp_<name>`, then the name appended to the allowed list unless it is there.
- Preflight says when a host has no module for it, and deploys all the same.
- Every client shoaladm builds for a deployment names the same, the bench's included. An A/B side
  that moves it (`AB_<side>_CONGESTION` in `stores-lab.sh`) moves both ends.
- The wizard keeps it as an edited inventory named it, and has no field for it.

**A name is a short word**: lowercase letters, digits and `_`, at most fifteen of them. The engine
judges both keys at its start, a client at its build and shoaladm in the inventory, so a unit never
writes anything else into its command line.

## Design choices

- **BBR on Shoal's sockets, not a host's setting.** Turning `tcp_slow_start_after_idle` off on the
  hosts is what side e measured, and it changes every connection on them: Shoal's lanes of rounds,
  its tables' clients and every other program's. BBR is asked for on the sockets whose idles were
  measured, and nothing else moves. Lab 3b turned this choice round: BBR's losses reached the lanes
  of rounds anyway, and cost the gets more than the host's setting cost anything, so
  [F113](slow-start-after-idle.md) makes the host's setting the default.
- **Both ends.** A congestion control governs what its own end sends. A put's bytes leave the client
  and a coordinator's lanes; a get's leave its holders' lanes and the node's end of the client's
  connection.
- **Only once a connection carries an object**, as item 304's minimum RTO is given back there. A
  table's connection carries lone frames, which no window holds back, and is left as it was.
- **The object lane alone among the lanes.** The lanes of rounds carry small frames, and the data and
  bulk lanes carry what lab 3a did not measure.
- **Neither the module nor the allowance is required.** A host without either runs its node under
  its own congestion control, which is correct and said in the node's log and at preflight.
  Refusing to start would make a kernel module a requirement of every deployment.
- **The allowed list is appended to, never written whole.** A write replaces the list, which would
  take away the host's own allowances, reno's included.
- ~~**On by default, in the engine.** The engine's default rather than shoaladm's, so a node started
  by hand on a host that allows BBR takes it too.~~ **Off by default since
  [F113](slow-start-after-idle.md)**, in the engine, a client and shoaladm alike.
- **The minimum RTO is unchanged.** F112 changes the window, not the probe. Whether a floor on these
  connections helps once their windows survive an idle is
  [a todo's](../appendix/todos.md#a-clients-connections-take-a-minimum-rto) measurement.

## Alternatives rejected

- ~~**`net.ipv4.tcp_slow_start_after_idle=0`, written by shoaladm to `/etc/sysctl.d`.** Measured, as
  side e, and host-wide: every connection on the host keeps its window through an idle, whatever
  program holds it.~~ Chosen after lab 3b, as [F113](slow-start-after-idle.md).
- **BBR as each host's default congestion control**, as lab 3a's side c ran it. It is host-wide too,
  and changes every connection's congestion control.
- **Documentation alone**, the tail left and its cause written down.
- **`CAP_NET_ADMIN` for the node** rather than an allowance. A node would hold a capability for one
  socket option, and with it the kernel loads a module by any name a socket asks for.
- **Every lane and every client connection.** Lab 3a measured the connections that carry objects; a
  table's frames and a group's rounds are single segments that no window holds back.

## Limitations

- ~~**Not measured alone yet.** Lab 3a ran BBR only as each host's default with a 5 ms minimum RTO
  beside it (side c), and the minimum alone halved this tail in item 304's first build. Lab 3b
  measures F112.~~ **Measured in lab 3b: across hosts it slows the gets and resends 15 to 29 times
  the segments where they meet at one link** ([Performance](#performance)). Linux's BBR does not
  slow down for loss, so flows that meet at one port keep sending past what its buffer holds. Off
  by default since [F113](slow-start-after-idle.md).
- **A client on a host that does not allow BBR keeps its host's congestion control.** A node's unit
  allows it on node hosts only. The lab's bench runs on europa, which runs a node.
- **BBR paces what it sends.** Beside cubic's flows on a shared link it takes another share than
  cubic would, and on loopback its pacing costs cpu: the loop leg's three nodes share europa's.
  ~~Neither is measured yet.~~ On loopback lab 3b found no cost in cpu, and a 64 KiB put's p99 at one
  in flight 1.3 ms longer ([Performance](#performance)).
- **The allowance is made again at every start**, not persisted. A host rebooted allows nothing
  until a node's unit starts on it.
- **Neither the module nor the allowance is undone by `destroy`**, as the `tls` module is not.
- **The data and bulk lanes keep the host's**, and a table's connection too.

## Invariants to uphold

- **A congestion control's name is judged before it is written anywhere**: at the engine's start,
  a client's build and in the inventory. A unit writes it into a command line it runs as root.
- **A refusal never ends a connection** and is said once a process.
- **The descriptor is held open by the connection's own halves**: a client's by its write half in
  `ConnShared`, a node's by its relays or its lane. Never a number kept past its socket, which the
  kernel may give to another.
- **Only the object lane, and a client's connection once it carries an object, take it.**
- **The allowed list is appended to, never written whole.**
- **The congestion control and the minimum RTO are set apart.** F112 never moves item 304's floor.

## Performance

Lab 3b measured it on 2026-10-25, on lab 3a's arms: the SSD leg at 2+1, puts and gets of 64 KiB and
1 MiB at one and sixteen in flight, F112 beside the build before it (F111, `3a12aeee`), with the
sampler beside each capture (`stores-sample.sh`). F112 was to work if the puts' p99 at one in flight
fell toward side e's (30 to 31 ms at 64 KiB, 49 to 51 at 1 MiB) and their rates rose, while no get
and no arm at sixteen in flight got worse. **The puts did, and the gets got worse**, so F112 failed
its own check, and [F113](slow-start-after-idle.md) made it opt-in.

**On the SSD leg at 2+1** (`w3-ssd-ec21-w3b`: before, F111, F112, F112, F111, before; four runs a
side, operations a second as median [lowest–highest], every host at kernel 7.0.0-34, europa the
bench's host; `stores-ab-report.py`):

| Arm | F111 | F112 | ÷ | p99 F111 → F112, ms |
| --- | --- | --- | --- | --- |
| put 64 KiB, 1 in flight | 41.6 [40.9–42.8] | 44.8 [44.5–45.5] | 1.08, apart | 62.2 → 34.8, apart |
| put 1 MiB, 1 in flight | 26.7 [26.5–26.9] | 27.7 [27.6–28.0] | 1.04, apart | 79.6 → 49.5, apart |
| put 64 KiB, 16 in flight | 303 [303–305] | 303 [303–304] | 1.00 | 72.6 → 72.2 |
| put 1 MiB, 16 in flight | 95.0 [94.0–95.6] | 96.0 [95.9–97.2] | 1.01, apart | 372 → 380 |
| get 64 KiB, 1 in flight | 540 [509–582] | 483 [480–498] | 0.89, apart | 3.25 → 4.55 |
| get 1 MiB, 1 in flight | 83.7 [81.7–85.6] | 77.4 [74.6–79.5] | 0.92, apart | 22.0 → 23.5 |
| get 64 KiB, 16 in flight | 2,081 [2,025–2,146] | 2,014 [1,953–2,068] | 0.97 | 13.5 → 14.5, apart |
| get 1 MiB, 16 in flight | 137 [135–138] | 128 [124–130] | 0.94, apart | 444 → 818, apart |

**The retransmissions** (`stores-ab-sockets.py`, every bench connection's state sampled each two
seconds, each capture's runs of the 1 MiB gets at sixteen in flight):

| Connections | F111, two captures | F112, two captures |
| --- | --- | --- |
| a node's end of the bench's connections, titan and hyperion | 12,247 to 16,153 | 272,069 to 411,649 |
| lanes of bytes, each host | 1,006 to 6,737 | 20,007 to 173,585 |
| lanes of rounds, each host | 472 to 976 | 9,987 to 15,095 |
| control lanes, each host | 18 to 44 | 341 to 526 |
| retransmission timeouts, europa and hyperion | 3 to 14 | 1,845 to 3,119 |

Hyperion's ends of the bench's connections resent 575 MB in one capture's runs, where F111's resent
21 MB. Every get's bytes in that arm meet at europa's 1 GbE port, where the bench reads them, and
Linux's BBR does not slow down for loss: each connection keeps sending at the bandwidth it measured,
and the switch drops what its buffer cannot hold. The lanes of rounds, which kept cubic, lost
segments beside them. The puts at sixteen in flight, whose bytes leave europa, resent 3.5 to 11 times
as many at the same rate. Lab 3a's side c, BBR as every host's default, had resent as many in the
same arm (`w3-o131-sockets.txt`, 299,367 to 456,523 segments a host), which its write-up, reading the
puts at one in flight, did not report.

**On europa's loopback** (`w3-loop-ec21`, `w3-loop-rep3`, every task's cpu by `pidstat`): every rate
within F111's, at 0.95 to 1.03, and the nodes' cpu a GiB 0.96 to 1.04 of F111's on every arm. A 64 KiB
put's p99 at one in flight was 1.3 ms longer, 3.87 and 3.98 ms where it was 2.55 and 2.60, every run
apart: BBR paces even what loopback could take at once.

**F113 took its place as the default** (lab 3c, 2026-10-25). Every node's host keeping its windows
through an idle gained the puts at one in flight as F112 had, 1.11 and 1.07 times as fast at 64 KiB
and 1 MiB with p99s of 30.4 and 48.9 ms. Every get and every arm at sixteen in flight stayed within
1% of F111's, and every kind of connection resent what F111's did
([F113's figures](slow-start-after-idle.md#performance)).

## Tests

| Test | What fails if the feature is reverted |
| --- | --- |
| `shoal-proto` `shared::sockets::tests::a_connection_runs_under_the_congestion_control_it_names` | The option is not set or not read back, or a name the host has no module for changes the connection |
| `shoal-proto` `shared::sockets::tests::a_congestion_controls_name_is_a_short_word` | A name a unit could write as more than a word is taken |
| `shoal-core` `server::peer::tests::only_the_object_lane_takes_the_object_congestion_control` | Another lane takes it, or the object lane does not |
| `shoal-core` ~~`server::conf::tests::an_object_connections_congestion_control_is_bbr_unless_named`~~ `server::conf::tests::an_object_connections_congestion_control_is_the_hosts_unless_named` (since F113) | Either key ~~is not `bbr` unless named, null is not the host's~~ takes one unless named, or a name no kernel gives is taken |
| `shoal` `objects` `a_connection_takes_its_congestion_control_once_it_carries_an_object` | Either end of a client's connection that carried an object is not under the named control, one that carried none is changed, or a client naming more than a word is built |
| `object_lanes_run_under_the_named_congestion_control` (fixture) | An object lane's dialing or accepting end, or a node's end of a client's connection that carried an object, is not under the named control, or a lane of rounds or a replication lane leaves the host's |
| `shoaladm` ~~`deploy::inventory::tests::object_congestion_is_bbr_unless_named`~~ `deploy::inventory::tests::object_congestion_is_the_hosts_unless_named` (since F113) | An inventory's key ~~is not `bbr` unless named~~ names one unless named, is not written back as read, or takes a name that is not a word |
| `shoaladm` ~~`deploy::render::tests::object_congestion_renders_only_when_it_is_not_bbr`~~ `deploy::render::tests::object_congestion_renders_only_when_named` (since F113) | A node's file names ~~the engine's own, or not null and a name~~ one the inventory left the host's, or not a named one in both sections |
| `shoaladm` `deploy::unit::tests::the_unit_runs_the_deployed_program` | A default unit ~~does not load and allow `bbr` before its node, or requires either~~ loads and allows a congestion control (since F113) |
| `shoaladm` `deploy::unit::tests::a_unit_loads_the_congestion_control_its_deployment_names` | A unit loads one its deployment left the host's, or not the one it names, `bbr` included, or requires either |
| `shoal-bench` `deploy_render` `a_rendered_object_congestion_reaches_the_engine` | The engine's loader reads a rendered key otherwise than the inventory named it, ~~null~~ an unnamed one included |

## Related

- [F113](slow-start-after-idle.md): the host's setting that keeps every window through an idle, the
  default since lab 3b measured this feature.
- [O131](../appendix/optimizations.md#o131-a-puts-tail-at-one-in-flight-across-hosts-is-twice-as-long-under-tls),
  the finding it builds, with lab 3a's figures.
- [Resolved #304](../appendix/resolved/lone-frame-loss-probe.md): the minimum RTO, which a client's
  connection gives back at the same object frame.
- [A client's connections take a minimum RTO](../appendix/todos.md#a-clients-connections-take-a-minimum-rto),
  the measurement F112 makes possible.
- [F103](plaintext-peer-lanes.md): an inventory's key that deploys differently, and the kernel
  module a unit loads before its node.
- [Resolved #310](../appendix/resolved/peer-connections-keepalive.md): the socket options both ends
  set alike, beside which F112's helpers live.
