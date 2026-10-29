# F113. Idle connections keep their window, on every node's host

## Context

A put at one in flight across hosts had a tail twice as far from its median as it should be: on the
lab's SSD leg at 2+1, a p99 of 63 ms for 64 KiB and 81 ms for 1 MiB
([O131](../appendix/optimizations.md#o131-a-puts-tail-at-one-in-flight-across-hosts-is-twice-as-long-under-tls)).
Lab 3a found the cause on 2026-10-24:
- the connections that carry objects sit idle between puts, past their retransmission timeout;
- while `net.ipv4.tcp_slow_start_after_idle` is 1, its default, Linux restarts such a connection's
  congestion window, down to ten segments;
- so each put's bytes went out in a window cut back to its start.

With the setting off on every host (lab 3a's side e), the same build's puts at one in flight ran
1.08 to 1.12 times as fast with p99s of 30.3 and 50.7 ms, and no other arm moved.

The user first chose BBR on Shoal's object sockets ([F112](object-congestion.md)), which keeps its
window through an idle and changes no other connection. Lab 3b measured it on 2026-10-25, F112
beside F111 on the same arms:
- **The puts gained what side e's had.** The 64 KiB puts at one in flight ran 1.08 times as fast
  with a p99 of 34.8 ms where it was 62.2, and the 1 MiB puts 1.04 times with 49.5 where it was 79.6.
- **The gets lost.** Three of the four get arms ran 0.89 to 0.94 times as fast, every run apart. The
  1 MiB gets at sixteen in flight had a p99 of 818 ms where it was 444.
- **Retransmissions rose 15 to 29 times.** The nodes' ends of the bench's connections resent 272,069
  to 411,649 segments in a capture of those 1 MiB gets, where F111's resent 12,247 to 16,153; the
  lanes of bytes 20,007 to 173,585, where they resent 1,006 to 6,737.
- **The other lanes lost segments beside them.** The lanes of rounds resent 15 to 21 times as many,
  and the control lanes 11 to 27 times. Puts at sixteen in flight resent 3.5 to 11 times as many,
  at the same rate.

Every get's bytes in that arm meet at one 1 GbE port, europa's, where the bench reads them. Linux's
BBR does not slow down for loss: each connection goes on sending at the bandwidth it measured, and
the switch drops what its buffer cannot hold. Lab 3a's side c, BBR as every host's default, had
resent as many in the same arm (`w3-o131-sockets.txt`), which its write-up, reading the puts at one
in flight, did not report. Side e had resent no more than side a.

So on 2026-10-25 the user chose the host setting lab 3a measured: shoaladm turns
`tcp_slow_start_after_idle` off on every node's host, and F112's BBR is a deployment's choice.

## What it does

**An inventory's `slow_start_after_idle`** says whether a connection on every node's host starts
again from a small window after an idle, as the kernel's setting does:
- `false` unless named: shoaladm sets `net.ipv4.tcp_slow_start_after_idle = 0`;
- `true` sets it to 1;
- `null` leaves each host's own.

**The setting is kept in a file of the node's unit**, `/etc/sysctl.d/60-<unit>.conf` (every unit's
name starts `shoal-`), which systemd applies at every boot.
- `start_unit` writes it as root and applies it through `/proc/sys` before it writes and starts the
  node's unit: on bootstrap, `deploy`, `add` and a rebuild.
- The file keeps the host's own value from before any Shoal unit set it (`# before: 1`), carried
  from any Shoal unit's file already there.
- A deployment says what it did, a node a line:
  `[a] net.ipv4.tcp_slow_start_after_idle is 0, where the host's own is 1`.

**`destroy` takes it back.** It removes each unit's file and, once no other Shoal unit's file is
there, writes back the host's own value the file kept. It does so whatever the inventory names now.
A deploy whose inventory names null does the same for its unit's file, so a deployment moved to the
host's own leaves nothing behind.

**F112's BBR is opt-in.** Every key that named it defaults to null, the host's own:
- the inventory's `object_congestion`;
- the engine's `networking.object_congestion` and `cluster.transport.object_congestion`;
- the client's `ShoalBuilder::object_congestion`.

A node's file names a congestion control, and its unit loads and allows one, only when the inventory
names one. Named, everything F112 does is unchanged.

**The lab's runs log it.** In `stores-lab.sh` an A/B side moves it with `AB_<side>_SLOWSTART`, and
each capture logs every host's value before and after it, which the bench's own `destroy` puts back.

## Design choices

- **The host's setting, on the figures.** Lab 3a's side e and lab 3b's F112 gained the puts alike;
  only BBR made the gets slower and resent their segments. A window kept through an idle is what the
  connection had before it. The kernel's default exists for a path whose capacity may have moved
  during a long idle, and the lab's do not.
- **A file of each unit, not one a host.** Several clusters' nodes share hosts: the lab's tmdb
  cluster sits beside every bench's. Each unit's file is written and removed with its unit, and each
  carries the host's own value, so the last removal puts it back whatever order the units leave in.
- **In `/etc/sysctl.d`, not the unit's `ExecStartPre`**, where F112's allowance is. A file holds
  through a reboot before any node starts, and `destroy` can take it back; a step at every start is
  never undone.
- **Applied at once**, not left for the next boot, since the node starts right after.
- **The deployment's default, not the engine's.** The engine cannot change a host's setting, and a
  node started by hand keeps its host's.
- **No glob in the scripts.** A host may log ssh in to zsh, which refuses a glob that matches
  nothing, so the scripts list the directory with `find`.
- **BBR kept as a setting.** It is built and tested, and a deployment whose object flows do not meet
  at one link may want what it gives without changing a host.

## Alternatives rejected

- **BBR by default, F112 as built.** Measured in lab 3b: it slowed the gets 6 to 11%, doubled the
  1 MiB gets' p99 at sixteen in flight, and resent 15 to 29 times the segments where the gets met at
  one link. Its loss reached the lanes of rounds beside them.
- **BBR only where a put's bytes leave**: a client's end and a coordinator's dialing end of a lane.
  Under BBR the puts at sixteen in flight resent 3.5 to 11 times as many too, and a put's chunks meet
  at their holders' links.
- **A switch on each socket.** Linux has none: the restart is the network namespace's setting, and
  only a congestion control with a `cong_control` of its own skips it.
- **The unit's `ExecStartPre`**, as F112's allowance is made. It is never undone, and is not there
  after a reboot until a node starts.
- **Documentation alone**: the tail left and its cause written down.

## Limitations

- **It is the host's.** Every connection on a node's host takes it, every program's. A host that
  serves several clusters keeps it while any Shoal unit's file is there.
- **A client on a host no node shares keeps its host's own.** A put's bytes leave the client's end,
  whose window then restarts after an idle, and no socket option can say otherwise. The lab's bench
  runs on europa, which runs a node.
- **A cluster deployed before F113 and upgraded in place is not changed.** An upgrade installs a
  program and restarts its node, and never rewrites the unit. A deploy writes the file.
- **Two units naming other values** leave the host at the last one written. Once one is removed,
  nothing applies the other's again until its next deploy or a boot.
- **A host's own file that sorts after `60-shoal-*`** and names the setting decides it at boot.
- **The engine has no say.** A node started by hand, or by another tool, keeps its host's setting.

## Invariants to uphold

- **A unit's file keeps the host's own value from before any Shoal unit set it**, carried from the
  first Shoal unit's file into every later one, and never a value a Shoal unit set.
- **The host's own value is written back only once no Shoal unit's file remains.**
- **`destroy` takes back what a unit set, whatever the inventory names now.**
- **No glob in a script run over ssh.** zsh refuses one that matches nothing.
- **BBR is never a default again without a capture of its retransmissions** beside the gets at
  sixteen in flight across hosts. The rates alone hid lab 3a's.

## Performance

**On the SSD leg at 2+1** (lab 3c, 2026-10-25, `w3-ssd-ec21-f113`), lab 3a's arms ran with F113
(257592b3) beside F111 (3a12aeee), in the order F111, F113, F113, F111:
- puts and gets of 64 KiB and 1 MiB, at one and sixteen in flight, two runs each;
- every bench connection's state and every host's TCP counters sampled each two seconds beside them;
- europa on kernel 7.0.0-38 since its reboot that afternoon, titan and hyperion on 7.0.0-34, for both
  sides alike.

F113 was to work if the puts' p99 at one in flight fell toward side e's (30 to 31 ms at 64 KiB, 49 to
51 at 1 MiB) and their rates rose, while every get and every arm at sixteen in flight stayed within
F111's, and the retransmissions with them. **It did.** Operations a second, the median of every run
[lowest–highest], and p99:

| Arm | F111 | F113 | F113 ÷ F111 | p99, ms |
| --- | --- | --- | --- | --- |
| put 64 KiB, 1 in flight | 41.2 [39.7–41.7] | 45.9 [45.6–46.3] | 1.11, apart | 62.7 → 30.4, apart |
| put 1 MiB, 1 in flight | 26.6 [26.1–26.9] | 28.5 [28.3–28.8] | 1.07, apart | 80.2 → 48.9, apart |
| put 64 KiB, 16 in flight | 302 [301–302] | 305 [304–306] | 1.01, apart | 75.3 → 72.2, within |
| put 1 MiB, 16 in flight | 93.9 [92.5–94.7] | 94.3 [93.9–94.5] | 1.00, within | 379 → 369, within |
| get 64 KiB, 1 in flight | 553 [536–582] | 547 [528–567] | 0.99, within | 3.11 → 3.08, within |
| get 64 KiB, 16 in flight | 2,137 [2,117–2,153] | 2,121 [2,112–2,124] | 0.99, within | 13.4 → 13.4, within |
| get 1 MiB, 1 in flight | 82.0 [79.4–87.2] | 82.5 [80.5–85.7] | 1.01, within | 21.8 → 22.2, within |
| get 1 MiB, 16 in flight | 135 [134–136] | 136 [132–139] | 1.01, within | 431 → 435, within |

- **The puts at one in flight ran as lab 3a's side e had**: 45.4 to 45.6 a second with a p99 of 30.2
  to 31.4 ms at 64 KiB, and 27.7 to 28.0 with 49.4 to 51.3 at 1 MiB. Their p50 moved by 3 ms or
  less, so the gain is the tail's.
- **The windows held through the idles.** The lanes of bytes on titan and hyperion carry a put's
  chunks to their holders. In the 64 KiB puts at one in flight they had a median window of 10 to 14
  segments with F111 and 66 to 92 with F113; in the 1 MiB puts, 10 with F111 (62 in one of titan's
  captures) and 140 to 167 with F113 (`stores-ab-idle.py`). Those two hosts counted 95 to 124 loss
  probes in a capture's runs of the 64 KiB puts with F111, and 0 to 2 with F113.
- **The setting was F113's alone.** Each F113 capture's deploy logged every host's value at 0 where
  its own was 1, and its `destroy` put back 1. Every host read 1 before and after every capture,
  F111's too.
- **Nothing was held or lost.** All 64 runs measured, and every check read 2,048 objects back. No
  lane refused a request, no budget shed a draw, no shard refused a task and no put was given up.

**The retransmissions were F111's**, in the arm where F112's resent 15 to 29 times as many: the 1 MiB
gets at sixteen in flight, each capture's runs (`stores-ab-sockets.py`).

| Connections | F111, two captures | F113, two captures |
| --- | --- | --- |
| a node's end of the bench's connections, titan and hyperion | 8,923 to 18,438 | 11,448 to 15,778 |
| lanes of bytes, each host | 1,037 to 7,404 | 923 to 7,230 |
| lanes of rounds, each host | 459 to 953 | 443 to 855 |
| control lanes, each host | 5 to 37 | 9 to 49 |
| retransmission timeouts, europa and hyperion | 6 to 24 | 3 to 12 |

The 1 MiB puts at sixteen in flight resent as many on both sides: europa's lanes of bytes 7,993 to
11,988 segments with F111 and 9,525 to 9,983 with F113. The samples' summaries are
`shoal-spike/results/stores-ab/w3-f113-sockets.txt` and `w3-f113-idle.txt`.

## Tests

| Test | What fails if the feature is reverted |
| --- | --- |
| `shoaladm` `deploy::inventory::tests::slow_start_after_idle_is_off_unless_named` | An inventory's hosts are not set to keep their windows unless it names otherwise, or null is not the host's, or the key is not written back as read |
| `shoaladm` `deploy::ops::tests::idle_connections_keep_their_window_until_the_last_unit_is_removed` | A unit's file does not keep the host's own value, carries a Shoal unit's instead, puts it back while another unit's file remains or not once the last is gone, or the scripts fail under sh or zsh |
| `shoaladm` `deploy::inventory::tests::object_congestion_is_the_hosts_unless_named` | An inventory's object connections take BBR unless named |
| `shoaladm` `deploy::render::tests::object_congestion_renders_only_when_named` | A node's file names a congestion control the inventory left the host's |
| `shoaladm` `deploy::unit::tests::the_unit_runs_the_deployed_program` | A default unit loads and allows a congestion control |
| `shoal-core` `server::conf::tests::an_object_connections_congestion_control_is_the_hosts_unless_named` | Either engine key takes BBR unless named |
| `shoal-bench` `deploy_render` `a_rendered_object_congestion_reaches_the_engine` | The engine's loader reads a congestion control from a file that names none |

## Related

- [O131](../appendix/optimizations.md#o131-a-puts-tail-at-one-in-flight-across-hosts-is-twice-as-long-under-tls),
  the finding it fixes, with lab 3a's figures.
- [F112](object-congestion.md): BBR on Shoal's object sockets, measured in lab 3b and made a
  deployment's choice here.
- [Resolved #304](../appendix/resolved/lone-frame-loss-probe.md): the minimum RTO of the lanes of
  rounds, and of a client's connection until it carries an object.
- [A client's connections take a minimum RTO](../appendix/todos.md#a-clients-connections-take-a-minimum-rto),
  the measurement a window kept through an idle makes possible.
- [F91](rotational-devices.md): the other host setting shoaladm makes and keeps through a boot, a
  disk's write cache.
