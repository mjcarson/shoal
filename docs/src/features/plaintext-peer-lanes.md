# F103. Plaintext peer lanes, a deployment's choice

Since this feature an inventory may name `plaintext_lanes: true`. Every node it deploys then runs
its peer lanes without TLS:
- shoaladm renders no `cluster.tls` for any node;
- it issues no leaf and mints no authority;
- no node loads the kernel's `tls` module.

The setting is off unless named, so a deployment's lanes run mutual TLS as they always did. It is
chosen when the cluster is bootstrapped and kept for the cluster's life.

## Context

**What O104's diagnosis found** (2026-10-22): a 2+1 get on one host spent a third of its object
executor's time in the lane's TCP and kTLS, kTLS alone 15%
([where the executor's time goes](../object-storage/store-optimizations/object-lane.md#where-the-executors-time-goes)).

**The comparison wasn't like for like.** It deployed Ceph, MinIO, RustFS and SeaweedFS without
encryption between their nodes, and Shoal with it. shoaladm rendered `cluster.tls` for every node,
so no inventory could say otherwise
([the rest of encryption](../appendix/todos.md#the-rest-of-encryption)).

**The engine already ran plaintext.** `cluster.tls` is optional, and a node without it runs every
lane plaintext. The fixture's clusters run that way unless `.peer_tls()`
([F50](cluster-operations.md)).

On 2026-10-22 the user asked for a setting, off by default.

## What it does

- **The key.** `plaintext_lanes: bool` at an inventory's top level. It is false unless named, and an
  inventory written back names it only when set.
- **The rendered file.** A node's `shoal.yml` has no `cluster.tls`. `ClusterConf.tls` is an `Option`.
- **Bootstrap, add and rebuild.**
  - No authority is minted and no leaf issued (`provision_tls`).
  - No `tls` directory is made.
  - The host check doesn't ask for the kernel's `tls` module.
- **The unit** runs no `modprobe tls` before the node.
- **Fixed at bootstrap.**
  - The deployment's record keeps `plaintext_lanes` (`ClusterRecord`).
  - `upgrade`, `reconfigure`, `add` and `rebuild` refuse, by name, an inventory naming the other
    kind (`ClusterRecord::judge_lanes`).
  - A record written before F103 is one of mutual TLS.
- **The engine** is unchanged, but for one warning at a cluster node's start when its lanes are
  plaintext.
- **The wizard** keeps the key of an inventory it edits. It has no field for it yet.
- **On the lab**, `stores-lab.sh ab` takes a side's `AB_<side>_PLAIN=true`.

## Design choices

- **The deployment's key, not the node's.** The engine's rule stays: no `cluster.tls` means
  plaintext lanes. A key that a node's `shoal.yml` must set to run plaintext would move every test
  configuration and the bench's cluster workloads, which run plaintext today. It would say
  nothing that shoaladm doesn't already.
- **Every lane, not the object lane alone.**
  - The data port serves the data, bulk, replication and object lanes on one listener.
  - TLS is set up before the hello says which lane a connection is.
  - So a lane-by-lane choice needs an acceptor that peeks at a connection's first byte, which F103
    doesn't build.
- **Fixed at bootstrap, because the two kinds never connect.**
  - A TLS node reads a plaintext hello as a TLS record.
  - A plaintext node reads a TLS ClientHello's first byte, 22, as a wire version past its own.
  - Neither forms a lane, and each redials forever.

  So no node-by-node change exists, and a command refuses rather than start one.
- **One warning, at start.** Nothing else reports that a node's lanes are plaintext.

## Alternatives rejected

- **A transition mode:** an acceptor that peeks for `0x16` and serves either kind, so a cluster
  could roll from one to the other. Filed, not built. It works only while the wire version stays
  below 22, and a switch is rare enough to be a new cluster.
- **Plaintext for the object lane alone**, the lane O104 measured: see the design choices.
- **`reconfigure` stopping every node to switch.** A whole cluster stopped is an outage. An operator
  chooses it by destroying and bootstrapping, the data carried by a backup and a restore.
- **Plaintext by default.** The user chose off by default: plaintext gives up both a peer's identity
  and every byte's confidentiality.

## Limitations

What a plaintext deployment gives up:
- **A peer's identity is its claim.** Its hello names a node and a cluster, and nothing checks them:
  a plaintext peer is taken at its word.
- **Anything that reaches a seed's control port can ask to join**, and a joiner is admitted
  automatically ([membership](../distributed/membership.md)).
- **A bundle forwarded by a peer is checked against no client's grants.** A node trusts what its
  peers forward, and without TLS anything that dials its data port can be a peer.
- **The cluster's and nodes' ids are told to any dialer**, in the hello's answer, refused or not.
- **Every byte between nodes travels in the clear**: rows, object bytes, snapshots and consensus
  traffic.

And besides:
- **A switch between the two kinds needs a new cluster.**
- **Clients are a separate setting.** `networking.tls` is the client listener's, and shoaladm renders
  none. This key changes nothing of it.
- ~~**Not yet measured on the lab.** Its run beside TLS follows.~~ Measured on 2026-10-23
  ([Performance](#performance)).

## Invariants to uphold

- **Every node of a deployment runs one kind of lane.** No command renders, upgrades or adds a node
  of the other kind than its record's.
- **Plaintext only when named.** Rendering, the PKI, the host check and the unit file all read the
  inventory's key. Absent, they do as they always did.
- **The engine's rule is unchanged.** `cluster.tls` absent means plaintext lanes. TLS is never
  inferred from anything else.

## Performance

Measured on the lab on 2026-10-23 (`f103-loop-ec21` and `f103-ssd-ec21`, under
`shoal-spike/results/stores-ab/`), at 2+1 on two legs:
- europa's loopback, its three nodes on one host;
- the SSD leg, a node on each of europa, titan and hyperion, over 1 GbE.

How it was run:
- **One build**, F104's commit (`628c3681`), which carries F103. Its node was the same binary in
  every capture. The sides differ in the inventory's key alone:
  - *TLS*, the key unnamed;
  - *plaintext*, `plaintext_lanes: true` (`AB_plain_PLAIN=true`).
- **The arms** are the comparison's gets and puts at 64 KiB and 1 MiB, one in flight and sixteen.
- **The order of the captures** was TLS, plaintext, plaintext, TLS, each on devices made fresh.
- **A side's figure** is the median of its eight runs, with the lowest and highest.
- **On the loopback each node's threads were sampled** every second, as O104's diagnosis did, and
  read over each run's measured window (`f103-threads.txt`, and `f103-cpu.txt` by
  `stores-ab-cpu.py`, beside the captures).
- **Nothing was lost.** The check at each capture's end read 2,048 objects back.

### On europa's loopback

The nodes share one host's cpus, so the lanes' encryption takes cpu the nodes would serve with.
Operations a second:

| Arm | Size | In flight | TLS | Plaintext | Plaintext ÷ TLS | p50 ms | p99 ms |
| --- | --- | --- | --- | --- | --- | --- | --- |
| Get | 64 KiB | 1 | 1,670 [1,648–1,696] | 1,890 [1,866–1,968] | **1.13, ahead** | 0.60 → 0.52 | 0.80 → 0.69 |
| | | 16 | 13,182 [12,767–13,388] | 16,383 [16,018–16,739] | **1.24, ahead** | 1.19 → 0.95 | 1.85 → 1.52 |
| | 1 MiB | 1 | 433 [420–439] | 474 [467–482] | **1.09, ahead** | 2.37 → 2.09 | 2.79 → 2.49 |
| | | 16 | 1,793 [1,772–1,819] | 2,010 [1,993–2,036] | **1.12, ahead** | 8.79 → 7.80 | 14.6 → 12.9 |
| Put | 64 KiB | 1 | 436 [386–442] | 477 [438–481] | 1.09, within | 2.28 → 2.07 | 2.69 → 2.50 |
| | | 16 | 2,403 [2,339–2,514] | 2,721 [2,703–2,774] | **1.13, ahead** | 6.63 → 5.80 | 10.1 → 9.25 |
| | 1 MiB | 1 | 128 [126–132] | 145 [144–147] | **1.13, ahead** | 7.64 → 6.77 | 12.1 → 11.5 |
| | | 16 | 534 [531–542] | 611 [596–614] | **1.14, ahead** | 30.1 → 26.3 | 36.5 → 32.5 |

The cpu a GiB moved cost, in cpu-seconds, TLS → plaintext, with the ratio:

| Arm | Size | In flight | Nodes | Object executors | Shards |
| --- | --- | --- | --- | --- | --- |
| Get | 64 KiB | 1 | 17.9 → 15.4 (0.86) | 3.90 → 3.34 (0.86) | 13.6 → 11.7 (0.86) |
| | | 16 | 11.7 → 9.38 (0.80) | 2.70 → 2.08 (0.77) | 8.93 → 7.23 (0.81) |
| | 1 MiB | 1 | 2.90 → 2.42 (0.83) | 1.36 → 1.06 (0.78) | 1.50 → 1.31 (0.88) |
| | | 16 | 3.00 → 2.67 (0.89) | 1.37 → 1.05 (0.77) | 1.59 → 1.59 (1.00) |
| Put | 64 KiB | 1 | 78.3 → 69.7 (0.89) | 15.1 → 13.3 (0.88) | 54.4 → 47.8 (0.88) |
| | | 16 | 54.1 → 47.8 (0.89) | 11.4 → 10.1 (0.89) | 37.4 → 32.5 (0.87) |
| | 1 MiB | 1 | 57.4 → 47.5 (0.83) | 12.1 → 10.2 (0.84) | 41.6 → 34.0 (0.82) |
| | | 16 | 7.12 → 6.32 (0.89) | 2.95 → 2.27 (0.77) | 3.73 → 3.61 (0.97) |

What this says:
- **Every arm ran faster plaintext**, by 9 to 24%, seven of the eight with their ranges apart. The
  64 KiB puts at one in flight overlap.
- **The nodes spent 11 to 20% less cpu a GiB.** The object executors, which carry the object lane,
  spent 11 to 23% less. O104's diagnosis put kTLS at 15% of their time in 2+1 gets
  ([where the executor's time goes](../object-storage/store-optimizations/object-lane.md#where-the-executors-time-goes)).
- **At sixteen in flight the gets' nodes used as much cpu on either side**, 9.42 and 9.44 cpus at
  64 KiB. Bound by their threads, they turned what plaintext saved into gets.
- **The 1 MiB puts at one in flight ran beside the reclamation of the arm before them**, on both
  sides, so their cpu a GiB counts that work too
  ([Resolved #301](../appendix/resolved/a-settle-waits-for-reclamation.md), fixed since). With the
  settle fixed, the same build ran them at 219 a second with TLS, not 128, at 8.0 cpu-seconds a
  GiB, not 57.4 (lab 2's `w2-loop-ec21`, its *before* side). Their row says more about the
  reclamation than about the lanes.

### On the SSD leg

Each node reaches the others over 1 GbE, and the bench driver is on europa. Operations a second:

| Arm | Size | In flight | TLS | Plaintext | Plaintext ÷ TLS | p50 ms | p99 ms |
| --- | --- | --- | --- | --- | --- | --- | --- |
| Get | 64 KiB | 1 | 428 [416–432] | 430 [419–445] | 1.01, within | 2.32 → 2.30 | 4.15 → 3.86 |
| | | 16 | 2,065 [2,055–2,074] | 1,993 [1,947–2,039] | **0.96, behind** | 7.77 → 8.09 | 13.8 → 14.2 |
| | 1 MiB | 1 | 63.4 [61.5–65.0] | 61.7 [59.6–62.3] | 0.97, within | 14.9 → 14.8 | 26.7 → 26.3 |
| | | 16 | 133 [132–134] | 127 [127–129] | **0.95, behind** | 90.6 → 93.9 | 406 → 438 |
| Put | 64 KiB | 1 | 39.6 [38.5–40.5] | 45.1 [44.5–45.5] | **1.14, ahead** | 21.9 → 21.7 | 63.8 → 30.5 |
| | | 16 | 298 [294–302] | 298 [296–302] | 1.00, within | 53.2 → 53.3 | 73.8 → 73.9 |
| | 1 MiB | 1 | 17.7 [17.5–18.2] | 19.2 [18.9–20.1] | **1.08, ahead** | 53.1 → 52.0 | 106 → 80.0 |
| | | 16 | 93.6 [92.3–95.1] | 95.4 [94.6–96.9] | 1.02, within | 150 → 148 | 353 → 354 |

What this says:
- **The gets at sixteen in flight were bound by europa's link, and their difference is the draw's.**
  - The link received 114 to 117.5 MB/s in every capture, so it was full on both sides.
  - Each deployment draws again which node leads which groups, and a get that titan or hyperion
    coordinates crosses the link whole, and one europa coordinates in part
    ([F102's draw](read-unit-cache.md#on-the-ssd-leg)).
  - Europa coordinated 43 to 50% of the strong reads in the plaintext captures, and 51 to 54% in
    the TLS ones.
  - Capture by capture, the rate followed europa's share. The 64 KiB gets at sixteen in flight ran
    1,952, 2,033, 2,063 and 2,068 a second as its share went 46, 50, 51 and 54%.
- **The other gets were within.**
- **A put at one in flight was faster, and its tail much shorter.**
  - Its p50 moved by 1 ms or less.
  - Its p90 went from 30.6 to 25.4 ms at 64 KiB, and from 76.3 to 68.0 at 1 MiB.
  - Its p99 went from 63.8 to 30.5 and from 106 to 80.0, every run of each side apart.

  ~~What in the lanes' TLS lengthens that tail across hosts, and not on one host, is not known.~~
  Under TLS the tail is each connection's window restarting after an idle: lab 3a took it away with
  `tcp_slow_start_after_idle` off (2026-10-24). Why these plaintext lanes escaped it was not
  established, since this run predates the sampler. It is filed as
  [O131](../appendix/optimizations.md#o131-a-puts-tail-at-one-in-flight-across-hosts-is-twice-as-long-under-tls).
  [F112](object-congestion.md) runs the object sockets under BBR, which keeps a window through an
  idle (2026-10-25), and since [F113](slow-start-after-idle.md) the hosts' own setting does it,
  BBR a deployment's choice.
- **Lab 2 ran TLS again on this build**, with each settle waiting out its arm's reclamation
  (`w2-ssd-ec21`'s before side, 2026-10-23).
  - The 64 KiB puts' tail repeated: a p99 of 63.5 ms [62.5–65.7].
  - The 1 MiB puts ran 25.0 to 25.4 a second with a p99 of 79.4 to 85.6 ms, not 17.7 and 106.
  - Here the 1 MiB puts at one in flight ran beside the reclamation of the 64 KiB puts before
    them, on both sides, as on the loopback. They wrote 2,038 to 2,065 MB a run, where lab 2 wrote
    1,905 to 1,956. So their row, like the loopback's, says more about the reclamation than about
    the lanes.
- **The puts at sixteen in flight were within.**

So plaintext lanes buy cpu. Where the nodes' cpu bounds them, as on one host, they ran 9 to 24%
faster at 2+1. Across the lab's 1 GbE link the gets are the link's either way, and a put at one in
flight loses less to its tail.

## Tests

| Test | What fails if the feature is reverted |
| --- | --- |
| `shoaladm` `deploy::inventory::tests::peer_lanes_are_tls_unless_named_plaintext` | Unnamed, the lanes are TLS and the key isn't written back; named, they are plaintext and it is |
| `shoaladm` `deploy::state::tests::a_cluster_keeps_the_peer_lanes_it_was_bootstrapped_with` | A cluster not deployed takes either kind; a record written before F103 is TLS and refuses plaintext by name; a plaintext record refuses TLS and keeps its kind written back |
| `shoaladm` `deploy::render::tests::plaintext_lanes_render_no_tls` | A plaintext deployment's file has no `cluster.tls`, and a TLS one's names the node's leaf with the binding on |
| `shoaladm` `deploy::unit::tests::a_plaintext_unit_loads_no_tls_module` | A plaintext node's unit runs no `modprobe tls`, and runs the node as before |
| `shoal-bench` `deploy_render` `a_rendered_plaintext_node_starts_without_certificates` | A rendered plaintext file is claimed, loaded and validated by the engine with no certificate anywhere, and its node starts and initializes |

## Related

- [F14. Encryption in transit](encryption-in-transit.md), kTLS;
  [F50](cluster-operations.md), certificate rotation and the fixture's `.peer_tls()`;
  [F51](cluster-deployment.md), shoaladm's deployment, which renders `cluster.tls`.
- [A 2+1 get crosses the object lane](../object-storage/store-optimizations/object-lane.md):
  O104's diagnosis, which found the lane's kTLS.
- [O131](../appendix/optimizations.md#o131-a-puts-tail-at-one-in-flight-across-hosts-is-twice-as-long-under-tls): a put's tail at one in flight across hosts under TLS, found by this
  page's run.
- [D4](../direction/encryption.md): plaintext "only defensible on a network that is trusted *and*
  isolated".
