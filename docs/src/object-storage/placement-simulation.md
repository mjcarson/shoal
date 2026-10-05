# X2. Placement, simulated

**Reported 2026-10-03.** This is the record of spike [X2](spikes.md#x2-placement-simulation).
It simulates S5's three placement candidates over generated pool maps, from the lab's three
hosts to fifty hosts of twenty-four devices, along with the variants of the second candidate
that the results called for. Each one was read for how evenly it fills devices, how much a
change to the map moves, whether it keeps the domain rule, what exceptions it needs, and what one
lookup costs. The pool map's frame was sized beside the tablet map's, which was measured again
first. The simulation is seeded and gives the same tables on every host. The lookups and the
frames were timed on titan and hyperion (Zen1) and on europa (Zen4).

It ends in a recommendation, which [S18](contract.md#q19-in-part-placement-2026-10-03) records as
the choice:

- **Weighted rendezvous over a pool's slices picks a placement group's set**: each failure
  domain's best-scoring slice, and the domains whose best slices score best.
- **The tablet group that owns the placement group records which member sits at which
  position**, beside the generation it already holds.
- **Each device has a seat.** The seat is the key the device is drawn by, and a replacement
  takes it over.
- **A device can have a placement weight the planner fits**, used only where a pool's devices
  differ in weight.
- **The number of placement groups a tablet is set for each pool**: a power of two fixed when the
  pool is made.

Three facts decide it:

- **The lab's shapes fill well.** Rendezvous puts the fullest device 3.7% above the mean at one
  placement group a tablet, and 0.9% at four. The trigger was a tenth.
- **For a set, it moves exactly the least each change could**, on every shape and every
  change. No rule that computes positions from the map does: they move 1.1 to 14 times the
  least on an erasure coded pool. [None can](#why-no-function-of-the-map-keeps-positions).
- **The pool map is 2.7 KB on the lab and 361 KB at fifty hosts.** It is never pushed to a
  client, so nothing about it needs deltas.

**Neither result that would have moved S5's preference came out.** But S5's statement of its
preferred function did not survive. Taking the best slice in each new domain "position by
position" gives each position to whichever domain's draw is best. So a change that alters any
draw reshuffles positions, even when no member of the set changes. On the lab with one device a
host, a reweight that needed no move moved 21% of a 2+1 pool's chunks. What X2 did not settle is under
[What X2 does not settle](#what-x2-does-not-settle).

## The question

[Q19](contract.md#questions-to-answer) asks how many placement groups a tablet holds, which
placement function a pool uses, which failure domains it reads, how a commit checks a
generation, and how large the pool map is and what pushing it costs. X2 is the part of it a
simulation can answer. How a commit checks a generation is a protocol question and belongs to
[X1](spikes.md#x1-the-stripe-protocol-as-a-model)'s model. Where a member's failure domain comes
from was decided with the user on the same day: `cluster.failure_domains: {host: ...}`, defaulting
to the hostname ([S1](prerequisites.md#required)).

S5 named three candidates ([the placement function](placement.md#the-placement-function)):

- the tablet rule extended, a window over an ordered slice list;
- weighted rendezvous, the preferred one;
- a table the planner assigns.

The spike's own section named two results in advance that would change the design. One was
weighted rendezvous leaving the fullest device more than about a tenth above the mean on the
lab's shape. The other was a pool map frame outgrowing what a whole push carries today by
enough to need deltas.

## How it was judged

Stated before any number was read, from S5's table of what the function must do, and from the
rule X4 and X5 applied to anything whose output is persisted.

**Required.** A candidate without one of these is not recommended, however well it fills:

| Requirement | Why |
| --- | --- |
| A function of the pool map alone, or of state the reader already holds | Every node computes it, and no node is asked ([P18](contract.md#the-contract)) |
| Never two chunks of a stripe in one failure domain, or on one device | [P11](contract.md#the-contract) counts distinct domains, and two slices of one device fail together |
| Spreads by weight | Devices differ in size, and a pool mixes them |
| Moves little when a device is added, removed, reweighted or replaced | Every chunk moved is a copy over a network, under a budget |
| Keeps positions | Stripe chunk `i` lives at position `i`. A chunk that slid to another position is a different chunk, and has to be written again |
| The same answer on every node, every cpu and every build | A chunk sits where the function said for as long as it is there |

**Then ranked by:**

- fill on the lab's shapes, which is the first trigger;
- movement over the least each change could make;
- the exceptions it needs;
- what a lookup costs on a Zen1 core;
- the pool map's frame against today's, which is the second trigger.

## What was run

### The harness

`shoal-spike placement` is a subcommand of the existing spike binary, in
`shoal-spike/src/placement/`. It is pure: it reads no engine type and adds no crate to
`Cargo.lock`, only a `serde` edge the workspace already resolves. It has four parts:

- a score;
- the shapes;
- the candidates;
- the measurements, which are split across threads with `std::thread::scope`.

`shoal-spike placement lookups` times one answer at a time on a pinned core. `shoal-spike
fanout`, which priced the tablet map's frame at F39, now also prices the tablet frame with
configured sets in it and the pool map's frame beside it.

**The score.** A draw is SplitMix64's output function applied to a placement group's key and a
slice's key, each mixed once. It is defined in five lines, not by any crate, which is X5's rule
for anything persisted. The weighted score is `-log2(u / 2^64) / w`, and the lowest wins. That is
`straw2`'s comparison, `ln(u) / w` highest, written the other way round. The logarithm comes from
a table of `log2(1 + i/4096)` in fixed point, with linear interpolation, and every operation after
it is a plain IEEE multiply. ~~This is Ceph's reason for `crush_ln`:~~ Ceph's `crush_ln` is a
fixed-point table too, though its source gives no reason
([X14](ceph-and-s3-sources.md#5-placement-indep-upmap-and-crush-compat)). Ours is that libm's `ln`
is not specified to the last bit, and two nodes on a near tie must not choose differently
([The logarithm](#the-logarithm)).

**The candidates.** Each is a function of the placement group and one view of the map, which is
built once for each map:

| Candidate | What it does |
| --- | --- |
| `window` | The tablet rule extended. The pool's slices are dealt into one list a host at a time, so that neighbours are on different hosts where they can be. Group `p` takes `list[(p + k) % N]` for position `k`. It repairs nothing, so an answer that puts two positions in one domain is counted |
| `rendezvous` | S5 as written: by weighted rendezvous, position by position, the best slice of a domain not yet used. That picks domains in the order of their best slices, which is how it is computed: one pass over the slices |
| `rendezvous by position` | Each position draws its own rounds, `pos + round × width`, keeps a draw unless an earlier position holds that domain, and draws again otherwise. It is the idea of CRUSH's `indep` mode. After 32 rounds the empty positions take, in turn, the best unused domain |
| `rendezvous by domain` | `rendezvous` drawn down the hierarchy: hosts by their weight, then a device in each chosen host, then a slice of it. What a lookup costs with thousands of slices |
| `by domain, by position` | `rendezvous by position` drawn down the hierarchy |
| `rendezvous, positions matched`, `by domain, positions matched` | The set from `rendezvous` or `rendezvous by domain`, and the positions from a matching. Every domain and position pair is drawn, with no weight in the draw, and pairs are taken highest first while both are free. This was added after the first run showed position shuffles |
| `rendezvous, positions kept` | `rendezvous`'s set with positions held as state, as a tablet group would hold them. A member that stays keeps its position and one that arrives takes a vacated one. Not a function of the map, and measured to show what keeping the state is worth |
| `table` | The planner's assignment. Every chunk goes on the allowed device least full for its weight, and a change is applied by the fewest moves: what left is placed again, a new device is filled from the fullest, and a shrunken one is emptied into the emptiest. Its fill is the best a shape allows, and its moves are the least a change can make |

Two more are built on `rendezvous`:

- **exceptions**: an `upmap`-style balancer. One chunk at a time moves off the fullest device
  onto the emptiest one the domain rule allows, counting the entries it takes to bring the
  fullest within 5%, 2% and 1% of the mean;
- **fitted weights**: a placement weight for each device, scaled over 24 rounds by the inverse
  square root of its fill. These are fitted to one consumer, or to a sample of eight, and judged
  on a consumer they were not fitted to.

**The shapes:**

| Shape | Hosts × devices | Pools | What it stands for |
| --- | --- | --- | --- |
| `lab-1` | 3 × 1 | r3/host, 2+1/host | The lab, one 500 GB device a host |
| `lab-2` | 3 × 2 | r3/host, 2+1/host, 4+2/device | The lab, two a host |
| `lab-fitted` | 3 hosts, 4 devices | r3/host, 2+1/host, 3+1/device | The lab as fitted, read from the hosts: europa's Optane 900P (261 GiB) and 990 PRO (931), and a 970 EVO (466) in titan and in hyperion |
| `6x12` | 6 × 12, 8 TB | r3/host, 4+2/host, 8+3/device | A small cluster |
| `6x12-mixed` | 6 × 12, 4 TB and 16 TB alternating in each host | 4+2/host, 8+3/device | Devices of two sizes, hosts of one weight |
| `6x12-uneven` | 3 hosts of 4 TB devices, 3 of 16 TB | r3/host, 4+2/host, 8+3/device | Hosts of two weights |
| `6x12-slices` | 6 × 12, every other device in four slices | 4+2/host, 8+3/device | Devices of one slice and of several |
| `50x24` | 50 × 24, 16 TB | r3/host, 8+3/host, 10+4/host | A large cluster |
| `two-classes` | 6 × (8 hdd of 16 TB + 4 ssd of 3.84 TB in two slices) | `bulk` 4+2/host on hdd, `fast` r3/host on ssd | Two classes, a pool on each |

**The rest of the setup:**

- **Placement groups**: a group's key names its consumer, its tablet and its sub-range, and there
  are 4096 × `g` of them for a consumer, at `g` = 1, 4, 16 and 64. Every table is for **one
  consumer**, a large bucket's case and the worst one, and holds every group to the same number
  of stripes; [a bucket of few stripes](#a-bucket-of-few-stripes) is the exception.
- **Changes**: each is made to host zero's first device of the pool's class. A device like it is
  added; it is removed; its weight is halved; it is replaced by a new device under a new seat;
  it is replaced under its own seat; and its host is lost.
- **The least a change could move**: the chunks the departing devices held, or the chunks a new
  device ends up with, or the chunks a reweighted device gave up.
- **A move**: on an erasure coded pool, a chunk that changed position counts as moved; on a
  replicated pool, only a changed set does.
- **Checks**: every answer is read back. Any candidate but the window that puts two positions in
  one domain stops the run, and none did.

### Where, and from which builds

| What | Host | Build | Governor |
| --- | --- | --- | --- |
| The simulation, `x2-placement.md` | europa, every thread | native, release | `powersave`; nothing it prints is a time |
| Lookups and frames | titan, cpu 2 | `znver1` | `performance` |
| The same | hyperion, cpu 2 | `znver1`, the same binary | `performance` |
| The same | europa, cpu 8 | `znver1`, the same binary | `performance` |

- No shoal unit was running on any host, so none was stopped. The governors went back to
  `schedutil` on titan and hyperion and `powersave` on europa afterwards.
- The simulation was run three times while the fitted weights' section was being written, and
  every section the runs had in common matched byte for byte. A run takes 75 seconds on europa.
- titan and hyperion agreed to a median of 0.1% a lookup cell, 1.0% at the 95th percentile,
  and 8.5% at the widest.
- rustc 1.100.0-nightly (2026-09-04), on the tree after `5eeff36`.

To run it again, see `CLAUDE.md`. The raw tables are in `shoal-spike/results/`: `x2-placement.md`,
and `x2-lookups-<host>.md` and `x2-fanout-<host>.md` for each host.

## Fill

### The lab

The fullest device over the mean, then the coefficient of variation across the pool's devices:

| Shape | Pool | Groups a tablet | Chunks a device | `window` | `rendezvous` | `by position` | `by domain` | `table` |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| lab-1 | r3/host, 2+1/host | any | 4,096 at 1 | +0.0% | +0.0% | +0.0% | +0.0% | +0.0% |
| lab-2 | r3/host | 1 | 2,048 | +0.0% / 0.0% | **+3.7% / 2.2%** | +2.0% / 1.2% | +0.3% / 0.2% | +0.0% |
| lab-2 | r3/host | 4 | 8,192 | +0.0% | **+0.9% / 0.7%** | +1.8% / 1.2% | +0.9% / 0.7% | +0.0% |
| lab-2 | r3/host | 16 | 32,768 | +0.0% | +0.5% / 0.3% | +0.4% / 0.3% | +0.3% / 0.2% | +0.0% |
| lab-2 | 4+2/device | any | 4,096 at 1 | +0.0% | +0.0% | +0.0% | +0.0% | +0.0% |
| lab-fitted | r3/host, 2+1/host | any | 3,072 at 1 | +103.4%, and breaks the rule for half the groups | +51.9% | +51.9% | +51.9% | **+51.9%** |
| lab-fitted | 3+1/device | any | 4,096 at 1 | +103.4% | +103.4% | +103.4% | +103.4% | **+103.4%** |

The 2+1/host rows are the r3/host rows: one is three distinct domains of three, and so is the
other.

**On the lab's shape the trigger does not fire.** Three hosts of two devices and a pool three
wide is where S5 said a statistical rule balances worst, and rendezvous leaves the fullest
device 3.7% over the mean at one group a tablet. With three hosts and three positions, every
group takes every host. Rendezvous decides only which of a host's two devices holds the chunk,
and 2,048 coin tosses a device are within a few percent. At four groups a tablet the fullest is
0.9% over.

**The lab as fitted is not balanced by anything.** europa's devices weigh 1,192 GiB against 466
on each Zen1 host. A pool three wide over three hosts gives every host a third of the chunks
whatever the weights, so titan and hyperion fill 52% faster than the mean, and the planner's
table does no better than rendezvous. The window ignores weight and does worse, and it puts
both of europa's devices in half the groups. That is a property of the pool's width against
its domains, not of the function. A pool whose width equals its domain count fills at the pace
of its smallest domain; [S4](pools-and-devices.md#what-a-node-checks-before-it-serves-a-pool)'s
readiness can say so.

### Wider shapes

`rendezvous`, the fullest over the mean, by groups a tablet:

| Shape | Pool | 1 | 4 | 16 | 64 | Chunks a device at 64 | `table` at 1 |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 6x12 | r3/host | +18.9% | +8.7% | +4.5% | +2.8% | 10,923 | +0.2% |
| 6x12 | 4+2/host | +10.4% | +6.5% | +4.0% | +1.3% | 21,845 | +0.2% |
| 6x12 | 8+3/device | +9.9% | +5.6% | +1.8% | +1.0% | 40,050 | +0.0% |
| 6x12-slices | 4+2/host | +11.0% | +5.8% | +2.7% | +1.3% | 21,845 | +0.2% |
| two-classes | bulk 4+2/host | +9.4% | +5.3% | +2.8% | +1.6% | 32,768 | +0.0% |
| 50x24 | r3/host | +105.1% | +48.9% | +26.3% | +12.9% | 655 | +7.4% |
| 50x24 | 10+4/host | +46.5% | +28.2% | +11.0% | +6.4% | 3,058 | +0.4% |
| 6x12-mixed | 8+3/device | +23.8% | +12.0% | +11.4% | **+9.8%** | 40,050 | +0.1% |
| 6x12-uneven | r3/host | +50.9% | +47.2% | +36.5% | **+32.2%** | 10,923 | +0.3% |
| 6x12-uneven | 4+2/host | +190.0% | +168.6% | +159.2% | +153.8% | 21,845 | **+150.4%** |

The four rendezvous forms are within a few points of each other at every shape; the hierarchy
and the rounds change which groups go where, not how evenly. The window balances a uniform shape
almost perfectly, at +0.3% for 50x24 at 64, because it deals the list in turn; on mixed sizes it is
+150%.

### Two kinds of imbalance

The table shows two kinds, and they need different remedies.

- **Statistical.** Where a pool's devices are alike, the fullest device's excess falls as the
  chunks a device grow, roughly as one over their square root. That holds on 6x12, 6x12-slices,
  two-classes and 50x24. About two thousand chunks a device for one consumer leave the fullest
  within 5 to 8%: 6x12 r3 is +4.5% at 2,731 and 50x24 8+3 is +7.9% at 2,403. More devices need
  more chunks for the same margin, because the fullest of more devices is further out. One
  placement group a tablet gives a consumer 4096 × width ÷ devices chunks a device. That is 2,048
  on lab-2, 341 on 6x12 at 4+2 and 48 on 50x24 at 10+4. So **the number of placement groups a
  tablet is the pool's to choose**: no one number serves the lab and fifty hosts.
- **Systematic.** Where the devices one group picks from differ in weight, rendezvous's
  inclusion of a heavy device is less than proportional. That is the bias of drawing several
  without replacement, and Ceph's balancer exists to correct it. More placement groups do not
  remove it: 6x12-mixed 8+3/device is still +9.8% at 40,050 chunks a device, and 6x12-uneven r3
  is +32.2%. The planner's table is +0.0% on both.
- **Neither.** 6x12-uneven 4+2/host is six wide over six hosts of two weights, and every
  candidate is 150% over, the table included. Like lab-fitted it is the pool's width against its
  domains.

### A bucket of few stripes

The tables above give every placement group the same bytes. A small bucket does not. Below,
`rendezvous` on lab-2 with each group holding a Poisson number of stripes:

| Pool | Groups a tablet | 10⁴ stripes | 10⁵ | 10⁶ | 10⁷ | Every group alike |
| --- | --- | --- | --- | --- | --- | --- |
| r3/host | 1 | +4.2% | +3.8% | +3.7% | +3.7% | +3.7% |
| r3/host | 4 | +0.7% | +0.8% | +1.0% | +1.0% | +0.9% |

Ten thousand stripes are 40 GiB at 4 MiB, and their scatter adds half a point on the lab. A
bucket's imbalance is placement's, not its stripes'.

## Exceptions and fitted weights

**Exceptions** are what `rendezvous` needs on the map to be within a margin. Each cell gives the
entries, and in brackets the share of the consumer's placement groups they touch:

| Shape | Pool | Groups a tablet | Fullest before | Within 5% | Within 2% | Within 1% |
| --- | --- | --- | --- | --- | --- | --- |
| lab-2 | r3/host | 1 | +3.7% | 0 | 36 (0.88%) | 56 (1.37%) |
| lab-2 | r3/host | 4 | +0.9% | 0 | 0 | 0 |
| 6x12 | 4+2/host | 4 | +6.5% | 22 (0.13%) | 258 (1.25%) | 539 (2.19%) |
| 6x12 | 4+2/host | 16 | +4.0% | 0 | 166 (0.25%) | 586 (0.72%) |
| 50x24 | 10+4/host | 64 | +6.4% | 72 (0.03%) | 3,975 (1.05%) | 11,754 (2.02%) |
| 6x12-mixed | 8+3/device | 16 | +11.4% | 3,943 (3.26%) | 8,231 (5.93%) | 9,671 (6.88%) |
| 6x12-mixed | 8+3/device | 64 | +9.8% | 14,455 (2.71%) | **31,771 (5.69%)** | 37,531 (6.71%) |
| 6x12-uneven | r3/host | 64 | +32.2% | 38,488 (12.95%) | 43,204 (14.53%) | 44,788 (15.07%) |
| lab-fitted | r3/host, 2+1/host | any | +51.9% | stuck | stuck | stuck |

Where the imbalance is statistical, the exceptions a margin needs are a few hundred and do not
grow with the groups: none at all on lab-2 from four a tablet. **Where it is systematic, they
grow with the groups**, because a fixed share of every consumer's groups sits where the bias put
it. At 115 bytes an exception in the map's frame ([the map](#what-the-map-holds-over-time)),
31,771 of them are 3.7 MB. That is the outcome S5 named as the bad one: exceptions as the rule,
not the remedy. It holds only for pools of mixed weights, and fitted weights correct it.

**Fitted weights.** Each device gets a placement weight beside its capacity, fitted by the
planner and committed in the pool map: one number a device, not a record a group. These are at
16 groups a tablet, judged on a consumer the weights were not fitted to:

| Shape | Pool | `rendezvous` | Fitted to one consumer | Fitted to a sample of eight | Exceptions to 2%, before → after | A device added, fitted again |
| --- | --- | --- | --- | --- | --- | --- |
| 6x12-mixed | 8+3/device | +10.6% | +3.4% | **+3.6%** | 7,708 → **141** | 1.03× the least |
| 6x12-uneven | r3/host | +37.7% | +9.2% | +9.5% | 10,809 → **435** | 1.16× |
| 6x12-uneven | 8+3/device | +11.1% | +4.0% | +3.4% | 7,936 → **55** | 1.04× |
| 6x12 | r3/host | +3.8% | +5.0% | +3.6% | 158 → 211 | 1.23× |
| 6x12 | 4+2/host | +2.7% | +3.6% | +2.5% | 70 → 65 | 1.00× |
| 50x24 | 10+4/host | +13.4% | +18.0% | +13.8% | 6,587 → 7,150 | 1.33× |
| lab-2 | r3/host | +0.8% | +1.3% | +0.8% | 0 → 0 | 1.00× |

- **They remove a systematic bias**, at a fit kept current for 1.03 to 1.16 times the least
  on an added device.
- **On a pool of alike devices they have nothing to correct.** Fitted to one consumer, they learn
  its luck and do worse on the next. Fitted to a sample of eight, they are merely harmless: the
  results on 6x12 and 50x24 are within noise of none.
- So a planner fits weights to a sample of many groups, and only for a pool whose devices
  differ in weight. What is left over is statistical and is exceptions' work.

## Movement

### A set

What a change moves, over the least it could, for a **replicated** pool, where a set is all that
matters. These are at four groups a tablet:

| Shape | Pool | Change | `window` | `rendezvous` | `by domain` | `table` |
| --- | --- | --- | --- | --- | --- | --- |
| lab-2 | r3/host | add | 4.00× | **1.00×** | 1.00× | 1.00× |
| lab-2 | r3/host | reweight ½ | none needed; none moved | **1.00×** | 1.00× | 1.00× |
| 6x12 | r3/host | add | 69.61× | **1.00×** | 1.62× | 1.00× |
| 6x12 | r3/host | remove | 68.76× | **1.00×** | 1.57× | 1.00× |
| 50x24 | r3/host | add | 1,075.77× | **1.00×** | 2.00× | 1.00× |
| 50x24 | r3/host | reweight ½ | none moved | **1.00×** | 2.27× | 1.00× |
| 50x24 | r3/host | host lost | 49.90× | **1.00×** | 1.00× | 1.00× |
| two-classes | fast r3/host | add | 23.37× | **1.00×** | 1.46× | 1.00× |

**Flat rendezvous moves exactly the least for a set**, on every shape and every change but one,
and the exception is a replacement under a new seat ([below](#a-replaced-device)). That is
`straw2`'s property, which S5 chose the candidate for, and it holds.

**The hierarchy costs that property.** A device's change moves its host's weight, and a host's
draw against every other host moves with it. So chunks leave the host from devices that did
not change: 1.5 to 2.3 times the least. ~~Ceph's operators know this as the second movement when
an OSD is taken out of the CRUSH map.~~ No Ceph document names a second movement, but Ceph's own
CRUSH makes one: an OSD marked out moved 1.00 to 1.02 times the least where the pool is narrower than its
domains, and removing it from the map afterwards moved 1.1 to 2.9 times the least again
([X14](ceph-and-s3-sources.md#5-placement-indep-upmap-and-crush-compat)). The window moves nearly everything whenever the list
changes length, as S5 expected.

### Positions

The same for an **erasure coded** pool, where a chunk that changes position is a chunk moved:

| Shape | Pool | Change | `rendezvous` | `by position` | `by domain` | `rendezvous, positions matched` | `by domain, positions matched` | `positions kept` | `table` |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| lab-1 | 2+1/host | add | 1.67× | 1.78× | 1.99× | 1.00× | 1.00× | **1.00×** | 1.00× |
| lab-1 | 2+1/host | reweight ½ | **10,360 moved, none needed** | 8,583 | 10,233 | 0 | 0 | **0** | 0 |
| lab-2 | 2+1/host | add | 1.60× | 1.86× | 2.02× | 1.00× | 1.00× | **1.00×** | 1.00× |
| lab-2 | 2+1/host | reweight ½ | 2.55× | 2.25× | 2.56× | 1.00× | 1.00× | **1.00×** | 1.00× |
| lab-2 | 4+2/device | add | 3.51× | 1.78× | 3.46× | 2.05× | 2.06× | **1.00×** | 1.00× |
| 6x12 | 4+2/host | add | 2.26× | 2.64× | 3.54× | 1.00× | 1.00× | **1.00×** | 1.00× |
| 6x12 | 8+3/device | add | 6.00× | 1.16× | 5.91× | 2.52× | 2.53× | **1.00×** | 1.00× |
| 6x12 | 8+3/device | host lost | 3.70× | 1.12× | 3.74× | 1.98× | 1.99× | **1.00×** | 1.00× |
| 50x24 | 10+4/host | add | 7.07× | 1.36× | 13.08× | 2.33× | 3.16× | **1.00×** | 1.00× |
| 50x24 | 10+4/host | reweight ½ | 8.43× | 1.42× | 10.86× | 2.27× | 3.16× | **1.00×** | 1.00× |
| two-classes | bulk 4+2/host | remove | 2.36× | 2.44× | 3.36× | 1.00× | 1.00× | **1.00×** | 1.00× |

- **S5's statement moves 1.4 to 8.4 times the least.** It assigns positions in the order of the
  domains' draws. A device that comes, goes or changes weight changes some draws, and every
  position after the first changed one can shift. On lab-1, three hosts of one device each, a
  reweight changes no set at all and still moves 21% of the pool's chunks.
- **Drawing by position, as CRUSH's `indep` mode does, moves less: 1.1 to 2.6 times**, and up to
  4.7 for a replacement under a new seat. It also costs the most to look up, and falls back on
  0.4% to 15% of groups where a pool is as wide as its domains ([feasibility](#feasibility)).
- **A matching that no weight enters moves no position for a reweight.** When a host's devices
  change, it moves none under a host domain either, since the host keeps its position. When the
  set of domains changes, it moves 2 to 3 times the least.
- **Positions held as state move exactly the least**, on every shape and every change, the
  planner's table among them.

### Why no function of the map keeps positions

The matching is the best of the rules tried, and no rule of this kind can be perfect. Take
three members, A, B and C, two positions, and any rule that gives a set of two its positions.
Say {A, B} gives A position 0 and B position 1. Then:

- for B leaving {A, B} for C to move nothing else, {A, C} must give C position 1;
- for A leaving {A, B} for C, {B, C} must give C position 0;
- and then the change from {A, C} to {B, C} moves C, which neither member's departure called
  for.

So any function of the set alone shuffles some position on some change. Where a set's chunks sit
depends on how the set was reached, and only state can remember that. The tablet group that owns
the placement group is where that state already lives: it holds the group's generation, and every
stager and reader learns that generation from it. They read it with the row
([S7](write-path.md#the-preferred-direction-step-by-step),
[S9](read-path.md)).

### A replaced device

Rendezvous draws a device by a key. If a replacement draws by a key of its own, it takes a fresh
share of groups and not its predecessor's, so the predecessor's share scatters over everybody.
If it takes over its predecessor's key, it takes the same groups and nothing else moves:

| Shape | Pool | Replace, new seat: `positions kept` | Replace, seat kept: `positions kept` | `table` |
| --- | --- | --- | --- | --- |
| lab-2 | r3/host | 1.34× | 1.00× | 1.00× |
| 6x12 | 8+3/device | 1.84× | 1.00× | 1.00× |
| 50x24 | r3/host | 2.21× | 1.00× | 1.00× |
| 50x24 | 10+4/host | 2.07× | 1.00× | 1.00× |

So the key a device is drawn by is a **seat**: committed in the pool map beside the device, minted
with it, and taken over by a replacement that the operator says is one. It is not the device's
identity. [S4](pools-and-devices.md#a-device-has-slices) is right that a replaced disk comes up
as a new device holding nothing, and it can still sit in its predecessor's seat. Ceph reaches the
same result by reusing the OSD's id (`ceph osd destroy`), which ties the two together; a seat keeps
them apart.

## Feasibility

No rendezvous form and no table ever put two positions in one domain. That was checked on every
answer of every run. The window does wherever hosts are unequal or a device has several slices:

| Shape | Pool | `window` breaks the rule for | `by position` falls back for |
| --- | --- | --- | --- |
| lab-fitted | r3/host | 50.0% of groups | 0.0% |
| 6x12-slices | 8+3/device | 73.3% | 0 |
| lab-2 | 4+2/device | 0 | 0.5% at 1 a tablet, 0.4% at 64 |
| 6x12-uneven | 4+2/host | 0 | 15% |

`by position` falls back where a pool is as wide as its domains, because the last position has
to draw the one domain left. It draws 8 rounds on average on lab-2 4+2/device and 16 on
6x12-uneven 4+2/host. A pool asking for more domains than it spans places nothing under any
candidate, and the tables say so.

## A lookup

Nanoseconds for one placement group's whole answer, one pinned core, at four groups a tablet.
The view is built once for each map:

| Shape | Pool | Slices | Build the view | `rendezvous` | libm `ln` | `by position` | `by domain` | `by domain, positions matched` | A table's read |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| **titan**, Zen1 | | | | | | | | | |
| lab-2 | 2+1/host | 6 | 0.9 µs | 132 | 195 | 345 | 163 | 413 | 4.4 |
| 6x12 | 4+2/host | 72 | 7.0 µs | 857 | 1,617 | 8,938 | 724 | 1,618 | 4.4 |
| 50x24 | 10+4/host | 1,200 | 119 µs | 12,453 | 24,239 | 149,137 | 3,687 | 8,487 | 4.4 |
| 50x24, four slices | 10+4/host | 4,800 | 333 µs | 40,213 | 88,337 | 605,060 | 3,912 | 8,756 | 4.4 |
| **europa**, Zen4 | | | | | | | | | |
| lab-2 | 2+1/host | 6 | 0.3 µs | 64 | 84 | 142 | 60 | 171 | 2.4 |
| 6x12 | 4+2/host | 72 | 2.1 µs | 454 | 743 | 3,314 | 260 | 721 | 2.4 |
| 50x24 | 10+4/host | 1,200 | 41 µs | 6,121 | 10,160 | 55,712 | 1,470 | 4,010 | 2.4 |
| 50x24, four slices | 10+4/host | 4,800 | 116 µs | 16,653 | 30,567 | 222,087 | 1,623 | 4,175 | 2.4 |

- **Flat rendezvous costs about ten nanoseconds a slice on Zen1**, and a lookup grows with the
  pool. That is 132 ns on the lab and 40 µs at fifty hosts of four-slice devices. The answer for
  a placement group at a generation never changes, so a node computes it once and keeps it
  ([O87](../appendix/optimizations.md#o87-a-placement-answer-is-computed-again-on-every-lookup)).
  On the lab, every group of a consumer at one a tablet is 4,096 lookups, half a millisecond of a
  Zen1 core.
- **The hierarchy is the cheap lookup**, at 3.7 µs on titan where flat costs 40, and it is the
  one that ripples ([a set](#a-set)). Caching makes flat's cost a cost per map change, and the
  ripple is a cost in bytes moved, so flat with a cache is the trade taken.
- **libm's logarithm is half the speed of the table's**, so determinism costs nothing here.
- **Drawing by position is dearest by far** at width, because every position draws over every
  slice, round after round.
- Building the view from a map takes 0.9 µs on the lab and 333 µs at fifty hosts on titan, once
  a committed change, on the node and not on a shard's path.

## The map

### Today's tablet frame, again

S5 and the spike's section quoted 13,493 bytes for the tablet map's frame at sixty-four
members and sixteen tables, from F39. Measured again on this tree, it is **16,555 bytes**.
F46's phase and state fields on every member account for the difference: the frame has no
configured sets or moves when it is first initialized. F45's configured sets are what grows it
after a rebalance:

| Configured sets | Moves | Frame bytes | titan encode µs | europa encode µs |
| --- | --- | --- | --- | --- |
| 0 | 0 | 16,555 | 29.5 | 10.9 |
| 16 | 0 | 24,896 | 43.8 | 17.1 |
| 64 | 0 | 50,004 | 88.2 | 35.4 |
| 64 | 8 | 54,075 | 95.7 | 38.3 |

Each set lists its sixty-four tablets one by one, so a cluster that has moved every replica set
pushes three times the frame it started with
([O88](../appendix/optimizations.md#o88-a-configured-set-lists-its-tablets-one-by-one)). It is a
finding about the tablet map, not about placement, and nothing here depends on it.

### The pool map's frame

The pool map was sized as JSON, the way the topology frame is pushed. It carries the pools, the
bindings, and every device with its slices nested, ids as UUIDs. That is a sketch for sizing and
not a format:

| Shape | Devices | Slices | Frame bytes | Over today's tablet frame | To 1,000 subscribers, titan | europa |
| --- | --- | --- | --- | --- | --- | --- |
| Today's tablet frame | | | 16,555 | 1× | 11.2 ms | 5.0 ms |
| lab-1 | 3 | 3 | 1,658 | 0.10× | 0.9 ms | 0.4 ms |
| lab-2 | 6 | 6 | **2,747** | 0.17× | 1.6 ms | 0.8 ms |
| lab-fitted | 4 | 4 | 2,150 | 0.13× | 1.2 ms | 0.6 ms |
| 6x12 | 72 | 72 | 22,367 | 1.35× | 15.4 ms | 7.7 ms |
| 6x12-slices | 72 | 180 | 29,315 | 1.77× | 20.4 ms | 9.7 ms |
| two-classes | 72 | 96 | 23,867 | 1.44× | 16.1 ms | 7.1 ms |
| 50x24 | 1,200 | 1,200 | **361,199** | 21.8× | 247 ms | 129 ms |
| 50x24, four slices | 1,200 | 4,800 | 598,591 | 36.2× | 403 ms | 211 ms |

**The second trigger fires only for a push the design does not make.** On the lab's shape the
frame is a sixth of today's, and at six hosts of twelve it is a third larger. At fifty hosts, a
whole frame pushed to a thousand subscribers would take a quarter of a second a version on Zen1
and would need deltas. But:

- **No client is pushed it.** A client places nothing ([Q15](contract.md#questions-to-answer)),
  as S5 says.
- **A node does not receive it either.** It derives the pool map from the control group's
  committed state, as it derives the tablet map, so each change crosses the network as one
  committed command of a few hundred bytes, which is a delta by construction.
- **A shard holds the node's copy behind an `Arc`.** That is how `MapCell` holds the tablet map
  today (`shoal-core/src/server/map.rs:1200`).

The whole frame is paid only in the control group's snapshot, and on an admin's read of it. If
[D7](../direction/shard-aware-routing.md) ever pushes the pool map to clients, it pushes deltas.

### What the map holds over time

What one more record adds to the frame:

| Record | Bytes |
| --- | --- |
| A device of one slice | 301 |
| A slice beyond its device's first | 64 |
| A change kept, so a generation can be computed again | 97 |
| A move listed | 89 |
| An exception | 115 |

And a busy map, in frame bytes:

| Shape | Devices alone | 64 changes kept | A move a device in flight | 1,000 exceptions | Every group a device's add moves, 100 consumers |
| --- | --- | --- | --- | --- | --- |
| lab-2 | 2,747 | 8,947 | 3,283 | 117,866 | **124,841,943** |
| 6x12 | 22,367 | 28,631 | 28,780 | 137,474 | 22,036,181 |
| 50x24 | 361,199 | 367,463 | 468,169 | 476,344 | 2,268,596 |

- **A generation is kept as the changes since the oldest one a placement group is still at.** A
  device carries the generation it joined at and the one it left at, and a reweight is one
  record. So the function at any kept generation is computed by filtering. Sixty-four of them
  cost 6 KB.
- **Moves in flight are the planner's, one a device at a time** ([S10](recovery.md#moves)), so
  they are bounded by devices. The groups a change will move but has not started are not listed:
  which they are follows from the two generations, and whether each has moved is its own tablet
  group's state. Listing them instead would put 125 MB on lab-2's map for a hundred consumers.
- **Exceptions are the one record that grows with the data**, which is why fitted weights take
  the systematic part of their work.

## The logarithm

Every chunk of every group at 64 groups a tablet, placed by `rendezvous` with the table's
logarithm and again with `f64::ln`:

| | Chunks | Chosen differently |
| --- | --- | --- |
| Every shape and pool | 39,321,600 | **4**, two on each of 6x12-mixed and 6x12-uneven 8+3/device |

The four are near ties, where the table's error and libm's rounding fall on different sides. A
second libm, or the same one on another cpu, can disagree with the first the same way, though
less often. Any rate above zero is a chunk that one node stages where another node reads.
So the logarithm is the table's, frozen as constants and checked against published values when
it is written. ~~That is what Ceph does with `crush_ln` for the same reason.~~ Ceph's `crush_ln` is a table
too, for no reason its source states ([X14](ceph-and-s3-sources.md#5-placement-indep-upmap-and-crush-compat)). It is also the faster
of the two.

F58's lead score is weighted rendezvous over `f64::ln` too, and its docstring claims the same
answer on every build. The exposure there is a group's lead moving back and forth, not a chunk.
It is filed as [item 212](../appendix/known-issues.md#212-a-leads-rendezvous-score-takes-libms-logarithm).

## What would have changed the design

The results X2 named in advance:

| If | Found | So |
| --- | --- | --- |
| Weighted rendezvous leaves the fullest device more than about a tenth above the mean on the lab's shape | **No**: +3.7% at one group a tablet and +0.9% at four, on three hosts of two devices. Where it is worse on the lab, at +51.9%, every candidate is, the planner's table included, because of the pool's width against its hosts | Rendezvous is the rule, and exceptions are the remedy |
| A pool map frame outgrows what a whole push carries today by enough to need deltas | **Only for a push nobody makes.** It is 2.7 KB on the lab, 22 KB at six hosts, 361 KB at fifty | No deltas. A node derives the map from committed commands; a client is not pushed it |

And three results nobody named, each of which changes S5:

- **The function cannot keep positions.** Positions are the tablet group's state, beside the
  generation ([why](#why-no-function-of-the-map-keeps-positions)).
- **A replacement scatters its predecessor's share unless it takes over its key.** A device has
  a seat.
- **Mixed weights bias rendezvous systematically.** A pool of mixed weights gets fitted
  placement weights, one a device, or its exceptions grow with its data.

## The comparison

| Option | Fill | Movement | Lookup, titan | Map | Strengths | Weaknesses |
| --- | --- | --- | --- | --- | --- | --- |
| **Rendezvous for the set, positions in the group, seats, fitted weights where needed** (recommended) | Lab +3.7% at 1 a tablet, +0.9% at 4; mixed sizes +3.6% after fitting | **1.00×** the least on every shape and change; a replacement 1.00× in its seat | 132 ns on the lab, 12 µs at 1,200 slices, cached | 301 B a device, a few hundred B a change | The one option that moves the least everywhere and puts nothing for a group on the map | State in each tablet group for each group that has moved. A lookup that grows with the pool. Fill is statistical, so a large pool needs more groups a tablet |
| `rendezvous`, S5 as written | The same | Sets 1.00×; positions 1.4× to 8.4× | The same | The same | A function of the map alone | Fails "keeps positions": every change to a draw reshuffles them |
| `rendezvous by position` (CRUSH `indep`) | Within a few points of `rendezvous` | 1.1× to 2.6× | 345 ns to 605 µs | The same | The best positions of any pure function | The dearest lookup, and falls back where a pool is as wide as its domains |
| `rendezvous by domain` | The same | Sets 1.5× to 2.3×: a device's change ripples through its host | 163 ns to 3.9 µs | The same | A lookup that barely grows with devices | Moves bytes that did not need to move, on every device change |
| Positions matched by domain | The same | 1.00× while the set's domains stay; 2× to 3× when they change | 413 ns to 8.8 µs | The same | No weight enters positions; a host's device swap moves none | Still a function of the set, so still shuffles |
| `window`, the tablet rule extended | +0.3% on a uniform shape; +103% to +150% on mixed | Up to 1,168× | 26 to 113 ns | The same, and a list order | Perfect on equal devices; the cheapest lookup | No weights; moves nearly everything; breaks the domain rule on unequal hosts and on sliced devices |
| `table`, the planner's assignment | The best a shape allows | 1.00× | 4.4 ns, a read | A record a group on the pushed map: 4096 × groups a tablet × consumers | Any balance; the least movement | The map grows with the data, which S5 and C4 both refuse |

## Recommendation

**Weighted rendezvous picks a placement group's set, flat over the pool's slices.** Each failure
domain's best slice is found by its score, `-log2(u / 2^64) × (1 / w)`. Here `u` is SplitMix64's
mix of the group's key and the slice's key, the logarithm comes from a fixed-point table, and `w`
is the slice's share of its device's placement weight. The `width` domains whose best slices
score lowest are the set. One pass over the slices finds it.

- **Positions are the tablet group's**, held beside the placement group's generation. When a
  group is first placed, its positions are the order of the domains' scores. When a move
  switches the generation, the commit that switches it records the new positions: a member that
  stays keeps its position, and one that arrives takes a vacated one. A permutation of at most
  sixteen positions is eight bytes, and a group that has never moved needs none. A reader
  learns them where it learns the generation, from the row.
- **A device has a seat**: the key it and its slices are drawn by, minted with it, committed in
  the pool map, and taken over by a device the operator names as its replacement. The device
  keeps an identity of its own, and comes up empty, as S4 requires.
- **A device has a placement weight beside its capacity**, equal to it unless the planner fits
  another. The planner fits only a pool whose devices differ in weight, against a sample of
  many groups, and commits the weights as one change. The statistical remainder is exceptions,
  which on the lab is none from four groups a tablet.
- **A pool's placement groups a tablet is a power of two**, fixed when the pool is made and
  chosen so one consumer puts about two thousand chunks on each device: one on the lab, eight at
  six hosts of twelve for 4+2, sixty-four at fifty hosts for 10+4. The prefix of a placement
  group is the tablet's twelve bits and log2 of that number, so it also bounds how far a tablet
  may later split under the pool before a split splits placement groups
  ([S5](placement.md#a-placement-group-is-a-sub-range-of-a-tablet)).
- **The failure domains placement reads are a device's id and its member's host**, the host
  from `cluster.failure_domains`. The field stays a list, so a rack is a third entry.
- **The pool map is pushed to nobody.** Each node derives it from the control group's committed
  commands and its shards share it. It keeps generations as change records, moves in flight
  one a device, the seats, the placement weights and the exceptions, and nothing for a placement
  group that is not moving or excepted.
- **A node caches a group's answer by generation**
  ([O87](../appendix/optimizations.md#o87-a-placement-answer-is-computed-again-on-every-lookup)).

## What X2 does not settle

- **How a commit checks a generation, and now its positions.** That is X1's model
  ([S16](testing.md#the-model)). The model's row holds the placement group's generation; it now
  also holds positions, and a move's commit is what changes both.
- **How a placement group's positions are written in a tablet group's state** and carried in its
  snapshot. That belongs to [S10](recovery.md#how-a-slice-learns-what-it-missed)'s record, at
  M14 and M16.
- **Growing a pool's placement groups.** Doubling them halves each group's sub-range, and each
  half is drawn afresh. What that moves was not simulated, and the number is fixed at the pool's
  creation until it is
  ([todos](../appendix/todos.md#growing-a-pools-placement-groups)).
- **The planner's fitting**: when it runs, how often, and how it keeps a fit current as the map
  changes. X2 fits once over 24 rounds and refits once after an add.
- **Racks.** No deployment has one ([S1](prerequisites.md#optional)).
- ~~**Ceph's own code.** How CRUSH keeps an erasure coded pool's positions, and what `upmap` and
  `crush-compat` do exactly, is [X14](spikes.md#x14-ceph-and-s3-at-the-source)'s reading. X2
  measured the ideas, not Ceph's implementation of them.~~ **Ceph's own code**, read and run by
  [X14](ceph-and-s3-sources.md#5-placement-indep-upmap-and-crush-compat) on X2's shapes through
  `crushtool`. Its `indep` moved 2.0 to 3.5 times the least for a device added, removed or
  reweighted, more than `rendezvous by position`, because it draws down the hierarchy. It moved
  1.00 to 1.02 times for a device marked out wherever the pool was narrower than its domains, and
  1.30 to 1.57 where it was as wide, and removing that device then moved 1.1 to 2.9 times the least
  again. Its straw2 filled within a few points of `rendezvous`. `upmap`
  balances counts of placement groups against a target in proportion to weight, one pair at a
  time inside the failure domain. `crush-compat` fits a second set of weights used only for
  placement. Neither was run.

## What it did not measure

- **Real devices, real moves or real bytes.** It counts chunks. What a move costs a device or a
  link is [X12](spikes.md#x12-recovery-and-scrub-rates)'s.
- **Many consumers at once.** Every table is one consumer, the worst case. Several consumers'
  groups in one pool add up, so their fill is better than one's, and a fit to a sample is what
  makes weights serve all of them.
- **Objects of mixed sizes.** A stripe's key spreads an object's stripes across groups whatever
  the object's size, so only the stripe count matters, and [a bucket of few
  stripes](#a-bucket-of-few-stripes) is that.
- **A cached lookup.** The cache is O87's to build and measure.
- **ARM or an Intel cpu.** The score is integer arithmetic, a table and one IEEE multiply, which
  is why it is the same everywhere. Its speed was measured on Zen1 and Zen4 alone.

## Related

- [X2](spikes.md#x2-placement-simulation) for what was planned.
- [S5](placement.md) for the design this settles in part.
- [S4](pools-and-devices.md) for devices, slices and seats.
- [S10](recovery.md#moves) for moves and the group's state.
- [S18](contract.md#q19-in-part-placement-2026-10-03) for the decision.
- [S1](prerequisites.md#the-order) for the two prerequisites it lets start.
- [C4](../distributed/tablet-map.md) and [Q11 and Q13 at M3](../distributed/protocol.md#q11-and-q13-at-m3)
  for the tablet map's frame.
- [X4's record](erasure-coding-crates.md) and [X5's](checksums.md) for the form this page copies.
- [O87](../appendix/optimizations.md#o87-a-placement-answer-is-computed-again-on-every-lookup) and
  [O88](../appendix/optimizations.md#o88-a-configured-set-lists-its-tablets-one-by-one) for the
  two findings that are a node's to act on.
