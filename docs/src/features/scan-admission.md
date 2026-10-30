# F116. An admission class apart from the drivers'

A walk page is now admitted by its **class**. A shard reads at most two pages of its own drivers
and an operator's walks at once, as it did, and at most `scans.pages_in_flight` pages asked as a
scan's, two by default, each class shedding only its own: a client's scan that fills its count is
refused `Shedding` by its class's name, and the backfill that brings a stale copy current is read
beside it. A page names its class on the admin walk (`"class": "scan"`); a page that names none is a
driver's or an operator's, as every page was, so no driver, the lab's loader and no older client
changed. A node can answer no scan at all (`scans.enabled: false`), and a scan's page holds at most
4 MiB whatever it asks. The bench's scan ([F115](bench-scans.md)) asks its pages as a scan's, and
refuses a cluster member whose build would take them as a driver's.

## Context

This is [W1](../future-work/scans/prerequisites.md#an-admission-class-apart-from-the-internal-drivers)'s
admission row, one of the four the scans plan's M23 begins with, built before M23 by the user's
decision of 2026-10-29 that the four engine rows apply to pages asked as scans only, the drivers
untouched until M23 moves them.

A shard read two walk pages at once and refused the rest `Shedding`
(`PAGES_IN_FLIGHT`, `shoal-core/src/server/shard/walk.rs`), whoever asked: F88's backfill and copier,
F89's light scrub, F92's reclaimer, F93's archive backup, an operator's walk and, since F115, the
bench's scan. Backfill fails its attempt on any refusal; the scrub, the copier and the reclaimer ask
a shed page again a hundred times, 20 ms apart. So a scan driving every shard's two pages at once, as
[W6](../future-work/scans/distribution.md) has a client's scan drive them, would shed the walk that
recovery waits on, and the object store's durability would wait on someone's export. W1 calls the
count part of the protocol a driver obeys, to be fixed once, before clients exist.

## What it does

- **A class on every page.** `WalkClass` (`shoal-proto/src/shared/protocol/admin.rs`) is `Driver` or
  `Scan`, `Driver` by default. `AdminKind::Walk` gains `class`, left out of a request that names
  none; `WalkAsk` (`shoal-core/src/server/shard/walk.rs`) carries it, `WalkAsk::as_scan` asks a page
  as a scan's, and `WalkAsk::of_admin` reads an absent class as a driver's.
- **A count a class.** A page is admitted by its class's count on the executor hosting its tablet:
  `PAGES_IN_FLIGHT`, two, for a driver's, `scans.pages_in_flight` for a scan's. A page past it is
  refused `Shedding`, saying `this shard is reading 2 scan pages already`. The other class's pages
  are not counted against it.
- **Scans off.** A node whose `scans.enabled` is false refuses a scan's page `InvalidRequest`,
  `this node answers no scan's pages`, which a scan does not ask again; its drivers' walks go on.
- **A scan's page is held to 4 MiB** (`SCAN_MAX_BYTES`), whatever its `bytes`: once its reads wait
  for a device's idle time (F119) and draw on a budget (F118), a page of a few seconds of a busy
  device's trickle at most still lands inside an admin read's deadline.
- **Advertised.** `walk_class` joins the `Members` frame's `admin_reads`. The bench's scan driver
  asks every member known as a cluster's for it first and refuses a member without it: an older
  node ignores the field and admits the scan's pages as a driver's, which is the failure this
  feature removes. A node alone answers no `Members` and runs no driver a scan could shed, so it is
  not asked.
- **Settings.**

  ```yaml
  scans:
    enabled: true          # default; whether this node answers a scan's pages at all
    pages_in_flight: 2     # default; scan pages one shard reads at once before it sheds another
  ```

  A node refuses at start `pages_in_flight: 0` while scans are answered, naming `scans.enabled` as
  the way to answer none. A deployed node takes the section from its inventory's `scans:` since
  2026-10-30, rendered into its file as written and judged by its start
  ([the shoaladm page](../operations/shoaladm.md#scan-settings)); before, it ran the defaults.

- **The fixture's `WALK` verb** takes `class=scan` or `class=driver`.

## Design choices

**Two counts, not one larger one.** A larger shared count would let a scan take every slot a
driver had; the point is that neither class can take the other's.

**A scan's count is a setting; the drivers' stays a constant.** The drivers' count is part of how
they pace themselves and is fixed with them; a scan's is an operator's lever, the first of the
settings [W7](../future-work/scans/isolation.md#what-an-operator-sets) lists.

**An absent class is a driver's.** Every request written before this feature, the lab loader's
check of copies and an operator's walk keep exactly what they had; only a request that says it is a
scan's is counted as one.

**Scans off is `InvalidRequest`, not `Unavailable`.** `Unavailable` is a refusal for now, which the
bench and the drivers ask again; a node that answers no scans will not answer the next ask either.

**The cap on a scan's bytes is the page's, not the request's.** A request asking more is answered
with a page of at most 4 MiB, as a page asking more than a tablet holds is answered with less; a
refusal would make a client learn the cap by failing.

**Counting the pages of a class by looking.** A shard reads a few pages at once, so filtering its
map by class costs less than keeping a second count right on every way a page ends.

## Alternatives rejected

**A wire version for the class.** Every field of the admin walk after its table and tablet is
`#[serde(default)]` and `AdminKind` has no `deny_unknown_fields`, so an older node decodes a
request naming a class and ignores it; what goes wrong is admission, not decoding, and the
`walk_class` admin read is how a client tells, as every other admin read is told.

**A count a principal.** One tenant's scans could still take every shard's scan count; that is a
later addition that changes no frame ([W7](../future-work/scans/isolation.md#admission)).

**Admitting a scan's page on the executor that read the request.** The count belongs to the
executor whose loop captures and reads the page, the one hosting its tablet, as the drivers'
always has.

## Limitations

- ~~**Only the admission moved.** A scan's page is captured and read exactly as a driver's is: its
  capture on the loop, its reads in the medium queue beside every get's, unbudgeted and unpaced,
  until W1's other three rows, [the scan class](../future-work/scans/prerequisites.md#a-scan-class-on-every-table-shard),
  [the budget](../future-work/scans/prerequisites.md#a-budget-for-transient-read-buffers) and
  [the device pacing](../future-work/scans/prerequisites.md#table-reads-paced-by-a-devices-idle-time).~~
  **W1's other three rows have landed since.** Since [F117](scan-class.md) a scan's page is captured
  from the loop's second inbox and read in the scan queue, since [F118](scan-budget.md) its buffers are
  drawn from the node's scan budget, and since [F119](scan-pacing.md) its archived reads wait for their
  device's idle time or its trickle.
- **Admin only.** A scan's page is still an admin walk, a `cluster.admins` principal's on a cluster;
  the `scan` privilege is M24's.
- **No figure counts a class.** A shed is in the refusal and in the bench's windows
  ([F115](bench-scans.md)); a node's counters of walks and scans are M23's.
- **No count a principal**, so one admin's scans take a shard's whole scan count.

## Invariants to uphold

- **Client scans never take a driver's admission**: a page is counted only against its own class's
  count, and the drivers' count is `PAGES_IN_FLIGHT` whatever any setting says.
- **A page that names no class is a driver's**, so no request written before this feature changes
  what it is admitted by.
- **A scan's page is never sent to a cluster member that does not advertise `walk_class`.**
- **A node answering no scans refuses a scan's page by a code no scan asks again.**

## Performance

Nothing on a query's path changed. A walk page's admission filters a map of a few entries by class
where it read its length; a page of either class is otherwise read as it was. No lab figure: the
class changes which page is shed, not what any page costs. [X16](../future-work/scans/walk-beside-the-foreground.md)'s before and after, which takes
this row with the other three, found the cell alone the same on both sides (read p99 7.68 and
7.64 ms, insert p99 24.7 and 24.5 ms, ranges overlapping).

## Tests

| Test | Proves |
| --- | --- |
| `client_scans_never_shed_a_drivers_page` (`shoal/tests/scan_admission.rs`) | With a shard's two scan pages held in flight, a third is shed naming the scan class and a driver's page of the same tablet is read and answered; before this feature it was shed. W7's M23 row, proved now |
| `a_node_answering_no_scans_refuses_a_scans_page_by_name` (same) | Scans off refuses a scan's page `InvalidRequest` naming `scans.enabled`, and a driver's is answered |
| `no_scan_page_in_flight_is_refused_at_start` (same) | `pages_in_flight: 0` with scans answered refuses the node's start by name |
| `a_scan_page_holds_at_most_its_bytes` (`shoal-core/src/server/shard/walk.rs`) | A scan's page holds at most 4 MiB, a driver's what it asked |
| `an_admin_walk_without_a_class_is_a_drivers` (same) | An admin walk naming no class is the driver's page it was; one naming a scan's is a scan's |
| `a_walk_names_its_class_only_for_a_scan` (`shoal-proto/src/shared/protocol/admin.rs`) | A driver's walk request writes no `class`, a scan's reads back as one |

The hold the first uses, `faults::hold_walk_reads`, keeps a table's pages of one class in flight
before their reads and counts how many it holds, so the test waits on the count and not on a time.

## Related

[W7](../future-work/scans/isolation.md#admission) for the design; [F87](tablet-walk.md) for the walk;
[F115](bench-scans.md) for the bench's scan that asks as one; [F88](recovery-and-moves.md),
[F89](scrub-and-repair.md) and [F92](reclamation-and-device-lifecycle.md) for the drivers it keeps
their pages for.
