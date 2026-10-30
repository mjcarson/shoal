# F122. The scan oracle and the fixture's scan

M22 of the scans plan, the harness and the facts, is delivered. A scan's history can now be judged:
**a pure oracle in `shoal-model`** (`shoal_model::scan`) checks a scan's pages against every write
its tablet groups committed, P20 to P22 of [W13](../future-work/scans/contract.md) for each group
alone and nothing across groups, which is P23 kept as the oracle's own scope. **The fixture's `SCAN`
verb** walks tablets as a client's scan drives them, each page asked as a scan's at its group's
floor, and answers every page with what the oracle needs. **A fixture row holds a scan beside
writes, overwrites and removals to the oracle.** And **the scans plan's acceptance tables are held
to the book's check**, every name once across three parts and every test of a delivered gate a
function, which found one name the plan had written twice. The bench's scan kind and event
([F115](bench-scans.md)) and X16's baseline, M22's other two deliveries, landed ahead of it.

## Context

The scans plan's contract (W13, agreed 2026-10-29) states what a scan promises as properties a
history can be checked against: P20, every partition live at a group's floor and still live when the
scan reaches its key handed out exactly once; P21, no page below its group's floor and every row at
or after its page's boundary; P22, a page at `One` a whole voter copy's committed state. P23 says what
is not promised: no two groups at one moment, no order across tablets, no snapshot. M22 asks for an
oracle that checks P20 to P22 for each group and nothing across groups, "so no test can lean on what
P23 withholds", before any engine change: M23 to M25 change the walk, and their tests need a judge
that was not written beside the code it judges.

C11's oracle (`shoal_model::oracle`) was the model: pure, deterministic, its scope one tablet,
naming no shoal crate. The byte ledger (`shoal_model::bytes`, M11) showed how a pure judge meets a
real cluster: the test records what it was told and the ledger judges it.

## What it does

- **The history** (`ScanHistory`): the tablets the scan walks, each with its group; every committed
  write (`ScanWrite`: group, tablet, key, the index its session token named, the value after it or
  none for a removal); and every page the scan used (`ScanPage`: group, tablet, boundary, the ticks
  it was asked and answered at on one clock, the cursor it began after, the cursor the next page
  begins after, and each partition with its value). A value is a number the test chose.
- **The checks** (`check`), group by group in ascending order, each group's in a fixed order, the
  first violation returned and every violation naming its group:
  - every page is of a walked tablet, under the tablet's own group (`OutsideItsPage`);
  - **P20**: no partition handed out twice (`HandedOutTwice`);
  - each tablet's pages join from its start to its end, once each (`Unwalked`);
  - every partition inside its page's cursors and its tablet (`OutsideItsPage`);
  - **P21**: no page's boundary below its group's floor when it was asked, the highest boundary of
    the group's pages answered before (`BelowFloor`);
  - **P22**: every value one a committed write gave its partition (`NotCommitted`), and **P21** a
    value its partition held at its page's boundary or after it (`OlderThanBoundary`);
  - **P20**: every partition live at the group's floor and never removed after it handed out
    (`Missed`), the floor here being the group's lowest boundary, where its scan began.
- **The fixture's `SCAN` verb**, `SCAN <table> tablets=<a>,<b>,... [limit=<n>] [pause=<ms>]
  [marker=<text>]`, walks each tablet to its end on the node it is sent to, every page asked as a
  scan's (`WalkClass::Scan`), a tablet's first page with no floor and each after it at its group's
  floor. A page refused for now (`is_transient`) is asked again and is not one of the scan's pages.
  Each partition's value is read out of its archived bytes after a marker (`marked_value`). It
  answers within twenty seconds, under a reply's thirty, and says `finished: false` if it ran out of
  them or a page was refused for good.
- **The fixture's row**, `a_scan_beside_writes_is_held_to_the_oracle`: 240 notes in four tablets,
  then 80 overwritten, 40 removed and 40 written new while one node's copy is scanned four partitions
  a page, each write's token kept; the history goes to the oracle, and a failure leaves it as JSON
  under `CARGO_TARGET_TMPDIR` to replay.
- **The acceptance check** (`shoal-bench/tests/acceptance_tables.rs`) reads the scans plan's tables
  beside the other two parts', against `future-work/scans/milestones.md` and M22 to M28, a table's
  milestone column read from its header (`milestone_column`), since the plan's tables put the gate
  second.

## Design choices

- **Judged by values a test chose, not bytes.** The oracle names no shoal crate, as the model's other
  judges do, so a schedule or a history needing the engine to replay is never what it holds. A note's
  text carries its value, which the verb reads back.
- **The floor of P21 is the scan's knowledge, by its clock.** A page's floor is the highest boundary
  of the group's pages answered before it was asked: what the scan could have passed on. Two pages in
  flight may be answered in either order, and neither is held to the other's boundary.
- **The floor of P20 is where the group's scan began**, its lowest boundary: a partition live there
  and never removed after is owed, and one made or removed meanwhile may or may not be handed out, as
  P23 allows.
- **Handed out twice is checked before the pages join**, since a page asked again of another copy and
  used beside the first answer is the violation P20 names, and it shows as two pages at one cursor.
- **The verb answers synchronously, bounded.** A start and status pair would need the scan to outlive
  the command on a thread of its own, and the child's pool is neither `Sync` nor shared; twenty
  seconds is ample for the pages a fixture row asks for.

## Alternatives rejected

- **A status verb beside a background scan**, as `REPAIR` and `REPAIR_STATUS` are. The pool is owned
  by the child's command loop and stopped by value at its exit, and `ShoalPool` is not `Sync`; sharing
  it would need an engine change, which M22 makes none of.
- **Pages in flight together from the verb.** For the same reason; the bench's driver
  ([F115](bench-scans.md)) asks many at once through clients, and M24's driver will.
- **Decoding a partition's row in the child.** The partition types are private outside the bench
  feature; a marker in the row's text is read where rkyv lays it out whole.
- **Checking a scan against one moment of the whole table.** That is a snapshot, which P23 refuses to
  promise; the oracle's tests hold it to accepting two groups read at different moments.

## Limitations

- **The verb asks one page at a time**, on one node, so P21's floor is never met across copies and
  P20's once is never met across a retried page here; those are M25's rows, and the oracle's own
  tests reach them.
- **Values only for a table whose rows carry the marker.** The fixture's `Note`; a sorted table's
  partition is many rows, and its value would have to be the test's own digest of them.
- **No move, failover or install under the scan.** The history gives a tablet one group; M25's rows
  bring the rest.
- **Unknown outcomes are not modelled.** Every write in the history is one its client was answered;
  a write that timed out has no place yet, and the fixture's row makes none.

## Invariants to uphold

- **The oracle relates nothing across groups.** Each group is judged on its own pages and writes; a
  check that reads another group's boundary or order is P23 broken in the judge.
- **`shoal-model` names no shoal crate**, so the oracle can judge a cluster that does not build.
- **A test name appears on one page of the book**, whatever part it is in, and every test of a
  delivered gate is a function in the workspace.
- **The verb passes a group's floor once it knows the group**, and reports every page it used and no
  page it did not.

## Performance

Not applicable: nothing on a node's path changed. The fixture's row takes about fifteen seconds,
most of it the cluster's start and 400 writes one at a time.

## Tests

| Test | Proves |
| --- | --- |
| `scan_oracle_promises_nothing_across_groups` (`shoal-model/src/scan.rs`) | W13's M22 row: two groups read at different moments, tablets interleaved, are accepted; a partition twice, a page below its floor and a row older than its boundary are each refused within its group |
| `scan_oracle_refuses_what_a_scan_missed_or_made_up` (same) | A partition owed and missed, a value never written and a tablet unwalked are refused; one removed after the floor may be missed, and a later write may be read |
| `a_scan_beside_writes_is_held_to_the_oracle` (`shoal/tests/cluster_fixture.rs`) | A scan of a running cluster's copy beside overwrites, removals and new rows keeps P20 to P22 |
| `scan_acceptance_tables_name_real_tests_and_valid_gates` (`shoal-bench/tests/acceptance_tables.rs`) | The scans plan's rows name one gate each, its section names their page, every name is unique across three parts, and M22's tests exist |
| `acceptance_cells_drop_struck_names_and_match_whole_numbers` (same) | A table's milestone column is read from its header |

## Related

[W13](../future-work/scans/contract.md), the contract; [the milestones](../future-work/scans/milestones.md#m22-the-harness-and-the-facts);
[F115](bench-scans.md), the bench's scan kind and event; [X16](../future-work/scans/walk-beside-the-foreground.md),
the baseline; [F87](tablet-walk.md), the walk the verb drives; [F36](cluster-harness.md), the fixture
and the model.
