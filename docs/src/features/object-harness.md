# F78 — M11, the object store's harness and the facts

## Context

[M11](../object-storage/milestones.md#m11-step-0-the-harness-and-the-facts) is the object
store's first gate, and like [M0](../distributed/milestones.md#m0-step-0-the-harness-and-the-facts)
before it ([F36](cluster-harness.md)) it stores nothing. It builds what every later gate is judged
by: the stripe model and its schedules, the fixture's storage faults, a byte ledger, a driver that
runs a schema's own operation kinds and counts bytes, and a check on this part's acceptance tables.
Its eight acceptance rows are on [S15](../object-storage/performance.md#acceptance-tests),
[S16](../object-storage/testing.md#acceptance-tests) and
[S18](../object-storage/contract.md#acceptance-tests).

Most of it landed ahead of the gate, each piece as a change of its own:

- [X1](../object-storage/stripe-model.md) built the stripe model in `shoal-model/src/stripe/`,
  with thirty-three saved schedules, and both of its tests:
  `object_model_preserves_acknowledged_bytes` and `every_unsafe_policy_has_a_saved_schedule`;
- [F69](driver-operation-kinds.md) taught the driver the kinds a schema supplies and the bytes on
  the wire both ways, with `driver_runs_kinds_a_schema_supplies`, `driver_counts_bytes_both_ways`
  and `table_arm_ids_are_unchanged`;
- [F70](storage-faults.md) built three of S16's five faults - a torn write, a full disk, a lost
  device - and `device_faults_do_what_they_say` for them.

Three things were left, and this feature is them. The byte ledger did not exist, so
`byte_ledger_judges_the_three_outcomes` named no function. `device_faults_do_what_they_say` asserts
"an emptied one is empty, a flipped bit is flipped" on its row, and F70 had built neither, saying
so ([S1](../object-storage/prerequisites.md#the-order)): they were not prerequisites. And
`acceptance_tables.rs` read the distributed chapter alone and knew M0 to M10c, so nothing held this
part's tables to anything and `object_acceptance_tables_name_real_tests_and_valid_gates` named no
function either. The gate's evidence also asks that a table capture from before F69 and one from
after it be accepted by `compare` as one benchmark; F69 showed it on 2026-10-03, and since then
later features have added to what a capture holds and [F74](client-routing.md) changed a spec's
default routing, so it was taken again.

## What it does

**A byte ledger, `shoal_model::bytes::ByteLedger`.** S16's write ledger for ranges of bytes
([the fixture](../object-storage/testing.md#the-fixture)). A test records each write to an object -
its offset, its bytes, the step it was sent at - and later what it was told, `Acknowledged`,
`Refused` or `Unknown`; and each answered read, with the bytes it got, its level (`Default` or
`Strong`, [P12](../object-storage/contract.md#the-contract)) and the steps it was sent and answered
at. `finish` makes every write still waiting unknown. `judge` then holds the history to the three
outcomes' contracts, object by object:

- an acknowledged write took effect, whole, before it was answered, so a strong read that begins
  after it shows it or something newer;
- a refused write never took effect, so no read shows it;
- an unknown write took effect whole or not at all, and never in part.

The ledger keeps no bytes. It cuts every range into granules of a size it is made with and keeps
each granule's CRC-64/NVME; a range starts and ends on a granule's edge, and a granule nothing wrote
reads as zeros. A history is right when one total order of its writes and reads explains every read:
real time binds the order, except that nothing has to come before a default read, which may be
stale; every acknowledged write and every read is in it, an unknown write may be, a refused write
never is; and a read shows, granule by granule, the last write before it in the order. Before that
search four checks run that each prove a violation on their own, and so can name the read and the
write at fault:

| Violation | What it proves |
| --- | --- |
| `Unwritten` | A granule of the read matches no write and is not zeros: bytes made up, or torn |
| `RefusedRead` | A granule matches only a refused write |
| `Missing` | A strong read shows, where an acknowledged write landed before it began, only what that write replaced |
| `Part` | A read shows a write in one granule and, in another the write covered, only what it replaced |
| `NotLinearizable` | No order explains the reads: an unknown write seen and later gone, two reads that disagree on which of two writes came last, a mixture of two writes |
| `TooManyOps` | An object past the search's bound of 64 writes and reads |

A record the ledger cannot judge is refused when it is made, as a `LedgerError`: a range off a
granule's edge, a range of nothing, a write completed twice, a read answered before it was sent.
The ledger serializes, so a history the judge refused can be written beside a failing test's logs
and judged again.

**Two more faults, `shoal/tests/cluster/faults.rs`.** S16's last two, applied by the test process
through `std::fs` rather than armed in a child:

- `empty_dir` removes everything under a directory and keeps the directory, which is what a
  replaced disk mounted at the old path looks like, and syncs the removals through it.
  `Cluster::empty_dir(id, rel)` refuses while the node is up: the fault is defined while its child
  is down;
- `flip_bit` changes one bit of one byte of a file in place, `fdatasync`s it, and returns the byte
  before and after. A byte past the end and a ninth bit are refused, so nothing is ever appended.
  `Cluster::flip_bit(id, rel, offset, bit)` applies it to a node's storage.

`device_faults_do_what_they_say` gained both, after its torn, full and lost steps and the recovery
that follows them. Node one is stopped and one bit of the middle byte of its largest archive is
flipped: read back, the file has its length and differs in that byte alone, by that bit. Emptying
node zero's storage while it runs is refused. Node two is stopped and its storage emptied: the
directory is there and holds nothing, and node one's files are as they were. Two cheap self-tests
hold each fault to its name on a directory of their own.

**The acceptance check reads both parts.** `shoal-bench/tests/acceptance_tables.rs` parses a
`Chapter` - its directory under `docs/src`, its milestones and the umbrella gates written in a
substage - so one parser serves both. `acceptance_tables_have_unique_tests_and_valid_milestones`
holds the distributed chapter as before; `object_acceptance_tables_name_real_tests_and_valid_gates`
holds this part's 108 rows to the same rules: a name once across both parts, a row's gate one of
M11 to M21 whose section names the row's page, and every test of a delivered gate a function in the
workspace. A struck-through test name, which a renamed row keeps beside its new one, is dropped
before parsing, and a page's number is matched whole, so `S1` is not named by `S16`.

M11's section on the milestones page now opens `**Delivered**`, so the check requires all eight of
its tests, and they exist.

## Design choices

**The ledger lives in `shoal-model`.** S16's invariant is that the ledger "shares no code with
what it judges", and the crate holds it by construction: it depends on `serde` and `serde_json`
and names no shoal crate. C11's ledger and oracle for the tablet model are there already, `shoal`
takes the crate as a dev-dependency, which is where M13's and M15's fixture tests will record into
it, and `cargo test -p shoal-model` runs while the engine does not build.

**A checksum a granule, of its own code.** A checksum of a whole range cannot tell a write shown
in part from one absent, or judge a read of part of a range; a checksum a granule can do both
without keeping the bytes, and S16 asks for a checksum. The function is CRC-64/NVME, written in
`bytes/crc.rs` from its six parameters with a table built at compile time and held to the two check
values X5 recorded. It is the definition M13 adds to the product through `crc-fast`, so a figure a
failure prints is the one a slice would store for the same bytes, but none of the product's code.

**A search, with checks in front of it.** A read judged on its own cannot see a write that one
strong read shows and a later one does not, or two reads that order two writes both ways; only an
order of the whole object's history can. The search is C11's: try each write that can go next,
remember every point explored, bound the operations an object. A read changes nothing, so it is
placed as soon as the state explains it and every operation real time puts before it is placed,
which loses no order and keeps the search to the writes. Its answer cannot name a culprit, so four
checks run first that each prove a violation alone and name one; a history they pass still goes to
the search.

**Content, not identity.** A read is compared with what each granule holds, by checksum, so two
writes of the same bytes need no tie-breaking and a history that writes the same thing twice is
judged without help.

**The two faults are done from outside.** An emptied device is defined while its node is down, so
there is no child to arm it in, and a flipped bit is something a device does to bytes a node has
already written, not a call the node makes. Both are a few lines of `std::fs` in the fixture,
which puts no fault code in the product.

**One parser over a `Chapter`.** The object part's tables are written to the distributed chapter's
rules, so a second test file would have been a second copy of the parser to keep in step. The
umbrella substages (M9 and M10, whose gates are written in M9a and M10a) became data, and the object
part has none.

## Alternatives rejected

**Keeping the bytes.** Exact at any alignment, and the ledger would hold everything a test wrote. A
fixture at M15 writes stripes of mebibytes, and M16's backfill more, where a checksum a 64 KiB unit
is eight bytes.

**gxhash or `crc-fast` as the checksum.** gxhash is what the engine checksums its own records with
(`checksum_of` in the WAL), so the ledger would share the code it judges; `crc-fast` is what M13
adds to the product for the same reason in reverse. Neither is in `shoal-model`'s graph, which is
`serde` alone.

**The ledger in the fixture's module tree.** `shoal/tests/cluster/` is compiled into
`cluster_fixture` and nothing else, and its children are the engine; M13's driver test would have
had no way to reach it.

**The stripe model's oracle.** `StripeLedger` judges symbols - a unit is `Write(OpId)`, `Zero`,
`Torn` - against the row states the model keeps. A fixture has bytes and no row to consult.

**Bytes at any offset.** A granule shared by two writes would need its bytes, or a checksum of
every sub-range, to judge. A range off a granule's edge is refused by name instead, and a test of
an object whose size is not a multiple of its unit chooses a granule that divides it.

**The emptied and flipped faults as `FAULT_DIR` verbs.** A verb runs in a child, and an emptied
device has no child; a flip through the engine (the `CORRUPT` verb, [F44](repair.md)) needs a
compacted partition, a live node and the engine's own archive code, while S16's flipped bit is for
any file a node wrote, stripe chunk units from M14 on.

**Testing what a node does with them now.** A node whose only storage directory was emptied claims
it as new ([Resolved #46](../appendix/resolved/unmarked-directory-refused.md)), and a flipped bit
in an archive is [F44](repair.md)'s, already tested through `CORRUPT`. What the object store makes
of each is M14's (a replaced device refused by its claim) and M17's (a scrub that finds it), and
the faults are now there for those gates to use.

**Matching a page's number as a substring.** The old check accepted `S1` wherever `S16` appeared.
Both parts pass the whole-number match, so it was made stricter at no cost.

## Limitations

- **Nothing records into the ledger yet.** No object byte is stored before M13, so at M11 the
  ledger has judged histories written by hand and nothing else. Its first users are M13's
  `every_acknowledged_byte_is_read_back` and M15's fixture tests.
- **Ranges start and end on a granule's edge.** See above.
- **No size.** A granule nothing wrote reads as zeros, and a read is judged for the bytes it was
  answered. What a read past an object's end returns, and a truncate, are S3's and M15's to record
  on top of it.
- **A default read is bounded by nothing.** It may show any state the order passed through before
  it completed; a session that holds a reader to what it already saw is not modelled.
- **A retry is one write.** A caller records it once, invoked at its first attempt and completed
  when an attempt is answered; the attempts themselves are not kept, as C11's ledger keeps them.
- **Each recorded write is atomic.** That is the contract for one stripe. A write that spans
  stripes is recorded as one write a stripe, since P19 promises no atomicity across them.
- **Sixty-four operations an object.** Past it the ledger answers `TooManyOps` rather than search.
  The search is exponential in writes that overlap in time; real time keeps it small in practice.
- **The two new faults are judged on disk.** What a node does with an emptied or flipped directory
  is not tested here, and a child still has one storage directory: devices come at M14.
- **A flip's sync is a call.** Only a power cut shows that `fdatasync` put the byte on the device;
  the test holds the byte read back.
- **`compare` refuses captures across a kernel update.** Each node's memory is compared to the
  byte, and MemTotal moves by kilobytes with the kernel
  ([item 226](../appendix/known-issues.md#226-compare-refuses-two-captures-whose-nodes-memory-differs-by-kilobytes)).

## Invariants to uphold

- **`shoal-model` names no shoal crate.** `cargo tree -p shoal-model | grep -c glommio` is 0. The
  ledger's separation from what it judges rests on it.
- **The ledger's checksum is its own code**, and holds CRC-64/NVME's check values.
- **A check in front of the search reports only what proves a violation alone.** One that could
  fire on a history some order explains would refuse a right history; the search is the judge of
  everything else.
- **A read has no effect on what a granule holds.** Placing a read as soon as it fits is what
  keeps the search to the writes, and it is sound only while that holds.
- **An unknown write has no completion and is never required; a refused write never takes
  effect; nothing has to come before a default read.** Those three lines are the outcomes' and the
  read levels' contracts in the search.
- **A directory is emptied only while its node is down.**
- **A milestone is delivered when its section's first words are `**Delivered`.** The check reads
  nothing else, for either part.

## Performance

**The ledger** judges every history in its tests in under a millisecond, the 64-operation one at
the bound included. Nothing on a node's path changed.

**The capture compared again.** On 2026-10-11, `shoaladm bench run` took F69's spec once more -
`read100,insert100`, bundles 1 and 16, two runs each, ten seconds measured after two of warmup,
seed 7 - with `--routing endpoints`, which is what a spec from before
[F74](client-routing.md) reads back as. It ran on a side cluster of the bench's own built from
F69's copy of `tmdb_cluster.yaml`, on europa, titan and hyperion beside the running tmdb cluster,
from a clean worktree at `064a2d1`, the commit M11 is built on. M11 changes no crate the bench
builds, so that is M11's driver and node. `shoaladm bench compare` then compared it with F69's
capture from before the driver's change, taken at `1b16939`:

| Fact | F69's before (`1b16939`) | M11's (`064a2d1`) |
| --- | --- | --- |
| Spec digest | `17ffbd62…` | `17ffbd62…` |
| Schema | `Catalog`, fingerprint `9254393988939342253` | the same |
| Inventory shape | `e46a45e7…` | the same |
| Dataset digest, format, mode, flavor, driver | | the same |
| Nodes' memory | europa 47,002,193,920, titan 15,120,388,096 | europa 47,002,177,536, titan 15,120,396,288 |

`compare` refused one fact, `nodes`, and only for memory: europa's MemTotal was 16 KiB less and
titan's 8 KiB more, hyperion's the same, after a kernel update between the two (europa's went from
7.0.0-31 to 7.0.0-34). With `--allow nodes` it joined every arm by id - `read100/b1/none`,
`read100/b16/none`, `insert100/b1/none`, `insert100/b16/none` - and none was absent on either
side. The columns added since the before side was taken, F69's bytes and
[F71](bench-device-memory.md)'s device and memory figures among them, read `not on both sides`. So the driver's change and everything after it left a table workload the same benchmark,
with no arm id moved, which is the gate's clause; the memory fact is filed as item 226 rather than
waved through.

The rates are not a result: europa compiled the cluster fixture while the capture ran, and nine
features landed between the two sides. The insert arms ran out of rows inside their warmup on both
sides, as F69 recorded, since the committed dataset is two thousand rows.

**The faults** run on a stopped node, so they cost a node nothing. `device_faults_do_what_they_say`
passed on europa in 6.3 seconds with both new steps.

## Tests

| Test | What breaks if the feature is reverted |
| --- | --- |
| `byte_ledger::byte_ledger_judges_the_three_outcomes` | S16's row: an acknowledged write a strong read misses, a refused write read, an unknown write read in part over zeros and over what it replaced, an unknown write seen and then gone, two writes read as a mixture, and damaged bytes each named as their violation, beside the five histories that are right, a stale default read among them |
| `byte_ledger::byte_ledger_refuses_unaligned_ranges` | A range off a granule's edge, a range of nothing, an answer before its question, a write told twice or never recorded, each refused and none kept |
| `byte_ledger::byte_ledger_judges_objects_apart` | A write to one object explaining, or condemning, a read of another |
| `byte_ledger::byte_ledger_orders_concurrent_writes` | Two writes overlapping in time read in either order, but not in both by two reads; a granule only one of them wrote missed; default reads of earlier states; the same bytes written twice |
| `byte_ledger::byte_ledger_bounds_its_search` | Sixty-four operations judged, sixty-five refused as `TooManyOps` |
| `byte_ledger::byte_ledger_is_judged_again_from_json` | A history and its verdict surviving a round trip through JSON; `pending` and `finish` |
| `bytes::crc::tests::crc64_nvme_meets_its_check_values` | The ledger's CRC-64/NVME against the catalogue's check value and 4,096 zeros |
| `cluster_fixture::an_emptied_directory_keeps_itself_and_nothing_under_it` | The directory removed with its contents, a sibling touched, or a missing directory made |
| `cluster_fixture::a_flipped_bit_changes_that_bit_alone` | Another byte or bit changed, the length moved, or a byte past the end appended |
| `cluster_fixture::device_faults_do_what_they_say` | S16's row, now all five: a flipped bit on a stopped node's archive changing that bit alone, an emptied node's storage there and empty with the other nodes' untouched, and emptying a running node refused |
| `acceptance_tables::object_acceptance_tables_name_real_tests_and_valid_gates` | S16's row: this part's tables held to nothing, a name used in both parts, a row naming a gate whose section does not name its page, or one of M11's eight tests missing |
| `acceptance_tables::acceptance_cells_drop_struck_names_and_match_whole_numbers` | A renamed row read as its struck name, or `S1` taken as named by `S16` |
| `acceptance_tables::acceptance_tables_have_unique_tests_and_valid_milestones` | The distributed chapter's check, now through the same parser and the whole-number match |

M11's other five rows were built ahead of the gate and are on their own pages:
`object_model_preserves_acknowledged_bytes` and `every_unsafe_policy_has_a_saved_schedule`
([X1](../object-storage/stripe-model.md#what-was-run)), `driver_runs_kinds_a_schema_supplies`,
`driver_counts_bytes_both_ways` and `table_arm_ids_are_unchanged` ([F69](driver-operation-kinds.md#tests)).

## Related

- [M11](../object-storage/milestones.md#m11-step-0-the-harness-and-the-facts), the gate this
  delivers, and [What's left to do](../object-storage/whats-left-todo.md), where its box is green
- [S16](../object-storage/testing.md), which asks for the ledger, the faults and the check;
  [S15](../object-storage/performance.md) and [S18](../object-storage/contract.md) for the other
  rows
- [X1](../object-storage/stripe-model.md), [F69](driver-operation-kinds.md) and
  [F70](storage-faults.md), the gate's work done ahead of it
- [F36](cluster-harness.md), M0's harness, which this follows; C11's ledger is
  `shoal_model::oracle`
- [X5](../object-storage/checksums.md) for CRC-64/NVME
- [Item 226](../appendix/known-issues.md#226-compare-refuses-two-captures-whose-nodes-memory-differs-by-kilobytes),
  filed on the way
