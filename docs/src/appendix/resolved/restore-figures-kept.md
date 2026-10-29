# 327. A group's restore finished by a drive that never installed it reported nothing restored

A restore drives each group of a backup through three phases, each committed before the next, so
a driver that stops leaves a phase the next leader resumes from
([F49](../../features/backup-and-recovery.md)): loading, installing (a file at a boundary of the
run's own, on every member), and verifying (a scrub every member must pass). The group's outcome,
`Restored { boundary, records, bytes, retries, verified }`, is what an operator and a test read to
say what came back.

Filed and fixed in one change (2026-10-27), from the flaky-test plan's reading of item 142's most
frequent row.

## Symptom

`single_node_data_has_a_verified_cluster_migration_path` failed in 21 loaded runs from 2026-10-11
to 2026-10-27, the most of any row on [Tests That Fail Under Load](../flaky-tests.md). Each time
every group of the restore came to `Restored`, verified by its scrub, and the row's sum of the
groups' records was short of the export's 400: from 0 to 283. It was followed as one of
[item 142](../known-issues.md#142-two-fixture-tests-fail-intermittently-on-an-idle-host)'s deadlines,
*judged its restore before the records landed*.

It was not a deadline. Any restore whose group was finished by a drive that did not install it
says the group restored nothing: `boundary: 0`, `records: 0`, `bytes: 0`. That happens when the
installing drive loses its lead, which a group restarted by its own install often does, and since
[item 297](restore-rides-out-a-moment.md) when a verifying scrub is driven again. The rows were
there; the record was wrong.

## Cause

The outcome took its figures from the manifest of the file the drive built, a local of the drive
(`restore.rs`):

```rust
let (boundary, records, bytes, retries) = installed.as_ref().map_or((0, 0, 0, 0), |manifest| ...);
```

A drive that starts at `Verifying`, the phase the record names after the install, never built a
file, so it reported zeros. `GroupRestore` kept nothing of the install for it to read. The
migration row's three groups hold about 133 of the 400 records each, so a group finished by
another drive took a third of the sum with it: 283, 267, 150, 133 and 117 are the sums of whole
groups.

## Evidence

**Established by reading the source and reproduced.**
`a_restore_resumed_at_verifying_reports_what_it_installed` was written first. Every node of the
restoring cluster hands its first group restore back right after its `Verifying` commit, as a
driver that loses its lead does (`HAND_BACK_RESTORES`, `faults::hand_back_restores`), so every group
is finished by a drive that did not install it. On the tree before the fix:

```text
group 16812314358545672396 reported no file installed: {"attempts":0,"driver":"45283253-87ff-4c2c-be93-029e8bc5f059","failed_in":null,"files":["Note/492529fcdc31a748-28.snap"],"generation":0,"outcome":{"Restored":{"boundary":0,"bytes":0,"records":0,"retries":0,"verified":5}},"phase":"Done"}
```

The file was installed and verified at index 5, and the record says nothing was.

## The fix

- **A group's record keeps the file it installed** (`GroupRestore::installed`, an `InstalledFile`
  of its boundary, records, bytes and retries). It is set in the commit that names `Verifying`.
- **Every later commit carries it.** `RestoreContext::installed` starts from the record, and
  `at()` writes it into every commit the drive makes.
- **The outcome reports it**, whichever drive finished the group.
- **A progress that names no file keeps the recorded one** (`apply_restore_progress`). A drive that
  never knew the file, and a replica built before the field, which drops it, then change the record
  and move the topology version the same way. Without that, the two would count a different
  version.
- **A retry clears it** (`GroupRestore::retry`): the group installs again, and the new file is the
  one its record names.

No wire version gates it. The field is JSON with `#[serde(default)]` in the control state, its log
and its network, and a replica that drops it reports zeros as before.

## Alternatives rejected

- **Count the rows at the verifying scrub.** A digest report counts the rows a copy holds now,
  which a client's write past the boundary changes, and says nothing of the file's bytes or its
  remembered requests.
- **Read the backup's files again at `Verifying`.** It costs a pass over every file of the group to
  write a report, and the file installed was filtered from them to the group's tablets.
- **Keep the installing drive on the group until it is done.** A drive cannot keep a lead it lost,
  which is the case this is about.

## Invariants to uphold

- **A restore's outcome reports the file installed, whoever finishes the group.**
- **Every progress from `Verifying` on carries the file, or names none and keeps the record's.**
  Nothing but a retry clears it.
- **A record changes, and the version moves, only for what every replica sees.** A field one
  replica drops must not move the version on another.

## Still open

- A replica built before the field drops it: a drive of such a node, resumed at `Verifying`, reports
  zeros as before, until every member runs a build with it.

## Tests

| Test | What fails if the fix is reverted |
| --- | --- |
| `a_restore_resumed_at_verifying_reports_what_it_installed` (`cluster_fixture`) | Every group handed back after `Verifying` reports boundary 0, bytes 0 and no records, and the sum is short of the backup's 60 notes |
| `a_restores_installed_file_is_kept_until_its_group_installs_again` (`shoal-core`, `control::types`) | A progress naming no file erases the recorded one and moves the version (*a word naming no file moved the version*, run with the merge taken out), or a retry keeps a file it replaces |
| `single_node_data_has_a_verified_cluster_migration_path` (`cluster_fixture`) | Under load its record sums to fewer records than the export holds, as in its 21 loaded runs |

## Related

- [Item 142](../known-issues.md#142-two-fixture-tests-fail-intermittently-on-an-idle-host), whose
  deadline this row was.
- [Resolved #297](restore-rides-out-a-moment.md), which drives a group again from `Verifying` more
  often, and found the attempts lost the same way.
- [Resolved #155](restore-retries-unreachable.md) and [#183](restore-driver-uncommitted-done.md), the
  hand-backs this record survives.
- [F49](../../features/backup-and-recovery.md).
