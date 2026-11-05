# 360. M23's exit row named a test no function is, so the workspace's acceptance check failed

## Symptom

`cargo test --workspace` failed one test, every time, from the moment M23 was marked delivered
(`40bcbd4b`, 2026-11-04):

```text
thread 'scan_acceptance_tables_name_real_tests_and_valid_gates' panicked at shoal-bench/tests/acceptance_tables.rs:353:9:
`a_scan_beside_the_reference_cell_holds_its_budget` gates delivered milestone M23 but no `fn a_scan_beside_the_reference_cell_holds_its_budget` exists in the workspace
```

Every workspace run after it, those of Resolved #42 and #207, #241, #338 and #340, counted it as
their one failure.

## Cause

[F122](../../features/scan-oracle.md) holds the scans plan's acceptance tables to
[C11](../../distributed/testing.md#invariants-to-uphold)'s rule: every test a *delivered* milestone's
rows name has an executable entry point, which the check reads as a function of that name somewhere
in the workspace. [W7](../../future-work/scans/isolation.md#acceptance-tests)'s table opens with
`a_scan_beside_the_reference_cell_holds_its_budget`, gated on M23, which its own row describes as "the
lab's run ... An exit, not a unit test". M23's budget was judged by the exit's rounds on the lab
([M23's exit](../../future-work/scans/m23-exit.md)), and no function has that name.

While M23 was open the rule did not apply, since a test may land before its milestone is complete.
When its section was marked **Delivered** the check met a row it had no word for: the distributed
chapter and the object storage part, which C11's rule was written for, name only functions, and the
scans plan is the first to name a lab run as a gate's test. The check was right that no function
existed; the table was right that M23's acceptance is a lab run. What was missing was a way for a
row to say which it is.

## Evidence

**Established by running it**: the workspace run of [Resolved #42, #207](sort-keys-of-any-type.md)
met it, and the two files it reads, `isolation.md` and `milestones.md`, were HEAD's byte for byte, so
it failed at `40bcbd4b` alone. On this change's tree before the fix,
`cargo test -p shoal-bench --test acceptance_tables` ran four tests, three passing and the scans
table's failing as above; on the fix it runs five, every one passing, the new
`a_lab_exit_row_is_held_to_its_record` among them.

## The fix

Decided with the user (2026-11-05): the check learns rows that are a lab exit.

- **A row says it is an exit.** Its *Proves* cell carries `**Lab exit**` and links the page that
  records the run. W7's row now ends "**Lab exit**, recorded in
  [M23's exit](../../future-work/scans/m23-exit.md#the-rounds-again)".
- **An exit is held to its record.** Once its milestone is delivered, the page its first link after
  the mark names, read relative to the row's own page, must exist under `docs/src`
  (`exit_record`, `exit_holds` in `shoal-bench/tests/acceptance_tables.rs`). An exit that links
  nothing, links only an anchor of its own page, or links a page that is not there fails by name.
- **Every other row is held as before**, to a function of its name.
- C11's invariant on [Testing](../../distributed/testing.md#invariants-to-uphold) says what an exit's
  entry point is: its record.

## Alternatives rejected

- **Take the row out of W7's table**, its exit kept in W7's prose and on M23's exit page. The table is
  where a reader looks for what a gate is judged on, and M23 was judged on this row more than on any
  other; a table without it says M23 was judged on unit tests alone.
- **A function of that name that reads the exit's record and holds its ratios to W7's rule.** It
  would pass for ever on a committed file, so it proves nothing a reader of the record does not see,
  and it would parse a report whose format changed between the exit's runs. The rounds' reading was
  the user's decision, recorded with its evidence; a test cannot make it again.
- **Recognising an exit by the words "An exit, not a unit test".** A sentence is prose, and a row
  reworded for any other reason would silently turn back into a function row. A mark in bold is
  plainly a convention, and a struck mark is no mark.

## Invariants to uphold

- **A row is an exit only by its mark**, `**Lab exit**` in its *Proves* cell and not struck through;
  every other row of a delivered milestone still needs a function of its name.
- **An exit's record is a page beside the row's**, linked relative to it, as every link in the book is.
- **The check judges that the record exists, never what it says.** Whether the run met its budget is
  decided on the record by the user, as M23's was.
- **An exit of an undelivered milestone is not checked**, since its record is written when the exit runs.

## Still open

- The check reads a record's page and not its anchor, so a heading renamed on the record would leave
  the row pointing at the page's top. mdbook does not check anchors either.

## Tests

| Test | Where | Breaks if |
| --- | --- | --- |
| `scan_acceptance_tables_name_real_tests_and_valid_gates` | `shoal-bench/tests/acceptance_tables.rs` | An exit row is held to a function again, or W7's row loses its mark or its record |
| `a_lab_exit_row_is_held_to_its_record` | `shoal-bench/tests/acceptance_tables.rs` | A struck mark counts, the first link after the mark is not the record, or an exit linking nothing, an anchor alone or a missing page passes |

## Related

[F122](../../features/scan-oracle.md), which made the scans plan's tables checked;
[C11](../../distributed/testing.md), whose invariant it holds; [M23's exit](../../future-work/scans/m23-exit.md),
the record; [W7](../../future-work/scans/isolation.md#acceptance-tests), the row.
