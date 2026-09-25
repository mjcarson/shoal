# 154. `cluster admin` reported a restore with a failed group as done

## Symptom

`cluster admin -i tmdb.yml restore <dir>` followed its operation to the end, printed `following
<op>: done`, and exited with success. One of its 36 groups had failed (#153). Only a read of the
whole dataset afterwards showed 66,191 rows missing.

## Cause

A repair, backup or restore record is done when every group is `Done`, and `Done` is terminal for
a group that failed as much as for one that succeeded: the outcome says which. The command
followed the record to done and returned success without reading the outcomes. The cluster tab
draws the outcomes, so a person watching sees a failure. A script got only the exit status.

## Evidence

**Found on the lab**, where the restore's record, read again with `status <op>`, carried
`Done {"Failed":{"reason":"… did not take the quarantine: … ConnectionReset …"}}` for one group.
**Tested** by `a_followed_records_failed_groups_are_read_out` (`shoalctl`, `deploy/ops.rs`),
written with the fix.

## The fix

When the record is done, `cluster admin` reads its group lines for a `Failed` outcome
(`failed_lines`), and fails the command naming each one.

## Alternatives rejected

- **Exit success and print a warning.** A script that restores and then serves would carry on.

## Invariants to uphold

- **A command that follows an operation fails if any part of it failed.**

## Still open

- Nothing.

## Tests

| Test | What breaks if this is reverted |
| --- | --- |
| `a_followed_records_failed_groups_are_read_out` (`shoalctl`, `deploy/ops.rs`) | A done record with a failed group is reported as a success |

## Related

- [Resolved #153](install-dir-absent.md), the failure it hid.
