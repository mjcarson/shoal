# 97. `stage_join.rs` had not compiled since F36, and needed `/opt/shoal` to run

## Symptom

The one integration test that starts a server under `--features stage-profile` and reads the
report it wrote could not be run on a host without `/opt/shoal`. Before that it could not be
compiled at all: [F36](../../features/cluster-harness.md) added `server` and `cluster` to
`RunRequest` and did not add them to this test's initializer, and because the binary is behind
a feature no default run enables, nothing said so until
[F38](../../features/inter-node-transport.md) fixed the initializer two milestones later. What
F38 found underneath is the half this page resolves: the test resolved the **committed**
`shoal.yml`, whose storage paths are `/opt/shoal`, and on any host that has no such directory
it failed at server start before it could join anything.

## Cause

`RunRequest.conf` was `PathBuf::from("../shoal.yml")`. `conf::resolve` reads that file, appends
the workload's slug to both writer paths and starts a server there, which is right for a
capture - every workload shares one base so a change to it moves all of them - and wrong for a
test, which has to run wherever the suite does. The benchmark host has `/opt/shoal`; the
development host, where the test is most likely to be run, does not.

The compile failure had the same root: a test behind a feature is built by nobody unless a
runbook says to build it, and none did.

## Evidence

**Reproduced.** `cargo test -p shoal-bench --features stage-profile --test stage_join` on
`europa`, against the tree at `7633c10`:

```text
thread 'a_grid_arm_joins_its_stage_records' panicked at shoal-bench/tests/stage_join.rs:61:6:
the arm runs: failed to start a server: IO(Os { code: 13, kind: PermissionDenied, message: "Permission denied" })
```

With the fix the same command passes in three and a half seconds.

## The fix

The test writes a scratch copy of the committed `shoal.yml` - parsed with `serde_yaml`, the two
storage paths moved under a `tempfile` directory the test owns, everything else kept - and
points `RunRequest.conf` at the copy. The directory is made under `CARGO_TARGET_TMPDIR` rather
than `/tmp`, for the reason `shoal/tests/utils.rs` gives: `/tmp` is usually tmpfs, where glommio
silently gives up direct I/O and the server under test is a different server. After the run the
test asserts the arm's storage subdirectory exists under the scratch root, so an edit that
points it back at the committed file fails on that line rather than on whichever host lacks
`/opt/shoal`.

The runbook half: `CLAUDE.md` and [Test Coverage](../test-coverage.md) now carry the command
that runs this test and a `cargo check -p shoal-bench --features stage-profile,hotpath
--all-targets` that compiles both feature-gated binaries, so a field added to `RunRequest`
breaks a command somebody runs rather than a binary nobody builds.

## Alternatives rejected

**A minimal config written from scratch.** It would run, but it would measure a server
configured differently from every capture - fewer cores, default writer knobs - and the point of
this test is that the stage layer joins on the configuration the captures use. Copying the
committed file and moving only the storage keeps that.

**Skip when `/opt/shoal` is absent.** A test that skips on the host it is most often run on is a
test that does not run. The F14 TLS tests skip without a kernel module because nothing can be
done about the module; a directory is not that.

**A CI job for the feature-gated binaries.** There is no CI in this repository, and adding one
for two binaries is a larger change than the runbook line that does the same job here.

## Invariants to uphold

- **`stage_join.rs` names no path outside its own temporary directory.** The committed file is
  read, never run against; the assertion on the arm's subdirectory is what enforces it.
- **The scratch storage is under `CARGO_TARGET_TMPDIR`.** A server on tmpfs is not the server
  being measured.
- **A field added to `RunRequest` is added here too.** The runbook's `cargo check` line is what
  catches it; keep the line when the runbook is rewritten.

## Still open

- The `hotpath` and `stage-profile` binaries are compiled by a runbook line, not by anything
  automatic. A change that breaks them still goes unnoticed until somebody runs the line.

## Tests

| Test | Where | What breaks if this is reverted |
| --- | --- | --- |
| `a_grid_arm_joins_its_stage_records` | `shoal-bench/tests/stage_join.rs` | Fails at server start with `PermissionDenied` on any host without `/opt/shoal`, and on one with it fails the assertion that the server wrote under the scratch storage |

## Related

[Resolved #76](stage-join.md), the defect this test was written for;
[F36](../../features/cluster-harness.md), which added the fields that broke the build;
[F38](../../features/inter-node-transport.md), which fixed the initializer and filed this;
[Test Coverage](../test-coverage.md), the runbook that now builds the feature-gated binaries.
