# 186. A topology observed during a rehome could write the marker back to the old executor count

Found in round 13 of the lab testing, chasing two of [item 142](../known-issues.md#142-two-fixture-tests-fail-intermittently-on-an-idle-host)'s
failures of `local_rehome_recovers_after_each_crash_point`: *"node two never died at
before_finalize"* and *"No such file or directory"* from a child. Both were this.

## Symptom

The rehome matrix ([F47](../../features/local-rehome.md)) restarts node two at a new executor count,
armed to die at each point of the rehome, then starts it again to finish. In one run of seven alone,
the child armed for `before_finalize` (two executors to one) never died: it started one executor,
ran no rehome at all, and served. The test waited 120 s for it to die. Earlier suite runs had also
failed the test with *"No such file or directory"* from a child.

## Cause

The storage marker (`shoal-meta.json`) is rewritten by a read-modify-write in five places:

- the claim;
- a rehome's end (`finish_rehome`, which records the new executor count);
- a mirror onto another root;
- the control thread when it adopts a cluster (`adopt_cluster`);
- the control thread whenever it observes a newer topology version (`observe_topology`).

Each reads the file, changes a field and writes it back through one staged temporary file. The
pool starts the control plane (`server.rs`, `ControlPlane::start`) *before* it runs a rehome
(`Rehome::run`), so on a cluster node the control thread observes topology versions while the
rehome is finishing. Two rewrites on two threads could interleave:

1. The control thread reads the marker: executors 2, topology *v*.
2. The rehome's end reads it, sets executors 1 and writes it.
3. The control thread sets topology *v + 1* and writes what it read: executors 2.

The rehome's count was lost, and the marker disagreed with the files. In the failing run the
previous start's rehome from one executor to two was the one lost: the files were laid out for two
and the marker said one. The next start, armed at one, compared one with one and saw no change of
count. It ran no rehome, never reached its crash point, and opened one executor over files laid
out for two. The two rewrites also staged under the same temporary name, so one could rename the
other's staged file away and fail on *"No such file or directory"*.

## Evidence

**Reproduced twice.** In the fixture: `local_rehome_recovers_after_each_crash_point` alone failed
one run in seven with child logs on (`target/lab/r13/142/rehome-loop.sh`). The armed child's log
shows one executor, no claim line saying the count changed, and no rehome, while the child before
it had logged *"rehomed the storage directory from=1 to=2"*.

In a unit test: `a_topology_observed_during_a_rehome_keeps_its_count` runs `finish_rehome` and
`observe_topology` on two threads at once, 200 times, and checks both fields afterwards. On the
unfixed tree it failed at once, on the staged file's collision:

```text
a rehome: IO(Os { code: 2, kind: NotFound, message: "No such file or directory" })
```

## The fix

**One process-wide lock over every read-modify-write of a marker** (`MARKER_WRITES` in
`shoal-core/src/server/meta.rs`). The claim, `finish_rehome`, `mirror`, `adopt_cluster` and
`observe_topology` each hold it from their read to their rename. The writers are in one process, and
none calls another while holding it.

## Alternatives rejected

- **Starting the control plane after the rehome.** It would close this path. But the control
  thread is up early on purpose, so that a node rehoming for minutes is still a member its peers can
  reach, and the marker would still have four other writers that no rule keeps apart.
- **A per-field file.** It would make the topology's high-water mark and the executor count
  independent, but the claim reads both, and the marker's one atomic rename is what makes a
  restart see a consistent identity.
- **Compare and swap on the file's contents.** A lock in one process is simpler, and every writer
  is in one process.

## Invariants to uphold

- **Every rewrite of a marker holds `marker_lock()` from its read to its rename.** A new writer that
  reads the marker, changes it and writes it back without the lock reopens this.
- **No writer calls another while holding the lock.** The lock is not reentrant, so a nested call
  deadlocks.
- **A read that only reads needs no lock.** The rename is atomic, so a reader sees one whole marker
  or the other.

## Still open

Nothing of this item. Item 142's other rehome failure shape, if one recurs, is to be read against
this fix first.

## Tests

| Test | What breaks if the fix is reverted |
| --- | --- |
| `shoal-core` `server::meta::tests::a_topology_observed_during_a_rehome_keeps_its_count` | A rehome's count or a topology version is lost, or a rewrite fails on the other's staged file |
| `shoal` `cluster_fixture::local_rehome_recovers_after_each_crash_point` | A start at a new count finds the old count in the marker and runs no rehome, one run in several |

## Related

- [F47](../../features/local-rehome.md), the rehome and its marker field.
- [Known Issues #142](../known-issues.md#142-two-fixture-tests-fail-intermittently-on-an-idle-host).
