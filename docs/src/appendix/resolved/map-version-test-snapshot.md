# 116. `map_versions_install_atomically_and_resync` read the newest map version once and expected it to stay

## Symptom

The M3 map-version test failed in the F52 whole-workspace run at six threads, and passed alone
straight afterwards:

```text
panicked at shoal/tests/cluster_fixture.rs:3140:9:
assertion `left == right` failed: node 0 holds {… "members":{"a2fd6437-…":{… "health":"down",
"incarnation":7, …}, …}, …}
  left: Number(17)
 right: 16
```

## Cause

After five rapid restarts of node one, the test read `newest` from node zero's `MEMBERS` once.
It then required every node, the slowed client and a late client to be on **exactly** that
version. But the map keeps moving after five restarts. The detector judges the restarted member
by its reports, and under a loaded host it called that member `Down`, which committed version
17 after the test had read 16. That member was node one at incarnation seven, five restarts
past its first. The server did nothing wrong. The test treated one read as the last version,
when it was only the version at the time of the read.

## Evidence

**Established from the recorded run and the source.** In the run's output, node zero's map
holds version 17 with one member `down`, against the test's 16. `wait_map_version` waits for
"at or past", so it passed. The exact-equality assertion straight after it failed. Nothing in
the test prevents a detector verdict between the read and the assertion, and the detector
commits one whenever a member's reports lag past its threshold, which a loaded host causes.

## The fix

The test now waits for the cluster to **settle**. The new `wait_map_settled` reads every node's
`MAP` until they all hold the same version with every member `up`, and returns that version.
Each check after that is **at or past** that version:

- each node's map, which still has exactly three members;
- the slowed client, which still only moves forward;
- the late client's first frame.

Every version the burst produced, and that a late client could have missed, is at or below the
settled version. So "at or past" still proves the late client was handed the newest map and not
one it missed.

## Alternatives rejected

**Raise the detector's threshold in this test's cluster.** That would hide a legitimate verdict
to protect a test that is not about the detector. The next source of a version, such as a
voter promotion or a verdict on another node, would fail the test the same way.

**Retry the whole test body on a version mismatch.** That would pass, but it would no longer
assert the property. A test that retries until it sees equality cannot fail on a real
regression either.

## Invariants to uphold

- **A fixture test never assumes the map version it read is the last one.** It either waits for
  the cluster to settle or asserts "at or past". A committed verdict is a new version.
- **The properties this test pins are atomicity and monotonicity:** no member count other than
  three is ever installed, and no client moves backwards. They do not depend on which version
  is last.

## Still open

- Nothing. The behaviour under test was never wrong; the test's reading of it was.

## Tests

| Test | Where | What breaks if this is reverted |
| --- | --- | --- |
| `map_versions_install_atomically_and_resync` | `shoal/tests/cluster_fixture.rs` | Fails whenever a verdict is committed between the version read and the assertions, as it did under the loaded workspace run |

## Related

[F39. Membership](../../features/membership.md), the test's feature;
[Tablet map](../../distributed/tablet-map.md); [Resolved #115](retry-sidecar-crash-window.md),
the other failure in the same run.
