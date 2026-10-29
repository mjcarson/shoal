# 266. A narrowed scrub request overwrote a whole pool's request still pending

An operator's scrub of a pool can be narrowed to a bucket, a device or a slice
([F93](../../features/operations-and-the-real-cluster.md)). A request is a count on the pool's map,
with the newest request's target beside it, and each tablet group's leader begins one pass for the
newest count with that target. So a narrowed request asked while a whole one was pending replaced
it: the pass that served both scrubbed only the narrowed target. And a narrowed pass set every tablet
group's last pass to its end, the groups it scrubbed nothing of included, which put each one's next
scheduled pass off a whole interval and made the pool's last scrubbed figure say every group had
just been scrubbed. Filed by a code review of F87 to F93; the second reading found the schedule half.

## Symptom

A light scrub of the whole pool asked, then one narrowed to a device before either was begun: the
pool reported both served once a pass had scrubbed 2,759 of its 4,096 placement groups, the device's.
A light lost journal's scrub, which a node asks of a whole pool, was cut down the same way by an
operator's narrowed light request after it. And after a narrowed pass the pool's `light_last_ms`, the
oldest last pass over its groups, stood at the narrowed pass's end.

## Cause

`ScrubRequests::ask_for` counted the request and set its kind's target to the newest
(`*count += 1; *narrowed = target;`). `due` begins a pass for the newest count once a group's cursor
serves fewer, and the driver took `requests.target(kind)` for it, the newest target alone. A pass's
`ScrubBegin` and `ScrubEnd` carry the driver's clock, and the apply records them as the group's
`began_ms` and `last_ms`, which the schedule and the figures read, whatever the pass scrubbed.

## Evidence

**Reproduced** by two fixture tests written first and run on the unfixed tree, three nodes of one
device each at two copies.

```text
---- a_narrowed_scrub_never_narrows_a_whole_one_pending stdout ----
the pass serving a whole request and a narrowed one scrubbed 2759 of the pool's 4096 placement groups

---- a_narrowed_scrub_moves_no_groups_schedule stdout ----
assertion `left == right` failed: a pass narrowed to one device moved the pool's oldest last light pass
  left: Some(1792232616358)
 right: Some(1792232578058)
```

## The fix

- **A request remembers where its target's run began** (`light_since`, `deep_since`): `ask_for`
  begins a run at a request whose target is unlike the newest one's. `ScrubRequests::target_for`
  answers a group the newest target when every request past the last it served is in that run, and
  the whole pool when one before it is not, since a pass serves every request it has not.
- **A pass narrowed to less than its group begins and ends at the group's last finished time**, so it
  moves neither the next scheduled pass nor the last scrubbed time. One narrowed to the group's own
  bucket scrubs all of the group and moves them as a whole pass does.
- Both fields are new and default to zero, skipped when zero, in the pool map's JSON; a pool whose
  requests came before them is judged by its newest target, as before, until its next request.

## Alternatives rejected

- **A target per request.** Every request kept on the pool map for as long as some group has not
  served it; one number a kind says the same for the one question a pass asks.
- **A pass for each target of the requests a group has not served.** The whole pool covers them,
  in one pass, and requests with differing targets are rare.
- **A command that serves a request without a pass, for a group a narrowed one does not name.** A
  new placement command and a new wire version, where the times a begin and an end carry are already
  the driver's to choose.

## Invariants to uphold

- **A pass serves every request its group has not served**, so its target is the narrowest one that
  covers them all: the newest one's only while every one of them names it.
- **A group's last pass time is a pass of all of it.** A narrowed pass serves its request and leaves
  the time where the last whole pass left it.

## Still open

A narrowed pass that interrupts a scheduled pass under way begins at the group's last finished time,
so the scheduled pass is due again as soon as the narrowed one ends, from its start: its progress is
lost, as it was before this fix.

## Tests

| Test | What breaks if the fix is reverted |
| --- | --- |
| `cluster_fixture::a_narrowed_scrub_never_narrows_a_whole_one_pending` | A whole pool's request pending beside a narrowed one is served by the narrowed pass |
| `cluster_fixture::a_narrowed_scrub_moves_no_groups_schedule` | A narrowed pass moves every group's last pass time |
| `shoal-proto scrub::tests::a_narrowed_request_widens_over_an_unserved_one` | A group that has not served an earlier request of another target narrows to the newest |

## Related

[F93](../../features/operations-and-the-real-cluster.md), which made a narrowed scrub;
[F89](../../features/scrub-and-repair.md) for the cursors, the schedule and the requests.
