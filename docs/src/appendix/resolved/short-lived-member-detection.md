# 101. A member that fell silent before its fifth report was never called `Down`

## Symptom

A cluster member killed a few seconds after it joined stayed `Up` forever. The leader's
phi-accrual detector had two or three intervals from it, and with fewer than `min_samples` - five
by default - it declined to judge: no phi, no suspicion, no `Down` commit, however long the
silence lasted. Every M3 test killed members that had been reporting for long enough, so nothing
saw it until the M6 grace test killed one at the start of its run and waited thirty seconds for a
verdict that never came.

## Cause

`Detector::phi` in `shoal-core/src/server/control/detector.rs` returned `None` while a member's
observed intervals were fewer than `min_samples`, and `suspects` treated `None` as not suspected.
`view` reported such a member's phi as a literal zero. The floor was meant to keep a member from
being suspected on the jitter of its first report; it did that by giving the detector nothing to
say at all, for as long as the member had not reported enough - and a member that stopped
reporting never would. A seeded member, one a new leader learned from the log rather than from a
report, was already judged at the expected pace with a full set of seeded intervals, so the gap
was only for a member whose first *real* reports were also its last.

## Evidence

**Reproduced.** `down_retains_placement_during_grace` in `shoal/tests/cluster_fixture.rs` starts
three nodes at a factor of three with a two hundred millisecond detector interval, kills node two
as soon as the placement is held, and polls the leader's members for a `Down` verdict with an
episode. Against the tree before the fix it timed out after thirty seconds, and the detector view
it printed on the way out is the whole defect - three samples, thirty seconds of silence, a phi of
zero:

```text
thread 'down_retains_placement_during_grace' panicked at shoal/tests/cluster_fixture.rs:4627:9:
the dead member was never called down: {... "health":"up" ...}
detector: {"members":[
  {"node":"2d88270a-…","samples":100,"phi":0.000804845840719195,"since_last_ms":54.99,"mean_ms":200.1,"stddev_ms":50.0},
  {"node":"fc5731c6-…","samples":3,"phi":0.0,"since_last_ms":30032.05,"mean_ms":211.2,"stddev_ms":50.0}
]}
```

The unit test `a_member_silent_before_its_fifth_report_is_suspected` beside the detector is the
same shape in isolation: two reports a hundred milliseconds apart and then silence, which the old
detector answered with `None` for as long as it was asked. With the fix the member's phi is under
one just after the pace and over eight after a second of silence, and the fixture test sees
`Down` with an episode in about a second at a two hundred millisecond interval.

## The fix

`fit` pads a member's observed intervals with the expected interval up to `min_samples` before
it takes the mean and the deviation, so a member with one real interval is judged as though its
other four had arrived exactly on time. `phi` always answers; `view` always computes it; and the
view's `samples` count says how many of the intervals are real. That is the same rule a seeded
member already had - a new leader seeds every member it knows with `min_samples` expected
intervals - applied to the case between a seed and a full history.

## Alternatives rejected

**Suspect any member whose silence exceeds a fixed multiple of the interval, before it has a
history.** A second rule with its own threshold beside the phi rule, and the two would disagree
at the boundary. Padding makes the one rule cover the case.

**Lower `min_samples`.** One or two real intervals from a member that has just joined are
mostly the join itself; a fit over them would suspect a calm member at the first hiccup. The
floor is right for what a fit is *of*; what was wrong was declining to fit.

**Have the join seed the member the way a new leader does.** It would, and it is a fair reading
of what a leader admitting a member knows. It would also have left the gap for a member whose
seed was replaced by its first real report and then went silent, which is exactly the order the
grace test produces; the padding covers both.

## Invariants to uphold

- **The detector always has a verdict for a member it has heard from.** `phi` returns `None`
  only for a node it does not track; `suspects` may take `is_some_and` as "tracked and over the
  threshold" and nothing else.
- **Padding is with the expected interval, never with zero and never with the observed mean.**
  Zero would make the deviation floor the whole fit and suspect at once; the observed mean
  would make a member that reported twice quickly look regular at a pace it never promised.
- **The floor on the deviation is a quarter of the expected interval, on padded and full sets
  alike.** The padding does not change what a regular reporter's jitter looks like.
- **`view.samples` counts real intervals.** An operator reading the admin view can tell a member
  judged from one report from one judged from a hundred; the padding is not reported as
  evidence.

## Still open

- ~~The grace itself. A `Down` member keeps its placement, which the fixture test asserts, and
  nothing removes it: `auto_remove_after` is M9b's.~~ Delivered by
  [F46](../../features/capacity-rebalancing.md): the grace is counted and expires into a removal.
- A member that is called `Down` and comes back inside the same incarnation is judged from its
  next report at the expected pace, which is what the fixture's restart sees; a member that
  comes back under a new incarnation starts over, as before.

## Tests

| Test | Where | What breaks if this is reverted |
| --- | --- | --- |
| `a_member_silent_before_its_fifth_report_is_suspected` | `shoal-core/src/server/control/detector.rs` | A member with one real interval and a second of silence is not suspected, or its phi is reported as zero |
| `down_retains_placement_during_grace` | `shoal/tests/cluster_fixture.rs` | A member killed at the start of a run is never called `Down`; the test times out after thirty seconds with the view above |
| `phi_grows_with_silence`, `a_seeded_member_gets_a_grace_period` | `shoal-core/src/server/control/detector.rs` | The padded fit changes what a full history or a seed answers |

## Related

[F39. Membership](../../features/membership.md), whose detector this is;
[F42. Primary failover](../../features/primary-failover.md), whose grace test found it;
[C3. Membership](../../distributed/membership.md).
