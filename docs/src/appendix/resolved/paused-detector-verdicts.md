# 147. A paused control leader, once resumed, called live members down, and the leader stayed down

## Symptom

After two pause tests on the lab ([cluster testing](../../cluster-testing/correctness.md#pause-one-node)),
`cluster status` showed europa `down` with a 1,800 s grace, while europa was running, serving,
and *leading the control group*. It stayed that way for over 25 minutes, until it was dealt with
by hand. `cluster upgrade` refused to start ("europa is down, not up"), and the cluster counted
two members up of three. Nothing was lost, and the data groups were unaffected.

## Cause

Two defects, one after the other.

**A resumed detector judged by its own silence.** The phi-accrual detector
(`control/detector.rs`) runs on the control leader and judges each member by the time since its
last status report. At 14:55:50 hyperion, then the control leader, was stopped with `SIGSTOP` for
20 s, and europa took over. When hyperion resumed, its next report tick ran before it learned it
had been replaced. Its detector saw 20 s since anyone had reported, phi 300 for everyone, and it
proposed europa and titan `Down`. A proposal from a member goes to the leader, so europa's own
group committed both verdicts (topology versions 73 and 74). The silence was hyperion's, not
theirs.

**A leader held down never came back up.** titan reported again, and europa's detector committed
it `Up`. Nobody reports europa to europa: a leader's detector never judges itself, and a member
is committed `Up` only on a fresh report from it that reaches the leader. So europa's own record
stayed `Down`, and nothing would change that while it led. Its grace did not count down, since
`accrue_graces` skips the leader's own record. Only a change of leader would have fixed it. The
same pair of pauses in the other order leaves the same state.

## Evidence

**Found on the lab.** hyperion's journal at the resume: `a member fell silent node=046154cc…
phi=300.0` and `…0f307f49… phi=300.0`, then `a member's health was committed` for europa at
version 73. europa's journal: titan and hyperion committed `Up` at versions 75 and 76, and nothing
more about itself.

**Reproduced** with `a_paused_control_leader_calls_nobody_down` in `shoal/tests/cluster_fixture.rs`.
Three nodes. The control leader is paused until another leads, then resumed. That second leader
is paused until a third leads, then resumed. Against the unfixed tree:

```text
control leaders: 0, then 2, then 1
Error: NotReady("not every member is up everywhere: [(0, 0, "up"), (0, 1, "down"), (0, 2, "up"), (1, 0, "up"), (1, 1, "down"), …, (2, 1, "down"), …]")
```

Node one, the control leader, is `down` on every node 15 s after the resume, which is the lab's
state. With the pause guard alone disabled, the stricter test fails at the resume: *"the resumed
node 2 called a live member down: [(0, 0, "down"), (0, 1, "down"), (1, 0, "down"), (1, 1,
"down")]"*.

## The fix

In `judge_members` (`shoal-core/src/server/control/plane.rs`):

- **A loop that stood still judges nobody.** The plane records when it last judged, on every
  report tick, leading or not. If more than `PAUSED_TICKS` report intervals (four, two seconds at
  the default) passed since then, this node's own loop was not running. It then heard nothing from
  anybody for as long, and that tells it nothing about them. The detector's evidence is reset and
  re-seeded, exactly as a new leader's is, and nobody is judged on that tick.
- **A leader held down commits itself up.** A leader whose own record says `Down` proposes itself
  `Up` at its committed incarnation. It is running by construction, and nothing else would ever
  report it.

Either fix alone clears the reproduction's end state. The first prevents the false verdicts, which
also briefly marked titan down on the lab. The second repairs a leader held down however it got
there.

## Alternatives rejected

- **Refuse health verdicts forwarded from a member.** The resumed node's proposals reached the
  real leader through `propose`, which forwards whatever it is given. Refusing forwarded
  `SetHealth` would stop this path, but a leader deposed at the moment it proposes is not the only
  way a stale verdict arrives: a leader that stalled *without* being replaced has the same gap and
  commits its own verdicts directly. The pause guard covers both.
- **Judge silence against the leader's own heartbeat rather than wall time.** openraft does not
  expose when this node's executor last ran, and the report tick is already that measurement.
- **A larger pause allowance.** At the default interval a member silent for four seconds reaches
  the default phi threshold of 8. An allowance longer than that lets a stall of that length call
  everyone down.

## Invariants to uphold

- **`last_judged` is updated on every report tick, leading or not.** A node that becomes leader
  after a pause has to know that its loop stood still, too.
- **A leader never proposes a verdict about itself other than `Up`.** It never judges itself
  `Down` by its own detector.
- **After a reset, members are seeded, not left empty.** An empty detector judges nobody, and a
  member that really died would never be called down.

## Still open

- A member that stalls without leading is still called down by the leader for the length of its
  stall, correctly. The data groups' leaders on it move by election, as in the pause test.

## Tests

| Test | What breaks if this is reverted |
| --- | --- |
| `a_paused_control_leader_calls_nobody_down` (`shoal/tests/cluster_fixture.rs`) | A resumed ex-leader calls live members down (the pause guard), and the leader it calls down stays down (both fixes) |
| Pause one node ([cluster testing](../../cluster-testing/correctness.md#pause-one-node)) | The control leader is left `down` after two pauses, and `cluster upgrade` refuses to run |

## Related

- [F39](../../features/membership.md), the failure detector and health.
- [F46](../../features/capacity-rebalancing.md), graces and removal.
- [Resolved #144](post-heal-elections.md), the data groups' side of a node coming back.
