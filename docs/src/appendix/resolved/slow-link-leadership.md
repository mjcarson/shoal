# 182. A node whose links were slow went on leading a third of the groups

## Symptom

On the lab, `tc netem` added 100 ms to everything hyperion sent on its peer ports, under the
mixed bench ([cluster testing, section 12](../../cluster-testing/correctness.md#12-scenarios-nobody-had-run)).
The whole cluster ran at about 22,000 operations a second for the 20 s the delay lasted, a third of
its rate before. No error was answered, no node was judged down, and every acknowledged write was
read back. The cluster was simply slow, all of it, for one node's links.

## Cause

hyperion led twelve of the thirty-six groups, a third, which is what the placement asks and what
the handback ([O63](../optimizations.md#o63-leadership-never-returns-to-a-groups-placement-primary))
keeps. Every write to those groups waited on hyperion's links: a round trip to a follower before
it committed, and one more for every write hopped to it from another member. Every pipelined
client kept some of those writes in its window, and the window filled with them. Nothing measured
how fast a member's links were, and nothing moved a lead for it. Leads are balanced by count.

## Evidence

**Established on the lab**, as above (`target/lab/r11/sc/netem-100ms`): 69,000 operations a
second in the ten seconds before the delay, 22,000 in the middle of it, 71,000 after, write p99
504 ms during it.

**Reproduced in the fixture** by `a_node_with_slow_links_hands_its_leads_on`, which holds every
chunk on every lane into and out of node one for 100 ms (a new proxy state, `Lag`). With the
judgement below forced off:

```text
node one still leads 4 of its 4 groups with its links slow
```

## The fix

The control thread already pings every member once a second over the control lane, and records
each round trip. Each peer's figure is now the least of its last three round trips, and each peer
keeps the lowest figure it has seen as its baseline, which creeps up with a time constant of
nearly three hours (`LinkRtt` in `server/control/links.rs`). The node judges **its own** links
slow once every peer heard from in the last 10 s, at least two of them, has read past 10 ms and
past twenty times its baseline for 3 s running (`LinkJudge`). It judges them well again as soon as
they read under 5 ms or ten times the baseline. Judged over every peer at once, one slow node sees
all its peers slow while each of them sees only it, so only the slow node judges itself.

**A partition's answers are not a slow link.** The first cut smoothed the round trips, and the
workspace suite's `a_silently_cut_node_rejoins_without_elections` failed on it: a blackholed
node's pings were answered all at once when it healed, with round trips of up to 1.26 s, and the
node judged its links slow and moved its leads, which the test caught as two more elections. So
the figure is the least of three samples, which the first fresh answer brings down; no answer
counts for five after a ping went unanswered; and the judgement needs three seconds of it.

The judgement is process-wide (`links::impaired()`). Every shard reads it on its tick
(`check_disk` in `server/shard/groups.rs`) and, while it holds:

- hands every group it leads to the voter furthest along, again every five seconds, as a node
  under the append reserve does ([#156](wal-failure-stops-the-node.md));
- answers `MayLead` with no, so the handback leaves the leads where they went;
- skips its own handback.

It goes on standing for election and taking writes: if every member judged itself slow, somebody
still has to lead.

**On the lab**, the same delay again (`r11/sc/netem-100ms-182`): hyperion judged its links slow
about a second in, with smoothed round trips of 30 ms to both peers, and handed on all twelve leads
within 0.6 s. The cluster served 56,000–70,000 operations a second for the rest of the delay. Seven
seconds after it lifted, hyperion judged its links well again, and the handback returned its
placement's twelve leads. All 501,081 acknowledged inserts were read back through each member.

## Alternatives rejected

- **Judging from the replication lane's own round trips.** They include the follower's sync and
  apply, so a slow disk would read as a slow link, and a busy node's leads would be moved for the
  wrong reason. The control lane's pings are answered by the control thread alone.
- **Leaving it to the control leader.** It sees every member's pings, and could commit a verdict
  into the map. But a verdict takes a proposal and a map push, and the slow node itself already
  knows, a second in.
- **Refusing writes on the slow node.** It is slow, not wrong. Its reads and the writes it
  coordinates are still answered, only later.
- **Weighting leadership by commit latency.** That is the general form of this, and is filed as
  [weighted leadership](../todos.md#leadership-is-spread-evenly-whatever-each-member-can-do).
  This fix is the case with nothing to weigh: one member is far worse than all the others.

## Invariants to uphold

- **A node judges only its own links, from every peer at once, and never with fewer than two.**
  With one peer, both ends of a slow link would judge themselves slow, and neither would lead.
- **The judgement never stops a node standing for election.** Handing leads on is a preference.
  Liveness is not traded for it.
- **The baseline only creeps up.** A fault of minutes has to stay a fault. A network that is
  slower for good is learnt within the day.

## Still open

- The floor is fixed at 10 ms. A deployment across datacentres whose members are that far apart
  by nature learns them as baselines, but a member slower than twenty times its baseline and still
  under 10 ms is never judged.
- Writes coordinated through the slow node still pay its links, so a client connected only to it
  is still slow.

## Tests

| Test | What breaks if this is reverted |
| --- | --- |
| `a_node_with_slow_links_hands_its_leads_on` (`shoal/tests/cluster_fixture.rs`) | A node whose links are slow keeps its leads, takes one back, or never leads again once they are well |
| `impaired_only_when_every_peer_is_slow`, `the_floor_and_the_recovery_line_hold`, `answers_after_a_miss_are_passed_over`, `a_burst_of_late_answers_is_undone_by_a_fresh_one` (`server/control/links.rs`) | One slow peer, a single peer, a few milliseconds, or a partition's held answers are judged impaired, or recovery comes before the round trips are well down |
| `a_silently_cut_node_rejoins_without_elections` (`shoal/tests/cluster_fixture.rs`) | A healed node judges its links slow from its partition's held answers and moves its leads |
| The 100 ms delay ([cluster testing, section 12](../../cluster-testing/correctness.md#12-scenarios-nobody-had-run)) | One node's slow links take the whole cluster to a third of its rate |

## Related

- [Resolved #156](wal-failure-stops-the-node.md), whose handoff and `MayLead` this reuses.
- [O63](../optimizations.md#o63-leadership-never-returns-to-a-groups-placement-primary), the handback.
