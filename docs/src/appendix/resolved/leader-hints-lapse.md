# 223. A routed client's strong reads followed a leader hint after the lead had moved back

## Symptom

A client routing by topology ([F74](../../features/client-routing.md)) sends a strong read - one at
`Quorum`, or a table whose policy reads that way - to its group's leader, so the copy serving it
holds the read barrier itself. On the lab, the bench's routed clients asked a barrier of another
node for about two `Quorum` reads in five at a bundle of one, and for more than one in two at
bundles of sixteen and sixty-four: little better than a client sending through its endpoint,
which asks one for three reads in five. The share grew from arm to arm of the same run, while every lead
was moving back to where the client was routing by its weights.

## Cause

A committed write that hopped to its group's leader is answered with that leader, and the client
kept it by group (`LeaderHints`), routing the group's writes and strong reads there over its
preferred leader. A hint was replaced only by the answer of another write that hopped. Only a
write is told, and only when it hops: a read that asks a barrier of the leader is answered with
rows and nothing else. So a hint was never corrected by a read, and a client that went on to read
alone kept the hint for good.

Nothing keeps a lead where a hint found it. The balancer hands each lead back to its preferred
leader once the group has settled, a group a shard at a time
([O63](../optimizations.md#o63-leadership-never-returns-to-a-groups-placement-primary), aimed by [F58](../../features/weighted-leadership.md)), which is exactly where the client would have sent
the read without the hint. The bench made the case every run: its preload is the first thing a
fresh cluster is asked, while the leads are still at their placement primaries, so the preload's
routed writes taught its clients a lead for each group led away from its preferred leader; the
read arms that followed used the same clients, and every lead the balancer handed back turned one
hint stale.

## Evidence

**Reproduced** on the lab and by a fixture test written before the fix, run against the unfixed
tree.

The lab: F74's `Quorum` pass at commit `77f37df`, `read100` at bundles of 1, 16 and 64 through the
bench's three clients, one on each member (europa, titan, hyperion; performance governor), four
rounds a side. Barrier hops a second over the members at each arm's last sample (a ten second rate)
against the arm's reads a second, medians of four, the share the median of each round's:

| Bundle | Through the endpoints | Routed |
| --- | --- | --- |
| 1 | 9,275 hops/s of 15,723 reads/s, 0.60 | 6,126 of 16,899, 0.38 |
| 16 | 14,035 of 23,594, 0.60 | 12,315 of 22,994, 0.56 |
| 64 | 15,961 of 26,556, 0.60 | 14,054 of 25,302, 0.57 |

A routed read should ask no barrier elsewhere once the leads are where the client sends it, and a
client through its one endpoint asks one for two reads in three at a factor of three. The routed
share rose arm by arm within each run - the arms run in bundle order on the same clients - which
is what hints going stale one hand-back at a time looks like, and the fixture then showed it alone.

The test, at lead weights of 4:1:1 so that a fresh cluster's leads start away from their
preferred leaders, against the unfixed tree:

```text
$ cargo test -p shoal --test cluster_fixture -- --exact a_routed_clients_strong_reads_follow_a_lead_moved_back
running 1 test
test a_routed_clients_strong_reads_follow_a_lead_moved_back ... FAILED

failures:

---- a_routed_clients_strong_reads_follow_a_lead_moved_back stdout ----

thread 'a_routed_clients_strong_reads_follow_a_lead_moved_back' (2186698) panicked at shoal/tests/cluster_fixture.rs:22181:9:
after 1 writes hopped, routed strong reads asked 106 barriers of other nodes while every lead stayed at its preferred leader

test result: FAILED. 0 passed; 1 failed; 0 ignored; 0 measured; 154 filtered out; finished in 25.80s
```

One write hopped, so the client was told one group's lead; once the balancer had handed that lead
to node zero, the reads of that group - 106 of 300 - still went to the old leader, which asked node
zero for each barrier.

Found by F74's own lab A/B, which measured the `Quorum` pass to see the barrier hop removed and
found it mostly still there.

## The fix

A hint lapses `LEADER_HINT_FOR` (five seconds) after the write that taught it, and a group whose
hint has lapsed is routed by its weights again: to its preferred leader, where the balancer puts
every lead it can. A lead that is still away when its hint lapses costs the group's next write one
hop, whose answer teaches the client again; a strong read sent to the preferred leader in that
time asks a barrier of the leader, as it did before F74. `LeaderHints` keeps the time beside each
leader, and `Router::plan` reads the clock once a bundle.

## Alternatives rejected

| Alternative | Why not |
| --- | --- |
| A hint on a strong read's answer too | The exact remedy - every hop teaches, whichever kind - but a read's answer carries no token to name its group, and a read is answered from four places (sealed in the table, gathered from shares, forwarded whole, refused); the hint would need its group in the frame and a path through each. Filed in [TODOs](../todos.md#client-routing) |
| Ignore hints for reads, keep them for writes | A lead the balancer cannot hand back - followers too far behind under writes - would leave the client's reads hopping for as long as the writes follow the hint correctly |
| Drop every hint when a topology frame arrives | A lead moves without a frame: the balancer's hand-back commits nothing to the control group |
| Confirm a hint when a write sent to it is answered with no hint | A write answered from a node that forwarded it carries no hint either, so the absence does not say who led |
| A longer lapse | Every second of it is a second a stale hint misroutes reads; the five seconds is the balancer's own pace, and a lapse costs a write one hop a group |

## Invariants to uphold

- **A hint is evidence about a moment, and the preferred leader is where leads go.** Nothing may
  follow a hint for longer than a lead is likely to stay put; the balancer is what moves leads
  back, so its pace bounds the lapse.
- **A lapsed hint is never wrong to drop**: the query goes where it would without hints, and a
  write that then hops teaches the client again.
- **A hint is still advice** (F74): a hinted node that cannot be reached is passed over, and
  nothing waits on a hint being right.

## Still open

A strong read is still told nothing: a lead away from its preferred leader that no write touches
costs every strong read of its group a barrier hop once the hint has lapsed. The fix for that is
the hint on a read's answer above.

## Tests

| Test | What breaks if the fix is reverted |
| --- | --- |
| `shoal-client` `routing::tests::a_hint_lapses_back_to_the_preferred_leader` | a hint followed after `LEADER_HINT_FOR`, or dropped before it |
| `cluster_fixture` `a_routed_clients_strong_reads_follow_a_lead_moved_back` | at lead weights of 4:1:1, routed writes taught while the leads sat at their placement primaries, then `Quorum` reads once every lead was at its preferred leader asked barriers of other nodes |

## Related

[F74](../../features/client-routing.md), which added the hints; [F58](../../features/weighted-leadership.md) and
[O63](../optimizations.md#o63-leadership-never-returns-to-a-groups-placement-primary), whose balancer moves leads back; [Resolved #220](stale-topology-retried.md), F74's other defect.
