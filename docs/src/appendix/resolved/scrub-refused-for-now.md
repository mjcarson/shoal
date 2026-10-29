# 330. A light scrub that could not ask a chunk's owner for now moved past its placement group

A light scrub lists each placement group's chunks on every holder and judges each stripe. A
stripe with no row whose object its consumer names nowhere is what a reclamation left on a holder
it did not reach, and the scrub removes it under a seal
([F92](../../features/reclamation-and-device-lifecycle.md)). Asking the consumer is a strong read of
the object's entry. Each placement group finished is committed as a `ScrubStep`, which moves the
pass's cursor past it ([F89](../../features/scrub-and-repair.md)).

Found and fixed in the flaky-test plan's confirmation (2026-10-27).

## Symptom

`unexplained_chunk_is_found_and_reclaimed` failed in w7's third round and in w8's third with *node
two's unexplained copy removed never came*: the light scrub the row asked for ended, and node two's
orphaned copy was still there 60 s later. The row had left the flaky page with
[Resolved #118](fixture-setup-under-load.md)'s retried scrub request.

## Cause

`scrub_group` judged each stripe and logged a failed judgement at DEBUG, then went on. The group
counted as done, its step was committed, and the pass moved past it. A stripe a query was refused
*for now* for, as a loaded host refuses a strong read while the group behind it cannot confirm its
leader, was not judged in that pass, and nothing said so. A row-less chunk is found only by a light
scrub, so it stayed until a later pass, which a scheduled scrub may make a day later.

## Evidence

**Established by the loop's child logs.** In w8's third round, node zero's log has, at the time the
row's scrub ran:

```text
DEBUG shoal_core::server::shard::objects::scrub: msg="a scrub could not judge a stripe" object=01a17f771e25f96cf0fe786d34abaec4 stripe=0 error="QuorumUnavailable: group cd10c97b726fca7d could not confirm its leader: not enough for a quorum, …"
```

followed by the pass finishing, and no removal anywhere. The excerpt is in
`target/lab/flaky/runs/w8/round-3/scrub-node0-excerpt.log`.

**Reproduced** by `a_scrub_refused_for_now_judges_its_group_again`, written first. It is the
unexplained chunk's row with every node refusing its scrubs' next two reads of an owner for now
(`faults::refuse_scrub_owner_reads`, the fixture's `REFUSE_SCRUB_OWNER_READS`). On the tree before:

```text
the light scrub ended with node two's unexplained copy still there
```

## The fix

- **A scrub's metadata refusal is marked when it is for now** (`meta_error`): the codes a client
  asks again on, a group between leaders or short of a quorum, a host too busy, a link cut.
- **A placement group with a stripe refused for now is not done.** Every stripe is still judged,
  but `scrub_group` then fails: its step is not committed, the pass stops there, and the next poll
  scrubs the group again from the cursor. A stripe that failed for another reason is skipped as
  before.

## Alternatives rejected

- **Ask again inside the pass, after a pause.** The pass would hold its shard's scrub and its
  device's ceiling while it waits on a refusal the next poll rides out anyway.
- **Count the stripe skipped and go on.** The figure would say so, but the chunk would still wait for
  a later pass.
- **Have the row ask for another scrub.** The row would pass, and an operator's scrub would still end
  with what it was asked to find left behind.

## Invariants to uphold

- **A placement group's step is committed only once every stripe of it was judged or failed for
  good.** A refusal for now holds the cursor.
- **Every metadata refusal a scrub makes goes through `meta_error`**, or a refusal for now reads as
  one for good and its stripe is skipped again.

## Still open

- **A refusal for now that lasts holds its pass at that group**, polled again until the cluster
  can confirm the group's leader. Nothing bounds it, and nothing should: the pass resumes the moment
  it can judge the stripe.

## Tests

| Test | What fails if the fix is reverted |
| --- | --- |
| `a_scrub_refused_for_now_judges_its_group_again` (`cluster_fixture`) | The pass asked for ends with node two's unexplained copy still there |
| `unexplained_chunk_is_found_and_reclaimed` (`cluster_fixture`) | Under load, the owner's read is refused once and the copy survives the pass |

## Related

- [F89](../../features/scrub-and-repair.md), the scrub's passes and cursor.
- [F92](../../features/reclamation-and-device-lifecycle.md), the unexplained chunk's removal.
- [Resolved #118, #292, #295, #298](fixture-setup-under-load.md), the row's scrub request asked again.
- [Tests That Fail Under Load](../flaky-tests.md).
