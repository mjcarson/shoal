# 345. A backfill gave its pass up on a shed page

## Symptom

A placement group's backfill, which rebuilds every stripe a returning position missed, gave its
whole pass up when one page of its walk was shed. Its shard reads two of its drivers' pages at once
([F116](../../features/scan-admission.md)). With the copier, the light scrub or the reclaimer
holding both, the backfill's page was refused `Shedding` and the backfill failed with it. It said
only "a rebuild did not finish; the next poll tries again", at DEBUG, and waited for the recovery
driver's next poll, half a second later by default, while the drivers beside it asked their shed
pages again every 20 ms. Under a busy recovery the one driver that restores a position's
durability was the one that waited longest for its turn.

## Cause

Every driver walks a tablet or a placement group's range through `TabletWalk`
(`shoal-core/src/server/shard/walk.rs`), which answered a shed page as a refusal. The copier
(`shard/objects/moves.rs`), the light scrub (`shard/objects/scrub.rs`), the reclaimer
(`shard/objects/reclaim.rs`) and the backup's archive (`shard/backup.rs`) each matched `Shedding`
and asked again after 20 ms, up to a hundred times in a walk (a thousand for the backup). The
backfill (`Recovery::backfill`, `shard/objects/recover.rs`) took the page with `?`, so any refusal,
a shed one included, ended its pass.

## Evidence

Established by reading the source, while M23's seventh step moved every driver's pages onto the
rebuilt walk (2026-10-31). The backfill's loop was
`while let Some(page) = walk.next().await.map_err(|refusal| format!("{refusal:?}"))? {`, beside
three loops of its siblings with the arm
`Err(refusal) if refusal.code == ErrorCode::Shedding && shed < 100`, and the recovery poll answers
a failed backfill only at its next tick (`object_timings.recovery_poll`, 500 ms). It was not
reproduced against a running node. A backfill's walk is shed only while its shard's other drivers
hold both its pages in flight, and a shed backfill is only slower, never wrong, so a test would have
to hold a shard's drivers' class full beside a backfill that is due and tell a retry 20 ms later
from a poll 500 ms later. The test below holds the rule that replaced the four copies instead.

## The fix

`TabletWalk::next` asks a shed page again itself (`asked_past_sheds`): after `SHED_WAIT` (20 ms),
up to `SHED_TRIES` (100) sheds in a walk, and then hands the shed back as before. A walk that needs
more tries says so (`TabletWalk::shed_tries`): the backup's archive keeps its thousand. The four
drivers' own shed arms are gone, so every driver, the backfill among them, waits its turn the same
way, and no driver can forget to.

## Alternatives rejected

- **The backfill's own shed arm, as its siblings had.** It fixes this driver and leaves the next
  one to forget it; four copies of one rule had already drifted.
- **Exempting the backfill from its class's count.** The count bounds a shard's drivers' reads in
  flight; a driver past it would read beside the others with nothing bounding them.
- **A queue of drivers' pages in place of a shed.** A queued page holds its driver as long as a
  shed one waits, and needs a bound of its own; the shed and the retry are what F116 built.

## Invariants to uphold

- **Every driver walks through `TabletWalk`**, so its shed retry is every driver's. A driver that
  asks pages another way asks a shed one again itself.
- **A shed page is asked again only after a wait**, never at once, and only up to its walk's tries.
- **Any other refusal is handed back at once**, the cursor where it was.

## Still open

Nothing of this item. A backfill shed past its tries still waits for its next poll, as every driver
waits for its own next pass.

## Tests

| Test | What breaks if the fix is reverted |
| --- | --- |
| `a_shed_page_is_asked_again_within_its_tries` (`shoal-core/src/server/shard/walk.rs`) | A walk asking past sheds is answered once twice shed, past its tries hands the shed back, and hands any other refusal back at once; without the retry the first shed is handed back |
| `client_scans_never_shed_a_drivers_page` (`shoal/tests/scan_admission.rs`) | F116's row, kept: a scan never takes a driver's admission |

## Related

- [F116, a scan's admission apart from the drivers'](../../features/scan-admission.md), whose
  count sheds a driver's page
- [F129, the drivers on the rebuilt walk](../../features/drivers-on-the-rebuilt-walk.md), which
  found it
- [F88, recovery and moves](../../features/recovery-and-moves.md), whose backfill this is
