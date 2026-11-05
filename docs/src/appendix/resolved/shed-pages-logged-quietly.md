# 349. A driver's shed page was logged at ERROR, about a thousand lines a device event

## Symptom

A driver's walk page that its shard shed was logged as an error: *this shard is reading 2 driver
pages already*. A shard reads at most two of its drivers' pages at once and sheds the next, which is
the admission working, and since [Resolved #345](backfill-shed-page.md) every driver asks a shed page
again itself, every 20 ms up to its tries. When a device fails, its thousands of placement groups'
moves ask their pages at once. On M23's exit's drivers rounds (2026-10-31) europa logged 1,187 of these
errors in one capture before M23 and 1,006 after, dozens in one millisecond: they buried anything else
logged then, and cost the loop a formatted line each.

## Cause

`walk_page` (`shoal-core/src/server/shard/walk.rs`), the one call every driver's `TabletWalk` asks a
page through, was instrumented with `err(Debug)`. tracing's `err` logs every error the function
returns at ERROR, whatever it is, so a shed went out at the same level as a page whose loop never
answered. And since a driver asks a shed page again, one page shed ten times logged ten errors before
it was read.

## Evidence

**Established by reproducing it.** `a_drivers_shed_page_logs_no_error` (`shoal/tests/shed_page_logs.rs`)
runs a driver's walk over a loop of the test's own that sheds every page, as a shard's admission does,
with the walk asking a shed page again five times, and counts what the walk's module logs by level. On
the tree before the fix:

```text
thread 'a_drivers_shed_page_logs_no_error' panicked at shoal/tests/shed_page_logs.rs:134:5:
assertion `left == right` failed: a shed page, the admission working, was logged at ERROR
  left: 6
 right: 0
```

Six pages shed, six errors. On the fix the same walk logs no error and one WARN, when it gives the page
up past its tries.

## The fix

- **`walk_page` logs nothing of its own.** Its span stays, without `err(Debug)`.
- **A shed asked again is DEBUG** (`asked_past_sheds`), with how many have been asked again: the
  admission working.
- **The walk says once what it gave back** (`TabletWalk::next`, `note_refusal`), with its table, tablet,
  code and how many sheds it asked again: a shed past its tries, a page its loop did not answer within
  its deadline, and a copy that cannot answer by name are each a WARN, which the driver that asked
  handles and logs as its own outcome; a loop that stopped, which only a shard stopping meets, is DEBUG
  (`refusal_level`).

A node still counts every shed it gives (`count_refusal`, the walk figures of
[F123](../../features/walk-and-scan-counters.md)), so a quieter log loses no figure.

## Alternatives rejected

- **`err(level = "debug", Debug)` on `walk_page`.** It sets one level for every error the function
  returns, so a page whose loop did not answer for 30 s would go out at DEBUG too, and a driver asking a
  shed page ten times would still log ten lines.
- **Returning a shed as an `Ok`.** A shed is a refusal every driver handles as one; making it an answer
  would move the test of what a page said into every caller.
- **Rate-limiting the error**, as a node's write sheds warn once in ten seconds
  ([Resolved #357](preload-sheds-waited-out.md)). A shed page asked again is not a failure at any rate,
  and the walk already knows the one moment worth saying something: when it gives up.

## Invariants to uphold

- **Nothing a walk is refused is logged as an ERROR of its own.** A driver that fails because of it
  says so as its own outcome.
- **A shed asked again is never logged above DEBUG**, and a walk says at most once what it gave back.
- **The shard counts what it sheds**, whatever the walk logs.

## Still open

Nothing of this item.

## Tests

| Test | Where | Breaks if |
| --- | --- | --- |
| `a_drivers_shed_page_logs_no_error` | `shoal/tests/shed_page_logs.rs` | A shed page is logged at ERROR again, by `walk_page`'s span or anywhere in the walk's module, or the walk stops saying once that it gave a page up |
| `a_walks_refusal_is_never_an_error` | `shoal-core/src/server/shard/walk.rs` | A refusal a walk gives back is given a level above WARN, or a stopped loop one above DEBUG |
| `a_shed_page_is_asked_again_within_its_tries` | `shoal-core/src/server/shard/walk.rs` | The walk stops asking a shed page again, which the quieter log relies on |

## Related

[Resolved #345](backfill-shed-page.md), which made every driver ask a shed page again;
[F116](../../features/scan-admission.md), the admission that sheds;
[F129](../../features/drivers-on-the-rebuilt-walk.md), the drivers on the rebuilt walk;
[M23's exit](../../future-work/scans/m23-exit.md), whose drivers rounds found it.
