# 44. A compaction discarded a damaged log's tail in silence

## Symptom

An inactive intent log with a torn or corrupt entry in the middle is compacted, deleted, and
never mentioned. The records before the damage are merged into archives; the records after it go
with the file. Nothing in the log output, nothing in a counter, nothing an operator could notice.

The case that *was* reported is the smaller one. A log the reader could get nothing out of at all
warned before it was deleted ([item 14](empty-rotated-logs.md)). A log the reader got five
hundred records out of before it gave up did not — so the more data a compaction dropped, the
quieter it was about it.

## Cause

`compact_intent` (`.../fs/compactor.rs`) had the report inside one arm of a branch that exists
for an unrelated reason:

```rust
let truncated = self.sort_intent_log(&path).await?;
let partitions = if self.changes.is_empty() {
    // warn if this log was empty because we could not read any of it
    if truncated {
        event!(Level::WARN, msg = "Discarding an intent log we could read no entries from", ..);
    }
    Vec::default()
} else {
    self.load_partitions_for_intents().await?;
    self.apply_intents().await?;
    self.write_partition().await?
};
// delete our no longer needed inactive intent log, which is safe for both arms
glommio::io::remove(path).await?;
```

`self.changes.is_empty()` asks *"is there anything to compact"*, which is a question about work,
not about loss. It is empty for the ordinary reason far more often than the damaged one — startup
forces a rotation, so a table nobody wrote to hands the compactor an empty log on every restart
([item 14](empty-rotated-logs.md)) — and `truncated` was placed there to tell those two apart.
That is a real distinction and the warning is correct where it sits. It just is not the only
place a log gets deleted.

The `else` arm deletes too, at the same shared `remove` below the branch, and it deletes a log
that `truncated` may equally be set for. `sort_intent_log` returns the flag; the `else` arm
ignores it.

The same split shows in the counters. `apply_intents` builds a `RecoveryStats`
([item 9](orphaned-update-intents.md)) and warns when it is not clean, but populates only
`orphaned_updates` and `updates_after_delete` — the two an *applied* intent can produce.
`truncated_logs` exists on the struct and stays zero on the compaction path, so even the number
that was designed to carry this could not.

## Evidence

**Established by reading the source, not by reproduction**, and the distinction matters here more
than usual: the defect is that nothing is emitted, so there is no failing behaviour to catch. A
test that drives a real compaction over a hand-damaged log and asserts on a log line would need
to capture `tracing` output across glommio's executor threads, and the integration suite installs
no subscriber at all — nothing in `shoal/tests/` calls `trace::setup`, and the library never
calls it either. That gap was already filed before this item existed, as the third piece of the
[observability work](../todos.md#observability): *"no test can observe any event the server
emits"*. This is the second defect it has kept out of reach, which is worth noting as an argument
for closing it.

What was reproduced is the classification, which is where the mistake actually lived. The four
`classify_tail` tests below fail against a tree without the helper because the distinction they
pin did not exist to be got wrong — the code had two states where it needed three.

The reading that established it is the `remove(path)` above, which sits *below* the branch, and
the comment on it — "safe for both arms" — which is about durability and is correct about
durability. It was written by [item 14](empty-rotated-logs.md), which moved the delete out of the
`else` arm so an empty log would be cleaned up. Moving the delete out and leaving the report in
is what created the gap: before that change, the arm that reported and the arm that deleted were
the same arm.

## The fix

The branch that decides what was lost is now separate from the branch that decides what work to
do, and it names three outcomes rather than two:

```rust
pub(crate) enum TailLoss {
    /// This log was read to its end and nothing was discarded
    None,
    /// This log stopped on damage before a single record could be read from it
    Whole,
    /// This log gave us records and then stopped on damage, dropping the rest
    Tail,
}

pub(crate) fn classify_tail(truncated: bool, read_any: bool) -> TailLoss
```

`compact_intent` classifies once, before it touches anything, and reports below the branch beside
the `remove` it is describing:

```rust
let truncated = self.sort_intent_log(&path).await?;
// work out what deleting this log is about to cost us before we touch it
let loss = classify_tail(truncated, !self.changes.is_empty());
let partitions = if self.changes.is_empty() { .. } else { .. };
match loss {
    TailLoss::None => (),
    TailLoss::Whole => event!(Level::WARN, msg = "Discarding an intent log we could read no entries from", ..),
    TailLoss::Tail  => event!(Level::WARN, msg = "Discarding the unreadable tail of an intent log we compacted", ..),
}
glommio::io::remove(path).await?;
```

The two messages stay distinct because the two events are: one is a log that gave us nothing, the
other a log we compacted most of. Collapsing them would make the count right and the message
useless.

`apply_intents` now seeds its `RecoveryStats` with `truncated_logs` from the same value rather
than starting from `default()`, so a compaction that dropped records reports them alongside the
intents it could not apply, and its `WARN` carries all four counters instead of two.

This is the shape [item 13](eviction-log-underflow.md) used for the same reason: the decision
inside a log statement was wrong, and the log statement was unreachable from a test, so the
decision was lifted into a total function that is not.

## Alternatives rejected

**Keep the damaged log on disk instead of deleting it.** The most appealing option and the wrong
one. The compactor has no quarantine concept, and a log left in the intent directory is not inert
— `find_inactive_intent_logs` picks it up by name, so every subsequent startup replays it, hits
the same damage, and re-drops the same tail, forever. Renaming it out of the pattern would work
and is a real feature (a `.corrupt` sideline an operator can inspect), but it is a feature, not
this fix, and it is filed in [Todos](../todos.md#quarantining-a-damaged-intent-log).

**Fail the compaction and refuse to start.** A torn tail on a log is the *expected* result of a
crash, not an exception — see [item 47](../known-issues.md#47-a-torn-tail-on-the-active-log-is-counted-as-data-loss),
which is the other half of this same confusion. Refusing to start would turn every unclean
shutdown into an outage.

**Count records dropped rather than logs.** The reader cannot know. It stops at the first damaged
entry precisely because it cannot trust what follows to be a record at all, so there is no
denominator — "the rest of the file" is the only honest answer, and a log count is the honest
unit for it.

**Report from `sort_intent_log` directly.** It knows `truncated` first. But it does not know
whether anything was read out of the log into `self.changes`, which is what separates the two
messages, and giving it that knowledge means giving it the caller's state.

## Invariants to uphold

- **Every path out of `compact_intent` that removes a log must first account for what it could
  not read.** The `remove` is unconditional and below the branch; the report has to be too. This
  is exactly what went wrong — the delete moved out of an arm and the report did not follow it.
- **`classify_tail` is called before the branch, not inside it.** It reads `self.changes` to
  decide `read_any`, and `apply_intents` drains `self.changes`. Classifying after the `else` arm
  would see an empty map and call every damaged log `Whole`.
- **A clean read of an empty log stays `TailLoss::None`.** Startup forces a rotation, so an
  untouched table produces one of these on every restart ([item 14](empty-rotated-logs.md)). If
  emptiness alone ever starts counting as loss, every restart reports a compaction that discarded
  data and the counter becomes noise — the same failure [item 47](../known-issues.md#47-a-torn-tail-on-the-active-log-is-counted-as-data-loss)
  describes on the recovery side.
- **`truncated_logs` on the compaction path counts logs, not records.** `RecoveryStats::merge`
  saturating-adds it, and `is_clean` treats any non-zero value as loss.

## Still open

**The damaged records themselves are still discarded.** This fix makes the loss audible, not
smaller. A log with one flipped bit near its start still loses everything after that bit, on the
compaction path exactly as on the recovery path, and for the same reason — the reader cannot tell
a torn tail from a corrupt record with good records behind it
([Recovery](../../storage/recovery.md#truncation-and-corruption)).

**Nothing preserves the evidence.** The file is deleted, so an operator who reads the warning has
nothing left to examine. Quarantining it is filed in [Todos](../todos.md#quarantining-a-damaged-intent-log).

**The compaction path still cannot emit `unreplayable_entries`.** Of the four counters it now
carries, three can be non-zero; an intent that cannot be replayed at all is caught by the reader
before `apply_intents` ever sees it. The field is carried for the merge, not because compaction
can produce it.

## Tests

| Test | What it pins |
| --- | --- |
| `classify_tail_reports_a_dropped_tail` (`.../storage/fs/tests.rs`) | The case this item is. A damaged log that yielded records is `Tail`, so it is reported rather than passed over as work-that-happened |
| `classify_tail_reports_a_log_dropped_whole` (`.../storage/fs/tests.rs`) | The case [item 14](empty-rotated-logs.md) already covered stays covered and stays distinguishable from the one above |
| `classify_tail_reports_no_loss_for_a_clean_log` (`.../storage/fs/tests.rs`) | An empty log read to its end is not loss. Reverting this reports discarded data on every restart of an unwritten table |
| `classify_tail_separates_loss_from_a_clean_read` (`.../storage/fs/tests.rs`) | Both damaged shapes reach `truncated_logs`, so neither can fall back into silence |

## Related

- [Item 14](empty-rotated-logs.md) introduced the `truncated` flag and the delete-on-both-arms
  that between them created this gap.
- [Item 9](orphaned-update-intents.md) built `RecoveryStats` and the reader flag this reports
  through.
- [Item 47](../known-issues.md#47-a-torn-tail-on-the-active-log-is-counted-as-data-loss) is the
  same distinction drawn wrong in the other direction, on the recovery path.
- [Compaction](../../storage/compaction.md#intent-log-compaction) carries the current code.
- [Observability](../../operations/observability.md) lists what these events emit.
