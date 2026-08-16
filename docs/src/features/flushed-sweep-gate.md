# F5. The flushed sweep runs on a wakeup, not on every message

A write is acknowledged when its bytes are durable, so the shard has to keep asking "what is durable
now?". It used to ask after every single message it handled — 711,638 times in a run that served
617,175 queries — and almost every one of those asks walked every table to learn nothing had changed.
It now asks when a write has landed, or when a log has grown past the size it rotates at, and at no
other time.

## Context

Filed as [O17](../appendix/optimizations.md), ranked **A2**, the head of that page's priority queue
once [F4](validated-archives.md) closed A1. It was the only entry there whose evidence was a profile
rather than an argument, and the reason it counted is worth restating: the
[caveat on profile numbers](../performance/baseline.md#profile--where-the-time-goes) is
about *durations* — the instrumented binary perturbs them — but a **call count is not perturbed**.
Calls against queries was a structural fact about the loop, readable off the profile without
trusting a single timestamp on it.

The entry was filed on `B1`'s 705,886; the `o3-after-repeat` capture immediately before this change
read **711,638 against the same 617,175 queries**. The ~0.8% difference is the count of non-query
messages moving a little between runs, and 711,638 is the figure everything below compares to,
because it is the one taken on the tree this changed.

`Shard::run` called `handle_flushed` unconditionally at the bottom of every loop iteration. From
there it fanned out through the derive-generated `ShoalDatabase::handle_flushed`, into each table's
`get_flushed`, into `StorageSupport::compact_if_needed(false)` — which borrows the shared
`FlushState` for a watermark, tests a rotation threshold, and hands back a `FlushProgress` — and then
back out through a `drain(..)`/`extend` over an empty vec. Every message paid that: every query,
every gathered share, every partition load, and every `DataFlushed` wakeup.

**The costs were two, not one.** The sweep, and the `#[instrument(name = "Shard::handle_flushed")]`
around it. The span is the one that does not show up as a hotpath scope, because it is not one: the
subscriber is a `Registry` with a `fmt` layer filtered at `Info` (`server/trace.rs`), `#[instrument]`
defaults to `INFO`, and `shoal.yml` sets `level: Info` — so the callsite was enabled and every call
allocated a span in the registry's slab, entered it, exited it and closed it. **That cost is in the
uninstrumented binary too**, which is what separates it from everything else on the profile.
[O3](../appendix/optimizations.md) named this same pattern on `RkyvSupport::access` and F4 left it in
place there, because after F4 that span fires once per partition load.

## What it does

**The shard sweeps its tables when the sweep can do something, and skips it otherwise.**

```rust
// sweep our tables only when that sweep could do something
if self.data_flushed || self.tables.compaction_due() {
    // check for any flushed response to handle
    self.handle_flushed().await?;
}
```

`server/shard.rs`. Two conditions, one for each thing the sweep does.

**`data_flushed` is a shard-level flag set by the `ServerMsg::DataFlushed` arm**, which was
previously a literal no-op, and cleared by `handle_flushed` on entry. It answers "can a pending
response have become releasable?". Nothing else can make one releasable — see
[Invariants](#invariants-to-uphold).

**`compaction_due` is a new synchronous predicate**, threaded through every layer between the shard
and the writer:

| Layer | Method |
| --- | --- |
| `StorageSupport` (`tables/storage.rs`) | `fn compaction_due(&self) -> bool` |
| `FileSystem` (`tables/storage/fs.rs`) | `get_unflushed_pos() > table_conf.latency_sensitive.intent_log_size` |
| `PersistentSortedTable` / `PersistentUnsortedTable` | forward to their storage |
| `ShoalDatabase` (`shared/traits.rs`) | generated per field by `shoal-derive`, short-circuiting on the first table that says yes |

It is the same test `compact_if_needed` already made, factored out so both call one predicate.
`get_unflushed_pos()` is `file_pos + buff_pos`: two field reads, no borrow of the shared flush state,
no await. That is what makes it cheap enough to ask on a path that runs per message.

**The `#[instrument]` on `handle_flushed` is gone.** `Shard::reply` parents itself off the query's
own span (`#[instrument(parent = &span, ..)]`) rather than off the ambient one, so no reply is
orphaned by its absence. The `hotpath::measure` attribute stays — the call count is how this change
is measured, and deleting it would delete the evidence.

**The post-loop call stays unconditional.** Shutdown has to drain whatever is still pending whether
or not a wakeup happened to arrive for it.

## Design choices

**The gate is exact rather than approximate.** [O17](../appendix/optimizations.md) proposed "a dirty
flag, or gate it on `DataFlushed` having arrived", and accepted a tradeoff: "a compaction check is
delayed by at most one message". That tradeoff is not paid here. Rotation is driven by bytes
*accepted*, not bytes durable, so a `DataFlushed`-only gate really would delay it — but the accepted
byte count is two field reads away, so rotation fires on exactly the message it fired on before.

**That is not a purity argument, it is a test-suite argument.** `build_pressured_config`
(`shoal/tests/utils.rs`) sets `intent_log_size` to 4 KiB precisely to force rotation every few
writes, and four eviction tests plus `empty_rotated_intent_logs_are_deleted` are built on that
cadence. A gate that let rotation drift to buffer-flush cadence would change what those tests
exercise while leaving them green, which is worse than breaking them.

**The flag is on the shard, not on the tables.** A durable watermark belongs to one intent log per
table, so a per-table flag looks more precise. It is not worth it: the message carries no table id
(and [deliberately carries no position either](../storage/intent-log.md)), so a per-table flag would
have to be set on all of them anyway.

**Both halves of the gate are cheap in the same way.** `data_flushed` is a bool; `compaction_due` is
a `bool`-returning `fn`, not an `async fn`, all the way down. If either had needed an await, the gate
would have cost roughly what it saves.

## Alternatives rejected

**Gate on `DataFlushed` alone.** The change O17 actually described, and the cheapest possible diff.
Rejected for the rotation cadence above.

**An early-out inside `get_flushed`, keyed on `self.pending.is_empty()`.** Superficially the natural
place — the table knows whether it has anything parked. It does almost nothing: a write query has
*just* pushed onto `pending`, so on the messages that dominate this workload the queue is non-empty
and the early-out never fires. What decides whether a response can be released is the watermark
moving, not the queue being occupied, and those are not the same question.

**A flag set by writes instead of by completions.** "Something was written since the last sweep" is
easy to maintain and wrong in the expensive direction. 512,175 of the 617,175 queries in the profiled
run commit, and each of those would set the flag, so the sweep would still run at least half a
million times — most of the calls this removes, still there.

**Drop the span to `level = "trace"` instead of removing it.** Leaves the callsite for a future
debugging session at the cost of a cached interest check per message. Rejected because the span was
never useful — it wraps a function that usually does nothing, has no fields (`skip(self)`, no other
arguments), and is not a parent to anything. A span that only ever says "I ran" on a path that runs
per message is noise whether or not it is enabled.

**Waking on a timer instead of on completions.** Decouples the sweep from the message loop entirely,
and adds a latency floor to every acknowledgement equal to half the tick. The completion already
knows the exact moment the answer changed; a timer is a worse version of information already in hand.

## Limitations

**This removes CPU from a path that is waiting on storage.** The profile's central finding is that
`write_helper` costs 30.5 ms per call against roughly 350 ns for the insert it persists, so the write
path's wall clock is set by `fdatasync`, not by the CPU beside it. The sweep was real work and it is
gone, but the macro layer is not expected to resolve it — see [Performance](#performance). This is a
smaller change than its call-count delta makes it look.

**The remaining calls are more expensive on average, and that is the point.** Average duration rises
from 1.888 µs to 21.286 µs, because the calls that survive are the ones that release responses or
rotate a log. The trivial no-ops that used to drag the average down are what was removed. Anyone
reading the profile after this needs to know that, or the row looks like a regression.

**A shard that stops receiving messages entirely still stops sweeping** — as it did before, because
the loop blocks on `recv().await`. Nothing is stranded by this: the arrival of the completion is
itself a message.

**[Item 37](../appendix/known-issues.md) is neither fixed nor made worse.** The unbounded
acknowledgement delay for the last writes before a lull is about `tables.flush()` being gated on
`shard_local_rx.is_empty()`, which this does not touch; and its escape hatch — an intent log
rotation, which syncs unconditionally — still fires on exactly the message it fired on before,
because `compaction_due` is exact. A reader who assumes the new gate sits in front of that escape
would be wrong, which is the only reason it is worth saying here.

**Error surfacing is now behind the gate.** `check_error` runs at the top of `compact_if_needed`, so
an IO error reaches the shard when the sweep runs. That is still prompt, but only because both
`DataFlushed` sends fire on their error paths as well as their success paths, which opens the gate —
and because `tables.flush()` on the idle path calls `check_error` too and is not gated. One more
thing resting on invariant 1.

## Invariants to uphold

This is the section to read before changing `tables/storage/fs/stream.rs`.

1. **A durable watermark must never advance without a `ServerMsg::DataFlushed` following it.** This
   is the whole gate. Both watermarks in `FlushState` are covered today:
   - `written_pos` (`Durability::Async`) advances in `write_helper`'s `on_complete`, which then sends
     `DataFlushed` unconditionally — including on the IO error path.
   - `synced_pos` (`Durability::Fsync`) advances inside `start_sync`'s spawned task, which then sends
     `DataFlushed`, also on the error path. `start_sync` early-returns *without* sending when a sync
     is already in flight — that is group commit, and it is safe only because the in-flight sync's
     completion sends its own message and then recurses into another sync when `written_pos >
     synced_pos`, which sends again. **Removing that recursion strands acknowledgements.**
2. **A watermark advanced with no message must be inside a sweep that drains.** One such path exists:
   `sync_blocking`'s `mark_synced`. Both of its callers are covered, and any third one has to be
   checked against this rule. `refresh` runs inside `compact_if_needed`, whose rotation branch calls
   `drain_all` in the same sweep; `StorageSupport::shutdown` runs after the unconditional post-loop
   `handle_flushed`.
3. **`commit` must keep returning a position strictly above the current durable watermark.** This is
   why staging a response cannot make one releasable, and therefore why the gate can ignore write
   messages. It holds for two reasons, and the two unit tests pin one each: `commit` returns
   `get_unflushed_pos()`, which counts bytes staged in a buffer the kernel has not been handed yet
   (`submitting_a_write_does_not_advance_a_watermark` — submission moves neither watermark), and
   `PendingResponse` only releases at or below a watermark it is given
   (`staging_a_response_releases_nothing` — adding to the queue never releases from it). A change
   that made `commit` return a *durable* position would break the gate without failing either test.
4. **`compaction_due` must stay synchronous and allocation-free, at every layer.** It runs on the
   per-message path. An await, a lock, or a borrow of `FlushState` in any implementation turns the
   gate into a cost rather than a saving.
5. **`compaction_due` and `compact_if_needed`'s threshold test must not drift apart.** They are one
   predicate today, called from two places. If a future rotation trigger is added to
   `compact_if_needed` — a time bound, a record count — it has to be added to `compaction_due` in the
   same change, or rotation silently stops firing on that trigger.
6. **The post-loop `handle_flushed` stays unconditional.** It is the only thing that drains pending
   responses at shutdown.
7. **`handle_flushed` must keep clearing `data_flushed` on entry, not at the call site.** There are
   two call sites and only one of them tests the flag.

## Performance

Captured by `scripts/bench.sh` — the shell harness [F7](bench-runner.md) replaced with
`shoal-bench` — `o17-after` against the `o3-after-repeat` capture that preceded it,
on the hardware in [Performance Baseline](../performance/baseline.md).

**The result is the call count, and it was the one thing the profile could adjudicate on its own:**

| `shard::handle_flushed` | Before | After |
| --- | --- | --- |
| Calls | 711,638 | 21,279 |
| Avg | 1.888 µs | 21.286 µs |
| Total, summed over 12 shards | 1.344 s | 0.453 s |
| % of wall clock | 1,113% | 375% |

**97.0% of the calls are gone and 66% of the time in them with it.** The controls in the same capture
say it is the same workload: `handle_query` and `reply` both read 617,175 calls, unchanged to the
call. That second one is also the proof that no response was stranded — every query answered before
is still answered.

**21,279 is lower than one per write, and that is group commit showing up in the count.** The run
made 59,221 `stream::write` calls and 77,515 `start_sync` calls, most of the latter early-returning
into a sync already in flight. Only a sync that *completes* sends a wakeup, so roughly three writes
retire per `DataFlushed`. The gate inherits that batching for free.

**The call count reproduced. Both captures put it at the same place:**

| Capture | Calls | Avg | % of wall clock |
| --- | --- | --- | --- |
| `o17-after` | 21,279 | 21.286 µs | 375% |
| `o17-after-repeat` | 19,910 | 24.861 µs | 410% |

**The macro layer could not see it, exactly as predicted — and proving that took an extra
experiment.** The three sequential captures read 1.800 s (before), 1.833 s, 1.859 s, and the
*minimum* of each five-run set rose monotonically: 1786 ms, 1802 ms, 1832 ms. A consistent upward
drift is not what "inside the noise" looks like, and a change that only removes work cannot cause
one — so rather than write it off, the two binaries were run **interleaved on the same machine
minutes apart**, with the gate condition replaced by `if true ||` to restore the old behaviour and
nothing else touched:

| Pass | Binary | Median of 5 | Range |
| --- | --- | --- | --- |
| 1 | gated | 1,823 ms | 1,800 – 1,869 |
| 2 | gate forced open | 1,820 ms | 1,770 – 1,890 |
| 3 | gated | 1,857 ms | 1,769 – 1,888 |

**The two gated passes differ from each other by 34 ms — more than either differs from the ungated
one — and all three ranges overlap almost entirely.** The macro layer cannot distinguish the two
binaries, and the sequential drift was the machine over a long benchmarking session, not the change.
Which is what this layer was always going to say: the write path waits on `fdatasync`, and the whole
scope was ~112 ms per shard against a 1.8 s run on the *instrumented* build that inflates it.

The drift is recorded rather than smoothed over because the alternative was to publish +1.8% under
"inside the spread" and be wrong about why. Note the A/B isolates the **gate**; the span removal is
present in both arms and is not measured by it — see
[O25](../appendix/optimizations.md#o25-two-instrument-spans-remain-on-per-query-paths) for why that
is hard to measure at all.

**The micro suite is a pure control here**, since every benchmark id is partition-layer and none of
them can reach `shard.rs`, `storage.rs`, or `fs.rs`. Against the trailing baseline, 57 of 59 ids
landed inside the noise band in each capture — **and the two that did not were different ids each
time**:

| Capture | Outside the band |
| --- | --- |
| `o17-after` | `maybe_loaded/exists_key/256` **+9.7%**, `maybe_loaded/get_range_64/16` **+10.8%** |
| `o17-after-repeat` | `get_key/1024` **−19.8%**, `maybe_loaded/get_key/256` **−9.1%** |

Four ids, no overlap, and both of the first capture's moved back inside the band in the second. That
is the confirming repeat doing its job — an outlier that does not reproduce is the machine, which is
the whole reason the protocol requires a second capture. Against the frozen `B1` the `seek_bytes/*`
ids read +17% to +33%, which is [F4](validated-archives.md)'s by-design regression and predates this
entirely.

Captured under the `powersave` governor with EPP `performance`, the same environment as
[F4](validated-archives.md).

## Tests

| Test | What breaks if this is reverted |
| --- | --- |
| `staging_a_response_releases_nothing` (`tables/storage.rs`) | Invariant 3 — that adding to `pending` can never release from it, which is why write messages can be skipped |
| `submitting_a_write_does_not_advance_a_watermark` (`fs/stream_tests.rs`) | Invariant 1 from the other side — that submission moves neither watermark, in both durability modes |
| `watermark_waits_for_contiguous_completions`, `watermark_advances_in_order`, `watermark_is_monotonic` (`fs/stream_tests.rs`) | Pre-existing. A watermark that advanced past in-flight data would release responses the gate never gets told about |
| `writes_survive_eviction`, `delete_survives_eviction`, `get_stops_at_its_limit_under_eviction`, `projection_survives_a_blocked_disk_read` (`shoal/tests/persistent_sorted_table.rs`) | All on `build_pressured_config` with a 4 KiB `intent_log_size`. A gate that delays rotation changes what these exercise; a gate that swallows a wakeup hangs them |
| `empty_rotated_intent_logs_are_deleted` | Rotation cadence — it needs three rotations across three server cycles |
| `multi_log_recovery_keeps_earlier_intents` | A stranded response would leave the log in a state its recovery does not expect |

The gate has no direct test of its own, and that is a real gap rather than an oversight: `Shard` has
no `#[cfg(test)]` module and constructing one needs a live glommio reactor, a ring, and a channel
mesh. The two unit tests above pin the *premises* the gate rests on, which is the part that can break
silently; the integration tests catch the gate itself, but only by hanging.

## Related

- [O17](../appendix/optimizations.md) — the entry this closes, kept struck through
- [F4. Archives are validated once](validated-archives.md) — closed A1, which is what made this the
  head of the queue
- [F3. A three layer performance harness](performance-harness.md) — where the call count came from
- [The intent log](../storage/intent-log.md) — `DataFlushed`, the watermarks, and why the message
  carries no position
- [Performance Baseline](../performance/baseline.md) — the profile, and the finding that
  the write path waits on storage
