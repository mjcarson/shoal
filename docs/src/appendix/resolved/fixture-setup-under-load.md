# 118, 292, 295, 298. Fixture rows failed in their setup on answers a loaded host gives

Each of these rows failed before or beside what it is about. Its setup took an answer the engine
gives rightly under load as a failure, or it compared what the engine changes as it runs. A client
retries what these rows did not, and a comparison of identities is the comparison the row meant.

Filed from item 118's runs (2026-09) and from [Tests That Fail Under Load](../flaky-tests.md)
(292, 295 and 298 from 2026-10-17 on); fixed in the flaky-test plan's second wave (2026-10-27).

## Symptom

- **118**:
  - `mixed_versions_exchange_real_cluster_operations` was refused *StaleVersion … written against
    topology version 15 and the cluster is at 19* where it expected the pinned members named.
  - `unexplained_chunk_is_found_and_reclaimed`, six runs, and `stale_is_not_reported_as_corrupt`,
    four, had their scrub refused *StaleVersion*, one version behind.
- **292**, `canonical_digest_ignores_archive_layout_at_same_boundary`: a setup write answered
  *OutcomeUnknown: the peer did not answer within the deadline*, in twelve runs.
- **295**, `lost_seeds_do_not_rebootstrap_existing_directory`: *node 1 came back with other
  members*, though both lists named the same three. They differed in one member's `free_bytes`, by
  4,096.
- **298**, `a_first_write_queued_past_the_memory_is_refused_retriably`: one of its 4,200 filler
  writes shed, *1024 writes in its log queue … waited 1.29s*, in six runs.

## Cause

- **118.** Every administrative request names the topology version it was written against, and a
  node's view of that version can lag its leader's. The fixture's `scrub_now` asked once, and the
  child's helpers (`admin`, the `REPAIR` and `MOVE` verbs, `plan_op`) asked eight times a tenth of a
  second apart. A lagging view outlasted both. The refusal is right; the budget was a count where an
  operator's tool has a deadline.
- **292.** The row's three rounds of writes and deletes went through node zero under a 3 s deadline
  and returned the first unknown answer as the row's failure, though the row is about digests.
- **295.** The row compared each member's whole record, which carries what the member reports as it
  runs, a disk's free bytes among them.
- **298.** The row sent its 4,200 fillers in one bundle, past the group's log queue of 1,024. A write
  that waits for a place too long is shed, `Shedding`, by design
  ([Resolved #129](overload-sheds.md)), which a client sends again.

## Evidence

**Established by reading the source and the runs' messages**, each named on its item: the refusals
are the engine's rule for each case, and each row's assertion would have passed once its setup
went through.

## The fix

- **118**: `scrub_now` asks again on `StaleVersion` under one operation id, reading the version
  again each time, and the child's four helpers do the same until `ADMIN_RETRY_WINDOW`, 10 s, has
  passed. Since [Resolved #337](initialization-rides-out-an-election.md) they ask `NotLeader` again
  too, for 15 s.
- **292**: the row's setup writes and deletes go through `write_note_eventually` and
  `delete_note_eventually`, which ask again on the codes a group between leaders or a host too
  loaded to answer gives. A write or delete repeated is the same row on every replica. The
  row's routed reads were missed: in the confirmation's A/B (2026-10-27)
  it failed twice in `wait_note_routed`, *the peer did not answer within the deadline*, a read
  through node one forwarded past the cluster's 3 s query deadline. That helper now asks again on
  the same codes until its own deadline, beside the quarantine refusal it already waited out; a
  read says nothing of the copy it did not reach.
- **295**: the row compares the members' identities, their node ids sorted, beside the cluster id it
  already compared. A second cluster would differ in those, which is what the row is about.
- **298**: the fillers go in bundles of 512, each answered before the next, so the log queue never
  fills and none waits for a place. Every one is still minted after the queued write.

## Alternatives rejected

- **Treat a stale version as success** (118). A request refused is not applied, and the row's next
  step depends on it.
- **Lengthen the setup writes' deadline** (292). A loaded host can outrun any deadline; asking again
  is what a client does.
- **Compare the records with their running figures removed** (295). A record grows fields; its
  identities are the claim.

## Invariants to uphold

- **A row's setup asks again what a client would ask again**, under one identity where one exists,
  and fails only on an answer no retry changes.
- **A row compares what its claim names**, not a record that also carries figures that move.

## Still open

- Nothing of these four.

## Tests

| Test | What fails if the fix is reverted |
| --- | --- |
| `unexplained_chunk_is_found_and_reclaimed` and `stale_is_not_reported_as_corrupt` (`cluster_fixture`) | Under load, a scrub asked once at a version one behind is refused `StaleVersion` |
| `mixed_versions_exchange_real_cluster_operations` (`cluster_fixture`) | Under load, an `ACTIVATE` refused for a stale version after eight tries |
| `canonical_digest_ignores_archive_layout_at_same_boundary` (`cluster_fixture`) | Under load, a setup write answered `OutcomeUnknown`, or a routed read past the query deadline |
| `lost_seeds_do_not_rebootstrap_existing_directory` (`cluster_fixture`) | Under load, two reads of a member's free bytes 4,096 apart |
| `a_first_write_queued_past_the_memory_is_refused_retriably` (`cluster_fixture`) | Under load, a filler of the single bundle shed |

## Related

- [Resolved #129](overload-sheds.md), the shed a filler met.
- [Resolved #116](map-version-test-snapshot.md), item 118's shape: a version read once and taken
  for the current one.
- [Tests That Fail Under Load](../flaky-tests.md).
